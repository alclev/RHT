#pragma once

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <fstream>
#include <thread>
#include <unordered_map>
#include <vector>

#include "logging.h"
#include "util.h"

struct connection_map {
  std::unordered_map<int, std::vector<int>> inbound;
  std::unordered_map<int, std::vector<int>> outbound;
};

class TCP_fully_connected {
 public:
  explicit TCP_fully_connected(const config_t& cfg) : cfg_(cfg) {}

  void init() {
    start_server();
    // wait for a bit to let the server start
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    // open the env file
    const char* val = std::getenv("USER");
    if (!val) {
      LOGGING_FATAL("Could not read USER env variable.");
    }
    std::string usr(val);
    std::string env_path = "/home/" + usr + "/sunlab.env";
    std::fstream file(env_path, std::ios::in);
    if (!file.is_open()) {
      LOGGING_FATAL("Failed to open env file at path: {}", env_path);
    }
    std::stringstream buf;
    buf << file.rdbuf();
    std::string line;
    while (std::getline(buf, line)) {
      std::stringstream tmp(line);
      std::string id_str;
      tmp >> id_str;
      int id = std::atoi(id_str.c_str());
      if(id == (int)cfg_.global_id) continue;
      // If the id that we observe in the env file exists in our "nodes"
      // list...
      if (std::find(cfg_.nodes.begin(), cfg_.nodes.end(), id) !=
          cfg_.nodes.end()) {
        std::string host;
        tmp >> host;
        LOGGING_INFO("Attempting to connect to {}...", host);
        // Resolve hostname
        addrinfo hints{};
        hints.ai_family = AF_INET;
        hints.ai_socktype = SOCK_STREAM;

        // resolve hostname to raw ip
        addrinfo* result = nullptr;
        if (getaddrinfo(host.c_str(), std::to_string(cfg_.port).c_str(), &hints,
                        &result) != 0) {
          LOGGING_FATAL("Failed to resolve hostname: {}", host);
          continue;
        }

        // creating num_threads connections per host
        for (int i = 0; i < (int)cfg_.num_threads; ++i) {
          int sock_fd = socket(AF_INET, SOCK_STREAM, 0);
          if (sock_fd < 0) {
            LOGGING_FATAL("Failed to create socket: {}", strerror(errno));
            continue;
          }

          if (::connect(sock_fd, result->ai_addr, result->ai_addrlen) < 0) {
            LOGGING_FATAL("Failed to connect to {}: {}", line, strerror(errno));
            close(sock_fd);
            continue;
          }
          outboud_fds_.push_back(sock_fd);
          outbound_map_[id].push_back(sock_fd);
        }
        freeaddrinfo(result);
      }
    }
    int expected = static_cast<int>((cfg_.system_size - 1) * cfg_.num_threads);
    // block until we get the expected number of connections
    while (num_conns_.load() < expected) {
      LOGGING_DEBUG("Waiting for connections... {}/{}", num_conns_.load(),
                    expected);
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    LOGGING_INFO("Received all connections.");
    // connections are established, now we need to order the inbound connections
    // in a map using a handshake technique

    // send init on all outbound fds first
    for (int fd : outboud_fds_) {
      send_init(fd, init_msg{cfg_.global_id, 0});
    }
    // then handle inbound
    for (int fd : inbound_fds_) {
      init_msg ack;
      recv_init(fd, ack);
      inbound_map_[ack.node_id].push_back(fd);
    }

#ifdef DUMP
    // Dump connection map for debugging
    LOGGING_INFO("Inbound connections:");
    for (auto kv : inbound_map_) {
      LOGGING_INFO("Node {}", kv.first);
      for (int i = 0; i < (int)kv.second.size(); ++i) {
        LOGGING_INFO("sd_{}: {}", i, kv.second[i]);
      }
    }
    LOGGING_INFO("Outbound connections:");
    for (auto kv : outbound_map_) {
      LOGGING_INFO("Node {}", kv.first);
      for (int i = 0; i < (int)kv.second.size(); ++i) {
        LOGGING_INFO("sd_{}: {}", i, kv.second[i]);
      }
    }
#endif
  }

  void start_server() {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
      LOGGING_FATAL("Failed to create server socket: {}", strerror(errno));
      return;
    }

    int opt = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) <
        0) {
      LOGGING_FATAL("Failed to set SO_REUSEADDR: {}", strerror(errno));
      close(server_fd);
      return;
    }

    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(cfg_.port);

    if (bind(server_fd, reinterpret_cast<sockaddr*>(&server_addr),
             sizeof(server_addr)) < 0) {
      LOGGING_FATAL("Failed to bind server socket: {}", strerror(errno));
      close(server_fd);
      return;
    }

    if (listen(server_fd, 128) < 0) {
      LOGGING_FATAL("Failed to listen on server socket: {}", strerror(errno));
      close(server_fd);
      return;
    }

    LOGGING_INFO("Server listening on port {}", cfg_.port);

    std::thread([this, server_fd]() {
      while (true) {
        sockaddr_in client_addr{};
        socklen_t client_len = sizeof(client_addr);

        int client_fd = accept(
            server_fd, reinterpret_cast<sockaddr*>(&client_addr), &client_len);
        if (client_fd < 0) {
          LOGGING_INFO("Accept failed: {}", strerror(errno));
          continue;
        }
        inbound_fds_.push_back(client_fd);
        num_conns_.fetch_add(1);
      }
    }).detach();
  }

  connection_map get_connection_map() {
    return connection_map{inbound_map_, outbound_map_};
  }

 private:
  config_t cfg_;
  std::vector<int> inbound_fds_;
  std::vector<int> outboud_fds_;
  std::atomic<bool> server_running_;
  std::atomic<int> num_conns_;
  std::unordered_map<int, std::vector<int>> inbound_map_;
  std::unordered_map<int, std::vector<int>> outbound_map_;
};