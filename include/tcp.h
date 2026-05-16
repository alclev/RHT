#pragma once

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <fstream>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

#include "logging.h"
#include "util.h"

class TCP_Fully_Connected {
public:
  explicit TCP_Fully_Connected(const config_t &cfg) : cfg_(cfg) {}

  void init() {
    start_server();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    const char *val = std::getenv("USER");
    if (!val)
      LOGGING_FATAL("Could not read USER env variable.");
    std::string env_path = "/home/" + std::string(val) + "/sunlab.env";

    std::fstream file(env_path, std::ios::in);
    if (!file.is_open())
      LOGGING_FATAL("Failed to open env file at path: {}", env_path);

    std::stringstream buf;
    buf << file.rdbuf();
    std::string line;
    while (std::getline(buf, line)) {
      std::stringstream tmp(line);
      std::string id_str;
      tmp >> id_str;
      int id = std::atoi(id_str.c_str());
      if (id == (int)cfg_.global_id)
        continue;
      if (std::find(cfg_.nodes.begin(), cfg_.nodes.end(), id) ==
          cfg_.nodes.end())
        continue;

      std::string host;
      tmp >> host;
      LOGGING_INFO("Connecting to {}...", host);

      addrinfo hints{}, *result = nullptr;
      hints.ai_family = AF_INET;
      hints.ai_socktype = SOCK_STREAM;
      if (getaddrinfo(host.c_str(), std::to_string(cfg_.port).c_str(), &hints,
                      &result) != 0)
        LOGGING_FATAL("Failed to resolve hostname: {}", host);

      for (int i = 0; i < (int)cfg_.conns_per_node; ++i) {
        int fd = socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0)
          LOGGING_FATAL("Failed to create socket: {}", strerror(errno));

        while (::connect(fd, result->ai_addr, result->ai_addrlen) < 0) {
          if (errno != ECONNREFUSED)
            LOGGING_FATAL("Failed to connect to {}: {}", host, strerror(errno));
          std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        // send our global id so receiver can map this fd to us
        send_init(fd, init_msg{cfg_.global_id, (uint64_t)i});

        // limit send buffer for fast failure detection
        int sndbuf = 4096;
        setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf));

        peer_fds_[id].push_back(fd);
      }

      freeaddrinfo(result);
    }

    int expected =
        static_cast<int>((cfg_.system_size - 1) * cfg_.conns_per_node);
    while (num_conns_.load() < expected) {
      LOGGING_DEBUG("Waiting for connections... {}/{}", num_conns_.load(),
                    expected);
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    LOGGING_INFO("All connections established.");
  }

  // returns all fds for a given peer
  std::vector<int> &fds(int node_id) { return peer_fds_.at(node_id); }

  // returns the i-th fd for a given peer
  int fd(int node_id, int i = 0) { return peer_fds_.at(node_id).at(i); }

  std::unordered_map<int, std::vector<int>> &peer_fds() { return peer_fds_; }

private:
  void start_server() {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0)
      LOGGING_FATAL("Failed to create server socket: {}", strerror(errno));

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(cfg_.port);

    if (bind(server_fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0)
      LOGGING_FATAL("Failed to bind: {}", strerror(errno));
    if (listen(server_fd, 128) < 0)
      LOGGING_FATAL("Failed to listen: {}", strerror(errno));

    LOGGING_INFO("Listening on port {}", cfg_.port);

    std::thread([this, server_fd]() {
      while (true) {
        sockaddr_in client_addr{};
        socklen_t len = sizeof(client_addr);
        int fd =
            accept(server_fd, reinterpret_cast<sockaddr *>(&client_addr), &len);
        if (fd < 0)
          continue;

        // read who connected so we can map fd to peer global id
        init_msg msg;
        recv_init(fd, msg);

        {
          std::lock_guard<std::mutex> lock(mu_);
          peer_fds_[(int)msg.node_id].push_back(fd);
        }
        num_conns_.fetch_add(1);
      }
    }).detach();
  }

  config_t cfg_;
  std::unordered_map<int, std::vector<int>> peer_fds_;
  std::atomic<int> num_conns_{0};
  std::mutex mu_;
};