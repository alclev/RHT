#pragma once
#include <sys/socket.h>

#include <array>
#include <cstdint>
#include <sstream>
#include <string>

static constexpr uint64_t kNull = UINT64_MAX;

#define MAX_MULTI 16
#define kNullKey 0

struct config_t {
  uint64_t node_id;
  uint64_t global_id;
  std::vector<int> nodes;
  uint64_t system_size;
  uint16_t port;
  uint64_t num_ops;
  uint64_t key_range;
  uint64_t replication_degree;
  uint64_t num_threads;
  bool verbose;
  std::string ToString() const {
    std::ostringstream os;
    os << "\n"
       << "Node_id: " << node_id << "\n"
       << "Global_id: " << global_id << "\n"
       << "System size: " << system_size << "\n"
       << "Port: " << port << "\n"
       << "Num_ops: " << num_ops << "\n"
       << "Key_range: " << key_range << "\n"
       << "Replication_degree: " << replication_degree << "\n"
       << "Num_threads: " << num_threads << "\n"
       << "Verbose: " << (verbose ? "true" : "false") << "\n";
    return os.str();
  }
};

struct init_msg {
  uint64_t node_id;
  uint64_t thread_id;
};

enum class op_type : uint8_t {
  GET = 1,
  PUT = 2,
  MULTI_PUT = 3,
  BARRIER = 4,
  SHUTDOWN = 5,
  GET_RESULT = 6,
  PUT_RESULT = 7,
  MULTI_RESUlT = 8
};


// Utility functions for implementing 2pc
namespace TwoPC {

// I want to leave room for introducing extra states if necessary, later on
enum class exec_state : uint8_t {
  ARE_YOU_READY = 0,
  READY_TO_COMMIT = 1,
  COMMIT = 2, 
  ACK = 3,
  ABORT = 4,
  NULLOPT = 5
};

}  // namespace TwoPC

struct thread_metrics {
    uint64_t total_ops = 0;
    double total_time_us = 0;  
    std::vector<double> latencies_us;
};

template <typename K, typename V>
struct kv_pair {
  K key;
  V value;
  std::string ToString() const {
    std::ostringstream os;
    os << "key: " << key << ", value: " << value;
    return os.str();
  }
};

template <typename T>
struct op_bundle {
  op_type type;
  std::array<kv_pair<int, T>, MAX_MULTI> kv_list;
  TwoPC::exec_state state = TwoPC::exec_state::NULLOPT;
  uint64_t id = 0; // transaction id
  std::string ToString() const {
    std::ostringstream os;
    os << "op_type: " << static_cast<int>(type) << "\n";
    int i = 0;
    while (i < MAX_MULTI && kv_list[i].key != kNullKey) {
      os << "kv_list[" << i << "]: (" << kv_list[i].key << ", "
         << kv_list[i].value << ")\n";
      ++i;
    }
    return os.str();
  }
};

template <typename T>
struct op_result {
  T value;
  bool success;
};

template <typename T>
void send_result(int fd, const op_result<T>& res) {
  static_assert(std::is_trivially_copyable_v<op_result<T>>);
  ssize_t n = ::send(fd, &res, sizeof(res), 0);
  if (n != sizeof(res)) {
    perror("send_result: send error");
    std::abort();
  }
}

template <typename T>
void recv_result(int fd, op_result<T>& res) {
  ssize_t n = ::recv(fd, &res, sizeof(res), MSG_WAITALL);
  if (n != sizeof(res)) {
    perror("recv_result: recv error");
    std::abort();
  }
}

template <typename T>
void send_op(int fd, const op_bundle<T>& op) {
  static_assert(std::is_trivially_copyable_v<op_bundle<T>>);
  ssize_t n = ::send(fd, &op, sizeof(op), 0);
  if (n != sizeof(op)) {
    perror("send error");
    std::abort();
  }
}

template <typename T>
void recv_op(int fd, op_bundle<T>& op) {
  ssize_t n = ::recv(fd, &op, sizeof(op), MSG_WAITALL);
  if (n != sizeof(op)) {
    perror("recv error");
    std::abort();
  }
}

inline void send_init(int fd, const init_msg& msg) {
  ssize_t n = ::send(fd, &msg, sizeof(msg), 0);
  if (n != sizeof(msg)) {
    perror("send_init error");
    std::abort();
  }
}

inline void recv_init(int fd, init_msg& msg) {
  ssize_t n = ::recv(fd, &msg, sizeof(msg), MSG_WAITALL);
  if (n != sizeof(msg)) {
    perror("recv_init error");
    std::abort();
  }
}
