#pragma once
#include <sys/socket.h>

#include <array>
#include <cstdint>
#include <sstream>
#include <string>

static constexpr uint64_t kNull = UINT64_MAX;
static constexpr uint64_t kTimeout_ms = 500;

#define MAX_MULTI 3
#define kNullKey 0
#define kNullVal 0

struct config_t {
  uint64_t node_id;
  uint64_t global_id;
  std::vector<int> nodes;
  uint64_t system_size;
  uint64_t conns_per_node;
  uint64_t testtime_s;
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
  GET = 0,
  PUT = 1,
  MULTI_PUT = 2,
  GET_RESULT = 3,
  PUT_RESULT = 4,
  MULTI_RESULT = 5,
  NEW_LEADER = 6,
  HEARTBEAT = 7,
  BARRIER = 8,
  SHUTDOWN = 9,
  ACK = 10
};

template <typename K, typename V> struct kv_pair {
  K key;
  V value;
  std::string ToString() const {
    std::ostringstream os;
    os << "key: " << key << ", value: " << value;
    return os.str();
  }
};

template <typename T> struct op_bundle {
  op_type type;                                   // op type
  std::array<kv_pair<int, T>, MAX_MULTI> kv_list; // list of kv-pairs
  uint64_t id = 0;                                // transaction id
  uint64_t num_replicated = 0; // current number of replications

  std::string ToString() const {
    std::ostringstream os;
    os << "------------OP------------" << "\n";
    os << "op_type: " << static_cast<int>(type) << "\n";
    int i = 0;
    while (i < MAX_MULTI && kv_list[i].key != kNullKey) {
      os << "kv_list[" << i << "]: (" << kv_list[i].key << ", "
         << kv_list[i].value << ")\n";
      ++i;
    }
    os << "op id: " << id << "\n";
    os << "num replications: " << num_replicated << '\n';
    os << "--------------------------" << "\n";
    return os.str();
  }
};

template <typename T> struct op_result {
  op_type type;
  T value;
  bool success;
};

// Utility functions for implementing 2pc
namespace TwoPC {

enum class msg_type : uint8_t {
  PREPARE = 0,
  PREPARE_ACK = 1,
  COMMIT = 2,
  COMMIT_ACK = 3,
  ABORT = 4
};

template <typename T> struct msg {
  msg_type type;
  uint64_t txn_id;
  op_bundle<T> op;
};

template <typename T> bool send_msg(int fd, const TwoPC::msg<T> &m) {
  static_assert(std::is_trivially_copyable_v<TwoPC::msg<T>>);
  struct timeval tv;
  tv.tv_sec = kTimeout_ms / 1000;
  tv.tv_usec = (kTimeout_ms % 1000) * 1000;
  setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

  ssize_t n = ::send(fd, &m, sizeof(m), 0);
  if (n != sizeof(m)) {
    if (errno == EAGAIN || errno == EWOULDBLOCK)
      return false;
    perror("TwoPC::send_msg error");
    __builtin_trap();
  }
  return true;
}

template <typename T> bool recv_msg(int fd, TwoPC::msg<T> &m) {
  struct timeval tv;
  tv.tv_sec = kTimeout_ms / 1000;
  tv.tv_usec = (kTimeout_ms % 1000) * 1000;
  setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

  ssize_t n = ::recv(fd, &m, sizeof(m), MSG_WAITALL);
  if (n != sizeof(m)) {
    if (errno == EAGAIN || errno == EWOULDBLOCK)
      return false;
    perror("TwoPC::recv_msg error");
    __builtin_trap();
  }
  return true;
}

} // namespace TwoPC

// Utility functions for implementing consensus
namespace Consensus {

enum class msg_type : uint8_t {
  PREPARE = 0,
  PREPARE_ACK = 1,
  ACCEPT = 2,
  ACCEPT_ACK = 3,
  ABORT = 4
};

template <typename T> struct msg {
  msg_type type;
  // ballot that we are proposing
  uint64_t ballot;
  // highest accepted ballot observed
  uint64_t max_ballot;
  // value associated with max_ballot
  op_bundle<T> val;
};

template <typename T> bool send_msg(int fd, const Consensus::msg<T> &m) {
  static_assert(std::is_trivially_copyable_v<Consensus::msg<T>>);
  struct timeval tv;
  tv.tv_sec = kTimeout_ms / 1000;
  tv.tv_usec = (kTimeout_ms % 1000) * 1000;
  setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

  ssize_t n = ::send(fd, &m, sizeof(m), 0);
  if (n != sizeof(m)) {
    if (errno == EAGAIN || errno == EWOULDBLOCK)
      return false;
    perror("send_msg error");
    __builtin_trap();
  }
  return true;
}

template <typename T> bool recv_msg(int fd, Consensus::msg<T> &m) {
  struct timeval tv;
  tv.tv_sec = kTimeout_ms / 1000;
  tv.tv_usec = (kTimeout_ms % 1000) * 1000;
  setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

  ssize_t n = ::recv(fd, &m, sizeof(m), MSG_WAITALL);
  if (n <= 0 || n != sizeof(m)) {
    return false;
  }
  return true;
}

} // namespace Consensus

struct thread_metrics {
  uint64_t total_ops = 0;
  double total_time_us = 0;
  std::vector<double> latencies_us;
};

template <typename T> bool send_op(int fd, const op_bundle<T> &op) {
  static_assert(std::is_trivially_copyable_v<op_bundle<T>>);
  struct timeval tv;
  tv.tv_sec = kTimeout_ms / 1000;
  tv.tv_usec = (kTimeout_ms % 1000) * 1000;
  setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

  ssize_t n = ::send(fd, &op, sizeof(op), 0);
  if (n <= 0 || n != sizeof(op)) {
    return false;
  }
  return true;
}

template <typename T> bool recv_op(int fd, op_bundle<T> &op) {
  struct timeval tv;
  tv.tv_sec = kTimeout_ms / 1000;
  tv.tv_usec = (kTimeout_ms % 1000) * 1000;
  setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

  ssize_t n = ::recv(fd, &op, sizeof(op), MSG_WAITALL);
  if (n <= 0 || n != sizeof(op)) {
    return false;
  }
  return true;
}

inline bool send_init(int fd, const init_msg &msg) {
  struct timeval tv;
  tv.tv_sec = kTimeout_ms / 1000;
  tv.tv_usec = (kTimeout_ms % 1000) * 1000;
  setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

  ssize_t n = ::send(fd, &msg, sizeof(msg), 0);
  if (n != sizeof(msg)) {
    if (errno == EAGAIN || errno == EWOULDBLOCK)
      return false;
    perror("send_init error");
    __builtin_trap();
  }
  return true;
}

inline bool recv_init(int fd, init_msg &msg) {
  struct timeval tv;
  tv.tv_sec = kTimeout_ms / 1000;
  tv.tv_usec = (kTimeout_ms % 1000) * 1000;
  setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

  ssize_t n = ::recv(fd, &msg, sizeof(msg), MSG_WAITALL);
  if (n != sizeof(msg)) {
    if (errno == EAGAIN || errno == EWOULDBLOCK)
      return false;
    perror("recv_init error");
    __builtin_trap();
  }
  return true;
}

inline void PinToCore(size_t core_id) {
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(core_id, &cpuset);
  pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
}

#define CALC_LATENCY                                                           \
  [&](std::tuple<double, double, double, double> *result,                      \
      std::vector<double> &latencies) {                                        \
    double latency_avg = 0.0;                                                  \
    double latency_stddev = 0.0;                                               \
    double latency_50p = 0.0;                                                  \
    double latency_99p = 0.0;                                                  \
    double latency_99_9p = 0.0;                                                \
    [[maybe_unused]] double latency_max = 0.0;                                 \
    int latency_max_idx = 0;                                                   \
    if (latencies.size() > 0) {                                                \
      latency_avg = std::accumulate(latencies.begin(), latencies.end(), 0.0);  \
      latency_avg /= static_cast<double>(latencies.size());                    \
      latency_stddev = std::accumulate(latencies.begin(), latencies.end(), 0,  \
                                       [latency_avg](double a, double b) {     \
                                         return a + std::abs(latency_avg - b); \
                                       });                                     \
      latency_stddev /= static_cast<double>(latencies.size());                 \
      latency_stddev = std::sqrt(latency_stddev);                              \
      latency_max_idx =                                                        \
          std::distance(latencies.begin(),                                     \
                        std::max_element(latencies.begin(), latencies.end())); \
      latency_max = latencies[latency_max_idx];                                \
      std::sort(latencies.begin(), latencies.end());                           \
      latency_50p =                                                            \
          latencies[static_cast<uint32_t>((latencies.size() * .50))];          \
      latency_99p =                                                            \
          latencies[static_cast<uint32_t>((latencies.size() * .99))];          \
      latency_99_9p =                                                          \
          latencies[static_cast<uint32_t>((latencies.size() * .999))];         \
      *result = std::make_tuple(latency_avg, latency_50p, latency_99p,         \
                                latency_99_9p);                                \
    }                                                                          \
  };
