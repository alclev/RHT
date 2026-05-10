#pragma once

#include <thread>

#include "hashtable.h"
#include "logging.h"
#include "tcp.h"
#include "util.h"

class Worker {
public:
  explicit Worker(const config_t &cfg,
                  const std::vector<op_bundle<int>> &workload)
      : cfg_(cfg), workload_(workload) {
    // Were are dividing the workload into even chunks for each of the threads,
    // and caching for later use
    int chunk_size = (cfg_.num_ops / cfg_.num_threads);
    workload_slices_.resize(cfg_.num_threads);
    for (int tid = 0; tid < (int)cfg_.num_threads; ++tid) {
      int offset = chunk_size * tid;
      std::vector<op_bundle<int>> workload_slice(
          workload_.begin() + offset, workload_.begin() + offset + chunk_size);
      workload_slices_.at(tid) = workload_slice;
    }

    // Establish static boundaries and and shard_size
    size_t num_workers = cfg_.system_size;
    uint64_t shard_size = cfg_.key_range / num_workers;
    boundaries_.reserve(num_workers);
    for (size_t i = 0; i < num_workers; ++i) {
      boundaries_.push_back(i * shard_size);
    }
    thread_metrics_.resize(cfg_.num_threads);
  }
  ~Worker() { delete ht_; }

  void run() {
    // TCP initialization
    TCP_Fully_Connected tcp_fc(cfg_);
    tcp_fc.init();

    connections_ = tcp_fc.peer_fds();
    for (const auto &kv : connections_) {
      auto global_id = kv.first;
      // Designate the first sd for the main thread doing consensus
      primary_sds_[global_to_local(kv.first)] = kv.second[0];
      // Designate the second sd for the failure detector threads
      fail_detector_sds_[global_to_local(kv.first)] = kv.second[1];
    }
#if LOG_LEVEL == DEBUG
    std::stringstream ss;
    for (auto kv : connections_) {
      ss << "Node id: " << kv.first << "\n";
      for (int i = 0; i < kv.second.size(); ++i) {
        ss << "sd: " << kv.second[i] << "\n";
      }
    }
    LOGGING_INFO("{}", ss.str());
#endif

    LOGGING_INFO("Successfully established network connections.");
    // Initialize this nodes portion of the hashtable
    ht_ = new HashTable<int, int>(cfg_, cfg_.key_range);
    std::array<kv_pair<int, int>, 3> kvlist = {kv_pair<int, int>{0, 0},
                                               kNullKey, kNullKey};
    if (cfg_.node_id == 0) {
      for (auto [local_id, conn] : primary_sds_) {
        LOGGING_INFO("Sending to local id {}, fd {}", local_id, conn);
        send_op(conn, op_bundle<int>{op_type::GET, kvlist, 0, 0});
        LOGGING_INFO("Sent to local id {}", local_id);
      }
      for (auto [local_id, conn] : primary_sds_) {
        LOGGING_INFO("Waiting for ack from local id {}, fd {}", local_id, conn);
        op_bundle<int> op;
        recv_op(conn, op);
        LOGGING_INFO("Received ack from local id {}", local_id);
      }
    } else {
      LOGGING_INFO("Waiting for op from node 0, fd {}",
                   connections_[cfg_.nodes[0]][0]);
      op_bundle<int> op;
      recv_op(connections_[cfg_.nodes[0]][0], op);
      LOGGING_INFO("Received op, sending ack");
      send_op(connections_[cfg_.nodes[0]][0],
              op_bundle<int>{op_type::GET_RESULT, kvlist, op.id, 0});
      LOGGING_INFO("Ack sent");
    }
    // double total_ops = 0;
    // double max_time_us = 0;
    // double latency_sum = 0;
    // uint64_t latency_count = 0;

    // for (auto& m : thread_metrics_) {
    //   total_ops += m.total_ops;
    //   max_time_us = std::max(max_time_us, m.total_time_us);
    //   for (auto& l : m.latencies_us) {
    //     latency_sum += l;
    //     latency_count++;
    //   }
    // }

    // double throughput = total_ops / (max_time_us / 1e6);
    // double avg_latency = latency_sum / latency_count;

    // LOGGING_INFO("Throughput: {} ops/sec | Avg latency: {} us", throughput,
    //              avg_latency);
    // // open file to write metrics to csv
    // if (cfg_.node_id == 0) {
    //   std::ofstream metrics_file("metrics.csv");
    //   metrics_file
    //       <<
    //       "Throughput,AvgLatency,NumThreads,RepDegree,KeyRange,SystemSize\n";
    //   metrics_file << throughput << "," << avg_latency << ","
    //                << cfg_.num_threads << "," << cfg_.replication_degree <<
    //                ","
    //                << cfg_.key_range << "," << cfg_.system_size << "\n";
    //   metrics_file.close();
    // }
    LOGGING_INFO("Done. Cleaning up...");
  }

  void send_requests(int tid) {
    thread_metrics metrics;
    std::vector<op_bundle<int>> work_slice;
    try {
      work_slice = workload_slices_.at(tid);
    } catch (const std::out_of_range &e) {
      LOGGING_FATAL("Thread {} failed to get work slice: {}", tid, e.what());
    }
    LOGGING_ASSERT(!work_slice.empty(), "Work slice is empty from the start!");

    while (!work_slice.empty()) {
      // obtain the next operation to push to the system
      auto op = work_slice.back();
      work_slice.pop_back();
      // TODO
    }
  }

  void op_handler(op_bundle<int> &op, int sender, bool is_local = false) {
    // TODO
  }

private:
  int global_to_local(int global_id) {
    auto it = std::find(cfg_.nodes.begin(), cfg_.nodes.end(), global_id);
    if (it == cfg_.nodes.end()) {
      LOGGING_FATAL("Global id {} not found in node list!", global_id);
    }
    return std::distance(cfg_.nodes.begin(), it);
  }

  config_t cfg_;
  std::vector<op_bundle<int>> workload_;
  std::vector<std::vector<op_bundle<int>>> workload_slices_;
  // <node id, socket_descriptors>
  std::unordered_map<int, std::vector<int>> connections_;
  // this node's portion of the hashtable
  HashTable<int, int> *ht_;
  // Boundaries used for deciding target and replica nodes based on key
  std::vector<int> boundaries_;

  std::unordered_map<int, int> primary_sds_;
  std::unordered_map<int, int> fail_detector_sds_;

  std::vector<thread_metrics> thread_metrics_;
};