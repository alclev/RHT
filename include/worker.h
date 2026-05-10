#pragma once

#include <queue>
#include <thread>

#include "consensus.h"
#include "hashtable.h"
#include "logging.h"
#include "tcp.h"
#include "util.h"

class Worker {
public:
  explicit Worker(const config_t &cfg,
                  const std::vector<op_bundle<int>> &workload)
      : cfg_(cfg), workload_(workload), thread_metrics_(cfg_.num_threads),
        log_offset_(0), failure_detected_(cfg.system_size) {
    // Were are dividing the workload into even chunks for each of the threads,
    // and caching for later use
    int chunk_size = (cfg_.num_ops / cfg_.num_threads);

    // Establish static boundaries and and shard_size
    size_t num_workers = cfg_.system_size;
    uint64_t shard_size = cfg_.key_range / num_workers;
    boundaries_.reserve(num_workers);
    for (size_t i = 0; i < num_workers; ++i) {
      boundaries_.push_back(i * shard_size);
    }
  }

  ~Worker() {
    threads_running_.store(false, std::memory_order_release);
    if (commit_handler_.joinable()) {
      commit_handler_.join();
    }
    delete ht_;
    delete paxos_;
  }

  void run() {
    // TCP initialization
    TCP_Fully_Connected tcp_fc(cfg_);
    tcp_fc.init();

    connections_ = tcp_fc.peer_fds();
    for (const auto &kv : connections_) {
      // auto global_id = kv.first;
      // Designate the first sd for the main thread doing consensus
      primary_sds_[global_to_local(kv.first)] = kv.second[0];
      // Designate the second sd for the failure detector threads
      fail_detector_sds_[global_to_local(kv.first)] = kv.second[1];
    }
#if LOG_LEVEL == DEBUG
    std::stringstream ss;
    ss << "Primary socket descriptors:\n";
    for (const auto &kv : primary_sds_) {
      ss << "(node_id, fd)" << " = (" << kv.first << ", " << kv.second << ")\n";
    }
    ss << "\nFailure-detector socket descriptors:\n";
    for (const auto &kv : fail_detector_sds_) {
      ss << "(node_id, fd)" << " = (" << kv.first << ", " << kv.second << ")\n";
    }
    ss << std::endl;
    LOGGING_INFO("{}", ss.str());
#endif

    LOGGING_INFO("Successfully established network connections.");
    // Initialize this nodes portion of the hashtable
    ht_ = new HashTable<int, int>(cfg_, cfg_.key_range);
    paxos_ = new Paxos<int>(cfg_, primary_sds_);

    // spawn the commit handler thread
    commit_handler_ = std::thread([this]() {
      while (threads_running_.load(std::memory_order_acquire)) {
        op_bundle<int> op;
        {
          std::lock_guard<std::mutex> lock(commit_queue_mu_);
          if (commit_queue_.empty())
            continue;
          op = commit_queue_.front();
          commit_queue_.pop();
        }
        op_handler(op, current_leader_id_);
        log_offset_++;
      }
    });

    // TODO fix this condition -- add timing
    while (true) {
      int new_leader_id = -1;
      if (!system_stable_.load(std::memory_order_acquire)) {
        // system is not stable
        // live node with lowest node id is allowed to prepare for next leader
        // election
        LOGGING_INFO("System is not stable. Starting leader election...");
        for (size_t i = 0; i < failure_detected_.size(); ++i) {
          if (!failure_detected_[i].load(std::memory_order_acquire)) {
            new_leader_id = i;
            break;
          }
        }
        bool prepared = false;
        if (cfg_.node_id == new_leader_id) {
          // start the prepare phase
          prepared = paxos_->Prepare();
          if (prepared) {
            // we successfully prepared
            is_leader_.store(true);
            system_stable_.store(true);
            current_leader_id_ = cfg_.node_id;
            LOGGING_INFO("I am the leader now!");
            // embed the node id in txn id of op_bundle for NEW_LEADER msg
            op_bundle<int> op{op_type::NEW_LEADER, {}, cfg_.node_id, 0};
            // broadcast that you are the new leader
            for (auto [id, conn] : primary_sds_) {
              send_op(conn, op);
            }
            // wait for acks
            int num_acks = 0;
            for (auto [id, conn] : primary_sds_) {
              op_bundle<int> ack;
              recv_op(conn, ack);
              if (ack.type == op_type::ACK)
                num_acks++;
            }
            LOGGING_ASSERT(num_acks >= paxos_->quorum(),
                           "Failed to get quorum acks for leader broadcast");

          } else {
            LOGGING_FATAL("Supposed to prepare and failed at slot {}",
                          log_offset_);
          }
        } else {
          // we are not the new leader, so we wait to respond
          // wait for leader broadcast
          bool acked = paxos_->PrepareAck(primary_sds_.at(new_leader_id));
          if (acked) {
            LOGGING_DEBUG("In follower mode.");
            // wait for NEW_LEADER broadcast
            op_bundle<int> leader_msg;
            recv_op(primary_sds_.at(new_leader_id), leader_msg);
            if (leader_msg.type == op_type::NEW_LEADER) {
              LOGGING_INFO("I acknowledge new leader {}", leader_msg.id);
              is_leader_.store(false);
              system_stable_.store(true);
              current_leader_id_ = new_leader_id;
              // ack back
              op_bundle<int> ack{op_type::ACK, {}, leader_msg.id, 0};
              send_op(primary_sds_.at(new_leader_id), ack);
            }
          }
        }
      }
      // otherwise, we are stable and promise regardless
      if (is_leader_) {
        LOGGING_DEBUG("[LEADER] Staging commit for op id {} at log offset {}",
                      workload_.back().id, log_offset_);
        auto &op = workload_.back();
        workload_.pop_back();
        bool ok = paxos_->Accept(op);
        LOGGING_ASSERT(ok, "Failed to Accept() in leader path");
        {
          std::lock_guard<std::mutex> lock(commit_queue_mu_);
          commit_queue_.push(op);
        }
      } else {
        LOGGING_DEBUG("[FOLLOWER] Waiting for commit from leader {}...",
                      current_leader_id_);
        bool ok = paxos_->AcceptAck(primary_sds_.at(current_leader_id_));
        LOGGING_ASSERT(ok, "Failed to AcceptAck() in follower path");
        {
          std::lock_guard<std::mutex> lock(commit_queue_mu_);
          commit_queue_.push(paxos_->getLastAccepted());
        }
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

      // LOGGING_INFO("Throughput: {} ops/sec | Avg latency: {} us",
      // throughput,
      //              avg_latency);
      // // open file to write metrics to csv
      // if (cfg_.node_id == 0) {
      //   std::ofstream metrics_file("metrics.csv");
      //   metrics_file
      //       <<
      //       "Throughput,AvgLatency,NumThreads,RepDegree,KeyRange,SystemSize\n";
      //   metrics_file << throughput << "," << avg_latency << ","
      //                << cfg_.num_threads << "," << cfg_.replication_degree
      //                <<
      //                ","
      //                << cfg_.key_range << "," << cfg_.system_size << "\n";
      //   metrics_file.close();
      // }
    }
    LOGGING_INFO("Done. Cleaning up...");
  }

  void op_handler(op_bundle<int> &op, int sender, bool is_local = false) {

    switch (op.type) {
    case op_type::GET: {
      LOGGING_DEBUG("[COMMIT HANDLER] Handling GET operation:\n{}",
                    op.ToString());
      break;
    }
    case op_type::PUT: {
      LOGGING_DEBUG("[COMMIT HANDLER] Handling PUT operation:\n{}",
                    op.ToString());
      break;
    }
    case op_type::MULTI_PUT: {
      LOGGING_DEBUG("[COMMIT HANDLER] Handling MULTI_PUT operation:\n{}",
                    op.ToString());
      break;
    }
    default:
      LOGGING_FATAL("[COMMIT HANDLER] Unknown operation type.");
    }
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
  // <node id, socket_descriptors>
  std::unordered_map<int, std::vector<int>> connections_;
  // this node's portion of the hashtable
  HashTable<int, int> *ht_;
  Paxos<int> *paxos_;
  // Boundaries used for deciding target and replica nodes based on key
  std::vector<int> boundaries_;
  std::atomic<bool> threads_running_{true};

  // socket descriptors for the main thread and failure detector
  std::unordered_map<int, int> primary_sds_;
  std::unordered_map<int, int> fail_detector_sds_;

  // leader election
  std::atomic<bool> is_leader_{false};
  std::atomic<bool> system_stable_{false};
  int current_leader_id_ = -1;

  // failure detection
  std::vector<std::atomic<bool>> failure_detected_;

  // commit handler thread
  std::thread commit_handler_;
  std::queue<op_bundle<int>> commit_queue_;
  std::mutex commit_queue_mu_;
  uint64_t log_offset_;

  std::vector<thread_metrics> thread_metrics_;
};