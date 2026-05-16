#pragma once

#include <immintrin.h>
#include <queue>
#include <thread>

#include "consensus.h"
#include "hashtable.h"
#include "logging.h"
#include "tcp.h"
#include "util.h"
#include <unordered_set>

class Worker {
public:
  explicit Worker(const config_t &cfg,
                  const std::vector<op_bundle<int>> &workload)
      : cfg_(cfg), workload_(workload), failure_detected_(cfg.system_size),
        log_offset_(0), thread_metrics_(cfg_.num_threads) {
    // Were are dividing the workload into even chunks for each of the threads,
    // and caching for later use

    // Establish static boundaries and shard_size
    auto num_shards = std::floor(cfg_.system_size / cfg_.replication_degree);
    uint64_t shard_size = cfg_.key_range / num_shards;
    boundaries_.reserve(num_shards);
    for (size_t i = 0; i < num_shards; ++i) {
      boundaries_.push_back(i * shard_size);
    }
    node_priority_lists_.reserve(num_shards);
    for (size_t i = 0; i < num_shards; ++i) {
      std::vector<int> priority_list;
      // although we only need rep degree here, to make fully fault tolerant I
      // opt to include the entire system list, totally ordered by node id
      for (size_t j = 0; j < cfg_.system_size; ++j) {
        priority_list.push_back((i + j) % cfg_.system_size);
        LOGGING_DEBUG("Node {} added to priority list of shard {}",
                      (i + j) % cfg_.system_size, i);
      }
      node_priority_lists_.push_back(priority_list);
    }
    // initialize failure_detected_ to false for all nodes
    for (auto &fd : failure_detected_) {
      fd.store(false);
    }
  }

  ~Worker() {
    threads_running_.store(false, std::memory_order_release);
    commit_handler_.join();
    failure_detector_thread_.join();

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

      barrier_sds_[global_to_local(kv.first)] = kv.second[2];
    }
    std::chrono::time_point<std::chrono::high_resolution_clock> failover_start;

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
    paxos_ = new Paxos<int>(cfg_, primary_sds_, failure_detected_);

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
        // if (op.type == op_type::SHUTDOWN) {
        //   LOGGING_INFO(
        //       "Received SHUTDOWN command. Exiting commit handler thread.");
        //   break;
        // }
        op_handler(op);
        log_offset_++;
      }
    });

    // spawn failure detector thread to monitor leader
    failure_detector_thread_ = std::thread([this]() {
      // while (threads_running_.load(std::memory_order_acquire)) {
      //   // If the system is not stable, current_leader_id_ won't be populated
      //   if (!system_stable_.load(std::memory_order_acquire)) {
      //     _mm_pause();
      //     continue;
      //   }
      //   if (is_leader_) {
      //     // we are the leader, so send heartbeats
      //     auto hb_op = op_bundle<int>{op_type::HEARTBEAT, {}, 0, 0};
      //     for (auto &[id, conn] : fail_detector_sds_) {
      //       if (failure_detected_[id].load(std::memory_order_acquire))
      //         continue;
      //       bool success = send_op(conn, hb_op);
      //       if (!success) {
      //         failure_detected_[id].store(true, std::memory_order_release);
      //       }
      //     }
      //   } else {
      //     // we are a follower, listen for heartbeats
      //     auto hb_op = op_bundle<int>{};
      //     int leader_id = current_leader_id_.load(std::memory_order_acquire);
      //     auto conn = fail_detector_sds_.at(leader_id);
      //     // quick check to see if it hasnt already been marked as failed by
      //     the
      //     // main thread
      //     if (failure_detected_[leader_id].load(std::memory_order_acquire))
      //       continue;

      //     bool success = recv_op(conn, hb_op);
      //     if (!success) {
      //       LOGGING_DEBUG("Leader failed, triggering failure protocol...");
      //       failure_detected_[leader_id].store(true,
      //       std::memory_order_release); system_stable_.store(false,
      //       std::memory_order_release);
      //     }
      //   }
      // }
    });

    // metrics
    std::vector<double> latencies_us;
    latencies_us.reserve(cfg_.num_ops);
    uint64_t committed_ops = 0;

    auto start_total = std::chrono::high_resolution_clock::now();
    while (threads_running_.load(std::memory_order_acquire)) {
      auto lat_start = std::chrono::high_resolution_clock::now();
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
        if ((int)cfg_.node_id == new_leader_id) {
          // start the prepare phase
          prepared = paxos_->Prepare();
          if (prepared) {
            // we successfully prepared
            is_leader_.store(true);
            current_leader_id_.store(cfg_.node_id, std::memory_order_release);
            system_stable_.store(true, std::memory_order_release);
            if (cfg_.node_id != 0) {
              LOGGING_INFO("[FAILOVER] {}",
                           std::chrono::duration<double>(
                               std::chrono::high_resolution_clock::now() -
                               failover_start)
                               .count());
            }

            LOGGING_INFO("I am the leader now!");
            LOGGING_INFO("[LEADER ELECTION] System stable.");
            // embed the node id in txn id of op_bundle for NEW_LEADER msg
            op_bundle<int> op{op_type::NEW_LEADER, {}, cfg_.node_id, 0};
            // broadcast that you are the new leader
            for (auto [id, conn] : primary_sds_) {
              if (failure_detected_[id].load(std::memory_order_acquire))
                continue;
              bool success = send_op(conn, op);
              if (!success) {
                LOGGING_DEBUG("[FAILURE DETECTOR] Node {} failed", id);
                failure_detected_[id].store(true, std::memory_order_release);
              }
            }
            // wait for acks
            int num_acks = 0;
            for (auto [id, conn] : primary_sds_) {
              if (failure_detected_[id].load(std::memory_order_acquire))
                continue;
              op_bundle<int> ack;
              bool success = recv_op(conn, ack);
              if (!success) {
                LOGGING_DEBUG("[FAILURE DETECTOR] Node {} failed", id);
                failure_detected_[id].store(true, std::memory_order_release);
                continue;
              }
              if (ack.type == op_type::ACK)
                num_acks++;
            }
            LOGGING_ASSERT(num_acks >= (int)paxos_->quorum(),
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
              current_leader_id_.store(new_leader_id,
                                       std::memory_order_release);
              system_stable_.store(true, std::memory_order_release);

              // ack back
              op_bundle<int> ack{op_type::ACK, {}, leader_msg.id, 0};

              // presumably the new leader is alive
              bool success = send_op(primary_sds_.at(new_leader_id), ack);
              if (!success) {
                LOGGING_DEBUG("[FAILURE DETECTOR] Leader {} failed!",
                              new_leader_id);
                failure_detected_[new_leader_id].store(
                    true, std::memory_order_release);
              }
            }
          }
        }
      }
      // otherwise, we are stable and promise regardless
      if (is_leader_.load(std::memory_order_acquire)) {
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
        latencies_us.push_back(
            std::chrono::duration<double, std::micro>(
                std::chrono::high_resolution_clock::now() - lat_start)
                .count());
        committed_ops++;

      } else {

        int leader_id = current_leader_id_.load(std::memory_order_acquire);
        LOGGING_DEBUG("[FOLLOWER] Waiting for commit from leader {}...",
                      leader_id);
        bool ok = paxos_->AcceptAck(primary_sds_.at(leader_id));
        // LOGGING_INFO("AcceptAck return: {}", ok ? "true" : "false");
        if (!ok) {
          system_stable_.store(false, std::memory_order_release);
          // I know that leader 0 will be the one that fails. slighly hacky
          failure_detected_[current_leader_id_.load(std::memory_order_acquire)]
              .store(true, std::memory_order_release);
          failover_start = std::chrono::high_resolution_clock::now();
          LOGGING_INFO("Failed to AcceptAck() in follower path");

          continue;
        }

        {
          std::lock_guard<std::mutex> lock(commit_queue_mu_);
          commit_queue_.push(paxos_->getLastAccepted());
        }
      }
    }
    auto end_total = std::chrono::high_resolution_clock::now();
    double total_time_s =
        std::chrono::duration<double>(end_total - start_total).count();
    double throughput = committed_ops / total_time_s;
    std::tuple<double, double, double, double> results;
    std::function<void(std::tuple<double, double, double, double> * result,
                       std::vector<double> & latencies)>
        calc = CALC_LATENCY;
    calc(&results, latencies_us);
    // system_size, replication_degree, key_range, lat_us_avg, lat_us_p90,
    // lat_us_p99, election_lat, thru_avg_ops_s
#if LOG_LEVEL == RELEASE
    if (is_leader_) {
      std::stringstream ss;
      ss << cfg_.system_size << "," << cfg_.replication_degree << ","
         << cfg_.key_range << "," << std::get<0>(results) << ","
         << std::get<1>(results) << "," << std::get<2>(results) << ","
         << std::get<3>(results) << "," << latencies_us.front() << ","
         << throughput << std::endl;
      LOGGING_INFO("[PARSE] {}", ss.str());
    }
#else
    if (is_leader_) {
      std::stringstream ss;
      ss << "System size: " << cfg_.system_size << "\n"
         << "Replication degree: " << cfg_.replication_degree << "\n"
         << "Key range: " << cfg_.key_range << "\n"
         << "Avg latency (us): " << std::get<0>(results) << "\n"
         << "P90 latency (us): " << std::get<1>(results) << "\n"
         << "P99 latency (us): " << std::get<2>(results) << "\n"
         << "Throughput (ops/s): " << throughput << std::endl;
      LOGGING_INFO("{}", ss.str());
    }
#endif
    LOGGING_INFO("Done. Cleaning up...");
  }

  void op_handler(op_bundle<int> &op) {
    switch (op.type) {
    case op_type::SHUTDOWN: {
      LOGGING_INFO("Received SHUTDOWN command. Exiting commit handler thread.");
      threads_running_.store(false, std::memory_order_release);
      break;
    }
    case op_type::GET: {
      LOGGING_DEBUG("[COMMIT HANDLER] Handling GET operation:\n{}",
                    op.ToString());
      int key = op.kv_list[0].key;
      int shard = select_shard(key);
      auto &priority_lst = node_priority_lists_[shard];

      // find the highest priority replica that is alive using failure_detected_
      int target = -1;
      for (int node_id : priority_lst) {
        if (!failure_detected_[node_id].load(std::memory_order_acquire)) {
          target = node_id;
          break;
        }
      }
      LOGGING_DEBUG("GET operation with key {} will be served by node {}", key,
                    target);
      if ((int)cfg_.node_id == target) {
        // we are the target, so we can serve the read locally
        [[maybe_unused]] auto val = ht_->get(key);
        LOGGING_DEBUG("GET result for key {} is {}", key,
                      val.has_value() ? std::to_string(val.value())
                                      : "nullopt");
      }
      break;
    }
    case op_type::PUT: {
      LOGGING_DEBUG("[COMMIT HANDLER] Handling PUT operation:\n{}",
                    op.ToString());
      int key = op.kv_list[0].key;
      int shard = select_shard(key);
      auto &priority_lst = node_priority_lists_[shard];
      std::vector<int> targets;
      for (int node_id : priority_lst) {
        if (!failure_detected_[node_id].load(std::memory_order_acquire)) {
          LOGGING_DEBUG(
              "PUT operation with key {} will be replicated to node {}", key,
              node_id);
          targets.push_back(node_id);
          if (targets.size() >= cfg_.replication_degree) {
            break;
          }
        }
      }
      if (std::find(targets.begin(), targets.end(), cfg_.node_id) !=
          targets.end()) {
        // we are a target, so we should apply the write locally
        ht_->put(key, op.kv_list[0].value);
      }
      // not a target, move on
      break;
    }
    case op_type::MULTI_PUT: {
      LOGGING_DEBUG("[COMMIT HANDLER] Handling MULTI_PUT operation:\n{}",
                    op.ToString());
      auto key_1 = op.kv_list[0].key;
      auto key_2 = op.kv_list[1].key;
      auto key_3 = op.kv_list[2].key;
      std::vector<int> keys{key_1, key_2, key_3};

      if (key_1 == key_2 || key_1 == key_3 || key_2 == key_3) {
        break; // duplicate keys, ignore
      }

      // for 3 different keys
      std::vector<std::vector<int>> targets(3);

      for (size_t i = 0; i < keys.size(); ++i) {
        int shard = select_shard(keys[i]);
        auto &priority_lst = node_priority_lists_[shard];

        for (int node_id : priority_lst) {
          if (targets[i].size() >= cfg_.replication_degree)
            break;
          if (!failure_detected_[node_id].load(std::memory_order_acquire)) {
            targets[i].push_back(node_id);
          }
        }
      }

      // commit the ops
      for (int i = 0; i < (int)keys.size(); ++i) {
        if (std::find(targets[i].begin(), targets[i].end(), cfg_.node_id) !=
            targets[i].end()) {
          // we are a target, so we should apply the write locally
          ht_->put(keys[i], op.kv_list[i].value);
        }
      }

      break;
    }
    default:
      LOGGING_DEBUG("[COMMIT HANDLER] Unknown operation type {}",
                    static_cast<int>(op.type));
    }
  }

  void arrive_barrier() {
    LOGGING_INFO("Arriving at barrier. Waiting for other nodes...");
    uint64_t expected = (cfg_.system_size - 1);

    for (auto &[id, conn] : barrier_sds_) {
      if (failure_detected_[id].load(std::memory_order_acquire))
        continue;
      op_bundle<int> barrier_msg{op_type::BARRIER, {}, 0, 0};
      bool success = send_op(conn, barrier_msg);
      if (!success) {
        LOGGING_DEBUG("[FAILURE DETECTOR] Node {} failed", id);
        failure_detected_[id].store(true, std::memory_order_release);
      }
    }

    for (auto &[id, conn] : barrier_sds_) {
      op_bundle<int> ack;
      if (failure_detected_[id].load(std::memory_order_acquire))
        continue;
      bool success = recv_op(conn, ack);
      if (!success) {
        LOGGING_DEBUG("[FAILURE DETECTOR] Node {} failed", id);
        failure_detected_[id].store(true, std::memory_order_release);
        continue;
      }
      if (ack.type == op_type::BARRIER) {
        LOGGING_DEBUG("Received barrier ack from node {}", id);
        expected--;
      }
    }
    // TODO: need to adjust for failures
    LOGGING_DEBUG("Expected barrier acks remaining: {}", expected);
    LOGGING_ASSERT(expected == 0, "Barrier synchronization failed");
  }

private:
  int global_to_local(int global_id) {
    auto it = std::find(cfg_.nodes.begin(), cfg_.nodes.end(), global_id);
    if (it == cfg_.nodes.end()) {
      LOGGING_FATAL("Global id {} not found in node list!", global_id);
    }
    return std::distance(cfg_.nodes.begin(), it);
  }

  // return shard index
  int select_shard(int key) {
    for (size_t i = 0; i < boundaries_.size(); ++i) {
      if (key < boundaries_[i]) {
        return i;
      }
    }
    return boundaries_.size() - 1;
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
  std::unordered_map<int, int> barrier_sds_;

  // leader election
  std::atomic<bool> is_leader_{false};
  std::atomic<bool> system_stable_{false};
  std::atomic<int> current_leader_id_{-1};

  // failure detection
  std::vector<std::atomic<bool>> failure_detected_;
  std::thread failure_detector_thread_;

  // commit handler thread
  std::thread commit_handler_;
  std::queue<op_bundle<int>> commit_queue_;
  std::mutex commit_queue_mu_;
  uint64_t log_offset_;
  std::vector<std::vector<int>> node_priority_lists_;

  std::vector<thread_metrics> thread_metrics_;
};