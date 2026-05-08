#pragma once

#include <thread>

#include "hashtable.h"
#include "logging.h"
#include "tcp.h"
#include "util.h"

class Worker {
 public:
  explicit Worker(const config_t& cfg,
                  const std::vector<op_bundle<int>>& workload)
      : cfg_(cfg),
        workload_(workload),
        barrier_count_(0),
        barrier_flag_(false) {
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
    TCP_fully_connected tcp_fc(cfg_);
    tcp_fc.init();

    connections_ = tcp_fc.get_connection_map();

    LOGGING_INFO("Successfully established network connections.");
    // Initialize this nodes portion of the hashtable
    ht_ = new HashTable<int, int>(cfg_, cfg_.key_range);

    std::vector<std::thread> threads;
    // initiate num_threads to HANDLE requests to other nodes
    for (int tid = 0; tid < (int)(cfg_.system_size * cfg_.num_threads); ++tid) {
      uint64_t node_id = tid / cfg_.num_threads;
      uint64_t thread_id = tid % cfg_.num_threads;
      if (node_id == cfg_.node_id) continue;
      threads.emplace_back([&, node_id, thread_id] {
        handle_requests(cfg_.nodes[node_id], thread_id);
      });
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // initiate num_threads to SEND requests to other nodes
    for (int tid = 0; tid < (int)cfg_.num_threads; ++tid) {
      threads.emplace_back([&, tid] { send_requests(tid); });
    }

    for (auto& t : threads) {
      t.join();
    }
    double total_ops = 0;
    double max_time_us = 0;
    double latency_sum = 0;
    uint64_t latency_count = 0;

    for (auto& m : thread_metrics_) {
      total_ops += m.total_ops;
      max_time_us = std::max(max_time_us, m.total_time_us);
      for (auto& l : m.latencies_us) {
        latency_sum += l;
        latency_count++;
      }
    }

    double throughput = total_ops / (max_time_us / 1e6);
    double avg_latency = latency_sum / latency_count;

    LOGGING_INFO("Throughput: {} ops/sec | Avg latency: {} us", throughput,
                 avg_latency);
    // open file to write metrics to csv
    if (cfg_.node_id == 0) {
      std::ofstream metrics_file("metrics.csv");
      metrics_file
          << "Throughput,AvgLatency,NumThreads,RepDegree,KeyRange,SystemSize\n";
      metrics_file << throughput << "," << avg_latency << ","
                   << cfg_.num_threads << "," << cfg_.replication_degree << ","
                   << cfg_.key_range << "," << cfg_.system_size << "\n";
      metrics_file.close();
    }
    LOGGING_INFO("Done. Cleaning up...");
  }

  void send_requests(int tid) {
    thread_metrics metrics;
    std::vector<op_bundle<int>> work_slice;
    try {
      work_slice = workload_slices_.at(tid);
    } catch (const std::out_of_range& e) {
      LOGGING_FATAL("Thread {} failed to get work slice: {}", tid, e.what());
    }
    LOGGING_ASSERT(!work_slice.empty(), "Work slice is empty from the start!");

    auto total_start = std::chrono::high_resolution_clock::now();

    while (!work_slice.empty()) {
      // obtain the next operation to push to the system
      auto op = work_slice.back();
      work_slice.pop_back();
      // Logic needs to be very deliberate here...
      // If we encounter a multi-put or any put for that matter with a rep
      // degree > 1, then we need to invoke 2PC
      auto start = std::chrono::high_resolution_clock::now();
      if (op.type == op_type::MULTI_PUT ||
          (cfg_.replication_degree > 1 && op.type == op_type::PUT)) {
        // Need to create quick mapping of key -> kv_pair
        std::unordered_map<int, kv_pair<int, int>> kv_map;
        for (auto& kv : op.kv_list) {
          kv_map[kv.key] = kv;
        }
        // <target_id, keys>
        std::unordered_map<int, std::vector<int>> targets;
        // indices will each contain vector of target nodes, i.e. the
        // replicas that will be delivering this operation
        for (auto& kv : op.kv_list) {
          // break out of loop early if single-put
          if (kv.key == kNullKey) break;
          std::vector<int> replicas = multi_replicas(kv.key);
          for (auto& r : replicas) {
            int global_id = cfg_.nodes[r];
            targets[global_id].push_back(kv.key);
          }
        }
        // Optimization: two different keys share a target, merge them into the
        // same op bundle
        std::unordered_map<int, op_bundle<int>> target_ops;
        for (auto& kv : targets) {
          op_bundle<int> new_op;
          new_op.type = op_type::MULTI_PUT;
          // fill the entire kv_list
          for (int i = 0; i < MAX_MULTI; ++i) {
            if (i < (int)kv.second.size()) {
              new_op.kv_list[i] = kv_map[kv.second[i]];
            } else {
              new_op.kv_list[i] = kv_pair<int, int>{kNullKey, 0};
            }
          }
          target_ops[kv.first] = new_op;
        }
        atomic_put(target_ops, tid);
      } else {
        // Otherwise, we have a simple single-op that does not need any 2PC
        LOGGING_ASSERT(op.kv_list[0].key != kNullKey,
                       "Single-op: Key of first element in kv list is invalid");
        int node_idx = cfg_.nodes[decide_single_replica(op.kv_list[0].key)];
        if (node_idx == (int)cfg_.global_id) {
          // LOGGING_INFO("Operation mapped to this node: {}", op.ToString());
          op_handler(op, -1, true);
          auto end = std::chrono::high_resolution_clock::now();
          metrics.latencies_us.push_back(
              std::chrono::duration_cast<std::chrono::microseconds>(end - start)
                  .count());
          metrics.total_ops++;
          continue;
        }
        int target = connections_.outbound[node_idx][tid];
        // LOGGING_DEBUG("Thread {} sending op to fd {}", tid, target);
        send_op(target, op);
        op_result<int> res;
        recv_result(target, res);
      }
      auto end = std::chrono::high_resolution_clock::now();
      metrics.latencies_us.push_back(
          std::chrono::duration_cast<std::chrono::microseconds>(end - start)
              .count());
      metrics.total_ops++;
    }

    auto total_end = std::chrono::high_resolution_clock::now();
    metrics.total_time_us =
        std::chrono::duration_cast<std::chrono::microseconds>(total_end -
                                                              total_start)
            .count();
    thread_metrics_[tid] = metrics;

    LOGGING_INFO("Node {} thread {} finished workload", cfg_.node_id, tid);
    // barrier sequence
    local_complete_.fetch_add(1);  // indicate this thread has finished

    if (tid == 0) {
      while (local_complete_.load() < (int)cfg_.num_threads) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
      barrier_flag_.store(true);

      auto shutdown_msg = op_bundle<int>{op_type::SHUTDOWN, {}};
      for (auto& [node_id, fds] : connections_.outbound) {
        for (auto& fd : fds) {
          send_op(fd, shutdown_msg);
        }
      }
    }

    while (!barrier_flag_.load()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  }

  void handle_requests(int node_id, int tid) {
    int fd;
    try {
      fd = connections_.inbound[node_id].at(tid);
    } catch (const std::out_of_range& e) {
      LOGGING_FATAL("Thread {} failed to get sd for node {} : {}", tid, node_id,
                    e.what());
    }
    LOGGING_DEBUG("Listening for incoming connections at sd {} on thread {}",
                  fd, tid);
    while (true) {
      op_bundle<int> incoming;
      recv_op(fd, incoming);
      LOGGING_DEBUG("Thread {} recevieved op: {}", tid, incoming.ToString());
      if (incoming.type == op_type::SHUTDOWN) return;
      op_handler(incoming, fd);
    }
  }

  void op_handler(op_bundle<int>& op, int sender, bool is_local = false) {
    if (op.state != TwoPC::exec_state::NULLOPT) {
      // Then we are handling a 2pc related msg
      if (op.state == TwoPC::exec_state::ARE_YOU_READY) {
        // acquire the locks for relavent keys
        bool all_locked = true;
        int locked_count = 0;
        for (kv_pair kv : op.kv_list) {
          if (kv.key == kNullKey) break;
          if (ht_->acquire_lock(kv.key)) {
            locked_count++;
          } else {
            all_locked = false;
            break;
          }
        }
        if (!all_locked) {
          // release what we got
          int released = 0;
          for (kv_pair kv : op.kv_list) {
            if (kv.key == kNullKey || released >= locked_count) break;
            ht_->release_lock(kv.key);
            released++;
          }
          op.state = TwoPC::exec_state::ABORT;
        } else {
          op.state = TwoPC::exec_state::READY_TO_COMMIT;
        }
        if (!is_local) send_op(sender, op);
      } else if (op.state == TwoPC::exec_state::COMMIT) {
        for (kv_pair kv : op.kv_list) {
          // want to break out of the loop early if we hit null key
          if (kv.key == kNullKey) break;
          // otherwise, we lock
          ht_->put_unlocked(kv.key, kv.value);
        }
        // Now we unlock in seperate phase
        for (kv_pair kv : op.kv_list) {
          // want to break out of the loop early if we hit null key
          if (kv.key == kNullKey) break;
          // otherwise, we lock
          ht_->release_lock(kv.key);
        }
        // Send back an ack indicating op is committed
        op.state = TwoPC::exec_state::ACK;
        if (!is_local) send_op(sender, op);
      } else if (op.state == TwoPC::exec_state::ABORT) {
        for (kv_pair kv : op.kv_list) {
          if (kv.key == kNullKey) break;
          ht_->release_lock(kv.key);
        }
      }
      return;
    }
    switch (op.type) {
      case op_type::GET: {
        // local get on the ht
        std::optional<int> result = ht_->get(op.kv_list[0].key);
        op_result<int> resp;
        if (!result.has_value()) {
          resp.success = false;
          // value doesnt matter, garbage
        } else {
          resp.success = true;
          resp.value = result.value();
        }
        if (!is_local) {
          // send the result back
          send_result(sender, resp);
        }
        break;
      }
      case op_type::PUT: {
        // local put on the ht
        auto& new_kv = op.kv_list[0];
        bool result = ht_->put(new_kv.key, new_kv.value);
        op_result<int> resp;
        // embed the responce in the success flag
        resp.success = result;
        // value doesn't matter here
        if (!is_local) {
          // send the result back
          send_result(sender, resp);
        }
        break;
      }
      case op_type::BARRIER: {
        LOGGING_ASSERT(cfg_.node_id == 0,
                       "Non-coordinator node received barrier cmd.");
        barrier_count_.fetch_add(1);
        LOGGING_INFO("barrier count: {}", barrier_count_.load());
        // send back a shutdown cmd
        // send_op(sender, op_bundle<int>{op_type::SHUTDOWN, {}});
        break;
      }
      default: {
        LOGGING_FATAL("Unknown op_type {}", static_cast<int>(op.type));
      }
    }
  }

  // Function to determine the node_id's of the replicas for the given key
  std::vector<int> multi_replicas(int key) {
    // Potential area of optimization: heavy heap allocation in critical path
    std::vector<int> primary_relicas;
    primary_relicas.reserve(cfg_.replication_degree);

    int primary = decide_single_replica(key);
    // we interate exactly replication_degree times
    for (int i = 0; i < (int)cfg_.replication_degree; ++i) {
      primary_relicas.push_back((primary + i) % cfg_.system_size);
    }
    return primary_relicas;
  }

  // Return the idx for nodes list
  int decide_single_replica(int key) {
    auto it = std::upper_bound(boundaries_.begin(), boundaries_.end(), key);
    return std::distance(boundaries_.begin(), it) - 1;
  }

  // Function that implements an atomic put over a variable number of target
  // nodes using 2PC
  void atomic_put(std::unordered_map<int, op_bundle<int>>& target_ops,
                  int tid) {
    std::vector<int> targets;
    targets.reserve(target_ops.size());
    for (auto& [global_id, op] : target_ops) {
      targets.push_back(global_id);
    }
    bool committed = false;
    uint64_t attempt = 0;

    while (!committed) {
      uint64_t txn_id = ((uint64_t)tid << 32) | attempt++;

      // ###################### Prepare ######################
      for (auto& [global_id, op] : target_ops) {
        op.state = TwoPC::exec_state::ARE_YOU_READY;
        op.id = txn_id;
        if (global_id == (int)cfg_.global_id) {
          op_handler(op, -1, true);
        } else {
          int fd = connections_.outbound[global_id][tid];
          send_op(fd, op);
        }
      }

      size_t ready_count = 0;
      bool aborted = false;
      std::vector<bool> done(targets.size(), false);
      op_bundle<int> incoming;

      while (ready_count < targets.size() && !aborted) {
        for (int i = 0; i < (int)targets.size(); ++i) {
          if (done[i]) continue;
          if (targets[i] == (int)cfg_.global_id) {
            // check if local op_handler aborted
            if (target_ops[targets[i]].state == TwoPC::exec_state::ABORT) {
              aborted = true;
              break;
            }
            ready_count++;
            done[i] = true;
            continue;
          }
          int fd = connections_.outbound[targets[i]][tid];
          recv_op(fd, incoming);
          // Stale response from previous attempt, discard
          if (incoming.id != txn_id) continue;

          if (incoming.state == TwoPC::exec_state::READY_TO_COMMIT) {
            ready_count++;
            done[i] = true;
          } else if (incoming.state == TwoPC::exec_state::ABORT) {
            aborted = true;
            break;
          }
        }
      }

      if (aborted) {
        for (int i = 0; i < (int)targets.size(); ++i) {
          if (targets[i] == (int)cfg_.global_id) {
            auto& op = target_ops[targets[i]];
            op.state = TwoPC::exec_state::ABORT;
            op_handler(op, -1, true);
          } else {
            auto& op = target_ops[targets[i]];
            op.state = TwoPC::exec_state::ABORT;
            int fd = connections_.outbound[targets[i]][tid];
            send_op(fd, op);
          }
        }
        std::this_thread::sleep_for(std::chrono::microseconds(50));
        continue;
      }

      // ###################### Commit ######################
      for (auto& [global_id, op] : target_ops) {
        op.state = TwoPC::exec_state::COMMIT;
        op.id = txn_id;
        if (global_id == (int)cfg_.global_id) {
          op_handler(op, -1, true);
        } else {
          int fd = connections_.outbound[global_id][tid];
          send_op(fd, op);
        }
      }

      std::fill(done.begin(), done.end(), false);
      int commit_count = 0;
      while (commit_count < (int)targets.size()) {
        for (int i = 0; i < (int)targets.size(); ++i) {
          if (done[i]) continue;
          if (targets[i] == (int)cfg_.global_id) {
            commit_count++;
            done[i] = true;
            continue;
          }
          int fd = connections_.outbound[targets[i]][tid];
          recv_op(fd, incoming);
          if (incoming.id != txn_id) continue;
          if (incoming.state == TwoPC::exec_state::ACK) {
            commit_count++;
            done[i] = true;
          }
        }
      }
      committed = true;
    }
  }

 private:
  config_t cfg_;
  std::vector<op_bundle<int>> workload_;
  std::vector<std::vector<op_bundle<int>>> workload_slices_;
  // <node id, socket_descriptors>
  connection_map connections_;
  // this node's portion of the hashtable
  HashTable<int, int>* ht_;
  // Boundaries used for deciding target and replica nodes based on key
  std::vector<int> boundaries_;

  std::atomic<int> local_complete_;
  std::atomic<int> barrier_count_;
  std::atomic<bool> barrier_flag_;

  std::vector<thread_metrics> thread_metrics_;
};