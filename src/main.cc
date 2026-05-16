#include <argparse/argparse.hpp>
#include <random>

#include "logging.h"
#include "util.h"
#include "worker.h"

int main(int argc, char **argv) {
  argparse::ArgumentParser program("DHT");
  // Injest the arguments
  // Two mandatory id's: 1. id to distinguish roles for each node
  // 2. The global sunlab id's used for establishing tcp connections
  program.add_argument("id").help("Node id.").required().scan<'u', uint64_t>();
  program.add_argument("global_id")
      .help("Global sunlab id.")
      .required()
      .scan<'u', uint64_t>();
  program.add_argument("node_list")
      .help("List of the nodes in the system (global id).");
  // System size
  program.add_argument("system_size")
      .help("Total number of nodes in system including coordinator.")
      .required()
      .scan<'u', uint64_t>();
  // -p: #port
  program.add_argument("-p")
      .help("Port number for tcp comms")
      .default_value(uint16_t{1895})
      .scan<'u', uint16_t>();
  // -n: #ops
  program.add_argument("-n")
      .help("Size of the workload.")
      .default_value(uint64_t{50000})
      .scan<'u', uint64_t>();
  // -k: key range
  program.add_argument("-k")
      .help("key range")
      .default_value(uint64_t{1} << 15)
      .scan<'u', uint64_t>();
  // -r: replication degree
  program.add_argument("-r")
      .help("Degree of replication")
      .default_value(uint64_t{1})
      .scan<'u', uint64_t>();
  // -t: number of threads (default 1)
  program.add_argument("-t")
      .help("number of threads")
      .default_value(uint64_t{8})
      .scan<'u', uint64_t>();
  // -v: verbose mode
  program.add_argument("-v")
      .help("verbose mode")
      .default_value(false)
      .implicit_value(true);

  try {
    program.parse_args(argc, argv);
  } catch (const std::exception &err) {
    std::cerr << err.what() << std::endl;
    std::cerr << program;
    return 1;
  }
  // Parse other nodes
  std::vector<int> nodes;
  std::string other_nodes_str = program.get<std::string>("node_list");
  std::stringstream ss(other_nodes_str);
  std::string item;
  while (std::getline(ss, item, ',')) {
    nodes.push_back(std::stoi(item));
  }

  // Connection map (one per peer):
  // 1. primary -- consensus (1)
  // 2. failure detector sockets (1)
  // 3. barrier sockets (1)
  // e.g. for system_size=10 --> 
  // we expect 9 connections, each with 3 sockets = 27 total sockets
  const int conns_per_node = 3;
  const int testtime_s = 10;

  config_t cfg{program.get<uint64_t>("id"),
               program.get<uint64_t>("global_id"),
               nodes,
               program.get<uint64_t>("system_size"),
               conns_per_node,
               testtime_s,
               program.get<uint16_t>("-p"),
               program.get<uint64_t>("-n"),
               program.get<uint64_t>("-k"),
               program.get<uint64_t>("-r"),
               program.get<uint64_t>("-t"),
               program.get<bool>("-v")};
  LOGGING_INFO("Experiment configuration:", cfg.ToString());

  LOGGING_INFO("Generating workload...");
  // Choosing the value of type int for this experiment
  std::vector<op_bundle<int>> workload;
  std::mt19937_64 rng{std::random_device{}()};
  // We have reserved 0 as a centinal value for null key
  std::uniform_int_distribution<int> key_dist(1, cfg.key_range);
  std::uniform_int_distribution<int> val_dist(0,
                                              std::numeric_limits<int>::max());
  std::uniform_int_distribution<uint64_t> perc_dist(0, 100);

  workload.reserve(cfg.num_ops);
  // shutdown command at the beginning of the workload
  workload.push_back(op_bundle<int>{op_type::SHUTDOWN, {}});
  for (uint64_t i = 0; i < cfg.num_ops - 1; ++i) {
    int r = perc_dist(rng);
    auto kv_0 = kv_pair<int, int>{key_dist(rng), val_dist(rng)};
    std::array<kv_pair<int, int>, MAX_MULTI> kv_list = {
        kv_0, {kNullKey, 0}, {kNullKey, 0}};
    if (r < 20) {
      // Put on single key -- 20%
      workload.push_back(op_bundle<int>{op_type::PUT, kv_list, i});
    } else if (r >= 20 && r < 40) {
      // Put on multiple keys -- 20%
      // Make two more kv's
      auto kv_1 = kv_pair<int, int>{key_dist(rng), val_dist(rng)};
      auto kv_2 = kv_pair<int, int>{key_dist(rng), val_dist(rng)};
      std::array<kv_pair<int, int>, MAX_MULTI> kv_multilist = {kv_0, kv_1,
                                                               kv_2};
      workload.push_back(op_bundle<int>{op_type::MULTI_PUT, kv_multilist, i});
    } else {
      // Get -- 60%
      // for get we only need the key so the value will essentially be ignored
      // for brevity
      workload.push_back(op_bundle<int>{op_type::GET, kv_list, i});
    }
  }

#ifdef WORKLOAD_DUMP
  // Dump workload for debugging
  for (size_t i = 0; i < 100; ++i) {
    LOGGING_INFO("Workload[{}]: {}", i, workload[i].ToString());
  }
#endif

  LOGGING_INFO("I am node {} with global id {}", cfg.node_id, cfg.global_id);
  Worker worker(cfg, workload);
  worker.run();
  worker.arrive_barrier();

  LOGGING_INFO("Done. Exiting...");
  return 0;
}