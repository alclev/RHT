#pragma once

#include <cstdint>

#include "util.h"

template <typename T> class Paxos {
public:
  explicit Paxos(const config_t &cfg, const std::unordered_map<int, int> &conns)
      : cfg_(cfg), system_size_(cfg.system_size), conns_(conns),
        quorum_(quorum()) {}

  bool Prepare() {
    while (true) {
      ballot_ = (ballot_round_++ << 32) | cfg_.node_id;

      Consensus::msg<T> m;
      m.type = Consensus::msg_type::PREPARE;
      m.ballot = ballot_;
      m.max_ballot = 0;

      for (auto &[id, conn] : conns_) {
        Consensus::send_msg(conn, m);
      }

      int acks = 0;
      uint64_t max_ballot_seen = 0;
      op_bundle<T> highest_val{};

      for (auto &[id, conn] : conns_) {
        Consensus::msg<T> ack;
        Consensus::recv_msg(conn, ack);
        if (ack.type == Consensus::msg_type::PREPARE_ACK) {
          if (ack.max_ballot > max_ballot_seen) {
            max_ballot_seen = ack.max_ballot;
            highest_val = ack.val;
          }
          acks++;
        } else if (ack.type == Consensus::msg_type::ABORT) {
          // saw a higher ballot — bump our round past it
          uint64_t their_round = ack.max_ballot >> 32;
          if (their_round >= ballot_round_) {
            ballot_round_ = their_round + 1;
          }
        }
      }

      if (acks >= quorum_) {
        if (max_ballot_seen > 0) {
          pending_val_ = highest_val;
        }
        return true;
      }
      // didn't get quorum, retry with higher ballot
    }
  }
  bool PrepareAck(int leader_fd) {
    Consensus::msg<T> m;
    Consensus::recv_msg(leader_fd, m);

    // reject if ballot is stale
    if (m.ballot < promised_ballot_) {
      Consensus::msg<T> nack;
      nack.type = Consensus::msg_type::ABORT;
      nack.ballot = m.ballot;
      nack.max_ballot = promised_ballot_;
      Consensus::send_msg(leader_fd, nack);
      return false;
    }

    // promise not to accept anything lower
    promised_ballot_ = m.ballot;

    Consensus::msg<T> ack;
    ack.type = Consensus::msg_type::PREPARE_ACK;
    ack.ballot = m.ballot;
    // tell the leader the highest ballot we have already accepted
    ack.max_ballot = accepted_ballot_;
    ack.val = accepted_val_;

    Consensus::send_msg(leader_fd, ack);
    return true;
  }

  bool Accept(op_bundle<T> &op) {
    Consensus::msg<T> m;
    m.type = Consensus::msg_type::ACCEPT;
    m.ballot = ballot_;
    m.val = op;

    // broadcast to all peers
    for (auto &[id, conn] : conns_) {
      Consensus::send_msg(conn, m);
    }

    // collect acks
    int acks = 0;
    for (auto &[id, conn] : conns_) {
      Consensus::msg<T> ack;
      Consensus::recv_msg(conn, ack);
      if (ack.type == Consensus::msg_type::ACCEPT_ACK) {
        acks++;
      }
    }
    return acks >= quorum_;
  }

  bool AcceptAck(int leader_fd) {
    Consensus::msg<T> m;
    Consensus::recv_msg(leader_fd, m);

    // reject if ballot is stale
    if (m.ballot < promised_ballot_) {
      Consensus::msg<T> nack;
      nack.type = Consensus::msg_type::ABORT;
      nack.ballot = m.ballot;
      nack.max_ballot = promised_ballot_;
      Consensus::send_msg(leader_fd, nack);
      return false;
    }

    // accept the value
    accepted_ballot_ = m.ballot;
    accepted_val_ = m.val;

    Consensus::msg<T> ack;
    ack.type = Consensus::msg_type::ACCEPT_ACK;
    ack.ballot = m.ballot;

    Consensus::send_msg(leader_fd, ack);
    return true;
  }

  uint64_t quorum() { return (system_size_ / 2) + 1; }
  op_bundle<T> getLastAccepted() { return accepted_val_; }

private:
  config_t cfg_;
  uint64_t system_size_;
  uint64_t ballot_round_ = 0;
  uint64_t ballot_ = 0;
  uint64_t promised_ballot_ = 0;   
  uint64_t accepted_ballot_ = 0;   
  op_bundle<T> accepted_val_{};    
  op_bundle<T> pending_val_{};     
  uint64_t quorum_;
  std::unordered_map<int, int> conns_;
};