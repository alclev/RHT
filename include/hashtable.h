#pragma once

#include <atomic>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>

template <typename K, typename V>
class HashTable {
 public:
  explicit HashTable(const config_t& cfg, size_t buckets = (1ULL << 12),
                     size_t stripes = (1ULL << 16))
      : buckets_(buckets), bucket_count_(buckets), count_(0), cfg_(cfg) {
    // initialize our list of locks
    locks_.reserve(stripes);
    for (size_t i = 0; i < stripes; ++i) {
      locks_.emplace_back(std::make_unique<std::mutex>());
    }
  }

  bool put(const K& key, const V& value) {
    size_t h = hasher_(key);
    size_t s = h % locks_.size();

    {
      // lock using raii
      std::lock_guard<std::mutex> g(*locks_[s]);
      size_t b = h % buckets_.size();
      for (const auto& kv : buckets_[b]) {
        if (kv.first == key) return false;
      }
      buckets_[b].emplace_back(key, value);
    }  // unlock (out of scope)

    if (++count_ > bucket_count_.load() * 2) {
      rehash();
    }
    return true;
  }

  // Special version of put to be used during 2pc bc the lock has already been
  // acquired and we dont want to deadlock
  bool put_unlocked(const K& key, const V& value) {
    size_t h = hasher_(key);
    size_t b = h % buckets_.size();
    for (const auto& kv : buckets_[b]) {
      if (kv.first == key) return false;
    }
    buckets_[b].emplace_back(key, value);

    if (++count_ > bucket_count_.load() * 2) {
      rehash();
    }
    return true;
  }

  std::optional<V> get(const K& key) {
    size_t h = hasher_(key);
    size_t s = h % locks_.size();
    std::lock_guard<std::mutex> g(*locks_[s]);
    size_t b = h % buckets_.size();
    for (const auto& kv : buckets_[b]) {
      if (kv.first == key) return kv.second;
    }
    return std::nullopt;
  }

  bool acquire_lock(const K& key) {
    size_t h = hasher_(key);
    size_t s = h % locks_.size();
    return locks_[s]->try_lock();
  }

  void release_lock(const K& key) {
    size_t h = hasher_(key);
    size_t s = h % locks_.size();
    locks_[s]->unlock();
  }

 private:
  void rehash() {
    for (auto& m : locks_) m->lock();

    size_t current_buckets = buckets_.size();
    if (count_ <= current_buckets * 2) {
      for (auto& m : locks_) m->unlock();
      return;
    }

    size_t new_size = current_buckets * 2;
    std::vector<std::list<std::pair<K, V>>> new_buckets(new_size);

    for (auto& bucket : buckets_) {
      for (auto& kv : bucket) {
        size_t b = hasher_(kv.first) % new_size;
        new_buckets[b].push_back(std::move(kv));
      }
    }

    buckets_ = std::move(new_buckets);
    bucket_count_.store(new_size);

    for (auto& m : locks_) m->unlock();
  }
  std::vector<std::list<std::pair<K, V>>> buckets_;
  std::vector<std::unique_ptr<std::mutex>> locks_;
  std::atomic<size_t> bucket_count_;
  std::atomic<size_t> count_;
  config_t cfg_;
  std::hash<K> hasher_;
};