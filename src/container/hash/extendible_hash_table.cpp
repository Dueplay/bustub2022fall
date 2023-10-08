//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <math.h>
#include <sstream>
#include <utility>

#include "common/logger.h"
#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  dir_.push_back(std::make_shared<Bucket>(bucket_size_, 0));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  // std::scoped_lock用于在多线程环境中安全地锁定多个互斥量,并按照给定的锁顺序获取锁。
  // 主要优势是它可以避免死锁，并且在作用域结束时自动释放所有已锁定的互斥量。
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const
    -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  return dir_[IndexOf(key)]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  return dir_[IndexOf(key)]->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);
  while (!dir_[IndexOf(key)]->Insert(key, value)) {
    // bucket full
    // 将dir扩大一倍，满的bucket中的元素重新分到整个dir中
    auto index = IndexOf(key);
    auto bucket_ptr = dir_[index];
    auto global_extend = false;
    if (bucket_ptr->GetDepth() == global_depth_) {
      global_depth_++;
      dir_.resize(std::pow(2, global_depth_), nullptr);
      global_extend = true;
    }

    RedistributeBucket(bucket_ptr, index);

    if (global_extend) {
      auto curr_dir_size = dir_.size();
      auto old_dir_size = curr_dir_size >> 1;
      for (auto i = old_dir_size; i < curr_dir_size; i++) {
        if (dir_[i] == nullptr) {
          dir_[i] = dir_[i - old_dir_size];
        }
      }
    }
  }
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket(
    std::shared_ptr<Bucket> bucket, int64_t index) -> void {
  auto old_depth = bucket->GetDepth();
  int64_t old_gap = std::pow(2, old_depth);
  bucket->IncrementDepth();

  // this overflow bucket will be split into two bucket
  auto even_new_bucket =
      std::make_shared<Bucket>(bucket_size_, bucket->GetDepth());
  auto odd_new_bucket =
      std::make_shared<Bucket>(bucket_size_, bucket->GetDepth());
  num_buckets_++;

  // redistribute the directory pointer for this bucket‘s split
  for (u_int64_t i = index % old_gap; i < dir_.size(); i += old_gap) {
    if (dir_[i] == nullptr || dir_[i] == bucket) {
      auto prefix_bit = (i >> old_depth) & 1;
      dir_[i] = (prefix_bit == 0) ? even_new_bucket : odd_new_bucket;
    }
  }

  for (auto it = bucket->list_.rbegin(); it != bucket->list_.rend(); it++) {
    // each will have a new hash index, re-distribute items in old bucket
    auto item_index = IndexOf(it->first);
    dir_[item_index]->Insert(it->first, it->second);
  }
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::PrettyPrint() {
  std::scoped_lock<std::mutex> lock(latch_);

  std::string buffer =
      "\n\t Global depth " + std::to_string(GetGlobalDepthInternal()) +
      ", Dir size " + std::to_string(dir_.size()) + ", Number of buckets " +
      std::to_string(GetNumBucketsInternal());
  for (size_t i = 0; i < dir_.size(); ++i) {
    buffer += "\n\t\t dir[" + std::to_string(i) + "]: Local depth " +
              std::to_string(GetLocalDepthInternal(i)) + ", elements [ ";
    std::ostringstream oss;
    auto &items = dir_[i]->GetItems();
    for (auto it = items.begin(); it != items.end(); ++it) {
      oss << it->first << ' ';
    }
    buffer += oss.str() + "]";
  }

  LOG_INFO("HashTable stats: %s", buffer.c_str());
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth)
    : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  // find_if 将*first作为参数传给pred
  /*
  template<class InputIterator, class UnaryPredicate>
  InputIterator find_if (InputIterator first, InputIterator last, UnaryPredicate
  pred)
  {
    while (first!=last) {
      if (pred(*first)) return first;
      ++first;
    }
    return last;
  }
  */
  auto it = std::find_if(
      list_.begin(), list_.end(),
      [key](const std::pair<K, V> &element) { return element.first == key; });
  if (it != list_.end()) {
    value = it->second;
    return true;
  } else {
    return false;
  }
  // simple
  /*
  for (const auto &[k, v] : list_) {
    if (key == k) {
      value = v;
      return true;
    }
  }
  return false; // not found
  */
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  // std 算法库
  auto it = std::find_if(
      list_.begin(), list_.end(),
      [key](const std::pair<K, V> &element) { return element.first == key; });
  if (it != list_.end()) {
    list_.erase(it);
    return true;
  } else {
    return false;
  }
  // 遍历的方法
  /*
  for (auto it = list_.cbegin(); it != list_.cend(); it++) {
    if (key == it->first) {
      list_.erase(it);
      return true;
    }
  }
  return false;
  */
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value)
    -> bool {
  auto it = std::find_if(
      list_.begin(), list_.end(),
      [key](const std::pair<K, V> &element) { return element.first == key; });
  if (it != list_.end()) {
    it->second = value;
  } else {
    if (IsFull()) {
      return false;
    }
    list_.insert(list_.end(), std::make_pair(key, value));
  }
  return true;

  // 遍历
  /*
  for (auto &[k, v] : list_) {
    if (key == k) {
      v = value; // update
      return true;
    }
  }
  if (IsFull()) {
    return false;
  }
  //
  emplace_front接受构造新元素所需的参数，并将这些参数传递给新元素的构造函数,插入到前面，局部性
  list_.emplace_front(key, value);
  return true;
  */
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

} // namespace bustub
