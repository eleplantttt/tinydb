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

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstddef>
#include <cstdlib>
#include <functional>
#include <list>
#include <memory>
// #include <mutex>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"
#include "type/value.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  // 初始化：全局深度为0，局部深度为0，桶数量为1
  dir_.emplace_back(std::make_shared<Bucket>(bucket_size, 0));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::shared_lock lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::shared_lock lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::shared_lock lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::CalIndex(size_t &idx, int depth) -> size_t {   // 计算索引
  return idx ^ (1 << (depth - 1));                                            // 将depth-1(旧的全局深度)左移一位与 idx 进行按位异或运算
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::shared_lock lock(latch_);
  auto index = IndexOf(key);
  return dir_[index]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::unique_lock lock(latch_);
  auto index = IndexOf(key);
  return dir_[index]->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::unique_lock lock(latch_);                                      // 独占锁(写锁)
  while (true) {                                                         // 循环扩容防止分裂一次以后，还是被分配到同一个桶
    size_t idx;
    bool ok;
    {
      idx = IndexOf(key);
      ok = dir_[idx]->Insert(key, value);                                 // 插入成功
      if (ok) {
        return ;
      }
    }
    if (dir_[idx]->GetDepth() == global_depth_) {                         // 局部深度等于全局深度 桶分裂且目录扩张
      Expansion();
      RedistributeBucket(idx);
    } else {                                                              // 桶分裂
      RedistributeBucket(idx);
    }
  }
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Expansion() -> void {
  size_t old_size = 1 << global_depth_++;                                // 二进制位向左移一位，就是两倍扩容前
  size_t new_size = 1 << global_depth_;                                  // 二进制位向左移一位，就是两倍扩容前
  dir_.reserve(new_size);                                             // 扩容到新容量
  for (size_t i = old_size; i < new_size ; ++i) {                        // 从后一半开始指针重定向old_size开始
    dir_.push_back(dir_[i - old_size]);          // eg:从i=old_size=4(100) 全局深度已经加一为3 传入 传出dir[4]
  }
}
// localDepth < globalDepth 多个dir_指向一个同一个新桶
// 只会分裂出一个新桶
// 注意把多个dir_分成两种指向
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket(size_t idx) -> void {
  int old_depth = dir_[idx]->GetDepth();                                  // 先拿桶的原局部深度为分裂做准备
  size_t old_size = 1 << (global_depth_ - 1);
  size_t  old_idx = idx & ((1 << old_depth) - 1);                         // 拿到旧的指针指向
  auto &old_bucket = dir_[old_idx];                                       // 指向旧桶指针不变
//  auto &new_bucket = dir_[CalIndex(old_idx, old_depth + 1)];     // 指针定向新桶
  auto &new_bucket = dir_[old_idx + old_size];     // 指针定向新桶
  new_bucket.reset(new Bucket(bucket_size_, old_depth));                   // 重置bucket

  old_bucket->IncrementDepth();                                             // 局部深度自增
  new_bucket->IncrementDepth();

//  size_t diff = global_depth_ - old_depth;                                   // 差 1
//  int n = (1 << diff) - 1;                                                   //  n = 1

  for (int i = 0; i <= 1 ; ++i) {                                            // 溢出桶重新分桶                 ********
    size_t index = (i << old_depth) + old_idx;                               // i = 0 old_bucket i = 1 new_bucket
    dir_[index] = static_cast<bool>(i & 1) ? new_bucket : old_bucket;        // 指针指向确定
  }

  num_buckets_ += 1;                                                         // 桶数量加1
  auto list = std::move(old_bucket->GetItems());                             // 拿到旧桶的所有数据

  for (auto &[k, v] : list) {                                                // 遍历旧桶重新分配
    dir_[IndexOf(k)]->Insert(k, v);
  }
}
//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  std::shared_lock lock(mutex_);
  return std::any_of(list_.begin(), list_.end(), [&key, &value](const auto &item) {
    if (item.first == key) {
      value = item.second;
      return true;
    }
    return false;
  });
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  std::unique_lock lock(mutex_);
  return std::any_of(list_.begin(), list_.end(), [&key, this](const auto &item) {
    if (item.first == key) {
      this->list_.remove(item);
      return true;
    }
    return false;
  });
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  std::unique_lock lock(mutex_);
  for (auto &[k, v] : list_) {                                      // 少了更新
    if (k == key) {
      v = value;
      return true;
    }
  }
  if (IsFullWithGuard()) {                                          // 链表满了
    return false;
  }
  list_.emplace_back(key, value);                              // 插入的情况
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
