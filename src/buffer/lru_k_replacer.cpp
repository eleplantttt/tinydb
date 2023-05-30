//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <exception>
// #include <mutex>

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}
// 驱逐某一页帧，并frame_id为入参，传递驱逐的页帧。
auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard lock(latch_);
  bool evicted = EvitFromSet(inf_set_, frame_id) || EvitFromSet(kth_set_, frame_id);
  if (evicted) {
      RemoveRecord(*frame_id);
      // LOG_DEBUG("evicted %d cur_size %ld replacer_size %ld", *frame_id, cur_size_, replacer_size_);
  }
    return evicted;
}
// 访问记录更新
void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
    std::lock_guard lock(latch_);
    current_timestamp_ = GetTimeStamp();
    // BUSTUB_ASSERT(frame_cnt_map_.count(frame_id) == 0, "RecordAccess invalid %d");
    if (frame_cnt_.count(frame_id) == 0 && replacer_size_ == 0) {           // 没有该页帧记录并且剩余容量为0
        // BUSTUB_ASSERT("RecordAccess invalid %d", frame_id);
        return;
    }

    size_t &frame_cnt = frame_cnt_[frame_id];                       // 获取该页帧被访问次数
    // 没有历史访问
    if(frame_cnt == 0) {                                            // 没有访问过（新页帧）
        evitable_[frame_id] = false;                                // 默认设置为不可驱逐
        frame_time_[frame_id] = current_timestamp_;                 // 设置时间戳
        replacer_size_--;                                           // 容量减一
    }
    // 有过历史访问但不可驱逐
    if (!evitable_[frame_id]) {                                     // 若该页帧不可驱逐
        // frame_time_[frame_id] = current_timestamp_;
        if (++frame_cnt >= k_) {                                    // 判断加上这一次的访问是不是到达k次
            frame_time_[frame_id] = current_timestamp_;             // 更新时间戳
        }
        // LOG_DEBUG("record unevitable %d cnt %ld cur_size %ld replacer_size %ld", frame_id, frame_cnt_[frame_id],
        // cur_size_, replacer_size_);
        return;
    }
    // 有过历史访问但可驱逐
    if (frame_cnt < k_ - 1) {                                       // 记录数少于k-1次，直接加入
        frame_cnt++;                                                // 访问次数加一
    } else {                                                        // 加上这一次访问变k次或者已经k次以上
        RemoveFromSet(frame_id);                                    // 先删除页帧不管是kth和inf
        frame_time_[frame_id] = current_timestamp_;                 // 设置新的时间戳
        frame_cnt++;                                                // 访问次数加一
        InsertToSet(frame_id);                                      // 加入页帧到inf，大于等于k次的加入kth
    }
    // LOG_DEBUG("record evitable %d cnt %ld cur_size %ld replacer_size %ld", frame_id, frame_cnt_[frame_id],
    // cur_size_,replacer_size_);
}
// 设置页帧可驱逐和不可驱逐
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    std::lock_guard lock(latch_);
    // 没有历史记录或者驱逐标记对上
    if (frame_time_.count(frame_id) == 0 || evitable_[frame_id] == set_evictable) {
        // LOG_DEBUG("set evitable %d cur_size %ld replacer_size %ld", frame_id, cur_size_, replacer_size_);
        return;
    }
    evitable_[frame_id] = set_evictable;
    // 如果设置为可驱逐
    if (set_evictable) {
        // 根据访问次数放入kth 或 inf
        InsertToSet(frame_id);
        cur_size_++;
        // LOG_DEBUG("set evitable %d cur_size %ld replacer_size %ld", frame_id, cur_size_, replacer_size_);
    } else {        // 设置为不可驱逐
        // 根据访问次数找到对应的kth和inf并删除
        RemoveFromSet(frame_id);
        cur_size_--;
        // LOG_DEBUG("set unevitable %d cur_size %ld replacer_size %ld", frame_id, cur_size_, replacer_size_);
    }
}
// 删除某一页帧
void LRUKReplacer::Remove(frame_id_t frame_id) {
    std::lock_guard lock(latch_);
    // 没有访问记录
    if (frame_time_.count(frame_id) == 0) {
        return;
    }
    BUSTUB_ASSERT(evitable_[frame_id], "unremovalbe!");
    RemoveFromSet(frame_id);
    RemoveRecord(frame_id);
}
// 获取可驱逐的数量
auto LRUKReplacer::Size() -> size_t {
    std::lock_guard lock(latch_);
    return this->cur_size_;
}

// 向集合插入页帧
auto LRUKReplacer::InsertToSet(frame_id_t frame_id) -> void {
    assert(frame_time_.count(frame_id) != 0);
    Node node{frame_id, frame_time_[frame_id]};
    if (frame_cnt_[frame_id] >= k_) {
        kth_set_.insert(node);
    } else {
        inf_set_.insert(node);
    }
}
// 从集合中删除页帧
auto LRUKReplacer::RemoveFromSet(frame_id_t frame_id) -> void {
    assert(frame_time_.count(frame_id) != 0);
    Node node{frame_id, frame_time_[frame_id]};
    if (frame_cnt_[frame_id] >= k_) {           // 大于k的在kth集合中删
        kth_set_.erase(node);
    } else {                                    // 在没达到k次访问的集合中删
        inf_set_.erase(node);
    }
}
// 删除页帧的参数记录改变
auto LRUKReplacer::RemoveRecord(frame_id_t frame_id) -> void {
    evitable_.erase(frame_id);
    frame_time_.erase(frame_id);
    frame_cnt_.erase(frame_id);
    cur_size_--;
    replacer_size_++;
}
// 从set里面驱逐某一页帧并且参数frame_id返回其id
auto LRUKReplacer::EvitFromSet(std::set<Node> &set_, frame_id_t *frame_id) -> bool {
    for (auto it : set_) {
        auto id = it.id_;
        set_.erase(it);
        *frame_id = id;
        return true;
    }
    return false;
}
// 获取时间戳
auto LRUKReplacer::GetTimeStamp() -> int64_t {
    struct timespec abs_time{};
    clock_gettime(CLOCK_REALTIME, &abs_time);
    const int k = 1000000000;
    return abs_time.tv_sec * k + abs_time.tv_nsec;
}

}  // namespace bustub
