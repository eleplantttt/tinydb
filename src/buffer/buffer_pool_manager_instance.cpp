//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"
#include <cstddef>
// #include <mutex>

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);

  bool is_free_page = false;  // 给is_free_page设初值false

  for (size_t i = 0; i < pool_size_; i++) {  // 遍历所有页面，将没被pin(使用)的页面设置为空页
    if (pages_[i].GetPinCount() == 0) {
      is_free_page = true;
      break;
    }
  }

  if (!is_free_page) {  // 若所有页面都被pin则说明没有页可用
    return nullptr;
  }

  *page_id = AllocatePage();

  frame_id_t frame_id;

  if (!free_list_.empty()) {  // 从空闲页列表里面找
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {  // 没找到空闲页,从lru_k里面驱逐的拿
    // assert(replacer_->Evict(&frame_id));
    replacer_->Evict(&frame_id);  // 按照lru_k驱逐一个页面，frame_id变为剔除页id
    page_id_t evicted_page_id = pages_[frame_id].GetPageId();

    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(evicted_page_id, pages_[frame_id].GetData());  // 刷回磁盘
      pages_[frame_id].is_dirty_ = false;
    }

    pages_[frame_id].ResetMemory();  // 页空间内存释放为变成新空间

    page_table_->Remove(evicted_page_id);  // 从页空间表中删除被驱逐页
  }

  page_table_->Insert(*page_id, frame_id);  // frame_id(磁盘)和page_id(内存)在页空间对应

  pages_[frame_id].page_id_ = *page_id;  // 设置新页的id为传入id
  pages_[frame_id].pin_count_ = 1;       // 将新页的pin标识符置一

  replacer_->RecordAccess(frame_id);         // lru记录更新
  replacer_->SetEvictable(frame_id, false);  // 设置该页面不可驱逐

  return &pages_[frame_id];
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);

  frame_id_t frame_id;
  if (page_table_->Find(page_id, frame_id)) {  // 找到该page页拿到frame_id
    pages_[frame_id].pin_count_++;             // pin住
    replacer_->RecordAccess(frame_id);         // 刷新lru
    replacer_->SetEvictable(frame_id, false);  // 设置不可剔除
    return &pages_[frame_id];
  }

  bool is_free_page = false;  // 设置非空页       // 从这往下和newPgImp一样

  for (size_t i = 0; i < pool_size_; i++) {  // 遍历查看是否有空闲页（一直更新）
    if (pages_[i].GetPinCount() == 0) {
      is_free_page = true;
      break;
    }
  }

  if (!is_free_page) {
    return nullptr;
  }

  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    // assert(replacer_->Evict(&frame_id));
    replacer_->Evict(&frame_id);
    page_id_t evicted_page_id = pages_[frame_id].GetPageId();

    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(evicted_page_id, pages_[frame_id].GetData());
      pages_[frame_id].is_dirty_ = false;
    }

    pages_[frame_id].ResetMemory();

    page_table_->Remove(evicted_page_id);
  }

  page_table_->Insert(page_id, frame_id);

  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].pin_count_ = 1;

  disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());

  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  return &pages_[frame_id];
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }

  if (pages_[frame_id].GetPinCount() <= 0) {
    return false;
  }

  if (is_dirty) {
    pages_[frame_id].is_dirty_ = is_dirty;
  }

  pages_[frame_id].pin_count_--;

  if (pages_[frame_id].pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }

  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }

  frame_id_t frame_id;

  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }

  disk_manager_->WritePage(page_id, pages_[frame_id].data_);  // 为什么不是GetData()?
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock<std::mutex> lock(latch_);
  for (size_t frame_id = 0; frame_id < pool_size_; frame_id++) {
    FlushPgImp(pages_[frame_id].GetPageId());
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;

  if (!page_table_->Find(page_id, frame_id)) {
    return true;  // bug     false改true
  }

  if (pages_[frame_id].GetPinCount() > 0) {
    return false;
  }

  replacer_->Remove(frame_id);

  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].is_dirty_ = false;

  page_table_->Remove(page_id);
  free_list_.push_back(frame_id);
  DeallocatePage(page_id);

  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
