//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include <fmt/printf.h>
#include <sys/types.h>
#include <unistd.h>
#include <algorithm>
#include <bitset>
#include <cassert>
#include <chrono>  // NOLINT
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <initializer_list>
#include <iterator>
#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <ostream>
#include <vector>

#include <fmt/chrono.h>  // NOLINT
#include <fmt/format.h>  // NOLINT
#include "catalog/column.h"
#include "catalog/table_generator.h"
#include "common/config.h"
#include "common/logger.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "fmt/color.h"
#include "fmt/core.h"
#include "fmt/ranges.h"


#ifdef LOGTABLE
#define MY_LOGT(format, ...) Log(__FILE__, __LINE__, FMT_STRING(format), __VA_ARGS__)
#else
#define MY_LOGT(format, ...) ((void)0)
#endif

#ifdef LOGROW
#define MY_LOGR(format, ...) Log(__FILE__, __LINE__, FMT_STRING(format), __VA_ARGS__)
#else
#define MY_LOGR(format, ...) ((void)0)
#endif

#ifdef LOGCYCLE
#define MY_LOGC(format, ...) Log(__FILE__, __LINE__, FMT_STRING(format), __VA_ARGS__)
#else
#define MY_LOGC(format, ...) ((void)0)
#endif

namespace bustub {

#define ANYLOCK                                                                                                        \
  {                                                                                                                    \
    LockMode::INTENTION_SHARED, LockMode::INTENTION_EXCLUSIVE, LockMode::SHARED_INTENTION_EXCLUSIVE, LockMode::SHARED, \
        LockMode::EXCLUSIVE                                                                                            \
  }

/*
    Compatibility Matrix
        IS   IX   S   SIX   X
  IS    √    √    √    √    ×
  IX    √    √    ×    ×    ×
  S     √    ×    √    ×    ×
  SIX   √    ×    ×    ×    ×
  X     ×    ×    ×    ×    ×
*/
auto LockManager::LockRequestQueue::CheckCompatibility(LockMode lock_mode, ListType::iterator ite) -> bool {
  // all earlier requests should have been granted already
  // check compatible with the locks that are currently held
  // dp
  int mask = 0;
  assert(ite != request_queue_.end());
  ite++;
  for (auto it = request_queue_.begin(); it != ite; it++) {
    const auto &request = *it;
    const auto mode = request->lock_mode_;
    auto *txn = TransactionManager::GetTransaction(request->txn_id_);
    if (txn->GetState() == TransactionState::ABORTED) {
      continue ;
    }

    auto confilct = [](int mask, std::vector<LockMode> lock_modes) -> bool {
      return std::any_of(lock_modes.begin(), lock_modes.end(),
                         [mask](const auto lock_mode) { return (mask & 1 << static_cast<int>(lock_mode)) != 0; });
    };

    switch (mode) {
      case LockMode::INTENTION_SHARED:
        if (confilct(mask, {LockMode::EXCLUSIVE})) {
          return false;
        }
        break;
      case LockMode::INTENTION_EXCLUSIVE:
        if (confilct(mask, {LockMode::SHARED_INTENTION_EXCLUSIVE, LockMode::SHARED, LockMode::EXCLUSIVE})) {
          return false;
        }
        break;
      case LockMode::SHARED:
        if (confilct(mask,
                     {LockMode::INTENTION_EXCLUSIVE, LockMode::SHARED_INTENTION_EXCLUSIVE, LockMode::EXCLUSIVE})) {
          return false;
        }
        break;
      case LockMode::SHARED_INTENTION_EXCLUSIVE:
        if (confilct(mask, {LockMode::INTENTION_EXCLUSIVE, LockMode::SHARED_INTENTION_EXCLUSIVE, LockMode::SHARED,
                            LockMode::EXCLUSIVE})) {
          return false;
        }
        break;
      case LockMode::EXCLUSIVE:
        if (mask != 0) {
          return false;
        }
        break;
    }
    mask |= 1 << static_cast<int>(mode);
  }
  return true;
}

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
   const auto &txn_id = txn->GetTransactionId();
   const auto &txn_level = txn->GetIsolationLevel();
   const auto &txn_state = txn->GetState();

   switch (txn_level) {
     case IsolationLevel::REPEATABLE_READ:
       // All locks are allowed in the GROWING state
       // No locks are allowed in the SHRINKING state
       if (txn_state == TransactionState::SHRINKING) {
         txn->SetState(TransactionState::ABORTED);
         LOG_WARN("LOCK_ON_SHRINKING");
         throw TransactionAbortException(txn_id, AbortReason::LOCK_ON_SHRINKING);
       }
       break ;
     case IsolationLevel::READ_COMMITTED:
       // All locks are allowed in the GROWING state
       // Only IS, S locks are allowed in the SHRINKING state
       if (txn_state == TransactionState::SHRINKING) {
         if (lock_mode != LockMode::INTENTION_SHARED && lock_mode != LockMode::SHARED) {
           txn->SetState(TransactionState::ABORTED);
           LOG_WARN("LOCK_ON_SHRINKING");
           throw TransactionAbortException(txn_id, AbortReason::LOCK_ON_SHRINKING);
         }
       }
       break ;
     case IsolationLevel::READ_UNCOMMITTED:
       // The transaction is required to take only IX, X locks.
       // X, IX locks are allowed in the GROWING state.
       if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
           lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
         txn->SetState(TransactionState::ABORTED);
         LOG_WARN("LOCK_SHARED_ON_READ_UNCOMMITTED");
         throw TransactionAbortException(txn_id, AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
       }
       if (txn_state == TransactionState::SHRINKING) {
         txn->SetState(TransactionState::ABORTED);
         LOG_WARN("LOCK_ON_SHRINKING");
         throw TransactionAbortException(txn_id, AbortReason::LOCK_ON_SHRINKING);
       }
   }

   auto old_lock_mode = IsTableLocked(txn, oid, ANYLOCK);

   bool upgrade = false;
   // 原表有锁
   if (old_lock_mode.has_value()) {
     // 新锁旧锁一样
     if (old_lock_mode == lock_mode) {
       return true;
     }
     // 新锁旧锁不一样
     switch (old_lock_mode.value()) {
       // 根据锁升级判断
       case LockMode::INTENTION_SHARED:
         break ;
       case LockMode::SHARED:
       case LockMode::INTENTION_EXCLUSIVE:
         if (lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE) {
           txn->SetState(TransactionState::ABORTED);
           LOG_WARN("INCOMPATIBLE_UPGRADE");
           throw TransactionAbortException(txn_id, AbortReason::INCOMPATIBLE_UPGRADE);
         }
         break ;
       case LockMode::EXCLUSIVE:
         txn->SetState(TransactionState::ABORTED);
         LOG_WARN("INCOMPATIBLE_UPGRADE");
         throw TransactionAbortException(txn_id, AbortReason::INCOMPATIBLE_UPGRADE);
       case LockMode::SHARED_INTENTION_EXCLUSIVE:
         if (lock_mode != LockMode::EXCLUSIVE) {
           txn->SetState(TransactionState::ABORTED);
           LOG_WARN("INCOMPATIBLE_UPGRADE");
           throw TransactionAbortException(txn_id, AbortReason::INCOMPATIBLE_UPGRADE);
         }
         break ;
     }
     upgrade = true;
   }

   table_lock_map_latch_.lock();
   auto table_lock_map_it = table_lock_map_.find(oid);
   std::shared_ptr<LockRequestQueue> lrque;
   auto lock_request = std::make_shared<LockRequest>(txn_id, lock_mode, oid);

   txn_variant_map_latch_.lock();
   txn_variant_map_[txn_id] = oid;
   txn_variant_map_latch_.unlock();

   MY_LOGT(YELLOW("txn {} {} locktable   {} {:^5}"), txn_id, txn_level, oid, lock_mode);

   if (table_lock_map_it != table_lock_map_.end()) {
     lrque = table_lock_map_it->second;
     // 注意顺序，否则会出现txn乱序
     std::unique_lock lock(lrque->latch_);
     table_lock_map_latch_.unlock();
     auto &request_queue = lrque->request_queue_;
     decltype(request_queue.begin()) request_it;
     // 升级锁
     if (upgrade) {
       // 如果有正在升级的锁
       if (lrque->upgrading_ != INVALID_TXN_ID) {
         txn->SetState(TransactionState::ABORTED);
         throw TransactionAbortException(txn_id, AbortReason::UPGRADE_CONFLICT);
       }
       // 找到原事务在请求队列的位置
       request_it = std::find_if(request_queue.begin(),request_queue.end(),
                                 [txn_id](const auto &request) { return request->txn_id_ == txn_id; });
       assert(request_it != request_queue.end() && request_it->get()->granted_);

       lrque->upgrading_ = txn_id;

       // 升级的锁优先级比较高，应该位于等待队列的第一个
       auto it = std::find_if(request_it, request_queue.end(), [](const auto &request) { return !request->granted_; });
       request_queue.erase(request_it);  // 相当于释放了原来的锁
       request_it = request_queue.insert(it, lock_request);

       // 把原锁从事务的锁集合中删除
       switch (old_lock_mode.value()) {
         case LockMode::SHARED:
           txn->GetSharedTableLockSet()->erase(oid);
           break;
         case LockMode::EXCLUSIVE:
           break;
         case LockMode::INTENTION_SHARED:
           txn->GetIntentionSharedTableLockSet()->erase(oid);
           break;
         case LockMode::INTENTION_EXCLUSIVE:
           txn->GetIntentionExclusiveTableLockSet()->erase(oid);
           break;
         case LockMode::SHARED_INTENTION_EXCLUSIVE:
           txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
           break;
       }
       MY_LOGT(BBLUE("txn {} {} locktable   {} {} -> {} UPGRADE"), txn_id, txn_level, oid, old_lock_mode.value(),
               lock_mode);
     } else {
       // 没升级锁
       request_it = request_queue.insert(request_queue.end(), lock_request);
     }

     lrque->cv_.wait(lock, [&]() {
       return txn->GetState() == TransactionState::ABORTED || lrque->CheckCompatibility(lock_mode, request_it);
     });

     if (txn->GetState() == TransactionState::ABORTED) {
       request_queue.erase(request_it);
       return false;
     }
     lock_request->granted_ = true;
     if (upgrade) {
       lrque->upgrading_ = INVALID_TXN_ID;
     }
   } else {
     // 新事务
     lrque = std::make_shared<LockRequestQueue>();
     table_lock_map_[oid] = lrque;
     lrque->request_queue_.push_back(lock_request);
     lock_request->granted_ = true;
     table_lock_map_latch_.unlock();
   }

   MY_LOGT(BYELLOW("txn {} {} locktable   {} {:^5} SUCCESS"), txn_id, txn_level, oid, lock_mode);

   switch (lock_mode) {
     case LockMode::SHARED:
       txn->GetSharedTableLockSet()->insert(oid);
       break;
     case LockMode::EXCLUSIVE:
       txn->GetExclusiveTableLockSet()->insert(oid);
       break;
     case LockMode::INTENTION_SHARED:
       txn->GetIntentionSharedTableLockSet()->insert(oid);
       break;
     case LockMode::INTENTION_EXCLUSIVE:
       txn->GetIntentionExclusiveTableLockSet()->insert(oid);
       break;
     case LockMode::SHARED_INTENTION_EXCLUSIVE:
       txn->GetSharedIntentionExclusiveTableLockSet()->insert(oid);
       break;
   }
   return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool { return true; }

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool { return true; }

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool { return false; }

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
    }
  }
}

// 用可空可有的操作判断并返回该表的锁类型
auto LockManager::IsTableLocked(Transaction *txn, const table_oid_t &oid, const std::vector<LockMode> &lock_modes)
    -> std::optional<LockMode> {
  std::optional<LockMode> mode = std::nullopt;
  for (const auto &lock_mode : lock_modes) {
    switch (lock_mode) {
       case LockMode::SHARED:
         if (txn->IsTableSharedLocked(oid)) {
           mode = std::make_optional<LockMode>(LockMode::SHARED);
         }
         break;
       case LockMode::EXCLUSIVE:
         if (txn->IsTableExclusiveLocked(oid)) {
           mode = std::make_optional<LockMode>(LockMode::EXCLUSIVE);
         }
         break;
       case LockMode::INTENTION_SHARED:
         if (txn->IsTableIntentionSharedLocked(oid)) {
           mode = std::make_optional<LockMode>(LockMode::INTENTION_SHARED);
         }
         break;
       case LockMode::INTENTION_EXCLUSIVE:
         if (txn->IsTableIntentionExclusiveLocked(oid)) {
           mode = std::make_optional<LockMode>(LockMode::INTENTION_EXCLUSIVE);
         }
         break;
       case LockMode::SHARED_INTENTION_EXCLUSIVE:
         if (txn->IsTableSharedIntentionExclusiveLocked(oid)) {
           mode = std::make_optional<LockMode>(LockMode::SHARED_INTENTION_EXCLUSIVE);
         }
         break;
    }
    if (mode) {
       return mode;
    }
  }
  return std::nullopt;
}
}  // namespace bustub
