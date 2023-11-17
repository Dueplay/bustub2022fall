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

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {
auto LockManager::IsLockRequestValid(Transaction *txn, AbortReason &reason, bool &is_upgrade, LockMode &prev_mode,
                                     std::shared_ptr<LockRequestQueue> &queue, bool on_table, LockMode mode,
                                     table_oid_t table_id, RID rid) -> bool {
  /* supported lock mode*/
  if (on_table) {
    // table locking support all lock modes
  } else {
    // row locking should not support Intention locks
    if (mode != LockMode::SHARED && mode != LockMode::EXCLUSIVE) {
      reason = AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW;
      return false;
    }
  }

  /** isolation level */
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    // all lock required, no lock on shrinking stage
    if (txn->GetState() == TransactionState::SHRINKING) {
      reason = AbortReason::LOCK_ON_SHRINKING;
      return false;
    }
  } else if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    // all lock required, only IS/S on shrinking stage
    if (txn->GetState() == TransactionState::SHRINKING) {
      if (mode != LockMode::INTENTION_SHARED && mode != LockMode::SHARED) {
        reason = AbortReason::LOCK_ON_SHRINKING;
        return false;
      }
    }
  } else if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    // only required to take IX,X locks
    if (mode != LockMode::INTENTION_EXCLUSIVE && mode != LockMode::EXCLUSIVE) {
      reason = AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED;
      return false;
    }
    if (txn->GetState() == TransactionState::SHRINKING) {
      reason = AbortReason::LOCK_ON_SHRINKING;
      return false;
    }
  }

  /* multiple-level locking */
  if (!on_table) {
    if (mode == LockMode::EXCLUSIVE) {
      // need X,IX, or SIX lock on table if requesting X lock on row
      if (!txn->IsTableExclusiveLocked(table_id) && !txn->IsTableIntentionExclusiveLocked(table_id) &&
          !txn->IsTableSharedIntentionExclusiveLocked(table_id)) {
        reason = AbortReason::TABLE_LOCK_NOT_PRESENT;
        return false;
      }
    } else if (mode == LockMode::SHARED) {
      // any lock on table suffices if requesting S lock on row
      if (!txn->IsTableSharedLocked(table_id) && !txn->IsTableIntentionSharedLocked(table_id) &&
          !txn->IsTableExclusiveLocked(table_id) && !txn->IsTableIntentionExclusiveLocked(table_id) &&
          !txn->IsTableSharedIntentionExclusiveLocked(table_id)) {
        reason = AbortReason::TABLE_LOCK_NOT_PRESENT;
        return false;
      }
    }
  }

  /** lock upgrade */
  is_upgrade = false;  // reset to default for safety
  if (on_table) {
    // table locking request
    if (txn->IsTableSharedLocked(table_id)) {
      prev_mode = LockMode::SHARED;
      is_upgrade = true;
    } else if (txn->IsTableIntentionSharedLocked(table_id)) {
      prev_mode = LockMode::INTENTION_SHARED;
      is_upgrade = true;
    } else if (txn->IsTableExclusiveLocked(table_id)) {
      prev_mode = LockMode::EXCLUSIVE;
      is_upgrade = true;
    } else if (txn->IsTableIntentionExclusiveLocked(table_id)) {
      prev_mode = LockMode::INTENTION_EXCLUSIVE;
      is_upgrade = true;
    } else if (txn->IsTableSharedIntentionExclusiveLocked(table_id)) {
      prev_mode = LockMode::SHARED_INTENTION_EXCLUSIVE;
      is_upgrade = true;
    }
  } else {
    // row locking request
    if (txn->IsRowSharedLocked(table_id, rid)) {
      prev_mode = LockMode::SHARED;
      is_upgrade = true;
    } else if (txn->IsRowExclusiveLocked(table_id, rid)) {
      prev_mode = LockMode::EXCLUSIVE;
      is_upgrade = true;
    }
  }

  if (is_upgrade) {
    if (1) {
    }
  }
  
}

auto LockManager::IsUnlockRequestValid(Transaction *txn, AbortReason &reason, LockMode &mode,
                                       std::shared_ptr<LockRequestQueue> &queue, bool on_table, table_oid_t table_id,
                                       RID rid) -> bool {}
auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  /* lock and fetch lock request queue & txn*/
  auto queue = GetTableQueue(oid);  // in lock mode to read from map
  std::unique_lock<std::mutex> lock(queue->latch_);
  txn->LockTxn();
  bool is_upgrade;
  AbortReason reason;
  LockMode prev_mode;
  /* check if this is validate request */
  auto is_valid_request = IsLockRequestValid(txn, reason, is_upgrade, prev_mode, queue, true, lock_mode, oid, RID());
  if () }

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

}  // namespace bustub
