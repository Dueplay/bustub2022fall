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
    if (upgrade_matrix_[prev_mode].find(mode) == upgrade_matrix_[prev_mode].end() && prev_mode != mode) {
      // incompatible upgrade type
      reason = AbortReason::INCOMPATIBLE_UPGRADE;
      return false;
    }
    if (queue->upgrading_ != INVALID_TXN_ID && prev_mode != mode) {
      // only one txn should be allowed to upgrade its lock on a given resource
      reason = AbortReason::UPGRADE_CONFLICT;
      return false;
    }
  }

  return true;
}

auto LockManager::IsUnlockRequestValid(Transaction *txn, AbortReason &reason, LockMode &mode,
                                       std::shared_ptr<LockRequestQueue> &queue, bool on_table, table_oid_t table_id,
                                       RID rid) -> bool {
  // ensure the transaction is really holding a lock it's attempting to release
  if (on_table) {
    bool table_s_locked = txn->IsTableSharedLocked(table_id);
    bool table_is_locked = txn->IsTableIntentionSharedLocked(table_id);
    bool table_x_locked = txn->IsTableExclusiveLocked(table_id);
    bool table_ix_locked = txn->IsTableIntentionExclusiveLocked(table_id);
    bool table_six_locked = txn->IsTableSharedIntentionExclusiveLocked(table_id);
    if (!table_s_locked && !table_is_locked && !table_x_locked && !table_ix_locked && !table_six_locked) {
      // no lock held at all on table
      reason = AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD;
      return false;
    }
    if (table_s_locked) {
      mode = LockMode::SHARED;
    } else if (table_is_locked) {
      mode = LockMode::INTENTION_SHARED;
    } else if (table_x_locked) {
      mode = LockMode::EXCLUSIVE;
    } else if (table_ix_locked) {
      mode = LockMode::INTENTION_EXCLUSIVE;
    } else if (table_six_locked) {
      mode = LockMode::SHARED_INTENTION_EXCLUSIVE;
    }
  } else {
    bool row_s_locked = txn->IsRowSharedLocked(table_id, rid);
    bool row_x_locked = txn->IsRowExclusiveLocked(table_id, rid);
    if (!row_s_locked && !row_x_locked) {
      // no lock held at all on row
      reason = AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD;
      return false;
    }
    if (row_s_locked) {
      mode = LockMode::SHARED;
    } else if (row_x_locked) {
      mode = LockMode::EXCLUSIVE;
    }
  }

  // ensure all row locks are already released before releasing table lock
  if (on_table) {
    if ((txn->GetTransactionId() != queue->upgrading_) &&
        (!txn->GetSharedRowLockSet()->operator[](table_id).empty() ||
         !txn->GetExclusiveRowLockSet()->operator[](table_id).empty())) {
      // some row locks are persisted still, not applicable in upgrading lock request cases
      reason = AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS;
      return false;
    }
  }

  return true;
}

void LockManager::UpdateTransactionStateOnUnlock(Transaction *txn, LockMode unlock_mode) {
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    // unlocking S/X lock should set the txn state to SHRINKING
    if (unlock_mode == LockMode::SHARED || unlock_mode == LockMode::EXCLUSIVE) {
      txn->SetState(TransactionState::SHRINKING);
    }
  } else if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED ||
             txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    // unlocking X lock should set the txn state to SHRINKING
    if (unlock_mode == LockMode::EXCLUSIVE) {
      txn->SetState(TransactionState::SHRINKING);
    }
  }
}
auto LockManager::CouldLockRequestProceed(const std::shared_ptr<LockManager::LockRequest> &request, Transaction *txn,
                                          const std::shared_ptr<LockRequestQueue> &queue, bool is_upgrade,
                                          bool &already_abort) -> bool {
  txn->LockTxn();
  already_abort = false;
  if (txn->GetState() == TransactionState::ABORTED) {
    already_abort = true;
    txn->UnlockTxn();
    return true;
  }

  // Check if this transaction is the first one un-granted
  auto self = std::find(queue->request_queue_.begin(), queue->request_queue_.end(), request);
  auto first_ungranted =
      std::find_if_not(queue->request_queue_.begin(), queue->request_queue_.end(),
                       [](const std::shared_ptr<LockRequest> &request) { return request->granted_; });
  if (self != first_ungranted) {
    // not this request's turn yet, wait
    txn->UnlockTxn();
    return false;
  }

  // Check if current request lock mode is compatible with all previous granted request's lock mode
  auto is_compatible = queue->IsCompatibleUntil(self, compatible_matrix_);
  if (!is_compatible) {
    txn->UnlockTxn();
    // wait
    return false;
  }

  // This request can proceed, txn acquire lock, add lock record into txn's set
  if (request->on_table_) {
    if (request->lock_mode_ == LockMode::SHARED) {
      txn->GetSharedTableLockSet()->insert(request->oid_);
    } else if (request->lock_mode_ == LockMode::INTENTION_SHARED) {
      txn->GetIntentionSharedTableLockSet()->insert(request->oid_);
    } else if (request->lock_mode_ == LockMode::EXCLUSIVE) {
      txn->GetExclusiveTableLockSet()->insert(request->oid_);
    } else if (request->lock_mode_ == LockMode::INTENTION_EXCLUSIVE) {
      txn->GetIntentionExclusiveTableLockSet()->insert(request->oid_);
    } else if (request->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      txn->GetSharedIntentionExclusiveTableLockSet()->insert(request->oid_);
    }
  } else {
    if (request->lock_mode_ == LockMode::SHARED) {
      txn->GetSharedRowLockSet()->operator[](request->oid_).insert(request->rid_);
    } else if (request->lock_mode_ == LockMode::EXCLUSIVE) {
      txn->GetExclusiveRowLockSet()->operator[](request->oid_).insert(request->rid_);
    }
  }
  // this txn's lock request mark as granted
  request->granted_ = true;
  if (is_upgrade) {
    // no more waiting upgrading request now
    queue->upgrading_ = INVALID_TXN_ID;
  }
  txn->UnlockTxn();
  return true;
}

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
  if (!is_valid_request) {
    /* not valid , unlock + abort + throw exception */
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    throw TransactionAbortException(txn->GetTransactionId(), reason);
  }

  /* if it's upgrade request, special treatment */
  if (is_upgrade) {
    if (prev_mode == lock_mode) {
      // same request， not upgrade, return true
      txn->UnlockTxn();
      return true;
    }
    /* an upgrade is equivalent to an unlock + a new lock */
    // no releasing lock here, atomic release + upgrade
    queue->upgrading_ = txn->GetTransactionId();
    LockManager::UnlockTableHelper(txn, oid, true);
  }

  /* valid , make a request and add to queue for this resource at proper position(tail or first un-granted)*/
  auto request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
  // is_upgrade 要比其他请求锁的优先级高，因此放在第一个未被授予的位置
  queue->InsertIntoQueue(request, is_upgrade);

  /* acquire coordination mutex and wait for it's the first un-granted request and could proceed */
  txn->UnlockTxn();
  bool already_abort = false;
  /* proceed, add into this transaction's lock set and notify all in the queue if not abort */
  /**
   * wait如果 pred 为 true，则函数立即返回，并且不会解锁 lock。
    如果 pred 为 false，则函数会解锁 lock 并阻塞当前线程，直到其他线程调用 notify_one() 或 notify_all() 来唤醒它。
    唤醒后，它会重新获得 lock 并再次检查 pred，如果 pred 变为 true，则返回。
  */
  queue->cv_.wait(lock,
                  [&]() -> bool { return CouldLockRequestProceed(request, txn, queue, is_upgrade, already_abort); });

  /* Remove this request from the queue since it's aborted */
  if (already_abort) {
    if (is_upgrade) {
      // no more waiting upgrading request now
      queue->upgrading_ = INVALID_TXN_ID;
    }
    auto it = std::find_if(queue->request_queue_.begin(), queue->request_queue_.end(),
                           [&](const std::shared_ptr<LockRequest> &request) -> bool {
                             return request->txn_id_ == txn->GetTransactionId() && request->oid_ == oid;
                           });
    queue->request_queue_.erase(it);
    lock.unlock();
    queue->cv_.notify_all();
    return false;
  }

  // notify other waiting threads
  lock.unlock();
  queue->cv_.notify_all();
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  return UnlockTableHelper(txn, oid, false);
}

auto LockManager::UnlockTableHelper(Transaction *txn, const table_oid_t &oid, bool from_upgrade) -> bool {
  /* lock and fetch queue & transaction */
  auto queue = GetTableQueue(oid);
  std::unique_lock<std::mutex> lock;
  if (!from_upgrade) {
    // upgrade request, not lock queue, because held the queue's lock
    lock = std::unique_lock<std::mutex>(queue->latch_);
    txn->LockTxn();
  }
  AbortReason reason;
  LockMode locked_mode;
  /* check if this is a validate request ,ie check this table if held lock, no held lock so not need unlock */
  auto is_valid_request = IsUnlockRequestValid(txn, reason, locked_mode, queue, true, oid, RID());
  if (!is_valid_request) {
    /* not valid, unlock + abort + throw exception */
    txn->SetState(TransactionState::ABORTED);
    if (!from_upgrade) {
      txn->UnlockTxn();
    }
    throw TransactionAbortException(txn->GetTransactionId(), reason);
  }

  /* potentially update the transaction state */
  if (!from_upgrade && txn->GetState() != TransactionState::COMMITTED && txn->GetState() != TransactionState::ABORTED) {
    UpdateTransactionStateOnUnlock(txn, locked_mode);
  }
  /* Remove this request from the queue since it's completed */
  auto it = std::find_if(queue->request_queue_.begin(), queue->request_queue_.end(),
                         [&](const std::shared_ptr<LockRequest> &request) -> bool {
                           return request->txn_id_ == txn->GetTransactionId() && request->oid_ == oid;
                         });
  queue->request_queue_.erase(it);
  /* Remove from the transaction's lock set */
  if (locked_mode == LockMode::SHARED) {
    txn->GetSharedTableLockSet()->erase(oid);
  } else if (locked_mode == LockMode::INTENTION_SHARED) {
    txn->GetIntentionSharedTableLockSet()->erase(oid);
  } else if (locked_mode == LockMode::EXCLUSIVE) {
    txn->GetExclusiveTableLockSet()->erase(oid);
  } else if (locked_mode == LockMode::INTENTION_EXCLUSIVE) {
    txn->GetIntentionExclusiveTableLockSet()->erase(oid);
  } else if (locked_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
  }

  /* unlock transaction */
  if (!from_upgrade) {
    txn->UnlockTxn();
    /* raise up other waiting threads */
    lock.unlock();
    queue->cv_.notify_all();
  }
  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  // fetch and lock queue & txn
  auto queue = GetRowQueue(rid);  // in lock mode to read from map
  std::unique_lock<std::mutex> lock(queue->latch_);
  txn->LockTxn();
  bool is_upgrade;
  AbortReason reason;
  LockMode prev_mode;
  auto is_valid_request = IsLockRequestValid(txn, reason, is_upgrade, prev_mode, queue, false, lock_mode, oid, rid);
  if (!is_valid_request) {
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    throw TransactionAbortException(txn->GetTransactionId(), reason);
  }

  if (is_upgrade) {
    if (prev_mode == lock_mode) {
      txn->UnlockTxn();
      return true;
    }
    queue->upgrading_ = txn->GetTransactionId();
    LockManager::UnlockRowHelper(txn, oid, rid, true);
  }

  auto request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
  queue->InsertIntoQueue(request, is_upgrade);

  txn->UnlockTxn();
  bool already_abort = false;

  /* proceed, add into this transaction's lock set and notify all in the queue if not aborted */
  queue->cv_.wait(lock,
                  [&]() -> bool { return CouldLockRequestProceed(request, txn, queue, is_upgrade, already_abort); });
  if (already_abort) {
    if (is_upgrade) {
      queue->upgrading_ = INVALID_TXN_ID;
    }
    auto it = std::find_if(queue->request_queue_.begin(), queue->request_queue_.end(),
                           [&](const std::shared_ptr<LockRequest> &request) -> bool {
                             return request->txn_id_ == txn->GetTransactionId() && request->oid_ == oid &&
                                    request->rid_ == rid;
                           });
    queue->request_queue_.erase(it);
    lock.unlock();
    queue->cv_.notify_all();
    return false;
  }

  lock.unlock();
  queue->cv_.notify_all();
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {
  return UnlockRowHelper(txn, oid, rid, false);
}

auto LockManager::UnlockRowHelper(Transaction *txn, const table_oid_t &oid, const RID &rid, bool from_upgrade) -> bool {
  auto queue = GetRowQueue(rid);
  std::unique_lock<std::mutex> lock;
  if (!from_upgrade) {
    lock = std::unique_lock<std::mutex>(queue->latch_);
    txn->LockTxn();
  }
  AbortReason reason;
  LockMode locked_mode;
  auto is_valid_request = IsUnlockRequestValid(txn, reason, locked_mode, queue, false, oid, rid);
  if (!is_valid_request) {
    txn->SetState(TransactionState::ABORTED);
    if (!from_upgrade) {
      txn->UnlockTxn();
    }
    throw TransactionAbortException(txn->GetTransactionId(), reason);
  }

  if (!from_upgrade && txn->GetState() != TransactionState::COMMITTED && txn->GetState() != TransactionState::ABORTED) {
    UpdateTransactionStateOnUnlock(txn, locked_mode);
  }
  /* Remove this request from the queue since it's completed */
  auto it =
      std::find_if(queue->request_queue_.begin(), queue->request_queue_.end(),
                   [&](const std::shared_ptr<LockRequest> &request) -> bool {
                     return request->txn_id_ == txn->GetTransactionId() && request->oid_ == oid && request->rid_ == rid;
                   });
  queue->request_queue_.erase(it);
  /* Remove from the transaction's lock set */
  if (locked_mode == LockMode::SHARED) {
    txn->GetSharedRowLockSet()->operator[](oid).erase(rid);
  } else if (locked_mode == LockMode::EXCLUSIVE) {
    txn->GetExclusiveRowLockSet()->at(oid).erase(rid);
  }
  // not upgrade just unlockrow finsh, should notify other wait
  if (!from_upgrade) {
    txn->UnlockTxn();
    lock.unlock();
    queue->cv_.notify_all();
  }
  return true;
}
void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) { waits_for_[t1].emplace(t2); }

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) { waits_for_[t1].erase(t2); }

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  // assume the graph is already fully built
  std::deque<txn_id_t> path;
  std::set<txn_id_t> visited;
  for (const auto &[start_node, end_node_set] : waits_for_) {
    if (visited.find(start_node) == visited.end()) {
      auto cycle_id = DepthFirstSearch(start_node, visited, path);
      if (cycle_id != NO_CYCLE) {
        // trim the path and retain only those involved in cycle
        auto it = std::find(path.begin(), path.end(), cycle_id);
        path.erase(path.begin(), it);
        std::sort(path.begin(), path.end());
        txn_id_t to_abort = path.back();
        *txn_id = to_abort;  // pick the youngest to abort
        return true;
      }
    }
  }
  return false;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges;
  for (const auto &[start_edge, end_edge_set] : waits_for_) {
    for (const auto &end_edge : end_edge_set) {
      edges.emplace_back(start_edge, end_edge);
    }
  }
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
      // no more new transaction requests from this point
      // {} 是unique_lock的范围
      std::unique_lock<std::mutex> table_lock(table_lock_map_latch_);
      std::unique_lock<std::mutex> row_lock(row_lock_map_latch_);
      LockManager::RebuildWaitForGraph();
      txn_id_t to_abort_txn = NO_CYCLE;
      while (LockManager::HasCycle(&to_abort_txn)) {
        // has cycle, remove this transaction from graph
        LockManager::TrimGraph(to_abort_txn);
        auto to_abort_ptr = TransactionManager::GetTransaction(to_abort_txn);
        to_abort_ptr->SetState(TransactionState::ABORTED);
      }
      if (to_abort_txn != NO_CYCLE) {
        // if we ever find a single cycle to be aborted, notify everyone
        LockManager::NotifyAllTransaction();
      }
    }
  }
}

}  // namespace bustub
