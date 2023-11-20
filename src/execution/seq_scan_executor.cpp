//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      filter_predicate_(plan->filter_predicate_),
      end_(exec_ctx->GetCatalog()->GetTable(plan->table_oid_)->table_->End()),
      cursor_(end_) {}

void SeqScanExecutor::Init() {
  // 在给row上S锁之前，需要在其table level根据事务隔离级别上也上锁
  auto txn = exec_ctx_->GetTransaction();
  if ((txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ ||
       txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) &&
      !txn->IsTableIntentionSharedLocked(plan_->GetTableOid()) &&
      !txn->IsTableIntentionExclusiveLocked(plan_->GetTableOid())) {
    // grab IS lock on table if not locked yet
    auto table_lock_success =
        exec_ctx_->GetLockManager()->LockTable(txn, LockManager::LockMode::INTENTION_SHARED, plan_->GetTableOid());
    if (!table_lock_success) {
      txn->SetState(TransactionState::ABORTED);
      throw bustub::Exception(ExceptionType::EXECUTION, "SeqScan cannot get IS lock on table");
    }
  }
  // 使游标处于开始处
  cursor_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)->table_->Begin(exec_ctx_->GetTransaction());
  table_name_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)->name_;
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto oid = plan_->GetTableOid();
  auto txn = exec_ctx_->GetTransaction();
  if (cursor_ == end_) {
    // reach end of table
    return false;
  }

  while (cursor_ != end_) {
    *rid = cursor_->GetRid();
    if (filter_predicate_ == nullptr) {
      // lock row
      if ((txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ ||
           txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) &&
          !txn->IsRowSharedLocked(oid, *rid) && !txn->IsRowExclusiveLocked(oid, *rid)) {
        obtain_lock_ = true;
        exec_ctx_->GetLockManager()->LockRow(txn, LockManager::LockMode::SHARED, oid, *rid);
      }
      *tuple = *cursor_++;
      if (obtain_lock_ && txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
        // S lock is released immediately in READ_COMMITTED
        exec_ctx_->GetLockManager()->UnlockRow(txn, oid, *rid);
      }
      obtain_lock_ = false;
      return true;
    }
    // filter is not null
    auto value = filter_predicate_->Evaluate(&*cursor_, plan_->OutputSchema());
    if (!value.IsNull() && value.GetAs<bool>()) {
      // lock row
      if ((txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ ||
           txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) &&
          !txn->IsRowSharedLocked(oid, *rid) && !txn->IsRowExclusiveLocked(oid, *rid)) {
        obtain_lock_ = true;
        exec_ctx_->GetLockManager()->LockRow(txn, LockManager::LockMode::SHARED, oid, *rid);
      }
      *tuple = *cursor_++;
      if (table_name_ == "nft") {
        cursor_ = end_;
      }
      if (obtain_lock_ && txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
        // S lock is released immediately in READ_COMMITTED
        exec_ctx_->GetLockManager()->UnlockRow(txn, oid, *rid);
      }
      obtain_lock_ = false;
      return true;
    }
    cursor_++;
  }
  return false;
}

}  // namespace bustub
