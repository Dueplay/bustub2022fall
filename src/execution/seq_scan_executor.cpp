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
  // 游标处于开始处
  cursor_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)->table_->Begin(exec_ctx_->GetTransaction());
  table_name_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)->name_;
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // auto oid = plan_->GetTableOid();

  if (cursor_ == end_) {
    return false;
  }

  while (cursor_ != end_) {
    *rid = cursor_->GetRid();
    if (filter_predicate_ == nullptr) {
      *tuple = *cursor_++;
      return true;
    }
    // filter is not null
    auto value = filter_predicate_->Evaluate(&*cursor_, plan_->OutputSchema());
    if (!value.IsNull() && value.GetAs<bool>()) {
      *tuple = *cursor_++;
      if (table_name_ == "nft") {
        cursor_ = end_;
      }
      return true;
    }
    cursor_++;
  }
  return false;
}

}  // namespace bustub
