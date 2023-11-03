//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_executor)),
      right_child_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  RID rid_holder;
  left_child_->Init();
  right_child_->Init();
  left_tuple_ = Tuple{};
  right_tuple_ = Tuple{};
  left_status_ = left_child_->Next(&left_tuple_, &rid_holder);
  right_status_ = false;
  left_join_found_ = false;
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  RID rid_holder;
  while (true) {
    right_status_ = right_child_->Next(&right_tuple_, &rid_holder);
    // 内表一遍结束
    if (!right_status_) {
      // 左连接时，内表没有匹配上的可以返回null value
      if (plan_->GetJoinType() == JoinType::LEFT && !left_join_found_) {
        std::vector<Value> value;
        MergeValueFromTuple(value, true);
        *tuple = Tuple(value, &plan_->OutputSchema());
        left_join_found_ = true;
        return true;
      }
      // 外表下一个行接着和内表匹配，内表需要重新开始
      left_status_ = left_child_->Next(&left_tuple_, &rid_holder);
      left_join_found_ = false;
      // 外表遍历结束
      if (!left_status_) {
        return false;
      }
      // 内表重新开始
      right_child_->Init();
      right_status_ = right_child_->Next(&right_tuple_, &rid_holder);
      if (!right_status_) {
        // 内表为空，还是需要进入下次循环，因为有左连接
        continue;
      }
    }

    auto pred_value = plan_->Predicate().EvaluateJoin(&left_tuple_, left_child_->GetOutputSchema(), &right_tuple_,
                                                      right_child_->GetOutputSchema());
    if (!pred_value.IsNull() && pred_value.GetAs<bool>()) {
      std::vector<Value> value;
      MergeValueFromTuple(value, false);
      *tuple = Tuple(value, &plan_->OutputSchema());
      left_join_found_ = true;
      return true;
    }
  }
  return false;
}

}  // namespace bustub
