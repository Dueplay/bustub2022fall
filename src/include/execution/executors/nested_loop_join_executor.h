//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.h
//
// Identification: src/include/execution/executors/nested_loop_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * NestedLoopJoinExecutor executes a nested-loop JOIN on two tables.
 */
class NestedLoopJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new NestedLoopJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The NestedLoop join plan to be executed
   * @param left_executor The child executor that produces tuple for the left
   * side of join
   * @param right_executor The child executor that produces tuple for the right
   * side of join
   */
  NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                         std::unique_ptr<AbstractExecutor> &&left_executor,
                         std::unique_ptr<AbstractExecutor> &&right_executor);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join
   * @param[out] rid The next tuple RID produced, not used by nested loop join.
   * @return `true` if a tuple was produced, `false` if there are no more
   * tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the insert */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  void MergeValueFromTuple(std::vector<Value> &value, bool right_null) {
    auto left_col_cnt = left_child_->GetOutputSchema().GetColumnCount();
    auto right_col_cnt = right_child_->GetOutputSchema().GetColumnCount();
    for (unsigned int i = 0; i < left_col_cnt; i++) {
      value.push_back(left_tuple_.GetValue(&left_child_->GetOutputSchema(), i));
    }

    for (unsigned int i = 0; i < right_col_cnt; i++) {
      if (!right_null) {
        value.push_back(right_tuple_.GetValue(&right_child_->GetOutputSchema(), i));
      } else {
        value.push_back(ValueFactory::GetNullValueByType(right_child_->GetOutputSchema().GetColumn(i).GetType()));
      }
    }
  }
  /** The NestedLoopJoin plan node to be executed. */
  const NestedLoopJoinPlanNode *plan_;
  // left child
  std::unique_ptr<AbstractExecutor> left_child_;
  std::unique_ptr<AbstractExecutor> right_child_;
  Tuple left_tuple_{};
  Tuple right_tuple_{};
  bool left_status_{false};
  bool right_status_{false};
  // if the tuple in outer table has found a match in inner table before wrap around
  bool left_join_found_{false};
};

}  // namespace bustub
