//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.h
//
// Identification: src/include/execution/executors/nested_index_join_executor.h
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/nested_index_join_plan.h"
#include "storage/table/tmp_tuple.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * IndexJoinExecutor executes index join operations.
 */
class NestIndexJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Creates a new nested index join executor.
   * @param exec_ctx the context that the hash join should be performed in
   * @param plan the nested index join plan node
   * @param child_executor the outer table
   */
  NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                        std::unique_ptr<AbstractExecutor> &&child_executor);

  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  void Init() override;

  auto Next(Tuple *tuple, RID *rid) -> bool override;

 private:
  void MergeValueFromTuple(std::vector<Value> &value, bool right_null) {
    auto left_col_cnt = outer_schema_.GetColumnCount();
    auto right_col_cnt = inner_schema_.GetColumnCount();
    for (unsigned int i = 0; i < left_col_cnt; i++) {
      value.push_back(outer_tuple_.GetValue(&outer_schema_, i));
    }

    for (unsigned int i = 0; i < right_col_cnt; i++) {
      if (!right_null) {
        value.push_back(inner_tuple_.GetValue(&inner_schema_, i));
      } else {
        value.push_back(ValueFactory::GetNullValueByType(inner_schema_.GetColumn(i).GetType()));
      }
    }
  }
  /** The nested index join plan node. */
  const NestedIndexJoinPlanNode *plan_;
  // 只有一个孩子，是外表的seqscan
  std::unique_ptr<AbstractExecutor> child_executor_;
  // 内表索引
  IndexInfo *index_{nullptr};

  Tuple outer_tuple_{};
  RID outer_rid_{};

  Tuple inner_tuple_{};
  RID inner_rid_{};

  // the index matching rids in the inner
  std::vector<RID> match_rids_{};
  // pointer to inner table
  TableInfo *inner_table_ptr_{nullptr};
  // inner table schema
  Schema inner_schema_;
  // outer table schema
  Schema outer_schema_;
  Schema key_schema_;
};
}  // namespace bustub
