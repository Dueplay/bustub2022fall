//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      cursor_(aht_.End()),
      end_(aht_.End()) {}

void AggregationExecutor::Init() {
  // 拉取子节点的tuple，聚集在hash table中
  aht_.Clear();
  child_->Init();
  allow_empty_output_ = true;
  Tuple child_tuple{};
  RID rid_holder{};
  auto status = child_->Next(&child_tuple, &rid_holder);
  while (status) {
    auto agg_key = MakeAggregateKey(&child_tuple);
    auto agg_value = MakeAggregateValue(&child_tuple);
    aht_.InsertCombine(agg_key, agg_value);
    status = child_->Next(&child_tuple, &rid_holder);
  }
  // 构建hash表后创建其迭代器
  cursor_ = aht_.Begin();
  end_ = aht_.End();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  const auto output_schema = plan_->OutputSchema();
  if (cursor_ == end_) {
    if (cursor_ == aht_.Begin() && allow_empty_output_) {
      // empty table,special treatment
      if (!plan_->GetGroupBys().empty()) {
        // GetGroupBy col not empty, but table empty,error
        return false;
      }
      auto value = aht_.GenerateInitialAggregateValue();
      *tuple = Tuple(value.aggregates_, &output_schema);
      allow_empty_output_ = false;
      return true;
    }
    // aggregation finished or no more empty-table output
    return false;
  }
  auto key = cursor_.Key();
  auto value = cursor_.Val();
  // merge group-by col and agg cols
  std::copy(value.aggregates_.begin(), value.aggregates_.end(), std::back_inserter(key.group_bys_));
  ++cursor_;
  *tuple = Tuple(key.group_bys_, &output_schema);
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
