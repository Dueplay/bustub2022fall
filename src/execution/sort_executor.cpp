#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  Tuple tuple_holder{};
  RID rid_holder{};
  child_executor_->Init();
  sorted_.clear();
  auto status = child_executor_->Next(&tuple_holder, &rid_holder);
  while (status) {
    sorted_.emplace_back(tuple_holder);
    status = child_executor_->Next(&tuple_holder, &rid_holder);
  }
  // 自定义sort排序函数
  auto orderby_keys = plan_->GetOrderBy();
  auto schema = GetOutputSchema();
  auto sorter = [&](Tuple &lhs, Tuple &rhs) {
    for (const auto &[order_type, expr] : orderby_keys) {
      auto left_value = expr->Evaluate(&lhs, schema);
      auto right_value = expr->Evaluate(&rhs, schema);
      if (left_value.CompareEquals(right_value) == CmpBool::CmpTrue) {
        continue;
      }
      auto comp = left_value.CompareLessThan(right_value);
      // desc降序,最大的第一个输出，在 vector的最后面 :min,min+1, max-1, ... max
      return (comp == CmpBool::CmpTrue && order_type == OrderByType::DESC) ||
             (comp == CmpBool::CmpFalse && order_type != OrderByType::DESC);
    }
    return false;
  };
  std::sort(sorted_.begin(), sorted_.end(), sorter);
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (sorted_.empty()) {
    return false;
  }
  const auto &tuple_next = sorted_.back();
  *tuple = tuple_next;
  sorted_.pop_back();
  return true;
}

}  // namespace bustub
