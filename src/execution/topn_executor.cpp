#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_executor_->Init();
  sorted_.clear();
  size_t n = plan_->GetN();
  auto orderby_keys = plan_->GetOrderBy();
  auto schema = GetOutputSchema();
  auto comp = [&](const Tuple &lhs, const Tuple &rhs) {
    for (const auto &[order_type, expr] : orderby_keys) {
      auto left_value = expr->Evaluate(&lhs, schema);
      auto right_value = expr->Evaluate(&rhs, schema);
      if (left_value.CompareEquals(right_value) == CmpBool::CmpTrue) {
        continue;
      }
      auto comp = left_value.CompareLessThan(right_value);
      // 升序 ：1 < 2,...100，top = 100
      // 降序 2 > 1,
      return (comp == CmpBool::CmpTrue && order_type != OrderByType::DESC) ||
             (comp == CmpBool::CmpFalse && order_type == OrderByType::DESC);
    }
    return false;
  };

  Tuple tuple_holder{};
  RID rid_holder{};
  std::priority_queue<Tuple, std::vector<Tuple>, decltype(comp)> pq(comp);
  auto status = child_executor_->Next(&tuple_holder, &rid_holder);
  while (status) {
    if (pq.size() < n) {
      pq.push(tuple_holder);
    } else {
      if (comp(tuple_holder, pq.top())) {
        // 升序时，是大顶堆，1,2,3 ... 100.当前tuple比堆顶的小，弹出堆顶
        // 降序时，是小顶堆，当前tuple比堆顶的大，弹出堆顶
        pq.pop();
        pq.push(tuple_holder);
      }
    }
    status = child_executor_->Next(&tuple_holder, &rid_holder);
  }
  sorted_.reserve(pq.size());
  while (!pq.empty()) {
    // 升序需要先输入最小的，逆序一下. 因为是从vector的后面开始
    sorted_.push_back(pq.top());
    pq.pop();
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (sorted_.empty()) {
    return false;
  }
  *tuple = sorted_.back();
  sorted_.pop_back();
  return true;
}

}  // namespace bustub
