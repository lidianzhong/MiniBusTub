#include "execution/executors/values_executor.h"

namespace bustub {

ValuesExecutor::ValuesExecutor(ExecutorContext *exec_ctx, const ValuesPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), dummy_schema_(Schema({})) {}

void ValuesExecutor::Init() { cursor_ = 0; }

auto ValuesExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // 如果 cursor_ 到头了，那么返回 false
  if (cursor_ >= plan_->GetValues().size()) {
    return false;
  }

  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());

  // 通过 cursor_ 获取一行的 tuple
  const auto &row_expr = plan_->GetValues()[cursor_];
  for (const auto &col : row_expr) {
    values.push_back(col->Evaluate(nullptr, dummy_schema_));
  }

  // 将生成的 value 变成元组
  *tuple = Tuple{values, &GetOutputSchema()};
  cursor_ += 1;

  return true;
}

}  // namespace bustub
