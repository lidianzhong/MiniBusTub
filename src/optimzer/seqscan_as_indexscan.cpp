#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule

  // 首先这个 plan 需要是 SeqScan
  if (plan->GetType() == PlanType::SeqScan) {
    const auto &seq_plan = dynamic_cast<const SeqScanPlanNode &>(*plan);
    // 其次这个 SeqScan 需要有过滤 predicate
    if (seq_plan.filter_predicate_ != nullptr) {
      const auto & comp_expr = std::dynamic_pointer_cast<const ComparisonExpression>(seq_plan.filter_predicate_);
      if (comp_expr) {
        const auto & column_expr = std::dynamic_pointer_cast<const ColumnValueExpression>(comp_expr->GetChildAt(0));
        const auto value_expr = std::dynamic_pointer_cast<ConstantValueExpression>(comp_expr->GetChildAt(1));
        if (column_expr) {
          auto col_idx = column_expr->GetColIdx();

          // 获取表中所有的索引
          auto index_infos = catalog_.GetTableIndexes(seq_plan.table_name_);
          for (auto index_info : index_infos) {
            for (auto key_attr : index_info->index_->GetKeyAttrs()) {
              if (key_attr == col_idx) {
                return std::make_shared<IndexScanPlanNode>(seq_plan.output_schema_, seq_plan.table_oid_, index_info->index_oid_, seq_plan.filter_predicate_, value_expr.get());
              }
            }
          }
        }
      }
    }
  }

  return plan;
}

}  // namespace bustub
