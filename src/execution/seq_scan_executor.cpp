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
#include "execution/expressions/comparison_expression.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  auto* catalog = exec_ctx_->GetCatalog();
  auto* table_info = catalog->GetTable(plan_->table_oid_);

  // 创建迭代器以便遍历 table 的 tuple
  itr_ = std::make_unique<TableIterator>(table_info->table_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  BUSTUB_ASSERT(itr_ != nullptr, "itr is nullptr. Did you call the Init() function?");

  // 还有未扫描的元组
  while (!itr_->IsEnd()) {
    // 元组被标记为已删除，则跳过
    if (itr_->GetTuple().first.is_deleted_) {
      ++(*itr_);
      continue;
    }

    // 获取到扫描到的 tuple
    const Tuple scan_tuple = itr_->GetTuple().second;

    // 是否要过滤掉这个 tuple
    if (plan_->filter_predicate_ != nullptr && !plan_->filter_predicate_->Evaluate(&scan_tuple, GetOutputSchema()).GetAs<bool>()) {
      ++(*itr_);
      continue;
    }

    *tuple = scan_tuple;
    *rid = tuple->GetRid();

    ++(*itr_);

    return true;
  }

  // 元组已经扫描完了
  return false;
}

}  // namespace bustub
