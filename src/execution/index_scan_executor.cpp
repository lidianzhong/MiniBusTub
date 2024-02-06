//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), dummy_schema_(Schema({})) {}

void IndexScanExecutor::Init() {

  // 获取索引
  auto* index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  htable_ = dynamic_cast<HashTableIndexForTwoIntegerColumn*> (index_info->index_.get());

  // 获取索引需要的 key (Tuple 类型)
  Value key_value = plan_->pred_key_->val_;

  // 获取 Value
  htable_->ScanKey(Tuple{{key_value}, htable_->GetKeySchema()}, &ret_rids_);

}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  BUSTUB_ASSERT(htable_ != nullptr, "htable_ is nullptr. Is there an Init error?");

  if (cursor_ >= ret_rids_.size()) {
    return false;
  }

  auto ret_rid = ret_rids_[cursor_];

  // 根据 RID 获取到元组
  auto* catalog = exec_ctx_->GetCatalog();
  auto* table_info = catalog->GetTable(plan_->table_oid_);
  *tuple = table_info->table_->GetTuple(ret_rid).second;

  *rid = tuple->GetRid();
  cursor_ += 1;

  return true;

}

}  // namespace bustub
