//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() { child_executor_->Init(); }

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (has_out_) {
    return false;
  }

  Tuple child_tuple{};
  int32_t insert_count{0};
  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());

  // 获取要插入元组的 table
  auto *catalog = exec_ctx_->GetCatalog();
  auto *table_info = catalog->GetTable(plan_->table_oid_);

  RID dummy_rid{};
  while (child_executor_->Next(&child_tuple, &dummy_rid)) {
    // 插入 tuple
    auto insert_rid = table_info->table_->InsertTuple({}, child_tuple);
    if (!insert_rid) {
      std::cout << "zhong:" << "插入失败" << '\n';
      continue;
    }
    // TODO(zhong): meta 、插入失败有问题

    const auto &index_infos = catalog->GetTableIndexes(table_info->name_);
    for (auto index_info : index_infos) {
      // 用该列的 value 值生成 索引的 key
      index_info->index_->InsertEntry(child_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()), *insert_rid);
    }

    ++insert_count;
  }

  // child_executor_->Next 已经读完了
  values.emplace_back(ValueFactory::GetIntegerValue(insert_count));

  *tuple = Tuple{values, &GetOutputSchema()};
  *rid = {};

  has_out_ = true;

  return true;
}

}  // namespace bustub
