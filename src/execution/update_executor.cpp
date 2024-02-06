//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {

}

void UpdateExecutor::Init() {
  child_executor_->Init();

  // 获取 table
  auto* catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(plan_->table_oid_);

}

auto UpdateExecutor::Next(Tuple *tuple, RID *rid) -> bool {

  if (has_out_) {
    return false;
  }

  Tuple old_tuple{};
  RID old_rid{};
  int32_t update_count{0};


  while (child_executor_->Next(&old_tuple, &old_rid)) {
    // 更新 tuple
    // 1. 删除 tuple
    auto delete_tuple_meta = table_info_->table_->GetTupleMeta(old_rid);
    delete_tuple_meta.is_deleted_ = true;

    table_info_->table_->UpdateTupleMeta(delete_tuple_meta, old_rid);

    // 2. 插入 tuple
    std::vector<Value> values{};
    values.reserve(GetOutputSchema().GetColumnCount());
    for (const auto& col_expr : plan_->GetExpressions()) {
      values.push_back(col_expr->Evaluate(&old_tuple, child_executor_->GetOutputSchema()));
    }

    Tuple insert_tuple{values, &child_executor_->GetOutputSchema()};
    auto insert_rid = table_info_->table_->InsertTuple({}, insert_tuple);
    if (!insert_rid) {
      continue;
    }
    // TODO(zhong): meta 、插入失败未处理

    auto* catalog = exec_ctx_->GetCatalog();
    const auto &index_infos = catalog->GetTableIndexes(table_info_->name_);

    // 删除之前的 tuple 对应的索引
    for (auto index_info : index_infos) {

      // 索引在table中的列
      auto key_attr = index_info->index_->GetKeyAttrs()[0];
      // 获取该列的Value值
      auto key_value = old_tuple.GetValue(&child_executor_->GetOutputSchema(),key_attr);
      auto key_schema = Schema{{{"index_key", key_value.GetTypeId()}}};
      // 用该列的 value 值生成 索引的 key
      Tuple index_tuple = {{key_value}, &key_schema};
      index_info->index_->DeleteEntry(index_tuple, old_rid);
    }

    // 插入新的 tuple 的索引
    for (auto index_info : index_infos) {

      // 索引在table中的列
      auto key_attr = index_info->index_->GetKeyAttrs()[0];
      // 获取该列的Value值
      auto key_value = insert_tuple.GetValue(&child_executor_->GetOutputSchema(),key_attr);
      auto key_schema = Schema{{{"index_key", key_value.GetTypeId()}}};
      // 用该列的 value 值生成 索引的 key
      Tuple index_tuple = {{key_value}, &key_schema};
      index_info->index_->InsertEntry(index_tuple, insert_rid.value());
    }

    ++update_count;
  }

  // child_executor_->Next 已经读完了
  std::vector<Value> ret_values{};
  ret_values.emplace_back(TypeId::INTEGER, update_count);

  *tuple = Tuple{ret_values, &GetOutputSchema()};
  *rid = {};

  has_out_ = true;

  return true;

}

}  // namespace bustub
