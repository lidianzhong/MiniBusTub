//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
}

auto DeleteExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (has_out_) {
    return false;
  }

  Tuple old_tuple{};
  RID old_rid{};
  int32_t delete_count{0};

  // 获取 table
  auto* catalog = exec_ctx_->GetCatalog();
  auto* table_info = catalog->GetTable(plan_->table_oid_);

  while (child_executor_->Next(&old_tuple, &old_rid)) {
    // 删除 tuple
    auto delete_tuple_meta = table_info->table_->GetTupleMeta(old_rid);
    delete_tuple_meta.is_deleted_ = true;

    table_info->table_->UpdateTupleMeta(delete_tuple_meta, old_rid);

    // TODO(zhong): meta 、插入失败 未处理

    // 删除之前的 tuple 对应的索引
    const auto &index_infos = catalog->GetTableIndexes(table_info->name_);
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


    ++delete_count;
  }

  // child_executor_->Next 已经读完了
  std::vector<Value> ret_values{};
  ret_values.reserve(GetOutputSchema().GetColumnCount());
  ret_values.emplace_back(TypeId::INTEGER, delete_count);

  *tuple = Tuple{ret_values, &GetOutputSchema()};
  *rid = {};

  has_out_ = true;

  return true;
}

}  // namespace bustub
