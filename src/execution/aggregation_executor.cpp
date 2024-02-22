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
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      aht_(plan->aggregates_, plan->agg_types_),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  // 调用 child_executor_ 的 Init() 函数
  child_executor_->Init();

  Tuple child_tuple{};
  RID child_rid{};
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    // 获取到 child_tuple 的 Aggregate key 和 Aggregate value 字段，类型是 vector<Value>
    auto agg_key = MakeAggregateKey(&child_tuple);
    auto agg_value = MakeAggregateValue(&child_tuple);

    // 注意：这里需要确定分组的值不能为 NULL,如果有 null 值，就跳过
    if (!std::all_of(agg_key.group_bys_.begin(), agg_key.group_bys_.end(),
                     [](const Value &val) -> bool { return !val.IsNull(); })) {
      // TODO(zhong): 对 GROUP BY 中 NULL 值的处理？
      std::cout << "zhong:" << "group by has null value, skip" << '\n';
      continue;
    }

    // 将这个元组放入 map 集合中
    aht_.InsertCombine(agg_key, agg_value);
  }

  aht_iterator_ = aht_.Begin();

  // 如果发现表为空，那么就将 is_empty_table_handle_ 赋上值，用 false 表示还未处理
  if (aht_iterator_ == aht_.End()) {
    is_empty_table_handle_ = false;
  }

  // 如果分组为空，那么也是直接输出空
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // 如果是空表 && 已经处理
  if (is_empty_table_handle_.has_value() && is_empty_table_handle_ == true) {
    return false;
  }

  // 是空表 && 但未处理
  if (is_empty_table_handle_.has_value() && is_empty_table_handle_ == false) {
    // 如果是空表还 group by，直接返回 false
    if (!plan_->group_bys_.empty()) {
      return false;
    }


    *tuple = {aht_.GenerateInitialAggregateValue().aggregates_, &GetOutputSchema()};
    *rid = RID{};

    is_empty_table_handle_ = true;

    return true;
  }

  // 不是空表

  if (aht_iterator_ == aht_.End()) {
    return false;
  }

  std::vector<Value> values;
  values.insert(values.end(), aht_iterator_.Key().group_bys_.begin(), aht_iterator_.Key().group_bys_.end());
  values.insert(values.end(), aht_iterator_.Val().aggregates_.begin(), aht_iterator_.Val().aggregates_.end());

  Tuple ret_tuple{values, &GetOutputSchema()};
  *tuple = ret_tuple;
  *rid = RID{};

  ++aht_iterator_;

  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
