//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.h
//
// Identification: src/include/execution/executors/index_scan_executor.h
//
// Copyright (c) 2015-20, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "common/rid.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/index_scan_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * IndexScanExecutor executes an index scan over a table.
 */

class IndexScanExecutor : public AbstractExecutor {
 public:
  /**
   * Creates a new index scan executor.
   * @param exec_ctx the executor context
   * @param plan the index scan plan to be executed
   */
  IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan);

  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  void Init() override;

  auto Next(Tuple *tuple, RID *rid) -> bool override;

 private:
  /** The index scan plan node to be executed. */
  const IndexScanPlanNode *plan_;

  /** 从 plan 中获取要使用到的索引 */
  Index* htable_{nullptr};

  /** 虚拟的 schema，用于提供 GetOutputSchema() 的返回值 */
  const Schema dummy_schema_;

  /** cursor 用于处理现在处理的第几个 value */
  size_t cursor_{0};

  /** 查询获取到的 value 集合 */
  std::vector<RID> ret_rids_;

};
}  // namespace bustub
