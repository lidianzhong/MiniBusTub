//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// executor_context.h
//
// Identification: src/include/execution/executor_context.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <deque>
#include <memory>
#include <unordered_set>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "execution/check_options.h"

namespace bustub {
class AbstractExecutor;
/**
 * ExecutorContext stores all the context necessary to run an executor.
 */
class ExecutorContext {
 public:
  /**
   * Creates an ExecutorContext for the transaction that is executing the query.
   * @param catalog The catalog that the executor uses
   * @param bpm The buffer pool manager that the executor uses
   * @param txn_mgr The transaction manager that the executor uses
   * @param lock_mgr The lock manager that the executor uses
   */
  ExecutorContext(Catalog *catalog, BufferPoolManager *bpm, bool is_delete)
      : catalog_{catalog},
        bpm_{bpm},
        is_delete_(is_delete) {
    nlj_check_exec_set_ = std::deque<std::pair<AbstractExecutor *, AbstractExecutor *>>(
        std::deque<std::pair<AbstractExecutor *, AbstractExecutor *>>{});
    check_options_ = std::make_shared<CheckOptions>();
  }

  ~ExecutorContext() = default;

  DISALLOW_COPY_AND_MOVE(ExecutorContext);

  /** @return the catalog */
  auto GetCatalog() -> Catalog * { return catalog_; }

  /** @return the buffer pool manager */
  auto GetBufferPoolManager() -> BufferPoolManager * { return bpm_; }

  /** @return the set of nlj check executors */
  auto GetNLJCheckExecutorSet() -> std::deque<std::pair<AbstractExecutor *, AbstractExecutor *>> & {
    return nlj_check_exec_set_;
  }

  /** @return the check options */
  auto GetCheckOptions() -> std::shared_ptr<CheckOptions> { return check_options_; }

  void AddCheckExecutor(AbstractExecutor *left_exec, AbstractExecutor *right_exec) {
    nlj_check_exec_set_.emplace_back(left_exec, right_exec);
  }

  void InitCheckOptions(std::shared_ptr<CheckOptions> &&check_options) {
    BUSTUB_ASSERT(check_options, "nullptr");
    check_options_ = std::move(check_options);
  }

  auto IsDelete() const -> bool { return is_delete_; }

 private:
  /** The database catalog associated with this executor context */
  Catalog *catalog_;
  /** The buffer pool manager associated with this executor context */
  BufferPoolManager *bpm_;
  /** The set of NLJ check executors associated with this executor context */
  std::deque<std::pair<AbstractExecutor *, AbstractExecutor *>> nlj_check_exec_set_;
  /** The set of check options associated with this executor context */
  std::shared_ptr<CheckOptions> check_options_;
  bool is_delete_;
};

}  // namespace bustub
