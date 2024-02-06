//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// abstract_executor.h
//
// Identification: src/include/execution/executors/abstract_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/executor_context.h"
#include "storage/table/tuple.h"

namespace bustub {
class ExecutorContext;
/**
 * AbstractExecutor 实现了火山迭代器模型，这是所有 executor 继承的基类，定义了所有 executors 支持的最小接口。
 *
 * The AbstractExecutor implements the Volcano tuple-at-a-time iterator model.
 * This is the base class from which all executors in the BustTub execution
 * engine inherit, and defines the minimal interface that all executors support.
 */
class AbstractExecutor {
 public:
  /**
   * Construct a new AbstractExecutor instance.
   * @param exec_ctx the executor context that the executor runs with
   */
  explicit AbstractExecutor(ExecutorContext *exec_ctx) : exec_ctx_{exec_ctx} {}

  /** Virtual destructor. */
  virtual ~AbstractExecutor() = default;

  /**
   * 初始化 executor
   * 在 Next() 函数调用之前，必须调用 Init() 函数
   *
   * Initialize the executor.
   * @warning This function must be called before Next() is called!
   */
  virtual void Init() = 0;

  /**
   * 目的是从执行器(executor)中获取下一个元组(tuple)
   * 参数是 Tuple 和它的 RID
   * 如果成功获取到了下一个元组，那么返回 true，如果没有更多的元组，返回 false.
   *
   * Yield the next tuple from this executor.
   * @param[out] tuple The next tuple produced by this executor
   * @param[out] rid The next tuple RID produced by this executor
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  virtual auto Next(Tuple *tuple, RID *rid) -> bool = 0;

  /** 返回执行器(executor)所产生的元组(tuple)的模式(schema) */
  /** @return The schema of the tuples that this executor produces */
  virtual auto GetOutputSchema() const -> const Schema & = 0;

  /** 返回 ExecutorContext */
  /** @return The executor context in which this executor runs */
  auto GetExecutorContext() -> ExecutorContext * { return exec_ctx_; }

 protected:
  /** The executor context in which the executor runs */
  ExecutorContext *exec_ctx_;
};
}  // namespace bustub
