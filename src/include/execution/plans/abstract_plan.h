//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// abstract_plan.h
//
// Identification: src/include/execution/plans/abstract_plan.h
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/schema.h"
#include "fmt/format.h"

namespace bustub {

#define BUSTUB_PLAN_NODE_CLONE_WITH_CHILDREN(cname)                                                          \
  auto CloneWithChildren(std::vector<AbstractPlanNodeRef> children) const->std::unique_ptr<AbstractPlanNode> \
      override {                                                                                             \
    auto plan_node = cname(*this);                                                                           \
    plan_node.children_ = children;                                                                          \
    return std::make_unique<cname>(std::move(plan_node));                                                    \
  }

/** PlanType 表示了这个系统支持的 plan 类型 */
/** PlanType represents the types of plans that we have in our system. */
enum class PlanType {
  SeqScan,          // 逐行扫描整个表
  IndexScan,        // 使用索引来检索
  Insert,           // 向表中插入数据
  Update,           // 更新表中的数据
  Delete,           // 删除表中的数据
  Aggregation,      // 执行聚合操作，如 SUM、AVG 等
  Limit,            // 限制查询结果的行数
  NestedLoopJoin,   // 执行嵌套循环连接
  NestedIndexJoin,  // 执行嵌套索引连接
  HashJoin,         // 执行哈希连接
  Filter,           // 执行过滤操作，根据指定的条件过滤行
  Values,           // 直接提供一组常量值
  Projection,       // 投影计划，用于选择查询结果的特定列
  Sort,             // 对查询结果进行排序
  TopN,             // 获取查询结果的前N行
  TopNPerGroup,     // 获取每个分组的前N行
  MockScan,         // 模拟扫描计划
  InitCheck,        // 初始化检查计划
  Window            // 执行窗口函数操作
};

class AbstractPlanNode;
using AbstractPlanNodeRef = std::shared_ptr<const AbstractPlanNode>;

/**
 * AbstractPlanNode 代表整个系统中所有可能的计划类型(plan type)。
 * 它是作为树(tree)的节点，每个节点的子节点的数量是可变的。
 * 根据火山模型(volcano model)，计划节点(plan node)接收子节点的元组(tuple)
 *
 * AbstractPlanNode represents all the possible types of plan nodes in our system.
 * Plan nodes are modeled as trees, so each plan node can have a variable number of children.
 * Per the Volcano model, the plan node receives the tuples of its children.
 * The ordering of the children may matter.
 */
class AbstractPlanNode {
 public:
  /**
   * Create a new AbstractPlanNode with the specified output schema and children.
   * @param output_schema The schema for the output of this plan node
   * @param children The children of this plan node
   */
  AbstractPlanNode(SchemaRef output_schema, std::vector<AbstractPlanNodeRef> children)
      : output_schema_(std::move(output_schema)), children_(std::move(children)) {}

  /** Virtual destructor. */
  virtual ~AbstractPlanNode() = default;

  /** @return the schema for the output of this plan node */
  auto OutputSchema() const -> const Schema & { return *output_schema_; }

  /** @return the child of this plan node at index child_idx */
  auto GetChildAt(uint32_t child_idx) const -> AbstractPlanNodeRef { return children_[child_idx]; }

  /** @return the children of this plan node */
  auto GetChildren() const -> const std::vector<AbstractPlanNodeRef> & { return children_; }

  /** @return the type of this plan node */
  virtual auto GetType() const -> PlanType = 0;

  /** @return the string representation of the plan node and its children */
  auto ToString(bool with_schema = true) const -> std::string {
    if (with_schema) {
      return fmt::format("{} | {}{}", PlanNodeToString(), output_schema_, ChildrenToString(2, with_schema));
    }
    return fmt::format("{}{}", PlanNodeToString(), ChildrenToString(2, with_schema));
  }

  /** @return the cloned plan node with new children */
  virtual auto CloneWithChildren(std::vector<AbstractPlanNodeRef> children) const
      -> std::unique_ptr<AbstractPlanNode> = 0;

  /**
   * 当前计划节点(plan node)的输出模式(output schema).
   * 在火山模型中，每个计划节点(plan node)都会吐出元组(tuple)，这将告诉你使用什么模式(schema)
   *
   * The schema for the output of this plan node. In the volcano model, every plan node will spit out tuples,
   * and this tells you what schema this plan node's tuples will have.
   */
  SchemaRef output_schema_;

  /** plan node 的孩子节点 */
  /** The children of this plan node. */
  std::vector<AbstractPlanNodeRef> children_;

 protected:
  /** 返回 plan node 自身的字符串表现形式 */
  /** @return the string representation of the plan node itself */
  virtual auto PlanNodeToString() const -> std::string { return "<unknown>"; }

  /** 返回 plan node 的孩子节点的字符串表现形式 */
  /** @return the string representation of the plan node's children */
  auto ChildrenToString(int indent, bool with_schema = true) const -> std::string;

 private:
};

}  // namespace bustub

template <typename T>
struct fmt::formatter<T, std::enable_if_t<std::is_base_of<bustub::AbstractPlanNode, T>::value, char>>
    : fmt::formatter<std::string> {
  template <typename FormatCtx>
  auto format(const T &x, FormatCtx &ctx) const {
    return fmt::formatter<std::string>::format(x.ToString(), ctx);
  }
};

template <typename T>
struct fmt::formatter<std::unique_ptr<T>, std::enable_if_t<std::is_base_of<bustub::AbstractPlanNode, T>::value, char>>
    : fmt::formatter<std::string> {
  template <typename FormatCtx>
  auto format(const std::unique_ptr<T> &x, FormatCtx &ctx) const {
    return fmt::formatter<std::string>::format(x->ToString(), ctx);
  }
};
