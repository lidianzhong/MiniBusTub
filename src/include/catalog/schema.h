#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "catalog/column.h"
#include "common/exception.h"
#include "type/type.h"

namespace bustub {

class Schema;
using SchemaRef = std::shared_ptr<const Schema>;

class Schema {
 public:
  /**
   * 表模式，由一组列构成，顺序为从左往右
   * @param columns 组成的一组列
   */
  explicit Schema(const std::vector<Column> &columns);

  /**
   * 函数的功能是 给你指定列 attrs，复制一份 Schema
   */
  static auto CopySchema(const Schema *from, const std::vector<uint32_t> &attrs) -> Schema {
    // clos vector用于存放新 Schema 的列
    std::vector<Column> cols;
    cols.reserve(attrs.size()); // 预分配足够的空间，有助于提高性能，避免重新分配
    // 遍历给定的 attrs 列，将列加入到 clos vector 中
    for (const auto i : attrs) {
      cols.emplace_back(from->columns_[i]);
    }
    // 返回新的复制的 Schema
    return Schema{cols};
  }

  /** @return 表模式中的所有列 */
  auto GetColumns() const -> const std::vector<Column> & { return columns_; }

  /**
   * 返回表模式中的指定列
   * @param col_idx 指定列的索引
   * @return 指定列
   */
  auto GetColumn(const uint32_t col_idx) const -> const Column & { return columns_[col_idx]; }

  /**
   * 根据列的名字来查找表模式中的特定列的索引，如果查找的模式有重复的名字，返回找到的第一个列的索引
   * @param col_name 要查找的列的名字
   * @return 返回找到的列的索引，如果不存在抛出异常
   */
  auto GetColIdx(const std::string &col_name) const -> uint32_t {
    if (auto col_idx = TryGetColIdx(col_name)) {
      return *col_idx;
    }
    UNREACHABLE("Column does not exist");
  }

  /**
   * 根据列的名字来查找表模式中的特定列的索引，如果查找的模式有重复的名字，返回找到的第一个列的索引
   * @param col_name 要查找的列的名字
   * @return 返回找到的列的索引，如果不存在返回 `std::nullopt`
   */
  auto TryGetColIdx(const std::string &col_name) const -> std::optional<uint32_t> {
    for (uint32_t i = 0; i < columns_.size(); ++i) {
      if (columns_[i].GetName() == col_name) {
        return std::optional{i};
      }
    }
    return std::nullopt;
  }

  /** 返回 非内联列的索引 */
  /** @return the indices of non-inlined columns */
  auto GetUnlinedColumns() const -> const std::vector<uint32_t> & { return uninlined_columns_; }

  /** 返回 列的数量 */
  /** @return the number of columns in the schema for the tuple */
  auto GetColumnCount() const -> uint32_t { return static_cast<uint32_t>(columns_.size()); }

  /** 返回 非内联列的数量*/
  /** @return the number of non-inlined columns */
  auto GetUnlinedColumnCount() const -> uint32_t { return static_cast<uint32_t>(uninlined_columns_.size()); }

  /** 返回 元组占用的字节数 */
  /** @return the number of bytes used by one tuple */
  inline auto GetLength() const -> uint32_t { return length_; }

  /** @return true if all columns are inlined, false otherwise */
  inline auto IsInlined() const -> bool { return tuple_is_inlined_; }

  /** @return string representation of this schema */
  auto ToString(bool simplified = true) const -> std::string;

 private:
  /** Fixed-length column size, i.e. the number of bytes used by one tuple. */
  uint32_t length_;

  /** All the columns in the schema, inlined and uninlined. */
  std::vector<Column> columns_;

  /** 如果所有的 column 都是 inline，返回 true。否则返回 false */
  /** True if all the columns are inlined, false otherwise. */
  bool tuple_is_inlined_{true};

  /** Indices of all uninlined columns. */
  std::vector<uint32_t> uninlined_columns_;
};

}  // namespace bustub

template <typename T>
struct fmt::formatter<T, std::enable_if_t<std::is_base_of<bustub::Schema, T>::value, char>>
    : fmt::formatter<std::string> {
  template <typename FormatCtx>
  auto format(const bustub::Schema &x, FormatCtx &ctx) const {
    return fmt::formatter<std::string>::format(x.ToString(), ctx);
  }
};

template <typename T>
struct fmt::formatter<std::shared_ptr<T>, std::enable_if_t<std::is_base_of<bustub::Schema, T>::value, char>>
    : fmt::formatter<std::string> {
  template <typename FormatCtx>
  auto format(const std::shared_ptr<T> &x, FormatCtx &ctx) const {
    if (x != nullptr) {
      return fmt::formatter<std::string>::format(x->ToString(), ctx);
    }
    return fmt::formatter<std::string>::format("", ctx);
  }
};

template <typename T>
struct fmt::formatter<std::unique_ptr<T>, std::enable_if_t<std::is_base_of<bustub::Schema, T>::value, char>>
    : fmt::formatter<std::string> {
  template <typename FormatCtx>
  auto format(const std::unique_ptr<T> &x, FormatCtx &ctx) const {
    if (x != nullptr) {
      return fmt::formatter<std::string>::format(x->ToString(), ctx);
    }
    return fmt::formatter<std::string>::format("", ctx);
  }
};
