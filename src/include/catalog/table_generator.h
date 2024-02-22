#pragma once

#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "execution/executor_context.h"
#include "storage/table/table_heap.h"

namespace bustub {

// static constexpr uint32_t TEST1_SIZE = 1000;
static constexpr uint32_t TEST_VARLEN_SIZE = 10; // varchar类型默认长度

/**
 * 表生成器，生成一些预留表，供测试使用
 */
class TableGenerator {
 public:
  explicit TableGenerator(ExecutorContext *exec_ctx) : exec_ctx_{exec_ctx} {}

  /**
   * 主要函数，生成 tables
   */
  void GenerateTestTables();

 private:
  /** 要生成的列的值的分布是怎么样的，是Uniform,还是Serial呢..., 在 GenNumericValues()函数中使用 */
  enum class Dist : uint8_t { Uniform, Zipf_50, Zipf_75, Zipf_95, Zipf_99, Serial, Cyclic };

  /**
   * 列名、列的类型、是否允许为空、值的分布、列的最小值、列的最大值、serial_counter_ // TODO(zhong): serial_counter_
   */
  struct ColumnInsertMeta {
    /**
     * Name of the column
     */
    const char *name_;
    /**
     * Type of the column
     */
    const TypeId type_;
    /**
     * Whether the column is nullable
     */
    bool nullable_;
    /**
     * Distribution of values
     */
    Dist dist_;
    /**
     * Min value of the column
     */
    uint64_t min_;
    /**
     * Max value of the column
     */
    uint64_t max_;
    /**
     * Counter to generate serial data
     */
    uint64_t serial_counter_{0};

    /**
     * Constructor
     */
    ColumnInsertMeta(const char *name, const TypeId type, bool nullable, Dist dist, uint64_t min, uint64_t max)
        : name_(name), type_(type), nullable_(nullable), dist_(dist), min_(min), max_(max) {}
  };

  /**
   * 表名、表的行数、表中的所有列
   */
  struct TableInsertMeta {
    /**
     * Name of the table
     */
    const char *name_;
    /**
     * Number of rows
     */
    uint32_t num_rows_;
    /**
     * Columns
     */
    std::vector<ColumnInsertMeta> col_meta_;

    /**
     * Constructor
     */
    TableInsertMeta(const char *name, uint32_t num_rows, std::vector<ColumnInsertMeta> col_meta)
        : name_(name), num_rows_(num_rows), col_meta_(std::move(col_meta)) {}
  };

  /**
   * 辅助函数，在table的创建中提供填充元组数据用的
   * @param info 是使用它里面的schema信息
   * @param table_meta 填充数据依据的格式，按它的格式来填充数据
   */
  void FillTable(TableInfo *info, TableInsertMeta *table_meta);

  /**
   * 辅助函数，在table的填充数据过程中(也就是FillTable函数)产生值用的
   * @param col_meta 依据它的格式来生成数据，比如 Uniform, Serial...
   * @param count 生成值的数量
   * @return 返回生成好的值，是一列Value
   */
  auto MakeValues(ColumnInsertMeta *col_meta, uint32_t count) -> std::vector<Value>;

  /**
   * 辅助函数，在 MakeValues() 函数中进行调用
   */
  template <typename CppType>
  auto GenNumericValues(ColumnInsertMeta *col_meta, uint32_t count) -> std::vector<Value>;

 private:
  ExecutorContext *exec_ctx_;
};
}
