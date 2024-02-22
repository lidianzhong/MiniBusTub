#include "catalog/table_generator.h"

#include <algorithm>
#include <random>
#include <vector>
#include "common/config.h"

namespace bustub {

template <typename CppType>
auto TableGenerator::GenNumericValues(ColumnInsertMeta *col_meta, uint32_t count) -> std::vector<Value> {
  std::vector<Value> values{};
  values.reserve(count);

  // 处理序列分布的列
  if (col_meta->dist_ == Dist::Serial) {
    for (uint32_t i = 0; i < count; i++) {
      values.emplace_back(Value(col_meta->type_, static_cast<CppType>(col_meta->serial_counter_ + col_meta->min_)));
      col_meta->serial_counter_ += 1;
    }
    return values;
  }

  // 处理循环类型的列
  if (col_meta->dist_ == Dist::Cyclic) {
    for (uint32_t i = 0; i < count; i++) {
      values.emplace_back(Value(col_meta->type_, static_cast<CppType>(col_meta->serial_counter_)));
      col_meta->serial_counter_ += 1;
      if (col_meta->serial_counter_ > col_meta->max_) {
        col_meta->serial_counter_ = 0;
      }
    }
    return values;
  }

  // 如果列的分布类型不是序列或循环，则假定该列的数据分布是随机
  std::default_random_engine generator;
  std::conditional_t<std::is_integral_v<CppType>, std::uniform_int_distribution<CppType>,
                     std::uniform_real_distribution<CppType>>
      distribution(static_cast<CppType>(col_meta->min_), static_cast<CppType>(col_meta->max_));
  for (uint32_t i = 0; i < count; i++) {
    values.emplace_back(Value(col_meta->type_, distribution(generator)));
  }
  return values;
}

auto TableGenerator::MakeValues(ColumnInsertMeta *col_meta, uint32_t count) -> std::vector<Value> {
  std::vector<Value> values;
  switch (col_meta->type_) {
    case TypeId::TINYINT:
      return GenNumericValues<int8_t>(col_meta, count);
    case TypeId::SMALLINT:
      return GenNumericValues<int16_t>(col_meta, count);
    case TypeId::INTEGER:
      return GenNumericValues<int32_t>(col_meta, count);
    case TypeId::BIGINT:
      return GenNumericValues<int64_t>(col_meta, count);
    case TypeId::DECIMAL:
      return GenNumericValues<double>(col_meta, count);
    default:
      UNREACHABLE("Not yet implemented");
  }
}

/**
 * 填充元组数据操作，按照meta指定的格式进行填充数据
 * @param info
 * @param table_meta
 */
void TableGenerator::FillTable(TableInfo *info, TableInsertMeta *table_meta) {
  uint32_t num_inserted = 0; // 初始化已插入的元组数量为0
  uint32_t batch_size = 128; // 设置批处理大小为128，即每次插入的元组数量
  // 循环直到已插入的元组数量达到或超过meta中指定的行数
  while (num_inserted < table_meta->num_rows_) {
    std::vector<std::vector<Value>> values; // vector<Value>表示一个列，每个列表示一组值
    // num_values 表示当前批处理应该插入的元组数量，取批处理大小和剩余需要插入的元组数量的较小值。
    uint32_t num_values = std::min(batch_size, table_meta->num_rows_ - num_inserted);
    // 遍历表格meta的列信息，每个列生成指定数量num_value的值
    for (auto &col_meta : table_meta->col_meta_) {
      values.emplace_back(MakeValues(&col_meta, num_values));
    }
    // 对每个元组进行循环，是按照行遍历
    for (uint32_t i = 0; i < num_values; i++) {
      std::vector<Value> entry;
      entry.reserve(values.size());
      for (const auto &col : values) {
        entry.emplace_back(col[i]);
      }
      // 对元组执行插入操作
      auto rid = info->table_->InsertTuple(TupleMeta{0, false}, Tuple(entry, &info->schema_));
      BUSTUB_ENSURE(rid != std::nullopt, "Sequential insertion cannot fail");
      num_inserted++;
    }
  }
}

void TableGenerator::GenerateTestTables() {
  /**
   * 这里是对每个测试表的配置。配置变量有：表名 name、元组行数 size、表模式 schema
   */
  std::vector<TableInsertMeta> insert_meta{
      // The empty table
      {"empty_table",
       0,
       {{"colA", TypeId::INTEGER, false, Dist::Serial, 0, 0}}},

      // test_simple_seq_1
      {"test_simple_seq_1",
       10,
       {{"col1", TypeId::INTEGER, false, Dist::Serial, 0, 10}}},

      // test_simple_seq_2
      {"test_simple_seq_2",
       10,
       {{"col1", TypeId::INTEGER, false, Dist::Serial, 0, 10},
                 {"col2", TypeId::INTEGER, false, Dist::Serial, 10, 20}}},

      // Table 1
      {"test_1",
       1000,
       {{"colA", TypeId::INTEGER, false, Dist::Serial, 0, 0},
                 {"colB", TypeId::INTEGER, false, Dist::Uniform, 0, 9},
                 {"colC", TypeId::INTEGER, false, Dist::Uniform, 0, 9999},
                 {"colD", TypeId::INTEGER, false, Dist::Uniform, 0, 99999}}},

      // Table 2
      {"test_2",
       100,
       {{"colA", TypeId::INTEGER, false, Dist::Serial, 0, 99},
                 {"colB", TypeId::INTEGER, true, Dist::Uniform, 0, 999},
                 {"colC", TypeId::INTEGER, true, Dist::Cyclic, 0, 9}}},

  };

  // 根据表模式创建对应表
  for (auto &table_meta : insert_meta) {
    // 准备好表table的表模式schema
    std::vector<Column> cols{};
    cols.reserve(table_meta.col_meta_.size());
    for (const auto &col_meta : table_meta.col_meta_) {
      if (col_meta.type_ != TypeId::VARCHAR) {
        cols.emplace_back(col_meta.name_, col_meta.type_);
      } else {
        cols.emplace_back(col_meta.name_, col_meta.type_, TEST_VARLEN_SIZE);
      }
    }
    Schema schema(cols);
    // 创建表table
    auto info = exec_ctx_->GetCatalog()->CreateTable( table_meta.name_, schema);
    // 向表table中填入元组数据
    FillTable(info, &table_meta);
  }
}
}
