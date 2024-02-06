#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <tuple>

#include "binder/binder.h"
#include "binder/bound_expression.h"
#include "binder/bound_statement.h"
#include "binder/statement/create_statement.h"
#include "binder/statement/explain_statement.h"
#include "binder/statement/index_statement.h"
#include "binder/statement/select_statement.h"
#include "binder/statement/set_show_statement.h"
#include "buffer/buffer_pool_manager.h"
#include "catalog/schema.h"
#include "catalog/table_generator.h"
#include "common/bustub_instance.h"
#include "common/exception.h"
#include "common/util/string_util.h"
#include "execution/check_options.h"
#include "execution/execution_engine.h"
#include "execution/executor_context.h"
#include "execution/executors/mock_scan_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/abstract_plan.h"
#include "fmt/core.h"
#include "fmt/format.h"
#include "optimizer/optimizer.h"
#include "planner/planner.h"
#include "storage/disk/disk_manager.h"
#include "storage/disk/disk_manager_memory.h"
#include "type/value_factory.h"

namespace bustub {

auto BustubInstance::MakeExecutorContext(bool is_modify) -> std::unique_ptr<ExecutorContext> {
  return std::make_unique<ExecutorContext>(catalog_.get(), buffer_pool_manager_.get(), is_modify);
}

BustubInstance::BustubInstance(const std::string &db_file_name) {

  // Storage related.
  disk_manager_ = std::make_unique<DiskManager>(db_file_name);

  // We need more frames for GenerateTestTable to work. Therefore, we use 128 instead of the default
  // buffer pool size specified in `config.h`.
  try {
    buffer_pool_manager_ =
        std::make_unique<BufferPoolManager>(128, disk_manager_.get(), LRUK_REPLACER_K);
  } catch (NotImplementedException &e) {
    std::cerr << "BufferPoolManager is not implemented, only mock tables are supported." << std::endl;
    buffer_pool_manager_ = nullptr;
  }

  // Catalog related.
  catalog_ = std::make_unique<Catalog>(buffer_pool_manager_.get());

  // Execution engine related.
  execution_engine_ = std::make_unique<ExecutionEngine>(buffer_pool_manager_.get(), catalog_.get());
}

BustubInstance::BustubInstance() {

  // Storage related.
  disk_manager_ = std::make_unique<DiskManagerUnlimitedMemory>();

  // We need more frames for GenerateTestTable to work. Therefore, we use 128 instead of the default
  // buffer pool size specified in `config.h`.
  try {
    buffer_pool_manager_ =
        std::make_unique<BufferPoolManager>(128, disk_manager_.get(), LRUK_REPLACER_K);
  } catch (NotImplementedException &e) {
    std::cerr << "BufferPoolManager is not implemented, only mock tables are supported." << std::endl;
    buffer_pool_manager_ = nullptr;
  }

  // Catalog related.
  catalog_ = std::make_unique<Catalog>(buffer_pool_manager_.get());

  // Execution engine related.
  execution_engine_ = std::make_unique<ExecutionEngine>(buffer_pool_manager_.get(), catalog_.get());
}

/**
 * 使用 \dt 命令显示所有的 table
 * @param writer
 */
void BustubInstance::CmdDisplayTables(ResultWriter &writer) {
  auto table_names = catalog_->GetTableNames();
  writer.BeginTable(false);
  // 标题行
  writer.BeginHeader();
  writer.WriteHeaderCell("oid");
  writer.WriteHeaderCell("name");
  writer.WriteHeaderCell("cols");
  writer.EndHeader();
  // 结果集的每一行
  for (const auto &name : table_names) {
    writer.BeginRow();
    const auto *table_info = catalog_->GetTable(name);
    writer.WriteCell(fmt::format("{}", table_info->oid_));
    writer.WriteCell(table_info->name_);
    writer.WriteCell(table_info->schema_.ToString());
    writer.EndRow();
  }
  writer.EndTable();
}

void BustubInstance::CmdDisplayIndices(ResultWriter &writer) {
  auto table_names = catalog_->GetTableNames();
  writer.BeginTable(false);
  // 标题行
  writer.BeginHeader();
  writer.WriteHeaderCell("table_name");
  writer.WriteHeaderCell("index_oid");
  writer.WriteHeaderCell("index_name");
  writer.WriteHeaderCell("index_cols");
  writer.EndHeader();
  // 结果集的每一行
  for (const auto &table_name : table_names) {
    for (const auto *index_info : catalog_->GetTableIndexes(table_name)) {
      writer.BeginRow();
      writer.WriteCell(table_name);
      writer.WriteCell(fmt::format("{}", index_info->index_oid_));
      writer.WriteCell(index_info->name_);
      writer.WriteCell(index_info->key_schema_.ToString());
      writer.EndRow();
    }
  }
  writer.EndTable();
}

void BustubInstance::WriteOneCell(const std::string &cell, ResultWriter &writer) { writer.OneCell(cell); }

void BustubInstance::CmdDisplayHelp(ResultWriter &writer) {
  std::string help = R"(Welcome to the MiniBusTub shell!

\dt: 显示所有表的信息
\di: 显示所有索引的信息
\help: 再次显示当前信息

MiniBusTub shell 支持基本的增删改查，支持使用索引，支持使用 `explain` 命令查看当前的查询计划
)";
  WriteOneCell(help, writer);
}


/**
 * 执行 sql 语句
 * @param sql 需要执行的 sql 语句
 * @param writer 结果的输出方式
 * @return
 */
auto BustubInstance::ExecuteSql(const std::string &sql, ResultWriter &writer) -> bool {
  // 处理预设命令
  if (!sql.empty() && sql[0] == '\\') {
    if (sql == "\\dt") {
      CmdDisplayTables(writer); // 显示所有的 table
      return true;
    }
    if (sql == "\\di") {
      CmdDisplayIndices(writer); // 显示索引
      return true;
    }
    if (sql == "\\help") {
      CmdDisplayHelp(writer); // 显示 Help
      return true;
    }
    throw Exception(fmt::format("unsupported internal command: {}", sql));
  }

  bool is_successful = true;

  // ============================= Parse =============================
  // 生成 Postgres AST，结果保存到 statement_nodes_ 中
  std::shared_lock<std::shared_mutex> l(catalog_lock_);
  bustub::Binder binder(*catalog_);
  binder.ParseAndSave(sql);
  l.unlock();

  // 一个 sql 语句是一个 stmt
  for (auto *stmt : binder.statement_nodes_) {

    // ============================= Bind =============================
    // 将 Postgres AST 改写为 BusTub AST
    auto statement = binder.BindStatement(stmt);

    bool is_delete = false;

    switch (statement->type_) {
      case StatementType::CREATE_STATEMENT: {
        const auto &create_stmt = dynamic_cast<const CreateStatement &>(*statement);
        HandleCreateStatement(create_stmt, writer);
        continue;
      }
      case StatementType::INDEX_STATEMENT: {
        const auto &index_stmt = dynamic_cast<const IndexStatement &>(*statement);
        HandleIndexStatement(index_stmt, writer);
        continue;
      }
      case StatementType::VARIABLE_SHOW_STATEMENT: {
        const auto &show_stmt = dynamic_cast<const VariableShowStatement &>(*statement);
        HandleVariableShowStatement(show_stmt, writer);
        continue;
      }
      case StatementType::VARIABLE_SET_STATEMENT: {
        const auto &set_stmt = dynamic_cast<const VariableSetStatement &>(*statement);
        HandleVariableSetStatement(set_stmt, writer);
        continue;
      }
      case StatementType::EXPLAIN_STATEMENT: {
        const auto &explain_stmt = dynamic_cast<const ExplainStatement &>(*statement);
        HandleExplainStatement( explain_stmt, writer);
        continue;
      }
      case StatementType::DELETE_STATEMENT:
      case StatementType::UPDATE_STATEMENT:
        is_delete = true;
      default:
        break;
    }

    std::shared_lock<std::shared_mutex> l(catalog_lock_);

    // ============================= Plan =============================
    // Plan the query.
    bustub::Planner planner(*catalog_);
    planner.PlanQuery(*statement);

    // ============================= Optimize =============================
    // Optimize the query.
    bustub::Optimizer optimizer(*catalog_, IsForceStarterRule());
    auto optimized_plan = optimizer.Optimize(planner.plan_);

    l.unlock();

    // ============================= Execute =============================
    // Execute the query.
    auto exec_ctx = MakeExecutorContext(is_delete);
    std::vector<Tuple> result_set{};

    is_successful &= execution_engine_->Execute(optimized_plan, &result_set, exec_ctx.get());

    // 将 execute 的结果作为 string 类型的 vector 返回
    // Return the result set as a vector of string.
    auto schema = planner.plan_->OutputSchema();

    // Generate header for the result set.
    writer.BeginTable(false);
    writer.BeginHeader();
    for (const auto &column : schema.GetColumns()) {
      writer.WriteHeaderCell(column.GetName());
    }
    writer.EndHeader();

    // Transforming result set into strings.
    for (const auto &tuple : result_set) {
      writer.BeginRow();
      for (uint32_t i = 0; i < schema.GetColumnCount(); i++) {
        writer.WriteCell(tuple.GetValue(&schema, i).ToString());
      }
      writer.EndRow();
    }
    writer.EndTable();
  }

  return is_successful;
}

/**
 * FOR TEST ONLY. Generate test tables in this BusTub instance.
 * It's used in the shell to predefine some tables, as we don't support
 * create / drop table and insert for now. Should remove it in the future.
 */
void BustubInstance::GenerateTestTable() {
  auto exec_ctx = MakeExecutorContext(false);
  TableGenerator gen{exec_ctx.get()};

  std::shared_lock<std::shared_mutex> l(catalog_lock_);
  gen.GenerateTestTables();
  l.unlock();
}

/**
 * FOR TEST ONLY. Generate test tables in this BusTub instance.
 * It's used in the shell to predefine some tables, as we don't support
 * create / drop table and insert for now. Should remove it in the future.
 */
void BustubInstance::GenerateMockTable() {
  std::shared_lock<std::shared_mutex> l(catalog_lock_);
  for (auto table_name = &mock_table_list[0]; *table_name != nullptr; table_name++) {
    catalog_->CreateTable(*table_name, GetMockTableSchemaOf(*table_name));
  }
  l.unlock();
}

BustubInstance::~BustubInstance() = default;

}  // namespace bustub
