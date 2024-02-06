#pragma once

#include <atomic>
#include <fstream>
#include <future>  // NOLINT
#include <mutex>   // NOLINT
#include <string>

#include "common/config.h"

namespace bustub {

/**
 * DiskManager 负责与磁盘交互. 它可以将 a page 的数据写入磁盘, 也可以从磁盘中读取 a page 内容.
 */
class DiskManager {
 public:
  /**
   * DiskManager 的构造函数
   * @param db_file 需要写入的数据库文件名
   */
  explicit DiskManager(const std::string &db_file);

  /** FOR TEST / LEADERBOARD ONLY, used by DiskManagerMemory */
  DiskManager() = default;

  virtual ~DiskManager() = default;

  /**
   * 关闭 disk manager，关闭所有文件资源。
   */
  void ShutDown();

  /**
   * 向数据库文件写入 page
   * @param page_id
   * @param page_data
   */
  virtual void WritePage(page_id_t page_id, const char *page_data);

  /**
   * 从数据库文件中读取 page
   * @param page_id
   * @param[out] page_data
   */
  virtual void ReadPage(page_id_t page_id, char *page_data);

  /**
   * 将整个日志缓冲区写入磁盘
   * @param log_data 日志的数据
   * @param size 日志的大小
   */
  void WriteLog(char *log_data, int size);

  /**
   * 从日志文件中读入一条日志记录
   * 如果读取成功，将返回 true，否则返回 false
   * @param[out] log_data output buffer
   * @param size size of the log entry
   * @param offset offset of the log entry in the file
   * @return true if the read was successful, false otherwise
   */
  auto ReadLog(char *log_data, int size, int offset) -> bool;

  /** @return 磁盘 flush 的次数 */
  auto GetNumFlushes() const -> int;

  /** @return true iff the in-memory content has not been flushed yet */
  auto GetFlushState() const -> bool;

  /** @return 磁盘执行写操作的次数 */
  auto GetNumWrites() const -> int;

  /**
   * Sets the future which is used to check for non-blocking flushes.
   * @param f the non-blocking flush check
   */
  inline void SetFlushLogFuture(std::future<void> *f) { flush_log_f_ = f; }

  /** Checks if the non-blocking flush future was set. */
  inline auto HasFlushLogFuture() -> bool { return flush_log_f_ != nullptr; }

 protected:
  auto GetFileSize(const std::string &file_name) -> int;
  // stream to write log file
  std::fstream log_io_;
  std::string log_name_;
  // stream to write db file
  std::fstream db_io_;
  std::string file_name_;
  int num_flushes_{0};
  int num_writes_{0};
  bool flush_log_{false};
  std::future<void> *flush_log_f_{nullptr};
  // With multiple buffer pool instances, need to protect file access
  std::mutex db_io_latch_;
};

}  // namespace bustub
