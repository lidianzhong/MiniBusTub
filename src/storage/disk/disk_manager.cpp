//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_manager.cpp
//
// Identification: src/storage/disk/disk_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sys/stat.h>
#include <cassert>
#include <cstring>
#include <iostream>
#include <mutex>  // NOLINT
#include <string>
#include <thread>  // NOLINT

#include "common/exception.h"
#include "common/logger.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

static char *buffer_used;

/**
 * 构造函数: 打开 / 创建 一个数据库 & 日志文件
 * @input db_file: 数据库文件名
 *
 */
DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::string::size_type n = file_name_.rfind('.'); // 查找文件名的".", 比如 db.file
  if (n == std::string::npos) {
    LOG_DEBUG("wrong file format"); // 如果找不到点, 说明文件格式错误
    return;
  }
  log_name_ = file_name_.substr(0, n) + ".log"; // 文件名点之前的部分拼接".log", 得到日志文件名

  log_io_.open(log_name_, std::ios::binary | std::ios::in | std::ios::app | std::ios::out); // 尝试打开日志文件
  if (!log_io_.is_open()) {
      log_io_.clear();
      // 如果文件不存在或者打开失败, 则尝试创建一个新的日志文件
      log_io_.open(log_name_, std::ios::binary | std::ios::trunc | std::ios::out | std::ios::in);
    if (!log_io_.is_open()) {
      throw Exception("can't open dblog file"); // 日志文件仍然打开失败, 抛出异常
    }
  }

  std::scoped_lock scoped_db_io_latch(db_io_latch_); // 防止多个线程同时对数据库文件进行操作
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out | std::ios::in);
    if (!db_io_.is_open()) {
      throw Exception("can't open db file");
    }
  }
  buffer_used = nullptr; // TODO(zhong): buffer_used 的作用是什么？
}

/**
 * 关闭所有的文件流
 *
 */
void DiskManager::ShutDown() {
  {
    std::scoped_lock scoped_db_io_latch(db_io_latch_);
    db_io_.close(); // 关闭数据库文件
  }
  log_io_.close(); // 关闭日志文件
}

/**
 * 将指定页的内容写入磁盘文件
 *
 */
void DiskManager::WritePage(page_id_t page_id, const char *page_data) {
  std::scoped_lock scoped_db_io_latch(db_io_latch_); // 使用锁确保在执行文件IO操作时没有其他线程干扰
  size_t offset = static_cast<size_t>(page_id) * BUSTUB_PAGE_SIZE; // 计算要写入的页的偏移量
  // set write cursor to offset
  num_writes_ += 1; // 写入计数器加一
  db_io_.seekp(offset);
  db_io_.write(page_data, BUSTUB_PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad()) {
    LOG_DEBUG("I/O error while writing");
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}

/**
 * 从给定的内存区域读取特定页的内容
 *
 */
void DiskManager::ReadPage(page_id_t page_id, char *page_data) {
  std::scoped_lock scoped_db_io_latch(db_io_latch_); // 使用锁确保在执行文件IO操作时没有其他线程干扰
  int offset = page_id * BUSTUB_PAGE_SIZE; // 计算要读取的页的偏移量
  // check if read beyond file length
  if (offset > GetFileSize(file_name_)) {
    LOG_DEBUG("I/O error reading past end of file"); // 如果发现偏移量比文件大小还大, 报错
    // std::cerr << "I/O error while reading" << std::endl;
  } else {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, BUSTUB_PAGE_SIZE);
    if (db_io_.bad()) {
      LOG_DEBUG("I/O error while reading");
      return;
    }
    // if file ends before reading BUSTUB_PAGE_SIZE
    int read_count = db_io_.gcount(); // 获取已经读取数据的结尾
    if (read_count < BUSTUB_PAGE_SIZE) {
      LOG_DEBUG("Read less than a page");
      db_io_.clear();
      // 从已经读取的末尾处开始，将剩余部分的值填充为 0
      memset(page_data + read_count, 0, BUSTUB_PAGE_SIZE - read_count);
    }
  }
}

/**
 * 将日志内容写入磁盘文件
 * Only return when sync is done, and only perform sequence write
 */
void DiskManager::WriteLog(char *log_data, int size) {
  // enforce swap log buffer
  assert(log_data != buffer_used);
  buffer_used = log_data;

  if (size == 0) {  // no effect on num_flushes_ if log buffer is empty
    return;
  }

  flush_log_ = true;

  if (flush_log_f_ != nullptr) {
    // used for checking non-blocking flushing
    assert(flush_log_f_->wait_for(std::chrono::seconds(10)) == std::future_status::ready);
  }

  num_flushes_ += 1;
  // sequence write
  log_io_.write(log_data, size);

  // check for I/O error
  if (log_io_.bad()) {
    LOG_DEBUG("I/O error while writing log");
    return;
  }
  // needs to flush to keep disk file in sync
  log_io_.flush();
  flush_log_ = false;
}

/**
 * 从内存中读取日志内容
 * Always read from the beginning and perform sequence read
 * @return: false means already reach the end
 */
auto DiskManager::ReadLog(char *log_data, int size, int offset) -> bool {
  if (offset >= GetFileSize(log_name_)) {
    // LOG_DEBUG("end of log file");
    // LOG_DEBUG("file size is %d", GetFileSize(log_name_));
    return false;
  }
  log_io_.seekp(offset);
  log_io_.read(log_data, size);

  if (log_io_.bad()) {
    LOG_DEBUG("I/O error while reading log");
    return false;
  }
  // if log file ends before reading "size"
  int read_count = log_io_.gcount();
  if (read_count < size) {
    log_io_.clear();
    memset(log_data + read_count, 0, size - read_count);
  }

  return true;
}

/**
 * Returns 迄今为止 flush 的次数
 */
auto DiskManager::GetNumFlushes() const -> int { return num_flushes_; }

/**
 * Returns 迄今为止执行写操作的次数
 */
auto DiskManager::GetNumWrites() const -> int { return num_writes_; }

/**
 *  Returns 如果当前日志正在被 flush, 返回 ture
 */
auto DiskManager::GetFlushState() const -> bool { return flush_log_; }

/**
 * 辅助函数: 获取磁盘文件大小
 */
auto DiskManager::GetFileSize(const std::string &file_name) -> int {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? static_cast<int>(stat_buf.st_size) : -1;
}

}  // namespace bustub
