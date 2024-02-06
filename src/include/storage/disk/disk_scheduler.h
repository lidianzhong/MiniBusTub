#pragma once

#include <future>  // NOLINT
#include <optional>
#include <thread>  // NOLINT

#include "common/channel.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

/**
 * @brief DiskManager 执行的一次读取或写入的单个请求为一个 DiskRequest
 */
struct DiskRequest {
  /** 标记这个请求是读操作还是写操作 */
  bool is_write_;

  /**
   *  指向 page 物理内存开始处的的指针
   */
  char *data_;

  /** 这个磁盘读写请求的 page id */
  page_id_t page_id_;

  /** 回调, 当请求完成时，向请求发出者发送请求已完成的信号 */
  std::promise<bool> callback_;
};

/**
 * @brief DiskScheduler 负责对磁盘的读和写操作进行调度
 *
 * 将需要被调度的 DiskRequest 当作参数传入 Schedule() 函数。
 * 调度器维护一个后台工作线程，通过调用 disk manager 来处理调度请求。
 * 后台工作线程在构造函数中创建，在析构函数销毁线程。
 *
 */
class DiskScheduler {
 public:
  explicit DiskScheduler(DiskManager *disk_manager);
  ~DiskScheduler();

  /**
   * @brief 从 DiskManager 中调度一个请求来执行，传入需要被调度的 request
   * @param r The request to be scheduled.
   */
  void Schedule(DiskRequest r);

  /**
   *
   * @brief 后台工作线程以处理调度请求
   *
   * 后台工作线程需要处理请求。从 DiskScheduler 开始到销毁。
   *
   */
  void StartWorkerThread();

  using DiskSchedulerPromise = std::promise<bool>;

  /**
   * @brief 创建一个 Promise
   * @return std::promise<bool>
   */
  auto CreatePromise() -> DiskSchedulerPromise { return {}; };

 private:
  /** 指向的 disk manager */
  DiskManager *disk_manager_ __attribute__((__unused__));

  /** 共享队列，用来调度和处理请求。
   * 当 DiskScheduler 的析构函数被调用，将 `std::nullopt` 放入队列以发出停止执行的信号 */
  Channel<std::optional<DiskRequest>> request_queue_;

  /** 后台工作线程，向 disk manager 发送 scheduled 请求 */
  std::optional<std::thread> background_thread_;
};
}  // namespace bustub
