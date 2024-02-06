#pragma once

#include <list>
#include <memory>
#include <mutex>  // NOLINT
#include <unordered_map>

#include "buffer/lru_k_replacer.h"
#include "common/config.h"
#include "storage/disk/disk_scheduler.h"
#include "storage/page/page.h"
#include "storage/page/page_guard.h"

namespace bustub {

/**
 * BufferPoolManager 从它的内部 buffer pool 中读取 disk page。
 * BufferPoolManager reads disk pages to and from its internal buffer pool.
 */
class BufferPoolManager {
 public:
  /**
   * 创建一个新的 BufferPoolManager
   * 传入参数分别是 buffer pool 的大小，disk manager，LRU-K 算法中需要的常数 k
   *
   * @brief Creates a new BufferPoolManager.
   * @param pool_size the size of the buffer pool
   * @param disk_manager the disk manager
   * @param replacer_k the LookBack constant k for the LRU-K replacer
   */
  BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k = LRUK_REPLACER_K);

  /**
   * @brief 销毁 BufferPoolManager
   */
  ~BufferPoolManager();

  /** @brief 返回 buffer pool 的 size（也就是 frames 的数量） */
  auto GetPoolSize() -> size_t { return pool_size_; }

  /** @brief 返回 buffer pool 所有 pages 的指针（pages_ 是一个指向 Page 类型的指针） */
  auto GetPages() -> Page * { return pages_; }

  /**
   * 在  buffer pool 中创建一个新 page。
   * 除了所有的 frame 都在使用且不可逐出（也就是 page 是 pinned 状态）的时候将 page_id 设置为 nullptr，
   * 其余状态赋予 page_id 一个新的 page 的 id
   *
   * 你应该要么从 free list 中，要不从 replacer 中选择替换 frame（当 free list 没有可以用的了再从 replacer 中拿）。
   * 然后调用 AllocatePage() 方法获取一个新的 page_id。如果替换的 frame 是一个脏页，你应该先写回磁盘。你还需要重置新
   * page 的内存和数据 记住通过调用 replacer.SetEvictable(frame_id, false) 来 Pinned frame，使得 buffer pool manager
   * 在它 unpin 前不会逐出它
   *
   * 记得在 replacer 中记录 frame 访问历史，从而让 lru-k 工作。
   *
   * @param[out] page_id 创建 page 生成的 page id
   * @return 如果没有 new page 创建，那么返回 nullptr，否则返回创建的 page 的指针
   */
  auto NewPage(page_id_t *page_id) -> Page *;

  /**
   * 功能应该与 NewPage 相同，只不过您返回的是 BasicPageGuard 结构，而不是返回指向页面的指针。
   *
   * @brief PageGuard wrapper for NewPage
   *
   * @param[out] page_id, the id of the new page
   * @return BasicPageGuard holding a new page
   */
  auto NewPageGuarded(page_id_t *page_id) -> BasicPageGuard;

  /**
   * 从 buffer pool 中获取请求的 page。如果需要从磁盘中获取 page_id 但是所有的 frames 当前正在使用并且不可逐出时（也就是
   * pinned），返回 nullptr
   *
   * 首先从 buffer pool 中查找 page_id。
   * 如果没有找到，那就找一个替换的 frame，这个 frame 要不从 free list 里获取，要不通过 replacer 获取。
   * 然后通过使用 disk_scheduler_->Schedule() 调度读取 DiskRequest 从磁盘读取页面，并且替代 frame 中的旧 page。
   * 和 NewPage() 函数一样，如果这个 old page 是脏页，则需要将其写回磁盘并更新新页面的元数据
   *
   * 请记住禁用逐出并记录框架的访问历史记录，就像 NewPage() 一样。
   * @param page_id id of page to be fetched
   * @return 如果 page 不能执行 fetch 操作，返回 nullptr，否则返回 page 的指针
   */
  auto FetchPage(page_id_t page_id) -> Page *;

  /**
   * @brief PageGuard wrappers for FetchPage
   *
   * @param page_id 需要执行 fetch 操作的 page id
   * @return PageGuard
   */
  auto FetchPageBasic(page_id_t page_id) -> BasicPageGuard;
  auto FetchPageRead(page_id_t page_id) -> ReadPageGuard;
  auto FetchPageWrite(page_id_t page_id) -> WritePageGuard;

  /**
   * 取消 pin buffer pool 中的目标 page。如果 page_id 不在 buffer pool 中或者这个 page 的 pin count 已经为 0，返回 false
   *
   * 减少这个 page 的 pin count 数。如果 pin count 的数量到了 0，frame 通过 replacer 设置为 evictable
   *
   * 另外，在页面上设置 dirty flag 以指示该页面是否被修改。
   *
   * @param page_id 需要 unpin 的 page id
   * @param is_dirty 如果 page 需要被标记为 dirty，那么为 true，否则为 false。
   * @return 如果 page table 中没有这个 page 或者 在执行函数前 pin count <= 0 返回 false，否则返回 true。
   */
  auto UnpinPage(page_id_t page_id, bool is_dirty) -> bool;

  /**
   * 将 page 刷新到 disk，无论这个 page 是不是 dirty page。在刷新后取消 dirty page 的标志。
   *
   * 传入的 page 是要求不能为 INVALID_PAGE_ID
   *
   * 如果在页表中找不到该页，则为 false，否则为 true
   *
   * @brief Flush the target page to disk.
   *
   * @param page_id id of page to be flushed, cannot be INVALID_PAGE_ID
   * @return false if the page could not be found in the page table, true otherwise
   */
  auto FlushPage(page_id_t page_id) -> bool;

  /**
   * 将 buffer pool 中所有的 pages 刷新到磁盘
   *
   */
  void FlushAllPages();

  /**
   * 删除 buffer pool 中的 page。如果 page_id 不在 buffer pool，什么都不用做，返回 true 即可。
   * 如果 page 是 pinned 状态，不能被删除，立即返回 false。
   *
   * 在从页表中删除 page 后，停止在 replacer 中追踪这个 frame，并且将这个 frame 添加到 free list 中。
   * 同样，重置页面的内存和元数据。最后，你应该调用  DeallocatePage() 来模拟释放磁盘上的 page。
   *
   * @param page_id 需要删除的 page id
   * @return 如果 page 存在但删除失败返回 false，如果 page 不存在或者删除成功返回 true
   */
  auto DeletePage(page_id_t page_id) -> bool;

 private:
  /** Number of pages in the buffer pool. */
  const size_t pool_size_;
  /** The next page id to be allocated  */
  std::atomic<page_id_t> next_page_id_ = 0;

  /** Array of buffer pool pages. */
  Page *pages_;
  /** Pointer to the disk sheduler. */
  std::unique_ptr<DiskScheduler> disk_scheduler_;
  /** Page table for keeping track of buffer pool pages. */
  std::unordered_map<page_id_t, frame_id_t> page_table_;
  /** Replacer to find unpinned pages for replacement. */
  std::unique_ptr<LRUKReplacer> replacer_;
  /** List of free frames that don't have any pages on them. */
  std::list<frame_id_t> free_list_;
  /** This latch protects shared data structures. We recommend updating this comment to describe what it protects. */
  std::mutex latch_;

  /**
   * @brief 为磁盘分配一个 page。调用这个函数前需要先获取锁
   * @return 分配的 page 的 page id
   */
  auto AllocatePage() -> page_id_t;

};
}
