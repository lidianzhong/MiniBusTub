/**
 * Header page format:
 *  ---------------------------------------------------
 * | DirectoryPageIds(2048) | MaxDepth (4) | Free(2044)
 *  ---------------------------------------------------
 */

#pragma once

#include <cstdlib>
#include "common/config.h"
#include "common/macros.h"

namespace bustub {

static constexpr uint64_t HTABLE_HEADER_PAGE_METADATA_SIZE = sizeof(uint32_t);
static constexpr uint64_t HTABLE_HEADER_MAX_DEPTH = 9;
static constexpr uint64_t HTABLE_HEADER_ARRAY_SIZE = 1 << HTABLE_HEADER_MAX_DEPTH;

class ExtendibleHTableHeaderPage {
 public:
  // Delete all constructor / destructor to ensure memory safety
  ExtendibleHTableHeaderPage() = delete;
  DISALLOW_COPY_AND_MOVE(ExtendibleHTableHeaderPage);

  /**
   * 在从 buffer pool 中创建一个 header page 后，必须调用 init 方法，初始化一些初始值
   * @param max_depth Max depth in the header page
   */
  void Init(uint32_t max_depth = HTABLE_HEADER_MAX_DEPTH);

  /**
   * 把哈希值转换为 directory index 索引下标 (也就是 directory_page_ids[] 下标)
   * @param hash key 哈希得到的 hash 值
   * @return 返回 directory index
   */
  auto HashToDirectoryIndex(uint32_t hash) const -> uint32_t;

  /**
   * 获取 directory_idx 对应的 directory_page_ids[] 值
   * @param directory_idx 索引下标
   * @return directory page_id
   */
  auto GetDirectoryPageId(uint32_t directory_idx) const -> uint32_t;

  /**
   * 设置 directory_idx 处对应的 directory_page_ids[] 值
   * @param directory_idx 索引下标
   * @param directory_page_id
   */
  void SetDirectoryPageId(uint32_t directory_idx, page_id_t directory_page_id);

  /**
   * @brief header page能够处理的最大 directory page ids 的数量
   */
  auto MaxSize() const -> uint32_t;

    /**
   * 打印标题的占用信息
   *
   * Prints the header's occupancy information
   */
  void PrintHeader() const;

 private:
  /** An array of directory page ids */
  page_id_t directory_page_ids_[HTABLE_HEADER_ARRAY_SIZE];
  /** The maximum depth the header page could handle */
  uint32_t max_depth_;
};

static_assert(sizeof(page_id_t) == 4);

static_assert(sizeof(ExtendibleHTableHeaderPage) ==
              sizeof(page_id_t) * HTABLE_HEADER_ARRAY_SIZE + HTABLE_HEADER_PAGE_METADATA_SIZE);

static_assert(sizeof(ExtendibleHTableHeaderPage) <= BUSTUB_PAGE_SIZE);

}  // namespace bustub
