#include "storage/page/extendible_htable_header_page.h"

#include "common/exception.h"

namespace bustub {

void ExtendibleHTableHeaderPage::Init(uint32_t max_depth) {
  this->max_depth_ = max_depth;

  // 把 directory_page_ids_ 中的所有 page_id 都设置为 INVALID_PAGE_ID
  // 为了判断数组中对应下标是否创建了 directory
  std::fill(directory_page_ids_, directory_page_ids_ + (1 << max_depth), INVALID_PAGE_ID);
}

auto ExtendibleHTableHeaderPage::HashToDirectoryIndex(uint32_t hash) const -> uint32_t {
  // 取哈希值的前 max_depth_ 位
  return max_depth_ > 0 ? hash >> (sizeof(uint32_t) * 8 - max_depth_) : 0;
}

auto ExtendibleHTableHeaderPage::GetDirectoryPageId(uint32_t directory_idx) const -> uint32_t {
  return directory_page_ids_[directory_idx];
}

void ExtendibleHTableHeaderPage::SetDirectoryPageId(uint32_t directory_idx, page_id_t directory_page_id) {
  directory_page_ids_[directory_idx] = directory_page_id;
}

auto ExtendibleHTableHeaderPage::MaxSize() const -> uint32_t { return max_depth_; }

}  // namespace bustub
