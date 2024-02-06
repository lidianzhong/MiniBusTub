#include <optional>
#include <utility>

#include "common/exception.h"
#include "storage/page/extendible_htable_bucket_page.h"

namespace bustub {

template <typename K, typename V, typename KC>
void ExtendibleHTableBucketPage<K, V, KC>::Init(uint32_t max_size) {
  // 初始化 bucket page
  size_ = 0;
  max_size_ = max_size;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Lookup(const K &key, V &value, const KC &cmp) const -> bool {
  // 如果找到了 key 和 value，则返回 true，否则返回 false
  for (uint32_t tmp = 0; tmp < size_; tmp++) {
    if (!cmp(key, array_[tmp].first)) {
      value = array_[tmp].second;
      return true;
    }
  }

  return false;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Insert(const K &key, const V &value, const KC &cmp) -> bool {
  // 如果 bucket 满了，插入不成功，返回 false
  if (IsFull()) {
    return false;
  }

  // 如果找到了 key，说明 key 已经存在，返回 false
  for (uint32_t tmp = 0; tmp < size_; tmp++) {
    if (!cmp(key, array_[tmp].first)) {
      return false;
    }
  }

  // 插入 key 到 bucket 中
  array_[size_++] = {key, value};
  return true;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Remove(const K &key, const KC &cmp) -> bool {
  for (uint32_t tmp = 0; tmp < size_; tmp++) {
    if (!cmp(key, array_[tmp].first)) {
      // 找到 key-value 了， 与最后一个交换，然后 size - 1
      std::swap(array_[tmp], array_[size_ - 1]);
      size_--;
      return true;
    }
  }

  // 找不到这样的 key-value
  return false;
}

template <typename K, typename V, typename KC>
void ExtendibleHTableBucketPage<K, V, KC>::RemoveAt(uint32_t bucket_idx) {
  // 移除下标为 bucket_idx 处的 key_value 对, 与最后一个交换，然后 size - 1
  std::swap(array_[bucket_idx], array_[size_ - 1]);
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::KeyAt(uint32_t bucket_idx) const -> K {
  return array_[bucket_idx].first;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::ValueAt(uint32_t bucket_idx) const -> V {
  return array_[bucket_idx].second;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::EntryAt(uint32_t bucket_idx) const -> const std::pair<K, V> & {
  return array_[bucket_idx];
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Size() const -> uint32_t {
  return size_;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::IsFull() const -> bool {
  return size_ == max_size_;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::IsEmpty() const -> bool {
  return size_ == 0;
}

template class ExtendibleHTableBucketPage<int, int, IntComparator>;
template class ExtendibleHTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
