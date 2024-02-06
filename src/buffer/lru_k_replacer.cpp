//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

/**
 *
 * @param[out] frame_id 返回逐出的 frame id
 * @return 如果逐出成功返回 true, 否则返回 false
 *
 */
auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::unique_lock<std::mutex> lock(this->latch_);
  size_t mx_k_distance = 0;
  size_t early_timestamp;
  frame_id_t evict_id = -1;

  for (const auto &i : node_store_) {
    auto node = i.second;
    if (!node->GetEvictable()) {
      continue;
    }

    size_t now_k_distance = node->GetKDistance(this->current_timestamp_);
    if (now_k_distance > mx_k_distance) {
      mx_k_distance = now_k_distance;
      early_timestamp = node->GetEarlyTimestamp();
      evict_id = i.first;
    } else if (now_k_distance == mx_k_distance) {
      size_t now_early_timestamp = node->GetEarlyTimestamp();
      if (early_timestamp > now_early_timestamp) {
        early_timestamp = now_early_timestamp;
        evict_id = i.first;
      }
    }
  }

  if (evict_id == -1) {
    return false;
  }

  *frame_id = evict_id;
  node_store_.erase(evict_id);
  this->evictable_size_--;
  return true;
}

/**
 * @brief 记录 page 的一次访问。
 * 需要根据这个 page 的 frame_id 是否在 replacer 中分别判断。
 * 1) frame_id 是新加入的
 *      创建一个 LRUKNode
 *      加入到 node_store
 *      更新 replacer 相关信息
 * 2) frame_id 已经加入了
 *      调用 LRUKNode 的 RecordAccess 方法
 *      更新 replacer 相关信息
 * @param frame_id 涉及到的 frame_id
 */
void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::unique_lock<std::mutex> lock(this->latch_);

  BUSTUB_ASSERT(frame_id >= 0 && (size_t)frame_id < replacer_size_, "frame_id is out of range"); // TODO(zhong): replacer_size?

  if (node_store_.find(frame_id) == node_store_.end()) {
    // 将新的 frame_id 加入 node_store 中

    // 1. 生成 LRUKNode
    auto new_LRUKNode = std::make_shared<LRUKNode>(frame_id, ++this->current_timestamp_, this->k_);
    new_LRUKNode->SetEvictable(false);
    // 2. 加入 node_store 中
    node_store_[frame_id] = new_LRUKNode;
  } else {
    // frame_id 之前已经加入了
    node_store_[frame_id]->RecordAccess(++ this->current_timestamp_);
  }
}

/**
 * @brief 设置 LRUKNode 的是否可逐出状态 (evictable)
 * 如果发现 node_store 中没有要操作的 frame id, 那么一定出现错误了, 抛出异常
 *
 * @param frame_id 修改 frame id 的 LRUKNode 状态
 * @param set_evictable 想要设置的状态
 */
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::unique_lock<std::mutex> lock(this->latch_);

  BUSTUB_ASSERT(node_store_.find(frame_id) != node_store_.end(), "unknown exception!");

  auto now_frame = node_store_[frame_id];
  if (now_frame->GetEvictable() && !set_evictable) {
      now_frame->SetEvictable(false);
      this->evictable_size_ --;
  } else if (!now_frame->GetEvictable() && set_evictable) {
      now_frame->SetEvictable(true);
      this->evictable_size_ ++;
  }
}

/**
 * 直接移除一个 evictable 的 frame, 注意和 Evict 的区别
 * 如果移除的是 non-evictable, 那么抛出异常或中止程序
 * 如果移除的 frame 没有找到, 返回不报错
 *
 * @param frame_id
 */
void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::unique_lock<std::mutex> lock(this->latch_);

  // 如果移除的 frame 没有找到, 直接返回不报错
  if (node_store_.find(frame_id) == node_store_.end()) {
    return;
  }
  // 如果移除的是 non-evictable, 那么选择抛出异常
  if (!node_store_[frame_id]->GetEvictable()) {
      BUSTUB_ASSERT(false, "frame id is not evictable");
  }

  // 直接移除这个 frame
  node_store_.erase(frame_id);
  this->evictable_size_ --;
}

/**
 * 注意这里指定 replacer 的大小为可逐出 frame 的数量
 * @return 可逐出 frame 的数量
 */
auto LRUKReplacer::Size() -> size_t {
  std::unique_lock<std::mutex> lock(this->latch_);
  return evictable_size_;
}

/**
 * LRUKNode 的构造函数, 生成一个 LRUKNode 时就会进行一次 RecordAccess。
 */
LRUKNode::LRUKNode(frame_id_t fid, size_t current_timestamp_, size_t k) : fid_(fid), k_(k) {
  this->history_.push_front(current_timestamp_);
}
auto LRUKNode::GetEvictable() const -> bool { return this->is_evictable_; }
void LRUKNode::SetEvictable(bool set_evictable) { this->is_evictable_ = set_evictable; }
auto LRUKNode::GetKDistance(size_t current_timestamp_) -> size_t {
  if (this->history_.size() < k_) {
    return INF;
  }

  size_t k_tmp = 1;
  auto it = this->history_.begin();
  while (k_tmp < this->k_) {
    k_tmp++;
    it++;
  }
  return current_timestamp_ - *it;
}
auto LRUKNode::GetEarlyTimestamp() -> size_t { return this->history_.back(); }
void LRUKNode::RecordAccess(size_t current_timestamp_) { this->history_.push_front(current_timestamp_); }

}
