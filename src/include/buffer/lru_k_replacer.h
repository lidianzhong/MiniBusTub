#pragma once

#include <limits>
#include <list>
#include <memory>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <vector>

#include "common/config.h"
#include "common/macros.h"

namespace bustub {

const size_t INF = INT32_MAX;

class LRUKNode {
 private:
  /** 存放访问 page 最近的 k 次记录。最近的时间戳被存储在 list 的前端。 */

 public:
  LRUKNode() = default;
  explicit LRUKNode(frame_id_t fid, size_t current_timestamp_, size_t k);
  auto GetEvictable() const -> bool;
  void SetEvictable(bool set_evictable);
  auto GetKDistance(size_t current_timestamp_) -> size_t;
  auto GetEarlyTimestamp() -> size_t;
  void RecordAccess(size_t current_timestamp_);

  std::list<size_t> history_;
  frame_id_t fid_;
  size_t k_;
  bool is_evictable_{false};
};

/**
 * LRUKReplacer 实现了 LRU-k 替换调度算法。它的作用是决策在内存中没有多余的地方存储 page 时，该选出哪个不常用的 page 进行替换。
 * 这里的 "不常用" 的定义是：
 * 1) 一个 frame 的 k-distance 越大，越不常用，越容易被逐出 (当有多个 INF 时，逐出最早时间戳的那个)
 * 2) 当一个 frame 的访问次数小于 k 次时，k-distance 是 INF，非常大
 * 3) 当一个 frame 的访问次数达到 k 次时，k-distance 为当前时间戳 - 前 k 次访问的时间戳
 * 由上述定义可以观察到，总是会尝试逐出 frame 访问次数不大于 k 次的那些
 * 其次，在 frame 访问次数达到 k 次时，会逐出 frame 访问次数不大于 k 次的那些
 */
class LRUKReplacer {
 public:
  /**
   * @brief a new LRUKReplacer.
   * @param num_frames LRUKReplacer 需要存储的最大 frame 数量 TODO(zhong): num_frames 有什么用？
   */
  explicit LRUKReplacer(size_t num_frames, size_t k);

  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  /**
   * @brief Destroys the LRUReplacer.
   */
  ~LRUKReplacer() = default;

  /**
   * @brief 寻找到最大 k-distance，然后逐出这个 frame。注意，只考虑 'evictable' 的 frame。
   *
   * 少于 k 次访问记录的 frame 的 k-distance 为 INF。
   * 多个为 INF 的 frame，逐出时间戳最早的 frame
   *
   * 如果逐出成功，那么减少 replacer 的 size， 并且 remove frame
   *
   * @param[out] frame_id 被逐出的 frame 的 id
   * @return 如果逐出成功返回 true, 否则返回 false
   */
  auto Evict(frame_id_t *frame_id) -> bool;

  /**
   * @brief 记录 frame 的一次访问。
   *
   * @param frame_id 涉及到的 frame_id
   */
  void RecordAccess(frame_id_t frame_id);

  /**
   * @brief 设置 LRUKNode 的是否可逐出状态 (evictable)
   * 如果发现 node_store 中没有要操作的 frame id, 那么一定出现错误了, 抛出异常
   *
   * @param frame_id 修改 frame id 的 LRUKNode 状态
   * @param set_evictable 想要设置的状态
  */
  void SetEvictable(frame_id_t frame_id, bool set_evictable);

  /**
   * 直接移除一个 evictable 的 frame, 注意和 Evict 的区别
   * 如果移除的是 non-evictable, 那么抛出异常或中止程序
   * 如果移除的 frame 没有找到, 返回不报错
   *
   * @param frame_id
  */
  void Remove(frame_id_t frame_id);

  /**
   * 注意这里指定 replacer 的大小为可逐出 frame 的数量
   * @return 可逐出 frame 的数量
  */
  auto Size() -> size_t;

 private:
  std::unordered_map<frame_id_t, std::shared_ptr<LRUKNode>> node_store_;
  size_t current_timestamp_{0};
  size_t evictable_size_{0};
  size_t replacer_size_; // TODO(zhong): 为什么要将这个变量设置为等于 num_frames, 并且没啥用？
  size_t k_;
  std::mutex latch_;
};

}
