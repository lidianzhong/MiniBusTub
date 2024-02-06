#pragma once

#include "storage/page/page.h"

namespace bustub {

class BufferPoolManager;
class ReadPageGuard;
class WritePageGuard;

class BasicPageGuard {
 public:
  BasicPageGuard() = default;

  BasicPageGuard(BufferPoolManager *bpm, Page *page) : bpm_(bpm), page_(page) {}

  BasicPageGuard(const BasicPageGuard &) = delete;
  auto operator=(const BasicPageGuard &) -> BasicPageGuard & = delete;

  /**
   *
   * @brief Move constructor for BasicPageGuard
   *
   * 用一个存在的 BasicPageGuard 创建一个一样的 BasicPageGuard
   * 1. 新的 BasicPageGuard 应该与 旧的 BasicPageGuard 具有完全相同的状态
   * 2. 旧的 BasicPageGuard 不应该再被调用
   */
  BasicPageGuard(BasicPageGuard &&that) noexcept;

  /**
   * @brief Drop a page guard
   *
   * 1) 清空 page guard 的所有内容，使当前的 page guard 失效，
   * 2) 告诉 BPM，我们已经用完了这个 page (意思就是 unpin page)
   */
  void Drop();

  /**
   *
   * @brief Move assignment for BasicPageGuard
   *
   * 移动赋值运算符，注意 this 和 that 对象的释放
   */
  auto operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard &;

  /**
   *
   * @brief Destructor for BasicPageGuard
   *
   * 析构函数的好处是如果使用者在使用 page 时，忘记 pin 了，那么在这时候也会调用 Drop 函数来 Unpin page
   */
  ~BasicPageGuard();

  /**
   *
   * @brief Upgrade a BasicPageGuard to a ReadPageGuard
   * 能够返回添加读锁的 page guard，在调用这个函数后，this 对象将失效，ReadPageGuard 将生效
   * @return an upgraded ReadPageGuard
   */
  auto UpgradeRead() -> ReadPageGuard;

  /**
   *
   * @brief Upgrade a BasicPageGuard to a WritePageGuard
   * 能够返回添加写锁的 page guard，在调用这个函数后，this 对象将失效，WritePageGuard 将生效
   * @return an upgraded WritePageGuard
   */
  auto UpgradeWrite() -> WritePageGuard;

  auto PageId() -> page_id_t { return page_->GetPageId(); }

  auto GetData() -> const char * { return page_->GetData(); }

  template <class T>
  auto As() -> const T * {
    return reinterpret_cast<const T *>(GetData());
  }

  auto GetDataMut() -> char * {
    is_dirty_ = true;
    return page_->GetData();
  }

  template <class T>
  auto AsMut() -> T * {
    return reinterpret_cast<T *>(GetDataMut());
  }

 private:
  friend class ReadPageGuard;
  friend class WritePageGuard;

  BufferPoolManager *bpm_{nullptr};
  Page *page_{nullptr};
  bool is_dirty_{false};
};

class ReadPageGuard {
 public:
  ReadPageGuard() = default;
  ReadPageGuard(BufferPoolManager *bpm, Page *page) : guard_(bpm, page) {}
  ReadPageGuard(const ReadPageGuard &) = delete;
  auto operator=(const ReadPageGuard &) -> ReadPageGuard & = delete;

  /**
   * @brief Move constructor for ReadPageGuard
   *
   */
  ReadPageGuard(ReadPageGuard &&that) noexcept;

  /**
   * @brief Move assignment for ReadPageGuard
   *
   */
  auto operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard &;

  /**
   * @brief Drop a ReadPageGuard
   *
   */
  void Drop();

  /**
   *
   * @brief Destructor for ReadPageGuard
   *
   */
  ~ReadPageGuard();

  auto PageId() -> page_id_t { return guard_.PageId(); }

  auto GetData() -> const char * { return guard_.GetData(); }

  template <class T>
  auto As() -> const T * {
    return guard_.As<T>();
  }

 private:
  BasicPageGuard guard_;
};

class WritePageGuard {
 public:
  WritePageGuard() = default;
  WritePageGuard(BufferPoolManager *bpm, Page *page) : guard_(bpm, page) {}
  WritePageGuard(const WritePageGuard &) = delete;
  auto operator=(const WritePageGuard &) -> WritePageGuard & = delete;

  /**
   * @brief Move constructor for WritePageGuard
   *
   */
  WritePageGuard(WritePageGuard &&that) noexcept;

  /**
   * @brief Move assignment for WritePageGuard
   *
   */
  auto operator=(WritePageGuard &&that) noexcept -> WritePageGuard &;

  /**
   * @brief Drop a WritePageGuard
   *
   */
  void Drop();

  /**
   * @brief Destructor for WritePageGuard
   *
   */
  ~WritePageGuard();

  auto PageId() -> page_id_t { return guard_.PageId(); }

  auto GetData() -> const char * { return guard_.GetData(); }

  template <class T>
  auto As() -> const T * {
    return guard_.As<T>();
  }

  auto GetDataMut() -> char * { return guard_.GetDataMut(); }

  template <class T>
  auto AsMut() -> T * {
    return guard_.AsMut<T>();
  }

 private:
  BasicPageGuard guard_;
};

}  // namespace bustub
