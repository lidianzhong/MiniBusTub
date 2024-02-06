//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// table_heap.cpp
//
// Identification: src/storage/table/table_heap.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <mutex>  // NOLINT
#include <utility>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "fmt/format.h"
#include "storage/page/page_guard.h"
#include "storage/page/table_page.h"
#include "storage/table/table_heap.h"

namespace bustub {

/**
 * 创建一个 table heap
 * 具体步骤就是向 buffer pool 中请求一个 page，转成 table page 类型，再调用 Init() 函数
 */
TableHeap::TableHeap(BufferPoolManager *bpm) : bpm_(bpm) {
  // Initialize the first table page.
  auto guard = bpm->NewPageGuarded(&first_page_id_);
  last_page_id_ = first_page_id_;
  auto first_page = guard.AsMut<TablePage>();
  BUSTUB_ASSERT(first_page != nullptr, "Couldn't create a page for the table heap.");
  first_page->Init();
}

/**
 * 将 tuple 插入 table 中，首先尝试获取 table heap 中最后一个 page 进行插入，
 * 如果发现不可以，再向 table heap 插入一个新 page
 * 无论哪种情况，都会调用 page 的 InsertTuple(meta, tuple) 方法执行插入操作
 */
auto TableHeap::InsertTuple(const TupleMeta &meta, const Tuple &tuple) -> std::optional<RID> {
  std::unique_lock<std::mutex> guard(latch_);
  // 获取到 table heap 中最后一个 page
  auto page_guard = bpm_->FetchPageWrite(last_page_id_);
  while (true) {
    auto page = page_guard.AsMut<TablePage>();
    // 如果 page 不能获取到元组的偏移量，GetNextTupleOffset 会返回 nullopt, 就会继续进行
    if (page->GetNextTupleOffset(meta, tuple) != std::nullopt) {
      break;
    }

    // 如果 page 中一个元组也没有，但是我们又插入不了，说明 tuple 太大了，直接报错
    // if there's no tuple in the page, and we can't insert the tuple, then this tuple is too large.
    BUSTUB_ENSURE(page->GetNumTuples() != 0, "tuple is too large, cannot insert");

    // 因为当前 page 不能插入 tuple 了，所以考虑重新拿个页面装 tuple
    page_id_t next_page_id = INVALID_PAGE_ID;
    auto npg = bpm_->NewPage(&next_page_id);
    BUSTUB_ENSURE(next_page_id != INVALID_PAGE_ID, "cannot allocate page");

    // 设置当前 page 的下一个 page id 为新创建的这个
    page->SetNextPageId(next_page_id);

    // 把新创建的 page 类型转化为 TablePage
    auto next_page = reinterpret_cast<TablePage *>(npg->GetData());
    next_page->Init();

    page_guard.Drop();

    // acquire latch here as TSAN complains. Given we only have one insertion thread, this is fine.
    npg->WLatch();
    auto next_page_guard = WritePageGuard{bpm_, npg};

    // 把新创建的 page 加入到 table heap 中
    last_page_id_ = next_page_id;

    // 将锁的所有权给 page_guard
    page_guard = std::move(next_page_guard);
  }
  auto last_page_id = last_page_id_;

  auto page = page_guard.AsMut<TablePage>();
  auto slot_id = *page->InsertTuple(meta, tuple);

  // only allow one insertion at a time, otherwise it will deadlock.
  guard.unlock();

  page_guard.Drop();

  return RID(last_page_id, slot_id);
}

void TableHeap::UpdateTupleMeta(const TupleMeta &meta, RID rid) {
  auto page_guard = bpm_->FetchPageWrite(rid.GetPageId());
  auto page = page_guard.AsMut<TablePage>();
  page->UpdateTupleMeta(meta, rid);
}

auto TableHeap::GetTuple(RID rid) -> std::pair<TupleMeta, Tuple> {
  auto page_guard = bpm_->FetchPageRead(rid.GetPageId());
  auto page = page_guard.As<TablePage>();
  auto [meta, tuple] = page->GetTuple(rid);
  tuple.rid_ = rid;
  return std::make_pair(meta, std::move(tuple));
}

auto TableHeap::GetTupleMeta(RID rid) -> TupleMeta {
  auto page_guard = bpm_->FetchPageRead(rid.GetPageId());
  auto page = page_guard.As<TablePage>();
  return page->GetTupleMeta(rid);
}

auto TableHeap::MakeIterator() -> TableIterator {
  std::unique_lock<std::mutex> guard(latch_);
  auto last_page_id = last_page_id_;
  guard.unlock();

  auto page_guard = bpm_->FetchPageRead(last_page_id);
  auto page = page_guard.As<TablePage>();
  return {this, {first_page_id_, 0}, {last_page_id, page->GetNumTuples()}};
}

auto TableHeap::MakeEagerIterator() -> TableIterator { return {this, {first_page_id_, 0}, {INVALID_PAGE_ID, 0}}; }

auto TableHeap::UpdateTupleInPlace(const TupleMeta &meta, const Tuple &tuple, RID rid,
                                   std::function<bool(const TupleMeta &meta, const Tuple &table, RID rid)> &&check)
    -> bool {
  auto page_guard = bpm_->FetchPageWrite(rid.GetPageId());
  auto page = page_guard.AsMut<TablePage>();
  auto [old_meta, old_tup] = page->GetTuple(rid);
  if (check == nullptr || check(old_meta, old_tup, rid)) {
    page->UpdateTupleInPlaceUnsafe(meta, tuple, rid);
    return true;
  }
  return false;
}

}  // namespace bustub
