//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

  // TODO(students): remove this line after you have implemented the buffer pool manager
  //  throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager_instance.cpp`.");
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock lock(latch_);
  Page *page = nullptr;
  frame_id_t frame_id;
  if (!GetAvailableFrame(&frame_id)) {
    return nullptr;
  }
  page = &pages_[frame_id];
  page->page_id_ = AllocatePage();
  // by default is_dirty is false
  // page->is_dirty_ = false;
  page->pin_count_ = 1;
  page->ResetMemory();
  page_table_->Insert(page->GetPageId(), frame_id);
  *page_id = page->GetPageId();
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  return page;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock lock(latch_);

  Page *page = nullptr;
  frame_id_t frame_id;

  if (page_table_->Find(page_id, frame_id)) {
    // LOG_INFO("###### . I get here ############");
    page = &pages_[frame_id];
    page->pin_count_++;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    // replacer_->SetEvictable(frame_id, false);
    return page;
  }

  if (!GetAvailableFrame(&frame_id)) {
    return nullptr;
  }

  page = &pages_[frame_id];
  page->page_id_ = page_id;
  page->is_dirty_ = false;
  page->pin_count_ = 1;
  disk_manager_->ReadPage(page_id, page->GetData());

  page_table_->Insert(page->GetPageId(), frame_id);
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  return page;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock lock(latch_);
  Page *page = nullptr;
  frame_id_t frame_id;

  // LOG_INFO("i REACHED HERE BRO");

  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }
  page = &pages_[frame_id];

  if (page->GetPinCount() <= 0) {
    return false;
  }

  page->pin_count_--;

  if (page->GetPinCount() == 0) {
    replacer_->SetEvictable(frame_id, true);
  }

  if (!page->is_dirty_) {
    page->is_dirty_ = is_dirty;
  }

  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::scoped_lock lock(latch_);
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id) || page_id == INVALID_PAGE_ID) {
    return false;
  }

  disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].data_);
  pages_[frame_id].is_dirty_ = false;
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].page_id_ != INVALID_PAGE_ID) {
      disk_manager_->WritePage(pages_[i].page_id_, pages_[i].data_);
      pages_[i].is_dirty_ = false;
    }
  }
  // for (auto &item : page_table_->GetItems()) {
  //   for (const auto &[page_id, frame_id] : item->GetItems()) {
  //     Page *page = &pages_[frame_id];
  //     disk_manager_->WritePage(page->GetPageId(), page->GetData());
  //     page->is_dirty_ = false;
  //   }
  // }
}
/**
 * TODO(P1): Add implementation
 *
 * @brief Delete a page from the buffer pool. If page_id is not in the buffer pool, do nothing and return true. If the
 * page is pinned and cannot be deleted, return false immediately.
 *
 * After deleting the page from the page table, stop tracking the frame in the replacer and add the frame
 * back to the free list. Also, reset the page's memory and metadata. Finally, you should call DeallocatePage() to
 * imitate freeing the page on the disk.
 *
 * @param page_id id of page to be deleted
 * @return false if the page exists but could not be deleted, true if the page didn't exist or deletion succeeded
 */

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock lock(latch_);
  Page *page = nullptr;
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return true;
  }
  page = &pages_[frame_id];

  if (page->GetPinCount() != 0) {
    return false;
  }
  if (page->IsDirty()) {
    disk_manager_->WritePage(page->GetPageId(), page->GetData());
  }

  page->pin_count_ = 0;
  page->is_dirty_ = false;
  page->ResetMemory();
  page->page_id_ = INVALID_PAGE_ID;
  page_table_->Remove(page->GetPageId());
  replacer_->Remove(frame_id);
  free_list_.push_back(frame_id);
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManagerInstance::GetAvailableFrame(frame_id_t *out_frame_id) -> bool {
  frame_id_t fid;
  if (!free_list_.empty()) {
    fid = free_list_.front();
    free_list_.pop_front();
    *out_frame_id = fid;
    return true;
  }
  if (replacer_->Evict(&fid)) {
    if (pages_[fid].is_dirty_) {
      disk_manager_->WritePage(pages_[fid].page_id_, pages_[fid].data_);
      pages_[fid].is_dirty_ = false;
    }
    page_table_->Remove(pages_[fid].page_id_);
    *out_frame_id = fid;
    return true;
  }
  return false;
}
}  // namespace bustub
