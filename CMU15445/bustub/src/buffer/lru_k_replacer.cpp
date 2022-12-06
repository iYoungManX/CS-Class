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
#include "common/logger.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  for (auto it = hist_list_.rbegin(); it != hist_list_.rend(); ++it) {
    if (entries_[(*it)].evictable_) {
      *frame_id = *it;
      hist_list_.erase(std::next(it).base());
      entries_.erase(*frame_id);
      --curr_size_;
      return true;
    }
  }

  for (auto it = cache_list_.rbegin(); it != cache_list_.rend(); ++it) {
    if (entries_[(*it)].evictable_) {
      *frame_id = *it;
      cache_list_.erase(std::next(it).base());
      entries_.erase(*frame_id);
      --curr_size_;
      return true;
    }
  }

  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (frame_id > static_cast<int>(replacer_size_)) {
    throw std::invalid_argument(std::string("Invalid frame_id: ") + std::to_string(frame_id));
  }
  // for new frames, the default count is 0
  // 如果新来的 frame_id 不存在，unordered_map 的 [] 运算符会自动以默认值创建键值对。
  size_t new_count = ++entries_[frame_id].hit_count_;
  // LOG_INFO("# the value of new_count: %zu", new_count);
  //  new
  if (new_count == 1) {
    ++curr_size_;
    hist_list_.emplace_front(frame_id);
    entries_[frame_id].pos_ = hist_list_.begin();
  } else {
    // re
    if (new_count == k_) {
      hist_list_.erase(entries_[frame_id].pos_);
      cache_list_.emplace_front(frame_id);
      entries_[frame_id].pos_ = cache_list_.begin();
    } else if (new_count > k_) {
      cache_list_.erase(entries_[frame_id].pos_);
      cache_list_.emplace_front(frame_id);
      entries_[frame_id].pos_ = cache_list_.begin();
    }
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (frame_id > static_cast<int>(replacer_size_)) {
    throw std::invalid_argument(std::string("Invalid frame_id: ") + std::to_string(frame_id));
  }
  if (entries_.find(frame_id) == entries_.end()) {
    return;
  }
  if (set_evictable && !entries_[frame_id].evictable_) {
    curr_size_++;
  } else if (entries_[frame_id].evictable_ && !set_evictable) {
    curr_size_--;
  }

  entries_[frame_id].evictable_ = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);

  if (entries_.find(frame_id) == entries_.end()) {
    return;
  }

  if (!entries_[frame_id].evictable_) {
    throw std::logic_error(std::string("Can't remove an evictable frame") + std::to_string(frame_id));
  }

  if (entries_[frame_id].hit_count_ < k_) {
    hist_list_.erase(entries_[frame_id].pos_);
  } else {
    cache_list_.erase(entries_[frame_id].pos_);
  }
  --curr_size_;
  entries_.erase(frame_id);
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
