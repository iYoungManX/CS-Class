//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  this->SetPageId(page_id);
  this->SetParentPageId(parent_id);
  this->SetMaxSize(max_size);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient) {
  int move_nums = GetSize() / 2;
  MappingType *start = array_ + GetSize() - move_nums;
  recipient->CopyNFrom(start, move_nums);
  this->IncreaseSize(-1 * move_nums);
  recipient->IncreaseSize(move_nums);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType *items, int size) {
  for (int cnt = 0; cnt < size; cnt++) {
    this->array_[cnt] = *(items + cnt);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { this->next_page_id_ = next_page_id; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator)
    -> int {
  int index = KeyIndex(key, comparator);
  assert(index >= 0);
  int end = GetSize();
  for (int i = end; i > index; i--) {
    array_[i].first = array_[i - 1].first;
    array_[i].second = array_[i - 1].second;
  }
  array_[index].first = key;
  array_[index].second = value;
  IncreaseSize(1);
  return GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType *value, const KeyComparator &comparator) const
    -> bool {
  int target_index = KeyIndex(key, comparator);                                  // 查找第一个>=key的的下标
  if (target_index == GetSize() || comparator(key, KeyAt(target_index)) != 0) {  // =key的下标不存在（只有>key的下标）
    // LOG_INFO("leaf node Lookup FAILURE key>all not ==");
    return false;
  }
  // LOG_INFO("leaf node Lookup SUCCESS index=%d", target_index);
  *value = array_[target_index].second;  // value是传出参数
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator) -> int
{
  int target_index = KeyIndex(key, comparator);                                  // 查找第一个>=key的的下标
  if (target_index == GetSize() || comparator(key, KeyAt(target_index)) != 0) {  // =key的下标不存在（只有>key的下标）
    return GetSize();
  }
  // delete array_[target_index], move array_ after target_index to front by 1 size
  IncreaseSize(-1);
  for (int i = target_index; i < GetSize(); i++) {
    array_[i] = array_[i + 1];
  }
  return GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const -> int {
  assert(GetSize() >= 0);
  int l = 0;
  int r = GetSize() - 1;
  while (l <= r) {
    int mid = (r - l) / 2 + l;
    if (comparator(array_[mid].first, key) < 0) {
      l = mid + 1;
    } else {
      r = mid - 1;
    }
  }
  return r + 1;
}


INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient) {
  recipient->CopyNFrom(array_, GetSize());
  SetSize(0);
}


/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array_ offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  return array_[index].first;
}




INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient) {
  // LOG_INFO("LEAF BEGIN MoveFirstToEndOf");
  // first item (array[0]) of this page array copied to recipient page last
  recipient->CopyLastFrom(array_[0]);
  // LOG_INFO("LEAF BEGIN delete array_[0]");
  // delete array_[0], move array_ after index=0 to front by 1 size
  IncreaseSize(-1);
  for (int i = 0; i < GetSize(); i++) {
    array_[i] = array_[i + 1];
  }
  // LOG_INFO("LEAF END MoveFirstToEndOf");
}

/*
 * Copy the item into the end of my item list. (Append item to my array_)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
  // LOG_INFO("LEAF BEGIN CopyLastFrom");
  array_[GetSize()] = item;
  IncreaseSize(1);
  // LOG_INFO("LEAF END CopyLastFrom");
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient) {
  // last item (array_[size-1]) of this page array_ inserted to recipient page first
  recipient->CopyFirstFrom(array_[GetSize() - 1]);
  // remove last item of this page
  IncreaseSize(-1);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const MappingType &item) {
  // move array_ after index=0 to back by 1 size
  for (int i = GetSize(); i >= 0; i--) {
    array_[i + 1] = array_[i];
  }
  // insert item to array_[0]
  array_[0] = item;
  IncreaseSize(1);
}


INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
  // replace with your own code
  return array_[index];
}


template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
