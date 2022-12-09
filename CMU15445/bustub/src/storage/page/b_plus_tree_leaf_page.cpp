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
  SetPageType(IndexPageType::LEAF_PAGE);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetSize(0);                      // 最开始current size为0
  SetMaxSize(max_size);            // max_size=LEAF_PAGE_SIZE-1 这里也可以减1，方便后续的拆分(Split)函数
  SetNextPageId(INVALID_PAGE_ID);  // 最开始next page id不存在
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient) {
  int start_index = GetMinSize();  // (0,1,2) start index is 1; (0,1,2,3) start index is 2;
  int move_num = GetSize() - start_index;
  // 将this page的从array+start_index开始的move_num个元素复制到recipient page的array尾部
  recipient->CopyNFrom(array_ + start_index, move_num);  // this page array [start_index, size) copy to recipient page
  // NOTE: recipient page size has been updated in recipient->CopyNFrom
  IncreaseSize(-move_num);  // update this page size
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType *items, int size) {
  std::copy(items, items + size, array_ + GetSize());  // [items,items+size)复制到该page的array最后一个之后的空间
  IncreaseSize(size);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator)
    -> int {
  int insert_index = KeyIndex(key, comparator);  // 查找第一个>=key的的下标

  if (comparator(KeyAt(insert_index), key) == 0) {  // 重复的key
    return GetSize();
  }

  // 数组下标>=insert_index的元素整体后移1位
  // [insert_index, size - 1] --> [insert_index + 1, size]
  for (int i = GetSize(); i > insert_index; i--) {
    array_[i] = array_[i - 1];
  }
  array_[insert_index] = MappingType{key, value};  // insert pair
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
auto B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator) -> int {
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
  // 这里手写二分查找lower_bound，速度快于for循环的顺序查找
  // array类型为std::pair<KeyType, ValueType>
  // 叶结点的下标范围是[0,size-1]
  // std::scoped_lock lock{latch_};  // DEBUG
  int left = 0;
  int right = GetSize() - 1;
  while (left <= right) {
    int mid = left + (right - left) / 2;
    if (comparator(KeyAt(mid), key) >= 0) {  // 下标还需要减小
      right = mid - 1;
    } else {  // 下标还需要增大
      left = mid + 1;
    }
  }  // lower_bound
  int target_index = right + 1;
  return target_index;  // 返回array中第一个>=key的下标（如果key大于所有array，则找到下标为size）
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
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) -> const MappingType & {
  // replace with your own code
  return array_[index];
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
