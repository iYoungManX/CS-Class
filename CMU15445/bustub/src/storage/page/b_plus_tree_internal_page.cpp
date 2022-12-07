//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  this->SetPageId(page_id);
  this->SetParentPageId(parent_id);
  this->SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array_ offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  array_[index].first = key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                BufferPoolManager *buffer_pool_manager) {
  int move_nums = GetSize() / 2;
  // what is the array_ here?
  MappingType *start = array_ + GetSize() - move_nums;
  recipient->CopyNFrom(start, move_nums, buffer_pool_manager);
  this->IncreaseSize(-1 * move_nums);
  recipient->IncreaseSize(move_nums);
}


INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  for (int cnt = 0; cnt < size; cnt++) {
    this->array_[cnt] = *(items + cnt);
    // 注意！！内部节点分裂后，需要更新分配给新的内部节点的所有子节点的父节点指针
    auto child_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager->FetchPage(array_[cnt].second)->GetData());
    child_page->SetParentPageId(this->GetPageId());
    buffer_pool_manager->UnpinPage(array_[cnt].second, true);
  }
}



INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
  array_[0].second = old_value;
  array_[1].first = new_key;
  array_[1].second = new_value;
  SetSize(2);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                    const ValueType &new_value)->int {
  int index = ValueIndex(old_value);
  for (int i = GetSize(); i > index + 1; i--) {
    array_[i] = array_[i - 1];
  }
  array_[index + 1] = MappingType(new_key, new_value);
  IncreaseSize(1);
  return GetSize();
}


INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const->int {
  // 对于内部页面，key有序可以比较，但value无法比较，只能顺序查找
  for (int i = 0; i < GetSize(); i++) {  // 疑问：value应该是从0开始查找吧？key从1开始查找
    if (array_[i].second == value) {
      return i;  // 找到相同value
    }
  }
  // 找不到value，直接返回-1
  return -1;
}


INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager) {
  int move_num = this->GetSize();
  assert(recipient != nullptr);
  int recipient_start_index = recipient->GetSize();
  this->SetKeyAt(0, middle_key);
  for (int i = 0; i < move_num; i++) {
    recipient->array[recipient_start_index + i] = this->array[i];
    BPlusTreePage *child_page =
        reinterpret_cast<BPlusTreePage *>(buffer_pool_manager->FetchPage(array[i].second)->GetData());
    child_page->SetParentPageId(recipient->GetParentPageId());
    buffer_pool_manager->UnpinPage(array[i].second, true);
  }
  this->IncreaseSize(-1 * move_num);
  recipient->IncreaseSize(move_num);
}








/*
 * Helper method to get the value associated with input "index"(a.k.a array_
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { 
  return array_[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const -> ValueType {
  assert(GetSize() > 1);
  for (int i = 1; i < GetSize(); i++) {
    if (comparator(key, array_[i].first) < 0) {
      return array_[i - 1].second;
    }
  }
  return array_[GetSize() - 1].second;
}


// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
