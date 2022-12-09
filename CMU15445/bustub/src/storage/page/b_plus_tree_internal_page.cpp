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
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetSize(0);            // 最开始current size为0
  SetMaxSize(max_size);  // max_size=INTERNAL_PAGE_SIZE-1 这里一定要减1，因为内部页面的第一个key是无效的
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array__ offset)
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
  // 当前node的第一个key(即array[0].first)本是无效值(因为是内部结点)，但由于要移动当前node的整个array到recipient
  // 那么必须在移动前将当前node的第一个key 赋值为 父结点中下标为index的middle_key
  SetKeyAt(0, middle_key);  // 将分隔key设置在0的位置
  recipient->CopyNFrom(array_, GetSize(), buffer_pool_manager);
  // 对于内部结点的合并操作，要把需要删除的内部结点的叶子结点转移过去
  // recipient->SetKeyAt(GetSize(), middle_key);
  SetSize(0);
}



INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  // delete array[index], move array after index to front by 1 size
  IncreaseSize(-1);
  for (int i = index; i < GetSize(); i++) {
    array_[i] = array_[i + 1];
  }
}


INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() ->ValueType {
  SetSize(0);
  return ValueAt(0);
}


INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                      BufferPoolManager *buffer_pool_manager) {
  // 当前node的第一个key本是无效值(因为是内部结点)，但由于要移动当前node的array_[0]到recipient尾部
  // 那么必须在移动前将当前node的第一个key 赋值为 父结点中下标为1的middle_key
  SetKeyAt(0, middle_key);
  // first item (array_[0]) of this page array_ copied to recipient page last
  recipient->CopyLastFrom(array_[0], buffer_pool_manager);
  // delete array_[0]
  Remove(0);  // 函数复用
}



INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &item, BufferPoolManager *buffer_pool_manager) {
  array_[GetSize()] = item;

  // update parent page id of child page
  Page *child_page = buffer_pool_manager->FetchPage(ValueAt(GetSize()));
  BPlusTreePage *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
  child_node->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(child_page->GetPageId(), true);

  IncreaseSize(1);
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
  int left = 1;
  int right = GetSize() - 1;
  while (left <= right) {
    int mid = left + (right - left) / 2;
    if (comparator(KeyAt(mid), key) > 0) {  // 下标还需要减小
      right = mid - 1;
    } else {  // 下标还需要增大
      left = mid + 1;
    }
  }  // upper_bound
  int target_index = left;
  assert(target_index - 1 >= 0);
  // 注意，返回的value下标要减1，这样才能满足key(i-1) <= subtree(value(i)) < key(i)
  return ValueAt(target_index - 1);
}



/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                       BufferPoolManager *buffer_pool_manager) {
  // recipient的第一个key本是无效值(因为是内部结点)，但由于要移动当前node的array[GetSize()-1]到recipient首部
  // 那么必须在移动前将recipient的第一个key 赋值为 父结点中下标为index的middle_key
  recipient->SetKeyAt(0, middle_key);
  // last item (array[size-1]) of this page array inserted to recipient page first
  recipient->CopyFirstFrom(array_[GetSize() - 1], buffer_pool_manager);
  // remove last item of this page
  IncreaseSize(-1);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &item, BufferPoolManager *buffer_pool_manager) {
  // move array_ after index=0 to back by 1 size
  for (int i = GetSize(); i >= 0; i--) {
    array_[i + 1] = array_[i];
  }
  // insert item to array_[0]
  array_[0] = item;

  // update parent page id of child page
  Page *child_page = buffer_pool_manager->FetchPage(ValueAt(0));
  BPlusTreePage *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
  child_node->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(child_page->GetPageId(), true);

  IncreaseSize(1);
}



// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
