#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  // Page *page = FindLeafPageByOperation(key, Operation::FIND, transaction).first;
  // auto leaf_node = reinterpret_cast<LeafPage *>(page->GetData());
  // ValueType value{};
  // bool is_exist = leaf_node->Lookup(key, &value, comparator_);
  // page->RUnlatch();
  // buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), false);  // unpin leaf page

  // if (!is_exist) {
  //   return false;
  // }
  // result->push_back(value);
  // return true;
  Page *leaf_page = FindLeafPageByOperation(key, Operation::FIND, transaction).first;

  // 2 在leaf page里找这个key
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());  // 记得加上GetData()

  ValueType value{};
  bool is_exist = leaf_node->Lookup(key, &value, comparator_);

  // 3 page用完后记得unpin page（疑问：unpin这句话一定要放在这里吗？也就是用完之后马上unpin？）
  leaf_page->RUnlatch();
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);  // unpin leaf page

  if (!is_exist) {
    return false;
  }
  result->push_back(value);
  return true;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  {
    const std::lock_guard<std::mutex> guard(root_latch_);  // 注意新建根节点时要锁住
    // std::scoped_lock lock{root_latch_};
    if (IsEmpty()) {
      StartNewTree(key, value);
      return true;
    }
  }
  return InsertIntoLeaf(key, value, transaction);
}

// INDEX_TEMPLATE_ARGUMENTS
// auto BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) -> void {
//   Page *root_page = buffer_pool_manager_->NewPage(&root_page_id_);
//   if (root_page == nullptr) {
//     throw "out of memory";
//   }
//   auto leaf_page = reinterpret_cast<LeafPage *>(root_page->GetData());
//   leaf_page->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
//   // true为插入，false为更新
//   // create a new record<index_name + root_page_id> in header_page
//   UpdateRootPageId(true);
//   leaf_page->Insert(key, value, comparator_);
//   buffer_pool_manager_->UnpinPage(root_page_id_, true);
// }

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  // LOG_INFO("ENTER StartNewTree key=%ld thread=%lu", key.ToString(), getThreadId());  // DEBUG

  // 1 缓冲池申请一个new page，作为root page
  page_id_t new_page_id = INVALID_PAGE_ID;
  Page *root_page = buffer_pool_manager_->NewPage(&new_page_id);  // 注意new page的pin_count=1，之后记得unpin page
  if (nullptr == root_page) {
    throw std::runtime_error("out of memory");
  }
  // 2 page id赋值给root page id，并插入header page的root page id
  root_page_id_ = new_page_id;
  UpdateRootPageId(1);  // insert root page id in header page

  // 3 使用leaf page的Insert函数插入(key,value)
  LeafPage *root_node = reinterpret_cast<LeafPage *>(root_page->GetData());  // 记得加上GetData()
  root_node->Init(new_page_id, INVALID_PAGE_ID, leaf_max_size_);             // 记得初始化为leaf_max_size
  root_node->Insert(key, value, comparator_);
  // 4 unpin root page
  buffer_pool_manager_->UnpinPage(root_page->GetPageId(), true);  // 注意：这里dirty要置为true！

  // LOG_INFO("END StartNewTree key=%ld thread=%lu", key.ToString(), getThreadId());  // DEBUG
}

// INDEX_TEMPLATE_ARGUMENTS
// auto BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) -> Page* {
//   if (IsEmpty()) {
//     return nullptr;
//   }
//   page_id_t next_page_id = root_page_id_;
//   InternalPage *internal_page;
//   for (internal_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(next_page_id)->GetData());
//        !internal_page->IsLeafPage();
//        internal_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(next_page_id)->GetData())) {
//     page_id_t old_page_id = next_page_id;
//     if (leftMost) {
//       next_page_id = internal_page->ValueAt(0);
//     } else {
//       next_page_id = internal_page->Lookup(key, comparator_);
//     }
//     buffer_pool_manager_->UnpinPage(old_page_id, false);
//   }
//   return reinterpret_cast<Page *>(internal_page);
// }

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) -> Page * {
  return FindLeafPageByOperation(key, Operation::FIND, nullptr, leftMost, false).first;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPageByOperation(const KeyType &key, Operation operation, Transaction *transaction,
                                             bool leftMost, bool rightMost) -> std::pair<Page *, bool> {
  assert(operation == Operation::FIND ? !(leftMost && rightMost) : transaction != nullptr);

  // LOG_INFO("BEGIN FindLeafPage key=%ld Thread=%lu Operation=%d", key.ToString(), getThreadId(),
  //          OpToString(operation));  // DEBUG

  root_latch_.lock();
  bool is_root_page_id_latched = true;

  Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
  auto node = reinterpret_cast<BPlusTreePage *>(page->GetData());

  if (operation == Operation::FIND) {
    page->RLatch();
    is_root_page_id_latched = false;
    root_latch_.unlock();
  } else {
    page->WLatch();
    if (IsSafe(node, operation)) {
      is_root_page_id_latched = false;
      root_latch_.unlock();
    }
  }

  while (!node->IsLeafPage()) {
    auto i_node = reinterpret_cast<InternalPage *>(node);

    page_id_t child_node_page_id;
    if (leftMost) {
      child_node_page_id = i_node->ValueAt(0);
    } else if (rightMost) {
      child_node_page_id = i_node->ValueAt(i_node->GetSize() - 1);
    } else {
      child_node_page_id = i_node->Lookup(key, comparator_);
    }

    auto child_page = buffer_pool_manager_->FetchPage(child_node_page_id);
    auto child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());

    if (operation == Operation::FIND) {
      child_page->RLatch();
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    } else {
      child_page->WLatch();
      transaction->AddIntoPageSet(page);
      // child node is safe, release all locks on ancestors
      if (IsSafe(child_node, operation)) {
        if (is_root_page_id_latched) {
          is_root_page_id_latched = false;
          root_latch_.unlock();
        }
        UnlockUnpinPages(transaction);
      }
    }

    page = child_page;
    node = child_node;
  }  // end while

  // LOG_INFO("END FindLeafPage key=%ld Thread=%lu Operation=%d", key.ToString(), getThreadId(),
  //          OpToString(operation));  // DEBUG

  return std::make_pair(page, is_root_page_id_latched);
}

/* unlock all pages */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnlockPages(Transaction *transaction) {
  if (transaction == nullptr) {
    return;
  }

  // unlock 和 unpin 事务经过的所有parent page
  for (Page *page : *transaction->GetPageSet()) {  // 前面加*是因为page set是shared_ptr类型
    page->WUnlatch();
  }
  transaction->GetPageSet()->clear();  // 清空page set

  // 如果root的mutex被此线程锁住则解锁
  // if (root_is_latched_) {
  //   root_is_latched_ = false;
  //   root_latch_.unlock();
  // }
}

/* unlock and unpin all pages */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnlockUnpinPages(Transaction *transaction) {
  // LOG_INFO("ENTER UnlockUnpinPages Thread=%ld", getThreadId());

  // 如果root的mutex被此线程锁住则解锁
  // if (root_is_latched_) {
  //   root_is_latched_ = false;
  //   root_latch_.unlock();
  // }

  if (transaction == nullptr) {
    return;
  }

  // auto pages = transaction->GetPageSet().get();
  // LOG_INFO("transaction page set size = %d", (int)pages->size());

  // unlock 和 unpin 事务经过的所有parent page
  for (Page *page : *transaction->GetPageSet()) {  // 前面加*是因为page set是shared_ptr类型
    // BPlusTreePage *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    // LOG_INFO("unlock page id=%d Thread=%ld", page->GetPageId(), getThreadId());
    // if (node->IsRootPage() && root_is_latched_) {
    //   LOG_INFO("unlock root page id=%d Thread=%ld", page->GetPageId(), getThreadId());
    //   root_latch_.unlock();
    //   root_is_latched_ = false;
    // }

    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);  // 疑问：此处dirty为false还是true？
    // 应该是false，此函数只在向下find leaf page时使用，向上进行修改时是手动一步步unpin true，这里是一次性unpin

    // if (op == Operation::FIND) {
    //   // 释放读锁
    //   page->RUnlatch();
    //   buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    // } else {
    //   // 释放写锁
    //   page->WUnlatch();
    //   buffer_pool_manager_->UnpinPage(page->GetPageId(), false);  // 疑问：此处dirty为false还是true？
    // }
  }
  transaction->GetPageSet()->clear();  // 清空page set

  // delete page 疑问：此处需要区分operation是否为delete吗？
  // if (op == Operation::DELETE) {
  //   for (page_id_t page_id : *transaction->GetDeletedPageSet()) {
  //     buffer_pool_manager_->DeletePage(page_id);
  //   }
  //   transaction->GetDeletedPageSet()->clear();  // 清空deleted page set
  // }
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::IsSafe(N *node, Operation op) -> bool {
  if (node->IsRootPage()) {
    return (op == Operation::INSERT && node->GetSize() < node->GetMaxSize() - 1) ||
           (op == Operation::DELETE && node->GetSize() > 2);
  }

  if (op == Operation::INSERT) {
    // 疑问：maxsize要减1吗？
    return node->GetSize() < node->GetMaxSize() - 1;
  }

  if (op == Operation::DELETE) {
    // 此处逻辑需要和coalesce函数对应
    return node->GetSize() > node->GetMinSize();
  }

  // LOG_INFO("IsSafe Thread=%ld", getThreadId());

  return true;
}

// INDEX_TEMPLATE_ARGUMENTS
// auto BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction)->bool {
//   Page *page = FindLeafPage(key, false);
//   auto leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
//   ValueType v;
//   if (leaf_page->Lookup(key, &v, comparator_)) {
//     buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
//     return false;
//   }
//   leaf_page->Insert(key, value, comparator_);
//   // 到达叶节点的max_size就需要进行分裂
//   if (leaf_page->GetSize() >= leaf_page->GetMaxSize()) {
//     LeafPage *new_leaf_page = Split(leaf_page);
//     InsertIntoParent(leaf_page, new_leaf_page->KeyAt(0), new_leaf_page, transaction);
//     buffer_pool_manager_->UnpinPage(new_leaf_page->GetPageId(), true);
//   }
//   buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
//   return true;
// }

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  // LOG_INFO("ENTER InsertIntoLeaf key=%ld thread=%lu", key.ToString(), getThreadId());

  // bool is_root_latched_;  // DEBUG

  // 1 find the leaf page as insertion target
  // Page *leaf_page = FindLeafPage(key, false, transaction, Operation::INSERT,
  //  &is_root_latched_);  // pin and W Latch leaf page and ancesters
  auto [leaf_page, root_is_latched] = FindLeafPageByOperation(key, Operation::INSERT, transaction);

  auto leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());  // 注意，记得加上GetData()

  // ValueType lookup_value{};  // not used
  // bool is_exist = leaf_node->Lookup(key, &lookup_value, comparator_);

  int size = leaf_node->GetSize();

  // 2 the key not exist, so we can insert (key,value) to leaf node
  int new_size = leaf_node->Insert(key, value, comparator_);

  // if (is_exist) {
  if (new_size == size) {
    // assert(root_is_latched_ == true);
    if (root_is_latched) {
      // LOG_INFO("Before END InsertIntoLeaf duplicate keys: root_latch_.unlock()");
      root_latch_.unlock();
    }
    UnlockUnpinPages(transaction);  // 此函数中会释放叶子的所有现在被锁住的祖先（不包括叶子）
    // assert(root_is_latched_ == false);
    leaf_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);  // unpin leaf page
    // LOG_INFO("END InsertIntoLeaf duplicate keys! key=%ld thread=%lu", key.ToString(), getThreadId());
    return false;
  }

  if (new_size < leaf_node->GetMaxSize()) {
    // UnlockUnpinPages(transaction, Operation::INSERT);  // DEBUG 疑问：此处是否需要释放所有祖先节点的锁？
    // 似乎不需要，因为如果叶子节点插入后小于maxsize，说明其父节点在之前在findLeafPage就已经释放过锁了

    if (root_is_latched) {
      // LOG_INFO("Before END InsertIntoLeaf no split: root_latch_.unlock()");
      root_latch_.unlock();
    }

    leaf_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);  // unpin leaf page
    // LOG_INFO("END InsertIntoLeaf no split! key=%ld thread=%lu", key.ToString(), getThreadId());  // DEBUG
    return true;
  }

  // new_size >= leaf_node->GetMaxSize()
  LeafPage *new_leaf_node = Split(leaf_node);  // pin new leaf node

  bool *pointer_root_is_latched = new bool(root_is_latched);

  InsertIntoParent(leaf_node, new_leaf_node->KeyAt(0), new_leaf_node, transaction,
                   pointer_root_is_latched);  // 此函数内将会 W Unlatch

  assert((*pointer_root_is_latched) == false);

  delete pointer_root_is_latched;

  // 疑问：这里别人似乎没有unpin？？？(我觉得必须unpin，InsertIntoParent函数里面并不会unpin old node和new node)

  // DEBUG
  // if (is_root_latched_) {
  //   // LOG_INFO("Before END InsertIntoLeaf with split: root_latch_.unlock()");
  //   root_latch_.unlock();
  // }

  leaf_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);      // unpin leaf page
  buffer_pool_manager_->UnpinPage(new_leaf_node->GetPageId(), true);  // DEBUG: unpin new leaf node

  // LOG_INFO("END InsertIntoLeaf with split! key=%ld thread=%lu", key.ToString(), getThreadId());  // DEBUG
  return true;
}

// INDEX_TEMPLATE_ARGUMENTS
// template <typename N>
// auto BPLUSTREE_TYPE::Split(N *node) -> N * {
//   page_id_t new_page_id;
//   Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);
//   if (new_page == nullptr) {
//     throw "out of memory";
//   }
//   if (node->IsLeafPage()) {
//     auto new_leaf_page = reinterpret_cast<LeafPage *>(new_page->GetData());
//     auto old_leaf_page = reinterpret_cast<LeafPage *>(node);
//     new_leaf_page->Init(new_page_id, old_leaf_page->GetParentPageId(), leaf_max_size_);
//     old_leaf_page->MoveHalfTo(new_leaf_page);
//     new_leaf_page->SetNextPageId(old_leaf_page->GetNextPageId());
//     old_leaf_page->SetNextPageId(new_page_id);
//   } else {
//     auto new_internal_page = reinterpret_cast<InternalPage *>(new_page->GetData());
//     auto old_internal_page = reinterpret_cast<InternalPage *>(node);
//     new_internal_page->Init(new_page_id, old_internal_page->GetParentPageId(), internal_max_size_);
//     // 注意！！内部节点分裂后，需要更新分配给新的内部节点的所有子节点的父节点指向
//     old_internal_page->MoveHalfTo(new_internal_page, buffer_pool_manager_);
//   }
//   return reinterpret_cast<N *>(new_page->GetData());
// }

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  // 1 缓冲池申请一个new page
  page_id_t new_page_id = INVALID_PAGE_ID;
  Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);  // 注意new page的pin_count=1，之后记得unpin page
  if (nullptr == new_page) {
    throw std::runtime_error("out of memory");
  }
  // 2 分情况进行拆分
  auto new_node = reinterpret_cast<N *>(new_page->GetData());  // 记得加上GetData()
  new_node->SetPageType(node->GetPageType());                // DEBUG

  if (node->IsLeafPage()) {  // leaf page
    auto old_leaf_node = reinterpret_cast<LeafPage *>(node);
    auto new_leaf_node = reinterpret_cast<LeafPage *>(new_node);
    new_leaf_node->Init(new_page_id, node->GetParentPageId(), leaf_max_size_);  // 注意初始化parent id和max_size
    // old_leaf_node右半部分 移动至 new_leaf_node
    old_leaf_node->MoveHalfTo(new_leaf_node);
    // 更新叶子层的链表，示意如下：
    // 原来：old node ---> next node
    // 最新：old node ---> new node ---> next node
    new_leaf_node->SetNextPageId(old_leaf_node->GetNextPageId());  // 完成连接new node ---> next node
    old_leaf_node->SetNextPageId(new_leaf_node->GetPageId());      // 完成连接old node ---> new node
    new_node = reinterpret_cast<N *>(new_leaf_node);
  } else {  // internal page
    auto old_internal_node = reinterpret_cast<InternalPage *>(node);
    auto new_internal_node = reinterpret_cast<InternalPage *>(new_node);
    new_internal_node->Init(new_page_id, node->GetParentPageId(), internal_max_size_);  // 注意初始化parent id和max_size
    // old_internal_node右半部分 移动至 new_internal_node
    // new_node（原old_node的右半部分）的所有孩子结点的父指针更新为指向new_node
    old_internal_node->MoveHalfTo(new_internal_node, buffer_pool_manager_);
    new_node = reinterpret_cast<N *>(new_internal_node);
  }
  // fetch page and new page need to unpin page (do it outside)
  return new_node;  // 注意，此时new_node还没有unpin
}

// INDEX_TEMPLATE_ARGUMENTS
// void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
//                                       Transaction *transaction) {
//   if (old_node->IsRootPage()) {
//     // 创建新的根节点要更新root_page_id_和header_page
//     Page *new_page = buffer_pool_manager_->NewPage(&root_page_id_);
//     UpdateRootPageId(false);
//     InternalPage *new_root_page = reinterpret_cast<InternalPage *>(new_page->GetData());
//     new_root_page->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
//     new_root_page->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
//     old_node->SetParentPageId(root_page_id_);
//     new_node->SetParentPageId(root_page_id_);
//     buffer_pool_manager_->UnpinPage(root_page_id_, true);
//   } else {
//     page_id_t parent_page_id = old_node->GetParentPageId();
//     InternalPage *parent_page =
//         reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_page_id)->GetData());
//     parent_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
//     new_node->SetParentPageId(parent_page_id);
//     // 对内部节点来说，到达max_size+1才需要进行分裂
//     if (parent_page->GetSize() > parent_page->GetMaxSize()) {
//       InternalPage *uncle_page = Split(parent_page);
//       InsertIntoParent(parent_page, uncle_page->KeyAt(0), uncle_page, transaction);
//       buffer_pool_manager_->UnpinPage(uncle_page->GetPageId(), true);
//     }
//     buffer_pool_manager_->UnpinPage(parent_page_id, true);
//   }
// }

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction, bool *root_is_latched) {
  // 1 old_node是根结点，那么整棵树直接升高一层
  // 具体操作是创建一个新结点R当作根结点，其关键字为key，左右孩子结点分别为old_node和new_node
  if (old_node->IsRootPage()) {  // old node为根结点
    page_id_t new_page_id = INVALID_PAGE_ID;
    Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);  // 这里应该是NewPage，不是FetchPage！
    root_page_id_ = new_page_id;

    auto new_root_node = reinterpret_cast<InternalPage *>(new_page->GetData());
    new_root_node->Init(new_page_id, INVALID_PAGE_ID, internal_max_size_);  // 注意初始化parent page id和max_size
    // 修改新的根结点的孩子指针，即array[0].second指向old_node，array[1].second指向new_node；对于array[1].first则赋值为key
    new_root_node->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    // 修改old_node和new_node的父指针
    old_node->SetParentPageId(new_page_id);
    new_node->SetParentPageId(new_page_id);

    // buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);  // DEBUG
    buffer_pool_manager_->UnpinPage(new_page->GetPageId(), true);  // 修改了new_page->data，所以dirty置为true

    UpdateRootPageId(0);  // update root page id in header page

    // 新的root必定不在transaction的page_set_队列中
    if (*root_is_latched) {
      *root_is_latched = false;
      root_latch_.unlock();
    }

    UnlockPages(transaction);
    // LOG_INFO("InsertIntoParent old node is root: completed key=%ld thread=%lu", key.ToString(), getThreadId());
    return;  // 结束递归
  }

  // LOG_INFO("InsertIntoParent old node is NOT root: completed key=%ld thread=%lu", key.ToString(), getThreadId());

  // 2 old_node不是根结点
  // 找到old_node的父结点进行操作
  // a. 先直接插入(key,new_node->page_id)到父结点
  // b. 如果插入后父结点满了，则需要对父结点再进行拆分(Split)，并继续递归
  // buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);                      // DEBUG
  Page *parent_page = buffer_pool_manager_->FetchPage(old_node->GetParentPageId());  // pin parent page

  auto parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
  // 将(key,new_node->page_id)插入到父结点中 value==old_node->page_id 的下标之后
  parent_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());  // size+1

  // 父节点未满
  if (parent_node->GetSize() < parent_node->GetMaxSize()) {
    // DEBUG
    if (*root_is_latched) {
      *root_is_latched = false;
      root_latch_.unlock();
    }

    UnlockPages(transaction);  // unlock除了叶子结点以外的所有上锁的祖先节点
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);  // unpin parent page
    return;
  }

  // 父结点已满(注意，之前的insert使得size+1)，需要拆分，再递归InsertIntoParent
  // parent_node拆分成两个，分别是parent_node和new_parent_node
  InternalPage *new_parent_node = Split(parent_node);  // pin new parent node
  // 继续递归，下一层递归是将拆分后新结点new_parent_node的第一个key插入到parent_node的父结点
  InsertIntoParent(parent_node, new_parent_node->KeyAt(0), new_parent_node, transaction, root_is_latched);

  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);      // unpin parent page
  buffer_pool_manager_->UnpinPage(new_parent_node->GetPageId(), true);  // unpin new parent node
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */

// INDEX_TEMPLATE_ARGUMENTS
// void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
//   if (IsEmpty()) {
//     return;
//   }
//   Page *page = FindLeafPage(key, false);  // unpin
//   auto leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
//   assert(leaf_page != nullptr);
//   leaf_page->RemoveAndDeleteRecord(key, comparator_);
//   if (leaf_page->GetSize() < leaf_page->GetMinSize()) {
//     CoalesceOrRedistribute(leaf_page, transaction);
//   }
//   buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
// }

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  // LOG_INFO("ENTER Remove key=%ld thread=%lu", key.ToString(), getThreadId());

  // std::scoped_lock lock{latch_};  // DEBUG
  if (IsEmpty()) {
    return;
  }
  // find the leaf page as deletion target
  // Page *leaf_page = FindLeafPage(key, false, transaction, Operation::DELETE);  // pin leaf page
  // Page *leaf_page = FindLeafPageByOperation(key, Operation::DELETE, transaction).first;
  auto [leaf_page, root_is_latched] = FindLeafPageByOperation(key, Operation::DELETE, transaction);
  auto leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  int old_size = leaf_node->GetSize();
  int new_size = leaf_node->RemoveAndDeleteRecord(key, comparator_);  // 在leaf中删除key（如果不存在该key，则size不变）

  // 1 删除失败
  if (new_size == old_size) {
    if (root_is_latched) {
      root_latch_.unlock();
    }
    UnlockUnpinPages(transaction);

    leaf_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);  // unpin leaf page

    return;
  }

  // 2 删除成功，然后调用CoalesceOrRedistribute
  bool *pointer_root_is_latched = new bool(root_is_latched);

  bool leaf_should_delete = CoalesceOrRedistribute(leaf_node, transaction, pointer_root_is_latched);
  // NOTE: unlock and unpin are finished in CoalesceOrRedistribute
  // NOTE: root node must be unlocked in CoalesceOrRedistribute
  assert((*pointer_root_is_latched) == false);

  delete pointer_root_is_latched;

  if (leaf_should_delete) {
    transaction->AddIntoDeletedPageSet(leaf_page->GetPageId());
  }

  // if (root_is_latched) {
  //   LOG_INFO("END Remove key=%ld thread=%lu root_latch_.unlock()", key.ToString(), getThreadId());
  //   root_latch_.unlock();
  // }

  leaf_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);  // unpin leaf page

  // NOTE: ensure deleted pages have been unpined
  // 删除并清空deleted page set
  for (page_id_t page_id : *transaction->GetDeletedPageSet()) {
    buffer_pool_manager_->DeletePage(page_id);
  }
  transaction->GetDeletedPageSet()->clear();
}

// INDEX_TEMPLATE_ARGUMENTS
// template <typename N>
// auto BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction)->bool {
//   assert(node != nullptr);
//   if (node->IsRootPage()) {
//     return AdjustRoot(node);
//   }
//   N *sibling;
//   // 获取的sibling是否是node的下一个节点
//   // Unpin
//   bool node_prev_sibling = FindSibling(node, &sibling);
//   // unpin
//   InternalPage *parent_page =
//       reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(node->GetParentPageId())->GetData());

//   assert(node != nullptr && sibling != nullptr);
//   if (node->GetSize() + sibling->GetSize() <= MaxSize(node)) {
//     // 统一认为sibling在左边，node在右边
//     if (node_prev_sibling) {
//       N *temp = sibling;
//       sibling = node;
//       node = temp;
//     }
//     // index是右边的节点在父节点中的index
//     int index = parent_page->ValueIndex(node->GetPageId());
//     Coalesce(&sibling, &node, &parent_page, index, transaction);
//     buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
//     buffer_pool_manager_->UnpinPage(sibling->GetPageId(), true);
//     return true;
//   }
//   // index为underflow的节点在父节点中的index
//   int index = parent_page->ValueIndex(node->GetPageId());
//   Redistribute(sibling, node, index);
//   buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), false);
//   buffer_pool_manager_->UnpinPage(sibling->GetPageId(), true);
//   return false;
// }

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction, bool *root_is_latched) -> bool {
  if (node->IsRootPage()) {
    bool root_should_delete = AdjustRoot(node);

    // DEBUG
    if (*root_is_latched) {
      // LOG_INFO("CoalesceOrRedistribute node is root page root_latch_.unlock()");
      *root_is_latched = false;
      root_latch_.unlock();
    }

    UnlockPages(transaction);
    return root_should_delete;  // NOTE: size of root page can be less than min size
  }

  // 不需要合并或者重分配，直接返回false
  if (node->GetSize() >= node->GetMinSize()) {
    // 疑问：此处为什么不用root_latch_.unlock()

    if (*root_is_latched) {
      // LOG_INFO("CoalesceOrRedistribute node > minsize root_latch_.unlock()");
      *root_is_latched = false;
      root_latch_.unlock();
    }

    UnlockPages(transaction);
    return false;
  }

  // 需要合并或者重分配
  // 先获取node的parent page
  Page *parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  auto parent = reinterpret_cast<InternalPage *>(parent_page->GetData());

  // 获得node在parent的孩子指针(value)的index
  int index = parent->ValueIndex(node->GetPageId());
  // 寻找兄弟结点，尽量找到前一个结点(前驱结点)
  page_id_t sibling_page_id = parent->ValueAt(index == 0 ? 1 : index - 1);
  Page *sibling_page = buffer_pool_manager_->FetchPage(sibling_page_id);

  sibling_page->WLatch();  // 记得要锁住兄弟结点

  auto sibling_node = reinterpret_cast<N *>(sibling_page->GetData());

  // 1 Redistribute 当kv总和能支撑两个Node，那么重新分配即可，不必删除node
  if (node->GetSize() + sibling_node->GetSize() >= node->GetMaxSize()) {
    if (*root_is_latched) {
      // LOG_INFO("CoalesceOrRedistribute before Redistribute root_latch_.unlock()");
      *root_is_latched = false;
      root_latch_.unlock();
    }

    Redistribute(sibling_node, node, index);  // 无返回值

    UnlockPages(transaction);
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);

    sibling_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), true);

    return false;  // node不必被删除
  }

  // 2 Coalesce 当sibling和node只能凑成一个Node，那么合并两个结点到sibling，删除node
  // Coalesce函数继续递归调用CoalesceOrRedistribute
  bool parent_should_delete =
      Coalesce(&sibling_node, &node, &parent, index, transaction, root_is_latched);  // 返回值是parent是否需要被删除

  assert((*root_is_latched) == false);

  // 特别注意此处的解锁root
  // if (*root_is_latched) {
  //   LOG_INFO("CoalesceOrRedistribute after Coalesce root_latch_.unlock()");
  //   *root_is_latched = false;
  //   root_latch_.unlock();
  // }

  if (parent_should_delete) {
    transaction->AddIntoDeletedPageSet(parent->GetPageId());
  }

  // NOTE: parent unlock is finished in Coalesce
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);

  sibling_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), true);

  return true;  // node需要被删除
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::MaxSize(N *node) -> int {
  return node->IsLeafPage() ? node->GetMaxSize() - 1 : node->GetMaxSize();
}

// 如果获取的是前一个sibling，则返回false；下一个sibling，则返回true
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::FindSibling(N *node, N **sibling) -> bool {
  // unpin
  auto parent_page =
      reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(node->GetParentPageId())->GetData());
  int index = parent_page->ValueIndex(node->GetPageId());
  int sibling_index = index - 1;
  if (index == 0) {
    sibling_index = index + 1;
  }
  page_id_t sibling_page_id = parent_page->ValueAt(sibling_index);
  (*sibling) = reinterpret_cast<N *>(buffer_pool_manager_->FetchPage(sibling_page_id)->GetData());
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), false);
  return index == 0;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) -> bool {
  // Case 1: old_root_node是内部结点，且大小为1。表示内部结点其实已经没有key了，所以要把它的孩子更新成新的根结点
  // old_root_node (internal node) has only one size
  if (!old_root_node->IsLeafPage() && old_root_node->GetSize() == 1) {
    // LOG_INFO("AdjustRoot: delete the last element in root page, but root page still has one last child");
    // get child page as new root page
    auto internal_node = reinterpret_cast<InternalPage *>(old_root_node);
    page_id_t child_page_id = internal_node->RemoveAndReturnOnlyChild();

    // NOTE: don't need to unpin old_root_node, this operation will be done in CoalesceOrRedistribute function
    // buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), true);

    // update root page id
    root_page_id_ = child_page_id;
    UpdateRootPageId(0);
    // update parent page id of new root node
    Page *new_root_page = buffer_pool_manager_->FetchPage(root_page_id_);
    auto new_root_node = reinterpret_cast<InternalPage *>(new_root_page->GetData());
    new_root_node->SetParentPageId(INVALID_PAGE_ID);

    // if (root_is_latched) {
    //   root_latch_.unlock();
    // }

    buffer_pool_manager_->UnpinPage(new_root_page->GetPageId(), true);
    return true;
  }
  // Case 2: old_root_node是叶结点，且大小为0。直接更新root page id
  // all elements deleted from the B+ tree
  if (old_root_node->IsLeafPage() && old_root_node->GetSize() == 0) {
    // LOG_INFO("AdjustRoot: all elements deleted from the B+ tree");
    // NOTE: don't need to unpin old_root_node, this operation will be done in Remove function
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId(0);

    // if (root_is_latched) {
    //   root_latch_.unlock();
    // }

    return true;
  }

  // DEBUG
  // if (root_is_latched) {
  //   root_latch_.unlock();
  // }

  // 否则不需要有page被删除，直接返回false
  return false;
}

// INDEX_TEMPLATE_ARGUMENTS
// template <typename N>
// auto BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
//                               BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
//                               Transaction *transaction, bool *root_is_latched) -> bool {
//   if ((*node)->IsLeafPage()) {
//     auto leaf_node = reinterpret_cast<LeafPage *>(*node);
//     auto leaf_neighbor_node = reinterpret_cast<LeafPage *>(*neighbor_node);
//     leaf_node->MoveAllTo(leaf_neighbor_node);
//     leaf_neighbor_node->SetNextPageId(leaf_node->GetNextPageId());
//   } else {
//     auto internal_node = reinterpret_cast<InternalPage *>(*node);
//     auto internal_neighbor_node = reinterpret_cast<InternalPage *>(*neighbor_node);
//     internal_node->MoveAllTo(internal_neighbor_node, (*parent)->KeyAt(index), buffer_pool_manager_);
//   }
//   buffer_pool_manager_->UnpinPage((*node)->GetPageId(), true);
//   buffer_pool_manager_->DeletePage((*node)->GetPageId());
//   (*parent)->Remove(index);
//   assert((*parent) != nullptr);
//   if ((*parent)->GetSize() < (*parent)->GetMinSize()) {
//     return CoalesceOrRedistribute((*parent), transaction, root_is_latched);
//   }
//   return true;
// }


INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction, bool *root_is_latched) {
  // Assume that *neighbor_node is the left sibling of *node (neighbor -> node)
  // index表示node在parent中的孩子指针(value)的下标
  // key_index表示 交换后的 node在parent中的孩子指针(value)的下标
  // 若index=0，说明node为neighbor前驱，要保证neighbor为node的前驱，则交换变量neighbor和node，且key_index=1
  int key_index = index;
  if (index == 0) {
    std::swap(neighbor_node, node);  // 保证neighbor_node为node的前驱
    key_index = 1;
  }
  KeyType middle_key = (*parent)->KeyAt(key_index);  // middle_key only used in internal_node->MoveAllTo

  // Move items from node to neighbor_node
  if ((*node)->IsLeafPage()) {
    LeafPage *leaf_node = reinterpret_cast<LeafPage *>(*node);
    LeafPage *neighbor_leaf_node = reinterpret_cast<LeafPage *>(*neighbor_node);
    leaf_node->MoveAllTo(neighbor_leaf_node);
    neighbor_leaf_node->SetNextPageId(leaf_node->GetNextPageId());
    // LOG_INFO("Coalesce leaf, index=%d, pid=%d neighbor->node", index, (*node)->GetPageId());
  } else {
    InternalPage *internal_node = reinterpret_cast<InternalPage *>(*node);
    InternalPage *neighbor_internal_node = reinterpret_cast<InternalPage *>(*neighbor_node);
    // MoveAllTo do this: set node's first key to middle_key and move node to neighbor
    internal_node->MoveAllTo(neighbor_internal_node, middle_key, buffer_pool_manager_);
    // LOG_INFO("Coalesce internal, index=%d, pid=%d neighbor->node", index, (*node)->GetPageId());
  }
    // 在缓冲池中删除node
  // buffer_pool_manager_->UnpinPage((*node)->GetPageId(), true);
  // buffer_pool_manager_->DeletePage((*node)->GetPageId());

  // 删除node在parent中的kv信息
  (*parent)->Remove(key_index);  // 注意，是key_index，不是index

  // 因为parent中删除了kv对，所以递归调用CoalesceOrRedistribute函数判断parent结点是否需要被删除
  return CoalesceOrRedistribute(*parent, transaction, root_is_latched);
}


INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  Page *parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  auto parent = reinterpret_cast<InternalPage *>(parent_page->GetData());  // parent of node

  // node是之前刚被删除过一个key的结点
  // index=0，则neighbor是node后继结点，表示：node(left)      neighbor(right)
  // index>0，则neighbor是node前驱结点，表示：neighbor(left)  node(right)
  // 注意更新parent结点的相关kv对

  if (node->IsLeafPage()) {
    auto leaf_node = reinterpret_cast<LeafPage *>(node);
    auto neighbor_leaf_node = reinterpret_cast<LeafPage *>(neighbor_node);
    if (index == 0) {  // node -> neighbor
      // LOG_INFO("Redistribute leaf, index=0, pid=%d node->neighbor", node->GetPageId());
      // move neighbor's first to node's end
      neighbor_leaf_node->MoveFirstToEndOf(leaf_node);
      parent->SetKeyAt(1, neighbor_leaf_node->KeyAt(0));
    } else {  // neighbor -> node
      // move neighbor's last to node's front
      // LOG_INFO("Redistribute leaf, index=%d, pid=%d neighbor->node", index, node->GetPageId());
      neighbor_leaf_node->MoveLastToFrontOf(leaf_node);
      parent->SetKeyAt(index, leaf_node->KeyAt(0));
    }
  } else {
    auto internal_node = reinterpret_cast<InternalPage *>(node);
    auto neighbor_internal_node = reinterpret_cast<InternalPage *>(neighbor_node);
    if (index == 0) {  // case: node(left) and neighbor(right)
      // LOG_INFO("Redistribute internal, index=0, pid=%d node->neighbor", node->GetPageId());
      // MoveFirstToEndOf do this:
      // 1 set neighbor's first key to parent's second key（详见MoveFirstToEndOf函数）
      // 2 move neighbor's first to node's end
      neighbor_internal_node->MoveFirstToEndOf(internal_node, parent->KeyAt(1), buffer_pool_manager_);
      // set parent's second key to neighbor's "new" first key
      parent->SetKeyAt(1, neighbor_internal_node->KeyAt(0));
    } else {  // case: neighbor(left) and node(right)
      // LOG_INFO("Redistribute internal, index=%d, pid=%d neighbor->node", index, node->GetPageId());
      // MoveLastToFrontOf do this:
      // 1 set node's first key to parent's index key（详见MoveLastToFrontOf函数）
      // 2 move neighbor's last to node's front
      neighbor_internal_node->MoveLastToFrontOf(internal_node, parent->KeyAt(index), buffer_pool_manager_);
      // set parent's index key to node's "new" first key
      parent->SetKeyAt(index, internal_node->KeyAt(0));
    }
  }
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
  // LOG_INFO("END redistribute");
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  Page *leaf_page = FindLeafPageByOperation(KeyType(), Operation::FIND, nullptr, true).first;
  // LeafPage *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf_page, 0);  // 最左边的叶子且index=0
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  Page *leaf_page = FindLeafPageByOperation(key, Operation::FIND).first;
  auto leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  int index = leaf_node->KeyIndex(key, comparator_);  // 此处直接用KeyIndex，而不是Lookup
  // LOG_INFO("Tree.Begin before return INDEX class, index=%d leaf page id=%d leaf node page id=%d", index,
  //          leaf_page->GetPageId(), leaf_node->GetPageId());
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf_page, index);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  Page *leaf_page = FindLeafPageByOperation(KeyType(), Operation::FIND, nullptr, false, true).first;
  auto leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  // 从左向右开始遍历叶子层结点，直到最后一个
  // while (leaf_node->GetNextPageId() != INVALID_PAGE_ID) {
  //   int next_page_id = leaf_node->GetNextPageId();
  //   buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);        // unpin current leaf page
  //   Page *next_leaf_page = buffer_pool_manager_->FetchPage(next_page_id);  // pin next leaf page
  //   leaf_page = next_leaf_page;                                            // update current leaf page to next leaf
  //   page leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  // }  // 结束循环时，leaf_node为叶子层的最后一个结点（rightmost leaf page）
  // 注意传入的index为leaf_node->GetSize()
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf_page, leaf_node->GetSize());  // 注意：此时leaf_node没有unpin

}
/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
