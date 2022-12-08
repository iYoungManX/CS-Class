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
  Page *page = FindLeafPage(key, false);
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(page->GetData());
  ValueType value{};
  bool is_exist = leaf_node->Lookup(key, &value, comparator_);

  buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), false);  // unpin leaf page

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
  if (IsEmpty()) {
    StartNewTree(key, value);
    return true;
  }
  return InsertIntoLeaf(key, value, transaction);
}




INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) -> void {
  Page *root_page = buffer_pool_manager_->NewPage(&root_page_id_);
  if (root_page == nullptr) {
    throw "out of memory";
  }
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(root_page->GetData());
  leaf_page->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
  // true为插入，false为更新
  // create a new record<index_name + root_page_id> in header_page
  UpdateRootPageId(true);
  leaf_page->Insert(key, value, comparator_);
  buffer_pool_manager_->UnpinPage(root_page_id_, true);
}


INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) -> Page* {
  if (IsEmpty()) {
    return nullptr;
  }
  page_id_t next_page_id = root_page_id_;
  InternalPage *internal_page;
  for (internal_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(next_page_id)->GetData());
       !internal_page->IsLeafPage();
       internal_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(next_page_id)->GetData())) {
    page_id_t old_page_id = next_page_id;
    if (leftMost) {
      next_page_id = internal_page->ValueAt(0);
    } else {
      next_page_id = internal_page->Lookup(key, comparator_);
    }
    buffer_pool_manager_->UnpinPage(old_page_id, false);
  }
  return reinterpret_cast<Page *>(internal_page);
}


INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction)->bool {
  Page *page = FindLeafPage(key, false);
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  ValueType v;
  if (leaf_page->Lookup(key, &v, comparator_)) {
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return false;
  }
  leaf_page->Insert(key, value, comparator_);
  // 到达叶节点的max_size就需要进行分裂
  if (leaf_page->GetSize() >= leaf_page->GetMaxSize()) {
    LeafPage *new_leaf_page = Split(leaf_page);
    InsertIntoParent(leaf_page, new_leaf_page->KeyAt(0), new_leaf_page, transaction);
    buffer_pool_manager_->UnpinPage(new_leaf_page->GetPageId(), true);
  }
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  return true;
}


INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::Split(N *node) -> N* {
  page_id_t new_page_id;
  Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);
  if (new_page == nullptr) {
    throw "out of memory";
  }
  if (node->IsLeafPage()) {
    LeafPage *new_leaf_page = reinterpret_cast<LeafPage *>(new_page->GetData());
    LeafPage *old_leaf_page = reinterpret_cast<LeafPage *>(node);
    new_leaf_page->Init(new_page_id, old_leaf_page->GetParentPageId(), leaf_max_size_);
    old_leaf_page->MoveHalfTo(new_leaf_page);
    new_leaf_page->SetNextPageId(old_leaf_page->GetNextPageId());
    old_leaf_page->SetNextPageId(new_page_id);
  } else {
    InternalPage *new_internal_page = reinterpret_cast<InternalPage *>(new_page->GetData());
    InternalPage *old_internal_page = reinterpret_cast<InternalPage *>(node);
    new_internal_page->Init(new_page_id, old_internal_page->GetParentPageId(), internal_max_size_);
    // 注意！！内部节点分裂后，需要更新分配给新的内部节点的所有子节点的父节点指向
    old_internal_page->MoveHalfTo(new_internal_page, buffer_pool_manager_);
  }
  return reinterpret_cast<N *>(new_page->GetData());
}


INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  if (old_node->IsRootPage()) {
    // 创建新的根节点要更新root_page_id_和header_page
    Page *new_page = buffer_pool_manager_->NewPage(&root_page_id_);
    UpdateRootPageId(false);
    InternalPage *new_root_page = reinterpret_cast<InternalPage *>(new_page->GetData());
    new_root_page->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
    new_root_page->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    old_node->SetParentPageId(root_page_id_);
    new_node->SetParentPageId(root_page_id_);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
  } else {
    page_id_t parent_page_id = old_node->GetParentPageId();
    InternalPage *parent_page =
        reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_page_id)->GetData());
    parent_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
    new_node->SetParentPageId(parent_page_id);
    // 对内部节点来说，到达max_size+1才需要进行分裂
    if (parent_page->GetSize() > parent_page->GetMaxSize()) {
      InternalPage *uncle_page = Split(parent_page);
      InsertIntoParent(parent_page, uncle_page->KeyAt(0), uncle_page, transaction);
      buffer_pool_manager_->UnpinPage(uncle_page->GetPageId(), true);
    }
    buffer_pool_manager_->UnpinPage(parent_page_id, true);
  }
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

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if (IsEmpty()) {
    return;
  }
  Page *page = FindLeafPage(key, false);  // unpin
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  assert(leaf_page != nullptr);
  leaf_page->RemoveAndDeleteRecord(key, comparator_);
  if (leaf_page->GetSize() < leaf_page->GetMinSize()) {
    CoalesceOrRedistribute(leaf_page, transaction);
  }
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
}



INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction)->bool {
  assert(node != nullptr);
  if (node->IsRootPage()) {
    return AdjustRoot(node);
  }
  N *sibling;
  // 获取的sibling是否是node的下一个节点
  // Unpin
  bool node_prev_sibling = FindSibling(node, &sibling);
  // unpin
  InternalPage *parent_page =
      reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(node->GetParentPageId())->GetData());

  assert(node != nullptr && sibling != nullptr);
  if (node->GetSize() + sibling->GetSize() <= MaxSize(node)) {
    // 统一认为sibling在左边，node在右边
    if (node_prev_sibling) {
      N *temp = sibling;
      sibling = node;
      node = temp;
    }
    // index是右边的节点在父节点中的index
    int index = parent_page->ValueIndex(node->GetPageId());
    Coalesce(&sibling, &node, &parent_page, index, transaction);
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(sibling->GetPageId(), true);
    return true;
  }
  // index为underflow的节点在父节点中的index
  int index = parent_page->ValueIndex(node->GetPageId());
  Redistribute(sibling, node, index);
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), false);
  buffer_pool_manager_->UnpinPage(sibling->GetPageId(), true);
  return false;
}



INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::MaxSize(N *node) ->int {
  return node->IsLeafPage() ? node->GetMaxSize() - 1 : node->GetMaxSize();
}


// 如果获取的是前一个sibling，则返回false；下一个sibling，则返回true
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::FindSibling(N *node, N **sibling)->bool {
  // unpin
  InternalPage *parent_page =
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
auto BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) -> bool{
  // Case 1: old_root_node是内部结点，且大小为1。表示内部结点其实已经没有key了，所以要把它的孩子更新成新的根结点
  // old_root_node (internal node) has only one size
  if (!old_root_node->IsLeafPage() && old_root_node->GetSize() == 1) {
    // LOG_INFO("AdjustRoot: delete the last element in root page, but root page still has one last child");
    // get child page as new root page
    InternalPage *internal_node = reinterpret_cast<InternalPage *>(old_root_node);
    page_id_t child_page_id = internal_node->RemoveAndReturnOnlyChild();

    // NOTE: don't need to unpin old_root_node, this operation will be done in CoalesceOrRedistribute function
    // buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), true);

    // update root page id
    root_page_id_ = child_page_id;
    UpdateRootPageId(0);
    // update parent page id of new root node
    Page *new_root_page = buffer_pool_manager_->FetchPage(root_page_id_);
    InternalPage *new_root_node = reinterpret_cast<InternalPage *>(new_root_page->GetData());
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



INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction) ->bool{
  if ((*node)->IsLeafPage()) {
    LeafPage *leaf_node = reinterpret_cast<LeafPage *>(*node);
    LeafPage *leaf_neighbor_node = reinterpret_cast<LeafPage *>(*neighbor_node);
    leaf_node->MoveAllTo(leaf_neighbor_node);
    leaf_neighbor_node->SetNextPageId(leaf_node->GetNextPageId());
  } else {
    InternalPage *internal_node = reinterpret_cast<InternalPage *>(*node);
    InternalPage *internal_neighbor_node = reinterpret_cast<InternalPage *>(*neighbor_node);
    internal_node->MoveAllTo(internal_neighbor_node, (*parent)->KeyAt(index), buffer_pool_manager_);
  }
  buffer_pool_manager_->UnpinPage((*node)->GetPageId(), true);
  buffer_pool_manager_->DeletePage((*node)->GetPageId());
  (*parent)->Remove(index);
  assert((*parent) != nullptr);
  if ((*parent)->GetSize() < (*parent)->GetMinSize()) {
    return CoalesceOrRedistribute((*parent), transaction);
  }
  return true;
}



INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  Page *parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  InternalPage *parent = reinterpret_cast<InternalPage *>(parent_page->GetData());  // parent of node

  // node是之前刚被删除过一个key的结点
  // index=0，则neighbor是node后继结点，表示：node(left)      neighbor(right)
  // index>0，则neighbor是node前驱结点，表示：neighbor(left)  node(right)
  // 注意更新parent结点的相关kv对

  if (node->IsLeafPage()) {
    LeafPage *leaf_node = reinterpret_cast<LeafPage *>(node);
    LeafPage *neighbor_leaf_node = reinterpret_cast<LeafPage *>(neighbor_node);
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
    InternalPage *internal_node = reinterpret_cast<InternalPage *>(node);
    InternalPage *neighbor_internal_node = reinterpret_cast<InternalPage *>(neighbor_node);
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
  KeyType key{};  // not used
  Page *leaf_page = FindLeafPage(key, true);  // pin leftmost leaf page
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
  Page *leaf_page = FindLeafPage(key, false);
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  int index = leaf_node->KeyIndex(key, comparator_);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf_page, index);
  }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  KeyType key{};
  Page *leaf_page = FindLeafPage(key, false);
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf_page, leaf_node->GetSize());
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
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    //  true
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // false
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
