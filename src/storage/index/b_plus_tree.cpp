#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"


#ifdef LOGROOT
#define MY_LOGR(...) LOG_DEBUG(__VA_ARGS__)
#else
#define MY_LOGR(...) ((void)0)
#endif

#ifdef LOGGETVALUE
#define MY_LOGG(...) LOG_DEBUG(__VA_ARGS__)
#else
#define MY_LOGG(...) ((void)0)
#endif

#ifdef LOGINSERRT
#define MY_LOGI(...) LOG_DEBUG(__VA_ARGS__)
#else
#define MY_LOGI(...) ((void)0)
#endif

#ifdef LOGDELETE
#define MY_LOGD(...) LOG_DEBUG(__VA_ARGS__)
#else
#define MY_LOGD(...) ((void)0)
#endif

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
  MY_LOGG("GetValue %ld", key.ToString());
  Page *page;
  bool root_locked;
  page = FindLeafPage(key, OpeType::FIND, true, root_locked, transaction);
  if (page == nullptr) {
    MY_LOGG("GetValue %ld not found", key.ToString());
    return false;
  }
  LeafPage *leaf_node = CastLeafPage(page);
  ValueType value;
  bool find = leaf_node->GetValue(key, value, comparator_);
  if (find) {
    result->push_back(value);
  }
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  MY_LOGG("GetValue %ld found", key.ToString());
  assert(!root_locked);
  return find;
}

/*
 * @brief
 *
 * @param key
 * @param operation   FIND INSERT REMOVE
 * @param optimistic  optimistic or pessimistic
 * @param root_locked
 * @param transaction
 * @return std::pair<Page *, bool>
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, OpeType operation, bool optimistic, bool &root_locked,
                                  Transaction *transaction) -> Page * {
  Page *page{};
  BPlusTreePage *node;

  if (operation == OpeType::FIND) {
    root_latch_.Latch();
    root_locked = true;
    if (IsEmpty()) {
      root_latch_.UnLatch();
      root_locked = false;
      return nullptr;
    }
    page = buffer_pool_manager_->FetchPage(root_page_id_);
    node = CastBPlusPage(page);
    page->RLatch();
    root_latch_.UnLatch();
    root_locked = false;

    while (!node->IsLeafPage()) {
      InternalPage *internal_node = CastInternalPage(node);
      int index = internal_node->Search(key, comparator_);
      // LOG_DEBUG("lookup to page %d at index %d", page->GetPageId(), idx - 1);
      Page *next_page = FetchChildPage(internal_node, index);
      next_page->RLatch();

      assert(CastBPlusPage(next_page)->GetParentPageId() == internal_node->GetPageId());
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);

      BPlusTreePage *next_node = CastInternalPage(next_page);
      node = next_node;
      page = next_page;
    }
    return page;
  }

  root_latch_.Latch();
  root_locked = true;
  if (IsEmpty()) {
    if (operation == OpeType::INSERT) {
      page = NewLeafRootPage(&root_page_id_);
    } else {
      root_latch_.UnLatch();
      root_locked = false;
      return nullptr;
    }
  } else {
    page = buffer_pool_manager_->FetchPage(root_page_id_);
  }
  node = CastBPlusPage(page);

  if (optimistic) {
    if (node->IsLeafPage()) {
      // 当root 是leaf时，判断root安全,插入才能释放锁
      // 否则提前释放了锁，root在这一个insert中分裂， 而别的线程依旧在这个旧的root中插入
      page->WLatch();
    } else {
      page->RLatch();
      root_latch_.UnLatch();
      root_locked = false;
    }

    while (!node->IsLeafPage()) {
      InternalPage *internal_node = CastInternalPage(node);
      int index = internal_node->Search(key, comparator_);

      Page *next_page = FetchChildPage(internal_node, index);   // TODO(admin):
      BPlusTreePage *next_node = CastBPlusPage(next_page);

      // FIXME() NOT SAFE
      if (next_node->IsLeafPage()) {
        next_page->WLatch();
      } else {
        next_page->RLatch();
      }

      assert(next_node->GetParentPageId() == internal_node->GetPageId());
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);

      node = next_node;
      page = next_page;
    }
    return page;
  }

  assert(transaction != nullptr);
  // pessimistic
  page->WLatch();
  if (IsSafe(node, operation)) {                                  // TODO(admin):
    root_latch_.UnLatch();
    root_locked = false;
  }

  while (!node->IsLeafPage()) {
    InternalPage *internal_node = CastInternalPage(node);
    int index = internal_node->Search(key, comparator_);

    Page *next_page = FetchChildPage(internal_node, index);
    BPlusTreePage *next_node = CastInternalPage(next_page);

    next_page->WLatch();
    transaction->AddIntoPageSet(page);
    assert(next_node->GetParentPageId() == internal_node->GetPageId());

    if (IsSafe(next_node, operation)) {
      ClearAndUnlock(transaction, root_locked);                       // TODO(admin):
    }
    node = next_node;
    page = next_page;
  }
  return page;
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
  MY_LOGI("Insert %ld", key.ToString());
#ifndef PESSIMISTIC
  // LOG_DEBUG("\033[1;34m insert %ld\033[0m", key.ToString());
  bool root_locked = false;
  auto page = FindLeafPage(key, OpeType::INSERT, true, root_locked, transaction);

  LeafPage *leaf_node = CastLeafPage(page);

  if (IsSafe(leaf_node, OpeType::INSERT)) {
    bool inserted = leaf_node->Insert(key, value, comparator_);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    UnlockRoot(root_locked);
    return inserted;
  }

  // restart pessimistic
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  UnlockRoot(root_locked);
#endif
  return InsertPessimistic(key, value, transaction);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertPessimistic(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  MY_LOGI("Insert %ld pessimistic", key.ToString());
  bool root_locked = false;
  auto page = FindLeafPage(key, OpeType::INSERT, false, root_locked, transaction);

  LeafPage *leaf_node = CastLeafPage(page);

  if (IsSafe(leaf_node, OpeType::INSERT)) {
    assert(!root_locked);
    bool inserted = leaf_node->Insert(key, value, comparator_);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), inserted);
    // buffer_pool_manager_->CheckPinCount();
    return inserted;
  }

  // 直接插入，空间是足够的
  bool inserted = leaf_node->Insert(key, value, comparator_);
  // 插入失败，重复了
  if (!inserted) {
    ClearAndUnlock(transaction, root_locked);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    assert(!root_locked);
    return false;
  }

  int right_page_id;
  Page *right_page = NewLeafPage(&right_page_id, leaf_node->GetParentPageId());
  LeafPage *right_node = CastLeafPage(right_page);

  leaf_node->Split(right_node);
  right_node->SetNextPageId(leaf_node->GetNextPageId());
  leaf_node->SetNextPageId(right_node->GetNextPageId());

  if (right_most_ == leaf_node->GetPageId()) {
    right_most_ = right_page_id;
  }

  KeyType key0 = right_node->KeyAt(0);
  InsertInParent(leaf_node, right_node, key0, right_page_id, transaction);

  ClearAndUnlock(transaction, root_locked);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(right_page->GetPageId(), true);
  // buffer_pool_manager_->CheckPinCount();
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertInParent(BPlusTreePage *left_node, BPlusTreePage *right_node, const KeyType &key,
                                    page_id_t value, Transaction *transaction) {
  if (left_node->IsRootPage()) {
    Page *root_page{};

    root_page = NewInternalRootPage(&root_page_id_);
    InternalPage *root_node = CastInternalPage(root_page);

    root_node->SetValueAt(0, left_node->GetPageId());
    root_node->SetKeyAt(1, key);
    root_node->SetValueAt(1, right_node->GetPageId());
    root_node->SetSize(2);

    left_node->SetParentPageId(root_page_id_);
    right_node->SetParentPageId(root_page_id_);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    return;
  }

  assert(transaction->GetPageSet()->empty()
             ? true
             : transaction->GetPageSet()->back()->GetPageId() == left_node->GetParentPageId());

  // 有internal parent
  Page *parent_page = buffer_pool_manager_->FetchPage(left_node->GetParentPageId());
  InternalPage *parent_node = CastInternalPage(parent_page);
  transaction->GetPageSet()->pop_back();

  assert(parent_node->GetSize() <=parent_node->GetMaxSize());

  // parent有空位
  if (IsSafe(parent_node, OpeType::INSERT)) {
    int index = parent_node->LowerBound(key, comparator_);
    assert(index > 0);
    parent_node->InsertAt(index, key, right_node->GetPageId());
    // LOG_DEBUG("leaf page %d split parent_page id %d at index %d ", left_page->GetPageId(), parent_page->GetPageId(),
    //  index);
  } else {
    // split Internal parent node
    int parent_right_page_id;
    Page *parent_right_page = NewInternalPage(&parent_right_page_id, parent_node->GetParentPageId());
    InternalPage *parent_right_node = CastInternalPage(parent_right_page);

    parent_node->Split(parent_right_node, key, right_node->GetPageId(), comparator_);
    UpdateChild(parent_right_node, 0, parent_right_node->GetSize());

    KeyType key0 = parent_right_node->KeyAt(0);
    parent_right_node->SetKeyAt(0, KeyType{});

    InsertInParent(parent_node, parent_right_node, key0, parent_right_node->GetPageId(), transaction);

    buffer_pool_manager_->UnpinPage(parent_right_page->GetPageId(), true);
  }

  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
  transaction->AddIntoPageSet(parent_page);
}
/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
#ifndef PESSIMISTIC
    // LOG_DEBUG("\033[1;32mRemove %ld\033[0m\n", key.ToString());
    bool root_locked = false;
    auto page = FindLeafPage(key, OpeType::REMOVE, true, root_locked, transaction);

    if (page == nullptr) {
      assert(!root_locked);
      return;   // tree empty
    }

    LeafPage *leaf_node = CastLeafPage(page);

    if (IsSafe(leaf_node, OpeType::REMOVE)){
      bool removed = leaf_node->Remove(key, comparator_);
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), removed);
      UnlockRoot(root_locked);
      return;
    }

    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    UnlockRoot(root_locked);
#endif
    RemovePessimistic(key, transaction);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::RemovePessimistic(const KeyType &key, Transaction *transaction) {

}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RootMutex::Latch() {
  latch_.lock();
  MY_LOGR(BRED("latch lock"));
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RootMutex::UnLatch() {
  MY_LOGR(BGREEN("latch unlock"));
  latch_.unlock();
}
/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return 0; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
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
