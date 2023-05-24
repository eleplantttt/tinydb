//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// p0_trie.h
//
// Identification: src/include/primer/p0_trie.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/rwlatch.h"

namespace bustub {

/**
 * TrieNode is a generic container for any node in Trie.
 */
class TrieNode {
 public:
  /**
   * TODO(P0): Add implementation
   *
   * @brief Construct a new Trie Node object with the given key char.
   * is_end_ flag should be initialized to false in this constructor.
   *
   * @param key_char Key character of this trie node
   */
  explicit TrieNode(char key_char) : key_char_(key_char) { is_end_ = false; }  // 没有初始化is_end

  /**
   * TODO(P0): Add implementation
   *
   * @brief Move constructor for trie node object. The unique pointers stored
   * in children_ should be moved from other_trie_node to new trie node.
   *
   * @param other_trie_node Old trie node.
   */
  TrieNode(TrieNode &&other_trie_node) noexcept {
    children_ = std::move(other_trie_node.children_);
    is_end_ = other_trie_node.is_end_;
    key_char_ = other_trie_node.key_char_;
  }

  /**
   * @brief Destroy the TrieNode object.
   */
  virtual ~TrieNode() = default;

  /**
   * TODO(P0): Add implementation
   *
   * @brief Whether this trie node has a child node with specified key char.
   *
   * @param key_char Key char of child node.
   * @return True if this trie node has a child with given key, false otherwise.
   */
  auto HasChild(char key_char) const -> bool { return children_.count(key_char) == 1; }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Whether this trie node has any children at all. This is useful
   * when implementing 'Remove' functionality.
   *
   * @return True if this trie node has any child node, false if it has no child node.
   */
  auto HasChildren() const -> bool { return !children_.empty(); }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Whether this trie node is the ending character of a key string.
   *
   * @return True if is_end_ flag is true, false if is_end_ is false.
   */
  auto IsEndNode() const -> bool { return is_end_; }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Return key char of this trie node.
   *
   * @return key_char_ of this trie node.
   */
  auto GetKeyChar() const -> char { return key_char_; }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Insert a child node for this trie node into children_ map, given the key char and
   * unique_ptr of the child node. If specified key_char already exists in children_,
   * return nullptr. If parameter `child`'s key char is different than parameter
   * `key_char`, return nullptr.
   *
   * Note that parameter `child` is rvalue and should be moved when it is
   * inserted into children_map.
   *
   * The return value is a pointer to unique_ptr because pointer to unique_ptr can access the
   * underlying data without taking ownership of the unique_ptr. Further, we can set the return
   * value to nullptr when error occurs.
   *
   * @param key Key of child node
   * @param child Unique pointer created for the child node. This should be added to children_ map.
   * @return Pointer to unique_ptr of the inserted child node. If insertion fails, return nullptr.
   */
  auto InsertChildNode(char key_char, std::unique_ptr<TrieNode> &&child) -> std::unique_ptr<TrieNode> * {
    if (children_.count(key_char) != 0U) {
      return nullptr;
    }
    if (child->key_char_ != key_char) {  // 判断输入对不对的上key_char
      return nullptr;
    }
    children_[key_char] = std::move(child);
    return &children_[key_char];
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Get the child node given its key char. If child node for given key char does
   * not exist, return nullptr.
   *
   * @param key Key of child node
   * @return Pointer to unique_ptr of the child node, nullptr if child
   *         node does not exist.
   */
  auto GetChildNode(char key_char) -> std::unique_ptr<TrieNode> * {
    if (children_.count(key_char) == 0) {
      return nullptr;
    }
    return &children_[key_char];
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Remove child node from children_ map.
   * If key_char does not exist in children_, return immediately.
   *
   * @param key_char Key char of child node to be removed
   */
  void RemoveChildNode(char key_char) {
    if (children_.count(key_char) == 0) {
      return;
    }
    children_.erase(key_char);
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Set the is_end_ flag to true or false.
   *
   * @param is_end Whether this trie node is ending char of a key string
   */
  void SetEndNode(bool is_end) { is_end_ = is_end; }

 protected:
  /** Key character of this trie node */
  char key_char_;
  /** whether this node marks the end of a key */
  bool is_end_{false};
  /** A map of all child nodes of this trie node, which can be accessed by each
   * child node's key char. */
  std::unordered_map<char, std::unique_ptr<TrieNode>> children_;
};

/**
 * TrieNodeWithValue is a node that marks the ending of a key, and it can
 * hold a value of any type T.
 */
template <typename T>
class TrieNodeWithValue : public TrieNode {
 private:
  /* Value held by this trie node. */
  T value_;

 public:
  /**
   * TODO(P0): Add implementation
   *
   * @brief Construct a new TrieNodeWithValue object from a TrieNode object and specify its value.
   * This is used when a non-terminal TrieNode is converted to terminal TrieNodeWithValue.
   *
   * The children_ map of TrieNode should be moved to the new TrieNodeWithValue object.
   * Since it contains unique pointers, the first parameter is a rvalue reference.
   *
   * You should:
   * 1) invoke TrieNode's move constructor to move data from TrieNode to
   * TrieNodeWithValue.
   * 2) set value_ member variable of this node to parameter `value`.
   * 3) set is_end_ to true
   *
   * @param trieNode TrieNode whose data is to be moved to TrieNodeWithValue
   * @param value
   */
  TrieNodeWithValue(TrieNode &&trieNode, T value) : TrieNode(std::move(trieNode)) {
    value_ = value;
    is_end_ = true;
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Construct a new TrieNodeWithValue. This is used when a new terminal node is constructed.
   *
   * You should:
   * 1) Invoke the constructor for TrieNode with the given key_char.
   * 2) Set value_ for this node.
   * 3) set is_end_ to true.
   *
   * @param key_char Key char of this node
   * @param value Value of this node
   */
  TrieNodeWithValue(char key_char, T value) : TrieNode(key_char) {
    value_ = value;
    is_end_ = true;
  }

  /**
   * @brief Destroy the Trie Node With Value object
   */
  ~TrieNodeWithValue() override = default;

  /**
   * @brief Get the stored value_.
   *
   * @return Value of type T stored in this node
   */
  auto GetValue() const -> T { return value_; }
};

/**
 * Trie is a concurrent key-value store. Each key is a string and its corresponding
 * value can be any type.
 */
class Trie {
 private:
  /* 前缀树的根节点 */
  std::unique_ptr<TrieNode> root_;
  /* 前缀树的读写锁 */
  ReaderWriterLatch latch_;

 public:
  /**
   * TODO(P0): Add implementation
   *
   * @brief Construct a new Trie object. Initialize the root node with '\0'
   * character.
   */
  Trie() { root_ = std::make_unique<TrieNode>(TrieNode('\0')); }  // 智能指针

  /**
   * TODO(P0): Add implementation
   *
   * @brief Insert key-value pair into the trie.
   *
   * If the key is an empty string, return false immediately.
   *
   * If the key already exists, return false. Duplicated keys are not allowed and
   * you should never overwrite value of an existing key.
   *
   * When you reach the ending character of a key:
   * 1. If TrieNode with this ending character does not exist, create new TrieNodeWithValue
   * and add it to parent node's children_ map.
   * 2. If the terminal node is a TrieNode, then convert it into TrieNodeWithValue by
   * invoking the appropriate constructor.
   * 3. If it is already a TrieNodeWithValue,
   * then insertion fails and returns false. Do not overwrite existing data with new data.
   *
   * You can quickly check whether a TrieNode pointer holds TrieNode or TrieNodeWithValue
   * by checking the is_end_ flag. If is_end_ == false, then it points to TrieNode. If
   * is_end_ == true, it points to TrieNodeWithValue.
   *
   * @param key Key used to traverse the trie and find the correct node
   * @param value Value to be inserted
   * @return True if insertion succeeds, false if the key already exists
   * 如果key是空字符串立即返回false
     如果key存在返回false，而且不能重写
     在字符串的最后一个字符时有三种情况：
     1、最后一个字符不存在，则实现一个新的TrieNodeWithValue
     2、如果终止字符是TrieNode，则用移动构造转换为TrieNodeWithValue
     3、如果已经是TrieNodeWithValue，插入失败然后返回false不能重写
   */
  template <typename T>
  auto Insert(const std::string &key, T value) -> bool {
    latch_.WLock();
    std::unique_ptr<TrieNode> *parent;
    if (key.empty()) {
      latch_.WUnlock();
      return false;
    }
    auto p = &this->root_;
    for (auto key_char : key) {
      if (!p->get()->HasChild(key_char)) {
        p->get()->InsertChildNode(key_char, std::make_unique<TrieNode>(key_char));
      }
      parent = p;
      p = p->get()->GetChildNode(key_char);
    }
    if (p->get()->IsEndNode()) {
      latch_.WUnlock();
      return false;  // 3末尾是TrieNodewithvalue 就是有重复的值
    }
    auto temp_node = std::move(*p);
    parent->get()->RemoveChildNode(key.back());
    // 第一种构造TrieNodewithvalue
    auto temp = std::make_unique<TrieNodeWithValue<T>>(std::move(*temp_node), value);

    parent->get()->InsertChildNode(key.back(), std::move(temp));
    latch_.WUnlock();
    return true;
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Remove key value pair from the trie.
   * This function should also remove nodes that are no longer part of another
   * key. If key is empty or not found, return false.
   *
   * You should:
   * 1) Find the terminal node for the given key.
   * 2) If this terminal node does not have any children, remove it from its
   * parent's children_ map.
   * 3) Recursively remove nodes that have no children and are not terminal node
   * of another key.
   * 1) 找到最后的节点
   * 2) 如果这个节点没有孩子则删除
   * 3) 递归的删除没有孩子的节点和不是另一个键的最后节点
   * @param key Key used to traverse the trie and find the correct node
   * @return True if the key exists and is removed, false otherwise
   */
  //  迭代实现
  auto Remove(const std::string &key) -> bool {
    if (key.empty()) {
      return false;
    }
    latch_.WLock();
    auto node = &root_;
    std::vector<std::unique_ptr<TrieNode> *> trav_path;
    for (auto ch : key) {
      trav_path.emplace_back(node);
      auto next = node->get()->GetChildNode(ch);
      if (next == nullptr) {
        latch_.WUnlock();
        return false;
      }
      node = next;
    }

    if (node->get()->HasChildren()) {
      node->get()->SetEndNode(false);
    } else {
      for (int i = trav_path.size() - 1; i >= 0; i--) {
        auto pre = trav_path[i];
        if ((i < static_cast<int>(key.size() - 1)) && (node->get()->IsEndNode() || node->get()->HasChildren())) {
          break;
        }
        pre->get()->RemoveChildNode(key[i]);
        node = pre;
      }
    }
    latch_.WUnlock();
    return true;
  }
  // 递归实现
  // auto Remove(const std::string &key) -> bool {
  //     latch_.WLock();
  //     bool a = true;
  //     bool *success = &a;
  //     Dfs(key, &this->root_, 0, success);
  //     latch_.WUnlock();
  //     return *success;
  // }
  // auto Dfs(const std::string &key, std::unique_ptr<TrieNode> *root, size_t index, bool *success)
  //  -> std::unique_ptr<TrieNode> *{
  //     if(index == key.size()){
  //       if(!root->get()->IsEndNode()){      //没有is_end标志
  //         *success = false;
  //         return nullptr;
  //       }
  //       if(!root->get()->HasChildren()){    //没有孩子节点
  //         *success = true;
  //         return nullptr;
  //       }
  //       *success = true;
  //       root->get()->SetEndNode(false);
  //       return root;
  //     }

  //     std::unique_ptr<TrieNode> *node;
  //     if(root->get()->HasChild(key[index])){
  //         node = Dfs(key, root->get()->GetChildNode(key[index]), index + 1, success);
  //         if (!*success) {
  //             return nullptr;
  //         }
  //         if(node == nullptr) {
  //             root->get()->RemoveChildNode(key[index]);
  //             if (!root->get()->IsEndNode() && !root->get()->HasChildren()) {
  //                 return nullptr;
  //             }
  //         }
  //         return root;
  //     }
  //     *success = false;
  //     return nullptr;
  //  }
  /**
   * TODO(P0): Add implementation
   *
   * @brief Get the corresponding value of type T given its key.
   * If key is empty, set success to false.
   * If key does not exist in trie, set success to false.
   * If the given type T is not the same as the value type stored in TrieNodeWithValue
   * (ie. GetValue<int> is called but terminal node holds std::string),
   * set success to false.
   *
   * To check whether the two types are the same, dynamic_cast
   * the terminal TrieNode to TrieNodeWithValue<T>. If the casted result
   * is not nullptr, then type T is the correct type.
   *
   * @param key Key used to traverse the trie and find the correct node
   * @param success Whether GetValue is successful or not
   * @return Value of type T if type matches
   */
  template <typename T>
  auto GetValue(const std::string &key, bool *success) -> T {
    latch_.RLock();
    if (key.empty()) {
      *success = false;
      latch_.RUnlock();
      return {};
    }
    auto p = &root_;
    for (auto key_char : key) {
      if (p->get()->HasChild(key_char)) {
        p = p->get()->GetChildNode(key_char);
      } else {
        latch_.RUnlock();
        *success = false;
        return {};
      }
    }
    if (!p->get()->IsEndNode()) {
      *success = false;
      latch_.RUnlock();
      return {};
    }
    // 如果是TrieNodeWithValue则成功，不是TrieNodeWithValue而是TrieNode则为下行转换不成功返回nullptr
    auto res = dynamic_cast<TrieNodeWithValue<T> *>(p->get());
    if (res) {
      *success = true;
      latch_.RUnlock();
      return res->GetValue();
    }
    *success = false;
    latch_.RUnlock();
    return {};
  }
};
}  // namespace bustub
