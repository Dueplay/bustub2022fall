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
  SetSize(1);  // 第一个pair的key为空表示[-∞,key1)
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) { array_[index].second = value; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::SearchJumpIdx(const KeyType &key, KeyComparator &comparator) -> int {
  // find smallest i s.t. key <= array[i].key
  auto bigger_or_equal_key_idx = -1;
  auto left = 1;
  auto right = GetSize() - 1;
  while (left <= right) {
    auto mid = (left + right) / 2;
    // mid's key >= key
    if (comparator(KeyAt(mid), key) >= 0) {
      bigger_or_equal_key_idx = mid;
      right = mid - 1;
    } else {
      left = mid + 1;
    }
  }

  auto jump_idx = -1;
  if (bigger_or_equal_key_idx == -1) {
    // key 比array中的所有key都要大,应该在最后一个key对应的指针指向的page中
    jump_idx = GetSize() - 1;
  } else {
    if (comparator(key, KeyAt(bigger_or_equal_key_idx)) == 0) {
      // 等于，则在这个Index对应的指针指向的page
      jump_idx = bigger_or_equal_key_idx;
    } else {
      // key < bigger_or_equal_key_idx,所以在左边key的指针指向的page里
      jump_idx = bigger_or_equal_key_idx - 1;
    }
  }

  return jump_idx;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::SearchPage(const KeyType &key, KeyComparator &comparator) -> ValueType {
  return ValueAt(SearchJumpIdx(key, comparator));
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, KeyComparator &comparator)
    -> bool {
  auto insert_idx = GetSize();
  auto left = 1;
  auto right = GetSize() - 1;
  while (left <= right) {
    auto mid = (left + right) / 2;
    auto comp_res = comparator(key, KeyAt(mid));
    if (comp_res == 0) {
      // 已经存在这个key，不允许有duplicate key
      return false;
    }
    if (comp_res < 0) {
      // key < mid , mid might be the insert place
      insert_idx = mid;
      right = mid - 1;
    } else {
      left = mid + 1;
    }
  }
  ExcavateIndex(insert_idx);
  SetKeyAt(insert_idx, key);
  SetValueAt(insert_idx, value);
  IncreaseSize(1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::FindKeyPosition(const KeyType &key, KeyComparator &comparator) -> int {
  auto left = 1;
  auto right = GetSize() - 1;
  while (left <= right) {
    auto mid = (left + right) / 2;
    auto comp_res = comparator(key, KeyAt(mid));
    if (comp_res == 0) {
      return mid;
    }
    if (comp_res < 0) {
      right = mid - 1;
    } else {
      left = mid + 1;
    }
  }
  return -1;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveKey(const KeyType &key, KeyComparator &comparator) -> bool {
  auto to_delete_idx = FindKeyPosition(key, comparator);
  if (to_delete_idx == -1) {
    return false;
  }
  FillIndex(to_delete_idx + 1);
  DecreaseSize(1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient) {
  auto recipient_size = recipient->GetSize();
  auto size = GetSize();
  std::copy(&array_[0], &array_[size], &recipient->array_[recipient_size]);
  SetSize(0);
  recipient->IncreaseSize(size);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLatterHalfWithOneExtraTo(BPlusTreeInternalPage *recipient, const KeyType &key,
                                                                  const ValueType &value, KeyComparator &comparator) {
  BUSTUB_ASSERT(GetSize() == GetMaxSize(), "MoveLatterHalfWithOneExtraTo(): Assert GetSize() == GetMaxSize()");
  auto total_size = GetSize() + 1;
  Insert(key, value, comparator);
  auto remain_size = total_size / 2 + (total_size % 2 != 0);
  auto move_size = total_size - remain_size;
  std::copy(&array_[remain_size], &array_[total_size], recipient->array_);
  SetSize(remain_size);
  recipient->SetSize(move_size);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient) {
  auto recipient_size = recipient->GetSize();
  recipient->SetKeyAt(recipient_size, KeyAt(0));
  recipient->SetValueAt(recipient_size, ValueAt(0));
  recipient->IncreaseSize(1);
  FillIndex(1);
  DecreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient) {
  auto size = GetSize();
  recipient->ExcavateIndex(0);
  recipient->SetKeyAt(0, KeyAt(size - 1));
  recipient->SetValueAt(0, ValueAt(size - 1));
  recipient->IncreaseSize(1);
  DecreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetMappingSize() -> size_t { return sizeof(MappingType); }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetArray() -> char * { return reinterpret_cast<char *>(&array_[0]); }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::ExcavateIndex(int index) {
  std::copy_backward(array_ + index, array_ + GetSize(), array_ + GetSize() + 1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::FillIndex(int index) {
  std::copy(array_ + index, array_ + GetSize(), array_ + index - 1);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
