//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/b_plus_tree_index.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "container/hash/hash_function.h"
#include "storage/index/index.h"

#include "storage/index/generic_key.h"

namespace bustub {

/** We only support index table with one integer key for now in BusTub. Hardcode everything here. */

constexpr static const auto TWO_INTEGER_SIZE_B_TREE = 8;
using IntegerKeyType_BTree = GenericKey<TWO_INTEGER_SIZE_B_TREE>;
using IntegerValueType_BTree = RID;
using IntegerComparatorType_BTree = GenericComparator<TWO_INTEGER_SIZE_B_TREE>;
using IntegerHashFunctionType = HashFunction<IntegerKeyType_BTree>;

}  // namespace bustub
