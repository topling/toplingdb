//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <algorithm>
#include <cstdint>
#include <functional>

#include "port/port.h"
#include "port/likely.h"
#include <terark/valvec32.hpp>

namespace ROCKSDB_NAMESPACE {

// Binary heap implementation optimized for use in multi-way merge sort.
// Comparison to std::priority_queue:
// - In libstdc++, std::priority_queue::pop() usually performs just over logN
//   comparisons but never fewer.
// - std::priority_queue does not have a replace-top operation, requiring a
//   pop+push.  If the replacement element is the new top, this requires
//   around 2logN comparisons.
// - This heap's pop() uses a "schoolbook" downheap which requires up to ~2logN
//   comparisons.
// - This heap provides a replace_top() operation which requires [1, 2logN]
//   comparisons.  When the replacement element is also the new top, this
//   takes just 1 or 2 comparisons.
//
// The last property can yield an order-of-magnitude performance improvement
// when merge-sorting real-world non-random data.  If the merge operation is
// likely to take chunks of elements from the same input stream, only 1
// comparison per element is needed.  In RocksDB-land, this happens when
// compacting a database where keys are not randomly distributed across L0
// files but nearby keys are likely to be in the same L0 file.
//
// The container uses the same counterintuitive ordering as
// std::priority_queue: the comparison operator is expected to provide the
// less-than relation, but top() will return the maximum.

template <typename T, typename Compare = std::less<T>>
class BinaryHeap : private Compare {
 public:
  BinaryHeap() {}
  explicit BinaryHeap(Compare cmp) : Compare(std::move(cmp)) {}

  void push(const T& value) {
    data_.push_back(value);
    upheap(data_.size() - 1);
  }

  void push(T&& value) {
    data_.push_back(std::move(value));
    upheap(data_.size() - 1);
  }

  const T& top() const {
    assert(!empty());
    return data_.front();
  }

  T& top() {
    assert(!empty());
    return data_.front();
  }

  void replace_top(const T& value) {
    assert(!empty());
    data_.front() = value;
    downheap(get_root());
  }

  void replace_top(T&& value) {
    assert(!empty());
    data_.front() = std::move(value);
    downheap(get_root());
  }

  void update_top() {
    assert(!empty());
    downheap(get_root());
  }

  void pop() {
    assert(!empty());
    if (data_.size() > 1) {
      // Avoid self-move-assign, because it could cause problems with
      // classes which are not prepared for this and it trips up the
      // STL debugger when activated.
      data_.front() = std::move(data_.back());
    }
    data_.pop_back();
    if (!empty()) {
      downheap(get_root());
    } else {
      reset_root_cmp_cache();
    }
  }

  void swap(BinaryHeap& other) {
    std::swap(static_cast<Compare&>(*this), static_cast<Compare&>(other));
    data_.swap(other.data_);
    std::swap(root_cmp_cache_, other.root_cmp_cache_);
  }

  void clear() {
    data_.resize(0); // do not free memory
    reset_root_cmp_cache();
  }

  void reserve(size_t cap) { data_.reserve(cap); }

  bool empty() const { return data_.empty(); }

  size_t size() const { return data_.size(); }

 private:
  inline Compare& cmp_() { return *this; }
  void reset_root_cmp_cache() {
    root_cmp_cache_ = std::numeric_limits<size_t>::max();
  }
  static inline size_t get_root() { return 0; }
  static inline size_t get_parent(size_t index) { return (index - 1) / 2; }
  static inline size_t get_left(size_t index) { return 2 * index + 1; }
  static inline size_t get_right(size_t index) { return 2 * index + 2; }

  void upheap(size_t index) {
    assert(index < data_.size());
    T* data_ = this->data_.data();
    T v = std::move(data_[index]);
    while (index > get_root()) {
      const size_t parent = get_parent(index);
      if (!cmp_()(data_[parent], v)) {
        break;
      }
      data_[index] = std::move(data_[parent]);
      index = parent;
    }
    data_[index] = std::move(v);
    reset_root_cmp_cache();
  }

  void downheap(size_t index) {
    size_t heap_size = data_.size();
    assert(0 == index); ///< wiered, index must be 0
    if (UNLIKELY(1 >= heap_size)) {
      return;
    }
    T* data_ = this->data_.data();

    size_t picked_child;
    if (root_cmp_cache_ < heap_size) {
      assert(1 == root_cmp_cache_ || 2 == root_cmp_cache_);
      picked_child = root_cmp_cache_;
    } else if (2 < heap_size && cmp_()(data_[1], data_[2])) {
      picked_child = 2;
    } else {
      picked_child = 1;
    }
    if (!cmp_()(data_[0], data_[picked_child])) {
      // the tree does not change anything
      root_cmp_cache_ = picked_child;
      return;
    }

    reset_root_cmp_cache();
    T v = std::move(data_[index]);
    data_[index] = std::move(data_[picked_child]);
    index = picked_child;

    while (1) {
      const size_t left_child = get_left(index);
      if (UNLIKELY(left_child >= heap_size)) {
        break;
      }
      const size_t right_child = left_child + 1;
      assert(right_child == get_right(index));
      if (right_child < heap_size &&
                 cmp_()(data_[left_child], data_[right_child])) {
        picked_child = right_child;
      } else {
        picked_child = left_child;
      }
      if (!cmp_()(v, data_[picked_child])) {
        break;
      }
      data_[index] = std::move(data_[picked_child]);
      index = picked_child;
    }

/*
    if (index == 0) {
      // We did not change anything in the tree except for the value
      // of the root node, left and right child did not change, we can
      // cache that `picked_child` is the smallest child
      // so next time we compare againist it directly
      root_cmp_cache_ = picked_child;
    } else {
      // the tree changed, reset cache
      reset_root_cmp_cache();
    }
*/

    data_[index] = std::move(v);
  }

  terark::valvec32<T> data_;static_assert(std::is_trivially_destructible_v<T>);
  // Used to reduce number of cmp_ calls in downheap()
  size_t root_cmp_cache_ = std::numeric_limits<size_t>::max();
};

}  // namespace ROCKSDB_NAMESPACE
