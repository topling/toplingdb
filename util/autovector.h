//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <algorithm>
#include <cassert>
#include <initializer_list>
#include <iterator>
#include <stdexcept>
#include <vector>

#include "port/lang.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

#if defined(__GNUC__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wuninitialized"
#endif

// A vector that leverages pre-allocated stack-based array to achieve better
// performance for array with small amount of items.
//
// The interface resembles that of vector, but with less features since we aim
// to solve the problem that we have in hand, rather than implementing a
// full-fledged generic container.
//
// Currently we don't support:
//  * shrink_to_fit()
//     If used correctly, in most cases, people should not touch the
//     underlying vector at all.
//  * random insert()/erase(), please only use push_back()/pop_back().
//  * No move/swap operations. Each autovector instance has a
//     stack-allocated array and if we want support move/swap operations, we
//     need to copy the arrays other than just swapping the pointers. In this
//     case we'll just explicitly forbid these operations since they may
//     lead users to make false assumption by thinking they are inexpensive
//     operations.
//
// Naming style of public methods almost follows that of the STL's.
template <class T, size_t kSize = 8>
class autovector {
 public:
  // General STL-style container member types.
  using value_type = T;
  using difference_type = typename std::vector<T>::difference_type;
  using size_type = typename std::vector<T>::size_type;
  using reference = value_type&;
  using const_reference = const value_type&;
  using pointer = value_type*;
  using const_pointer = const value_type*;

  // This class is the base for regular/const iterator
  template <class TAutoVector, class TValueType>
  class iterator_impl {
   public:
    // -- iterator traits
    using self_type = iterator_impl<TAutoVector, TValueType>;
    using value_type = TValueType;
    using reference = TValueType&;
    using pointer = TValueType*;
    using difference_type = typename TAutoVector::difference_type;
    using iterator_category = std::random_access_iterator_tag;

    iterator_impl(TAutoVector* vect, size_t index)
        : vect_(vect), index_(index){};
    iterator_impl(const iterator_impl&) = default;
    ~iterator_impl() {}
    iterator_impl& operator=(const iterator_impl&) = default;

    // -- Advancement
    // ++iterator
    self_type& operator++() {
      ++index_;
      return *this;
    }

    // iterator++
    self_type operator++(int) {
      auto old = *this;
      ++index_;
      return old;
    }

    // --iterator
    self_type& operator--() {
      --index_;
      return *this;
    }

    // iterator--
    self_type operator--(int) {
      auto old = *this;
      --index_;
      return old;
    }

    self_type operator-(difference_type len) const {
      return self_type(vect_, index_ - len);
    }

    difference_type operator-(const self_type& other) const {
      assert(vect_ == other.vect_);
      return index_ - other.index_;
    }

    self_type operator+(difference_type len) const {
      return self_type(vect_, index_ + len);
    }

    self_type& operator+=(difference_type len) {
      index_ += len;
      return *this;
    }

    self_type& operator-=(difference_type len) {
      index_ -= len;
      return *this;
    }

    // -- Reference
    reference operator*() const {
      assert(vect_->size() >= index_);
      return (*vect_)[index_];
    }

    pointer operator->() const {
      assert(vect_->size() >= index_);
      return &(*vect_)[index_];
    }

    reference operator[](difference_type len) const { return *(*this + len); }

    // -- Logical Operators
    bool operator==(const self_type& other) const {
      assert(vect_ == other.vect_);
      return index_ == other.index_;
    }

    bool operator!=(const self_type& other) const { return !(*this == other); }

    bool operator>(const self_type& other) const {
      assert(vect_ == other.vect_);
      return index_ > other.index_;
    }

    bool operator<(const self_type& other) const {
      assert(vect_ == other.vect_);
      return index_ < other.index_;
    }

    bool operator>=(const self_type& other) const {
      assert(vect_ == other.vect_);
      return index_ >= other.index_;
    }

    bool operator<=(const self_type& other) const {
      assert(vect_ == other.vect_);
      return index_ <= other.index_;
    }

   private:
    TAutoVector* vect_ = nullptr;
    size_t index_ = 0;
  };

  using iterator = iterator_impl<autovector, value_type>;
  using const_iterator = iterator_impl<const autovector, const value_type>;
  using reverse_iterator = std::reverse_iterator<iterator>;
  using const_reverse_iterator = std::reverse_iterator<const_iterator>;

  autovector() {}

  autovector(std::initializer_list<T> init_list) {
    this->reserve(init_list.size());
    for (const T& item : init_list) {
      push_back(item);
    }
  }
  explicit autovector(size_t sz) { if (sz) resize(sz); }

  ~autovector() { clear(); }

  // -- Immutable operations
  // Indicate if all data resides in in-stack data structure.
  bool only_in_stack() const {
    // If no element was inserted at all, the vector's capacity will be `0`.
    return vect_.capacity() == 0;
  }

  size_type size() const { return num_stack_items_ + vect_.size(); }
  size_type num_stack_items() const { return num_stack_items_; }

  // resize does not guarantee anything about the contents of the newly
  // available elements
  void resize(size_type n) {
    if (n > kSize) {
      vect_.resize(n - kSize);
      while (num_stack_items_ < kSize) {
        new ((void*)(&values_[num_stack_items_])) value_type();
        num_stack_items_++;  // exception-safe: inc after cons finish
      }
    } else {
      vect_.clear();
      while (num_stack_items_ < n) {
        new ((void*)(&values_[num_stack_items_])) value_type();
        num_stack_items_++;  // exception-safe: inc after cons finish
      }
      while (num_stack_items_ > n) {
        values_[--num_stack_items_].~value_type();
      }
    }
  }

  bool empty() const { return num_stack_items_ == 0; }

  size_type capacity() const { return kSize + vect_.capacity(); }

  void reserve(size_t cap) {
    if (cap > kSize) {
      vect_.reserve(cap - kSize);
    }

    assert(cap <= capacity());
  }

  const_reference operator[](size_type n) const {
    assert(n < size());
    if (n < kSize) {
      return values_[n];
    }
    return vect_[n - kSize];
  }

  reference operator[](size_type n) {
    assert(n < size());
    if (n < kSize) {
      return values_[n];
    }
    return vect_[n - kSize];
  }

  const_reference at(size_type n) const {
    assert(n < size());
    return (*this)[n];
  }

  reference at(size_type n) {
    assert(n < size());
    return (*this)[n];
  }

  reference front() noexcept {
    assert(!empty());
    return values_[0];
  }

  const_reference front() const noexcept {
    assert(!empty());
    return values_[0];
  }

  reference back() noexcept {
    assert(!empty());
    return vect_.empty() ? values_[num_stack_items_-1] : vect_.back();
  }

  const_reference back() const noexcept {
    assert(!empty());
    return vect_.empty() ? values_[num_stack_items_-1] : vect_.back();
  }

  // -- Mutable Operations
  void push_back(T&& item) {
    size_t oldsize = num_stack_items_;
    if (oldsize < kSize) {
      new (&values_[oldsize]) T (std::move(item));
      num_stack_items_ = oldsize + 1;
    } else {
      vect_.push_back(item);
    }
  }

  void push_back(const T& item) {
    size_t oldsize = num_stack_items_;
    if (oldsize < kSize) {
      new (&values_[oldsize]) T (item);
      num_stack_items_ = oldsize + 1;
    } else {
      vect_.push_back(item);
    }
  }

  template <class... Args>
#if _LIBCPP_STD_VER > 14
  reference emplace_back(Args&&... args) {
    size_t oldsize = num_stack_items_;
    if (oldsize < kSize) {
      new ((void*)(&values_[oldsize]))
                   value_type(std::forward<Args>(args)...);
      return values_[oldsize];
    } else {
      return vect_.emplace_back(std::forward<Args>(args)...);
    }
  }
#else
  void emplace_back(Args&&... args) {
    size_t oldsize = num_stack_items_;
    if (oldsize < kSize) {
      new ((void*)(&values_[oldsize])) T (std::forward<Args>(args)...);
      num_stack_items_ = oldsize + 1;
    } else {
      vect_.emplace_back(std::forward<Args>(args)...);
    }
  }
#endif

  void pop_back() {
    assert(!empty());
    if (!vect_.empty()) {
      vect_.pop_back();
    } else {
      values_[--num_stack_items_].~value_type();
    }
  }

  void clear() {
    if (!std::is_trivially_destructible<T>::value) {
      size_t cnt = num_stack_items_;
      while (cnt) {
        values_[--cnt].~value_type();
      }
    }
    num_stack_items_ = 0;
    vect_.clear();
  }

  // -- Copy and Assignment
  autovector& assign(const autovector& other);

  autovector(const autovector& other) : vect_(other.vect_) {
    num_stack_items_ = other.num_stack_items_;
    std::uninitialized_copy_n(other.values_, other.num_stack_items_, values_);
  }

  autovector& operator=(const autovector& other) { return assign(other); }

  autovector(autovector&& other) noexcept : vect_(std::move(other.vect_)) {
    num_stack_items_ = other.num_stack_items_;
    std::uninitialized_move_n(other.values_, other.num_stack_items_, values_);
    other.num_stack_items_ = 0;
  }
  autovector& operator=(autovector&& other) noexcept;

  // -- Iterator Operations
  iterator begin() { return iterator(this, 0); }

  const_iterator begin() const { return const_iterator(this, 0); }

  iterator end() { return iterator(this, this->size()); }

  const_iterator end() const { return const_iterator(this, this->size()); }

  reverse_iterator rbegin() { return reverse_iterator(end()); }

  const_reverse_iterator rbegin() const {
    return const_reverse_iterator(end());
  }

  reverse_iterator rend() { return reverse_iterator(begin()); }

  const_reverse_iterator rend() const {
    return const_reverse_iterator(begin());
  }

  const T& top() const noexcept { return back(); }
  T& top() noexcept { return back(); }
  void pop() { pop_back(); }

 private:
  static void destroy(value_type* p, size_t n) {
    if (!std::is_trivially_destructible<value_type>::value) {
      while (n) p[--n].~value_type();
    }
  }

  // used only if there are more than `kSize` items.
  std::vector<T> vect_;
  size_type num_stack_items_ = 0;  // current number of items
  union {
    value_type values_[kSize];
  };
};

template <class T, size_t kSize>
inline autovector<T, kSize>& autovector<T, kSize>::assign(
    const autovector<T, kSize>& other) {
  // copy the internal vector
  vect_.assign(other.vect_.begin(), other.vect_.end());

  destroy(values_, num_stack_items_);
  // copy array
  num_stack_items_ = other.num_stack_items_;
  std::uninitialized_copy_n(other.values_, num_stack_items_, values_);

  return *this;
}

template <class T, size_t kSize>
inline autovector<T, kSize>& autovector<T, kSize>::operator=(
    autovector<T, kSize>&& other) noexcept {
  vect_ = std::move(other.vect_);
  destroy(values_, num_stack_items_);
  size_t n = other.num_stack_items_;
  num_stack_items_ = n;
  other.num_stack_items_ = 0;
  std::uninitialized_move_n(other.values_, n, values_);
  return *this;
}

#if defined(__GNUC__)
#pragma GCC diagnostic pop
#endif

}  // namespace ROCKSDB_NAMESPACE
