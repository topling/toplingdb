//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <set>

#include "table/internal_iterator.h"
#include "test_util/sync_point.h"

#if defined(_MSC_VER) || defined(__clang__)
#else
  #pragma GCC diagnostic push
  #pragma GCC diagnostic ignored "-Wpmf-conversions"
#endif

namespace ROCKSDB_NAMESPACE {

// A internal wrapper class with an interface similar to Iterator that caches
// the valid() and key() results for an underlying iterator.
// This can help avoid virtual function calls and also gives better
// cache locality.
template <class TValue = Slice>
class IteratorWrapperBase {
 public:
  IteratorWrapperBase() : iter_(nullptr) {}
  explicit IteratorWrapperBase(InternalIteratorBase<TValue>* _iter)
      : iter_(nullptr) {
    Set(_iter);
  }
  ~IteratorWrapperBase() {}
  InternalIteratorBase<TValue>* iter() const { return iter_; }

  // Set the underlying Iterator to _iter and return
  // previous underlying Iterator.
  InternalIteratorBase<TValue>* Set(InternalIteratorBase<TValue>* _iter) {
    InternalIteratorBase<TValue>* old_iter = iter_;

    iter_ = _iter;
    if (iter_ == nullptr) {
      result_.is_valid = false;
    } else {
     #if defined(_MSC_VER) || defined(__clang__)
     #else
      next_and_get_result_ = (NextAndGetResultFN)
          (_iter->*(&InternalIteratorBase<TValue>::NextAndGetResult));
      prepare_and_get_value_ = (PrepareAndGetValueFN)
          (_iter->*(&InternalIteratorBase<TValue>::PrepareAndGetValue));
     #endif
      Update();
    }
    return old_iter;
  }

  void DeleteIter(bool is_arena_mode) {
    if (iter_) {
      if (!is_arena_mode) {
        delete iter_;
      } else {
        iter_->~InternalIteratorBase<TValue>();
      }
    }
  }

  // Iterator interface methods
  bool Valid() const { return result_.is_valid; }
  Slice key() const {
    assert(Valid());
    return result_.key();
  }
  TValue value() const {
    assert(Valid());
    return iter_->value();
  }
  // Methods below require iter() != nullptr
  Status status() const {
    assert(iter_);
    return iter_->status();
  }

#ifdef __GNUC__
  inline __attribute__((always_inline))
#endif
  bool PrepareValue() {
    assert(Valid());
    if (result_.value_prepared) {
      return true;
    }
    if (iter_->PrepareValue()) {
      result_.value_prepared = true;
      return true;
    }

    assert(!iter_->Valid());
    result_.is_valid = false;
    return false;
  }
#ifdef __GNUC__
  inline __attribute__((always_inline))
#endif
  bool PrepareAndGetValue(TValue* v) {
    assert(Valid());
    /* ignore result_.value_prepared
    if (result_.value_prepared) {
      *v = iter_->value();
      return true;
    }
    */
    //return result_.value_prepared = iter_->PrepareAndGetValue(v);
   #if defined(_MSC_VER) || defined(__clang__)
    return iter_->PrepareAndGetValue(v); // do minimal work
   #else
    return prepare_and_get_value_(iter_, v);
   #endif
  }
#ifdef __GNUC__
  inline __attribute__((always_inline))
#endif
  bool Next() {
    assert(iter_);
#if defined(_MSC_VER) || defined(__clang__)
    const bool is_valid = iter_->NextAndGetResult(&result_);
#else
    const bool is_valid = next_and_get_result_(iter_, &result_);
#endif
    assert(is_valid == result_.is_valid);
    assert(!result_.is_valid || iter_->status().ok());
    return is_valid;
  }
/*
#ifdef __GNUC__
  inline __attribute__((always_inline))
#endif
  bool NextAndGetResult(IterateResult* result) {
    assert(iter_);
    const bool is_valid = iter_->NextAndGetResult(&result_);
    assert(is_valid == result_.is_valid);
    *result = result_;
    assert(!result_.is_valid || iter_->status().ok());
    return result_.is_valid;
  }
*/
  void Prev() {
    assert(iter_);
    UpdateImpl(iter_->PrevAndCheckValid());
  }
  void Seek(const Slice& k) {
    assert(iter_);
    iter_->Seek(k);
    Update();
  }
  void SeekForPrev(const Slice& k) {
    assert(iter_);
    iter_->SeekForPrev(k);
    Update();
  }
  void SeekToFirst() {
    assert(iter_);
    iter_->SeekToFirst();
    Update();
  }
  void SeekToLast() {
    assert(iter_);
    iter_->SeekToLast();
    Update();
  }

  bool MayBeOutOfLowerBound() {
    assert(Valid());
    return iter_->MayBeOutOfLowerBound();
  }

  IterBoundCheck UpperBoundCheckResult() {
    assert(Valid());
    return result_.bound_check_result;
  }

  void SetPinnedItersMgr(PinnedIteratorsManager* pinned_iters_mgr) {
    assert(iter_);
    iter_->SetPinnedItersMgr(pinned_iters_mgr);
  }
  bool IsKeyPinned() const {
    assert(Valid());
    return iter_->IsKeyPinned();
  }
  bool IsValuePinned() const {
    assert(Valid());
    return iter_->IsValuePinned();
  }

  bool IsValuePrepared() const { return result_.value_prepared; }

  Slice user_key() const {
    assert(Valid());
    return result_.user_key();
  }

  void UpdateReadaheadState(InternalIteratorBase<TValue>* old_iter) {
    if (old_iter && iter_) {
      ReadaheadFileInfo readahead_file_info;
      old_iter->GetReadaheadState(&readahead_file_info);
      iter_->SetReadaheadState(&readahead_file_info);
    }
  }

  bool IsDeleteRangeSentinelKey() const {
    return iter_->IsDeleteRangeSentinelKey();
  }

 protected:
  void Update() {
    UpdateImpl(iter_->Valid());
  }
  void UpdateImpl(bool is_valid) {
    result_.is_valid = is_valid;
    if (result_.is_valid) {
      assert(iter_->status().ok());
      result_.SetKey(iter_->key());
      result_.bound_check_result = IterBoundCheck::kUnknown;
      result_.value_prepared = false;
    }
  }

  InternalIteratorBase<TValue>* iter_;
  IterateResult result_;
#if defined(_MSC_VER) || defined(__clang__)
#else
  typedef bool (*NextAndGetResultFN)(InternalIteratorBase<TValue>*, IterateResult*);
  typedef bool (*PrepareAndGetValueFN)(InternalIteratorBase<TValue>*, TValue*);
  NextAndGetResultFN next_and_get_result_ = nullptr;
  PrepareAndGetValueFN prepare_and_get_value_ = nullptr;
#endif
};

template <class TValue = Slice>
class ThinIteratorWrapperBase {
 public:
  ThinIteratorWrapperBase() : iter_(nullptr) {}
  explicit ThinIteratorWrapperBase(InternalIteratorBase<TValue>* i) { Set(i); }
  InternalIteratorBase<TValue>* iter() const { return iter_; }

  InternalIteratorBase<TValue>* Set(InternalIteratorBase<TValue>* i) {
    auto old_iter = iter_;
    iter_ = i;
    if (i) {
     #if defined(_MSC_VER) || defined(__clang__)
     #else
      next_and_get_result_ = (NextAndGetResultFN)
          (i->*(&InternalIteratorBase<TValue>::NextAndGetResult));
      prepare_and_get_value_ = (PrepareAndGetValueFN)
          (i->*(&InternalIteratorBase<TValue>::PrepareAndGetValue));
     #endif
    }
    return old_iter;
  }

  void DeleteIter(bool is_arena_mode) {
    if (iter_) {
      if (!is_arena_mode) {
        delete iter_;
      } else {
        iter_->~InternalIteratorBase();
      }
    }
  }

  // Iterator interface methods
  bool Valid() const { return iter_ && iter_->Valid(); }
  Slice key() const { assert(Valid()); return iter_->key(); }
  TValue value() const { assert(Valid()); return iter_->value(); }

  // Methods below require iter() != nullptr
  Status status() const { assert(iter_); return iter_->status(); }
  bool PrepareValue() { assert(Valid()); return iter_->PrepareValue(); }
  bool PrepareAndGetValue(TValue* v) {
    assert(Valid());
   #if defined(_MSC_VER) || defined(__clang__)
    return iter_->PrepareAndGetValue(v);
   #else
    return prepare_and_get_value_(iter_, v);
   #endif
  }
  void Next() { assert(Valid()); iter_->Next(); }
  bool NextAndGetResult(IterateResult* r) {
    assert(iter_);
   #if defined(_MSC_VER) || defined(__clang__)
    return iter_->NextAndGetResult(r);
   #else
    return next_and_get_result_(iter_, r);
   #endif
  }
  void Prev() { assert(iter_); iter_->Prev(); }
  void Seek(const Slice& k) { assert(iter_); iter_->Seek(k); }
  void SeekForPrev(const Slice& k) { assert(iter_); iter_->SeekForPrev(k); }
  void SeekToFirst() { assert(iter_); iter_->SeekToFirst(); }
  void SeekToLast() { assert(iter_); iter_->SeekToLast(); }
  bool MayBeOutOfLowerBound() {
    assert(Valid());
    return iter_->MayBeOutOfLowerBound();
  }
  IterBoundCheck UpperBoundCheckResult() {
    assert(Valid());
    return iter_->UpperBoundCheckResult();
  }
  void SetPinnedItersMgr(PinnedIteratorsManager* pinned_iters_mgr) {
    assert(iter_);
    iter_->SetPinnedItersMgr(pinned_iters_mgr);
  }
  bool IsKeyPinned() const { assert(Valid()); return iter_->IsKeyPinned(); }
  bool IsValuePinned() const { assert(Valid()); return iter_->IsValuePinned(); }
  bool IsValuePrepared() const { return false; }
  Slice user_key() const { assert(Valid()); return iter_->user_key(); }
  void UpdateReadaheadState(InternalIteratorBase<TValue>* old_iter) {
    if (old_iter && iter_) {
      ReadaheadFileInfo readahead_file_info;
      old_iter->GetReadaheadState(&readahead_file_info);
      iter_->SetReadaheadState(&readahead_file_info);
    }
  }
  bool IsDeleteRangeSentinelKey() const {
    return iter_->IsDeleteRangeSentinelKey();
  }
 private:
  InternalIteratorBase<TValue>* iter_;
#if defined(_MSC_VER) || defined(__clang__)
#else
  typedef bool (*NextAndGetResultFN)(InternalIteratorBase<TValue>*, IterateResult*);
  typedef bool (*PrepareAndGetValueFN)(InternalIteratorBase<TValue>*, TValue*);
  NextAndGetResultFN next_and_get_result_ = nullptr;
  PrepareAndGetValueFN prepare_and_get_value_ = nullptr;
#endif
};
using ThinIteratorWrapper = ThinIteratorWrapperBase<Slice>;

using IteratorWrapper = IteratorWrapperBase<Slice>;

class Arena;
// Return an empty iterator (yields nothing) allocated from arena.
template <class TValue = Slice>
extern InternalIteratorBase<TValue>* NewEmptyInternalIterator(Arena* arena);

}  // namespace ROCKSDB_NAMESPACE

#if defined(_MSC_VER) || defined(__clang__)
#else
  #pragma GCC diagnostic pop
#endif
