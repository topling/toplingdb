//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cassert>

#include "rocksdb/comparator.h"
#include "table/internal_iterator.h"

namespace ROCKSDB_NAMESPACE {

// An internal iterator that wraps another one and ensures that any keys
// returned are strictly within a range [start, end). If the underlying
// iterator has already performed the bounds checking, it relies on that result;
// otherwise, it performs the necessary key comparisons itself. Both bounds
// are optional.
template<bool HasBegin, bool HasEnd>
struct ClippingIterBounds;

template<> struct ClippingIterBounds<true, true> {
  Slice m_start, m_end;
  ClippingIterBounds(const Slice* start, const Slice* end)
    : m_start(*start), m_end(*end) {
    assert(nullptr != start);
    assert(nullptr != end);
  }
  const Slice* start_() const { return &m_start; }
  const Slice* end_() const { return &m_end; }
};
template<> struct ClippingIterBounds<true, false> {
  Slice m_start;
  ClippingIterBounds(const Slice* start, const Slice* end)
    : m_start(*start) {
    assert(nullptr != start);
    assert(nullptr == end);
  }
  const Slice* start_() const { return &m_start; }
  const Slice* end_() const { return nullptr; }
};
template<> struct ClippingIterBounds<false, true> {
  Slice m_end;
  ClippingIterBounds(const Slice* start, const Slice* end)
    : m_end(*end) {
    assert(nullptr == start);
    assert(nullptr != end);
  }
  const Slice* start_() const { return nullptr; }
  const Slice* end_() const { return &m_end; }
};

template<bool HasBegin, bool HasEnd, class LessCMP>
class ClippingIterator final : public InternalIterator, ClippingIterBounds<HasBegin, HasEnd>, LessCMP {
  using bounds = ClippingIterBounds<HasBegin, HasEnd>;
  using bounds::start_;
  using bounds::end_;
  bool less(const Slice& x, const Slice& y) const {
    return static_cast<const LessCMP&>(*this)(x, y);
  }
 public:
  ClippingIterator(InternalIterator* iter, const Slice* start, const Slice* end,
                   const LessCMP& cmp)
      : bounds(start, end), LessCMP(cmp), iter_(iter), valid_(false) {
    assert(iter_);
    assert(!start || !end || !less(*end, *start));

    UpdateAndEnforceBounds();
  }

  bool Valid() const override { return valid_; }

  void SeekToFirst() override {
    if (start_()) {
      iter_->Seek(*start_());
    } else {
      iter_->SeekToFirst();
    }

    UpdateValid();
    UpdateAndEnforceUpperBound();
  }

  void SeekToLast() override {
    if (end_()) {
      iter_->SeekForPrev(*end_());

      // Upper bound is exclusive, so we need a key which is strictly smaller
      if (iter_->Valid() && !less(iter_->key(), *end_())) {
        iter_->Prev();
      }
    } else {
      iter_->SeekToLast();
    }

    UpdateValid();
    UpdateAndEnforceLowerBound();
  }

  void Seek(const Slice& target) override {
    if (start_() && less(target, *start_())) {
      iter_->Seek(*start_());
      UpdateValid();
      UpdateAndEnforceUpperBound();
      return;
    }

    if (end_() && !less(target, *end_())) {
      valid_ = false;
      return;
    }

    iter_->Seek(target);
    UpdateValid();
    UpdateAndEnforceUpperBound();
  }

  void SeekForPrev(const Slice& target) override {
    if (start_() && less(target, *start_())) {
      valid_ = false;
      return;
    }

    if (end_() && !less(target, *end_())) {
      iter_->SeekForPrev(*end_());

      // Upper bound is exclusive, so we need a key which is strictly smaller
      if (iter_->Valid() && !less(iter_->key(), *end_())) {
        iter_->Prev();
      }

      UpdateValid();
      UpdateAndEnforceLowerBound();
      return;
    }

    iter_->SeekForPrev(target);
    UpdateValid();
    UpdateAndEnforceLowerBound();
  }

  void Next() override {
    assert(valid_);
    valid_ = iter_->NextAndCheckValid();
    UpdateAndEnforceUpperBound();
  }

  bool NextAndGetResult(IterateResult* result) override {
    assert(valid_);
    assert(result);

    valid_ = iter_->NextAndGetResult(result);

    if (UNLIKELY(!valid_)) {
      return false;
    }

    if (end_()) {
      EnforceUpperBoundImpl(result->bound_check_result);
      result->is_valid = valid_;
      if (!valid_) {
        return false;
      }
    }

    result->bound_check_result = IterBoundCheck::kInbound;

    return true;
  }

  void Prev() override {
    assert(valid_);
    valid_ = iter_->PrevAndCheckValid();
    UpdateAndEnforceLowerBound();
  }

  Slice key() const override {
    assert(valid_);
    return iter_->key();
  }

  Slice user_key() const override {
    assert(valid_);
    return iter_->user_key();
  }

  Slice value() const override {
    assert(valid_);
    return iter_->value();
  }

  Status status() const override { return iter_->status(); }

  bool PrepareValue() override {
    assert(valid_);

    if (iter_->PrepareValue()) {
      return true;
    }

    assert(!iter_->Valid());
    valid_ = false;
    return false;
  }

  bool MayBeOutOfLowerBound() override {
    assert(valid_);
    return false;
  }

  IterBoundCheck UpperBoundCheckResult() override {
    assert(valid_);
    return IterBoundCheck::kInbound;
  }

  void SetPinnedItersMgr(PinnedIteratorsManager* pinned_iters_mgr) override {
    iter_->SetPinnedItersMgr(pinned_iters_mgr);
  }

  bool IsKeyPinned() const override {
    assert(valid_);
    return iter_->IsKeyPinned();
  }

  bool IsValuePinned() const override {
    assert(valid_);
    return iter_->IsValuePinned();
  }

  Status GetProperty(std::string prop_name, std::string* prop) override {
    return iter_->GetProperty(prop_name, prop);
  }

  bool IsDeleteRangeSentinelKey() const override {
    assert(valid_);
    return iter_->IsDeleteRangeSentinelKey();
  }

 private:
  void UpdateValid() {
    assert(!iter_->Valid() || iter_->status().ok());

    valid_ = iter_->Valid();
  }

  void EnforceUpperBoundImpl(IterBoundCheck bound_check_result) {
    if (UNLIKELY(bound_check_result == IterBoundCheck::kInbound)) {
      return;
    }

    if (UNLIKELY(bound_check_result == IterBoundCheck::kOutOfBound)) {
      valid_ = false;
      return;
    }

    assert(bound_check_result == IterBoundCheck::kUnknown);

    if (!less(key(), *end_())) {
      valid_ = false;
    }
  }

  void EnforceUpperBound() {
    if (!valid_) {
      return;
    }

    if (!end_()) {
      return;
    }

    EnforceUpperBoundImpl(iter_->UpperBoundCheckResult());
  }

  void EnforceLowerBound() {
    if (!valid_) {
      return;
    }

    if (!start_()) {
      return;
    }

    if (!iter_->MayBeOutOfLowerBound()) {
      return;
    }

    if (less(key(), *start_())) {
      valid_ = false;
    }
  }

  void AssertBounds() {
    assert(!valid_ || !start_() || !less(key(), *start_()));
    assert(!valid_ || !end_() || less(key(), *end_()));
  }

  void UpdateAndEnforceBounds() {
    UpdateValid();
    EnforceUpperBound();
    EnforceLowerBound();
    AssertBounds();
  }

  void UpdateAndEnforceUpperBound() {
    EnforceUpperBound();
    AssertBounds();
  }

  void UpdateAndEnforceLowerBound() {
    EnforceLowerBound();
    AssertBounds();
  }

  InternalIterator* iter_;
  bool valid_;
};

template<class LessCMP>
std::unique_ptr<InternalIterator>
MakeClippingIteratorAux(InternalIterator* iter,
                        const Slice* start, const Slice* end, LessCMP cmp) {
  if (nullptr == start)
    return std::make_unique<ClippingIterator<false, true, LessCMP> >(iter, start, end, cmp);
  else if (nullptr == end)
    return std::make_unique<ClippingIterator<true, false, LessCMP> >(iter, start, end, cmp);
  else
    return std::make_unique<ClippingIterator<true,  true, LessCMP> >(iter, start, end, cmp);
}

inline
std::unique_ptr<InternalIterator>
MakeClippingIterator(InternalIterator* iter,
                     const Slice* start, const Slice* end,
                     const InternalKeyComparator* cmp) {
  if (cmp->IsForwardBytewise())
    return MakeClippingIteratorAux<BytewiseCompareInternalKey>(iter, start, end, {});
  else if (cmp->IsReverseBytewise())
    return MakeClippingIteratorAux<RevBytewiseCompareInternalKey>(iter, start, end, {});
  else
    return MakeClippingIteratorAux<FallbackVirtCmp>(iter, start, end, {cmp});
}

inline
std::unique_ptr<InternalIterator>
MakeClippingIterator(InternalIterator* iter,
                     const Slice* start, const Slice* end,
                     const Comparator* cmp) {
  if (cmp->IsForwardBytewise())
    return MakeClippingIteratorAux<ForwardBytewiseLessUserKey>(iter, start, end, {});
  else if (cmp->IsReverseBytewise())
    return MakeClippingIteratorAux<ReverseBytewiseLessUserKey>(iter, start, end, {});
  else
    return MakeClippingIteratorAux<VirtualFunctionLessUserKey>(iter, start, end, {cmp});
}

}  // namespace ROCKSDB_NAMESPACE
