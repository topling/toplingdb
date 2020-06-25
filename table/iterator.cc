//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/dbformat.h"
#include "rocksdb/iterator.h"
#include "table/internal_iterator.h"
#include "table/iterator_wrapper.h"
#include "util/arena.h"

namespace rocksdb {

// range_set are sorted
// for (int i = 0; i < size / 2; ++i)
//   range_set[2 * i] .. range_set[2 * i + 1] is closed interval
// all ranges non overlap
class RangeWrappedInternalIterator : public InternalIterator {
public:
  RangeWrappedInternalIterator(
      InternalIterator* iter,
      const InternalKeyComparator& internal_key_comp,
      const std::vector<InternalKey>* range_set)
      : iter_(iter),
        ic_(internal_key_comp),
        range_set_(range_set),
        smallest_(nullptr),
        largest_(nullptr),
        invalid_(false) {
    assert(iter_);
    assert(range_set_->size() >= 2);
  }

  bool Valid() const override final {
    return !invalid_ && iter_->Valid();
  }
  void SeekToFirst() override final {
    Seek(range_set_->front().Encode());
  }
  void SeekToLast() override final {
    SeekForPrev(range_set_->back().Encode());
  }
  void Seek(const Slice& target) override final {
    Slice s = target;
    while (true) {
      iter_->Seek(s);
      if (!iter_->Valid()) {
        break;
      }
      auto find = std::upper_bound(
          range_set_->begin(), range_set_->end(),
          iter_->key(), [this](const Slice& l, const InternalKey& r) {
                            return ic_.Compare(l, r.Encode()) < 0;
                        });
      if ((find - range_set_->begin()) % 2 == 1) {
        largest_ = &*find;
        smallest_ = largest_ - 1;
        invalid_ = false;
        break;
      }
      if (find == range_set_->begin()) {
        SeekToFirst();
        break;
      }
      largest_ = &find[-1];
      if (iter_->key() == largest_->Encode()) {
        smallest_ = largest_ - 1;
        invalid_ = false;
        break;
      }
      if (find == range_set_->end()) {
        invalid_ = true;
        break;
      }
      s = largest_[1].Encode();
    }
    assert(!Valid() ||
           (ic_.Compare(smallest_->Encode(), iter_->key()) <= 0 &&
            ic_.Compare(largest_->Encode(), iter_->key()) >= 0));
  }
  void SeekForPrev(const Slice& target) override final {
    Slice s = target;
    while (true) {
      iter_->SeekForPrev(s);
      if (!iter_->Valid()) {
        break;
      }
      auto find = std::upper_bound(
          range_set_->rbegin(), range_set_->rend(),
          iter_->key(), [this](const Slice& l, const InternalKey& r) {
                            return ic_.Compare(l, r.Encode()) > 0;
                        });
      if ((find - range_set_->rbegin()) % 2 == 1) {
        smallest_ = &*find;
        largest_ = smallest_ + 1;
        invalid_ = false;
        break;
      }
      if (find == range_set_->rbegin()) {
        SeekToLast();
        break;
      }
      smallest_ = &find[-1];
      if (iter_->key() == smallest_->Encode()) {
        largest_ = smallest_ + 1;
        invalid_ = false;
        break;
      }
      if (find == range_set_->rend()) {
        invalid_ = true;
        break;
      }
      s = smallest_[-1].Encode();
    }
    assert(!Valid() ||
           (ic_.Compare(smallest_->Encode(), iter_->key()) <= 0 &&
            ic_.Compare(largest_->Encode(), iter_->key()) >= 0));
  }
  void Next() override final {
    assert(!invalid_);
    iter_->Next();
    if (iter_->Valid() && ic_.Compare(iter_->key(), largest_->Encode()) > 0) {
      if (largest_ == &range_set_->back()) {
        invalid_ = true;
      } else {
        Seek(smallest_[2].Encode());
      }
    }
  }
  void Prev() override final {
    assert(!invalid_);
    iter_->Prev();
    if (iter_->Valid() && ic_.Compare(iter_->key(), smallest_->Encode()) < 0) {
      if (smallest_ == &range_set_->front()) {
        invalid_ = true;
      } else {
        SeekForPrev(largest_[-2].Encode());
      }
    }
  }
  Slice key() const override final {
    assert(!invalid_);
    return iter_->key();
  }
  Slice value() const override final {
    assert(!invalid_);
    return iter_->value();
  }
  Status status() const override final {
    return iter_->status();
  }
  void SetPinnedItersMgr(PinnedIteratorsManager* pinned_iters_mgr) override final {
    iter_->SetPinnedItersMgr(pinned_iters_mgr);
  }
  bool IsKeyPinned() const override final {
    return iter_->IsKeyPinned();
  }
  bool IsValuePinned() const override final {
    return iter_->IsValuePinned();
  }
  Status GetProperty(std::string prop_name, std::string* prop) override final {
    return iter_->GetProperty(prop_name, prop);
  }

private:
  InternalIterator * iter_;
  const InternalKeyComparator& ic_;
  const std::vector<InternalKey>* range_set_;
  const InternalKey* smallest_;
  const InternalKey* largest_;
  bool invalid_;
};

Cleanable::Cleanable() {
  cleanup_.function = nullptr;
  cleanup_.next = nullptr;
}

Cleanable::~Cleanable() { DoCleanup(); }

Cleanable::Cleanable(Cleanable&& other) {
  *this = std::move(other);
}

Cleanable& Cleanable::operator=(Cleanable&& other) {
  if (this != &other) {
    cleanup_ = other.cleanup_;
    other.cleanup_.function = nullptr;
    other.cleanup_.next = nullptr;
  }
  return *this;
}

// If the entire linked list was on heap we could have simply add attach one
// link list to another. However the head is an embeded object to avoid the cost
// of creating objects for most of the use cases when the Cleanable has only one
// Cleanup to do. We could put evernything on heap if benchmarks show no
// negative impact on performance.
// Also we need to iterate on the linked list since there is no pointer to the
// tail. We can add the tail pointer but maintainin it might negatively impact
// the perforamnce for the common case of one cleanup where tail pointer is not
// needed. Again benchmarks could clarify that.
// Even without a tail pointer we could iterate on the list, find the tail, and
// have only that node updated without the need to insert the Cleanups one by
// one. This however would be redundant when the source Cleanable has one or a
// few Cleanups which is the case most of the time.
// TODO(myabandeh): if the list is too long we should maintain a tail pointer
// and have the entire list (minus the head that has to be inserted separately)
// merged with the target linked list at once.
void Cleanable::DelegateCleanupsTo(Cleanable* other) {
  assert(other != nullptr);
  if (cleanup_.function == nullptr) {
    return;
  }
  Cleanup* c = &cleanup_;
  other->RegisterCleanup(c->function, c->arg1, c->arg2);
  c = c->next;
  while (c != nullptr) {
    Cleanup* next = c->next;
    other->RegisterCleanup(c);
    c = next;
  }
  cleanup_.function = nullptr;
  cleanup_.next = nullptr;
}

void Cleanable::RegisterCleanup(Cleanable::Cleanup* c) {
  assert(c != nullptr);
  if (cleanup_.function == nullptr) {
    cleanup_.function = c->function;
    cleanup_.arg1 = c->arg1;
    cleanup_.arg2 = c->arg2;
    delete c;
  } else {
    c->next = cleanup_.next;
    cleanup_.next = c;
  }
}

void Cleanable::RegisterCleanup(CleanupFunction func, void* arg1, void* arg2) {
  assert(func != nullptr);
  Cleanup* c;
  if (cleanup_.function == nullptr) {
    c = &cleanup_;
  } else {
    c = new Cleanup;
    c->next = cleanup_.next;
    cleanup_.next = c;
  }
  c->function = func;
  c->arg1 = arg1;
  c->arg2 = arg2;
}

Status Iterator::GetProperty(std::string prop_name, std::string* prop) {
  if (prop == nullptr) {
    return Status::InvalidArgument("prop is nullptr");
  }
  if (prop_name == "rocksdb.iterator.is-key-pinned") {
    *prop = "0";
    return Status::OK();
  }
  return Status::InvalidArgument("Undentified property.");
}

namespace {
class EmptyIterator : public Iterator {
 public:
  explicit EmptyIterator(const Status& s) : status_(s) { }
  virtual bool Valid() const override { return false; }
  virtual void Seek(const Slice& target) override {}
  virtual void SeekForPrev(const Slice& target) override {}
  virtual void SeekToFirst() override {}
  virtual void SeekToLast() override {}
  virtual void Next() override { assert(false); }
  virtual void Prev() override { assert(false); }
  Slice key() const override {
    assert(false);
    return Slice();
  }
  Slice value() const override {
    assert(false);
    return Slice();
  }
  virtual Status status() const override { return status_; }

 private:
  Status status_;
};

class EmptyInternalIterator : public SourceInternalIterator {
 public:
  explicit EmptyInternalIterator(const Status& s) : status_(s) {}
  virtual bool Valid() const override { return false; }
  virtual void Seek(const Slice& target) override {}
  virtual void SeekForPrev(const Slice& target) override {}
  virtual void SeekToFirst() override {}
  virtual void SeekToLast() override {}
  virtual void Next() override { assert(false); }
  virtual void Prev() override { assert(false); }
  Slice key() const override {
    assert(false);
    return Slice();
  }
  Slice value() const override {
    assert(false);
    return Slice();
  }
  virtual Status status() const override { return status_; }

 private:
  Status status_;
};
}  // namespace

Iterator* NewEmptyIterator() {
  return new EmptyIterator(Status::OK());
}

Iterator* NewErrorIterator(const Status& status) {
  return new EmptyIterator(status);
}

InternalIterator* NewRangeWrappedInternalIterator(
    InternalIterator* iter, const InternalKeyComparator& ic,
    const std::vector<InternalKey>* range_set, Arena* arena) {
  if (arena == nullptr) {
    return new RangeWrappedInternalIterator(iter, ic, range_set);
  } else {
    auto mem = arena->AllocateAligned(
        sizeof(RangeWrappedInternalIterator));
    return new (mem) RangeWrappedInternalIterator(iter, ic, range_set);
  }
}

SourceInternalIterator* NewEmptyInternalIterator() {
  return new EmptyInternalIterator(Status::OK());
}

SourceInternalIterator* NewEmptyInternalIterator(Arena* arena) {
  if (arena == nullptr) {
    return NewEmptyInternalIterator();
  } else {
    auto mem = arena->AllocateAligned(sizeof(EmptyIterator));
    return new (mem) EmptyInternalIterator(Status::OK());
  }
}

SourceInternalIterator* NewErrorInternalIterator(const Status& status) {
  return new EmptyInternalIterator(status);
}

SourceInternalIterator* NewErrorInternalIterator(const Status& status,
                                                 Arena* arena) {
  if (arena == nullptr) {
    return NewErrorInternalIterator(status);
  } else {
    auto mem = arena->AllocateAligned(sizeof(EmptyIterator));
    return new (mem) EmptyInternalIterator(status);
  }
}

}  // namespace rocksdb
