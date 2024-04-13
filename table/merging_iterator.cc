//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/merging_iterator.h"

#include "db/arena_wrapped_db_iter.h"

#if defined(__AVX512VL__) && defined(__AVX512VBMI2__)
  #include <immintrin.h>
#endif

#include <boost/predef/other/endian.h>

#if defined(__clang__)
  #pragma clang diagnostic ignored "-Winconsistent-missing-override"
  #pragma clang diagnostic ignored "-Wshorten-64-to-32"
#endif

namespace ROCKSDB_NAMESPACE {
// MergingIterator uses a min/max heap to combine data from point iterators.
// Range tombstones can be added and keys covered by range tombstones will be
// skipped.
//
// The following are implementation details and can be ignored by user.
// For merging iterator to process range tombstones, it treats the start and end
// keys of a range tombstone as two keys and put them into minHeap_ or maxHeap_
// together with regular point keys. Each range tombstone is active only within
// its internal key range [start_key, end_key). An `active_` set is used to
// track levels that have an active range tombstone. Take forward scanning
// for example. Level j is in active_ if its current range tombstone has its
// start_key popped from minHeap_ and its end_key in minHeap_. If the top of
// minHeap_ is a point key from level L, we can determine if the point key is
// covered by any range tombstone by checking if there is an l <= L in active_.
// The case of l == L also involves checking range tombstone's sequence number.
//
// The following (non-exhaustive) list of invariants are maintained by
// MergingIterator during forward scanning. After each InternalIterator API,
// i.e., Seek*() and Next(), and FindNextVisibleKey(), if minHeap_ is not empty:
// (1) minHeap_.top().type == ITERATOR
// (2) minHeap_.top()->key() is not covered by any range tombstone.
//
// After each call to SeekImpl() in addition to the functions mentioned above:
// (3) For all level i and j <= i, range_tombstone_iters_[j].prev.end_key() <
// children_[i].iter.key(). That is, range_tombstone_iters_[j] is at or before
// the first range tombstone from level j with end_key() >
// children_[i].iter.key().
// (4) For all level i and j <= i, if j in active_, then
// range_tombstone_iters_[j]->start_key() < children_[i].iter.key().
// - When range_tombstone_iters_[j] is !Valid(), we consider its `prev` to be
// the last range tombstone from that range tombstone iterator.
// - When referring to range tombstone start/end keys, assume it is the value of
// HeapItem::tombstone_pik. This value has op_type = kMaxValid, which makes
// range tombstone keys have distinct values from point keys.
//
// Applicable class variables have their own (forward scanning) invariants
// listed in the comments above their definition.

// Represents an element in the min/max heap. Each HeapItem corresponds to a
// point iterator or a range tombstone iterator, differentiated by
// HeapItem::type.
struct HeapItem {
  enum Type : uint8_t { ITERATOR, DELETE_RANGE_START, DELETE_RANGE_END };

  // corresponding range tombstone iterator's start or end key value
  // depending on value of `type`.
  // Will be overwritten before use, initialize here so compiler does not
  // complain.
  union {
    IteratorWrapper   iter;
    ParsedInternalKey tombstone_pik;
  };
  Type type;
  uint32_t level;

  HeapItem() : iter() {
    level = 0;
    type = Type::ITERATOR;
  }

  HeapItem(size_t _level, InternalIteratorBase<Slice>* _iter) : iter() {
    level = uint32_t(_level);
    type = Type::ITERATOR;
    iter.Set(_iter);
  }

  ~HeapItem() {}

  void SetTombstoneKey(ParsedInternalKey&& pik) {
    // op_type is already initialized in MergingIterator::Finish().
    tombstone_pik.user_key = pik.user_key;
    tombstone_pik.sequence = pik.sequence;
  }
};

class MinHeapItemComparator {
  public:
  explicit MinHeapItemComparator(const InternalKeyComparator* comparator)
      : comparator_(comparator) {}

  bool operator()(HeapItem* a, HeapItem* b) const {
    if (LIKELY(a->type == HeapItem::Type::ITERATOR)) {
      if (LIKELY(b->type == HeapItem::Type::ITERATOR)) {
        return comparator_->Compare(a->iter.key(), b->iter.key()) > 0;
      } else {
        return comparator_->Compare(a->iter.key(), b->tombstone_pik) > 0;
      }
    } else {
      if (LIKELY(b->type == HeapItem::Type::ITERATOR)) {
        return comparator_->Compare(a->tombstone_pik, b->iter.key()) > 0;
      } else {
        return comparator_->Compare(a->tombstone_pik, b->tombstone_pik) > 0;
      }
    }
  }
private:
  const InternalKeyComparator* comparator_;
};

class MaxHeapItemComparator {
  public:
  explicit MaxHeapItemComparator(const InternalKeyComparator* comparator)
      : comparator_(comparator) {}

  bool operator()(HeapItem* a, HeapItem* b) const {
    if (LIKELY(a->type == HeapItem::Type::ITERATOR)) {
      if (LIKELY(b->type == HeapItem::Type::ITERATOR)) {
        return comparator_->Compare(a->iter.key(), b->iter.key()) < 0;
      } else {
        return comparator_->Compare(a->iter.key(), b->tombstone_pik) < 0;
      }
    } else {
      if (LIKELY(b->type == HeapItem::Type::ITERATOR)) {
        return comparator_->Compare(a->tombstone_pik, b->iter.key()) < 0;
      } else {
        return comparator_->Compare(a->tombstone_pik, b->tombstone_pik) < 0;
      }
    }
  }
private:
  const InternalKeyComparator* comparator_;
};

#if defined(_MSC_VER) /* Visual Studio */
#define FORCE_INLINE __forceinline
#elif defined(__GNUC__)
#define FORCE_INLINE inline __attribute__((always_inline))
#else
#define FORCE_INLINE inline
#endif

#if 0
  #define bswap_prefix __bswap_64
  using UintPrefix = uint64_t;
#else
  using UintPrefix = unsigned __int128;
  #if defined(__GNUC__) && __GNUC_MINOR__ + 1000 * __GNUC__ > 12000
    #define bswap_prefix __builtin_bswap128
  #else
    FORCE_INLINE UintPrefix bswap_prefix(UintPrefix x) {
      return UintPrefix(__bswap_64(uint64_t(x))) << 64 | __bswap_64(uint64_t(x >> 64));
    }
  #endif
#endif

template<size_t PrefixLen>
__always_inline UintPrefix LoadPrefixZeroSuffix(const void* src) {
 #if defined(BOOST_ENDIAN_LITTLE_BYTE)
  if (PrefixLen == 12) { // gcc can not optimize memcpy + memset gracefully
    // gcc can optimize this code better
    union {
      UintPrefix u128;
      uint64_t u64[2];
    } un;
    un.u64[0] = ((const uint64_t*)src)[0];
    un.u64[1] = ((const uint32_t*)src)[2]; // zero extend uint32 to uint64
    return un.u128;
  }
 #endif
  UintPrefix dst;
  memcpy(&dst, src, PrefixLen);
  memset((char*)&dst + PrefixLen, 0, sizeof(dst) - PrefixLen);
  return dst;
}
__always_inline
UintPrefix LoadPrefixZeroSuffixDynaLen(const void* src, size_t PrefixLen) {
  UintPrefix dst;
  memcpy(&dst, src, PrefixLen);
  memset((char*)&dst + PrefixLen, 0, sizeof(dst) - PrefixLen);
  return dst;
}
FORCE_INLINE UintPrefix HostPrefixCacheUK(const Slice& uk) {
#if defined(BOOST_ENDIAN_LITTLE_BYTE)
  if (LIKELY(uk.size_ >= sizeof(UintPrefix))) {
    return bswap_prefix(unaligned_load<UintPrefix>(uk.data_));
  } else {
   #if defined(__AVX512VL__) && defined(__AVX512VBMI2__)
   #pragma message "__AVX512VL__ && __AVX512VBMI2__, use _mm_maskz_expandloadu_epi8"
    auto mask = uint16_t(~(-1 << uk.size_));
    return bswap_prefix((UintPrefix)_mm_maskz_expandloadu_epi8(mask, uk.data_));
   #else
    return bswap_prefix(LoadPrefixZeroSuffixDynaLen(uk.data_, uk.size_));
   #endif
  }
#else
  #error "HostPrefixCacheUK: Not support bigendian yet"
#endif
}
FORCE_INLINE UintPrefix HostPrefixCacheIK(const Slice& ik) {
#if defined(BOOST_ENDIAN_LITTLE_BYTE)
  if (LIKELY(ik.size_ >= sizeof(UintPrefix) + 8)) {
    return bswap_prefix(unaligned_load<UintPrefix>(ik.data_));
  } else {
   #if defined(__AVX512VL__) && defined(__AVX512VBMI2__)
    auto mask = uint16_t(~(-1 << (ik.size_ - 8)));
    return bswap_prefix((UintPrefix)_mm_maskz_expandloadu_epi8(mask, ik.data_));
   #else
    if (LIKELY(8 + 8 == ik.size_)) {
      return bswap_prefix(LoadPrefixZeroSuffix<8>(ik.data_));
    }
    else if (LIKELY(12 + 8 == ik.size_)) {
      return bswap_prefix(LoadPrefixZeroSuffix<12>(ik.data_));
    }
    else {
      return bswap_prefix(LoadPrefixZeroSuffixDynaLen(ik.data_, ik.size_ - 8));
    }
   #endif
  }
#else
  #error "HostPrefixCacheIK: Not support bigendian yet"
#endif
}

struct HeapItemAndPrefix {
  FORCE_INLINE HeapItemAndPrefix() = default;
  FORCE_INLINE HeapItemAndPrefix(HeapItem* item) : item_ptr(item) {
    iter_type = item->type;
    UpdatePrefixCache(*this);
  }
  UintPrefix key_prefix = 0;
  HeapItem* item_ptr;
  HeapItem::Type iter_type;

  HeapItem* operator->() const noexcept { return item_ptr; }

  FORCE_INLINE friend void UpdatePrefixCache(HeapItemAndPrefix& x) {
    auto p = x.item_ptr;
    if (LIKELY(HeapItem::ITERATOR == x.iter_type))
      x.key_prefix = HostPrefixCacheIK(p->iter.key());
    else
      x.key_prefix = HostPrefixCacheUK(p->tombstone_pik.user_key);
  }
};
struct HeapItemAndPrefixFast : HeapItemAndPrefix {
  using HeapItemAndPrefix::HeapItemAndPrefix;
  FORCE_INLINE friend void UpdatePrefixCache(HeapItemAndPrefixFast& x) {
    ROCKSDB_ASSERT_EQ(HeapItem::ITERATOR, x.iter_type);
    x.key_prefix = HostPrefixCacheIK(x.item_ptr->iter.key());
  }
};
static_assert(sizeof(HeapItemAndPrefixFast) == sizeof(HeapItemAndPrefix));
inline static void UpdatePrefixCache(HeapItem*) {} // do nothing

static FORCE_INLINE bool BytewiseCompareInternalKey(Slice x, Slice y) noexcept {
  size_t n = std::min(x.size_, y.size_) - 8;
  int cmp = memcmp(x.data_, y.data_, n);
  if (0 != cmp) return cmp < 0;
  if (x.size_ != y.size_) return x.size_ < y.size_;
  return GetUnalignedU64(x.data_ + n) > GetUnalignedU64(y.data_ + n);
}
static bool BytewiseCompareInternalKey(Slice x, const ParsedInternalKey& y)
noexcept {
  size_t nx = x.size_ - 8;
  size_t n = std::min(nx, y.user_key.size_);
  int cmp = memcmp(x.data_, y.user_key.data_, n);
  if (0 != cmp) return cmp < 0;
  if (nx != y.user_key.size_) return nx < y.user_key.size_;
  return GetUnalignedU64(x.data_ + nx) > (y.sequence << 8 | y.type);
}
static bool BytewiseCompareInternalKey(const ParsedInternalKey& x, Slice y)
noexcept {
  size_t ny = y.size_ - 8;
  size_t n = std::min(x.user_key.size_, ny);
  int cmp = memcmp(x.user_key.data_, y.data_, n);
  if (0 != cmp) return cmp < 0;
  if (x.user_key.size_ != ny) return x.user_key.size_ < ny;
  return (x.sequence << 8 | x.type) > GetUnalignedU64(y.data_ + ny);
}
static bool BytewiseCompareInternalKey(const ParsedInternalKey& x,
                                       const ParsedInternalKey& y)
noexcept {
  size_t n = std::min(x.user_key.size_, y.user_key.size_);
  int cmp = memcmp(x.user_key.data_, y.user_key.data_, n);
  if (0 != cmp) return cmp < 0;
  if (x.user_key.size_ != y.user_key.size_)
    return x.user_key.size_ < y.user_key.size_;
  else
    return (x.sequence << 8 | x.type) > (y.sequence << 8 | y.type);
}

static FORCE_INLINE bool RevBytewiseCompareInternalKey(Slice x,
                                                       Slice y) noexcept {
  size_t n = std::min(x.size_, y.size_) - 8;
  int cmp = memcmp(x.data_, y.data_, n);
  if (0 != cmp) return cmp > 0;
  if (x.size_ != y.size_) return x.size_ > y.size_;
  return GetUnalignedU64(x.data_ + n) > GetUnalignedU64(y.data_ + n);
}
static bool RevBytewiseCompareInternalKey(Slice x, const ParsedInternalKey& y)
noexcept {
  size_t nx = x.size_ - 8;
  size_t n = std::min(nx, y.user_key.size_);
  int cmp = memcmp(x.data_, y.user_key.data_, n);
  if (0 != cmp) return cmp > 0;
  if (nx != y.user_key.size_) return nx > y.user_key.size_;
  return GetUnalignedU64(x.data_ + nx) > (y.sequence << 8 | y.type);
}
static bool RevBytewiseCompareInternalKey(const ParsedInternalKey& x, Slice y)
noexcept {
  size_t ny = y.size_ - 8;
  size_t n = std::min(x.user_key.size_, ny);
  int cmp = memcmp(x.user_key.data_, y.data_, n);
  if (0 != cmp) return cmp > 0;
  if (x.user_key.size_ != ny) return x.user_key.size_ > ny;
  return (x.sequence << 8 | x.type) > GetUnalignedU64(y.data_ + ny);
}
static bool RevBytewiseCompareInternalKey(const ParsedInternalKey& x,
                                          const ParsedInternalKey& y)
noexcept {
  size_t n = std::min(x.user_key.size_, y.user_key.size_);
  int cmp = memcmp(x.user_key.data_, y.user_key.data_, n);
  if (0 != cmp) return cmp > 0;
  if (x.user_key.size_ != y.user_key.size_)
    return x.user_key.size_ > y.user_key.size_;
  else
    return (x.sequence << 8 | x.type) > (y.sequence << 8 | y.type);
}

class MinHeapBytewiseComp {
 public:
  MinHeapBytewiseComp(const InternalKeyComparator*) {}
  FORCE_INLINE
  bool operator()(HeapItemAndPrefix const &a, HeapItemAndPrefix const &b) const {
    if (LIKELY(a.key_prefix != b.key_prefix))
      return a.key_prefix > b.key_prefix;
    else if (LIKELY(a.iter_type == HeapItem::ITERATOR)) {
      if (LIKELY(b.iter_type == HeapItem::ITERATOR))
        return BytewiseCompareInternalKey(b->iter.key(), a->iter.key());
      else
        return BytewiseCompareInternalKey(b->tombstone_pik, a->iter.key());
    }
    else {
      if (LIKELY(b.iter_type == HeapItem::ITERATOR))
        return BytewiseCompareInternalKey(b->iter.key(), a->tombstone_pik);
      else
        return BytewiseCompareInternalKey(b->tombstone_pik, a->tombstone_pik);
    }
  }
  class IterOnly {
  public:
    IterOnly(const InternalKeyComparator*) {}
    FORCE_INLINE
    bool operator()(HeapItemAndPrefixFast const &a, HeapItemAndPrefixFast const &b) const {
      if (LIKELY(a.key_prefix != b.key_prefix))
        return a.key_prefix > b.key_prefix;
      else
        return BytewiseCompareInternalKey(b->iter.key(), a->iter.key());
    }
  };
};

class MaxHeapBytewiseComp {
 public:
  MaxHeapBytewiseComp(const InternalKeyComparator*) {}
  FORCE_INLINE
  bool operator()(HeapItemAndPrefix const &a, HeapItemAndPrefix const &b) const {
    if (LIKELY(a.key_prefix != b.key_prefix))
      return a.key_prefix < b.key_prefix;
    else if (LIKELY(a.iter_type == HeapItem::ITERATOR)) {
      if (LIKELY(b.iter_type == HeapItem::ITERATOR))
        return BytewiseCompareInternalKey(a->iter.key(), b->iter.key());
      else
        return BytewiseCompareInternalKey(a->iter.key(), b->tombstone_pik);
    }
    else {
      if (LIKELY(b.iter_type == HeapItem::ITERATOR))
        return BytewiseCompareInternalKey(a->tombstone_pik, b->iter.key());
      else
        return BytewiseCompareInternalKey(a->tombstone_pik, b->tombstone_pik);
    }
  }
  class IterOnly {
  public:
    IterOnly(const InternalKeyComparator*) {}
    FORCE_INLINE
    bool operator()(HeapItemAndPrefixFast const &a, HeapItemAndPrefixFast const &b) const {
      if (LIKELY(a.key_prefix != b.key_prefix))
        return a.key_prefix < b.key_prefix;
      else
        return BytewiseCompareInternalKey(a->iter.key(), b->iter.key());
    }
  };
};

class MinHeapRevBytewiseComp {
 public:
  MinHeapRevBytewiseComp(const InternalKeyComparator*) {}
  FORCE_INLINE
  bool operator()(HeapItemAndPrefix const &a, HeapItemAndPrefix const &b) const {
    if (LIKELY(a.key_prefix != b.key_prefix))
      return a.key_prefix < b.key_prefix;
    else if (LIKELY(a.iter_type == HeapItem::ITERATOR)) {
      if (LIKELY(b.iter_type == HeapItem::ITERATOR))
        return RevBytewiseCompareInternalKey(b->iter.key(), a->iter.key());
      else
        return RevBytewiseCompareInternalKey(b->tombstone_pik, a->iter.key());
    }
    else {
      if (LIKELY(b.iter_type == HeapItem::ITERATOR))
        return RevBytewiseCompareInternalKey(b->iter.key(), a->tombstone_pik);
      else
        return RevBytewiseCompareInternalKey(b->tombstone_pik, a->tombstone_pik);
    }
  }
  class IterOnly {
  public:
    IterOnly(const InternalKeyComparator*) {}
    FORCE_INLINE
    bool operator()(HeapItemAndPrefixFast const &a, HeapItemAndPrefixFast const &b) const {
      if (LIKELY(a.key_prefix != b.key_prefix))
        return a.key_prefix < b.key_prefix;
      else
        return RevBytewiseCompareInternalKey(b->iter.key(), a->iter.key());
    }
  };
};

class MaxHeapRevBytewiseComp {
 public:
  MaxHeapRevBytewiseComp(const InternalKeyComparator*) {}
  FORCE_INLINE
  bool operator()(HeapItemAndPrefix const &a, HeapItemAndPrefix const &b) const {
    if (LIKELY(a.key_prefix != b.key_prefix))
      return a.key_prefix > b.key_prefix;
    else if (LIKELY(a.iter_type == HeapItem::ITERATOR)) {
      if (LIKELY(b.iter_type == HeapItem::ITERATOR))
        return RevBytewiseCompareInternalKey(a->iter.key(), b->iter.key());
      else
        return RevBytewiseCompareInternalKey(a->iter.key(), b->tombstone_pik);
    }
    else {
      if (LIKELY(b.iter_type == HeapItem::ITERATOR))
        return RevBytewiseCompareInternalKey(a->tombstone_pik, b->iter.key());
      else
        return RevBytewiseCompareInternalKey(a->tombstone_pik, b->tombstone_pik);
    }
  }
  class IterOnly {
  public:
    IterOnly(const InternalKeyComparator*) {}
    FORCE_INLINE
    bool operator()(HeapItemAndPrefixFast const &a, HeapItemAndPrefixFast const &b) const {
      if (LIKELY(a.key_prefix != b.key_prefix))
        return a.key_prefix > b.key_prefix;
      else
        return RevBytewiseCompareInternalKey(a->iter.key(), b->iter.key());
    }
  };
};

class MergingIterator : public InternalIterator {
 public:
  // these Methods should be defined here, but for minimal diff with
  // upstream RocksDB code, we declare them as virtual here and define
  // them in derived template class
  virtual void AddIterator(InternalIterator*) = 0;
  virtual void AddRangeTombstoneIterator(TruncatedRangeDelIterator*) = 0;
  virtual void Finish() = 0;
  virtual void OptimizeVtable() = 0;

  // We could also use an autovector with a larger reserved size.
  // HeapItem for all child point iterators.
  std::vector<HeapItem> children_;

  // range_tombstone_iters_[i] contains range tombstones in the sorted run that
  // corresponds to children_[i]. range_tombstone_iters_.empty() means not
  // handling range tombstones in merging iterator. range_tombstone_iters_[i] ==
  // nullptr means the sorted run of children_[i] does not have range
  // tombstones.
  terark::valvec32<TruncatedRangeDelIterator*> range_tombstone_iters_;
};

template <class MinHeapComparator, class MaxHeapComparator, class Item = HeapItemAndPrefix>
class MergingIterTmpl final : public MergingIterator {
  using  MergerMinIterHeap = BinaryHeap<Item, MinHeapComparator>;
  struct MergerMaxIterHeap : BinaryHeap<Item, MaxHeapComparator> {
    // to minimize code diff, do not need change maxHeap_->xxx to maxHeap_.xxx
    inline MergerMaxIterHeap* operator->() { return this; }
    inline const MergerMaxIterHeap* operator->() const { return this; }
  };
  static_assert(sizeof(MergerMinIterHeap) == sizeof(MergerMaxIterHeap));

public:
  MergingIterTmpl() {}
  void OptimizeVtable() final {
    if constexpr (std::is_same_v<Item, HeapItemAndPrefix>) {
      static MergingIterTmpl<typename MinHeapComparator::IterOnly,
                             typename MaxHeapComparator::IterOnly,
                             HeapItemAndPrefixFast> iter_only;
      void*& vtab = reinterpret_cast<void**>(this)[0];
      vtab = reinterpret_cast<void**>(&iter_only)[0];
    }
  }

  MergingIterTmpl(const InternalKeyComparator* comparator,
                  InternalIterator** children, int n, bool is_arena_mode,
                  bool prefix_seek_mode,
                  const Slice* iterate_upper_bound = nullptr)
      : is_arena_mode_(is_arena_mode),
        prefix_seek_mode_(prefix_seek_mode),
        direction_(kForward),
        current_(nullptr),
        minHeap_(MinHeapComparator(comparator)),
        comparator_(comparator),
        pinned_iters_mgr_(nullptr),
        iterate_upper_bound_(iterate_upper_bound) {
    children_.resize(n);
    for (int i = 0; i < n; i++) {
      children_[i].level = i;
      children_[i].iter.Set(children[i]);
    }
    static_assert( // Hot fields should lie in same cache line
      (offsetof(MergingIterTmpl, range_tombstone_iters_)) / CACHE_LINE_SIZE ==
      (offsetof(MergingIterTmpl, comparator_) - 1) / CACHE_LINE_SIZE);
  }

  void considerStatus(Status s) {
    if (!s.ok() && status_.ok()) {
      status_ = s;
    }
  }

  virtual void AddIterator(InternalIterator* iter) {
    children_.emplace_back(children_.size(), iter);
    if (pinned_iters_mgr_) {
      iter->SetPinnedItersMgr(pinned_iters_mgr_);
    }
    // Invalidate to ensure `Seek*()` is called to construct the heaps before
    // use.
    current_ = nullptr;
  }

  // There must be either no range tombstone iterator or the same number of
  // range tombstone iterators as point iterators after all iters are added.
  // The i-th added range tombstone iterator and the i-th point iterator
  // must point to the same LSM level.
  // Merging iterator takes ownership of `iter` and is responsible for freeing
  // it. One exception to this is when a LevelIterator moves to a different SST
  // file or when Iterator::Refresh() is called, the range tombstone iterator
  // could be updated. In that case, this merging iterator is only responsible
  // for freeing the new range tombstone iterator that it has pointers to in
  // range_tombstone_iters_.
  void AddRangeTombstoneIterator(TruncatedRangeDelIterator* iter) {
    range_tombstone_iters_.emplace_back(iter);
  }

  // Called by MergingIteratorBuilder when all point iterators and range
  // tombstone iterators are added. Initializes HeapItems for range tombstone
  // iterators.
  void Finish() {
    if (!range_tombstone_iters_.empty()) {
      assert(range_tombstone_iters_.size() == children_.size());
      pinned_heap_item_.resize(range_tombstone_iters_.size());
      for (size_t i = 0; i < range_tombstone_iters_.size(); ++i) {
        pinned_heap_item_[i].level = i;
        // Range tombstone end key is exclusive. If a point internal key has the
        // same user key and sequence number as the start or end key of a range
        // tombstone, the order will be start < end key < internal key with the
        // following op_type change. This is helpful to ensure keys popped from
        // heap are in expected order since range tombstone start/end keys will
        // be distinct from point internal keys. Strictly speaking, this is only
        // needed for tombstone end points that are truncated in
        // TruncatedRangeDelIterator since untruncated tombstone end points
        // always have kMaxSequenceNumber and kTypeRangeDeletion (see
        // TruncatedRangeDelIterator::start_key()/end_key()).
        pinned_heap_item_[i].tombstone_pik.type = kTypeMaxValid;
      }
    }
    minHeap_.reserve(children_.size() + range_tombstone_iters_.size());
  }

  ~MergingIterTmpl() override {
    minHeap_.~MergerMinIterHeap();
    for (auto child : range_tombstone_iters_) {
      delete child;
    }

    for (auto& child : children_) {
      child.iter.DeleteIter(is_arena_mode_);
    }
    status_.PermitUncheckedError();
  }

  bool Valid() const override { return current_ != nullptr && status_.ok(); }

  Status status() const override {
    // this function should be called very rarely, so it can be slow.
    // pay this function slow to gain PrepareAndGetValue faster.
    if (status_.ok() && current_) {
      const_cast<Status&>(status_) = current_->status();
    }
    return status_;
  }

  // Add range_tombstone_iters_[level] into min heap.
  // Updates active_ if the end key of a range tombstone is inserted.
  // pinned_heap_items_[level].type is updated based on `start_key`.
  //
  // If range_tombstone_iters_[level] is after iterate_upper_bound_,
  // it is removed from the heap.
  // @param start_key specifies which end point of the range tombstone to add.
  void InsertRangeTombstoneToMinHeap(size_t level, bool start_key = true,
                                     bool replace_top = false) {
    assert(!range_tombstone_iters_.empty() &&
           range_tombstone_iters_[level]->Valid());
    // Maintains Invariant(phi)
    if (start_key) {
      pinned_heap_item_[level].type = HeapItem::Type::DELETE_RANGE_START;
      ParsedInternalKey pik = range_tombstone_iters_[level]->start_key();
      // iterate_upper_bound does not have timestamp
      if (iterate_upper_bound_ &&
          comparator_->user_comparator()->CompareWithoutTimestamp(
              pik.user_key, true /* a_has_ts */, *iterate_upper_bound_,
              false /* b_has_ts */) >= 0) {
        if (replace_top) {
          // replace_top implies this range tombstone iterator is still in
          // minHeap_ and at the top.
          minHeap_.pop();
        }
        return;
      }
      pinned_heap_item_[level].SetTombstoneKey(std::move(pik));
      // Checks Invariant(active_)
      assert(active_.count(level) == 0);
    } else {
      // allow end key to go over upper bound (if present) since start key is
      // before upper bound and the range tombstone could still cover a
      // range before upper bound.
      // Maintains Invariant(active_)
      pinned_heap_item_[level].SetTombstoneKey(
          range_tombstone_iters_[level]->end_key());
      pinned_heap_item_[level].type = HeapItem::Type::DELETE_RANGE_END;
      active_.insert(level);
    }
    if (replace_top) {
      minHeap_.replace_top(&pinned_heap_item_[level]);
    } else {
      minHeap_.push(&pinned_heap_item_[level]);
    }
  }

  // Add range_tombstone_iters_[level] into max heap.
  // Updates active_ if the start key of a range tombstone is inserted.
  // @param end_key specifies which end point of the range tombstone to add.
  void InsertRangeTombstoneToMaxHeap(size_t level, bool end_key = true,
                                     bool replace_top = false) {
    assert(!range_tombstone_iters_.empty() &&
           range_tombstone_iters_[level]->Valid());
    if (end_key) {
      pinned_heap_item_[level].SetTombstoneKey(
          range_tombstone_iters_[level]->end_key());
      pinned_heap_item_[level].type = HeapItem::Type::DELETE_RANGE_END;
      assert(active_.count(level) == 0);
    } else {
      pinned_heap_item_[level].SetTombstoneKey(
          range_tombstone_iters_[level]->start_key());
      pinned_heap_item_[level].type = HeapItem::Type::DELETE_RANGE_START;
      active_.insert(level);
    }
    if (replace_top) {
      maxHeap_->replace_top(&pinned_heap_item_[level]);
    } else {
      maxHeap_->push(&pinned_heap_item_[level]);
    }
  }

  // Remove HeapItems from top of minHeap_ that are of type DELETE_RANGE_START
  // until minHeap_ is empty or the top of the minHeap_ is not of type
  // DELETE_RANGE_START. Each such item means a range tombstone becomes active,
  // so `active_` is updated accordingly.
  void PopDeleteRangeStart() {
    while (!minHeap_.empty() &&
           minHeap_.top()->type == HeapItem::Type::DELETE_RANGE_START) {
      TEST_SYNC_POINT_CALLBACK("MergeIterator::PopDeleteRangeStart", nullptr);
      // Invariant(rti) holds since
      // range_tombstone_iters_[minHeap_.top()->level] is still valid, and
      // parameter `replace_top` is set to true here to ensure only one such
      // HeapItem is in minHeap_.
      InsertRangeTombstoneToMinHeap(
          minHeap_.top()->level, false /* start_key */, true /* replace_top */);
    }
  }

  // Remove HeapItems from top of maxHeap_ that are of type DELETE_RANGE_END
  // until maxHeap_ is empty or the top of the maxHeap_ is not of type
  // DELETE_RANGE_END. Each such item means a range tombstone becomes active,
  // so `active_` is updated accordingly.
  void PopDeleteRangeEnd() {
    while (!maxHeap_->empty() &&
           maxHeap_->top()->type == HeapItem::Type::DELETE_RANGE_END) {
      // insert start key of this range tombstone and updates active_
      InsertRangeTombstoneToMaxHeap(maxHeap_->top()->level, false /* end_key */,
                                    true /* replace_top */);
    }
  }

  void SeekToFirst() override {
    ClearHeaps();
    status_ = Status::OK();
    for (auto& child : children_) {
      child.iter.SeekToFirst();
      AddToMinHeapOrCheckStatus(&child);
    }

    for (size_t i = 0; i < range_tombstone_iters_.size(); ++i) {
      if (range_tombstone_iters_[i]) {
        range_tombstone_iters_[i]->SeekToFirst();
        if (range_tombstone_iters_[i]->Valid()) {
          // It is possible to be invalid due to snapshots.
          InsertRangeTombstoneToMinHeap(i);
        }
      }
    }
    FindNextVisibleKey();
    direction_ = kForward;
    current_ = CurrentForward();
  }

  void SeekToLast() override {
    ClearHeaps();
    InitMaxHeap();
    status_ = Status::OK();
    for (auto& child : children_) {
      child.iter.SeekToLast();
      AddToMaxHeapOrCheckStatus(&child);
    }

    for (size_t i = 0; i < range_tombstone_iters_.size(); ++i) {
      if (range_tombstone_iters_[i]) {
        range_tombstone_iters_[i]->SeekToLast();
        if (range_tombstone_iters_[i]->Valid()) {
          // It is possible to be invalid due to snapshots.
          InsertRangeTombstoneToMaxHeap(i);
        }
      }
    }
    FindPrevVisibleKey();
    direction_ = kReverse;
    current_ = CurrentReverse();
  }

  // Position this merging iterator at the first key >= target (internal key).
  // If range tombstones are present, keys covered by range tombstones are
  // skipped, and this merging iter points to the first non-range-deleted key >=
  // target after Seek(). If !Valid() and status().ok() then this iterator
  // reaches the end.
  //
  // If range tombstones are present, cascading seeks may be called (an
  // optimization adapted from Pebble https://github.com/cockroachdb/pebble).
  // Roughly, if there is a range tombstone [start, end) that covers the
  // target user key at level L, then this range tombstone must cover the range
  // [target key, end) in all levels > L. So for all levels > L, we can pretend
  // the target key is `end`. This optimization is applied at each level and
  // hence the name "cascading seek".
  void Seek(const Slice& target) override {
    // Define LevelNextVisible(i, k) to be the first key >= k in level i that is
    // not covered by any range tombstone.
    // After SeekImpl(target, 0), invariants (3) and (4) hold.
    // For all level i, target <= children_[i].iter.key() <= LevelNextVisible(i,
    // target). By the contract of FindNextVisibleKey(), Invariants (1)-(4)
    // holds after this call, and minHeap_.top().iter points to the
    // first key >= target among children_ that is not covered by any range
    // tombstone.
    status_ = Status::OK();
    SeekImpl(target);
    FindNextVisibleKey();

    direction_ = kForward;
    {
      PERF_TIMER_GUARD(seek_min_heap_time);
      current_ = CurrentForward();
    }
  }

  void SeekForPrev(const Slice& target) override {
    assert(range_tombstone_iters_.empty() ||
           range_tombstone_iters_.size() == children_.size());
    status_ = Status::OK();
    SeekForPrevImpl(target);
    FindPrevVisibleKey();

    direction_ = kReverse;
    {
      PERF_TIMER_GUARD(seek_max_heap_time);
      current_ = CurrentReverse();
    }
  }

  void Next() override {
    DoNext(); // ignore return value
  }
  bool DoNext() {
    assert(Valid());
    // Ensure that all children are positioned after key().
    // If we are moving in the forward direction, it is already
    // true for all the non-current children since current_ is
    // the smallest child and key() == current_->key().
    if (UNLIKELY(direction_ != kForward)) {
      // The loop advanced all non-current children to be > key() so current_
      // should still be strictly the smallest key.
      SwitchToForward();
      if (UNLIKELY(!status_.ok()))
        return false;
    }

    // For the heap modifications below to be correct, current_ must be the
    // current top of the heap.
    assert(current_ == CurrentForward());
    // as the current points to the current record. move the iterator forward.
    if (LIKELY(current_->Next())) {
      // current is still valid after the Next() call above.  Call
      // replace_top() to restore the heap property.  When the same child
      // iterator yields a sequence of keys, this is cheap.
      assert(current_->status().ok());
      UpdatePrefixCache(minHeap_.top());
      minHeap_.update_top();
      if (LIKELY((std::is_same_v<Item, HeapItemAndPrefixFast>) || range_tombstone_iters_.empty())) {
        current_ = &minHeap_.top()->iter; // current_ = CurrentForward();
        return true;
      }
    } else {
      // current stopped being valid, remove it from the heap.
      considerStatus(current_->status());
      minHeap_.pop();
    }
    // Invariants (3) and (4) hold when after advancing current_.
    // Let k be the smallest key among children_[i].iter.key().
    // k <= children_[i].iter.key() <= LevelNextVisible(i, k) holds for all
    // level i. After FindNextVisible(), Invariants (1)-(4) hold and
    // minHeap_.top()->key() is the first key >= k from any children_ that is
    // not covered by any range tombstone.
    FindNextVisibleKey();
    if (LIKELY(!minHeap_.empty())) {
      current_ = &minHeap_.top()->iter;
      return status_.ok();
    } else {
      current_ = nullptr;
      return false;
    }
  }

  bool NextAndGetResult(IterateResult* result) override {
    bool is_valid = DoNext();
    result->is_valid = is_valid;
    if (LIKELY(is_valid)) {
      result->SetKey(this->key());
      result->bound_check_result = UpperBoundCheckResult();
      result->value_prepared = current_->IsValuePrepared();
    }
    return is_valid;
  }

  void Prev() override {
    assert(Valid());
    // Ensure that all children are positioned before key().
    // If we are moving in the reverse direction, it is already
    // true for all the non-current children since current_ is
    // the largest child and key() == current_->key().
    if (UNLIKELY(direction_ != kReverse)) {
      // Otherwise, retreat the non-current children.  We retreat current_
      // just after the if-block.
      SwitchToBackward();
    }

    // For the heap modifications below to be correct, current_ must be the
    // current top of the heap.
    assert(current_ == CurrentReverse());
    current_->Prev();
    if (LIKELY(current_->Valid())) {
      // current is still valid after the Prev() call above.  Call
      // replace_top() to restore the heap property.  When the same child
      // iterator yields a sequence of keys, this is cheap.
      assert(current_->status().ok());
      UpdatePrefixCache(maxHeap_->top());
      maxHeap_->replace_top(maxHeap_->top());
      if (LIKELY(range_tombstone_iters_.empty())) {
        current_ = &maxHeap_->top()->iter; // current_ = CurrentReverse();
        return;
      }
    } else {
      // current stopped being valid, remove it from the heap.
      considerStatus(current_->status());
      maxHeap_->pop();
    }
    FindPrevVisibleKey();
    current_ = CurrentReverse();
  }

  Slice key() const override {
    assert(Valid());
    return current_->key();
  }

  Slice value() const override {
    assert(Valid());
    return current_->value();
  }

  bool PrepareValue() override {
    assert(Valid());
    if (current_->PrepareValue()) {
      return true;
    }

    considerStatus(current_->status());
    assert(!status_.ok());
    return false;
  }

  bool PrepareAndGetValue(Slice* v) override {
    assert(Valid());
    return current_->PrepareAndGetValue(v);
  }

  // Here we simply relay MayBeOutOfLowerBound/MayBeOutOfUpperBound result
  // from current child iterator. Potentially as long as one of child iterator
  // report out of bound is not possible, we know current key is within bound.
  bool MayBeOutOfLowerBound() override {
    assert(Valid());
    return current_->MayBeOutOfLowerBound();
  }

  IterBoundCheck UpperBoundCheckResult() override {
    assert(Valid());
    return current_->UpperBoundCheckResult();
  }

  void SetPinnedItersMgr(PinnedIteratorsManager* pinned_iters_mgr) override {
    pinned_iters_mgr_ = pinned_iters_mgr;
    for (auto& child : children_) {
      child.iter.SetPinnedItersMgr(pinned_iters_mgr);
    }
  }

  bool IsKeyPinned() const override {
    assert(Valid());
    return pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled() &&
           current_->IsKeyPinned();
  }

  bool IsValuePinned() const override {
    assert(Valid());
    return pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled() &&
           current_->IsValuePinned();
  }

 private:
  friend class MergeIteratorBuilder;
  // Clears heaps for both directions, used when changing direction or seeking
  void ClearHeaps(bool clear_active = true);
  // Ensures that maxHeap_ is initialized when starting to go in the reverse
  // direction
  void InitMaxHeap();
  // Advance this merging iterator until the current key (minHeap_.top()) is
  // from a point iterator and is not covered by any range tombstone,
  // or that there is no more keys (heap is empty). SeekImpl() may be called
  // to seek to the end of a range tombstone as an optimization.
  void FindNextVisibleKey();
  void FindPrevVisibleKey();

  void FindNextVisibleKeySlowPath();
  void FindPrevVisibleKeySlowPath();

  // Advance this merging iterators to the first key >= `target` for all
  // components from levels >= starting_level. All iterators before
  // starting_level are untouched.
  //
  // @param range_tombstone_reseek Whether target is some range tombstone
  // end, i.e., whether this SeekImpl() call is a part of a "cascading seek".
  // This is used only for recoding relevant perf_context.
  void SeekImpl(const Slice& target, size_t starting_level = 0,
                bool range_tombstone_reseek = false);

  // Seek to fist key <= target key (internal key) for
  // children_[starting_level:].
  void SeekForPrevImpl(const Slice& target, size_t starting_level = 0,
                       bool range_tombstone_reseek = false);

  bool is_arena_mode_;
  bool prefix_seek_mode_;
  // Which direction is the iterator moving?
  enum Direction : uint8_t { kForward, kReverse };
  Direction direction_;

  // Invariant: at the end of each InternalIterator API,
  // current_ points to minHeap_.top().iter (maxHeap_ if backward scanning)
  // or nullptr if no child iterator is valid.
  // This follows from that current_ = CurrentForward()/CurrentReverse() is
  // called at the end of each InternalIterator API.
  IteratorWrapper* current_;

  union {
    MergerMinIterHeap minHeap_;
    MergerMaxIterHeap maxHeap_;
  };

  const InternalKeyComparator* comparator_;
  // HeapItem for range tombstone start and end keys. Each range tombstone
  // iterator will have at most one side (start key or end key) in a heap
  // at the same time, so this vector will be of size children_.size();
  // pinned_heap_item_[i] corresponds to the start key and end key HeapItem
  // for range_tombstone_iters_[i].
  std::vector<HeapItem> pinned_heap_item_;

  // Levels (indices into range_tombstone_iters_/children_ ) that currently have
  // "active" range tombstones. See comments above MergingIterator for meaning
  // of "active".
  // Invariant(active_): i is in active_ iff range_tombstone_iters_[i]->Valid()
  // and pinned_heap_item_[i].type == DELETE_RANGE_END.
  std::set<size_t> active_;

  bool SkipNextDeleted();

  bool SkipPrevDeleted();

  // If any of the children have non-ok status, this is one of them.
  Status status_;
  PinnedIteratorsManager* pinned_iters_mgr_;

  // Used to bound range tombstones. For point keys, DBIter and SSTable iterator
  // take care of boundary checking.
  const Slice* iterate_upper_bound_;

  // In forward direction, process a child that is not in the min heap.
  // If valid, add to the min heap. Otherwise, check status.
  void AddToMinHeapOrCheckStatus(HeapItem*);

  // In backward direction, process a child that is not in the max heap.
  // If valid, add to the min heap. Otherwise, check status.
  void AddToMaxHeapOrCheckStatus(HeapItem*);

  void SwitchToForward();

  // Switch the direction from forward to backward without changing the
  // position. Iterator should still be valid.
  void SwitchToBackward();

  IteratorWrapper* CurrentForward() const {
    assert(direction_ == kForward);
    assert(minHeap_.empty() ||
           minHeap_.top()->type == HeapItem::Type::ITERATOR);
    return !minHeap_.empty() ? &minHeap_.top()->iter : nullptr;
  }

  IteratorWrapper* CurrentReverse() const {
    assert(direction_ == kReverse);
    assert(maxHeap_->empty() ||
           maxHeap_->top()->type == HeapItem::Type::ITERATOR);
    return !maxHeap_->empty() ? &maxHeap_->top()->iter : nullptr;
  }
};

#define MergingIterMethod(Ret) \
  template <class MinHeapComparator, class MaxHeapComparator, class Item> \
  Ret MergingIterTmpl<MinHeapComparator, MaxHeapComparator, Item>::

// Pre-condition:
// - Invariants (3) and (4) hold for i < starting_level
// - For i < starting_level, range_tombstone_iters_[i].prev.end_key() <
// `target`.
// - For i < starting_level, if i in active_, then
// range_tombstone_iters_[i]->start_key() < `target`.
//
// Post-condition:
// - Invariants (3) and (4) hold for all level i.
// - (*) target <= children_[i].iter.key() <= LevelNextVisible(i, target)
// for i >= starting_level
// - (**) target < pinned_heap_item_[i].tombstone_pik if
// range_tombstone_iters_[i].Valid() for i >= starting_level
//
// Proof sketch:
// Invariant (3) holds for all level i.
// For j <= i < starting_level, it follows from Pre-condition that (3) holds
// and that SeekImpl(-, starting_level) does not update children_[i] or
// range_tombstone_iters_[j].
// For j < starting_level and i >= starting_level, it follows from
// - Pre-condition that range_tombstone_iters_[j].prev.end_key() < `target`
// - range_tombstone_iters_[j] is not updated in SeekImpl(), and
// - children_[i].iter.Seek(current_search_key) is called with
// current_search_key >= target (shown below).
//   When current_search_key is updated, it is updated to some
//   range_tombstone_iter->end_key() after
//   range_tombstone_iter->SeekInternalKey(current_search_key) was called. So
//   current_search_key increases if updated and >= target.
// For starting_level <= j <= i:
// children_[i].iter.Seek(k1) and range_tombstone_iters_[j]->SeekInternalKey(k2)
// are called in SeekImpl(). Seek(k1) positions children_[i] at the first key >=
// k1 from level i. SeekInternalKey(k2) positions range_tombstone_iters_[j] at
// the first range tombstone from level j with end_key() > k2. It suffices to
// show that k1 >= k2. Since k1 and k2 are values of current_search_key where
// k1 = k2 or k1 is value of a later current_search_key than k2, so k1 >= k2.
//
// Invariant (4) holds for all level >= 0.
// By Pre-condition Invariant (4) holds for i < starting_level.
// Since children_[i], range_tombstone_iters_[i] and contents of active_ for
// i < starting_level do not change (4) holds for j <= i < starting_level.
// By Pre-condition: for all j < starting_level, if j in active_, then
// range_tombstone_iters_[j]->start_key() < target. For i >= starting_level,
// children_[i].iter.Seek(k) is called for k >= target. So
// children_[i].iter.key() >= target > range_tombstone_iters_[j]->start_key()
// for j < starting_level and i >= starting_level. So invariant (4) holds for
// j < starting_level and i >= starting_level.
// For starting_level <= j <= i, j is added to active_ only if
// - range_tombstone_iters_[j]->SeekInternalKey(k1) was called
// - range_tombstone_iters_[j]->start_key() <= k1
// Since children_[i].iter.Seek(k2) is called for some k2 >= k1 and for all
// starting_level <= j <= i, (4) also holds for all starting_level <= j <= i.
//
// Post-condition (*): target <= children_[i].iter.key() <= LevelNextVisible(i,
// target) for i >= starting_level.
// target <= children_[i].iter.key() follows from that Seek() is called on some
// current_search_key >= target for children_[i].iter. If current_search_key
// is updated from k1 to k2 when level = i, we show that the range [k1, k2) is
// not visible for children_[j] for any j > i. When current_search_key is
// updated from k1 to k2,
//  - range_tombstone_iters_[i]->SeekInternalKey(k1) was called
//  - range_tombstone_iters_[i]->Valid()
//  - range_tombstone_iters_[i]->start_key().user_key <= k1.user_key
//  - k2 = range_tombstone_iters_[i]->end_key()
// We assume that range_tombstone_iters_[i]->start_key() has a higher sequence
// number compared to any key from levels > i that has the same user key. So no
// point key from levels > i in range [k1, k2) is visible. So
// children_[i].iter.key() <= LevelNextVisible(i, target).
//
// Post-condition (**) target < pinned_heap_item_[i].tombstone_pik for i >=
// starting_level if range_tombstone_iters_[i].Valid(). This follows from that
// SeekInternalKey() being called for each range_tombstone_iters_ with some key
// >= `target` and that we pick start/end key that is > `target` to insert to
// minHeap_.
MergingIterMethod(void)SeekImpl(const Slice& target, size_t starting_level,
                               bool range_tombstone_reseek) {
  // active range tombstones before `starting_level` remain active
  ClearHeaps(false /* clear_active */);
  ParsedInternalKey pik;
  if (!range_tombstone_iters_.empty()) {
    // pik is only used in InsertRangeTombstoneToMinHeap().
    ParseInternalKey(target, &pik, false).PermitUncheckedError();
  }

  // TODO: perhaps we could save some upheap cost by add all child iters first
  //  and then do a single heapify.
  // Invariant(children_) for level < starting_level
  for (size_t level = 0; level < starting_level; ++level) {
    PERF_TIMER_GUARD(seek_min_heap_time);
    AddToMinHeapOrCheckStatus(&children_[level]);
  }
  if (!range_tombstone_iters_.empty()) {
    // Add range tombstones from levels < starting_level. We can insert from
    // pinned_heap_item_ for the following reasons:
    // - pinned_heap_item_[level] is in minHeap_ iff
    // range_tombstone_iters[level]->Valid().
    // - If `level` is in active_, then range_tombstone_iters_[level]->Valid()
    // and pinned_heap_item_[level] is of type RANGE_DELETION_END.
    for (size_t level = 0; level < starting_level; ++level) {
      // Restores Invariants(rti), (phi) and (active_) for level <
      // starting_level
      if (range_tombstone_iters_[level] &&
          range_tombstone_iters_[level]->Valid()) {
        // use an iterator on active_ if performance becomes an issue here
        if (active_.count(level) > 0) {
          assert(pinned_heap_item_[level].type ==
                 HeapItem::Type::DELETE_RANGE_END);
          // if it was active, then start key must be within upper_bound,
          // so we can add to minHeap_ directly.
          minHeap_.push(&pinned_heap_item_[level]);
        } else {
          assert(pinned_heap_item_[level].type ==
                 HeapItem::Type::DELETE_RANGE_START);
          // this takes care of checking iterate_upper_bound, but with an extra
          // key comparison if range_tombstone_iters_[level] was already out of
          // bound. Consider using a new HeapItem type or some flag to remember
          // boundary checking result.
          InsertRangeTombstoneToMinHeap(level);
        }
      } else {
        assert(!active_.count(level));
      }
    }
    // levels >= starting_level will be reseeked below, so clearing their active
    // state here.
    active_.erase(active_.lower_bound(starting_level), active_.end());
  }

  IterKey current_search_key;
  current_search_key.SetInternalKey(target, false /* copy */);
  // Seek target might change to some range tombstone end key, so
  // we need to remember them for async requests.
  // (level, target) pairs
  autovector<std::pair<size_t, std::string>> prefetched_target;
  for (auto level = starting_level; level < children_.size(); ++level) {
    {
      PERF_TIMER_GUARD(seek_child_seek_time);
      children_[level].iter.Seek(current_search_key.GetInternalKey());
    }

    PERF_COUNTER_ADD(seek_child_seek_count, 1);

    if (!range_tombstone_iters_.empty()) {
      if (range_tombstone_reseek) {
        // This seek is to some range tombstone end key.
        // Should only happen when there are range tombstones.
        PERF_COUNTER_ADD(internal_range_del_reseek_count, 1);
      }
      if (children_[level].iter.status().IsTryAgain()) {
        prefetched_target.emplace_back(
            level, current_search_key.GetInternalKey().ToString());
      }
      auto range_tombstone_iter = range_tombstone_iters_[level];
      if (range_tombstone_iter) {
        range_tombstone_iter->SeekInternalKey(
            current_search_key.GetInternalKey());
        // Invariants (rti) and (phi)
        if (range_tombstone_iter->Valid()) {
          // If range tombstone starts after `current_search_key`,
          // we should insert start key to heap as the range tombstone is not
          // active yet.
          InsertRangeTombstoneToMinHeap(
              level, comparator_->Compare(range_tombstone_iter->start_key(),
                                          pik) > 0 /* start_key */);
          // current_search_key < end_key guaranteed by the SeekInternalKey()
          // and Valid() calls above. Here we only need to compare user_key
          // since if target.user_key ==
          // range_tombstone_iter->start_key().user_key and target <
          // range_tombstone_iter->start_key(), no older level would have any
          // key in range [target, range_tombstone_iter->start_key()], so no
          // keys in range [target, range_tombstone_iter->end_key()) from older
          // level would be visible. So it is safe to seek to
          // range_tombstone_iter->end_key().
          //
          // TODO: range_tombstone_iter->Seek() finds the max covering
          //  sequence number, can make it cheaper by not looking for max.
          if (comparator_->user_comparator()->Compare(
                  range_tombstone_iter->start_key().user_key,
                  current_search_key.GetUserKey()) <= 0) {
            range_tombstone_reseek = true;
            // Note that for prefix seek case, it is possible that the prefix
            // is not the same as the original target, it should not affect
            // correctness. Besides, in most cases, range tombstone start and
            // end key should have the same prefix?
            current_search_key.SetInternalKey(range_tombstone_iter->end_key());
          }
        }
      }
    }
    // child.iter.status() is set to Status::TryAgain indicating asynchronous
    // request for retrieval of data blocks has been submitted. So it should
    // return at this point and Seek should be called again to retrieve the
    // requested block and add the child to min heap.
    if (children_[level].iter.status().IsTryAgain()) {
      continue;
    }
    {
      // Strictly, we timed slightly more than min heap operation,
      // but these operations are very cheap.
      PERF_TIMER_GUARD(seek_min_heap_time);
      AddToMinHeapOrCheckStatus(&children_[level]);
    }
  }

  if (range_tombstone_iters_.empty()) {
    for (auto& child : children_) {
      if (child.iter.status().IsTryAgain()) {
        child.iter.Seek(target);
        {
          PERF_TIMER_GUARD(seek_min_heap_time);
          AddToMinHeapOrCheckStatus(&child);
        }
        PERF_COUNTER_ADD(number_async_seek, 1);
      }
    }
  } else {
    for (auto& prefetch : prefetched_target) {
      // (level, target) pairs
      children_[prefetch.first].iter.Seek(prefetch.second);
      {
        PERF_TIMER_GUARD(seek_min_heap_time);
        AddToMinHeapOrCheckStatus(&children_[prefetch.first]);
      }
      PERF_COUNTER_ADD(number_async_seek, 1);
    }
  }
}

// Returns true iff the current key (min heap top) should not be returned
// to user (of the merging iterator). This can be because the current key
// is deleted by some range tombstone, the current key is some fake file
// boundary sentinel key, or the current key is an end point of a range
// tombstone. Advance the iterator at heap top if needed. Heap order is restored
// and `active_` is updated accordingly.
// See FindNextVisibleKey() for more detail on internal implementation
// of advancing child iters.
// When false is returned, if minHeap is not empty, then minHeap_.top().type
// == ITERATOR
//
// REQUIRES:
// - min heap is currently not empty, and iter is in kForward direction.
// - minHeap_ top is not DELETE_RANGE_START (so that `active_` is current).
MergingIterMethod(bool)SkipNextDeleted() {
  // 3 types of keys:
  // - point key
  // - file boundary sentinel keys
  // - range deletion end key
  auto current = minHeap_.top();
  if (current->type == HeapItem::Type::DELETE_RANGE_END) {
    // Invariant(active_): range_tombstone_iters_[current->level] is about to
    // become !Valid() or that its start key is going to be added to minHeap_.
    active_.erase(current->level);
    assert(range_tombstone_iters_[current->level] &&
           range_tombstone_iters_[current->level]->Valid());
    range_tombstone_iters_[current->level]->Next();
    // Maintain Invariants (rti) and (phi)
    if (range_tombstone_iters_[current->level]->Valid()) {
      InsertRangeTombstoneToMinHeap(current->level, true /* start_key */,
                                    true /* replace_top */);
    } else {
      // TruncatedRangeDelIterator does not have status
      minHeap_.pop();
    }
    return true /* current key deleted */;
  }
  if (current->iter.IsDeleteRangeSentinelKey()) {
    // If the file boundary is defined by a range deletion, the range
    // tombstone's end key must come before this sentinel key (see op_type in
    // SetTombstoneKey()).
    assert(ExtractValueType(current->iter.key()) != kTypeRangeDeletion ||
           active_.count(current->level) == 0);
    // When entering a new file, range tombstone iter from the old file is
    // freed, but the last key from that range tombstone iter may still be in
    // the heap. We need to ensure the data underlying its corresponding key
    // Slice is still alive. We do so by popping the range tombstone key from
    // heap before calling iter->Next(). Technically, this change is not needed:
    // if there is a range tombstone end key that is after file boundary
    // sentinel key in minHeap_, the range tombstone end key must have been
    // truncated at file boundary. The underlying data of the range tombstone
    // end key Slice is the SST file's largest internal key stored as file
    // metadata in Version. However, since there are too many implicit
    // assumptions made, it is safer to just ensure range tombstone iter is
    // still alive.
    minHeap_.pop();
    // Remove last SST file's range tombstone end key if there is one.
    // This means file boundary is before range tombstone end key,
    // which could happen when a range tombstone and a user key
    // straddle two SST files. Note that in TruncatedRangeDelIterator
    // constructor, parsed_largest.sequence is decremented 1 in this case.
    // Maintains Invariant(rti) that at most one
    // pinned_heap_item_[current->level] is in minHeap_.
    if (range_tombstone_iters_[current->level] &&
        range_tombstone_iters_[current->level]->Valid()) {
      if (!minHeap_.empty() && minHeap_.top()->level == current->level) {
        assert(minHeap_.top()->type == HeapItem::Type::DELETE_RANGE_END);
        minHeap_.pop();
        // Invariant(active_): we are about to enter a new SST file with new
        // range_tombstone_iters[current->level]. Either it is !Valid() or its
        // start key is going to be added to minHeap_.
        active_.erase(current->level);
      } else {
        // range tombstone is still valid, but it is not on heap.
        // This should only happen if the range tombstone is over iterator
        // upper bound.
        assert(iterate_upper_bound_ &&
               comparator_->user_comparator()->CompareWithoutTimestamp(
                   range_tombstone_iters_[current->level]->start_key().user_key,
                   true /* a_has_ts */, *iterate_upper_bound_,
                   false /* b_has_ts */) >= 0);
      }
    }
    // LevelIterator enters a new SST file
    // Invariant(children_): current is popped from heap and added back only if
    // it is valid
    if (current->iter.Next()) {
      assert(current->iter.status().ok());
      UpdatePrefixCache(current);
      minHeap_.push(current);
    } else {
      // TODO(cbi): check status and early return if non-ok.
      considerStatus(current->iter.status());
    }
    // Invariants (rti) and (phi)
    if (range_tombstone_iters_[current->level] &&
        range_tombstone_iters_[current->level]->Valid()) {
      InsertRangeTombstoneToMinHeap(current->level);
    }
    return true /* current key deleted */;
  }
  assert(current->type == HeapItem::Type::ITERATOR);
  // Point key case: check active_ for range tombstone coverage.
  ParsedInternalKey pik(current->iter.key());
  if (!active_.empty()) {
    auto i = *active_.begin();
    if (i < current->level) {
      // range tombstone is from a newer level, definitely covers
      assert(comparator_->Compare(range_tombstone_iters_[i]->start_key(),
                                  pik) <= 0);
      assert(comparator_->Compare(pik, range_tombstone_iters_[i]->end_key()) <
             0);
      std::string target;
      AppendInternalKey(&target, range_tombstone_iters_[i]->end_key());
      SeekImpl(target, current->level, true);
      return true /* current key deleted */;
    } else if (i == current->level) {
      // range tombstone is from the same level as current, check sequence
      // number. By `active_` we know current key is between start key and end
      // key.
      assert(comparator_->Compare(range_tombstone_iters_[i]->start_key(),
                                  pik) <= 0);
      assert(comparator_->Compare(pik, range_tombstone_iters_[i]->end_key()) <
             0);
      if (pik.sequence < range_tombstone_iters_[current->level]->seq()) {
        // covered by range tombstone
        // Invariant (children_)
        if (current->iter.Next()) {
          UpdatePrefixCache(current);
          minHeap_.replace_top(current);
        } else {
          considerStatus(current->iter.status());
          minHeap_.pop();
        }
        return true /* current key deleted */;
      } else {
        return false /* current key not deleted */;
      }
    } else {
      return false /* current key not deleted */;
      // range tombstone from an older sorted run with current key < end key.
      // current key is not deleted and the older sorted run will have its range
      // tombstone updated when the range tombstone's end key are popped from
      // minHeap_.
    }
  }
  // we can reach here only if active_ is empty
  assert(active_.empty());
  assert(minHeap_.top()->type == HeapItem::Type::ITERATOR);
  return false /* current key not deleted */;
}

MergingIterMethod(void)SeekForPrevImpl(const Slice& target,
                                      size_t starting_level,
                                      bool range_tombstone_reseek) {
  // active range tombstones before `starting_level` remain active
  ClearHeaps(false /* clear_active */);
  InitMaxHeap();
  ParsedInternalKey pik;
  if (!range_tombstone_iters_.empty()) {
    ParseInternalKey(target, &pik, false).PermitUncheckedError();
  }
  for (size_t level = 0; level < starting_level; ++level) {
    PERF_TIMER_GUARD(seek_max_heap_time);
    AddToMaxHeapOrCheckStatus(&children_[level]);
  }
  if (!range_tombstone_iters_.empty()) {
    // Add range tombstones before starting_level.
    for (size_t level = 0; level < starting_level; ++level) {
      if (range_tombstone_iters_[level] &&
          range_tombstone_iters_[level]->Valid()) {
        assert(static_cast<bool>(active_.count(level)) ==
               (pinned_heap_item_[level].type ==
                HeapItem::Type::DELETE_RANGE_START));
        maxHeap_->push(&pinned_heap_item_[level]);
      } else {
        assert(!active_.count(level));
      }
    }
    // levels >= starting_level will be reseeked below,
    active_.erase(active_.lower_bound(starting_level), active_.end());
  }

  IterKey current_search_key;
  current_search_key.SetInternalKey(target, false /* copy */);
  // Seek target might change to some range tombstone end key, so
  // we need to remember them for async requests.
  // (level, target) pairs
  autovector<std::pair<size_t, std::string>> prefetched_target;
  for (auto level = starting_level; level < children_.size(); ++level) {
    {
      PERF_TIMER_GUARD(seek_child_seek_time);
      children_[level].iter.SeekForPrev(current_search_key.GetInternalKey());
    }

    PERF_COUNTER_ADD(seek_child_seek_count, 1);

    if (!range_tombstone_iters_.empty()) {
      if (range_tombstone_reseek) {
        // This seek is to some range tombstone end key.
        // Should only happen when there are range tombstones.
        PERF_COUNTER_ADD(internal_range_del_reseek_count, 1);
      }
      if (children_[level].iter.status().IsTryAgain()) {
        prefetched_target.emplace_back(
            level, current_search_key.GetInternalKey().ToString());
      }
      auto range_tombstone_iter = range_tombstone_iters_[level];
      if (range_tombstone_iter) {
        range_tombstone_iter->SeekForPrev(current_search_key.GetUserKey());
        if (range_tombstone_iter->Valid()) {
          InsertRangeTombstoneToMaxHeap(
              level, comparator_->Compare(range_tombstone_iter->end_key(),
                                          pik) <= 0 /* end_key */);
          // start key <= current_search_key guaranteed by the Seek() call above
          // Only interested in user key coverage since older sorted runs must
          // have smaller sequence numbers than this tombstone.
          if (comparator_->user_comparator()->Compare(
                  current_search_key.GetUserKey(),
                  range_tombstone_iter->end_key().user_key) < 0) {
            range_tombstone_reseek = true;
            current_search_key.SetInternalKey(
                range_tombstone_iter->start_key().user_key, kMaxSequenceNumber,
                kValueTypeForSeekForPrev);
          }
        }
      }
    }
    // child.iter.status() is set to Status::TryAgain indicating asynchronous
    // request for retrieval of data blocks has been submitted. So it should
    // return at this point and Seek should be called again to retrieve the
    // requested block and add the child to min heap.
    if (children_[level].iter.status().IsTryAgain()) {
      continue;
    }
    {
      // Strictly, we timed slightly more than min heap operation,
      // but these operations are very cheap.
      PERF_TIMER_GUARD(seek_max_heap_time);
      AddToMaxHeapOrCheckStatus(&children_[level]);
    }
  }

  if (range_tombstone_iters_.empty()) {
    for (auto& child : children_) {
      if (child.iter.status().IsTryAgain()) {
        child.iter.SeekForPrev(target);
        {
          PERF_TIMER_GUARD(seek_min_heap_time);
          AddToMaxHeapOrCheckStatus(&child);
        }
        PERF_COUNTER_ADD(number_async_seek, 1);
      }
    }
  } else {
    for (auto& prefetch : prefetched_target) {
      // (level, target) pairs
      children_[prefetch.first].iter.SeekForPrev(prefetch.second);
      {
        PERF_TIMER_GUARD(seek_max_heap_time);
        AddToMaxHeapOrCheckStatus(&children_[prefetch.first]);
      }
      PERF_COUNTER_ADD(number_async_seek, 1);
    }
  }
}

// See more in comments above SkipNextDeleted().
// REQUIRES:
// - max heap is currently not empty, and iter is in kReverse direction.
// - maxHeap_ top is not DELETE_RANGE_END (so that `active_` is current).
MergingIterMethod(bool)SkipPrevDeleted() {
  // 3 types of keys:
  // - point key
  // - file boundary sentinel keys
  // - range deletion start key
  auto current = maxHeap_->top();
  if (current->type == HeapItem::Type::DELETE_RANGE_START) {
    active_.erase(current->level);
    assert(range_tombstone_iters_[current->level] &&
           range_tombstone_iters_[current->level]->Valid());
    range_tombstone_iters_[current->level]->Prev();
    if (range_tombstone_iters_[current->level]->Valid()) {
      InsertRangeTombstoneToMaxHeap(current->level, true /* end_key */,
                                    true /* replace_top */);
    } else {
      maxHeap_->pop();
    }
    return true /* current key deleted */;
  }
  if (current->iter.IsDeleteRangeSentinelKey()) {
    // LevelIterator enters a new SST file
    maxHeap_->pop();
    // Remove last SST file's range tombstone key if there is one.
    if (!maxHeap_->empty() && maxHeap_->top()->level == current->level &&
        maxHeap_->top()->type == HeapItem::Type::DELETE_RANGE_START) {
      maxHeap_->pop();
      active_.erase(current->level);
    }
    current->iter.Prev();
    if (current->iter.Valid()) {
      assert(current->iter.status().ok());
      UpdatePrefixCache(current);
      maxHeap_->push(current);
    } else {
      considerStatus(current->iter.status());
    }

    if (range_tombstone_iters_[current->level] &&
        range_tombstone_iters_[current->level]->Valid()) {
      InsertRangeTombstoneToMaxHeap(current->level);
    }
    return true /* current key deleted */;
  }
  assert(current->type == HeapItem::Type::ITERATOR);
  // Point key case: check active_ for range tombstone coverage.
  ParsedInternalKey pik(current->iter.key());
  if (!active_.empty()) {
    auto i = *active_.begin();
    if (i < current->level) {
      // range tombstone is from a newer level, definitely covers
      assert(comparator_->Compare(range_tombstone_iters_[i]->start_key(),
                                  pik) <= 0);
      assert(comparator_->Compare(pik, range_tombstone_iters_[i]->end_key()) <
             0);
      std::string target;
      AppendInternalKey(&target, range_tombstone_iters_[i]->start_key());
      // This is different from SkipNextDeleted() which does reseek at sorted
      // runs >= level (instead of i+1 here). With min heap, if level L is at
      // top of the heap, then levels <L all have internal keys > level L's
      // current internal key, which means levels <L are already at a different
      // user key. With max heap, if level L is at top of the heap, then levels
      // <L all have internal keys smaller than level L's current internal key,
      // which might still be the same user key.
      SeekForPrevImpl(target, i + 1, true);
      return true /* current key deleted */;
    } else if (i == current->level) {
      // By `active_` we know current key is between start key and end key.
      assert(comparator_->Compare(range_tombstone_iters_[i]->start_key(),
                                  pik) <= 0);
      assert(comparator_->Compare(pik, range_tombstone_iters_[i]->end_key()) <
             0);
      if (pik.sequence < range_tombstone_iters_[current->level]->seq()) {
        current->iter.Prev();
        if (current->iter.Valid()) {
          UpdatePrefixCache(current);
          maxHeap_->replace_top(current);
        } else {
          considerStatus(current->iter.status());
          maxHeap_->pop();
        }
        return true /* current key deleted */;
      } else {
        return false /* current key not deleted */;
      }
    } else {
      return false /* current key not deleted */;
    }
  }

  assert(active_.empty());
  assert(maxHeap_->top()->type == HeapItem::Type::ITERATOR);
  return false /* current key not deleted */;
}

MergingIterMethod(void)AddToMinHeapOrCheckStatus(HeapItem* child) {
  // Invariant(children_)
  if (child->iter.Valid()) {
    assert(child->iter.status().ok());
    minHeap_.push(child);
  } else {
    considerStatus(child->iter.status());
  }
}

MergingIterMethod(void)AddToMaxHeapOrCheckStatus(HeapItem* child) {
  if (child->iter.Valid()) {
    assert(child->iter.status().ok());
    maxHeap_->push(child);
  } else {
    considerStatus(child->iter.status());
  }
}

// Advance all non current_ child to > current_.key().
// We advance current_ after the this function call as it does not require
// Seek().
// Advance all range tombstones iters, including the one corresponding to
// current_, to the first tombstone with end_key > current_.key().
// TODO: potentially do cascading seek here too
// TODO: show that invariants hold
MergingIterMethod(void)SwitchToForward() {
  ClearHeaps();
  Slice target = key();
  for (auto& child : children_) {
    if (&child.iter != current_) {
      child.iter.Seek(target);
      // child.iter.status() is set to Status::TryAgain indicating asynchronous
      // request for retrieval of data blocks has been submitted. So it should
      // return at this point and Seek should be called again to retrieve the
      // requested block and add the child to min heap.
      if (child.iter.status() == Status::TryAgain()) {
        continue;
      }
      if (child.iter.Valid() && comparator_->Equal(target, child.iter.key())) {
        assert(child.iter.status().ok());
        child.iter.Next();
      }
    }
    AddToMinHeapOrCheckStatus(&child);
  }

  for (auto& child : children_) {
    if (child.iter.status() == Status::TryAgain()) {
      child.iter.Seek(target);
      if (child.iter.Valid() && comparator_->Equal(target, child.iter.key())) {
        assert(child.iter.status().ok());
        child.iter.Next();
      }
      AddToMinHeapOrCheckStatus(&child);
    }
  }

  // Current range tombstone iter also needs to seek for the following case:
  // Previous direction is backward, so range tombstone iter may point to a
  // tombstone before current_. If there is no such tombstone, then the range
  // tombstone iter is !Valid(). Need to reseek here to make it valid again.
  if (!range_tombstone_iters_.empty()) {
    ParsedInternalKey pik(target);
    for (size_t i = 0; i < range_tombstone_iters_.size(); ++i) {
      auto iter = range_tombstone_iters_[i];
      if (iter) {
        iter->Seek(pik.user_key);
        // The while loop is needed as the Seek() call above is only for user
        // key. We could have a range tombstone with end_key covering user_key,
        // but still is smaller than target. This happens when the range
        // tombstone is truncated at iter.largest_.
        while (iter->Valid() &&
               comparator_->Compare(iter->end_key(), pik) <= 0) {
          iter->Next();
        }
        if (range_tombstone_iters_[i]->Valid()) {
          InsertRangeTombstoneToMinHeap(
              i, comparator_->Compare(range_tombstone_iters_[i]->start_key(),
                                      pik) > 0 /* start_key */);
        }
      }
    }
  }

  direction_ = kForward;
  assert(current_ == CurrentForward());
}

// Advance all range tombstones iters, including the one corresponding to
// current_, to the first tombstone with start_key <= current_.key().
MergingIterMethod(void)SwitchToBackward() {
  ClearHeaps();
  InitMaxHeap();
  Slice target = key();
  for (auto& child : children_) {
    if (&child.iter != current_) {
      child.iter.SeekForPrev(target);
      TEST_SYNC_POINT_CALLBACK("MergeIterator::Prev:BeforePrev", &child);
      if (child.iter.Valid() && comparator_->Equal(target, child.iter.key())) {
        assert(child.iter.status().ok());
        child.iter.Prev();
      }
    }
    AddToMaxHeapOrCheckStatus(&child);
  }

  ParsedInternalKey pik(target);
  for (size_t i = 0; i < range_tombstone_iters_.size(); ++i) {
    auto iter = range_tombstone_iters_[i];
    if (iter) {
      iter->SeekForPrev(pik.user_key);
      // Since the SeekForPrev() call above is only for user key,
      // we may end up with some range tombstone with start key having the
      // same user key at current_, but with a smaller sequence number. This
      // makes current_ not at maxHeap_ top for the CurrentReverse() call
      // below. If there is a range tombstone start key with the same user
      // key and the same sequence number as current_.key(), it will be fine as
      // in InsertRangeTombstoneToMaxHeap() we change op_type to be the smallest
      // op_type.
      while (iter->Valid() &&
             comparator_->Compare(iter->start_key(), pik) > 0) {
        iter->Prev();
      }
      if (iter->Valid()) {
        InsertRangeTombstoneToMaxHeap(
            i, comparator_->Compare(range_tombstone_iters_[i]->end_key(),
                                    pik) <= 0 /* end_key */);
      }
    }
  }

  direction_ = kReverse;
  if (!prefix_seek_mode_) {
    // Note that we don't do assert(current_ == CurrentReverse()) here
    // because it is possible to have some keys larger than the seek-key
    // inserted between Seek() and SeekToLast(), which makes current_ not
    // equal to CurrentReverse().
    current_ = CurrentReverse();
  }
  assert(current_ == CurrentReverse());
}

MergingIterMethod(void)ClearHeaps(bool clear_active) {
  minHeap_.clear();
  if (clear_active) {
    active_.clear();
  }
}

MergingIterMethod(inline void)InitMaxHeap() {
  maxHeap_.clear();
}

// Assume there is a next key that is not covered by range tombstone.
// Pre-condition:
// - Invariants (3) and (4)
// - There is some k where k <= children_[i].iter.key() <= LevelNextVisible(i,
// k) for all levels i (LevelNextVisible() defined in Seek()).
//
// Define NextVisible(k) to be the first key >= k from among children_ that
// is not covered by any range tombstone.
// Post-condition:
// - Invariants (1)-(4) hold
// - (*): minHeap_->top()->key() == NextVisible(k)
//
// Loop invariants:
// - Invariants (3) and (4)
// - (*): k <= children_[i].iter.key() <= LevelNextVisible(i, k)
//
// Progress: minHeap_.top()->key() is non-decreasing and strictly increases in
// a finite number of iterations.
// TODO: it is possible to call SeekImpl(k2) after SeekImpl(k1) with
//  k2 < k1 in the same FindNextVisibleKey(). For example, l1 has a range
//  tombstone [2,3) and l2 has a range tombstone [1, 4). Point key 1 from l5
//  triggers SeekImpl(4 /* target */, 5). Then point key 2 from l3 triggers
//  SeekImpl(3 /* target */, 3).
//  Ideally we should only move iterators forward in SeekImpl(), and the
//  progress condition can be made simpler: iterator only moves forward.
//
// Proof sketch:
// Post-condition:
// Invariant (1) holds when this method returns:
// Ignoring the empty minHeap_ case, there are two cases:
// Case 1: active_ is empty and !minHeap_.top()->iter.IsDeleteRangeSentinelKey()
// By invariants (rti) and (active_), active_ being empty means if a
// pinned_heap_item_[i] is in minHeap_, it has type DELETE_RANGE_START. Note
// that PopDeleteRangeStart() was called right before the while loop condition,
// so minHeap_.top() is not of type DELETE_RANGE_START. So minHeap_.top() must
// be of type ITERATOR.
// Case 2: SkipNextDeleted() returns false. The method returns false only when
// minHeap_.top().type == ITERATOR.
//
// Invariant (2) holds when this method returns:
// From Invariant (1), minHeap_.top().type == ITERATOR. Suppose it is
// children_[i] for some i. Suppose that children_[i].iter.key() is covered by
// some range tombstone. This means there is a j <= i and a range tombstone from
// level j with start_key() < children_[i].iter.key() < end_key().
// - If range_tombstone_iters_[j]->Valid(), by Invariants (rti) and (phi),
// pinned_heap_item_[j] is in minHeap_, and pinned_heap_item_[j].tombstone_pik
// is either start or end key of this range tombstone. If
// pinned_heap_item_[j].tombstone_pik < children_[i].iter.key(), it would be at
// top of minHeap_ which would contradict Invariant (1). So
// pinned_heap_item_[j].tombstone_pik > children_[i].iter.key().
// By Invariant (3), range_tombstone_iters_[j].prev.end_key() <
// children_[i].iter.key(). We assume that in each level, range tombstones
// cover non-overlapping ranges. So range_tombstone_iters_[j] is at
// the range tombstone with start_key() < children_[i].iter.key() < end_key()
// and has its end_key() in minHeap_. By Invariants (phi) and (active_),
// j is in active_. From while loop condition, SkipNextDeleted() must have
// returned false for this method to return.
//   - If j < i, then SeekImpl(range_tombstone_iters_[j']->end_key(), i)
// was called for some j' < i and j' in active_. Note that since j' is in
// active_, pinned_heap_item_[j'] is in minHeap_ and has tombstone_pik =
// range_tombstone_iters_[j']->end_key(). So
// range_tombstone_iters_[j']->end_key() must be larger than
// children_[i].iter.key() to not be at top of minHeap_. This means after
// SeekImpl(), children_[i] would be at a key > children_[i].iter.key()
// -- contradiction.
//   - If j == i, children_[i]->Next() would have been called and children_[i]
// would be at a key > children_[i].iter.key() -- contradiction.
// - If !range_tombstone_iters_[j]->Valid(). Then range_tombstone_iters_[j]
// points to an SST file with all range tombstones from that file exhausted.
// The file must come before the file containing the first
// range tombstone with start_key() < children_[i].iter.key() < end_key().
// Assume files from same level have non-overlapping ranges, the current file's
// meta.largest is less than children_[i].iter.key(). So the file boundary key,
// which has value meta.largest must have been popped from minHeap_ before
// children_[i].iter.key(). So range_tombstone_iters_[j] would not point to
// this SST file -- contradiction.
// So it is impossible for children_[i].iter.key() to be covered by a range
// tombstone.
//
// Post-condition (*) holds when the function returns:
// From loop invariant (*) that k <= children_[i].iter.key() <=
// LevelNextVisible(i, k) and Invariant (2) above, when the function returns,
// minHeap_.top()->key() is the smallest LevelNextVisible(i, k) among all levels
// i. This is equal to NextVisible(k).
//
// Invariant (3) holds after each iteration:
// PopDeleteRangeStart() does not change range tombstone position.
// In SkipNextDeleted():
//   - If DELETE_RANGE_END is popped from minHeap_, it means the range
//   tombstone's end key is < all other point keys, so it is safe to advance to
//   next range tombstone.
//   - If file boundary is popped (current->iter.IsDeleteRangeSentinelKey()),
//   we assume that file's last range tombstone's
//   end_key <= file boundary key < all other point keys. So it is safe to
//   move to the first range tombstone in the next SST file.
//   - If children_[i]->Next() is called, then it is fine as it is advancing a
//   point iterator.
//   - If SeekImpl(target, l) is called, then (3) follows from SeekImpl()'s
//   post-condition if its pre-condition holds. First pre-condition follows
//   from loop invariant where Invariant (3) holds for all levels i.
//   Now we should second pre-condition holds. Since Invariant (3) holds for
//   all i, we have for all j <= l, range_tombstone_iters_[j].prev.end_key()
//   < children_[l].iter.key(). `target` is the value of
//   range_tombstone_iters_[j'].end_key() for some j' < l and j' in active_.
//   By Invariant (active_) and (rti), pinned_heap_item_[j'] is in minHeap_ and
//   pinned_heap_item_[j'].tombstone_pik = range_tombstone_iters_[j'].end_key().
//   This end_key must be larger than children_[l].key() since it was not at top
//   of minHeap_. So for all levels j <= l,
//   range_tombstone_iters_[j].prev.end_key() < children_[l].iter.key() < target
//
// Invariant (4) holds after each iteration:
// A level i is inserted into active_ during calls to PopDeleteRangeStart().
// In that case, range_tombstone_iters_[i].start_key() < all point keys
// by heap property and the assumption that point keys and range tombstone keys
// are distinct.
// If SeekImpl(target, l) is called, then there is a range_tombstone_iters_[j]
// where target = range_tombstone_iters_[j]->end_key() and children_[l]->key()
// < target. By loop invariants, (3) and (4) holds for levels.
// Since target > children_[l]->key(), it also holds that for j < l,
// range_tombstone_iters_[j].prev.end_key() < target and that if j in active_,
// range_tombstone_iters_[i]->start_key() < target. So all pre-conditions of
// SeekImpl(target, l) holds, and (4) follow from its post-condition.
// All other places either in this function either advance point iterators
// or remove some level from active_, so (4) still holds.
//
// Look Invariant (*): for all level i, k <= children_[i] <= LevelNextVisible(i,
// k).
// k <= children_[i] follows from loop `progress` condition.
// Consider when children_[i] is changed for any i. It is through
// children_[i].iter.Next() or SeekImpl() in SkipNextDeleted().
// If children_[i].iter.Next() is called, there is a range tombstone from level
// i where tombstone seqno > children_[i].iter.key()'s seqno and i in active_.
// By Invariant (4), tombstone's start_key < children_[i].iter.key(). By
// invariants (active_), (phi), and (rti), tombstone's end_key is in minHeap_
// and that children_[i].iter.key() < end_key. So children_[i].iter.key() is
// not visible, and it is safe to call Next().
// If SeekImpl(target, l) is called, by its contract, when SeekImpl() returns,
// target <= children_[i]->key() <= LevelNextVisible(i, target) for i >= l,
// and children_[<l] is not touched. We know `target` is
// range_tombstone_iters_[j]->end_key() for some j < i and j is in active_.
// By Invariant (4), range_tombstone_iters_[j]->start_key() <
// children_[i].iter.key() for all i >= l. So for each level i >= l, the range
// [children_[i].iter.key(), target) is not visible. So after SeekImpl(),
// children_[i].iter.key() <= LevelNextVisible(i, target) <=
// LevelNextVisible(i, k).
//
// `Progress` holds for each iteration:
// Very sloppy intuition:
// - in PopDeleteRangeStart(): the value of a pinned_heap_item_.tombstone_pik_
// is updated from the start key to the end key of the same range tombstone.
// We assume that start key <= end key for the same range tombstone.
// - in SkipNextDeleted()
//   - If the top of heap is DELETE_RANGE_END, the range tombstone is advanced
//     and the relevant pinned_heap_item_.tombstone_pik is increased or popped
//     from minHeap_.
//   - If the top of heap is a file boundary key, then both point iter and
//     range tombstone iter are advanced to the next file.
//   - If the top of heap is ITERATOR and current->iter.Next() is called, it
//     moves to a larger point key.
//   - If the top of heap is ITERATOR and SeekImpl(k, l) is called, then all
//     iterators from levels >= l are advanced to some key >= k by its contract.
//     And top of minHeap_ before SeekImpl(k, l) was less than k.
// There are special cases where different heap items have the same key,
// e.g. when two range tombstone end keys share the same value). In
// these cases, iterators are being advanced, so the minimum key should increase
// in a finite number of steps.

// Repeatedly check and remove heap top key if it is not a point key
// that is not covered by range tombstones. SeekImpl() is called to seek to end
// of a range tombstone if the heap top is a point key covered by some range
// tombstone from a newer sorted run. If the covering tombstone is from current
// key's level, then the current child iterator is simply advanced to its next
// key without reseeking.
MergingIterMethod(inline void)FindNextVisibleKey() {
  if (LIKELY(range_tombstone_iters_.empty())) {
    return;
  }
  FindNextVisibleKeySlowPath();
}
MergingIterMethod(void)FindNextVisibleKeySlowPath() {
  // When active_ is empty, we know heap top cannot be a range tombstone end
  // key. It cannot be a range tombstone start key per PopDeleteRangeStart().
  PopDeleteRangeStart();
  // PopDeleteRangeStart() implies heap top is not DELETE_RANGE_START
  // active_ being empty implies no DELETE_RANGE_END in heap.
  // So minHeap_->top() must be of type ITERATOR.
  while (
      !minHeap_.empty() &&
      (!active_.empty() || minHeap_.top()->iter.IsDeleteRangeSentinelKey()) &&
      SkipNextDeleted()) {
    PopDeleteRangeStart();
  }
  // Checks Invariant (1)
  assert(minHeap_.empty() || minHeap_.top()->type == HeapItem::Type::ITERATOR);
}

MergingIterMethod(inline void)FindPrevVisibleKey() {
  if (LIKELY(range_tombstone_iters_.empty())) {
    return;
  }
  FindPrevVisibleKeySlowPath();
}
MergingIterMethod(void)FindPrevVisibleKeySlowPath() {
  PopDeleteRangeEnd();
  // PopDeleteRangeEnd() implies heap top is not DELETE_RANGE_END
  // active_ being empty implies no DELETE_RANGE_START in heap.
  // So maxHeap_->top() must be of type ITERATOR.
  while (
      !maxHeap_->empty() &&
      (!active_.empty() || maxHeap_->top()->iter.IsDeleteRangeSentinelKey()) &&
      SkipPrevDeleted()) {
    PopDeleteRangeEnd();
  }
}

template<class MinHeapComparator, class MaxHeapComparator, class Item = HeapItemAndPrefix>
static MergingIterator* NewIterTpl(const InternalKeyComparator* cmp,
                                   InternalIterator** list, int n,
                                   Arena* arena, bool prefix_seek_mode,
                                   const Slice* upper_bound) {
  using Iter = MergingIterTmpl<MinHeapComparator, MaxHeapComparator, Item>;
  if (arena == nullptr) {
    return new Iter(cmp, list, n, false, prefix_seek_mode, upper_bound);
  } else {
    auto mem = arena->AllocateAligned(sizeof(Iter));
    return new (mem) Iter(cmp, list, n, true, prefix_seek_mode, upper_bound);
  }
}

static MergingIterator* NewIter(const InternalKeyComparator* cmp,
                                InternalIterator** list, int n,
                                Arena* arena, bool prefix_seek_mode,
                                const Slice* upper_bound) {
  if (cmp->IsForwardBytewise()) {
    return NewIterTpl<MinHeapBytewiseComp, MaxHeapBytewiseComp>
        (cmp, list, n, arena, prefix_seek_mode, upper_bound);
  } else if (cmp->IsReverseBytewise()) {
    return NewIterTpl<MinHeapRevBytewiseComp, MaxHeapRevBytewiseComp>
        (cmp, list, n, arena, prefix_seek_mode, upper_bound);
  } else {
    return NewIterTpl<MinHeapItemComparator, MaxHeapItemComparator, HeapItem*>
        (cmp, list, n, arena, prefix_seek_mode, upper_bound);
  }
}

InternalIterator* NewMergingIterator(const InternalKeyComparator* cmp,
                                     InternalIterator** list, int n,
                                     Arena* arena, bool prefix_seek_mode) {
  assert(n >= 0);
  if (n == 0) {
    return NewEmptyInternalIterator<Slice>(arena);
  } else if (n == 1) {
    return list[0];
  } else {
    return NewIter(cmp, list, n, arena, prefix_seek_mode, nullptr);
  }
}

MergeIteratorBuilder::MergeIteratorBuilder(
    const InternalKeyComparator* comparator, Arena* a, bool prefix_seek_mode,
    const Slice* iterate_upper_bound)
    : first_iter(nullptr), use_merging_iter(false), arena(a) {
  ROCKSDB_VERIFY(nullptr != arena);
  merge_iter = NewIter(comparator, nullptr, 0, a, prefix_seek_mode, iterate_upper_bound);
}

MergeIteratorBuilder::~MergeIteratorBuilder() {
  if (first_iter != nullptr) {
    first_iter->~InternalIterator();
  }
  if (merge_iter != nullptr) {
    merge_iter->~MergingIterator();
  }
}

void MergeIteratorBuilder::AddIterator(InternalIterator* iter) {
  if (!use_merging_iter && first_iter != nullptr) {
    merge_iter->AddIterator(first_iter);
    use_merging_iter = true;
    first_iter = nullptr;
  }
  if (use_merging_iter) {
    merge_iter->AddIterator(iter);
  } else {
    first_iter = iter;
  }
}

void MergeIteratorBuilder::AddPointAndTombstoneIterator(
    InternalIterator* point_iter, TruncatedRangeDelIterator* tombstone_iter,
    TruncatedRangeDelIterator*** tombstone_iter_ptr) {
  // tombstone_iter_ptr != nullptr means point_iter is a LevelIterator.
  bool add_range_tombstone = tombstone_iter ||
                             !merge_iter->range_tombstone_iters_.empty() ||
                             tombstone_iter_ptr;
  if (!use_merging_iter && (add_range_tombstone || first_iter)) {
    use_merging_iter = true;
    if (first_iter) {
      merge_iter->AddIterator(first_iter);
      first_iter = nullptr;
    }
  }
  if (use_merging_iter) {
    merge_iter->AddIterator(point_iter);
    if (add_range_tombstone) {
      // If there was a gap, fill in nullptr as empty range tombstone iterators.
      while (merge_iter->range_tombstone_iters_.size() <
             merge_iter->children_.size() - 1) {
        merge_iter->AddRangeTombstoneIterator(nullptr);
      }
      merge_iter->AddRangeTombstoneIterator(tombstone_iter);
    }

    if (tombstone_iter_ptr) {
      // This is needed instead of setting to &range_tombstone_iters_[i]
      // directly here since the memory address of range_tombstone_iters_[i]
      // might change during vector resizing.
      range_del_iter_ptrs_.emplace_back(
          merge_iter->range_tombstone_iters_.size() - 1, tombstone_iter_ptr);
    }
  } else {
    first_iter = point_iter;
  }
}

InternalIterator* MergeIteratorBuilder::Finish(ArenaWrappedDBIter* db_iter) {
  InternalIterator* ret = nullptr;
  if (!use_merging_iter) {
    ret = first_iter;
    first_iter = nullptr;
  } else {
    if (range_del_iter_ptrs_.empty() &&
        merge_iter->range_tombstone_iters_.empty()) {
      merge_iter->OptimizeVtable();
    }
    for (auto& p : range_del_iter_ptrs_) {
      *(p.second) = &(merge_iter->range_tombstone_iters_[p.first]);
    }
    if (db_iter && !merge_iter->range_tombstone_iters_.empty()) {
      // memtable is always the first level
      db_iter->SetMemtableRangetombstoneIter(
          &merge_iter->range_tombstone_iters_.front());
    }
    merge_iter->Finish();
    ret = merge_iter;
    merge_iter = nullptr;
  }
  return ret;
}

}  // namespace ROCKSDB_NAMESPACE
