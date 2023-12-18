// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Slice is a simple structure containing a pointer into some external
// storage and a size.  The user of a Slice must ensure that the slice
// is not used after the corresponding external storage has been
// deallocated.
//
// Multiple threads can invoke const methods on a Slice without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same Slice must use
// external synchronization.

#pragma once

#include <cassert>
#include <cstddef>
#include <cstdio>
#include <cstring>
#include <string>
#include <string_view>  // RocksDB now requires C++17 support

#include "rocksdb/cleanable.h"
#include "preproc.h"

namespace ROCKSDB_NAMESPACE {

class Slice {
 public:
  // Create an empty slice.
  Slice() : data_(""), size_(0) {}

  // Create a slice that refers to d[0,n-1].
  Slice(const char* d, size_t n) : data_(d), size_(n) {}

  Slice(const unsigned char* d, size_t n) : data_((const char*)d), size_(n) {}

  Slice(std::nullptr_t, size_t n) : data_(nullptr), size_(n) {}

  // Create a slice that refers to the contents of "s"
  /* implicit */
  Slice(const std::string& s) : data_(s.data()), size_(s.size()) {}

  // Create a slice that refers to the same contents as "sv"
  /* implicit */
  Slice(const std::string_view& sv) : data_(sv.data()), size_(sv.size()) {}

  // Create a slice that refers to s[0,strlen(s)-1]
  /* implicit */
  Slice(const char* s) : data_(s) { size_ = (s == nullptr) ? 0 : strlen(s); }

  // Create a single slice from SliceParts using buf as storage.
  // buf must exist as long as the returned Slice exists.
  Slice(const struct SliceParts& parts, std::string* buf);

  const char* begin() const { return data_; }
  const char* end() const { return data_ + size_; }
  Slice substr(size_t pos) const {
    assert(pos <= size_);
    return Slice(data_ + pos, size_ - pos);
  }
  Slice substr(size_t pos, size_t len) const {
    assert(pos <= size_);
    assert(pos + len <= size_);
    return Slice(data_ + pos, len);
  }

  // Return a pointer to the beginning of the referenced data
  const char* data() const { return data_; }

  // Return the length (in bytes) of the referenced data
  size_t size() const { return size_; }

  // Return true iff the length of the referenced data is zero
  bool empty() const { return size_ == 0; }

  // Return the ith byte in the referenced data.
  // REQUIRES: n < size()
  char operator[](size_t n) const {
    assert(n < size());
    return data_[n];
  }

  // Change this slice to refer to an empty array
  void clear() {
    data_ = "";
    size_ = 0;
  }

  // Drop the first "n" bytes from this slice.
  void remove_prefix(size_t n) {
    assert(n <= size());
    data_ += n;
    size_ -= n;
  }

  void remove_suffix(size_t n) {
    assert(n <= size());
    size_ -= n;
  }

  // Return a string that contains the copy of the referenced data.
  // when hex is true, returns a string of twice the length hex encoded (0-9A-F)
  std::string ToString(bool hex) const;
  std::string ToString() const { return std::string(data_, size_); }
  std::string hex() const { return ToString(true); }

  // Return a string_view that references the same data as this slice.
  std::string_view ToStringView() const {
    return std::string_view(data_, size_);
  }

  // Decodes the current slice interpreted as an hexadecimal string into result,
  // if successful returns true, if this isn't a valid hex string
  // (e.g not coming from Slice::ToString(true)) DecodeHex returns false.
  // This slice is expected to have an even number of 0-9A-F characters
  // also accepts lowercase (a-f)
  bool DecodeHex(std::string* result) const;

  // Three-way comparison.  Returns value:
  //   <  0 iff "*this" <  "b",
  //   == 0 iff "*this" == "b",
  //   >  0 iff "*this" >  "b"
  int compare(const Slice& b) const;

  // Return true iff "x" is a prefix of "*this"
  bool starts_with(const Slice& x) const {
    return ((size_ >= x.size_) && (memcmp(data_, x.data_, x.size_) == 0));
  }

  bool ends_with(const Slice& x) const {
    return ((size_ >= x.size_) &&
            (memcmp(data_ + size_ - x.size_, x.data_, x.size_) == 0));
  }

  // trim spaces
  void trim() {
    while (size_ && isspace((unsigned char)data_[0])) data_++, size_--;
    while (size_ && isspace((unsigned char)data_[size_-1])) size_--;
  }

  // Compare two slices and returns the first byte where they differ
  size_t difference_offset(const Slice& b) const;

  // private: make these public for rocksdbjni access
  const char* data_;
  size_t size_;

  // Intentionally copyable
};

/**
 * A Slice that can be pinned with some cleanup tasks, which will be run upon
 * ::Reset() or object destruction, whichever is invoked first. This can be used
 * to avoid memcpy by having the PinnableSlice object referring to the data
 * that is locked in the memory and release them after the data is consumed.
 */
class PinnableSlice : public Slice, public Cleanable {
 public:
  PinnableSlice() { buf_ = &self_space_; }
  explicit PinnableSlice(std::string* buf) { buf_ = buf; }

  PinnableSlice(PinnableSlice&& other);
  PinnableSlice& operator=(PinnableSlice&& other);

  // No copy constructor and copy assignment allowed.
  PinnableSlice(PinnableSlice&) = delete;
  PinnableSlice& operator=(PinnableSlice&) = delete;

  inline void PinSlice(const Slice& s, CleanupFunction f, void* arg1,
                       void* arg2) {
    assert(!pinned_);
    pinned_ = true;
    data_ = s.data();
    size_ = s.size();
    RegisterCleanup(f, arg1, arg2);
    assert(pinned_);
  }

  inline void PinSlice(const Slice& s, Cleanable* cleanable) {
    assert(!pinned_);
    pinned_ = true;
    data_ = s.data();
    size_ = s.size();
    if (cleanable != nullptr) {
      cleanable->DelegateCleanupsTo(this);
    }
    assert(pinned_);
  }

  inline void SyncToString(std::string* s) const {
    assert(s == buf_);
    if (pinned_) {
      s->assign(data_, size_);
    } else {
      assert(size_ == s->size());
      assert(data_ == s->data() || size_ == 0);
    }
  }
  inline void SyncToString() const { SyncToString(buf_); }

  inline void PinSelf(const Slice& slice) {
    assert(!pinned_);
    buf_->assign(slice.data(), slice.size());
    data_ = buf_->data();
    size_ = buf_->size();
    assert(!pinned_);
  }

  inline void PinSelf() {
    assert(!pinned_);
    data_ = buf_->data();
    size_ = buf_->size();
    assert(!pinned_);
  }

  void remove_suffix(size_t n) {
    assert(n <= size());
    if (pinned_) {
      size_ -= n;
    } else {
      buf_->erase(size() - n, n);
      PinSelf();
    }
  }

  void remove_prefix(size_t n) {
    assert(n <= size());
    if (pinned_) {
      data_ += n;
      size_ -= n;
    } else {
      buf_->erase(0, n);
      PinSelf();
    }
  }

  void Reset() {
    Cleanable::Reset();
    pinned_ = false;
    size_ = 0;
  }

  inline std::string* GetSelf() { return buf_; }

  inline bool IsPinned() const { return pinned_; }

 private:
  friend class PinnableSlice4Test;
  std::string self_space_;
  std::string* buf_;
  bool pinned_ = false;
};

// A set of Slices that are virtually concatenated together.  'parts' points
// to an array of Slices.  The number of elements in the array is 'num_parts'.
struct SliceParts {
  SliceParts(const Slice* _parts, int _num_parts)
      : parts(_parts), num_parts(_num_parts) {}
  SliceParts() : parts(nullptr), num_parts(0) {}

  const Slice* parts;
  int num_parts;
};

inline bool operator==(const Slice& x, const Slice& y) {
  return ((x.size() == y.size()) &&
          (memcmp(x.data(), y.data(), x.size()) == 0));
}

inline bool operator!=(const Slice& x, const Slice& y) { return !(x == y); }

inline int Slice::compare(const Slice& b) const {
  assert(data_ != nullptr && b.data_ != nullptr);
  const size_t min_len = (size_ < b.size_) ? size_ : b.size_;
  int r = memcmp(data_, b.data_, min_len);
  if (r == 0) {
    if (size_ < b.size_)
      r = -1;
    else if (size_ > b.size_)
      r = +1;
  }
  return r;
}

inline bool operator<(const Slice& x, const Slice& y) {
  const size_t min_len = (x.size_ < y.size_) ? x.size_ : y.size_;
  int r = memcmp(x.data_, y.data_, min_len);
  if (r != 0)
    return r < 0;
  else
    return x.size_ < y.size_;
}
inline bool operator>(const Slice& x, const Slice& y) { return y < x; }
inline bool operator>=(const Slice& x, const Slice& y) { return !(x < y); }
inline bool operator<=(const Slice& x, const Slice& y) { return !(y < x); }

inline std::string operator+(const Slice& x, const Slice& y) {
  std::string z; z.reserve(x.size_ + y.size_);
  z.append(x.data_, x.size_);
  z.append(y.data_, y.size_);
  return z;
}

inline size_t Slice::difference_offset(const Slice& b) const {
  size_t off = 0;
  const size_t len = (size_ < b.size_) ? size_ : b.size_;
  for (; off < len; off++) {
    if (data_[off] != b.data_[off]) break;
  }
  return off;
}

template<class ByteArray>
inline Slice SliceOf(const ByteArray& ba) {
  static_assert(sizeof(ba[0]) == 1);
  return Slice((const char*)ba.data(), ba.size());
}

template<class ByteArray>
inline Slice SubSlice(const ByteArray& x, size_t pos) {
  static_assert(sizeof(x.data()[0]) == 1, "ByteArray elem size must be 1");
  ROCKSDB_ASSERT_LE(pos, x.size());
  return Slice((const char*)x.data() + pos, x.size() - pos);
}

template<class ByteArray>
inline Slice SubSlice(const ByteArray& x, size_t pos, size_t len) {
  static_assert(sizeof(x.data()[0]) == 1, "ByteArray elem size must be 1");
  ROCKSDB_ASSERT_LE(pos, x.size());
  ROCKSDB_ASSERT_LE(pos + len, x.size());
  return Slice((const char*)x.data() + pos, len);
}

}  // namespace ROCKSDB_NAMESPACE
