//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <string>
#include <utility>

#include "rocksdb/slice.h"
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {

// A helper class useful for DBImpl::Get()
class LookupKey {
 public:
  // Initialize *this for looking up user_key at a snapshot with
  // the specified sequence number.
  LookupKey(const Slice& _user_key, SequenceNumber sequence,
            const Slice* ts = nullptr);

  ~LookupKey();

  const char* memtable_key_data() const { return kstart_ - kstart_[-4]; }

#if 0 // not used now
  // Return a key suitable for lookup in a MemTable.
  Slice memtable_key() const {
    size_t klen_len = kstart_[-4];
    return Slice(kstart_ - klen_len, klen_len + klength_);
  }
#endif

  // Return an internal key (suitable for passing to an internal iterator)
  Slice internal_key() const { return Slice(kstart_, klength_); }

  // Return the user key.
  // If user-defined timestamp is enabled, then timestamp is included in the
  // result.
  Slice user_key() const { return Slice(kstart_, klength_ - 8); }

 private:
  // We construct a char array of the form:
  // buf = kstart_ - 4
  // buf[0] is offset of varint32 encoded klength
  // max klength is 3 bytes varint32, which is 2**(7*3) = 2M
  //     klen_len                     <-- buf[0], klen_offset = 4 - klen_len
  //     unused                       <-- buf[1 ~ klen_offset),
  //     klength  varint32            <-- buf[klen_offset ~ 4)
  //     userkey  char[user key len]  <-- buf + 4 = kstart_, aligned to 8
  //     tag      uint64
  // The array is a suitable MemTable key.
  // The suffix starting with "userkey" can be used as an InternalKey.
  const char* kstart_;
  uint32_t    klength_; // internal key len
  char space_[116];  // Avoid allocation for short keys

  // No copying allowed
  LookupKey(const LookupKey&);
  void operator=(const LookupKey&);
};
static_assert(sizeof(LookupKey) == 128);

inline LookupKey::~LookupKey() {
  assert(size_t(kstart_) % 8 == 0); // must be aligned to 8
  if (kstart_ != space_ + 4) delete[] (kstart_ - 8);
}

}  // namespace ROCKSDB_NAMESPACE
