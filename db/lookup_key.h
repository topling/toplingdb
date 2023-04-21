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
#include "port/likely.h"

namespace ROCKSDB_NAMESPACE {

// A helper class useful for DBImpl::Get()
#pragma pack(push, 1)
class LookupKey {
 public:
  // Initialize *this for looking up user_key at a snapshot with
  // the specified sequence number.
  LookupKey(const Slice& _user_key, SequenceNumber sequence,
            const Slice* ts = nullptr);

  ~LookupKey();

  const char* memtable_key_data() const {
    if (LIKELY(klength_ <= sizeof(space_) - 4))
      return space_ + 4 - klen_len_;
    else
      return longstart_ - klen_len_;
  }

#if 0 // not used now
  // Return a key suitable for lookup in a MemTable.
  Slice memtable_key() const {
    if (LIKELY(klength_ <= sizeof(space_) - 4))
      return Slice(space_ + 4 - klen_len_, klen_len_ + klength_);
    else
      return Slice(longstart_ - klen_len_, klen_len_ + klength_);
  }
#endif

  // Return an internal key (suitable for passing to an internal iterator)
  Slice internal_key() const {
    if (LIKELY(klength_ <= sizeof(space_) - 4))
      return Slice(space_ + 4, klength_);
    else
      return Slice(longstart_, klength_);
  }

  // Return the user key.
  // If user-defined timestamp is enabled, then timestamp is included in the
  // result.
  Slice user_key() const {
    if (LIKELY(klength_ <= sizeof(space_) - 4))
      return Slice(space_ + 4, klength_ - 8);
    else
      return Slice(longstart_, klength_ - 8);
  }

 private:
  // We construct a char array of the form:
  // short keys: klength_ <= sizeof(space_) - 4
  //     klen_len               <-- space_[0], klen_offset = 4 - klen_len
  //     unused                 <-- space_[1 ~ klen_offset),
  //     klength  varint32      <-- space_[klen_offset ~ 4)
  //     userkey  char          <-- space_[4 ~ 4 + ukey_len), aligned to 8
  //     tag      uint64
  // long keys: klength_ > sizeof(space_) - 4
  //     klen_len_              <-- space_[0]
  //     unused                 <-- space_[1~4)
  //     longstart_             <-- ptr to key data, klen_offset = 8 - klen_len
  //        unused              <-- longstart_[-8 ~ -8 + klen_offset)
  //        klength  varint32   <-- longstart_[-klen_len, 0)
  //        userkey  char       <-- longstart_[0 ~ ukey_len), aligned to 8
  //        tag      uint64
  //
  // The array is a suitable MemTable key.
  // The suffix starting with "userkey" can be used as an InternalKey.
  uint32_t  klength_; // internal key len
  union {
    char space_[124]; // Avoid allocation for short keys
    struct {
      char klen_len_;
      char klen_data_[3];     // for short keys
      const char* longstart_; // for long  keys
    };
  };

  // No copying allowed
  LookupKey(const LookupKey&);
  void operator=(const LookupKey&);
};
#pragma pack(pop)

static_assert(sizeof(LookupKey) == 128);

inline LookupKey::~LookupKey() {
  if (UNLIKELY(klength_ > sizeof(space_) - 4)) {
    assert(size_t(longstart_) % 8 == 0); // must be aligned to 8
    delete[] (longstart_ - 8);
  }
}

}  // namespace ROCKSDB_NAMESPACE
