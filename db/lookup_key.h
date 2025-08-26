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
class LookupKey {
 public:
  // Initialize *this for looking up user_key at a snapshot with
  // the specified sequence number.
  LookupKey(const Slice _user_key, SequenceNumber sequence);
  LookupKey(const Slice _user_key, SequenceNumber sequence, const Slice* ts);

  ~LookupKey();

  const char* memtable_key_data() const {
    if (LIKELY(klength_ <= sizeof(space_)))
      return space_ - short_klen_len;
    else
      return longstart_ - klen_len_;
  }

#if 0 // not used now
  // Return a key suitable for lookup in a MemTable.
  Slice memtable_key() const {
    if (LIKELY(klength_ <= sizeof(space_)))
      return Slice(space_ - short_klen_len, short_klen_len + klength_);
    else
      return Slice(longstart_ - klen_len_, klen_len_ + klength_);
  }
#endif

  // Return an internal key (suitable for passing to an internal iterator)
  Slice internal_key() const {
    if (LIKELY(klength_ <= sizeof(space_)))
      return Slice(space_, klength_);
    else
      return Slice(longstart_, klength_);
  }

  // Return the user key.
  // If user-defined timestamp is enabled, then timestamp is included in the
  // result.
  Slice user_key() const {
    if (LIKELY(klength_ <= sizeof(space_)))
      return Slice(space_, klength_ - 8);
    else
      return Slice(longstart_, klength_ - 8);
  }

 private:
  // We construct a char array of the form:
  // short keys: klength_ <= sizeof(space_)
  //     klen_len               <-- short VarUint length, always is 1
  //     unused                 <-- klen_data_[0 ~ 2),
  //     klength  varint32      <-- klen_data_[2]
  //     userkey  char          <-- space_[0 ~ ukey_len), aligned to 8
  //     tag      uint64
  // long keys: klength_ > sizeof(space_)
  //     klen_len_
  //     unused
  //     longstart_             <-- ptr to key data, klen_offset = 8 - klen_len
  //        unused              <-- longstart_[-8 ~ -8 + klen_offset)
  //        klength  varint32   <-- longstart_[-klen_len, 0)
  //        userkey  char       <-- longstart_[0 ~ ukey_len), aligned to 8
  //        tag      uint64
  //
  // The array is a suitable MemTable key.
  // The suffix starting with "userkey" can be used as an InternalKey.
  static constexpr int short_klen_len = 1; // short VarUint length is 1 byte
  uint32_t  klength_; // internal key len
  char klen_len_;
  char klen_data_[3]; // Just for short keys, meaningless for long keys
  union {
    char space_[120]; // Avoid allocation for short keys
    const char* longstart_; // for long  keys
  };

  // No copying allowed
  LookupKey(const LookupKey&);
  void operator=(const LookupKey&);
};

static_assert(sizeof(LookupKey) == 128);

inline LookupKey::~LookupKey() {
  if (UNLIKELY(klength_ > sizeof(space_))) {
    assert(size_t(longstart_) % 8 == 0); // must be aligned to 8
    delete[] (longstart_ - 8);
  }
}

}  // namespace ROCKSDB_NAMESPACE
