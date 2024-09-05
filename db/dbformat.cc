//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "db/dbformat.h"

#include <stdio.h>

#include <cinttypes>

#include "db/lookup_key.h"
#include "monitoring/perf_context_imp.h"
#include "port/port.h"
#include "util/coding.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

// kValueTypeForSeek defines the ValueType that should be passed when
// constructing a ParsedInternalKey object for seeking to a particular
// sequence number (since we sort sequence numbers in decreasing order
// and the value type is embedded as the low 8 bits in the sequence
// number in internal keys, we need to use the highest-numbered
// ValueType, not the lowest).
const ValueType kValueTypeForSeek = kTypeWideColumnEntity;
const ValueType kValueTypeForSeekForPrev = kTypeDeletion;
const std::string kDisableUserTimestamp("");

EntryType GetEntryType(ValueType value_type) {
  switch (value_type) {
    case kTypeValue:
      return kEntryPut;
    case kTypeDeletion:
      return kEntryDelete;
    case kTypeDeletionWithTimestamp:
      return kEntryDeleteWithTimestamp;
    case kTypeSingleDeletion:
      return kEntrySingleDelete;
    case kTypeMerge:
      return kEntryMerge;
    case kTypeRangeDeletion:
      return kEntryRangeDeletion;
    case kTypeBlobIndex:
      return kEntryBlobIndex;
    case kTypeWideColumnEntity:
      return kEntryWideColumnEntity;
    default:
      return kEntryOther;
  }
}

void AppendInternalKey(std::string* result, const ParsedInternalKey& key) {
  result->reserve(key.user_key.size() + 8);
  result->append(key.user_key.data(), key.user_key.size());
  PutFixed64(result, PackSequenceAndType(key.sequence, key.type));
}

void AppendInternalKeyWithDifferentTimestamp(std::string* result,
                                             const ParsedInternalKey& key,
                                             const Slice& ts) {
  assert(key.user_key.size() >= ts.size());
  result->reserve(key.user_key.size() + 8);
  result->append(key.user_key.data(), key.user_key.size() - ts.size());
  result->append(ts.data(), ts.size());
  PutFixed64(result, PackSequenceAndType(key.sequence, key.type));
}

void AppendInternalKeyFooter(std::string* result, SequenceNumber s,
                             ValueType t) {
  PutFixed64(result, PackSequenceAndType(s, t));
}

void AppendKeyWithMinTimestamp(std::string* result, const Slice& key,
                               size_t ts_sz) {
  assert(ts_sz > 0);
  const std::string kTsMin(ts_sz, static_cast<unsigned char>(0));
  result->append(key.data(), key.size());
  result->append(kTsMin.data(), ts_sz);
}

void AppendKeyWithMaxTimestamp(std::string* result, const Slice& key,
                               size_t ts_sz) {
  assert(ts_sz > 0);
  const std::string kTsMax(ts_sz, static_cast<unsigned char>(0xff));
  result->append(key.data(), key.size());
  result->append(kTsMax.data(), ts_sz);
}

void AppendUserKeyWithMaxTimestamp(std::string* result, const Slice& key,
                                   size_t ts_sz) {
  assert(ts_sz > 0);
  result->append(key.data(), key.size() - ts_sz);

  static constexpr char kTsMax[] = "\xff\xff\xff\xff\xff\xff\xff\xff\xff";
  if (ts_sz < strlen(kTsMax)) {
    result->append(kTsMax, ts_sz);
  } else {
    result->append(std::string(ts_sz, '\xff'));
  }
}

void PadInternalKeyWithMinTimestamp(std::string* result, const Slice& key,
                                    size_t ts_sz) {
  assert(ts_sz > 0);
  size_t user_key_size = key.size() - kNumInternalBytes;
  result->reserve(key.size() + ts_sz);
  result->append(key.data(), user_key_size);
  result->append(ts_sz, static_cast<unsigned char>(0));
  result->append(key.data() + user_key_size, kNumInternalBytes);
}

void StripTimestampFromInternalKey(std::string* result, const Slice& key,
                                   size_t ts_sz) {
  assert(key.size() >= ts_sz + kNumInternalBytes);
  result->reserve(key.size() - ts_sz);
  result->append(key.data(), key.size() - kNumInternalBytes - ts_sz);
  result->append(key.data() + key.size() - kNumInternalBytes,
                 kNumInternalBytes);
}

std::string ParsedInternalKey::DebugString(bool log_err_key, bool hex) const {
  std::string result = "'";
  if (log_err_key) {
    result += user_key.ToString(hex);
  } else {
    result += "<redacted>";
  }

  char buf[50];
  snprintf(buf, sizeof(buf), "' seq:%" PRIu64 ", type:%d", sequence,
           static_cast<int>(type));

  result += buf;
  return result;
}

std::string InternalKey::DebugString(bool hex) const {
  std::string result;
  ParsedInternalKey parsed;
  if (ParseInternalKey(rep_, &parsed, false /* log_err_key */).ok()) {
    result = parsed.DebugString(true /* log_err_key */, hex);  // TODO
  } else {
    result = "(bad)";
    result.append(EscapeString(rep_));
  }
  return result;
}

int InternalKeyComparator::Compare(const ParsedInternalKey& a,
                                   const ParsedInternalKey& b) const {
  // Order by:
  //    increasing user key (according to user-supplied comparator)
  //    decreasing sequence number
  //    decreasing type (though sequence# should be enough to disambiguate)
  int r = user_comparator_.Compare(a.user_key, b.user_key);
  if (r == 0) {
    if (a.sequence > b.sequence) {
      r = -1;
    } else if (a.sequence < b.sequence) {
      r = +1;
    } else if (a.type > b.type) {
      r = -1;
    } else if (a.type < b.type) {
      r = +1;
    }
  }
  return r;
}

int InternalKeyComparator::Compare(const Slice& a,
                                   const ParsedInternalKey& b) const {
  // Order by:
  //    increasing user key (according to user-supplied comparator)
  //    decreasing sequence number
  //    decreasing type (though sequence# should be enough to disambiguate)
  int r = user_comparator_.Compare(ExtractUserKey(a), b.user_key);
  if (r == 0) {
    const uint64_t anum =
        DecodeFixed64(a.data() + a.size() - kNumInternalBytes);
    const uint64_t bnum = (b.sequence << 8) | b.type;
    if (anum > bnum) {
      r = -1;
    } else if (anum < bnum) {
      r = +1;
    }
  }
  return r;
}

int InternalKeyComparator::Compare(const ParsedInternalKey& a,
                                   const Slice& b) const {
  return -Compare(b, a);
}

LookupKey::LookupKey(const Slice& _user_key, SequenceNumber s,
                     const Slice* ts) {
  static_assert(offsetof(LookupKey, longstart_) == 8);
  size_t usize = _user_key.size();
  size_t ts_sz = (nullptr == ts) ? 0 : ts->size();
  klength_ = uint32_t(usize + ts_sz + 8);
  char buf[8];
  auto klen_len = EncodeVarint32(buf, klength_) - buf;
  klen_len_ = char(klen_len);
  char* dst;
  if (LIKELY(klength_ <= sizeof(space_) - 4)) {
    dst = space_ + 4 - klen_len;
  } else {
    char* ptr = new char[usize + ts_sz + 16]; // precise space
    dst = ptr + 8 - klen_len;
    longstart_ = ptr + 8;
  }
  ROCKSDB_ASSUME(klen_len >= 1 && klen_len <= 5);
  memcpy(dst, buf, klen_len); dst += klen_len;
  memcpy(dst, _user_key.data(), usize);
  dst += usize;
  if (UNLIKELY(nullptr != ts)) {
    memcpy(dst, ts->data(), ts_sz);
    dst += ts_sz;
  }
  EncodeFixed64(dst, PackSequenceAndType(s, kValueTypeForSeek));
}

void IterKey::EnlargeBuffer(size_t key_size) {
  // If size is smaller than buffer size, continue using current buffer,
  // or the static allocated one, as default
  assert(key_size > buf_size_);
  // Need to enlarge the buffer.
  ResetBuffer();
  buf_ = new char[key_size];
  buf_size_ = key_size;
}

void IterKey::TrimAppend(const size_t shared_len, const char* non_shared_data,
                         const size_t non_shared_len) {
  assert(shared_len <= key_size_);
  size_t total_size = shared_len + non_shared_len;

  if (IsKeyPinned() /* key is not in buf_ */) {
    // Copy the key from external memory to buf_ (copy shared_len bytes)
    EnlargeBufferIfNeeded(total_size);
    memcpy(buf(), key_, shared_len);
  } else if (total_size > buf_size_) {
    // Need to allocate space, delete previous space
    char* p = new char[total_size];
    memcpy(p, key_, shared_len);

    if (buf_size_ != sizeof(space_)) {
      delete[] buf_;
    }

    buf_ = p;
    buf_size_ = total_size;
  }

  memcpy(buf() + shared_len, non_shared_data, non_shared_len);
  key_ = buf();
  key_size_ = total_size;
}

}  // namespace ROCKSDB_NAMESPACE
