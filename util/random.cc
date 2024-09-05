//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#include "util/random.h"

#include <limits.h>
#include <stdint.h>
#include <string.h>

#include <thread>
#include <utility>

#include "port/likely.h"
#include "util/thread_local.h"

namespace ROCKSDB_NAMESPACE {

static ROCKSDB_STATIC_TLS ROCKSDB_RAW_TLS Random* g_tls_instance = nullptr;

Random* Random::GetTLSInstance() {
  if (nullptr == g_tls_instance) {
    static thread_local ROCKSDB_STATIC_TLS Random tls_instance(
      (uint32_t)std::hash<std::thread::id>()(std::this_thread::get_id()));
    g_tls_instance = &tls_instance;
  }
  return g_tls_instance;
}

std::string Random::HumanReadableString(int len) {
  std::string ret;
  ret.resize(len);
  for (int i = 0; i < len; ++i) {
    ret[i] = static_cast<char>('a' + Uniform(26));
  }
  return ret;
}

std::string Random::RandomString(int len) {
  std::string ret;
  ret.resize(len);
  for (int i = 0; i < len; i++) {
    ret[i] = static_cast<char>(' ' + Uniform(95));  // ' ' .. '~'
  }
  return ret;
}

std::string Random::RandomBinaryString(int len) {
  std::string ret;
  ret.resize(len);
  for (int i = 0; i < len; i++) {
    ret[i] = static_cast<char>(Uniform(CHAR_MAX));
  }
  return ret;
}

}  // namespace ROCKSDB_NAMESPACE
