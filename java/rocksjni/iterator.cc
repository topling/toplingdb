// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ and enables
// calling c++ ROCKSDB_NAMESPACE::Iterator methods from Java side.

#include "rocksdb/iterator.h"

#include <jni.h>
#include <stdio.h>
#include <stdlib.h>

#include <algorithm>

#include "include/org_rocksdb_RocksIterator.h"
#include "rocksjni/portal.h"
#include "kv_helper.h"

namespace ROCKSDB_NAMESPACE {
  JZeroCopyIter::~JZeroCopyIter() {
    delete own_iter;
    own_iter = nullptr;
    iter = nullptr;
  }

#define THROW_ON_INVALID_VALUE(zc_iter, ReturnOnError) \
  if (UNLIKELY(zc_iter->value.size() == size_t(-1))) { \
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew   \
                       (env, zc_iter->iter->status()); \
    ReturnOnError; \
  } else (void)(0)

  // ensure that the JZeroCopyIter accessing in java side is correct
  static_assert(offsetof(JZeroCopyIter, key.data_) == 8);
  static_assert(offsetof(JZeroCopyIter, key.size_) == 16);
  static_assert(offsetof(JZeroCopyIter, value.data_) == 24);
  static_assert(offsetof(JZeroCopyIter, value.size_) == 32);

  // If such callback class is reusable, move it to a common header file.
  struct JCallbackSeek {
    void operator()(Slice target_slice) const;
    JCallbackSeek(jlong h, JNIEnv* _env) : handle(h), env(_env) {}
    jlong handle;
    JNIEnv* env;
  };
  struct JCallbackSeekForPrev {
    void operator()(Slice target_slice) const;
    JCallbackSeekForPrev(jlong h, JNIEnv* _env) : handle(h), env(_env) {}
    jlong handle;
    JNIEnv* env;
  };

  void JCallbackSeek::operator()(Slice target_slice) const {
    auto zc_iter = reinterpret_cast<JZeroCopyIter*>(handle & jlong(~1L));
    zc_iter->key = zc_iter->iter->SeekWithKey(target_slice);
    bool fetch_value = (handle & 1) != 0;
    if (zc_iter->key.data() != nullptr && fetch_value) {
      zc_iter->value = zc_iter->iter->value();
      THROW_ON_INVALID_VALUE(zc_iter, return);
    } else {
      zc_iter->value.data_ = nullptr;
    }
  }
  void JCallbackSeekForPrev::operator()(Slice target_slice) const {
    auto zc_iter = reinterpret_cast<JZeroCopyIter*>(handle & jlong(~1L));
    zc_iter->key = zc_iter->iter->SeekForPrevWithKey(target_slice);
    bool fetch_value = (handle & 1) != 0;
    if (zc_iter->key.data() != nullptr && fetch_value) {
      zc_iter->value = zc_iter->iter->value();
      THROW_ON_INVALID_VALUE(zc_iter, return);
    } else {
      zc_iter->value.data_ = nullptr;
    }
  }

} // namespace ROCKSDB_NAMESPACE

using ROCKSDB_NAMESPACE::JZeroCopyIter;
using ROCKSDB_NAMESPACE::JCallbackSeek;
using ROCKSDB_NAMESPACE::JCallbackSeekForPrev;

/*
 * Class:     org_rocksdb_RocksIterator
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_RocksIterator_disposeInternal(JNIEnv* /*env*/,
                                                    jobject /*jobj*/,
                                                    jlong handle) {
  auto* it = reinterpret_cast<JZeroCopyIter*>(handle);
  assert(it != nullptr);
  delete it;
}

/*
 * Class:     org_rocksdb_RocksIterator
 * Method:    isValid0
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_RocksIterator_isValid0(JNIEnv* /*env*/,
                                                 jobject /*jobj*/,
                                                 jlong handle) {
  auto* it = reinterpret_cast<JZeroCopyIter*>(handle);
  bool valid_fast = it->key.data() != nullptr;
  assert(valid_fast == it->iter->Valid());
  return valid_fast;
}

/*
 * Class:     org_rocksdb_RocksIterator
 * Method:    seekToFirst0
 * Signature: (J)V
 */
void Java_org_rocksdb_RocksIterator_seekToFirst0(JNIEnv* env,
                                                 jobject /*jobj*/,
                                                 jlong handle) {
  auto it = reinterpret_cast<JZeroCopyIter*>(handle & jlong(~1L));
  it->key = it->iter->SeekToFirstWithKey();
  if (it->key.data() != nullptr && (handle & 1)) {
    it->value = it->iter->value();
    THROW_ON_INVALID_VALUE(it, return);
  } else {
    it->value.data_ = nullptr;
  }
}

/*
 * Class:     org_rocksdb_RocksIterator
 * Method:    seekToLast0
 * Signature: (J)V
 */
void Java_org_rocksdb_RocksIterator_seekToLast0(JNIEnv* env,
                                                jobject /*jobj*/,
                                                jlong handle) {
  auto it = reinterpret_cast<JZeroCopyIter*>(handle & jlong(~1L));
  it->key = it->iter->SeekToLastWithKey();
  if (it->key.data() != nullptr && (handle & 1)) {
    it->value = it->iter->value();
    THROW_ON_INVALID_VALUE(it, return);
  } else {
    it->value.data_ = nullptr;
  }
}

/*
 * Class:     org_rocksdb_RocksIterator
 * Method:    next0
 * Signature: (J)V
 */
void Java_org_rocksdb_RocksIterator_next0(JNIEnv* env, jobject /*jobj*/,
                                          jlong handle) {
  auto it = reinterpret_cast<JZeroCopyIter*>(handle & jlong(~1L));
  it->key = it->iter->NextWithKey();
  if (it->key.data() != nullptr && (handle & 1)) {
    it->value = it->iter->value();
    THROW_ON_INVALID_VALUE(it, return);
  } else {
    it->value.data_ = nullptr;
  }
}

/*
 * Class:     org_rocksdb_RocksIterator
 * Method:    prev0
 * Signature: (J)V
 */
void Java_org_rocksdb_RocksIterator_prev0(JNIEnv* env, jobject /*jobj*/,
                                          jlong handle) {
  auto it = reinterpret_cast<JZeroCopyIter*>(handle & jlong(~1L));
  it->key = it->iter->PrevWithKey();
  if (it->key.data() != nullptr && (handle & 1)) {
    it->value = it->iter->value();
    THROW_ON_INVALID_VALUE(it, return);
  } else {
    it->value.data_ = nullptr;
  }
}

/*
 * Class:     org_rocksdb_RocksIterator
 * Method:    refresh0
 * Signature: (J)V
 */
void Java_org_rocksdb_RocksIterator_refresh0(JNIEnv* env, jobject /*jobj*/,
                                             jlong handle) {
  auto zc_it = reinterpret_cast<JZeroCopyIter*>(handle);
  auto it = zc_it->own_iter;
  ROCKSDB_NAMESPACE::Status s = it->Refresh();
  if (it->Valid()) {
    zc_it->key = it->key();
    if (zc_it->value.data_) {
      zc_it->value = it->value();
      THROW_ON_INVALID_VALUE(zc_it, return);
    }
  } else {
    zc_it->key = ROCKSDB_NAMESPACE::Slice(nullptr, 0);
    zc_it->value.data_ = nullptr;
  }

  if (s.ok()) {
    return;
  }

  ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
}

/*
 * Class:     org_rocksdb_RocksIterator
 * Method:    seek0
 * Signature: (J[BI)V
 */
void Java_org_rocksdb_RocksIterator_seek0(JNIEnv* env, jobject /*jobj*/,
                                          jlong handle, jbyteArray jtarget,
                                          jint jtarget_len) {
  JCallbackSeek seek(handle, env);
  ROCKSDB_NAMESPACE::JniUtil::k_op_region(seek, env, jtarget, 0, jtarget_len);
}

/*
 * This method supports fetching into indirect byte buffers;
 * the Java wrapper extracts the byte[] and passes it here.
 * In this case, the buffer offset of the key may be non-zero.
 *
 * Class:     org_rocksdb_RocksIterator
 * Method:    seek0
 * Signature: (J[BII)V
 */
void Java_org_rocksdb_RocksIterator_seekByteArray0(
    JNIEnv* env, jobject /*jobj*/, jlong handle, jbyteArray jtarget,
    jint jtarget_off, jint jtarget_len) {
  JCallbackSeek seek(handle, env);
  ROCKSDB_NAMESPACE::JniUtil::k_op_region(seek, env, jtarget, jtarget_off,
                                          jtarget_len);
}

/*
 * Class:     org_rocksdb_RocksIterator
 * Method:    seekDirect0
 * Signature: (JLjava/nio/ByteBuffer;II)V
 */
void Java_org_rocksdb_RocksIterator_seekDirect0(JNIEnv* env, jobject /*jobj*/,
                                                jlong handle, jobject jtarget,
                                                jint jtarget_off,
                                                jint jtarget_len) {
  JCallbackSeek seek(handle, env);
  ROCKSDB_NAMESPACE::JniUtil::k_op_direct(seek, env, jtarget, jtarget_off,
                                          jtarget_len);
}

/*
 * Class:     org_rocksdb_RocksIterator
 * Method:    seekForPrevDirect0
 * Signature: (JLjava/nio/ByteBuffer;II)V
 */
void Java_org_rocksdb_RocksIterator_seekForPrevDirect0(
    JNIEnv* env, jobject /*jobj*/, jlong handle, jobject jtarget,
    jint jtarget_off, jint jtarget_len) {
  JCallbackSeekForPrev seekPrev(handle, env);
  ROCKSDB_NAMESPACE::JniUtil::k_op_direct(seekPrev, env, jtarget, jtarget_off,
                                          jtarget_len);
}

/*
 * Class:     org_rocksdb_RocksIterator
 * Method:    seekForPrev0
 * Signature: (J[BI)V
 */
void Java_org_rocksdb_RocksIterator_seekForPrev0(JNIEnv* env, jobject /*jobj*/,
                                                 jlong handle,
                                                 jbyteArray jtarget,
                                                 jint jtarget_len) {
  JCallbackSeekForPrev seek(handle, env);
  ROCKSDB_NAMESPACE::JniUtil::k_op_region(seek, env, jtarget, 0, jtarget_len);
}

/*
 * This method supports fetching into indirect byte buffers;
 * the Java wrapper extracts the byte[] and passes it here.
 * In this case, the buffer offset of the key may be non-zero.
 *
 * Class:     org_rocksdb_RocksIterator
 * Method:    seek0
 * Signature: (J[BII)V
 */
void Java_org_rocksdb_RocksIterator_seekForPrevByteArray0(
    JNIEnv* env, jobject /*jobj*/, jlong handle, jbyteArray jtarget,
    jint jtarget_off, jint jtarget_len) {
  JCallbackSeekForPrev seek(handle, env);
  ROCKSDB_NAMESPACE::JniUtil::k_op_region(seek, env, jtarget, jtarget_off,
                                          jtarget_len);
}

/*
 * Class:     org_rocksdb_RocksIterator
 * Method:    status0
 * Signature: (J)V
 */
void Java_org_rocksdb_RocksIterator_status0(JNIEnv* env, jobject /*jobj*/,
                                            jlong handle) {
  auto* it = reinterpret_cast<JZeroCopyIter*>(handle)->iter;
  ROCKSDB_NAMESPACE::Status s = it->status();

  if (s.ok()) {
    return;
  }

  ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
}

/*
 * Class:     org_rocksdb_RocksIterator
 * Method:    key0
 * Signature: (J)[B
 */
jbyteArray Java_org_rocksdb_RocksIterator_key0(JNIEnv* env, jobject /*jobj*/,
                                               jlong handle) {
  auto zc_it = reinterpret_cast<JZeroCopyIter*>(handle);
  assert(zc_it->iter->Valid());
  assert(zc_it->key.data() != nullptr);
  ROCKSDB_NAMESPACE::Slice key_slice = zc_it->key;

  jbyteArray jkey = env->NewByteArray(static_cast<jsize>(key_slice.size()));
  if (jkey == nullptr) {
    // exception thrown: OutOfMemoryError
    return nullptr;
  }
  env->SetByteArrayRegion(
      jkey, 0, static_cast<jsize>(key_slice.size()),
      const_cast<jbyte*>(reinterpret_cast<const jbyte*>(key_slice.data())));
  return jkey;
}

/*
 * Class:     org_rocksdb_RocksIterator
 * Method:    keyDirect0
 * Signature: (JLjava/nio/ByteBuffer;II)I
 */
jint Java_org_rocksdb_RocksIterator_keyDirect0(JNIEnv* env, jobject /*jobj*/,
                                               jlong handle, jobject jtarget,
                                               jint jtarget_off,
                                               jint jtarget_len) {
  auto zc_it = reinterpret_cast<JZeroCopyIter*>(handle);
  assert(zc_it->iter->Valid());
  assert(zc_it->key.data() != nullptr);
  ROCKSDB_NAMESPACE::Slice key_slice = zc_it->key;
  return ROCKSDB_NAMESPACE::JniUtil::copyToDirect(env, key_slice, jtarget,
                                                  jtarget_off, jtarget_len);
}

/*
 * This method supports fetching into indirect byte buffers;
 * the Java wrapper extracts the byte[] and passes it here.
 *
 * Class:     org_rocksdb_RocksIterator
 * Method:    keyByteArray0
 * Signature: (J[BII)I
 */
jint Java_org_rocksdb_RocksIterator_keyByteArray0(JNIEnv* env, jobject /*jobj*/,
                                                  jlong handle, jbyteArray jkey,
                                                  jint jkey_off,
                                                  jint jkey_len) {
  auto* zc_it = reinterpret_cast<JZeroCopyIter*>(handle);
  assert(zc_it->iter->Valid());
  assert(zc_it->key.data() != nullptr);
  ROCKSDB_NAMESPACE::Slice key_slice = zc_it->key;
  jsize copy_size = std::min(static_cast<uint32_t>(key_slice.size()),
                             static_cast<uint32_t>(jkey_len));
  env->SetByteArrayRegion(
      jkey, jkey_off, copy_size,
      const_cast<jbyte*>(reinterpret_cast<const jbyte*>(key_slice.data())));

  return static_cast<jsize>(key_slice.size());
}

/*
 * Class:     org_rocksdb_RocksIterator
 * Method:    value0
 * Signature: (J)V
 */
void Java_org_rocksdb_RocksIterator_value0(JNIEnv* env, jobject /*jobj*/,
                                                 jlong handle) {
  auto* zc_it = reinterpret_cast<JZeroCopyIter*>(handle);
  assert(zc_it->iter->Valid());
  assert(zc_it->key.data() != nullptr);
  zc_it->value = zc_it->iter->value();
  THROW_ON_INVALID_VALUE(zc_it, return);
}

/*
 * Class:     org_rocksdb_RocksIterator
 * Method:    valueDirect0
 * Signature: (JLjava/nio/ByteBuffer;II)I
 */
jint Java_org_rocksdb_RocksIterator_valueDirect0(JNIEnv* env, jobject /*jobj*/,
                                                 jlong handle, jobject jtarget,
                                                 jint jtarget_off,
                                                 jint jtarget_len) {
  auto* zc_it = reinterpret_cast<JZeroCopyIter*>(handle);
  assert(zc_it->iter->Valid());
  assert(zc_it->key.data() != nullptr);
  if (zc_it->value.data() == nullptr) {
    zc_it->value = zc_it->iter->value();
    THROW_ON_INVALID_VALUE(zc_it, return 0);
  }
  ROCKSDB_NAMESPACE::Slice value_slice = zc_it->value;
  return ROCKSDB_NAMESPACE::JniUtil::copyToDirect(env, value_slice, jtarget,
                                                  jtarget_off, jtarget_len);
}

/*
 * This method supports fetching into indirect byte buffers;
 * the Java wrapper extracts the byte[] and passes it here.
 *
 * Class:     org_rocksdb_RocksIterator
 * Method:    valueByteArray0
 * Signature: (J[BII)I
 */
jint Java_org_rocksdb_RocksIterator_valueByteArray0(
    JNIEnv* env, jobject /*jobj*/, jlong handle, jbyteArray jvalue_target,
    jint jvalue_off, jint jvalue_len) {
  auto* zc_it = reinterpret_cast<JZeroCopyIter*>(handle);
  assert(zc_it->iter->Valid());
  assert(zc_it->key.data() != nullptr);
  if (zc_it->value.data() == nullptr) {
    zc_it->value = zc_it->iter->value();
    THROW_ON_INVALID_VALUE(zc_it, return 0);
  }
  ROCKSDB_NAMESPACE::Slice value_slice = zc_it->value;
  jsize copy_size = std::min(static_cast<uint32_t>(value_slice.size()),
                             static_cast<uint32_t>(jvalue_len));
  env->SetByteArrayRegion(
      jvalue_target, jvalue_off, copy_size,
      const_cast<jbyte*>(reinterpret_cast<const jbyte*>(value_slice.data())));

  return static_cast<jsize>(value_slice.size());
}

/*
 * Class:     org_rocksdb_RocksIterator
 * Method:    nativeRefreshForDatabaseGC
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_rocksdb_RocksIterator_nativeRefreshForDatabaseGC
(JNIEnv* env, jobject, jlong jiter)
{
  auto zc_it = reinterpret_cast<JZeroCopyIter*>(jiter);
  auto iter = zc_it->own_iter;
  bool is_valid = iter->Valid();
  ROCKSDB_NAMESPACE::Status s = iter->RefreshKeepSnapshot(true);
  if (is_valid) {
    zc_it->key = iter->key();
    zc_it->value = iter->value();
    THROW_ON_INVALID_VALUE(zc_it, return);
  }
  ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
}

#define AllocaSliceFromJByteArray(slice, jbytearr) \
  ROCKSDB_NAMESPACE::Slice slice;                  \
  slice.size_ = env->GetArrayLength(jbytearr);     \
  slice.data_ = (const char*)alloca(slice.size_);  \
  env->GetByteArrayRegion(jbytearr, 0, slice.size_, (jbyte*)slice.data_); \
  if (env->ExceptionCheck()) { return -1; }

/*
 * Class:     org_rocksdb_RocksIterator
 * Method:    countKeysInRange0
 * Signature: (J[B[BI)J
 */
JNIEXPORT jlong JNICALL Java_org_rocksdb_RocksIterator_countKeysInRange0
(JNIEnv* env, jobject, jlong jiter, jbyteArray jbeg_key, jbyteArray jend_key, jint fixed_user_key_len)
{
  auto zc_it = reinterpret_cast<JZeroCopyIter*>(jiter);
  auto iter = zc_it->iter;
  AllocaSliceFromJByteArray(beg_key, jbeg_key);
  AllocaSliceFromJByteArray(end_key, jend_key);
  return iter->CountKeysInRange(beg_key, end_key, fixed_user_key_len);
}
