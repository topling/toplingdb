// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ and enables
// calling c++ ROCKSDB_NAMESPACE::WriteBatch methods from Java side.
#include "rocksdb/write_batch.h"

#include <memory>

#include "db/memtable.h"
#include "db/write_batch_internal.h"
#include "include/org_rocksdb_WriteBatch.h"
#include "include/org_rocksdb_WriteBatch_Handler.h"
#include "logging/logging.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/status.h"
#include "rocksdb/write_buffer_manager.h"
#include "rocksjni/cplusplus_to_java_convert.h"
#include "rocksjni/kv_helper.h"
#include "rocksjni/writebatchhandlerjnicallback.h"
#include "table/scoped_arena_iterator.h"

namespace ROCKSDB_NAMESPACE {
struct JniWriteBatch : public WriteBatch {
  using WriteBatch::content_flags_;
  jlong m_addr;
  jlong m_size;
  jlong m_capa;
  void updateJavaAddrSizeCapFromNative() {
    m_addr = (jlong)WriteBatch::rep_.data();
    m_size = (jlong)WriteBatch::rep_.size();
    m_capa = (jlong)WriteBatch::rep_.capacity();
  }
  void updateNativeDataSizeFromJava() {
    ROCKSDB_ASSERT_EQ(m_addr, jlong(rep_.data()));
    ROCKSDB_ASSERT_GE(m_size, jlong(12));
    ROCKSDB_ASSERT_LE(m_size, jlong(rep_.capacity()));
    ROCKSDB_ASSERT_EQ(m_capa, jlong(rep_.capacity()));
    terark::string_resize_no_touch_memory(&rep_, m_size);
  }
  void ensureCapacity(jlong newcap, JNIEnv* env) {
    updateNativeDataSizeFromJava();
    if (max_bytes_ && size_t(newcap) > max_bytes_) {
      RocksDBExceptionJni::ThrowNew(env, Status::MemoryLimit());
      return;
    }
    ROCKSDB_ASSERT_GT(newcap, m_capa);
    newcap = std::max(newcap, m_capa * 2);
    if (max_bytes_) { // max_bytes_ maybe size_t(-1) SIZE_MAX
      newcap = (jlong)std::min(size_t(newcap), max_bytes_);
    }
    rep_.reserve(size_t(newcap));
    m_addr = (jlong)rep_.data();
  //m_size = (jlong)rep_.size(); // not needed
    m_capa = (jlong)rep_.capacity();
  }
  explicit JniWriteBatch(size_t reserved_bytes = 0, size_t max_bytes = 0)
      : JniWriteBatch(reserved_bytes, max_bytes, 0, 0) {}
  explicit JniWriteBatch(size_t reserved_bytes, size_t max_bytes,
                         size_t protection_bytes_per_key,
                         size_t default_cf_ts_sz)
      : WriteBatch(reserved_bytes, max_bytes,
                   protection_bytes_per_key, default_cf_ts_sz)
  {
    updateJavaAddrSizeCapFromNative();
  }
  // explicit JniWriteBatch(const std::string& rep) : WriteBatch(rep) {
  //   updateJavaAddrSizeCapFromNative();
  // }
  explicit JniWriteBatch(std::string&& rep) : WriteBatch(std::move(rep)) {
    updateJavaAddrSizeCapFromNative();
  }
  JniWriteBatch(const JniWriteBatch&) = delete;
  void Clear() override {
    updateNativeDataSizeFromJava();
    WriteBatch::Clear();
    updateJavaAddrSizeCapFromNative();
  }
  void SetSavePoint() override {
    updateNativeDataSizeFromJava();
    WriteBatch::SetSavePoint();
    updateJavaAddrSizeCapFromNative();
  }
  Status RollbackToSavePoint() override {
    updateNativeDataSizeFromJava();
    Status s = WriteBatch::RollbackToSavePoint();
    updateJavaAddrSizeCapFromNative();
    return s;
  }
  Status PopSavePoint() override {
    updateNativeDataSizeFromJava();
    Status s = WriteBatch::PopSavePoint();
    updateJavaAddrSizeCapFromNative();
    return s;
  }
  void SetMaxBytes(size_t n) override {
    updateNativeDataSizeFromJava();
    WriteBatch::SetMaxBytes(n);
    updateJavaAddrSizeCapFromNative();
  }
  using CFH = ColumnFamilyHandle;
  using WriteBatch::Put;
  Status Put(CFH* cf, const Slice& k, const Slice& v) override {
    updateNativeDataSizeFromJava();
    Status s = WriteBatch::Put(cf, k, v);
    updateJavaAddrSizeCapFromNative();
    return s;
  }
  Status Put(CFH* cf, const Slice& k, const Slice& ts, const Slice& v)
  override {
    updateNativeDataSizeFromJava();
    Status s = WriteBatch::Put(cf, k, ts, v);
    updateJavaAddrSizeCapFromNative();
    return s;
  }
  Status Put(CFH* cf, const KeyValuePopulator& kvp) override {
    updateNativeDataSizeFromJava();
    Status s = WriteBatch::Put(cf, kvp);
    updateJavaAddrSizeCapFromNative();
    return s;
  }
  using WriteBatch::Merge;
  Status Merge(CFH* cf, const Slice& k, const Slice& v) override {
    updateNativeDataSizeFromJava();
    Status s = WriteBatch::Merge(cf, k, v);
    updateJavaAddrSizeCapFromNative();
    return s;
  }
  Status Merge(CFH* cf, const Slice& k, const Slice& ts, const Slice& v)
  override {
    updateNativeDataSizeFromJava();
    Status s = WriteBatch::Merge(cf, k, ts, v);
    updateJavaAddrSizeCapFromNative();
    return s;
  }
  Status Merge(CFH* cf, const KeyValuePopulator& kvp) override {
    updateNativeDataSizeFromJava();
    Status s = WriteBatch::Merge(cf, kvp);
    updateJavaAddrSizeCapFromNative();
    return s;
  }
  using WriteBatch::DeleteRange;
  Status DeleteRange(CFH* cf, const Slice& k, const Slice& v) override {
    updateNativeDataSizeFromJava();
    Status s = WriteBatch::DeleteRange(cf, k, v);
    updateJavaAddrSizeCapFromNative();
    return s;
  }
  Status DeleteRange(CFH* cf, const Slice& x, const Slice& y, const Slice& z)
  override {
    updateNativeDataSizeFromJava();
    Status s = WriteBatch::DeleteRange(cf, x, y, z);
    updateJavaAddrSizeCapFromNative();
    return s;
  }
  using WriteBatch::Delete;
  Status Delete(CFH* cf, const Slice& k) override {
    updateNativeDataSizeFromJava();
    Status s = WriteBatch::Delete(cf, k);
    updateJavaAddrSizeCapFromNative();
    return s;
  }
  Status Delete(CFH* cf, const Slice& k, const Slice& ts) override {
    updateNativeDataSizeFromJava();
    Status s = WriteBatch::Delete(cf, k, ts);
    updateJavaAddrSizeCapFromNative();
    return s;
  }
  Status Delete(CFH* cf, const KeyValuePopulator& kvp) override {
    updateNativeDataSizeFromJava();
    Status s = WriteBatch::Delete(cf, kvp);
    updateJavaAddrSizeCapFromNative();
    return s;
  }
  using WriteBatch::SingleDelete;
  Status SingleDelete(CFH* cf, const Slice& k) override {
    updateNativeDataSizeFromJava();
    Status s = WriteBatch::SingleDelete(cf, k);
    updateJavaAddrSizeCapFromNative();
    return s;
  }
  Status SingleDelete(CFH* cf, const Slice& k, const Slice& ts) override {
    updateNativeDataSizeFromJava();
    Status s = WriteBatch::SingleDelete(cf, k, ts);
    updateJavaAddrSizeCapFromNative();
    return s;
  }
  Status SingleDelete(CFH* cf, const KeyValuePopulator& kvp) override {
    updateNativeDataSizeFromJava();
    Status s = WriteBatch::SingleDelete(cf, kvp);
    updateJavaAddrSizeCapFromNative();
    return s;
  }
  Status PutLogData(const Slice& b) override {
    updateNativeDataSizeFromJava();
    Status s = WriteBatch::PutLogData(b);
    updateJavaAddrSizeCapFromNative();
    return s;
  }
};
} // namespace ROCKSDB_NAMESPACE

class JNIKeyValuePopulator0 : public ROCKSDB_NAMESPACE::KeyValuePopulator {
  JNIEnv* env_;
  jbyteArray jkey_, jval_;
public:
  virtual ~JNIKeyValuePopulator0() = default;
  JNIKeyValuePopulator0(JNIEnv* env,
                        jbyteArray jkey, jint jkey_len,
                        jbyteArray jval, jint jval_len)
    : KeyValuePopulator(jkey_len, jval_len),
      env_(env), jkey_(jkey), jval_(jval) { }
  void PopulateKeyValue(char* key, char* val) const override {
    env_->GetByteArrayRegion(jkey_, 0, (jint)key_len_, (jbyte*)key);
    ROCKSDB_NAMESPACE::KVException::ThrowOnError(env_);
    env_->GetByteArrayRegion(jval_, 0, (jint)val_len_, (jbyte*)val);
    ROCKSDB_NAMESPACE::KVException::ThrowOnError(env_);
  }
};

class JNIKeyOnlyPopulator0 : public ROCKSDB_NAMESPACE::KeyValuePopulator {
  JNIEnv* env_;
  jbyteArray jkey_;
public:
  virtual ~JNIKeyOnlyPopulator0() = default;
  JNIKeyOnlyPopulator0(JNIEnv* env, jbyteArray jkey, jint jkey_len)
    : KeyValuePopulator(jkey_len, 0), env_(env), jkey_(jkey) { }
  void PopulateKeyValue(char* key, char*) const override {
    env_->GetByteArrayRegion(jkey_, 0, (jint)key_len_, (jbyte*)key);
    ROCKSDB_NAMESPACE::KVException::ThrowOnError(env_);
  }
};

/*
 * Class:     org_rocksdb_WriteBatch
 * Method:    getAddrSizeCapOffset
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_org_rocksdb_WriteBatch_getAddrSizeCapOffset
(JNIEnv *, jclass)
{
  return offsetof(ROCKSDB_NAMESPACE::JniWriteBatch, m_addr);
}

/*
 * Class:     org_rocksdb_WriteBatch
 * Method:    get_content_flags_offset
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_org_rocksdb_WriteBatch_get_1content_1flags_1offset
(JNIEnv*, jclass)
{
  return offsetof(ROCKSDB_NAMESPACE::JniWriteBatch, content_flags_);
}

/*
 * Class:     org_rocksdb_WriteBatch
 * Method:    updateNativeDataSizeFromJava0
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_rocksdb_WriteBatch_updateNativeDataSizeFromJava0
(JNIEnv*, jobject, jlong jwb)
{
  auto wb = (ROCKSDB_NAMESPACE::JniWriteBatch*)jwb;
  wb->updateNativeDataSizeFromJava();
}

/*
 * Class:     org_rocksdb_WriteBatch
 * Method:    updateJavaAddrSizeCapFromNative0
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_rocksdb_WriteBatch_updateJavaAddrSizeCapFromNative0
(JNIEnv *, jobject, jlong jwb)
{
  auto wb = (ROCKSDB_NAMESPACE::JniWriteBatch*)jwb;
  wb->updateJavaAddrSizeCapFromNative();
}

/*
 * Class:     org_rocksdb_WriteBatch
 * Method:    ensureCapacity
 * Signature: (JJ)V
 */
JNIEXPORT void JNICALL Java_org_rocksdb_WriteBatch_ensureCapacity
(JNIEnv* env, jobject, jlong jwb, jlong newcap)
{
  auto wb = (ROCKSDB_NAMESPACE::JniWriteBatch*)jwb;
  wb->ensureCapacity(newcap, env);
}

/*
 * Class:     org_rocksdb_WriteBatch
 * Method:    newWriteBatch
 * Signature: (I)J
 */
jlong Java_org_rocksdb_WriteBatch_newWriteBatch__I(JNIEnv* /*env*/,
                                                   jclass /*jcls*/,
                                                   jint jreserved_bytes) {
  auto* wb =
      new ROCKSDB_NAMESPACE::JniWriteBatch(static_cast<size_t>(jreserved_bytes));
  return GET_CPLUSPLUS_POINTER(wb);
}

/*
 * Class:     org_rocksdb_WriteBatch
 * Method:    newWriteBatch
 * Signature: ([BI)J
 */
jlong Java_org_rocksdb_WriteBatch_newWriteBatch___3BI(JNIEnv* env,
                                                      jclass /*jcls*/,
                                                      jbyteArray jserialized,
                                                      jint jserialized_length) {
  jboolean has_exception = JNI_FALSE;
  std::string serialized = ROCKSDB_NAMESPACE::JniUtil::byteString<std::string>(
      env, jserialized, jserialized_length,
      [](const char* str, const size_t len) { return std::string(str, len); },
      &has_exception);
  if (has_exception == JNI_TRUE) {
    // exception occurred
    return 0;
  }

  auto* wb = new ROCKSDB_NAMESPACE::JniWriteBatch(std::move(serialized));
  return GET_CPLUSPLUS_POINTER(wb);
}

/*
 * Class:     org_rocksdb_WriteBatch
 * Method:    count0
 * Signature: (J)I
 */
jint Java_org_rocksdb_WriteBatch_count0(JNIEnv* /*env*/, jobject /*jobj*/,
                                        jlong jwb_handle) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);

  return static_cast<jint>(wb->Count());
}

/*
 * Class:     org_rocksdb_WriteBatch
 * Method:    clear0
 * Signature: (J)V
 */
void Java_org_rocksdb_WriteBatch_clear0(JNIEnv* /*env*/, jobject /*jobj*/,
                                        jlong jwb_handle) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);

  wb->Clear();
}

/*
 * Class:     org_rocksdb_WriteBatch
 * Method:    setSavePoint0
 * Signature: (J)V
 */
void Java_org_rocksdb_WriteBatch_setSavePoint0(JNIEnv* /*env*/,
                                               jobject /*jobj*/,
                                               jlong jwb_handle) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);

  wb->SetSavePoint();
}

/*
 * Class:     org_rocksdb_WriteBatch
 * Method:    rollbackToSavePoint0
 * Signature: (J)V
 */
void Java_org_rocksdb_WriteBatch_rollbackToSavePoint0(JNIEnv* env,
                                                      jobject /*jobj*/,
                                                      jlong jwb_handle) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);

  auto s = wb->RollbackToSavePoint();

  if (s.ok()) {
    return;
  }
  ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
}

/*
 * Class:     org_rocksdb_WriteBatch
 * Method:    popSavePoint
 * Signature: (J)V
 */
void Java_org_rocksdb_WriteBatch_popSavePoint(JNIEnv* env, jobject /*jobj*/,
                                              jlong jwb_handle) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);

  auto s = wb->PopSavePoint();

  if (s.ok()) {
    return;
  }
  ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
}

/*
 * Class:     org_rocksdb_WriteBatch
 * Method:    setMaxBytes
 * Signature: (JJ)V
 */
void Java_org_rocksdb_WriteBatch_setMaxBytes(JNIEnv* /*env*/, jobject /*jobj*/,
                                             jlong jwb_handle,
                                             jlong jmax_bytes) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);

  wb->SetMaxBytes(static_cast<size_t>(jmax_bytes));
}

/*
 * Class:     org_rocksdb_WriteBatch
 * Method:    put
 * Signature: (J[BI[BI)V
 */
void Java_org_rocksdb_WriteBatch_put__J_3BI_3BI(JNIEnv* env, jobject jobj,
                                                jlong jwb_handle,
                                                jbyteArray jkey, jint jkey_len,
                                                jbyteArray jentry_value,
                                                jint jentry_value_len) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);
 #if JNI_USE_KEY_VALUE_POPULATOR
  JNIKeyValuePopulator0 kvp(env, jkey, jkey_len, jentry_value, jentry_value_len);
  ROCKSDB_NAMESPACE::Status status = wb->Put(nullptr, kvp);
  ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
 #else
  auto put = [&wb](ROCKSDB_NAMESPACE::Slice key,
                   ROCKSDB_NAMESPACE::Slice value) {
    return wb->Put(key, value);
  };
  std::unique_ptr<ROCKSDB_NAMESPACE::Status> status =
      ROCKSDB_NAMESPACE::JniUtil::kv_op(put, env, jobj, jkey, jkey_len,
                                        jentry_value, jentry_value_len);
  if (status != nullptr && !status->ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
  }
 #endif
}

/*
 * Class:     org_rocksdb_WriteBatch
 * Method:    put
 * Signature: (J[BI[BIJ)V
 */
void Java_org_rocksdb_WriteBatch_put__J_3BI_3BIJ(
    JNIEnv* env, jobject jobj, jlong jwb_handle, jbyteArray jkey, jint jkey_len,
    jbyteArray jentry_value, jint jentry_value_len, jlong jcf_handle) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);
  auto* cf_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  assert(cf_handle != nullptr);
 #if JNI_USE_KEY_VALUE_POPULATOR
  JNIKeyValuePopulator0 kvp(env, jkey, jkey_len, jentry_value, jentry_value_len);
  ROCKSDB_NAMESPACE::Status status = wb->Put(cf_handle, kvp);
  ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
 #else
  auto put = [&wb, &cf_handle](ROCKSDB_NAMESPACE::Slice key,
                               ROCKSDB_NAMESPACE::Slice value) {
    return wb->Put(cf_handle, key, value);
  };
  std::unique_ptr<ROCKSDB_NAMESPACE::Status> status =
      ROCKSDB_NAMESPACE::JniUtil::kv_op(put, env, jobj, jkey, jkey_len,
                                        jentry_value, jentry_value_len);
  if (status != nullptr && !status->ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
  }
 #endif
}

/*
 * Class:     org_rocksdb_WriteBatch
 * Method:    putDirect
 * Signature: (JLjava/nio/ByteBuffer;IILjava/nio/ByteBuffer;IIJ)V
 */
void Java_org_rocksdb_WriteBatch_putDirect(JNIEnv* env, jobject /*jobj*/,
                                           jlong jwb_handle, jobject jkey,
                                           jint jkey_offset, jint jkey_len,
                                           jobject jval, jint jval_offset,
                                           jint jval_len, jlong jcf_handle) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);
  auto* cf_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  auto put = [&wb, &cf_handle](ROCKSDB_NAMESPACE::Slice& key,
                               ROCKSDB_NAMESPACE::Slice& value) {
    if (cf_handle == nullptr) {
      wb->Put(key, value);
    } else {
      wb->Put(cf_handle, key, value);
    }
  };
  ROCKSDB_NAMESPACE::JniUtil::kv_op_direct(
      put, env, jkey, jkey_offset, jkey_len, jval, jval_offset, jval_len);
}

/*
 * Class:     org_rocksdb_WriteBatch
 * Method:    merge
 * Signature: (J[BI[BI)V
 */
void Java_org_rocksdb_WriteBatch_merge__J_3BI_3BI(
    JNIEnv* env, jobject jobj, jlong jwb_handle, jbyteArray jkey, jint jkey_len,
    jbyteArray jentry_value, jint jentry_value_len) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);
 #if JNI_USE_KEY_VALUE_POPULATOR
  JNIKeyValuePopulator0 kvp(env, jkey, jkey_len, jentry_value, jentry_value_len);
  ROCKSDB_NAMESPACE::Status status = wb->Merge(nullptr, kvp);
  ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
 #else
  auto merge = [&wb](ROCKSDB_NAMESPACE::Slice key,
                     ROCKSDB_NAMESPACE::Slice value) {
    return wb->Merge(key, value);
  };
  std::unique_ptr<ROCKSDB_NAMESPACE::Status> status =
      ROCKSDB_NAMESPACE::JniUtil::kv_op(merge, env, jobj, jkey, jkey_len,
                                        jentry_value, jentry_value_len);
  if (status != nullptr && !status->ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
  }
 #endif
}

/*
 * Class:     org_rocksdb_WriteBatch
 * Method:    merge
 * Signature: (J[BI[BIJ)V
 */
void Java_org_rocksdb_WriteBatch_merge__J_3BI_3BIJ(
    JNIEnv* env, jobject jobj, jlong jwb_handle, jbyteArray jkey, jint jkey_len,
    jbyteArray jentry_value, jint jentry_value_len, jlong jcf_handle) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);
  auto* cf_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  assert(cf_handle != nullptr);
 #if JNI_USE_KEY_VALUE_POPULATOR
  JNIKeyValuePopulator0 kvp(env, jkey, jkey_len, jentry_value, jentry_value_len);
  ROCKSDB_NAMESPACE::Status status = wb->Merge(cf_handle, kvp);
  ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
 #else
  auto merge = [&wb, &cf_handle](ROCKSDB_NAMESPACE::Slice key,
                                 ROCKSDB_NAMESPACE::Slice value) {
    return wb->Merge(cf_handle, key, value);
  };
  std::unique_ptr<ROCKSDB_NAMESPACE::Status> status =
      ROCKSDB_NAMESPACE::JniUtil::kv_op(merge, env, jobj, jkey, jkey_len,
                                        jentry_value, jentry_value_len);
  if (status != nullptr && !status->ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
  }
 #endif
}

/*
 * Class:     org_rocksdb_WriteBatch
 * Method:    delete
 * Signature: (J[BI)V
 */
void Java_org_rocksdb_WriteBatch_delete__J_3BI(JNIEnv* env, jobject jobj,
                                               jlong jwb_handle,
                                               jbyteArray jkey, jint jkey_len) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);
 #if JNI_USE_KEY_VALUE_POPULATOR
  JNIKeyOnlyPopulator0 kvp(env, jkey, jkey_len);
  ROCKSDB_NAMESPACE::Status status = wb->Delete(nullptr, kvp);
  ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
 #else
  auto remove = [&wb](ROCKSDB_NAMESPACE::Slice key) { return wb->Delete(key); };
  std::unique_ptr<ROCKSDB_NAMESPACE::Status> status =
      ROCKSDB_NAMESPACE::JniUtil::k_op(remove, env, jobj, jkey, jkey_len);
  if (status != nullptr && !status->ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
  }
 #endif
}

/*
 * Class:     org_rocksdb_WriteBatch
 * Method:    delete
 * Signature: (J[BIJ)V
 */
void Java_org_rocksdb_WriteBatch_delete__J_3BIJ(JNIEnv* env, jobject jobj,
                                                jlong jwb_handle,
                                                jbyteArray jkey, jint jkey_len,
                                                jlong jcf_handle) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);
  auto* cf_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  assert(cf_handle != nullptr);
 #if JNI_USE_KEY_VALUE_POPULATOR
  JNIKeyOnlyPopulator0 kvp(env, jkey, jkey_len);
  ROCKSDB_NAMESPACE::Status status = wb->Delete(cf_handle, kvp);
  ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
 #else
  auto remove = [&wb, &cf_handle](ROCKSDB_NAMESPACE::Slice key) {
    return wb->Delete(cf_handle, key);
  };
  std::unique_ptr<ROCKSDB_NAMESPACE::Status> status =
      ROCKSDB_NAMESPACE::JniUtil::k_op(remove, env, jobj, jkey, jkey_len);
  if (status != nullptr && !status->ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
  }
 #endif
}

/*
 * Class:     org_rocksdb_WriteBatch
 * Method:    singleDelete
 * Signature: (J[BI)V
 */
void Java_org_rocksdb_WriteBatch_singleDelete__J_3BI(JNIEnv* env, jobject jobj,
                                                     jlong jwb_handle,
                                                     jbyteArray jkey,
                                                     jint jkey_len) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);
 #if JNI_USE_KEY_VALUE_POPULATOR
  JNIKeyOnlyPopulator0 kvp(env, jkey, jkey_len);
  ROCKSDB_NAMESPACE::Status status = wb->SingleDelete(nullptr, kvp);
  ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
 #else
  auto single_delete = [&wb](ROCKSDB_NAMESPACE::Slice key) {
    return wb->SingleDelete(key);
  };
  std::unique_ptr<ROCKSDB_NAMESPACE::Status> status =
      ROCKSDB_NAMESPACE::JniUtil::k_op(single_delete, env, jobj, jkey,
                                       jkey_len);
  if (status != nullptr && !status->ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
  }
 #endif
}

/*
 * Class:     org_rocksdb_WriteBatch
 * Method:    singleDelete
 * Signature: (J[BIJ)V
 */
void Java_org_rocksdb_WriteBatch_singleDelete__J_3BIJ(JNIEnv* env, jobject jobj,
                                                      jlong jwb_handle,
                                                      jbyteArray jkey,
                                                      jint jkey_len,
                                                      jlong jcf_handle) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);
  auto* cf_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  assert(cf_handle != nullptr);
 #if JNI_USE_KEY_VALUE_POPULATOR
  JNIKeyOnlyPopulator0 kvp(env, jkey, jkey_len);
  ROCKSDB_NAMESPACE::Status status = wb->SingleDelete(cf_handle, kvp);
  ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
 #else
  auto single_delete = [&wb, &cf_handle](ROCKSDB_NAMESPACE::Slice key) {
    return wb->SingleDelete(cf_handle, key);
  };
  std::unique_ptr<ROCKSDB_NAMESPACE::Status> status =
      ROCKSDB_NAMESPACE::JniUtil::k_op(single_delete, env, jobj, jkey,
                                       jkey_len);
  if (status != nullptr && !status->ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
  }
 #endif
}

/*
 * Class:     org_rocksdb_WriteBatch
 * Method:    deleteDirect
 * Signature: (JLjava/nio/ByteBuffer;IIJ)V
 */
void Java_org_rocksdb_WriteBatch_deleteDirect(JNIEnv* env, jobject /*jobj*/,
                                              jlong jwb_handle, jobject jkey,
                                              jint jkey_offset, jint jkey_len,
                                              jlong jcf_handle) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);
  auto* cf_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  auto remove = [&wb, &cf_handle](ROCKSDB_NAMESPACE::Slice& key) {
    if (cf_handle == nullptr) {
      wb->Delete(key);
    } else {
      wb->Delete(cf_handle, key);
    }
  };
  ROCKSDB_NAMESPACE::JniUtil::k_op_direct(remove, env, jkey, jkey_offset,
                                          jkey_len);
}

/*
 * Class:     org_rocksdb_WriteBatch
 * Method:    deleteRange
 * Signature: (J[BI[BI)V
 */
void Java_org_rocksdb_WriteBatch_deleteRange__J_3BI_3BI(
    JNIEnv* env, jobject jobj, jlong jwb_handle, jbyteArray jbegin_key,
    jint jbegin_key_len, jbyteArray jend_key, jint jend_key_len) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);
  auto deleteRange = [&wb](ROCKSDB_NAMESPACE::Slice beginKey,
                           ROCKSDB_NAMESPACE::Slice endKey) {
    return wb->DeleteRange(beginKey, endKey);
  };
  std::unique_ptr<ROCKSDB_NAMESPACE::Status> status =
      ROCKSDB_NAMESPACE::JniUtil::kv_op(deleteRange, env, jobj, jbegin_key,
                                        jbegin_key_len, jend_key, jend_key_len);
  if (status != nullptr && !status->ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
  }
}

/*
 * Class:     org_rocksdb_WriteBatch
 * Method:    deleteRange
 * Signature: (J[BI[BIJ)V
 */
void Java_org_rocksdb_WriteBatch_deleteRange__J_3BI_3BIJ(
    JNIEnv* env, jobject jobj, jlong jwb_handle, jbyteArray jbegin_key,
    jint jbegin_key_len, jbyteArray jend_key, jint jend_key_len,
    jlong jcf_handle) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);
  auto* cf_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  assert(cf_handle != nullptr);
  auto deleteRange = [&wb, &cf_handle](ROCKSDB_NAMESPACE::Slice beginKey,
                                       ROCKSDB_NAMESPACE::Slice endKey) {
    return wb->DeleteRange(cf_handle, beginKey, endKey);
  };
  std::unique_ptr<ROCKSDB_NAMESPACE::Status> status =
      ROCKSDB_NAMESPACE::JniUtil::kv_op(deleteRange, env, jobj, jbegin_key,
                                        jbegin_key_len, jend_key, jend_key_len);
  if (status != nullptr && !status->ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
  }
}

/*
 * Class:     org_rocksdb_WriteBatch
 * Method:    putLogData
 * Signature: (J[BI)V
 */
void Java_org_rocksdb_WriteBatch_putLogData(JNIEnv* env, jobject jobj,
                                            jlong jwb_handle, jbyteArray jblob,
                                            jint jblob_len) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);
  auto putLogData = [&wb](ROCKSDB_NAMESPACE::Slice blob) {
    return wb->PutLogData(blob);
  };
  std::unique_ptr<ROCKSDB_NAMESPACE::Status> status =
      ROCKSDB_NAMESPACE::JniUtil::k_op(putLogData, env, jobj, jblob, jblob_len);
  if (status != nullptr && !status->ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
  }
}

/*
 * Class:     org_rocksdb_WriteBatch
 * Method:    iterate
 * Signature: (JJ)V
 */
void Java_org_rocksdb_WriteBatch_iterate(JNIEnv* env, jobject /*jobj*/,
                                         jlong jwb_handle,
                                         jlong handlerHandle) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);

  ROCKSDB_NAMESPACE::Status s = wb->Iterate(
      reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatchHandlerJniCallback*>(
          handlerHandle));

  if (s.ok()) {
    return;
  }
  ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
}

/*
 * Class:     org_rocksdb_WriteBatch
 * Method:    data
 * Signature: (J)[B
 */
jbyteArray Java_org_rocksdb_WriteBatch_data(JNIEnv* env, jobject /*jobj*/,
                                            jlong jwb_handle) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);

  auto data = wb->Data();
  return ROCKSDB_NAMESPACE::JniUtil::copyBytes(env, data);
}

/*
 * Class:     org_rocksdb_WriteBatch
 * Method:    getDataSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_WriteBatch_getDataSize(JNIEnv* /*env*/, jobject /*jobj*/,
                                              jlong jwb_handle) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);

  auto data_size = wb->GetDataSize();
  return static_cast<jlong>(data_size);
}

/*
 * Class:     org_rocksdb_WriteBatch
 * Method:    hasPut
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_WriteBatch_hasPut(JNIEnv* /*env*/, jobject /*jobj*/,
                                            jlong jwb_handle) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);

  return wb->HasPut();
}

/*
 * Class:     org_rocksdb_WriteBatch
 * Method:    hasDelete
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_WriteBatch_hasDelete(JNIEnv* /*env*/,
                                               jobject /*jobj*/,
                                               jlong jwb_handle) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);

  return wb->HasDelete();
}

/*
 * Class:     org_rocksdb_WriteBatch
 * Method:    hasSingleDelete
 * Signature: (J)Z
 */
JNIEXPORT jboolean JNICALL Java_org_rocksdb_WriteBatch_hasSingleDelete(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jwb_handle) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);

  return wb->HasSingleDelete();
}

/*
 * Class:     org_rocksdb_WriteBatch
 * Method:    hasDeleteRange
 * Signature: (J)Z
 */
JNIEXPORT jboolean JNICALL Java_org_rocksdb_WriteBatch_hasDeleteRange(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jwb_handle) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);

  return wb->HasDeleteRange();
}

/*
 * Class:     org_rocksdb_WriteBatch
 * Method:    hasMerge
 * Signature: (J)Z
 */
JNIEXPORT jboolean JNICALL Java_org_rocksdb_WriteBatch_hasMerge(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jwb_handle) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);

  return wb->HasMerge();
}

/*
 * Class:     org_rocksdb_WriteBatch
 * Method:    hasBeginPrepare
 * Signature: (J)Z
 */
JNIEXPORT jboolean JNICALL Java_org_rocksdb_WriteBatch_hasBeginPrepare(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jwb_handle) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);

  return wb->HasBeginPrepare();
}

/*
 * Class:     org_rocksdb_WriteBatch
 * Method:    hasEndPrepare
 * Signature: (J)Z
 */
JNIEXPORT jboolean JNICALL Java_org_rocksdb_WriteBatch_hasEndPrepare(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jwb_handle) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);

  return wb->HasEndPrepare();
}

/*
 * Class:     org_rocksdb_WriteBatch
 * Method:    hasCommit
 * Signature: (J)Z
 */
JNIEXPORT jboolean JNICALL Java_org_rocksdb_WriteBatch_hasCommit(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jwb_handle) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);

  return wb->HasCommit();
}

/*
 * Class:     org_rocksdb_WriteBatch
 * Method:    hasRollback
 * Signature: (J)Z
 */
JNIEXPORT jboolean JNICALL Java_org_rocksdb_WriteBatch_hasRollback(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jwb_handle) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);

  return wb->HasRollback();
}

/*
 * Class:     org_rocksdb_WriteBatch
 * Method:    markWalTerminationPoint
 * Signature: (J)V
 */
void Java_org_rocksdb_WriteBatch_markWalTerminationPoint(JNIEnv* /*env*/,
                                                         jobject /*jobj*/,
                                                         jlong jwb_handle) {
#if 0
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);

  wb->MarkWalTerminationPoint();
#else
  ROCKSDB_DIE("This function should not be called");
#endif
}

/*
 * Class:     org_rocksdb_WriteBatch
 * Method:    getWalTerminationPoint
 * Signature: (J)Lorg/rocksdb/WriteBatch/SavePoint;
 */
jobject Java_org_rocksdb_WriteBatch_getWalTerminationPoint(JNIEnv* env,
                                                           jobject /*jobj*/,
                                                           jlong jwb_handle) {
#if 0
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);

  auto save_point = wb->GetWalTerminationPoint();
  return ROCKSDB_NAMESPACE::WriteBatchSavePointJni::construct(env, save_point);
#else
  ROCKSDB_DIE("This function should not be called");
#endif
}

/*
 * Class:     org_rocksdb_WriteBatch
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_WriteBatch_disposeInternal(JNIEnv* /*env*/,
                                                 jobject /*jobj*/,
                                                 jlong handle) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(handle);
  assert(wb != nullptr);
  delete wb;
}

/*
 * Class:     org_rocksdb_WriteBatch_Handler
 * Method:    createNewHandler0
 * Signature: ()J
 */
jlong Java_org_rocksdb_WriteBatch_00024Handler_createNewHandler0(JNIEnv* env,
                                                                 jobject jobj) {
  auto* wbjnic = new ROCKSDB_NAMESPACE::WriteBatchHandlerJniCallback(env, jobj);
  return GET_CPLUSPLUS_POINTER(wbjnic);
}
