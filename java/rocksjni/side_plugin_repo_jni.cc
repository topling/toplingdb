#include "include/org_rocksdb_SidePluginRepo.h"
#include "include/org_rocksdb_RocksDB.h"
#include "rocksdb/cache.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/types.h"
#include "rocksdb/version.h"
#include "rocksjni/cplusplus_to_java_convert.h"
#include "rocksjni/portal.h"

#include <topling/side_plugin_repo.h>
#include <topling/side_plugin_factory.h>

using namespace rocksdb;

static jlong GetNativeHandle(JNIEnv* env, jobject jobj) {
  jclass clazz = env->GetObjectClass(jobj);
  jfieldID handleFieldID = env->GetFieldID(clazz, "nativeHandle_", "J"); // long
  return env->GetLongField(jobj, handleFieldID);
}

template<class OPT>
static void PutOPT
(JNIEnv* env, jobject jrepo, jstring jname, jstring jspec, jobject joptions)
{
  auto p_opt = (OPT*)GetNativeHandle(env, joptions);
  auto repo = (SidePluginRepo*)GetNativeHandle(env, jrepo);
  const auto* name = env->GetStringUTFChars(jname, nullptr);
  const auto* spec = env->GetStringUTFChars(jspec, nullptr);
  auto sp_opt = std::make_shared<OPT>(*p_opt);
  repo->Put(name, spec, sp_opt);
  env->ReleaseStringUTFChars(jspec, spec);
  env->ReleaseStringUTFChars(jname, name);
}

template<class OPT>
static jboolean CloneOPT(JNIEnv* env, jobject jrepo, jlong jdest, jstring jname) {
  auto repo = (SidePluginRepo*)GetNativeHandle(env, jrepo);
  const auto* name = env->GetStringUTFChars(jname, nullptr);
  std::shared_ptr<OPT> dbo = repo->Get(name);
  const bool exists = nullptr != dbo;
  if (exists) {
    *(OPT*)jdest = *dbo;
  }
  env->ReleaseStringUTFChars(jname, name);
  return exists;
}

extern "C" {
/*
 * Class:     org_rocksdb_SidePluginRepo
 * Method:    importAutoFile
 * Signature: (Lorg/rocksdb/Slice;)V
 */
void Java_org_rocksdb_SidePluginRepo_importAutoFile
(JNIEnv *env, jobject jrepo, jstring jfname)
{
  const auto* fname = env->GetStringUTFChars(jfname, nullptr);
  ROCKSDB_VERIFY(fname != nullptr);
  jclass clazz = env->GetObjectClass(jrepo);
  jfieldID handleFieldID = env->GetFieldID(clazz, "nativeHandle_", "J"); // long
  auto repo = (SidePluginRepo*)env->GetLongField(jrepo, handleFieldID);
  auto status = repo->ImportAutoFile(fname);
  env->ReleaseStringUTFChars(jfname, fname);
  if (!status.ok()) {
    RocksDBExceptionJni::ThrowNew(env, status);
  }
}

static jobject CreateJDB
(JNIEnv* env, DB* db, ColumnFamilyHandle** cfh_a, size_t cfh_n)
{
  jlongArray jcfh_a = nullptr;
  if (cfh_n) {
    jcfh_a = env->NewLongArray(cfh_n);
    env->SetLongArrayRegion(jcfh_a, 0, jsize(cfh_n), (jlong*)cfh_a);
  }
  jclass clazz = env->FindClass("org/rocksdb/RocksDB");
  jmethodID methodID = env->GetStaticMethodID(clazz, "fromNativeHandles", "(J[J)Lorg/rocksdb/RocksDB;");
  return env->CallStaticObjectMethod(clazz, methodID, db, jcfh_a);
}

/*
 * Class:     org_rocksdb_SidePluginRepo
 * Method:    nativeOpenDB
 * Signature: (JLjava/lang/String;)Lorg/rocksdb/RocksDB;
 */
jobject Java_org_rocksdb_SidePluginRepo_nativeOpenDB
(JNIEnv* env, jobject jrepo, jlong nativeHandle, jstring jdbname)
{
  DB* db = nullptr;
  auto repo = (SidePluginRepo*)nativeHandle;
  rocksdb::Status status;
  if (jdbname) {
    const auto* dbname = env->GetStringUTFChars(jdbname, nullptr);
    ROCKSDB_VERIFY(dbname != nullptr);
    status = repo->OpenDB(std::string(dbname), &db);
    env->ReleaseStringUTFChars(jdbname, dbname);
  } else {
    status = repo->OpenDB(&db);
  }
  if (status.ok()) {
    return CreateJDB(env, db, nullptr, 0);
  } else {
    RocksDBExceptionJni::ThrowNew(env, status);
    return 0;
  }
}

/*
 * Class:     org_rocksdb_SidePluginRepo
 * Method:    nativeOpenDBMultiCF
 * Signature: (JLjava/lang/String;)Lorg/rocksdb/RocksDB;
 */
jobject Java_org_rocksdb_SidePluginRepo_nativeOpenDBMultiCF
(JNIEnv* env, jobject jrepo, jlong nativeHandle, jstring jdbname)
{
  DB_MultiCF* dbm = nullptr;
  auto repo = (SidePluginRepo*)nativeHandle;
  rocksdb::Status status;
  if (jdbname) {
    const auto* dbname = env->GetStringUTFChars(jdbname, nullptr);
    ROCKSDB_VERIFY(dbname != nullptr);
    status = repo->OpenDB(std::string(dbname), &dbm);
    env->ReleaseStringUTFChars(jdbname, dbname);
  } else {
    status = repo->OpenDB(&dbm);
  }
  if (status.ok()) {
    return CreateJDB(env, dbm->db, dbm->cf_handles.data(), dbm->cf_handles.size());
  } else {
    RocksDBExceptionJni::ThrowNew(env, status);
    return nullptr;
  }
}

/*
 * Class:     org_rocksdb_SidePluginRepo
 * Method:    startHttpServer
 * Signature: ()V
 */
void Java_org_rocksdb_SidePluginRepo_startHttpServer
(JNIEnv* env, jobject jrepo)
{
  jclass clazz = env->GetObjectClass(jrepo);
  jfieldID handleFieldID = env->GetFieldID(clazz, "nativeHandle_", "J"); // long
  auto repo = (SidePluginRepo*)env->GetLongField(jrepo, handleFieldID);
  auto status = repo->StartHttpServer();
  if (!status.ok()) {
    RocksDBExceptionJni::ThrowNew(env, status);
  }
}

/*
 * Class:     org_rocksdb_SidePluginRepo
 * Method:    closeHttpServer
 * Signature: ()V
 */
void Java_org_rocksdb_SidePluginRepo_closeHttpServer
(JNIEnv* env, jobject jrepo)
{
  jclass clazz = env->GetObjectClass(jrepo);
  jfieldID handleFieldID = env->GetFieldID(clazz, "nativeHandle_", "J"); // long
  auto repo = (SidePluginRepo*)env->GetLongField(jrepo, handleFieldID);
  repo->CloseHttpServer();
}

/*
 * Class:     org_rocksdb_SidePluginRepo
 * Method:    nativeCloseAllDB
 * Signature: (J)V
 */
void Java_org_rocksdb_SidePluginRepo_nativeCloseAllDB
(JNIEnv* env, jobject jrepo, jlong nativeHandle)
{
  auto repo = (SidePluginRepo*)nativeHandle;
  repo->CloseAllDB(false); // dont close DB and cf
}

/*
 * Class:     org_rocksdb_SidePluginRepo
 * Method:    nativePutDB
 * Signature: (Ljava/lang/String;Ljava/lang/String;Lorg/rocksdb/RocksDB;[Lorg/rocksdb/ColumnFamilyHandle;)V
 */
JNIEXPORT void JNICALL Java_org_rocksdb_SidePluginRepo_nativePutDB
(JNIEnv* env, jobject jrepo, jstring jname, jstring jspec, jobject jdb, jobjectArray j_cf_handles)
{
  auto repo = (SidePluginRepo*)GetNativeHandle(env, jrepo);
  auto db = (DB*)GetNativeHandle(env, jdb);
  const auto* name = env->GetStringUTFChars(jname, nullptr);
  const auto* spec = env->GetStringUTFChars(jspec, nullptr);
  const size_t cf_num = env->GetArrayLength(j_cf_handles);
  std::vector<ColumnFamilyHandle*> cf_handles(cf_num);
  for (size_t i = 0; i < cf_num; i++) {
    jobject jcfh = env->GetObjectArrayElement(j_cf_handles, i);
    cf_handles[i] = (ColumnFamilyHandle*)GetNativeHandle(env, jcfh);
  }
  repo->Put(name, spec, db, cf_handles);
  env->ReleaseStringUTFChars(jspec, spec);
  env->ReleaseStringUTFChars(jname, name);
}

/*
 * Class:     org_rocksdb_SidePluginRepo
 * Method:    nativeCloneCFOptions
 * Signature: (JLjava/lang/String;)Z
 */
JNIEXPORT jboolean JNICALL Java_org_rocksdb_SidePluginRepo_nativeCloneCFOptions
(JNIEnv* env, jobject jrepo, jlong jdest, jstring jname)
{
  return CloneOPT<ColumnFamilyOptions>(env, jrepo, jdest, jname);
}

/*
 * Class:     org_rocksdb_SidePluginRepo
 * Method:    nativeCloneDBOptions
 * Signature: (JLjava/lang/String;)Z
 */
JNIEXPORT jboolean JNICALL Java_org_rocksdb_SidePluginRepo_nativeCloneDBOptions
(JNIEnv* env, jobject jrepo, jlong jdest, jstring jname)
{
  return CloneOPT<DBOptions>(env, jrepo, jdest, jname);
}

/*
 * Class:     org_rocksdb_SidePluginRepo
 * Method:    put
 * Signature: (Ljava/lang/String;Ljava/lang/String;Lorg/rocksdb/Options;)V
 */
void Java_org_rocksdb_SidePluginRepo_put__Ljava_lang_String_2Ljava_lang_String_2Lorg_rocksdb_Options_2
(JNIEnv* env, jobject jrepo, jstring jname, jstring jspec, jobject joptions)
{
  PutOPT<Options>(env, jrepo, jname, jspec, joptions);
}

/*
 * Class:     org_rocksdb_SidePluginRepo
 * Method:    put
 * Signature: (Ljava/lang/String;Ljava/lang/String;Lorg/rocksdb/DBOptions;)V
 */
void Java_org_rocksdb_SidePluginRepo_put__Ljava_lang_String_2Ljava_lang_String_2Lorg_rocksdb_DBOptions_2
(JNIEnv* env, jobject jrepo, jstring jname, jstring jspec, jobject joptions)
{
  PutOPT<DBOptions>(env, jrepo, jname, jspec, joptions);
}

/*
 * Class:     org_rocksdb_SidePluginRepo
 * Method:    put
 * Signature: (Ljava/lang/String;Ljava/lang/String;Lorg/rocksdb/ColumnFamilyOptions;)V
 */
void Java_org_rocksdb_SidePluginRepo_put__Ljava_lang_String_2Ljava_lang_String_2Lorg_rocksdb_ColumnFamilyOptions_2
(JNIEnv* env, jobject jrepo, jstring jname, jstring jspec, jobject joptions)
{
  PutOPT<ColumnFamilyOptions>(env, jrepo, jname, jspec, joptions);
}

static DB_MultiCF* Get_DB_MultiCF(JNIEnv* env, DB* db, SidePluginRepo* repo) {
  auto& dbr = repo->m_impl->db;
  auto iter = dbr.p2name.find(db);
  if (dbr.p2name.end() == iter) {
    Status status = Status::InvalidArgument("NotFound db by ptr in repo");
    RocksDBExceptionJni::ThrowNew(env, status);
    return nullptr;
  }
  const auto& dbname = iter->second.name;
  auto i2 = dbr.name2p->find(dbname);
  if (dbr.name2p->end() == i2) {
    Status status = Status::InvalidArgument("NotFound db by name in repo");
    RocksDBExceptionJni::ThrowNew(env, status);
    return nullptr;
  }
  DB_Ptr dbp = i2->second;
  if (nullptr == dbp.dbm) {
    Status status = Status::InvalidArgument("DB_Ptr is not a DB_MultiCF");
    RocksDBExceptionJni::ThrowNew(env, status);
    return nullptr;
  }
  return dbp.dbm;
}

/*
 * Class:     org_rocksdb_SidePluginRepo
 * Method:    nativeCreateCF
 * Signature: (JJLjava/lang/String;Ljava/lang/String;)J
 */
JNIEXPORT jlong JNICALL Java_org_rocksdb_SidePluginRepo_nativeCreateCF
  (JNIEnv* env, jobject, jlong hrepo, jlong hdb, jstring jcfname, jstring jspec)
{
  auto repo = (SidePluginRepo*)hrepo;
  auto db = (DB*)hdb;
  DB_MultiCF* dbm = Get_DB_MultiCF(env, db, repo);
  if (!dbm) {
    return 0;
  }
  const char* cfname = env->GetStringUTFChars(jcfname, nullptr);
  const char* spec = env->GetStringUTFChars(jspec, nullptr);
  ROCKSDB_SCOPE_EXIT(
    env->ReleaseStringUTFChars(jspec, spec);
    env->ReleaseStringUTFChars(jcfname, cfname);
  );
  ColumnFamilyHandle* cfh = nullptr;
  Status status = dbm->CreateColumnFamily(cfname, spec, &cfh);
  if (!status.ok()) {
    RocksDBExceptionJni::ThrowNew(env, status);
    return 0;
  }
  return (jlong)cfh;
}

/*
 * Class:     org_rocksdb_SidePluginRepo
 * Method:    nativeDropCF
 * Signature: (JJLjava/lang/String;)V
 */
JNIEXPORT void JNICALL Java_org_rocksdb_SidePluginRepo_nativeDropCF__JJLjava_lang_String_2
  (JNIEnv* env, jobject, jlong hrepo, jlong hdb, jstring jcfname)
{
  auto repo = (SidePluginRepo*)hrepo;
  auto db = (DB*)hdb;
  DB_MultiCF* dbm = Get_DB_MultiCF(env, db, repo);
  if (!dbm) {
    return;
  }
  const char* cfname = env->GetStringUTFChars(jcfname, nullptr);
  ROCKSDB_SCOPE_EXIT(env->ReleaseStringUTFChars(jcfname, cfname));
  Status status = dbm->DropColumnFamily(cfname);
  if (!status.ok()) {
    RocksDBExceptionJni::ThrowNew(env, status);
  }
}

/*
 * Class:     org_rocksdb_SidePluginRepo
 * Method:    nativeDropCF
 * Signature: (JJJ)V
 */
JNIEXPORT void JNICALL Java_org_rocksdb_SidePluginRepo_nativeDropCF__JJJ
  (JNIEnv* env, jobject, jlong hrepo, jlong hdb, jlong hcf)
{
  auto repo = (SidePluginRepo*)hrepo;
  auto db = (DB*)hdb;
  DB_MultiCF* dbm = Get_DB_MultiCF(env, db, repo);
  if (!dbm) {
    return;
  }
  auto cfh = (ColumnFamilyHandle*)hcf;
  Status status = dbm->DropColumnFamily(cfh);
  if (!status.ok()) {
    RocksDBExceptionJni::ThrowNew(env, status);
  }
}

/*
 * Class:     org_rocksdb_SidePluginRepo
 * Method:    newSidePluginRepo
 * Signature: ()J
 */
jlong Java_org_rocksdb_SidePluginRepo_newSidePluginRepo
(JNIEnv* env, jclass clazz)
{
  auto repo = new SidePluginRepo();
  return GET_CPLUSPLUS_POINTER(repo);
}

/*
 * Class:     org_rocksdb_SidePluginRepo
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_SidePluginRepo_disposeInternal
(JNIEnv* env, jobject jrepo, jlong nativeHandle)
{
  auto repo = (SidePluginRepo*)nativeHandle;
  delete repo;
}

} // extern "C"
