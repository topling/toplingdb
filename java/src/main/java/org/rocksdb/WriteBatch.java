// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.nio.ByteBuffer;
import sun.misc.Unsafe;

/**
 * WriteBatch holds a collection of updates to apply atomically to a DB.
 * <p>
 * The updates are applied in the order in which they are added
 * to the WriteBatch.  For example, the value of "key" will be "v3"
 * after the following batch is written:
 * <p>
 *    batch.put("key", "v1");
 *    batch.remove("key");
 *    batch.put("key", "v2");
 *    batch.put("key", "v3");
 * <p>
 * Multiple threads can invoke const methods on a WriteBatch without
 * external synchronization, but if any of the threads may call a
 * non-const method, all threads accessing the same WriteBatch must use
 * external synchronization.
 */
public class WriteBatch extends AbstractWriteBatch {
  /**
   * Constructs a WriteBatch instance.
   */
  public WriteBatch() {
    this(0);
  }

  /**
   * Constructs a WriteBatch instance with a given size.
   *
   * @param reserved_bytes reserved size for WriteBatch
   */
  public WriteBatch(final int reserved_bytes) {
    super(newWriteBatch(reserved_bytes));
    isJniWriteBatch = BUILD_WRITE_BATCH_IN_JAVA_SIDE;
  }

  /**
   * Constructs a WriteBatch instance from a serialized representation
   * as returned by {@link #data()}.
   *
   * @param serialized the serialized representation.
   */
  public WriteBatch(final byte[] serialized) {
    super(newWriteBatch(serialized, serialized.length));
    isJniWriteBatch = BUILD_WRITE_BATCH_IN_JAVA_SIDE;
  }

  private final boolean isJniWriteBatch;
  private static final boolean BUILD_WRITE_BATCH_IN_JAVA_SIDE;
  static {
    final String env = System.getenv("BUILD_WRITE_BATCH_IN_JAVA_SIDE");
    if (env != null)
      BUILD_WRITE_BATCH_IN_JAVA_SIDE = Boolean.parseBoolean(env);
    else
      BUILD_WRITE_BATCH_IN_JAVA_SIDE = true; // default
  }
  private static final Unsafe myUnsafe = DirectSlice.getUnsafe();
  private static final long writeVarLenPrefixedByteArray(long dst, byte[] ba) {
    int len = ba.length;
    dst = EncodeVarint32(dst, len);
    myUnsafe.copyMemory(ba, Unsafe.ARRAY_BYTE_BASE_OFFSET, null, dst, len);
    return dst + len;
  }
  public static final int VarUint32Length(int v) {
    if (v < (1 <<  7))
      return 1;
    else if (v < (1 << 14))
      return 2;
    else if (v < (1 << 21))
      return 3;
    else if (v < (1 << 28))
      return 4;
    else
      return 5;
  }
  // copy from coding.cc
  public static final long EncodeVarint32(long dst, int v) {
    long ptr = dst;
    final int B = 128;
    if (v < (1 << 7)) {
      myUnsafe.putByte(ptr++, (byte)v);
    } else if (v < (1 << 14)) {
      myUnsafe.putByte(ptr++, (byte)(v | B));
      myUnsafe.putByte(ptr++, (byte)(v >>> 7));
    } else if (v < (1 << 21)) {
      myUnsafe.putByte(ptr++, (byte)(v | B));
      myUnsafe.putByte(ptr++, (byte)((v >>> 7) | B));
      myUnsafe.putByte(ptr++, (byte)(v >>> 14));
    } else if (v < (1 << 28)) {
      myUnsafe.putByte(ptr++, (byte)(v | B));
      myUnsafe.putByte(ptr++, (byte)((v >>> 7) | B));
      myUnsafe.putByte(ptr++, (byte)((v >>> 14) | B));
      myUnsafe.putByte(ptr++, (byte)(v >>> 21));
    } else {
      myUnsafe.putByte(ptr++, (byte)(v | B));
      myUnsafe.putByte(ptr++, (byte)((v >>> 7) | B));
      myUnsafe.putByte(ptr++, (byte)((v >>> 14) | B));
      myUnsafe.putByte(ptr++, (byte)((v >>> 21) | B));
      myUnsafe.putByte(ptr++, (byte)(v >>> 28));
    }
    return ptr;
  }

  private static final  int ADDR_SIZE_CAP_OFFSET = getAddrSizeCapOffset();
  private static native int getAddrSizeCapOffset();
  private static final  int content_flags_offset = get_content_flags_offset();
  private static native int get_content_flags_offset();
  private final boolean fastContentFlags(int flag) {
    int bits = myUnsafe.getInt(nativeHandle_ + content_flags_offset);
    return (bits & flag) != 0;
  }
  private final native void updateNativeDataSizeFromJava0(long handle);
  final void updateNativeDataSizeFromJava() {
    if (isJniWriteBatch) {
      updateNativeDataSizeFromJava0(nativeHandle_);
    }
  }
  private final native void updateJavaAddrSizeCapFromNative0(long handle);
  final void updateJavaAddrSizeCapFromNative() {
    if (isJniWriteBatch) {
      updateJavaAddrSizeCapFromNative0(nativeHandle_);
    }
  }
  private final native void ensureCapacity(long handle, long newCap);
  private final long repDataAddr() {
    return myUnsafe.getLong(nativeHandle_ + ADDR_SIZE_CAP_OFFSET);
  }
  private final long repSize() {
    return myUnsafe.getLong(nativeHandle_ + ADDR_SIZE_CAP_OFFSET + 8);
  }
  private final long repCap() {
    return myUnsafe.getLong(nativeHandle_ + ADDR_SIZE_CAP_OFFSET + 16);
  }
  private final void updateRepSize(long ptr) {
    long addr = repDataAddr();
    long size = ptr - addr;
    assert size > 12;
    assert size <= repCap();
    myUnsafe.putByte(ptr, (byte)0); // end of str
    myUnsafe.putLong(nativeHandle_ + ADDR_SIZE_CAP_OFFSET + 8, size);
  }
  private final void finishOperation(long ptr, int flag) {
    long addr = repDataAddr();
    long size = ptr - addr;
    assert size > 12;
    assert size <= repCap();
    int cnt = myUnsafe.getInt(addr + 8);
    myUnsafe.putInt(addr + 8, cnt + 1);
    myUnsafe.putByte(ptr, (byte)0); // end of str
    myUnsafe.putLong(nativeHandle_ + ADDR_SIZE_CAP_OFFSET + 8, size);
    long flagsPtr = nativeHandle_ + content_flags_offset;
    int old = myUnsafe.getInt(flagsPtr);
    myUnsafe.putInt(flagsPtr, old | flag);
  }

  private static final byte kTypeDeletion = 0x0;
  private static final byte kTypeValue = 0x1;
  private static final byte kTypeMerge = 0x2;
  private static final byte kTypeLogData = 0x3;               // WAL only.
  private static final byte kTypeColumnFamilyDeletion = 0x4;  // WAL only.
  private static final byte kTypeColumnFamilyValue = 0x5;     // WAL only.
  private static final byte kTypeColumnFamilyMerge = 0x6;     // WAL only.
  private static final byte kTypeSingleDeletion = 0x7;
  private static final byte kTypeColumnFamilySingleDeletion = 0x8;  // WAL only.

  private static final int DEFERRED = 1 << 0;
  private static final int HAS_PUT = 1 << 1;
  private static final int HAS_DELETE = 1 << 2;
  private static final int HAS_SINGLE_DELETE = 1 << 3;
  private static final int HAS_MERGE = 1 << 4;
  private static final int HAS_BEGIN_PREPARE = 1 << 5;
  private static final int HAS_END_PREPARE = 1 << 6;
  private static final int HAS_COMMIT = 1 << 7;
  private static final int HAS_ROLLBACK = 1 << 8;
  private static final int HAS_DELETE_RANGE = 1 << 9;
  private static final int HAS_BLOB_INDEX = 1 << 10;
  private static final int HAS_BEGIN_UNPREPARE = 1 << 11;
  private static final int HAS_PUT_ENTITY = 1 << 12;

  private final int fastCount() {
    int cnt = myUnsafe.getInt(repDataAddr() + 8);
    assert count0(nativeHandle_) == cnt;
    return cnt;
  }

  @Override
  public final int count() {
    if (isJniWriteBatch)
      return fastCount();
    else
      return count0(nativeHandle_);
  }

  @Override
  public final void put(final byte[] key, final byte[] value) throws RocksDBException {
    put(null, key, value);
  }

  private static final long operationSize(int cf_id, byte[] key) {
    return 1 + (cf_id != 0 ? VarUint32Length(cf_id) : 0)
             + VarUint32Length(key.length) + key.length;
  }
  private static final long operationSize(int cf_id, byte[] key, byte[] value) {
    return operationSize(cf_id, key) + VarUint32Length(value.length) + value.length;
  }
  @Override
  public void put(final ColumnFamilyHandle cf, final byte[] key, final byte[] value)
      throws RocksDBException {
    if (!isJniWriteBatch) {
      if (cf != null) {
        put(nativeHandle_, key, key.length, value, value.length, cf.nativeHandle_);
      } else {
        put(nativeHandle_, key, key.length, value, value.length);
      }
      return;
    }
    int cf_id = cf != null ? cf.getID() : 0;
    long old_size = repSize();
    long inc_size = operationSize(cf_id, key, value);
    if (old_size + inc_size > repCap()) { // slow path, call to jni
      ensureCapacity(nativeHandle_, old_size + inc_size);
      assert repCap() >= old_size + inc_size;
    }
    long ptr = repDataAddr() + old_size;
    if (cf_id == 0) {
      myUnsafe.putByte(ptr, kTypeValue);
      ptr += 1;
    } else {
      myUnsafe.putByte(ptr, kTypeColumnFamilyValue);
      ptr = EncodeVarint32(ptr + 1, cf_id);
    }
    ptr = writeVarLenPrefixedByteArray(ptr, key);
    ptr = writeVarLenPrefixedByteArray(ptr, value);
    finishOperation(ptr, HAS_PUT);
  }

  @Override
  public void merge(final byte[] key, final byte[] value) throws RocksDBException {
    merge(null, key, value);
  }

  @Override
  public void merge(final ColumnFamilyHandle cf, final byte[] key, final byte[] value)
      throws RocksDBException {
    if (!isJniWriteBatch) {
      if (cf != null) {
        merge(nativeHandle_, key, key.length, value, value.length, cf.nativeHandle_);
      } else {
        merge(nativeHandle_, key, key.length, value, value.length);
      }
      return;
    }
    int cf_id = cf != null ? cf.getID() : 0;
    long old_size = repSize();
    long inc_size = operationSize(cf_id, key, value);
    if (old_size + inc_size > repCap()) { // slow path, call to jni
      ensureCapacity(nativeHandle_, old_size + inc_size);
      assert repCap() >= old_size + inc_size;
    }
    long ptr = repDataAddr() + old_size;
    if (cf_id == 0) {
      myUnsafe.putByte(ptr, kTypeMerge);
      ptr += 1;
    } else {
      myUnsafe.putByte(ptr, kTypeColumnFamilyMerge);
      ptr = EncodeVarint32(ptr + 1, cf_id);
    }
    ptr = writeVarLenPrefixedByteArray(ptr, key);
    ptr = writeVarLenPrefixedByteArray(ptr, value);
    finishOperation(ptr, HAS_MERGE);
  }

  @Override
  public void delete(final byte[] key) throws RocksDBException {
    delete(null, key);
  }

  @Override
  public void delete(final ColumnFamilyHandle cf, final byte[] key)
      throws RocksDBException {
    if (!isJniWriteBatch) {
      if (cf != null) {
        delete(nativeHandle_, key, key.length, cf.nativeHandle_);
      } else {
        delete(nativeHandle_, key, key.length);
      }
      return;
    }
    int cf_id = cf != null ? cf.getID() : 0;
    long old_size = repSize();
    long inc_size = operationSize(cf_id, key);
    if (old_size + inc_size > repCap()) { // slow path, call to jni
      ensureCapacity(nativeHandle_, old_size + inc_size);
      assert repCap() >= old_size + inc_size;
    }
    long ptr = repDataAddr() + old_size;
    if (cf_id == 0) {
      myUnsafe.putByte(ptr, kTypeDeletion);
      ptr += 1;
    } else {
      myUnsafe.putByte(ptr, kTypeColumnFamilyDeletion);
      ptr = EncodeVarint32(ptr + 1, cf_id);
    }
    ptr = writeVarLenPrefixedByteArray(ptr, key);
    finishOperation(ptr, HAS_DELETE);
  }

  @Override
  public void singleDelete(final byte[] key) throws RocksDBException {
    singleDelete(null, key);
  }

  @Override
  public void singleDelete(final ColumnFamilyHandle cf, final byte[] key)
      throws RocksDBException {
    if (!isJniWriteBatch) {
      if (cf != null) {
        singleDelete(nativeHandle_, key, key.length, cf.nativeHandle_);
      } else {
        singleDelete(nativeHandle_, key, key.length);
      }
      return;
    }
    int cf_id = cf != null ? cf.getID() : 0;
    long old_size = repSize();
    long inc_size = operationSize(cf_id, key);
    if (old_size + inc_size > repCap()) { // slow path, call to jni
      ensureCapacity(nativeHandle_, old_size + inc_size);
      assert repCap() >= old_size + inc_size;
    }
    long ptr = repDataAddr() + old_size;
    if (cf_id == 0) {
      myUnsafe.putByte(ptr, kTypeSingleDeletion);
      ptr += 1;
    } else {
      myUnsafe.putByte(ptr, kTypeColumnFamilySingleDeletion);
      ptr = EncodeVarint32(ptr + 1, cf_id);
    }
    ptr = writeVarLenPrefixedByteArray(ptr, key);
    finishOperation(ptr, HAS_SINGLE_DELETE);
  }

  @Override
  public void putLogData(final byte[] blob) throws RocksDBException {
    if (!isJniWriteBatch) {
      putLogData(nativeHandle_, blob, blob.length);
      return;
    }
    long old_size = repSize();
    long inc_size = 1 + VarUint32Length(blob.length) + blob.length;
    if (old_size + inc_size > repCap()) { // slow path, call to jni
      ensureCapacity(nativeHandle_, old_size + inc_size);
      assert repCap() >= old_size + inc_size;
    }
    long ptr = repDataAddr() + old_size;
    myUnsafe.putByte(ptr, kTypeLogData);
    ptr = writeVarLenPrefixedByteArray(ptr + 1, blob);
    updateRepSize(ptr);
  }

  /**
   * Support for iterating over the contents of a batch.
   *
   * @param handler A handler that is called back for each
   *                update present in the batch
   *
   * @throws RocksDBException If we cannot iterate over the batch
   */
  public void iterate(final Handler handler) throws RocksDBException {
    updateNativeDataSizeFromJava();
    iterate(nativeHandle_, handler.nativeHandle_);
  }

  /**
   * Retrieve the serialized version of this batch.
   *
   * @return the serialized representation of this write batch.
   *
   * @throws RocksDBException if an error occurs whilst retrieving
   *   the serialized batch data.
   */
  public byte[] data() throws RocksDBException {
    if (isJniWriteBatch) {
      long slice = nativeHandle_ + ADDR_SIZE_CAP_OFFSET;
      return DirectSlice.copyOfNativeByteArray(slice);
    }
    return data(nativeHandle_);
  }

  /**
   * Retrieve data size of the batch.
   *
   * @return the serialized data size of the batch.
   */
  public long getDataSize() {
    if (isJniWriteBatch) {
      return repSize();
    }
    return getDataSize(nativeHandle_);
  }

  /**
   * Returns true if Put will be called during Iterate.
   *
   * @return true if Put will be called during Iterate.
   */
  public boolean hasPut() {
    boolean b = fastContentFlags(HAS_PUT);
    assert b == hasPut(nativeHandle_);
    return b;
  }

  /**
   * Returns true if Delete will be called during Iterate.
   *
   * @return true if Delete will be called during Iterate.
   */
  public boolean hasDelete() {
    boolean b = fastContentFlags(HAS_DELETE);
    assert b == hasDelete(nativeHandle_);
    return b;
  }

  /**
   * Returns true if SingleDelete will be called during Iterate.
   *
   * @return true if SingleDelete will be called during Iterate.
   */
  public boolean hasSingleDelete() {
    boolean b = fastContentFlags(HAS_SINGLE_DELETE);
    assert b == hasSingleDelete(nativeHandle_);
    return b;
  }

  /**
   * Returns true if DeleteRange will be called during Iterate.
   *
   * @return true if DeleteRange will be called during Iterate.
   */
  public boolean hasDeleteRange() {
    boolean b = fastContentFlags(HAS_DELETE_RANGE);
    assert b == hasDeleteRange(nativeHandle_);
    return b;
  }

  /**
   * Returns true if Merge will be called during Iterate.
   *
   * @return true if Merge will be called during Iterate.
   */
  public boolean hasMerge() {
    boolean b = fastContentFlags(HAS_MERGE);
    assert b == hasMerge(nativeHandle_);
    return b;
  }

  /**
   * Returns true if MarkBeginPrepare will be called during Iterate.
   *
   * @return true if MarkBeginPrepare will be called during Iterate.
   */
  public boolean hasBeginPrepare() {
    boolean b = fastContentFlags(HAS_BEGIN_PREPARE);
    assert b == hasBeginPrepare(nativeHandle_);
    return b;
  }

  /**
   * Returns true if MarkEndPrepare will be called during Iterate.
   *
   * @return true if MarkEndPrepare will be called during Iterate.
   */
  public boolean hasEndPrepare() {
    boolean b = fastContentFlags(HAS_END_PREPARE);
    assert b == hasEndPrepare(nativeHandle_);
    return b;
  }

  /**
   * Returns true if MarkCommit will be called during Iterate.
   *
   * @return true if MarkCommit will be called during Iterate.
   */
  public boolean hasCommit() {
    boolean b = fastContentFlags(HAS_COMMIT);
    assert b == hasCommit(nativeHandle_);
    return b;
  }

  /**
   * Returns true if MarkRollback will be called during Iterate.
   *
   * @return true if MarkRollback will be called during Iterate.
   */
  public boolean hasRollback() {
    boolean b = fastContentFlags(HAS_ROLLBACK);
    assert b == hasRollback(nativeHandle_);
    return b;
  }

  @Override
  public WriteBatch getWriteBatch() {
    return this;
  }

  /**
   * Marks this point in the WriteBatch as the last record to
   * be inserted into the WAL, provided the WAL is enabled.
   */
  public void markWalTerminationPoint() {
    markWalTerminationPoint(nativeHandle_);
  }

  /**
   * Gets the WAL termination point.
   * <p>
   * See {@link #markWalTerminationPoint()}
   *
   * @return the WAL termination point
   */
  public SavePoint getWalTerminationPoint() {
    return getWalTerminationPoint(nativeHandle_);
  }

  @Override
  WriteBatch getWriteBatch(final long handle) {
    return this;
  }

  /**
   * <p>Private WriteBatch constructor which is used to construct
   * WriteBatch instances from C++ side. As the reference to this
   * object is also managed from C++ side the handle will be disowned.</p>
   *
   * @param nativeHandle address of native instance.
   */
  WriteBatch(final long nativeHandle) {
    this(nativeHandle, false);
  }

  /**
   * <p>Private WriteBatch constructor which is used to construct
   * WriteBatch instances. </p>
   *
   * @param nativeHandle address of native instance.
   * @param owningNativeHandle whether to own this reference from the C++ side or not
   */
  WriteBatch(final long nativeHandle, final boolean owningNativeHandle) {
    super(nativeHandle);
    if(!owningNativeHandle)
      disOwnNativeHandle();
    isJniWriteBatch = false;
  }

  @Override protected final native void disposeInternal(final long handle);
  @Override final native int count0(final long handle);
  @Override final native void put(final long handle, final byte[] key,
      final int keyLen, final byte[] value, final int valueLen);
  @Override final native void put(final long handle, final byte[] key,
      final int keyLen, final byte[] value, final int valueLen,
      final long cfHandle);
  @Override
  final native void putDirect(final long handle, final ByteBuffer key, final int keyOffset,
      final int keyLength, final ByteBuffer value, final int valueOffset, final int valueLength,
      final long cfHandle);
  @Override final native void merge(final long handle, final byte[] key,
      final int keyLen, final byte[] value, final int valueLen);
  @Override final native void merge(final long handle, final byte[] key,
      final int keyLen, final byte[] value, final int valueLen,
      final long cfHandle);
  @Override final native void delete(final long handle, final byte[] key,
      final int keyLen) throws RocksDBException;
  @Override final native void delete(final long handle, final byte[] key,
      final int keyLen, final long cfHandle) throws RocksDBException;
  @Override final native void singleDelete(final long handle, final byte[] key,
      final int keyLen) throws RocksDBException;
  @Override final native void singleDelete(final long handle, final byte[] key,
      final int keyLen, final long cfHandle) throws RocksDBException;
  @Override
  final native void deleteDirect(final long handle, final ByteBuffer key, final int keyOffset,
      final int keyLength, final long cfHandle) throws RocksDBException;
  @Override
  final native void deleteRange(final long handle, final byte[] beginKey, final int beginKeyLen,
      final byte[] endKey, final int endKeyLen);
  @Override
  final native void deleteRange(final long handle, final byte[] beginKey, final int beginKeyLen,
      final byte[] endKey, final int endKeyLen, final long cfHandle);
  @Override final native void putLogData(final long handle,
      final byte[] blob, final int blobLen) throws RocksDBException;
  @Override final native void clear0(final long handle);
  @Override final native void setSavePoint0(final long handle);
  @Override final native void rollbackToSavePoint0(final long handle);
  @Override final native void popSavePoint(final long handle) throws RocksDBException;
  @Override final native void setMaxBytes(final long nativeHandle,
    final long maxBytes);

  private static native long newWriteBatch(final int reserved_bytes);
  private static native long newWriteBatch(final byte[] serialized, final int serializedLength);
  private native void iterate(final long handle, final long handlerHandle)
      throws RocksDBException;
  private native byte[] data(final long nativeHandle) throws RocksDBException;
  private native long getDataSize(final long nativeHandle);
  private native boolean hasPut(final long nativeHandle);
  private native boolean hasDelete(final long nativeHandle);
  private native boolean hasSingleDelete(final long nativeHandle);
  private native boolean hasDeleteRange(final long nativeHandle);
  private native boolean hasMerge(final long nativeHandle);
  private native boolean hasBeginPrepare(final long nativeHandle);
  private native boolean hasEndPrepare(final long nativeHandle);
  private native boolean hasCommit(final long nativeHandle);
  private native boolean hasRollback(final long nativeHandle);
  private native void markWalTerminationPoint(final long nativeHandle);
  private native SavePoint getWalTerminationPoint(final long nativeHandle);

  /**
   * Handler callback for iterating over the contents of a batch.
   */
  public abstract static class Handler extends RocksCallbackObject {
    public Handler() {
      super(0L);
    }

    @Override
    protected long initializeNative(final long... nativeParameterHandles) {
      return createNewHandler0();
    }

    public abstract void put(final int columnFamilyId, final byte[] key,
        final byte[] value) throws RocksDBException;
    public abstract void put(final byte[] key, final byte[] value);
    public abstract void merge(final int columnFamilyId, final byte[] key,
        final byte[] value) throws RocksDBException;
    public abstract void merge(final byte[] key, final byte[] value);
    public abstract void delete(final int columnFamilyId, final byte[] key)
        throws RocksDBException;
    public abstract void delete(final byte[] key);
    public abstract void singleDelete(final int columnFamilyId,
        final byte[] key) throws RocksDBException;
    public abstract void singleDelete(final byte[] key);
    public abstract void deleteRange(final int columnFamilyId,
        final byte[] beginKey, final byte[] endKey) throws RocksDBException;
    public abstract void deleteRange(final byte[] beginKey,
        final byte[] endKey);
    public abstract void logData(final byte[] blob);
    public abstract void putBlobIndex(final int columnFamilyId,
        final byte[] key, final byte[] value) throws RocksDBException;
    public abstract void markBeginPrepare() throws RocksDBException;
    public abstract void markEndPrepare(final byte[] xid)
        throws RocksDBException;
    public abstract void markNoop(final boolean emptyBatch)
        throws RocksDBException;
    public abstract void markRollback(final byte[] xid)
        throws RocksDBException;
    public abstract void markCommit(final byte[] xid)
        throws RocksDBException;
    public abstract void markCommitWithTimestamp(final byte[] xid, final byte[] ts)
        throws RocksDBException;

    /**
     * shouldContinue is called by the underlying iterator
     * {@link WriteBatch#iterate(Handler)}. If it returns false,
     * iteration is halted. Otherwise, it continues
     * iterating. The default implementation always
     * returns true.
     *
     * @return boolean value indicating if the
     *     iteration is halted.
     */
    public boolean shouldContinue() {
      return true;
    }

    private native long createNewHandler0();
  }

  /**
   * A structure for describing the save point in the Write Batch.
   */
  public static class SavePoint {
    private long size;
    private long count;
    private long contentFlags;

    public SavePoint(final long size, final long count,
        final long contentFlags) {
      this.size = size;
      this.count = count;
      this.contentFlags = contentFlags;
    }

    public void clear() {
      this.size = 0;
      this.count = 0;
      this.contentFlags = 0;
    }

    /**
     * Get the size of the serialized representation.
     *
     * @return the size of the serialized representation.
     */
    public long getSize() {
      return size;
    }

    /**
     * Get the number of elements.
     *
     * @return the number of elements.
     */
    public long getCount() {
      return count;
    }

    /**
     * Get the content flags.
     *
     * @return the content flags.
     */
    public long getContentFlags() {
      return contentFlags;
    }

    public boolean isCleared() {
      return (size | count | contentFlags) == 0;
    }
  }
}
