// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import sun.misc.Unsafe;

/**
 * Base class for slices which will receive direct
 * ByteBuffer based access to the underlying data.
 * <p>
 * ByteBuffer backed slices typically perform better with
 * larger keys and values. When using smaller keys and
 * values consider using @see org.rocksdb.Slice
 */
public class DirectSlice extends AbstractSlice<ByteBuffer> {
  public static final DirectSlice NONE = new DirectSlice();

  /**
   * Indicates whether we have to free the memory pointed to by the Slice
   */
  private final boolean internalBuffer;
  private volatile boolean cleared = false;
  private volatile long internalBufferOffset = 0;

  private static final Unsafe myUnsafe;
  static {
    try {
      Field field = Unsafe.class.getDeclaredField("theUnsafe");
      field.setAccessible(true);
      myUnsafe = (Unsafe)field.get(null);
    } catch (Exception e) {
        throw new RuntimeException("Failed to get Unsafe instance", e);
    }
  }
  public static Unsafe getUnsafe() { return myUnsafe; }

  static int getFieldOffset(Class<?> clazz, String fieldName) throws Exception {
    Class<?> currentClass = clazz;
    while (currentClass != null) {
      try {
        Field field = currentClass.getDeclaredField(fieldName);
        // java --add-opens java.base/java.nio=ALL-UNNAMED is required
        // for this line, but we set fields by Unsafe, so we can ignore
        // the access check, and Unsafe can access private fields
        //field.setAccessible(true); // not needed for Unsafe
        return (int) myUnsafe.objectFieldOffset(field);
      } catch (NoSuchFieldException e) {
        currentClass = currentClass.getSuperclass();
      }
    }
    throw new NoSuchFieldException("Field '" + fieldName + "' not found in " + clazz);
  }

  private static final Method cleanMethod;
  private static final boolean destroyDirectBufferCleaner;
  private static final int cleanerOffset;
  private static final int addressOffset;
  private static final int capacityOffset;
  static {
    int cleanerOffset0 = -1; // -1 for field is not found or not accessible
    int addressOffset0 = -1;
    int capacityOffset0 = -1;
    try {
      Class<?> clazz = newZeroCopyDirectBuffer0().getClass();
      cleanerOffset0 = getFieldOffset(clazz, "cleaner");
      addressOffset0 = getFieldOffset(clazz, "address");
      capacityOffset0 = getFieldOffset(clazz, "capacity");
    } catch (Exception e) {
      cleanerOffset0 = -1;
      addressOffset0 = -1;
      capacityOffset0 = -1;
      System.err.println(
        "ERROR: Failed to find fields: cleaner,address,capacity in DirectByteBuffer,\n" +
        "   zero copy with old API is disabled, try add java startup option\n" +
        "        --add-opens java.base/java.nio=ALL-UNNAMED,\n" +
        "   detailed err:\n" + e.getMessage());
    }
    cleanerOffset = cleanerOffset0;
    addressOffset = addressOffset0;
    capacityOffset = capacityOffset0;

    // default to false for safety and backward compatibility
    boolean destroyDirectBufferCleaner0 = false;
    Method cleanMethod0 = null;
    String env = System.getenv("ROCKSDB_FORCE_DIRECT_BUFFER_ZERO_COPY");
    if (env != null) {
      destroyDirectBufferCleaner0 = Boolean.parseBoolean(env);
    }
    if (cleanerOffset > 0 && destroyDirectBufferCleaner0) {
      try {
        ByteBuffer bufferWithCleaner = ByteBuffer.allocateDirect(64);
        Object cleaner = myUnsafe.getObject(bufferWithCleaner, cleanerOffset);
        Class<?> clazz = cleaner.getClass();
        cleanMethod0 = clazz.getDeclaredMethod("clean");
        //cleanMethod0.setAccessible(true); // it is public, not needed
      }
      catch (Exception e) {
        System.err.println(
          "WARN: Failed to access cleaner.clean() method, " +
          "zero copy on ByteBuffer.allocateDirect(cap) is not supported.\n" +
          "    detailed err:\n" + e.getMessage());
        cleanMethod0 = null;
        destroyDirectBufferCleaner0 = false;
      }
    }
    cleanMethod = cleanMethod0;
    destroyDirectBufferCleaner = destroyDirectBufferCleaner0;
  }

  private static native ByteBuffer newZeroCopyDirectBuffer0();

  private static final MethodHandle constructZeroCopyBuffer;
  static {
    MethodHandle constructor = null;
    try {
      Class<?> clazz = Class.forName("java.nio.DirectByteBuffer");
      MethodHandles.Lookup lookup = MethodHandles.privateLookupIn(clazz, MethodHandles.lookup());
      MethodHandle directByteBufferConstructor = lookup.findConstructor(
            clazz, MethodType.methodType(void.class, long.class, long.class));
      constructor = directByteBufferConstructor.asType(
            MethodType.methodType(ByteBuffer.class, long.class, long.class));
    } catch (Exception e) {
      System.err.println("WARN: access java.nio.DirectByteBuffer failed: " + e + "\n" +
            "use --add-opens java.base/java.nio=ALL-UNNAMED to " +
            "create DirectByteBuffer faster(280ns to 60ns) by MethodHandle");
    }
    constructZeroCopyBuffer = constructor;
  }
  public static ByteBuffer newZeroCopyDirectBuffer() {
    if (constructZeroCopyBuffer != null) {
      try {
        long addr = 0L, cap = 0L;
        return (ByteBuffer)constructZeroCopyBuffer.invokeExact(addr, cap);
      } catch (Throwable e) {
        throw new RuntimeException(e);
      }
    }
    // this is slow path, constructorAsByteBuffer.invokeExact is faster
    if (!supportZeroCopy()) {
      throw new UnsupportedOperationException(
        "Zero-copy is not supported, try add java startup option " +
        "--add-opens java.base/java.nio=ALL-UNNAMED");
    }
    return newZeroCopyDirectBuffer0();
  }
  public static void directBorrowMemory(ByteBuffer buf, long ptr, long cap) {
    if (!buf.isDirect()) {
      throw new IllegalArgumentException("The ByteBuffer must be direct");
    }
    if (!supportZeroCopy()) {
      throw new UnsupportedOperationException(
        "Zero-copy is not supported, try add java startup option " +
        "--add-opens java.base/java.nio=ALL-UNNAMED");
    }
    if (!supportDirectBorrowMemory(buf)) {
      throw new UnsupportedOperationException(
        "The ByteBuffer has a cleaner, can not reset address and capacity");
    }
    directBorrowMemoryUnchecked(buf, ptr, cap);
  }
  public static void directBorrowMemoryUnchecked(ByteBuffer buf, long ptr, long cap) {
    // java makes simple things complicated,
    // to implement zero-copy, we have to do such dirty work
    //System.out.println("Reset DirectByteBuffer: " + buf + ", new ptr: " + ptr + ", cap: " + cap);
    if (destroyDirectBufferCleaner) {
      Object cleaner = myUnsafe.getObject(buf, cleanerOffset);
      if (cleaner != null) {
        // call cleaner.clean(), then destroy the cleaner, so that the buffer
        // can be reset and reused, otherwise, it will may cause memory leaks
        //System.out.println("Clean DirectByteBuffer: " + buf + ", new ptr: " + ptr + ", cap: " + cap);
        try {
          cleanMethod.invoke(cleaner);
          myUnsafe.putObject(buf, cleanerOffset, null);
        } catch (Exception e) {
          System.err.println(
            "ERROR: Failed to invoke DirectByteBuffer.cleaner.clean(), " +
            "detailed err:\n" + e.getMessage());
        }
      }
    }
    myUnsafe.putLong(buf, addressOffset, ptr);
    myUnsafe.putInt(buf, capacityOffset, (int)cap);
    buf.clear(); // reset position, limit, mark
  }
  public static boolean supportZeroCopy() {
    return cleanerOffset != -1;
  }
  public static boolean supportDirectBorrowMemory(ByteBuffer buffer) {
    // to help zero-copy, we need to check if the buffer has a cleaner,
    // if it has, its address and capacity can not be reset, thus can not
    // use zero-copy.
    assert buffer.isDirect() : "The ByteBuffer must be direct";
    if (cleanerOffset == -1) {
      return false; // can not be reset
    }
    if (destroyDirectBufferCleaner) {
      return true; // can be reset
    }
    return myUnsafe.getObject(buffer, cleanerOffset) == null;
  }
  public static int copyToDirectBuffer(long ptr, long len, ByteBuffer buf) {
    assert buf.isDirect() : "The ByteBuffer must be direct";
    long destAddr = myUnsafe.getLong(buf, addressOffset);
    int cplen = Math.min((int)len, buf.remaining());
    myUnsafe.copyMemory(ptr, destAddr + buf.position(), cplen);
    return cplen;
  }
  public static long getDirectAddress(ByteBuffer buf) {
    assert buf.isDirect();
    assert addressOffset >= 0;
    return myUnsafe.getLong(buf, addressOffset);
  }
  public static long getDirectCapacity(ByteBuffer buf) {
    assert buf.isDirect();
    assert capacityOffset >= 0;
    return myUnsafe.getLong(buf, capacityOffset);
  }

  private static final jdk.internal.misc.Unsafe moreUnsafe;
  static {
    String env = System.getenv("USE_INTERNAL_UNSAFE");
    if (env != null && Boolean.parseBoolean(env)) {
      moreUnsafe = jdk.internal.misc.Unsafe.getUnsafe();
    } else {
      moreUnsafe = null;
    }
  }
  public static final byte[] copyOfNativeByteArray(long slice) {
    long ptr = myUnsafe.getLong(slice + 0);
    long len = myUnsafe.getLong(slice + 8);
    return copyOfNativeByteArray(ptr, len);
  }
  public static final byte[] copyOfNativeByteArray(long addr, long len) {
    if (len > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("array len " + len + " exceeds Integer.MAX_VALUE");
    }
    final Object ba;
    if (moreUnsafe == null) {
      ba = new byte[(int)len];
    } else {
      ba = moreUnsafe.allocateUninitializedArray(byte.class, (int)len);
    }
    myUnsafe.copyMemory(null, addr, ba, Unsafe.ARRAY_BYTE_BASE_OFFSET, len);
    return (byte[])ba;
  }
  // allocate and copy byte[] in java side is faster than in JNI side
  public static final long nativeCopyOf(byte[] ba) {
    long addr = myUnsafe.allocateMemory(ba.length);
    myUnsafe.copyMemory(ba, Unsafe.ARRAY_BYTE_BASE_OFFSET, null, addr, ba.length);
    return addr;
  }
  public static final long nativeCopyOf(byte[] ba, int offset, int len) {
    assert offset >= 0;
    assert offset <= ba.length; // (ba.length, 0) is a valid sub sequence
    assert len >= 0;
    assert offset + len <= ba.length;
    long addr = myUnsafe.allocateMemory(len);
    myUnsafe.copyMemory(ba, Unsafe.ARRAY_BYTE_BASE_OFFSET + offset, null, addr, len);
    return addr;
  }
  public static final long nativeSliceOf(byte[] ba) {
    long slice = myUnsafe.allocateMemory(16 + ba.length);
    long data = slice + 16;
    myUnsafe.putLong(slice + 0,  data);
    myUnsafe.putLong(slice + 8,  ba.length);
    myUnsafe.copyMemory(ba, Unsafe.ARRAY_BYTE_BASE_OFFSET, null, data, ba.length);
    return slice;
  }
  public static final long createNativeSliceVec(byte[][] baa) {
    long sumKeyLen = 0;
    for (byte[] ba : baa) sumKeyLen += ba.length;
    long sliceVec = myUnsafe.allocateMemory(16 * baa.length + sumKeyLen);
    long currData = sliceVec + 16 * baa.length;
    for (int i = 0; i < baa.length; i++) { // fill C++ rocksdb::Slice
      myUnsafe.putLong(sliceVec + 16*i + 0, currData);
      myUnsafe.putLong(sliceVec + 16*i + 8, baa[i].length);
      currData += baa[i].length;
    }
    currData = sliceVec + 16 * baa.length; // re-init
    for (int i = 0; i < baa.length; i++) { // copy keys content
      myUnsafe.copyMemory(
        baa[i], Unsafe.ARRAY_BYTE_BASE_OFFSET,
        null, currData, baa[i].length);
      currData += baa[i].length;
    }
    return sliceVec;
  }
  public static final byte[][] copyNativeSliceVec(int num, long sliceVec) {
    byte[][] baa = new byte[num][];
    for (int i = 0; i < num; i++) { // C++ side rocksdb::Slice
      long data = myUnsafe.getLong(sliceVec + i*16 + 0);
      long size = myUnsafe.getLong(sliceVec + i*16 + 8);
      if (data != 0) {
        baa[i] = copyOfNativeByteArray(data, size);
      }
    }
    return baa;
  }

  /**
   * Called from JNI to construct a new Java DirectSlice
   * without an underlying C++ object set
   * at creation time.
   * <p>
   * Note: You should be aware that it is intentionally marked as
   * package-private. This is so that developers cannot construct their own
   * default DirectSlice objects (at present). As developers cannot construct
   * their own DirectSlice objects through this, they are not creating
   * underlying C++ DirectSlice objects, and so there is nothing to free
   * (dispose) from Java.
   */
  DirectSlice() {
    super();
    this.internalBuffer = false;
  }

  /**
   * Constructs a slice
   * where the data is taken from
   * a String.
   *
   * @param str The string
   */
  public DirectSlice(final String str) {
    super(createNewSliceFromString(str));
    this.internalBuffer = true;
  }

  /**
   * Constructs a slice where the data is
   * read from the provided
   * ByteBuffer up to a certain length
   *
   * @param data The buffer containing the data
   * @param length The length of the data to use for the slice
   */
  public DirectSlice(final ByteBuffer data, final int length) {
    super(createNewDirectSlice0(ensureDirect(data), length));
    this.internalBuffer = false;
  }

  /**
   * Constructs a slice where the data is
   * read from the provided
   * ByteBuffer
   *
   * @param data The bugger containing the data
   */
  public DirectSlice(final ByteBuffer data) {
    super(createNewDirectSlice1(ensureDirect(data)));
    this.internalBuffer = false;
  }

  private static ByteBuffer ensureDirect(final ByteBuffer data) {
    if(!data.isDirect()) {
      throw new IllegalArgumentException("The ByteBuffer must be direct");
    }
    return data;
  }

  /**
   * Retrieves the byte at a specific offset
   * from the underlying data
   *
   * @param offset The (zero-based) offset of the byte to retrieve
   *
   * @return the requested byte
   */
  public byte get(final int offset) {
    return get0(getNativeHandle(), offset);
  }

  @Override
  public void clear() {
    clear0(getNativeHandle(), !cleared && internalBuffer, internalBufferOffset);
    cleared = true;
  }

  @Override
  public void removePrefix(final int n) {
    removePrefix0(getNativeHandle(), n);
    this.internalBufferOffset += n;
  }

  public void setLength(final int n) {
    setLength0(getNativeHandle(), n);
  }

  @Override
  protected void disposeInternal() {
    final long nativeHandle = getNativeHandle();
    if(!cleared && internalBuffer) {
      disposeInternalBuf(nativeHandle, internalBufferOffset);
    }
    disposeInternal(nativeHandle);
  }

  private static native long createNewDirectSlice0(final ByteBuffer data, final int length);
  private static native long createNewDirectSlice1(final ByteBuffer data);
  @Override protected final native ByteBuffer data0(long handle);
  private native byte get0(long handle, int offset);
  private native void clear0(long handle, boolean internalBuffer,
      long internalBufferOffset);
  private native void removePrefix0(long handle, int length);
  private native void setLength0(long handle, int length);
  private native void disposeInternalBuf(final long handle,
      long internalBufferOffset);
}
