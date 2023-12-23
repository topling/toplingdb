/**
 * Copyright (c) 2023-present, Topling, Inc.  All rights reserved.
 * This source code is licensed under both the GPLv2 (found in the
 * COPYING file in the root directory) and Apache 2.0 License
 * (found in the LICENSE.Apache file in the root directory).
 */
/**
 * This file is copied from GetBenchmarks.java and modified for
 * using SidePluginRepo.
 */
package org.rocksdb.jmh;

import static org.rocksdb.util.KVUtils.ba;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.openjdk.jmh.annotations.*;
import org.rocksdb.*;
import org.rocksdb.util.FileUtils;

@State(Scope.Benchmark)
public class SideGetBenchmarks {
  @Param({"1000", "100000"}) int keyCount;
  @Param({"12", "64", "128"}) int keySize;
  @Param({"64", "1024", "65536"}) int valueSize;
  @Param({"jmh-side-conf.json"}) String sideConf;
  @Param({""}) String dbname;
  @Param({""}) String dbpath;
  @Param({"dbo"}) String dboName;

  SidePluginRepo repo;
  ReadOptions readOptions;
  private AtomicInteger cfHandlesIdx = new AtomicInteger(1);
  ColumnFamilyHandle[] cfHandles;
  int cfs = 0;  // number of column families
  RocksDB db;
  private final AtomicInteger keyIndex = new AtomicInteger();
  private ByteBuffer keyBuf;
  private ByteBuffer valueBuf;
  private byte[] keyArr;
  private byte[] valueArr;

  @Setup(Level.Trial)
  public void setup() throws IOException, RocksDBException {
    RocksDB.loadLibrary();

    readOptions = new ReadOptions();
    repo = new SidePluginRepo();
    repo.importAutoFile(sideConf);

    final List<ColumnFamilyHandle> cfHandlesList = new ArrayList<ColumnFamilyHandle>();
    if (dbname.isEmpty()) {
      db = repo.openDB(cfHandlesList);
    } else {
      // use legacy rocksdb method to open db
      DBOptions dbo = repo.getDBOptions(dboName);
      String cfname = "default";
      ColumnFamilyOptions cfo = repo.getCFOptions(cfname);
      List<ColumnFamilyDescriptor> cf_desc = new ArrayList<ColumnFamilyDescriptor>();
      cf_desc.add(new ColumnFamilyDescriptor(cfname.getBytes(), cfo));
      db = RocksDB.open(dbo, dbpath, cf_desc, cfHandlesList);
      repo.put(dbname, db);
    }
    repo.startHttpServer();
    cfHandles = cfHandlesList.toArray(new ColumnFamilyHandle[0]);
    cfs = cfHandles.length - 1; // conform old GetBenchmarks

    // store initial data for retrieving via get
    keyArr = new byte[keySize];
    valueArr = new byte[valueSize];
    Arrays.fill(keyArr, (byte) 0x30);
    Arrays.fill(valueArr, (byte) 0x30);
    for (int i = 0; i <= cfs; i++) {
      for (int j = 0; j < keyCount; j++) {
        final byte[] keyPrefix = ba("key" + j);
        final byte[] valuePrefix = ba("value" + j);
        System.arraycopy(keyPrefix, 0, keyArr, 0, keyPrefix.length);
        System.arraycopy(valuePrefix, 0, valueArr, 0, valuePrefix.length);
        db.put(cfHandles[i], keyArr, valueArr);
      }
    }

    try (final FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true)) {
      db.flush(flushOptions);
    }

    keyBuf = ByteBuffer.allocateDirect(keySize);
    valueBuf = ByteBuffer.allocateDirect(valueSize);
    Arrays.fill(keyArr, (byte) 0x30);
    Arrays.fill(valueArr, (byte) 0x30);
    keyBuf.put(keyArr);
    keyBuf.flip();
    valueBuf.put(valueArr);
    valueBuf.flip();
  }

  @TearDown(Level.Trial)
  public void cleanup() throws IOException {
    repo.closeHttpServer();
    repo.closeAllDB(); // aslo can be repo.close()
  /* // not needed, will be closed in repo.closeAllDB(),
     // also dup close will not yield bad side effect
    for (final ColumnFamilyHandle cfHandle : cfHandles) {
      cfHandle.close();
    }
    db.close();
  */
    readOptions.close();
  }

  private ColumnFamilyHandle getColumnFamily() {
    if (cfs == 0) {
      return cfHandles[0];
    } else if (cfs == 1) {
      return cfHandles[1];
    } else {
      int idx = cfHandlesIdx.getAndIncrement();
      if (idx > cfs) {
        cfHandlesIdx.set(1); // doesn't ensure a perfect distribution, but it's ok
        idx = 0;
      }
      return cfHandles[idx];
    }
  }

  /**
   * Takes the next position in the index.
   */
  private int next() {
    int idx;
    int nextIdx;
    while (true) {
      idx = keyIndex.get();
      nextIdx = idx + 1;
      if (nextIdx >= keyCount) {
        nextIdx = 0;
      }

      if (keyIndex.compareAndSet(idx, nextIdx)) {
        break;
      }
    }
    return idx;
  }

  // String -> byte[]
  private byte[] getKeyArr() {
    final int MAX_LEN = 9; // key100000
    final int keyIdx = next();
    final byte[] keyPrefix = ba("key" + keyIdx);
    System.arraycopy(keyPrefix, 0, keyArr, 0, keyPrefix.length);
    Arrays.fill(keyArr, keyPrefix.length, MAX_LEN, (byte) 0x30);
    return keyArr;
  }

  // String -> ByteBuffer
  private ByteBuffer getKeyBuf() {
    final int MAX_LEN = 9; // key100000
    final int keyIdx = next();
    final String keyStr = "key" + keyIdx;
    for (int i = 0; i < keyStr.length(); ++i) {
      keyBuf.put(i, (byte) keyStr.charAt(i));
    }
    for (int i = keyStr.length(); i < MAX_LEN; ++i) {
      keyBuf.put(i, (byte) 0x30);
    }
    // Reset position for future reading
    keyBuf.position(0);
    return keyBuf;
  }

  private byte[] getValueArr() {
    return valueArr;
  }

  private ByteBuffer getValueBuf() {
    return valueBuf;
  }

  @Benchmark
  public void get() throws RocksDBException {
    db.get(getColumnFamily(), getKeyArr());
  }

  @Benchmark
  public void preallocatedGet() throws RocksDBException {
    db.get(getColumnFamily(), getKeyArr(), getValueArr());
  }

  @Benchmark
  public void preallocatedByteBufferGet() throws RocksDBException {
    int res = db.get(getColumnFamily(), readOptions, getKeyBuf(), getValueBuf());
    // For testing correctness:
    //    assert res > 0;
    //    final byte[] ret = new byte[valueSize];
    //    valueBuf.get(ret);
    //    System.out.println(str(ret));
    //    valueBuf.flip();
  }
}