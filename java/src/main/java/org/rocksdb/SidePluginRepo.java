// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;
import java.util.List;
import java.util.ArrayList;

public class SidePluginRepo extends RocksObject {
    static {
        RocksDB.loadLibrary();
    }
    public native void importAutoFile(String fname) throws RocksDBException;
    public RocksDB openDB(String js) throws RocksDBException {
        RocksDB db = nativeOpenDB(nativeHandle_, js);
        dblist_.add(db);
        return db;
    }
    public RocksDB openDB(String js, List<ColumnFamilyHandle> out_cfhs) throws RocksDBException {
        RocksDB db = nativeOpenDBMultiCF(nativeHandle_, js);
        dblist_.add(db);
        out_cfhs.addAll(db.getOwnedColumnFamilyHandles());
        return db;
    }

    ///@{ open the DB defined in js["open"]
    public RocksDB openDB() throws RocksDBException {
        RocksDB db = nativeOpenDB(nativeHandle_, null);
        dblist_.add(db);
        return db;
    }
    public RocksDB openDB(List<ColumnFamilyHandle> out_cfhs) throws RocksDBException {
        RocksDB db = nativeOpenDBMultiCF(nativeHandle_, null);
        dblist_.add(db);
        out_cfhs.addAll(db.getOwnedColumnFamilyHandles());
        return db;
    }
    //@}

    // if js is null, open db defined in RepoJS["open"]
    protected native RocksDB nativeOpenDB(long handle, String js) throws RocksDBException;
    protected native RocksDB nativeOpenDBMultiCF(long handle, String js) throws RocksDBException;

    public native void startHttpServer() throws RocksDBException; // http server for inspection
    public native void closeHttpServer();

    // synonym to closeAllDB
    public void close() {
        closeAllDB();
    }
    // user must ensure all dbs are alive when calling this function
    // consistency to C++ native func name CloseAllDB
    public void closeAllDB() {
        if (owningHandle_.compareAndSet(true, false)) {
            nativeCloseAllDB(nativeHandle_);
            for (final RocksDB db : dblist_) {
                db.close();
            }
            disposeInternal(nativeHandle_);
        }
        dblist_ = null;
    }
    // call native->CloseAllDB(false)
    private native void nativeCloseAllDB(long handle);

    public native void put(String name, String spec, Options opt);
    public native void put(String name, String spec, DBOptions dbo);
    public native void put(String name, String spec, ColumnFamilyOptions cfo);

    public SidePluginRepo() {
        super(newSidePluginRepo());
    }
    static private native long newSidePluginRepo();

    private List<RocksDB> dblist_ = new ArrayList<RocksDB>();
    protected native void disposeInternal(final long handle);
}
