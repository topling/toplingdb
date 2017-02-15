# TerarkZipTable

## terocksdb
We call &lt;&lt;rocksdb with TerarkZipTable&gt;&gt; as terocksdb, or terocks. With terocks, you can use terocks as official rocksdb(without terocks feature).

If your application is using rocksdb, you can seamlessly switch to `terocks` and use `TerarkZipTable`, even without recompilation.

See [terark-zip-rocksdb](https://github.com/terark/terark-zip-rocksdb) for more detail.

## Using TerarkZipTable by set rocksdb options

Applications can directly config [TerarkZipTableOptions](https://github.com/terark/terark-zip-rocksdb/blob/master/src/table/terark_zip_table.h#L17) and use `TerarkZipTable` with [NewTerarkZipTableFactory](https://github.com/terark/terark-zip-rocksdb/blob/master/src/table/terark_zip_table.h#L89). This needs to modify and recompile application code, See [terark-zip-rocksdb](https://github.com/terark/terark-zip-rocksdb) for more detail.

To simplify `TerarkZipTable` usage, we provide the following way:

## Using TerarkZipTable by environment vars

If environment var `TerarkZipTable_localTempDir` is defined(must not be empty),
terocks(this modified `librocksdb`) will use `TerarkZipTable` as SSTable, and use `AdaptiveTableFactory` as fallback(mainly for reading existing original rocksdb SSTable).

All env var names are [TerarkZipTableOptions](https://github.com/terark/terark-zip-rocksdb/blob/master/src/table/terark_zip_table.h#L17) field names prefixed with `TerarkZipTable_`.

If an existing application using rocksdb, to switch to `terocks`, just override(overwrite the original librocksdb.so, or change `LD_LIBRARY_PATH` ...), and preload terark libs:

```bash
env LD_LIBRARY_PATH=/path/to/terocks/lib:$LD_LIBRARY_PATH \
    LD_PRELOAD=libterark-zip-rocksdb-trial-r.so:libterark-core-r.so:libterark-fsa-r.so:libterark-zbs-r.so \
    TerarkZipTable_localTempDir=/path/to/some/temp/dir \
    TerarkZipTable_indexNestLevel=2 \
    TerarkZipTable_indexCacheRatio=0.005 \
    TerarkZipTable_smallTaskMemory=1G \
    TerarkZipTable_softZipWorkingMemLimit=16G \
    TerarkZipTable_hardZipWorkingMemLimit=32G \
    app_exe_file app_args...
```

Only `TerarkZipTable_localTempDir` is required, others are optional(and works good for most cases).

If `TerarkZipTable_localTempDir` is not defined, `TerarkZipTable` will not be used.

If `TerarkZipTable_localTempDir` is defined, but libterark-xxx are not `LD_PRELOAD`'ed, db::Open(..) will return rocksdb::Status::InvalidArgument(...).


Files in `/path/to/terocks/lib` may looks like this:
```
$ ls -l pkg/terark-zip-rocksdb-trial-Linux-x86_64-g++-4.8-bmi2-1/lib
total 60552
lrwxrwxrwx. 1 wheel wheel       27 Feb 15 11:31 libterark-core-d.so -> libterark-core-g++-4.8-d.so
-rwxrwxr-x. 1 wheel wheel  6122760 Feb 15 11:31 libterark-core-g++-4.8-d.so
-rwxrwxr-x. 1 wheel wheel   974736 Feb 15 11:31 libterark-core-g++-4.8-r.so
lrwxrwxrwx. 1 wheel wheel       27 Feb 15 11:31 libterark-core-r.so -> libterark-core-g++-4.8-r.so
lrwxrwxrwx. 1 wheel wheel       26 Feb 15 11:31 libterark-fsa-d.so -> libterark-fsa-g++-4.8-d.so
-rwxrwxr-x. 1 wheel wheel 32263000 Feb 15 11:31 libterark-fsa-g++-4.8-d.so
-rwxrwxr-x. 1 wheel wheel  9206256 Feb 15 11:32 libterark-fsa-g++-4.8-r.so
lrwxrwxrwx. 1 wheel wheel       26 Feb 15 11:32 libterark-fsa-r.so -> libterark-fsa-g++-4.8-r.so
lrwxrwxrwx. 1 wheel wheel       26 Feb 15 11:31 libterark-zbs-d.so -> libterark-zbs-g++-4.8-d.so
-rwxrwxr-x. 1 wheel wheel  3792368 Feb 15 11:31 libterark-zbs-g++-4.8-d.so
-rwxrwxr-x. 1 wheel wheel   608784 Feb 15 11:32 libterark-zbs-g++-4.8-r.so
lrwxrwxrwx. 1 wheel wheel       26 Feb 15 11:32 libterark-zbs-r.so -> libterark-zbs-g++-4.8-r.so
lrwxrwxrwx. 1 wheel wheel       40 Feb 15 13:47 libterark-zip-rocksdb-trial-d.so -> libterark-zip-rocksdb-trial-g++-4.8-d.so
-rwxrwxr-x. 1 wheel wheel  4724184 Feb 15 13:47 libterark-zip-rocksdb-trial-g++-4.8-d.so
-rwxrwxr-x. 1 wheel wheel  4301744 Feb 15 13:47 libterark-zip-rocksdb-trial-g++-4.8-r.so
lrwxrwxrwx. 1 wheel wheel       40 Feb 15 13:47 libterark-zip-rocksdb-trial-r.so -> libterark-zip-rocksdb-trial-g++-4.8-r.so

```

## Why not staticly link libterark-xxx into librocksdb.so

Such libterark-xxx has two versions, bmi2-1 or bmi2-0, bmi2-1 runs faster than bmi2-0, but can not run on CPUs older than haswell, bmi2-0 is a bit slower, but can run on older CPUs.

We also provide librocksdb.so with libterark-xxx statically linked into, these librocksdb.so is just for our partners. In this case, `LD_PRELOAD` is not needed.

