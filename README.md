## [中文版](README-zh_cn.md)
## ToplingDB: A Persistent Key-Value Store for External Storage
ToplingDB is developed and maintained by [Topling Inc](https://topling.cn). See [ToplingDB Branch Name Convention](https://github.com/topling/toplingdb/wiki/ToplingDB-Branch-Name-Convention).

## Quick Start
ToplingDB requires C++17, gcc 8.3 or newer is recommended, clang also works.

ToplingDB is forked form [RocksDB](https://github.com/facebook/rocksdb), much faster than RocksDB, try it by yourself:
### Compile & run db_bench
```bash
sudo yum -y install git libaio-devel gcc-c++ gflags-devel zlib-devel bzip2-devel libcurl-devel liburing-devel snappy-devel jemalloc-devel
#sudo apt-get update -y && sudo apt-get install -y libjemalloc-dev libaio-dev libgflags-dev zlib1g-dev libbz2-dev libcurl4-gnutls-dev liburing-dev libsnappy-dev libbz2-dev liblz4-dev libzstd-dev
git clone https://github.com/topling/toplingdb
cd toplingdb
make -j`nproc` db_bench DEBUG_LEVEL=0
sudo make install PREFIX=/some/path # default is /usr/local
```

After compile, you can run bundled [db_bench.sh](db_bench.sh)(need [port 2011](https://github.com/topling/rockside/blob/master/sample-conf/db_bench_enterprise.yaml#L4 "use port 2011 for embeded http server")), then use ToplingDB [in C++](https://github.com/topling/sideplugin-wiki-en/wiki/101 "maybe migrate from rocksdb"), or in [Java](https://github.com/topling/sideplugin-wiki-en/wiki/SidePlugin-Java-Binding "Bundled in this repo"), [Rust](https://github.com/topling/rust-toplingdb "A seperated repo").

## Introduction
ToplingDB's submodule **[rockside](https://github.com/topling/rockside)** is the entry point of ToplingDB, see **[SidePlugin wiki](https://github.com/topling/sideplugin-wiki-en/wiki)**.

ToplingDB has much more key features than RocksDB:
1. [SidePlugin](https://github.com/topling/sideplugin-wiki-en/wiki) enables users to write a json(or yaml) to define DB configs
1. [Embedded Http Server](https://github.com/topling/sideplugin-wiki-en/wiki/WebView) enables users to view almost all DB info on web, this is a component of [SidePlugin](https://github.com/topling/sideplugin-wiki-en/wiki)
1. [Embedded Http Server](https://github.com/topling/sideplugin-wiki-en/wiki/WebView) enables users to [online change](https://github.com/topling/sideplugin-wiki-en/wiki/Online-Change-Options) db/cf options and all db meta objects(such as MemTabFactory, TableFactory, WriteBufferManager ...) without restart the running process
1. Many improvements and refactories on RocksDB, aimed for performance and extendibility
1. memtable as wal log index, omit Flush MemTable to L0, reduce write amp, further improves for large MemTable
1. Topling transaction lock management, 5x faster than rocksdb
1. MultiGet with concurrent IO by fiber/coroutine + io_uring, much faster than RocksDB's async MultiGet
1. Topling [de-virtualization](https://github.com/topling/sideplugin-wiki-en/wiki/Devirtualization-And-Key-Prefix-Cache-Principle), de-virtualize hotspot (virtual) functions, and key prefix caches, [bechmarks](https://github.com/topling/sideplugin-wiki-en/wiki/Devirtualization-And-Key-Prefix-Cache-Benchmark)
1. Topling zero copy for point search(Get/MultiGet) and Iterator
1. Topling memtable as log index, omit memtable flush to L0
1. Builtin SidePlugin**s** for existing RocksDB components(Cache, Comparator, TableFactory, MemTableFactory...)
1. Builtin Prometheus metrics support, this is based on [Embedded Http Server](https://github.com/topling/sideplugin-wiki-en/wiki/WebView)
1. Many bugfixes for RocksDB, a small part of such fixes was [Pull Requested](https://github.com/facebook/rocksdb/pulls?q=is%3Apr+author%3Arockeet) to [upstream RocksDB](https://github.com/facebook/rocksdb)

## ToplingDB cloud native DB services
1. [MyTopling](https://github.com/topling/mytopling)(MySQL on ToplingDB), [MyTopling on aliyun](https://market.aliyun.com/products?k=mytopling)
1. [Todis](https://github.com/topling/todis)(Redis on ToplingDB)

## ToplingDB Components
With SidePlugin mechanics, plugins/components can be physically separated from core toplingdb
1. Can be compiled to a separated dynamic lib and loaded at runtime
2. User code need not any changes, just change json/yaml files
3. Topling's non-open-source enterprise plugins/components are delivered in this way

### Repository dir structure
```bash
toplingdb
 \__ sideplugin
      \__ rockside                 (submodule , sideplugin core and framework)
      \__ topling-zip              (auto clone, zip and core lib)
      \__ cspp-memtab              (auto clone, sideplugin component)
      \__ cspp-wbwi                (auto clone, sideplugin component)
      \__ topling-sst              (auto clone, sideplugin component)
      \__ topling-rocks            (auto clone, sideplugin component)
      \__ topling-zip_table_reader (auto clone, sideplugin component)
      \__ topling-dcompact         (auto clone, sideplugin component)
           \_ tools/dcompact       (dcompact-worker binary app)
```

 Repository    | Permission | Description (and components)
-------------- | ---------- | -----------
[ToplingDB](https://github.com/topling/toplingdb) | public | Top repository, forked from [RocksDB](https://github.com/facebook/rocksdb) with our fixes, refactories and enhancements
[rockside](https://github.com/topling/rockside) | public | This is a submodule, contains:<ul><li>SidePlugin framework and Builtin SidePlugin**s**</li><li>Embedded Http Server and Prometheus metrics</li></ul>
[cspp-wbwi<br>(**W**rite**B**atch**W**ith**I**ndex)](https://github.com/topling/cspp-wbwi) | public | With CSPP and carefully coding, **CSPP_WBWI** is 20x faster than rocksdb SkipList based WBWI
[cspp-memtable](https://github.com/topling/cspp-memtable/blob/memtable_as_log_index/README_EN.md) | public | (**CSPP** is **C**rash **S**afe **P**arallel **P**atricia trie) MemTab, which outperforms SkipList on all aspects: 3x lower memory usage, 7x single thread performance, perfect multi-thread scaling)
[topling-sst](https://github.com/topling/topling-sst) | public | 1. [SingleFastTable](https://github.com/topling/sideplugin-wiki-en/wiki/SingleFastTable)(designed for L0 and L1)<br/> 2. VecAutoSortTable(designed for MyTopling bulk_load).<br/> 3. Deprecated [ToplingFastTable](https://github.com/topling/sideplugin-wiki-en/wiki/ToplingFastTable), CSPPAutoSortTable
[topling-dcompact](https://github.com/topling/topling-dcompact) | public | Distributed Compaction with general dcompact_worker application, offload compactions to elastic computing clusters, much more powerful than RocksDB's Remote Compaction
[topling-rocks](https://github.com/topling/topling-rocks) | **private** | For build [Topling**Zip**Table](https://github.com/topling/sideplugin-wiki-en/wiki/ToplingZipTable), an SST implementation optimized for RAM and SSD space, aimed for L2+ level compaction, which uses topling dedicated searchable in-memory data compression algorithms
[topling-zip_table_reader](https://github.com/topling/topling-zip_table_reader) | public | For read Topling**Zip**Table by community users, builder of Topling**Zip**Table is in [topling-rocks](https://github.com/topling/topling-rocks)

To simplify the compiling, repo**s** are auto cloned in ToplingDB's Makefile, community users will auto clone public repo successfully but fail to auto clone **private** repo, thus ToplingDB is built without **private** components, this is so called **community** version.

## Configurable features
For performance and simplicity, ToplingDB disabled some RocksDB features by default:

Feature|Control MACRO
-------|-------------
Dynamic creation of ColumnFamily | ROCKSDB_DYNAMIC_CREATE_CF
User level timestamp on key | TOPLINGDB_WITH_TIMESTAMP
Wide Columns | TOPLINGDB_WITH_WIDE_COLUMNS
fabricated features for read | TOPLINGDB_WITH_FABRICATED_COMPLEXITY

To enable these features, add `-D${MACRO_NAME}` to var `EXTRA_CXXFLAGS`, such as build ToplingDB for java with dynamic ColumnFamily:
```
make -j`nproc` EXTRA_CXXFLAGS='-DROCKSDB_DYNAMIC_CREATE_CF' rocksdbjava
```
## License
To conform open source license, the following term of disallowing bytedance is deleted since 2023-04-24,
that is say: bytedance using ToplingDB is no longer illeagal and is not a shame.

~~We disallow bytedance using this software, other terms are identidal with
upstream rocksdb license,~~ see [LICENSE.Apache](LICENSE.Apache), [COPYING](COPYING) and
[LICENSE.leveldb](LICENSE.leveldb).

The terms of disallowing bytedance are also deleted in [LICENSE.Apache](LICENSE.Apache), [COPYING](COPYING) and
[LICENSE.leveldb](LICENSE.leveldb).

<hr/>
<hr/>
<hr/>

## RocksDB: A Persistent Key-Value Store for Flash and RAM Storage

[![CircleCI Status](https://circleci.com/gh/facebook/rocksdb.svg?style=svg)](https://circleci.com/gh/facebook/rocksdb)

RocksDB is developed and maintained by Facebook Database Engineering Team.
It is built on earlier work on [LevelDB](https://github.com/google/leveldb) by Sanjay Ghemawat (sanjay@google.com)
and Jeff Dean (jeff@google.com)

This code is a library that forms the core building block for a fast
key-value server, especially suited for storing data on flash drives.
It has a Log-Structured-Merge-Database (LSM) design with flexible tradeoffs
between Write-Amplification-Factor (WAF), Read-Amplification-Factor (RAF)
and Space-Amplification-Factor (SAF). It has multi-threaded compactions,
making it especially suitable for storing multiple terabytes of data in a
single database.

Start with example usage here: https://github.com/facebook/rocksdb/tree/main/examples

See the [github wiki](https://github.com/facebook/rocksdb/wiki) for more explanation.

The public interface is in `include/`.  Callers should not include or
rely on the details of any other header files in this package.  Those
internal APIs may be changed without warning.

Questions and discussions are welcome on the [RocksDB Developers Public](https://www.facebook.com/groups/rocksdb.dev/) Facebook group and [email list](https://groups.google.com/g/rocksdb) on Google Groups.

## License

RocksDB is dual-licensed under both the GPLv2 (found in the COPYING file in the root directory) and Apache 2.0 License (found in the LICENSE.Apache file in the root directory).  You may select, at your option, one of the above-listed licenses.
