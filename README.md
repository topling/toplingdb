## ToplingDB: A Persistent Key-Value Store for External Storage
ToplingDB is developed and maintained by [Topling Inc](https://topling.cn). It is built with [RocksDB](https://github.com/facebook/rocksdb).

ToplingDB has much more key features than RocksDB:
1. [SidePlugin](https://github.com/topling/rockside/wiki) enables users to write a json(or yaml) to define DB instance configs
1. [Embeded Http Server](https://github.com/topling/rockside/wiki/WebView) enables users to view almost all DB info on web, this is a component of [SidePlugin](https://github.com/topling/rockside/wiki)
1. Many improves and refactories on RocksDB, aimed for performance and extendibility
1. [Topling**CSPP**MemTab](https://github.com/topling/rockside/wiki/ToplingCSPPMemTab)(**CSPP** is **C**rash **S**afe **P**arallel **P**atricia trie) MemTab, which outperforms SkipList on all aspects: 3x lower memory usage, 7x single thread performance, perfect multi-thread scaling
1. [Topling**Fast**Table](https://github.com/topling/rockside/wiki/ToplingFastTable) is an SST implementation optimized for speed, aimed for MemTable flush and L0->L1 compaction.
1. [Topling**Zip**Table](https://github.com/topling/rockside/wiki/ToplingZipTable) is an SST implementation optimized for RAM and SSD space, aimed for L2+ level compaction, which used dedicated searchable in-memory data compression algorithms.
1. [Distributed Compaction](https://github.com/topling/rockside/wiki/Distributed-Compaction) for offload compactions on elastic computing clusters, this is more general than RocksDB Compaction Service.
1. Builtin SidePlugin**s** for existing RocksDB components(Cache, Comparator, TableFactory, MemTableFactory...)
1. Builtin Prometheus metrics support, this is based on [Embeded Http Server](https://github.com/topling/rockside/wiki/WebView)
1. Many bugfixes for RocksDB, a small part of such fixes was [Pull Requested](https://github.com/facebook/rocksdb/pulls?q=is%3Apr+author%3Arockeet) to [upstream RocksDB](https://github.com/facebook/rocksdb)

## ToplingDB cloud native services
1. [Todis](https://github.com/topling/todis)(Redis on ToplingDB), [Todis on aliyun](https://topling.cn/products)
2. ToplingSQL(MySQL on ToplingDB), comming soon...

## ToplingDB Components
With SidePlugin mechanics, plugins/components can be physically seperated from core toplingdb
1. Compiled to a seperated dynamic lib and loaded at runtime
2. User code need not any changes, just change json/yaml files
3. Topling's non-open-source enterprise plugins/components are delivered in this way

Component      | Open Source Repo
-------------- | ------------------
SidePlugin     | [rockside](https://github.com/topling/rockside)
Embeded Http Server | [rockside](https://github.com/topling/rockside)
Refactories  and Enhancements  | [ToplingDB](https://github.com/topling/toplingdb)
Topling**CSPP**MemTab| Not Yet
Topling**Fast**Table | Not Yet
Topling**Zip**Table | Not Yet
Distributed Compaction | Not Yet
Builtin SidePlugin**s** | [rockside](https://github.com/topling/rockside)
Prometheus metrics | [rockside](https://github.com/topling/rockside)

## Run db_bench
ToplingDB requires gcc 8.4 or newer, or new clang(in near 3 years).

Even without Topling performance components, ToplingDB is much faster than upstream RocksDB:
```bash
sudo yum -y install git libaio-devel gcc-c++ gflags-devel zlib-devel bzip2-devel
git clone https://github.com/topling/toplingdb
cd toplingdb
make -j`nproc` db_bench DEBUG_LEVEL=0
cp sideplugin/rockside/src/topling/web/{style.css,index.html} ${/path/to/dbdir}
cp sideplugin/rockside/sample-conf/lcompact_community.yaml .
export LD_LIBRARY_PATH=`find sideplugin -name lib_shared`
# change ./lcompact_community.yaml
# 1. path items (search nvme-shared), if you have no fast disk(such as on a cloud server), use /dev/shm
# 2. change max_background_compactions to your cpu core num
# command option -json can accept json and yaml files, here use yaml file for more human readable
./db_bench -json lcompact_community.yaml -num 10000000 -disable_wal=true -value_size 2000 -benchmarks=fillrandom,readrandom -batch_size=10
# you can access http://127.0.0.1:8081 to see webview
# you can see this db_bench is much faster than RocksDB
```
## License
We disallow bytedance using this software, other terms are identidal with
upstream rocksdb license, see [LICENSE.Apache](LICENSE.Apache), [COPYING](COPYING) and
[LICENSE.leveldb](LICENSE.leveldb).

<hr/>
<hr/>
<hr/>

## RocksDB: A Persistent Key-Value Store for Flash and RAM Storage

[![CircleCI Status](https://circleci.com/gh/facebook/rocksdb.svg?style=svg)](https://circleci.com/gh/facebook/rocksdb)
[![TravisCI Status](https://api.travis-ci.com/facebook/rocksdb.svg?branch=main)](https://travis-ci.com/github/facebook/rocksdb)
[![Appveyor Build status](https://ci.appveyor.com/api/projects/status/fbgfu0so3afcno78/branch/main?svg=true)](https://ci.appveyor.com/project/Facebook/rocksdb/branch/main)
[![PPC64le Build Status](http://140-211-168-68-openstack.osuosl.org:8080/buildStatus/icon?job=rocksdb&style=plastic)](http://140-211-168-68-openstack.osuosl.org:8080/job/rocksdb)

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

Design discussions are conducted in https://www.facebook.com/groups/rocksdb.dev/ and https://rocksdb.slack.com/

## License

RocksDB is dual-licensed under both the GPLv2 (found in the COPYING file in the root directory) and Apache 2.0 License (found in the LICENSE.Apache file in the root directory).  You may select, at your option, one of the above-listed licenses.
