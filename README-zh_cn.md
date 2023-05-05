## ToplingDB: 一个外存上的持久化 Key-Value 存储引擎
ToplingDB 由[北京拓扑岭科技有限公司](https://topling.cn)开发与维护，从 [RocksDB](https://github.com/facebook/rocksdb) 分叉而来，详情参考 [ToplingDB 分支名称约定](https://github.com/topling/toplingdb/wiki/ToplingDB-Branch-Name-Convention)。

ToplingDB 的子模块 **[rockside](https://github.com/topling/rockside)** 是 ToplingDB 的入口，详情参考 **[SidePlugin wiki](https://github.com/topling/rockside/wiki)**。

ToplingDB 兼容 RocksDB API 的同时，增加了很多非常重要的功能与改进：
1. [SidePlugin](https://github.com/topling/rockside/wiki) 让用户可以通过 json/yaml 文件来定义 DB 配置
1. [内嵌 Http](https://github.com/topling/rockside/wiki/WebView) 让用户可以通过 Web 查看几乎所有 DB 信息，这是 [SidePlugin](https://github.com/topling/rockside/wiki) 的一个子功能
1. [内嵌 Http](https://github.com/topling/rockside/wiki/WebView) 让用户可以无需重启进程，[在线修改](https://github.com/topling/rockside/wiki/Online-Change-Options) 各种 db/cf 配置，包括修改 DB 元对象（例如 MemTabFactory, TableFactory, WriteBufferManager ...）
1. 为提升性能和可扩展性而实施的很多重构与改进，例如 MemTable 的重构
1. 对事务处理的改进，特别是 TransactionDB 中 Lock 的管理，热点代码有 5x 以上的性能提升
1. MultiGet 中使用 fiber/coroutine + io_uring 实现了并发 IO，比 RocksDB 自身的异步 MultiGet 又快又简洁，相应的代码量要少 100 倍不止
1. [去虚拟化](https://github.com/topling/rockside/wiki/Devirtualization-And-Key-Prefix-Cache-Principle)，消除热点代码中的虚函数调用（主要是 Comparator），并且增加了 Key 前缀缓存，参考相应 [bechmarks](https://github.com/topling/rockside/wiki/Devirtualization-And-Key-Prefix-Cache-Benchmark)
1. 点查和迭代器扫描中的 Zero Copy，对大 Value 效果尤其显著
1. 将现存的 RocksDB 组件作为**内置插件**纳入 SidePlugin 体系，例如 Cache, Comparator, TableFactory, MemTableFactory...
1. 内置 Prometheus 指标的支持，这是在[内嵌 Http](https://github.com/topling/rockside/wiki/WebView) 中实现的
1. 修复了很多 RocksDB 的 bug，我们已将其中易于合并到 RocksDB 的很多修复与改进给上游 RocksDB 发了 [Pull Request](https://github.com/facebook/rocksdb/pulls?q=is%3Apr+author%3Arockeet)

## ToplingDB 云原生数据库服务
1. [MyTopling](https://github.com/topling/mytopling)(MySQL on ToplingDB), [阿里云上的托管 MyTopling](https://topling.cn/products/mytopling/)
1. [Todis](https://github.com/topling/todis)(Redis on ToplingDB), [阿里云上的托管 Todis](https://topling.cn/products/todis-enterprise/)

## ToplingDB 组件
通过 SidePlugin 的实现机制，插件（组件）可以与 ToplingDB 的核心代码实现物理隔离
1. 可以编译为一个单独的动态库，实现运行时动态加载
1. 应用代码不需要为插件做任何改变，只需要修改 json/yaml 配置

### git 仓库的目录结构
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
 仓库    | 权限 | 说明
-------------- | ---------- | -----------
[ToplingDB](https://github.com/topling/toplingdb) | public | 顶级仓库，分叉自 [RocksDB](https://github.com/facebook/rocksdb)，增加了我们的改进与修复
[rockside](https://github.com/topling/rockside) | public | ToplingDB 子模块，包含：<ul><li>SidePlugin 框架和内置插件</li><li>内嵌的 Http 服务和 Prometheus 指标</li></ul>
[cspp-wbwi<br>(**W**rite**B**atch**W**ith**I**ndex)](https://github.com/topling/cspp-wbwi) | public | 使用 Topling CSPP Trie 实现的 **CSPP_WBWI** 相比 rocksdb SkipList WBWI 最多有 20 倍以上的性能提升
[cspp-memtable](https://github.com/topling/cspp-memtable) | public | (**CSPP** is **C**rash **S**afe **P**arallel **P**atricia trie) MemTab, 相比 SkipList 有全方位的提升：内存用量最多降低 3 倍，单线程性能提升 7 倍，并且多线程线性提升)
[topling-sst](https://github.com/topling/topling-sst) | public | 1. [SingleFastTable](https://github.com/topling/rockside/wiki/SingleFastTable)(主要用于 L0 和 L1)<br/> 2. VecAutoSortTable(主要用于 MyTopling bulk_load).<br/> 3. 已弃用：[ToplingFastTable](https://github.com/topling/rockside/wiki/ToplingFastTable), CSPPAutoSortTable
[topling-dcompact](https://github.com/topling/topling-dcompact) | public | 分布式 Compact 与通用的 dcompact_worker 程序, 将 Compact 转移到弹性计算集群。<br/>相比 RocksDB 自身的 Remote Compaction，ToplingDB 的分布式 Compact 功能完备，使用便捷，对上层应用非常友好
[topling-rocks](https://github.com/topling/topling-rocks) | **private** | 创建 [Topling**Zip**Table](https://github.com/topling/rockside/wiki/ToplingZipTable)，基于 Topling 可检索内存压缩算法的 SST，压缩率更高，且内存占用更低，一般用于 L2 及更深层 SST
[topling-zip_table_reader](https://github.com/topling/topling-zip_table_reader) | public | 让社区版用户可以读取 Topling**Zip**Table，但创建需要私有仓库 [topling-rocks](https://github.com/topling/topling-rocks)

为了简化编译流程，ToplingDB 在 Makefile 中会自动 clone 各个组件的 github 仓库，社区版用户可以成功 clone 公开的仓库，但克隆私有仓库（例如 topling-rocks）会失败，所以社区版用户编译出来的 ToplingDB 无法创建 Topling**Zip**Table，但可以读取 Topling**Zip**Table。

## 运行 db_bench
ToplingDB 需要 C++17，推荐 gcc 8.3 以上，或者 clang 也行。

即便没有 Topling**Zip**Table，ToplingDB 也比 RocksDB 要快得多，您可以通过运行 db_bench 来验证性能：
```bash
sudo yum -y install git libaio-devel gcc-c++ gflags-devel zlib-devel bzip2-devel libcurl-devel liburing-devel
git clone https://github.com/topling/toplingdb
cd toplingdb
make -j`nproc` db_bench DEBUG_LEVEL=0
cp sideplugin/rockside/src/topling/web/{style.css,index.html} ${/path/to/dbdir}
cp sideplugin/rockside/sample-conf/db_bench_*.yaml .
export LD_LIBRARY_PATH=`find sideplugin -name lib_shared`
# change db_bench_community.yaml as your needs
# 1. use default path(/dev/shm) if you have no fast disk(such as a cloud server)
# 2. change max_background_compactions to your cpu core num
# 3. if you have github repo topling-rocks permissions, you can use db_bench_enterprise.yaml
# 4. use db_bench_community.yaml is faster than upstream RocksDB
# 5. use db_bench_enterprise.yaml is much faster than db_bench_community.yaml
# command option -json can accept json and yaml files, here use yaml file for more human readable
./db_bench -json=db_bench_community.yaml -num=10000000 -disable_wal=true -value_size=20 -benchmarks=fillrandom,readrandom -batch_size=10
# you can access http://127.0.0.1:2011 to see webview
# you can see this db_bench is much faster than RocksDB
```
## 可配置的功能
为了性能和简化，ToplingDB 默认禁用了一些 RocksDB 的功能：

功能|控制参数（预编译宏）
-------|-------------
动态创建 ColumnFamily | ROCKSDB_DYNAMIC_CREATE_CF
用户层 timestamp | TOPLINGDB_WITH_TIMESTAMP
宽列 | TOPLINGDB_WITH_WIDE_COLUMNS

**注意**: SidePlugin 暂不支持动态创建 ColumnFamily，混用 SidePlugin 和动态创建 ColumnFamily时，动态创建的 ColumnFamily 不能在 Web 中展示

为了启用这些功能，需要为 make 命令显式添加 `EXTRA_CXXFLAGS="-D${MACRO_1} -D${MACRO_2} ..."`，例如编译带动态创建 ColumnFamily 的 rocksdbjava:
```
make -j`nproc` EXTRA_CXXFLAGS='-DROCKSDB_DYNAMIC_CREATE_CF' rocksdbjava
```
## License
为了兼容开源协议，下列原先禁止字节跳动使用本软件的条款从 2023-04-24 起已被删除，也就是说，字节跳动使用 ToplingDB 的行为不再是非法的，也不是无耻的。

~~我们禁止字节跳动使用本软件，其它条款与上游 RocksDB 完全相同，~~ 详情参考 [LICENSE.Apache](LICENSE.Apache), [COPYING](COPYING), [LICENSE.leveldb](LICENSE.leveldb).

相应 LICENSE 文件中禁止字节跳动使用本软件的条款也已经删除：[LICENSE.Apache](LICENSE.Apache), [COPYING](COPYING), [LICENSE.leveldb](LICENSE.leveldb).

<hr/>
以下是上游 RocksDB 的原版 README
<hr/>
<hr/>

## RocksDB: A Persistent Key-Value Store for Flash and RAM Storage

[![CircleCI Status](https://circleci.com/gh/facebook/rocksdb.svg?style=svg)](https://circleci.com/gh/facebook/rocksdb)
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

Questions and discussions are welcome on the [RocksDB Developers Public](https://www.facebook.com/groups/rocksdb.dev/) Facebook group and [email list](https://groups.google.com/g/rocksdb) on Google Groups.

## License

RocksDB is dual-licensed under both the GPLv2 (found in the COPYING file in the root directory) and Apache 2.0 License (found in the LICENSE.Apache file in the root directory).  You may select, at your option, one of the above-listed licenses.
