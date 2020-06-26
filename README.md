## Note for TerarkDB
To compile this fork of RocksDB, you need to clone [terark-zip-rocksdb](https://github.com/terark/terark-zip-rocksdb) and add `terark-zip-rocksdb/src` to include path, but `libterark-zip-rocksdb` is not need for compiling. -- The simplest way is to put this repository and [terark-zip-rocksdb](https://github.com/terark/terark-zip-rocksdb) in a same directory, and do nothing for include path(already added in [Makefile](Makefile#L15)).

## RocksDB: A Persistent Key-Value Store for Flash and RAM Storage

[![Build Status](https://travis-ci.org/facebook/rocksdb.svg?branch=master)](https://travis-ci.org/facebook/rocksdb)
[![Build status](https://ci.appveyor.com/api/projects/status/fbgfu0so3afcno78/branch/master?svg=true)](https://ci.appveyor.com/project/Facebook/rocksdb/branch/master)


RocksDB is developed and maintained by Facebook Database Engineering Team.
It is built on earlier work on LevelDB by Sanjay Ghemawat (sanjay@google.com)
and Jeff Dean (jeff@google.com)

This code is a library that forms the core building block for a fast
key value server, especially suited for storing data on flash drives.
It has a Log-Structured-Merge-Database (LSM) design with flexible tradeoffs
between Write-Amplification-Factor (WAF), Read-Amplification-Factor (RAF)
and Space-Amplification-Factor (SAF). It has multi-threaded compactions,
making it specially suitable for storing multiple terabytes of data in a
single database.

Start with example usage here: https://github.com/facebook/rocksdb/tree/master/examples

See the [github wiki](https://github.com/facebook/rocksdb/wiki) for more explanation.

The public interface is in `include/`.  Callers should not include or
rely on the details of any other header files in this package.  Those
internal APIs may be changed without warning.

Design discussions are conducted in https://www.facebook.com/groups/rocksdb.dev/
