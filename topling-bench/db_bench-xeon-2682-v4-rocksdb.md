```bash
$ ./db_bench -key_size=8 -value_size=100 -num=100000000 -benchmarks=fillrandom \
   -db=/dev/shm/db_bench_rocksdb -write_buffer_size=805306368 -target_file_size_multiplier=2
Set seed to 1687772239001478 because --seed was 0
Initializing RocksDB Options from the specified file
Initializing RocksDB Options from command-line flags
Integrated BlobDB: blob cache disabled
RocksDB:    version 8.4.0
Date:       Mon Jun 26 17:37:19 2023
CPU:        64 * Intel(R) Xeon(R) CPU E5-2682 v4 @ 2.50GHz
CPUCache:   40960 KB
Keys:       8 bytes each (+ 0 bytes user-defined timestamp)
Values:     100 bytes each (50 bytes after compression)
Entries:    100000000
Prefix:    0 bytes
Keys per prefix:    0
RawSize:    10299.7 MB (estimated)
FileSize:   5531.3 MB (estimated)
Write rate: 0 bytes/second
Read rate: 0 ops/second
Compression: Snappy
Compression sampling rate: 0
Memtablerep: SkipListFactory
Perf Level: 1
------------------------------------------------
Initializing RocksDB Options from the specified file
Initializing RocksDB Options from command-line flags
Integrated BlobDB: blob cache disabled
DB path: [/dev/shm/db_bench_rocksdb]
fillrandom   :       4.146 micros/op 241207 ops/sec 414.582 seconds 100000000 operations;   24.8 MB/s
```

```bash
$ ./db_bench -key_size=8 -value_size=100 -num=100000000 -benchmarks=fillrandom \
   -db=/dev/shm/db_bench_rocksdb -write_buffer_size=805306368 -target_file_size_multiplier=2 -batch_size=100
Set seed to 1687773005512007 because --seed was 0
Initializing RocksDB Options from the specified file
Initializing RocksDB Options from command-line flags
Integrated BlobDB: blob cache disabled
RocksDB:    version 8.4.0
Date:       Mon Jun 26 17:50:05 2023
CPU:        64 * Intel(R) Xeon(R) CPU E5-2682 v4 @ 2.50GHz
CPUCache:   40960 KB
Keys:       8 bytes each (+ 0 bytes user-defined timestamp)
Values:     100 bytes each (50 bytes after compression)
Entries:    100000000
Prefix:    0 bytes
Keys per prefix:    0
RawSize:    10299.7 MB (estimated)
FileSize:   5531.3 MB (estimated)
Write rate: 0 bytes/second
Read rate: 0 ops/second
Compression: Snappy
Compression sampling rate: 0
Memtablerep: SkipListFactory
Perf Level: 1
------------------------------------------------
Initializing RocksDB Options from the specified file
Initializing RocksDB Options from command-line flags
Integrated BlobDB: blob cache disabled
DB path: [/dev/shm/db_bench_rocksdb]
fillrandom   :       2.247 micros/op 445056 ops/sec 224.691 seconds 100000000 operations;   45.8 MB/s
```

```bash
$ ./db_bench -key_size=8 -value_size=100 -num=100000000 -benchmarks=readrandom \
   -db=/dev/shm/db_bench_rocksdb -use_existing_db=true -cache_size=10000000000
Set seed to 1687775355805633 because --seed was 0
Initializing RocksDB Options from the specified file
Initializing RocksDB Options from command-line flags
Integrated BlobDB: blob cache disabled
RocksDB:    version 8.4.0
Date:       Mon Jun 26 18:29:15 2023
CPU:        64 * Intel(R) Xeon(R) CPU E5-2682 v4 @ 2.50GHz
CPUCache:   40960 KB
Keys:       8 bytes each (+ 0 bytes user-defined timestamp)
Values:     100 bytes each (50 bytes after compression)
Entries:    100000000
Prefix:    0 bytes
Keys per prefix:    0
RawSize:    10299.7 MB (estimated)
FileSize:   5531.3 MB (estimated)
Write rate: 0 bytes/second
Read rate: 0 ops/second
Compression: Snappy
Compression sampling rate: 0
Memtablerep: SkipListFactory
Perf Level: 1
------------------------------------------------
DB path: [/dev/shm/db_bench_rocksdb]
readrandom   :       4.277 micros/op 233818 ops/sec 427.682 seconds 100000000 operations;   15.2 MB/s (63221157 of 100000000 found)
```

```bash
$ ./db_bench -key_size=8 -value_size=100 -num=100000000 -benchmarks=readrandom \
   -db=/dev/shm/db_bench_rocksdb -use_existing_db=true -cache_size=20000000000 \
   -threads=10
Set seed to 1687776505031212 because --seed was 0
Initializing RocksDB Options from the specified file
Initializing RocksDB Options from command-line flags
Integrated BlobDB: blob cache disabled
RocksDB:    version 8.4.0
Date:       Mon Jun 26 18:48:25 2023
CPU:        64 * Intel(R) Xeon(R) CPU E5-2682 v4 @ 2.50GHz
CPUCache:   40960 KB
Keys:       8 bytes each (+ 0 bytes user-defined timestamp)
Values:     100 bytes each (50 bytes after compression)
Entries:    100000000
Prefix:    0 bytes
Keys per prefix:    0
RawSize:    10299.7 MB (estimated)
FileSize:   5531.3 MB (estimated)
Write rate: 0 bytes/second
Read rate: 0 ops/second
Compression: Snappy
Compression sampling rate: 0
Memtablerep: SkipListFactory
Perf Level: 1
------------------------------------------------
DB path: [/dev/shm/db_bench_rocksdb]
readrandom   :       4.616 micros/op 2141609 ops/sec 466.939 seconds 1000000000 operations;  139.4 MB/s (63209964 of 100000000 found)
```

----------------------------------------------------------------------------
## dont compress, dont use dcompact
```bash
  -min_level_to_compress=10 # indicate dont compress
```

### fillrandom, -batch_size=100
```bash
$ ./db_bench -key_size=8 -value_size=100 -num=100000000 -benchmarks=fillrandom \
   -db=/dev/shm/db_bench_rocksdb -write_buffer_size=805306368 \
   -target_file_size_base=33554432 -target_file_size_multiplier=2 \
   -min_level_to_compress=10 -batch_size=100
Set seed to 1688876358061989 because --seed was 0
Initializing RocksDB Options from the specified file
Initializing RocksDB Options from command-line flags
Integrated BlobDB: blob cache disabled
RocksDB:    version 8.4.0
Date:       Sun Jul  9 12:19:18 2023
CPU:        64 * Intel(R) Xeon(R) CPU E5-2682 v4 @ 2.50GHz
CPUCache:   40960 KB
Keys:       8 bytes each (+ 0 bytes user-defined timestamp)
Values:     100 bytes each (50 bytes after compression)
Entries:    100000000
Prefix:    0 bytes
Keys per prefix:    0
RawSize:    10299.7 MB (estimated)
FileSize:   5531.3 MB (estimated)
Write rate: 0 bytes/second
Read rate: 0 ops/second
Compression: Snappy
Compression sampling rate: 0
Memtablerep: SkipListFactory
Perf Level: 1
------------------------------------------------
Initializing RocksDB Options from the specified file
Initializing RocksDB Options from command-line flags
Integrated BlobDB: blob cache disabled
DB path: [/dev/shm/db_bench_rocksdb]
fillrandom   :       2.156 micros/op 463819 ops/sec 215.601 seconds 100000000 operations;   47.8 MB/s
```
