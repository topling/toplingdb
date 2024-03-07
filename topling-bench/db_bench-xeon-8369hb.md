# ToplingZipTable(db_bench_enterprise.yaml)
## fillseq,flush
```
$ ./db_bench -json=sideplugin/rockside/sample-conf/db_bench_enterprise.yaml -key_size=8 -value_size=20 -batch_size=10 -num=100000000 -disable_wal=true -benchmarks=fillseq,flush
Set seed to 1676438095160867 because --seed was 0
Initializing RocksDB Options from the specified file
Initializing RocksDB Options from command-line flags
Integrated BlobDB: blob cache disabled
RocksDB:    version 7.10.0
Date:       Wed Feb 15 13:14:55 2023
CPU:        16 * Intel(R) Xeon(R) Platinum 8369HB CPU @ 3.30GHz
CPUCache:   33792 KB
Keys:       8 bytes each (+ 0 bytes user-defined timestamp)
Values:     20 bytes each (10 bytes after compression)
Entries:    100000000
Prefix:    0 bytes
Keys per prefix:    0
RawSize:    2670.3 MB (estimated)
FileSize:   1716.6 MB (estimated)
Write rate: 0 bytes/second
Read rate: 0 ops/second
Compression: Snappy
Compression sampling rate: 0
Memtablerep: CSPPMemTabFactory
Perf Level: 1
WARNING: Snappy compression is not enabled
------------------------------------------------
Initializing RocksDB Options from the specified file
Initializing RocksDB Options from command-line flags
Integrated BlobDB: blob cache disabled
DB path: [/dev/shm/db_bench_enterprise]
fillseq      :       0.339 micros/op 2949754 ops/sec 33.901 seconds 100000000 operations;   78.8 MB/s
flush memtable
```

## RandomRead on uncompacted data
```
$ TOPLINGDB_GetContext_sampling=kNone ./db_bench -json=sideplugin/rockside/sample-conf/db_bench_enterprise.yaml -key_size=8 -num=100000000 -benchmarks=readrandom -enable_zero_copy=true
Set seed to 1676438203811720 because --seed was 0
Initializing RocksDB Options from the specified file
Initializing RocksDB Options from command-line flags
Integrated BlobDB: blob cache disabled
RocksDB:    version 7.10.0
Date:       Wed Feb 15 13:16:43 2023
CPU:        16 * Intel(R) Xeon(R) Platinum 8369HB CPU @ 3.30GHz
CPUCache:   33792 KB
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
Memtablerep: CSPPMemTabFactory
Perf Level: 1
WARNING: Snappy compression is not enabled
------------------------------------------------
DB path: [/dev/shm/db_bench_enterprise]
readrandom   :       0.254 micros/op 3939416 ops/sec 25.384 seconds 100000000 operations;  105.2 MB/s (100000000 of 100000000 found)
```

## Compact
```
$ TOPLINGDB_GetContext_sampling=kNone ./db_bench -json=sideplugin/rockside/sample-conf/db_bench_enterprise.yaml -key_size=8  -value_size=20 -num=100000000 -benchmarks=compact -enable_zero_copy=true
Set seed to 1676438297492867 because --seed was 0
Initializing RocksDB Options from the specified file
Initializing RocksDB Options from command-line flags
Integrated BlobDB: blob cache disabled
RocksDB:    version 7.10.0
Date:       Wed Feb 15 13:18:17 2023
CPU:        16 * Intel(R) Xeon(R) Platinum 8369HB CPU @ 3.30GHz
CPUCache:   33792 KB
Keys:       8 bytes each (+ 0 bytes user-defined timestamp)
Values:     20 bytes each (10 bytes after compression)
Entries:    100000000
Prefix:    0 bytes
Keys per prefix:    0
RawSize:    2670.3 MB (estimated)
FileSize:   1716.6 MB (estimated)
Write rate: 0 bytes/second
Read rate: 0 ops/second
Compression: Snappy
Compression sampling rate: 0
Memtablerep: CSPPMemTabFactory
Perf Level: 1
WARNING: Snappy compression is not enabled
------------------------------------------------
DB path: [/dev/shm/db_bench_enterprise]
compact      : 30842721.000 micros/op 0 ops/sec 30.843 seconds 1 operations;
```

## RandomRead on compacted data
```
$ TOPLINGDB_GetContext_sampling=kNone ./db_bench -json=sideplugin/rockside/sample-conf/db_bench_enterprise.yaml -key_size=8 -num=100000000 -benchmarks=readrandom -enable_zero_copy=true
Set seed to 1676438381467415 because --seed was 0
Initializing RocksDB Options from the specified file
Initializing RocksDB Options from command-line flags
Integrated BlobDB: blob cache disabled
RocksDB:    version 7.10.0
Date:       Wed Feb 15 13:19:41 2023
CPU:        16 * Intel(R) Xeon(R) Platinum 8369HB CPU @ 3.30GHz
CPUCache:   33792 KB
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
Memtablerep: CSPPMemTabFactory
Perf Level: 1
WARNING: Snappy compression is not enabled
------------------------------------------------
DB path: [/dev/shm/db_bench_enterprise]
readrandom   :       0.234 micros/op 4279978 ops/sec 23.365 seconds 100000000 operations;  114.3 MB/s (100000000 of 100000000 found)
```

# BlockBasedTable(db_bench_community.yaml)

## fillseq,flush
```
./db_bench -json sideplugin/rockside/sample-conf/db_bench_community.yaml -key_size=8 -value_size=20 -batch_size=10 -num=100000000 -disable_wal=true -benchmarks=fillseq,flush
Set seed to 1676438629475919 because --seed was 0
Initializing RocksDB Options from the specified file
Initializing RocksDB Options from command-line flags
Integrated BlobDB: blob cache disabled
RocksDB:    version 7.10.0
Date:       Wed Feb 15 13:23:49 2023
CPU:        16 * Intel(R) Xeon(R) Platinum 8369HB CPU @ 3.30GHz
CPUCache:   33792 KB
Keys:       8 bytes each (+ 0 bytes user-defined timestamp)
Values:     20 bytes each (10 bytes after compression)
Entries:    100000000
Prefix:    0 bytes
Keys per prefix:    0
RawSize:    2670.3 MB (estimated)
FileSize:   1716.6 MB (estimated)
Write rate: 0 bytes/second
Read rate: 0 ops/second
Compression: Snappy
Compression sampling rate: 0
Memtablerep: CSPPMemTabFactory
Perf Level: 1
WARNING: Snappy compression is not enabled
------------------------------------------------
Initializing RocksDB Options from the specified file
Initializing RocksDB Options from command-line flags
Integrated BlobDB: blob cache disabled
DB path: [/dev/shm/db_bench_community]
fillseq      :       0.328 micros/op 3049273 ops/sec 32.795 seconds 100000000 operations;   81.4 MB/s
flush memtable
```

## RandomRead on uncompacted data
```
$ TOPLINGDB_GetContext_sampling=kNone ./db_bench -json=sideplugin/rockside/sample-conf/db_bench_community.yaml -key_size=8 -num=100000000 -benchmarks=readrandom -enable_zero_copy=true
Set seed to 1676438939813903 because --seed was 0
Initializing RocksDB Options from the specified file
Initializing RocksDB Options from command-line flags
Integrated BlobDB: blob cache disabled
RocksDB:    version 7.10.0
Date:       Wed Feb 15 13:28:59 2023
CPU:        16 * Intel(R) Xeon(R) Platinum 8369HB CPU @ 3.30GHz
CPUCache:   33792 KB
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
Memtablerep: CSPPMemTabFactory
Perf Level: 1
WARNING: Snappy compression is not enabled
------------------------------------------------
DB path: [/dev/shm/db_bench_community]
readrandom   :       2.656 micros/op 376435 ops/sec 265.650 seconds 100000000 operations;   10.1 MB/s (100000000 of 100000000 found)
```

## Compact
```
$ TOPLINGDB_GetContext_sampling=kNone ./db_bench -json=sideplugin/rockside/sample-conf/db_bench_community.yaml -key_size=8  -value_size=20 -num=100000000 -benchmarks=compact -enable_zero_copy=true
Set seed to 1676439254405979 because --seed was 0
Initializing RocksDB Options from the specified file
Initializing RocksDB Options from command-line flags
Integrated BlobDB: blob cache disabled
RocksDB:    version 7.10.0
Date:       Wed Feb 15 13:34:14 2023
CPU:        16 * Intel(R) Xeon(R) Platinum 8369HB CPU @ 3.30GHz
CPUCache:   33792 KB
Keys:       8 bytes each (+ 0 bytes user-defined timestamp)
Values:     20 bytes each (10 bytes after compression)
Entries:    100000000
Prefix:    0 bytes
Keys per prefix:    0
RawSize:    2670.3 MB (estimated)
FileSize:   1716.6 MB (estimated)
Write rate: 0 bytes/second
Read rate: 0 ops/second
Compression: Snappy
Compression sampling rate: 0
Memtablerep: CSPPMemTabFactory
Perf Level: 1
WARNING: Snappy compression is not enabled
------------------------------------------------
DB path: [/dev/shm/db_bench_community]
compact      : 24342250.000 micros/op 0 ops/sec 24.342 seconds 1 operations;
```

## RandomRead on compacted data
```
$ TOPLINGDB_GetContext_sampling=kNone ./db_bench -json=sideplugin/rockside/sample-conf/db_bench_community.yaml -key_size=8 -num=100000000 -benchmarks=readrandom -enable_zero_copy=true
Set seed to 1676439330656750 because --seed was 0
Initializing RocksDB Options from the specified file
Initializing RocksDB Options from command-line flags
Integrated BlobDB: blob cache disabled
RocksDB:    version 7.10.0
Date:       Wed Feb 15 13:35:30 2023
CPU:        16 * Intel(R) Xeon(R) Platinum 8369HB CPU @ 3.30GHz
CPUCache:   33792 KB
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
Memtablerep: CSPPMemTabFactory
Perf Level: 1
WARNING: Snappy compression is not enabled
------------------------------------------------
DB path: [/dev/shm/db_bench_community]
readrandom   :       2.652 micros/op 377083 ops/sec 265.194 seconds 100000000 operations;   10.1 MB/s (100000000 of 100000000 found)
```
