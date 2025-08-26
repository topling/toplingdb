## do compress, dont use dcompact

### fillrandom, -batch_size=1
```bash
# level_writers is [fast, zip, zip, zip, zip, zip, zip], not using dcompact
# write_buffer_size=805306368 (768M in yaml)
$ ./db_bench -json=sideplugin/rockside/sample-conf/db_bench_enterprise.yaml \
    -key_size=8 -value_size=100 -num=100000000 -benchmarks=fillrandom
Set seed to 1687768983050499 because --seed was 0
Initializing RocksDB Options from the specified file
Initializing RocksDB Options from command-line flags
Integrated BlobDB: blob cache disabled
RocksDB:    version 8.4.0
Date:       Mon Jun 26 16:43:03 2023
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
Memtablerep: CSPPMemTabFactory
Perf Level: 1
WARNING: Assertions are enabled; benchmarks unnecessarily slow
------------------------------------------------
Initializing RocksDB Options from the specified file
Initializing RocksDB Options from command-line flags
Integrated BlobDB: blob cache disabled
DB path: [/dev/shm/db_bench_enterprise]
fillrandom   :       3.784 micros/op 264258 ops/sec 378.417 seconds 100000000 operations;   27.2 MB/s
$ date -Iseconds
2023-06-26T16:53:26+08:00
# manual compact was not run
$ du -hs /dev/shm/db_bench_enterprise
6.8G    /dev/shm/db_bench_enterprise
```

### fillrandom, -batch_size=100
```bash
 ./db_bench -json=sideplugin/rockside/sample-conf/db_bench_enterprise.yaml \
   -key_size=8 -value_size=100 -num=100000000 -benchmarks=fillrandom -batch_size=100
Set seed to 1688870904117107 because --seed was 0
Initializing RocksDB Options from the specified file
Initializing RocksDB Options from command-line flags
Integrated BlobDB: blob cache disabled
RocksDB:    version 8.4.0
Date:       Sun Jul  9 10:48:24 2023
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
Memtablerep: CSPPMemTabFactory
Perf Level: 1
WARNING: Assertions are enabled; benchmarks unnecessarily slow
------------------------------------------------
Initializing RocksDB Options from the specified file
Initializing RocksDB Options from command-line flags
Integrated BlobDB: blob cache disabled
DB path: [/dev/shm/db_bench_enterprise]
fillrandom   :       1.087 micros/op 919548 ops/sec 108.749 seconds 100000000 operations;   94.7 MB/s
```

### readrandom threads=1
```bash
$ env TOPLINGDB_GetContext_sampling=kNone \
  ./db_bench -json=sideplugin/rockside/sample-conf/db_bench_enterprise.yaml \
    -key_size=8 -value_size=100 -num=100000000 -benchmarks=readrandom \
    -enable_zero_copy=true -use_existing_db=true -threads=1
Set seed to 1687769667102703 because --seed was 0
Initializing RocksDB Options from the specified file
Initializing RocksDB Options from command-line flags
Integrated BlobDB: blob cache disabled
RocksDB:    version 8.4.0
Date:       Mon Jun 26 16:54:34 2023
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
Memtablerep: CSPPMemTabFactory
Perf Level: 1
WARNING: Assertions are enabled; benchmarks unnecessarily slow
------------------------------------------------
DB path: [/dev/shm/db_bench_enterprise]
readrandom   :       1.063 micros/op 940472 ops/sec 106.330 seconds 100000000 operations;   61.2 MB/s (63218875 of 100000000 found)
```

### readrandom threads=10
```bash
$ env TOPLINGDB_GetContext_sampling=kNone \
  ./db_bench -json=sideplugin/rockside/sample-conf/db_bench_enterprise.yaml \
    -key_size=8 -value_size=100 -num=100000000 -benchmarks=readrandom \
    -enable_zero_copy=true -use_existing_db=true -threads=10
Set seed to 1687770446091066 because --seed was 0
Initializing RocksDB Options from the specified file
Initializing RocksDB Options from command-line flags
Integrated BlobDB: blob cache disabled
RocksDB:    version 8.4.0
Date:       Mon Jun 26 17:07:26 2023
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
Memtablerep: CSPPMemTabFactory
Perf Level: 1
WARNING: Assertions are enabled; benchmarks unnecessarily slow
------------------------------------------------
DB path: [/dev/shm/db_bench_enterprise]
readrandom   :       1.009 micros/op 9569133 ops/sec 104.503 seconds 1000000000 operations;  623.0 MB/s (63212041 of 100000000 found)
```

------------------------------------------------

## dont compress, dont use dcompact
```yaml
  level_writers: [fast, fast, fast, fast, fast, fast, fast]
```

### fillrandom, -batch_size=1
```bash
$ ./db_bench -json=sideplugin/rockside/sample-conf/db_bench_enterprise.yaml \
    -key_size=8 -value_size=100 -num=100000000 -benchmarks=fillrandom -batch_size=1
Set seed to 1688868437022237 because --seed was 0
Initializing RocksDB Options from the specified file
Initializing RocksDB Options from command-line flags
Integrated BlobDB: blob cache disabled
RocksDB:    version 8.4.0
Date:       Sun Jul  9 10:07:17 2023
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
Memtablerep: CSPPMemTabFactory
Perf Level: 1
WARNING: Assertions are enabled; benchmarks unnecessarily slow
------------------------------------------------
Initializing RocksDB Options from the specified file
Initializing RocksDB Options from command-line flags
Integrated BlobDB: blob cache disabled
DB path: [/dev/shm/db_bench_enterprise]
fillrandom   :       3.485 micros/op 286911 ops/sec 348.540 seconds 100000000 operations;   29.6 MB/s
```

### fillrandom, -batch_size=100
```bash
rm -rf /dev/shm/db_bench_enterprise
$ ./db_bench -json=sideplugin/rockside/sample-conf/db_bench_enterprise.yaml \
    -key_size=8 -value_size=100 -num=100000000 -benchmarks=fillrandom -batch_size=100
Set seed to 1687772819444666 because --seed was 0
Initializing RocksDB Options from the specified file
Initializing RocksDB Options from command-line flags
Integrated BlobDB: blob cache disabled
RocksDB:    version 8.4.0
Date:       Mon Jun 26 17:46:59 2023
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
Memtablerep: CSPPMemTabFactory
Perf Level: 1
WARNING: Assertions are enabled; benchmarks unnecessarily slow
------------------------------------------------
Initializing RocksDB Options from the specified file
Initializing RocksDB Options from command-line flags
Integrated BlobDB: blob cache disabled
DB path: [/dev/shm/db_bench_enterprise]
fillrandom   :       1.042 micros/op 959254 ops/sec 104.248 seconds 100000000 operations;   98.8 MB/s
```

### readrandom, -threads=1
```bash
$ env TOPLINGDB_GetContext_sampling=kNone \
  ./db_bench -json=sideplugin/rockside/sample-conf/db_bench_enterprise.yaml \
    -key_size=8 -value_size=100 -num=100000000 -benchmarks=readrandom \
    -enable_zero_copy=true -use_existing_db=true -threads=1
Set seed to 1688868216331497 because --seed was 0
Initializing RocksDB Options from the specified file
Initializing RocksDB Options from command-line flags
Integrated BlobDB: blob cache disabled
RocksDB:    version 8.4.0
Date:       Sun Jul  9 10:03:36 2023
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
Memtablerep: CSPPMemTabFactory
Perf Level: 1
WARNING: Assertions are enabled; benchmarks unnecessarily slow
------------------------------------------------
DB path: [/dev/shm/db_bench_enterprise]
readrandom   :       1.026 micros/op 974600 ops/sec 102.606 seconds 100000000 operations;   63.5 MB/s (63217964 of 100000000 found)
```

### readrandom, -threads=10
```bash
$ env TOPLINGDB_GetContext_sampling=kNone \
  ./db_bench -json=sideplugin/rockside/sample-conf/db_bench_enterprise.yaml \
    -key_size=8 -value_size=100 -num=100000000 -benchmarks=readrandom \
    -enable_zero_copy=true -use_existing_db=true -threads=10
Set seed to 1688867994530419 because --seed was 0
Initializing RocksDB Options from the specified file
Initializing RocksDB Options from command-line flags
Integrated BlobDB: blob cache disabled
RocksDB:    version 8.4.0
Date:       Sun Jul  9 09:59:54 2023
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
Memtablerep: CSPPMemTabFactory
Perf Level: 1
WARNING: Assertions are enabled; benchmarks unnecessarily slow
------------------------------------------------
DB path: [/dev/shm/db_bench_enterprise]
readrandom   :       1.004 micros/op 9659017 ops/sec 103.530 seconds 1000000000 operations;  628.9 MB/s (63214797 of 100000000 found)
```
