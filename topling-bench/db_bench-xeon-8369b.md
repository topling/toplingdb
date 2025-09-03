minDictZipValueSize: 300000
write_buffer_size: 64M
#enable_pipelined_write: true
unordered_write: true

$ env TOPLINGDB_GetContext_sampling=kNone ./db_bench \
    -json=sideplugin/rockside/sample-conf/db_bench_enterprise.yaml \
    -key_size=8 -value_size=200 -num=10000000 -batch_size=100 \
    -disable_wal=true -benchmarks=fillseq,readseq,readrandom \
    -enable_zero_copy=1 -scan_omit_value
Set seed to 1712991792731197 because --seed was 0
Initializing RocksDB Options from the specified file
Initializing RocksDB Options from command-line flags
Integrated BlobDB: blob cache disabled
RocksDB:    version 8.4.2
Date:       Sat Apr 13 15:03:12 2024
CPU:        32 * Intel(R) Xeon(R) Platinum 8369B CPU @ 2.70GHz
CPUCache:   49152 KB
Keys:       8 bytes each (+ 0 bytes user-defined timestamp)
Values:     200 bytes each (100 bytes after compression)
Entries:    10000000
Prefix:    0 bytes
Keys per prefix:    0
RawSize:    1983.6 MB (estimated)
FileSize:   1030.0 MB (estimated)
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
fillseq      :       0.352 micros/op 2839701 ops/sec 3.521 seconds 10000000 operations;  563.3 MB/s
DB path: [/dev/shm/db_bench_enterprise]
readseq      :       0.034 micros/op 29490782 ops/sec 0.339 seconds 10000000 operations;  225.0 MB/s
DB path: [/dev/shm/db_bench_enterprise]
readrandom   :       0.313 micros/op 3190895 ops/sec 3.134 seconds 10000000 operations;  633.0 MB/s (10000000 of 10000000 found)

$ env TOPLINGDB_GetContext_sampling=kNone ./db_bench \
    -json=sideplugin/rockside/sample-conf/db_bench_enterprise.yaml \
    -key_size=8 -value_size=200 -num=10000000 -batch_size=100 \
    -benchmarks=readseq,readrandom \
    -enable_zero_copy=1 -scan_omit_value -use_existing_db=1
Set seed to 1712992034783844 because --seed was 0
Initializing RocksDB Options from the specified file
Initializing RocksDB Options from command-line flags
Integrated BlobDB: blob cache disabled
RocksDB:    version 8.4.2
Date:       Sat Apr 13 15:07:14 2024
CPU:        32 * Intel(R) Xeon(R) Platinum 8369B CPU @ 2.70GHz
CPUCache:   49152 KB
Keys:       8 bytes each (+ 0 bytes user-defined timestamp)
Values:     200 bytes each (100 bytes after compression)
Entries:    10000000
Prefix:    0 bytes
Keys per prefix:    0
RawSize:    1983.6 MB (estimated)
FileSize:   1030.0 MB (estimated)
Write rate: 0 bytes/second
Read rate: 0 ops/second
Compression: Snappy
Compression sampling rate: 0
Memtablerep: CSPPMemTabFactory
Perf Level: 1
WARNING: Snappy compression is not enabled
------------------------------------------------
DB path: [/dev/shm/db_bench_enterprise]
readseq      :       0.032 micros/op 31063618 ops/sec 0.322 seconds 10000000 operations;  237.0 MB/s
DB path: [/dev/shm/db_bench_enterprise]
readrandom   :       0.224 micros/op 4463243 ops/sec 2.241 seconds 10000000 operations;  885.3 MB/s (10000000 of 10000000 found)