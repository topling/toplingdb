# Abnormal-exit recovery: DB::Open

For the recovery mechanism, configuration, and limitations, see the [Chinese crash-safe recovery guide](https://github.com/topling/rockside/wiki/Crash-Safe-Recovery).

`crash_recover_bench` fills one memtable to 75% of a 2GiB (2,147,483,648-byte)
`write_buffer_size`, targeting 1.5GiB (1,610,612,736 bytes), then `_exit`s
without `Close`. Keys are 16 bytes and values are 128 bytes. Each run copies that
directory with `cp -a --sparse=always` and times only `DB::Open`. A Get of
key 0 and the last key runs after the timer. Default is 5 runs.

Measured on 2026-10-02, directories under `/dev/shm`.

CSPP crash-safe sets `TOPLINGDB_EASY_MIGRATE_CONF=tools/crash_recover_bench.yaml`.
Default SkipList / WAL replay leaves that variable unset, so it does not read
the yaml. Both configurations use the same benchmark binary and the same
ToplingDB build (`librocksdb.so.8.10.2`); the control is not an independently
built upstream RocksDB binary.
`write_buffer_size` is 2GiB either way. CSPP reached 9,664,512 keys and
1,610,644,224 active-memtable bytes; default SkipList reached 9,090,048 keys
and 1,610,614,784 bytes. The active memtable stops at the same 75%
byte target; the key counts differ.

| recovery path | five runs (ms) | avg (ms) | ratio |
| --- | --- | ---: | ---: |
| CSPP crash-safe | 6.545, 4.692, 3.820, 4.768, 5.120 | 4.989 | 1.0 |
| Default SkipList / WAL replay | 6124.357, 5849.884, 5984.324, 5943.391, 6033.802 | 5987.152 | 1200.1 |

Both groups exited with status 0. The copied images contained no SST and had
not flushed early. Separate LOG checks confirmed CSPP leftover conversion
and WAL-tail recovery without fallback, and full WAL recovery for default SkipList.
