#!/bin/bash
echo "##############################################################"
common_args=(
  -key_size=8 -value_size=20 -num=100000000
  -db=/dev/shm/db_bench_rocksdb -bloom_bits=10
  -disable_wal=true
 #-write_buffer_size=805306368 # 768M
  -write_buffer_size=1073741824 # 1G
  -target_file_size_base=33554432
 #-target_file_size_base=67108864
  -target_file_size_multiplier=2
  -min_level_to_compress=10
 #-min_level_to_compress=1
 #-max_background_compactions=3
  -cache_size=34359738368
  -enable_pipelined_write=true
  -max_write_buffer_number=17
  -max_background_flushes=16
  -subcompactions=13
  -max_background_compactions=13
)
read_args=(${common_args[@]} -use_existing_db=true)

rm -rf /dev/shm/db_bench_rocksdb
./db_bench ${common_args[@]} -benchmarks=fillrandom -batch_size=100 -threads=1

rm -rf /dev/shm/db_bench_rocksdb
./db_bench ${common_args[@]} -benchmarks=fillrandom -batch_size=100

./db_bench ${read_args[@]} -benchmarks=readrandom  -threads=1
./db_bench ${read_args[@]} -benchmarks=readrandom  -threads=10
./db_bench ${read_args[@]} -benchmarks=readseq
./db_bench ${read_args[@]} -benchmarks=readreverse
