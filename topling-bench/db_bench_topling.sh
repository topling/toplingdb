#!/bin/bash
echo "##############################################################"
#export SidePluginRepo_DebugLevel=2
common_args=(
  -json=sideplugin/rockside/sample-conf/db_bench_enterprise.yaml
  -key_size=8 -value_size=20 -num=100000000
  -disable_wal=true
)
read_args=(${common_args[@]} -enable_zero_copy=true -use_existing_db=true)

rm -rf /dev/shm/db_bench_enterprise
./db_bench ${common_args[@]} -benchmarks=fillrandom -batch_size=100 -threads=1

rm -rf /dev/shm/db_bench_enterprise
./db_bench ${common_args[@]} -benchmarks=fillrandom -batch_size=100

env TOPLINGDB_GetContext_sampling=kNone \
./db_bench ${read_args[@]} -benchmarks=readrandom -threads=1

env TOPLINGDB_GetContext_sampling=kNone \
./db_bench ${read_args[@]} -benchmarks=readrandom -threads=10

./db_bench ${read_args[@]} -benchmarks=readseq
./db_bench ${read_args[@]} -benchmarks=readreverse
