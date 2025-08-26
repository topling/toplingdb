#!/bin/bash -ex

#rm -rf /dev/shm/db_bench_enterprise
#rm -rf /tmp/db_bench_enterprise
mkdir -p /dev/shm/db_bench_enterprise
#mkdir -p /tmp/db_bench_enterprise
cp sideplugin/rockside/src/topling/web/{index.html,style.css} /dev/shm/db_bench_enterprise/

export TOPLINGDB_GetContext_sampling=kNone
export ROCKSDB_KICK_OUT_OPTIONS_FILE=1
export LD_LIBRARY_PATH=/opt/lib:/usr/local/lib:$LD_LIBRARY_PATH
ulimit -n 100000
args=(
    -json sideplugin/rockside/sample-conf/db_bench_enterprise.yaml
    -num=10000000 -key_size=16
    -value_size=2000
    -batch_size=100
   #-benchmarks=fillseq,compact,nextwithkey,nextwithkey,nextwithkey,nextwithkey,nextwithkey,readseq,readseq,readseq,readseq,readseq
    -benchmarks=fillrandom,readrandom
   #-threads=8
    -enable_zero_copy # ToplingDB specific, for point search by Get/MultiGet
)
./db_bench ${args[@]} "$@"
