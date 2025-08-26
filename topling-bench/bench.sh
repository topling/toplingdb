ulimit -n 100000
rm -rf /dev/shm/db_bench_enterprise

env TOPLINGDB_GetContext_sampling=kNone ./db_bench \
  -json=sideplugin/rockside/sample-conf/db_bench_enterprise.yaml \
  -key_size=8 -value_size=200 -num=100000000 \
  -benchmarks=fillseq \
  -enable_zero_copy=false -use_existing_db=false

env TOPLINGDB_GetContext_sampling=kNone ./db_bench \
  -json=sideplugin/rockside/sample-conf/db_bench_enterprise.yaml \
  -key_size=8 -value_size=200 -num=100000000 \
  -benchmarks=readwhilewriting \
  -enable_zero_copy=false -use_existing_db=true

env TOPLINGDB_GetContext_sampling=kNone ./db_bench \
  -json=sideplugin/rockside/sample-conf/db_bench_enterprise.yaml \
  -key_size=8 -value_size=200 -num=100000000 \
  -benchmarks=fillseq \
  -enable_zero_copy=true -use_existing_db=false

env TOPLINGDB_GetContext_sampling=kNone ./db_bench \
  -json=sideplugin/rockside/sample-conf/db_bench_enterprise.yaml \
  -key_size=8 -value_size=200 -num=100000000 \
  -benchmarks=readwhilewriting \
  -enable_zero_copy=true -use_existing_db=true
