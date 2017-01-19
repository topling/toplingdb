#/usr/bin/bash

for dir in db memtable table include util utilities
do
  diff --exclude='*_test.cc' --exclude='*_bench.cc' --exclude='*.d' --exclude='*.o' -r $@ ../lpterark-rocksdb/$dir $dir
done
