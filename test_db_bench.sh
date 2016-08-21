#you should run "make db_bench" firstly

num=100000
readnum=10000
valuesize=1024
dbdir=/data/rocksdbdata
terarktempdir=/data/tmp

rm -rf $dbdir
rm -rf $terarktempdir

echo 3 > /proc/sys/vm/drop_caches

#terarkzip table
./db_bench --benchmarks=fillrandom,readrandom --num=$num --value_size=$valuesize --db=$dbdir --terarktempdir=$terarktempdir --use_terarkzip_table=true --mmap_read=true --mmap_write=true

#plain_table

#./db_bench --benchmarks=fillrandom,readrandom --num=$num --value_size=$valuesize --db=$dbdir --use_plain_table=true --use_terarkzip_table=false --mmap_read=true --mmap_write=true --memtablerep=prefix_hash --prefix_size=16

#cuckoo_table

#./db_bench --benchmarks=fillseq,readrandom --num=$num --value_size=$valuesize --db=$dbdir --use_cuckoo_table=true --use_plain_table=false --use_terarkzip_table=false --mmap_read=true --mmap_write=true

#block_based table

#./db_bench --benchmarks=fillrandom,readrandom --num=$num --value_size=$valuesize --db=$dbdir --use_cuckoo_table=false --use_plain_table=false --use_terarkzip_table=false --mmap_read=true --mmap_write=true
