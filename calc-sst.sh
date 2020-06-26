
dir=$(cd $(dirname $0);pwd)

grep 'default\] Compaction start summary' databaseDir/data/.rocksdb/LOG | perl $dir/calc-sst.pl
