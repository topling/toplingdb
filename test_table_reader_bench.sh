echo 3 > /proc/sys/vm/drop_caches

#terarkzip table
#./table_reader_bench --table_factory=terark_zip --through_db=false

#plain_table
#./table_reader_bench --table_factory=plain_table --through_db=false 

#cuckoo_table
#./table_reader_bench --table_factory=cuckoo_hash --through_db=false

#block_based table
./table_reader_bench --table_factory=block_based --through_db=false

