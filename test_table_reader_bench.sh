#you should run "make table_reader_bench" firstly


echo 3 > /proc/sys/vm/drop_caches

#terarkzip table
./table_reader_bench --table_factory=terark_zip --through_db=false --iterator=false

#plain_table
#./table_reader_bench --table_factory=plain_table --through_db=false --iterator=false  

#cuckoo_table
#./table_reader_bench --table_factory=cuckoo_hash --through_db=false --iterator=false

#block_based table
#./table_reader_bench --table_factory=block_based --through_db=false --iterator=false

