#!/bin/bash
set -e
mydir=$(realpath $(dirname $0))
topdir=$(realpath $mydir/../..)
dbdir=/dev/shm/db_bench_enterprise # defined in db_bench_enterprise.yaml
mkdir -p $dbdir
cp $topdir/sideplugin/rockside/src/topling/web/{style.css,index.html} $dbdir
args=(
    --add-opens java.base/java.nio=ALL-UNNAMED
    -jar $mydir/target/rocksdbjni-jmh-1.0-SNAPSHOT-benchmarks.jar
    -p keyCount=10000
    -p keySize=128
    -p valueSize=512
    #-p valueSize=65536 # larger value size showing zero copy faster
    #-p dbname=db_bench_enterprise
    #-p dbpath=/dev/shm/db_bench_enterprise
    -p sideConf=$topdir/sideplugin/rockside/sample-conf/db_bench_enterprise.yaml
    SideGetBenchmarks
)
#libtopling=`cd $mydir/../target; echo libterark* | sed 's/ /:/g'`

# both libjemalloc.so and librocksdbjni-linux64.so used static tls -- the fast tls
# LD_PRELOAD: is required for static tls
# LD_PRELOAD: libjemalloc.so must be the first
# LD_PRELOAD: librocksdbjni-linux64.so has static tls
envs=(
    LD_LIBRARY_PATH=$topdir/java/target:$LD_LIBRARY_PATH
    #LD_PRELOAD=libjemalloc.so:$libtopling:librocksdbjni-linux64.so
    LD_PRELOAD=libjemalloc.so:librocksdbjni-linux64.so
    ROCKSDB_SHAREDLIB_DIR=LD_LIBRARY_PATH
    #SidePluginRepo_DebugLevel=2
)
env ${envs[@]} java ${args[@]}
