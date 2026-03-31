#!/usr/bin/bash

export UPDATE_REPO=0
#export ROCKSDB_DISABLE_GFLAGS=1
export TOPLING_ZIP_TABLE_TRIAL_DAYS=90
MAJOR_DOT_MINOR=`build_tools/version.sh major`.`build_tools/version.sh minor`

make -j60 libsnappy.a liblz4.a libbz2.a BUILD_PREFIX=bconf-0/
GetDebugLevel=(2 0)
for ((i=0;i<16;i++)); do
    export DEBUG_LEVEL=${GetDebugLevel[$((i/1%2))]}
    export USE_LTO=$((i/2%2))
    export DISABLE_JEMALLOC=$((i/4%2))
    export TOPLING_USE_DYNAMIC_TLS=$((i/8%2))
    make -j`nproc` upload-trial BUILD_PREFIX=bconf-${i}/
done
# The last bconf-15 is release build which:
# DEBUG_LEVEL=0,USE_LTO=1,DISABLE_JEMALLOC=1,TOPLING_USE_DYNAMIC_TLS=1
export BUILD_PREFIX=bconf-15/
rm -rf toplingdb-${MAJOR_DOT_MINOR}
make install-dcompact install-dev db_bench -j`nproc` \
    PREFIX=toplingdb-${MAJOR_DOT_MINOR} STRIP_DEBUG_INFO=1

install -C -m 755 db_bench    toplingdb-${MAJOR_DOT_MINOR}/bin
install -C -m 755 db_bench.sh toplingdb-${MAJOR_DOT_MINOR}
strip toplingdb-${MAJOR_DOT_MINOR}/bin/db_bench
sed -e 's:sideplugin/rockside/src/topling/web:site:' \
    -e 's:sideplugin/rockside/sample-conf:toplingdb-conf:' \
    -e 's:\./db_bench:bin/db_bench:' \
    -e '/ulimit/iexport LD_LIBRARY_PATH=lib:$LD_LIBRARY_PATH' \
    -i toplingdb-${MAJOR_DOT_MINOR}/db_bench.sh
sdk=toplingdb-${MAJOR_DOT_MINOR}-trail${TOPLING_ZIP_TABLE_TRIAL_DAYS}.tgz
tar czf ${sdk} toplingdb-${MAJOR_DOT_MINOR}
ossutil cp --region=cn-qingdao -f ${sdk} oss://topling-tools/
