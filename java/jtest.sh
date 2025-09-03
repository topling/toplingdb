
export ROCKSDB_GET_PREFER_ZERO_COPY=true
export ROCKSDB_FORCE_DIRECT_BUFFER_ZERO_COPY=true
export TOPLINGDB_EAGER_FETCH_VALUE=true

#export CXX=g++-12
#export CC=gcc-12
#export DEBUG_LEVEL=1

export CPU="-march=native"
export UPDATE_REPO=0
export BUILD_PREFIX=../build-toplingdb/
export PREFIX=/opt

make -j`nproc` jtest
