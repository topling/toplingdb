#!/usr/bin/bash

set -e

# you should just change the 2 lines
ANDROID_HOME=${HOME}/osc/android
export ANDROID_NDK_HOME=${ANDROID_HOME}/ndk/26.1.10909125

# these lines need not change
CXX_HOME=${ANDROID_NDK_HOME}/toolchains/llvm/prebuilt/linux-x86_64
export CXX=${CXX_HOME}/bin/aarch64-linux-android34-clang++
export CC=${CXX_HOME}/bin/aarch64-linux-android34-clang
export LD=${CXX}
export AR=${CXX_HOME}/bin/llvm-ar
export STRIP_CMD=${CXX_HOME}/bin/llvm-strip
export ENABLE_AUTO_CHECK_LD=0
export JAVAC_ARGS="-source 17 -target 17"
export BUILD_PREFIX=../build-toplingdb/
export ROCKSDB_DISABLE_GFLAGS=1
export CPU="-O2" # fool the make
export WITH_BMI2=na
export WITH_TOPLING_ROCKS=0
export WITH_TOPLING_DCOMPACT=0
export DISABLE_JEMALLOC=1
export EXTRA_CXXFLAGS="-Wno-deprecated-builtins -Wno-shorten-64-to-32 -DBOOST_NO_CXX98_FUNCTION_BASE"
export EXTRA_CXXFLAGS="-Wno-deprecated-builtins -DBOOST_NO_CXX98_FUNCTION_BASE"
export W_shorten_64_to_32=0
export ARCHFLAG="-arch aarch64"
export MACHINE=aarch64
export TARGET_OS=OS_ANDROID_CROSSCOMPILE
export COMPILER=clang-17.0
export UNAME_MachineSystem=android34-aarch64
export UNAME_System=android34
export MARCH=aarch64
export TOPLING_DISABLE_FIBER_AIO=1
export ZLIB_READY_SKIP_CHECK=1
export IS_CYGWIN=0
export STRIP_DEBUG_INFO=1
export ROCKSDB_JAR_WITH_DYNAMIC_LIBS=1
export JAVA_STATIC_DEPS_CXXFLAGS="-fPIC"

#make UPDATE_REPO=0 DEBUG_LEVEL=0 DISABLE_JEMALLOC=1 TOPLING_USE_DYNAMIC_TLS=1 -j60 clean
make UPDATE_REPO=0 DEBUG_LEVEL=0 DISABLE_JEMALLOC=1 TOPLING_USE_DYNAMIC_TLS=1 -j60 libsnappy.a liblz4.a libbz2.a
make UPDATE_REPO=0 DEBUG_LEVEL=0 DISABLE_JEMALLOC=1 TOPLING_USE_DYNAMIC_TLS=1 -j60 rocksdbjava
