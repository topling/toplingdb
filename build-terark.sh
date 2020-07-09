#!/usr/bin/env bash
set -e

if test ${CXX}A = A; then
	echo use default CXX=g++
	CXX=g++
fi

tmpfile=`mktemp -u compiler-XXXXXX`
COMPILER=`${CXX} ../terark/tools/configure/compiler.cpp -o ${tmpfile}.exe && ./${tmpfile}.exe`
rm -f ${tmpfile}*
UNAME_MachineSystem=`uname -m -s | sed 's:[ /]:-:g'`
BUILD_NAME=${UNAME_MachineSystem}-${COMPILER}-bmi2-${WITH_BMI2}
BUILD_ROOT=build/${BUILD_NAME}

# BUILD_TYPE should be a list, such as "afr rls dbg" or "rls"
if test ${BUILD_TYPE}A = A; then
	BUILD_TYPE="rls dbg afr"
fi

#TERARK_PLUGINS_SRC="`echo ../terark-zip-rocksdb/src/table/*.cc`" \
TERARK_PLUGINS_SRC="\
 ../terark-zip-rocksdb/src/table/terark_zip_common.cc \
 ../terark-zip-rocksdb/src/table/terark_zip_config.cc \
 ../terark-zip-rocksdb/src/table/terark_zip_index.cc \
 ../terark-zip-rocksdb/src/table/terark_zip_table_builder.cc \
 ../terark-zip-rocksdb/src/table/terark_zip_table.cc \
 ../terark-zip-rocksdb/src/table/terark_zip_table_reader.cc \
"

afr_sig=a
dbg_sig=d
rls_sig=r

for type in $BUILD_TYPE; do
	sig=`eval 'echo $'${type}_sig`
	libs=`echo -lterark-{zbs,fsa,core}-${COMPILER}-$sig`
	env CXX=$CXX \
		DISABLE_WARNING_AS_ERROR=1 \
		LIB_SOURCES="$TERARK_PLUGINS_SRC" \
		OBJ_DIR=${BUILD_ROOT}/$type \
		EXTRA_CXXFLAGS='-I../terark/src -I../terark/boost-include -I../terark/3rdparty/zstd' \
		EXTRA_LDFLAGS="-L../terark/$BUILD_ROOT/lib_shared $libs" \
	  make WARNING_FLAGS='-Wall -Wno-shadow' $@
done
