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

afr_sig=a
dbg_sig=d
rls_sig=r

for type in $BUILD_TYPE; do
	sig=`eval 'echo $'${type}_sig`
	libs=`echo -lterark-{zbs,fsa,core}-${COMPILER}-$sig`
	env CXX=$CXX \
		LIB_SOURCES="`echo ../terark-zip-rocksdb/src/table/*.cc`" \
		OBJ_DIR=${BUILD_ROOT}/$type \
		EXTRA_CXXFLAGS='-I../terark/src -I../terark/boost-include' \
		EXTRA_LDFLAGS="-L../terark/$BUILD_ROOT/lib_shared $libs" \
	  make $@
done
