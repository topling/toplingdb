#!/usr/bin/env bash
set -e

if test ${CXX}A = A; then
	echo use default CXX=g++
	CXX=g++
fi

TERARK_CORE_DIR=`../terark-core`
WITH_BMI2=`bash ${TERARK_CORE_DIR}/cpu_has_bmi2.sh`
tmpfile=`mktemp -u compiler-XXXXXX`
COMPILER=`${CXX} ${TERARK_CORE_DIR}/tools/configure/compiler.cpp -o ${tmpfile}.exe && ./${tmpfile}.exe`
rm -f ${tmpfile}*
UNAME_MachineSystem=`uname -m -s | sed 's:[ /]:-:g'`
BUILD_NAME=${UNAME_MachineSystem}-${COMPILER}-bmi2-${WITH_BMI2}
BUILD_ROOT=build/${BUILD_NAME}

# BUILD_TYPE should be a list, such as "afr rls dbg" or "rls"
if test ${BUILD_TYPE}A = A; then
	BUILD_TYPE="rls dbg afr"
fi

TERARK_PLUGINS_DIR=`cd ../terark-zip-rocksdb; pwd`
#TERARK_PLUGINS_SRC="`echo ${TERARK_PLUGINS_DIR}/src/table/*.cc`" \
TERARK_PLUGINS_SRC="\
 ${TERARK_PLUGINS_DIR}/src/table/terark_zip_common.cc \
 ${TERARK_PLUGINS_DIR}/src/table/terark_zip_config.cc \
 ${TERARK_PLUGINS_DIR}/src/table/terark_zip_index.cc \
 ${TERARK_PLUGINS_DIR}/src/table/terark_zip_table_builder.cc \
 ${TERARK_PLUGINS_DIR}/src/table/terark_zip_table.cc \
 ${TERARK_PLUGINS_DIR}/src/table/terark_zip_table_reader.cc \
"

afr_sig=a
dbg_sig=d
rls_sig=r

afr_level="1"
dbg_level="2"
rls_level="0"

for type in $BUILD_TYPE; do
	sig=`eval 'echo $'${type}_sig`
	libs=`echo -lterark-{zbs,fsa,core}-${COMPILER}-$sig`
	DEBUG_LEVEL=`eval 'echo $'${type}_level`
	env CXX=$CXX \
		DISABLE_WARNING_AS_ERROR=1 \
		EXTRA_LIB_SOURCES="$TERARK_PLUGINS_SRC" \
		OBJ_DIR=${BUILD_ROOT}/$type \
		EXTRA_CXXFLAGS="-I${TERARK_CORE_DIR}/src -I${TERARK_CORE_DIR}/boost-include -I${TERARK_CORE_DIR}/3rdparty/zstd -fPIC" \
		EXTRA_LDFLAGS="-L${TERARK_CORE_DIR}/$BUILD_ROOT/lib_shared $libs" \
		USE_RTTI=1 \
		DEBUG_LEVEL=$DEBUG_LEVEL \
	  make WARNING_FLAGS='-Wall -Wno-shadow -Wno-class-memaccess' $@
done

