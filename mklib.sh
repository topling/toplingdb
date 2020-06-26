
if [ "$USER" != "root" ]; then
	echo "### must use sudo ###"
	exit 1
fi

set -e
set -x
export LC_ALL=C
export LANG=C
UNAME_MachineSystem=`uname -m -s | sed 's:[ /]:-:g'`
if [ -z "$BMI2" ]; then
	BMI2=0
fi

function make_bin() {
	local dbgLevel=$1
	shift
	make DEBUG_LEVEL=$dbgLevel CXX=$CXX clean
	rm -f librocksdb*
	ln -s ${LIBDIR}/librocksdb* .
	make DEBUG_LEVEL=$dbgLevel CXX=$CXX $@ -j32
	for binName in $@; do
		rm -f /opt/${COMPILER}/bin/${binName}
		cp -a ${binName} /opt/${COMPILER}/bin || true
		cp -a ${binName} ${LIBDIR}
	done
	rm librocksdb*
}

function make_lib() {
	local dbgLevel=$1
	local libName=$2
	make DEBUG_LEVEL=$dbgLevel CXX=$CXX clean 
	make DEBUG_LEVEL=$dbgLevel CXX=$CXX shared_lib -j32
	make DEBUG_LEVEL=$dbgLevel CXX=$CXX static_lib -j32
	BUILD_NAME=${UNAME_MachineSystem}-${COMPILER}-bmi2-${BMI2}
	BUILD_ROOT=build/${BUILD_NAME}
	xdir=${BUILD_ROOT}/shared_lib/dbg-$dbgLevel
	rm -f /opt/${COMPILER}/lib64/${libName}.*
	rm -f ${LIBDIR}/${libName}.*
	cp -a ${libName}.a ${xdir}/${libName}.*  /opt/${COMPILER}/lib64
	cp -a ${libName}.a ${xdir}/${libName}.*  ${LIBDIR}
	cp -a /usr/lib64/libzstd.so*             ${LIBDIR}
}

OLD_LD_LIBRARY_PATH=$LD_LIBRARY_PATH

CompilerList="g++-4.7 g++-4.8 g++-4.9 g++-5.3 g++-5.4 g++-6 g++-6.1 g++-6.2 g++-6.3 g++-6.4 clang++"
#CompilerList="g++-5.3 g++-5.4 g++-6 g++-6.1 g++-6.2 g++-6.3 g++-6.4 clang++"
#CompilerList="g++-6.2 g++-5.3 g++-5.4 clang++"
#CompilerList="g++-6.1 g++-5.3 clang++"
#CompilerList="g++-5.3 clang++"
#CompilerList="g++-5.4"
#CompilerList="g++-4.8 g++-4.9"
#CompilerList="g++-4.8"
for CXX in $CompilerList
do
	if which $CXX; then
		tmpfile=`mktemp --suffix=.exe`
		COMPILER=`${CXX} terark-tools/detect-compiler.cpp -o ${tmpfile} && ${tmpfile} && rm -f ${tmpfile}`
		LIBDIR=${UNAME_MachineSystem}-${COMPILER}
		export LD_LIBRARY_PATH=/opt/$CXX/lib64:$OLD_LD_LIBRARY_PATH
		#export EXTRA_LDFLAGS="-L/opt/$CXX/lib64"
		mkdir -p ${LIBDIR}
		make_lib 2 librocksdb_debug
		make_lib 0 librocksdb
		make_bin 0 ldb db_bench
	else
		echo Not found compiler: $CXX
	fi
done

