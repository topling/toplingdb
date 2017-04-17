
if [ "$USER" != "root" ]; then
	echo "### must use sudo ###"
	exit 1
fi

set -e
set -x
export LC_ALL=C
export LANG=C
UNAME_MachineSystem=`uname -m -s | sed 's:[ /]:-:g'`

function make_bin() {
	local dbgLevel=$1
	shift
	make DEBUG_LEVEL=$dbgLevel CXX=$CXX clean 
	rm -f librocksdb*
	ln -s ${LIBDIR}/librocksdb* .
	make DEBUG_LEVEL=$dbgLevel CXX=$CXX $@
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
	rm -f /opt/${COMPILER}/lib64/${libName}.*
	cp -a ${libName}.* /opt/${COMPILER}/lib64 || true
	mv ${libName}.* ${LIBDIR}
}

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
		mkdir -p ${LIBDIR}
		make_lib 2 librocksdb_debug
		make_lib 0 librocksdb
		make_bin 0 ldb db_bench
	else
		echo Not found compiler: $CXX
	fi
done

