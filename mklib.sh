set -e
set -x
export LC_ALL=C
export LANG=C

#CompilerList="g++-4.7 g++-4.8 g++-4.9 g++-6 g++-6.1 g++-6.2 g++-5.3 g++-5.4 clang++"
CompilerList="g++-4.8 g++-4.9 g++-6 g++-6.1 g++-6.2 g++-5.3 g++-5.4 clang++"
#CompilerList="g++-6.2 g++-5.3 g++-5.4 clang++"
#CompilerList="g++-6.1 g++-5.3 clang++"
#CompilerList="g++-5.3 clang++"
#CompilerList="g++-5.4"
for CXX in $CompilerList
do
	if which $CXX; then
		COMPILER=`${CXX} terark-tools/detect-compiler.cpp -o a && ./a && rm -f a a.exe`
		UNAME_MachineSystem=`uname -m -s | sed 's:[ /]:-:g'`
		LIBDIR=${UNAME_MachineSystem}-${COMPILER}
		mkdir -p ${LIBDIR}
		make clean CXX=$CXX
		make shared_lib CXX=$CXX
		make static_lib CXX=$CXX -j32
		mv librocksdb.* ${LIBDIR}
	else
		echo Not found compiler: $CXX
	fi
done

