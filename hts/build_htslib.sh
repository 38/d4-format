#!/bin/bash

set -ex

pushd ${OUT_DIR}

HTSLIB_VERSION=${1}

rm -rf ${OUT_DIR}/htslib

git clone -b ${HTSLIB_VERSION} https://github.com/samtools/htslib.git

cd htslib

cat > config.h << CONFIG_H
#define HAVE_LIBBZ2 1
#define HAVE_DRAND48 1
CONFIG_H

sed -i 's/hfile_libcurl\.o//g' Makefile
	
function is_musl() {
	if [ ! -z $(echo $TARGET | grep musl) ]; then 
		return 0
	else
		return 1
	fi
}

is_musl && sed -i 's/gcc/musl-gcc/g' Makefile


curl 'https://zlib.net/zlib-1.2.11.tar.gz' | tar xz
cd zlib-1.2.11
is_musl && CC=musl-gcc ./configure || ./configure
make
cp libz.a ..
cd ..

curl https://pilotfiber.dl.sourceforge.net/project/bzip2/bzip2-1.0.6.tar.gz | tar xz
cd bzip2-1.0.6
is_musl && sed -i 's/gcc/musl-gcc/g' Makefile
is_musl || sed -i 's/CFLAGS=/CFLAGS=-fPIC /g' Makefile
make
cp libbz2.a ..
cd ..

sed -i 's/CPPFLAGS =/CPPFLAGS = -Izlib-1.2.11 -Ibzip2-1.0.6/g' Makefile

is_musl || sed -i 's/CFLAGS *=/CFLAGS = -fPIC/g' Makefile

make -j8 lib-static

exit 0
