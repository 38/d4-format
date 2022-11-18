#!/bin/bash

if [ "${DOCS_RS}" = "1" ]
then
	exit 0
fi

set -x
cd $1
rm -rf ${1}/libBigWig
if [ "x${BIGWIG_SRC_PREFIX}" = "x" ]
then
	git clone -b ${2} http://github.com/dpryan79/libBigWig
else
	cp -rvf ${BIGWIG_SRC_PREFIX} libBigWig
fi
cd libBigWig
perl -i -pe 's/-lcurl//g' Makefile

if [ ! -z $(echo ${TARGET} | grep "musl") ]
then
	export CC=musl-gcc
fi

function is_musl() {
	if [ ! -z $(echo $TARGET | grep musl) ]; then 
		return 0
	else
		return 1
	fi
}

if [ "x${ZLIB_SRC}" != "x" ]
then
	tar xz ${ZLIB_SRC}
else
	curl -L 'http://github.com/madler/zlib/archive/refs/tags/v1.2.11.tar.gz' | tar xz
fi
cd zlib-1.2.11
is_musl && CC=musl-gcc ./configure || ./configure
make
cp libz.a ..
cd ..

CFLAGS="-DNOCURL -O3 -Izlib-1.2.11" make -j8 lib-static

