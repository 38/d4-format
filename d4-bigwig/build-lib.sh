#!/bin/bash

if [ "${DOCS_RS}" = "1" ]
then
	exit 0
fi

set -x
cd $1
rm -rf ${1}/libBigWig
git clone -b ${2} https://github.com/dpryan79/libBigWig
cd libBigWig
sed -i 's/-lcurl//g' Makefile

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

curl 'https://zlib.net/zlib-1.2.11.tar.gz' | tar xz
cd zlib-1.2.11
is_musl && CC=musl-gcc ./configure || ./configure
make
cp libz.a ..
cd ..

CFLAGS="-DNOCURL -O3 -Izlib-1.2.11" make -j8 lib-static

