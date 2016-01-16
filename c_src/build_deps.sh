#!/bin/sh

# /bin/sh on Solaris is not a POSIX compatible shell, but /usr/bin/ksh is.
if [ `uname -s` = 'SunOS' -a "${POSIX_SHELL}" != "true" ]; then
    POSIX_SHELL="true"
    export POSIX_SHELL
    exec /usr/bin/ksh $0 $@
fi
unset POSIX_SHELL # clear it so if we invoke other scripts, they run as ksh as well

ROCKSDB_VSN="rocksdb-4.1"

SNAPPY_VSN="1.1.1"

set -e

if [ `basename $PWD` != "c_src" ]; then
    # originally "pushd c_src" of bash
    # but no need to use directory stack push here
    cd c_src
fi

BASEDIR="$PWD"

# detecting gmake and if exists use it
# if not use make
# (code from github.com/tuncer/re2/c_src/build_deps.sh
which gmake 1>/dev/null 2>/dev/null && MAKE=gmake
MAKE=${MAKE:-make}

# Changed "make" to $MAKE

case "$1" in
    rm-deps)
        rm -rf rocksdb system snappy-$SNAPPY_VSN
        ;;

    clean)
        rm -rf system snappy-$SNAPPY_VSN
        if [ -d rocksdb ]; then
            (cd rocksdb && $MAKE clean)
        fi
        ;;

    test)
        export CFLAGS="$CFLAGS -I $BASEDIR/system/include"
        export CXXFLAGS="$CXXFLAGS -I $BASEDIR/system/include"
        export LDFLAGS="$LDFLAGS -L$BASEDIR/system/lib"
        export LD_LIBRARY_PATH="$BASEDIR/rocksdb:$BASEDIR/system/lib:$LD_LIBRARY_PATH"

        (cd rocksdb && $MAKE ldb_tests)

        ;;

    get-deps)
        if [ ! -d rocksdb ]; then
            git clone git://github.com/facebook/rocksdb
            (cd rocksdb && git checkout $ROCKSDB_VSN)
        fi
        ;;

    *)
        if [ ! -d snappy-$SNAPPY_VSN ]; then
            tar -xzf snappy-$SNAPPY_VSN.tar.gz
            (cd snappy-$SNAPPY_VSN && ./configure --prefix=$BASEDIR/system --libdir=$BASEDIR/system/lib --with-pic)
        fi

        (cd snappy-$SNAPPY_VSN && $MAKE && $MAKE install)

        export CFLAGS="$CFLAGS -I $BASEDIR/system/include"
        export CXXFLAGS="$CXXFLAGS -I $BASEDIR/system/include"
        export LDFLAGS="$LDFLAGS -L$BASEDIR/system/lib"
        export LD_LIBRARY_PATH="$BASEDIR/system/lib:$LD_LIBRARY_PATH"

        if [ ! -d rocksdb ]; then
            git clone git://github.com/facebook/rocksdb
            (cd rocksdb && git checkout $ROCKSDB_VSN)
        fi
        if [ ! -f rocksdb/librocksdb.a ]; then
            (cd rocksdb && CXXFLAGS=-fPIC $MAKE static_lib)
        fi
        ;;
esac
