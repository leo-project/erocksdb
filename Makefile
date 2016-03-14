BUILD_CONFIG = make_config.mk

all: compile test

get-deps:
	./c_src/build_deps.sh get-deps

rm-deps:
	./c_src/build_deps.sh rm-deps

compile:
	PLATFORM_LDFLAGS="`cat c_src/rocksdb/${BUILD_CONFIG} |grep PLATFORM_LDFLAGS| awk -F= '{print $$2}'|sed -e 's/-lsnappy//'`" ./rebar compile

test: compile
	./rebar eunit

clean:
	./rebar clean
