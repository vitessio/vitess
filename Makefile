# Copyright 2012, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

MAKEFLAGS = -s

all: build unit_test queryservice_test integration_test

build:
	cd go/cmd/mysqlctl; go build
	cd go/cmd/normalizer; go build
	cd go/cmd/topo2topo; go build
	cd go/cmd/vt_binlog_player; go build
	cd go/cmd/vt_binlog_server; go build
	cd go/cmd/vtaction; go build
	cd go/cmd/vtclient2; go build
	cd go/cmd/vtctl; go build
	cd go/cmd/vtctld; go build
	cd go/cmd/vtocc; go build
	cd go/cmd/vttablet; go build
	cd go/cmd/zk; go build
	cd go/cmd/zkclient2; go build
	cd go/cmd/zkctl; go build
	cd go/cmd/zkns2pdns; go build
	cd go/cmd/zkocc; go build

# alphabetically ordered unit tests
# the ones that are commented out don't pass
unit_test:
	go test ./go/...

queryservice_test:
	echo "queryservice test"
	if [ -e "/usr/bin/memcached" ]; then \
		time test/queryservice_test.py -m ; \
	else \
		time test/queryservice_test.py ; \
	fi

# export VT_TEST_FLAGS=-v for instance

integration_test:
	cd test ; echo "schema test"; time ./schema.py $$VT_TEST_FLAGS
	cd test ; echo "sharded test"; time ./sharded.py $$VT_TEST_FLAGS
	cd test ; echo "tabletmanager test"; time ./tabletmanager.py $$VT_TEST_FLAGS
	cd test ; echo "zkocc test"; time ./zkocc_test.py $$VT_TEST_FLAGS
	cd test ; echo "connection test"; time ./connection_test.py
	cd test ; echo "updatestream test"; time ./update_stream.py
	cd test ; echo "rowcache_invalidator test"; time ./rowcache_invalidator.py
	cd test ; echo "secure test"; time ./secure.py $$VT_TEST_FLAGS
	cd test ; echo "resharding test"; time ./resharding.py $$VT_TEST_FLAGS

clean:
	cd go/cmd/mysqlctl; go clean
	cd go/cmd/normalizer; go clean
	cd go/cmd/vtaction; go clean
	cd go/cmd/vtclient2; go clean
	cd go/cmd/vtctl; go clean
	cd go/cmd/vtocc; go clean
	cd go/cmd/vttablet; go clean
	cd go/cmd/zk; go clean
	cd go/cmd/zkctl; go clean
	cd go/cmd/zkocc; go clean
