# Copyright 2012, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

MAKEFLAGS = -s

all: build unit_test queryservice_test integration_test

build:
	go install ./go/...

clean:
	go clean -i ./go/...

unit_test:
	go test ./go/...

unit_test_race:
	go test -race ./go/...

queryservice_test:
	echo "queryservice test"
	if [ -e "/usr/bin/memcached" ]; then \
		time test/queryservice_test.py -m ; \
	else \
		time test/queryservice_test.py ; \
	fi
	time test/sensitive_info_test.py

# export VT_TEST_FLAGS=-v for instance

integration_test:
	cd test ; echo "schema test"; time ./schema.py $$VT_TEST_FLAGS
	cd test ; echo "sharded test"; time ./sharded.py $$VT_TEST_FLAGS
	cd test ; echo "tabletmanager test"; time ./tabletmanager.py $$VT_TEST_FLAGS
	cd test ; echo "clone test"; time ./clone.py $$VT_TEST_FLAGS
	cd test ; echo "reparent test"; time ./reparent.py $$VT_TEST_FLAGS
	cd test ; echo "zkocc test"; time ./zkocc_test.py $$VT_TEST_FLAGS
	cd test ; echo "updatestream test"; time ./update_stream.py
	cd test ; echo "rowcache_invalidator test"; time ./rowcache_invalidator.py
	cd test ; echo "secure test"; time ./secure.py $$VT_TEST_FLAGS
	cd test ; echo "resharding test"; time ./resharding.py $$VT_TEST_FLAGS
	cd test ; echo "resharding_bytes test"; time ./resharding_bytes.py $$VT_TEST_FLAGS
	cd test ; echo "vtdb test"; time ./vtdb_test.py $$VT_TEST_FLAGS
	cd test ; echo "vtgate test"; time ./vtgate_test.py $$VT_TEST_FLAGS
	cd test ; echo "keyrange test"; time ./keyrange_test.py $$VT_TEST_FLAGS
	cd test ; echo "vertical_split test"; time ./vertical_split.py $$VT_TEST_FLAGS
	cd test ; echo "vertical_split_vtgate test"; time ./vertical_split_vtgate.py $$VT_TEST_FLAGS
	cd test ; echo "initial_sharding test"; time ./initial_sharding.py $$VT_TEST_FLAGS
	cd test ; echo "initial_sharding_bytes test"; time ./initial_sharding_bytes.py $$VT_TEST_FLAGS
	cd test ; echo "keyspace_test test"; time ./keyspace_test.py $$VT_TEST_FLAGS

# Build this target only if you want to regenerate the bson files
bson:
	bsongen -file ./go/mysql/proto/structs.go -type QueryResult -o ./go/mysql/proto/query_result_bson.go
	bsongen -file ./go/mysql/proto/structs.go -type Field -o ./go/mysql/proto/field_bson.go
	bsongen -file ./go/vt/vtgate/proto/vtgate_proto.go -type QueryResult -o ./go/vt/vtgate/proto/query_result_bson.go
	bsongen -file ./go/vt/key/key.go -type KeyRange -o ./go/vt/key/key_range_bson.go
