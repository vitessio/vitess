# Copyright 2012, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

MAKEFLAGS = -s

all: build bson unit_test queryservice_test integration_test

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

bson:
	bsongen -file ./go/mysql/proto/structs.go -type QueryResult -o ./go/mysql/proto/query_result_bson.go
	bsongen -file ./go/mysql/proto/structs.go -type Field -o ./go/mysql/proto/field_bson.go
	bsongen -file ./go/vt/vtgate/proto/vtgate_proto.go -type QueryResult -o ./go/vt/vtgate/proto/query_result_bson.go
	bsongen -file ./go/vt/key/key.go -type KeyRange -o ./go/vt/key/key_range_bson.go
	bsongen -file ./go/vt/tabletserver/proto/structs.go -type Query -o ./go/vt/tabletserver/proto/query_bson.go
	bsongen -file ./go/vt/tabletserver/proto/structs.go -type Session -o ./go/vt/tabletserver/proto/session_bson.go
	bsongen -file ./go/vt/tabletserver/proto/structs.go -type BoundQuery -o ./go/vt/tabletserver/proto/bound_query_bson.go
	bsongen -file ./go/vt/tabletserver/proto/structs.go -type QueryList -o ./go/vt/tabletserver/proto/query_list_bson.go
	bsongen -file ./go/vt/tabletserver/proto/structs.go -type QueryResultList -o ./go/vt/tabletserver/proto/query_result_list_bson.go
	bsongen -file ./go/vt/vtgate/proto/vtgate_proto.go -type QueryShard -o ./go/vt/vtgate/proto/query_shard_bson.go
	bsongen -file ./go/vt/vtgate/proto/vtgate_proto.go -type BatchQueryShard -o ./go/vt/vtgate/proto/batch_query_shard_bson.go
	bsongen -file ./go/vt/vtgate/proto/vtgate_proto.go -type StreamQueryKeyRange -o ./go/vt/vtgate/proto/stream_query_keyrange_bson.go
	bsongen -file ./go/vt/topo/srvshard.go -type SrvShard -o ./go/vt/topo/srvshard_bson.go
	bsongen -file ./go/vt/topo/srvshard.go -type SrvKeyspace -o ./go/vt/topo/srvkeyspace_bson.go
	bsongen -file ./go/vt/topo/srvshard.go -type KeyspacePartition -o ./go/vt/topo/keyspace_partition_bson.go
	bsongen -file ./go/vt/vtgate/proto/vtgate_proto.go -type Session -o ./go/vt/vtgate/proto/session_bson.go
	bsongen -file ./go/vt/vtgate/proto/vtgate_proto.go -type ShardSession -o ./go/vt/vtgate/proto/shard_session_bson.go
	bsongen -file ./go/vt/binlog/proto/binlog_transaction.go -type BinlogTransaction -o ./go/vt/binlog/proto/binlog_transaction_bson.go
	bsongen -file ./go/vt/binlog/proto/binlog_transaction.go -type Statement -o ./go/vt/binlog/proto/statement_bson.go
	bsongen -file ./go/vt/binlog/proto/stream_event.go -type StreamEvent -o ./go/vt/binlog/proto/stream_event_bson.go
	bsongen -file ./go/zk/zkocc_structs.go -type ZkPath -o ./go/zk/zkpath_bson.go
	bsongen -file ./go/zk/zkocc_structs.go -type ZkPathV -o ./go/zk/zkpathv_bson.go
