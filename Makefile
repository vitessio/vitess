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

#export VT_TEST_FLAGS=-v for instance
integration_test_files = clone.py \
	initial_sharding_bytes.py \
	initial_sharding.py \
	keyrange_test.py \
	keyspace_test.py \
	reparent.py \
	resharding_bytes.py \
	resharding.py \
	rowcache_invalidator.py \
	secure.py \
	schema.py \
	sharded.py \
	tabletmanager.py \
	update_stream.py \
	vertical_split.py \
	vertical_split_vtgate.py \
	vtdb_test.py \
	vtgate_test.py \
	zkocc_test.py

.ONESHELL:
SHELL = /bin/bash
integration_test:
	cd test ; \
	for t in $(integration_test_files) ; do \
		echo Running test/$$t... ; \
		output=$$(time ./$$t $$VT_TEST_FLAGS 2>&1) ; \
		if [[ $$? != 0 ]]; then \
			echo $output >&2 ; \
			exit 1 ; \
		fi ; \
		echo ; \
	done

bson:
	bsongen -file ./go/mysql/proto/structs.go -type QueryResult -o ./go/mysql/proto/query_result_bson.go
	bsongen -file ./go/mysql/proto/structs.go -type Field -o ./go/mysql/proto/field_bson.go
	bsongen -file ./go/vt/key/key.go -type KeyRange -o ./go/vt/key/key_range_bson.go
	bsongen -file ./go/vt/key/key.go -type KeyspaceId -o ./go/vt/key/keyspace_id_bson.go
	bsongen -file ./go/vt/key/key.go -type KeyspaceIdType -o ./go/vt/key/keyspace_id_type_bson.go
	bsongen -file ./go/vt/tabletserver/proto/structs.go -type Query -o ./go/vt/tabletserver/proto/query_bson.go
	bsongen -file ./go/vt/tabletserver/proto/structs.go -type Session -o ./go/vt/tabletserver/proto/session_bson.go
	bsongen -file ./go/vt/tabletserver/proto/structs.go -type BoundQuery -o ./go/vt/tabletserver/proto/bound_query_bson.go
	bsongen -file ./go/vt/tabletserver/proto/structs.go -type QueryList -o ./go/vt/tabletserver/proto/query_list_bson.go
	bsongen -file ./go/vt/tabletserver/proto/structs.go -type QueryResultList -o ./go/vt/tabletserver/proto/query_result_list_bson.go
	bsongen -file ./go/vt/vtgate/proto/vtgate_proto.go -type QueryShard -o ./go/vt/vtgate/proto/query_shard_bson.go
	bsongen -file ./go/vt/vtgate/proto/vtgate_proto.go -type BatchQueryShard -o ./go/vt/vtgate/proto/batch_query_shard_bson.go
	bsongen -file ./go/vt/vtgate/proto/vtgate_proto.go -type KeyspaceIdQuery -o ./go/vt/vtgate/proto/keyspace_id_query_bson.go
	bsongen -file ./go/vt/vtgate/proto/vtgate_proto.go -type KeyRangeQuery -o ./go/vt/vtgate/proto/key_range_query_bson.go
	bsongen -file ./go/vt/vtgate/proto/vtgate_proto.go -type EntityIdsQuery -o ./go/vt/vtgate/proto/entity_ids_query_bson.go
	bsongen -file ./go/vt/vtgate/proto/vtgate_proto.go -type KeyspaceIdBatchQuery -o ./go/vt/vtgate/proto/keyspace_id_batch_query_bson.go
	bsongen -file ./go/vt/vtgate/proto/vtgate_proto.go -type Session -o ./go/vt/vtgate/proto/session_bson.go
	bsongen -file ./go/vt/vtgate/proto/vtgate_proto.go -type ShardSession -o ./go/vt/vtgate/proto/shard_session_bson.go
	bsongen -file ./go/vt/vtgate/proto/vtgate_proto.go -type QueryResult -o ./go/vt/vtgate/proto/query_result_bson.go
	bsongen -file ./go/vt/topo/srvshard.go -type SrvShard -o ./go/vt/topo/srvshard_bson.go
	bsongen -file ./go/vt/topo/srvshard.go -type SrvKeyspace -o ./go/vt/topo/srvkeyspace_bson.go
	bsongen -file ./go/vt/topo/srvshard.go -type KeyspacePartition -o ./go/vt/topo/keyspace_partition_bson.go
	bsongen -file ./go/vt/topo/tablet.go -type TabletType -o ./go/vt/topo/tablet_type_bson.go
	bsongen -file ./go/vt/binlog/proto/binlog_transaction.go -type BinlogTransaction -o ./go/vt/binlog/proto/binlog_transaction_bson.go
	bsongen -file ./go/vt/binlog/proto/binlog_transaction.go -type Statement -o ./go/vt/binlog/proto/statement_bson.go
	bsongen -file ./go/vt/binlog/proto/stream_event.go -type StreamEvent -o ./go/vt/binlog/proto/stream_event_bson.go
	bsongen -file ./go/zk/zkocc_structs.go -type ZkPath -o ./go/zk/zkpath_bson.go
	bsongen -file ./go/zk/zkocc_structs.go -type ZkPathV -o ./go/zk/zkpathv_bson.go
	bsongen -file ./go/zk/zkocc_structs.go -type ZkStat -o ./go/zk/zkstat_bson.go
	bsongen -file ./go/zk/zkocc_structs.go -type ZkNode -o ./go/zk/zknode_bson.go
	bsongen -file ./go/zk/zkocc_structs.go -type ZkNodeV -o ./go/zk/zknodev_bson.go

