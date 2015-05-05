// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package schemamanager

import (
	"testing"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
)

func TestOpenVtGateExecutor(t *testing.T) {
	fakeConn := newFakeVtGateConn()
	exec := newFakeVtGateExecutor(fakeConn)
	if err := exec.Open(); err != nil {
		t.Fatalf("failed to call executor.Open: %v", err)
	}
	exec.Close()
}

func TestValidate(t *testing.T) {
	fakeConn := newFakeVtGateConn()
	exec := newFakeVtGateExecutor(fakeConn)
	defer exec.Close()

	invalidSelect := []string{"select  from test_table"}
	if err := exec.Validate(invalidSelect); err == nil {
		t.Fatalf("exec.Validate should fail due to given invalid select statement")
	}
	invalidUpdate := []string{"update from test_table"}
	if err := exec.Validate(invalidUpdate); err == nil {
		t.Fatalf("exec.Validate should fail due to given invalid update statement")
	}
	invalidDelete := []string{"delete * from test_table"}
	if err := exec.Validate(invalidDelete); err == nil {
		t.Fatalf("exec.Validate should fail due to given invalid delete statement")
	}
	validSelect := []string{"select * from test_table"}
	if err := exec.Validate(validSelect); err != nil {
		t.Fatalf("exec.Validate should success but get error: %v", err)
	}
	validUpdate := []string{"update test_table set col1=1"}
	if err := exec.Validate(validUpdate); err != nil {
		t.Fatalf("exec.Validate should success but get error: %v", err)
	}
	validDelete := []string{"delete from test_table where col1=1"}
	if err := exec.Validate(validDelete); err != nil {
		t.Fatalf("exec.Validate should success but get error: %v", err)
	}
}

func TestExecuteWithoutOpen(t *testing.T) {
	shards := []string{"0", "1"}
	sqls := []string{"insert into  test_table values (1, 2)"}
	fakeConn := newFakeVtGateConn()
	exec := newFakeVtGateExecutor(fakeConn)
	result := exec.Execute(sqls, shards)
	if result.ExecutorErr == "" {
		t.Fatalf("execute should fail because Execute() is being called before Open()")
	}
}

func TestExecuteDML(t *testing.T) {
	shards := []string{"0", "1"}
	validSqls := []string{"insert into  test_table values (1, 2)"}
	invalidSqls := []string{"insert into test_table ..."}
	fakeConn := newFakeVtGateConn()

	for _, sql := range validSqls {
		for _, shard := range shards {
			fakeConn.AddShardQuery(
				&proto.QueryShard{
					Sql:           sql,
					BindVariables: nil,
					Keyspace:      "test_keyspace",
					Shards:        []string{shard},
					TabletType:    topo.TYPE_MASTER,
					Session: &proto.Session{
						InTransaction: true,
					},
				},
				&mproto.QueryResult{})
		}
	}

	exec := newFakeVtGateExecutor(fakeConn)
	exec.Open()
	defer exec.Close()
	result := exec.Execute(invalidSqls, shards)
	if len(result.FailedShards) == 0 && result.ExecutorErr == "" {
		t.Fatalf("execute should fail due to an invalid sql")
	}
	result = exec.Execute(validSqls, shards)
	if len(result.FailedShards) > 0 {
		t.Fatalf("execute failed, error: %v", result.FailedShards)
	}
	if result.ExecutorErr != "" {
		t.Fatalf("execute failed, sqls: %v, error: %s", validSqls, result.ExecutorErr)
	}
}

func TestExecuteDDL(t *testing.T) {
	fakeConn := newFakeVtGateConn()
	shards := []string{"0", "1"}

	validSqls := []string{"alter table test_table add column_01 int"}
	for _, sql := range validSqls {
		for _, shard := range shards {
			fakeConn.AddShardQuery(
				&proto.QueryShard{
					Sql:           sql,
					BindVariables: nil,
					Keyspace:      "test_keyspace",
					Shards:        []string{shard},
					TabletType:    topo.TYPE_MASTER,
					Session:       nil,
				},
				&mproto.QueryResult{})
		}
	}
	exec := newFakeVtGateExecutor(fakeConn)
	exec.Open()
	defer exec.Close()
	result := exec.Execute(validSqls, shards)
	if len(result.FailedShards) > 0 {
		t.Fatalf("execute failed, error: %v", result.FailedShards)
	}
	if result.ExecutorErr != "" {
		t.Fatalf("execute failed, sqls: %v, error: %s", validSqls, result.ExecutorErr)
	}
	// alter a non exist table
	invalidSqls := []string{"alter table table_not_exist add column_01 int"}
	result = exec.Execute(invalidSqls, shards)
	if len(result.FailedShards) == 0 {
		t.Fatalf("execute should fail")
	}
}
