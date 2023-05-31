/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vtgate

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// This file contains tests for all the autocommit code paths
// to make sure that single round-trip commits are executed
// correctly whenever possible.

// TestAutocommitUpdateSharded: instant-commit.
func TestAutocommitUpdateSharded(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()

	_, err := autocommitExec(executor, "update user set a=2 where id = 1")
	require.NoError(t, err)

	assertQueries(t, sbc1, []*querypb.BoundQuery{{
		Sql:           "update `user` set a = 2 where id = 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}})
	testCommitCount(t, "sbc1", sbc1, 0)

	assertQueries(t, sbc2, nil)
	testCommitCount(t, "sbc1", sbc1, 0)
}

// TestAutocommitUpdateLookup: transaction: select before update.
func TestAutocommitUpdateLookup(t *testing.T) {
	executor, sbc1, _, sbclookup := createExecutorEnv()
	sbclookup.SetResults([]*sqltypes.Result{sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("b|a", "int64|varbinary"),
		"2|1",
	)})

	_, err := autocommitExec(executor, "update music set a=2 where id = 2")
	require.NoError(t, err)

	vars, err := sqltypes.BuildBindVariable([]any{sqltypes.NewInt64(2)})
	require.NoError(t, err)

	assertQueries(t, sbclookup, []*querypb.BoundQuery{{
		Sql: "select music_id, user_id from music_user_map where music_id in ::music_id for update",
		BindVariables: map[string]*querypb.BindVariable{
			"music_id": vars,
		},
	}})
	testCommitCount(t, "sbclookup", sbclookup, 1)

	assertQueries(t, sbc1, []*querypb.BoundQuery{{
		Sql:           "update music set a = 2 where id = 2",
		BindVariables: map[string]*querypb.BindVariable{},
	}})
	testCommitCount(t, "sbc1", sbc1, 1)
}

// TestAutocommitUpdateVindexChange: transaction: select & update before final update.
func TestAutocommitUpdateVindexChange(t *testing.T) {
	executor, sbc, _, sbclookup := createExecutorEnv()
	sbc.SetResults([]*sqltypes.Result{sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("id|name|lastname|name_lastname_keyspace_id_map", "int64|int32|varchar|int64"),
		"1|1|foo|0",
	),
	})

	_, err := autocommitExec(executor, "update user2 set name='myname', lastname='mylastname' where id = 1")
	require.NoError(t, err)

	assertQueries(t, sbclookup, []*querypb.BoundQuery{{
		Sql: "delete from name_lastname_keyspace_id_map where `name` = :name and lastname = :lastname and keyspace_id = :keyspace_id",
		BindVariables: map[string]*querypb.BindVariable{
			"lastname":    sqltypes.StringBindVariable("foo"),
			"name":        sqltypes.Int32BindVariable(1),
			"keyspace_id": sqltypes.BytesBindVariable([]byte("\x16k@\xb4J\xbaK\xd6")),
		},
	}, {
		Sql: "insert into name_lastname_keyspace_id_map(`name`, lastname, keyspace_id) values (:name_0, :lastname_0, :keyspace_id_0)",
		BindVariables: map[string]*querypb.BindVariable{
			"name_0":        sqltypes.StringBindVariable("myname"),
			"lastname_0":    sqltypes.StringBindVariable("mylastname"),
			"keyspace_id_0": sqltypes.BytesBindVariable([]byte("\x16k@\xb4J\xbaK\xd6")),
		},
	}})
	testCommitCount(t, "sbclookup", sbclookup, 1)

	assertQueries(t, sbc, []*querypb.BoundQuery{{
		Sql:           "select id, `name`, lastname, `name` = 'myname' and lastname = 'mylastname' from user2 where id = 1 for update",
		BindVariables: map[string]*querypb.BindVariable{},
	}, {
		Sql:           "update user2 set `name` = 'myname', lastname = 'mylastname' where id = 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}})
	testCommitCount(t, "sbc", sbc, 1)
}

// TestAutocommitDeleteSharded: instant-commit.
func TestAutocommitDeleteSharded(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()

	_, err := autocommitExec(executor, "delete from user_extra where user_id = 1")
	require.NoError(t, err)

	assertQueries(t, sbc1, []*querypb.BoundQuery{{
		Sql:           "delete from user_extra where user_id = 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}})
	testCommitCount(t, "sbc1", sbc1, 0)

	assertQueries(t, sbc2, nil)
	testCommitCount(t, "sbc1", sbc1, 0)
}

// TestAutocommitDeleteLookup: transaction: select before update.
func TestAutocommitDeleteLookup(t *testing.T) {
	executor, sbc1, _, sbclookup := createExecutorEnv()
	sbc1.SetResults([]*sqltypes.Result{sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("id|name|lastname", "int64|int32|varchar"),
		"1|1|foo",
	),
	})
	sbclookup.SetResults([]*sqltypes.Result{sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("b|a", "int64|varbinary"),
		"1|1",
	)})

	_, err := autocommitExec(executor, "delete from music where id = 1")
	require.NoError(t, err)
	vars, err := sqltypes.BuildBindVariable([]any{sqltypes.NewInt64(1)})
	require.NoError(t, err)

	assertQueries(t, sbclookup, []*querypb.BoundQuery{{
		Sql: "select music_id, user_id from music_user_map where music_id in ::music_id for update",
		BindVariables: map[string]*querypb.BindVariable{
			"music_id": vars,
		},
	}, {
		Sql: "delete from music_user_map where music_id = :music_id and user_id = :user_id",
		BindVariables: map[string]*querypb.BindVariable{
			"music_id": sqltypes.Int32BindVariable(1),
			"user_id":  sqltypes.Uint64BindVariable(1),
		},
	}})
	testCommitCount(t, "sbclookup", sbclookup, 1)

	assertQueries(t, sbc1, []*querypb.BoundQuery{{
		Sql:           "select user_id, id from music where id = 1 for update",
		BindVariables: map[string]*querypb.BindVariable{},
	}, {
		Sql:           "delete from music where id = 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}})
	testCommitCount(t, "sbc1", sbc1, 1)
}

// TestAutocommitDeleteIn: instant-commit.
func TestAutocommitDeleteIn(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()

	_, err := autocommitExec(executor, "delete from user_extra where user_id in (1, 2)")
	require.NoError(t, err)

	assertQueries(t, sbc1, []*querypb.BoundQuery{{
		Sql:           "delete from user_extra where user_id in (1, 2)",
		BindVariables: map[string]*querypb.BindVariable{},
	}})
	testCommitCount(t, "sbc1", sbc1, 0)

	assertQueries(t, sbc2, nil)
	testCommitCount(t, "sbc2", sbc2, 0)
}

// TestAutocommitDeleteMultiShard: instant-commit.
func TestAutocommitDeleteMultiShard(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()

	_, err := autocommitExec(executor, "delete from user_extra where user_id = user_id + 1")
	require.NoError(t, err)

	assertQueries(t, sbc1, []*querypb.BoundQuery{{
		Sql:           "delete from user_extra where user_id = user_id + 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}})
	testCommitCount(t, "sbc1", sbc1, 1)

	assertQueries(t, sbc2, []*querypb.BoundQuery{{
		Sql:           "delete from user_extra where user_id = user_id + 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}})
	testCommitCount(t, "sbc2", sbc2, 1)
}

// TestAutocommitDeleteMultiShardAutoCommit: instant-commit.
func TestAutocommitDeleteMultiShardAutoCommit(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()

	_, err := autocommitExec(executor, "delete /*vt+ MULTI_SHARD_AUTOCOMMIT=1 */ from user_extra where user_id = user_id + 1")
	require.NoError(t, err)

	assertQueries(t, sbc1, []*querypb.BoundQuery{{
		Sql:           "delete /*vt+ MULTI_SHARD_AUTOCOMMIT=1 */ from user_extra where user_id = user_id + 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}})
	testCommitCount(t, "sbc1", sbc1, 0)

	assertQueries(t, sbc2, []*querypb.BoundQuery{{
		Sql:           "delete /*vt+ MULTI_SHARD_AUTOCOMMIT=1 */ from user_extra where user_id = user_id + 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}})
	testCommitCount(t, "sbc1", sbc1, 0)
}

// TestAutocommitInsertSharded: instant-commit.
func TestAutocommitInsertSharded(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()

	_, err := autocommitExec(executor, "insert into user_extra(user_id, v) values (1, 2)")
	require.NoError(t, err)

	assertQueries(t, sbc1, []*querypb.BoundQuery{{
		Sql: "insert into user_extra(user_id, v) values (:_user_id_0, 2)",
		BindVariables: map[string]*querypb.BindVariable{
			"_user_id_0": sqltypes.Int64BindVariable(1),
		},
	}})
	testCommitCount(t, "sbc1", sbc1, 0)

	assertQueries(t, sbc2, nil)
	testCommitCount(t, "sbc1", sbc1, 0)
}

// TestAutocommitInsertLookup: transaction: select before update.
func TestAutocommitInsertLookup(t *testing.T) {
	executor, sbc1, _, sbclookup := createExecutorEnv()

	_, err := autocommitExec(executor, "insert into user(id, v, name) values (1, 2, 'myname')")
	require.NoError(t, err)

	assertQueries(t, sbclookup, []*querypb.BoundQuery{{
		Sql: "insert into name_user_map(`name`, user_id) values (:name_0, :user_id_0)",
		BindVariables: map[string]*querypb.BindVariable{
			"name_0":    sqltypes.StringBindVariable("myname"),
			"user_id_0": sqltypes.Uint64BindVariable(1),
		},
	}})
	testCommitCount(t, "sbclookup", sbclookup, 1)

	assertQueries(t, sbc1, []*querypb.BoundQuery{{
		Sql: "insert into `user`(id, v, `name`) values (:_Id_0, 2, :_name_0)",
		BindVariables: map[string]*querypb.BindVariable{
			"_Id_0":   sqltypes.Int64BindVariable(1),
			"_name_0": sqltypes.StringBindVariable("myname"),
			"__seq0":  sqltypes.Int64BindVariable(1),
		},
	}})
	testCommitCount(t, "sbc1", sbc1, 1)
}

// TestAutocommitInsertShardAutoCommit: instant-commit.
func TestAutocommitInsertMultishardAutoCommit(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()

	_, err := autocommitExec(executor, "insert /*vt+ MULTI_SHARD_AUTOCOMMIT=1 */ into user_extra(user_id, v) values (1, 2), (3, 4)")
	require.NoError(t, err)

	assertQueries(t, sbc1, []*querypb.BoundQuery{{
		Sql: "insert /*vt+ MULTI_SHARD_AUTOCOMMIT=1 */ into user_extra(user_id, v) values (:_user_id_0, 2)",
		BindVariables: map[string]*querypb.BindVariable{
			"_user_id_0": sqltypes.Int64BindVariable(1),
			"_user_id_1": sqltypes.Int64BindVariable(3),
		},
	}})
	testCommitCount(t, "sbc1", sbc1, 0)

	assertQueries(t, sbc2, []*querypb.BoundQuery{{
		Sql: "insert /*vt+ MULTI_SHARD_AUTOCOMMIT=1 */ into user_extra(user_id, v) values (:_user_id_1, 4)",
		BindVariables: map[string]*querypb.BindVariable{
			"_user_id_0": sqltypes.Int64BindVariable(1),
			"_user_id_1": sqltypes.Int64BindVariable(3),
		},
	}})
	testCommitCount(t, "sbc2", sbc2, 0)

	executor, sbc1, sbc2, _ = createExecutorEnv()
	// Make the first shard fail - the second completes anyway
	sbc1.MustFailCodes[vtrpcpb.Code_INVALID_ARGUMENT] = 1
	_, err = autocommitExec(executor, "insert /*vt+ MULTI_SHARD_AUTOCOMMIT=1 */ into user_extra(user_id, v) values (1, 2), (3, 4)")
	require.Error(t, err)
	require.Contains(t, err.Error(), "INVALID_ARGUMENT", "expected invalid argument error")

	testCommitCount(t, "sbc1", sbc1, 0)

	assertQueries(t, sbc2, []*querypb.BoundQuery{{
		Sql: "insert /*vt+ MULTI_SHARD_AUTOCOMMIT=1 */ into user_extra(user_id, v) values (:_user_id_1, 4)",
		BindVariables: map[string]*querypb.BindVariable{
			"_user_id_0": sqltypes.Int64BindVariable(1),
			"_user_id_1": sqltypes.Int64BindVariable(3),
		},
	}})
	testCommitCount(t, "sbc2", sbc2, 0)

}

func TestAutocommitInsertMultishard(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()

	_, err := autocommitExec(executor, "insert into user_extra(user_id, v) values (1, 2), (3, 4)")
	require.NoError(t, err)

	assertQueries(t, sbc1, []*querypb.BoundQuery{{
		Sql: "insert into user_extra(user_id, v) values (:_user_id_0, 2)",
		BindVariables: map[string]*querypb.BindVariable{
			"_user_id_0": sqltypes.Int64BindVariable(1),
			"_user_id_1": sqltypes.Int64BindVariable(3),
		},
	}})
	testCommitCount(t, "sbc1", sbc1, 1)

	assertQueries(t, sbc2, []*querypb.BoundQuery{{
		Sql: "insert into user_extra(user_id, v) values (:_user_id_1, 4)",
		BindVariables: map[string]*querypb.BindVariable{
			"_user_id_0": sqltypes.Int64BindVariable(1),
			"_user_id_1": sqltypes.Int64BindVariable(3),
		},
	}})
	testCommitCount(t, "sbc2", sbc2, 1)
}

// TestAutocommitInsertAutoinc: instant-commit: sequence fetch is not transactional.
func TestAutocommitInsertAutoinc(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()

	_, err := autocommitExec(executor, "insert into main1(id, name) values (null, 'myname')")
	require.NoError(t, err)

	assertQueries(t, sbclookup, []*querypb.BoundQuery{{
		Sql:           "select next :n /* INT64 */ values from user_seq",
		BindVariables: map[string]*querypb.BindVariable{"n": sqltypes.Int64BindVariable(1)},
	}, {
		Sql: "insert into main1(id, `name`) values (:__seq0, 'myname')",
		BindVariables: map[string]*querypb.BindVariable{
			"__seq0": sqltypes.Int64BindVariable(1),
		},
	}})
	testCommitCount(t, "sbclookup", sbclookup, 0)
}

// TestAutocommitTransactionStarted: no instant-commit.
func TestAutocommitTransactionStarted(t *testing.T) {
	executor, sbc1, _, _ := createExecutorEnv()

	session := &vtgatepb.Session{
		TargetString:    "@primary",
		Autocommit:      true,
		InTransaction:   true,
		TransactionMode: vtgatepb.TransactionMode_MULTI,
	}

	// single shard query - no savepoint needed
	sql := "update `user` set a = 2 where id = 1"
	_, err := executor.Execute(context.Background(), "TestExecute", NewSafeSession(session), sql, map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	require.Len(t, sbc1.Queries, 1)
	require.Equal(t, sql, sbc1.Queries[0].Sql)
	testCommitCount(t, "sbc1", sbc1, 0)

	sbc1.Queries = nil
	sbc1.CommitCount.Store(0)

	// multi shard query - savepoint needed
	sql = "update `user` set a = 2 where id in (1, 4)"
	_, err = executor.Execute(context.Background(), "TestExecute", NewSafeSession(session), sql, map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	require.Len(t, sbc1.Queries, 2)
	require.Contains(t, sbc1.Queries[0].Sql, "savepoint")
	require.Equal(t, sql, sbc1.Queries[1].Sql)
	testCommitCount(t, "sbc1", sbc1, 0)
}

// TestAutocommitDirectTarget: instant-commit.
func TestAutocommitDirectTarget(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()

	session := &vtgatepb.Session{
		TargetString:    "TestUnsharded/0@primary",
		Autocommit:      true,
		TransactionMode: vtgatepb.TransactionMode_MULTI,
	}
	sql := "insert into `simple`(val) values ('val')"

	_, err := executor.Execute(context.Background(), "TestExecute", NewSafeSession(session), sql, map[string]*querypb.BindVariable{})
	require.NoError(t, err)

	assertQueries(t, sbclookup, []*querypb.BoundQuery{{
		Sql:           sql,
		BindVariables: map[string]*querypb.BindVariable{},
	}})
	testCommitCount(t, "sbclookup", sbclookup, 0)
}

// TestAutocommitDirectRangeTarget: no instant-commit.
func TestAutocommitDirectRangeTarget(t *testing.T) {
	executor, sbc1, _, _ := createExecutorEnv()

	session := &vtgatepb.Session{
		TargetString:    "TestExecutor[-]@primary",
		Autocommit:      true,
		TransactionMode: vtgatepb.TransactionMode_MULTI,
	}
	sql := "delete from sharded_user_msgs limit 1000"

	_, err := executor.Execute(context.Background(), "TestExecute", NewSafeSession(session), sql, map[string]*querypb.BindVariable{})
	require.NoError(t, err)

	assertQueries(t, sbc1, []*querypb.BoundQuery{{
		Sql:           sql,
		BindVariables: map[string]*querypb.BindVariable{},
	}})
	testCommitCount(t, "sbc1", sbc1, 1)
}

func autocommitExec(executor *Executor, sql string) (*sqltypes.Result, error) {
	session := &vtgatepb.Session{
		TargetString:    "@primary",
		Autocommit:      true,
		TransactionMode: vtgatepb.TransactionMode_MULTI,
	}

	return executor.Execute(context.Background(), "TestExecute", NewSafeSession(session), sql, map[string]*querypb.BindVariable{})
}
