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
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/config"
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/utils"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	_ "vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vttablet/sandboxconn"
)

func TestUpdateEqual(t *testing.T) {
	executor, sbc1, sbc2, sbclookup, ctx := createExecutorEnv(t)

	logChan := executor.queryLogger.Subscribe("Test")
	defer executor.queryLogger.Unsubscribe(logChan)

	// Update by primary vindex.
	session := &vtgatepb.Session{
		TargetString: "@primary",
	}
	_, err := executorExec(ctx, executor, session, "update user set a=2 where id = 1", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "update `user` set a = 2 where id = 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	assertQueries(t, sbc1, wantQueries)
	assertQueries(t, sbc2, nil)
	testQueryLog(t, executor, logChan, "TestExecute", "UPDATE", "update `user` set a = 2 where id = 1", 1)

	sbc1.Queries = nil
	_, err = executorExec(ctx, executor, session, "update user set a=2 where id = 3", nil)
	require.NoError(t, err)
	wantQueries = []*querypb.BoundQuery{{
		Sql:           "update `user` set a = 2 where id = 3",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	assertQueries(t, sbc2, wantQueries)
	assertQueries(t, sbc1, nil)

	// Update by secondary vindex.
	sbc1.Queries = nil
	sbc2.Queries = nil
	sbclookup.SetResults([]*sqltypes.Result{{}})
	_, err = executorExec(ctx, executor, session, "update music set a=2 where id = 2", nil)
	require.NoError(t, err)
	vars, err := sqltypes.BuildBindVariable([]any{sqltypes.NewInt64(2)})
	require.NoError(t, err)
	wantQueries = []*querypb.BoundQuery{{
		Sql: "select music_id, user_id from music_user_map where music_id in ::music_id for update",
		BindVariables: map[string]*querypb.BindVariable{
			"music_id": vars,
		},
	}}
	assertQueries(t, sbclookup, wantQueries)
	assertQueries(t, sbc2, nil)
	assertQueries(t, sbc1, nil)

	// Update changes lookup vindex values.
	sbc1.Queries = nil
	sbc2.Queries = nil
	sbclookup.Queries = nil
	sbc1.SetResults([]*sqltypes.Result{sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("id|name|lastname|name_lastname_keyspace_id_map", "int64|int32|varchar|int64"),
		"1|1|foo|0",
	),
	})

	_, err = executorExec(ctx, executor, session, "update user2 set `name`='myname', lastname='mylastname' where id = 1", nil)
	require.NoError(t, err)
	wantQueries = []*querypb.BoundQuery{
		{
			Sql:           "select id, `name`, lastname, `name` = 'myname' and lastname = 'mylastname' from user2 where id = 1 for update",
			BindVariables: map[string]*querypb.BindVariable{},
		},
		{
			Sql:           "update user2 set `name` = 'myname', lastname = 'mylastname' where id = 1",
			BindVariables: map[string]*querypb.BindVariable{},
		},
	}
	assertQueries(t, sbc1, wantQueries)
	assertQueries(t, sbc2, nil)

	wantQueries = []*querypb.BoundQuery{
		{
			Sql: "delete from name_lastname_keyspace_id_map where `name` = :name and lastname = :lastname and keyspace_id = :keyspace_id",
			BindVariables: map[string]*querypb.BindVariable{
				"lastname":    sqltypes.StringBindVariable("foo"),
				"name":        sqltypes.Int32BindVariable(1),
				"keyspace_id": sqltypes.BytesBindVariable([]byte("\x16k@\xb4J\xbaK\xd6")),
			},
		},
		{
			Sql: "insert into name_lastname_keyspace_id_map(`name`, lastname, keyspace_id) values (:name_0, :lastname_0, :keyspace_id_0)",
			BindVariables: map[string]*querypb.BindVariable{
				"name_0":        sqltypes.StringBindVariable("myname"),
				"lastname_0":    sqltypes.StringBindVariable("mylastname"),
				"keyspace_id_0": sqltypes.BytesBindVariable([]byte("\x16k@\xb4J\xbaK\xd6")),
			},
		},
	}

	assertQueries(t, sbclookup, wantQueries)
}

func TestUpdateFromSubQuery(t *testing.T) {
	executor, sbc1, sbc2, _, ctx := createExecutorEnv(t)

	executor.pv = querypb.ExecuteOptions_Gen4
	logChan := executor.queryLogger.Subscribe("Test")
	defer executor.queryLogger.Unsubscribe(logChan)

	fields := []*querypb.Field{
		{Name: "count(*)", Type: sqltypes.Int64},
	}
	sbc2.SetResults([]*sqltypes.Result{{
		Fields: fields,
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(4),
		}},
	}})

	// Update by primary vindex, but first execute subquery
	session := &vtgatepb.Session{
		TargetString: "@primary",
	}
	_, err := executorExec(ctx, executor, session, "update user set a=(select count(*) from user where id = 3) where id = 1", nil)
	require.NoError(t, err)
	wantQueriesSbc1 := []*querypb.BoundQuery{{
		Sql: "update `user` set a = :__sq1 where id = 1",
		BindVariables: map[string]*querypb.BindVariable{
			"__sq1": sqltypes.Int64BindVariable(4),
		},
	}}
	wantQueriesSbc2 := []*querypb.BoundQuery{{
		Sql:           "select count(*) from `user` where id = 3 lock in share mode",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	assertQueries(t, sbc1, wantQueriesSbc1)
	assertQueries(t, sbc2, wantQueriesSbc2)
	testQueryLog(t, executor, logChan, "TestExecute", "UPDATE", "update `user` set a = (select count(*) from `user` where id = 3) where id = 1", 2)
}

func TestUpdateEqualWithNoVerifyAndWriteOnlyLookupUniqueVindexes(t *testing.T) {
	res := []*sqltypes.Result{sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id|wo_lu_col|erl_lu_col|srl_lu_col|nrl_lu_col|nv_lu_col|lu_col|t2_lu_vdx",
			"int64|int64|int64|int64|int64|int64|int64|int64",
		),
		"1|2|2|2|2|2|1|0",
	)}
	executor, sbc1, sbc2, sbcLookup, ctx := createCustomExecutorSetValues(t, executorVSchema, res)

	session := &vtgatepb.Session{
		TargetString: "@primary",
	}
	_, err := executorExec(ctx, executor, session, "update t2_lookup set lu_col = 5 where wo_lu_col = 2", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{
		{
			Sql:           "select id, wo_lu_col, erl_lu_col, srl_lu_col, nrl_lu_col, nv_lu_col, lu_col, lu_col = 5 from t2_lookup where wo_lu_col = 2 for update",
			BindVariables: map[string]*querypb.BindVariable{},
		}, {
			Sql:           "update t2_lookup set lu_col = 5 where wo_lu_col = 2",
			BindVariables: map[string]*querypb.BindVariable{},
		}}

	assertQueries(t, sbc1, wantQueries)
	assertQueries(t, sbc2, wantQueries)

	bq1 := &querypb.BoundQuery{
		Sql: "delete from lu_idx where lu_col = :lu_col and keyspace_id = :keyspace_id",
		BindVariables: map[string]*querypb.BindVariable{
			"keyspace_id": sqltypes.Uint64BindVariable(1),
			"lu_col":      sqltypes.Int64BindVariable(1),
		},
	}
	bq2 := &querypb.BoundQuery{
		Sql: "insert into lu_idx(lu_col, keyspace_id) values (:lu_col_0, :keyspace_id_0)",
		BindVariables: map[string]*querypb.BindVariable{
			"keyspace_id_0": sqltypes.Uint64BindVariable(1),
			"lu_col_0":      sqltypes.Int64BindVariable(5),
		},
	}
	lookWant := []*querypb.BoundQuery{
		bq1, bq2,
		bq1, bq2,
		bq1, bq2,
		bq1, bq2,
		bq1, bq2,
		bq1, bq2,
		bq1, bq2,
		bq1, bq2,
	}
	assertQueries(t, sbcLookup, lookWant)
}

func TestUpdateInTransactionLookupDefaultReadLock(t *testing.T) {
	res := []*sqltypes.Result{sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id|wo_lu_col|erl_lu_col|srl_lu_col|nrl_lu_col|nv_lu_col|lu_col|t2_lu_vdx",
			"int64|int64|int64|int64|int64|int64|int64|int64",
		),
		"1|2|2|2|2|2|1|0",
	)}
	executor, sbc1, sbc2, sbcLookup, ctx := createCustomExecutorSetValues(t, executorVSchema, res)

	safeSession := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	_, err := executorExecSession(ctx,
		executor,
		"update t2_lookup set lu_col = 5 where nv_lu_col = 2",
		nil,
		safeSession.Session,
	)

	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{
		{
			Sql:           "select id, wo_lu_col, erl_lu_col, srl_lu_col, nrl_lu_col, nv_lu_col, lu_col, lu_col = 5 from t2_lookup where nv_lu_col = 2 and lu_col = 1 for update",
			BindVariables: map[string]*querypb.BindVariable{},
		}, {
			Sql:           "update t2_lookup set lu_col = 5 where nv_lu_col = 2",
			BindVariables: map[string]*querypb.BindVariable{},
		},
	}

	assertQueries(t, sbc1, wantQueries)
	assertQueries(t, sbc2, wantQueries)

	vars, _ := sqltypes.BuildBindVariable([]any{
		sqltypes.NewInt64(2),
	})
	bq1 := &querypb.BoundQuery{
		Sql: "select nv_lu_col, keyspace_id from nv_lu_idx where nv_lu_col in ::nv_lu_col for update",
		BindVariables: map[string]*querypb.BindVariable{
			"nv_lu_col": vars,
		},
	}
	bq2 := &querypb.BoundQuery{
		Sql: "insert into lu_idx(lu_col, keyspace_id) values (:lu_col_0, :keyspace_id_0)",
		BindVariables: map[string]*querypb.BindVariable{
			"keyspace_id_0": sqltypes.Uint64BindVariable(1),
			"lu_col_0":      sqltypes.Int64BindVariable(5),
		},
	}
	lookWant := []*querypb.BoundQuery{
		bq1, bq2,
		bq1, bq2,
		bq1, bq2,
		bq1, bq2,
		bq1, bq2,
		bq1, bq2,
		bq1, bq2,
		bq1, bq2,
	}

	assertQueries(t, sbcLookup, lookWant)
}

func TestUpdateInTransactionLookupExclusiveReadLock(t *testing.T) {
	res := []*sqltypes.Result{sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id|wo_lu_col|erl_lu_col|srl_lu_col|nrl_lu_col|nv_lu_col|lu_col|t2_lu_vdx",
			"int64|int64|int64|int64|int64|int64|int64|int64",
		),
		"1|2|2|2|2|2|1|0",
	)}
	executor, sbc1, sbc2, sbcLookup, ctx := createCustomExecutorSetValues(t, executorVSchema, res)

	safeSession := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	_, err := executorExecSession(ctx,
		executor,
		"update t2_lookup set lu_col = 5 where erl_lu_col = 2",
		nil,
		safeSession.Session,
	)

	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{
		{
			Sql:           "select id, wo_lu_col, erl_lu_col, srl_lu_col, nrl_lu_col, nv_lu_col, lu_col, lu_col = 5 from t2_lookup where nv_lu_col = 2 and lu_col = 1 for update",
			BindVariables: map[string]*querypb.BindVariable{},
		}, {
			Sql:           "update t2_lookup set lu_col = 5 where erl_lu_col = 2",
			BindVariables: map[string]*querypb.BindVariable{},
		},
	}

	assertQueries(t, sbc1, wantQueries)
	assertQueries(t, sbc2, wantQueries)

	vars, _ := sqltypes.BuildBindVariable([]any{
		sqltypes.NewInt64(2),
	})
	bq1 := &querypb.BoundQuery{
		Sql: "select erl_lu_col, keyspace_id from erl_lu_idx where erl_lu_col in ::erl_lu_col for update",
		BindVariables: map[string]*querypb.BindVariable{
			"erl_lu_col": vars,
		},
	}
	bq2 := &querypb.BoundQuery{
		Sql: "insert into lu_idx(lu_col, keyspace_id) values (:lu_col_0, :keyspace_id_0)",
		BindVariables: map[string]*querypb.BindVariable{
			"keyspace_id_0": sqltypes.Uint64BindVariable(1),
			"lu_col_0":      sqltypes.Int64BindVariable(5),
		},
	}
	lookWant := []*querypb.BoundQuery{
		bq1, bq2,
		bq1, bq2,
		bq1, bq2,
		bq1, bq2,
		bq1, bq2,
		bq1, bq2,
		bq1, bq2,
		bq1, bq2,
	}

	assertQueries(t, sbcLookup, lookWant)
}

func TestUpdateInTransactionLookupSharedReadLock(t *testing.T) {
	res := []*sqltypes.Result{sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id|wo_lu_col|erl_lu_col|srl_lu_col|nrl_lu_col|nv_lu_col|lu_col|t2_lu_vdx",
			"int64|int64|int64|int64|int64|int64|int64|int64",
		),
		"1|2|2|2|2|2|1|0",
	)}
	executor, sbc1, sbc2, sbcLookup, ctx := createCustomExecutorSetValues(t, executorVSchema, res)

	safeSession := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	_, err := executorExecSession(ctx,
		executor,
		"update t2_lookup set lu_col = 5 where srl_lu_col = 2",
		nil,
		safeSession.Session,
	)

	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{
		{
			Sql:           "select id, wo_lu_col, erl_lu_col, srl_lu_col, nrl_lu_col, nv_lu_col, lu_col, lu_col = 5 from t2_lookup where nv_lu_col = 2 and lu_col = 1 for update",
			BindVariables: map[string]*querypb.BindVariable{},
		}, {
			Sql:           "update t2_lookup set lu_col = 5 where srl_lu_col = 2",
			BindVariables: map[string]*querypb.BindVariable{},
		},
	}

	assertQueries(t, sbc1, wantQueries)
	assertQueries(t, sbc2, wantQueries)

	vars, _ := sqltypes.BuildBindVariable([]any{
		sqltypes.NewInt64(2),
	})
	bq1 := &querypb.BoundQuery{
		Sql: "select srl_lu_col, keyspace_id from srl_lu_idx where srl_lu_col in ::srl_lu_col lock in share mode",
		BindVariables: map[string]*querypb.BindVariable{
			"srl_lu_col": vars,
		},
	}
	bq2 := &querypb.BoundQuery{
		Sql: "insert into lu_idx(lu_col, keyspace_id) values (:lu_col_0, :keyspace_id_0)",
		BindVariables: map[string]*querypb.BindVariable{
			"keyspace_id_0": sqltypes.Uint64BindVariable(1),
			"lu_col_0":      sqltypes.Int64BindVariable(5),
		},
	}
	lookWant := []*querypb.BoundQuery{
		bq1, bq2,
		bq1, bq2,
		bq1, bq2,
		bq1, bq2,
		bq1, bq2,
		bq1, bq2,
		bq1, bq2,
		bq1, bq2,
	}

	assertQueries(t, sbcLookup, lookWant)
}

func TestUpdateInTransactionLookupNoReadLock(t *testing.T) {
	res := []*sqltypes.Result{sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id|wo_lu_col|erl_lu_col|srl_lu_col|nrl_lu_col|nv_lu_col|lu_col|t2_lu_vdx",
			"int64|int64|int64|int64|int64|int64|int64|int64",
		),
		"1|2|2|2|2|2|1|0",
	)}
	executor, sbc1, sbc2, sbcLookup, ctx := createCustomExecutorSetValues(t, executorVSchema, res)

	safeSession := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	_, err := executorExecSession(ctx,
		executor,
		"update t2_lookup set lu_col = 5 where nrl_lu_col = 2",
		nil,
		safeSession.Session,
	)

	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{
		{
			Sql:           "select id, wo_lu_col, erl_lu_col, srl_lu_col, nrl_lu_col, nv_lu_col, lu_col, lu_col = 5 from t2_lookup where nrl_lu_col = 2 and lu_col = 1 for update",
			BindVariables: map[string]*querypb.BindVariable{},
		}, {
			Sql:           "update t2_lookup set lu_col = 5 where nrl_lu_col = 2",
			BindVariables: map[string]*querypb.BindVariable{},
		},
	}

	assertQueries(t, sbc1, wantQueries)
	assertQueries(t, sbc2, wantQueries)

	vars, _ := sqltypes.BuildBindVariable([]any{
		sqltypes.NewInt64(2),
	})
	bq1 := &querypb.BoundQuery{
		Sql: "select nrl_lu_col, keyspace_id from nrl_lu_idx where nrl_lu_col in ::nrl_lu_col",
		BindVariables: map[string]*querypb.BindVariable{
			"nrl_lu_col": vars,
		},
	}
	bq2 := &querypb.BoundQuery{
		Sql: "insert into lu_idx(lu_col, keyspace_id) values (:lu_col_0, :keyspace_id_0)",
		BindVariables: map[string]*querypb.BindVariable{
			"keyspace_id_0": sqltypes.Uint64BindVariable(1),
			"lu_col_0":      sqltypes.Int64BindVariable(5),
		},
	}
	lookWant := []*querypb.BoundQuery{
		bq1, bq2,
		bq1, bq2,
		bq1, bq2,
		bq1, bq2,
		bq1, bq2,
		bq1, bq2,
		bq1, bq2,
		bq1, bq2,
	}

	assertQueries(t, sbcLookup, lookWant)
}

func TestUpdateMultiOwned(t *testing.T) {
	vschema := `
{
	"sharded": true,
	"vindexes": {
		"hash_index": {
			"type": "hash"
		},
		"lookup1": {
			"type": "lookup_hash_unique",
			"owner": "user",
			"params": {
				"table": "music_user_map",
				"from": "from1,from2",
				"to": "user_id"
			}
		},
		"lookup2": {
			"type": "lookup_hash_unique",
			"owner": "user",
			"params": {
				"table": "music_user_map",
				"from": "from1,from2",
				"to": "user_id"
			}
		},
		"lookup3": {
			"type": "lookup_hash_unique",
			"owner": "user",
			"params": {
				"table": "music_user_map",
				"from": "from1,from2",
				"to": "user_id"
			}
		}
	},
	"tables": {
		"user": {
			"column_vindexes": [
				{
					"column": "id",
					"name": "hash_index"
				},
				{
					"columns": ["a", "b"],
					"name": "lookup1"
				},
				{
					"columns": ["c", "d"],
					"name": "lookup2"
				},
				{
					"columns": ["e", "f"],
					"name": "lookup3"
				}
			]
		}
	}
}
`
	executor, sbc1, sbc2, sbclookup, ctx := createCustomExecutor(t, vschema, config.DefaultMySQLVersion)

	sbc1.SetResults([]*sqltypes.Result{
		sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("id|a|b|c|d|e|f|lookup1|lookup3", "int64|int64|int64|int64|int64|int64|int64|int64|int64"),
			"1|10|20|30|40|50|60|0|0",
		),
	})
	session := &vtgatepb.Session{
		TargetString: "@primary",
	}
	_, err := executorExec(ctx, executor, session, "update user set a=1, b=2, f=4, e=3 where id=1", nil)
	if err != nil {
		t.Fatal(err)
	}
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select id, a, b, c, d, e, f, a = 1 and b = 2, e = 3 and f = 4 from `user` where id = 1 for update",
		BindVariables: map[string]*querypb.BindVariable{},
	}, {
		Sql:           "update `user` set a = 1, b = 2, f = 4, e = 3 where id = 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	assertQueries(t, sbc1, wantQueries)
	assertQueries(t, sbc2, nil)

	wantQueries = []*querypb.BoundQuery{{
		Sql: "delete from music_user_map where from1 = :from1 and from2 = :from2 and user_id = :user_id",
		BindVariables: map[string]*querypb.BindVariable{
			"from1":   sqltypes.Int64BindVariable(10),
			"from2":   sqltypes.Int64BindVariable(20),
			"user_id": sqltypes.Uint64BindVariable(1),
		},
	}, {
		Sql: "insert into music_user_map(from1, from2, user_id) values (:from1_0, :from2_0, :user_id_0)",
		BindVariables: map[string]*querypb.BindVariable{
			"from1_0":   sqltypes.Int64BindVariable(1),
			"from2_0":   sqltypes.Int64BindVariable(2),
			"user_id_0": sqltypes.Uint64BindVariable(1),
		},
	}, {
		Sql: "delete from music_user_map where from1 = :from1 and from2 = :from2 and user_id = :user_id",
		BindVariables: map[string]*querypb.BindVariable{
			"from1":   sqltypes.Int64BindVariable(50),
			"from2":   sqltypes.Int64BindVariable(60),
			"user_id": sqltypes.Uint64BindVariable(1),
		},
	}, {
		Sql: "insert into music_user_map(from1, from2, user_id) values (:from1_0, :from2_0, :user_id_0)",
		BindVariables: map[string]*querypb.BindVariable{
			"from1_0":   sqltypes.Int64BindVariable(3),
			"from2_0":   sqltypes.Int64BindVariable(4),
			"user_id_0": sqltypes.Uint64BindVariable(1),
		},
	}}

	assertQueries(t, sbclookup, wantQueries)
}

func TestUpdateComments(t *testing.T) {
	executor, sbc1, sbc2, _, ctx := createExecutorEnv(t)

	session := &vtgatepb.Session{
		TargetString: "@primary",
	}
	_, err := executorExec(ctx, executor, session, "update user set a=2 where id = 1 /* trailing */", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "update `user` set a = 2 where id = 1 /* trailing */",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	assertQueries(t, sbc1, wantQueries)
	assertQueries(t, sbc2, nil)
}

func TestUpdateNormalize(t *testing.T) {
	executor, sbc1, sbc2, _, ctx := createExecutorEnv(t)

	executor.normalize = true
	session := &vtgatepb.Session{
		TargetString: "@primary",
	}
	_, err := executorExec(ctx, executor, session, "/* leading */ update user set a=2 where id = 1 /* trailing */", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql: "/* leading */ update `user` set a = :a /* INT64 */ where id = :id /* INT64 */ /* trailing */",
		BindVariables: map[string]*querypb.BindVariable{
			"a":  sqltypes.TestBindVariable(int64(2)),
			"id": sqltypes.TestBindVariable(int64(1)),
		},
	}}
	assertQueries(t, sbc1, wantQueries)
	assertQueries(t, sbc2, nil)
	sbc1.Queries = nil

	// Force the query to go to the "wrong" shard and ensure that normalization still happens
	session.TargetString = "TestExecutor/40-60"
	_, err = executorExec(ctx, executor, session, "/* leading */ update user set a=2 where id = 1 /* trailing */", nil)
	require.NoError(t, err)
	wantQueries = []*querypb.BoundQuery{{
		Sql: "/* leading */ update `user` set a = :a /* INT64 */ where id = :id /* INT64 */ /* trailing */",
		BindVariables: map[string]*querypb.BindVariable{
			"a":  sqltypes.TestBindVariable(int64(2)),
			"id": sqltypes.TestBindVariable(int64(1)),
		},
	}}
	assertQueries(t, sbc1, nil)
	assertQueries(t, sbc2, wantQueries)
	sbc2.Queries = nil
}

func TestDeleteEqual(t *testing.T) {
	executor, sbc, _, sbclookup, ctx := createExecutorEnv(t)

	sbc.SetResults([]*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "Id", Type: sqltypes.Int64},
			{Name: "name", Type: sqltypes.VarChar},
		},
		RowsAffected: 1,
		InsertID:     0,
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(1),
			sqltypes.NewVarChar("myname"),
		}},
	}})
	session := &vtgatepb.Session{
		TargetString: "@primary",
	}
	_, err := executorExec(ctx, executor, session, "delete from user where id = 1", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select Id, `name` from `user` where id = 1 for update",
		BindVariables: map[string]*querypb.BindVariable{},
	}, {
		Sql:           "delete from `user` where id = 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	assertQueries(t, sbc, wantQueries)

	wantQueries = []*querypb.BoundQuery{{
		Sql: "delete from name_user_map where `name` = :name and user_id = :user_id",
		BindVariables: map[string]*querypb.BindVariable{
			"user_id": sqltypes.Uint64BindVariable(1),
			"name":    sqltypes.ValueBindVariable(sqltypes.NewVarChar("myname")),
		},
	}}
	assertQueries(t, sbclookup, wantQueries)

	sbc.Queries = nil
	sbclookup.Queries = nil
	sbc.SetResults([]*sqltypes.Result{{}})
	_, err = executorExec(ctx, executor, session, "delete from user where id = 1", nil)
	require.NoError(t, err)
	wantQueries = []*querypb.BoundQuery{{
		Sql:           "select Id, `name` from `user` where id = 1 for update",
		BindVariables: map[string]*querypb.BindVariable{},
	}, {
		Sql:           "delete from `user` where id = 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	assertQueries(t, sbc, wantQueries)
	assertQueries(t, sbclookup, nil)

	sbc.Queries = nil
	sbclookup.Queries = nil
	sbclookup.SetResults([]*sqltypes.Result{{}})
	_, err = executorExec(ctx, executor, session, "delete from music where id = 1", nil)
	require.NoError(t, err)
	vars, err := sqltypes.BuildBindVariable([]any{sqltypes.NewInt64(1)})
	require.NoError(t, err)
	wantQueries = []*querypb.BoundQuery{{
		Sql: "select music_id, user_id from music_user_map where music_id in ::music_id for update",
		BindVariables: map[string]*querypb.BindVariable{
			"music_id": vars,
		},
	}}
	assertQueries(t, sbclookup, wantQueries)
	assertQueries(t, sbc, nil)

	sbc.Queries = nil
	sbclookup.Queries = nil
	sbclookup.SetResults([]*sqltypes.Result{{}})
	_, err = executorExec(ctx, executor, session, "delete from user_extra where user_id = 1", nil)
	require.NoError(t, err)
	wantQueries = []*querypb.BoundQuery{{
		Sql:           "delete from user_extra where user_id = 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	assertQueries(t, sbc, wantQueries)
	assertQueries(t, sbclookup, nil)

	sbc.Queries = nil
	sbclookup.Queries = nil
	sbc.SetResults([]*sqltypes.Result{sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("id|name|lastname", "int64|int32|varchar"),
		"1|1|foo",
	),
	})
	_, err = executorExec(ctx, executor, session, "delete from user2 where id = 1", nil)
	require.NoError(t, err)
	wantQueries = []*querypb.BoundQuery{
		{
			Sql:           "select id, `name`, lastname from user2 where id = 1 for update",
			BindVariables: map[string]*querypb.BindVariable{},
		},
		{
			Sql:           "delete from user2 where id = 1",
			BindVariables: map[string]*querypb.BindVariable{},
		},
	}
	assertQueries(t, sbc, wantQueries)

	wantQueries = []*querypb.BoundQuery{
		{
			Sql: "delete from name_lastname_keyspace_id_map where `name` = :name and lastname = :lastname and keyspace_id = :keyspace_id",
			BindVariables: map[string]*querypb.BindVariable{
				"lastname":    sqltypes.ValueBindVariable(sqltypes.NewVarChar("foo")),
				"name":        sqltypes.Int32BindVariable(1),
				"keyspace_id": sqltypes.BytesBindVariable([]byte("\x16k@\xb4J\xbaK\xd6")),
			},
		},
	}

	assertQueries(t, sbclookup, wantQueries)
}

func TestUpdateScatter(t *testing.T) {
	executor, sbc1, sbc2, _, ctx := createExecutorEnv(t)

	session := &vtgatepb.Session{
		TargetString: "@primary",
	}
	_, err := executorExec(ctx, executor, session, "update user_extra set col = 2", nil)
	require.NoError(t, err)
	// Queries get annotatted.
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "update user_extra set col = 2",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	assertQueries(t, sbc1, wantQueries)
	assertQueries(t, sbc2, wantQueries)
}

func TestDeleteScatter(t *testing.T) {
	executor, sbc1, sbc2, _, ctx := createExecutorEnv(t)

	session := &vtgatepb.Session{
		TargetString: "@primary",
	}
	_, err := executorExec(ctx, executor, session, "delete from user_extra", nil)
	require.NoError(t, err)
	// Queries get annotatted.
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "delete from user_extra",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	assertQueries(t, sbc1, wantQueries)
	assertQueries(t, sbc2, wantQueries)
}

func TestUpdateEqualWithMultipleLookupVindex(t *testing.T) {
	executor, sbc1, sbc2, sbcLookup, ctx := createExecutorEnv(t)

	sbcLookup.SetResults([]*sqltypes.Result{sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("lu_col|keyspace_id", "int64|varbinary"),
		"1|1",
	)})

	sbc1.SetResults([]*sqltypes.Result{sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id|wo_lu_col|erl_lu_col|srl_lu_col|nrl_lu_col|nv_lu_col|lu_col|t2_lu_vdx",
			"int64|int64|int64|int64|int64|int64|int64|int64",
		),
		"1|2|2|2|2|2|1|0",
	)})

	session := &vtgatepb.Session{
		TargetString: "@primary",
	}
	_, err := executorExec(ctx, executor, session, "update t2_lookup set lu_col = 5 where wo_lu_col = 2 and lu_col = 1", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{
		{
			Sql:           "select id, wo_lu_col, erl_lu_col, srl_lu_col, nrl_lu_col, nv_lu_col, lu_col, lu_col = 5 from t2_lookup where wo_lu_col = 2 and lu_col = 1 for update",
			BindVariables: map[string]*querypb.BindVariable{},
		}, {
			Sql:           "update t2_lookup set lu_col = 5 where wo_lu_col = 2 and lu_col = 1",
			BindVariables: map[string]*querypb.BindVariable{},
		}}

	vars, _ := sqltypes.BuildBindVariable([]any{
		sqltypes.NewInt64(1),
	})
	lookWant := []*querypb.BoundQuery{{
		Sql: "select lu_col, keyspace_id from lu_idx where lu_col in ::lu_col for update",
		BindVariables: map[string]*querypb.BindVariable{
			"lu_col": vars,
		},
	}, {
		Sql: "delete from lu_idx where lu_col = :lu_col and keyspace_id = :keyspace_id",
		BindVariables: map[string]*querypb.BindVariable{
			"keyspace_id": sqltypes.Uint64BindVariable(1),
			"lu_col":      sqltypes.Int64BindVariable(1),
		},
	}, {
		Sql: "insert into lu_idx(lu_col, keyspace_id) values (:lu_col_0, :keyspace_id_0)",
		BindVariables: map[string]*querypb.BindVariable{
			"keyspace_id_0": sqltypes.Uint64BindVariable(1),
			"lu_col_0":      sqltypes.Int64BindVariable(5),
		},
	}}
	assertQueries(t, sbcLookup, lookWant)
	assertQueries(t, sbc1, wantQueries)
	assertQueries(t, sbc2, nil)
}

func TestUpdateUseHigherCostVindexIfBackfilling(t *testing.T) {
	executor, sbc1, sbc2, sbcLookup, ctx := createExecutorEnv(t)

	sbcLookup.SetResults([]*sqltypes.Result{sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("lu_col|keyspace_id", "int64|varbinary"),
		"1|1",
		"2|1",
	)})

	sbc1.SetResults([]*sqltypes.Result{sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id|wo_lu_col|erl_lu_col|srl_lu_col|nrl_lu_col|nv_lu_col|lu_col|t2_lu_vdx",
			"int64|int64|int64|int64|int64|int64|int64|int64",
		),
		"1|2|2|2|2|2|1|0",
		"1|2|2|2|2|2|2|0",
	)})

	session := &vtgatepb.Session{
		TargetString: "@primary",
	}
	_, err := executorExec(ctx, executor, session, "update t2_lookup set lu_col = 5 where wo_lu_col = 2 and lu_col in (1, 2)", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{
		{
			Sql:           "select id, wo_lu_col, erl_lu_col, srl_lu_col, nrl_lu_col, nv_lu_col, lu_col, lu_col = 5 from t2_lookup where wo_lu_col = 2 and lu_col in (1, 2) for update",
			BindVariables: map[string]*querypb.BindVariable{},
		}, {
			Sql:           "update t2_lookup set lu_col = 5 where wo_lu_col = 2 and lu_col in (1, 2)",
			BindVariables: map[string]*querypb.BindVariable{},
		}}

	vars, _ := sqltypes.BuildBindVariable([]any{
		sqltypes.NewInt64(1),
		sqltypes.NewInt64(2),
	})
	lookWant := []*querypb.BoundQuery{{
		Sql: "select lu_col, keyspace_id from lu_idx where lu_col in ::lu_col for update",
		BindVariables: map[string]*querypb.BindVariable{
			"lu_col": vars,
		},
	}, {
		Sql: "delete from lu_idx where lu_col = :lu_col and keyspace_id = :keyspace_id",
		BindVariables: map[string]*querypb.BindVariable{
			"keyspace_id": sqltypes.Uint64BindVariable(1),
			"lu_col":      sqltypes.Int64BindVariable(1),
		},
	}, {
		Sql: "insert into lu_idx(lu_col, keyspace_id) values (:lu_col_0, :keyspace_id_0)",
		BindVariables: map[string]*querypb.BindVariable{
			"keyspace_id_0": sqltypes.Uint64BindVariable(1),
			"lu_col_0":      sqltypes.Int64BindVariable(5),
		},
	}, {
		Sql: "delete from lu_idx where lu_col = :lu_col and keyspace_id = :keyspace_id",
		BindVariables: map[string]*querypb.BindVariable{
			"keyspace_id": sqltypes.Uint64BindVariable(1),
			"lu_col":      sqltypes.Int64BindVariable(2),
		},
	}, {
		Sql: "insert into lu_idx(lu_col, keyspace_id) values (:lu_col_0, :keyspace_id_0)",
		BindVariables: map[string]*querypb.BindVariable{
			"keyspace_id_0": sqltypes.Uint64BindVariable(1),
			"lu_col_0":      sqltypes.Int64BindVariable(5),
		},
	}}
	assertQueries(t, sbcLookup, lookWant)
	assertQueries(t, sbc1, wantQueries)
	assertQueries(t, sbc2, nil)
}

func TestDeleteEqualWithNoVerifyAndWriteOnlyLookupUniqueVindex(t *testing.T) {
	res := []*sqltypes.Result{sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id|wo_lu_col|erl_lu_col|srl_lu_col|nrl_lu_col|nv_lu_col|lu_col",
			"int64|int64|int64|int64|int64|int64|int64",
		),
		"1|1|1|1|1|1|1",
	)}
	executor, sbc1, sbc2, sbcLookup, ctx := createCustomExecutorSetValues(t, executorVSchema, res)

	session := &vtgatepb.Session{
		TargetString: "@primary",
	}
	_, err := executorExec(ctx, executor, session, "delete from t2_lookup where wo_lu_col = 1", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{
		{
			Sql:           "select id, wo_lu_col, erl_lu_col, srl_lu_col, nrl_lu_col, nv_lu_col, lu_col from t2_lookup where wo_lu_col = 1 for update",
			BindVariables: map[string]*querypb.BindVariable{},
		}, {
			Sql:           "delete from t2_lookup where wo_lu_col = 1",
			BindVariables: map[string]*querypb.BindVariable{},
		}}

	bq1 := &querypb.BoundQuery{
		Sql: "delete from wo_lu_idx where wo_lu_col = :wo_lu_col and keyspace_id = :keyspace_id",
		BindVariables: map[string]*querypb.BindVariable{
			"keyspace_id": {Type: querypb.Type_VARBINARY, Value: []byte("\x16k@\xb4J\xbaK\xd6")},
			"wo_lu_col":   sqltypes.Int64BindVariable(1),
		},
	}
	bq2 := &querypb.BoundQuery{
		Sql: "delete from erl_lu_idx where erl_lu_col = :erl_lu_col and keyspace_id = :keyspace_id",
		BindVariables: map[string]*querypb.BindVariable{
			"keyspace_id": {Type: querypb.Type_VARBINARY, Value: []byte("\x16k@\xb4J\xbaK\xd6")},
			"erl_lu_col":  sqltypes.Int64BindVariable(1),
		},
	}
	bq3 := &querypb.BoundQuery{
		Sql: "delete from srl_lu_idx where srl_lu_col = :srl_lu_col and keyspace_id = :keyspace_id",
		BindVariables: map[string]*querypb.BindVariable{
			"keyspace_id": {Type: querypb.Type_VARBINARY, Value: []byte("\x16k@\xb4J\xbaK\xd6")},
			"srl_lu_col":  sqltypes.Int64BindVariable(1),
		},
	}
	bq4 := &querypb.BoundQuery{
		Sql: "delete from nrl_lu_idx where nrl_lu_col = :nrl_lu_col and keyspace_id = :keyspace_id",
		BindVariables: map[string]*querypb.BindVariable{
			"keyspace_id": {Type: querypb.Type_VARBINARY, Value: []byte("\x16k@\xb4J\xbaK\xd6")},
			"nrl_lu_col":  sqltypes.Int64BindVariable(1),
		},
	}
	bq5 := &querypb.BoundQuery{
		Sql: "delete from nv_lu_idx where nv_lu_col = :nv_lu_col and keyspace_id = :keyspace_id",
		BindVariables: map[string]*querypb.BindVariable{
			"keyspace_id": {Type: querypb.Type_VARBINARY, Value: []byte("\x16k@\xb4J\xbaK\xd6")},
			"nv_lu_col":   sqltypes.Int64BindVariable(1),
		},
	}
	bq6 := &querypb.BoundQuery{
		Sql: "delete from lu_idx where lu_col = :lu_col and keyspace_id = :keyspace_id",
		BindVariables: map[string]*querypb.BindVariable{
			"keyspace_id": sqltypes.Uint64BindVariable(1),
			"lu_col":      sqltypes.Int64BindVariable(1),
		},
	}
	lookWant := []*querypb.BoundQuery{
		bq1, bq2, bq3, bq4, bq5, bq6,
		bq1, bq2, bq3, bq4, bq5, bq6,
		bq1, bq2, bq3, bq4, bq5, bq6,
		bq1, bq2, bq3, bq4, bq5, bq6,
		bq1, bq2, bq3, bq4, bq5, bq6,
		bq1, bq2, bq3, bq4, bq5, bq6,
		bq1, bq2, bq3, bq4, bq5, bq6,
		bq1, bq2, bq3, bq4, bq5, bq6,
	}
	assertQueries(t, sbcLookup, lookWant)
	assertQueries(t, sbc1, wantQueries)
	assertQueries(t, sbc2, wantQueries)
}

func TestDeleteEqualWithMultipleLookupVindex(t *testing.T) {
	executor, sbc1, sbc2, sbcLookup, ctx := createCustomExecutorSetValues(t, executorVSchema, nil)

	sbcLookup.SetResults([]*sqltypes.Result{sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("lu_col|keyspace_id", "int64|varbinary"),
		"1|1",
	)})

	sbc1.SetResults([]*sqltypes.Result{sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id|wo_lu_col|erl_lu_col|srl_lu_col|nrl_lu_col|nv_lu_col|lu_col",
			"int64|int64|int64|int64|int64|int64|int64",
		),
		"1|1|1|1|1|1|1",
	)})

	session := &vtgatepb.Session{
		TargetString: "@primary",
	}
	_, err := executorExec(ctx, executor, session, "delete from t2_lookup where wo_lu_col = 1 and lu_col = 1", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{
		{
			Sql:           "select id, wo_lu_col, erl_lu_col, srl_lu_col, nrl_lu_col, nv_lu_col, lu_col from t2_lookup where wo_lu_col = 1 and lu_col = 1 for update",
			BindVariables: map[string]*querypb.BindVariable{},
		}, {
			Sql:           "delete from t2_lookup where wo_lu_col = 1 and lu_col = 1",
			BindVariables: map[string]*querypb.BindVariable{},
		}}

	vars, _ := sqltypes.BuildBindVariable([]any{
		sqltypes.NewInt64(1),
	})
	lookWant := []*querypb.BoundQuery{{
		Sql: "select lu_col, keyspace_id from lu_idx where lu_col in ::lu_col for update",
		BindVariables: map[string]*querypb.BindVariable{
			"lu_col": vars,
		},
	}, {
		Sql: "delete from wo_lu_idx where wo_lu_col = :wo_lu_col and keyspace_id = :keyspace_id",
		BindVariables: map[string]*querypb.BindVariable{
			"keyspace_id": {Type: querypb.Type_VARBINARY, Value: []byte("\x16k@\xb4J\xbaK\xd6")},
			"wo_lu_col":   sqltypes.Int64BindVariable(1),
		},
	}, {
		Sql: "delete from erl_lu_idx where erl_lu_col = :erl_lu_col and keyspace_id = :keyspace_id",
		BindVariables: map[string]*querypb.BindVariable{
			"keyspace_id": {Type: querypb.Type_VARBINARY, Value: []byte("\x16k@\xb4J\xbaK\xd6")},
			"erl_lu_col":  sqltypes.Int64BindVariable(1),
		},
	}, {
		Sql: "delete from srl_lu_idx where srl_lu_col = :srl_lu_col and keyspace_id = :keyspace_id",
		BindVariables: map[string]*querypb.BindVariable{
			"keyspace_id": {Type: querypb.Type_VARBINARY, Value: []byte("\x16k@\xb4J\xbaK\xd6")},
			"srl_lu_col":  sqltypes.Int64BindVariable(1),
		},
	}, {
		Sql: "delete from nrl_lu_idx where nrl_lu_col = :nrl_lu_col and keyspace_id = :keyspace_id",
		BindVariables: map[string]*querypb.BindVariable{
			"keyspace_id": {Type: querypb.Type_VARBINARY, Value: []byte("\x16k@\xb4J\xbaK\xd6")},
			"nrl_lu_col":  sqltypes.Int64BindVariable(1),
		},
	}, {
		Sql: "delete from nv_lu_idx where nv_lu_col = :nv_lu_col and keyspace_id = :keyspace_id",
		BindVariables: map[string]*querypb.BindVariable{
			"keyspace_id": {Type: querypb.Type_VARBINARY, Value: []byte("\x16k@\xb4J\xbaK\xd6")},
			"nv_lu_col":   sqltypes.Int64BindVariable(1),
		},
	}, {
		Sql: "delete from lu_idx where lu_col = :lu_col and keyspace_id = :keyspace_id",
		BindVariables: map[string]*querypb.BindVariable{
			"keyspace_id": sqltypes.Uint64BindVariable(1),
			"lu_col":      {Type: querypb.Type_INT64, Value: []byte("1")},
		},
	}}
	assertQueries(t, sbcLookup, lookWant)

	assertQueries(t, sbc1, wantQueries)
	assertQueries(t, sbc2, nil)
}

func TestDeleteUseHigherCostVindexIfBackfilling(t *testing.T) {
	executor, sbc1, sbc2, sbcLookup, ctx := createCustomExecutorSetValues(t, executorVSchema, nil)

	sbcLookup.SetResults([]*sqltypes.Result{sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("lu_col|keyspace_id", "int64|varbinary"),
		"1|1",
		"2|1",
	)})

	sbc1.SetResults([]*sqltypes.Result{sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id|wo_lu_col|erl_lu_col|srl_lu_col|nrl_lu_col|nv_lu_col|lu_col",
			"int64|int64|int64|int64|int64|int64|int64",
		),
		"1|1|1|1|1|1|1",
		"1|1|1|1|1|1|2",
	)})

	session := &vtgatepb.Session{
		TargetString: "@primary",
	}
	_, err := executorExec(ctx, executor, session, "delete from t2_lookup where wo_lu_col = 1 and lu_col in (1, 2)", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{
		{
			Sql:           "select id, wo_lu_col, erl_lu_col, srl_lu_col, nrl_lu_col, nv_lu_col, lu_col from t2_lookup where wo_lu_col = 1 and lu_col in (1, 2) for update",
			BindVariables: map[string]*querypb.BindVariable{},
		}, {
			Sql:           "delete from t2_lookup where wo_lu_col = 1 and lu_col in (1, 2)",
			BindVariables: map[string]*querypb.BindVariable{},
		}}

	vars, _ := sqltypes.BuildBindVariable([]any{
		sqltypes.NewInt64(1),
		sqltypes.NewInt64(2),
	})
	lookWant := []*querypb.BoundQuery{{
		Sql: "select lu_col, keyspace_id from lu_idx where lu_col in ::lu_col for update",
		BindVariables: map[string]*querypb.BindVariable{
			"lu_col": vars,
		},
	}, {
		Sql: "delete from wo_lu_idx where wo_lu_col = :wo_lu_col and keyspace_id = :keyspace_id",
		BindVariables: map[string]*querypb.BindVariable{
			"keyspace_id": {Type: querypb.Type_VARBINARY, Value: []byte("\x16k@\xb4J\xbaK\xd6")},
			"wo_lu_col":   sqltypes.Int64BindVariable(1),
		},
	}, {
		Sql: "delete from erl_lu_idx where erl_lu_col = :erl_lu_col and keyspace_id = :keyspace_id",
		BindVariables: map[string]*querypb.BindVariable{
			"keyspace_id": {Type: querypb.Type_VARBINARY, Value: []byte("\x16k@\xb4J\xbaK\xd6")},
			"erl_lu_col":  sqltypes.Int64BindVariable(1),
		},
	}, {
		Sql: "delete from srl_lu_idx where srl_lu_col = :srl_lu_col and keyspace_id = :keyspace_id",
		BindVariables: map[string]*querypb.BindVariable{
			"keyspace_id": {Type: querypb.Type_VARBINARY, Value: []byte("\x16k@\xb4J\xbaK\xd6")},
			"srl_lu_col":  sqltypes.Int64BindVariable(1),
		},
	}, {
		Sql: "delete from nrl_lu_idx where nrl_lu_col = :nrl_lu_col and keyspace_id = :keyspace_id",
		BindVariables: map[string]*querypb.BindVariable{
			"keyspace_id": {Type: querypb.Type_VARBINARY, Value: []byte("\x16k@\xb4J\xbaK\xd6")},
			"nrl_lu_col":  sqltypes.Int64BindVariable(1),
		},
	}, {
		Sql: "delete from nv_lu_idx where nv_lu_col = :nv_lu_col and keyspace_id = :keyspace_id",
		BindVariables: map[string]*querypb.BindVariable{
			"keyspace_id": {Type: querypb.Type_VARBINARY, Value: []byte("\x16k@\xb4J\xbaK\xd6")},
			"nv_lu_col":   sqltypes.Int64BindVariable(1),
		},
	}, {
		Sql: "delete from lu_idx where lu_col = :lu_col and keyspace_id = :keyspace_id",
		BindVariables: map[string]*querypb.BindVariable{
			"keyspace_id": sqltypes.Uint64BindVariable(1),
			"lu_col":      sqltypes.Int64BindVariable(1),
		},
	}, {
		Sql: "delete from wo_lu_idx where wo_lu_col = :wo_lu_col and keyspace_id = :keyspace_id",
		BindVariables: map[string]*querypb.BindVariable{
			"keyspace_id": {Type: querypb.Type_VARBINARY, Value: []byte("\x16k@\xb4J\xbaK\xd6")},
			"wo_lu_col":   sqltypes.Int64BindVariable(1),
		},
	}, {
		Sql: "delete from erl_lu_idx where erl_lu_col = :erl_lu_col and keyspace_id = :keyspace_id",
		BindVariables: map[string]*querypb.BindVariable{
			"keyspace_id": {Type: querypb.Type_VARBINARY, Value: []byte("\x16k@\xb4J\xbaK\xd6")},
			"erl_lu_col":  sqltypes.Int64BindVariable(1),
		},
	}, {
		Sql: "delete from srl_lu_idx where srl_lu_col = :srl_lu_col and keyspace_id = :keyspace_id",
		BindVariables: map[string]*querypb.BindVariable{
			"keyspace_id": {Type: querypb.Type_VARBINARY, Value: []byte("\x16k@\xb4J\xbaK\xd6")},
			"srl_lu_col":  sqltypes.Int64BindVariable(1),
		},
	}, {
		Sql: "delete from nrl_lu_idx where nrl_lu_col = :nrl_lu_col and keyspace_id = :keyspace_id",
		BindVariables: map[string]*querypb.BindVariable{
			"keyspace_id": {Type: querypb.Type_VARBINARY, Value: []byte("\x16k@\xb4J\xbaK\xd6")},
			"nrl_lu_col":  sqltypes.Int64BindVariable(1),
		},
	}, {
		Sql: "delete from nv_lu_idx where nv_lu_col = :nv_lu_col and keyspace_id = :keyspace_id",
		BindVariables: map[string]*querypb.BindVariable{
			"keyspace_id": {Type: querypb.Type_VARBINARY, Value: []byte("\x16k@\xb4J\xbaK\xd6")},
			"nv_lu_col":   sqltypes.Int64BindVariable(1),
		},
	}, {
		Sql: "delete from lu_idx where lu_col = :lu_col and keyspace_id = :keyspace_id",
		BindVariables: map[string]*querypb.BindVariable{
			"keyspace_id": sqltypes.Uint64BindVariable(1),
			"lu_col":      sqltypes.Int64BindVariable(2),
		},
	}}
	assertQueries(t, sbcLookup, lookWant)

	assertQueries(t, sbc1, wantQueries)
	assertQueries(t, sbc2, nil)
}

func TestDeleteByDestination(t *testing.T) {
	executor, sbc1, sbc2, _, ctx := createExecutorEnv(t)

	session := &vtgatepb.Session{
		TargetString: "@primary",
	}
	_, err := executorExec(ctx, executor, session, "delete from `TestExecutor[-]`.user_extra limit 10", nil)
	require.NoError(t, err)
	// Queries get annotatted.
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "delete from user_extra limit 10",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	assertQueries(t, sbc1, wantQueries)
	assertQueries(t, sbc2, wantQueries)
}

func TestDeleteComments(t *testing.T) {
	executor, sbc, _, sbclookup, ctx := createExecutorEnv(t)

	sbc.SetResults([]*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "Id", Type: sqltypes.Int64},
			{Name: "name", Type: sqltypes.VarChar},
		},
		RowsAffected: 1,
		InsertID:     0,
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(1),
			sqltypes.NewVarChar("myname"),
		}},
	}})
	session := &vtgatepb.Session{
		TargetString: "@primary",
	}
	_, err := executorExec(ctx, executor, session, "delete from user where id = 1 /* trailing */", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select Id, `name` from `user` where id = 1 for update /* trailing */",
		BindVariables: map[string]*querypb.BindVariable{},
	}, {
		Sql:           "delete from `user` where id = 1 /* trailing */",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	assertQueries(t, sbc, wantQueries)

	wantQueries = []*querypb.BoundQuery{{
		Sql: "delete from name_user_map where `name` = :name and user_id = :user_id /* trailing */",
		BindVariables: map[string]*querypb.BindVariable{
			"user_id": sqltypes.Uint64BindVariable(1),
			"name":    sqltypes.ValueBindVariable(sqltypes.NewVarChar("myname")),
		},
	}}
	assertQueries(t, sbclookup, wantQueries)
}

func TestInsertSharded(t *testing.T) {
	executor, sbc1, sbc2, sbclookup, ctx := createExecutorEnv(t)

	logChan := executor.queryLogger.Subscribe("Test")
	defer executor.queryLogger.Unsubscribe(logChan)

	session := &vtgatepb.Session{
		TargetString: "@primary",
	}
	_, err := executorExec(ctx, executor, session, "insert into user(id, v, name) values (1, 2, 'myname')", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql: "insert into `user`(id, v, `name`) values (:_Id_0, 2, :_name_0)",
		BindVariables: map[string]*querypb.BindVariable{
			"_Id_0":   sqltypes.Int64BindVariable(1),
			"_name_0": sqltypes.StringBindVariable("myname"),
		},
	}}
	assertQueries(t, sbc1, wantQueries)
	assertQueries(t, sbc2, nil)
	wantQueries = []*querypb.BoundQuery{{
		Sql: "insert into name_user_map(`name`, user_id) values (:name_0, :user_id_0)",
		BindVariables: map[string]*querypb.BindVariable{
			"name_0":    sqltypes.StringBindVariable("myname"),
			"user_id_0": sqltypes.Uint64BindVariable(1),
		},
	}}
	assertQueries(t, sbclookup, wantQueries)

	testQueryLog(t, executor, logChan, "MarkSavepoint", "SAVEPOINT", "savepoint x", 0)
	testQueryLog(t, executor, logChan, "VindexCreate", "INSERT", "insert into name_user_map(`name`, user_id) values (:name_0, :user_id_0)", 1)
	testQueryLog(t, executor, logChan, "TestExecute", "INSERT", "insert into `user`(id, v, `name`) values (1, 2, 'myname')", 1)

	sbc1.Queries = nil
	sbclookup.Queries = nil
	_, err = executorExec(ctx, executor, session, "insert into user(id, v, name) values (3, 2, 'myname2')", nil)
	require.NoError(t, err)
	wantQueries = []*querypb.BoundQuery{{
		Sql: "insert into `user`(id, v, `name`) values (:_Id_0, 2, :_name_0)",
		BindVariables: map[string]*querypb.BindVariable{
			"_Id_0":   sqltypes.Int64BindVariable(3),
			"_name_0": sqltypes.StringBindVariable("myname2"),
		},
	}}
	assertQueries(t, sbc2, wantQueries)
	assertQueries(t, sbc1, nil)
	wantQueries = []*querypb.BoundQuery{{
		Sql: "insert into name_user_map(`name`, user_id) values (:name_0, :user_id_0)",
		BindVariables: map[string]*querypb.BindVariable{
			"name_0":    sqltypes.StringBindVariable("myname2"),
			"user_id_0": sqltypes.Uint64BindVariable(3),
		},
	}}
	assertQueries(t, sbclookup, wantQueries)
	testQueryLog(t, executor, logChan, "MarkSavepoint", "SAVEPOINT", "savepoint x", 2)
	testQueryLog(t, executor, logChan, "VindexCreate", "INSERT", "insert into name_user_map(`name`, user_id) values (:name_0, :user_id_0)", 1)
	testQueryLog(t, executor, logChan, "TestExecute", "INSERT", "insert into `user`(id, v, `name`) values (3, 2, 'myname2')", 1)

	sbc1.Queries = nil
	_, err = executorExec(ctx, executor, session, "insert into user2(id, name, lastname) values (2, 'myname', 'mylastname')", nil)
	require.NoError(t, err)
	wantQueries = []*querypb.BoundQuery{{
		Sql: "insert into user2(id, `name`, lastname) values (:_id_0, :_name_0, :_lastname_0)",
		BindVariables: map[string]*querypb.BindVariable{
			"_id_0":       sqltypes.Int64BindVariable(2),
			"_name_0":     sqltypes.StringBindVariable("myname"),
			"_lastname_0": sqltypes.StringBindVariable("mylastname"),
		},
	}}
	assertQueries(t, sbc1, wantQueries)
	testQueryLog(t, executor, logChan, "MarkSavepoint", "SAVEPOINT", "savepoint x", 3)
	testQueryLog(t, executor, logChan, "VindexCreate", "INSERT", "insert into name_lastname_keyspace_id_map(`name`, lastname, keyspace_id) values (:name_0, :lastname_0, :keyspace_id_0)", 1)
	testQueryLog(t, executor, logChan, "TestExecute", "INSERT", "insert into user2(id, `name`, lastname) values (2, 'myname', 'mylastname')", 1)

	// insert with binary values
	executor.normalize = true
	sbc1.Queries = nil
	sbc2.Queries = nil
	sbclookup.Queries = nil
	_, err = executorExec(ctx, executor, session, "insert into user(id, v, name) values (1, 2, _binary 'myname')", nil)
	require.NoError(t, err)
	wantQueries = []*querypb.BoundQuery{{
		Sql: "insert into `user`(id, v, `name`) values (:_Id_0, :vtg2 /* INT64 */, :_name_0)",
		BindVariables: map[string]*querypb.BindVariable{
			"_Id_0":   sqltypes.Int64BindVariable(1),
			"_name_0": sqltypes.BytesBindVariable([]byte("myname")),
			"vtg2":    sqltypes.Int64BindVariable(2),
		},
	}}
	assertQueries(t, sbc1, wantQueries)
	assertQueries(t, sbc2, nil)
	wantQueries = []*querypb.BoundQuery{{
		Sql: "insert into name_user_map(`name`, user_id) values (:name_0, :user_id_0)",
		BindVariables: map[string]*querypb.BindVariable{
			"name_0":    sqltypes.BytesBindVariable([]byte("myname")),
			"user_id_0": sqltypes.Uint64BindVariable(1),
		},
	}}
	assertQueries(t, sbclookup, wantQueries)

	testQueryLog(t, executor, logChan, "MarkSavepoint", "SAVEPOINT", "savepoint x", 3)
	testQueryLog(t, executor, logChan, "VindexCreate", "INSERT", "insert into name_user_map(`name`, user_id) values (:name_0, :user_id_0)", 1)
	testQueryLog(t, executor, logChan, "TestExecute", "INSERT", "insert into `user`(id, v, `name`) values (:vtg1 /* INT64 */, :vtg2 /* INT64 */, _binary :vtg3 /* VARCHAR */)", 1)
}

func TestInsertNegativeValue(t *testing.T) {
	executor, sbc1, sbc2, sbclookup, ctx := createExecutorEnv(t)
	executor.normalize = true

	logChan := executor.queryLogger.Subscribe("Test")
	defer executor.queryLogger.Unsubscribe(logChan)

	session := &vtgatepb.Session{
		TargetString: "@primary",
	}
	_, err := executorExec(ctx, executor, session, "insert into user(id, v, name) values (1, -2, 'myname')", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql: "insert into `user`(id, v, `name`) values (:_Id_0, -:vtg2 /* INT64 */, :_name_0)",
		BindVariables: map[string]*querypb.BindVariable{
			"_Id_0":   sqltypes.Int64BindVariable(1),
			"vtg2":    sqltypes.Int64BindVariable(2),
			"_name_0": sqltypes.StringBindVariable("myname"),
		},
	}}
	assertQueries(t, sbc1, wantQueries)
	assertQueries(t, sbc2, nil)
	wantQueries = []*querypb.BoundQuery{{
		Sql: "insert into name_user_map(`name`, user_id) values (:name_0, :user_id_0)",
		BindVariables: map[string]*querypb.BindVariable{
			"name_0":    sqltypes.StringBindVariable("myname"),
			"user_id_0": sqltypes.Uint64BindVariable(1),
		},
	}}
	assertQueries(t, sbclookup, wantQueries)

	testQueryLog(t, executor, logChan, "MarkSavepoint", "SAVEPOINT", "savepoint x", 0)
	testQueryLog(t, executor, logChan, "VindexCreate", "INSERT", "insert into name_user_map(`name`, user_id) values (:name_0, :user_id_0)", 1)
	testQueryLog(t, executor, logChan, "TestExecute", "INSERT", "insert into `user`(id, v, `name`) values (:vtg1 /* INT64 */, -:vtg2 /* INT64 */, :vtg3 /* VARCHAR */)", 1)
}

func TestInsertShardedKeyrange(t *testing.T) {
	executor, _, _, _, ctx := createExecutorEnv(t)

	// If a unique vindex returns a keyrange, we fail the insert
	session := &vtgatepb.Session{
		TargetString: "@primary",
	}
	_, err := executorExec(ctx, executor, session, "insert into keyrange_table(krcol_unique, krcol) values(1, 1)", nil)
	require.EqualError(t, err, "VT09024: could not map [INT64(1)] to a unique keyspace id: DestinationKeyRange(-10)")
}

func TestInsertShardedAutocommitLookup(t *testing.T) {

	vschema := `
{
	"sharded": true,
	"vindexes": {
		"hash_index": {
			"type": "hash"
		},
		"name_user_map": {
			"type": "lookup_hash",
			"owner": "user",
			"params": {
				"table": "name_user_map",
				"from": "name",
				"to": "user_id",
				"autocommit": "true"
			}
		},
		"music_user_map": {
			"type": "lookup_hash",
			"owner": "user",
			"params": {
				"table": "music_user_map",
				"from": "music",
				"to": "user_id",
				"multi_shard_autocommit": "true"
			}
		}
	},
	"tables": {
		"user": {
			"column_vindexes": [
				{
					"column": "Id",
					"name": "hash_index"
				},
				{
					"column": "name",
					"name": "name_user_map"
				},
				{
					"column": "music",
					"name": "music_user_map"
				}
			],
			"auto_increment": {
				"column": "id",
				"sequence": "user_seq"
			},
			"columns": [
				{
					"name": "textcol",
					"type": "VARCHAR"
				}
			]
		}
	}
}
`
	executor, sbc1, sbc2, sbclookup, ctx := createCustomExecutor(t, vschema, config.DefaultMySQLVersion)

	_, err := executorExecSession(ctx, executor, "insert into user(id, v, name, music) values (1, 2, 'myname', 'star')", nil, &vtgatepb.Session{})
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql: "insert into `user`(id, v, `name`, music) values (:_Id_0, 2, :_name_0, :_music_0)",
		BindVariables: map[string]*querypb.BindVariable{
			"_Id_0":    sqltypes.Int64BindVariable(1),
			"_music_0": sqltypes.StringBindVariable("star"),
			"_name_0":  sqltypes.StringBindVariable("myname"),
		},
	}}
	assertQueries(t, sbc1, wantQueries)
	assertQueries(t, sbc2, nil)
	wantQueries = []*querypb.BoundQuery{{
		Sql: "insert into name_user_map(`name`, user_id) values (:name_0, :user_id_0) on duplicate key update `name` = values(`name`), user_id = values(user_id)",
		BindVariables: map[string]*querypb.BindVariable{
			"name_0":    sqltypes.StringBindVariable("myname"),
			"user_id_0": sqltypes.Uint64BindVariable(1),
		},
	}, {
		Sql: "insert /*vt+ MULTI_SHARD_AUTOCOMMIT=1 */ into music_user_map(music, user_id) values (:music_0, :user_id_0) on duplicate key update music = values(music), user_id = values(user_id)",
		BindVariables: map[string]*querypb.BindVariable{
			"music_0":   sqltypes.StringBindVariable("star"),
			"user_id_0": sqltypes.Uint64BindVariable(1),
		},
	}}
	// autocommit should go as ExecuteBatch
	assertQueries(t, sbclookup, wantQueries)
}

func TestInsertShardedIgnore(t *testing.T) {
	executor, sbc1, sbc2, sbclookup, ctx := createExecutorEnv(t)

	int1 := sqltypes.Int64BindVariable(1)
	int2 := sqltypes.Int64BindVariable(2)
	int3 := sqltypes.Int64BindVariable(3)
	int4 := sqltypes.Int64BindVariable(4)
	int5 := sqltypes.Int64BindVariable(5)
	int6 := sqltypes.Int64BindVariable(6)
	uint1 := sqltypes.Uint64BindVariable(1)
	uint3 := sqltypes.Uint64BindVariable(3)

	var1 := &querypb.BindVariable{Type: querypb.Type_TUPLE,
		Values: []*querypb.Value{{Type: int1.Type, Value: int1.Value}},
	}
	var2 := &querypb.BindVariable{Type: querypb.Type_TUPLE,
		Values: []*querypb.Value{{Type: int2.Type, Value: int2.Value}},
	}
	var3 := &querypb.BindVariable{Type: querypb.Type_TUPLE,
		Values: []*querypb.Value{{Type: int3.Type, Value: int3.Value}},
	}
	var4 := &querypb.BindVariable{Type: querypb.Type_TUPLE,
		Values: []*querypb.Value{{Type: int4.Type, Value: int4.Value}},
	}
	var5 := &querypb.BindVariable{Type: querypb.Type_TUPLE,
		Values: []*querypb.Value{{Type: int5.Type, Value: int5.Value}},
	}
	var6 := &querypb.BindVariable{Type: querypb.Type_TUPLE,
		Values: []*querypb.Value{{Type: int6.Type, Value: int6.Value}},
	}
	fields := sqltypes.MakeTestFields("b|a", "int64|int64")
	field := sqltypes.MakeTestFields("a", "int64")
	tcases := []struct {
		query string
		input []*sqltypes.Result

		expectedQueries [3][]*querypb.BoundQuery
		errString       string
	}{{
		// First row: first shard.
		query: "insert ignore into insert_ignore_test(pv, owned, verify) values (1, 1, 1)",
		input: []*sqltypes.Result{
			// select music_id
			sqltypes.MakeTestResult(fields, "1|1"),
			// insert ins_lookup 1
			sqltypes.MakeTestResult(nil),
			// select ins_lookup 1
			sqltypes.MakeTestResult(field, "1"),
		},
		expectedQueries: [3][]*querypb.BoundQuery{
			{{
				Sql:           "insert ignore into insert_ignore_test(pv, owned, verify) values (:_pv_0, :_owned_0, :_verify_0)",
				BindVariables: map[string]*querypb.BindVariable{"_pv_0": int1, "_owned_0": int1, "_verify_0": int1},
			}},
			nil,
			{{
				Sql:           "select music_id, user_id from music_user_map where music_id in ::music_id for update",
				BindVariables: map[string]*querypb.BindVariable{"music_id": var1},
			}, {
				Sql:           "insert ignore into ins_lookup(fromcol, tocol) values (:fromcol_0, :tocol_0)",
				BindVariables: map[string]*querypb.BindVariable{"fromcol_0": int1, "tocol_0": uint1},
			}, {
				Sql:           "select fromcol from ins_lookup where fromcol = :fromcol and tocol = :tocol",
				BindVariables: map[string]*querypb.BindVariable{"fromcol": int1, "tocol": uint1},
			}},
		},
	}, {
		// Second row: will fail because primary vindex will fail to map.
		query: "insert ignore into insert_ignore_test(pv, owned, verify) values (2, 2, 2)",
		input: []*sqltypes.Result{
			// select music_id
			sqltypes.MakeTestResult(fields),
		},
		expectedQueries: [3][]*querypb.BoundQuery{
			nil,
			nil,
			{{
				Sql:           "select music_id, user_id from music_user_map where music_id in ::music_id for update",
				BindVariables: map[string]*querypb.BindVariable{"music_id": var2},
			}},
		},
		errString: "could not map [INT64(2)] to a keyspace id",
	}, {
		// Third row: will fail because verification will fail on owned vindex after Create.
		query: "insert ignore into insert_ignore_test(pv, owned, verify) values (3, 3, 1)",
		input: []*sqltypes.Result{
			// select music_id
			sqltypes.MakeTestResult(fields, "3|1"),
			// insert ins_lookup 3
			sqltypes.MakeTestResult(nil),
			// select ins_lookup 3
			sqltypes.MakeTestResult(field),
		},
		expectedQueries: [3][]*querypb.BoundQuery{
			nil,
			nil,
			{{
				Sql:           "select music_id, user_id from music_user_map where music_id in ::music_id for update",
				BindVariables: map[string]*querypb.BindVariable{"music_id": var3},
			}, {
				Sql:           "insert ignore into ins_lookup(fromcol, tocol) values (:fromcol_0, :tocol_0)",
				BindVariables: map[string]*querypb.BindVariable{"fromcol_0": int3, "tocol_0": uint1},
			}, {
				Sql:           "select fromcol from ins_lookup where fromcol = :fromcol and tocol = :tocol",
				BindVariables: map[string]*querypb.BindVariable{"fromcol": int3, "tocol": uint1},
			}},
		},
	}, {
		// Fourth row: will fail because verification will fail on unowned hash vindex.
		query: "insert ignore into insert_ignore_test(pv, owned, verify) values (4, 4, 4)",
		input: []*sqltypes.Result{
			// select music_id
			sqltypes.MakeTestResult(fields, "4|1"),
			// insert ins_lookup 4
			sqltypes.MakeTestResult(nil),
			// select ins_lookup 4
			sqltypes.MakeTestResult(field, "4"),
			sqltypes.MakeTestResult(nil),
		},
		expectedQueries: [3][]*querypb.BoundQuery{
			nil,
			nil,
			{{
				Sql:           "select music_id, user_id from music_user_map where music_id in ::music_id for update",
				BindVariables: map[string]*querypb.BindVariable{"music_id": var4},
			}, {
				Sql:           "insert ignore into ins_lookup(fromcol, tocol) values (:fromcol_0, :tocol_0)",
				BindVariables: map[string]*querypb.BindVariable{"fromcol_0": int4, "tocol_0": uint1},
			}, {
				Sql:           "select fromcol from ins_lookup where fromcol = :fromcol and tocol = :tocol",
				BindVariables: map[string]*querypb.BindVariable{"fromcol": int4, "tocol": uint1},
			}},
		},
	}, {
		// Fifth row: first shard.
		query: "insert ignore into insert_ignore_test(pv, owned, verify) values (5, 5, 1)",
		input: []*sqltypes.Result{
			// select music_id
			sqltypes.MakeTestResult(fields, "5|1"),
			// select ins_lookup 5
			sqltypes.MakeTestResult(field, "5"),
		},
		expectedQueries: [3][]*querypb.BoundQuery{
			{{
				Sql:           "insert ignore into insert_ignore_test(pv, owned, verify) values (:_pv_0, :_owned_0, :_verify_0)",
				BindVariables: map[string]*querypb.BindVariable{"_pv_0": int5, "_owned_0": int5, "_verify_0": int1},
			}},
			nil,
			{{
				Sql:           "select music_id, user_id from music_user_map where music_id in ::music_id for update",
				BindVariables: map[string]*querypb.BindVariable{"music_id": var5},
			}, {
				Sql:           "insert ignore into ins_lookup(fromcol, tocol) values (:fromcol_0, :tocol_0)",
				BindVariables: map[string]*querypb.BindVariable{"fromcol_0": int5, "tocol_0": uint1},
			}, {
				Sql:           "select fromcol from ins_lookup where fromcol = :fromcol and tocol = :tocol",
				BindVariables: map[string]*querypb.BindVariable{"fromcol": int5, "tocol": uint1},
			}},
		},
	}, {
		// Sixth row: second shard (because 3 hash maps to 40-60).
		query: "insert ignore into insert_ignore_test(pv, owned, verify) values (6, 6, 3)",
		input: []*sqltypes.Result{
			// select music_id
			sqltypes.MakeTestResult(fields, "6|3"),
			// select ins_lookup 6
			sqltypes.MakeTestResult(field, "6"),
		},
		expectedQueries: [3][]*querypb.BoundQuery{
			nil,
			{{
				Sql:           "insert ignore into insert_ignore_test(pv, owned, verify) values (:_pv_0, :_owned_0, :_verify_0)",
				BindVariables: map[string]*querypb.BindVariable{"_pv_0": int6, "_owned_0": int6, "_verify_0": int3},
			}},
			{{
				Sql:           "select music_id, user_id from music_user_map where music_id in ::music_id for update",
				BindVariables: map[string]*querypb.BindVariable{"music_id": var6},
			}, {
				Sql:           "insert ignore into ins_lookup(fromcol, tocol) values (:fromcol_0, :tocol_0)",
				BindVariables: map[string]*querypb.BindVariable{"fromcol_0": int6, "tocol_0": uint3},
			}, {
				Sql:           "select fromcol from ins_lookup where fromcol = :fromcol and tocol = :tocol",
				BindVariables: map[string]*querypb.BindVariable{"fromcol": int6, "tocol": uint3},
			}},
		},
	}}

	session := &vtgatepb.Session{Autocommit: true}
	for _, tcase := range tcases {
		t.Run(tcase.query, func(t *testing.T) {
			// reset
			sbc1.Queries = nil
			sbc2.Queries = nil
			sbclookup.Queries = nil

			// Build the sequence of responses for sbclookup. This should
			// match the sequence of queries we validate below.
			sbclookup.SetResults(tcase.input)
			_, err := executorExec(ctx, executor, session, tcase.query, nil)
			if tcase.errString != "" {
				require.ErrorContains(t, err, tcase.errString)
			}
			utils.MustMatch(t, tcase.expectedQueries[0], sbc1.Queries, "sbc1 queries do not match")
			utils.MustMatch(t, tcase.expectedQueries[1], sbc2.Queries, "sbc2 queries do not match")
			utils.MustMatch(t, tcase.expectedQueries[2], sbclookup.Queries, "sbclookup queries do not match")
		})
	}
}

func TestInsertOnDupKey(t *testing.T) {
	// This test just sanity checks that the statement is getting passed through
	// correctly. The full set of use cases are covered by TestInsertShardedIgnore.
	executor, sbc1, sbc2, sbclookup, ctx := createExecutorEnv(t)
	sbclookup.SetResults([]*sqltypes.Result{sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("b|a", "int64|varbinary"),
		"1|1",
	)})
	query := "insert into insert_ignore_test(pv, owned, verify) values (1, 1, 1) on duplicate key update col = 2"
	session := &vtgatepb.Session{
		TargetString: "@primary",
	}
	_, err := executorExec(ctx, executor, session, query, nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql: "insert into insert_ignore_test(pv, owned, verify) values (:_pv_0, :_owned_0, :_verify_0) on duplicate key update col = 2",
		BindVariables: map[string]*querypb.BindVariable{
			"_pv_0":     sqltypes.Int64BindVariable(1),
			"_owned_0":  sqltypes.Int64BindVariable(1),
			"_verify_0": sqltypes.Int64BindVariable(1),
		},
	}}
	assertQueries(t, sbc1, wantQueries)
	assertQueries(t, sbc2, nil)
	vars, err := sqltypes.BuildBindVariable([]any{sqltypes.NewInt64(1)})
	require.NoError(t, err)
	wantQueries = []*querypb.BoundQuery{{
		Sql: "select music_id, user_id from music_user_map where music_id in ::music_id for update",
		BindVariables: map[string]*querypb.BindVariable{
			"music_id": vars,
		},
	}, {
		Sql: "insert ignore into ins_lookup(fromcol, tocol) values (:fromcol_0, :tocol_0)",
		BindVariables: map[string]*querypb.BindVariable{
			"fromcol_0": sqltypes.Int64BindVariable(1),
			"tocol_0":   sqltypes.Uint64BindVariable(1),
		},
	}, {
		Sql: "select fromcol from ins_lookup where fromcol = :fromcol and tocol = :tocol",
		BindVariables: map[string]*querypb.BindVariable{
			"fromcol": sqltypes.Int64BindVariable(1),
			"tocol":   sqltypes.Uint64BindVariable(1),
		},
	}}
	assertQueries(t, sbclookup, wantQueries)
}

func TestAutocommitFail(t *testing.T) {
	executor, sbc1, _, _, ctx := createExecutorEnv(t)

	query := "insert into user (id) values (1)"
	sbc1.MustFailCodes[vtrpcpb.Code_ALREADY_EXISTS] = 1
	session := &vtgatepb.Session{
		TargetString: "@primary",
		Autocommit:   true,
	}

	_, err := executorExec(ctx, executor, session, query, nil)
	require.Error(t, err)

	// make sure we have closed and rolled back any transactions started
	assert.False(t, session.InTransaction, "left with tx open")
}

func TestInsertComments(t *testing.T) {
	executor, sbc1, sbc2, sbclookup, ctx := createExecutorEnv(t)

	session := &vtgatepb.Session{
		TargetString: "@primary",
	}
	_, err := executorExec(ctx, executor, session, "insert into user(id, v, name) values (1, 2, 'myname') /* trailing */", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql: "insert into `user`(id, v, `name`) values (:_Id_0, 2, :_name_0) /* trailing */",
		BindVariables: map[string]*querypb.BindVariable{
			"_Id_0":   sqltypes.Int64BindVariable(1),
			"_name_0": sqltypes.StringBindVariable("myname"),
		},
	}}
	assertQueries(t, sbc1, wantQueries)
	assertQueries(t, sbc2, nil)
	wantQueries = []*querypb.BoundQuery{{
		Sql: "insert into name_user_map(`name`, user_id) values (:name_0, :user_id_0) /* trailing */",
		BindVariables: map[string]*querypb.BindVariable{
			"name_0":    sqltypes.StringBindVariable("myname"),
			"user_id_0": sqltypes.Uint64BindVariable(1),
		},
	}}
	assertQueries(t, sbclookup, wantQueries)
}

func TestInsertGeneratorSharded(t *testing.T) {
	executor, sbc, _, sbclookup, ctx := createExecutorEnv(t)

	sbclookup.SetResults([]*sqltypes.Result{{
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(1),
		}},
		RowsAffected: 1,
		InsertID:     1,
	}})
	session := &vtgatepb.Session{
		TargetString: "@primary",
	}
	result, err := executorExec(ctx, executor, session, "insert into user(v, `name`) values (2, 'myname')", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql: "insert into `user`(v, `name`, id) values (2, :_name_0, :_Id_0)",
		BindVariables: map[string]*querypb.BindVariable{
			"_Id_0":   sqltypes.Int64BindVariable(1),
			"_name_0": sqltypes.StringBindVariable("myname"),
		},
	}}
	assertQueries(t, sbc, wantQueries)
	wantQueries = []*querypb.BoundQuery{{
		Sql:           "select next :n /* INT64 */ values from user_seq",
		BindVariables: map[string]*querypb.BindVariable{"n": sqltypes.Int64BindVariable(1)},
	}, {
		Sql: "insert into name_user_map(`name`, user_id) values (:name_0, :user_id_0)",
		BindVariables: map[string]*querypb.BindVariable{
			"name_0":    sqltypes.StringBindVariable("myname"),
			"user_id_0": sqltypes.Uint64BindVariable(1),
		},
	}}
	assertQueries(t, sbclookup, wantQueries)
	wantResult := &sqltypes.Result{
		InsertID:     1,
		RowsAffected: 1,
	}
	utils.MustMatch(t, wantResult, result)
}

func TestInsertAutoincSharded(t *testing.T) {
	router, sbc, _, _, ctx := createExecutorEnv(t)

	// Fake a mysql auto-inc response.
	wantResult := &sqltypes.Result{
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(1),
		}},
		RowsAffected: 1,
		InsertID:     2,
	}
	sbc.SetResults([]*sqltypes.Result{wantResult})
	session := &vtgatepb.Session{
		TargetString: "@primary",
	}
	result, err := executorExec(ctx, router, session, "insert into user_extra(user_id) values (2)", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql: "insert into user_extra(user_id) values (:_user_id_0)",
		BindVariables: map[string]*querypb.BindVariable{
			"_user_id_0": sqltypes.Int64BindVariable(2),
		},
	}}
	assertQueries(t, sbc, wantQueries)
	if !result.Equal(wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}
	assert.EqualValues(t, 2, session.LastInsertId)
}

func TestInsertGeneratorUnsharded(t *testing.T) {
	executor, _, _, sbclookup, ctx := createExecutorEnv(t)
	session := &vtgatepb.Session{
		TargetString: "@primary",
	}
	result, err := executorExec(ctx, executor, session, "insert into main1(id, name) values (null, 'myname')", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select next :n /* INT64 */ values from user_seq",
		BindVariables: map[string]*querypb.BindVariable{"n": sqltypes.Int64BindVariable(1)},
	}, {
		Sql: "insert into main1(id, `name`) values (:__seq0, 'myname')",
		BindVariables: map[string]*querypb.BindVariable{
			"__seq0": sqltypes.Int64BindVariable(1),
		},
	}}
	assertQueries(t, sbclookup, wantQueries)
	wantResult := &sqltypes.Result{
		InsertID:     1,
		RowsAffected: 1,
	}
	utils.MustMatch(t, wantResult, result)
}

func TestInsertAutoincUnsharded(t *testing.T) {
	router, _, _, sbclookup, ctx := createExecutorEnv(t)

	logChan := router.queryLogger.Subscribe("Test")
	defer router.queryLogger.Unsubscribe(logChan)

	// Fake a mysql auto-inc response.
	query := "insert into `simple`(val) values ('val')"
	wantResult := &sqltypes.Result{
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(1),
		}},
		RowsAffected: 1,
		InsertID:     2,
	}
	sbclookup.SetResults([]*sqltypes.Result{wantResult})

	session := &vtgatepb.Session{
		TargetString: "@primary",
	}
	result, err := executorExec(ctx, router, session, query, nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql:           query,
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	assertQueries(t, sbclookup, wantQueries)
	assert.Equal(t, result, wantResult)

	testQueryLog(t, router, logChan, "TestExecute", "INSERT", "insert into `simple`(val) values ('val')", 1)
}

func TestInsertLookupOwned(t *testing.T) {
	executor, sbc, _, sbclookup, ctx := createExecutorEnv(t)

	session := &vtgatepb.Session{
		TargetString: "@primary",
	}
	_, err := executorExec(ctx, executor, session, "insert into music(user_id, id) values (2, 3)", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql: "insert into music(user_id, id) values (:_user_id_0, :_id_0)",
		BindVariables: map[string]*querypb.BindVariable{
			"_user_id_0": sqltypes.Int64BindVariable(2),
			"_id_0":      sqltypes.Int64BindVariable(3),
		},
	}}
	assertQueries(t, sbc, wantQueries)
	wantQueries = []*querypb.BoundQuery{{
		Sql: "insert into music_user_map(music_id, user_id) values (:music_id_0, :user_id_0)",
		BindVariables: map[string]*querypb.BindVariable{
			"music_id_0": sqltypes.Int64BindVariable(3),
			"user_id_0":  sqltypes.Uint64BindVariable(2),
		},
	}}
	assertQueries(t, sbclookup, wantQueries)
}

func TestInsertLookupOwnedGenerator(t *testing.T) {
	executor, sbc, _, sbclookup, ctx := createExecutorEnv(t)

	sbclookup.SetResults([]*sqltypes.Result{{
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(4),
		}},
		RowsAffected: 1,
		InsertID:     1,
	}})
	session := &vtgatepb.Session{
		TargetString: "@primary",
	}
	result, err := executorExec(ctx, executor, session, "insert into music(user_id) values (2)", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql: "insert into music(user_id, id) values (:_user_id_0, :_id_0)",
		BindVariables: map[string]*querypb.BindVariable{
			"_user_id_0": sqltypes.Int64BindVariable(2),
			"_id_0":      sqltypes.Int64BindVariable(4),
		},
	}}
	assertQueries(t, sbc, wantQueries)
	wantQueries = []*querypb.BoundQuery{{
		Sql:           "select next :n /* INT64 */ values from user_seq",
		BindVariables: map[string]*querypb.BindVariable{"n": sqltypes.Int64BindVariable(1)},
	}, {
		Sql: "insert into music_user_map(music_id, user_id) values (:music_id_0, :user_id_0)",
		BindVariables: map[string]*querypb.BindVariable{
			"music_id_0": sqltypes.Int64BindVariable(4),
			"user_id_0":  sqltypes.Uint64BindVariable(2),
		},
	}}
	assertQueries(t, sbclookup, wantQueries)
	wantResult := &sqltypes.Result{
		InsertID:     4,
		RowsAffected: 1,
	}
	utils.MustMatch(t, wantResult, result)
}

func TestInsertLookupUnowned(t *testing.T) {
	executor, sbc, _, sbclookup, ctx := createExecutorEnv(t)

	session := &vtgatepb.Session{
		TargetString: "@primary",
	}
	_, err := executorExec(ctx, executor, session, "insert into music_extra(user_id, music_id) values (2, 3)", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql: "insert into music_extra(user_id, music_id) values (:_user_id_0, :_music_id_0)",
		BindVariables: map[string]*querypb.BindVariable{
			"_user_id_0":  sqltypes.Int64BindVariable(2),
			"_music_id_0": sqltypes.Int64BindVariable(3),
		},
	}}
	assertQueries(t, sbc, wantQueries)
	wantQueries = []*querypb.BoundQuery{{
		Sql: "select music_id from music_user_map where music_id = :music_id and user_id = :user_id",
		BindVariables: map[string]*querypb.BindVariable{
			"music_id": sqltypes.Int64BindVariable(3),
			"user_id":  sqltypes.Uint64BindVariable(2),
		},
	}}
	assertQueries(t, sbclookup, wantQueries)
}

func TestInsertLookupUnownedUnsupplied(t *testing.T) {
	executor, sbc, _, sbclookup, ctx := createExecutorEnv(t)
	sbclookup.SetResults([]*sqltypes.Result{sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("b|a", "int64|varbinary"),
		"3|1",
	)})
	session := &vtgatepb.Session{
		TargetString: "@primary",
	}
	_, err := executorExec(ctx, executor, session, "insert into music_extra_reversed(music_id) values (3)", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql: "insert into music_extra_reversed(music_id, user_id) values (:_music_id_0, :_user_id_0)",
		BindVariables: map[string]*querypb.BindVariable{
			"_user_id_0":  sqltypes.Uint64BindVariable(1),
			"_music_id_0": sqltypes.Int64BindVariable(3),
		},
	}}
	assertQueries(t, sbc, wantQueries)
	vars, err := sqltypes.BuildBindVariable([]any{sqltypes.NewInt64(3)})
	require.NoError(t, err)
	wantQueries = []*querypb.BoundQuery{{
		Sql: "select music_id, user_id from music_user_map where music_id in ::music_id for update",
		BindVariables: map[string]*querypb.BindVariable{
			"music_id": vars,
		},
	}}
	assertQueries(t, sbclookup, wantQueries)
}

// If a statement gets broken up into two, and the first one fails,
// then an error should be returned normally.
func TestInsertPartialFail1(t *testing.T) {
	executor, _, _, sbclookup, _ := createExecutorEnv(t)

	// Make the first DML fail, there should be no rollback.
	sbclookup.MustFailCodes[vtrpcpb.Code_INVALID_ARGUMENT] = 1

	_, err := executor.Execute(
		context.Background(),
		nil,
		"TestExecute",
		NewSafeSession(&vtgatepb.Session{InTransaction: true}),
		"insert into user(id, v, name) values (1, 2, 'myname')",
		nil,
	)
	require.Error(t, err)
}

// If a statement gets broken up into two, and the second one fails
// after successful execution of the first, then the transaction must
// be rolled back due to partial execution.
func TestInsertPartialFail2(t *testing.T) {
	executor, sbc1, _, _, _ := createExecutorEnv(t)

	// Make the second DML fail, it should result in a rollback.
	sbc1.MustFailExecute[sqlparser.StmtInsert] = 1

	safeSession := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	_, err := executor.Execute(
		context.Background(),
		nil,
		"TestExecute",
		safeSession,
		"insert into user(id, v, name) values (1, 2, 'myname')",
		nil,
	)

	want := "reverted partial DML execution failure"
	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Errorf("insert first DML fail: %v, must start with %s", err, want)
	}

	assert.True(t, safeSession.InTransaction())
	wantQueries := []*querypb.BoundQuery{
		{
			Sql:           "savepoint x",
			BindVariables: map[string]*querypb.BindVariable{},
		}, {
			Sql: "insert into `user`(id, v, `name`) values (:_Id_0, 2, :_name_0)",
			BindVariables: map[string]*querypb.BindVariable{
				"_Id_0":   sqltypes.Int64BindVariable(1),
				"_name_0": sqltypes.StringBindVariable("myname"),
			},
		}, {
			Sql:           "rollback to x",
			BindVariables: map[string]*querypb.BindVariable{},
		}}
	assertQueriesWithSavepoint(t, sbc1, wantQueries)
}

func TestMultiInsertSharded(t *testing.T) {
	executor, sbc1, sbc2, sbclookup, ctx := createExecutorEnv(t)

	session := &vtgatepb.Session{
		TargetString: "@primary",
	}
	_, err := executorExec(ctx, executor, session, "insert into user(id, v, name) values (1, 1, 'myname1'),(3, 3, 'myname3')", nil)
	require.NoError(t, err)
	wantQueries1 := []*querypb.BoundQuery{{
		Sql: "insert into `user`(id, v, `name`) values (:_Id_0, 1, :_name_0)",
		BindVariables: map[string]*querypb.BindVariable{
			"_Id_0":   sqltypes.Int64BindVariable(1),
			"_name_0": sqltypes.StringBindVariable("myname1"),
		},
	}}

	wantQueries2 := []*querypb.BoundQuery{{
		Sql: "insert into `user`(id, v, `name`) values (:_Id_1, 3, :_name_1)",
		BindVariables: map[string]*querypb.BindVariable{
			"_Id_1":   sqltypes.Int64BindVariable(3),
			"_name_1": sqltypes.StringBindVariable("myname3"),
		},
	}}
	assertQueries(t, sbc1, wantQueries1)
	assertQueries(t, sbc2, wantQueries2)

	wantQueries1 = []*querypb.BoundQuery{{
		Sql: "insert into name_user_map(`name`, user_id) values (:name_0, :user_id_0), (:name_1, :user_id_1)",
		BindVariables: map[string]*querypb.BindVariable{
			"name_0":    sqltypes.StringBindVariable("myname1"),
			"user_id_0": sqltypes.Uint64BindVariable(1),
			"name_1":    sqltypes.StringBindVariable("myname3"),
			"user_id_1": sqltypes.Uint64BindVariable(3),
		},
	}}
	assertQueries(t, sbclookup, wantQueries1)

	sbc1.Queries = nil
	sbclookup.Queries = nil
	sbc2.Queries = nil
	_, err = executorExec(ctx, executor, session, "insert into user(id, v, name) values (1, 1, 'myname1'),(2, 2, 'myname2')", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql: "insert into `user`(id, v, `name`) values (:_Id_0, 1, :_name_0),(:_Id_1, 2, :_name_1)",
		BindVariables: map[string]*querypb.BindVariable{
			"_Id_0":   sqltypes.Int64BindVariable(1),
			"_name_0": sqltypes.StringBindVariable("myname1"),
			"_Id_1":   sqltypes.Int64BindVariable(2),
			"_name_1": sqltypes.StringBindVariable("myname2"),
		},
	}}

	assertQueries(t, sbc1, wantQueries)
	assertQueries(t, sbc2, nil)
	wantQueries = []*querypb.BoundQuery{{
		Sql: "insert into name_user_map(`name`, user_id) values (:name_0, :user_id_0), (:name_1, :user_id_1)",
		BindVariables: map[string]*querypb.BindVariable{
			"name_0":    sqltypes.StringBindVariable("myname1"),
			"user_id_0": sqltypes.Uint64BindVariable(1),
			"name_1":    sqltypes.StringBindVariable("myname2"),
			"user_id_1": sqltypes.Uint64BindVariable(2),
		},
	}}
	assertQueries(t, sbclookup, wantQueries)

	// Insert multiple rows in a multi column vindex
	sbc1.Queries = nil
	sbclookup.Queries = nil
	sbc2.Queries = nil
	_, err = executorExec(ctx, executor, session, "insert into user2(id, `name`, lastname) values (2, 'myname', 'mylastname'), (3, 'myname2', 'mylastname2')", nil)
	require.NoError(t, err)
	wantQueries = []*querypb.BoundQuery{{
		Sql: "insert into user2(id, `name`, lastname) values (:_id_0, :_name_0, :_lastname_0)",
		BindVariables: map[string]*querypb.BindVariable{
			"_id_0":       sqltypes.Int64BindVariable(2),
			"_name_0":     sqltypes.StringBindVariable("myname"),
			"_lastname_0": sqltypes.StringBindVariable("mylastname"),
		},
	}}
	assertQueries(t, sbc1, wantQueries)
	wantQueries = []*querypb.BoundQuery{{
		Sql: "insert into user2(id, `name`, lastname) values (:_id_1, :_name_1, :_lastname_1)",
		BindVariables: map[string]*querypb.BindVariable{
			"_id_1":       sqltypes.Int64BindVariable(3),
			"_name_1":     sqltypes.StringBindVariable("myname2"),
			"_lastname_1": sqltypes.StringBindVariable("mylastname2"),
		},
	}}
	assertQueries(t, sbc2, wantQueries)
	wantQueries = []*querypb.BoundQuery{{
		Sql: "insert into name_lastname_keyspace_id_map(`name`, lastname, keyspace_id) values (:name_0, :lastname_0, :keyspace_id_0), (:name_1, :lastname_1, :keyspace_id_1)",
		BindVariables: map[string]*querypb.BindVariable{
			"name_0":        sqltypes.StringBindVariable("myname"),
			"lastname_0":    sqltypes.StringBindVariable("mylastname"),
			"keyspace_id_0": sqltypes.BytesBindVariable([]byte("\x06\xe7\xea\"Βp\x8f")),
			"name_1":        sqltypes.StringBindVariable("myname2"),
			"lastname_1":    sqltypes.StringBindVariable("mylastname2"),
			"keyspace_id_1": sqltypes.BytesBindVariable([]byte("N\xb1\x90ɢ\xfa\x16\x9c")),
		},
	}}
	assertQueries(t, sbclookup, wantQueries)
}

func TestMultiInsertGenerator(t *testing.T) {
	executor, sbc, _, sbclookup, ctx := createExecutorEnv(t)

	sbclookup.SetResults([]*sqltypes.Result{{
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(1),
		}},
		RowsAffected: 1,
		InsertID:     1,
	}})
	session := &vtgatepb.Session{
		TargetString: "@primary",
	}
	result, err := executorExec(ctx, executor, session, "insert into music(user_id, `name`) values (:u, 'myname1'),(:u, 'myname2')", map[string]*querypb.BindVariable{"u": sqltypes.Int64BindVariable(2)})
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql: "insert into music(user_id, `name`, id) values (:_user_id_0, 'myname1', :_id_0),(:_user_id_1, 'myname2', :_id_1)",
		BindVariables: map[string]*querypb.BindVariable{
			"_id_0":      sqltypes.Int64BindVariable(1),
			"_user_id_0": sqltypes.Int64BindVariable(2),
			"_id_1":      sqltypes.Int64BindVariable(2),
			"_user_id_1": sqltypes.Int64BindVariable(2),
		},
	}}
	assertQueries(t, sbc, wantQueries)
	wantQueries = []*querypb.BoundQuery{{
		Sql:           "select next :n /* INT64 */ values from user_seq",
		BindVariables: map[string]*querypb.BindVariable{"n": sqltypes.Int64BindVariable(2)},
	}, {
		Sql: "insert into music_user_map(music_id, user_id) values (:music_id_0, :user_id_0), (:music_id_1, :user_id_1)",
		BindVariables: map[string]*querypb.BindVariable{
			"user_id_0":  sqltypes.Uint64BindVariable(2),
			"music_id_0": sqltypes.Int64BindVariable(1),
			"user_id_1":  sqltypes.Uint64BindVariable(2),
			"music_id_1": sqltypes.Int64BindVariable(2),
		},
	}}
	assertQueries(t, sbclookup, wantQueries)
	wantResult := &sqltypes.Result{
		InsertID:     1,
		RowsAffected: 1,
	}
	utils.MustMatch(t, wantResult, result)
}

func TestMultiInsertGeneratorSparse(t *testing.T) {
	executor, sbc, _, sbclookup, ctx := createExecutorEnv(t)

	sbclookup.SetResults([]*sqltypes.Result{{
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(1),
		}},
		RowsAffected: 1,
		InsertID:     1,
	}})
	session := &vtgatepb.Session{
		TargetString: "@primary",
	}
	result, err := executorExec(ctx, executor, session, "insert into music(id, user_id, name) values (NULL, :u, 'myname1'),(2, :u, 'myname2'), (NULL, :u, 'myname3')", map[string]*querypb.BindVariable{"u": sqltypes.Int64BindVariable(2)})
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql: "insert into music(id, user_id, `name`) values (:_id_0, :_user_id_0, 'myname1'),(:_id_1, :_user_id_1, 'myname2'),(:_id_2, :_user_id_2, 'myname3')",
		BindVariables: map[string]*querypb.BindVariable{
			"_id_0":      sqltypes.Int64BindVariable(1),
			"_user_id_0": sqltypes.Int64BindVariable(2),
			"_id_1":      sqltypes.Int64BindVariable(2),
			"_user_id_1": sqltypes.Int64BindVariable(2),
			"_id_2":      sqltypes.Int64BindVariable(2),
			"_user_id_2": sqltypes.Int64BindVariable(2),
		},
	}}
	assertQueries(t, sbc, wantQueries)
	wantQueries = []*querypb.BoundQuery{{
		Sql:           "select next :n /* INT64 */ values from user_seq",
		BindVariables: map[string]*querypb.BindVariable{"n": sqltypes.Int64BindVariable(2)},
	}, {
		Sql: "insert into music_user_map(music_id, user_id) values (:music_id_0, :user_id_0), (:music_id_1, :user_id_1), (:music_id_2, :user_id_2)",
		BindVariables: map[string]*querypb.BindVariable{
			"user_id_0":  sqltypes.Uint64BindVariable(2),
			"music_id_0": sqltypes.Int64BindVariable(1),
			"user_id_1":  sqltypes.Uint64BindVariable(2),
			"music_id_1": sqltypes.Int64BindVariable(2),
			"user_id_2":  sqltypes.Uint64BindVariable(2),
			"music_id_2": sqltypes.Int64BindVariable(2),
		},
	}}
	assertQueries(t, sbclookup, wantQueries)
	wantResult := &sqltypes.Result{
		InsertID:     1,
		RowsAffected: 1,
	}
	utils.MustMatch(t, wantResult, result)
}

func TestInsertBadAutoInc(t *testing.T) {
	vschema := `
{
	"sharded": true,
	"vindexes": {
		"hash_index": {
			"type": "hash"
		}
	},
	"tables": {
		"bad_auto": {
			"column_vindexes": [
				{
					"column": "id",
					"name": "hash_index"
				}
			],
			"auto_increment": {
				"column": "id",
				"sequence": "absent"
			}
		}
	}
}
`
	executor, _, _, _, ctx := createCustomExecutor(t, vschema, config.DefaultMySQLVersion)

	// If auto inc table cannot be found, the table should not be added to vschema.
	session := &vtgatepb.Session{
		TargetString: "@primary",
	}
	_, err := executorExec(ctx, executor, session, "insert into bad_auto(v, name) values (1, 'myname')", nil)
	want := "table bad_auto not found"
	if err == nil || err.Error() != want {
		t.Errorf("bad auto inc err: %v, want %v", err, want)
	}
}

func TestKeyDestRangeQuery(t *testing.T) {

	type testCase struct {
		inputQuery, targetString string
		expectedSbc1Query        string
		expectedSbc2Query        string
	}
	deleteInput := "DELETE FROM sharded_user_msgs LIMIT 1000"
	deleteOutput := "delete from sharded_user_msgs limit 1000"

	selectInput := "SELECT * FROM sharded_user_msgs LIMIT 1"
	selectOutput := "select * from sharded_user_msgs limit 1"
	updateInput := "UPDATE sharded_user_msgs set message='test' LIMIT 1"
	updateOutput := "update sharded_user_msgs set message = 'test' limit 1"
	insertInput := "INSERT INTO sharded_user_msgs(message) VALUES('test')"
	insertOutput := "insert into sharded_user_msgs(message) values ('test')"
	tests := []testCase{
		{
			inputQuery:        deleteInput,
			targetString:      "TestExecutor[-60]",
			expectedSbc1Query: deleteOutput,
			expectedSbc2Query: deleteOutput,
		},
		{
			inputQuery:        deleteInput,
			targetString:      "TestExecutor[40-60]",
			expectedSbc2Query: deleteOutput,
		},
		{
			inputQuery:        deleteInput,
			targetString:      "TestExecutor[-]",
			expectedSbc1Query: deleteOutput,
			expectedSbc2Query: deleteOutput,
		},
		{
			inputQuery:        selectInput,
			targetString:      "TestExecutor[-]",
			expectedSbc1Query: selectOutput,
			expectedSbc2Query: selectOutput,
		},
		{
			inputQuery:        updateInput,
			targetString:      "TestExecutor[-]",
			expectedSbc1Query: updateOutput,
			expectedSbc2Query: updateOutput,
		},
		{
			inputQuery:        insertInput,
			targetString:      "TestExecutor:40-60",
			expectedSbc2Query: insertOutput,
		},
		{
			inputQuery:        insertInput,
			targetString:      "TestExecutor:-20",
			expectedSbc1Query: insertOutput,
		},
	}

	for _, tc := range tests {
		t.Run(tc.targetString+" - "+tc.inputQuery, func(t *testing.T) {
			executor, sbc1, sbc2, _, ctx := createExecutorEnv(t)

			session := &vtgatepb.Session{
				TargetString: tc.targetString,
			}
			_, err := executorExec(ctx, executor, session, tc.inputQuery, nil)
			require.NoError(t, err)

			if tc.expectedSbc1Query == "" {
				require.Empty(t, sbc1.BatchQueries, "sbc1")
			} else {
				assertQueriesContain(t, tc.expectedSbc1Query, "sbc1", sbc1)
			}

			if tc.expectedSbc2Query == "" {
				require.Empty(t, sbc2.BatchQueries)
			} else {
				assertQueriesContain(t, tc.expectedSbc2Query, "sbc2", sbc2)
			}
		})
	}

	// it does not work for inserts
	executor, _, _, _, ctx := createExecutorEnv(t)
	session := &vtgatepb.Session{
		TargetString: "TestExecutor[-]",
	}
	_, err := executorExec(ctx, executor, session, insertInput, nil)

	require.EqualError(t, err, "VT03023: INSERT not supported when targeting a key range: TestExecutor[-]")
}

func assertQueriesContain(t *testing.T, sql, sbcName string, sbc *sandboxconn.SandboxConn) {
	t.Helper()
	expectedQuery := []*querypb.BoundQuery{{
		Sql:           sql,
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	assertQueries(t, sbc, expectedQuery)
}

// Prepared statement tests
func TestUpdateEqualWithPrepare(t *testing.T) {
	executor, sbc1, sbc2, sbclookup, ctx := createExecutorEnv(t)

	logChan := executor.queryLogger.Subscribe("Test")
	defer executor.queryLogger.Unsubscribe(logChan)

	session := &vtgatepb.Session{
		TargetString: "@primary",
	}
	_, err := executorPrepare(ctx, executor, session, "update music set a = :a0 where id = :id0", map[string]*querypb.BindVariable{
		"a0":  sqltypes.Int64BindVariable(3),
		"id0": sqltypes.Int64BindVariable(2),
	})
	require.NoError(t, err)

	var wantQueries []*querypb.BoundQuery

	assertQueries(t, sbclookup, wantQueries)
	assertQueries(t, sbc2, nil)
	assertQueries(t, sbc1, nil)
}
func TestInsertShardedWithPrepare(t *testing.T) {
	executor, sbc1, sbc2, sbclookup, ctx := createExecutorEnv(t)

	logChan := executor.queryLogger.Subscribe("Test")
	defer executor.queryLogger.Unsubscribe(logChan)

	session := &vtgatepb.Session{
		TargetString: "@primary",
	}
	_, err := executorPrepare(ctx, executor, session, "insert into user(id, v, name) values (:_Id0, 2, ':_name_0')", map[string]*querypb.BindVariable{
		"_Id0":    sqltypes.Int64BindVariable(1),
		"_name_0": sqltypes.BytesBindVariable([]byte("myname")),
		"__seq0":  sqltypes.Int64BindVariable(1),
	})
	require.NoError(t, err)

	var wantQueries []*querypb.BoundQuery

	assertQueries(t, sbc1, wantQueries)
	assertQueries(t, sbc2, nil)

	assertQueries(t, sbclookup, wantQueries)
}

func TestDeleteEqualWithPrepare(t *testing.T) {
	executor, sbc, _, sbclookup, ctx := createExecutorEnv(t)

	session := &vtgatepb.Session{
		TargetString: "@primary",
	}
	_, err := executorPrepare(ctx, executor, session, "delete from user where id = :id0", map[string]*querypb.BindVariable{
		"id0": sqltypes.Int64BindVariable(1),
	})
	require.NoError(t, err)

	var wantQueries []*querypb.BoundQuery

	assertQueries(t, sbc, wantQueries)

	assertQueries(t, sbclookup, wantQueries)
}

func TestUpdateLastInsertID(t *testing.T) {
	executor, sbc1, _, _, ctx := createExecutorEnv(t)

	executor.normalize = true

	sql := "update user set a = last_insert_id() where id = 1"
	session := &vtgatepb.Session{
		TargetString: "@primary",
		LastInsertId: 43,
	}
	_, err := executorExec(ctx, executor, session, sql, map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql: "update `user` set a = :__lastInsertId where id = :id /* INT64 */",
		BindVariables: map[string]*querypb.BindVariable{
			"__lastInsertId": sqltypes.Uint64BindVariable(43),
			"id":             sqltypes.Int64BindVariable(1)},
	}}

	assertQueries(t, sbc1, wantQueries)
}

func TestUpdateReference(t *testing.T) {
	executor, sbc1, sbc2, sbclookup, ctx := createExecutorEnv(t)

	logChan := executor.queryLogger.Subscribe("Test")
	defer executor.queryLogger.Unsubscribe(logChan)

	session := &vtgatepb.Session{
		TargetString: "@primary",
	}
	_, err := executorExec(ctx, executor, session, "update zip_detail set status = 'CLOSED' where id = 1", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "update zip_detail set `status` = 'CLOSED' where id = 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	assertQueries(t, sbc1, nil)
	assertQueries(t, sbc2, nil)
	assertQueries(t, sbclookup, wantQueries)

	testQueryLog(t, executor, logChan, "TestExecute", "UPDATE", "update zip_detail set `status` = 'CLOSED' where id = 1", 1)

	sbclookup.Queries = nil

	_, err = executorExec(ctx, executor, session, "update TestUnsharded.zip_detail set status = 'CLOSED' where id = 1", nil)
	require.NoError(t, err)
	wantQueries = []*querypb.BoundQuery{{
		Sql:           "update zip_detail set `status` = 'CLOSED' where id = 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	assertQueries(t, sbc1, nil)
	assertQueries(t, sbc2, nil)
	assertQueries(t, sbclookup, wantQueries)

	testQueryLog(t, executor, logChan, "TestExecute", "UPDATE",
		"update TestUnsharded.zip_detail set `status` = 'CLOSED' where id = 1", 1)

	sbclookup.Queries = nil

	_, err = executorExec(ctx, executor, session, "update TestExecutor.zip_detail set status = 'CLOSED' where id = 1", nil)
	require.NoError(t, err) // Gen4 planner can redirect the query to correct source for update when reference table is involved.
}

func TestDeleteLookupOwnedEqual(t *testing.T) {
	executor, sbc1, sbc2, _, ctx := createExecutorEnv(t)

	sbc1.SetResults([]*sqltypes.Result{
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("uniq_col|keyspace_id", "int64|varbinary"), "1|N±\u0090ɢú\u0016\u009C"),
	})
	session := &vtgatepb.Session{
		TargetString: "@primary",
	}
	_, err := executorExec(ctx, executor, session, "delete from t1 where unq_col = 1", nil)
	require.NoError(t, err)
	tupleBindVar, _ := sqltypes.BuildBindVariable([]int64{1})
	sbc1wantQueries := []*querypb.BoundQuery{{
		Sql: "select unq_col, keyspace_id from t1_lkp_idx where unq_col in ::__vals for update",
		BindVariables: map[string]*querypb.BindVariable{
			"__vals":  tupleBindVar,
			"unq_col": tupleBindVar,
		},
	}}
	sbc2wantQueries := []*querypb.BoundQuery{{
		Sql:           "select id, unq_col from t1 where unq_col = 1 for update",
		BindVariables: map[string]*querypb.BindVariable{},
	}, {
		Sql:           "delete from t1 where unq_col = 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	assertQueries(t, sbc1, sbc1wantQueries)
	assertQueries(t, sbc2, sbc2wantQueries)
}

func TestDeleteReference(t *testing.T) {
	executor, sbc1, sbc2, sbclookup, ctx := createExecutorEnv(t)

	logChan := executor.queryLogger.Subscribe("Test")
	defer executor.queryLogger.Unsubscribe(logChan)

	session := &vtgatepb.Session{
		TargetString: "@primary",
	}
	_, err := executorExec(ctx, executor, session, "delete from zip_detail where id = 1", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "delete from zip_detail where id = 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	assertQueries(t, sbc1, nil)
	assertQueries(t, sbc2, nil)
	assertQueries(t, sbclookup, wantQueries)

	testQueryLog(t, executor, logChan, "TestExecute", "DELETE", "delete from zip_detail where id = 1", 1)

	sbclookup.Queries = nil

	_, err = executorExec(ctx, executor, session, "delete from zip_detail where id = 1", nil)
	require.NoError(t, err)
	wantQueries = []*querypb.BoundQuery{{
		Sql:           "delete from zip_detail where id = 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	assertQueries(t, sbc1, nil)
	assertQueries(t, sbc2, nil)
	assertQueries(t, sbclookup, wantQueries)

	testQueryLog(t, executor, logChan, "TestExecute", "DELETE", "delete from zip_detail where id = 1", 1)

	sbclookup.Queries = nil

	_, err = executorExec(ctx, executor, session, "delete from TestExecutor.zip_detail where id = 1", nil)
	require.NoError(t, err) // Gen4 planner can redirect the query to correct source for update when reference table is involved.
}

func TestReservedConnDML(t *testing.T) {
	executor, _, _, sbc, ctx := createExecutorEnv(t)

	logChan := executor.queryLogger.Subscribe("TestReservedConnDML")
	defer executor.queryLogger.Unsubscribe(logChan)

	session := NewAutocommitSession(&vtgatepb.Session{EnableSystemSettings: true})

	_, err := executor.Execute(ctx, nil, "TestReservedConnDML", session, "use "+KsTestUnsharded, nil)
	require.NoError(t, err)

	wantQueries := []*querypb.BoundQuery{
		{Sql: "select 1 from dual where @@default_week_format != 1", BindVariables: map[string]*querypb.BindVariable{}},
	}
	sbc.SetResults([]*sqltypes.Result{
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("id", "int64"), "1"),
	})
	_, err = executor.Execute(ctx, nil, "TestReservedConnDML", session, "set default_week_format = 1", nil)
	require.NoError(t, err)
	assertQueries(t, sbc, wantQueries)

	_, err = executor.Execute(ctx, nil, "TestReservedConnDML", session, "begin", nil)
	require.NoError(t, err)

	wantQueries = append(wantQueries,
		&querypb.BoundQuery{Sql: "set default_week_format = 1", BindVariables: map[string]*querypb.BindVariable{}},
		&querypb.BoundQuery{Sql: "insert into `simple`() values ()", BindVariables: map[string]*querypb.BindVariable{}})
	_, err = executor.Execute(ctx, nil, "TestReservedConnDML", session, "insert into `simple`() values ()", nil)
	require.NoError(t, err)
	assertQueries(t, sbc, wantQueries)

	_, err = executor.Execute(ctx, nil, "TestReservedConnDML", session, "commit", nil)
	require.NoError(t, err)

	_, err = executor.Execute(ctx, nil, "TestReservedConnDML", session, "begin", nil)
	require.NoError(t, err)

	sbc.EphemeralShardErr = sqlerror.NewSQLError(sqlerror.CRServerGone, sqlerror.SSNetError, "connection gone")
	// as the first time the query fails due to connection loss i.e. reserved conn lost. It will be recreated to set statement will be executed again.
	wantQueries = append(wantQueries,
		&querypb.BoundQuery{Sql: "set default_week_format = 1", BindVariables: map[string]*querypb.BindVariable{}},
		&querypb.BoundQuery{Sql: "insert into `simple`() values ()", BindVariables: map[string]*querypb.BindVariable{}})
	_, err = executor.Execute(ctx, nil, "TestReservedConnDML", session, "insert into `simple`() values ()", nil)
	require.NoError(t, err)
	assertQueries(t, sbc, wantQueries)

	_, err = executor.Execute(ctx, nil, "TestReservedConnDML", session, "commit", nil)
	require.NoError(t, err)
}

func TestStreamingDML(t *testing.T) {
	method := "TestStreamingDML"

	executor, _, _, sbc, ctx := createExecutorEnv(t)

	logChan := executor.queryLogger.Subscribe(method)
	defer executor.queryLogger.Unsubscribe(logChan)

	session := NewAutocommitSession(&vtgatepb.Session{})

	tcases := []struct {
		query  string
		result *sqltypes.Result

		inTx        bool
		openTx      bool
		changedRows int
		commitCount int
		expQuery    []*querypb.BoundQuery
	}{{
		query: "begin",

		inTx:     true,
		expQuery: []*querypb.BoundQuery{},
	}, {
		query:  "insert into `simple`() values ()",
		result: &sqltypes.Result{RowsAffected: 1},

		inTx:        true,
		openTx:      true,
		changedRows: 1,
		expQuery: []*querypb.BoundQuery{{
			Sql:           "insert into `simple`() values ()",
			BindVariables: map[string]*querypb.BindVariable{},
		}},
	}, {
		query:  "update `simple` set name = 'V' where col = 2",
		result: &sqltypes.Result{RowsAffected: 3},

		inTx:        true,
		openTx:      true,
		changedRows: 3,
		expQuery: []*querypb.BoundQuery{{
			Sql:           "update `simple` set `name` = 'V' where col = 2",
			BindVariables: map[string]*querypb.BindVariable{},
		}},
	}, {
		query:  "delete from `simple`",
		result: &sqltypes.Result{RowsAffected: 12},

		inTx:        true,
		openTx:      true,
		changedRows: 12,
		expQuery: []*querypb.BoundQuery{{
			Sql:           "delete from `simple`",
			BindVariables: map[string]*querypb.BindVariable{},
		}},
	}, {
		query: "commit",

		commitCount: 1,
		expQuery:    []*querypb.BoundQuery{},
	}}

	var qr *sqltypes.Result
	for _, tcase := range tcases {
		sbc.Queries = nil
		sbc.SetResults([]*sqltypes.Result{tcase.result})
		err := executor.StreamExecute(ctx, nil, method, session, tcase.query, nil, func(result *sqltypes.Result) error {
			qr = result
			return nil
		})
		require.NoError(t, err)
		// should tx start
		assert.Equal(t, tcase.inTx, session.GetInTransaction())
		// open transaction
		assert.Equal(t, tcase.openTx, len(session.ShardSessions) > 0)
		// row affected as returned by result
		assert.EqualValues(t, tcase.changedRows, qr.RowsAffected)
		// match the query received on tablet
		assertQueries(t, sbc, tcase.expQuery)

		assert.EqualValues(t, tcase.commitCount, sbc.CommitCount.Load())
	}
}

func TestPartialVindexInsertQueryFailure(t *testing.T) {
	executor, sbc1, sbc2, _, ctx := createExecutorEnv(t)

	logChan := executor.queryLogger.Subscribe("Test")
	defer executor.queryLogger.Unsubscribe(logChan)

	session := NewAutocommitSession(&vtgatepb.Session{})
	require.True(t, session.GetAutocommit())
	require.False(t, session.InTransaction())

	_, err := executorExecSession(ctx, executor, "begin", nil, session.Session)
	require.NoError(t, err)
	require.True(t, session.GetAutocommit())
	require.True(t, session.InTransaction())

	// fail the second lookup insert query i.e t1_lkp_idx(3, ksid)
	sbc2.MustFailExecute[sqlparser.StmtInsert] = 1
	wantQ := []*querypb.BoundQuery{{
		Sql:           "savepoint x",
		BindVariables: map[string]*querypb.BindVariable{},
	}, {
		Sql: "insert into t1_lkp_idx(unq_col, keyspace_id) values (:_unq_col_0, :keyspace_id_0)",
		BindVariables: map[string]*querypb.BindVariable{
			"keyspace_id_0": sqltypes.BytesBindVariable([]byte("\x16k@\xb4J\xbaK\xd6")),
			"_unq_col_0":    sqltypes.Int64BindVariable(1),
		},
	}, {
		Sql:           "rollback to x",
		BindVariables: map[string]*querypb.BindVariable{},
	}}

	_, err = executorExecSession(ctx, executor, "insert into t1(id, unq_col) values (1, 1), (2, 3)", nil, session.Session)
	require.Error(t, err)
	require.Contains(t, err.Error(), "reverted partial DML execution failure")
	require.True(t, session.GetAutocommit())
	require.True(t, session.InTransaction())

	assertQueriesWithSavepoint(t, sbc1, wantQ)

	// only parameter in expected query changes
	wantQ[1].Sql = "insert into t1_lkp_idx(unq_col, keyspace_id) values (:_unq_col_1, :keyspace_id_1)"
	wantQ[1].BindVariables = map[string]*querypb.BindVariable{
		"keyspace_id_1": sqltypes.BytesBindVariable([]byte("\x06\xe7\xea\"Βp\x8f")),
		"_unq_col_1":    sqltypes.Int64BindVariable(3),
	}
	assertQueriesWithSavepoint(t, sbc2, wantQ)

	testQueryLog(t, executor, logChan, "TestExecute", "BEGIN", "begin", 0)
	testQueryLog(t, executor, logChan, "MarkSavepoint", "SAVEPOINT", "savepoint x", 0)
	testQueryLog(t, executor, logChan, "VindexCreate", "SAVEPOINT_ROLLBACK", "rollback to x", 0)
	testQueryLog(t, executor, logChan, "TestExecute", "INSERT", "insert into t1(id, unq_col) values (1, 1), (2, 3)", 0)
}

func TestPartialVindexInsertQueryFailureAutoCommit(t *testing.T) {
	executor, sbc1, sbc2, _, ctx := createExecutorEnv(t)

	logChan := executor.queryLogger.Subscribe("Test")
	defer executor.queryLogger.Unsubscribe(logChan)

	session := NewAutocommitSession(&vtgatepb.Session{})
	require.True(t, session.GetAutocommit())
	require.False(t, session.InTransaction())

	// fail the second lookup insert query i.e t1_lkp_idx(3, ksid)
	sbc2.MustFailExecute[sqlparser.StmtInsert] = 1
	wantQ := []*querypb.BoundQuery{{
		Sql: "insert into t1_lkp_idx(unq_col, keyspace_id) values (:_unq_col_0, :keyspace_id_0)",
		BindVariables: map[string]*querypb.BindVariable{
			"keyspace_id_0": sqltypes.BytesBindVariable([]byte("\x16k@\xb4J\xbaK\xd6")),
			"_unq_col_0":    sqltypes.Int64BindVariable(1),
		},
	}}

	_, err := executorExecSession(ctx, executor, "insert into t1(id, unq_col) values (1, 1), (2, 3)", nil, session.Session)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "transaction rolled back to reverse changes of partial DML execution")
	assert.True(t, session.GetAutocommit())
	assert.False(t, session.InTransaction())

	assertQueriesWithSavepoint(t, sbc1, wantQ)

	// only parameter in expected query changes
	wantQ[0].Sql = "insert into t1_lkp_idx(unq_col, keyspace_id) values (:_unq_col_1, :keyspace_id_1)"
	wantQ[0].BindVariables = map[string]*querypb.BindVariable{
		"keyspace_id_1": sqltypes.BytesBindVariable([]byte("\x06\xe7\xea\"Βp\x8f")),
		"_unq_col_1":    sqltypes.Int64BindVariable(3),
	}
	assertQueriesWithSavepoint(t, sbc2, wantQ)

	testQueryLog(t, executor, logChan, "VindexCreate", "INSERT", "insert into t1_lkp_idx(unq_col, keyspace_id) values (:unq_col_0, :keyspace_id_0), (:unq_col_1, :keyspace_id_1)", 2)
	testQueryLog(t, executor, logChan, "TestExecute", "INSERT", "insert into t1(id, unq_col) values (1, 1), (2, 3)", 0)
}

// TestMultiInternalSavepoint shows that the internal savepoint created for rolling back any partial dml changes on a failure is not removed from the savepoint list.
// Any new transaction opened on a different shard will apply those savepoints as well.
// The change for it cannot be done as the executor level and will be made at the VTGate entry point.
// Test TestMultiInternalSavepointVtGate shows that it fixes the behaviour.
func TestMultiInternalSavepoint(t *testing.T) {
	executor, sbc1, sbc2, _, ctx := createExecutorEnv(t)

	session := NewAutocommitSession(&vtgatepb.Session{})
	_, err := executorExecSession(ctx, executor, "begin", nil, session.Session)
	require.NoError(t, err)

	// this query goes to multiple shards so internal savepoint will be created.
	_, err = executorExecSession(ctx, executor, "insert into user_extra(user_id) values (1), (4)", nil, session.Session)
	require.NoError(t, err)

	wantQ := []*querypb.BoundQuery{{
		Sql:           "savepoint x",
		BindVariables: map[string]*querypb.BindVariable{},
	}, {
		Sql: "insert into user_extra(user_id) values (:_user_id_0)",
		BindVariables: map[string]*querypb.BindVariable{
			"_user_id_0": sqltypes.Int64BindVariable(1),
		},
	}}
	assertQueriesWithSavepoint(t, sbc1, wantQ)
	require.Len(t, sbc2.Queries, 0)
	sbc1.Queries = nil

	_, err = executorExecSession(ctx, executor, "insert into user_extra(user_id) values (3), (6)", nil, session.Session)
	require.NoError(t, err)
	wantQ = []*querypb.BoundQuery{{
		Sql:           "savepoint x",
		BindVariables: map[string]*querypb.BindVariable{},
	}, {
		Sql:           "savepoint y",
		BindVariables: map[string]*querypb.BindVariable{},
	}, {
		Sql: "insert into user_extra(user_id) values (:_user_id_0)",
		BindVariables: map[string]*querypb.BindVariable{
			"_user_id_0": sqltypes.Int64BindVariable(3),
		},
	}}
	assertQueriesWithSavepoint(t, sbc2, wantQ)
	wantQ = []*querypb.BoundQuery{{
		Sql:           "savepoint y",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	assertQueriesWithSavepoint(t, sbc1, wantQ)
}

func TestInsertSelectFromDual(t *testing.T) {
	executor, sbc1, sbc2, sbclookup, _ := createExecutorEnv(t)

	logChan := executor.queryLogger.Subscribe("TestInsertSelect")
	defer executor.queryLogger.Unsubscribe(logChan)

	session := NewAutocommitSession(&vtgatepb.Session{})

	query := "insert into user(id, v, name) select 1, 2, 'myname' from dual"
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select 1, 2, 'myname' from dual lock in share mode",
		BindVariables: map[string]*querypb.BindVariable{},
	}, {
		Sql: "insert into `user`(id, v, `name`) values (:_c0_0, :_c0_1, :_c0_2)",
		BindVariables: map[string]*querypb.BindVariable{
			"_c0_0": sqltypes.Int64BindVariable(1),
			"_c0_1": sqltypes.Int64BindVariable(2),
			"_c0_2": sqltypes.StringBindVariable("myname"),
		},
	}}

	wantlkpQueries := []*querypb.BoundQuery{{
		Sql: "insert into name_user_map(`name`, user_id) values (:name_0, :user_id_0)",
		BindVariables: map[string]*querypb.BindVariable{
			"name_0":    sqltypes.StringBindVariable("myname"),
			"user_id_0": sqltypes.Uint64BindVariable(1),
		},
	}}

	for _, workload := range []string{"olap", "oltp"} {
		t.Run(workload, func(t *testing.T) {
			sbc1.Queries = nil
			sbc2.Queries = nil
			sbclookup.Queries = nil
			wQuery := fmt.Sprintf("set @@workload = %s", workload)
			// set result for dual query.
			sbc1.SetResults([]*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("1|2|myname", "int64|int64|varchar"), "1|2|myname")})

			_, err := executor.Execute(context.Background(), nil, "TestInsertSelect", session, wQuery, nil)
			require.NoError(t, err)

			_, err = executor.Execute(context.Background(), nil, "TestInsertSelect", session, query, nil)
			require.NoError(t, err)

			assertQueries(t, sbc1, wantQueries)
			assertQueries(t, sbc2, nil)
			assertQueries(t, sbclookup, wantlkpQueries)

			testQueryLog(t, executor, logChan, "TestInsertSelect", "SET", wQuery, 0)
			testQueryLog(t, executor, logChan, "VindexCreate", "INSERT", "insert into name_user_map(`name`, user_id) values (:name_0, :user_id_0)", 1)
			testQueryLog(t, executor, logChan, "TestInsertSelect", "INSERT", "insert into `user`(id, v, `name`) select 1, 2, 'myname' from dual", 2)
		})
	}
}

func TestInsertSelectFromTable(t *testing.T) {
	executor, sbc1, sbc2, sbclookup, _ := createExecutorEnv(t)

	logChan := executor.queryLogger.Subscribe("TestInsertSelect")
	defer executor.queryLogger.Unsubscribe(logChan)

	session := NewAutocommitSession(&vtgatepb.Session{})

	query := "insert into user(id, name) select c1, c2 from music"
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select c1, c2 from music lock in share mode",
		BindVariables: map[string]*querypb.BindVariable{},
	}, {
		Sql: "insert into `user`(id, `name`) values (:_c0_0, :_c0_1), (:_c1_0, :_c1_1), (:_c2_0, :_c2_1), (:_c3_0, :_c3_1), (:_c4_0, :_c4_1), (:_c5_0, :_c5_1), (:_c6_0, :_c6_1), (:_c7_0, :_c7_1)",
		BindVariables: map[string]*querypb.BindVariable{
			"_c0_0": sqltypes.Int32BindVariable(1), "_c0_1": sqltypes.StringBindVariable("foo"),
			"_c1_0": sqltypes.Int32BindVariable(1), "_c1_1": sqltypes.StringBindVariable("foo"),
			"_c2_0": sqltypes.Int32BindVariable(1), "_c2_1": sqltypes.StringBindVariable("foo"),
			"_c3_0": sqltypes.Int32BindVariable(1), "_c3_1": sqltypes.StringBindVariable("foo"),
			"_c4_0": sqltypes.Int32BindVariable(1), "_c4_1": sqltypes.StringBindVariable("foo"),
			"_c5_0": sqltypes.Int32BindVariable(1), "_c5_1": sqltypes.StringBindVariable("foo"),
			"_c6_0": sqltypes.Int32BindVariable(1), "_c6_1": sqltypes.StringBindVariable("foo"),
			"_c7_0": sqltypes.Int32BindVariable(1), "_c7_1": sqltypes.StringBindVariable("foo"),
		},
	}}

	wantlkpQueries := []*querypb.BoundQuery{{
		Sql: "insert into name_user_map(`name`, user_id) values (:name_0, :user_id_0), (:name_1, :user_id_1), (:name_2, :user_id_2), (:name_3, :user_id_3), (:name_4, :user_id_4), (:name_5, :user_id_5), (:name_6, :user_id_6), (:name_7, :user_id_7)",
		BindVariables: map[string]*querypb.BindVariable{
			"name_0": sqltypes.StringBindVariable("foo"), "user_id_0": sqltypes.Uint64BindVariable(1),
			"name_1": sqltypes.StringBindVariable("foo"), "user_id_1": sqltypes.Uint64BindVariable(1),
			"name_2": sqltypes.StringBindVariable("foo"), "user_id_2": sqltypes.Uint64BindVariable(1),
			"name_3": sqltypes.StringBindVariable("foo"), "user_id_3": sqltypes.Uint64BindVariable(1),
			"name_4": sqltypes.StringBindVariable("foo"), "user_id_4": sqltypes.Uint64BindVariable(1),
			"name_5": sqltypes.StringBindVariable("foo"), "user_id_5": sqltypes.Uint64BindVariable(1),
			"name_6": sqltypes.StringBindVariable("foo"), "user_id_6": sqltypes.Uint64BindVariable(1),
			"name_7": sqltypes.StringBindVariable("foo"), "user_id_7": sqltypes.Uint64BindVariable(1),
		},
	}}

	for _, workload := range []string{"olap", "oltp"} {
		sbc1.Queries = nil
		sbc2.Queries = nil
		sbclookup.Queries = nil
		wQuery := fmt.Sprintf("set @@workload = %s", workload)
		_, err := executor.Execute(context.Background(), nil, "TestInsertSelect", session, wQuery, nil)
		require.NoError(t, err)

		_, err = executor.Execute(context.Background(), nil, "TestInsertSelect", session, query, nil)
		require.NoError(t, err)

		assertQueries(t, sbc1, wantQueries)
		assertQueries(t, sbc2, wantQueries[:1]) // select scatter query went scatter.
		assertQueries(t, sbclookup, wantlkpQueries)

		testQueryLog(t, executor, logChan, "TestInsertSelect", "SET", wQuery, 0)
		testQueryLog(t, executor, logChan, "VindexCreate", "INSERT", "insert into name_user_map(`name`, user_id) values (:name_0, :user_id_0), (:name_1, :user_id_1), (:name_2, :user_id_2), (:name_3, :user_id_3), (:name_4, :user_id_4), (:name_5, :user_id_5), (:name_6, :user_id_6), (:name_7, :user_id_7)", 1)
		testQueryLog(t, executor, logChan, "TestInsertSelect", "INSERT", "insert into `user`(id, `name`) select c1, c2 from music", 9) // 8 from select and 1 from insert.
	}
}

func TestInsertReference(t *testing.T) {
	executor, sbc1, sbc2, sbclookup, ctx := createExecutorEnv(t)

	logChan := executor.queryLogger.Subscribe("Test")
	defer executor.queryLogger.Unsubscribe(logChan)

	session := &vtgatepb.Session{
		TargetString: "@primary",
	}
	_, err := executorExec(ctx, executor, session, "insert into zip_detail(id, status) values (1, 'CLOSED')", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "insert into zip_detail(id, `status`) values (1, 'CLOSED')",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	assertQueries(t, sbc1, nil)
	assertQueries(t, sbc2, nil)
	assertQueries(t, sbclookup, wantQueries)

	testQueryLog(t, executor, logChan, "TestExecute", "INSERT", "insert into zip_detail(id, `status`) values (1, 'CLOSED')", 1)

	sbclookup.Queries = nil

	_, err = executorExec(ctx, executor, session, "insert into TestUnsharded.zip_detail(id, status) values (1, 'CLOSED')", nil)
	require.NoError(t, err)
	wantQueries = []*querypb.BoundQuery{{
		Sql:           "insert into zip_detail(id, `status`) values (1, 'CLOSED')",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	assertQueries(t, sbc1, nil)
	assertQueries(t, sbc2, nil)
	assertQueries(t, sbclookup, wantQueries)

	testQueryLog(t, executor, logChan, "TestExecute", "INSERT",
		"insert into TestUnsharded.zip_detail(id, `status`) values (1, 'CLOSED')", 1)

	sbclookup.Queries = nil

	_, err = executorExec(ctx, executor, session, "insert into TestExecutor.zip_detail(id, status) values (1, 'CLOSED')", nil)
	require.NoError(t, err) // Gen4 planner can redirect the query to correct source for update when reference table is involved.
}

func TestDeleteMultiTable(t *testing.T) {
	executor, sbc1, sbc2, sbclookup, ctx := createExecutorEnv(t)
	executor.vschema.Keyspaces["TestExecutor"].Tables["user"].PrimaryKey = sqlparser.Columns{sqlparser.NewIdentifierCI("id")}

	logChan := executor.queryLogger.Subscribe("TestDeleteMultiTable")
	defer executor.queryLogger.Unsubscribe(logChan)

	session := &vtgatepb.Session{TargetString: "@primary"}
	_, err := executorExec(ctx, executor, session, "delete user from user join music on user.col = music.col where music.user_id = 1", nil)
	require.NoError(t, err)

	var dmlVals []*querypb.Value
	for i := 0; i < 8; i++ {
		dmlVals = append(dmlVals, sqltypes.ValueToProto(sqltypes.NewInt32(1)))
	}

	bq := &querypb.BoundQuery{
		Sql:           "select 1 from music where music.user_id = 1 and music.col = :user_col",
		BindVariables: map[string]*querypb.BindVariable{"user_col": sqltypes.StringBindVariable("foo")},
	}
	wantQueries := []*querypb.BoundQuery{
		{Sql: "select `user`.id, `user`.col from `user`", BindVariables: map[string]*querypb.BindVariable{}},
		bq, bq, bq, bq, bq, bq, bq, bq,
		{Sql: "select `user`.Id, `user`.`name` from `user` where `user`.id in ::dml_vals for update", BindVariables: map[string]*querypb.BindVariable{"dml_vals": {Type: querypb.Type_TUPLE, Values: dmlVals}}},
		{Sql: "delete from `user` where `user`.id in ::dml_vals", BindVariables: map[string]*querypb.BindVariable{"dml_vals": {Type: querypb.Type_TUPLE, Values: dmlVals}}}}
	assertQueries(t, sbc1, wantQueries)

	wantQueries = []*querypb.BoundQuery{
		{Sql: "select `user`.id, `user`.col from `user`", BindVariables: map[string]*querypb.BindVariable{}},
		{Sql: "select `user`.Id, `user`.`name` from `user` where `user`.id in ::dml_vals for update", BindVariables: map[string]*querypb.BindVariable{"dml_vals": {Type: querypb.Type_TUPLE, Values: dmlVals}}},
		{Sql: "delete from `user` where `user`.id in ::dml_vals", BindVariables: map[string]*querypb.BindVariable{"dml_vals": {Type: querypb.Type_TUPLE, Values: dmlVals}}},
	}
	assertQueries(t, sbc2, wantQueries)

	bq = &querypb.BoundQuery{
		Sql: "delete from name_user_map where `name` = :name and user_id = :user_id",
		BindVariables: map[string]*querypb.BindVariable{
			"name":    sqltypes.StringBindVariable("foo"),
			"user_id": sqltypes.Uint64BindVariable(1),
		}}
	wantQueries = []*querypb.BoundQuery{
		bq, bq, bq, bq, bq, bq, bq, bq,
	}
	assertQueries(t, sbclookup, wantQueries)

	testQueryLog(t, executor, logChan, "MarkSavepoint", "SAVEPOINT", "savepoint s1", 8)
	testQueryLog(t, executor, logChan, "VindexDelete", "DELETE", "delete from name_user_map where `name` = :name and user_id = :user_id", 1)
	// select `user`.id, `user`.col from `user` - 8 shard
	// select 1 from music where music.user_id = 1 and music.col = :user_col - 8 shards
	// select Id, `name` from `user` where (`user`.id) in ::dml_vals for update - 1 shard
	// delete from `user` where (`user`.id) in ::dml_vals - 1 shard
	testQueryLog(t, executor, logChan, "TestExecute", "DELETE", "delete `user` from `user` join music on `user`.col = music.col where music.user_id = 1", 18)
}
