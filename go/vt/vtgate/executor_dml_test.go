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
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/mysql"

	"vitess.io/vitess/go/test/utils"

	"github.com/stretchr/testify/require"

	"context"

	"vitess.io/vitess/go/sqltypes"
	_ "vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vttablet/sandboxconn"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

func TestUpdateEqual(t *testing.T) {
	executor, sbc1, sbc2, sbclookup := createLegacyExecutorEnv()

	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	// Update by primary vindex.
	_, err := executorExec(executor, "update user set a=2 where id = 1", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "update `user` set a = 2 where id = 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries: %+v, want %+v\n", sbc1.Queries, wantQueries)
	}
	if sbc2.Queries != nil {
		t.Errorf("sbc2.Queries: %+v, want nil\n", sbc2.Queries)
	}
	testQueryLog(t, logChan, "TestExecute", "UPDATE", "update user set a=2 where id = 1", 1)

	sbc1.Queries = nil
	_, err = executorExec(executor, "update user set a=2 where id = 3", nil)
	require.NoError(t, err)
	wantQueries = []*querypb.BoundQuery{{
		Sql:           "update `user` set a = 2 where id = 3",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	if !reflect.DeepEqual(sbc2.Queries, wantQueries) {
		t.Errorf("sbc2.Queries: %+v, want %+v\n", sbc2.Queries, wantQueries)
	}
	if sbc1.Queries != nil {
		t.Errorf("sbc1.Queries: %+v, want nil\n", sbc1.Queries)
	}

	// Update by secondary vindex.
	sbc1.Queries = nil
	sbc2.Queries = nil
	sbclookup.SetResults([]*sqltypes.Result{{}})
	_, err = executorExec(executor, "update music set a=2 where id = 2", nil)
	require.NoError(t, err)
	vars, err := sqltypes.BuildBindVariable([]interface{}{sqltypes.NewInt64(2)})
	require.NoError(t, err)
	wantQueries = []*querypb.BoundQuery{{
		Sql: "select music_id, user_id from music_user_map where music_id in ::music_id for update",
		BindVariables: map[string]*querypb.BindVariable{
			"music_id": vars,
		},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: %+v, want %+v\n", sbclookup.Queries, wantQueries)
	}
	if sbc2.Queries != nil {
		t.Errorf("sbc2.Queries: %+v, want nil\n", sbc2.Queries)
	}
	if sbc1.Queries != nil {
		t.Errorf("sbc1.Queries: %+v, want nil\n", sbc1.Queries)
	}

	// Update changes lookup vindex values.
	sbc1.Queries = nil
	sbc2.Queries = nil
	sbclookup.Queries = nil
	sbc1.SetResults([]*sqltypes.Result{sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("id|name|lastname|name_lastname_keyspace_id_map", "int64|int32|varchar|int64"),
		"1|1|foo|0",
	),
	})

	_, err = executorExec(executor, "update user2 set `name`='myname', lastname='mylastname' where id = 1", nil)
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
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries: %+v, want %+v\n", sbc1.Queries, wantQueries)
	}
	if sbc2.Queries != nil {
		t.Errorf("sbc2.Queries: %+v, want nil\n", sbc2.Queries)
	}

	wantQueries = []*querypb.BoundQuery{
		{
			Sql: "delete from name_lastname_keyspace_id_map where `name` = :name and lastname = :lastname and keyspace_id = :keyspace_id",
			BindVariables: map[string]*querypb.BindVariable{
				"lastname":    sqltypes.ValueBindVariable(sqltypes.NewVarChar("foo")),
				"name":        sqltypes.Int32BindVariable(1),
				"keyspace_id": sqltypes.BytesBindVariable([]byte("\026k@\264J\272K\326")),
			},
		},
		{
			Sql: "insert into name_lastname_keyspace_id_map(`name`, lastname, keyspace_id) values (:name_0, :lastname_0, :keyspace_id_0)",
			BindVariables: map[string]*querypb.BindVariable{
				"name_0":        sqltypes.BytesBindVariable([]byte("myname")),
				"lastname_0":    sqltypes.BytesBindVariable([]byte("mylastname")),
				"keyspace_id_0": sqltypes.BytesBindVariable([]byte("\026k@\264J\272K\326")),
			},
		},
	}

	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: %+v, want %+v\n", sbclookup.Queries, wantQueries)
	}
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
	executor, sbc1, sbc2, sbclookup := createCustomExecutor(vschema)

	sbc1.SetResults([]*sqltypes.Result{
		sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("id|a|b|c|d|e|f|lookup1|lookup3", "int64|int64|int64|int64|int64|int64|int64|int64|int64"),
			"1|10|20|30|40|50|60|0|0",
		),
	})
	_, err := executorExec(executor, "update user set a=1, b=2, f=4, e=3 where id=1", nil)
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
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries:\n%+v, want\n%+v\n", sbc1.Queries, wantQueries)
	}
	if sbc2.Queries != nil {
		t.Errorf("sbc2.Queries: %+v, want nil\n", sbc2.Queries)
	}

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

	utils.MustMatch(t, wantQueries, sbclookup.Queries, "sbclookup.Queries")
}

func TestUpdateComments(t *testing.T) {
	executor, sbc1, sbc2, _ := createLegacyExecutorEnv()

	_, err := executorExec(executor, "update user set a=2 where id = 1 /* trailing */", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "update `user` set a = 2 where id = 1 /* trailing */",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries: %+v, want %+v\n", sbc1.Queries, wantQueries)
	}
	if sbc2.Queries != nil {
		t.Errorf("sbc2.Queries: %+v, want nil\n", sbc2.Queries)
	}
}

func TestUpdateNormalize(t *testing.T) {
	executor, sbc1, sbc2, _ := createLegacyExecutorEnv()

	executor.normalize = true
	_, err := executorExec(executor, "/* leading */ update user set a=2 where id = 1 /* trailing */", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql: "/* leading */ update `user` set a = :vtg1 where id = :vtg2 /* trailing */",
		BindVariables: map[string]*querypb.BindVariable{
			"vtg1": sqltypes.TestBindVariable(int64(2)),
			"vtg2": sqltypes.TestBindVariable(int64(1)),
		},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries: %+v, want %+v\n", sbc1.Queries, wantQueries)
	}
	if sbc2.Queries != nil {
		t.Errorf("sbc2.Queries: %+v, want nil\n", sbc2.Queries)
	}
	sbc1.Queries = nil

	// Force the query to go to the "wrong" shard and ensure that normalization still happens
	masterSession.TargetString = "TestExecutor/40-60"
	_, err = executorExec(executor, "/* leading */ update user set a=2 where id = 1 /* trailing */", nil)
	require.NoError(t, err)
	wantQueries = []*querypb.BoundQuery{{
		Sql: "/* leading */ update `user` set a = :vtg1 where id = :vtg2 /* trailing */",
		BindVariables: map[string]*querypb.BindVariable{
			"vtg1": sqltypes.TestBindVariable(int64(2)),
			"vtg2": sqltypes.TestBindVariable(int64(1)),
		},
	}}
	assert.Empty(t, sbc1.Queries)
	utils.MustMatch(t, sbc2.Queries, wantQueries, "didn't get expected queries")
	sbc2.Queries = nil
	masterSession.TargetString = ""
}

func TestDeleteEqual(t *testing.T) {
	executor, sbc, _, sbclookup := createLegacyExecutorEnv()

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
	_, err := executorExec(executor, "delete from user where id = 1", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select Id, `name` from `user` where id = 1 for update",
		BindVariables: map[string]*querypb.BindVariable{},
	}, {
		Sql:           "delete from `user` where id = 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	if !reflect.DeepEqual(sbc.Queries, wantQueries) {
		t.Errorf("sbc.Queries:\n%+v, want\n%+v\n", sbc.Queries, wantQueries)
	}

	wantQueries = []*querypb.BoundQuery{{
		Sql: "delete from name_user_map where `name` = :name and user_id = :user_id",
		BindVariables: map[string]*querypb.BindVariable{
			"user_id": sqltypes.Uint64BindVariable(1),
			"name":    sqltypes.ValueBindVariable(sqltypes.NewVarChar("myname")),
		},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries:\n%+v, want\n%+v\n", sbclookup.Queries, wantQueries)
	}

	sbc.Queries = nil
	sbclookup.Queries = nil
	sbc.SetResults([]*sqltypes.Result{{}})
	_, err = executorExec(executor, "delete from user where id = 1", nil)
	require.NoError(t, err)
	wantQueries = []*querypb.BoundQuery{{
		Sql:           "select Id, `name` from `user` where id = 1 for update",
		BindVariables: map[string]*querypb.BindVariable{},
	}, {
		Sql:           "delete from `user` where id = 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	if !reflect.DeepEqual(sbc.Queries, wantQueries) {
		t.Errorf("sbc.Queries:\n%+v, want\n%+v\n", sbc.Queries, wantQueries)
	}
	if sbclookup.Queries != nil {
		t.Errorf("sbclookup.Queries: %+v, want nil\n", sbclookup.Queries)
	}

	sbc.Queries = nil
	sbclookup.Queries = nil
	sbclookup.SetResults([]*sqltypes.Result{{}})
	_, err = executorExec(executor, "delete from music where id = 1", nil)
	require.NoError(t, err)
	vars, err := sqltypes.BuildBindVariable([]interface{}{sqltypes.NewInt64(1)})
	require.NoError(t, err)
	wantQueries = []*querypb.BoundQuery{{
		Sql: "select music_id, user_id from music_user_map where music_id in ::music_id for update",
		BindVariables: map[string]*querypb.BindVariable{
			"music_id": vars,
		},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: %+v, want %+v\n", sbclookup.Queries, wantQueries)
	}
	if sbc.Queries != nil {
		t.Errorf("sbc.Queries: %+v, want nil\n", sbc.Queries)
	}

	sbc.Queries = nil
	sbclookup.Queries = nil
	sbclookup.SetResults([]*sqltypes.Result{{}})
	_, err = executorExec(executor, "delete from user_extra where user_id = 1", nil)
	require.NoError(t, err)
	wantQueries = []*querypb.BoundQuery{{
		Sql:           "delete from user_extra where user_id = 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	if !reflect.DeepEqual(sbc.Queries, wantQueries) {
		t.Errorf("sbc.Queries:\n%+v, want\n%+v\n", sbc.Queries, wantQueries)
	}
	if sbclookup.Queries != nil {
		t.Errorf("sbc.Queries: %+v, want nil\n", sbc.Queries)
	}

	sbc.Queries = nil
	sbclookup.Queries = nil
	sbc.SetResults([]*sqltypes.Result{sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("id|name|lastname", "int64|int32|varchar"),
		"1|1|foo",
	),
	})
	_, err = executorExec(executor, "delete from user2 where id = 1", nil)
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
	if !reflect.DeepEqual(sbc.Queries, wantQueries) {
		t.Errorf("sbc.Queries: %+v, want %+v\n", sbc.Queries, wantQueries)
	}

	wantQueries = []*querypb.BoundQuery{
		{
			Sql: "delete from name_lastname_keyspace_id_map where `name` = :name and lastname = :lastname and keyspace_id = :keyspace_id",
			BindVariables: map[string]*querypb.BindVariable{
				"lastname":    sqltypes.ValueBindVariable(sqltypes.NewVarChar("foo")),
				"name":        sqltypes.Int32BindVariable(1),
				"keyspace_id": sqltypes.BytesBindVariable([]byte("\026k@\264J\272K\326")),
			},
		},
	}

	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: %+v, want %+v\n", sbclookup.Queries, wantQueries)
	}
}

func TestUpdateScatter(t *testing.T) {
	executor, sbc1, sbc2, _ := createLegacyExecutorEnv()
	_, err := executorExec(executor, "update user_extra set col = 2", nil)
	require.NoError(t, err)
	// Queries get annotatted.
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "update user_extra set col = 2",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc.Queries:\n%+v, want\n%+v\n", sbc1.Queries, wantQueries)
	}
	if !reflect.DeepEqual(sbc2.Queries, wantQueries) {
		t.Errorf("sbc.Queries:\n%+v, want\n%+v\n", sbc2.Queries, wantQueries)
	}
}

func TestDeleteScatter(t *testing.T) {
	executor, sbc1, sbc2, _ := createLegacyExecutorEnv()
	_, err := executorExec(executor, "delete from user_extra", nil)
	require.NoError(t, err)
	// Queries get annotatted.
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "delete from user_extra",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc.Queries:\n%+v, want\n%+v\n", sbc1.Queries, wantQueries)
	}
	if !reflect.DeepEqual(sbc2.Queries, wantQueries) {
		t.Errorf("sbc.Queries:\n%+v, want\n%+v\n", sbc2.Queries, wantQueries)
	}
}

func TestDeleteByDestination(t *testing.T) {
	executor, sbc1, sbc2, _ := createLegacyExecutorEnv()
	// This query is not supported in v3, so we know for sure is taking the DeleteByDestination route
	_, err := executorExec(executor, "delete from `TestExecutor[-]`.user_extra limit 10", nil)
	require.NoError(t, err)
	// Queries get annotatted.
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "delete from user_extra limit 10",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc.Queries:\n%+v, want\n%+v\n", sbc1.Queries, wantQueries)
	}
	if !reflect.DeepEqual(sbc2.Queries, wantQueries) {
		t.Errorf("sbc.Queries:\n%+v, want\n%+v\n", sbc2.Queries, wantQueries)
	}
}

func TestDeleteComments(t *testing.T) {
	executor, sbc, _, sbclookup := createLegacyExecutorEnv()

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
	_, err := executorExec(executor, "delete from user where id = 1 /* trailing */", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select Id, `name` from `user` where id = 1 for update /* trailing */",
		BindVariables: map[string]*querypb.BindVariable{},
	}, {
		Sql:           "delete from `user` where id = 1 /* trailing */",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	if !reflect.DeepEqual(sbc.Queries, wantQueries) {
		t.Errorf("sbc.Queries:\n%+v, want\n%+v\n", sbc.Queries, wantQueries)
	}

	wantQueries = []*querypb.BoundQuery{{
		Sql: "delete from name_user_map where `name` = :name and user_id = :user_id /* trailing */",
		BindVariables: map[string]*querypb.BindVariable{
			"user_id": sqltypes.Uint64BindVariable(1),
			"name":    sqltypes.ValueBindVariable(sqltypes.NewVarChar("myname")),
		},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries:\n%+v, want\n%+v\n", sbclookup.Queries, wantQueries)
	}
}

func TestInsertSharded(t *testing.T) {
	executor, sbc1, sbc2, sbclookup := createLegacyExecutorEnv()

	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	_, err := executorExec(executor, "insert into user(id, v, name) values (1, 2, 'myname')", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql: "insert into `user`(id, v, `name`) values (:_Id_0, 2, :_name_0)",
		BindVariables: map[string]*querypb.BindVariable{
			"_Id_0":   sqltypes.Int64BindVariable(1),
			"_name_0": sqltypes.BytesBindVariable([]byte("myname")),
			"__seq0":  sqltypes.Int64BindVariable(1),
		},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries:\n%+v, want\n%+v\n", sbc1.Queries, wantQueries)
	}
	if sbc2.Queries != nil {
		t.Errorf("sbc2.Queries: %+v, want nil\n", sbc2.Queries)
	}
	wantQueries = []*querypb.BoundQuery{{
		Sql: "insert into name_user_map(`name`, user_id) values (:name_0, :user_id_0)",
		BindVariables: map[string]*querypb.BindVariable{
			"name_0":    sqltypes.BytesBindVariable([]byte("myname")),
			"user_id_0": sqltypes.Uint64BindVariable(1),
		},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: \n%+v, want \n%+v", sbclookup.Queries, wantQueries)
	}

	testQueryLog(t, logChan, "VindexCreate", "INSERT", "insert into name_user_map(name, user_id) values(:name_0, :user_id_0)", 1)
	testQueryLog(t, logChan, "TestExecute", "INSERT", "insert into user(id, v, name) values (1, 2, 'myname')", 1)

	sbc1.Queries = nil
	sbclookup.Queries = nil
	_, err = executorExec(executor, "insert into user(id, v, name) values (3, 2, 'myname2')", nil)
	require.NoError(t, err)
	wantQueries = []*querypb.BoundQuery{{
		Sql: "insert into `user`(id, v, `name`) values (:_Id_0, 2, :_name_0)",
		BindVariables: map[string]*querypb.BindVariable{
			"_Id_0":   sqltypes.Int64BindVariable(3),
			"__seq0":  sqltypes.Int64BindVariable(3),
			"_name_0": sqltypes.BytesBindVariable([]byte("myname2")),
		},
	}}
	if !reflect.DeepEqual(sbc2.Queries, wantQueries) {
		t.Errorf("sbc2.Queries:\n%+v, want\n%+v\n", sbc2.Queries, wantQueries)
	}
	if sbc1.Queries != nil {
		t.Errorf("sbc1.Queries: %+v, want nil\n", sbc1.Queries)
	}
	wantQueries = []*querypb.BoundQuery{{
		Sql: "insert into name_user_map(`name`, user_id) values (:name_0, :user_id_0)",
		BindVariables: map[string]*querypb.BindVariable{
			"name_0":    sqltypes.BytesBindVariable([]byte("myname2")),
			"user_id_0": sqltypes.Uint64BindVariable(3),
		},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: \n%+v, want \n%+v\n", sbclookup.Queries, wantQueries)
	}

	sbc1.Queries = nil
	_, err = executorExec(executor, "insert into user2(id, name, lastname) values (2, 'myname', 'mylastname')", nil)
	require.NoError(t, err)
	wantQueries = []*querypb.BoundQuery{{
		Sql: "insert into user2(id, `name`, lastname) values (:_id_0, :_name_0, :_lastname_0)",
		BindVariables: map[string]*querypb.BindVariable{
			"_id_0":       sqltypes.Int64BindVariable(2),
			"_name_0":     sqltypes.BytesBindVariable([]byte("myname")),
			"_lastname_0": sqltypes.BytesBindVariable([]byte("mylastname")),
		},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries:\n%+v, want\n%+v\n", sbc1.Queries, wantQueries)
	}
}

func TestInsertShardedKeyrange(t *testing.T) {
	executor, _, _, _ := createLegacyExecutorEnv()

	// If a unique vindex returns a keyrange, we fail the insert
	_, err := executorExec(executor, "insert into keyrange_table(krcol_unique, krcol) values(1, 1)", nil)
	require.EqualError(t, err, "could not map [INT64(1)] to a unique keyspace id: DestinationKeyRange(-10)")
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
	executor, sbc1, sbc2, sbclookup := createCustomExecutor(vschema)

	_, err := executorExec(executor, "insert into user(id, v, name) values (1, 2, 'myname')", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql: "insert into `user`(id, v, `name`) values (:_Id_0, 2, :_name_0)",
		BindVariables: map[string]*querypb.BindVariable{
			"_Id_0":   sqltypes.Int64BindVariable(1),
			"_name_0": sqltypes.BytesBindVariable([]byte("myname")),
			"__seq0":  sqltypes.Int64BindVariable(1),
		},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries:\n%+v, want\n%+v\n", sbc1.Queries, wantQueries)
	}
	if sbc2.Queries != nil {
		t.Errorf("sbc2.Queries: %+v, want nil\n", sbc2.Queries)
	}
	wantQueries = []*querypb.BoundQuery{{
		Sql: "insert into name_user_map(`name`, user_id) values (:name_0, :user_id_0) on duplicate key update `name` = values(`name`), user_id = values(user_id)",
		BindVariables: map[string]*querypb.BindVariable{
			"name_0":    sqltypes.BytesBindVariable([]byte("myname")),
			"user_id_0": sqltypes.Uint64BindVariable(1),
		},
	}}
	// autocommit should go as ExecuteBatch
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.BatchQueries[0]: \n%+v, want \n%+v", sbclookup.BatchQueries[0], wantQueries)
	}
}

func TestInsertShardedIgnore(t *testing.T) {
	executor, sbc1, sbc2, sbclookup := createLegacyExecutorEnv()

	// Build the sequence of responses for sbclookup. This should
	// match the sequence of queries we validate below.
	fields := sqltypes.MakeTestFields("b|a", "int64|int64")
	field := sqltypes.MakeTestFields("a", "int64")
	sbclookup.SetResults([]*sqltypes.Result{
		// select music_id
		sqltypes.MakeTestResult(fields, "1|1", "3|1", "4|1", "5|1", "6|3"),
		// insert ins_lookup
		{},
		// select ins_lookup 1
		sqltypes.MakeTestResult(field, "1"),
		// select ins_lookup 3
		{},
		// select ins_lookup 4
		sqltypes.MakeTestResult(field, "4"),
		// select ins_lookup 5
		sqltypes.MakeTestResult(field, "5"),
		// select ins_lookup 6
		sqltypes.MakeTestResult(field, "6"),
	})
	// First row: first shard.
	// Second row: will fail because primary vindex will fail to map.
	// Third row: will fail because verification will fail on owned vindex after Create.
	// Fourth row: will fail because verification will fail on unowned hash vindex.
	// Fifth row: first shard.
	// Sixth row: second shard (because 3 hash maps to 40-60).
	query := "insert ignore into insert_ignore_test(pv, owned, verify) values (1, 1, 1), (2, 2, 2), (3, 3, 1), (4, 4, 4), (5, 5, 1), (6, 6, 3)"
	_, err := executorExec(executor, query, nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql: "insert ignore into insert_ignore_test(pv, owned, verify) values (:_pv_0, :_owned_0, :_verify_0),(:_pv_4, :_owned_4, :_verify_4)",
		BindVariables: map[string]*querypb.BindVariable{
			"_pv_0":     sqltypes.Int64BindVariable(1),
			"_pv_4":     sqltypes.Int64BindVariable(5),
			"_pv_5":     sqltypes.Int64BindVariable(6),
			"_owned_0":  sqltypes.Int64BindVariable(1),
			"_owned_4":  sqltypes.Int64BindVariable(5),
			"_owned_5":  sqltypes.Int64BindVariable(6),
			"_verify_0": sqltypes.Int64BindVariable(1),
			"_verify_4": sqltypes.Int64BindVariable(1),
			"_verify_5": sqltypes.Int64BindVariable(3),
		},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries:\n%+v, want\n%+v\n", sbc1.Queries, wantQueries)
	}
	wantQueries = []*querypb.BoundQuery{{
		Sql: "insert ignore into insert_ignore_test(pv, owned, verify) values (:_pv_5, :_owned_5, :_verify_5)",
		BindVariables: map[string]*querypb.BindVariable{
			"_pv_0":     sqltypes.Int64BindVariable(1),
			"_pv_4":     sqltypes.Int64BindVariable(5),
			"_pv_5":     sqltypes.Int64BindVariable(6),
			"_owned_0":  sqltypes.Int64BindVariable(1),
			"_owned_4":  sqltypes.Int64BindVariable(5),
			"_owned_5":  sqltypes.Int64BindVariable(6),
			"_verify_0": sqltypes.Int64BindVariable(1),
			"_verify_4": sqltypes.Int64BindVariable(1),
			"_verify_5": sqltypes.Int64BindVariable(3),
		},
	}}
	if !reflect.DeepEqual(sbc2.Queries, wantQueries) {
		t.Errorf("sbc2.Queries:\n%+v, want\n%+v\n", sbc2.Queries, wantQueries)
	}

	vars, err := sqltypes.BuildBindVariable([]interface{}{
		sqltypes.NewInt64(1),
		sqltypes.NewInt64(2),
		sqltypes.NewInt64(3),
		sqltypes.NewInt64(4),
		sqltypes.NewInt64(5),
		sqltypes.NewInt64(6),
	})
	require.NoError(t, err)
	wantQueries = []*querypb.BoundQuery{{
		Sql: "select music_id, user_id from music_user_map where music_id in ::music_id for update",
		BindVariables: map[string]*querypb.BindVariable{
			"music_id": vars,
		},
	}, {
		Sql: "insert ignore into ins_lookup(fromcol, tocol) values (:fromcol_0, :tocol_0), (:fromcol_1, :tocol_1), (:fromcol_2, :tocol_2), (:fromcol_3, :tocol_3), (:fromcol_4, :tocol_4)",
		BindVariables: map[string]*querypb.BindVariable{
			"fromcol_0": sqltypes.Int64BindVariable(1),
			"tocol_0":   sqltypes.Uint64BindVariable(1),
			"fromcol_1": sqltypes.Int64BindVariable(3),
			"tocol_1":   sqltypes.Uint64BindVariable(1),
			"fromcol_2": sqltypes.Int64BindVariable(4),
			"tocol_2":   sqltypes.Uint64BindVariable(1),
			"fromcol_3": sqltypes.Int64BindVariable(5),
			"tocol_3":   sqltypes.Uint64BindVariable(1),
			"fromcol_4": sqltypes.Int64BindVariable(6),
			"tocol_4":   sqltypes.Uint64BindVariable(3),
		},
	}, {
		Sql: "select fromcol from ins_lookup where fromcol = :fromcol and tocol = :tocol",
		BindVariables: map[string]*querypb.BindVariable{
			"fromcol": sqltypes.Int64BindVariable(1),
			"tocol":   sqltypes.Uint64BindVariable(1),
		},
	}, {
		Sql: "select fromcol from ins_lookup where fromcol = :fromcol and tocol = :tocol",
		BindVariables: map[string]*querypb.BindVariable{
			"fromcol": sqltypes.Int64BindVariable(3),
			"tocol":   sqltypes.Uint64BindVariable(1),
		},
	}, {
		Sql: "select fromcol from ins_lookup where fromcol = :fromcol and tocol = :tocol",
		BindVariables: map[string]*querypb.BindVariable{
			"fromcol": sqltypes.Int64BindVariable(4),
			"tocol":   sqltypes.Uint64BindVariable(1),
		},
	}, {
		Sql: "select fromcol from ins_lookup where fromcol = :fromcol and tocol = :tocol",
		BindVariables: map[string]*querypb.BindVariable{
			"fromcol": sqltypes.Int64BindVariable(5),
			"tocol":   sqltypes.Uint64BindVariable(1),
		},
	}, {
		Sql: "select fromcol from ins_lookup where fromcol = :fromcol and tocol = :tocol",
		BindVariables: map[string]*querypb.BindVariable{
			"fromcol": sqltypes.Int64BindVariable(6),
			"tocol":   sqltypes.Uint64BindVariable(3),
		},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: \n%+v, want \n%+v", sbclookup.Queries, wantQueries)
	}

	// Test the 0 rows case,
	sbc1.Queries = nil
	sbc2.Queries = nil
	sbclookup.Queries = nil
	sbclookup.SetResults([]*sqltypes.Result{
		{},
	})
	query = "insert ignore into insert_ignore_test(pv, owned, verify) values (1, 1, 1)"
	qr, err := executorExec(executor, query, nil)
	require.NoError(t, err)
	if !reflect.DeepEqual(qr, &sqltypes.Result{}) {
		t.Errorf("qr: %v, want empty result", qr)
	}
	if sbc1.Queries != nil {
		t.Errorf("sbc1.Queries: %+v, want nil\n", sbc1.Queries)
	}
	if sbc2.Queries != nil {
		t.Errorf("sbc2.Queries: %+v, want nil\n", sbc2.Queries)
	}
	vars, err = sqltypes.BuildBindVariable([]interface{}{sqltypes.NewInt64(1)})
	require.NoError(t, err)
	wantQueries = []*querypb.BoundQuery{{
		Sql: "select music_id, user_id from music_user_map where music_id in ::music_id for update",
		BindVariables: map[string]*querypb.BindVariable{
			"music_id": vars,
		},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: \n%+v, want \n%+v", sbclookup.Queries, wantQueries)
	}
}

func TestInsertOnDupKey(t *testing.T) {
	// This test just sanity checks that the statement is getting passed through
	// correctly. The full set of use cases are covered by TestInsertShardedIgnore.
	executor, sbc1, sbc2, sbclookup := createLegacyExecutorEnv()
	sbclookup.SetResults([]*sqltypes.Result{sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("b|a", "int64|varbinary"),
		"1|1",
	)})
	query := "insert into insert_ignore_test(pv, owned, verify) values (1, 1, 1) on duplicate key update col = 2"
	_, err := executorExec(executor, query, nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql: "insert into insert_ignore_test(pv, owned, verify) values (:_pv_0, :_owned_0, :_verify_0) on duplicate key update col = 2",
		BindVariables: map[string]*querypb.BindVariable{
			"_pv_0":     sqltypes.Int64BindVariable(1),
			"_owned_0":  sqltypes.Int64BindVariable(1),
			"_verify_0": sqltypes.Int64BindVariable(1),
		},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries:\n%+v, want\n%+v\n", sbc1.Queries, wantQueries)
	}
	if sbc2.Queries != nil {
		t.Errorf("sbc2.Queries: %+v, want nil\n", sbc2.Queries)
	}
	vars, err := sqltypes.BuildBindVariable([]interface{}{sqltypes.NewInt64(1)})
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
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: \n%+v, want \n%+v", sbclookup.Queries, wantQueries)
	}
}

func TestAutocommitFail(t *testing.T) {
	executor, sbc1, _, _ := createLegacyExecutorEnv()

	query := "insert into user (id) values (1)"
	sbc1.MustFailCodes[vtrpcpb.Code_ALREADY_EXISTS] = 1
	masterSession.Reset()
	masterSession.Autocommit = true
	defer func() {
		masterSession.Autocommit = false
	}()
	_, err := executorExec(executor, query, nil)
	require.Error(t, err)

	// make sure we have closed and rolled back any transactions started
	assert.False(t, masterSession.InTransaction, "left with tx open")
}

func TestInsertComments(t *testing.T) {
	executor, sbc1, sbc2, sbclookup := createLegacyExecutorEnv()

	_, err := executorExec(executor, "insert into user(id, v, name) values (1, 2, 'myname') /* trailing */", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql: "insert into `user`(id, v, `name`) values (:_Id_0, 2, :_name_0) /* trailing */",
		BindVariables: map[string]*querypb.BindVariable{
			"_Id_0":   sqltypes.Int64BindVariable(1),
			"_name_0": sqltypes.BytesBindVariable([]byte("myname")),
			"__seq0":  sqltypes.Int64BindVariable(1),
		},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries:\n%+v, want\n%+v\n", sbc1.Queries, wantQueries)
	}
	if sbc2.Queries != nil {
		t.Errorf("sbc2.Queries: %+v, want nil\n", sbc2.Queries)
	}
	wantQueries = []*querypb.BoundQuery{{
		Sql: "insert into name_user_map(`name`, user_id) values (:name_0, :user_id_0) /* trailing */",
		BindVariables: map[string]*querypb.BindVariable{
			"name_0":    sqltypes.BytesBindVariable([]byte("myname")),
			"user_id_0": sqltypes.Uint64BindVariable(1),
		},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: \n%+v, want \n%+v", sbclookup.Queries, wantQueries)
	}
}

func TestInsertGeneratorSharded(t *testing.T) {
	executor, sbc, _, sbclookup := createLegacyExecutorEnv()

	sbclookup.SetResults([]*sqltypes.Result{{
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(1),
		}},
		RowsAffected: 1,
		InsertID:     1,
	}})
	result, err := executorExec(executor, "insert into user(v, `name`) values (2, 'myname')", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql: "insert into `user`(v, `name`, id) values (2, :_name_0, :_Id_0)",
		BindVariables: map[string]*querypb.BindVariable{
			"_Id_0":   sqltypes.Int64BindVariable(1),
			"__seq0":  sqltypes.Int64BindVariable(1),
			"_name_0": sqltypes.BytesBindVariable([]byte("myname")),
		},
	}}
	if !reflect.DeepEqual(sbc.Queries, wantQueries) {
		t.Errorf("sbc.Queries: %+v, want %+v\n", sbc.Queries, wantQueries)
	}
	wantQueries = []*querypb.BoundQuery{{
		Sql:           "select next :n values from user_seq",
		BindVariables: map[string]*querypb.BindVariable{"n": sqltypes.Int64BindVariable(1)},
	}, {
		Sql: "insert into name_user_map(`name`, user_id) values (:name_0, :user_id_0)",
		BindVariables: map[string]*querypb.BindVariable{
			"name_0":    sqltypes.BytesBindVariable([]byte("myname")),
			"user_id_0": sqltypes.Uint64BindVariable(1),
		},
	}}
	utils.MustMatch(t, wantQueries, sbclookup.Queries, "sbclookup.Queries")
	wantResult := &sqltypes.Result{
		InsertID:     1,
		RowsAffected: 1,
	}
	utils.MustMatch(t, wantResult, result)
}

func TestInsertAutoincSharded(t *testing.T) {
	router, sbc, _, _ := createLegacyExecutorEnv()

	// Fake a mysql auto-inc response.
	wantResult := &sqltypes.Result{
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(1),
		}},
		RowsAffected: 1,
		InsertID:     2,
	}
	sbc.SetResults([]*sqltypes.Result{wantResult})
	result, err := executorExec(router, "insert into user_extra(user_id) values (2)", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql: "insert into user_extra(user_id) values (:_user_id_0)",
		BindVariables: map[string]*querypb.BindVariable{
			"_user_id_0": sqltypes.Int64BindVariable(2),
		},
	}}
	if !reflect.DeepEqual(sbc.Queries, wantQueries) {
		t.Errorf("sbc.Queries:\n%+v, want\n%+v\n", sbc.Queries, wantQueries)
	}
	if !result.Equal(wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}
	assert.Equal(t, masterSession.LastInsertId, uint64(2))
}

func TestInsertGeneratorUnsharded(t *testing.T) {
	executor, _, _, sbclookup := createLegacyExecutorEnv()
	result, err := executorExec(executor, "insert into main1(id, name) values (null, 'myname')", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select next :n values from user_seq",
		BindVariables: map[string]*querypb.BindVariable{"n": sqltypes.Int64BindVariable(1)},
	}, {
		Sql: "insert into main1(id, `name`) values (:__seq0, 'myname')",
		BindVariables: map[string]*querypb.BindVariable{
			"__seq0": sqltypes.Int64BindVariable(1),
		},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: \n%#v, want \n%#v\n", sbclookup.Queries, wantQueries)
	}
	wantResult := &sqltypes.Result{
		InsertID:     1,
		RowsAffected: 1,
	}
	utils.MustMatch(t, wantResult, result)
}

func TestInsertAutoincUnsharded(t *testing.T) {
	router, _, _, sbclookup := createLegacyExecutorEnv()

	// Fake a mysql auto-inc response.
	query := "insert into simple(val) values ('val')"
	wantResult := &sqltypes.Result{
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(1),
		}},
		RowsAffected: 1,
		InsertID:     2,
	}
	sbclookup.SetResults([]*sqltypes.Result{wantResult})

	result, err := executorExec(router, query, nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql:           query,
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: \n%#v, want \n%#v\n", sbclookup.Queries, wantQueries)
	}
	if !result.Equal(wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}
}

func TestInsertLookupOwned(t *testing.T) {
	executor, sbc, _, sbclookup := createLegacyExecutorEnv()

	_, err := executorExec(executor, "insert into music(user_id, id) values (2, 3)", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql: "insert into music(user_id, id) values (:_user_id_0, :_id_0)",
		BindVariables: map[string]*querypb.BindVariable{
			"_user_id_0": sqltypes.Int64BindVariable(2),
			"_id_0":      sqltypes.Int64BindVariable(3),
			"__seq0":     sqltypes.Int64BindVariable(3),
		},
	}}
	if !reflect.DeepEqual(sbc.Queries, wantQueries) {
		t.Errorf("sbc.Queries:\n%+v, want\n%+v\n", sbc.Queries, wantQueries)
	}
	wantQueries = []*querypb.BoundQuery{{
		Sql: "insert into music_user_map(music_id, user_id) values (:music_id_0, :user_id_0)",
		BindVariables: map[string]*querypb.BindVariable{
			"music_id_0": sqltypes.Int64BindVariable(3),
			"user_id_0":  sqltypes.Uint64BindVariable(2),
		},
	}}
	utils.MustMatch(t, wantQueries, sbclookup.Queries, "sbclookup.Queries")
}

func TestInsertLookupOwnedGenerator(t *testing.T) {
	executor, sbc, _, sbclookup := createLegacyExecutorEnv()

	sbclookup.SetResults([]*sqltypes.Result{{
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(4),
		}},
		RowsAffected: 1,
		InsertID:     1,
	}})
	result, err := executorExec(executor, "insert into music(user_id) values (2)", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql: "insert into music(user_id, id) values (:_user_id_0, :_id_0)",
		BindVariables: map[string]*querypb.BindVariable{
			"_user_id_0": sqltypes.Int64BindVariable(2),
			"_id_0":      sqltypes.Int64BindVariable(4),
			"__seq0":     sqltypes.Int64BindVariable(4),
		},
	}}
	if !reflect.DeepEqual(sbc.Queries, wantQueries) {
		t.Errorf("sbc.Queries:\n%+v, want\n%+v\n", sbc.Queries, wantQueries)
	}
	wantQueries = []*querypb.BoundQuery{{
		Sql:           "select next :n values from user_seq",
		BindVariables: map[string]*querypb.BindVariable{"n": sqltypes.Int64BindVariable(1)},
	}, {
		Sql: "insert into music_user_map(music_id, user_id) values (:music_id_0, :user_id_0)",
		BindVariables: map[string]*querypb.BindVariable{
			"music_id_0": sqltypes.Int64BindVariable(4),
			"user_id_0":  sqltypes.Uint64BindVariable(2),
		},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries:\n%+v, want\n%+v\n", sbclookup.Queries, wantQueries)
	}
	wantResult := &sqltypes.Result{
		InsertID:     4,
		RowsAffected: 1,
	}
	utils.MustMatch(t, wantResult, result)
}

func TestInsertLookupUnowned(t *testing.T) {
	executor, sbc, _, sbclookup := createLegacyExecutorEnv()

	_, err := executorExec(executor, "insert into music_extra(user_id, music_id) values (2, 3)", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql: "insert into music_extra(user_id, music_id) values (:_user_id_0, :_music_id_0)",
		BindVariables: map[string]*querypb.BindVariable{
			"_user_id_0":  sqltypes.Int64BindVariable(2),
			"_music_id_0": sqltypes.Int64BindVariable(3),
		},
	}}
	if !reflect.DeepEqual(sbc.Queries, wantQueries) {
		t.Errorf("sbc.Queries: %+v, want %+v\n", sbc.Queries, wantQueries)
	}
	wantQueries = []*querypb.BoundQuery{{
		Sql: "select music_id from music_user_map where music_id = :music_id and user_id = :user_id",
		BindVariables: map[string]*querypb.BindVariable{
			"music_id": sqltypes.Int64BindVariable(3),
			"user_id":  sqltypes.Uint64BindVariable(2),
		},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries:\n%v, want\n%v\n", sbclookup.Queries, wantQueries)
	}
}

func TestInsertLookupUnownedUnsupplied(t *testing.T) {
	executor, sbc, _, sbclookup := createLegacyExecutorEnv()
	sbclookup.SetResults([]*sqltypes.Result{sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("b|a", "int64|varbinary"),
		"3|1",
	)})
	_, err := executorExec(executor, "insert into music_extra_reversed(music_id) values (3)", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql: "insert into music_extra_reversed(music_id, user_id) values (:_music_id_0, :_user_id_0)",
		BindVariables: map[string]*querypb.BindVariable{
			"_user_id_0":  sqltypes.Uint64BindVariable(1),
			"_music_id_0": sqltypes.Int64BindVariable(3),
		},
	}}
	if !reflect.DeepEqual(sbc.Queries, wantQueries) {
		t.Errorf("sbc.Queries:\n%+v, want\n%+v\n", sbc.Queries, wantQueries)
	}
	vars, err := sqltypes.BuildBindVariable([]interface{}{sqltypes.NewInt64(3)})
	require.NoError(t, err)
	wantQueries = []*querypb.BoundQuery{{
		Sql: "select music_id, user_id from music_user_map where music_id in ::music_id for update",
		BindVariables: map[string]*querypb.BindVariable{
			"music_id": vars,
		},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: %+v, want %+v\n", sbclookup.Queries, wantQueries)
	}
}

// If a statement gets broken up into two, and the first one fails,
// then an error should be returned normally.
func TestInsertPartialFail1(t *testing.T) {
	executor, _, _, sbclookup := createLegacyExecutorEnv()

	// Make the first DML fail, there should be no rollback.
	sbclookup.MustFailCodes[vtrpcpb.Code_INVALID_ARGUMENT] = 1

	_, err := executor.Execute(
		context.Background(),
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
	executor, sbc1, _, _ := createLegacyExecutorEnv()

	// Make the second DML fail, it should result in a rollback.
	sbc1.MustFailCodes[vtrpcpb.Code_INVALID_ARGUMENT] = 1

	_, err := executor.Execute(
		context.Background(),
		"TestExecute",
		NewSafeSession(&vtgatepb.Session{InTransaction: true}),
		"insert into user(id, v, name) values (1, 2, 'myname')",
		nil,
	)
	want := "transaction rolled back"
	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Errorf("insert first DML fail: %v, must start with %s", err, want)
	}
}

func TestMultiInsertSharded(t *testing.T) {
	executor, sbc1, sbc2, sbclookup := createLegacyExecutorEnv()

	_, err := executorExec(executor, "insert into user(id, v, name) values (1, 1, 'myname1'),(3, 3, 'myname3')", nil)
	require.NoError(t, err)
	wantQueries1 := []*querypb.BoundQuery{{
		Sql: "insert into `user`(id, v, `name`) values (:_Id_0, 1, :_name_0)",
		BindVariables: map[string]*querypb.BindVariable{
			"_Id_0":   sqltypes.Int64BindVariable(1),
			"_name_0": sqltypes.BytesBindVariable([]byte("myname1")),
			"__seq0":  sqltypes.Int64BindVariable(1),
			"_Id_1":   sqltypes.Int64BindVariable(3),
			"_name_1": sqltypes.BytesBindVariable([]byte("myname3")),
			"__seq1":  sqltypes.Int64BindVariable(3),
		},
	}}

	wantQueries2 := []*querypb.BoundQuery{{
		Sql: "insert into `user`(id, v, `name`) values (:_Id_1, 3, :_name_1)",
		BindVariables: map[string]*querypb.BindVariable{
			"_Id_0":   sqltypes.Int64BindVariable(1),
			"_name_0": sqltypes.BytesBindVariable([]byte("myname1")),
			"__seq0":  sqltypes.Int64BindVariable(1),
			"_Id_1":   sqltypes.Int64BindVariable(3),
			"_name_1": sqltypes.BytesBindVariable([]byte("myname3")),
			"__seq1":  sqltypes.Int64BindVariable(3),
		},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries1) {
		t.Errorf("sbc1.Queries:\n%+v, want\n%+v\n", sbc1.Queries, wantQueries1)
	}

	if !reflect.DeepEqual(sbc2.Queries, wantQueries2) {
		t.Errorf("sbc2.Queries:\n%+v, want\n%+v\n", sbc2.Queries, wantQueries2)
	}

	wantQueries1 = []*querypb.BoundQuery{{
		Sql: "insert into name_user_map(`name`, user_id) values (:name_0, :user_id_0), (:name_1, :user_id_1)",
		BindVariables: map[string]*querypb.BindVariable{
			"name_0":    sqltypes.BytesBindVariable([]byte("myname1")),
			"user_id_0": sqltypes.Uint64BindVariable(1),
			"name_1":    sqltypes.BytesBindVariable([]byte("myname3")),
			"user_id_1": sqltypes.Uint64BindVariable(3),
		},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries1) {
		t.Errorf("sbclookup.Queries: \n%+v, want \n%+v", sbclookup.Queries, wantQueries1)
	}

	sbc1.Queries = nil
	sbclookup.Queries = nil
	sbc2.Queries = nil
	_, err = executorExec(executor, "insert into user(id, v, name) values (1, 1, 'myname1'),(2, 2, 'myname2')", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql: "insert into `user`(id, v, `name`) values (:_Id_0, 1, :_name_0),(:_Id_1, 2, :_name_1)",
		BindVariables: map[string]*querypb.BindVariable{
			"_Id_0":   sqltypes.Int64BindVariable(1),
			"__seq0":  sqltypes.Int64BindVariable(1),
			"_name_0": sqltypes.BytesBindVariable([]byte("myname1")),
			"_Id_1":   sqltypes.Int64BindVariable(2),
			"__seq1":  sqltypes.Int64BindVariable(2),
			"_name_1": sqltypes.BytesBindVariable([]byte("myname2")),
		},
	}}

	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries:\n%+v, want\n%+v\n", sbc1.Queries, wantQueries)
	}
	if sbc2.Queries != nil {
		t.Errorf("sbc2.Queries: %+v, want nil\n", sbc2.Queries)
	}
	wantQueries = []*querypb.BoundQuery{{
		Sql: "insert into name_user_map(`name`, user_id) values (:name_0, :user_id_0), (:name_1, :user_id_1)",
		BindVariables: map[string]*querypb.BindVariable{
			"name_0":    sqltypes.BytesBindVariable([]byte("myname1")),
			"user_id_0": sqltypes.Uint64BindVariable(1),
			"name_1":    sqltypes.BytesBindVariable([]byte("myname2")),
			"user_id_1": sqltypes.Uint64BindVariable(2),
		},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: \n%+v, want \n%+v\n", sbclookup.Queries, wantQueries)
	}

	// Insert multiple rows in a multi column vindex
	sbc1.Queries = nil
	sbclookup.Queries = nil
	sbc2.Queries = nil
	_, err = executorExec(executor, "insert into user2(id, `name`, lastname) values (2, 'myname', 'mylastname'), (3, 'myname2', 'mylastname2')", nil)
	require.NoError(t, err)
	wantQueries = []*querypb.BoundQuery{{
		Sql: "insert into user2(id, `name`, lastname) values (:_id_0, :_name_0, :_lastname_0)",
		BindVariables: map[string]*querypb.BindVariable{
			"_id_0":       sqltypes.Int64BindVariable(2),
			"_name_0":     sqltypes.BytesBindVariable([]byte("myname")),
			"_lastname_0": sqltypes.BytesBindVariable([]byte("mylastname")),
			"_id_1":       sqltypes.Int64BindVariable(3),
			"_name_1":     sqltypes.BytesBindVariable([]byte("myname2")),
			"_lastname_1": sqltypes.BytesBindVariable([]byte("mylastname2")),
		},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries:\n%+v, want\n%+v\n", sbc1.Queries, wantQueries)
	}
	wantQueries = []*querypb.BoundQuery{{
		Sql: "insert into name_lastname_keyspace_id_map(`name`, lastname, keyspace_id) values (:name_0, :lastname_0, :keyspace_id_0), (:name_1, :lastname_1, :keyspace_id_1)",
		BindVariables: map[string]*querypb.BindVariable{
			"name_0":        sqltypes.BytesBindVariable([]byte("myname")),
			"lastname_0":    sqltypes.BytesBindVariable([]byte("mylastname")),
			"keyspace_id_0": sqltypes.BytesBindVariable([]byte("\006\347\352\"\316\222p\217")),
			"name_1":        sqltypes.BytesBindVariable([]byte("myname2")),
			"lastname_1":    sqltypes.BytesBindVariable([]byte("mylastname2")),
			"keyspace_id_1": sqltypes.BytesBindVariable([]byte("N\261\220\311\242\372\026\234")),
		},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: \n%+v, want \n%+v\n", sbclookup.Queries, wantQueries)
	}
}

func TestMultiInsertGenerator(t *testing.T) {
	executor, sbc, _, sbclookup := createLegacyExecutorEnv()

	sbclookup.SetResults([]*sqltypes.Result{{
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(1),
		}},
		RowsAffected: 1,
		InsertID:     1,
	}})
	result, err := executorExec(executor, "insert into music(user_id, `name`) values (:u, 'myname1'),(:u, 'myname2')", map[string]*querypb.BindVariable{"u": sqltypes.Int64BindVariable(2)})
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql: "insert into music(user_id, `name`, id) values (:_user_id_0, 'myname1', :_id_0),(:_user_id_1, 'myname2', :_id_1)",
		BindVariables: map[string]*querypb.BindVariable{
			"u":          sqltypes.Int64BindVariable(2),
			"_id_0":      sqltypes.Int64BindVariable(1),
			"__seq0":     sqltypes.Int64BindVariable(1),
			"_user_id_0": sqltypes.Int64BindVariable(2),
			"_id_1":      sqltypes.Int64BindVariable(2),
			"__seq1":     sqltypes.Int64BindVariable(2),
			"_user_id_1": sqltypes.Int64BindVariable(2),
		},
	}}
	if !reflect.DeepEqual(sbc.Queries, wantQueries) {
		t.Errorf("sbc.Queries:\n%+v, want\n%+v\n", sbc.Queries, wantQueries)
	}
	wantQueries = []*querypb.BoundQuery{{
		Sql:           "select next :n values from user_seq",
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
	utils.MustMatch(t, wantQueries, sbclookup.Queries, "sbclookup.Queries")
	wantResult := &sqltypes.Result{
		InsertID:     1,
		RowsAffected: 1,
	}
	utils.MustMatch(t, wantResult, result)
}

func TestMultiInsertGeneratorSparse(t *testing.T) {
	executor, sbc, _, sbclookup := createLegacyExecutorEnv()

	sbclookup.SetResults([]*sqltypes.Result{{
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(1),
		}},
		RowsAffected: 1,
		InsertID:     1,
	}})
	result, err := executorExec(executor, "insert into music(id, user_id, name) values (NULL, :u, 'myname1'),(2, :u, 'myname2'), (NULL, :u, 'myname3')", map[string]*querypb.BindVariable{"u": sqltypes.Int64BindVariable(2)})
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql: "insert into music(id, user_id, `name`) values (:_id_0, :_user_id_0, 'myname1'),(:_id_1, :_user_id_1, 'myname2'),(:_id_2, :_user_id_2, 'myname3')",
		BindVariables: map[string]*querypb.BindVariable{
			"u":          sqltypes.Int64BindVariable(2),
			"_id_0":      sqltypes.Int64BindVariable(1),
			"__seq0":     sqltypes.Int64BindVariable(1),
			"_user_id_0": sqltypes.Int64BindVariable(2),
			"_id_1":      sqltypes.Int64BindVariable(2),
			"__seq1":     sqltypes.Int64BindVariable(2),
			"_user_id_1": sqltypes.Int64BindVariable(2),
			"_id_2":      sqltypes.Int64BindVariable(2),
			"__seq2":     sqltypes.Int64BindVariable(2),
			"_user_id_2": sqltypes.Int64BindVariable(2),
		},
	}}
	if !reflect.DeepEqual(sbc.Queries, wantQueries) {
		t.Errorf("sbc.Queries:\n%+v, want\n%+v\n", sbc.Queries, wantQueries)
	}
	wantQueries = []*querypb.BoundQuery{{
		Sql:           "select next :n values from user_seq",
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
	utils.MustMatch(t, wantQueries, sbclookup.Queries, "sbclookup.Queries")
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
	executor, _, _, _ := createCustomExecutor(vschema)

	// If auto inc table cannot be found, the table should not be added to vschema.
	_, err := executorExec(executor, "insert into bad_auto(v, name) values (1, 'myname')", nil)
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
			executor, sbc1, sbc2, _ := createLegacyExecutorEnv()

			masterSession.TargetString = tc.targetString
			_, err := executorExec(executor, tc.inputQuery, nil)
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
	executor, _, _, _ := createLegacyExecutorEnv()
	masterSession.TargetString = "TestExecutor[-]"
	_, err := executorExec(executor, insertInput, nil)

	require.EqualError(t, err, "range queries are not allowed for insert statement: TestExecutor[-]")

	masterSession.TargetString = ""
}

func assertQueriesContain(t *testing.T, sql, sbcName string, sbc *sandboxconn.SandboxConn) {
	t.Helper()
	expectedQuery := []*querypb.BoundQuery{{
		Sql:           sql,
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	testQueries(t, sbcName, sbc, expectedQuery)
}

// Prepared statement tests
func TestUpdateEqualWithPrepare(t *testing.T) {
	executor, sbc1, sbc2, sbclookup := createLegacyExecutorEnv()

	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	_, err := executorPrepare(executor, "update music set a = :a0 where id = :id0", map[string]*querypb.BindVariable{
		"a0":  sqltypes.Int64BindVariable(3),
		"id0": sqltypes.Int64BindVariable(2),
	})
	require.NoError(t, err)

	var wantQueries []*querypb.BoundQuery

	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: %+v, want %+v\n", sbclookup.Queries, wantQueries)
	}
	if sbc2.Queries != nil {
		t.Errorf("sbc2.Queries: %+v, want nil\n", sbc2.Queries)
	}
	if sbc1.Queries != nil {
		t.Errorf("sbc1.Queries: %+v, want nil\n", sbc1.Queries)
	}
}
func TestInsertShardedWithPrepare(t *testing.T) {
	executor, sbc1, sbc2, sbclookup := createLegacyExecutorEnv()

	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	_, err := executorPrepare(executor, "insert into user(id, v, name) values (:_Id0, 2, ':_name_0')", map[string]*querypb.BindVariable{
		"_Id0":    sqltypes.Int64BindVariable(1),
		"_name_0": sqltypes.BytesBindVariable([]byte("myname")),
		"__seq0":  sqltypes.Int64BindVariable(1),
	})
	require.NoError(t, err)

	var wantQueries []*querypb.BoundQuery

	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries:\n%+v, want\n%+v\n", sbc1.Queries, wantQueries)
	}
	if sbc2.Queries != nil {
		t.Errorf("sbc2.Queries: %+v, want nil\n", sbc2.Queries)
	}

	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: \n%+v, want \n%+v", sbclookup.Queries, wantQueries)
	}
}

func TestDeleteEqualWithPrepare(t *testing.T) {
	executor, sbc, _, sbclookup := createLegacyExecutorEnv()
	_, err := executorPrepare(executor, "delete from user where id = :id0", map[string]*querypb.BindVariable{
		"id0": sqltypes.Int64BindVariable(1),
	})
	require.NoError(t, err)

	var wantQueries []*querypb.BoundQuery

	if !reflect.DeepEqual(sbc.Queries, wantQueries) {
		t.Errorf("sbc.Queries:\n%+v, want\n%+v\n", sbc.Queries, wantQueries)
	}

	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries:\n%+v, want\n%+v\n", sbclookup.Queries, wantQueries)
	}
}

func TestUpdateLastInsertID(t *testing.T) {
	executor, sbc1, _, _ := createLegacyExecutorEnv()
	executor.normalize = true

	sql := "update user set a = last_insert_id() where id = 1"
	masterSession.LastInsertId = 43
	_, err := executorExec(executor, sql, map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql: "update `user` set a = :__lastInsertId where id = :vtg1",
		BindVariables: map[string]*querypb.BindVariable{
			"__lastInsertId": sqltypes.Uint64BindVariable(43),
			"vtg1":           sqltypes.Int64BindVariable(1)},
	}}

	require.Equal(t, wantQueries, sbc1.Queries)
}

func TestDeleteLookupOwnedEqual(t *testing.T) {
	executor, sbc1, sbc2, _ := createLegacyExecutorEnv()

	sbc1.SetResults([]*sqltypes.Result{
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("uniq_col|keyspace_id", "int64|varbinary"), "1|N±\u0090ɢú\u0016\u009C"),
	})
	_, err := executorExec(executor, "delete from t1 where unq_col = 1", nil)
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
	utils.MustMatch(t, sbc1.Queries, sbc1wantQueries, "")
	utils.MustMatch(t, sbc2.Queries, sbc2wantQueries, "")
}

func TestReservedConnDML(t *testing.T) {
	executor, _, _, sbc := createExecutorEnv()

	logChan := QueryLogger.Subscribe("TestReservedConnDML")
	defer QueryLogger.Unsubscribe(logChan)

	ctx := context.Background()
	session := NewAutocommitSession(&vtgatepb.Session{EnableSystemSettings: true})

	_, err := executor.Execute(ctx, "TestReservedConnDML", session, "use "+KsTestUnsharded, nil)
	require.NoError(t, err)

	wantQueries := []*querypb.BoundQuery{
		{Sql: "select 1 from dual where @@default_week_format != 1", BindVariables: map[string]*querypb.BindVariable{}},
	}
	sbc.SetResults([]*sqltypes.Result{
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("id", "int64"), "1"),
	})
	_, err = executor.Execute(ctx, "TestReservedConnDML", session, "set default_week_format = 1", nil)
	require.NoError(t, err)
	utils.MustMatch(t, wantQueries, sbc.Queries)

	_, err = executor.Execute(ctx, "TestReservedConnDML", session, "begin", nil)
	require.NoError(t, err)

	wantQueries = append(wantQueries,
		&querypb.BoundQuery{Sql: "set @@default_week_format = 1", BindVariables: map[string]*querypb.BindVariable{}},
		&querypb.BoundQuery{Sql: "insert into simple values ()", BindVariables: map[string]*querypb.BindVariable{}})
	_, err = executor.Execute(ctx, "TestReservedConnDML", session, "insert into simple() values ()", nil)
	require.NoError(t, err)
	utils.MustMatch(t, wantQueries, sbc.Queries)

	_, err = executor.Execute(ctx, "TestReservedConnDML", session, "commit", nil)
	require.NoError(t, err)

	_, err = executor.Execute(ctx, "TestReservedConnDML", session, "begin", nil)
	require.NoError(t, err)

	sbc.EphemeralShardErr = mysql.NewSQLError(mysql.CRServerGone, mysql.SSNetError, "connection gone")
	// as the first time the query fails due to connection loss i.e. reserved conn lost. It will be recreated to set statement will be executed again.
	wantQueries = append(wantQueries,
		&querypb.BoundQuery{Sql: "set @@default_week_format = 1", BindVariables: map[string]*querypb.BindVariable{}},
		&querypb.BoundQuery{Sql: "insert into simple values ()", BindVariables: map[string]*querypb.BindVariable{}})
	_, err = executor.Execute(ctx, "TestReservedConnDML", session, "insert into simple() values ()", nil)
	require.NoError(t, err)
	utils.MustMatch(t, wantQueries, sbc.Queries)

	_, err = executor.Execute(ctx, "TestReservedConnDML", session, "commit", nil)
	require.NoError(t, err)
}
