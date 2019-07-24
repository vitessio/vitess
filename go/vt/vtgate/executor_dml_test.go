/*
Copyright 2017 Google Inc.

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

	"golang.org/x/net/context"

	"vitess.io/vitess/go/sqltypes"
	_ "vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vttablet/sandboxconn"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

func TestUpdateEqual(t *testing.T) {
	executor, sbc1, sbc2, sbclookup := createExecutorEnv()

	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	// Update by primary vindex.
	_, err := executorExec(executor, "update user set a=2 where id = 1", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "update user set a = 2 where id = 1 /* vtgate:: keyspace_id:166b40b44aba4bd6 */",
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
	if err != nil {
		t.Error(err)
	}
	wantQueries = []*querypb.BoundQuery{{
		Sql:           "update user set a = 2 where id = 3 /* vtgate:: keyspace_id:4eb190c9a2fa169c */",
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
	if err != nil {
		t.Error(err)
	}
	wantQueries = []*querypb.BoundQuery{{
		Sql: "select user_id from music_user_map where music_id = :music_id",
		BindVariables: map[string]*querypb.BindVariable{
			"music_id": sqltypes.Int64BindVariable(2),
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
	_, err = executorExec(executor, "update user2 set name='myname', lastname='mylastname' where id = 1", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries = []*querypb.BoundQuery{
		{
			Sql:           "select name, lastname from user2 where id = 1 for update",
			BindVariables: map[string]*querypb.BindVariable{},
		},
		{
			Sql:           "update user2 set name = 'myname', lastname = 'mylastname' where id = 1 /* vtgate:: keyspace_id:166b40b44aba4bd6 */",
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
			Sql: "delete from name_lastname_keyspace_id_map where name = :name and lastname = :lastname and keyspace_id = :keyspace_id",
			BindVariables: map[string]*querypb.BindVariable{
				"lastname":    sqltypes.StringBindVariable("foo"),
				"name":        sqltypes.Int32BindVariable(1),
				"keyspace_id": sqltypes.BytesBindVariable([]byte("\026k@\264J\272K\326")),
			},
		},
		{
			Sql: "insert into name_lastname_keyspace_id_map(name, lastname, keyspace_id) values (:name0, :lastname0, :keyspace_id0)",
			BindVariables: map[string]*querypb.BindVariable{
				"name0":        sqltypes.BytesBindVariable([]byte("myname")),
				"lastname0":    sqltypes.BytesBindVariable([]byte("mylastname")),
				"keyspace_id0": sqltypes.BytesBindVariable([]byte("\026k@\264J\272K\326")),
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
			sqltypes.MakeTestFields("a|b|c|d|e|f", "int64|int64|int64|int64|int64|int64"),
			"10|20|30|40|50|60",
		),
	})
	_, err := executorExec(executor, "update user set a=1, b=2, f=4, e=3 where id=1", nil)
	if err != nil {
		t.Fatal(err)
	}
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select a, b, c, d, e, f from user where id = 1 for update",
		BindVariables: map[string]*querypb.BindVariable{},
	}, {
		Sql:           "update user set a = 1, b = 2, f = 4, e = 3 where id = 1 /* vtgate:: keyspace_id:166b40b44aba4bd6 */",
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
		Sql: "insert into music_user_map(from1, from2, user_id) values (:from10, :from20, :user_id0)",
		BindVariables: map[string]*querypb.BindVariable{
			"from10":   sqltypes.Int64BindVariable(1),
			"from20":   sqltypes.Int64BindVariable(2),
			"user_id0": sqltypes.Uint64BindVariable(1),
		},
	}, {
		Sql: "delete from music_user_map where from1 = :from1 and from2 = :from2 and user_id = :user_id",
		BindVariables: map[string]*querypb.BindVariable{
			"from1":   sqltypes.Int64BindVariable(50),
			"from2":   sqltypes.Int64BindVariable(60),
			"user_id": sqltypes.Uint64BindVariable(1),
		},
	}, {
		Sql: "insert into music_user_map(from1, from2, user_id) values (:from10, :from20, :user_id0)",
		BindVariables: map[string]*querypb.BindVariable{
			"from10":   sqltypes.Int64BindVariable(3),
			"from20":   sqltypes.Int64BindVariable(4),
			"user_id0": sqltypes.Uint64BindVariable(1),
		},
	}}

	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries:\n%+v, want\n%+v\n", sbclookup.Queries, wantQueries)
	}
}

func TestUpdateComments(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()

	_, err := executorExec(executor, "update user set a=2 where id = 1 /* trailing */", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "update user set a = 2 where id = 1 /* vtgate:: keyspace_id:166b40b44aba4bd6 */ /* trailing */",
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
	executor, sbc1, sbc2, _ := createExecutorEnv()

	executor.normalize = true
	_, err := executorExec(executor, "/* leading */ update user set a=2 where id = 1 /* trailing */", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries := []*querypb.BoundQuery{{
		Sql: "/* leading */ update user set a = :vtg1 where id = :vtg2 /* vtgate:: keyspace_id:166b40b44aba4bd6 */ /* trailing */",
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
	if err != nil {
		t.Error(err)
	}
	wantQueries = []*querypb.BoundQuery{{
		Sql: "/* leading */ update user set a = :vtg1 where id = :vtg2 /* trailing *//* vtgate:: filtered_replication_unfriendly */",
		BindVariables: map[string]*querypb.BindVariable{
			"vtg1": sqltypes.TestBindVariable(int64(2)),
			"vtg2": sqltypes.TestBindVariable(int64(1)),
		},
	}}
	if sbc1.Queries != nil {
		t.Errorf("sbc1.Queries: %+v, want nil\n", sbc1.Queries)
	}
	if !reflect.DeepEqual(sbc2.Queries, wantQueries) {
		t.Errorf("sbc2.Queries: %+v, want %+v\n", sbc2.Queries, wantQueries)
	}
	sbc2.Queries = nil
	masterSession.TargetString = ""
}

func TestDeleteEqual(t *testing.T) {
	executor, sbc, _, sbclookup := createExecutorEnv()

	sbc.SetResults([]*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "name", Type: sqltypes.VarChar},
		},
		RowsAffected: 1,
		InsertID:     0,
		Rows: [][]sqltypes.Value{{
			sqltypes.NewVarChar("myname"),
		}},
	}})
	_, err := executorExec(executor, "delete from user where id = 1", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select name from user where id = 1 for update",
		BindVariables: map[string]*querypb.BindVariable{},
	}, {
		Sql:           "delete from user where id = 1 /* vtgate:: keyspace_id:166b40b44aba4bd6 */",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	if !reflect.DeepEqual(sbc.Queries, wantQueries) {
		t.Errorf("sbc.Queries:\n%+v, want\n%+v\n", sbc.Queries, wantQueries)
	}

	wantQueries = []*querypb.BoundQuery{{
		Sql: "delete from name_user_map where name = :name and user_id = :user_id",
		BindVariables: map[string]*querypb.BindVariable{
			"user_id": sqltypes.Uint64BindVariable(1),
			"name":    sqltypes.StringBindVariable("myname"),
		},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries:\n%+v, want\n%+v\n", sbclookup.Queries, wantQueries)
	}

	sbc.Queries = nil
	sbclookup.Queries = nil
	sbc.SetResults([]*sqltypes.Result{{}})
	_, err = executorExec(executor, "delete from user where id = 1", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries = []*querypb.BoundQuery{{
		Sql:           "select name from user where id = 1 for update",
		BindVariables: map[string]*querypb.BindVariable{},
	}, {
		Sql:           "delete from user where id = 1 /* vtgate:: keyspace_id:166b40b44aba4bd6 */",
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
	if err != nil {
		t.Error(err)
	}
	wantQueries = []*querypb.BoundQuery{{
		Sql: "select user_id from music_user_map where music_id = :music_id",
		BindVariables: map[string]*querypb.BindVariable{
			"music_id": sqltypes.Int64BindVariable(1),
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
	if err != nil {
		t.Error(err)
	}
	wantQueries = []*querypb.BoundQuery{{
		Sql:           "delete from user_extra where user_id = 1 /* vtgate:: keyspace_id:166b40b44aba4bd6 */",
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
	_, err = executorExec(executor, "delete from user2 where id = 1", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries = []*querypb.BoundQuery{
		{
			Sql:           "select name, lastname from user2 where id = 1 for update",
			BindVariables: map[string]*querypb.BindVariable{},
		},
		{
			Sql:           "delete from user2 where id = 1 /* vtgate:: keyspace_id:166b40b44aba4bd6 */",
			BindVariables: map[string]*querypb.BindVariable{},
		},
	}
	if !reflect.DeepEqual(sbc.Queries, wantQueries) {
		t.Errorf("sbc.Queries: %+v, want %+v\n", sbc.Queries, wantQueries)
	}

	wantQueries = []*querypb.BoundQuery{
		{
			Sql: "delete from name_lastname_keyspace_id_map where name = :name and lastname = :lastname and keyspace_id = :keyspace_id",
			BindVariables: map[string]*querypb.BindVariable{
				"lastname":    sqltypes.StringBindVariable("foo"),
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
	executor, sbc1, sbc2, _ := createExecutorEnv()
	_, err := executorExec(executor, "update user_extra set col = 2", nil)
	if err != nil {
		t.Error(err)
	}
	// Queries get annotatted.
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "update user_extra set col = 2/* vtgate:: filtered_replication_unfriendly */",
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
	executor, sbc1, sbc2, _ := createExecutorEnv()
	_, err := executorExec(executor, "delete from user_extra", nil)
	if err != nil {
		t.Error(err)
	}
	// Queries get annotatted.
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "delete from user_extra/* vtgate:: filtered_replication_unfriendly */",
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
	executor, sbc1, sbc2, _ := createExecutorEnv()
	// This query is not supported in v3, so we know for sure is taking the DeleteByDestination route
	_, err := executorExec(executor, "delete from `TestExecutor[-]`.user_extra limit 10", nil)
	if err != nil {
		t.Error(err)
	}
	// Queries get annotatted.
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "delete from user_extra limit 10/* vtgate:: filtered_replication_unfriendly */",
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
	executor, sbc, _, sbclookup := createExecutorEnv()

	sbc.SetResults([]*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "name", Type: sqltypes.VarChar},
		},
		RowsAffected: 1,
		InsertID:     0,
		Rows: [][]sqltypes.Value{{
			sqltypes.NewVarChar("myname"),
		}},
	}})
	_, err := executorExec(executor, "delete from user where id = 1 /* trailing */", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select name from user where id = 1 for update /* trailing */",
		BindVariables: map[string]*querypb.BindVariable{},
	}, {
		Sql:           "delete from user where id = 1 /* vtgate:: keyspace_id:166b40b44aba4bd6 */ /* trailing */",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	if !reflect.DeepEqual(sbc.Queries, wantQueries) {
		t.Errorf("sbc.Queries:\n%+v, want\n%+v\n", sbc.Queries, wantQueries)
	}

	wantQueries = []*querypb.BoundQuery{{
		Sql: "delete from name_user_map where name = :name and user_id = :user_id /* trailing */",
		BindVariables: map[string]*querypb.BindVariable{
			"user_id": sqltypes.Uint64BindVariable(1),
			"name":    sqltypes.StringBindVariable("myname"),
		},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries:\n%+v, want\n%+v\n", sbclookup.Queries, wantQueries)
	}
}

func TestInsertSharded(t *testing.T) {
	executor, sbc1, sbc2, sbclookup := createExecutorEnv()

	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	_, err := executorExec(executor, "insert into user(id, v, name) values (1, 2, 'myname')", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries := []*querypb.BoundQuery{{
		Sql: "insert into user(id, v, name) values (:_Id0, 2, :_name0) /* vtgate:: keyspace_id:166b40b44aba4bd6 */",
		BindVariables: map[string]*querypb.BindVariable{
			"_Id0":   sqltypes.Int64BindVariable(1),
			"_name0": sqltypes.BytesBindVariable([]byte("myname")),
			"__seq0": sqltypes.Int64BindVariable(1),
		},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries:\n%+v, want\n%+v\n", sbc1.Queries, wantQueries)
	}
	if sbc2.Queries != nil {
		t.Errorf("sbc2.Queries: %+v, want nil\n", sbc2.Queries)
	}
	wantQueries = []*querypb.BoundQuery{{
		Sql: "insert into name_user_map(name, user_id) values (:name0, :user_id0)",
		BindVariables: map[string]*querypb.BindVariable{
			"name0":    sqltypes.BytesBindVariable([]byte("myname")),
			"user_id0": sqltypes.Uint64BindVariable(1),
		},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: \n%+v, want \n%+v", sbclookup.Queries, wantQueries)
	}

	testQueryLog(t, logChan, "VindexCreate", "INSERT", "insert into name_user_map(name, user_id) values(:name0, :user_id0)", 1)
	testQueryLog(t, logChan, "TestExecute", "INSERT", "insert into user(id, v, name) values (1, 2, 'myname')", 1)

	sbc1.Queries = nil
	sbclookup.Queries = nil
	_, err = executorExec(executor, "insert into user(id, v, name) values (3, 2, 'myname2')", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries = []*querypb.BoundQuery{{
		Sql: "insert into user(id, v, name) values (:_Id0, 2, :_name0) /* vtgate:: keyspace_id:4eb190c9a2fa169c */",
		BindVariables: map[string]*querypb.BindVariable{
			"_Id0":   sqltypes.Int64BindVariable(3),
			"__seq0": sqltypes.Int64BindVariable(3),
			"_name0": sqltypes.BytesBindVariable([]byte("myname2")),
		},
	}}
	if !reflect.DeepEqual(sbc2.Queries, wantQueries) {
		t.Errorf("sbc2.Queries:\n%+v, want\n%+v\n", sbc2.Queries, wantQueries)
	}
	if sbc1.Queries != nil {
		t.Errorf("sbc1.Queries: %+v, want nil\n", sbc1.Queries)
	}
	wantQueries = []*querypb.BoundQuery{{
		Sql: "insert into name_user_map(name, user_id) values (:name0, :user_id0)",
		BindVariables: map[string]*querypb.BindVariable{
			"name0":    sqltypes.BytesBindVariable([]byte("myname2")),
			"user_id0": sqltypes.Uint64BindVariable(3),
		},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: \n%+v, want \n%+v\n", sbclookup.Queries, wantQueries)
	}

	sbc1.Queries = nil
	_, err = executorExec(executor, "insert into user2(id, name, lastname) values (2, 'myname', 'mylastname')", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries = []*querypb.BoundQuery{{
		Sql: "insert into user2(id, name, lastname) values (:_id0, :_name0, :_lastname0) /* vtgate:: keyspace_id:06e7ea22ce92708f */",
		BindVariables: map[string]*querypb.BindVariable{
			"_id0":       sqltypes.Int64BindVariable(2),
			"_name0":     sqltypes.BytesBindVariable([]byte("myname")),
			"_lastname0": sqltypes.BytesBindVariable([]byte("mylastname")),
		},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries:\n%+v, want\n%+v\n", sbc1.Queries, wantQueries)
	}
}

func TestInsertShardedKeyrange(t *testing.T) {
	executor, _, _, _ := createExecutorEnv()

	// If a unique vindex returns a keyrange, we fail the insert
	_, err := executorExec(executor, "insert into keyrange_table(krcol_unique, krcol) values(1, 1)", nil)
	want := "execInsertSharded: getInsertShardedRoute: could not map INT64(1) to a unique keyspace id: DestinationKeyRange(-10)"
	if err == nil || err.Error() != want {
		t.Errorf("executorExec error: %v, want %s", err, want)
	}
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
	if err != nil {
		t.Error(err)
	}
	wantQueries := []*querypb.BoundQuery{{
		Sql: "insert into user(id, v, name) values (:_Id0, 2, :_name0) /* vtgate:: keyspace_id:166b40b44aba4bd6 */",
		BindVariables: map[string]*querypb.BindVariable{
			"_Id0":   sqltypes.Int64BindVariable(1),
			"_name0": sqltypes.BytesBindVariable([]byte("myname")),
			"__seq0": sqltypes.Int64BindVariable(1),
		},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries:\n%+v, want\n%+v\n", sbc1.Queries, wantQueries)
	}
	if sbc2.Queries != nil {
		t.Errorf("sbc2.Queries: %+v, want nil\n", sbc2.Queries)
	}
	wantQueries = []*querypb.BoundQuery{{
		Sql: "insert into name_user_map(name, user_id) values (:name0, :user_id0) on duplicate key update name = values(name), user_id = values(user_id)",
		BindVariables: map[string]*querypb.BindVariable{
			"name0":    sqltypes.BytesBindVariable([]byte("myname")),
			"user_id0": sqltypes.Uint64BindVariable(1),
		},
	}}
	// autocommit should go as ExecuteBatch
	if !reflect.DeepEqual(sbclookup.BatchQueries[0], wantQueries) {
		t.Errorf("sbclookup.BatchQueries[0]: \n%+v, want \n%+v", sbclookup.BatchQueries[0], wantQueries)
	}
}

func TestInsertShardedIgnore(t *testing.T) {
	executor, sbc1, sbc2, sbclookup := createExecutorEnv()

	// Build the sequence of responses for sbclookup. This should
	// match the sequence of queries we validate below.
	fields := sqltypes.MakeTestFields("a", "int64")
	sbclookup.SetResults([]*sqltypes.Result{
		// select music_id 1
		sqltypes.MakeTestResult(fields, "1"),
		// select music_id 2
		{},
		// select music_id 3
		sqltypes.MakeTestResult(fields, "1"),
		// select music_id 4
		sqltypes.MakeTestResult(fields, "1"),
		// select music_id 5
		sqltypes.MakeTestResult(fields, "1"),
		// select music_id 6
		sqltypes.MakeTestResult(fields, "3"),
		// insert ins_lookup
		{},
		// select ins_lookup 1
		sqltypes.MakeTestResult(fields, "1"),
		// select ins_lookup 3
		{},
		// select ins_lookup 4
		sqltypes.MakeTestResult(fields, "4"),
		// select ins_lookup 5
		sqltypes.MakeTestResult(fields, "5"),
		// select ins_lookup 6
		sqltypes.MakeTestResult(fields, "6"),
	})
	// First row: first shard.
	// Second row: will fail because primary vindex will fail to map.
	// Third row: will fail because verification will fail on owned vindex after Create.
	// Fourth row: will fail because verification will fail on unowned hash vindex.
	// Fifth row: first shard.
	// Sixth row: second shard (because 3 hash maps to 40-60).
	query := "insert ignore into insert_ignore_test(pv, owned, verify) values (1, 1, 1), (2, 2, 2), (3, 3, 1), (4, 4, 4), (5, 5, 1), (6, 6, 3)"
	_, err := executorExec(executor, query, nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries := []*querypb.BoundQuery{{
		Sql: "insert ignore into insert_ignore_test(pv, owned, verify) values (:_pv0, :_owned0, :_verify0),(:_pv4, :_owned4, :_verify4) /* vtgate:: keyspace_id:166b40b44aba4bd6,166b40b44aba4bd6 */",
		BindVariables: map[string]*querypb.BindVariable{
			"_pv0":     sqltypes.Int64BindVariable(1),
			"_pv2":     sqltypes.Int64BindVariable(3),
			"_pv3":     sqltypes.Int64BindVariable(4),
			"_pv4":     sqltypes.Int64BindVariable(5),
			"_pv5":     sqltypes.Int64BindVariable(6),
			"_owned0":  sqltypes.Int64BindVariable(1),
			"_owned2":  sqltypes.Int64BindVariable(3),
			"_owned3":  sqltypes.Int64BindVariable(4),
			"_owned4":  sqltypes.Int64BindVariable(5),
			"_owned5":  sqltypes.Int64BindVariable(6),
			"_verify0": sqltypes.Int64BindVariable(1),
			"_verify4": sqltypes.Int64BindVariable(1),
			"_verify5": sqltypes.Int64BindVariable(3),
		},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries:\n%+v, want\n%+v\n", sbc1.Queries, wantQueries)
	}
	wantQueries = []*querypb.BoundQuery{{
		Sql: "insert ignore into insert_ignore_test(pv, owned, verify) values (:_pv5, :_owned5, :_verify5) /* vtgate:: keyspace_id:4eb190c9a2fa169c */",
		BindVariables: map[string]*querypb.BindVariable{
			"_pv0":     sqltypes.Int64BindVariable(1),
			"_pv2":     sqltypes.Int64BindVariable(3),
			"_pv3":     sqltypes.Int64BindVariable(4),
			"_pv4":     sqltypes.Int64BindVariable(5),
			"_pv5":     sqltypes.Int64BindVariable(6),
			"_owned0":  sqltypes.Int64BindVariable(1),
			"_owned2":  sqltypes.Int64BindVariable(3),
			"_owned3":  sqltypes.Int64BindVariable(4),
			"_owned4":  sqltypes.Int64BindVariable(5),
			"_owned5":  sqltypes.Int64BindVariable(6),
			"_verify0": sqltypes.Int64BindVariable(1),
			"_verify4": sqltypes.Int64BindVariable(1),
			"_verify5": sqltypes.Int64BindVariable(3),
		},
	}}
	if !reflect.DeepEqual(sbc2.Queries, wantQueries) {
		t.Errorf("sbc2.Queries:\n%+v, want\n%+v\n", sbc2.Queries, wantQueries)
	}
	wantQueries = []*querypb.BoundQuery{{
		Sql: "select user_id from music_user_map where music_id = :music_id",
		BindVariables: map[string]*querypb.BindVariable{
			"music_id": sqltypes.Int64BindVariable(1),
		},
	}, {
		Sql: "select user_id from music_user_map where music_id = :music_id",
		BindVariables: map[string]*querypb.BindVariable{
			"music_id": sqltypes.Int64BindVariable(2),
		},
	}, {
		Sql: "select user_id from music_user_map where music_id = :music_id",
		BindVariables: map[string]*querypb.BindVariable{
			"music_id": sqltypes.Int64BindVariable(3),
		},
	}, {
		Sql: "select user_id from music_user_map where music_id = :music_id",
		BindVariables: map[string]*querypb.BindVariable{
			"music_id": sqltypes.Int64BindVariable(4),
		},
	}, {
		Sql: "select user_id from music_user_map where music_id = :music_id",
		BindVariables: map[string]*querypb.BindVariable{
			"music_id": sqltypes.Int64BindVariable(5),
		},
	}, {
		Sql: "select user_id from music_user_map where music_id = :music_id",
		BindVariables: map[string]*querypb.BindVariable{
			"music_id": sqltypes.Int64BindVariable(6),
		},
	}, {
		Sql: "insert ignore into ins_lookup(fromcol, tocol) values (:fromcol0, :tocol0), (:fromcol1, :tocol1), (:fromcol2, :tocol2), (:fromcol3, :tocol3), (:fromcol4, :tocol4)",
		BindVariables: map[string]*querypb.BindVariable{
			"fromcol0": sqltypes.Int64BindVariable(1),
			"tocol0":   sqltypes.Uint64BindVariable(1),
			"fromcol1": sqltypes.Int64BindVariable(3),
			"tocol1":   sqltypes.Uint64BindVariable(1),
			"fromcol2": sqltypes.Int64BindVariable(4),
			"tocol2":   sqltypes.Uint64BindVariable(1),
			"fromcol3": sqltypes.Int64BindVariable(5),
			"tocol3":   sqltypes.Uint64BindVariable(1),
			"fromcol4": sqltypes.Int64BindVariable(6),
			"tocol4":   sqltypes.Uint64BindVariable(3),
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
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(qr, &sqltypes.Result{}) {
		t.Errorf("qr: %v, want empty result", qr)
	}
	if sbc1.Queries != nil {
		t.Errorf("sbc1.Queries: %+v, want nil\n", sbc1.Queries)
	}
	if sbc2.Queries != nil {
		t.Errorf("sbc2.Queries: %+v, want nil\n", sbc2.Queries)
	}
	wantQueries = []*querypb.BoundQuery{{
		Sql: "select user_id from music_user_map where music_id = :music_id",
		BindVariables: map[string]*querypb.BindVariable{
			"music_id": sqltypes.Int64BindVariable(1),
		},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: \n%+v, want \n%+v", sbclookup.Queries, wantQueries)
	}
}

func TestInsertOnDupKey(t *testing.T) {
	// This test just sanity checks that the statement is getting passed through
	// correctly. The full set of use cases are covered by TestInsertShardedIgnore.
	executor, sbc1, sbc2, sbclookup := createExecutorEnv()
	query := "insert into insert_ignore_test(pv, owned, verify) values (1, 1, 1) on duplicate key update col = 2"
	_, err := executorExec(executor, query, nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries := []*querypb.BoundQuery{{
		Sql: "insert into insert_ignore_test(pv, owned, verify) values (:_pv0, :_owned0, :_verify0) on duplicate key update col = 2 /* vtgate:: keyspace_id:166b40b44aba4bd6 */",
		BindVariables: map[string]*querypb.BindVariable{
			"_pv0":     sqltypes.Int64BindVariable(1),
			"_owned0":  sqltypes.Int64BindVariable(1),
			"_verify0": sqltypes.Int64BindVariable(1),
		},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries:\n%+v, want\n%+v\n", sbc1.Queries, wantQueries)
	}
	if sbc2.Queries != nil {
		t.Errorf("sbc2.Queries: %+v, want nil\n", sbc2.Queries)
	}
	wantQueries = []*querypb.BoundQuery{{
		Sql: "select user_id from music_user_map where music_id = :music_id",
		BindVariables: map[string]*querypb.BindVariable{
			"music_id": sqltypes.Int64BindVariable(1),
		},
	}, {
		Sql: "insert ignore into ins_lookup(fromcol, tocol) values (:fromcol0, :tocol0)",
		BindVariables: map[string]*querypb.BindVariable{
			"fromcol0": sqltypes.Int64BindVariable(1),
			"tocol0":   sqltypes.Uint64BindVariable(1),
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

func TestInsertComments(t *testing.T) {
	executor, sbc1, sbc2, sbclookup := createExecutorEnv()

	_, err := executorExec(executor, "insert into user(id, v, name) values (1, 2, 'myname') /* trailing */", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries := []*querypb.BoundQuery{{
		Sql: "insert into user(id, v, name) values (:_Id0, 2, :_name0) /* vtgate:: keyspace_id:166b40b44aba4bd6 */ /* trailing */",
		BindVariables: map[string]*querypb.BindVariable{
			"_Id0":   sqltypes.Int64BindVariable(1),
			"_name0": sqltypes.BytesBindVariable([]byte("myname")),
			"__seq0": sqltypes.Int64BindVariable(1),
		},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries:\n%+v, want\n%+v\n", sbc1.Queries, wantQueries)
	}
	if sbc2.Queries != nil {
		t.Errorf("sbc2.Queries: %+v, want nil\n", sbc2.Queries)
	}
	wantQueries = []*querypb.BoundQuery{{
		Sql: "insert into name_user_map(name, user_id) values (:name0, :user_id0) /* trailing */",
		BindVariables: map[string]*querypb.BindVariable{
			"name0":    sqltypes.BytesBindVariable([]byte("myname")),
			"user_id0": sqltypes.Uint64BindVariable(1),
		},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: \n%+v, want \n%+v", sbclookup.Queries, wantQueries)
	}
}

func TestInsertGeneratorSharded(t *testing.T) {
	executor, sbc, _, sbclookup := createExecutorEnv()

	sbclookup.SetResults([]*sqltypes.Result{{
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(1),
		}},
		RowsAffected: 1,
		InsertID:     1,
	}})
	result, err := executorExec(executor, "insert into user(v, name) values (2, 'myname')", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries := []*querypb.BoundQuery{{
		Sql: "insert into user(v, name, id) values (2, :_name0, :_Id0) /* vtgate:: keyspace_id:166b40b44aba4bd6 */",
		BindVariables: map[string]*querypb.BindVariable{
			"_Id0":   sqltypes.Int64BindVariable(1),
			"__seq0": sqltypes.Int64BindVariable(1),
			"_name0": sqltypes.BytesBindVariable([]byte("myname")),
		},
	}}
	if !reflect.DeepEqual(sbc.Queries, wantQueries) {
		t.Errorf("sbc.Queries: %+v, want %+v\n", sbc.Queries, wantQueries)
	}
	wantQueries = []*querypb.BoundQuery{{
		Sql:           "select next :n values from user_seq",
		BindVariables: map[string]*querypb.BindVariable{"n": sqltypes.Int64BindVariable(1)},
	}, {
		Sql: "insert into name_user_map(name, user_id) values (:name0, :user_id0)",
		BindVariables: map[string]*querypb.BindVariable{
			"name0":    sqltypes.BytesBindVariable([]byte("myname")),
			"user_id0": sqltypes.Uint64BindVariable(1),
		},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: \n%#v, want \n%#v\n", sbclookup.Queries, wantQueries)
	}
	wantResult := *sandboxconn.SingleRowResult
	wantResult.InsertID = 1
	if !result.Equal(&wantResult) {
		t.Errorf("result: %+v, want %+v", result, &wantResult)
	}
}

func TestInsertAutoincSharded(t *testing.T) {
	router, sbc, _, _ := createExecutorEnv()

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
	if err != nil {
		t.Error(err)
	}
	wantQueries := []*querypb.BoundQuery{{
		Sql: "insert into user_extra(user_id) values (:_user_id0) /* vtgate:: keyspace_id:06e7ea22ce92708f */",
		BindVariables: map[string]*querypb.BindVariable{
			"_user_id0": sqltypes.Int64BindVariable(2),
		},
	}}
	if !reflect.DeepEqual(sbc.Queries, wantQueries) {
		t.Errorf("sbc.Queries:\n%+v, want\n%+v\n", sbc.Queries, wantQueries)
	}
	if !result.Equal(wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}
}

func TestInsertGeneratorUnsharded(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()
	result, err := executorExec(executor, "insert into main1(id, name) values (null, 'myname')", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select next :n values from user_seq",
		BindVariables: map[string]*querypb.BindVariable{"n": sqltypes.Int64BindVariable(1)},
	}, {
		Sql: "insert into main1(id, name) values (:__seq0, 'myname')",
		BindVariables: map[string]*querypb.BindVariable{
			"__seq0": sqltypes.Int64BindVariable(1),
		},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: \n%#v, want \n%#v\n", sbclookup.Queries, wantQueries)
	}
	wantResult := *sandboxconn.SingleRowResult
	wantResult.InsertID = 1
	if !result.Equal(&wantResult) {
		t.Errorf("result: %+v, want %+v", result, &wantResult)
	}
}

func TestInsertAutoincUnsharded(t *testing.T) {
	router, _, _, sbclookup := createExecutorEnv()

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
	if err != nil {
		t.Error(err)
	}
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
	executor, sbc, _, sbclookup := createExecutorEnv()

	_, err := executorExec(executor, "insert into music(user_id, id) values (2, 3)", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries := []*querypb.BoundQuery{{
		Sql: "insert into music(user_id, id) values (:_user_id0, :_id0) /* vtgate:: keyspace_id:06e7ea22ce92708f */",
		BindVariables: map[string]*querypb.BindVariable{
			"_user_id0": sqltypes.Int64BindVariable(2),
			"_id0":      sqltypes.Int64BindVariable(3),
			"__seq0":    sqltypes.Int64BindVariable(3),
		},
	}}
	if !reflect.DeepEqual(sbc.Queries, wantQueries) {
		t.Errorf("sbc.Queries:\n%+v, want\n%+v\n", sbc.Queries, wantQueries)
	}
	wantQueries = []*querypb.BoundQuery{{
		Sql: "insert into music_user_map(music_id, user_id) values (:music_id0, :user_id0)",
		BindVariables: map[string]*querypb.BindVariable{
			"music_id0": sqltypes.Int64BindVariable(3),
			"user_id0":  sqltypes.Uint64BindVariable(2),
		},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: %+v, want %+v\n", sbclookup.Queries, wantQueries)
	}
}

func TestInsertLookupOwnedGenerator(t *testing.T) {
	executor, sbc, _, sbclookup := createExecutorEnv()

	sbclookup.SetResults([]*sqltypes.Result{{
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(4),
		}},
		RowsAffected: 1,
		InsertID:     1,
	}})
	result, err := executorExec(executor, "insert into music(user_id) values (2)", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries := []*querypb.BoundQuery{{
		Sql: "insert into music(user_id, id) values (:_user_id0, :_id0) /* vtgate:: keyspace_id:06e7ea22ce92708f */",
		BindVariables: map[string]*querypb.BindVariable{
			"_user_id0": sqltypes.Int64BindVariable(2),
			"_id0":      sqltypes.Int64BindVariable(4),
			"__seq0":    sqltypes.Int64BindVariable(4),
		},
	}}
	if !reflect.DeepEqual(sbc.Queries, wantQueries) {
		t.Errorf("sbc.Queries:\n%+v, want\n%+v\n", sbc.Queries, wantQueries)
	}
	wantQueries = []*querypb.BoundQuery{{
		Sql:           "select next :n values from user_seq",
		BindVariables: map[string]*querypb.BindVariable{"n": sqltypes.Int64BindVariable(1)},
	}, {
		Sql: "insert into music_user_map(music_id, user_id) values (:music_id0, :user_id0)",
		BindVariables: map[string]*querypb.BindVariable{
			"music_id0": sqltypes.Int64BindVariable(4),
			"user_id0":  sqltypes.Uint64BindVariable(2),
		},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries:\n%+v, want\n%+v\n", sbclookup.Queries, wantQueries)
	}
	wantResult := *sandboxconn.SingleRowResult
	wantResult.InsertID = 4
	if !result.Equal(&wantResult) {
		t.Errorf("result:\n%+v, want\n%+v", result, &wantResult)
	}
}

func TestInsertLookupUnowned(t *testing.T) {
	executor, sbc, _, sbclookup := createExecutorEnv()

	_, err := executorExec(executor, "insert into music_extra(user_id, music_id) values (2, 3)", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries := []*querypb.BoundQuery{{
		Sql: "insert into music_extra(user_id, music_id) values (:_user_id0, :_music_id0) /* vtgate:: keyspace_id:06e7ea22ce92708f */",
		BindVariables: map[string]*querypb.BindVariable{
			"_user_id0":  sqltypes.Int64BindVariable(2),
			"_music_id0": sqltypes.Int64BindVariable(3),
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
	executor, sbc, _, sbclookup := createExecutorEnv()

	_, err := executorExec(executor, "insert into music_extra_reversed(music_id) values (3)", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries := []*querypb.BoundQuery{{
		Sql: "insert into music_extra_reversed(music_id, user_id) values (:_music_id0, :_user_id0) /* vtgate:: keyspace_id:166b40b44aba4bd6 */",
		BindVariables: map[string]*querypb.BindVariable{
			"_user_id0":  sqltypes.Uint64BindVariable(1),
			"_music_id0": sqltypes.Int64BindVariable(3),
		},
	}}
	if !reflect.DeepEqual(sbc.Queries, wantQueries) {
		t.Errorf("sbc.Queries:\n%+v, want\n%+v\n", sbc.Queries, wantQueries)
	}
	wantQueries = []*querypb.BoundQuery{{
		Sql: "select user_id from music_user_map where music_id = :music_id",
		BindVariables: map[string]*querypb.BindVariable{
			"music_id": sqltypes.Int64BindVariable(3),
		},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: %+v, want %+v\n", sbclookup.Queries, wantQueries)
	}
}

// If a statement gets broken up into two, and the first one fails,
// then an error should be returned normally.
func TestInsertPartialFail1(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()

	// Make the first DML fail, there should be no rollback.
	sbclookup.MustFailCodes[vtrpcpb.Code_INVALID_ARGUMENT] = 1

	_, err := executor.Execute(
		context.Background(),
		"TestExecute",
		NewSafeSession(&vtgatepb.Session{InTransaction: true}),
		"insert into user(id, v, name) values (1, 2, 'myname')",
		nil,
	)
	want := "execInsertSharded:"
	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Errorf("insert first DML fail: %v, must start with %s", err, want)
	}
}

// If a statement gets broken up into two, and the second one fails
// after successful execution of the first, then the transaction must
// be rolled back due to partial execution.
func TestInsertPartialFail2(t *testing.T) {
	executor, sbc1, _, _ := createExecutorEnv()

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
	executor, sbc1, sbc2, sbclookup := createExecutorEnv()

	_, err := executorExec(executor, "insert into user(id, v, name) values (1, 1, 'myname1'),(3, 3, 'myname3')", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries1 := []*querypb.BoundQuery{{
		Sql: "insert into user(id, v, name) values (:_Id0, 1, :_name0) /* vtgate:: keyspace_id:166b40b44aba4bd6 */",
		BindVariables: map[string]*querypb.BindVariable{
			"_Id0":   sqltypes.Int64BindVariable(1),
			"_name0": sqltypes.BytesBindVariable([]byte("myname1")),
			"__seq0": sqltypes.Int64BindVariable(1),
			"_Id1":   sqltypes.Int64BindVariable(3),
			"_name1": sqltypes.BytesBindVariable([]byte("myname3")),
			"__seq1": sqltypes.Int64BindVariable(3),
		},
	}}

	wantQueries2 := []*querypb.BoundQuery{{
		Sql: "insert into user(id, v, name) values (:_Id1, 3, :_name1) /* vtgate:: keyspace_id:4eb190c9a2fa169c */",
		BindVariables: map[string]*querypb.BindVariable{
			"_Id0":   sqltypes.Int64BindVariable(1),
			"_name0": sqltypes.BytesBindVariable([]byte("myname1")),
			"__seq0": sqltypes.Int64BindVariable(1),
			"_Id1":   sqltypes.Int64BindVariable(3),
			"_name1": sqltypes.BytesBindVariable([]byte("myname3")),
			"__seq1": sqltypes.Int64BindVariable(3),
		},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries1) {
		t.Errorf("sbc1.Queries:\n%+v, want\n%+v\n", sbc1.Queries, wantQueries1)
	}

	if !reflect.DeepEqual(sbc2.Queries, wantQueries2) {
		t.Errorf("sbc2.Queries:\n%+v, want\n%+v\n", sbc2.Queries, wantQueries2)
	}

	wantQueries1 = []*querypb.BoundQuery{{
		Sql: "insert into name_user_map(name, user_id) values (:name0, :user_id0), (:name1, :user_id1)",
		BindVariables: map[string]*querypb.BindVariable{
			"name0":    sqltypes.BytesBindVariable([]byte("myname1")),
			"user_id0": sqltypes.Uint64BindVariable(1),
			"name1":    sqltypes.BytesBindVariable([]byte("myname3")),
			"user_id1": sqltypes.Uint64BindVariable(3),
		},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries1) {
		t.Errorf("sbclookup.Queries: \n%+v, want \n%+v", sbclookup.Queries, wantQueries1)
	}

	sbc1.Queries = nil
	sbclookup.Queries = nil
	sbc2.Queries = nil
	_, err = executorExec(executor, "insert into user(id, v, name) values (1, 1, 'myname1'),(2, 2, 'myname2')", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries := []*querypb.BoundQuery{{
		Sql: "insert into user(id, v, name) values (:_Id0, 1, :_name0),(:_Id1, 2, :_name1) /* vtgate:: keyspace_id:166b40b44aba4bd6,06e7ea22ce92708f */",
		BindVariables: map[string]*querypb.BindVariable{
			"_Id0":   sqltypes.Int64BindVariable(1),
			"__seq0": sqltypes.Int64BindVariable(1),
			"_name0": sqltypes.BytesBindVariable([]byte("myname1")),
			"_Id1":   sqltypes.Int64BindVariable(2),
			"__seq1": sqltypes.Int64BindVariable(2),
			"_name1": sqltypes.BytesBindVariable([]byte("myname2")),
		},
	}}

	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries:\n%+v, want\n%+v\n", sbc1.Queries, wantQueries)
	}
	if sbc2.Queries != nil {
		t.Errorf("sbc2.Queries: %+v, want nil\n", sbc2.Queries)
	}
	wantQueries = []*querypb.BoundQuery{{
		Sql: "insert into name_user_map(name, user_id) values (:name0, :user_id0), (:name1, :user_id1)",
		BindVariables: map[string]*querypb.BindVariable{
			"name0":    sqltypes.BytesBindVariable([]byte("myname1")),
			"user_id0": sqltypes.Uint64BindVariable(1),
			"name1":    sqltypes.BytesBindVariable([]byte("myname2")),
			"user_id1": sqltypes.Uint64BindVariable(2),
		},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: \n%+v, want \n%+v\n", sbclookup.Queries, wantQueries)
	}

	// Insert multiple rows in a multi column vindex
	sbc1.Queries = nil
	sbclookup.Queries = nil
	sbc2.Queries = nil
	_, err = executorExec(executor, "insert into user2(id, name, lastname) values (2, 'myname', 'mylastname'), (3, 'myname2', 'mylastname2')", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries = []*querypb.BoundQuery{{
		Sql: "insert into user2(id, name, lastname) values (:_id0, :_name0, :_lastname0) /* vtgate:: keyspace_id:06e7ea22ce92708f */",
		BindVariables: map[string]*querypb.BindVariable{
			"_id0":       sqltypes.Int64BindVariable(2),
			"_name0":     sqltypes.BytesBindVariable([]byte("myname")),
			"_lastname0": sqltypes.BytesBindVariable([]byte("mylastname")),
			"_id1":       sqltypes.Int64BindVariable(3),
			"_name1":     sqltypes.BytesBindVariable([]byte("myname2")),
			"_lastname1": sqltypes.BytesBindVariable([]byte("mylastname2")),
		},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries:\n%+v, want\n%+v\n", sbc1.Queries, wantQueries)
	}
	wantQueries = []*querypb.BoundQuery{{
		Sql: "insert into name_lastname_keyspace_id_map(name, lastname, keyspace_id) values (:name0, :lastname0, :keyspace_id0), (:name1, :lastname1, :keyspace_id1)",
		BindVariables: map[string]*querypb.BindVariable{
			"name0":        sqltypes.BytesBindVariable([]byte("myname")),
			"lastname0":    sqltypes.BytesBindVariable([]byte("mylastname")),
			"keyspace_id0": sqltypes.BytesBindVariable([]byte("\006\347\352\"\316\222p\217")),
			"name1":        sqltypes.BytesBindVariable([]byte("myname2")),
			"lastname1":    sqltypes.BytesBindVariable([]byte("mylastname2")),
			"keyspace_id1": sqltypes.BytesBindVariable([]byte("N\261\220\311\242\372\026\234")),
		},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: \n%+v, want \n%+v\n", sbclookup.Queries, wantQueries)
	}
}

func TestMultiInsertGenerator(t *testing.T) {
	executor, sbc, _, sbclookup := createExecutorEnv()

	sbclookup.SetResults([]*sqltypes.Result{{
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(1),
		}},
		RowsAffected: 1,
		InsertID:     1,
	}})
	result, err := executorExec(executor, "insert into music(user_id, name) values (:u, 'myname1'),(:u, 'myname2')", map[string]*querypb.BindVariable{"u": sqltypes.Int64BindVariable(2)})
	if err != nil {
		t.Error(err)
	}
	wantQueries := []*querypb.BoundQuery{{
		Sql: "insert into music(user_id, name, id) values (:_user_id0, 'myname1', :_id0),(:_user_id1, 'myname2', :_id1) /* vtgate:: keyspace_id:06e7ea22ce92708f,06e7ea22ce92708f */",
		BindVariables: map[string]*querypb.BindVariable{
			"u":         sqltypes.Int64BindVariable(2),
			"_id0":      sqltypes.Int64BindVariable(1),
			"__seq0":    sqltypes.Int64BindVariable(1),
			"_user_id0": sqltypes.Int64BindVariable(2),
			"_id1":      sqltypes.Int64BindVariable(2),
			"__seq1":    sqltypes.Int64BindVariable(2),
			"_user_id1": sqltypes.Int64BindVariable(2),
		},
	}}
	if !reflect.DeepEqual(sbc.Queries, wantQueries) {
		t.Errorf("sbc.Queries:\n%+v, want\n%+v\n", sbc.Queries, wantQueries)
	}
	wantQueries = []*querypb.BoundQuery{{
		Sql:           "select next :n values from user_seq",
		BindVariables: map[string]*querypb.BindVariable{"n": sqltypes.Int64BindVariable(2)},
	}, {
		Sql: "insert into music_user_map(music_id, user_id) values (:music_id0, :user_id0), (:music_id1, :user_id1)",
		BindVariables: map[string]*querypb.BindVariable{
			"user_id0":  sqltypes.Uint64BindVariable(2),
			"music_id0": sqltypes.Int64BindVariable(1),
			"user_id1":  sqltypes.Uint64BindVariable(2),
			"music_id1": sqltypes.Int64BindVariable(2),
		},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: \n%#v, want \n%#v\n", sbclookup.Queries, wantQueries)
	}
	wantResult := *sandboxconn.SingleRowResult
	wantResult.InsertID = 1
	if !result.Equal(&wantResult) {
		t.Errorf("result: %+v, want %+v", result, &wantResult)
	}
}

func TestMultiInsertGeneratorSparse(t *testing.T) {
	executor, sbc, _, sbclookup := createExecutorEnv()

	sbclookup.SetResults([]*sqltypes.Result{{
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(1),
		}},
		RowsAffected: 1,
		InsertID:     1,
	}})
	result, err := executorExec(executor, "insert into music(id, user_id, name) values (NULL, :u, 'myname1'),(2, :u, 'myname2'), (NULL, :u, 'myname3')", map[string]*querypb.BindVariable{"u": sqltypes.Int64BindVariable(2)})
	if err != nil {
		t.Error(err)
	}
	wantQueries := []*querypb.BoundQuery{{
		Sql: "insert into music(id, user_id, name) values (:_id0, :_user_id0, 'myname1'),(:_id1, :_user_id1, 'myname2'),(:_id2, :_user_id2, 'myname3') /* vtgate:: keyspace_id:06e7ea22ce92708f,06e7ea22ce92708f,06e7ea22ce92708f */",
		BindVariables: map[string]*querypb.BindVariable{
			"u":         sqltypes.Int64BindVariable(2),
			"_id0":      sqltypes.Int64BindVariable(1),
			"__seq0":    sqltypes.Int64BindVariable(1),
			"_user_id0": sqltypes.Int64BindVariable(2),
			"_id1":      sqltypes.Int64BindVariable(2),
			"__seq1":    sqltypes.Int64BindVariable(2),
			"_user_id1": sqltypes.Int64BindVariable(2),
			"_id2":      sqltypes.Int64BindVariable(2),
			"__seq2":    sqltypes.Int64BindVariable(2),
			"_user_id2": sqltypes.Int64BindVariable(2),
		},
	}}
	if !reflect.DeepEqual(sbc.Queries, wantQueries) {
		t.Errorf("sbc.Queries:\n%+v, want\n%+v\n", sbc.Queries, wantQueries)
	}
	wantQueries = []*querypb.BoundQuery{{
		Sql:           "select next :n values from user_seq",
		BindVariables: map[string]*querypb.BindVariable{"n": sqltypes.Int64BindVariable(2)},
	}, {
		Sql: "insert into music_user_map(music_id, user_id) values (:music_id0, :user_id0), (:music_id1, :user_id1), (:music_id2, :user_id2)",
		BindVariables: map[string]*querypb.BindVariable{
			"user_id0":  sqltypes.Uint64BindVariable(2),
			"music_id0": sqltypes.Int64BindVariable(1),
			"user_id1":  sqltypes.Uint64BindVariable(2),
			"music_id1": sqltypes.Int64BindVariable(2),
			"user_id2":  sqltypes.Uint64BindVariable(2),
			"music_id2": sqltypes.Int64BindVariable(2),
		},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: \n%#v, want \n%#v\n", sbclookup.Queries, wantQueries)
	}
	wantResult := *sandboxconn.SingleRowResult
	wantResult.InsertID = 1
	if !result.Equal(&wantResult) {
		t.Errorf("result: %+v, want %+v", result, &wantResult)
	}
}

func TestKeyDestRangeQuery(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()
	// it works in a single shard key range
	masterSession.TargetString = "TestExecutor[40-60]"

	_, err := executorExec(executor, "DELETE FROM sharded_user_msgs LIMIT 1000", nil)
	if err != nil {
		t.Error(err)
	}
	sql := "DELETE FROM sharded_user_msgs LIMIT 1000"
	wantQueries := []*querypb.BoundQuery{{
		Sql:           sql + "/* vtgate:: filtered_replication_unfriendly */",
		BindVariables: map[string]*querypb.BindVariable{},
	}}

	if len(sbc1.Queries) != 0 {
		t.Errorf("sbc1.Queries: %+v, want %+v\n", sbc1.Queries, []*querypb.BoundQuery{})
	}
	testQueries(t, "sbc2", sbc2, wantQueries)

	sbc1.Queries = nil
	sbc2.Queries = nil

	// it works with keyrange spanning two shards
	masterSession.TargetString = "TestExecutor[-60]"

	_, err = executorExec(executor, sql, nil)
	if err != nil {
		t.Error(err)
	}
	testQueries(t, "sbc1", sbc1, wantQueries)
	testQueries(t, "sbc1", sbc2, wantQueries)

	sbc1.Queries = nil
	sbc2.Queries = nil

	// it works with open ended key range
	masterSession.TargetString = "TestExecutor[-]"

	_, err = executorExec(executor, sql, nil)
	if err != nil {
		t.Error(err)
	}

	testQueries(t, "sbc1", sbc1, wantQueries)
	testQueries(t, "sbc2", sbc2, wantQueries)

	sbc1.Queries = nil
	sbc2.Queries = nil

	// it works for select
	sql = "SELECT * FROM sharded_user_msgs LIMIT 1"
	wantQueries = []*querypb.BoundQuery{{
		Sql:           sql,
		BindVariables: map[string]*querypb.BindVariable{},
	}}

	_, err = executorExec(executor, sql, nil)
	if err != nil {
		t.Error(err)
	}

	testQueries(t, "sbc1", sbc1, wantQueries)
	testQueries(t, "sbc2", sbc2, wantQueries)

	sbc1.Queries = nil
	sbc2.Queries = nil

	// it works for updates
	sql = "UPDATE sharded_user_msgs set message='test' LIMIT 1"

	wantQueries = []*querypb.BoundQuery{{
		Sql:           sql + "/* vtgate:: filtered_replication_unfriendly */",
		BindVariables: map[string]*querypb.BindVariable{},
	}}

	_, err = executorExec(executor, sql, nil)
	if err != nil {
		t.Error(err)
	}

	testQueries(t, "sbc1", sbc1, wantQueries)
	testQueries(t, "sbc2", sbc2, wantQueries)

	sbc1.Queries = nil
	sbc2.Queries = nil

	// it does not work for inserts
	_, err = executorExec(executor, "INSERT INTO sharded_user_msgs(message) VALUE('test')", nil)

	want := "range queries not supported for inserts: TestExecutor[-]"
	if err == nil || err.Error() != want {
		t.Errorf("got: %v, want %s", err, want)
	}

	sbc1.Queries = nil
	sbc2.Queries = nil
	masterSession.TargetString = ""
}

func TestKeyShardDestQuery(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()
	// it works in a single shard key range
	masterSession.TargetString = "TestExecutor:40-60"

	_, err := executorExec(executor, "DELETE FROM sharded_user_msgs LIMIT 1000", nil)
	if err != nil {
		t.Error(err)
	}
	sql := "DELETE FROM sharded_user_msgs LIMIT 1000"
	wantQueries := []*querypb.BoundQuery{{
		Sql:           sql + "/* vtgate:: filtered_replication_unfriendly */",
		BindVariables: map[string]*querypb.BindVariable{},
	}}

	if len(sbc1.Queries) != 0 {
		t.Errorf("sbc1.Queries: %+v, want %+v\n", sbc1.Queries, []*querypb.BoundQuery{})
	}
	testQueries(t, "sbc2", sbc2, wantQueries)

	sbc1.Queries = nil
	sbc2.Queries = nil

	masterSession.TargetString = "TestExecutor:40-60"

	_, err = executorExec(executor, sql, nil)
	if err != nil {
		t.Error(err)
	}

	testQueries(t, "sbc2", sbc2, wantQueries)

	sbc1.Queries = nil
	sbc2.Queries = nil

	// it works for select
	sql = "SELECT * FROM sharded_user_msgs LIMIT 1"
	wantQueries = []*querypb.BoundQuery{{
		Sql:           sql,
		BindVariables: map[string]*querypb.BindVariable{},
	}}

	_, err = executorExec(executor, sql, nil)
	if err != nil {
		t.Error(err)
	}

	if len(sbc1.Queries) != 0 {
		t.Errorf("sbc1.Queries: %+v, want %+v\n", sbc1.Queries, []*querypb.BoundQuery{})
	}

	testQueries(t, "sbc2", sbc2, wantQueries)

	sbc1.Queries = nil
	sbc2.Queries = nil

	// it works for updates
	sql = "UPDATE sharded_user_msgs set message='test' LIMIT 1"

	wantQueries = []*querypb.BoundQuery{{
		Sql:           sql + "/* vtgate:: filtered_replication_unfriendly */",
		BindVariables: map[string]*querypb.BindVariable{},
	}}

	_, err = executorExec(executor, sql, nil)

	if err != nil {
		t.Error(err)
	}

	if len(sbc1.Queries) != 0 {
		t.Errorf("sbc1.Queries: %+v, want %+v\n", sbc1.Queries, []*querypb.BoundQuery{})
	}

	testQueries(t, "sbc2", sbc2, wantQueries)

	sbc1.Queries = nil
	sbc2.Queries = nil

	// it works for inserts

	sql = "INSERT INTO sharded_user_msgs(message) VALUE('test')"
	_, err = executorExec(executor, sql, nil)

	wantQueries = []*querypb.BoundQuery{{
		Sql:           sql + "/* vtgate:: filtered_replication_unfriendly */",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	if err != nil {
		t.Error(err)
	}

	if len(sbc1.Queries) != 0 {
		t.Errorf("sbc1.Queries: %+v, want %+v\n", sbc1.Queries, []*querypb.BoundQuery{})
	}

	testQueries(t, "sbc2", sbc2, wantQueries)

	sbc1.Queries = nil
	sbc2.Queries = nil
	masterSession.TargetString = ""
}

// Prepared statement tests
func TestUpdateEqualWithPrepare(t *testing.T) {
	executor, sbc1, sbc2, sbclookup := createExecutorEnv()

	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	_, err := executorPrepare(executor, "update music set a = :a0 where id = :id0", map[string]*querypb.BindVariable{
		"a0":  sqltypes.Int64BindVariable(3),
		"id0": sqltypes.Int64BindVariable(2),
	})
	if err != nil {
		t.Error(err)
	}

	wantQueries := []*querypb.BoundQuery{}
	wantQueries = nil

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
	executor, sbc1, sbc2, sbclookup := createExecutorEnv()

	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	_, err := executorPrepare(executor, "insert into user(id, v, name) values (:_Id0, 2, ':_name0')", map[string]*querypb.BindVariable{
		"_Id0":   sqltypes.Int64BindVariable(1),
		"_name0": sqltypes.BytesBindVariable([]byte("myname")),
		"__seq0": sqltypes.Int64BindVariable(1),
	})
	if err != nil {
		t.Error(err)
	}

	wantQueries := []*querypb.BoundQuery{}
	wantQueries = nil

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
	executor, sbc, _, sbclookup := createExecutorEnv()
	_, err := executorPrepare(executor, "delete from user where id = :id0", map[string]*querypb.BindVariable{
		"id0": sqltypes.Int64BindVariable(1),
	})
	if err != nil {
		t.Error(err)
	}
	wantQueries := []*querypb.BoundQuery{}
	wantQueries = nil

	if !reflect.DeepEqual(sbc.Queries, wantQueries) {
		t.Errorf("sbc.Queries:\n%+v, want\n%+v\n", sbc.Queries, wantQueries)
	}

	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries:\n%+v, want\n%+v\n", sbclookup.Queries, wantQueries)
	}
}
