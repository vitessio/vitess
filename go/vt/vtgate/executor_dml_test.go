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

	"github.com/youtube/vitess/go/sqltypes"
	_ "github.com/youtube/vitess/go/vt/vtgate/vindexes"
	"github.com/youtube/vitess/go/vt/vttablet/sandboxconn"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

func TestUpdateEqual(t *testing.T) {
	executor, sbc1, sbc2, sbclookup := createExecutorEnv()

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

func TestUpdateEqualFail(t *testing.T) {
	executor, _, _, _ := createExecutorEnv()
	s := getSandbox("TestExecutor")

	_, err := executorExec(executor, "update user set a=2 where id = :aa", nil)
	want := "execUpdateEqual: missing bind var aa"
	if err == nil || err.Error() != want {
		t.Errorf("executorExec: %v, want %v", err, want)
	}

	s.SrvKeyspaceMustFail = 1
	_, err = executorExec(executor, "update user set a=2 where id = :id", map[string]*querypb.BindVariable{
		"id": sqltypes.Int64BindVariable(1),
	})
	want = "execUpdateEqual: keyspace TestExecutor fetch error: topo error GetSrvKeyspace"
	if err == nil || err.Error() != want {
		t.Errorf("executorExec: %v, want %v", err, want)
	}

	_, err = executorExec(executor, "update user set a=2 where id = :id", map[string]*querypb.BindVariable{
		"id": sqltypes.StringBindVariable("aa"),
	})
	want = `execUpdateEqual: hash.Map: could not parse value: aa`
	if err == nil || err.Error() != want {
		t.Errorf("executorExec: %v, want %v", err, want)
	}

	s.ShardSpec = "80-"
	_, err = executorExec(executor, "update user set a=2 where id = :id", map[string]*querypb.BindVariable{
		"id": sqltypes.Int64BindVariable(1),
	})
	want = "execUpdateEqual: KeyspaceId 166b40b44aba4bd6 didn't match any shards"
	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Errorf("executorExec: %v, want prefix %v", err, want)
	}
	s.ShardSpec = DefaultShardSpec
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

func TestDeleteEqualFail(t *testing.T) {
	executor, _, _, _ := createExecutorEnv()
	s := getSandbox("TestExecutor")

	_, err := executorExec(executor, "delete from user where id = :aa", nil)
	want := "execDeleteEqual: missing bind var aa"
	if err == nil || err.Error() != want {
		t.Errorf("executorExec: %v, want %v", err, want)
	}

	s.SrvKeyspaceMustFail = 1
	_, err = executorExec(executor, "delete from user where id = :id", map[string]*querypb.BindVariable{
		"id": sqltypes.Int64BindVariable(1),
	})
	want = "execDeleteEqual: keyspace TestExecutor fetch error: topo error GetSrvKeyspace"
	if err == nil || err.Error() != want {
		t.Errorf("executorExec: %v, want %v", err, want)
	}

	_, err = executorExec(executor, "delete from user where id = :id", map[string]*querypb.BindVariable{
		"id": sqltypes.StringBindVariable("aa"),
	})
	want = `execDeleteEqual: hash.Map: could not parse value: aa`
	if err == nil || err.Error() != want {
		t.Errorf("executorExec: %v, want %v", err, want)
	}

	s.ShardSpec = "80-"
	_, err = executorExec(executor, "delete from user where id = :id", map[string]*querypb.BindVariable{
		"id": sqltypes.Int64BindVariable(1),
	})
	want = "execDeleteEqual: KeyspaceId 166b40b44aba4bd6 didn't match any shards"
	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Errorf("executorExec: %v, want prefix %v", err, want)
	}
	s.ShardSpec = DefaultShardSpec
}

func TestInsertSharded(t *testing.T) {
	executor, sbc1, sbc2, sbclookup := createExecutorEnv()

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
		Sql: "select music_id from music_user_map where ((music_id = :music_id0 and user_id = :user_id0))",
		BindVariables: map[string]*querypb.BindVariable{
			"music_id0": sqltypes.Int64BindVariable(3),
			"user_id0":  sqltypes.Uint64BindVariable(2),
		},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: %+v, want %+v\n", sbclookup.Queries, wantQueries)
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

func TestInsertFail(t *testing.T) {
	executor, sbc, _, sbclookup := createExecutorEnv()

	_, err := executorExec(executor, "insert into user(id, v, name) values (:aa, 2, 'myname')", nil)
	want := "execInsertSharded: handleGenerate: missing bind var aa"
	if err == nil || err.Error() != want {
		t.Errorf("executorExec: %v, want %v", err, want)
	}

	_, err = executorExec(executor, "insert into main1(id, v, name) values (:aa, 2, 'myname')", nil)
	want = "execInsertUnsharded: handleGenerate: missing bind var aa"
	if err == nil || err.Error() != want {
		t.Errorf("executorExec: %v, want %v", err, want)
	}

	sbclookup.MustFailCodes[vtrpcpb.Code_INVALID_ARGUMENT] = 1
	_, err = executorExec(executor, "insert into user(id, v, name) values (null, 2, 'myname')", nil)
	want = "execInsertSharded: "
	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Errorf("executorExec: %v, want prefix %v", err, want)
	}

	sbclookup.MustFailCodes[vtrpcpb.Code_INVALID_ARGUMENT] = 1
	_, err = executorExec(executor, "insert into user(id, v, name) values (1, 2, 'myname')", nil)
	want = "execInsertSharded: getInsertShardedRoute: lookup.Create: "
	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Errorf("executorExec: %v, want prefix %v", err, want)
	}

	_, err = executorExec(executor, "insert into ksid_table(keyspace_id) values (null)", nil)
	want = "execInsertSharded: getInsertShardedRoute: value must be supplied for column keyspace_id"
	if err == nil || err.Error() != want {
		t.Errorf("executorExec: %v, want %v", err, want)
	}

	sbclookup.MustFailCodes[vtrpcpb.Code_INVALID_ARGUMENT] = 1
	_, err = executorExec(executor, "insert into music_extra_reversed(music_id, user_id) values (1, 1)", nil)
	want = "execInsertSharded: getInsertShardedRoute: lookup.Map"
	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Errorf("executorExec: %v, want prefix %v", err, want)
	}

	sbclookup.SetResults([]*sqltypes.Result{{}})
	_, err = executorExec(executor, "insert into music_extra_reversed(music_id, user_id) values (1, 1)", nil)
	want = "execInsertSharded: getInsertShardedRoute: could not map INT64(1) to a keyspace id"
	if err == nil || err.Error() != want {
		t.Errorf("paramsSelectEqual: executorExec: %v, want %v", err, want)
	}

	getSandbox("TestExecutor").SrvKeyspaceMustFail = 1
	_, err = executorExec(executor, "insert into user(id, v, name) values (1, 2, 'myname')", nil)
	want = "execInsertSharded: getInsertShardedRoute: keyspace TestExecutor fetch error: topo error GetSrvKeyspace"
	if err == nil || err.Error() != want {
		t.Errorf("executorExec: %v, want %v", err, want)
	}

	getSandbox("TestExecutor").ShardSpec = "80-"
	_, err = executorExec(executor, "insert into user(id, v, name) values (1, 2, 'myname')", nil)
	want = "execInsertSharded: getInsertShardedRoute: KeyspaceId 166b40b44aba4bd6 didn't match any shards"
	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Errorf("executorExec: %v, want prefix %v", err, want)
	}
	getSandbox("TestExecutor").ShardSpec = DefaultShardSpec

	sbclookup.MustFailCodes[vtrpcpb.Code_INVALID_ARGUMENT] = 1
	_, err = executorExec(executor, "insert into music(user_id, id) values (1, null)", nil)
	want = "execInsertSharded:"
	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Errorf("executorExec: %v, want prefix %v", err, want)
	}

	sbclookup.MustFailCodes[vtrpcpb.Code_INVALID_ARGUMENT] = 1
	_, err = executorExec(executor, "insert into music(user_id, id) values (1, 2)", nil)
	want = "execInsertSharded: getInsertShardedRoute: lookup.Create: execInsertUnsharded: target: TestUnsharded.0.master"
	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Errorf("executorExec: %v, want prefix %v", err, want)
	}

	_, err = executorExec(executor, "insert into music_extra(user_id, music_id) values (1, null)", nil)
	want = "execInsertSharded: getInsertShardedRoute: value must be supplied for column music_id"
	if err == nil || err.Error() != want {
		t.Errorf("executorExec: %v, want %v", err, want)
	}

	_, err = executorExec(executor, "insert into music_extra_reversed(music_id, user_id) values (1, 'aa')", nil)
	want = `execInsertSharded: getInsertShardedRoute: hash.Verify: could not parse value: aa`
	if err == nil || err.Error() != want {
		t.Errorf("executorExec: %v, want %v", err, want)
	}

	_, err = executorExec(executor, "insert into music_extra_reversed(music_id, user_id) values (1, 3)", nil)
	want = "execInsertSharded: getInsertShardedRoute: values [INT64(3)] for column user_id does not map to keyspaceids"
	if err == nil || err.Error() != want {
		t.Errorf("executorExec: %v, want %v", err, want)
	}

	sbc.MustFailCodes[vtrpcpb.Code_INVALID_ARGUMENT] = 1
	_, err = executorExec(executor, "insert into user(id, v, name) values (1, 2, 'myname')", nil)
	want = "execInsertSharded: target: TestExecutor.-20.master"
	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Errorf("executorExec: %v, want prefix %v", err, want)
	}

	_, err = executorExec(executor, "insert into noauto_table(id) values (null)", nil)
	want = "execInsertSharded: getInsertShardedRoute: value must be supplied for column id"
	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Errorf("executorExec: %v, want prefix %v", err, want)
	}

	_, err = executorExec(executor, "insert into user(id, v, name) values (1, 2, null)", nil)
	want = "execInsertSharded: getInsertShardedRoute: value must be supplied for column name"
	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Errorf("executorExec: %v, want prefix %v", err, want)
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
		&vtgatepb.Session{InTransaction: true},
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
		&vtgatepb.Session{InTransaction: true},
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
