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
	"fmt"
	"reflect"
	"strings"
	"testing"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/discovery"
	_ "github.com/youtube/vitess/go/vt/vtgate/vindexes"
	"github.com/youtube/vitess/go/vt/vttablet/sandboxconn"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

func TestSelectNext(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()

	query := "select next :n values from user_seq"
	bv := map[string]*querypb.BindVariable{"n": sqltypes.Int64BindVariable(2)}
	_, err := executorExec(executor, query, bv)
	if err != nil {
		t.Error(err)
	}
	wantQueries := []*querypb.BoundQuery{{
		Sql:           query,
		BindVariables: map[string]*querypb.BindVariable{"n": sqltypes.Int64BindVariable(2)},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries:\n%v, want\n%v\n", sbclookup.Queries, wantQueries)
	}
}

func TestExecDBA(t *testing.T) {
	executor, sbc1, _, _ := createExecutorEnv()

	query := "select * from information_schema.foo"
	_, err := executor.Execute(
		context.Background(),
		&vtgatepb.Session{TargetString: "TestExecutor"},
		query,
		map[string]*querypb.BindVariable{},
	)
	if err != nil {
		t.Error(err)
	}
	wantQueries := []*querypb.BoundQuery{{
		Sql:           query,
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: %+v, want %+v\n", sbc1.Queries, wantQueries)
	}
}

func TestUnsharded(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()

	_, err := executorExec(executor, "select id from music_user_map where id = 1", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select id from music_user_map where id = 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: %+v, want %+v\n", sbclookup.Queries, wantQueries)
	}

	_, err = executorExec(executor, "update music_user_map set id = 1", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries = []*querypb.BoundQuery{{
		Sql:           "select id from music_user_map where id = 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}, {
		Sql:           "update music_user_map set id = 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: %+v, want %+v\n", sbclookup.Queries, wantQueries)
	}

	sbclookup.Queries = nil
	_, err = executorExec(executor, "delete from music_user_map", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries = []*querypb.BoundQuery{{
		Sql:           "delete from music_user_map",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: %+v, want %+v\n", sbclookup.Queries, wantQueries)
	}

	sbclookup.Queries = nil
	_, err = executorExec(executor, "insert into music_user_map values (1)", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries = []*querypb.BoundQuery{{
		Sql:           "insert into music_user_map values (1)",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: %+v, want %+v\n", sbclookup.Queries, wantQueries)
	}
}

func TestUnshardedComments(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()

	_, err := executorExec(executor, "select id from music_user_map where id = 1 /* trailing */", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select id from music_user_map where id = 1 /* trailing */",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: %+v, want %+v\n", sbclookup.Queries, wantQueries)
	}

	_, err = executorExec(executor, "update music_user_map set id = 1 /* trailing */", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries = []*querypb.BoundQuery{{
		Sql:           "select id from music_user_map where id = 1 /* trailing */",
		BindVariables: map[string]*querypb.BindVariable{},
	}, {
		Sql:           "update music_user_map set id = 1 /* trailing */",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: %+v, want %+v\n", sbclookup.Queries, wantQueries)
	}

	sbclookup.Queries = nil
	_, err = executorExec(executor, "delete from music_user_map /* trailing */", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries = []*querypb.BoundQuery{{
		Sql:           "delete from music_user_map /* trailing */",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: %+v, want %+v\n", sbclookup.Queries, wantQueries)
	}

	sbclookup.Queries = nil
	_, err = executorExec(executor, "insert into music_user_map values (1) /* trailing */", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries = []*querypb.BoundQuery{{
		Sql:           "insert into music_user_map values (1) /* trailing */",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: %+v, want %+v\n", sbclookup.Queries, wantQueries)
	}
}

func TestStreamUnsharded(t *testing.T) {
	executor, _, _, _ := createExecutorEnv()

	sql := "select id from music_user_map where id = 1"
	result, err := executorStream(executor, sql)
	if err != nil {
		t.Error(err)
	}
	wantResult := sandboxconn.StreamRowResult
	if !result.Equal(wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}
}

func TestStreamBuffering(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()

	// This test is similar to TestStreamUnsharded except that it returns a Result > 10 bytes,
	// such that the splitting of the Result into multiple Result responses gets tested.
	sbclookup.SetResults([]*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
			{Name: "col", Type: sqltypes.VarChar},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt32(1),
			sqltypes.NewVarChar("01234567890123456789"),
		}, {
			sqltypes.NewInt32(2),
			sqltypes.NewVarChar("12345678901234567890"),
		}},
	}})

	results := make(chan *sqltypes.Result, 10)
	err := executor.StreamExecute(
		context.Background(),
		masterSession,
		"select id from music_user_map where id = 1",
		nil,
		querypb.Target{
			TabletType: topodatapb.TabletType_MASTER,
		},
		func(qr *sqltypes.Result) error {
			results <- qr
			return nil
		},
	)
	close(results)
	if err != nil {
		t.Error(err)
	}
	wantResults := []*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
			{Name: "col", Type: sqltypes.VarChar},
		},
	}, {
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt32(1),
			sqltypes.NewVarChar("01234567890123456789"),
		}},
	}, {
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt32(2),
			sqltypes.NewVarChar("12345678901234567890"),
		}},
	}}
	var gotResults []*sqltypes.Result
	for r := range results {
		gotResults = append(gotResults, r)
	}
	if !reflect.DeepEqual(gotResults, wantResults) {
		t.Logf("len: %d", len(gotResults))
		for i := range gotResults {
			t.Errorf("Buffered streaming:\n%v, want\n%v", gotResults[i], wantResults[i])
		}
	}
}

func TestShardFail(t *testing.T) {
	executor, _, _, _ := createExecutorEnv()

	getSandbox(KsTestUnsharded).SrvKeyspaceMustFail = 1

	_, err := executorExec(executor, "select id from sharded_table where id = 1", nil)
	want := "paramsAllShards: unsharded keyspace TestXBadSharding has multiple shards: possible cause: sharded keyspace is marked as unsharded in vschema"
	if err == nil || err.Error() != want {
		t.Errorf("executorExec: %v, want %v", err, want)
	}
}

func TestSelectBindvars(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()

	_, err := executorExec(executor, "select id from user where id = :id", map[string]*querypb.BindVariable{
		"id": sqltypes.Int64BindVariable(1),
	})
	if err != nil {
		t.Error(err)
	}
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select id from user where id = :id",
		BindVariables: map[string]*querypb.BindVariable{"id": sqltypes.Int64BindVariable(1)},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries: %+v, want %+v\n", sbc1.Queries, wantQueries)
	}
	if sbc2.Queries != nil {
		t.Errorf("sbc2.Queries: %+v, want nil\n", sbc2.Queries)
	}
	sbc1.Queries = nil

	_, err = executorExec(executor, "select id from user where name in (:name1, :name2)", map[string]*querypb.BindVariable{
		"name1": sqltypes.StringBindVariable("foo1"),
		"name2": sqltypes.StringBindVariable("foo2"),
	})
	if err != nil {
		t.Error(err)
	}
	wantQueries = []*querypb.BoundQuery{{
		Sql: "select id from user where name in ::__vals",
		BindVariables: map[string]*querypb.BindVariable{
			"name1":  sqltypes.StringBindVariable("foo1"),
			"name2":  sqltypes.StringBindVariable("foo2"),
			"__vals": sqltypes.TestBindVariable([]interface{}{"foo1", "foo2"}),
		},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries: %+v, want %+v\n", sbc1.Queries, wantQueries)
	}
	sbc1.Queries = nil

	_, err = executorExec(executor, "select id from user where name in (:name1, :name2)", map[string]*querypb.BindVariable{
		"name1": sqltypes.BytesBindVariable([]byte("foo1")),
		"name2": sqltypes.BytesBindVariable([]byte("foo2")),
	})
	if err != nil {
		t.Error(err)
	}
	wantQueries = []*querypb.BoundQuery{{
		Sql: "select id from user where name in ::__vals",
		BindVariables: map[string]*querypb.BindVariable{
			"name1":  sqltypes.BytesBindVariable([]byte("foo1")),
			"name2":  sqltypes.BytesBindVariable([]byte("foo2")),
			"__vals": sqltypes.TestBindVariable([]interface{}{[]byte("foo1"), []byte("foo2")}),
		},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries: %+v, want %+v\n", sbc1.Queries, wantQueries)
	}
}

func TestSelectEqual(t *testing.T) {
	executor, sbc1, sbc2, sbclookup := createExecutorEnv()

	_, err := executorExec(executor, "select id from user where id = 1", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select id from user where id = 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries: %+v, want %+v\n", sbc1.Queries, wantQueries)
	}
	if sbc2.Queries != nil {
		t.Errorf("sbc2.Queries: %+v, want nil\n", sbc2.Queries)
	}
	sbc1.Queries = nil

	_, err = executorExec(executor, "select id from user where id = 3", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries = []*querypb.BoundQuery{{
		Sql:           "select id from user where id = 3",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	if !reflect.DeepEqual(sbc2.Queries, wantQueries) {
		t.Errorf("sbc2.Queries: %+v, want %+v\n", sbc2.Queries, wantQueries)
	}
	if execCount := sbc1.ExecCount.Get(); execCount != 1 {
		t.Errorf("sbc1.ExecCount: %v, want 1\n", execCount)
	}
	if sbc1.Queries != nil {
		t.Errorf("sbc1.Queries: %+v, want nil\n", sbc1.Queries)
	}
	sbc2.Queries = nil

	_, err = executorExec(executor, "select id from user where id = '3'", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries = []*querypb.BoundQuery{{
		Sql:           "select id from user where id = '3'",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	if !reflect.DeepEqual(sbc2.Queries, wantQueries) {
		t.Errorf("sbc2.Queries: %+v, want %+v\n", sbc2.Queries, wantQueries)
	}
	if execCount := sbc1.ExecCount.Get(); execCount != 1 {
		t.Errorf("sbc1.ExecCount: %v, want 1\n", execCount)
	}
	if sbc1.Queries != nil {
		t.Errorf("sbc1.Queries: %+v, want nil\n", sbc1.Queries)
	}
	sbc2.Queries = nil

	_, err = executorExec(executor, "select id from user where name = 'foo'", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries = []*querypb.BoundQuery{{
		Sql:           "select id from user where name = 'foo'",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries: %+v, want %+v\n", sbc1.Queries, wantQueries)
	}
	wantQueries = []*querypb.BoundQuery{{
		Sql: "select user_id from name_user_map where name = :name",
		BindVariables: map[string]*querypb.BindVariable{
			"name": sqltypes.BytesBindVariable([]byte("foo")),
		},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: %+v, want %+v\n", sbclookup.Queries, wantQueries)
	}
}

func TestSelectDual(t *testing.T) {
	executor, sbc1, _, lookup := createExecutorEnv()

	_, err := executorExec(executor, "select @@aa.bb from dual", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select @@aa.bb from dual",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries: %+v, want %+v\n", sbc1.Queries, wantQueries)
	}

	_, err = executorExec(executor, "select @@aa.bb from TestUnsharded.dual", nil)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(lookup.Queries, wantQueries) {
		t.Errorf("sbc1.Queries: %+v, want %+v\n", sbc1.Queries, wantQueries)
	}
}

func TestSelectComments(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()

	_, err := executorExec(executor, "select id from user where id = 1 /* trailing */", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select id from user where id = 1 /* trailing */",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries: %+v, want %+v\n", sbc1.Queries, wantQueries)
	}
	if sbc2.Queries != nil {
		t.Errorf("sbc2.Queries: %+v, want nil\n", sbc2.Queries)
	}
	sbc1.Queries = nil
}

func TestSelectCaseSensitivity(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()

	_, err := executorExec(executor, "select Id from user where iD = 1", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select Id from user where iD = 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries: %+v, want %+v\n", sbc1.Queries, wantQueries)
	}
	if sbc2.Queries != nil {
		t.Errorf("sbc2.Queries: %+v, want nil\n", sbc2.Queries)
	}
	sbc1.Queries = nil
}

func TestSelectEqualNotFound(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()

	sbclookup.SetResults([]*sqltypes.Result{{}})
	result, err := executorExec(executor, "select id from music where id = 1", nil)
	if err != nil {
		t.Error(err)
	}
	wantResult := &sqltypes.Result{}
	if !result.Equal(wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}

	sbclookup.SetResults([]*sqltypes.Result{{}})
	result, err = executorExec(executor, "select id from user where name = 'foo'", nil)
	if err != nil {
		t.Error(err)
	}
	wantResult = &sqltypes.Result{}
	if !result.Equal(wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}
}

func TestStreamSelectEqual(t *testing.T) {
	executor, _, _, _ := createExecutorEnv()

	sql := "select id from user where id = 1"
	result, err := executorStream(executor, sql)
	if err != nil {
		t.Error(err)
	}
	wantResult := sandboxconn.StreamRowResult
	if !result.Equal(wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}
}

func TestSelectEqualFail(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()
	s := getSandbox("TestExecutor")

	_, err := executorExec(executor, "select id from user where id = (select count(*) from music)", nil)
	want := "unsupported"
	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Errorf("executorExec: %v, must start with %v", err, want)
	}

	_, err = executorExec(executor, "select id from user where id = :aa", nil)
	want = "paramsSelectEqual: missing bind var aa"
	if err == nil || err.Error() != want {
		t.Errorf("executorExec: %v, want %v", err, want)
	}

	s.SrvKeyspaceMustFail = 1
	_, err = executorExec(executor, "select id from user where id = 1", nil)
	want = "paramsSelectEqual: keyspace TestExecutor fetch error: topo error GetSrvKeyspace"
	if err == nil || err.Error() != want {
		t.Errorf("executorExec: %v, want %v", err, want)
	}

	sbclookup.MustFailCodes[vtrpcpb.Code_INVALID_ARGUMENT] = 1
	_, err = executorExec(executor, "select id from music where id = 1", nil)
	want = "paramsSelectEqual: lookup.Map"
	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Errorf("executorExec: %v, want prefix %v", err, want)
	}

	s.ShardSpec = "80-"
	_, err = executorExec(executor, "select id from user where id = 1", nil)
	want = "paramsSelectEqual: KeyspaceId 166b40b44aba4bd6 didn't match any shards"
	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Errorf("executorExec: %v, want prefix %v", err, want)
	}
	s.ShardSpec = DefaultShardSpec

	sbclookup.MustFailCodes[vtrpcpb.Code_INVALID_ARGUMENT] = 1
	_, err = executorExec(executor, "select id from user where name = 'foo'", nil)
	want = "paramsSelectEqual: lookup.Map"
	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Errorf("executorExec: %v, want prefix %v", err, want)
	}

	s.ShardSpec = "80-"
	_, err = executorExec(executor, "select id from user where name = 'foo'", nil)
	want = "paramsSelectEqual: KeyspaceId 166b40b44aba4bd6 didn't match any shards"
	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Errorf("executorExec: %v, want prefix %v", err, want)
	}
	s.ShardSpec = DefaultShardSpec
}

func TestSelectIN(t *testing.T) {
	executor, sbc1, sbc2, sbclookup := createExecutorEnv()

	// Constant in IN is just a number, not a bind variable.
	_, err := executorExec(executor, "select id from user where id in (1)", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries := []*querypb.BoundQuery{{
		Sql: "select id from user where id in ::__vals",
		BindVariables: map[string]*querypb.BindVariable{
			"__vals": sqltypes.TestBindVariable([]interface{}{int64(1)}),
		},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries: %+v, want %+v\n", sbc1.Queries, wantQueries)
	}
	if sbc2.Queries != nil {
		t.Errorf("sbc2.Queries: %+v, want nil\n", sbc2.Queries)
	}

	// Constant in IN is just a couple numbers, not bind variables.
	// They result in two different queries on two shards.
	sbc1.Queries = nil
	sbc2.Queries = nil
	_, err = executorExec(executor, "select id from user where id in (1, 3)", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries = []*querypb.BoundQuery{{
		Sql: "select id from user where id in ::__vals",
		BindVariables: map[string]*querypb.BindVariable{
			"__vals": sqltypes.TestBindVariable([]interface{}{int64(1)}),
		},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries: %+v, want %+v\n", sbc1.Queries, wantQueries)
	}
	wantQueries = []*querypb.BoundQuery{{
		Sql: "select id from user where id in ::__vals",
		BindVariables: map[string]*querypb.BindVariable{
			"__vals": sqltypes.TestBindVariable([]interface{}{int64(3)}),
		},
	}}
	if !reflect.DeepEqual(sbc2.Queries, wantQueries) {
		t.Errorf("sbc2.Queries: %+v, want %+v\n", sbc2.Queries, wantQueries)
	}

	// In is a bind variable list, that will end up on two shards.
	// This is using an []interface{} for the bind variable list.
	sbc1.Queries = nil
	sbc2.Queries = nil
	_, err = executorExec(executor, "select id from user where id in ::vals", map[string]*querypb.BindVariable{
		"vals": sqltypes.TestBindVariable([]interface{}{int64(1), int64(3)}),
	})
	if err != nil {
		t.Error(err)
	}
	wantQueries = []*querypb.BoundQuery{{
		Sql: "select id from user where id in ::__vals",
		BindVariables: map[string]*querypb.BindVariable{
			"__vals": sqltypes.TestBindVariable([]interface{}{int64(1)}),
			"vals":   sqltypes.TestBindVariable([]interface{}{int64(1), int64(3)}),
		},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries: %+v, want %+v\n", sbc1.Queries, wantQueries)
	}
	wantQueries = []*querypb.BoundQuery{{
		Sql: "select id from user where id in ::__vals",
		BindVariables: map[string]*querypb.BindVariable{
			"__vals": sqltypes.TestBindVariable([]interface{}{int64(3)}),
			"vals":   sqltypes.TestBindVariable([]interface{}{int64(1), int64(3)}),
		},
	}}
	if !reflect.DeepEqual(sbc2.Queries, wantQueries) {
		t.Errorf("sbc2.Queries: %+v, want %+v\n", sbc2.Queries, wantQueries)
	}

	// Convert a non-list bind variable.
	sbc1.Queries = nil
	sbc2.Queries = nil
	_, err = executorExec(executor, "select id from user where name = 'foo'", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries = []*querypb.BoundQuery{{
		Sql:           "select id from user where name = 'foo'",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries: %+v, want %+v\n", sbc1.Queries, wantQueries)
	}
	wantQueries = []*querypb.BoundQuery{{
		Sql: "select user_id from name_user_map where name = :name",
		BindVariables: map[string]*querypb.BindVariable{
			"name": sqltypes.BytesBindVariable([]byte("foo")),
		},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: %+v, want %+v\n", sbclookup.Queries, wantQueries)
	}
}

func TestStreamSelectIN(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()

	sql := "select id from user where id in (1)"
	result, err := executorStream(executor, sql)
	if err != nil {
		t.Error(err)
	}
	wantResult := sandboxconn.StreamRowResult
	if !result.Equal(wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}

	sql = "select id from user where id in (1, 3)"
	result, err = executorStream(executor, sql)
	if err != nil {
		t.Error(err)
	}
	wantResult = &sqltypes.Result{
		Fields: sandboxconn.StreamRowResult.Fields,
		Rows: [][]sqltypes.Value{
			sandboxconn.StreamRowResult.Rows[0],
			sandboxconn.StreamRowResult.Rows[0],
		},
		RowsAffected: 0,
	}
	if !result.Equal(wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}

	sql = "select id from user where name = 'foo'"
	result, err = executorStream(executor, sql)
	if err != nil {
		t.Error(err)
	}
	wantResult = sandboxconn.StreamRowResult
	if !result.Equal(wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}

	wantQueries := []*querypb.BoundQuery{{
		Sql: "select user_id from name_user_map where name = :name",
		BindVariables: map[string]*querypb.BindVariable{
			"name": sqltypes.BytesBindVariable([]byte("foo")),
		},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: %+v, want %+v\n", sbclookup.Queries, wantQueries)
	}
}

func TestSelectINFail(t *testing.T) {
	executor, _, _, _ := createExecutorEnv()

	_, err := executorExec(executor, "select id from user where id in (:aa)", nil)
	want := "paramsSelectIN: missing bind var aa"
	if err == nil || err.Error() != want {
		t.Errorf("executorExec: %v, want %v", err, want)
	}

	_, err = executorExec(executor, "select id from user where id in ::aa", nil)
	want = "paramsSelectIN: missing bind var aa"
	if err == nil || err.Error() != want {
		t.Errorf("executorExec: %v, want %v", err, want)
	}

	_, err = executorExec(executor, "select id from user where id in ::aa", map[string]*querypb.BindVariable{
		"aa": sqltypes.Int64BindVariable(1),
	})
	want = `paramsSelectIN: single value was supplied for TUPLE bind var aa`
	if err == nil || err.Error() != want {
		t.Errorf("executorExec:\n%v, want\n%v", err, want)
	}

	getSandbox("TestExecutor").SrvKeyspaceMustFail = 1
	_, err = executorExec(executor, "select id from user where id in (1)", nil)
	want = "paramsSelectEqual: keyspace TestExecutor fetch error: topo error GetSrvKeyspace"
	if err == nil || err.Error() != want {
		t.Errorf("executorExec: %v, want %v", err, want)
	}
}

func TestSelectScatter(t *testing.T) {
	// Special setup: Don't use createExecutorEnv.
	cell := "aa"
	hc := discovery.NewFakeHealthCheck()
	s := createSandbox("TestExecutor")
	s.VSchema = executorVSchema
	getSandbox(KsTestUnsharded).VSchema = unshardedVSchema
	serv := new(sandboxTopo)
	resolver := newTestResolver(hc, serv, cell)
	shards := []string{"-20", "20-40", "40-60", "60-80", "80-a0", "a0-c0", "c0-e0", "e0-"}
	var conns []*sandboxconn.SandboxConn
	for _, shard := range shards {
		sbc := hc.AddTestTablet(cell, shard, 1, "TestExecutor", shard, topodatapb.TabletType_MASTER, true, 1, nil)
		conns = append(conns, sbc)
	}
	executor := NewExecutor(context.Background(), serv, cell, "", resolver, false, testBufferSize)

	_, err := executorExec(executor, "select id from user", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select id from user",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	for _, conn := range conns {
		if !reflect.DeepEqual(conn.Queries, wantQueries) {
			t.Errorf("conn.Queries = %#v, want %#v", conn.Queries, wantQueries)
		}
	}
}

func TestStreamSelectScatter(t *testing.T) {
	// Special setup: Don't use createExecutorEnv.
	cell := "aa"
	hc := discovery.NewFakeHealthCheck()
	s := createSandbox("TestExecutor")
	s.VSchema = executorVSchema
	getSandbox(KsTestUnsharded).VSchema = unshardedVSchema
	serv := new(sandboxTopo)
	resolver := newTestResolver(hc, serv, cell)
	shards := []string{"-20", "20-40", "40-60", "60-80", "80-a0", "a0-c0", "c0-e0", "e0-"}
	var conns []*sandboxconn.SandboxConn
	for _, shard := range shards {
		sbc := hc.AddTestTablet(cell, shard, 1, "TestExecutor", shard, topodatapb.TabletType_MASTER, true, 1, nil)
		conns = append(conns, sbc)
	}
	executor := NewExecutor(context.Background(), serv, cell, "", resolver, false, testBufferSize)

	sql := "select id from user"
	result, err := executorStream(executor, sql)
	if err != nil {
		t.Error(err)
	}
	wantResult := &sqltypes.Result{
		Fields: sandboxconn.SingleRowResult.Fields,
		Rows: [][]sqltypes.Value{
			sandboxconn.StreamRowResult.Rows[0],
			sandboxconn.StreamRowResult.Rows[0],
			sandboxconn.StreamRowResult.Rows[0],
			sandboxconn.StreamRowResult.Rows[0],
			sandboxconn.StreamRowResult.Rows[0],
			sandboxconn.StreamRowResult.Rows[0],
			sandboxconn.StreamRowResult.Rows[0],
			sandboxconn.StreamRowResult.Rows[0],
		},
	}
	if !result.Equal(wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}
}

func TestSelectScatterFail(t *testing.T) {
	// Special setup: Don't use createExecutorEnv.
	cell := "aa"
	hc := discovery.NewFakeHealthCheck()
	s := createSandbox("TestExecutor")
	s.VSchema = executorVSchema
	getSandbox(KsTestUnsharded).VSchema = unshardedVSchema
	s.SrvKeyspaceMustFail = 1
	shards := []string{"-20", "20-40", "40-60", "60-80", "80-a0", "a0-c0", "c0-e0", "e0-"}
	var conns []*sandboxconn.SandboxConn
	for _, shard := range shards {
		sbc := hc.AddTestTablet(cell, shard, 1, "TestExecutor", shard, topodatapb.TabletType_MASTER, true, 1, nil)
		conns = append(conns, sbc)
	}
	serv := new(sandboxTopo)
	resolver := newTestResolver(hc, serv, cell)
	executor := NewExecutor(context.Background(), serv, cell, "", resolver, false, testBufferSize)

	_, err := executorExec(executor, "select id from user", nil)
	want := "paramsAllShards: keyspace TestExecutor fetch error: topo error GetSrvKeyspace"
	if err == nil || err.Error() != want {
		t.Errorf("executorExec: %v, want %v", err, want)
	}
}

// TestSelectScatterOrderBy will run an ORDER BY query that will scatter out to 8 shards and return the 8 rows (one per shard) sorted.
func TestSelectScatterOrderBy(t *testing.T) {
	// Special setup: Don't use createExecutorEnv.
	cell := "aa"
	hc := discovery.NewFakeHealthCheck()
	s := createSandbox("TestExecutor")
	s.VSchema = executorVSchema
	getSandbox(KsTestUnsharded).VSchema = unshardedVSchema
	serv := new(sandboxTopo)
	resolver := newTestResolver(hc, serv, cell)
	shards := []string{"-20", "20-40", "40-60", "60-80", "80-a0", "a0-c0", "c0-e0", "e0-"}
	var conns []*sandboxconn.SandboxConn
	for i, shard := range shards {
		sbc := hc.AddTestTablet(cell, shard, 1, "TestExecutor", shard, topodatapb.TabletType_MASTER, true, 1, nil)
		sbc.SetResults([]*sqltypes.Result{{
			Fields: []*querypb.Field{
				{Name: "col1", Type: sqltypes.Int32},
				{Name: "col2", Type: sqltypes.Int32},
			},
			RowsAffected: 1,
			InsertID:     0,
			Rows: [][]sqltypes.Value{{
				sqltypes.NewInt32(1),
				// i%4 ensures that there are duplicates across shards.
				// This will allow us to test that cross-shard ordering
				// still works correctly.
				sqltypes.NewInt32(int32(i % 4)),
			}},
		}})
		conns = append(conns, sbc)
	}
	executor := NewExecutor(context.Background(), serv, cell, "", resolver, false, testBufferSize)

	query := "select col1, col2 from user order by col2 desc"
	gotResult, err := executorExec(executor, query, nil)
	if err != nil {
		t.Error(err)
	}

	wantQueries := []*querypb.BoundQuery{{
		Sql:           query,
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	for _, conn := range conns {
		if !reflect.DeepEqual(conn.Queries, wantQueries) {
			t.Errorf("conn.Queries = %#v, want %#v", conn.Queries, wantQueries)
		}
	}

	wantResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "col1", Type: sqltypes.Int32},
			{Name: "col2", Type: sqltypes.Int32},
		},
		RowsAffected: 8,
		InsertID:     0,
	}
	for i := 0; i < 4; i++ {
		// There should be a duplicate for each row returned.
		for j := 0; j < 2; j++ {
			row := []sqltypes.Value{
				sqltypes.NewInt32(1),
				sqltypes.NewInt32(int32(3 - i)),
			}
			wantResult.Rows = append(wantResult.Rows, row)
		}
	}
	if !reflect.DeepEqual(gotResult, wantResult) {
		t.Errorf("scatter order by:\n%v, want\n%v", gotResult, wantResult)
	}
}

func TestStreamSelectScatterOrderBy(t *testing.T) {
	// Special setup: Don't use createExecutorEnv.
	cell := "aa"
	hc := discovery.NewFakeHealthCheck()
	s := createSandbox("TestExecutor")
	s.VSchema = executorVSchema
	getSandbox(KsTestUnsharded).VSchema = unshardedVSchema
	serv := new(sandboxTopo)
	resolver := newTestResolver(hc, serv, cell)
	shards := []string{"-20", "20-40", "40-60", "60-80", "80-a0", "a0-c0", "c0-e0", "e0-"}
	var conns []*sandboxconn.SandboxConn
	for i, shard := range shards {
		sbc := hc.AddTestTablet(cell, shard, 1, "TestExecutor", shard, topodatapb.TabletType_MASTER, true, 1, nil)
		sbc.SetResults([]*sqltypes.Result{{
			Fields: []*querypb.Field{
				{Name: "id", Type: sqltypes.Int32},
				{Name: "col", Type: sqltypes.Int32},
			},
			RowsAffected: 1,
			InsertID:     0,
			Rows: [][]sqltypes.Value{{
				sqltypes.NewInt32(1),
				sqltypes.NewInt32(int32(i % 4)),
			}},
		}})
		conns = append(conns, sbc)
	}
	executor := NewExecutor(context.Background(), serv, cell, "", resolver, false, 10)

	query := "select id, col from user order by col desc"
	gotResult, err := executorStream(executor, query)
	if err != nil {
		t.Error(err)
	}

	wantQueries := []*querypb.BoundQuery{{
		Sql:           query,
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	for _, conn := range conns {
		if !reflect.DeepEqual(conn.Queries, wantQueries) {
			t.Errorf("conn.Queries = %#v, want %#v", conn.Queries, wantQueries)
		}
	}

	wantResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
			{Name: "col", Type: sqltypes.Int32},
		},
	}
	for i := 0; i < 4; i++ {
		row := []sqltypes.Value{
			sqltypes.NewInt32(1),
			sqltypes.NewInt32(int32(3 - i)),
		}
		wantResult.Rows = append(wantResult.Rows, row, row)
	}
	if !reflect.DeepEqual(gotResult, wantResult) {
		t.Errorf("scatter order by:\n%v, want\n%v", gotResult, wantResult)
	}
}

// TestSelectScatterOrderByFail will run an ORDER BY query that will scatter out to 8 shards, with
// the order by column as VarChar, which is unsupported.
func TestSelectScatterOrderByFail(t *testing.T) {
	// Special setup: Don't use createExecutorEnv.
	cell := "aa"
	hc := discovery.NewFakeHealthCheck()
	s := createSandbox("TestExecutor")
	s.VSchema = executorVSchema
	getSandbox(KsTestUnsharded).VSchema = unshardedVSchema
	serv := new(sandboxTopo)
	resolver := newTestResolver(hc, serv, cell)
	shards := []string{"-20", "20-40", "40-60", "60-80", "80-a0", "a0-c0", "c0-e0", "e0-"}
	for _, shard := range shards {
		sbc := hc.AddTestTablet(cell, shard, 1, "TestExecutor", shard, topodatapb.TabletType_MASTER, true, 1, nil)
		sbc.SetResults([]*sqltypes.Result{{
			Fields: []*querypb.Field{
				{Name: "id", Type: sqltypes.Int32},
				{Name: "col", Type: sqltypes.Int32},
			},
			RowsAffected: 1,
			InsertID:     0,
			Rows: [][]sqltypes.Value{{
				sqltypes.NewInt32(1),
				sqltypes.NewVarChar("aaa"),
			}},
		}})
	}
	executor := NewExecutor(context.Background(), serv, cell, "", resolver, false, testBufferSize)

	_, err := executorExec(executor, "select id, col from user order by col asc", nil)
	want := "types are not comparable: VARCHAR vs VARCHAR"
	if err == nil || err.Error() != want {
		t.Errorf("scatter order by error: %v, want %s", err, want)
	}
}

// TestSelectScatterAggregate will run an aggregate query that will scatter out to 8 shards and return 4 aggregated rows.
func TestSelectScatterAggregate(t *testing.T) {
	// Special setup: Don't use createExecutorEnv.
	cell := "aa"
	hc := discovery.NewFakeHealthCheck()
	s := createSandbox("TestExecutor")
	s.VSchema = executorVSchema
	getSandbox(KsTestUnsharded).VSchema = unshardedVSchema
	serv := new(sandboxTopo)
	resolver := newTestResolver(hc, serv, cell)
	shards := []string{"-20", "20-40", "40-60", "60-80", "80-a0", "a0-c0", "c0-e0", "e0-"}
	var conns []*sandboxconn.SandboxConn
	for i, shard := range shards {
		sbc := hc.AddTestTablet(cell, shard, 1, "TestExecutor", shard, topodatapb.TabletType_MASTER, true, 1, nil)
		sbc.SetResults([]*sqltypes.Result{{
			Fields: []*querypb.Field{
				{Name: "col", Type: sqltypes.Int32},
				{Name: "sum(foo)", Type: sqltypes.Int32},
			},
			RowsAffected: 1,
			InsertID:     0,
			Rows: [][]sqltypes.Value{{
				sqltypes.NewInt32(int32(i % 4)),
				sqltypes.NewInt32(int32(i)),
			}},
		}})
		conns = append(conns, sbc)
	}
	executor := NewExecutor(context.Background(), serv, cell, "", resolver, false, testBufferSize)

	query := "select col, sum(foo) from user group by col"
	gotResult, err := executorExec(executor, query, nil)
	if err != nil {
		t.Error(err)
	}

	wantQueries := []*querypb.BoundQuery{{
		Sql:           query + " order by col asc",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	for _, conn := range conns {
		if !reflect.DeepEqual(conn.Queries, wantQueries) {
			t.Errorf("conn.Queries = %#v, want %#v", conn.Queries, wantQueries)
		}
	}

	wantResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "col", Type: sqltypes.Int32},
			{Name: "sum(foo)", Type: sqltypes.Int32},
		},
		RowsAffected: 4,
		InsertID:     0,
	}
	for i := 0; i < 4; i++ {
		row := []sqltypes.Value{
			sqltypes.NewInt32(int32(i)),
			sqltypes.NewInt32(int32(i*2 + 4)),
		}
		wantResult.Rows = append(wantResult.Rows, row)
	}
	if !reflect.DeepEqual(gotResult, wantResult) {
		t.Errorf("scatter order by:\n%v, want\n%v", gotResult, wantResult)
	}
}

func TestStreamSelectScatterAggregate(t *testing.T) {
	// Special setup: Don't use createExecutorEnv.
	cell := "aa"
	hc := discovery.NewFakeHealthCheck()
	s := createSandbox("TestExecutor")
	s.VSchema = executorVSchema
	getSandbox(KsTestUnsharded).VSchema = unshardedVSchema
	serv := new(sandboxTopo)
	resolver := newTestResolver(hc, serv, cell)
	shards := []string{"-20", "20-40", "40-60", "60-80", "80-a0", "a0-c0", "c0-e0", "e0-"}
	var conns []*sandboxconn.SandboxConn
	for i, shard := range shards {
		sbc := hc.AddTestTablet(cell, shard, 1, "TestExecutor", shard, topodatapb.TabletType_MASTER, true, 1, nil)
		sbc.SetResults([]*sqltypes.Result{{
			Fields: []*querypb.Field{
				{Name: "col", Type: sqltypes.Int32},
				{Name: "sum(foo)", Type: sqltypes.Int32},
			},
			RowsAffected: 1,
			InsertID:     0,
			Rows: [][]sqltypes.Value{{
				sqltypes.NewInt32(int32(i % 4)),
				sqltypes.NewInt32(int32(i)),
			}},
		}})
		conns = append(conns, sbc)
	}
	executor := NewExecutor(context.Background(), serv, cell, "", resolver, false, 10)

	query := "select col, sum(foo) from user group by col"
	gotResult, err := executorStream(executor, query)
	if err != nil {
		t.Error(err)
	}

	wantQueries := []*querypb.BoundQuery{{
		Sql:           query + " order by col asc",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	for _, conn := range conns {
		if !reflect.DeepEqual(conn.Queries, wantQueries) {
			t.Errorf("conn.Queries = %#v, want %#v", conn.Queries, wantQueries)
		}
	}

	wantResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "col", Type: sqltypes.Int32},
			{Name: "sum(foo)", Type: sqltypes.Int32},
		},
	}
	for i := 0; i < 4; i++ {
		row := []sqltypes.Value{
			sqltypes.NewInt32(int32(i)),
			sqltypes.NewInt32(int32(i*2 + 4)),
		}
		wantResult.Rows = append(wantResult.Rows, row)
	}
	if !reflect.DeepEqual(gotResult, wantResult) {
		t.Errorf("scatter order by:\n%v, want\n%v", gotResult, wantResult)
	}
}

// TestSelectScatterLimit will run a limit query (ordered for consistency) against
// a scatter route and verify that the limit primitive works as intended.
func TestSelectScatterLimit(t *testing.T) {
	// Special setup: Don't use createExecutorEnv.
	cell := "aa"
	hc := discovery.NewFakeHealthCheck()
	s := createSandbox("TestExecutor")
	s.VSchema = executorVSchema
	getSandbox(KsTestUnsharded).VSchema = unshardedVSchema
	serv := new(sandboxTopo)
	resolver := newTestResolver(hc, serv, cell)
	shards := []string{"-20", "20-40", "40-60", "60-80", "80-a0", "a0-c0", "c0-e0", "e0-"}
	var conns []*sandboxconn.SandboxConn
	for i, shard := range shards {
		sbc := hc.AddTestTablet(cell, shard, 1, "TestExecutor", shard, topodatapb.TabletType_MASTER, true, 1, nil)
		sbc.SetResults([]*sqltypes.Result{{
			Fields: []*querypb.Field{
				{Name: "col1", Type: sqltypes.Int32},
				{Name: "col2", Type: sqltypes.Int32},
			},
			RowsAffected: 1,
			InsertID:     0,
			Rows: [][]sqltypes.Value{{
				sqltypes.NewInt32(1),
				sqltypes.NewInt32(int32(i % 4)),
			}},
		}})
		conns = append(conns, sbc)
	}
	executor := NewExecutor(context.Background(), serv, cell, "", resolver, false, 10)

	query := "select col1, col2 from user order by col2 desc limit 3"
	gotResult, err := executorExec(executor, query, nil)
	if err != nil {
		t.Error(err)
	}

	wantQueries := []*querypb.BoundQuery{{
		Sql:           query,
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	for _, conn := range conns {
		if !reflect.DeepEqual(conn.Queries, wantQueries) {
			t.Errorf("conn.Queries = %#v, want %#v", conn.Queries, wantQueries)
		}
	}

	wantResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "col1", Type: sqltypes.Int32},
			{Name: "col2", Type: sqltypes.Int32},
		},
		RowsAffected: 3,
		InsertID:     0,
	}
	wantResult.Rows = append(wantResult.Rows,
		[]sqltypes.Value{
			sqltypes.NewInt32(1),
			sqltypes.NewInt32(3),
		},
		[]sqltypes.Value{
			sqltypes.NewInt32(1),
			sqltypes.NewInt32(3),
		},
		[]sqltypes.Value{
			sqltypes.NewInt32(1),
			sqltypes.NewInt32(2),
		})

	if !reflect.DeepEqual(gotResult, wantResult) {
		t.Errorf("scatter order by:\n%v, want\n%v", gotResult, wantResult)
	}
}

// TestStreamSelectScatterLimit will run a streaming limit query (ordered for consistency) against
// a scatter route and verify that the limit primitive works as intended.
func TestStreamSelectScatterLimit(t *testing.T) {
	// Special setup: Don't use createExecutorEnv.
	cell := "aa"
	hc := discovery.NewFakeHealthCheck()
	s := createSandbox("TestExecutor")
	s.VSchema = executorVSchema
	getSandbox(KsTestUnsharded).VSchema = unshardedVSchema
	serv := new(sandboxTopo)
	resolver := newTestResolver(hc, serv, cell)
	shards := []string{"-20", "20-40", "40-60", "60-80", "80-a0", "a0-c0", "c0-e0", "e0-"}
	var conns []*sandboxconn.SandboxConn
	for i, shard := range shards {
		sbc := hc.AddTestTablet(cell, shard, 1, "TestExecutor", shard, topodatapb.TabletType_MASTER, true, 1, nil)
		sbc.SetResults([]*sqltypes.Result{{
			Fields: []*querypb.Field{
				{Name: "col1", Type: sqltypes.Int32},
				{Name: "col2", Type: sqltypes.Int32},
			},
			RowsAffected: 1,
			InsertID:     0,
			Rows: [][]sqltypes.Value{{
				sqltypes.NewInt32(1),
				sqltypes.NewInt32(int32(i % 4)),
			}},
		}})
		conns = append(conns, sbc)
	}
	executor := NewExecutor(context.Background(), serv, cell, "", resolver, false, 10)

	query := "select col1, col2 from user order by col2 desc limit 3"
	gotResult, err := executorStream(executor, query)
	if err != nil {
		t.Error(err)
	}

	wantQueries := []*querypb.BoundQuery{{
		Sql:           query,
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	for _, conn := range conns {
		if !reflect.DeepEqual(conn.Queries, wantQueries) {
			t.Errorf("conn.Queries = %#v, want %#v", conn.Queries, wantQueries)
		}
	}

	wantResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "col1", Type: sqltypes.Int32},
			{Name: "col2", Type: sqltypes.Int32},
		},
	}
	wantResult.Rows = append(wantResult.Rows,
		[]sqltypes.Value{
			sqltypes.NewInt32(1),
			sqltypes.NewInt32(3),
		},
		[]sqltypes.Value{
			sqltypes.NewInt32(1),
			sqltypes.NewInt32(3),
		},
		[]sqltypes.Value{
			sqltypes.NewInt32(1),
			sqltypes.NewInt32(2),
		})

	if !reflect.DeepEqual(gotResult, wantResult) {
		t.Errorf("scatter order by:\n%v, want\n%v", gotResult, wantResult)
	}
}

// TODO(sougou): stream and non-stream testing are very similar.
// Could reuse code,
func TestSimpleJoin(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()
	result, err := executorExec(executor, "select u1.id, u2.id from user u1 join user u2 where u1.id = 1 and u2.id = 3", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select u1.id from user as u1 where u1.id = 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries: %+v, want %+v\n", sbc1.Queries, wantQueries)
	}
	wantQueries = []*querypb.BoundQuery{{
		Sql:           "select u2.id from user as u2 where u2.id = 3",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	if !reflect.DeepEqual(sbc2.Queries, wantQueries) {
		t.Errorf("sbc2.Queries: %+v, want %+v\n", sbc2.Queries, wantQueries)
	}
	wantResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			sandboxconn.SingleRowResult.Fields[0],
			sandboxconn.SingleRowResult.Fields[0],
		},
		Rows: [][]sqltypes.Value{
			{
				sandboxconn.SingleRowResult.Rows[0][0],
				sandboxconn.SingleRowResult.Rows[0][0],
			},
		},
		RowsAffected: 1,
	}
	if !result.Equal(wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}
}

func TestJoinComments(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()
	_, err := executorExec(executor, "select u1.id, u2.id from user u1 join user u2 where u1.id = 1 and u2.id = 3 /* trailing */", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select u1.id from user as u1 where u1.id = 1 /* trailing */",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries: %+v, want %+v\n", sbc1.Queries, wantQueries)
	}
	wantQueries = []*querypb.BoundQuery{{
		Sql:           "select u2.id from user as u2 where u2.id = 3 /* trailing */",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	if !reflect.DeepEqual(sbc2.Queries, wantQueries) {
		t.Errorf("sbc2.Queries: %+v, want %+v\n", sbc2.Queries, wantQueries)
	}
}

func TestSimpleJoinStream(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()
	result, err := executorStream(executor, "select u1.id, u2.id from user u1 join user u2 where u1.id = 1 and u2.id = 3")
	if err != nil {
		t.Error(err)
	}
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select u1.id from user as u1 where u1.id = 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries: %+v, want %+v\n", sbc1.Queries, wantQueries)
	}
	wantQueries = []*querypb.BoundQuery{{
		Sql:           "select u2.id from user as u2 where u2.id = 3",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	if !reflect.DeepEqual(sbc2.Queries, wantQueries) {
		t.Errorf("sbc2.Queries: %+v, want %+v\n", sbc2.Queries, wantQueries)
	}
	wantResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			sandboxconn.SingleRowResult.Fields[0],
			sandboxconn.SingleRowResult.Fields[0],
		},
		Rows: [][]sqltypes.Value{
			{
				sandboxconn.SingleRowResult.Rows[0][0],
				sandboxconn.SingleRowResult.Rows[0][0],
			},
		},
		RowsAffected: 0,
	}
	if !result.Equal(wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}
}

func TestVarJoin(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()
	result1 := []*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
			{Name: "col", Type: sqltypes.Int32},
		},
		RowsAffected: 1,
		InsertID:     0,
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt32(1),
			sqltypes.NewInt32(3),
		}},
	}}
	sbc1.SetResults(result1)
	_, err := executorExec(executor, "select u1.id, u2.id from user u1 join user u2 on u2.id = u1.col where u1.id = 1", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select u1.id, u1.col from user as u1 where u1.id = 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries: %+v, want %+v\n", sbc1.Queries, wantQueries)
	}
	// We have to use string representation because bindvars type is too complex.
	got := fmt.Sprintf("%+v", sbc2.Queries)
	want := `[sql:"select u2.id from user as u2 where u2.id = :u1_col" bind_variables:<key:"u1_col" value:<type:INT32 value:"3" > > ]`
	if got != want {
		t.Errorf("sbc2.Queries: %s, want %s\n", got, want)
	}
}

func TestVarJoinStream(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()
	result1 := []*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
			{Name: "col", Type: sqltypes.Int32},
		},
		RowsAffected: 1,
		InsertID:     0,
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt32(1),
			sqltypes.NewInt32(3),
		}},
	}}
	sbc1.SetResults(result1)
	_, err := executorStream(executor, "select u1.id, u2.id from user u1 join user u2 on u2.id = u1.col where u1.id = 1")
	if err != nil {
		t.Error(err)
	}
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select u1.id, u1.col from user as u1 where u1.id = 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries: %+v, want %+v\n", sbc1.Queries, wantQueries)
	}
	wantQueries = []*querypb.BoundQuery{{
		Sql:           "select u2.id from user as u2 where u2.id = :u1_col",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	// We have to use string representation because bindvars type is too complex.
	got := fmt.Sprintf("%+v", sbc2.Queries)
	want := `[sql:"select u2.id from user as u2 where u2.id = :u1_col" bind_variables:<key:"u1_col" value:<type:INT32 value:"3" > > ]`
	if got != want {
		t.Errorf("sbc2.Queries: %s, want %s\n", got, want)
	}
}

func TestLeftJoin(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()
	result1 := []*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
			{Name: "col", Type: sqltypes.Int32},
		},
		RowsAffected: 1,
		InsertID:     0,
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt32(1),
			sqltypes.NewInt32(3),
		}},
	}}
	emptyResult := []*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
		},
	}}
	sbc1.SetResults(result1)
	sbc2.SetResults(emptyResult)
	result, err := executorExec(executor, "select u1.id, u2.id from user u1 left join user u2 on u2.id = u1.col where u1.id = 1", nil)
	if err != nil {
		t.Error(err)
	}
	wantResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			sandboxconn.SingleRowResult.Fields[0],
			sandboxconn.SingleRowResult.Fields[0],
		},
		Rows: [][]sqltypes.Value{
			{
				sandboxconn.SingleRowResult.Rows[0][0],
				{},
			},
		},
		RowsAffected: 1,
	}
	if !result.Equal(wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}
}

func TestLeftJoinStream(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()
	result1 := []*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
			{Name: "col", Type: sqltypes.Int32},
		},
		RowsAffected: 1,
		InsertID:     0,
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt32(1),
			sqltypes.NewInt32(3),
		}},
	}}
	emptyResult := []*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
		},
	}}
	sbc1.SetResults(result1)
	sbc2.SetResults(emptyResult)
	result, err := executorStream(executor, "select u1.id, u2.id from user u1 left join user u2 on u2.id = u1.col where u1.id = 1")
	if err != nil {
		t.Error(err)
	}
	wantResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			sandboxconn.SingleRowResult.Fields[0],
			sandboxconn.SingleRowResult.Fields[0],
		},
		Rows: [][]sqltypes.Value{
			{
				sandboxconn.SingleRowResult.Rows[0][0],
				{},
			},
		},
		RowsAffected: 0,
	}
	if !result.Equal(wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}
}

func TestEmptyJoin(t *testing.T) {
	executor, sbc1, _, _ := createExecutorEnv()
	// Empty result requires a field query for the second part of join,
	// which is sent to shard 0.
	sbc1.SetResults([]*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
		},
	}, {
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
		},
	}})
	result, err := executorExec(executor, "select u1.id, u2.id from user u1 join user u2 on u2.id = u1.col where u1.id = 1", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select u1.id, u1.col from user as u1 where u1.id = 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}, {
		Sql: "select u2.id from user as u2 where 1 != 1",
		BindVariables: map[string]*querypb.BindVariable{
			"u1_col": sqltypes.NullBindVariable,
		},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries:\n%v, want\n%v\n", sbc1.Queries, wantQueries)
	}
	wantResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
			{Name: "id", Type: sqltypes.Int32},
		},
	}
	if !result.Equal(wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}
}

func TestEmptyJoinStream(t *testing.T) {
	executor, sbc1, _, _ := createExecutorEnv()
	// Empty result requires a field query for the second part of join,
	// which is sent to shard 0.
	sbc1.SetResults([]*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
		},
	}, {
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
		},
	}})
	result, err := executorStream(executor, "select u1.id, u2.id from user u1 join user u2 on u2.id = u1.col where u1.id = 1")
	if err != nil {
		t.Error(err)
	}
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select u1.id, u1.col from user as u1 where u1.id = 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}, {
		Sql: "select u2.id from user as u2 where 1 != 1",
		BindVariables: map[string]*querypb.BindVariable{
			"u1_col": sqltypes.NullBindVariable,
		},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries: %+v, want %+v\n", sbc1.Queries, wantQueries)
	}
	wantResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
			{Name: "id", Type: sqltypes.Int32},
		},
	}
	if !result.Equal(wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}
}

func TestEmptyJoinRecursive(t *testing.T) {
	executor, sbc1, _, _ := createExecutorEnv()
	// Make sure it also works recursively.
	sbc1.SetResults([]*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
		},
	}, {
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
			{Name: "col", Type: sqltypes.Int32},
		},
	}, {
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
		},
	}})
	result, err := executorExec(executor, "select u1.id, u2.id, u3.id from user u1 join (user u2 join user u3 on u3.id = u2.col) where u1.id = 1", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select u1.id from user as u1 where u1.id = 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}, {
		Sql:           "select u2.id, u2.col from user as u2 where 1 != 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}, {
		Sql: "select u3.id from user as u3 where 1 != 1",
		BindVariables: map[string]*querypb.BindVariable{
			"u2_col": sqltypes.NullBindVariable,
		},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries:\n%+v, want\n%+v\n", sbc1.Queries, wantQueries)
	}
	wantResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
			{Name: "id", Type: sqltypes.Int32},
			{Name: "id", Type: sqltypes.Int32},
		},
	}
	if !result.Equal(wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}
}

func TestEmptyJoinRecursiveStream(t *testing.T) {
	executor, sbc1, _, _ := createExecutorEnv()
	// Make sure it also works recursively.
	sbc1.SetResults([]*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
		},
	}, {
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
			{Name: "col", Type: sqltypes.Int32},
		},
	}, {
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
		},
	}})
	result, err := executorStream(executor, "select u1.id, u2.id, u3.id from user u1 join (user u2 join user u3 on u3.id = u2.col) where u1.id = 1")
	if err != nil {
		t.Error(err)
	}
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select u1.id from user as u1 where u1.id = 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}, {
		Sql:           "select u2.id, u2.col from user as u2 where 1 != 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}, {
		Sql: "select u3.id from user as u3 where 1 != 1",
		BindVariables: map[string]*querypb.BindVariable{
			"u2_col": sqltypes.NullBindVariable,
		},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries: %+v, want %+v\n", sbc1.Queries, wantQueries)
	}
	wantResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
			{Name: "id", Type: sqltypes.Int32},
			{Name: "id", Type: sqltypes.Int32},
		},
	}
	if !result.Equal(wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}
}

func TestJoinErrors(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()

	// First query fails
	sbc1.MustFailCodes[vtrpcpb.Code_INVALID_ARGUMENT] = 1
	_, err := executorExec(executor, "select u1.id, u2.id from user u1 join user u2 on u2.id = u1.col where u1.id = 1", nil)
	want := "INVALID_ARGUMENT error"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("err: %v, must contain %s", err, want)
	}

	// Field query fails
	sbc2.SetResults([]*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
		},
	}})
	sbc1.MustFailCodes[vtrpcpb.Code_INVALID_ARGUMENT] = 1
	_, err = executorExec(executor, "select u1.id, u2.id from user u1 join user u2 on u2.id = u1.col where u1.id = 3", nil)
	want = "INVALID_ARGUMENT error"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("err: %v, must contain %s", err, want)
	}

	// Second query fails
	sbc1.SetResults([]*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
			{Name: "col", Type: sqltypes.Int32},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt32(1),
			sqltypes.NewInt32(3),
		}},
	}})
	sbc2.MustFailCodes[vtrpcpb.Code_INVALID_ARGUMENT] = 1
	_, err = executorExec(executor, "select u1.id, u2.id from user u1 join user u2 on u2.id = u1.col where u1.id = 1", nil)
	want = "INVALID_ARGUMENT error"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("err: %v, must contain %s", err, want)
	}

	// Nested join query fails on get fields
	sbc2.SetResults([]*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
			{Name: "col", Type: sqltypes.Int32},
		},
	}})
	sbc1.MustFailCodes[vtrpcpb.Code_INVALID_ARGUMENT] = 1
	_, err = executorExec(executor, "select u1.id, u2.id from user u1 join (user u2 join user u3 on u3.id = u2.col) where u1.id = 3", nil)
	want = "INVALID_ARGUMENT error"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("err: %v, must contain %s", err, want)
	}

	// Field query fails on stream join
	sbc2.SetResults([]*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
		},
	}})
	sbc1.MustFailCodes[vtrpcpb.Code_INVALID_ARGUMENT] = 1
	_, err = executorStream(executor, "select u1.id, u2.id from user u1 join user u2 on u2.id = u1.col where u1.id = 3")
	want = "INVALID_ARGUMENT error"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("err: %v, must contain %s", err, want)
	}

	// Second query fails on stream join
	sbc1.SetResults([]*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
			{Name: "col", Type: sqltypes.Int32},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt32(1),
			sqltypes.NewInt32(3),
		}},
	}})
	sbc2.MustFailCodes[vtrpcpb.Code_INVALID_ARGUMENT] = 1
	_, err = executorStream(executor, "select u1.id, u2.id from user u1 join user u2 on u2.id = u1.col where u1.id = 1")
	want = "INVALID_ARGUMENT error"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("err: %v, must contain %s", err, want)
	}
}

func TestCrossShardSubquery(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()
	result1 := []*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
			{Name: "col", Type: sqltypes.Int32},
		},
		RowsAffected: 1,
		InsertID:     0,
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt32(1),
			sqltypes.NewInt32(3),
		}},
	}}
	sbc1.SetResults(result1)
	result, err := executorExec(executor, "select id1 from (select u1.id id1, u2.id from user u1 join user u2 on u2.id = u1.col where u1.id = 1) as t", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select u1.id as id1, u1.col from user as u1 where u1.id = 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries: %+v, want %+v\n", sbc1.Queries, wantQueries)
	}
	// We have to use string representation because bindvars type is too complex.
	got := fmt.Sprintf("%+v", sbc2.Queries)
	want := `[sql:"select u2.id from user as u2 where u2.id = :u1_col" bind_variables:<key:"u1_col" value:<type:INT32 value:"3" > > ]`
	if got != want {
		t.Errorf("sbc2.Queries: %s, want %s\n", got, want)
	}

	wantResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
		},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt32(1),
		}},
	}
	if !result.Equal(wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}
}

func TestCrossShardSubqueryStream(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()
	result1 := []*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
			{Name: "col", Type: sqltypes.Int32},
		},
		RowsAffected: 1,
		InsertID:     0,
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt32(1),
			sqltypes.NewInt32(3),
		}},
	}}
	sbc1.SetResults(result1)
	result, err := executorStream(executor, "select id1 from (select u1.id id1, u2.id from user u1 join user u2 on u2.id = u1.col where u1.id = 1) as t")
	if err != nil {
		t.Error(err)
	}
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select u1.id as id1, u1.col from user as u1 where u1.id = 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries:\n%+v, want\n%+v\n", sbc1.Queries, wantQueries)
	}
	// We have to use string representation because bindvars type is too complex.
	got := fmt.Sprintf("%+v", sbc2.Queries)
	want := `[sql:"select u2.id from user as u2 where u2.id = :u1_col" bind_variables:<key:"u1_col" value:<type:INT32 value:"3" > > ]`
	if got != want {
		t.Errorf("sbc2.Queries:\n%s, want\n%s\n", got, want)
	}

	wantResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt32(1),
		}},
	}
	if !result.Equal(wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}
}

func TestCrossShardSubqueryGetFields(t *testing.T) {
	executor, sbc1, _, sbclookup := createExecutorEnv()
	sbclookup.SetResults([]*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "col", Type: sqltypes.Int32},
		},
	}})
	result1 := []*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
			{Name: "col", Type: sqltypes.Int32},
		},
	}}
	sbc1.SetResults(result1)
	result, err := executorExec(executor, "select main1.col, t.id1 from main1 join (select u1.id id1, u2.id from user u1 join user u2 on u2.id = u1.col where u1.id = 1) as t", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select u1.id as id1, u1.col from user as u1 where 1 != 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}, {
		Sql: "select u2.id from user as u2 where 1 != 1",
		BindVariables: map[string]*querypb.BindVariable{
			"u1_col": sqltypes.NullBindVariable,
		},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries:\n%+v, want\n%+v\n", sbc1.Queries, wantQueries)
	}

	wantResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "col", Type: sqltypes.Int32},
			{Name: "id", Type: sqltypes.Int32},
		},
	}
	if !result.Equal(wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}
}
