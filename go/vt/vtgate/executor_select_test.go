// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

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
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/querytypes"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

func TestSelectNext(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()

	query := "select next :n values from user_seq"
	bv := map[string]interface{}{"n": 2}
	_, err := executorExec(executor, query, bv)
	if err != nil {
		t.Error(err)
	}
	wantQueries := []querytypes.BoundQuery{{
		Sql:           query,
		BindVariables: bv,
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: %+v, want %+v\n", sbclookup.Queries, wantQueries)
	}
}

func TestUnsharded(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()

	_, err := executorExec(executor, "select id from music_user_map where id = 1", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries := []querytypes.BoundQuery{{
		Sql:           "select id from music_user_map where id = 1",
		BindVariables: map[string]interface{}{},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: %+v, want %+v\n", sbclookup.Queries, wantQueries)
	}

	_, err = executorExec(executor, "update music_user_map set id = 1", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries = []querytypes.BoundQuery{{
		Sql:           "select id from music_user_map where id = 1",
		BindVariables: map[string]interface{}{},
	}, {
		Sql:           "update music_user_map set id = 1",
		BindVariables: map[string]interface{}{},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: %+v, want %+v\n", sbclookup.Queries, wantQueries)
	}

	sbclookup.Queries = nil
	_, err = executorExec(executor, "delete from music_user_map", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries = []querytypes.BoundQuery{{
		Sql:           "delete from music_user_map",
		BindVariables: map[string]interface{}{},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: %+v, want %+v\n", sbclookup.Queries, wantQueries)
	}

	sbclookup.Queries = nil
	_, err = executorExec(executor, "insert into music_user_map values (1)", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries = []querytypes.BoundQuery{{
		Sql:           "insert into music_user_map values (1)",
		BindVariables: map[string]interface{}{},
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
	wantQueries := []querytypes.BoundQuery{{
		Sql:           "select id from music_user_map where id = 1 /* trailing */",
		BindVariables: map[string]interface{}{},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: %+v, want %+v\n", sbclookup.Queries, wantQueries)
	}

	_, err = executorExec(executor, "update music_user_map set id = 1 /* trailing */", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries = []querytypes.BoundQuery{{
		Sql:           "select id from music_user_map where id = 1 /* trailing */",
		BindVariables: map[string]interface{}{},
	}, {
		Sql:           "update music_user_map set id = 1 /* trailing */",
		BindVariables: map[string]interface{}{},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: %+v, want %+v\n", sbclookup.Queries, wantQueries)
	}

	sbclookup.Queries = nil
	_, err = executorExec(executor, "delete from music_user_map /* trailing */", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries = []querytypes.BoundQuery{{
		Sql:           "delete from music_user_map /* trailing */",
		BindVariables: map[string]interface{}{},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: %+v, want %+v\n", sbclookup.Queries, wantQueries)
	}

	sbclookup.Queries = nil
	_, err = executorExec(executor, "insert into music_user_map values (1) /* trailing */", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries = []querytypes.BoundQuery{{
		Sql:           "insert into music_user_map values (1) /* trailing */",
		BindVariables: map[string]interface{}{},
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
	wantResult := sandboxconn.SingleRowResult
	if !reflect.DeepEqual(result, wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}
}

func TestShardFail(t *testing.T) {
	executor, _, _, _ := createExecutorEnv()

	getSandbox(KsTestUnsharded).SrvKeyspaceMustFail = 1

	_, err := executorExec(executor, "select id from sharded_table where id = 1", nil)
	want := "paramsAllShards: unsharded keyspace TestBadSharding has multiple shards: possible cause: sharded keyspace is marked as unsharded in vschema"
	if err == nil || err.Error() != want {
		t.Errorf("executorExec: %v, want %v", err, want)
	}
}

func TestSelectBindvars(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()

	_, err := executorExec(executor, "select id from user where id = :id", map[string]interface{}{
		"id": 1,
	})
	if err != nil {
		t.Error(err)
	}
	wantQueries := []querytypes.BoundQuery{{
		Sql:           "select id from user where id = :id",
		BindVariables: map[string]interface{}{"id": 1},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries: %+v, want %+v\n", sbc1.Queries, wantQueries)
	}
	if sbc2.Queries != nil {
		t.Errorf("sbc2.Queries: %+v, want nil\n", sbc2.Queries)
	}
	sbc1.Queries = nil

	_, err = executorExec(executor, "select id from user where name in (:name1, :name2)", map[string]interface{}{
		"name1": "foo1",
		"name2": "foo2",
	})
	if err != nil {
		t.Error(err)
	}
	wantQueries = []querytypes.BoundQuery{{
		Sql: "select id from user where name in ::__vals",
		BindVariables: map[string]interface{}{
			"name1":  "foo1",
			"name2":  "foo2",
			"__vals": []interface{}{"foo1", "foo2"},
		},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries: %+v, want %+v\n", sbc1.Queries, wantQueries)
	}
	sbc1.Queries = nil

	_, err = executorExec(executor, "select id from user where name in (:name1, :name2)", map[string]interface{}{
		"name1": []byte("foo1"),
		"name2": []byte("foo2"),
	})
	if err != nil {
		t.Error(err)
	}
	wantQueries = []querytypes.BoundQuery{{
		Sql: "select id from user where name in ::__vals",
		BindVariables: map[string]interface{}{
			"name1":  []byte("foo1"),
			"name2":  []byte("foo2"),
			"__vals": []interface{}{[]byte("foo1"), []byte("foo2")},
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
	wantQueries := []querytypes.BoundQuery{{
		Sql:           "select id from user where id = 1",
		BindVariables: map[string]interface{}{},
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
	wantQueries = []querytypes.BoundQuery{{
		Sql:           "select id from user where id = 3",
		BindVariables: map[string]interface{}{},
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
	wantQueries = []querytypes.BoundQuery{{
		Sql:           "select id from user where id = '3'",
		BindVariables: map[string]interface{}{},
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
	wantQueries = []querytypes.BoundQuery{{
		Sql:           "select id from user where name = 'foo'",
		BindVariables: map[string]interface{}{},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries: %+v, want %+v\n", sbc1.Queries, wantQueries)
	}
	wantQueries = []querytypes.BoundQuery{{
		Sql: "select user_id from name_user_map where name = :name",
		BindVariables: map[string]interface{}{
			"name": []byte("foo"),
		},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: %+v, want %+v\n", sbclookup.Queries, wantQueries)
	}
}

func TestSelectComments(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()

	_, err := executorExec(executor, "select id from user where id = 1 /* trailing */", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries := []querytypes.BoundQuery{{
		Sql:           "select id from user where id = 1 /* trailing */",
		BindVariables: map[string]interface{}{},
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
	wantQueries := []querytypes.BoundQuery{{
		Sql:           "select Id from user where iD = 1",
		BindVariables: map[string]interface{}{},
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
	if !reflect.DeepEqual(result, wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}

	sbclookup.SetResults([]*sqltypes.Result{{}})
	result, err = executorExec(executor, "select id from user where name = 'foo'", nil)
	if err != nil {
		t.Error(err)
	}
	wantResult = &sqltypes.Result{}
	if !reflect.DeepEqual(result, wantResult) {
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
	wantResult := sandboxconn.SingleRowResult
	if !reflect.DeepEqual(result, wantResult) {
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
	want = "paramsSelectEqual: could not find bind var :aa"
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
	wantQueries := []querytypes.BoundQuery{{
		Sql: "select id from user where id in ::__vals",
		BindVariables: map[string]interface{}{
			"__vals": []interface{}{int64(1)},
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
	wantQueries = []querytypes.BoundQuery{{
		Sql: "select id from user where id in ::__vals",
		BindVariables: map[string]interface{}{
			"__vals": []interface{}{int64(1)},
		},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries: %+v, want %+v\n", sbc1.Queries, wantQueries)
	}
	wantQueries = []querytypes.BoundQuery{{
		Sql: "select id from user where id in ::__vals",
		BindVariables: map[string]interface{}{
			"__vals": []interface{}{int64(3)},
		},
	}}
	if !reflect.DeepEqual(sbc2.Queries, wantQueries) {
		t.Errorf("sbc2.Queries: %+v, want %+v\n", sbc2.Queries, wantQueries)
	}

	// In is a bind variable list, that will end up on two shards.
	// This is using an []interface{} for the bind variable list.
	sbc1.Queries = nil
	sbc2.Queries = nil
	_, err = executorExec(executor, "select id from user where id in ::vals", map[string]interface{}{
		"vals": []interface{}{int64(1), int64(3)},
	})
	if err != nil {
		t.Error(err)
	}
	wantQueries = []querytypes.BoundQuery{{
		Sql: "select id from user where id in ::__vals",
		BindVariables: map[string]interface{}{
			"__vals": []interface{}{int64(1)},
			"vals":   []interface{}{int64(1), int64(3)},
		},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries: %+v, want %+v\n", sbc1.Queries, wantQueries)
	}
	wantQueries = []querytypes.BoundQuery{{
		Sql: "select id from user where id in ::__vals",
		BindVariables: map[string]interface{}{
			"__vals": []interface{}{int64(3)},
			"vals":   []interface{}{int64(1), int64(3)},
		},
	}}
	if !reflect.DeepEqual(sbc2.Queries, wantQueries) {
		t.Errorf("sbc2.Queries: %+v, want %+v\n", sbc2.Queries, wantQueries)
	}

	// In is a bind variable list, that will end up on two shards.
	// We use a BindVariable with TUPLE type.
	sbc1.Queries = nil
	sbc2.Queries = nil
	_, err = executorExec(executor, "select id from user where id in ::vals", map[string]interface{}{
		"vals": &querypb.BindVariable{
			Type: querypb.Type_TUPLE,
			Values: []*querypb.Value{
				{
					Type:  querypb.Type_INT64,
					Value: []byte{'1'},
				},
				{
					Type:  querypb.Type_INT64,
					Value: []byte{'3'},
				},
			},
		},
	})
	if err != nil {
		t.Error(err)
	}
	wantQueries = []querytypes.BoundQuery{{
		Sql: "select id from user where id in ::__vals",
		BindVariables: map[string]interface{}{
			"__vals": []interface{}{
				sqltypes.MakeTrusted(querypb.Type_INT64, []byte{'1'}),
			},
			"vals": &querypb.BindVariable{
				Type: querypb.Type_TUPLE,
				Values: []*querypb.Value{
					{
						Type:  querypb.Type_INT64,
						Value: []byte{'1'},
					},
					{
						Type:  querypb.Type_INT64,
						Value: []byte{'3'},
					},
				},
			},
		},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries: \n%+v, want \n%+v\n", sbc1.Queries, wantQueries)
	}
	wantQueries = []querytypes.BoundQuery{{
		Sql: "select id from user where id in ::__vals",
		BindVariables: map[string]interface{}{
			"__vals": []interface{}{
				sqltypes.MakeTrusted(querypb.Type_INT64, []byte{'3'}),
			},
			"vals": &querypb.BindVariable{
				Type: querypb.Type_TUPLE,
				Values: []*querypb.Value{
					{
						Type:  querypb.Type_INT64,
						Value: []byte{'1'},
					},
					{
						Type:  querypb.Type_INT64,
						Value: []byte{'3'},
					},
				},
			},
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
	wantQueries = []querytypes.BoundQuery{{
		Sql:           "select id from user where name = 'foo'",
		BindVariables: map[string]interface{}{},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries: %+v, want %+v\n", sbc1.Queries, wantQueries)
	}
	wantQueries = []querytypes.BoundQuery{{
		Sql: "select user_id from name_user_map where name = :name",
		BindVariables: map[string]interface{}{
			"name": []byte("foo"),
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
	wantResult := sandboxconn.SingleRowResult
	if !reflect.DeepEqual(result, wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}

	sql = "select id from user where id in (1, 3)"
	result, err = executorStream(executor, sql)
	if err != nil {
		t.Error(err)
	}
	wantResult = &sqltypes.Result{
		Fields: sandboxconn.SingleRowResult.Fields,
		Rows: [][]sqltypes.Value{
			sandboxconn.SingleRowResult.Rows[0],
			sandboxconn.SingleRowResult.Rows[0],
		},
		RowsAffected: 2,
	}
	if !reflect.DeepEqual(result, wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}

	sql = "select id from user where name = 'foo'"
	result, err = executorStream(executor, sql)
	if err != nil {
		t.Error(err)
	}
	wantResult = sandboxconn.SingleRowResult
	if !reflect.DeepEqual(result, wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}

	wantQueries := []querytypes.BoundQuery{{
		Sql: "select user_id from name_user_map where name = :name",
		BindVariables: map[string]interface{}{
			"name": []byte("foo"),
		},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: %+v, want %+v\n", sbclookup.Queries, wantQueries)
	}
}

func TestSelectINFail(t *testing.T) {
	executor, _, _, _ := createExecutorEnv()

	_, err := executorExec(executor, "select id from user where id in (:aa)", nil)
	want := "paramsSelectIN: could not find bind var :aa"
	if err == nil || err.Error() != want {
		t.Errorf("executorExec: %v, want %v", err, want)
	}

	_, err = executorExec(executor, "select id from user where id in ::aa", nil)
	want = "paramsSelectIN: could not find bind var ::aa"
	if err == nil || err.Error() != want {
		t.Errorf("executorExec: %v, want %v", err, want)
	}

	_, err = executorExec(executor, "select id from user where id in ::aa", map[string]interface{}{
		"aa": 1,
	})
	want = "paramsSelectIN: expecting list for bind var ::aa: 1"
	if err == nil || err.Error() != want {
		t.Errorf("executorExec: %v, want %v", err, want)
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
	executor := NewExecutor(context.Background(), serv, cell, "", resolver, false)

	_, err := executorExec(executor, "select id from user", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries := []querytypes.BoundQuery{{
		Sql:           "select id from user",
		BindVariables: map[string]interface{}{},
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
	executor := NewExecutor(context.Background(), serv, cell, "", resolver, false)

	sql := "select id from user"
	result, err := executorStream(executor, sql)
	if err != nil {
		t.Error(err)
	}
	wantResult := &sqltypes.Result{
		Fields: sandboxconn.SingleRowResult.Fields,
		Rows: [][]sqltypes.Value{
			sandboxconn.SingleRowResult.Rows[0],
			sandboxconn.SingleRowResult.Rows[0],
			sandboxconn.SingleRowResult.Rows[0],
			sandboxconn.SingleRowResult.Rows[0],
			sandboxconn.SingleRowResult.Rows[0],
			sandboxconn.SingleRowResult.Rows[0],
			sandboxconn.SingleRowResult.Rows[0],
			sandboxconn.SingleRowResult.Rows[0],
		},
		RowsAffected: 8,
	}
	if !reflect.DeepEqual(result, wantResult) {
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
	executor := NewExecutor(context.Background(), serv, cell, "", resolver, false)

	_, err := executorExec(executor, "select id from user", nil)
	want := "paramsAllShards: keyspace TestExecutor fetch error: topo error GetSrvKeyspace"
	if err == nil || err.Error() != want {
		t.Errorf("executorExec: %v, want %v", err, want)
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
	wantQueries := []querytypes.BoundQuery{{
		Sql:           "select u1.id from user as u1 where u1.id = 1",
		BindVariables: map[string]interface{}{},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries: %+v, want %+v\n", sbc1.Queries, wantQueries)
	}
	wantQueries = []querytypes.BoundQuery{{
		Sql:           "select u2.id from user as u2 where u2.id = 3",
		BindVariables: map[string]interface{}{},
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
	if !reflect.DeepEqual(result, wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}
}
func TestJoinComments(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()
	_, err := executorExec(executor, "select u1.id, u2.id from user u1 join user u2 where u1.id = 1 and u2.id = 3 /* trailing */", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries := []querytypes.BoundQuery{{
		Sql:           "select u1.id from user as u1 where u1.id = 1 /* trailing */",
		BindVariables: map[string]interface{}{},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries: %+v, want %+v\n", sbc1.Queries, wantQueries)
	}
	wantQueries = []querytypes.BoundQuery{{
		Sql:           "select u2.id from user as u2 where u2.id = 3 /* trailing */",
		BindVariables: map[string]interface{}{},
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
	wantQueries := []querytypes.BoundQuery{{
		Sql:           "select u1.id from user as u1 where u1.id = 1",
		BindVariables: map[string]interface{}{},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries: %+v, want %+v\n", sbc1.Queries, wantQueries)
	}
	wantQueries = []querytypes.BoundQuery{{
		Sql:           "select u2.id from user as u2 where u2.id = 3",
		BindVariables: map[string]interface{}{},
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
	if !reflect.DeepEqual(result, wantResult) {
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
			sqltypes.MakeTrusted(sqltypes.Int32, []byte("1")),
			sqltypes.MakeTrusted(sqltypes.Int32, []byte("3")),
		}},
	}}
	sbc1.SetResults(result1)
	_, err := executorExec(executor, "select u1.id, u2.id from user u1 join user u2 on u2.id = u1.col where u1.id = 1", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries := []querytypes.BoundQuery{{
		Sql:           "select u1.id, u1.col from user as u1 where u1.id = 1",
		BindVariables: map[string]interface{}{},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries: %+v, want %+v\n", sbc1.Queries, wantQueries)
	}
	wantQueries = []querytypes.BoundQuery{{
		Sql:           "select u2.id from user as u2 where u2.id = :u1_col",
		BindVariables: map[string]interface{}{},
	}}
	// We have to use string representation because bindvars type is too complex.
	got := fmt.Sprintf("%+v", sbc2.Queries)
	want := "[{Sql:select u2.id from user as u2 where u2.id = :u1_col BindVariables:map[u1_col:3]}]"
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
			sqltypes.MakeTrusted(sqltypes.Int32, []byte("1")),
			sqltypes.MakeTrusted(sqltypes.Int32, []byte("3")),
		}},
	}}
	sbc1.SetResults(result1)
	_, err := executorStream(executor, "select u1.id, u2.id from user u1 join user u2 on u2.id = u1.col where u1.id = 1")
	if err != nil {
		t.Error(err)
	}
	wantQueries := []querytypes.BoundQuery{{
		Sql:           "select u1.id, u1.col from user as u1 where u1.id = 1",
		BindVariables: map[string]interface{}{},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries: %+v, want %+v\n", sbc1.Queries, wantQueries)
	}
	wantQueries = []querytypes.BoundQuery{{
		Sql:           "select u2.id from user as u2 where u2.id = :u1_col",
		BindVariables: map[string]interface{}{},
	}}
	// We have to use string representation because bindvars type is too complex.
	got := fmt.Sprintf("%+v", sbc2.Queries)
	want := "[{Sql:select u2.id from user as u2 where u2.id = :u1_col BindVariables:map[u1_col:3]}]"
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
			sqltypes.MakeTrusted(sqltypes.Int32, []byte("1")),
			sqltypes.MakeTrusted(sqltypes.Int32, []byte("3")),
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
	if !reflect.DeepEqual(result, wantResult) {
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
			sqltypes.MakeTrusted(sqltypes.Int32, []byte("1")),
			sqltypes.MakeTrusted(sqltypes.Int32, []byte("3")),
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
	if !reflect.DeepEqual(result, wantResult) {
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
	wantQueries := []querytypes.BoundQuery{{
		Sql:           "select u1.id, u1.col from user as u1 where u1.id = 1",
		BindVariables: map[string]interface{}{},
	}, {
		Sql: "select u2.id from user as u2 where 1 != 1",
		BindVariables: map[string]interface{}{
			"u1_col": nil,
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
	if !reflect.DeepEqual(result, wantResult) {
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
	wantQueries := []querytypes.BoundQuery{{
		Sql:           "select u1.id, u1.col from user as u1 where u1.id = 1",
		BindVariables: map[string]interface{}{},
	}, {
		Sql: "select u2.id from user as u2 where 1 != 1",
		BindVariables: map[string]interface{}{
			"u1_col": nil,
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
	if !reflect.DeepEqual(result, wantResult) {
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
	wantQueries := []querytypes.BoundQuery{{
		Sql:           "select u1.id from user as u1 where u1.id = 1",
		BindVariables: map[string]interface{}{},
	}, {
		Sql:           "select u2.id, u2.col from user as u2 where 1 != 1",
		BindVariables: map[string]interface{}{},
	}, {
		Sql: "select u3.id from user as u3 where 1 != 1",
		BindVariables: map[string]interface{}{
			"u2_col": nil,
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
	if !reflect.DeepEqual(result, wantResult) {
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
	wantQueries := []querytypes.BoundQuery{{
		Sql:           "select u1.id from user as u1 where u1.id = 1",
		BindVariables: map[string]interface{}{},
	}, {
		Sql:           "select u2.id, u2.col from user as u2 where 1 != 1",
		BindVariables: map[string]interface{}{},
	}, {
		Sql: "select u3.id from user as u3 where 1 != 1",
		BindVariables: map[string]interface{}{
			"u2_col": nil,
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
	if !reflect.DeepEqual(result, wantResult) {
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
			sqltypes.MakeTrusted(sqltypes.Int32, []byte("1")),
			sqltypes.MakeTrusted(sqltypes.Int32, []byte("3")),
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
			sqltypes.MakeTrusted(sqltypes.Int32, []byte("1")),
			sqltypes.MakeTrusted(sqltypes.Int32, []byte("3")),
		}},
	}})
	sbc2.MustFailCodes[vtrpcpb.Code_INVALID_ARGUMENT] = 1
	_, err = executorStream(executor, "select u1.id, u2.id from user u1 join user u2 on u2.id = u1.col where u1.id = 1")
	want = "INVALID_ARGUMENT error"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("err: %v, must contain %s", err, want)
	}
}
