// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"reflect"
	"strings"
	"testing"
	"time"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqltypes"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
	_ "github.com/youtube/vitess/go/vt/vtgate/vindexes"
)

func TestUnsharded(t *testing.T) {
	router, _, _, sbclookup := createRouterEnv()

	_, err := routerExec(router, "select * from music_user_map where id = 1", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries := []tproto.BoundQuery{{
		Sql:           "select * from music_user_map where id = 1",
		BindVariables: map[string]interface{}{},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: %+v, want %+v\n", sbclookup.Queries, wantQueries)
	}

	_, err = routerExec(router, "update music_user_map set id = 1", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries = []tproto.BoundQuery{{
		Sql:           "select * from music_user_map where id = 1",
		BindVariables: map[string]interface{}{},
	}, {
		Sql:           "update music_user_map set id = 1",
		BindVariables: map[string]interface{}{},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: %+v, want %+v\n", sbclookup.Queries, wantQueries)
	}

	sbclookup.Queries = nil
	_, err = routerExec(router, "delete from music_user_map", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries = []tproto.BoundQuery{{
		Sql:           "delete from music_user_map",
		BindVariables: map[string]interface{}{},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: %+v, want %+v\n", sbclookup.Queries, wantQueries)
	}

	sbclookup.Queries = nil
	_, err = routerExec(router, "insert into music_user_map values(1)", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries = []tproto.BoundQuery{{
		Sql:           "insert into music_user_map values(1)",
		BindVariables: map[string]interface{}{},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: %+v, want %+v\n", sbclookup.Queries, wantQueries)
	}
}

func TestStreamUnsharded(t *testing.T) {
	router, _, _, _ := createRouterEnv()

	q := proto.Query{
		Sql:        "select * from music_user_map where id = 1",
		TabletType: topo.TYPE_MASTER,
	}
	result, err := routerStream(router, &q)
	if err != nil {
		t.Error(err)
	}
	wantResult := singleRowResult
	if !reflect.DeepEqual(result, wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}
}

func TestUnshardedFail(t *testing.T) {
	router, _, _, _ := createRouterEnv()

	getSandbox(TEST_UNSHARDED).SrvKeyspaceMustFail = 1
	_, err := routerExec(router, "select * from music_user_map where id = 1", nil)
	want := "paramsUnsharded: keyspace TestUnsharded fetch error: topo error GetSrvKeyspace"
	if err == nil || err.Error() != want {
		t.Errorf("routerExec: %v, want %v", err, want)
	}

	_, err = routerExec(router, "select * from sharded_table where id = 1", nil)
	want = "unsharded keyspace TestBadSharding has multiple shards"
	if err == nil || err.Error() != want {
		t.Errorf("routerExec: %v, want %v", err, want)
	}
}

func TestStreamUnshardedFail(t *testing.T) {
	router, _, _, _ := createRouterEnv()

	getSandbox(TEST_UNSHARDED).SrvKeyspaceMustFail = 1
	q := proto.Query{
		Sql:        "select * from music_user_map where id = 1",
		TabletType: topo.TYPE_MASTER,
	}
	_, err := routerStream(router, &q)
	want := "paramsUnsharded: keyspace TestUnsharded fetch error: topo error GetSrvKeyspace"
	if err == nil || err.Error() != want {
		t.Errorf("routerExec: %v, want %v", err, want)
	}

	q = proto.Query{
		Sql:        "update music_user_map set a = 1 where id = 1",
		TabletType: topo.TYPE_MASTER,
	}
	_, err = routerStream(router, &q)
	want = `query "update music_user_map set a = 1 where id = 1" cannot be used for streaming`
	if err == nil || err.Error() != want {
		t.Errorf("routerExec: %v, want %v", err, want)
	}
}

func TestSelectEqual(t *testing.T) {
	router, sbc1, sbc2, sbclookup := createRouterEnv()

	_, err := routerExec(router, "select * from user where id = 1", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries := []tproto.BoundQuery{{
		Sql:           "select * from user where id = 1",
		BindVariables: map[string]interface{}{},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries: %+v, want %+v\n", sbc1.Queries, wantQueries)
	}
	if sbc2.Queries != nil {
		t.Errorf("sbc2.Queries: %+v, want nil\n", sbc2.Queries)
	}

	sbc1.Queries = nil
	_, err = routerExec(router, "select * from user where id = 3", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries = []tproto.BoundQuery{{
		Sql:           "select * from user where id = 3",
		BindVariables: map[string]interface{}{},
	}}
	if !reflect.DeepEqual(sbc2.Queries, wantQueries) {
		t.Errorf("sbc2.Queries: %+v, want %+v\n", sbc2.Queries, wantQueries)
	}
	if sbc1.ExecCount != 1 {
		t.Errorf("sbc1.ExecCount: %v, want 1\n", sbc1.ExecCount)
	}
	if sbc1.Queries != nil {
		t.Errorf("sbc1.Queries: %+v, want nil\n", sbc1.Queries)
	}

	sbc2.Queries = nil
	_, err = routerExec(router, "select * from user where name = 'foo'", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries = []tproto.BoundQuery{{
		Sql:           "select * from user where name = 'foo'",
		BindVariables: map[string]interface{}{},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries: %+v, want %+v\n", sbc1.Queries, wantQueries)
	}
	wantQueries = []tproto.BoundQuery{{
		Sql: "select user_id from name_user_map where name = :name",
		BindVariables: map[string]interface{}{
			"name": "foo",
		},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: %+v, want %+v\n", sbclookup.Queries, wantQueries)
	}
}

func TestSelectEqualNotFound(t *testing.T) {
	router, _, _, sbclookup := createRouterEnv()

	sbclookup.setResults([]*mproto.QueryResult{&mproto.QueryResult{}})
	result, err := routerExec(router, "select * from music where id = 1", nil)
	if err != nil {
		t.Error(err)
	}
	wantResult := &mproto.QueryResult{}
	if !reflect.DeepEqual(result, wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}

	sbclookup.setResults([]*mproto.QueryResult{&mproto.QueryResult{}})
	result, err = routerExec(router, "select * from user where name = 'foo'", nil)
	if err != nil {
		t.Error(err)
	}
	wantResult = &mproto.QueryResult{}
	if !reflect.DeepEqual(result, wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}
}

func TestStreamSelectEqual(t *testing.T) {
	router, _, _, _ := createRouterEnv()

	q := proto.Query{
		Sql:        "select * from user where id = 1",
		TabletType: topo.TYPE_MASTER,
	}
	result, err := routerStream(router, &q)
	if err != nil {
		t.Error(err)
	}
	wantResult := singleRowResult
	if !reflect.DeepEqual(result, wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}
}

func TestSelectEqualFail(t *testing.T) {
	router, _, _, sbclookup := createRouterEnv()
	s := getSandbox("TestRouter")

	_, err := routerExec(router, "select * from user where id = (select count(*) from music)", nil)
	want := "cannot route query: select * from user where id = (select count(*) from music): has subquery"
	if err == nil || err.Error() != want {
		t.Errorf("routerExec: %v, want %v", err, want)
	}

	_, err = routerExec(router, "select * from user where id = :aa", nil)
	want = "paramsSelectEqual: could not find bind var :aa"
	if err == nil || err.Error() != want {
		t.Errorf("routerExec: %v, want %v", err, want)
	}

	s.SrvKeyspaceMustFail = 1
	_, err = routerExec(router, "select * from user where id = 1", nil)
	want = "paramsSelectEqual: keyspace TestRouter fetch error: topo error GetSrvKeyspace"
	if err == nil || err.Error() != want {
		t.Errorf("routerExec: %v, want %v", err, want)
	}

	sbclookup.mustFailServer = 1
	_, err = routerExec(router, "select * from music where id = 1", nil)
	want = "paramsSelectEqual: lookup.Map"
	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Errorf("routerExec: %v, want prefix %v", err, want)
	}

	s.ShardSpec = "80-"
	_, err = routerExec(router, "select * from user where id = 1", nil)
	want = "paramsSelectEqual: KeyspaceId 166b40b44aba4bd6 didn't match any shards"
	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Errorf("routerExec: %v, want prefix %v", err, want)
	}
	s.ShardSpec = DefaultShardSpec

	sbclookup.mustFailServer = 1
	_, err = routerExec(router, "select * from user where name = 'foo'", nil)
	want = "paramsSelectEqual: lookup.Map"
	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Errorf("routerExec: %v, want prefix %v", err, want)
	}

	s.ShardSpec = "80-"
	_, err = routerExec(router, "select * from user where name = 'foo'", nil)
	want = "paramsSelectEqual: KeyspaceId 166b40b44aba4bd6 didn't match any shards"
	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Errorf("routerExec: %v, want prefix %v", err, want)
	}
	s.ShardSpec = DefaultShardSpec
}

func TestSelectIN(t *testing.T) {
	router, sbc1, sbc2, sbclookup := createRouterEnv()

	_, err := routerExec(router, "select * from user where id in (1)", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries := []tproto.BoundQuery{{
		Sql: "select * from user where id in ::_vals",
		BindVariables: map[string]interface{}{
			"_vals": []interface{}{int64(1)},
		},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries: %+v, want %+v\n", sbc1.Queries, wantQueries)
	}
	if sbc2.Queries != nil {
		t.Errorf("sbc2.Queries: %+v, want nil\n", sbc2.Queries)
	}

	sbc1.Queries = nil
	_, err = routerExec(router, "select * from user where id in (1, 3)", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries = []tproto.BoundQuery{{
		Sql: "select * from user where id in ::_vals",
		BindVariables: map[string]interface{}{
			"_vals": []interface{}{int64(1)},
		},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries: %+v, want %+v\n", sbc1.Queries, wantQueries)
	}
	wantQueries = []tproto.BoundQuery{{
		Sql: "select * from user where id in ::_vals",
		BindVariables: map[string]interface{}{
			"_vals": []interface{}{int64(3)},
		},
	}}
	if !reflect.DeepEqual(sbc2.Queries, wantQueries) {
		t.Errorf("sbc2.Queries: %+v, want %+v\n", sbc2.Queries, wantQueries)
	}

	sbc1.Queries = nil
	sbc2.Queries = nil
	_, err = routerExec(router, "select * from user where name = 'foo'", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries = []tproto.BoundQuery{{
		Sql:           "select * from user where name = 'foo'",
		BindVariables: map[string]interface{}{},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries: %+v, want %+v\n", sbc1.Queries, wantQueries)
	}
	wantQueries = []tproto.BoundQuery{{
		Sql: "select user_id from name_user_map where name = :name",
		BindVariables: map[string]interface{}{
			"name": "foo",
		},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: %+v, want %+v\n", sbclookup.Queries, wantQueries)
	}
}

func TestStreamSelectIN(t *testing.T) {
	router, _, _, sbclookup := createRouterEnv()

	q := proto.Query{
		Sql:        "select * from user where id in (1)",
		TabletType: topo.TYPE_MASTER,
	}
	result, err := routerStream(router, &q)
	if err != nil {
		t.Error(err)
	}
	wantResult := singleRowResult
	if !reflect.DeepEqual(result, wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}

	q.Sql = "select * from user where id in (1, 3)"
	result, err = routerStream(router, &q)
	if err != nil {
		t.Error(err)
	}
	wantResult = &mproto.QueryResult{
		Fields: singleRowResult.Fields,
		Rows: [][]sqltypes.Value{
			singleRowResult.Rows[0],
			singleRowResult.Rows[0],
		},
		RowsAffected: 2,
	}
	if !reflect.DeepEqual(result, wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}

	q.Sql = "select * from user where name = 'foo'"
	result, err = routerStream(router, &q)
	if err != nil {
		t.Error(err)
	}
	wantResult = singleRowResult
	if !reflect.DeepEqual(result, wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}

	wantQueries := []tproto.BoundQuery{{
		Sql: "select user_id from name_user_map where name = :name",
		BindVariables: map[string]interface{}{
			"name": "foo",
		},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: %+v, want %+v\n", sbclookup.Queries, wantQueries)
	}
}

func TestSelectINFail(t *testing.T) {
	router, _, _, _ := createRouterEnv()

	_, err := routerExec(router, "select * from user where id in (:aa)", nil)
	want := "paramsSelectIN: could not find bind var :aa"
	if err == nil || err.Error() != want {
		t.Errorf("routerExec: %v, want %v", err, want)
	}

	getSandbox("TestRouter").SrvKeyspaceMustFail = 1
	_, err = routerExec(router, "select * from user where id in (1)", nil)
	want = "paramsSelectEqual: keyspace TestRouter fetch error: topo error GetSrvKeyspace"
	if err == nil || err.Error() != want {
		t.Errorf("routerExec: %v, want %v", err, want)
	}
}

func TestSelectKeyrange(t *testing.T) {
	router, sbc1, sbc2, _ := createRouterEnv()

	_, err := routerExec(router, "select * from user where keyrange('', '\x20')", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries := []tproto.BoundQuery{{
		Sql:           "select * from user",
		BindVariables: map[string]interface{}{},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries: %+v, want %+v\n", sbc1.Queries, wantQueries)
	}
	if sbc2.Queries != nil {
		t.Errorf("sbc2.Queries: %+v, want nil\n", sbc2.Queries)
	}

	sbc1.Queries = nil
	_, err = routerExec(router, "select * from user where keyrange('\x40', '\x60')", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries = []tproto.BoundQuery{{
		Sql:           "select * from user",
		BindVariables: map[string]interface{}{},
	}}
	if !reflect.DeepEqual(sbc2.Queries, wantQueries) {
		t.Errorf("sbc2.Queries: %+v, want %+v\n", sbc2.Queries, wantQueries)
	}
	if sbc1.Queries != nil {
		t.Errorf("sbc1.Queries: %+v, want nil\n", sbc1.Queries)
	}
}

func TestStreamSelectKeyrange(t *testing.T) {
	router, _, _, _ := createRouterEnv()

	q := proto.Query{
		Sql:        "select * from user where keyrange('', '\x20')",
		TabletType: topo.TYPE_MASTER,
	}
	result, err := routerStream(router, &q)
	if err != nil {
		t.Error(err)
	}
	wantResult := singleRowResult
	if !reflect.DeepEqual(result, wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}

	q.Sql = "select * from user where keyrange('\x40', '\x60')"
	result, err = routerStream(router, &q)
	if err != nil {
		t.Error(err)
	}
	wantResult = singleRowResult
	if !reflect.DeepEqual(result, wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}
}

func TestSelectKeyrangeFail(t *testing.T) {
	router, _, _, _ := createRouterEnv()

	_, err := routerExec(router, "select * from user where keyrange('', :aa)", nil)
	want := "paramsSelectKeyrange: could not find bind var :aa"
	if err == nil || err.Error() != want {
		t.Errorf("routerExec: %v, want %v", err, want)
	}

	_, err = routerExec(router, "select * from user where keyrange('', :aa)", map[string]interface{}{
		"aa": 1,
	})
	want = "paramsSelectKeyrange: expecting strings for keyrange: [ 1]"
	if err == nil || err.Error() != want {
		t.Errorf("routerExec: %v, want %v", err, want)
	}

	_, err = routerExec(router, "select * from user where keyrange('', :aa)", map[string]interface{}{
		"aa": "\x21",
	})
	want = "paramsSelectKeyrange: keyrange {Start: , End: 21} does not exactly match shards"
	if err == nil || err.Error() != want {
		t.Errorf("routerExec: %v, want %v", err, want)
	}

	_, err = routerExec(router, "select * from user where keyrange('', :aa)", map[string]interface{}{
		"aa": "\x40",
	})
	want = "keyrange must match exactly one shard: [ @]"
	if err == nil || err.Error() != want {
		t.Errorf("routerExec: %v, want %v", err, want)
	}
}

func TestSelectScatter(t *testing.T) {
	// Special setup: Don't use createRouterEnv.
	s := createSandbox("TestRouter")
	shards := []string{"-20", "20-40", "40-60", "60-80", "80-a0", "a0-c0", "c0-e0", "e0-"}
	var conns []*sandboxConn
	for _, shard := range shards {
		sbc := &sandboxConn{}
		conns = append(conns, sbc)
		s.MapTestConn(shard, sbc)
	}
	serv := new(sandboxTopo)
	scatterConn := NewScatterConn(serv, "", "aa", 1*time.Second, 10, 1*time.Millisecond)
	router := NewRouter(serv, "aa", routerSchema, "", scatterConn)

	_, err := routerExec(router, "select * from user", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries := []tproto.BoundQuery{{
		Sql:           "select * from user",
		BindVariables: map[string]interface{}{},
	}}
	for _, conn := range conns {
		if !reflect.DeepEqual(conn.Queries, wantQueries) {
			t.Errorf("conn.Queries = %#v, want %#v", conn.Queries, wantQueries)
		}
	}
}

func TestStreamSelectScatter(t *testing.T) {
	// Special setup: Don't use createRouterEnv.
	s := createSandbox("TestRouter")
	shards := []string{"-20", "20-40", "40-60", "60-80", "80-a0", "a0-c0", "c0-e0", "e0-"}
	var conns []*sandboxConn
	for _, shard := range shards {
		sbc := &sandboxConn{}
		conns = append(conns, sbc)
		s.MapTestConn(shard, sbc)
	}
	serv := new(sandboxTopo)
	scatterConn := NewScatterConn(serv, "", "aa", 1*time.Second, 10, 1*time.Millisecond)
	router := NewRouter(serv, "aa", routerSchema, "", scatterConn)

	q := proto.Query{
		Sql:        "select * from user",
		TabletType: topo.TYPE_MASTER,
	}
	result, err := routerStream(router, &q)
	if err != nil {
		t.Error(err)
	}
	wantResult := &mproto.QueryResult{
		Fields: singleRowResult.Fields,
		Rows: [][]sqltypes.Value{
			singleRowResult.Rows[0],
			singleRowResult.Rows[0],
			singleRowResult.Rows[0],
			singleRowResult.Rows[0],
			singleRowResult.Rows[0],
			singleRowResult.Rows[0],
			singleRowResult.Rows[0],
			singleRowResult.Rows[0],
		},
		RowsAffected: 8,
	}
	if !reflect.DeepEqual(result, wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}
}

func TestSelectScatterFail(t *testing.T) {
	// Special setup: Don't use createRouterEnv.
	s := createSandbox("TestRouter")
	s.SrvKeyspaceMustFail = 1
	shards := []string{"-20", "20-40", "40-60", "60-80", "80-a0", "a0-c0", "c0-e0", "e0-"}
	var conns []*sandboxConn
	for _, shard := range shards {
		sbc := &sandboxConn{}
		conns = append(conns, sbc)
		s.MapTestConn(shard, sbc)
	}
	serv := new(sandboxTopo)
	scatterConn := NewScatterConn(serv, "", "aa", 1*time.Second, 10, 1*time.Millisecond)
	router := NewRouter(serv, "aa", routerSchema, "", scatterConn)

	_, err := routerExec(router, "select * from user", nil)
	want := "paramsSelectScatter: keyspace TestRouter fetch error: topo error GetSrvKeyspace"
	if err == nil || err.Error() != want {
		t.Errorf("routerExec: %v, want %v", err, want)
	}
}
