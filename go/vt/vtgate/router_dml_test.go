// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"reflect"
	"strings"
	"testing"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqltypes"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	_ "github.com/youtube/vitess/go/vt/vtgate/vindexes"
)

func TestUpdateEqual(t *testing.T) {
	router, sbc1, sbc2, sbclookup := createRouterEnv()

	_, err := routerExec(router, "update user set a=2 where id = 1", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries := []tproto.BoundQuery{{
		Sql: "update user set a = 2 where id = 1 /* _routing keyspace_id:166b40b44aba4bd6 */",
		BindVariables: map[string]interface{}{
			"keyspace_id": "\x16k@\xb4J\xbaK\xd6",
		},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries: %+v, want %+v\n", sbc1.Queries, wantQueries)
	}
	if sbc2.Queries != nil {
		t.Errorf("sbc2.Queries: %+v, want nil\n", sbc2.Queries)
	}

	sbc1.Queries = nil
	_, err = routerExec(router, "update user set a=2 where id = 3", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries = []tproto.BoundQuery{{
		Sql: "update user set a = 2 where id = 3 /* _routing keyspace_id:4eb190c9a2fa169c */",
		BindVariables: map[string]interface{}{
			"keyspace_id": "N\xb1\x90ɢ\xfa\x16\x9c",
		},
	}}
	if !reflect.DeepEqual(sbc2.Queries, wantQueries) {
		t.Errorf("sbc2.Queries: %+v, want %+v\n", sbc2.Queries, wantQueries)
	}
	if sbc1.Queries != nil {
		t.Errorf("sbc1.Queries: %+v, want nil\n", sbc1.Queries)
	}

	sbc1.Queries = nil
	sbc2.Queries = nil
	sbclookup.setResults([]*mproto.QueryResult{&mproto.QueryResult{}})
	_, err = routerExec(router, "update music set a=2 where id = 2", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries = []tproto.BoundQuery{{
		Sql: "select user_id from music_user_map where music_id = :music_id",
		BindVariables: map[string]interface{}{
			"music_id": int64(2),
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

func TestUpdateEqualFail(t *testing.T) {
	router, _, _, _ := createRouterEnv()
	s := getSandbox("TestRouter")

	_, err := routerExec(router, "update user set a=2 where id = :aa", nil)
	want := "execUpdateEqual: could not find bind var :aa"
	if err == nil || err.Error() != want {
		t.Errorf("routerExec: %v, want %v", err, want)
	}

	s.SrvKeyspaceMustFail = 1
	_, err = routerExec(router, "update user set a=2 where id = :id", map[string]interface{}{
		"id": 1,
	})
	want = "execUpdateEqual: keyspace TestRouter fetch error: topo error GetSrvKeyspace"
	if err == nil || err.Error() != want {
		t.Errorf("routerExec: %v, want %v", err, want)
	}

	_, err = routerExec(router, "update user set a=2 where id = :id", map[string]interface{}{
		"id": "aa",
	})
	want = "execUpdateEqual: HashVindex.Map: unexpected type for aa: string"
	if err == nil || err.Error() != want {
		t.Errorf("routerExec: %v, want %v", err, want)
	}

	s.ShardSpec = "80-"
	_, err = routerExec(router, "update user set a=2 where id = :id", map[string]interface{}{
		"id": 1,
	})
	want = "execUpdateEqual: KeyspaceId 166b40b44aba4bd6 didn't match any shards"
	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Errorf("routerExec: %v, want prefix %v", err, want)
	}
}

func TestDeleteEqual(t *testing.T) {
	router, sbc, _, sbclookup := createRouterEnv()

	sbc.setResults([]*mproto.QueryResult{&mproto.QueryResult{
		Fields: []mproto.Field{
			{"id", 3},
			{"name", 253},
		},
		RowsAffected: 1,
		InsertId:     0,
		Rows: [][]sqltypes.Value{{
			{sqltypes.Numeric("1")},
			{sqltypes.String("myname")},
		}},
	}})
	_, err := routerExec(router, "delete from user where id = 1", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries := []tproto.BoundQuery{{
		Sql:           "select id, name from user where id = 1 for update",
		BindVariables: map[string]interface{}{},
	}, {
		Sql: "delete from user where id = 1 /* _routing keyspace_id:166b40b44aba4bd6 */",
		BindVariables: map[string]interface{}{
			"keyspace_id": "\x16k@\xb4J\xbaK\xd6",
		},
	}}
	if !reflect.DeepEqual(sbc.Queries, wantQueries) {
		t.Errorf("sbc.Queries: %+v, want %+v\n", sbc.Queries, wantQueries)
	}

	wantQueries = []tproto.BoundQuery{{
		Sql: "delete from user_idx where id in ::id",
		BindVariables: map[string]interface{}{
			"id": []interface{}{int64(1)},
		},
	}, {
		Sql: "delete from name_user_map where name in ::name and user_id = :user_id",
		BindVariables: map[string]interface{}{
			"user_id": int64(1),
			"name":    []interface{}{"myname"},
		},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: %+v, want %+v\n", sbclookup.Queries, wantQueries)
	}

	sbc.Queries = nil
	sbclookup.Queries = nil
	sbc.setResults([]*mproto.QueryResult{&mproto.QueryResult{}})
	_, err = routerExec(router, "delete from user where id = 1", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries = []tproto.BoundQuery{{
		Sql:           "select id, name from user where id = 1 for update",
		BindVariables: map[string]interface{}{},
	}, {
		Sql: "delete from user where id = 1 /* _routing keyspace_id:166b40b44aba4bd6 */",
		BindVariables: map[string]interface{}{
			"keyspace_id": "\x16k@\xb4J\xbaK\xd6",
		},
	}}
	if !reflect.DeepEqual(sbc.Queries, wantQueries) {
		t.Errorf("sbc.Queries: %+v, want %+v\n", sbc.Queries, wantQueries)
	}
	if sbclookup.Queries != nil {
		t.Errorf("sbclookup.Queries: %+v, want nil\n", sbclookup.Queries)
	}

	sbc.Queries = nil
	sbclookup.Queries = nil
	sbclookup.setResults([]*mproto.QueryResult{&mproto.QueryResult{}})
	_, err = routerExec(router, "delete from music where id = 1", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries = []tproto.BoundQuery{{
		Sql: "select user_id from music_user_map where music_id = :music_id",
		BindVariables: map[string]interface{}{
			"music_id": int64(1),
		},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: %+v, want %+v\n", sbclookup.Queries, wantQueries)
	}
	if sbc.Queries != nil {
		t.Errorf("sbc.Queries: %+v, want nil\n", sbc.Queries)
	}
}

func TestDeleteEqualFail(t *testing.T) {
	router, _, _, _ := createRouterEnv()
	s := getSandbox("TestRouter")

	_, err := routerExec(router, "delete from user where id = :aa", nil)
	want := "execDeleteEqual: could not find bind var :aa"
	if err == nil || err.Error() != want {
		t.Errorf("routerExec: %v, want %v", err, want)
	}

	s.SrvKeyspaceMustFail = 1
	_, err = routerExec(router, "delete from user where id = :id", map[string]interface{}{
		"id": 1,
	})
	want = "execDeleteEqual: keyspace TestRouter fetch error: topo error GetSrvKeyspace"
	if err == nil || err.Error() != want {
		t.Errorf("routerExec: %v, want %v", err, want)
	}

	_, err = routerExec(router, "delete from user where id = :id", map[string]interface{}{
		"id": "aa",
	})
	want = "execDeleteEqual: HashVindex.Map: unexpected type for aa: string"
	if err == nil || err.Error() != want {
		t.Errorf("routerExec: %v, want %v", err, want)
	}

	s.ShardSpec = "80-"
	_, err = routerExec(router, "delete from user where id = :id", map[string]interface{}{
		"id": 1,
	})
	want = "execDeleteEqual: KeyspaceId 166b40b44aba4bd6 didn't match any shards"
	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Errorf("routerExec: %v, want prefix %v", err, want)
	}
}

func TestDeleteVindexFail(t *testing.T) {
	router, sbc, _, sbclookup := createRouterEnv()

	sbc.mustFailServer = 1
	_, err := routerExec(router, "delete from user where id = 1", nil)
	want := "execDeleteEqual: shard, host: TestRouter.-20.master, {Uid:0 Host:-20 NamedPortMap:map[vt:1] Health:map[]}, error: err"
	if err == nil || err.Error() != want {
		t.Errorf("routerExec: %v, want %v", err, want)
	}

	sbc.setResults([]*mproto.QueryResult{&mproto.QueryResult{
		Fields: []mproto.Field{
			{"id", 3},
			{"name", 253},
		},
		RowsAffected: 1,
		InsertId:     0,
		Rows: [][]sqltypes.Value{{
			{sqltypes.String("foo")},
			{sqltypes.String("myname")},
		}},
	}})
	_, err = routerExec(router, "delete from user where id = 1", nil)
	want = `execDeleteEqual: strconv.ParseUint: parsing "foo": invalid syntax`
	if err == nil || err.Error() != want {
		t.Errorf("routerExec: %v, want %v", err, want)
	}

	sbclookup.mustFailServer = 1
	_, err = routerExec(router, "delete from user where id = 1", nil)
	want = "execDeleteEqual: HashVindex.Delete: shard, host: TestUnsharded.0.master, {Uid:0 Host:0 NamedPortMap:map[vt:1] Health:map[]}, error: err"
	if err == nil || err.Error() != want {
		t.Errorf("routerExec: %v, want %v", err, want)
	}

	sbclookup.mustFailServer = 1
	_, err = routerExec(router, "delete from music where user_id = 1", nil)
	want = "execDeleteEqual: lookupHash.Delete: shard, host: TestUnsharded.0.master, {Uid:0 Host:0 NamedPortMap:map[vt:1] Health:map[]}"
	if err == nil || err.Error() != want {
		t.Errorf("routerExec: %v, want %v", err, want)
	}
}

func TestInsertSharded(t *testing.T) {
	router, sbc1, sbc2, sbclookup := createRouterEnv()

	_, err := routerExec(router, "insert into user(id, v, name) values (1, 2, 'myname')", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries := []tproto.BoundQuery{{
		Sql: "insert into user(id, v, name) values (:_id, 2, :_name) /* _routing keyspace_id:166b40b44aba4bd6 */",
		BindVariables: map[string]interface{}{
			"keyspace_id": "\x16k@\xb4J\xbaK\xd6",
			"_id":         int64(1),
			"_name":       "myname",
		},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries: %+v, want %+v\n", sbc1.Queries, wantQueries)
	}
	if sbc2.Queries != nil {
		t.Errorf("sbc2.Queries: %+v, want nil\n", sbc2.Queries)
	}
	wantQueries = []tproto.BoundQuery{{
		Sql: "insert into user_idx(id) values(:id)",
		BindVariables: map[string]interface{}{
			"id": int64(1),
		},
	}, {
		Sql: "insert into name_user_map(name, user_id) values(:name, :user_id)",
		BindVariables: map[string]interface{}{
			"name":    "myname",
			"user_id": int64(1),
		},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: %+v, want %+v\n", sbclookup.Queries, wantQueries)
	}

	sbc1.Queries = nil
	sbclookup.Queries = nil
	_, err = routerExec(router, "insert into user(id, v, name) values (3, 2, 'myname2')", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries = []tproto.BoundQuery{{
		Sql: "insert into user(id, v, name) values (:_id, 2, :_name) /* _routing keyspace_id:4eb190c9a2fa169c */",
		BindVariables: map[string]interface{}{
			"keyspace_id": "N\xb1\x90ɢ\xfa\x16\x9c",
			"_id":         int64(3),
			"_name":       "myname2",
		},
	}}
	if !reflect.DeepEqual(sbc2.Queries, wantQueries) {
		t.Errorf("sbc2.Queries: %+v, want %+v\n", sbc2.Queries, wantQueries)
	}
	if sbc1.Queries != nil {
		t.Errorf("sbc1.Queries: %+v, want nil\n", sbc1.Queries)
	}
	wantQueries = []tproto.BoundQuery{{
		Sql: "insert into user_idx(id) values(:id)",
		BindVariables: map[string]interface{}{
			"id": int64(3),
		},
	}, {
		Sql: "insert into name_user_map(name, user_id) values(:name, :user_id)",
		BindVariables: map[string]interface{}{
			"name":    "myname2",
			"user_id": int64(3),
		},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: %+v, want %+v\n", sbclookup.Queries, wantQueries)
	}
}

func TestInsertGenerator(t *testing.T) {
	router, _, _, sbclookup := createRouterEnv()
	s := getSandbox("TestRouter")
	sbc := &sandboxConn{}
	s.MapTestConn("80-a0", sbc)

	_, err := routerExec(router, "insert into user(v, name) values (2, 'myname')", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries := []tproto.BoundQuery{{
		Sql: "insert into user(v, name, id) values (2, :_name, :_id) /* _routing keyspace_id:8ca64de9c1b123a7 */",
		BindVariables: map[string]interface{}{
			"keyspace_id": "\x8c\xa6M\xe9\xc1\xb1#\xa7",
			"_id":         int64(0),
			"_name":       "myname",
		},
	}}
	if !reflect.DeepEqual(sbc.Queries, wantQueries) {
		t.Errorf("sbc.Queries: %+v, want %+v\n", sbc.Queries, wantQueries)
	}
	wantQueries = []tproto.BoundQuery{{
		Sql: "insert into user_idx(id) values(:id)",
		BindVariables: map[string]interface{}{
			"id": nil,
		},
	}, {
		Sql: "insert into name_user_map(name, user_id) values(:name, :user_id)",
		BindVariables: map[string]interface{}{
			"name":    "myname",
			"user_id": int64(0),
		},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: %+v, want %+v\n", sbclookup.Queries, wantQueries)
	}
}

func TestInsertLookupOwned(t *testing.T) {
	router, sbc, _, sbclookup := createRouterEnv()

	_, err := routerExec(router, "insert into music(user_id, id) values (2, 3)", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries := []tproto.BoundQuery{{
		Sql: "insert into music(user_id, id) values (:_user_id, :_id) /* _routing keyspace_id:06e7ea22ce92708f */",
		BindVariables: map[string]interface{}{
			"keyspace_id": "\x06\xe7\xea\"Βp\x8f",
			"_user_id":    int64(2),
			"_id":         int64(3),
		},
	}}
	if !reflect.DeepEqual(sbc.Queries, wantQueries) {
		t.Errorf("sbc.Queries: %+v, want %+v\n", sbc.Queries, wantQueries)
	}
	wantQueries = []tproto.BoundQuery{{
		Sql: "insert into music_user_map(music_id, user_id) values(:music_id, :user_id)",
		BindVariables: map[string]interface{}{
			"music_id": int64(3),
			"user_id":  int64(2),
		},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: %+v, want %+v\n", sbclookup.Queries, wantQueries)
	}
}

func TestInsertLookupOwnedGenerator(t *testing.T) {
	router, sbc, _, sbclookup := createRouterEnv()

	_, err := routerExec(router, "insert into music(user_id) values (2)", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries := []tproto.BoundQuery{{
		Sql: "insert into music(user_id, id) values (:_user_id, :_id) /* _routing keyspace_id:06e7ea22ce92708f */",
		BindVariables: map[string]interface{}{
			"keyspace_id": "\x06\xe7\xea\"Βp\x8f",
			"_user_id":    int64(2),
			"_id":         int64(0),
		},
	}}
	if !reflect.DeepEqual(sbc.Queries, wantQueries) {
		t.Errorf("sbc.Queries: %+v, want %+v\n", sbc.Queries, wantQueries)
	}
	wantQueries = []tproto.BoundQuery{{
		Sql: "insert into music_user_map(music_id, user_id) values(:music_id, :user_id)",
		BindVariables: map[string]interface{}{
			"music_id": nil,
			"user_id":  int64(2),
		},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: %+v, want %+v\n", sbclookup.Queries, wantQueries)
	}
}

func TestInsertLookupUnowned(t *testing.T) {
	router, sbc, _, sbclookup := createRouterEnv()

	_, err := routerExec(router, "insert into music_extra(user_id, music_id) values (2, 3)", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries := []tproto.BoundQuery{{
		Sql: "insert into music_extra(user_id, music_id) values (:_user_id, :_music_id) /* _routing keyspace_id:06e7ea22ce92708f */",
		BindVariables: map[string]interface{}{
			"keyspace_id": "\x06\xe7\xea\"Βp\x8f",
			"_user_id":    int64(2),
			"_music_id":   int64(3),
		},
	}}
	if !reflect.DeepEqual(sbc.Queries, wantQueries) {
		t.Errorf("sbc.Queries: %+v, want %+v\n", sbc.Queries, wantQueries)
	}
	wantQueries = []tproto.BoundQuery{{
		Sql: "select music_id from music_user_map where music_id = :music_id and user_id = :user_id",
		BindVariables: map[string]interface{}{
			"music_id": int64(3),
			"user_id":  int64(2),
		},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: %+v, want %+v\n", sbclookup.Queries, wantQueries)
	}
}

func TestInsertLookupUnownedUnsupplied(t *testing.T) {
	router, sbc, _, sbclookup := createRouterEnv()

	_, err := routerExec(router, "insert into music_extra_reversed(music_id) values (3)", nil)
	if err != nil {
		t.Error(err)
	}
	wantQueries := []tproto.BoundQuery{{
		Sql: "insert into music_extra_reversed(music_id, user_id) values (:_music_id, :_user_id) /* _routing keyspace_id:166b40b44aba4bd6 */",
		BindVariables: map[string]interface{}{
			"keyspace_id": "\x16k@\xb4J\xbaK\xd6",
			"_user_id":    int64(1),
			"_music_id":   int64(3),
		},
	}}
	if !reflect.DeepEqual(sbc.Queries, wantQueries) {
		t.Errorf("sbc.Queries: %+v, want %+v\n", sbc.Queries, wantQueries)
	}
	wantQueries = []tproto.BoundQuery{{
		Sql: "select user_id from music_user_map where music_id = :music_id",
		BindVariables: map[string]interface{}{
			"music_id": int64(3),
		},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: %+v, want %+v\n", sbclookup.Queries, wantQueries)
	}
}
