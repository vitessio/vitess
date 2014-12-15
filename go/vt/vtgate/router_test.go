// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"path"
	"reflect"
	"testing"
	"time"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/testfiles"
	"github.com/youtube/vitess/go/vt/context"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtgate/planbuilder"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
	_ "github.com/youtube/vitess/go/vt/vtgate/vindexes"
)

type VTGateSchemaNormalized struct {
	Keyspaces map[string]struct {
		ShardingScheme int
		Indexes        map[string]struct {
			// Type is ShardKey or Lookup.
			Type      int
			From, To  string
			Owner     string
			IsAutoInc bool
		}
		Tables map[string]struct {
			IndexColumns []struct {
				Column    string
				IndexName string
			}
		}
	}
}

func TestUnsharded(t *testing.T) {
	schema, err := planbuilder.LoadSchemaJSON(locateFile("router_test.json"))
	if err != nil {
		t.Fatal(err)
	}
	createSandbox("TestRouter")
	s := createSandbox(TEST_UNSHARDED)
	sbc := &sandboxConn{}
	s.MapTestConn("0", sbc)
	serv := new(sandboxTopo)
	scatterConn := NewScatterConn(serv, "", "aa", 1*time.Second, 10, 1*time.Millisecond)
	router := NewRouter(serv, "aa", schema, "", scatterConn)
	q := proto.Query{
		Sql:        "select * from music_user_map where id = 1",
		TabletType: topo.TYPE_MASTER,
	}
	_, err = router.Execute(&context.DummyContext{}, &q)
	if err != nil {
		t.Error(err)
	}
	if sbc.ExecCount != 1 {
		t.Errorf("sbc.ExecCount: %v, want 1\n", sbc.ExecCount)
	}
	wantBind := map[string]interface{}{}
	if !reflect.DeepEqual(sbc.BindVars[0], wantBind) {
		t.Errorf("sbc.BindVars[0] = %#v, want %#v", sbc.BindVars[0], wantBind)
	}
	wantQuery := "select * from music_user_map where id = 1"
	if sbc.Queries[0] != wantQuery {
		t.Errorf("sbc.Queries[0]: %q, want %q\n", sbc.Queries[0], wantQuery)
	}

	q.Sql = "update music_user_map set id = 1"
	_, err = router.Execute(&context.DummyContext{}, &q)
	if err != nil {
		t.Error(err)
	}
	wantQueries := []string{
		"select * from music_user_map where id = 1",
		"update music_user_map set id = 1",
	}
	if !reflect.DeepEqual(sbc.Queries, wantQueries) {
		t.Errorf("sbc.Queries[0]: %q, want %q\n", sbc.Queries, wantQueries)
	}

	q.Sql = "delete from music_user_map"
	sbc.Queries = nil
	_, err = router.Execute(&context.DummyContext{}, &q)
	if err != nil {
		t.Error(err)
	}
	wantQuery = "delete from music_user_map"
	if sbc.Queries[0] != wantQuery {
		t.Errorf("sbc.Queries[0]: %q, want %q\n", sbc.Queries[0], wantQuery)
	}

	q.Sql = "insert into music_user_map values(1)"
	sbc.Queries = nil
	_, err = router.Execute(&context.DummyContext{}, &q)
	if err != nil {
		t.Error(err)
	}
	wantQuery = "insert into music_user_map values(1)"
	if sbc.Queries[0] != wantQuery {
		t.Errorf("sbc.Queries[0]: %q, want %q\n", sbc.Queries[0], wantQuery)
	}
}

func TestSelectEqual(t *testing.T) {
	schema, err := planbuilder.LoadSchemaJSON(locateFile("router_test.json"))
	if err != nil {
		t.Fatal(err)
	}
	s := createSandbox("TestRouter")
	sbc1 := &sandboxConn{}
	sbc2 := &sandboxConn{}
	s.MapTestConn("-20", sbc1)
	s.MapTestConn("40-60", sbc2)

	l := createSandbox("TestUnsharded")
	sbclookup := &sandboxConn{}
	l.MapTestConn("0", sbclookup)

	serv := new(sandboxTopo)
	scatterConn := NewScatterConn(serv, "", "aa", 1*time.Second, 10, 1*time.Millisecond)
	router := NewRouter(serv, "aa", schema, "", scatterConn)
	q := proto.Query{
		Sql:        "select * from user where id = 1",
		TabletType: topo.TYPE_MASTER,
	}
	_, err = router.Execute(&context.DummyContext{}, &q)
	if err != nil {
		t.Error(err)
	}
	wantBind := map[string]interface{}{}
	if !reflect.DeepEqual(sbc1.BindVars[0], wantBind) {
		t.Errorf("sbc1.BindVars[0] = %#v, want %#v", sbc1.BindVars[0], wantBind)
	}
	wantQuery := "select * from user where id = 1"
	if sbc1.Queries[0] != wantQuery {
		t.Errorf("sbc1.Queries[0]: %q, want %q\n", sbc1.Queries[0], wantQuery)
	}
	if sbc2.ExecCount != 0 {
		t.Errorf("sbc2.ExecCount: %v, want 0\n", sbc2.ExecCount)
	}

	q.Sql = "select * from user where id = 3"
	_, err = router.Execute(&context.DummyContext{}, &q)
	if err != nil {
		t.Error(err)
	}
	if sbc1.ExecCount != 1 {
		t.Errorf("sbc1.ExecCount: %v, want 1\n", sbc1.ExecCount)
	}
	wantBind = map[string]interface{}{}
	if !reflect.DeepEqual(sbc2.BindVars[0], wantBind) {
		t.Errorf("sbc2.BindVars[0] = %#v, want %#v", sbc2.BindVars[0], wantBind)
	}
	wantQuery = "select * from user where id = 3"
	if sbc2.Queries[0] != wantQuery {
		t.Errorf("sbc2.Queries[0]: %q, want %q\n", sbc2.Queries[0], wantQuery)
	}

	q.Sql = "select * from user where name = 'foo'"
	sbc1.BindVars = nil
	sbc1.Queries = nil
	_, err = router.Execute(&context.DummyContext{}, &q)
	if err != nil {
		t.Error(err)
	}
	wantBind = map[string]interface{}{}
	if !reflect.DeepEqual(sbc1.BindVars[0], wantBind) {
		t.Errorf("sbc1.BindVars[0] = %#v, want %#v", sbc1.BindVars[0], wantBind)
	}
	wantQuery = "select * from user where name = 'foo'"
	if sbc1.Queries[0] != wantQuery {
		t.Errorf("sbc1.Queries[0]: %q, want %q\n", sbc1.Queries[0], wantQuery)
	}

	wantBinds := []map[string]interface{}{{
		"name": []byte("foo"),
	}}
	if !reflect.DeepEqual(sbclookup.BindVars, wantBinds) {
		t.Errorf("sbclookup.BindVars = \n%#v, want \n%#v", sbclookup.BindVars, wantBinds)
	}
	wantQueries := []string{
		"select user_id from name_user_map where name = :name",
	}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: %q, want %q\n", sbclookup.Queries, wantQueries)
	}
}

func TestSelectIN(t *testing.T) {
	schema, err := planbuilder.LoadSchemaJSON(locateFile("router_test.json"))
	if err != nil {
		t.Fatal(err)
	}
	s := createSandbox("TestRouter")
	sbc1 := &sandboxConn{}
	sbc2 := &sandboxConn{}
	s.MapTestConn("-20", sbc1)
	s.MapTestConn("40-60", sbc2)

	l := createSandbox("TestUnsharded")
	sbclookup := &sandboxConn{}
	l.MapTestConn("0", sbclookup)

	serv := new(sandboxTopo)
	scatterConn := NewScatterConn(serv, "", "aa", 1*time.Second, 10, 1*time.Millisecond)
	router := NewRouter(serv, "aa", schema, "", scatterConn)
	q := proto.Query{
		Sql:        "select * from user where id in (1)",
		TabletType: topo.TYPE_MASTER,
	}
	_, err = router.Execute(&context.DummyContext{}, &q)
	if err != nil {
		t.Error(err)
	}
	wantBind := map[string]interface{}{
		"_vals": []interface{}{int64(1)},
	}
	if !reflect.DeepEqual(sbc1.BindVars[0], wantBind) {
		t.Errorf("sbc1.BindVars[0] = %#v, want %#v", sbc1.BindVars[0], wantBind)
	}
	wantQuery := "select * from user where id in ::_vals"
	if sbc1.Queries[0] != wantQuery {
		t.Errorf("sbc1.Queries[0]: %q, want %q\n", sbc1.Queries[0], wantQuery)
	}
	if sbc2.ExecCount != 0 {
		t.Errorf("sbc2.ExecCount: %v, want 0\n", sbc2.ExecCount)
	}

	q.Sql = "select * from user where id in (1, 3)"
	sbc1.Queries = nil
	_, err = router.Execute(&context.DummyContext{}, &q)
	if err != nil {
		t.Error(err)
	}
	wantBind = map[string]interface{}{
		"_vals": []interface{}{int64(1)},
	}
	if !reflect.DeepEqual(sbc1.BindVars[0], wantBind) {
		t.Errorf("sbc1.BindVars[0] = %#v, want %#v", sbc1.BindVars[0], wantBind)
	}
	if sbc1.Queries[0] != wantQuery {
		t.Errorf("sbc1.Queries[0]: %q, want %q\n", sbc1.Queries[0], wantQuery)
	}
	wantBind = map[string]interface{}{
		"_vals": []interface{}{int64(3)},
	}
	if !reflect.DeepEqual(sbc2.BindVars[0], wantBind) {
		t.Errorf("sbc2.BindVars[0] = %#v, want %#v", sbc2.BindVars[0], wantBind)
	}
	if sbc2.Queries[0] != wantQuery {
		t.Errorf("sbc2.Queries[0]: %q, want %q\n", sbc2.Queries[0], wantQuery)
	}

	q.Sql = "select * from user where name = 'foo'"
	sbc1.BindVars = nil
	sbc1.Queries = nil
	_, err = router.Execute(&context.DummyContext{}, &q)
	if err != nil {
		t.Error(err)
	}
	wantBind = map[string]interface{}{}
	if !reflect.DeepEqual(sbc1.BindVars[0], wantBind) {
		t.Errorf("sbc1.BindVars[0] = %#v, want %#v", sbc1.BindVars[0], wantBind)
	}
	wantQuery = "select * from user where name = 'foo'"
	if sbc1.Queries[0] != wantQuery {
		t.Errorf("sbc1.Queries[0]: %q, want %q\n", sbc1.Queries[0], wantQuery)
	}

	wantBinds := []map[string]interface{}{{
		"name": []byte("foo"),
	}}
	if !reflect.DeepEqual(sbclookup.BindVars, wantBinds) {
		t.Errorf("sbclookup.BindVars = \n%#v, want \n%#v", sbclookup.BindVars, wantBinds)
	}
	wantQueries := []string{
		"select user_id from name_user_map where name = :name",
	}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: %q, want %q\n", sbclookup.Queries, wantQueries)
	}
}

func TestSelectKeyrange(t *testing.T) {
	schema, err := planbuilder.LoadSchemaJSON(locateFile("router_test.json"))
	if err != nil {
		t.Fatal(err)
	}
	s := createSandbox("TestRouter")
	sbc1 := &sandboxConn{}
	sbc2 := &sandboxConn{}
	s.MapTestConn("-20", sbc1)
	s.MapTestConn("40-60", sbc2)
	serv := new(sandboxTopo)
	scatterConn := NewScatterConn(serv, "", "aa", 1*time.Second, 10, 1*time.Millisecond)
	router := NewRouter(serv, "aa", schema, "", scatterConn)
	q := proto.Query{
		Sql:        "select * from user where keyrange('', '\x20')",
		TabletType: topo.TYPE_MASTER,
	}
	_, err = router.Execute(&context.DummyContext{}, &q)
	if err != nil {
		t.Error(err)
	}
	wantBind := map[string]interface{}{}
	if !reflect.DeepEqual(sbc1.BindVars[0], wantBind) {
		t.Errorf("sbc1.BindVars[0] = %#v, want %#v", sbc1.BindVars[0], wantBind)
	}
	wantQuery := "select * from user"
	if sbc1.Queries[0] != wantQuery {
		t.Errorf("sbc1.Queries[0]: %q, want %q\n", sbc1.Queries[0], wantQuery)
	}
	if sbc2.ExecCount != 0 {
		t.Errorf("sbc2.ExecCount: %v, want 0\n", sbc2.ExecCount)
	}

	q.Sql = "select * from user where keyrange('\x40', '\x60')"
	_, err = router.Execute(&context.DummyContext{}, &q)
	if err != nil {
		t.Error(err)
	}
	if sbc1.ExecCount != 1 {
		t.Errorf("sbc1.ExecCount: %v, want 1\n", sbc1.ExecCount)
	}
	wantBind = map[string]interface{}{}
	if !reflect.DeepEqual(sbc2.BindVars[0], wantBind) {
		t.Errorf("sbc2.BindVars[0] = %#v, want %#v", sbc2.BindVars[0], wantBind)
	}
	wantQuery = "select * from user"
	if sbc2.Queries[0] != wantQuery {
		t.Errorf("sbc2.Queries[0]: %q, want %q\n", sbc2.Queries[0], wantQuery)
	}
}

func TestUpdateEqual(t *testing.T) {
	schema, err := planbuilder.LoadSchemaJSON(locateFile("router_test.json"))
	if err != nil {
		t.Fatal(err)
	}
	s := createSandbox("TestRouter")
	sbc1 := &sandboxConn{}
	sbc2 := &sandboxConn{}
	s.MapTestConn("-20", sbc1)
	s.MapTestConn("40-60", sbc2)
	serv := new(sandboxTopo)
	scatterConn := NewScatterConn(serv, "", "aa", 1*time.Second, 10, 1*time.Millisecond)
	router := NewRouter(serv, "aa", schema, "", scatterConn)
	q := proto.Query{
		Sql:        "update user set a=2 where id = 1",
		TabletType: topo.TYPE_MASTER,
	}
	_, err = router.Execute(&context.DummyContext{}, &q)
	if err != nil {
		t.Error(err)
	}
	wantBind := map[string]interface{}{
		"keyspace_id": "\x16k@\xb4J\xbaK\xd6",
	}
	if !reflect.DeepEqual(sbc1.BindVars[0], wantBind) {
		t.Errorf("sbc1.BindVars[0] = %#v, want %#v", sbc1.BindVars[0], wantBind)
	}
	wantQuery := "update user set a = 2 where id = 1 /* _routing keyspace_id:166b40b44aba4bd6 */"
	if sbc1.Queries[0] != wantQuery {
		t.Errorf("sbc1.Queries[0]: %q, want %q\n", sbc1.Queries[0], wantQuery)
	}
	if sbc2.ExecCount != 0 {
		t.Errorf("sbc2.ExecCount: %v, want 0\n", sbc2.ExecCount)
	}
	q.Sql = "update user set a=2 where id = 3"
	_, err = router.Execute(&context.DummyContext{}, &q)
	if err != nil {
		t.Error(err)
	}
	if sbc1.ExecCount != 1 {
		t.Errorf("sbc1.ExecCount: %v, want 1\n", sbc1.ExecCount)
	}
	wantBind = map[string]interface{}{
		"keyspace_id": "N\xb1\x90ɢ\xfa\x16\x9c",
	}
	if !reflect.DeepEqual(sbc2.BindVars[0], wantBind) {
		t.Errorf("sbc2.BindVars[0] = %#v, want %#v", sbc2.BindVars[0], wantBind)
	}
	wantQuery = "update user set a = 2 where id = 3 /* _routing keyspace_id:4eb190c9a2fa169c */"
	if sbc2.Queries[0] != wantQuery {
		t.Errorf("sbc2.Queries[0]: %q, want %q\n", sbc2.Queries[0], wantQuery)
	}
}

func TestDeleteEqual(t *testing.T) {
	schema, err := planbuilder.LoadSchemaJSON(locateFile("router_test.json"))
	if err != nil {
		t.Fatal(err)
	}
	s := createSandbox("TestRouter")
	sbc := &sandboxConn{}
	s.MapTestConn("-20", sbc)

	l := createSandbox("TestUnsharded")
	sbclookup := &sandboxConn{}
	l.MapTestConn("0", sbclookup)

	serv := new(sandboxTopo)
	scatterConn := NewScatterConn(serv, "", "aa", 1*time.Second, 10, 1*time.Millisecond)
	router := NewRouter(serv, "aa", schema, "", scatterConn)

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
	q := proto.Query{
		Sql:        "delete from user where id = 1",
		TabletType: topo.TYPE_MASTER,
	}
	_, err = router.Execute(&context.DummyContext{}, &q)
	if err != nil {
		t.Error(err)
	}
	wantBinds := []map[string]interface{}{{}, {
		"keyspace_id": "\x16k@\xb4J\xbaK\xd6",
	}}
	if !reflect.DeepEqual(sbc.BindVars, wantBinds) {
		t.Errorf("sbc.BindVars = %#v, want %#v", sbc.BindVars, wantBinds)
	}
	wantQueries := []string{
		"select id, name from user where id = 1 for update",
		"delete from user where id = 1 /* _routing keyspace_id:166b40b44aba4bd6 */",
	}
	if !reflect.DeepEqual(sbc.Queries, wantQueries) {
		t.Errorf("sbc.Queries: %q, want %q\n", sbc.Queries, wantQueries)
	}

	wantBinds = []map[string]interface{}{{
		"id": []interface{}{int64(1)},
	}, {
		"user_id": int64(1),
		"name":    []interface{}{"myname"},
	}}
	if !reflect.DeepEqual(sbclookup.BindVars, wantBinds) {
		t.Errorf("sbclookup.BindVars = \n%#v, want \n%#v", sbclookup.BindVars, wantBinds)
	}
	wantQueries = []string{
		"delete from user_idx where id in ::id",
		"delete from name_user_map where name in ::name and user_id = :user_id",
	}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: %q, want %q\n", sbclookup.Queries, wantQueries)
	}
}

func TestInsertSharded(t *testing.T) {
	schema, err := planbuilder.LoadSchemaJSON(locateFile("router_test.json"))
	if err != nil {
		t.Fatal(err)
	}
	s := createSandbox("TestRouter")
	sbc1 := &sandboxConn{}
	sbc2 := &sandboxConn{}
	s.MapTestConn("-20", sbc1)
	s.MapTestConn("40-60", sbc2)

	l := createSandbox("TestUnsharded")
	sbclookup := &sandboxConn{}
	l.MapTestConn("0", sbclookup)

	serv := new(sandboxTopo)
	scatterConn := NewScatterConn(serv, "", "aa", 1*time.Second, 10, 1*time.Millisecond)
	router := NewRouter(serv, "aa", schema, "", scatterConn)

	q := proto.Query{
		Sql:        "insert into user(id, v, name) values (1, 2, 'myname')",
		TabletType: topo.TYPE_MASTER,
	}
	_, err = router.Execute(&context.DummyContext{}, &q)
	if err != nil {
		t.Error(err)
	}
	wantBind := map[string]interface{}{
		"id": int64(1),
	}
	if !reflect.DeepEqual(sbclookup.BindVars[0], wantBind) {
		t.Errorf("sbclookup.BindVars[0] = %#v, want %#v", sbclookup.BindVars[0], wantBind)
	}
	wantQuery := "insert into user_idx(id) values(:id)"
	if sbclookup.Queries[0] != wantQuery {
		t.Errorf("sbclookup.Queries[0]: %q, want %q\n", sbclookup.Queries[0], wantQuery)
	}
	wantBind = map[string]interface{}{
		"keyspace_id": "\x16k@\xb4J\xbaK\xd6",
		"_id":         int64(1),
		"_name":       []byte("myname"),
	}
	if !reflect.DeepEqual(sbc1.BindVars[0], wantBind) {
		t.Errorf("sbc1.BindVars[0] = %#v, want %#v", sbc1.BindVars[0], wantBind)
	}
	wantQuery = "insert into user(id, v, name) values (:_id, 2, :_name) /* _routing keyspace_id:166b40b44aba4bd6 */"
	if sbc1.Queries[0] != wantQuery {
		t.Errorf("sbc1.Queries[0]: %q, want %q\n", sbc1.Queries[0], wantQuery)
	}
	if sbc2.ExecCount != 0 {
		t.Errorf("sbc2.ExecCount: %v, want 0\n", sbc2.ExecCount)
	}

	q.Sql = "insert into user(id, v, name) values (3, 2, 'myname2')"
	sbclookup.BindVars = nil
	sbclookup.Queries = nil
	_, err = router.Execute(&context.DummyContext{}, &q)
	if err != nil {
		t.Error(err)
	}
	if sbc1.ExecCount != 1 {
		t.Errorf("sbc1.ExecCount: %v, want 1\n", sbc1.ExecCount)
	}
	wantBind = map[string]interface{}{
		"id": int64(3),
	}
	if !reflect.DeepEqual(sbclookup.BindVars[0], wantBind) {
		t.Errorf("sbclookup.BindVars[0] = %#v, want %#v", sbclookup.BindVars[0], wantBind)
	}
	wantQuery = "insert into user_idx(id) values(:id)"
	if sbclookup.Queries[0] != wantQuery {
		t.Errorf("sbclookup.Queries[0]: %q, want %q\n", sbclookup.Queries[0], wantQuery)
	}
	wantBind = map[string]interface{}{
		"keyspace_id": "N\xb1\x90ɢ\xfa\x16\x9c",
		"_id":         int64(3),
		"_name":       []byte("myname2"),
	}
	if !reflect.DeepEqual(sbc2.BindVars[0], wantBind) {
		t.Errorf("sbc2.BindVars[0] = %#v, want %#v", sbc2.BindVars[0], wantBind)
	}
	wantQuery = "insert into user(id, v, name) values (:_id, 2, :_name) /* _routing keyspace_id:4eb190c9a2fa169c */"
	if sbc2.Queries[0] != wantQuery {
		t.Errorf("sbc2.Queries[0]: %q, want %q\n", sbc2.Queries[0], wantQuery)
	}
}

func TestInsertGenerator(t *testing.T) {
	schema, err := planbuilder.LoadSchemaJSON(locateFile("router_test.json"))
	if err != nil {
		t.Fatal(err)
	}
	s := createSandbox("TestRouter")
	sbc := &sandboxConn{}
	s.MapTestConn("80-a0", sbc)

	l := createSandbox("TestUnsharded")
	sbclookup := &sandboxConn{}
	l.MapTestConn("0", sbclookup)

	serv := new(sandboxTopo)
	scatterConn := NewScatterConn(serv, "", "aa", 1*time.Second, 10, 1*time.Millisecond)
	router := NewRouter(serv, "aa", schema, "", scatterConn)

	q := proto.Query{
		Sql:        "insert into user(v, name) values (2, 'myname')",
		TabletType: topo.TYPE_MASTER,
	}
	_, err = router.Execute(&context.DummyContext{}, &q)
	if err != nil {
		t.Error(err)
	}
	wantBind := map[string]interface{}{
		"id": nil,
	}
	if !reflect.DeepEqual(sbclookup.BindVars[0], wantBind) {
		t.Errorf("sbclookup.BindVars[0] = %#v, want %#v", sbclookup.BindVars[0], wantBind)
	}
	wantQuery := "insert into user_idx(id) values(:id)"
	if sbclookup.Queries[0] != wantQuery {
		t.Errorf("sbclookup.Queries[0]: %q, want %q\n", sbclookup.Queries[0], wantQuery)
	}
	wantBind = map[string]interface{}{
		"keyspace_id": "\x8c\xa6M\xe9\xc1\xb1#\xa7",
		"_id":         int64(0),
		"_name":       []byte("myname"),
	}
	if !reflect.DeepEqual(sbc.BindVars[0], wantBind) {
		t.Errorf("sbc.BindVars[0] = %#v, want %#v", sbc.BindVars[0], wantBind)
	}
	wantQuery = "insert into user(v, name, id) values (2, :_name, :_id) /* _routing keyspace_id:8ca64de9c1b123a7 */"
	if sbc.Queries[0] != wantQuery {
		t.Errorf("sbc.Queries[0]: %q, want %q\n", sbc.Queries[0], wantQuery)
	}
}

func TestInsertLookupOwned(t *testing.T) {
	schema, err := planbuilder.LoadSchemaJSON(locateFile("router_test.json"))
	if err != nil {
		t.Fatal(err)
	}
	s := createSandbox("TestRouter")
	sbc := &sandboxConn{}
	s.MapTestConn("-20", sbc)

	l := createSandbox("TestUnsharded")
	sbclookup := &sandboxConn{}
	l.MapTestConn("0", sbclookup)

	serv := new(sandboxTopo)
	scatterConn := NewScatterConn(serv, "", "aa", 1*time.Second, 10, 1*time.Millisecond)
	router := NewRouter(serv, "aa", schema, "", scatterConn)

	q := proto.Query{
		Sql:        "insert into music(user_id, id) values (2, 3)",
		TabletType: topo.TYPE_MASTER,
	}
	_, err = router.Execute(&context.DummyContext{}, &q)
	if err != nil {
		t.Error(err)
	}
	wantBind := map[string]interface{}{
		"music_id": int64(3),
		"user_id":  int64(2),
	}
	if !reflect.DeepEqual(sbclookup.BindVars[0], wantBind) {
		t.Errorf("sbclookup.BindVars[0] = %#v, want %#v", sbclookup.BindVars[0], wantBind)
	}
	wantQuery := "insert into music_user_map(music_id, user_id) values(:music_id, :user_id)"
	if sbclookup.Queries[0] != wantQuery {
		t.Errorf("sbclookup.Queries[0]: %q, want %q\n", sbclookup.Queries[0], wantQuery)
	}
	wantBind = map[string]interface{}{
		"keyspace_id": "\x06\xe7\xea\"Βp\x8f",
		"_user_id":    int64(2),
		"_id":         int64(3),
	}
	if !reflect.DeepEqual(sbc.BindVars[0], wantBind) {
		t.Errorf("sbc.BindVars[0] = %#v, want %#v", sbc.BindVars[0], wantBind)
	}
	wantQuery = "insert into music(user_id, id) values (:_user_id, :_id) /* _routing keyspace_id:06e7ea22ce92708f */"
	if sbc.Queries[0] != wantQuery {
		t.Errorf("sbc.Queries[0]: %q, want %q\n", sbc.Queries[0], wantQuery)
	}
}

func TestInsertLookupOwnedGenerator(t *testing.T) {
	schema, err := planbuilder.LoadSchemaJSON(locateFile("router_test.json"))
	if err != nil {
		t.Fatal(err)
	}
	s := createSandbox("TestRouter")
	sbc := &sandboxConn{}
	s.MapTestConn("-20", sbc)

	l := createSandbox("TestUnsharded")
	sbclookup := &sandboxConn{}
	l.MapTestConn("0", sbclookup)

	serv := new(sandboxTopo)
	scatterConn := NewScatterConn(serv, "", "aa", 1*time.Second, 10, 1*time.Millisecond)
	router := NewRouter(serv, "aa", schema, "", scatterConn)

	q := proto.Query{
		Sql:        "insert into music(user_id) values (2)",
		TabletType: topo.TYPE_MASTER,
	}
	_, err = router.Execute(&context.DummyContext{}, &q)
	if err != nil {
		t.Error(err)
	}
	wantBind := map[string]interface{}{
		"music_id": nil,
		"user_id":  int64(2),
	}
	if !reflect.DeepEqual(sbclookup.BindVars[0], wantBind) {
		t.Errorf("sbclookup.BindVars[0] = %#v, want %#v", sbclookup.BindVars[0], wantBind)
	}
	wantQuery := "insert into music_user_map(music_id, user_id) values(:music_id, :user_id)"
	if sbclookup.Queries[0] != wantQuery {
		t.Errorf("sbclookup.Queries[0]: %q, want %q\n", sbclookup.Queries[0], wantQuery)
	}
	wantBind = map[string]interface{}{
		"keyspace_id": "\x06\xe7\xea\"Βp\x8f",
		"_user_id":    int64(2),
		"_id":         int64(0),
	}
	if !reflect.DeepEqual(sbc.BindVars[0], wantBind) {
		t.Errorf("sbc.BindVars[0] = %#v, want %#v", sbc.BindVars[0], wantBind)
	}
	wantQuery = "insert into music(user_id, id) values (:_user_id, :_id) /* _routing keyspace_id:06e7ea22ce92708f */"
	if sbc.Queries[0] != wantQuery {
		t.Errorf("sbc.Queries[0]: %q, want %q\n", sbc.Queries[0], wantQuery)
	}
}

func TestInsertLookupUnowned(t *testing.T) {
	schema, err := planbuilder.LoadSchemaJSON(locateFile("router_test.json"))
	if err != nil {
		t.Fatal(err)
	}
	s := createSandbox("TestRouter")
	sbc := &sandboxConn{}
	s.MapTestConn("-20", sbc)

	l := createSandbox("TestUnsharded")
	sbclookup := &sandboxConn{}
	l.MapTestConn("0", sbclookup)

	serv := new(sandboxTopo)
	scatterConn := NewScatterConn(serv, "", "aa", 1*time.Second, 10, 1*time.Millisecond)
	router := NewRouter(serv, "aa", schema, "", scatterConn)

	q := proto.Query{
		Sql:        "insert into music_extra(user_id, music_id) values (2, 3)",
		TabletType: topo.TYPE_MASTER,
	}
	_, err = router.Execute(&context.DummyContext{}, &q)
	if err != nil {
		t.Error(err)
	}
	wantBind := map[string]interface{}{
		"music_id": int64(3),
		"user_id":  int64(2),
	}
	if !reflect.DeepEqual(sbclookup.BindVars[0], wantBind) {
		t.Errorf("sbclookup.BindVars[0] = %#v, want %#v", sbclookup.BindVars[0], wantBind)
	}
	wantQuery := "select music_id from music_user_map where music_id = :music_id and user_id = :user_id"
	if sbclookup.Queries[0] != wantQuery {
		t.Errorf("sbclookup.Queries[0]: %q, want %q\n", sbclookup.Queries[0], wantQuery)
	}
	wantBind = map[string]interface{}{
		"keyspace_id": "\x06\xe7\xea\"Βp\x8f",
		"_user_id":    int64(2),
		"_music_id":   int64(3),
	}
	if !reflect.DeepEqual(sbc.BindVars[0], wantBind) {
		t.Errorf("sbc.BindVars[0] = %#v, want %#v", sbc.BindVars[0], wantBind)
	}
	wantQuery = "insert into music_extra(user_id, music_id) values (:_user_id, :_music_id) /* _routing keyspace_id:06e7ea22ce92708f */"
	if sbc.Queries[0] != wantQuery {
		t.Errorf("sbc.Queries[0]: %q, want %q\n", sbc.Queries[0], wantQuery)
	}
}

func TestInsertLookupUnownedUnsupplied(t *testing.T) {
	schema, err := planbuilder.LoadSchemaJSON(locateFile("router_test.json"))
	if err != nil {
		t.Fatal(err)
	}
	s := createSandbox("TestRouter")
	sbc := &sandboxConn{}
	s.MapTestConn("-20", sbc)

	l := createSandbox("TestUnsharded")
	sbclookup := &sandboxConn{}
	l.MapTestConn("0", sbclookup)

	serv := new(sandboxTopo)
	scatterConn := NewScatterConn(serv, "", "aa", 1*time.Second, 10, 1*time.Millisecond)
	router := NewRouter(serv, "aa", schema, "", scatterConn)

	q := proto.Query{
		Sql:        "insert into music_extra_reversed(music_id) values (3)",
		TabletType: topo.TYPE_MASTER,
	}
	_, err = router.Execute(&context.DummyContext{}, &q)
	if err != nil {
		t.Error(err)
	}
	wantBind := map[string]interface{}{
		"music_id": int64(3),
	}
	if !reflect.DeepEqual(sbclookup.BindVars[0], wantBind) {
		t.Errorf("sbclookup.BindVars[0] = %#v, want %#v", sbclookup.BindVars[0], wantBind)
	}
	wantQuery := "select user_id from music_user_map where music_id = :music_id"
	if sbclookup.Queries[0] != wantQuery {
		t.Errorf("sbclookup.Queries[0]: %q, want %q\n", sbclookup.Queries[0], wantQuery)
	}
	wantBind = map[string]interface{}{
		"keyspace_id": "\x16k@\xb4J\xbaK\xd6",
		"_user_id":    int64(1),
		"_music_id":   int64(3),
	}
	if !reflect.DeepEqual(sbc.BindVars[0], wantBind) {
		t.Errorf("sbc.BindVars[0] = %#v, want %#v", sbc.BindVars[0], wantBind)
	}
	wantQuery = "insert into music_extra_reversed(music_id, user_id) values (:_music_id, :_user_id) /* _routing keyspace_id:166b40b44aba4bd6 */"
	if sbc.Queries[0] != wantQuery {
		t.Errorf("sbc.Queries[0]: %q, want %q\n", sbc.Queries[0], wantQuery)
	}
}

func locateFile(name string) string {
	if path.IsAbs(name) {
		return name
	}
	return testfiles.Locate("vtgate/" + name)
}
