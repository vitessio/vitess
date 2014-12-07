// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"path"
	"reflect"
	"testing"
	"time"

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
	if !reflect.DeepEqual(sbc.BindVars, wantBind) {
		t.Errorf("sbc.BindVars = %#v, got %#v", sbc.BindVars, wantBind)
	}
	wantQuery := "select * from music_user_map where id = 1"
	if sbc.Query != wantQuery {
		t.Errorf("sbc.Query: %q, want %q\n", sbc.Query, wantQuery)
	}

	q.Sql = "update music_user_map set id = 1"
	_, err = router.Execute(&context.DummyContext{}, &q)
	if err != nil {
		t.Error(err)
	}
	wantQuery = "update music_user_map set id = 1"
	if sbc.Query != wantQuery {
		t.Errorf("sbc.Query: %q, want %q\n", sbc.Query, wantQuery)
	}

	q.Sql = "delete from music_user_map"
	_, err = router.Execute(&context.DummyContext{}, &q)
	if err != nil {
		t.Error(err)
	}
	wantQuery = "delete from music_user_map"
	if sbc.Query != wantQuery {
		t.Errorf("sbc.Query: %q, want %q\n", sbc.Query, wantQuery)
	}

	q.Sql = "insert into music_user_map values(1)"
	_, err = router.Execute(&context.DummyContext{}, &q)
	if err != nil {
		t.Error(err)
	}
	wantQuery = "insert into music_user_map values(1)"
	if sbc.Query != wantQuery {
		t.Errorf("sbc.Query: %q, want %q\n", sbc.Query, wantQuery)
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
	if sbc1.ExecCount != 1 {
		t.Errorf("sbc1.ExecCount: %v, want 1\n", sbc1.ExecCount)
	}
	wantBind := map[string]interface{}{}
	if !reflect.DeepEqual(sbc1.BindVars, wantBind) {
		t.Errorf("sbc1.BindVars = %#v, got %#v", sbc1.BindVars, wantBind)
	}
	wantQuery := "select * from user where id = 1"
	if sbc1.Query != wantQuery {
		t.Errorf("sbc1.Query: %q, want %q\n", sbc1.Query, wantQuery)
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
	if sbc2.ExecCount != 1 {
		t.Errorf("sbc2.ExecCount: %v, want 1\n", sbc2.ExecCount)
	}
	wantBind = map[string]interface{}{}
	if !reflect.DeepEqual(sbc2.BindVars, wantBind) {
		t.Errorf("sbc2.BindVars = %#v, got %#v", sbc2.BindVars, wantBind)
	}
	wantQuery = "select * from user where id = 3"
	if sbc2.Query != wantQuery {
		t.Errorf("sbc2.Query: %q, want %q\n", sbc2.Query, wantQuery)
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
	serv := new(sandboxTopo)
	scatterConn := NewScatterConn(serv, "", "aa", 1*time.Second, 10, 1*time.Millisecond)
	router := NewRouter(serv, "aa", schema, "", scatterConn)
	q := proto.Query{
		Sql:        "insert into user(id, v) values (1, 2)",
		TabletType: topo.TYPE_MASTER,
	}
	_, err = router.Execute(&context.DummyContext{}, &q)
	if err != nil {
		t.Error(err)
	}
	if sbc1.ExecCount != 1 {
		t.Errorf("sbc1.ExecCount: %v, want 1\n", sbc1.ExecCount)
	}
	wantBind := map[string]interface{}{
		"keyspace_id": "\x16k@\xb4J\xbaK\xd6",
		"_id":         int64(1),
	}
	if !reflect.DeepEqual(sbc1.BindVars, wantBind) {
		t.Errorf("sbc1.BindVars = %#v, got %#v", sbc1.BindVars, wantBind)
	}
	wantQuery := "insert into user(id, v) values (1, 2) /* _routing keyspace_id:166b40b44aba4bd6 */"
	if sbc1.Query != wantQuery {
		t.Errorf("sbc1.Query: %q, want %q\n", sbc1.Query, wantQuery)
	}
	if sbc2.ExecCount != 0 {
		t.Errorf("sbc2.ExecCount: %v, want 0\n", sbc2.ExecCount)
	}
	q.Sql = "insert into user(id, v) values (3, 2)"
	_, err = router.Execute(&context.DummyContext{}, &q)
	if err != nil {
		t.Error(err)
	}
	if sbc1.ExecCount != 1 {
		t.Errorf("sbc1.ExecCount: %v, want 1\n", sbc1.ExecCount)
	}
	if sbc2.ExecCount != 1 {
		t.Errorf("sbc2.ExecCount: %v, want 1\n", sbc2.ExecCount)
	}
	wantBind = map[string]interface{}{
		"keyspace_id": "N\xb1\x90ɢ\xfa\x16\x9c",
		"_id":         int64(3),
	}
	if !reflect.DeepEqual(sbc2.BindVars, wantBind) {
		t.Errorf("sbc2.BindVars = %#v, got %#v", sbc2.BindVars, wantBind)
	}
	wantQuery = "insert into user(id, v) values (3, 2) /* _routing keyspace_id:4eb190c9a2fa169c */"
	if sbc2.Query != wantQuery {
		t.Errorf("sbc2.Query: %q, want %q\n", sbc2.Query, wantQuery)
	}
}

func locateFile(name string) string {
	if path.IsAbs(name) {
		return name
	}
	return testfiles.Locate("vtgate/" + name)
}
