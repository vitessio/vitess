// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"io/ioutil"
	"os"
	"path"
	"reflect"
	"testing"
	"time"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/testfiles"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtgate/planbuilder"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
	_ "github.com/youtube/vitess/go/vt/vtgate/vindexes"
	"golang.org/x/net/context"
)

var routerSchema = createTestSchema(`
{
  "Keyspaces": {
    "TestRouter": {
      "Sharded": true,
      "Vindexes": {
        "user_index": {
          "Type": "hash",
          "Owner": "user",
          "Params": {
            "Table": "user_idx",
            "Column": "id"
          }
        },
        "music_user_map": {
          "Type": "lookup_hash_unique",
          "Owner": "music",
          "Params": {
            "Table": "music_user_map",
            "From": "music_id",
            "To": "user_id"
          }
        },
        "name_user_map": {
          "Type": "lookup_hash_multi",
          "Owner": "user",
          "Params": {
            "Table": "name_user_map",
            "From": "name",
            "To": "user_id"
          }
        }
      },
      "Tables": {
        "user": {
          "ColVindexes": [
            {
              "Col": "id",
              "Name": "user_index"
            },
            {
              "Col": "name",
              "Name": "name_user_map"
            }
          ]
        },
        "user_extra": {
          "ColVindexes": [
            {
              "Col": "user_id",
              "Name": "user_index"
            }
          ]
        },
        "music": {
          "ColVindexes": [
            {
              "Col": "user_id",
              "Name": "user_index"
            },
            {
              "Col": "id",
              "Name": "music_user_map"
            }
          ]
        },
        "music_extra": {
          "ColVindexes": [
            {
              "Col": "user_id",
              "Name": "user_index"
            },
            {
              "Col": "music_id",
              "Name": "music_user_map"
            }
          ]
        },
        "music_extra_reversed": {
          "ColVindexes": [
            {
              "Col": "music_id",
              "Name": "music_user_map"
            },
            {
              "Col": "user_id",
              "Name": "user_index"
            }
          ]
        }
      }
    },
    "TestUnsharded": {
      "Sharded": false,
      "Tables": {
        "user_idx":{},
        "music_user_map":{},
        "name_user_map":{}
      }
    }
  }
}
`)

// createTestSchema creates a schema based on the JSON specs.
// It panics on failure.
func createTestSchema(schemaJSON string) *planbuilder.Schema {
	f, err := ioutil.TempFile("", "vtgate_schema")
	if err != nil {
		panic(err)
	}
	fname := f.Name()
	f.Close()
	defer os.Remove(fname)

	err = ioutil.WriteFile(fname, []byte(schemaJSON), 0644)
	if err != nil {
		panic(err)
	}
	schema, err := planbuilder.LoadSchemaJSON(fname)
	if err != nil {
		panic(err)
	}
	return schema
}

func TestUnsharded(t *testing.T) {
	createSandbox("TestRouter")
	s := createSandbox(TEST_UNSHARDED)
	sbc := &sandboxConn{}
	s.MapTestConn("0", sbc)
	serv := new(sandboxTopo)
	scatterConn := NewScatterConn(serv, "", "aa", 1*time.Second, 10, 1*time.Millisecond)
	router := NewRouter(serv, "aa", routerSchema, "", scatterConn)
	q := proto.Query{
		Sql:        "select * from music_user_map where id = 1",
		TabletType: topo.TYPE_MASTER,
	}
	_, err := router.Execute(context.Background(), &q)
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
	_, err = router.Execute(context.Background(), &q)
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
	_, err = router.Execute(context.Background(), &q)
	if err != nil {
		t.Error(err)
	}
	wantQuery = "delete from music_user_map"
	if sbc.Queries[0] != wantQuery {
		t.Errorf("sbc.Queries[0]: %q, want %q\n", sbc.Queries[0], wantQuery)
	}

	q.Sql = "insert into music_user_map values(1)"
	sbc.Queries = nil
	_, err = router.Execute(context.Background(), &q)
	if err != nil {
		t.Error(err)
	}
	wantQuery = "insert into music_user_map values(1)"
	if sbc.Queries[0] != wantQuery {
		t.Errorf("sbc.Queries[0]: %q, want %q\n", sbc.Queries[0], wantQuery)
	}
}

func TestStreamUnsharded(t *testing.T) {
	createSandbox("TestRouter")
	s := createSandbox(TEST_UNSHARDED)
	sbc := &sandboxConn{}
	s.MapTestConn("0", sbc)
	serv := new(sandboxTopo)
	scatterConn := NewScatterConn(serv, "", "aa", 1*time.Second, 10, 1*time.Millisecond)
	router := NewRouter(serv, "aa", routerSchema, "", scatterConn)
	q := proto.Query{
		Sql:        "select * from music_user_map where id = 1",
		TabletType: topo.TYPE_MASTER,
	}
	result, err := execStream(router, &q)
	if err != nil {
		t.Error(err)
	}
	wantResult := singleRowResult
	if !reflect.DeepEqual(result, wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}
}

func TestSelectEqual(t *testing.T) {
	s := createSandbox("TestRouter")
	sbc1 := &sandboxConn{}
	sbc2 := &sandboxConn{}
	s.MapTestConn("-20", sbc1)
	s.MapTestConn("40-60", sbc2)

	l := createSandbox(TEST_UNSHARDED)
	sbclookup := &sandboxConn{}
	l.MapTestConn("0", sbclookup)

	serv := new(sandboxTopo)
	scatterConn := NewScatterConn(serv, "", "aa", 1*time.Second, 10, 1*time.Millisecond)
	router := NewRouter(serv, "aa", routerSchema, "", scatterConn)
	q := proto.Query{
		Sql:        "select * from user where id = 1",
		TabletType: topo.TYPE_MASTER,
	}
	_, err := router.Execute(context.Background(), &q)
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
	_, err = router.Execute(context.Background(), &q)
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
	_, err = router.Execute(context.Background(), &q)
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
		"name": "foo",
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

func TestStreamSelectEqual(t *testing.T) {
	s := createSandbox("TestRouter")
	sbc := &sandboxConn{}
	s.MapTestConn("-20", sbc)
	serv := new(sandboxTopo)
	scatterConn := NewScatterConn(serv, "", "aa", 1*time.Second, 10, 1*time.Millisecond)
	router := NewRouter(serv, "aa", routerSchema, "", scatterConn)
	q := proto.Query{
		Sql:        "select * from user where id = 1",
		TabletType: topo.TYPE_MASTER,
	}
	result, err := execStream(router, &q)
	if err != nil {
		t.Error(err)
	}
	wantResult := singleRowResult
	if !reflect.DeepEqual(result, wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}
}

func TestSelectIN(t *testing.T) {
	s := createSandbox("TestRouter")
	sbc1 := &sandboxConn{}
	sbc2 := &sandboxConn{}
	s.MapTestConn("-20", sbc1)
	s.MapTestConn("40-60", sbc2)

	l := createSandbox(TEST_UNSHARDED)
	sbclookup := &sandboxConn{}
	l.MapTestConn("0", sbclookup)

	serv := new(sandboxTopo)
	scatterConn := NewScatterConn(serv, "", "aa", 1*time.Second, 10, 1*time.Millisecond)
	router := NewRouter(serv, "aa", routerSchema, "", scatterConn)
	q := proto.Query{
		Sql:        "select * from user where id in (1)",
		TabletType: topo.TYPE_MASTER,
	}
	_, err := router.Execute(context.Background(), &q)
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
	_, err = router.Execute(context.Background(), &q)
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
	_, err = router.Execute(context.Background(), &q)
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
		"name": "foo",
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

func TestStreamSelectIN(t *testing.T) {
	s := createSandbox("TestRouter")
	sbc1 := &sandboxConn{}
	sbc2 := &sandboxConn{}
	s.MapTestConn("-20", sbc1)
	s.MapTestConn("40-60", sbc2)

	l := createSandbox(TEST_UNSHARDED)
	sbclookup := &sandboxConn{}
	l.MapTestConn("0", sbclookup)

	serv := new(sandboxTopo)
	scatterConn := NewScatterConn(serv, "", "aa", 1*time.Second, 10, 1*time.Millisecond)
	router := NewRouter(serv, "aa", routerSchema, "", scatterConn)
	q := proto.Query{
		Sql:        "select * from user where id in (1)",
		TabletType: topo.TYPE_MASTER,
	}
	result, err := execStream(router, &q)
	if err != nil {
		t.Error(err)
	}
	wantResult := singleRowResult
	if !reflect.DeepEqual(result, wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}

	q.Sql = "select * from user where id in (1, 3)"
	sbc1.Queries = nil
	result, err = execStream(router, &q)
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
	sbc1.BindVars = nil
	sbc1.Queries = nil
	result, err = execStream(router, &q)
	if err != nil {
		t.Error(err)
	}
	wantResult = singleRowResult
	if !reflect.DeepEqual(result, wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}

	wantBinds := []map[string]interface{}{{
		"name": "foo",
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
	s := createSandbox("TestRouter")
	sbc1 := &sandboxConn{}
	sbc2 := &sandboxConn{}
	s.MapTestConn("-20", sbc1)
	s.MapTestConn("40-60", sbc2)
	serv := new(sandboxTopo)
	scatterConn := NewScatterConn(serv, "", "aa", 1*time.Second, 10, 1*time.Millisecond)
	router := NewRouter(serv, "aa", routerSchema, "", scatterConn)
	q := proto.Query{
		Sql:        "select * from user where keyrange('', '\x20')",
		TabletType: topo.TYPE_MASTER,
	}
	_, err := router.Execute(context.Background(), &q)
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
	_, err = router.Execute(context.Background(), &q)
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

func TestStreamSelectKeyrange(t *testing.T) {
	s := createSandbox("TestRouter")
	sbc1 := &sandboxConn{}
	sbc2 := &sandboxConn{}
	s.MapTestConn("-20", sbc1)
	s.MapTestConn("40-60", sbc2)
	serv := new(sandboxTopo)
	scatterConn := NewScatterConn(serv, "", "aa", 1*time.Second, 10, 1*time.Millisecond)
	router := NewRouter(serv, "aa", routerSchema, "", scatterConn)
	q := proto.Query{
		Sql:        "select * from user where keyrange('', '\x20')",
		TabletType: topo.TYPE_MASTER,
	}
	result, err := execStream(router, &q)
	if err != nil {
		t.Error(err)
	}
	wantResult := singleRowResult
	if !reflect.DeepEqual(result, wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}

	q.Sql = "select * from user where keyrange('\x40', '\x60')"
	result, err = execStream(router, &q)
	if err != nil {
		t.Error(err)
	}
	wantResult = singleRowResult
	if !reflect.DeepEqual(result, wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}
}

func TestSelectScatter(t *testing.T) {
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
	_, err := router.Execute(context.Background(), &q)
	if err != nil {
		t.Error(err)
	}
	wantBind := map[string]interface{}{}
	wantQuery := "select * from user"
	for _, conn := range conns {
		if !reflect.DeepEqual(conn.BindVars[0], wantBind) {
			t.Errorf("conn.BindVars[0] = %#v, want %#v", conn.BindVars[0], wantBind)
		}
		if conn.Queries[0] != wantQuery {
			t.Errorf("conn.Queries[0]: %q, want %q\n", conn.Queries[0], wantQuery)
		}
	}
}

func TestStreamSelectScatter(t *testing.T) {
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
	result, err := execStream(router, &q)
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

func TestUpdateEqual(t *testing.T) {
	s := createSandbox("TestRouter")
	sbc1 := &sandboxConn{}
	sbc2 := &sandboxConn{}
	s.MapTestConn("-20", sbc1)
	s.MapTestConn("40-60", sbc2)
	serv := new(sandboxTopo)
	scatterConn := NewScatterConn(serv, "", "aa", 1*time.Second, 10, 1*time.Millisecond)
	router := NewRouter(serv, "aa", routerSchema, "", scatterConn)
	q := proto.Query{
		Sql:        "update user set a=2 where id = 1",
		TabletType: topo.TYPE_MASTER,
	}
	_, err := router.Execute(context.Background(), &q)
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
	_, err = router.Execute(context.Background(), &q)
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
	s := createSandbox("TestRouter")
	sbc := &sandboxConn{}
	s.MapTestConn("-20", sbc)

	l := createSandbox(TEST_UNSHARDED)
	sbclookup := &sandboxConn{}
	l.MapTestConn("0", sbclookup)

	serv := new(sandboxTopo)
	scatterConn := NewScatterConn(serv, "", "aa", 1*time.Second, 10, 1*time.Millisecond)
	router := NewRouter(serv, "aa", routerSchema, "", scatterConn)

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
	_, err := router.Execute(context.Background(), &q)
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
	s := createSandbox("TestRouter")
	sbc1 := &sandboxConn{}
	sbc2 := &sandboxConn{}
	s.MapTestConn("-20", sbc1)
	s.MapTestConn("40-60", sbc2)

	l := createSandbox(TEST_UNSHARDED)
	sbclookup := &sandboxConn{}
	l.MapTestConn("0", sbclookup)

	serv := new(sandboxTopo)
	scatterConn := NewScatterConn(serv, "", "aa", 1*time.Second, 10, 1*time.Millisecond)
	router := NewRouter(serv, "aa", routerSchema, "", scatterConn)

	q := proto.Query{
		Sql:        "insert into user(id, v, name) values (1, 2, 'myname')",
		TabletType: topo.TYPE_MASTER,
	}
	_, err := router.Execute(context.Background(), &q)
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
		"_name":       "myname",
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
	_, err = router.Execute(context.Background(), &q)
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
		"_name":       "myname2",
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
	s := createSandbox("TestRouter")
	sbc := &sandboxConn{}
	s.MapTestConn("80-a0", sbc)

	l := createSandbox(TEST_UNSHARDED)
	sbclookup := &sandboxConn{}
	l.MapTestConn("0", sbclookup)

	serv := new(sandboxTopo)
	scatterConn := NewScatterConn(serv, "", "aa", 1*time.Second, 10, 1*time.Millisecond)
	router := NewRouter(serv, "aa", routerSchema, "", scatterConn)

	q := proto.Query{
		Sql:        "insert into user(v, name) values (2, 'myname')",
		TabletType: topo.TYPE_MASTER,
	}
	_, err := router.Execute(context.Background(), &q)
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
		"_name":       "myname",
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
	s := createSandbox("TestRouter")
	sbc := &sandboxConn{}
	s.MapTestConn("-20", sbc)

	l := createSandbox(TEST_UNSHARDED)
	sbclookup := &sandboxConn{}
	l.MapTestConn("0", sbclookup)

	serv := new(sandboxTopo)
	scatterConn := NewScatterConn(serv, "", "aa", 1*time.Second, 10, 1*time.Millisecond)
	router := NewRouter(serv, "aa", routerSchema, "", scatterConn)

	q := proto.Query{
		Sql:        "insert into music(user_id, id) values (2, 3)",
		TabletType: topo.TYPE_MASTER,
	}
	_, err := router.Execute(context.Background(), &q)
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
	s := createSandbox("TestRouter")
	sbc := &sandboxConn{}
	s.MapTestConn("-20", sbc)

	l := createSandbox(TEST_UNSHARDED)
	sbclookup := &sandboxConn{}
	l.MapTestConn("0", sbclookup)

	serv := new(sandboxTopo)
	scatterConn := NewScatterConn(serv, "", "aa", 1*time.Second, 10, 1*time.Millisecond)
	router := NewRouter(serv, "aa", routerSchema, "", scatterConn)

	q := proto.Query{
		Sql:        "insert into music(user_id) values (2)",
		TabletType: topo.TYPE_MASTER,
	}
	_, err := router.Execute(context.Background(), &q)
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
	s := createSandbox("TestRouter")
	sbc := &sandboxConn{}
	s.MapTestConn("-20", sbc)

	l := createSandbox(TEST_UNSHARDED)
	sbclookup := &sandboxConn{}
	l.MapTestConn("0", sbclookup)

	serv := new(sandboxTopo)
	scatterConn := NewScatterConn(serv, "", "aa", 1*time.Second, 10, 1*time.Millisecond)
	router := NewRouter(serv, "aa", routerSchema, "", scatterConn)

	q := proto.Query{
		Sql:        "insert into music_extra(user_id, music_id) values (2, 3)",
		TabletType: topo.TYPE_MASTER,
	}
	_, err := router.Execute(context.Background(), &q)
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
	s := createSandbox("TestRouter")
	sbc := &sandboxConn{}
	s.MapTestConn("-20", sbc)

	l := createSandbox(TEST_UNSHARDED)
	sbclookup := &sandboxConn{}
	l.MapTestConn("0", sbclookup)

	serv := new(sandboxTopo)
	scatterConn := NewScatterConn(serv, "", "aa", 1*time.Second, 10, 1*time.Millisecond)
	router := NewRouter(serv, "aa", routerSchema, "", scatterConn)

	q := proto.Query{
		Sql:        "insert into music_extra_reversed(music_id) values (3)",
		TabletType: topo.TYPE_MASTER,
	}
	_, err := router.Execute(context.Background(), &q)
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

func execStream(router *Router, q *proto.Query) (qr *mproto.QueryResult, err error) {
	results := make(chan *mproto.QueryResult, 10)
	err = router.StreamExecute(context.Background(), q, func(qr *mproto.QueryResult) error {
		results <- qr
		return nil
	})
	close(results)
	if err != nil {
		return nil, err
	}
	first := true
	for r := range results {
		if first {
			qr = &mproto.QueryResult{Fields: r.Fields}
			first = false
		}
		qr.Rows = append(qr.Rows, r.Rows...)
		qr.RowsAffected += r.RowsAffected
	}
	return qr, nil
}

func locateFile(name string) string {
	if path.IsAbs(name) {
		return name
	}
	return testfiles.Locate("vtgate/" + name)
}
