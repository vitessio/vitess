// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gorpcvtgateconn

import (
	"fmt"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateconn"
	"github.com/youtube/vitess/go/vt/vttest"
	"golang.org/x/net/context"
)

const (
	actionSelect = iota
	actionCommit
	actionRollback
)

var once sync.Once

func initEnv() {
	once.Do(func() {
		err := vttest.LocalLaunch(
			[]string{"0"},
			1,
			0,
			"test_keyspace",
			"create table test_table(id int auto_increment, val varchar(128), primary key(id))",
			`{"Keyspaces":{"test_keyspace":{"Tables":{"test_table":""}}}}`,
		)
		if err != nil {
			vttest.LocalTeardown()
			panic(err)
		}
	})
}

func TestMain(m *testing.M) {
	r := m.Run()
	vttest.LocalTeardown()
	os.Exit(r)
}

func TestExecuteCommit(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode.")
	}
	initEnv()

	conn := testDial(t)
	defer conn.Close()

	result, err := testExec(conn, "insert into test_table(val) values ('abcd')", nil, actionCommit)
	if err != nil {
		t.Error(err)
	}
	if result.InsertId == 0 {
		t.Errorf("InsertId: 0, want non-zero")
	}
	if result.RowsAffected != 1 {
		t.Errorf("RowsAffected: %d, want 1", result.RowsAffected)
	}

	result, err = testExec(conn, "select * from test_table", nil, actionSelect)
	if err != nil {
		t.Error(err)
	}
	wantFields := []mproto.Field{
		{Name: "id", Type: 3},
		{Name: "val", Type: 253},
	}
	wantVal := "abcd"
	if !reflect.DeepEqual(result.Fields, wantFields) {
		t.Errorf("Fields: \n%#v, want \n%#v", result.Fields, wantFields)
	}
	gotVal := result.Rows[0][1].String()
	if gotVal != wantVal {
		t.Errorf("val: %q, want %q", gotVal, wantVal)
	}

	_, err = testExec(conn, "delete from test_table", nil, actionCommit)
	if err != nil {
		t.Error(err)
	}
	if result.RowsAffected != 1 {
		t.Errorf("RowsAffected: %d, want 1", result.RowsAffected)
	}
}

func TestStreamExecute(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode.")
	}
	initEnv()

	conn := testDial(t)
	defer conn.Close()

	_, err := testExec(conn, "insert into test_table(val) values ('abcd')", nil, actionCommit)
	if err != nil {
		t.Error(err)
	}

	ch, errFunc := conn.StreamExecute(context.Background(), "select * from test_table", nil, "master")

	result := <-ch
	wantFields := []mproto.Field{
		{Name: "id", Type: 3},
		{Name: "val", Type: 253},
	}
	if !reflect.DeepEqual(result.Fields, wantFields) {
		t.Errorf("Fields: \n%#v, want \n%#v", result.Fields, wantFields)
	}

	result = <-ch
	wantVal := "abcd"
	gotVal := result.Rows[0][1].String()
	if gotVal != wantVal {
		t.Errorf("val: %q, want %q", gotVal, wantVal)
	}

	for result = range ch {
		fmt.Errorf("Result: %+v, want closed channel", result)
	}
	if err := errFunc(); err != nil {
		t.Error(err)
	}

	_, err = testExec(conn, "delete from test_table", nil, actionCommit)
	if err != nil {
		t.Error(err)
	}
}

func TestRollback(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode.")
	}
	initEnv()

	conn := testDial(t)
	defer conn.Close()

	_, err := testExec(conn, "insert into test_table(val) values ('abcd')", nil, actionRollback)
	if err != nil {
		t.Error(err)
	}

	result, err := testExec(conn, "select * from test_table", nil, actionSelect)
	if err != nil {
		t.Error(err)
	}
	if result.RowsAffected != 0 {
		t.Errorf("RowsAffected: %d, want 0", result.RowsAffected)
	}
}

func TestExecuteUnimplemented(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode.")
	}
	initEnv()

	conn := testDial(t)
	defer conn.Close()

	_, err := conn.ExecuteBatch(nil, nil, "")
	want := "not implemented yet"
	if err.Error() != want {
		t.Errorf("ExecuteBatch: %v, want %q", err, want)
	}

	_, err = conn.SplitQuery(nil, proto.BoundQuery{}, 1)
	if err.Error() != want {
		t.Errorf("SplitQuery: %v, want %q", err, want)
	}
}

func TestExecuteFail(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode.")
	}
	initEnv()

	conn := testDial(t)
	defer conn.Close()

	_, err := testExec(conn, "select * from notable", nil, actionSelect)
	want := "execute: cannot route query: select * from notable: table notable not found"
	if err == nil || err.Error() != want {
		t.Errorf("err: %v, want %q", err, want)
	}
}

func TestBadTx(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode.")
	}
	initEnv()

	conn := testDial(t)
	defer conn.Close()
	ctx := context.Background()

	err := conn.Commit(ctx)
	want := "commit: not in transaction"
	if err == nil || err.Error() != want {
		t.Errorf("Commit: %v, want %v", err, want)
	}

	err = conn.Rollback(ctx)
	if err != nil {
		t.Error(err)
	}

	err = conn.Begin(ctx)
	if err != nil {
		t.Error(err)
	}
	err = conn.Begin(ctx)
	want = "begin: already in a transaction"
	if err == nil || err.Error() != want {
		t.Errorf("Begin: %v, want %v", err, want)
	}
}

func testDial(t *testing.T) vtgateconn.VTGateConn {
	// TODO(sougou): Fetch port from the launch output.
	conn, err := dial(nil, "localhost:15007", time.Duration(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	return conn
}

func testExec(conn vtgateconn.VTGateConn, query string, bindVars map[string]interface{}, action int) (*mproto.QueryResult, error) {
	ctx := context.Background()
	var err error
	if action == actionCommit || action == actionRollback {
		err = conn.Begin(ctx)
		if err != nil {
			return nil, err
		}
	}
	result, err := conn.Execute(ctx, query, nil, "master")
	if err != nil {
		return nil, err
	}
	switch action {
	case actionCommit:
		err = conn.Commit(ctx)
	case actionRollback:
		err = conn.Rollback(ctx)
	}
	if err != nil {
		return nil, err
	}
	return result, nil
}
