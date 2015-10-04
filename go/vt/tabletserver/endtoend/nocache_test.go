// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package endtoend

import (
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/youtube/vitess/go/mysql"
	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/tabletserver/endtoend/framework"
	"github.com/youtube/vitess/go/vt/tabletserver/proto"
)

func TestSimpleRead(t *testing.T) {
	vstart := framework.DebugVars()
	_, err := framework.NewDefaultClient().Execute("select * from vtocc_test where intval=1", nil)
	if err != nil {
		t.Error(err)
		return
	}
	vend := framework.DebugVars()
	if err := compareIntDiff(vend, "Queries.TotalCount", vstart, 1); err != nil {
		t.Error(err)
	}
	if err := compareIntDiff(vend, "Queries.Histograms.PASS_SELECT.Count", vstart, 1); err != nil {
		t.Error(err)
	}
}

func TestBinary(t *testing.T) {
	client := framework.NewDefaultClient()
	defer client.Execute("delete from vtocc_test where intval in (4,5)", nil)

	binaryData := "\x00'\"\b\n\r\t\x1a\\\x00\x0f\xf0\xff"
	// Test without bindvars.
	_, err := client.Execute(
		"insert into vtocc_test values "+
			"(4, null, null, '\\0\\'\\\"\\b\\n\\r\\t\\Z\\\\\x00\x0f\xf0\xff')",
		nil,
	)
	if err != nil {
		t.Error(err)
		return
	}
	qr, err := client.Execute("select binval from vtocc_test where intval=4", nil)
	if err != nil {
		t.Error(err)
		return
	}
	want := mproto.QueryResult{
		Fields: []mproto.Field{
			{
				Name:  "binval",
				Type:  mysql.TypeVarString,
				Flags: mysql.FlagBinary,
			},
		},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			[]sqltypes.Value{
				sqltypes.Value{Inner: sqltypes.String(binaryData)},
			},
		},
	}
	if !reflect.DeepEqual(*qr, want) {
		t.Errorf("Execute: \n%#v, want \n%#v", *qr, want)
	}

	// Test with bindvars.
	_, err = client.Execute(
		"insert into vtocc_test values(5, null, null, :bindata)",
		map[string]interface{}{"bindata": binaryData},
	)
	if err != nil {
		t.Error(err)
		return
	}
	qr, err = client.Execute("select binval from vtocc_test where intval=5", nil)
	if err != nil {
		t.Error(err)
		return
	}
	if !reflect.DeepEqual(*qr, want) {
		t.Errorf("Execute: \n%#v, want \n%#v", *qr, want)
	}
}

func TestNocacheListArgs(t *testing.T) {
	client := framework.NewDefaultClient()
	query := "select * from vtocc_test where intval in ::list"

	qr, err := client.Execute(
		query,
		map[string]interface{}{
			"list": []interface{}{2, 3, 4},
		},
	)
	if err != nil {
		t.Error(err)
		return
	}
	if qr.RowsAffected != 2 {
		t.Errorf("rows affected: %d, want 2", qr.RowsAffected)
	}

	qr, err = client.Execute(
		query,
		map[string]interface{}{
			"list": []interface{}{3, 4},
		},
	)
	if err != nil {
		t.Error(err)
		return
	}
	if qr.RowsAffected != 1 {
		t.Errorf("rows affected: %d, want 1", qr.RowsAffected)
	}

	qr, err = client.Execute(
		query,
		map[string]interface{}{
			"list": []interface{}{3},
		},
	)
	if err != nil {
		t.Error(err)
		return
	}
	if qr.RowsAffected != 1 {
		t.Errorf("rows affected: %d, want 1", qr.RowsAffected)
	}

	// Error case
	_, err = client.Execute(
		query,
		map[string]interface{}{
			"list": []interface{}{},
		},
	)
	want := "error: empty list supplied for list"
	if err == nil || err.Error() != want {
		t.Errorf("error returned: %v, want %s", err, want)
		return
	}
}

func TestIntegrityError(t *testing.T) {
	vstart := framework.DebugVars()
	client := framework.NewDefaultClient()
	_, err := client.Execute("insert into vtocc_test values(1, null, null, null)", nil)
	want := "error: Duplicate entry '1'"
	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Errorf("Error: %v, want prefix %s", err, want)
	}
	if err := compareIntDiff(framework.DebugVars(), "InfoErrors.DupKey", vstart, 1); err != nil {
		t.Error(err)
	}
}

func TestTrailingComment(t *testing.T) {
	vstart := framework.DebugVars()
	v1 := framework.FetchInt(vstart, "QueryCacheLength")

	bindVars := map[string]interface{}{"ival": 1}
	client := framework.NewDefaultClient()

	for _, query := range []string{
		"select * from vtocc_test where intval=:ival",
		"select * from vtocc_test where intval=:ival /* comment */",
		"select * from vtocc_test where intval=:ival /* comment1 */ /* comment2 */",
	} {
		_, err := client.Execute(query, bindVars)
		if err != nil {
			t.Error(err)
			return
		}
		v2 := framework.FetchInt(framework.DebugVars(), "QueryCacheLength")
		if v2 != v1+1 {
			t.Errorf("QueryCacheLength(%s): %d, want %d", query, v2, v1+1)
		}
	}
}

func TestStrictMode(t *testing.T) {
	queries := []string{
		"insert into vtocc_a(eid, id, name, foo) values (7, 1+1, '', '')",
		"insert into vtocc_d(eid, id) values (1, 1)",
		"update vtocc_a set eid = 1+1 where eid = 1 and id = 1",
		"insert into vtocc_d(eid, id) values (1, 1)",
		"insert into upsert_test(id1, id2) values " +
			"(1, 1), (2, 2) on duplicate key update id1 = 1",
		"insert into upsert_test(id1, id2) select eid, id " +
			"from vtocc_a limit 1 on duplicate key update id2 = id1",
		"insert into upsert_test(id1, id2) values " +
			"(1, 1) on duplicate key update id1 = 2+1",
	}

	// Strict mode on.
	func() {
		client := framework.NewDefaultClient()
		err := client.Begin()
		if err != nil {
			t.Error(err)
			return
		}
		defer client.Rollback()

		want := "error: DML too complex"
		for _, query := range queries {
			_, err = client.Execute(query, nil)
			if err == nil || err.Error() != want {
				t.Errorf("Execute(%s): %v, want %s", query, err, want)
			}
		}
	}()

	// Strict mode off.
	func() {
		framework.DefaultServer.SetStrictMode(false)
		defer framework.DefaultServer.SetStrictMode(true)

		for _, query := range queries {
			client := framework.NewDefaultClient()
			err := client.Begin()
			if err != nil {
				t.Error(err)
				return
			}
			_, err = client.Execute(query, nil)
			if err != nil {
				t.Error(err)
			}
			client.Rollback()
		}
	}()
}

func TestUpsertNonPKHit(t *testing.T) {
	client := framework.NewDefaultClient()
	err := client.Begin()
	if err != nil {
		t.Error(err)
		return
	}
	defer client.Rollback()

	_, err = client.Execute("insert into upsert_test(id1, id2) values (1, 1)", nil)
	if err != nil {
		t.Error(err)
		return
	}
	_, err = client.Execute(
		"insert into upsert_test(id1, id2) values "+
			"(2, 1) on duplicate key update id2 = 2",
		nil,
	)
	want := "error: Duplicate entry '1' for key 'id2_idx'"
	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Errorf("Execute: %v, must start with %s", err, want)
	}
}

func TestSchemaReload(t *testing.T) {
	conn, err := mysql.Connect(connParams)
	if err != nil {
		t.Error(err)
		return
	}
	_, err = conn.ExecuteFetch("create table vtocc_temp(intval int)", 10, false)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		_, _ = conn.ExecuteFetch("drop table vtocc_temp", 10, false)
		conn.Close()
	}()
	framework.DefaultServer.ReloadSchema()
	client := framework.NewDefaultClient()
	waitTime := 50 * time.Millisecond
	for i := 0; i < 10; i++ {
		time.Sleep(waitTime)
		waitTime += 50 * time.Millisecond
		_, err = client.Execute("select * from vtocc_temp", nil)
		if err == nil {
			return
		}
		want := "error: table vtocc_temp not found in schema"
		if err.Error() != want {
			t.Errorf("Error: %v, want %s", err, want)
			return
		}
	}
	t.Error("schema did not reload")
}

func TestConsolidation(t *testing.T) {
	vstart := framework.DebugVars()
	defer framework.DefaultServer.SetPoolSize(framework.DefaultServer.PoolSize())
	framework.DefaultServer.SetPoolSize(1)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		framework.NewDefaultClient().Execute("select sleep(0.25) from dual", nil)
		wg.Done()
	}()
	go func() {
		framework.NewDefaultClient().Execute("select sleep(0.25) from dual", nil)
		wg.Done()
	}()
	wg.Wait()

	vend := framework.DebugVars()
	if err := compareIntDiff(vend, "Waits.TotalCount", vstart, 1); err != nil {
		t.Error(err)
	}
	if err := compareIntDiff(vend, "Waits.Histograms.Consolidations.Count", vstart, 1); err != nil {
		t.Error(err)
	}
}

func TestBatchRead(t *testing.T) {
	client := framework.NewDefaultClient()
	queries := []proto.BoundQuery{{
		Sql:           "select * from vtocc_a where id = :a",
		BindVariables: map[string]interface{}{"a": 2},
	}, {
		Sql:           "select * from vtocc_b where id = :b",
		BindVariables: map[string]interface{}{"b": 2},
	}}
	qr1 := mproto.QueryResult{
		Fields: []mproto.Field{{
			Name:  "eid",
			Type:  mysql.TypeLonglong,
			Flags: 0,
		}, {
			Name:  "id",
			Type:  mysql.TypeLong,
			Flags: 0,
		}, {
			Name:  "name",
			Type:  mysql.TypeVarString,
			Flags: 0,
		}, {
			Name:  "foo",
			Type:  mysql.TypeVarString,
			Flags: mysql.FlagBinary,
		}},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			[]sqltypes.Value{
				sqltypes.Value{Inner: sqltypes.Numeric("1")},
				sqltypes.Value{Inner: sqltypes.Numeric("2")},
				sqltypes.Value{Inner: sqltypes.String("bcde")},
				sqltypes.Value{Inner: sqltypes.String("fghi")},
			},
		},
	}
	qr2 := mproto.QueryResult{
		Fields: []mproto.Field{{
			Name:  "eid",
			Type:  mysql.TypeLonglong,
			Flags: 0,
		}, {
			Name:  "id",
			Type:  mysql.TypeLong,
			Flags: 0,
		}},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			[]sqltypes.Value{
				sqltypes.Value{Inner: sqltypes.Numeric("1")},
				sqltypes.Value{Inner: sqltypes.Numeric("2")},
			},
		},
	}
	want := &proto.QueryResultList{
		List: []mproto.QueryResult{qr1, qr2},
	}

	qrl, err := client.ExecuteBatch(queries, false)
	if err != nil {
		t.Error(err)
		return
	}
	if !reflect.DeepEqual(qrl, want) {
		t.Errorf("ExecueBatch: \n%#v, want \n%#v", qrl, want)
	}
}

func TestBatchTransaction(t *testing.T) {
	client := framework.NewDefaultClient()
	queries := []proto.BoundQuery{{
		Sql: "insert into vtocc_test values(4, null, null, null)",
	}, {
		Sql: "select * from vtocc_test where intval = 4",
	}, {
		Sql: "delete from vtocc_test where intval = 4",
	}}

	wantRows := [][]sqltypes.Value{
		[]sqltypes.Value{
			sqltypes.Value{Inner: sqltypes.Numeric("4")},
			sqltypes.Value{},
			sqltypes.Value{},
			sqltypes.Value{},
		},
	}

	// Not in transaction, AsTransaction false
	qrl, err := client.ExecuteBatch(queries, false)
	if err != nil {
		t.Error(err)
		return
	}
	if !reflect.DeepEqual(qrl.List[1].Rows, wantRows) {
		t.Errorf("Rows: \n%#v, want \n%#v", qrl.List[1].Rows, wantRows)
	}

	// Not in transaction, AsTransaction true
	qrl, err = client.ExecuteBatch(queries, true)
	if err != nil {
		t.Error(err)
		return
	}
	if !reflect.DeepEqual(qrl.List[1].Rows, wantRows) {
		t.Errorf("Rows: \n%#v, want \n%#v", qrl.List[1].Rows, wantRows)
	}

	// In transaction, AsTransaction false
	func() {
		err = client.Begin()
		if err != nil {
			t.Error(err)
			return
		}
		defer client.Commit()
		qrl, err = client.ExecuteBatch(queries, false)
		if err != nil {
			t.Error(err)
			return
		}
		if !reflect.DeepEqual(qrl.List[1].Rows, wantRows) {
			t.Errorf("Rows: \n%#v, want \n%#v", qrl.List[1].Rows, wantRows)
		}
	}()

	// In transaction, AsTransaction true
	func() {
		err = client.Begin()
		if err != nil {
			t.Error(err)
			return
		}
		defer client.Rollback()
		qrl, err = client.ExecuteBatch(queries, true)
		want := "error: cannot start a new transaction in the scope of an existing one"
		if err == nil || err.Error() != want {
			t.Errorf("Error: %v, want %s", err, want)
		}
	}()
}
