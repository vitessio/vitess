// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package endtoend

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/vttablet/endtoend/framework"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

func TestSimpleRead(t *testing.T) {
	vstart := framework.DebugVars()
	_, err := framework.NewClient().Execute("select * from vitess_test where intval=1", nil)
	if err != nil {
		t.Error(err)
		return
	}
	vend := framework.DebugVars()
	if err := compareIntDiff(vend, "Queries/TotalCount", vstart, 1); err != nil {
		t.Error(err)
	}
	if err := compareIntDiff(vend, "Queries/Histograms/PASS_SELECT/Count", vstart, 1); err != nil {
		t.Error(err)
	}
}

func TestBinary(t *testing.T) {
	client := framework.NewClient()
	defer client.Execute("delete from vitess_test where intval in (4,5)", nil)

	binaryData := "\x00'\"\b\n\r\t\x1a\\\x00\x0f\xf0\xff"
	// Test without bindvars.
	_, err := client.Execute(
		"insert into vitess_test values "+
			"(4, null, null, '\\0\\'\\\"\\b\\n\\r\\t\\Z\\\\\x00\x0f\xf0\xff')",
		nil,
	)
	if err != nil {
		t.Error(err)
		return
	}
	qr, err := client.Execute("select binval from vitess_test where intval=4", nil)
	if err != nil {
		t.Error(err)
		return
	}
	want := sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name:         "binval",
				Type:         sqltypes.VarBinary,
				Table:        "vitess_test",
				OrgTable:     "vitess_test",
				Database:     "vttest",
				OrgName:      "binval",
				ColumnLength: 256,
				Charset:      63,
				Flags:        128,
			},
		},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(sqltypes.VarBinary, []byte(binaryData)),
			},
		},
	}
	if !reflect.DeepEqual(*qr, want) {
		t.Errorf("Execute: \n%#v, want \n%#v", prettyPrint(*qr), prettyPrint(want))
	}

	// Test with bindvars.
	_, err = client.Execute(
		"insert into vitess_test values(5, null, null, :bindata)",
		map[string]interface{}{"bindata": binaryData},
	)
	if err != nil {
		t.Error(err)
		return
	}
	qr, err = client.Execute("select binval from vitess_test where intval=5", nil)
	if err != nil {
		t.Error(err)
		return
	}
	if !reflect.DeepEqual(*qr, want) {
		t.Errorf("Execute: \n%#v, want \n%#v", prettyPrint(*qr), prettyPrint(want))
	}
}

func TestNocacheListArgs(t *testing.T) {
	client := framework.NewClient()
	query := "select * from vitess_test where intval in ::list"

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
	want := "empty list supplied for list"
	if err == nil || err.Error() != want {
		t.Errorf("Error: %v, want %s", err, want)
		return
	}
}

func TestIntegrityError(t *testing.T) {
	vstart := framework.DebugVars()
	client := framework.NewClient()
	_, err := client.Execute("insert into vitess_test values(1, null, null, null)", nil)
	want := "Duplicate entry '1'"
	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Errorf("Error: %v, want prefix %s", err, want)
	}
	if err := compareIntDiff(framework.DebugVars(), "Errors/ALREADY_EXISTS", vstart, 1); err != nil {
		t.Error(err)
	}
}

func TestTrailingComment(t *testing.T) {
	vstart := framework.DebugVars()
	v1 := framework.FetchInt(vstart, "QueryCacheLength")

	bindVars := map[string]interface{}{"ival": 1}
	client := framework.NewClient()

	for _, query := range []string{
		"select * from vitess_test where intval=:ival",
		"select * from vitess_test where intval=:ival /* comment */",
		"select * from vitess_test where intval=:ival /* comment1 */ /* comment2 */",
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

func TestUpsertNonPKHit(t *testing.T) {
	client := framework.NewClient()
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
	want := "Duplicate entry '1' for key 'id2_idx'"
	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Errorf("Execute: %v, must start with %s", err, want)
	}
}

func TestSchemaReload(t *testing.T) {
	conn, err := sqldb.Connect(connParams)
	if err != nil {
		t.Error(err)
		return
	}
	defer conn.Close()

	_, err = conn.ExecuteFetch("create table vitess_temp(intval int)", 10, false)
	if err != nil {
		t.Error(err)
		return
	}
	defer conn.ExecuteFetch("drop table vitess_temp", 10, false)

	framework.Server.ReloadSchema(context.Background())
	client := framework.NewClient()
	waitTime := 50 * time.Millisecond
	for i := 0; i < 10; i++ {
		time.Sleep(waitTime)
		waitTime += 50 * time.Millisecond
		_, err = client.Execute("select * from vitess_temp", nil)
		if err == nil {
			return
		}
		want := "table vitess_temp not found in schema"
		if err.Error() != want {
			t.Errorf("Error: %v, want %s", err, want)
			return
		}
	}
	t.Error("schema did not reload")
}

func TestSidecarTables(t *testing.T) {
	conn, err := sqldb.Connect(connParams)
	if err != nil {
		t.Error(err)
		return
	}
	defer conn.Close()
	for _, table := range []string{
		"redo_state",
		"redo_statement",
		"dt_state",
		"dt_participant",
	} {
		_, err = conn.ExecuteFetch(fmt.Sprintf("describe _vt.%s", table), 10, false)
		if err != nil {
			t.Error(err)
			return
		}
	}
}

func TestConsolidation(t *testing.T) {
	vstart := framework.DebugVars()
	defer framework.Server.SetPoolSize(framework.Server.PoolSize())
	framework.Server.SetPoolSize(1)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		framework.NewClient().Execute("select sleep(0.25) from dual", nil)
		wg.Done()
	}()
	go func() {
		framework.NewClient().Execute("select sleep(0.25) from dual", nil)
		wg.Done()
	}()
	wg.Wait()

	vend := framework.DebugVars()
	if err := compareIntDiff(vend, "Waits/TotalCount", vstart, 1); err != nil {
		t.Error(err)
	}
	if err := compareIntDiff(vend, "Waits/Histograms/Consolidations/Count", vstart, 1); err != nil {
		t.Error(err)
	}
}

func TestBindInSelect(t *testing.T) {
	client := framework.NewClient()

	// Int bind var.
	qr, err := client.Execute(
		"select :bv from dual",
		map[string]interface{}{"bv": 1},
	)
	if err != nil {
		t.Error(err)
		return
	}
	want := &sqltypes.Result{
		Fields: []*querypb.Field{{
			Name:         "1",
			Type:         sqltypes.Int64,
			ColumnLength: 1,
			Charset:      63,
			Flags:        32897,
		}},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(sqltypes.Int64, []byte("1")),
			},
		},
	}
	if !reflect.DeepEqual(qr, want) {
		t.Errorf("Execute: \n%#v, want \n%#v", prettyPrint(*qr), prettyPrint(*want))
	}

	// String bind var.
	qr, err = client.Execute(
		"select :bv from dual",
		map[string]interface{}{"bv": "abcd"},
	)
	if err != nil {
		t.Error(err)
		return
	}
	want = &sqltypes.Result{
		Fields: []*querypb.Field{{
			Name:         "abcd",
			Type:         sqltypes.VarChar,
			ColumnLength: 12,
			Charset:      33,
			Decimals:     31,
			Flags:        1,
		}},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(sqltypes.VarChar, []byte("abcd")),
			},
		},
	}
	if !reflect.DeepEqual(qr, want) {
		t.Errorf("Execute: \n%#v, want \n%#v", prettyPrint(*qr), prettyPrint(*want))
	}

	// Binary bind var.
	qr, err = client.Execute(
		"select :bv from dual",
		map[string]interface{}{"bv": "\x00\xff"},
	)
	if err != nil {
		t.Error(err)
		return
	}
	want = &sqltypes.Result{
		Fields: []*querypb.Field{{
			Name:         "",
			Type:         sqltypes.VarChar,
			ColumnLength: 6,
			Charset:      33,
			Decimals:     31,
			Flags:        1,
		}},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(sqltypes.VarChar, []byte("\x00\xff")),
			},
		},
	}
	if !reflect.DeepEqual(qr, want) {
		t.Errorf("Execute: \n%#v, want \n%#v", prettyPrint(*qr), prettyPrint(*want))
	}
}

func TestHealth(t *testing.T) {
	response, err := http.Get(fmt.Sprintf("%s/debug/health", framework.ServerAddress))
	if err != nil {
		t.Error(err)
		return
	}
	defer response.Body.Close()
	result, err := ioutil.ReadAll(response.Body)
	if err != nil {
		t.Error(err)
		return
	}
	if string(result) != "ok" {
		t.Errorf("Health check: %s, want ok", result)
	}
}

func TestStreamHealth(t *testing.T) {
	var health *querypb.StreamHealthResponse
	framework.Server.BroadcastHealth(0, nil)
	if err := framework.Server.StreamHealth(context.Background(), func(shr *querypb.StreamHealthResponse) error {
		health = shr
		return io.EOF
	}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(*health.Target, framework.Target) {
		t.Errorf("Health: %+v, want %+v", *health.Target, framework.Target)
	}
}

func TestQueryStats(t *testing.T) {
	client := framework.NewClient()
	vstart := framework.DebugVars()

	start := time.Now()
	query := "select /* query_stats */ eid from vitess_a where eid = :eid"
	bv := map[string]interface{}{"eid": 1}
	_, _ = client.Execute(query, bv)
	stat := framework.QueryStats()[query]
	duration := int(time.Now().Sub(start))
	if stat.Time <= 0 || stat.Time > duration {
		t.Errorf("stat.Time: %d, must be between 0 and %d", stat.Time, duration)
	}
	if stat.MysqlTime <= 0 || stat.MysqlTime > duration {
		t.Errorf("stat.MysqlTime: %d, must be between 0 and %d", stat.MysqlTime, duration)
	}
	stat.Time = 0
	stat.MysqlTime = 0
	want := framework.QueryStat{
		Query:      query,
		Table:      "vitess_a",
		Plan:       "PASS_SELECT",
		QueryCount: 1,
		RowCount:   2,
		ErrorCount: 0,
	}
	if stat != want {
		t.Errorf("stat: %+v, want %+v", stat, want)
	}

	query = "select /* query_stats */ eid from vitess_a where dontexist(eid) = :eid"
	_, _ = client.Execute(query, bv)
	stat = framework.QueryStats()[query]
	stat.Time = 0
	stat.MysqlTime = 0
	want = framework.QueryStat{
		Query:      query,
		Table:      "vitess_a",
		Plan:       "PASS_SELECT",
		QueryCount: 1,
		RowCount:   0,
		ErrorCount: 1,
	}
	if stat != want {
		t.Errorf("stat: %+v, want %+v", stat, want)
	}
	vend := framework.DebugVars()
	if err := compareIntDiff(vend, "QueryCounts/vitess_a.PASS_SELECT", vstart, 2); err != nil {
		t.Error(err)
	}
	if err := compareIntDiff(vend, "QueryRowCounts/vitess_a.PASS_SELECT", vstart, 2); err != nil {
		t.Error(err)
	}
	if err := compareIntDiff(vend, "QueryErrorCounts/vitess_a.PASS_SELECT", vstart, 1); err != nil {
		t.Error(err)
	}
}

func TestDBAStatements(t *testing.T) {
	client := framework.NewClient()

	qr, err := client.Execute("show variables like 'version'", nil)
	if err != nil {
		t.Error(err)
		return
	}
	wantCol := sqltypes.MakeTrusted(sqltypes.VarChar, []byte("version"))
	if !reflect.DeepEqual(qr.Rows[0][0], wantCol) {
		t.Errorf("Execute: \n%#v, want \n%#v", qr.Rows[0][0], wantCol)
	}

	qr, err = client.Execute("describe vitess_a", nil)
	if err != nil {
		t.Error(err)
		return
	}
	if qr.RowsAffected != 4 {
		t.Errorf("RowsAffected: %d, want 4", qr.RowsAffected)
	}

	qr, err = client.Execute("explain vitess_a", nil)
	if err != nil {
		t.Error(err)
		return
	}
	if qr.RowsAffected != 4 {
		t.Errorf("RowsAffected: %d, want 4", qr.RowsAffected)
	}
}
