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

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/endtoend/framework"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
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
				sqltypes.NewVarBinary(binaryData),
			},
		},
	}
	if !qr.Equal(&want) {
		t.Errorf("Execute: \n%#v, want \n%#v", prettyPrint(*qr), prettyPrint(want))
	}

	// Test with bindvars.
	_, err = client.Execute(
		"insert into vitess_test values(5, null, null, :bindata)",
		map[string]*querypb.BindVariable{"bindata": sqltypes.StringBindVariable(binaryData)},
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
	if !qr.Equal(&want) {
		t.Errorf("Execute: \n%#v, want \n%#v", prettyPrint(*qr), prettyPrint(want))
	}
}

func TestNocacheListArgs(t *testing.T) {
	client := framework.NewClient()
	query := "select * from vitess_test where intval in ::list"

	qr, err := client.Execute(
		query,
		map[string]*querypb.BindVariable{
			"list": sqltypes.TestBindVariable([]interface{}{2, 3, 4}),
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
		map[string]*querypb.BindVariable{
			"list": sqltypes.TestBindVariable([]interface{}{3, 4}),
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
		map[string]*querypb.BindVariable{
			"list": sqltypes.TestBindVariable([]interface{}{3}),
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
		map[string]*querypb.BindVariable{
			"list": sqltypes.TestBindVariable([]interface{}{}),
		},
	)
	want := "empty list supplied for list (CallerID: dev)"
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

	bindVars := map[string]*querypb.BindVariable{"ival": sqltypes.Int64BindVariable(1)}
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
	err := client.Begin(false)
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
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &connParams)
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
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &connParams)
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
	defer framework.Server.SetPoolSize(framework.Server.PoolSize())
	framework.Server.SetPoolSize(1)

	for sleep := 0.1; sleep < 10.0; sleep *= 2 {
		vstart := framework.DebugVars()
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			query := fmt.Sprintf("select sleep(%v) from dual /* query: 1 */", sleep)
			framework.NewClient().Execute(query, nil)
			wg.Done()
		}()
		go func() {
			query := fmt.Sprintf("select sleep(%v) from dual /* query: 2 */", sleep)
			framework.NewClient().Execute(query, nil)
			wg.Done()
		}()
		wg.Wait()

		vend := framework.DebugVars()
		if err := compareIntDiff(vend, "Waits/TotalCount", vstart, 1); err != nil {
			t.Logf("DebugVars Waits/TotalCount not incremented with sleep=%v", sleep)
			continue
		}
		if err := compareIntDiff(vend, "Waits/Histograms/Consolidations/Count", vstart, 1); err != nil {
			t.Logf("DebugVars Waits/Histograms/Consolidations/Count not incremented with sleep=%v", sleep)
			continue
		}
		t.Logf("DebugVars properly incremented with sleep=%v", sleep)
		return
	}
	t.Error("DebugVars for consolidation not incremented")
}

func TestBindInSelect(t *testing.T) {
	client := framework.NewClient()

	// Int bind var.
	qr, err := client.Execute(
		"select :bv from dual",
		map[string]*querypb.BindVariable{"bv": sqltypes.Int64BindVariable(1)},
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
				sqltypes.NewInt64(1),
			},
		},
	}
	if !qr.Equal(want) {
		// MariaDB 10.3 has different behavior.
		want2 := want.Copy()
		want2.Fields[0].Type = sqltypes.Int32
		want2.Rows[0][0] = sqltypes.NewInt32(1)
		if !qr.Equal(want2) {
			t.Errorf("Execute:\n%v, want\n%v or\n%v", prettyPrint(*qr), prettyPrint(*want), prettyPrint(*want2))
		}
	}

	// String bind var.
	qr, err = client.Execute(
		"select :bv from dual",
		map[string]*querypb.BindVariable{"bv": sqltypes.StringBindVariable("abcd")},
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
			Flags:        1,
		}},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			{
				sqltypes.NewVarChar("abcd"),
			},
		},
	}
	// MariaDB 10.3 has different behavior.
	qr.Fields[0].Decimals = 0
	if !qr.Equal(want) {
		t.Errorf("Execute: \n%#v, want \n%#v", prettyPrint(*qr), prettyPrint(*want))
	}

	// Binary bind var.
	qr, err = client.Execute(
		"select :bv from dual",
		map[string]*querypb.BindVariable{"bv": sqltypes.StringBindVariable("\x00\xff")},
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
			Flags:        1,
		}},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			{
				sqltypes.NewVarChar("\x00\xff"),
			},
		},
	}
	// MariaDB 10.3 has different behavior.
	qr.Fields[0].Decimals = 0
	if !qr.Equal(want) {
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
	framework.Server.BroadcastHealth(0, nil, time.Minute)
	if err := framework.Server.StreamHealth(context.Background(), func(shr *querypb.StreamHealthResponse) error {
		health = shr
		return io.EOF
	}); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(health.Target, &framework.Target) {
		t.Errorf("Health: %+v, want %+v", *health.Target, framework.Target)
	}
}

func TestStreamHealth_Expired(t *testing.T) {
	var health *querypb.StreamHealthResponse
	framework.Server.BroadcastHealth(0, nil, time.Millisecond)
	time.Sleep(5 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond * 100)
	defer cancel()
	if err := framework.Server.StreamHealth(ctx, func(shr *querypb.StreamHealthResponse) error {
		health = shr
		return io.EOF
	}); err != nil {
		t.Fatal(err)
	}
	if health != nil {
		t.Errorf("Health: %v, want %v", health, nil)
	}
}

func TestQueryStats(t *testing.T) {
	client := framework.NewClient()
	vstart := framework.DebugVars()

	start := time.Now()
	query := "select /* query_stats */ eid from vitess_a where eid = :eid"
	bv := map[string]*querypb.BindVariable{"eid": sqltypes.Int64BindVariable(1)}
	if _, err := client.Execute(query, bv); err != nil {
		t.Fatal(err)
	}
	stat := framework.QueryStats()[query]
	duration := int(time.Since(start))
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

	// Query cache should be updated for errors that happen at MySQL level also.
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

	// Ensure BeginExecute also updates the stats and strips comments.
	query = "select /* begin_execute */ 1 /* trailing comment */"
	if _, err := client.BeginExecute(query, bv); err != nil {
		t.Fatal(err)
	}
	if err := client.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, ok := framework.QueryStats()[query]; ok {
		t.Errorf("query stats included trailing comments for BeginExecute: %v", framework.QueryStats())
	}
	stripped := "select /* begin_execute */ 1"
	if _, ok := framework.QueryStats()[stripped]; !ok {
		t.Errorf("query stats did not get updated for BeginExecute: %v", framework.QueryStats())
	}
}

func TestDBAStatements(t *testing.T) {
	client := framework.NewClient()

	qr, err := client.Execute("show variables like 'version'", nil)
	if err != nil {
		t.Error(err)
		return
	}
	wantCol := sqltypes.NewVarChar("version")
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

var testLogs []string

func recordInfof(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	testLogs = append(testLogs, msg)
	log.Infof(msg)
}

func recordErrorf(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	testLogs = append(testLogs, msg)
	log.Errorf(msg)
}

func setupTestLogger() {
	testLogs = make([]string, 0)
	tabletenv.Infof = recordInfof
	tabletenv.Errorf = recordErrorf
}

func clearTestLogger() {
	tabletenv.Infof = log.Infof
	tabletenv.Errorf = log.Errorf
}

func getTestLog(i int) string {
	if i < len(testLogs) {
		return testLogs[i]
	}
	return fmt.Sprintf("ERROR: log %d/%d does not exist", i, len(testLogs))
}

func TestLogTruncation(t *testing.T) {
	client := framework.NewClient()
	setupTestLogger()
	defer clearTestLogger()

	// Test that a long error string is not truncated by default
	_, err := client.Execute(
		"insert into vitess_test values(123, null, :data, null)",
		map[string]*querypb.BindVariable{"data": sqltypes.StringBindVariable("THIS IS A LONG LONG LONG LONG QUERY STRING THAT SHOULD BE SHORTENED")},
	)
	wantLog := `Data too long for column 'charval' at row 1 (errno 1406) (sqlstate 22001) (CallerID: dev): Sql: "insert into vitess_test values(123, null, :data, null)", BindVars: {#maxLimit: "type:INT64 value:\"10001\" "data: "type:VARCHAR value:\"THIS IS A LONG LONG LONG LONG QUERY STRING THAT SHOULD BE SHORTENED\" "}`
	wantErr := wantLog
	if err == nil {
		t.Errorf("query unexpectedly succeeded")
	}
	if getTestLog(0) != wantLog {
		t.Errorf("log was unexpectedly truncated: got\n'%s', want\n'%s'", getTestLog(0), wantLog)
	}

	if err.Error() != wantErr {
		t.Errorf("error was unexpectedly truncated: got\n'%s', want\n'%s'", err.Error(), wantErr)
	}

	// Test that the data too long error is truncated once the option is set
	*sqlparser.TruncateErrLen = 30
	_, err = client.Execute(
		"insert into vitess_test values(123, null, :data, null)",
		map[string]*querypb.BindVariable{"data": sqltypes.StringBindVariable("THIS IS A LONG LONG LONG LONG QUERY STRING THAT SHOULD BE SHORTENED")},
	)
	wantLog = `Data too long for column 'charval' at row 1 (errno 1406) (sqlstate 22001) (CallerID: dev): Sql: "insert into vitess [TRUNCATED]", BindVars: {#maxLim [TRUNCATED]`
	wantErr = `Data too long for column 'charval' at row 1 (errno 1406) (sqlstate 22001) (CallerID: dev): Sql: "insert into vitess_test values(123, null, :data, null)", BindVars: {#maxLimit: "type:INT64 value:\"10001\" "data: "type:VARCHAR value:\"THIS IS A LONG LONG LONG LONG QUERY STRING THAT SHOULD BE SHORTENED\" "}`
	if err == nil {
		t.Errorf("query unexpectedly succeeded")
	}
	if getTestLog(1) != wantLog {
		t.Errorf("log was not truncated properly: got\n'%s', want\n'%s'", getTestLog(1), wantLog)
	}
	if err.Error() != wantErr {
		t.Errorf("error was unexpectedly truncated: got\n'%s', want\n'%s'", err.Error(), wantErr)
	}

	// Test that trailing comments are preserved data too long error is truncated once the option is set
	*sqlparser.TruncateErrLen = 30
	_, err = client.Execute(
		"insert into vitess_test values(123, null, :data, null) /* KEEP ME */",
		map[string]*querypb.BindVariable{"data": sqltypes.StringBindVariable("THIS IS A LONG LONG LONG LONG QUERY STRING THAT SHOULD BE SHORTENED")},
	)
	wantLog = `Data too long for column 'charval' at row 1 (errno 1406) (sqlstate 22001) (CallerID: dev): Sql: "insert into vitess [TRUNCATED] /* KEEP ME */", BindVars: {#maxLim [TRUNCATED]`
	wantErr = `Data too long for column 'charval' at row 1 (errno 1406) (sqlstate 22001) (CallerID: dev): Sql: "insert into vitess_test values(123, null, :data, null) /* KEEP ME */", BindVars: {#maxLimit: "type:INT64 value:\"10001\" "data: "type:VARCHAR value:\"THIS IS A LONG LONG LONG LONG QUERY STRING THAT SHOULD BE SHORTENED\" "}`
	if err == nil {
		t.Errorf("query unexpectedly succeeded")
	}
	if getTestLog(2) != wantLog {
		t.Errorf("log was not truncated properly: got\n'%s', want\n'%s'", getTestLog(2), wantLog)
	}
	if err.Error() != wantErr {
		t.Errorf("error was unexpectedly truncated: got\n'%s', want\n'%s'", err.Error(), wantErr)
	}
}

func TestClientFoundRows(t *testing.T) {
	client := framework.NewClient()
	if _, err := client.Execute("insert into vitess_test(intval, charval) values(124, 'aa')", nil); err != nil {
		t.Fatal(err)
	}
	defer client.Execute("delete from vitess_test where intval= 124", nil)

	// CLIENT_FOUND_ROWS flag is off.
	if err := client.Begin(false); err != nil {
		t.Error(err)
	}
	qr, err := client.Execute("update vitess_test set charval='aa' where intval=124", nil)
	if err != nil {
		t.Error(err)
	}
	if qr.RowsAffected != 0 {
		t.Errorf("Execute(rowsFound==false): %d, want 0", qr.RowsAffected)
	}
	if err := client.Rollback(); err != nil {
		t.Error(err)
	}

	// CLIENT_FOUND_ROWS flag is on.
	if err := client.Begin(true); err != nil {
		t.Error(err)
	}
	qr, err = client.Execute("update vitess_test set charval='aa' where intval=124", nil)
	if err != nil {
		t.Error(err)
	}
	if qr.RowsAffected != 1 {
		t.Errorf("Execute(rowsFound==true): %d, want 1", qr.RowsAffected)
	}
	if err := client.Rollback(); err != nil {
		t.Error(err)
	}
}

func TestLastInsertId(t *testing.T) {
	client := framework.NewClient()
	_, err := client.Execute("insert ignore into vitess_autoinc_seq SET name = 'foo', sequence = 0", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Execute("delete from vitess_autoinc_seq where name = 'foo'", nil)

	if err := client.Begin(true); err != nil {
		t.Fatal(err)
	}
	defer client.Rollback()

	res, err := client.Execute("insert ignore into vitess_autoinc_seq SET name = 'foo', sequence = 0", nil)
	if err != nil {
		t.Fatal(err)
	}

	qr, err := client.Execute("update vitess_autoinc_seq set sequence=last_insert_id(sequence + 1) where name='foo'", nil)
	if err != nil {
		t.Error(err)
	}

	insID := res.InsertID

	if want, got := insID+1, qr.InsertID; want != got {
		t.Errorf("insertId mismatch; got %v, want %v", got, want)
	}

	qr, err = client.Execute("select sequence from vitess_autoinc_seq where name = 'foo'", nil)
	if err != nil {
		t.Error(err)
	}

	wantCol := sqltypes.NewUint64(insID + uint64(1))
	if !reflect.DeepEqual(qr.Rows[0][0], wantCol) {
		t.Errorf("Execute: \n%#v, want \n%#v", qr.Rows[0][0], wantCol)
	}
}

func TestAppDebugRequest(t *testing.T) {
	client := framework.NewClient()

	// Insert with normal user works

	if _, err := client.Execute("insert into vitess_test_debuguser(intval, charval) values(124, 'aa')", nil); err != nil {
		t.Fatal(err)
	}

	defer client.Execute("delete from vitess_test where intval= 124", nil)

	// Set vt_appdebug
	ctx := callerid.NewContext(
		context.Background(),
		&vtrpcpb.CallerID{},
		&querypb.VTGateCallerID{Username: "vt_appdebug"})

	want := "Access denied for user 'vt_appdebug'@'localhost'"

	client = framework.NewClientWithContext(ctx)

	// Start a transaction. This test the other flow that a client can use to insert a value.
	client.Begin(false)
	_, err := client.Execute("insert into vitess_test_debuguser(intval, charval) values(124, 'aa')", nil)

	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Errorf("Error: %v, want prefix %s", err, want)
	}

	// Normal flow, when a client is trying to insert a value and the insert is not in the
	// context of another transaction.
	_, err = client.Execute("insert into vitess_test_debuguser(intval, charval) values(124, 'aa')", nil)

	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Errorf("Error: %v, want prefix %s", err, want)
	}

	_, err = client.Execute("select * from vitess_test_debuguser where intval=1", nil)
	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Errorf("Error: %v, want prefix %s", err, want)
	}
}
