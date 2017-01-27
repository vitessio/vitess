// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"expvar"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/mysqlconn"
	"github.com/youtube/vitess/go/mysqlconn/fakesqldb"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/sqlparser"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

func TestSchemaInfoStrictMode(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	for query, result := range getSchemaInfoBaseTestQueries() {
		db.AddQuery(query, result)
	}
	schemaInfo := newTestSchemaInfo(10, 1*time.Second, 1*time.Second, false)
	t.Log(schemaInfo)
	defer handleAndVerifyTabletError(
		t,
		"schema info Open should fail because of underlying "+
			"connection cannot verify strict mode",
		vtrpcpb.ErrorCode_INTERNAL_ERROR,
	)
	schemaInfo.Open(db.ConnParams(), true)
}

func TestSchemaInfoOpenFailedDueToMissMySQLTime(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	db.AddQuery("select unix_timestamp()", &sqltypes.Result{
		// Make this query fail by returning 2 values.
		Fields: []*querypb.Field{
			{Type: sqltypes.Uint64},
		},
		Rows: [][]sqltypes.Value{
			{sqltypes.MakeString([]byte("1427325875"))},
			{sqltypes.MakeString([]byte("1427325875"))},
		},
	})
	schemaInfo := newTestSchemaInfo(10, 1*time.Second, 1*time.Second, false)
	defer handleAndVerifyTabletError(
		t,
		"schema info Open should fail because of it could not get MySQL time",
		vtrpcpb.ErrorCode_UNKNOWN_ERROR,
	)
	schemaInfo.Open(db.ConnParams(), false)
}

func TestSchemaInfoOpenFailedDueToIncorrectMysqlRowNum(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	db.AddQuery("select unix_timestamp()", &sqltypes.Result{
		Fields: []*querypb.Field{{
			Type: sqltypes.Uint64,
		}},
		Rows: [][]sqltypes.Value{
			// make this query fail by returning NULL
			{sqltypes.NULL},
		},
	})
	schemaInfo := newTestSchemaInfo(10, 1*time.Second, 1*time.Second, false)
	defer handleAndVerifyTabletError(
		t,
		"schema info Open should fail because of incorrect MySQL row number",
		vtrpcpb.ErrorCode_UNKNOWN_ERROR,
	)
	schemaInfo.Open(db.ConnParams(), false)
}

func TestSchemaInfoOpenFailedDueToInvalidTimeFormat(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	db.AddQuery("select unix_timestamp()", &sqltypes.Result{
		Fields: []*querypb.Field{{
			Type: sqltypes.VarChar,
		}},
		Rows: [][]sqltypes.Value{
			// make safety check fail, invalid time format
			{sqltypes.MakeString([]byte("invalid_time"))},
		},
	})
	schemaInfo := newTestSchemaInfo(10, 1*time.Second, 1*time.Second, false)
	defer handleAndVerifyTabletError(
		t,
		"schema info Open should fail because it could not get MySQL time",
		vtrpcpb.ErrorCode_UNKNOWN_ERROR,
	)
	schemaInfo.Open(db.ConnParams(), false)
}

func TestSchemaInfoOpenFailedDueToExecErr(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	for query, result := range getSchemaInfoBaseTestQueries() {
		db.AddQuery(query, result)
	}
	db.AddRejectedQuery(mysqlconn.BaseShowTables, fmt.Errorf("injected error"))
	schemaInfo := newTestSchemaInfo(10, 1*time.Second, 1*time.Second, false)
	defer handleAndVerifyTabletError(
		t,
		"schema info Open should fail because conn.Exec failed",
		vtrpcpb.ErrorCode_UNKNOWN_ERROR,
	)
	schemaInfo.Open(db.ConnParams(), false)
}

func TestSchemaInfoOpenFailedDueToTableInfoErr(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	for query, result := range getSchemaInfoBaseTestQueries() {
		db.AddQuery(query, result)
	}
	db.AddQuery(mysqlconn.BaseShowTables, &sqltypes.Result{
		Fields:       mysqlconn.BaseShowTablesFields,
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			mysqlconn.BaseShowTablesRow("test_table", false, ""),
		},
	})
	db.AddQuery("select * from test_table where 1 != 1", &sqltypes.Result{
		// this will cause NewTableInfo error, as it expects zero rows.
		Fields: []*querypb.Field{
			{
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: [][]sqltypes.Value{
			{sqltypes.MakeString([]byte(""))},
		},
	})
	schemaInfo := newTestSchemaInfo(10, 1*time.Second, 1*time.Second, false)
	defer handleAndVerifyTabletError(
		t,
		"schema info Open should fail because NewTableInfo failed",
		vtrpcpb.ErrorCode_INTERNAL_ERROR,
	)
	schemaInfo.Open(db.ConnParams(), false)
}

func TestSchemaInfoReload(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	ctx := context.Background()
	for query, result := range getSchemaInfoTestSupportedQueries() {
		db.AddQuery(query, result)
	}
	idleTimeout := 10 * time.Second
	schemaInfo := newTestSchemaInfo(10, 10*time.Second, idleTimeout, false)
	schemaInfo.Open(db.ConnParams(), true)
	defer schemaInfo.Close()
	// this new table does not exist
	newTable := sqlparser.NewTableIdent("test_table_04")
	tableInfo := schemaInfo.GetTable(newTable)
	if tableInfo != nil {
		t.Fatalf("table: %s exists; expecting nil", newTable)
	}
	schemaInfo.Reload(ctx)
	tableInfo = schemaInfo.GetTable(newTable)
	if tableInfo != nil {
		t.Fatalf("table: %s exists; expecting nil", newTable)
	}
	db.AddQuery(mysqlconn.BaseShowTables, &sqltypes.Result{
		// make this query return nothing during reload
		Fields: mysqlconn.BaseShowTablesFields,
	})
	db.AddQuery(mysqlconn.BaseShowTablesForTable(newTable.String()), &sqltypes.Result{
		Fields:       mysqlconn.BaseShowTablesFields,
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			mysqlconn.BaseShowTablesRow(newTable.String(), false, ""),
		},
	})

	db.AddQuery("select * from test_table_04 where 1 != 1", &sqltypes.Result{
		Fields: []*querypb.Field{{
			Name: "pk",
			Type: sqltypes.Int32,
		}},
	})
	db.AddQuery("describe test_table_04", &sqltypes.Result{
		Fields:       mysqlconn.DescribeTableFields,
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			mysqlconn.DescribeTableRow("pk", "int(11)", false, "PRI", "0"),
		},
	})
	db.AddQuery("show index from test_table_04", &sqltypes.Result{
		Fields:       mysqlconn.ShowIndexFromTableFields,
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			mysqlconn.ShowIndexFromTableRow("test_table_04", true, "PRIMARY", 1, "pk", false),
		},
	})

	schemaInfo.Reload(ctx)
	tableInfo = schemaInfo.GetTable(newTable)
	if tableInfo != nil {
		t.Fatalf("table: %s exists; expecting nil", newTable)
	}

	// test reload with new table: test_table_04
	db.AddQuery(mysqlconn.BaseShowTables, &sqltypes.Result{
		Fields:       mysqlconn.BaseShowTablesFields,
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			mysqlconn.BaseShowTablesRow(newTable.String(), false, ""),
		},
	})
	tableInfo = schemaInfo.GetTable(newTable)
	if tableInfo != nil {
		t.Fatalf("table: %s exists; expecting nil", newTable)
	}
	if err := schemaInfo.Reload(ctx); err != nil {
		t.Fatalf("schemaInfo.Reload() error: %v", err)
	}
	tableInfo = schemaInfo.GetTable(newTable)
	if tableInfo == nil {
		t.Fatalf("table: %s should exist", newTable)
	}
}

func TestSchemaInfoCreateOrUpdateTableFailedDuetoExecErr(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	for query, result := range getSchemaInfoTestSupportedQueries() {
		db.AddQuery(query, result)
	}
	db.AddRejectedQuery(mysqlconn.BaseShowTablesForTable("test_table"), fmt.Errorf("forced fail"))
	schemaInfo := newTestSchemaInfo(10, 1*time.Second, 1*time.Second, true)
	schemaInfo.Open(db.ConnParams(), false)
	defer schemaInfo.Close()
	originalSchemaErrorCount := schemaInfo.queryServiceStats.InternalErrors.Counts()["Schema"]
	// should silently fail: no errors returned, but increment a counter
	schemaInfo.CreateOrUpdateTable(context.Background(), "test_table")

	newSchemaErrorCount := schemaInfo.queryServiceStats.InternalErrors.Counts()["Schema"]
	schemaErrorDiff := newSchemaErrorCount - originalSchemaErrorCount
	if schemaErrorDiff != 1 {
		t.Errorf("InternalErrors.Schema counter should have increased by 1, instead got %v", schemaErrorDiff)
	}
}

func TestSchemaInfoCreateOrUpdateTable(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	for query, result := range getSchemaInfoTestSupportedQueries() {
		db.AddQuery(query, result)
	}
	existingTable := "test_table_01"
	db.AddQuery(mysqlconn.BaseShowTablesForTable(existingTable), &sqltypes.Result{
		Fields:       mysqlconn.BaseShowTablesFields,
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			mysqlconn.BaseShowTablesRow(existingTable, false, ""),
		},
	})
	schemaInfo := newTestSchemaInfo(10, 1*time.Second, 1*time.Second, false)
	schemaInfo.Open(db.ConnParams(), false)
	found := false
	schemaInfo.RegisterNotifier("test", func(schema map[string]*TableInfo) {
		_, found = schema["test_table_01"]
	})
	schemaInfo.CreateOrUpdateTable(context.Background(), "test_table_01")
	if !found {
		t.Error("Notifier: want true, got false")
	}
	schemaInfo.UnregisterNotifier("test")
	schemaInfo.Close()
}

func TestSchemaInfoDropTable(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	for query, result := range getSchemaInfoTestSupportedQueries() {
		db.AddQuery(query, result)
	}
	existingTable := sqlparser.NewTableIdent("test_table_01")
	db.AddQuery(mysqlconn.BaseShowTablesForTable(existingTable.String()), &sqltypes.Result{
		Fields:       mysqlconn.BaseShowTablesFields,
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			mysqlconn.BaseShowTablesRow(existingTable.String(), false, ""),
		},
	})
	schemaInfo := newTestSchemaInfo(10, 1*time.Second, 1*time.Second, false)
	schemaInfo.Open(db.ConnParams(), false)
	tableInfo := schemaInfo.GetTable(existingTable)
	if tableInfo == nil {
		t.Fatalf("table: %s should exist", existingTable)
	}
	found := false
	schemaInfo.RegisterNotifier("test", func(schema map[string]*TableInfo) {
		_, found = schema["test_table_01"]
	})
	schemaInfo.DropTable(existingTable)
	if found {
		t.Error("Notifier: want false, got true")
	}
	tableInfo = schemaInfo.GetTable(existingTable)
	if tableInfo != nil {
		t.Fatalf("table: %s should not exist", existingTable)
	}
	schemaInfo.Close()
}

func TestSchemaInfoGetPlanPanicDuetoEmptyQuery(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	for query, result := range getSchemaInfoTestSupportedQueries() {
		db.AddQuery(query, result)
	}
	schemaInfo := newTestSchemaInfo(10, 10*time.Second, 10*time.Second, false)
	schemaInfo.Open(db.ConnParams(), true)
	defer schemaInfo.Close()

	ctx := context.Background()
	logStats := NewLogStats(ctx, "GetPlanStats")
	defer handleAndVerifyTabletError(
		t,
		"schema info GetPlan should fail because of empty query",
		vtrpcpb.ErrorCode_UNKNOWN_ERROR,
	)
	schemaInfo.GetPlan(ctx, logStats, "")
}

func TestSchemaInfoQueryCacheFailDueToInvalidCacheSize(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	for query, result := range getSchemaInfoTestSupportedQueries() {
		db.AddQuery(query, result)
	}
	schemaInfo := newTestSchemaInfo(10, 10*time.Second, 10*time.Second, false)
	schemaInfo.Open(db.ConnParams(), true)
	defer schemaInfo.Close()
	defer handleAndVerifyTabletError(
		t,
		"schema info SetQueryCacheSize should use a positive size",
		vtrpcpb.ErrorCode_BAD_INPUT,
	)
	schemaInfo.SetQueryCacheCap(0)
}

func TestSchemaInfoQueryCache(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	for query, result := range getSchemaInfoTestSupportedQueries() {
		db.AddQuery(query, result)
	}

	firstQuery := "select * from test_table_01"
	secondQuery := "select * from test_table_02"
	db.AddQuery("select * from test_table_01 where 1 != 1", &sqltypes.Result{})
	db.AddQuery("select * from test_table_02 where 1 != 1", &sqltypes.Result{})

	schemaInfo := newTestSchemaInfo(10, 10*time.Second, 10*time.Second, true)
	schemaInfo.Open(db.ConnParams(), true)
	defer schemaInfo.Close()

	ctx := context.Background()
	logStats := NewLogStats(ctx, "GetPlanStats")
	schemaInfo.SetQueryCacheCap(1)
	firstPlan := schemaInfo.GetPlan(ctx, logStats, firstQuery)
	if firstPlan == nil {
		t.Fatalf("plan should not be nil")
	}
	secondPlan := schemaInfo.GetPlan(ctx, logStats, secondQuery)
	if secondPlan == nil {
		t.Fatalf("plan should not be nil")
	}
	expvar.Do(func(kv expvar.KeyValue) {
		_ = kv.Value.String()
	})
	schemaInfo.ClearQueryPlanCache()
}

func TestSchemaInfoExportVars(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	for query, result := range getSchemaInfoTestSupportedQueries() {
		db.AddQuery(query, result)
	}
	schemaInfo := newTestSchemaInfo(10, 1*time.Second, 1*time.Second, true)
	schemaInfo.Open(db.ConnParams(), true)
	defer schemaInfo.Close()
	expvar.Do(func(kv expvar.KeyValue) {
		_ = kv.Value.String()
	})
}

func TestUpdatedMysqlStats(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	ctx := context.Background()
	for query, result := range getSchemaInfoTestSupportedQueries() {
		db.AddQuery(query, result)
	}
	idleTimeout := 10 * time.Second
	schemaInfo := newTestSchemaInfo(10, 10*time.Second, idleTimeout, false)
	schemaInfo.Open(db.ConnParams(), true)
	defer schemaInfo.Close()
	// Add new table
	tableName := sqlparser.NewTableIdent("mysql_stats_test_table")
	db.AddQuery(mysqlconn.BaseShowTables, &sqltypes.Result{
		Fields:       mysqlconn.BaseShowTablesFields,
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			mysqlconn.BaseShowTablesRow(tableName.String(), false, ""),
		},
	})
	// Add queries necessary for CreateOrUpdateTable() and NewTableInfo()
	db.AddQuery(mysqlconn.BaseShowTablesForTable(tableName.String()), &sqltypes.Result{
		Fields:       mysqlconn.BaseShowTablesFields,
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			mysqlconn.BaseShowTablesRow(tableName.String(), false, ""),
		},
	})
	q := fmt.Sprintf("select * from %s where 1 != 1", tableName)
	db.AddQuery(q, &sqltypes.Result{
		Fields: []*querypb.Field{{
			Name: "pk",
			Type: sqltypes.Int32,
		}},
	})
	q = fmt.Sprintf("describe %s", tableName)
	db.AddQuery(q, &sqltypes.Result{
		Fields:       mysqlconn.DescribeTableFields,
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			mysqlconn.DescribeTableRow("pk", "int(11)", false, "PRI", "0"),
		},
	})
	q = fmt.Sprintf("show index from %s", tableName)
	db.AddQuery(q, &sqltypes.Result{
		Fields:       mysqlconn.ShowIndexFromTableFields,
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			mysqlconn.ShowIndexFromTableRow(tableName.String(), true, "PRIMARY", 1, "pk", false),
		},
	})

	if err := schemaInfo.Reload(ctx); err != nil {
		t.Fatalf("schemaInfo.Reload() error: %v", err)
	}
	tableInfo := schemaInfo.GetTable(tableName)
	if tableInfo == nil {
		t.Fatalf("table: %s should exist", tableName)
	}
	tr1 := tableInfo.TableRows
	dl1 := tableInfo.DataLength
	il1 := tableInfo.IndexLength
	df1 := tableInfo.DataFree
	mdl1 := tableInfo.MaxDataLength
	// Update existing table with new stats.
	row := mysqlconn.BaseShowTablesRow(tableName.String(), false, "")
	row[4] = sqltypes.MakeTrusted(sqltypes.Uint64, []byte("2")) // table_rows
	row[5] = sqltypes.MakeTrusted(sqltypes.Uint64, []byte("3")) // data_length
	row[6] = sqltypes.MakeTrusted(sqltypes.Uint64, []byte("4")) // index_length
	row[7] = sqltypes.MakeTrusted(sqltypes.Uint64, []byte("5")) // data_free
	row[8] = sqltypes.MakeTrusted(sqltypes.Uint64, []byte("6")) // max_data_length
	db.AddQuery(mysqlconn.BaseShowTablesForTable(tableName.String()), &sqltypes.Result{
		Fields:       mysqlconn.BaseShowTablesFields,
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			row,
		},
	})
	if err := schemaInfo.Reload(ctx); err != nil {
		t.Fatalf("schemaInfo.Reload() error: %v", err)
	}
	tableInfo = schemaInfo.GetTable(tableName)
	tr2 := tableInfo.TableRows
	dl2 := tableInfo.DataLength
	il2 := tableInfo.IndexLength
	df2 := tableInfo.DataFree
	mdl2 := tableInfo.MaxDataLength
	if tr1 == tr2 || dl1 == dl2 || il1 == il2 || df1 == df2 || mdl1 == mdl2 {
		t.Logf("%v==%v %v==%v %v==%v %v==%v %v==%v", tr1, tr2, dl1, dl2, il1, il2, df1, df2, mdl1, mdl2)
		t.Fatalf("MysqlStats() results failed to change between queries. ")
	}
}

func TestSchemaInfoStatsURL(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	for query, result := range getSchemaInfoTestSupportedQueries() {
		db.AddQuery(query, result)
	}
	query := "select * from test_table_01"
	db.AddQuery("select * from test_table_01 where 1 != 1", &sqltypes.Result{})
	schemaInfo := newTestSchemaInfo(10, 1*time.Second, 1*time.Second, false)
	schemaInfo.Open(db.ConnParams(), true)
	defer schemaInfo.Close()
	// warm up cache
	ctx := context.Background()
	logStats := NewLogStats(ctx, "GetPlanStats")
	schemaInfo.GetPlan(ctx, logStats, query)

	request, _ := http.NewRequest("GET", schemaInfo.endpoints[debugQueryPlansKey], nil)
	response := httptest.NewRecorder()
	schemaInfo.ServeHTTP(response, request)

	request, _ = http.NewRequest("GET", schemaInfo.endpoints[debugQueryStatsKey], nil)
	response = httptest.NewRecorder()
	schemaInfo.ServeHTTP(response, request)

	request, _ = http.NewRequest("GET", schemaInfo.endpoints[debugSchemaKey], nil)
	response = httptest.NewRecorder()
	schemaInfo.ServeHTTP(response, request)

	request, _ = http.NewRequest("GET", "/debug/unknown", nil)
	response = httptest.NewRecorder()
	schemaInfo.ServeHTTP(response, request)
}

func getSchemaInfoBaseTestQueries() map[string]*sqltypes.Result {
	return map[string]*sqltypes.Result{
		// queries for schema info
		"select unix_timestamp()": {
			Fields: []*querypb.Field{{
				Type: sqltypes.Uint64,
			}},
			Rows: [][]sqltypes.Value{
				{sqltypes.MakeString([]byte("1427325875"))},
			},
		},
	}
}

func getSchemaInfoTestSupportedQueries() map[string]*sqltypes.Result {
	return map[string]*sqltypes.Result{
		// queries for schema info
		"select unix_timestamp()": {
			Fields: []*querypb.Field{{
				Type: sqltypes.Uint64,
			}},
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				{sqltypes.MakeTrusted(sqltypes.Int32, []byte("1427325875"))},
			},
		},
		"select @@global.sql_mode": {
			Fields: []*querypb.Field{{
				Type: sqltypes.VarChar,
			}},
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				{sqltypes.MakeString([]byte("STRICT_TRANS_TABLES"))},
			},
		},
		"select @@autocommit": {
			Fields: []*querypb.Field{{
				Type: sqltypes.Uint64,
			}},
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				{sqltypes.MakeString([]byte("1"))},
			},
		},
		mysqlconn.BaseShowTables: {
			Fields:       mysqlconn.BaseShowTablesFields,
			RowsAffected: 3,
			Rows: [][]sqltypes.Value{
				mysqlconn.BaseShowTablesRow("test_table_01", false, ""),
				mysqlconn.BaseShowTablesRow("test_table_02", false, ""),
				mysqlconn.BaseShowTablesRow("test_table_03", false, ""),
			},
		},
		"select * from test_table_01 where 1 != 1": {
			Fields: []*querypb.Field{{
				Name: "pk",
				Type: sqltypes.Int32,
			}},
		},
		"describe test_table_01": {
			Fields:       mysqlconn.DescribeTableFields,
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				mysqlconn.DescribeTableRow("pk", "int(11)", false, "PRI", "0"),
			},
		},
		"select * from test_table_02 where 1 != 1": {
			Fields: []*querypb.Field{{
				Name: "pk",
				Type: sqltypes.Int32,
			}},
		},
		"describe test_table_02": {
			Fields:       mysqlconn.DescribeTableFields,
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				mysqlconn.DescribeTableRow("pk", "int(11)", false, "PRI", "0"),
			},
		},
		"select * from test_table_03 where 1 != 1": {
			Fields: []*querypb.Field{{
				Name: "pk",
				Type: sqltypes.Int32,
			}},
		},
		"describe test_table_03": {
			Fields:       mysqlconn.DescribeTableFields,
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				mysqlconn.DescribeTableRow("pk", "int(11)", false, "PRI", "0"),
			},
		},
		// for SplitQuery because it needs a primary key column
		"show index from test_table_01": {
			Fields:       mysqlconn.ShowIndexFromTableFields,
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				mysqlconn.ShowIndexFromTableRow("test_table_01", true, "PRIMARY", 1, "pk", false),
			},
		},
		"show index from test_table_02": {
			Fields:       mysqlconn.ShowIndexFromTableFields,
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				mysqlconn.ShowIndexFromTableRow("test_table_02", true, "PRIMARY", 1, "pk", false),
			},
		},
		"show index from test_table_03": {
			Fields:       mysqlconn.ShowIndexFromTableFields,
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				mysqlconn.ShowIndexFromTableRow("test_table_03", true, "PRIMARY", 1, "pk", false),
			},
		},
		"begin":  {},
		"commit": {},
	}
}

func handleAndVerifyTabletError(t *testing.T, msg string, tabletErrCode vtrpcpb.ErrorCode) {
	err := recover()
	if err == nil {
		t.Fatalf(msg)
	}
	verifyTabletError(t, err, tabletErrCode)
}

func verifyTabletError(t *testing.T, err interface{}, tabletErrCode vtrpcpb.ErrorCode) {
	tabletError, ok := err.(*TabletError)
	if !ok {
		t.Fatalf("should return a TabletError, but got err: %v", err)
	}
	if tabletError.ErrorCode != tabletErrCode {
		t.Fatalf("got a TabletError with error code %s but wanted %s: %v", tabletError.ErrorCode, tabletErrCode, err)
	}
}
