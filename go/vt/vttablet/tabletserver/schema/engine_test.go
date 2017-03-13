// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package schema

import (
	"expvar"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/mysqlconn"
	"github.com/youtube/vitess/go/mysqlconn/fakesqldb"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/schema/schematest"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/tabletenv"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

func TestOpenFailedDueToMissMySQLTime(t *testing.T) {
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
	se := newEngine(10, 1*time.Second, 1*time.Second, false)
	err := se.Open(db.ConnParams())
	want := "could not get MySQL time"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("se.Open: %v, want %s", err, want)
	}
}

func TestOpenFailedDueToIncorrectMysqlRowNum(t *testing.T) {
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
	se := newEngine(10, 1*time.Second, 1*time.Second, false)
	err := se.Open(db.ConnParams())
	want := "unexpected result for MySQL time"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("se.Open: %v, want %s", err, want)
	}
}

func TestOpenFailedDueToInvalidTimeFormat(t *testing.T) {
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
	se := newEngine(10, 1*time.Second, 1*time.Second, false)
	err := se.Open(db.ConnParams())
	want := "could not parse time"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("se.Open: %v, want %s", err, want)
	}
}

func TestOpenFailedDueToExecErr(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	for query, result := range schematest.Queries() {
		db.AddQuery(query, result)
	}
	db.AddRejectedQuery(mysqlconn.BaseShowTables, fmt.Errorf("injected error"))
	se := newEngine(10, 1*time.Second, 1*time.Second, false)
	err := se.Open(db.ConnParams())
	want := "could not get table list"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("se.Open: %v, want %s", err, want)
	}
}

func TestOpenFailedDueToTableErr(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	for query, result := range schematest.Queries() {
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
		// this will cause NewTable error, as it expects zero rows.
		Fields: []*querypb.Field{
			{
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: [][]sqltypes.Value{
			{sqltypes.MakeString([]byte(""))},
		},
	})
	se := newEngine(10, 1*time.Second, 1*time.Second, false)
	err := se.Open(db.ConnParams())
	want := "could not get schema for any tables"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("se.Open: %v, want %s", err, want)
	}
}

func TestReload(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	ctx := context.Background()
	for query, result := range schematest.Queries() {
		db.AddQuery(query, result)
	}
	idleTimeout := 10 * time.Second
	se := newEngine(10, 10*time.Second, idleTimeout, true)
	se.Open(db.ConnParams())
	defer se.Close()
	// this new table does not exist
	newTable := sqlparser.NewTableIdent("test_table_04")
	table := se.GetTable(newTable)
	if table != nil {
		t.Fatalf("table: %s exists; expecting nil", newTable)
	}
	se.Reload(ctx)
	table = se.GetTable(newTable)
	if table != nil {
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

	se.Reload(ctx)
	table = se.GetTable(newTable)
	if table != nil {
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
	table = se.GetTable(newTable)
	if table != nil {
		t.Fatalf("table: %s exists; expecting nil", newTable)
	}
	if err := se.Reload(ctx); err != nil {
		t.Fatalf("se.Reload() error: %v", err)
	}
	table = se.GetTable(newTable)
	if table == nil {
		t.Fatalf("table: %s should exist", newTable)
	}
}

func TestCreateOrUpdateTableFailedDuetoExecErr(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	for query, result := range schematest.Queries() {
		db.AddQuery(query, result)
	}
	db.AddRejectedQuery(mysqlconn.BaseShowTablesForTable("test_table"), fmt.Errorf("forced fail"))
	se := newEngine(10, 1*time.Second, 1*time.Second, false)
	se.Open(db.ConnParams())
	defer se.Close()
	originalSchemaErrorCount := tabletenv.InternalErrors.Counts()["Schema"]
	// should silently fail: no errors returned, but increment a counter
	se.TableWasCreatedOrAltered(context.Background(), "test_table")

	newSchemaErrorCount := tabletenv.InternalErrors.Counts()["Schema"]
	schemaErrorDiff := newSchemaErrorCount - originalSchemaErrorCount
	if schemaErrorDiff != 1 {
		t.Errorf("InternalErrors.Schema counter should have increased by 1, instead got %v", schemaErrorDiff)
	}
}

func TestCreateOrUpdateTable(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	for query, result := range schematest.Queries() {
		db.AddQuery(query, result)
	}
	se := newEngine(10, 1*time.Second, 1*time.Second, false)
	se.Open(db.ConnParams())
	defer se.Close()
	existingTable := "test_table_01"
	db.AddQuery(mysqlconn.BaseShowTablesForTable(existingTable), &sqltypes.Result{
		Fields:       mysqlconn.BaseShowTablesFields,
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			mysqlconn.BaseShowTablesRow(existingTable, false, ""),
		},
	})
	i := 0
	se.RegisterNotifier("test", func(schema map[string]*Table, created, altered, dropped []string) {
		switch i {
		case 0:
			if len(created) != 4 {
				t.Errorf("callback 0: %v, want len of 4\n", created)
			}
		case 1:
			want := []string{"test_table_01"}
			if !reflect.DeepEqual(altered, want) {
				t.Errorf("callback 0: %v, want %v\n", created, want)
			}
		default:
			t.Fatal("unexpected")
		}
		i++
	})
	defer se.UnregisterNotifier("test")
	if err := se.TableWasCreatedOrAltered(context.Background(), "test_table_01"); err != nil {
		t.Fatal(err)
	}
	if i < 2 {
		t.Error("Notifier did not get called")
	}
}

func TestExportVars(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	for query, result := range schematest.Queries() {
		db.AddQuery(query, result)
	}
	se := newEngine(10, 1*time.Second, 1*time.Second, true)
	se.Open(db.ConnParams())
	defer se.Close()
	expvar.Do(func(kv expvar.KeyValue) {
		_ = kv.Value.String()
	})
}

func TestUpdatedMysqlStats(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	ctx := context.Background()
	for query, result := range schematest.Queries() {
		db.AddQuery(query, result)
	}
	idleTimeout := 10 * time.Second
	se := newEngine(10, 10*time.Second, idleTimeout, true)
	se.Open(db.ConnParams())
	defer se.Close()
	// Add new table
	tableName := sqlparser.NewTableIdent("mysql_stats_test_table")
	db.AddQuery(mysqlconn.BaseShowTables, &sqltypes.Result{
		Fields:       mysqlconn.BaseShowTablesFields,
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			mysqlconn.BaseShowTablesRow(tableName.String(), false, ""),
		},
	})
	// Add queries necessary for TableWasCreatedOrAltered() and NewTable()
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

	if err := se.Reload(ctx); err != nil {
		t.Fatalf("se.Reload() error: %v", err)
	}
	table := se.GetTable(tableName)
	if table == nil {
		t.Fatalf("table: %s should exist", tableName)
	}
	tr1 := table.TableRows
	dl1 := table.DataLength
	il1 := table.IndexLength
	df1 := table.DataFree
	mdl1 := table.MaxDataLength
	// Update existing table with new stats.
	row := mysqlconn.BaseShowTablesRow(tableName.String(), false, "")
	row[2] = sqltypes.MakeTrusted(sqltypes.Uint64, []byte("0")) // smaller timestamp
	row[4] = sqltypes.MakeTrusted(sqltypes.Uint64, []byte("2")) // table_rows
	row[5] = sqltypes.MakeTrusted(sqltypes.Uint64, []byte("3")) // data_length
	row[6] = sqltypes.MakeTrusted(sqltypes.Uint64, []byte("4")) // index_length
	row[7] = sqltypes.MakeTrusted(sqltypes.Uint64, []byte("5")) // data_free
	row[8] = sqltypes.MakeTrusted(sqltypes.Uint64, []byte("6")) // max_data_length
	db.AddQuery(mysqlconn.BaseShowTables, &sqltypes.Result{
		Fields:       mysqlconn.BaseShowTablesFields,
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			row,
		},
	})
	if err := se.Reload(ctx); err != nil {
		t.Fatalf("se.Reload() error: %v", err)
	}
	table = se.GetTable(tableName)
	tr2 := table.TableRows
	dl2 := table.DataLength
	il2 := table.IndexLength
	df2 := table.DataFree
	mdl2 := table.MaxDataLength
	if tr1 == tr2 || dl1 == dl2 || il1 == il2 || df1 == df2 || mdl1 == mdl2 {
		t.Logf("%v==%v %v==%v %v==%v %v==%v %v==%v", tr1, tr2, dl1, dl2, il1, il2, df1, df2, mdl1, mdl2)
		t.Fatalf("MysqlStats() results failed to change between queries. ")
	}
}

func TestStatsURL(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	for query, result := range schematest.Queries() {
		db.AddQuery(query, result)
	}
	se := newEngine(10, 1*time.Second, 1*time.Second, true)
	se.Open(db.ConnParams())
	defer se.Close()

	request, _ := http.NewRequest("GET", "/debug/schema", nil)
	response := httptest.NewRecorder()
	se.ServeHTTP(response, request)
}

type dummyChecker struct {
}

func (dummyChecker) CheckMySQL() {}

var DummyChecker = dummyChecker{}

func newEngine(queryCacheSize int, reloadTime time.Duration, idleTimeout time.Duration, strict bool) *Engine {
	config := tabletenv.DefaultQsConfig
	config.QueryCacheSize = queryCacheSize
	config.SchemaReloadTime = float64(reloadTime) / 1e9
	config.IdleTimeout = float64(idleTimeout) / 1e9
	config.StrictMode = strict
	return NewEngine(DummyChecker, config)
}
