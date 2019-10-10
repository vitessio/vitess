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

package schema

import (
	"expvar"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema/schematest"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	querypb "vitess.io/vitess/go/vt/proto/query"
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
			{sqltypes.NewVarBinary("1427325875")},
			{sqltypes.NewVarBinary("1427325875")},
		},
	})
	se := newEngine(10, 1*time.Second, 1*time.Second, false, db)
	err := se.Open()
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
	se := newEngine(10, 1*time.Second, 1*time.Second, false, db)
	err := se.Open()
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
			{sqltypes.NewVarBinary("invalid_time")},
		},
	})
	se := newEngine(10, 1*time.Second, 1*time.Second, false, db)
	err := se.Open()
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
	db.AddRejectedQuery(mysql.BaseShowTables, fmt.Errorf("injected error"))
	se := newEngine(10, 1*time.Second, 1*time.Second, false, db)
	err := se.Open()
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
	db.AddQuery(mysql.BaseShowTables, &sqltypes.Result{
		Fields:       mysql.BaseShowTablesFields,
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			mysql.BaseShowTablesRow("test_table", false, ""),
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
			{sqltypes.NewVarBinary("")},
		},
	})
	se := newEngine(10, 1*time.Second, 1*time.Second, false, db)
	err := se.Open()
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
	se := newEngine(10, 10*time.Second, idleTimeout, true, db)
	se.Open()
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
	db.AddQuery(mysql.BaseShowTables, &sqltypes.Result{
		// make this query return nothing during reload
		Fields: mysql.BaseShowTablesFields,
	})
	db.AddQuery(mysql.BaseShowTablesForTable(newTable.String()), &sqltypes.Result{
		Fields:       mysql.BaseShowTablesFields,
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			mysql.BaseShowTablesRow(newTable.String(), false, ""),
		},
	})

	db.AddQuery("select * from test_table_04 where 1 != 1", &sqltypes.Result{
		Fields: []*querypb.Field{{
			Name: "pk",
			Type: sqltypes.Int32,
		}},
	})
	db.AddQuery("describe test_table_04", &sqltypes.Result{
		Fields:       mysql.DescribeTableFields,
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			mysql.DescribeTableRow("pk", "int(11)", false, "PRI", "0"),
		},
	})
	db.AddQuery("show index from test_table_04", &sqltypes.Result{
		Fields:       mysql.ShowIndexFromTableFields,
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			mysql.ShowIndexFromTableRow("test_table_04", true, "PRIMARY", 1, "pk", false),
		},
	})

	se.Reload(ctx)
	table = se.GetTable(newTable)
	if table != nil {
		t.Fatalf("table: %s exists; expecting nil", newTable)
	}

	// test reload with new table: test_table_04
	db.AddQuery(mysql.BaseShowTables, &sqltypes.Result{
		Fields:       mysql.BaseShowTablesFields,
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			mysql.BaseShowTablesRow(newTable.String(), false, ""),
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
	db.AddRejectedQuery(mysql.BaseShowTablesForTable("test_table"), fmt.Errorf("forced fail"))
	se := newEngine(10, 1*time.Second, 1*time.Second, false, db)
	se.Open()
	defer se.Close()
	originalSchemaErrorCount := tabletenv.InternalErrors.Counts()["Schema"]
	// should silently fail: no errors returned, but increment a counter
	se.tableWasCreatedOrAltered(context.Background(), "test_table")

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
	se := newEngine(10, 1*time.Second, 1*time.Second, false, db)
	se.Open()
	defer se.Close()
	existingTable := "test_table_01"
	db.AddQuery(mysql.BaseShowTablesForTable(existingTable), &sqltypes.Result{
		Fields:       mysql.BaseShowTablesFields,
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			mysql.BaseShowTablesRow(existingTable, false, ""),
		},
	})

	wasCreated, err := se.tableWasCreatedOrAltered(context.Background(), existingTable)
	if err != nil {
		t.Fatal(err)
	}
	if wasCreated {
		t.Error("wanted wasCreated == false")
	}
}

func TestExportVars(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	for query, result := range schematest.Queries() {
		db.AddQuery(query, result)
	}
	se := newEngine(10, 1*time.Second, 1*time.Second, true, db)
	se.Open()
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
	se := newEngine(10, 10*time.Second, idleTimeout, true, db)
	se.Open()
	defer se.Close()
	// Add new table
	tableName := sqlparser.NewTableIdent("mysql_stats_test_table")
	db.AddQuery(mysql.BaseShowTables, &sqltypes.Result{
		Fields:       mysql.BaseShowTablesFields,
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			mysql.BaseShowTablesRow(tableName.String(), false, ""),
		},
	})
	// Add queries necessary for tableWasCreatedOrAltered() and NewTable()
	db.AddQuery(mysql.BaseShowTablesForTable(tableName.String()), &sqltypes.Result{
		Fields:       mysql.BaseShowTablesFields,
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			mysql.BaseShowTablesRow(tableName.String(), false, ""),
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
		Fields:       mysql.DescribeTableFields,
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			mysql.DescribeTableRow("pk", "int(11)", false, "PRI", "0"),
		},
	})
	q = fmt.Sprintf("show index from %s", tableName)
	db.AddQuery(q, &sqltypes.Result{
		Fields:       mysql.ShowIndexFromTableFields,
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			mysql.ShowIndexFromTableRow(tableName.String(), true, "PRIMARY", 1, "pk", false),
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
	row := mysql.BaseShowTablesRow(tableName.String(), false, "")
	row[2] = sqltypes.NewUint64(0) // smaller timestamp
	row[4] = sqltypes.NewUint64(2) // table_rows
	row[5] = sqltypes.NewUint64(3) // data_length
	row[6] = sqltypes.NewUint64(4) // index_length
	row[7] = sqltypes.NewUint64(5) // data_free
	row[8] = sqltypes.NewUint64(6) // max_data_length
	db.AddQuery(mysql.BaseShowTables, &sqltypes.Result{
		Fields:       mysql.BaseShowTablesFields,
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
	se := newEngine(10, 1*time.Second, 1*time.Second, true, db)
	se.Open()
	defer se.Close()

	request, _ := http.NewRequest("GET", "/debug/schema", nil)
	response := httptest.NewRecorder()
	se.ServeHTTP(response, request)
}

type dummyChecker struct {
}

func (dummyChecker) CheckMySQL() {}

var DummyChecker = dummyChecker{}

func newEngine(queryPlanCacheSize int, reloadTime time.Duration, idleTimeout time.Duration, strict bool, db *fakesqldb.DB) *Engine {
	config := tabletenv.DefaultQsConfig
	config.QueryPlanCacheSize = queryPlanCacheSize
	config.SchemaReloadTime = float64(reloadTime) / 1e9
	config.IdleTimeout = float64(idleTimeout) / 1e9
	se := NewEngine(DummyChecker, config)
	se.InitDBConfig(newDBConfigs(db).DbaWithDB())
	return se
}

func newDBConfigs(db *fakesqldb.DB) *dbconfigs.DBConfigs {
	return dbconfigs.NewTestDBConfigs(*db.ConnParams(), *db.ConnParams(), "")
}
