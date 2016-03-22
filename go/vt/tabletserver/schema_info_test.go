// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"expvar"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/sqltypes"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
	"github.com/youtube/vitess/go/vt/schema"
	"github.com/youtube/vitess/go/vt/tabletserver/fakecacheservice"
	"github.com/youtube/vitess/go/vt/vttest/fakesqldb"
	"golang.org/x/net/context"
)

func TestSchemaInfoStrictMode(t *testing.T) {
	fakecacheservice.Register()
	db := fakesqldb.Register()
	for query, result := range getSchemaInfoBaseTestQueries() {
		db.AddQuery(query, result)
	}
	schemaInfo := newTestSchemaInfo(10, 1*time.Second, 1*time.Second, false)
	t.Log(schemaInfo)
	appParams := sqldb.ConnParams{Engine: db.Name}
	dbaParams := sqldb.ConnParams{Engine: db.Name}
	schemaInfo.cachePool.Open()
	defer schemaInfo.cachePool.Close()
	defer handleAndVerifyTabletError(
		t,
		"schema info Open should fail because of underlying "+
			"connection cannot verify strict mode",
		ErrFatal,
	)
	schemaInfo.Open(&appParams, &dbaParams, []SchemaOverride{}, true)
}

func TestSchemaInfoOpenFailedDueToMissMySQLTime(t *testing.T) {
	fakecacheservice.Register()
	db := fakesqldb.Register()
	db.AddQuery("select unix_timestamp()", &sqltypes.Result{
		// make this query fail
		RowsAffected: math.MaxUint64,
		Rows: [][]sqltypes.Value{
			{sqltypes.MakeString([]byte("1427325875"))},
		},
	})
	schemaInfo := newTestSchemaInfo(10, 1*time.Second, 1*time.Second, false)
	appParams := sqldb.ConnParams{Engine: db.Name}
	dbaParams := sqldb.ConnParams{Engine: db.Name}
	schemaInfo.cachePool.Open()
	defer schemaInfo.cachePool.Close()
	defer handleAndVerifyTabletError(
		t,
		"schema info Open should fail because of it could not get MySQL time",
		ErrFail,
	)
	schemaInfo.Open(&appParams, &dbaParams, []SchemaOverride{}, false)
}

func TestSchemaInfoOpenFailedDueToIncorrectMysqlRowNum(t *testing.T) {
	fakecacheservice.Register()
	db := fakesqldb.Register()
	db.AddQuery("select unix_timestamp()", &sqltypes.Result{
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			// make this query fail
			nil,
		},
	})
	schemaInfo := newTestSchemaInfo(10, 1*time.Second, 1*time.Second, false)
	appParams := sqldb.ConnParams{Engine: db.Name}
	dbaParams := sqldb.ConnParams{Engine: db.Name}
	schemaInfo.cachePool.Open()
	defer schemaInfo.cachePool.Close()
	defer handleAndVerifyTabletError(
		t,
		"schema info Open should fail because of incorrect MySQL row number",
		ErrFail,
	)
	schemaInfo.Open(&appParams, &dbaParams, []SchemaOverride{}, false)
}

func TestSchemaInfoOpenFailedDueToInvalidTimeFormat(t *testing.T) {
	fakecacheservice.Register()
	db := fakesqldb.Register()
	db.AddQuery("select unix_timestamp()", &sqltypes.Result{
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			// make safety check fail, invalid time format
			{sqltypes.MakeString([]byte("invalid_time"))},
		},
	})
	schemaInfo := newTestSchemaInfo(10, 1*time.Second, 1*time.Second, false)
	appParams := sqldb.ConnParams{Engine: db.Name}
	dbaParams := sqldb.ConnParams{Engine: db.Name}
	schemaInfo.cachePool.Open()
	defer schemaInfo.cachePool.Close()
	defer handleAndVerifyTabletError(
		t,
		"schema info Open should fail because it could not get MySQL time",
		ErrFail,
	)
	schemaInfo.Open(&appParams, &dbaParams, []SchemaOverride{}, false)
}

func TestSchemaInfoOpenFailedDueToExecErr(t *testing.T) {
	fakecacheservice.Register()
	db := fakesqldb.Register()
	for query, result := range getSchemaInfoBaseTestQueries() {
		db.AddQuery(query, result)
	}
	db.AddQuery(baseShowTables, &sqltypes.Result{
		// this will cause connection failed to execute baseShowTables query
		RowsAffected: math.MaxUint64,
	})
	schemaInfo := newTestSchemaInfo(10, 1*time.Second, 1*time.Second, false)
	appParams := sqldb.ConnParams{Engine: db.Name}
	dbaParams := sqldb.ConnParams{Engine: db.Name}
	schemaInfo.cachePool.Open()
	defer schemaInfo.cachePool.Close()
	defer handleAndVerifyTabletError(
		t,
		"schema info Open should fail because conn.Exec failed",
		ErrFail,
	)
	schemaInfo.Open(&appParams, &dbaParams, []SchemaOverride{}, false)
}

func TestSchemaInfoOpenFailedDueToTableInfoErr(t *testing.T) {
	fakecacheservice.Register()
	db := fakesqldb.Register()
	for query, result := range getSchemaInfoBaseTestQueries() {
		db.AddQuery(query, result)
	}
	db.AddQuery(baseShowTables, &sqltypes.Result{
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			createTestTableBaseShowTable("test_table"),
		},
	})
	db.AddQuery("select * from `test_table` where 1 != 1", &sqltypes.Result{
		// this will cause NewTableInfo error
		RowsAffected: math.MaxUint64,
	})
	schemaInfo := newTestSchemaInfo(10, 1*time.Second, 1*time.Second, false)
	appParams := sqldb.ConnParams{Engine: db.Name}
	dbaParams := sqldb.ConnParams{Engine: db.Name}
	schemaInfo.cachePool.Open()
	defer schemaInfo.cachePool.Close()
	defer handleAndVerifyTabletError(
		t,
		"schema info Open should fail because NewTableInfo failed",
		ErrFail,
	)
	schemaInfo.Open(&appParams, &dbaParams, []SchemaOverride{}, false)
}

func TestSchemaInfoOpenWithSchemaOverride(t *testing.T) {
	fakecacheservice.Register()
	db := fakesqldb.Register()
	for query, result := range getSchemaInfoTestSupportedQueries() {
		db.AddQuery(query, result)
	}
	schemaInfo := newTestSchemaInfo(10, 10*time.Second, 10*time.Second, false)
	appParams := sqldb.ConnParams{Engine: db.Name}
	dbaParams := sqldb.ConnParams{Engine: db.Name}
	schemaInfo.cachePool.Open()
	defer schemaInfo.cachePool.Close()
	schemaOverrides := getSchemaInfoTestSchemaOverride()
	// test cache type RW
	schemaInfo.Open(&appParams, &dbaParams, schemaOverrides, true)
	testTableInfo := schemaInfo.GetTable("test_table_01")
	if testTableInfo.Table.Type != schema.CacheRW {
		t.Fatalf("test_table_01's cache type should be RW")
	}
	schemaInfo.Close()
	// test cache type W
	schemaInfo.Open(&appParams, &dbaParams, schemaOverrides, true)
	testTableInfo = schemaInfo.GetTable("test_table_02")
	if testTableInfo.Table.Type != schema.CacheW {
		t.Fatalf("test_table_02's cache type should be W")
	}
	schemaInfo.Close()
}

func TestSchemaInfoReload(t *testing.T) {
	fakecacheservice.Register()
	db := fakesqldb.Register()
	for query, result := range getSchemaInfoTestSupportedQueries() {
		db.AddQuery(query, result)
	}
	idleTimeout := 10 * time.Second
	schemaInfo := newTestSchemaInfo(10, 10*time.Second, idleTimeout, false)
	appParams := sqldb.ConnParams{Engine: db.Name}
	dbaParams := sqldb.ConnParams{Engine: db.Name}
	schemaInfo.cachePool.Open()
	defer schemaInfo.cachePool.Close()
	// test cache type RW
	schemaInfo.Open(&appParams, &dbaParams, nil, true)
	defer schemaInfo.Close()
	// this new table does not exist
	newTable := "test_table_04"
	tableInfo := schemaInfo.GetTable(newTable)
	if tableInfo != nil {
		t.Fatalf("table: %s exists; expecting nil", newTable)
	}
	schemaInfo.Reload()
	tableInfo = schemaInfo.GetTable(newTable)
	if tableInfo != nil {
		t.Fatalf("table: %s exists; expecting nil", newTable)
	}
	db.AddQuery(baseShowTables, &sqltypes.Result{
		// make this query fail during reload
		RowsAffected: math.MaxUint64,
		Rows: [][]sqltypes.Value{
			createTestTableBaseShowTable(newTable),
		},
	})

	createOrDropTableQuery := fmt.Sprintf("%s and table_name = '%s'", baseShowTables, newTable)
	db.AddQuery(createOrDropTableQuery, &sqltypes.Result{
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			createTestTableBaseShowTable(newTable),
		},
	})

	db.AddQuery("select * from `test_table_04` where 1 != 1", &sqltypes.Result{
		Fields: []*querypb.Field{{
			Name: "pk",
			Type: sqltypes.Int32,
		}},
	})
	db.AddQuery("describe `test_table_04`", &sqltypes.Result{
		RowsAffected: 1,
		Rows:         [][]sqltypes.Value{createTestTableDescribe("pk")},
	})
	db.AddQuery("show index from `test_table_04`", &sqltypes.Result{
		RowsAffected: 1,
		Rows:         [][]sqltypes.Value{createTestTableShowIndex("pk")},
	})

	schemaInfo.Reload()
	tableInfo = schemaInfo.GetTable(newTable)
	if tableInfo != nil {
		t.Fatalf("table: %s exists; expecting nil", newTable)
	}

	// test reload with new table: test_table_04
	db.AddQuery(baseShowTables, &sqltypes.Result{
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			createTestTableBaseShowTable(newTable),
		},
	})
	tableInfo = schemaInfo.GetTable(newTable)
	if tableInfo != nil {
		t.Fatalf("table: %s exists; expecting nil", newTable)
	}
	schemaInfo.Reload()
	tableInfo = schemaInfo.GetTable(newTable)
	if tableInfo == nil {
		t.Fatalf("table: %s should exist", newTable)
	}
}

func TestSchemaInfoCreateOrUpdateTableFailedDuetoExecErr(t *testing.T) {
	fakecacheservice.Register()
	db := fakesqldb.Register()
	for query, result := range getSchemaInfoTestSupportedQueries() {
		db.AddQuery(query, result)
	}
	createOrDropTableQuery := fmt.Sprintf("%s and table_name = '%s'", baseShowTables, "test_table")
	db.AddQuery(createOrDropTableQuery, &sqltypes.Result{
		// make this query fail
		RowsAffected: math.MaxUint64,
		Rows: [][]sqltypes.Value{
			createTestTableBaseShowTable("test_table"),
		},
	})
	schemaInfo := newTestSchemaInfo(10, 1*time.Second, 1*time.Second, true)
	appParams := sqldb.ConnParams{Engine: db.Name}
	dbaParams := sqldb.ConnParams{Engine: db.Name}
	schemaInfo.cachePool.Open()
	defer schemaInfo.cachePool.Close()
	schemaInfo.Open(&appParams, &dbaParams, getSchemaInfoTestSchemaOverride(), false)
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
	fakecacheservice.Register()
	db := fakesqldb.Register()
	for query, result := range getSchemaInfoTestSupportedQueries() {
		db.AddQuery(query, result)
	}
	existingTable := "test_table_01"
	createOrDropTableQuery := fmt.Sprintf("%s and table_name = '%s'", baseShowTables, existingTable)
	db.AddQuery(createOrDropTableQuery, &sqltypes.Result{
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			createTestTableBaseShowTable(existingTable),
		},
	})
	schemaInfo := newTestSchemaInfo(10, 1*time.Second, 1*time.Second, false)
	appParams := sqldb.ConnParams{Engine: db.Name}
	dbaParams := sqldb.ConnParams{Engine: db.Name}
	schemaInfo.cachePool.Open()
	defer schemaInfo.cachePool.Close()
	schemaInfo.Open(&appParams, &dbaParams, getSchemaInfoTestSchemaOverride(), false)
	schemaInfo.CreateOrUpdateTable(context.Background(), "test_table_01")
	schemaInfo.Close()
}

func TestSchemaInfoDropTable(t *testing.T) {
	fakecacheservice.Register()
	db := fakesqldb.Register()
	for query, result := range getSchemaInfoTestSupportedQueries() {
		db.AddQuery(query, result)
	}
	existingTable := "test_table_01"
	createOrDropTableQuery := fmt.Sprintf("%s and table_name = '%s'", baseShowTables, existingTable)
	db.AddQuery(createOrDropTableQuery, &sqltypes.Result{
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			createTestTableBaseShowTable(existingTable),
		},
	})
	schemaInfo := newTestSchemaInfo(10, 1*time.Second, 1*time.Second, false)
	appParams := sqldb.ConnParams{Engine: db.Name}
	dbaParams := sqldb.ConnParams{Engine: db.Name}
	schemaInfo.cachePool.Open()
	defer schemaInfo.cachePool.Close()
	schemaInfo.Open(&appParams, &dbaParams, getSchemaInfoTestSchemaOverride(), false)
	tableInfo := schemaInfo.GetTable(existingTable)
	if tableInfo == nil {
		t.Fatalf("table: %s should exist", existingTable)
	}
	schemaInfo.DropTable(existingTable)
	tableInfo = schemaInfo.GetTable(existingTable)
	if tableInfo != nil {
		t.Fatalf("table: %s should not exist", existingTable)
	}
	schemaInfo.Close()
}

func TestSchemaInfoGetPlanPanicDuetoEmptyQuery(t *testing.T) {
	fakecacheservice.Register()
	db := fakesqldb.Register()
	for query, result := range getSchemaInfoTestSupportedQueries() {
		db.AddQuery(query, result)
	}
	schemaInfo := newTestSchemaInfo(10, 10*time.Second, 10*time.Second, false)
	appParams := sqldb.ConnParams{Engine: db.Name}
	dbaParams := sqldb.ConnParams{Engine: db.Name}
	schemaInfo.cachePool.Open()
	defer schemaInfo.cachePool.Close()
	schemaOverrides := getSchemaInfoTestSchemaOverride()
	// test cache type RW
	schemaInfo.Open(&appParams, &dbaParams, schemaOverrides, true)
	defer schemaInfo.Close()

	ctx := context.Background()
	logStats := newLogStats("GetPlanStats", ctx)
	defer handleAndVerifyTabletError(
		t,
		"schema info GetPlan should fail because of empty query",
		ErrFail,
	)
	schemaInfo.GetPlan(ctx, logStats, "")
}

func TestSchemaInfoQueryCacheFailDueToInvalidCacheSize(t *testing.T) {
	fakecacheservice.Register()
	db := fakesqldb.Register()
	for query, result := range getSchemaInfoTestSupportedQueries() {
		db.AddQuery(query, result)
	}
	schemaInfo := newTestSchemaInfo(10, 10*time.Second, 10*time.Second, false)
	appParams := sqldb.ConnParams{Engine: db.Name}
	dbaParams := sqldb.ConnParams{Engine: db.Name}
	schemaInfo.cachePool.Open()
	defer schemaInfo.cachePool.Close()
	schemaOverrides := getSchemaInfoTestSchemaOverride()
	// test cache type RW
	schemaInfo.Open(&appParams, &dbaParams, schemaOverrides, true)
	defer schemaInfo.Close()
	defer handleAndVerifyTabletError(
		t,
		"schema info SetQueryCacheSize should use a positive size",
		ErrFail,
	)
	schemaInfo.SetQueryCacheCap(0)
}

func TestSchemaInfoQueryCache(t *testing.T) {
	fakecacheservice.Register()
	db := fakesqldb.Register()
	for query, result := range getSchemaInfoTestSupportedQueries() {
		db.AddQuery(query, result)
	}

	firstQuery := "select * from test_table_01"
	secondQuery := "select * from test_table_02"
	db.AddQuery("select * from test_table_01 where 1 != 1", &sqltypes.Result{})
	db.AddQuery("select * from test_table_02 where 1 != 1", &sqltypes.Result{})

	schemaInfo := newTestSchemaInfo(10, 10*time.Second, 10*time.Second, true)
	appParams := sqldb.ConnParams{Engine: db.Name}
	dbaParams := sqldb.ConnParams{Engine: db.Name}
	schemaInfo.cachePool.Open()
	defer schemaInfo.cachePool.Close()
	schemaOverrides := getSchemaInfoTestSchemaOverride()
	// test cache type RW
	schemaInfo.Open(&appParams, &dbaParams, schemaOverrides, true)
	defer schemaInfo.Close()

	ctx := context.Background()
	logStats := newLogStats("GetPlanStats", ctx)
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
	fakecacheservice.Register()
	db := fakesqldb.Register()
	for query, result := range getSchemaInfoTestSupportedQueries() {
		db.AddQuery(query, result)
	}
	schemaInfo := newTestSchemaInfo(10, 1*time.Second, 1*time.Second, true)
	appParams := sqldb.ConnParams{Engine: db.Name}
	dbaParams := sqldb.ConnParams{Engine: db.Name}
	schemaInfo.cachePool.Open()
	defer schemaInfo.cachePool.Close()
	schemaInfo.Open(&appParams, &dbaParams, []SchemaOverride{}, true)
	defer schemaInfo.Close()
	expvar.Do(func(kv expvar.KeyValue) {
		_ = kv.Value.String()
	})
}

func TestUpdatedMysqlStats(t *testing.T) {
	fakecacheservice.Register()
	db := fakesqldb.Register()
	for query, result := range getSchemaInfoTestSupportedQueries() {
		db.AddQuery(query, result)
	}
	idleTimeout := 10 * time.Second
	schemaInfo := newTestSchemaInfo(10, 10*time.Second, idleTimeout, false)
	appParams := sqldb.ConnParams{Engine: db.Name}
	dbaParams := sqldb.ConnParams{Engine: db.Name}
	schemaInfo.cachePool.Open()
	defer schemaInfo.cachePool.Close()
	schemaInfo.Open(&appParams, &dbaParams, nil, true)
	defer schemaInfo.Close()
	// Add new table
	tableName := "mysql_stats_test_table"
	db.AddQuery(baseShowTables, &sqltypes.Result{
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			createTestTableBaseShowTable(tableName),
		},
	})
	// Add queries necessary for CreateOrUpdateTable() and NewTableInfo()
	q := fmt.Sprintf("%s and table_name = '%s'", baseShowTables, tableName)
	db.AddQuery(q, &sqltypes.Result{
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			createTestTableBaseShowTable(tableName),
		},
	})
	q = fmt.Sprintf("select * from `%s` where 1 != 1", tableName)
	db.AddQuery(q, &sqltypes.Result{
		Fields: []*querypb.Field{{
			Name: "pk",
			Type: sqltypes.Int32,
		}},
	})
	q = fmt.Sprintf("describe `%s`", tableName)
	db.AddQuery(q, &sqltypes.Result{
		RowsAffected: 1,
		Rows:         [][]sqltypes.Value{createTestTableDescribe("pk")},
	})
	q = fmt.Sprintf("show index from `%s`", tableName)
	db.AddQuery(q, &sqltypes.Result{
		RowsAffected: 1,
		Rows:         [][]sqltypes.Value{createTestTableShowIndex("pk")},
	})

	schemaInfo.Reload()
	tableInfo := schemaInfo.GetTable(tableName)
	if tableInfo == nil {
		t.Fatalf("table: %s should exist", tableName)
	}
	tr1 := tableInfo.TableRows
	dl1 := tableInfo.DataLength
	il1 := tableInfo.IndexLength
	df1 := tableInfo.DataFree
	// Update existing table with new stats.
	db.AddQuery(baseShowTables, &sqltypes.Result{
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			createTestTableUpdatedStats(tableName),
		},
	})
	schemaInfo.Reload()
	tableInfo = schemaInfo.GetTable(tableName)
	tr2 := tableInfo.TableRows
	dl2 := tableInfo.DataLength
	il2 := tableInfo.IndexLength
	df2 := tableInfo.DataFree
	if tr1 == tr2 || dl1 == dl2 || il1 == il2 || df1 == df2 {
		t.Fatalf("MysqlStats() results failed to change between queries. ")
	}
}

func TestSchemaInfoStatsURL(t *testing.T) {
	fakecacheservice.Register()
	db := fakesqldb.Register()
	for query, result := range getSchemaInfoTestSupportedQueries() {
		db.AddQuery(query, result)
	}
	query := "select * from test_table_01"
	db.AddQuery("select * from test_table_01 where 1 != 1", &sqltypes.Result{})
	schemaInfo := newTestSchemaInfo(10, 1*time.Second, 1*time.Second, false)
	appParams := sqldb.ConnParams{Engine: db.Name}
	dbaParams := sqldb.ConnParams{Engine: db.Name}
	schemaInfo.cachePool.Open()
	defer schemaInfo.cachePool.Close()
	schemaInfo.Open(&appParams, &dbaParams, []SchemaOverride{}, true)
	defer schemaInfo.Close()
	// warm up cache
	ctx := context.Background()
	logStats := newLogStats("GetPlanStats", ctx)
	schemaInfo.GetPlan(ctx, logStats, query)

	request, _ := http.NewRequest("GET", schemaInfo.endpoints[debugQueryPlansKey], nil)
	response := httptest.NewRecorder()
	schemaInfo.ServeHTTP(response, request)

	request, _ = http.NewRequest("GET", schemaInfo.endpoints[debugQueryStatsKey], nil)
	response = httptest.NewRecorder()
	schemaInfo.ServeHTTP(response, request)

	request, _ = http.NewRequest("GET", schemaInfo.endpoints[debugTableStatsKey], nil)
	response = httptest.NewRecorder()
	schemaInfo.ServeHTTP(response, request)

	request, _ = http.NewRequest("GET", schemaInfo.endpoints[debugSchemaKey], nil)
	response = httptest.NewRecorder()
	schemaInfo.ServeHTTP(response, request)

	request, _ = http.NewRequest("GET", "/debug/unknown", nil)
	response = httptest.NewRecorder()
	schemaInfo.ServeHTTP(response, request)
}

func newTestSchemaInfoCachePool(enablePublishStats bool, queryServiceStats *QueryServiceStats) *CachePool {
	rowCacheConfig := RowCacheConfig{
		Binary:      "ls",
		Connections: 100,
	}
	randID := rand.Int63()
	name := fmt.Sprintf("TestCachePool-%d-", randID)
	statsURL := fmt.Sprintf("/debug/cache-%d", randID)
	return NewCachePool(
		name,
		rowCacheConfig,
		1*time.Second,
		statsURL,
		enablePublishStats,
		queryServiceStats,
	)
}

func getSchemaInfoBaseTestQueries() map[string]*sqltypes.Result {
	return map[string]*sqltypes.Result{
		// queries for schema info
		"select unix_timestamp()": {
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				{sqltypes.MakeString([]byte("1427325875"))},
			},
		},
	}
}

func getSchemaInfoTestSchemaOverride() []SchemaOverride {
	return []SchemaOverride{
		{
			Name:      "test_table_01",
			PKColumns: []string{"pk"},
			Cache: &struct {
				Type  string
				Table string
			}{
				Type:  "RW",
				Table: "test_table_01",
			},
		},
		// this should be ignored by schema info due to unknown table
		{
			Name:      "unknown_table",
			PKColumns: []string{"column_01"},
			Cache: &struct {
				Type  string
				Table string
			}{
				Type:  "RW",
				Table: "test_table",
			},
		},
		// this should be ignored by schema info due to invalid primary key column
		{
			Name:      "test_table_01",
			PKColumns: []string{"unknown_column"},
			Cache: &struct {
				Type  string
				Table string
			}{
				Type:  "RW",
				Table: "test_table",
			},
		},
		{
			Name:      "test_table_02",
			PKColumns: []string{"pk"},
			Cache: &struct {
				Type  string
				Table string
			}{
				Type:  "W",
				Table: "test_table_02",
			},
		},
		{
			Name:      "test_table_02",
			PKColumns: []string{"pk"},
			Cache: &struct {
				Type  string
				Table string
			}{
				Type: "W",
				// table is missing
				Table: "",
			},
		},
		{
			Name:      "test_table_02",
			PKColumns: []string{"pk"},
			Cache: &struct {
				Type  string
				Table string
			}{
				Type: "W",
				// table does not exist
				Table: "unknown_table",
			},
		},
		{
			Name:      "test_table_02",
			PKColumns: []string{"pk"},
			Cache: &struct {
				Type  string
				Table string
			}{
				Type: "W",
				// table does not have cache
				Table: "test_table_03",
			},
		},
		{
			Name:      "test_table_02",
			PKColumns: []string{"pk"},
			Cache: &struct {
				Type  string
				Table string
			}{
				// cache type unknown
				Type:  "UNKNOWN",
				Table: "test_table_02",
			},
		},
	}
}

func createTestTableBaseShowTable(tableName string) []sqltypes.Value {
	return []sqltypes.Value{
		sqltypes.MakeString([]byte(tableName)),
		sqltypes.MakeString([]byte("USER TABLE")),
		sqltypes.MakeTrusted(sqltypes.Int32, []byte("1427325875")),
		sqltypes.MakeString([]byte("")),
		sqltypes.MakeTrusted(sqltypes.Int32, []byte("1")),
		sqltypes.MakeTrusted(sqltypes.Int32, []byte("2")),
		sqltypes.MakeTrusted(sqltypes.Int32, []byte("3")),
		sqltypes.MakeTrusted(sqltypes.Int32, []byte("4")),
	}
}

func createTestTableUpdatedStats(tableName string) []sqltypes.Value {
	return []sqltypes.Value{
		sqltypes.MakeString([]byte(tableName)),
		sqltypes.MakeString([]byte("USER TABLE")),
		sqltypes.MakeTrusted(sqltypes.Int32, []byte("0")),
		sqltypes.MakeString([]byte("")),
		sqltypes.MakeTrusted(sqltypes.Int32, []byte("4")),
		sqltypes.MakeTrusted(sqltypes.Int32, []byte("5")),
		sqltypes.MakeTrusted(sqltypes.Int32, []byte("6")),
		sqltypes.MakeTrusted(sqltypes.Int32, []byte("7")),
	}
}

func createTestTableDescribe(pkColumnName string) []sqltypes.Value {
	return []sqltypes.Value{
		sqltypes.MakeString([]byte(pkColumnName)),
		sqltypes.MakeString([]byte("int")),
		sqltypes.MakeString([]byte{}),
		sqltypes.MakeString([]byte{}),
		sqltypes.MakeString([]byte("1")),
		sqltypes.MakeString([]byte{}),
	}
}

func createTestTableShowIndex(pkColumnName string) []sqltypes.Value {
	return []sqltypes.Value{
		sqltypes.MakeString([]byte{}),
		sqltypes.MakeString([]byte{}),
		sqltypes.MakeString([]byte("PRIMARY")),
		sqltypes.MakeString([]byte{}),
		sqltypes.MakeString([]byte(pkColumnName)),
		sqltypes.MakeString([]byte{}),
		sqltypes.MakeString([]byte("300")),
	}
}

func getSchemaInfoTestSupportedQueries() map[string]*sqltypes.Result {
	return map[string]*sqltypes.Result{
		// queries for schema info
		"select unix_timestamp()": {
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				{sqltypes.MakeTrusted(sqltypes.Int32, []byte("1427325875"))},
			},
		},
		"select @@global.sql_mode": {
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				{sqltypes.MakeString([]byte("STRICT_TRANS_TABLES"))},
			},
		},
		baseShowTables: {
			RowsAffected: 3,
			Rows: [][]sqltypes.Value{
				{
					sqltypes.MakeString([]byte("test_table_01")),
					sqltypes.MakeString([]byte("USER TABLE")),
					sqltypes.MakeTrusted(sqltypes.Int32, []byte("1427325875")),
					sqltypes.MakeString([]byte("")),
					sqltypes.MakeTrusted(sqltypes.Int32, []byte("1")),
					sqltypes.MakeTrusted(sqltypes.Int32, []byte("2")),
					sqltypes.MakeTrusted(sqltypes.Int32, []byte("3")),
					sqltypes.MakeTrusted(sqltypes.Int32, []byte("4")),
				},
				{
					sqltypes.MakeString([]byte("test_table_02")),
					sqltypes.MakeString([]byte("USER TABLE")),
					sqltypes.MakeTrusted(sqltypes.Int32, []byte("1427325875")),
					sqltypes.MakeString([]byte("")),
					sqltypes.MakeTrusted(sqltypes.Int32, []byte("1")),
					sqltypes.MakeTrusted(sqltypes.Int32, []byte("2")),
					sqltypes.MakeTrusted(sqltypes.Int32, []byte("3")),
					sqltypes.MakeTrusted(sqltypes.Int32, []byte("4")),
				},
				{
					sqltypes.MakeString([]byte("test_table_03")),
					sqltypes.MakeString([]byte("USER TABLE")),
					sqltypes.MakeTrusted(sqltypes.Int32, []byte("1427325875")),
					sqltypes.MakeString([]byte("")),
					sqltypes.MakeTrusted(sqltypes.Int32, []byte("1")),
					sqltypes.MakeTrusted(sqltypes.Int32, []byte("2")),
					sqltypes.MakeTrusted(sqltypes.Int32, []byte("3")),
					sqltypes.MakeTrusted(sqltypes.Int32, []byte("4")),
				},
			},
		},
		"select * from `test_table_01` where 1 != 1": {
			Fields: []*querypb.Field{{
				Name: "pk",
				Type: sqltypes.Int32,
			}},
		},
		"describe `test_table_01`": {
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				{
					sqltypes.MakeString([]byte("pk")),
					sqltypes.MakeString([]byte("int")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("1")),
					sqltypes.MakeString([]byte{}),
				},
			},
		},
		"select * from `test_table_02` where 1 != 1": {
			Fields: []*querypb.Field{{
				Name: "pk",
				Type: sqltypes.Int32,
			}},
		},
		"describe `test_table_02`": {
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				{
					sqltypes.MakeString([]byte("pk")),
					sqltypes.MakeString([]byte("int")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("1")),
					sqltypes.MakeString([]byte{}),
				},
			},
		},
		"select * from `test_table_03` where 1 != 1": {
			Fields: []*querypb.Field{{
				Name: "pk",
				Type: sqltypes.Int32,
			}},
		},
		"describe `test_table_03`": {
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				{
					sqltypes.MakeString([]byte("pk")),
					sqltypes.MakeString([]byte("int")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("1")),
					sqltypes.MakeString([]byte{}),
				},
			},
		},
		// for SplitQuery because it needs a primary key column
		"show index from `test_table_01`": {
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				{
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("PRIMARY")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("pk")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("300")),
				},
			},
		},
		"show index from `test_table_02`": {
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				{
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("PRIMARY")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("pk")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("300")),
				},
			},
		},
		"show index from `test_table_03`": {
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				{
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("PRIMARY")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("pk")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("300")),
				},
			},
		},
		"begin":  {},
		"commit": {},
	}
}

func handleAndVerifyTabletError(t *testing.T, msg string, tabletErrType int) {
	err := recover()
	if err == nil {
		t.Fatalf(msg)
	}
	verifyTabletError(t, err, tabletErrType)
}

func verifyTabletError(t *testing.T, err interface{}, tabletErrType int) {
	tabletError, ok := err.(*TabletError)
	if !ok {
		t.Fatalf("should return a TabletError, but got err: %v", err)
	}
	if tabletError.ErrorType != tabletErrType {
		t.Fatalf("should return a TabletError with error type: %s", getTabletErrorString(tabletErrType))
	}
}

func getTabletErrorString(tabletErrorType int) string {
	switch tabletErrorType {
	case ErrFail:
		return "ErrFail"
	case ErrRetry:
		return "ErrRetry"
	case ErrFatal:
		return "ErrFatal"
	case ErrTxPoolFull:
		return "ErrTxPoolFull"
	case ErrNotInTx:
		return "ErrNotInTx"
	}
	return ""
}
