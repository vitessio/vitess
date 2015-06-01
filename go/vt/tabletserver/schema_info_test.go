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

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/sqltypes"
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
	appParams := sqldb.ConnParams{}
	dbaParams := sqldb.ConnParams{}
	cachePool := newTestSchemaInfoCachePool(false, schemaInfo.queryServiceStats)
	cachePool.Open()
	defer cachePool.Close()
	defer handleAndVerifyTabletError(
		t,
		"schema info Open should fail because of underlying "+
			"connection cannot verify strict mode",
		ErrFatal,
	)
	schemaInfo.Open(&appParams, &dbaParams, []SchemaOverride{}, cachePool, true)
}

func TestSchemaInfoOpenFailedDueToMissMySQLTime(t *testing.T) {
	fakecacheservice.Register()
	db := fakesqldb.Register()
	db.AddQuery("select unix_timestamp()", &mproto.QueryResult{
		// make this query fail
		RowsAffected: math.MaxUint64,
		Rows: [][]sqltypes.Value{
			[]sqltypes.Value{sqltypes.MakeString([]byte("1427325875"))},
		},
	})
	schemaInfo := newTestSchemaInfo(10, 1*time.Second, 1*time.Second, false)
	appParams := sqldb.ConnParams{}
	dbaParams := sqldb.ConnParams{}
	cachePool := newTestSchemaInfoCachePool(false, schemaInfo.queryServiceStats)
	cachePool.Open()
	defer cachePool.Close()
	defer handleAndVerifyTabletError(
		t,
		"schema info Open should fail because of it could not get MySQL time",
		ErrFail,
	)
	schemaInfo.Open(&appParams, &dbaParams, []SchemaOverride{}, cachePool, false)
}

func TestSchemaInfoOpenFailedDueToIncorrectMysqlRowNum(t *testing.T) {
	fakecacheservice.Register()
	db := fakesqldb.Register()
	db.AddQuery("select unix_timestamp()", &mproto.QueryResult{
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			// make this query fail
			nil,
		},
	})
	schemaInfo := newTestSchemaInfo(10, 1*time.Second, 1*time.Second, false)
	appParams := sqldb.ConnParams{}
	dbaParams := sqldb.ConnParams{}
	cachePool := newTestSchemaInfoCachePool(false, schemaInfo.queryServiceStats)
	cachePool.Open()
	defer cachePool.Close()
	defer handleAndVerifyTabletError(
		t,
		"schema info Open should fail because of incorrect MySQL row number",
		ErrFail,
	)
	schemaInfo.Open(&appParams, &dbaParams, []SchemaOverride{}, cachePool, false)
}

func TestSchemaInfoOpenFailedDueToInvalidTimeFormat(t *testing.T) {
	fakecacheservice.Register()
	db := fakesqldb.Register()
	db.AddQuery("select unix_timestamp()", &mproto.QueryResult{
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			// make safety check fail, invalid time format
			[]sqltypes.Value{sqltypes.MakeString([]byte("invalid_time"))},
		},
	})
	schemaInfo := newTestSchemaInfo(10, 1*time.Second, 1*time.Second, false)
	appParams := sqldb.ConnParams{}
	dbaParams := sqldb.ConnParams{}
	cachePool := newTestSchemaInfoCachePool(false, schemaInfo.queryServiceStats)
	cachePool.Open()
	defer cachePool.Close()
	defer handleAndVerifyTabletError(
		t,
		"schema info Open should fail because it could not get MySQL time",
		ErrFail,
	)
	schemaInfo.Open(&appParams, &dbaParams, []SchemaOverride{}, cachePool, false)
}

func TestSchemaInfoOpenFailedDueToExecErr(t *testing.T) {
	fakecacheservice.Register()
	db := fakesqldb.Register()
	for query, result := range getSchemaInfoBaseTestQueries() {
		db.AddQuery(query, result)
	}
	db.AddQuery(baseShowTables, &mproto.QueryResult{
		// this will cause connection failed to execute baseShowTables query
		RowsAffected: math.MaxUint64,
	})
	schemaInfo := newTestSchemaInfo(10, 1*time.Second, 1*time.Second, false)
	appParams := sqldb.ConnParams{}
	dbaParams := sqldb.ConnParams{}
	cachePool := newTestSchemaInfoCachePool(false, schemaInfo.queryServiceStats)
	cachePool.Open()
	defer cachePool.Close()
	defer handleAndVerifyTabletError(
		t,
		"schema info Open should fail because conn.Exec failed",
		ErrFatal,
	)
	schemaInfo.Open(&appParams, &dbaParams, []SchemaOverride{}, cachePool, false)
}

func TestSchemaInfoOpenFailedDueToTableInfoErr(t *testing.T) {
	fakecacheservice.Register()
	db := fakesqldb.Register()
	for query, result := range getSchemaInfoBaseTestQueries() {
		db.AddQuery(query, result)
	}
	db.AddQuery(baseShowTables, &mproto.QueryResult{
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			createTestTableBaseShowTable("test_table"),
		},
	})
	db.AddQuery("describe `test_table`", &mproto.QueryResult{
		// this will cause NewTableInfo error
		RowsAffected: math.MaxUint64,
	})
	schemaInfo := newTestSchemaInfo(10, 1*time.Second, 1*time.Second, false)
	appParams := sqldb.ConnParams{}
	dbaParams := sqldb.ConnParams{}
	cachePool := newTestSchemaInfoCachePool(false, schemaInfo.queryServiceStats)
	cachePool.Open()
	defer cachePool.Close()
	defer handleAndVerifyTabletError(
		t,
		"schema info Open should fail because NewTableInfo failed",
		ErrFatal,
	)
	schemaInfo.Open(&appParams, &dbaParams, []SchemaOverride{}, cachePool, false)
}

func TestSchemaInfoOpenWithSchemaOverride(t *testing.T) {
	fakecacheservice.Register()
	db := fakesqldb.Register()
	for query, result := range getSchemaInfoTestSupportedQueries() {
		db.AddQuery(query, result)
	}
	schemaInfo := newTestSchemaInfo(10, 10*time.Second, 10*time.Second, false)
	appParams := sqldb.ConnParams{}
	dbaParams := sqldb.ConnParams{}
	cachePool := newTestSchemaInfoCachePool(false, schemaInfo.queryServiceStats)
	cachePool.Open()
	defer cachePool.Close()
	schemaOverrides := getSchemaInfoTestSchemaOverride()
	// test cache type RW
	schemaInfo.Open(&appParams, &dbaParams, schemaOverrides, cachePool, true)
	testTableInfo := schemaInfo.GetTable("test_table_01")
	if testTableInfo.Table.CacheType != schema.CACHE_RW {
		t.Fatalf("test_table_01's cache type should be RW")
	}
	schemaInfo.Close()
	// test cache type W
	schemaInfo.Open(&appParams, &dbaParams, schemaOverrides, cachePool, true)
	testTableInfo = schemaInfo.GetTable("test_table_02")
	if testTableInfo.Table.CacheType != schema.CACHE_W {
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
	appParams := sqldb.ConnParams{}
	dbaParams := sqldb.ConnParams{}
	cachePool := newTestSchemaInfoCachePool(false, schemaInfo.queryServiceStats)
	cachePool.Open()
	defer cachePool.Close()
	// test cache type RW
	schemaInfo.Open(&appParams, &dbaParams, nil, cachePool, true)
	defer schemaInfo.Close()
	// this new table does not exist
	newTable := "test_table_04"
	tableInfo := schemaInfo.GetTable(newTable)
	if tableInfo != nil {
		t.Fatalf("table: %s does not exist", newTable)
	}
	schemaInfo.Reload()
	tableInfo = schemaInfo.GetTable(newTable)
	if tableInfo != nil {
		t.Fatalf("table: %s does not exist", newTable)
	}
	reloadQuery := fmt.Sprintf("%s and unix_timestamp(create_time) >= %v", baseShowTables, schemaInfo.lastChange.Unix())

	db.AddQuery(reloadQuery, &mproto.QueryResult{
		// make this query fail duing reload
		RowsAffected: math.MaxUint64,
		Rows: [][]sqltypes.Value{
			createTestTableBaseShowTable("test_table_04"),
		},
	})

	createOrDropTableQuery := fmt.Sprintf("%s and table_name = '%s'", baseShowTables, newTable)
	db.AddQuery(createOrDropTableQuery, &mproto.QueryResult{
		RowsAffected: 1,
		Rows:         [][]sqltypes.Value{createTestTableDescribe("pk")},
	})

	db.AddQuery("describe `test_table_04`", &mproto.QueryResult{
		RowsAffected: 1,
		Rows:         [][]sqltypes.Value{createTestTableDescribe("pk")},
	})

	db.AddQuery("show index from `test_table_04`", &mproto.QueryResult{
		RowsAffected: 1,
		Rows:         [][]sqltypes.Value{createTestTableShowIndex("pk")},
	})

	schemaInfo.Reload()
	tableInfo = schemaInfo.GetTable(newTable)
	if tableInfo != nil {
		t.Fatalf("table: %s does not exist", newTable)
	}

	// test reload with new table: test_table_04
	db.AddQuery(reloadQuery, &mproto.QueryResult{
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			createTestTableBaseShowTable("test_table_04"),
		},
	})
	tableInfo = schemaInfo.GetTable(newTable)
	if tableInfo != nil {
		t.Fatalf("table: %s does not exist", newTable)
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
	db.AddQuery(createOrDropTableQuery, &mproto.QueryResult{
		// make this query fail
		RowsAffected: math.MaxUint64,
		Rows:         [][]sqltypes.Value{createTestTableDescribe("pk")},
	})
	schemaInfo := newTestSchemaInfo(10, 1*time.Second, 1*time.Second, false)
	appParams := sqldb.ConnParams{}
	dbaParams := sqldb.ConnParams{}
	cachePool := newTestSchemaInfoCachePool(false, schemaInfo.queryServiceStats)
	cachePool.Open()
	defer cachePool.Close()
	defer handleAndVerifyTabletError(
		t,
		"CreateOrUpdateTable should fail because it could not tables from MySQL",
		ErrFail,
	)
	schemaInfo.Open(&appParams, &dbaParams, getSchemaInfoTestSchemaOverride(), cachePool, false)
	defer schemaInfo.Close()
	schemaInfo.CreateOrUpdateTable(context.Background(), "test_table")
}

func TestSchemaInfoCreateOrUpdateTable(t *testing.T) {
	fakecacheservice.Register()
	db := fakesqldb.Register()
	for query, result := range getSchemaInfoTestSupportedQueries() {
		db.AddQuery(query, result)
	}
	existingTable := "test_table_01"
	createOrDropTableQuery := fmt.Sprintf("%s and table_name = '%s'", baseShowTables, existingTable)
	db.AddQuery(createOrDropTableQuery, &mproto.QueryResult{
		RowsAffected: 1,
		Rows:         [][]sqltypes.Value{createTestTableDescribe("pk")},
	})
	schemaInfo := newTestSchemaInfo(10, 1*time.Second, 1*time.Second, false)
	appParams := sqldb.ConnParams{}
	dbaParams := sqldb.ConnParams{}
	cachePool := newTestSchemaInfoCachePool(false, schemaInfo.queryServiceStats)
	cachePool.Open()
	defer cachePool.Close()
	schemaInfo.Open(&appParams, &dbaParams, getSchemaInfoTestSchemaOverride(), cachePool, false)
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
	db.AddQuery(createOrDropTableQuery, &mproto.QueryResult{
		RowsAffected: 1,
		Rows:         [][]sqltypes.Value{createTestTableDescribe("pk")},
	})
	schemaInfo := newTestSchemaInfo(10, 1*time.Second, 1*time.Second, false)
	appParams := sqldb.ConnParams{}
	dbaParams := sqldb.ConnParams{}
	cachePool := newTestSchemaInfoCachePool(false, schemaInfo.queryServiceStats)
	cachePool.Open()
	defer cachePool.Close()
	schemaInfo.Open(&appParams, &dbaParams, getSchemaInfoTestSchemaOverride(), cachePool, false)
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
	appParams := sqldb.ConnParams{}
	dbaParams := sqldb.ConnParams{}
	cachePool := newTestSchemaInfoCachePool(false, schemaInfo.queryServiceStats)
	cachePool.Open()
	defer cachePool.Close()
	schemaOverrides := getSchemaInfoTestSchemaOverride()
	// test cache type RW
	schemaInfo.Open(&appParams, &dbaParams, schemaOverrides, cachePool, true)
	defer schemaInfo.Close()

	ctx := context.Background()
	logStats := newSqlQueryStats("GetPlanStats", ctx)
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
	appParams := sqldb.ConnParams{}
	dbaParams := sqldb.ConnParams{}
	cachePool := newTestSchemaInfoCachePool(false, schemaInfo.queryServiceStats)
	cachePool.Open()
	defer cachePool.Close()
	schemaOverrides := getSchemaInfoTestSchemaOverride()
	// test cache type RW
	schemaInfo.Open(&appParams, &dbaParams, schemaOverrides, cachePool, true)
	defer schemaInfo.Close()
	defer handleAndVerifyTabletError(
		t,
		"schema info SetQueryCacheSize should use a positive size",
		ErrFail,
	)
	schemaInfo.SetQueryCacheSize(0)
}

func TestSchemaInfoQueryCache(t *testing.T) {
	fakecacheservice.Register()
	db := fakesqldb.Register()
	for query, result := range getSchemaInfoTestSupportedQueries() {
		db.AddQuery(query, result)
	}

	firstSqlQuery := "select * from test_table_01"
	secondSqlQuery := "select * from test_table_02"
	db.AddQuery("select * from test_table_01 where 1 != 1", &mproto.QueryResult{})
	db.AddQuery("select * from test_table_02 where 1 != 1", &mproto.QueryResult{})

	schemaInfo := newTestSchemaInfo(10, 10*time.Second, 10*time.Second, true)
	appParams := sqldb.ConnParams{}
	dbaParams := sqldb.ConnParams{}
	cachePool := newTestSchemaInfoCachePool(true, schemaInfo.queryServiceStats)
	cachePool.Open()
	defer cachePool.Close()
	schemaOverrides := getSchemaInfoTestSchemaOverride()
	// test cache type RW
	schemaInfo.Open(&appParams, &dbaParams, schemaOverrides, cachePool, true)
	defer schemaInfo.Close()

	ctx := context.Background()
	logStats := newSqlQueryStats("GetPlanStats", ctx)
	schemaInfo.SetQueryCacheSize(1)
	firstPlan := schemaInfo.GetPlan(ctx, logStats, firstSqlQuery)
	if firstPlan == nil {
		t.Fatalf("plan should not be nil")
	}
	secondPlan := schemaInfo.GetPlan(ctx, logStats, secondSqlQuery)
	if secondPlan == nil {
		t.Fatalf("plan should not be nil")
	}
	expvar.Do(func(kv expvar.KeyValue) {
		kv.Value.String()
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
	appParams := sqldb.ConnParams{}
	dbaParams := sqldb.ConnParams{}
	cachePool := newTestSchemaInfoCachePool(true, schemaInfo.queryServiceStats)
	cachePool.Open()
	defer cachePool.Close()
	schemaInfo.Open(&appParams, &dbaParams, []SchemaOverride{}, cachePool, true)
	defer schemaInfo.Close()
	expvar.Do(func(kv expvar.KeyValue) {
		kv.Value.String()
	})
}

func TestSchemaInfoStatsURL(t *testing.T) {
	fakecacheservice.Register()
	db := fakesqldb.Register()
	for query, result := range getSchemaInfoTestSupportedQueries() {
		db.AddQuery(query, result)
	}
	sqlQuery := "select * from test_table_01"
	db.AddQuery("select * from test_table_01 where 1 != 1", &mproto.QueryResult{})
	schemaInfo := newTestSchemaInfo(10, 1*time.Second, 1*time.Second, false)
	appParams := sqldb.ConnParams{}
	dbaParams := sqldb.ConnParams{}
	cachePool := newTestSchemaInfoCachePool(false, schemaInfo.queryServiceStats)
	cachePool.Open()
	defer cachePool.Close()
	schemaInfo.Open(&appParams, &dbaParams, []SchemaOverride{}, cachePool, true)
	defer schemaInfo.Close()
	// warm up cache
	ctx := context.Background()
	logStats := newSqlQueryStats("GetPlanStats", ctx)
	schemaInfo.GetPlan(ctx, logStats, sqlQuery)

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

func getSchemaInfoBaseTestQueries() map[string]*mproto.QueryResult {
	return map[string]*mproto.QueryResult{
		// queries for schema info
		"select unix_timestamp()": &mproto.QueryResult{
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				[]sqltypes.Value{sqltypes.MakeString([]byte("1427325875"))},
			},
		},
	}
}

func getSchemaInfoTestSchemaOverride() []SchemaOverride {
	return []SchemaOverride{
		SchemaOverride{
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
		SchemaOverride{
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
		SchemaOverride{
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
		SchemaOverride{
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
		SchemaOverride{
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
		SchemaOverride{
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
		SchemaOverride{
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
		SchemaOverride{
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
		sqltypes.MakeString([]byte("1427325875")),
		sqltypes.MakeString([]byte("")),
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

func getSchemaInfoTestSupportedQueries() map[string]*mproto.QueryResult {
	return map[string]*mproto.QueryResult{
		// queries for schema info
		"select unix_timestamp()": &mproto.QueryResult{
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				[]sqltypes.Value{sqltypes.MakeString([]byte("1427325875"))},
			},
		},
		"select @@global.sql_mode": &mproto.QueryResult{
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				[]sqltypes.Value{sqltypes.MakeString([]byte("STRICT_TRANS_TABLES"))},
			},
		},
		baseShowTables: &mproto.QueryResult{
			RowsAffected: 3,
			Rows: [][]sqltypes.Value{
				[]sqltypes.Value{
					sqltypes.MakeString([]byte("test_table_01")),
					sqltypes.MakeString([]byte("USER TABLE")),
					sqltypes.MakeString([]byte("1427325875")),
					sqltypes.MakeString([]byte("")),
				},
				[]sqltypes.Value{
					sqltypes.MakeString([]byte("test_table_02")),
					sqltypes.MakeString([]byte("USER TABLE")),
					sqltypes.MakeString([]byte("1427325875")),
					sqltypes.MakeString([]byte("")),
				},
				[]sqltypes.Value{
					sqltypes.MakeString([]byte("test_table_03")),
					sqltypes.MakeString([]byte("USER TABLE")),
					sqltypes.MakeString([]byte("1427325875")),
					sqltypes.MakeString([]byte("")),
				},
			},
		},
		"describe `test_table_01`": &mproto.QueryResult{
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				[]sqltypes.Value{
					sqltypes.MakeString([]byte("pk")),
					sqltypes.MakeString([]byte("int")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("1")),
					sqltypes.MakeString([]byte{}),
				},
			},
		},
		"describe `test_table_02`": &mproto.QueryResult{
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				[]sqltypes.Value{
					sqltypes.MakeString([]byte("pk")),
					sqltypes.MakeString([]byte("int")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("1")),
					sqltypes.MakeString([]byte{}),
				},
			},
		},
		"describe `test_table_03`": &mproto.QueryResult{
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				[]sqltypes.Value{
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
		"show index from `test_table_01`": &mproto.QueryResult{
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				[]sqltypes.Value{
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
		"show index from `test_table_02`": &mproto.QueryResult{
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				[]sqltypes.Value{
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
		"show index from `test_table_03`": &mproto.QueryResult{
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				[]sqltypes.Value{
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
		"begin":  &mproto.QueryResult{},
		"commit": &mproto.QueryResult{},
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
