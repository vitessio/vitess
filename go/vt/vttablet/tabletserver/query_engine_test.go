// Copyright 2017, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"context"
	"expvar"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/youtube/vitess/go/mysqlconn/fakesqldb"
	"github.com/youtube/vitess/go/sqltypes"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/schema"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/schema/schematest"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

func TestStrictTransTables(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	for query, result := range schematest.Queries() {
		db.AddQuery(query, result)
	}
	testUtils := newTestUtils()
	dbconfigs := testUtils.newDBConfigs(db)

	// Test default behavior.
	config := tabletenv.DefaultQsConfig
	// config.EnforceStrictTransTable is true by default.
	qe := NewQueryEngine(DummyChecker, schema.NewEngine(DummyChecker, config), config)
	qe.se.Open(db.ConnParams())
	if err := qe.Open(dbconfigs); err != nil {
		t.Error(err)
	}
	qe.Close()

	// Check that we fail if STRICT_TRANS_TABLES is not set.
	db.AddQuery(
		"select @@global.sql_mode",
		&sqltypes.Result{
			Fields: []*querypb.Field{{Type: sqltypes.VarChar}},
			Rows:   [][]sqltypes.Value{{sqltypes.MakeString([]byte(""))}},
		},
	)
	qe = NewQueryEngine(DummyChecker, schema.NewEngine(DummyChecker, config), config)
	err := qe.Open(dbconfigs)
	wantErr := "require sql_mode to be STRICT_TRANS_TABLES: got ''"
	if err == nil || err.Error() != wantErr {
		t.Errorf("Open: %v, want %s", err, wantErr)
	}
	qe.Close()

	// Test that we succeed if the enforcement flag is off.
	config.EnforceStrictTransTables = false
	qe = NewQueryEngine(DummyChecker, schema.NewEngine(DummyChecker, config), config)
	if err := qe.Open(dbconfigs); err != nil {
		t.Error(err)
	}
	qe.Close()
}

func TestGetPlanPanicDuetoEmptyQuery(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	for query, result := range schematest.Queries() {
		db.AddQuery(query, result)
	}
	qe := newTestQueryEngine(10, 10*time.Second, true)
	testUtils := newTestUtils()
	dbconfigs := testUtils.newDBConfigs(db)
	qe.se.Open(db.ConnParams())
	qe.Open(dbconfigs)
	defer qe.Close()

	ctx := context.Background()
	logStats := tabletenv.NewLogStats(ctx, "GetPlanStats")
	_, err := qe.GetPlan(ctx, logStats, "")
	want := "syntax error"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("qe.GetPlan: %v, want %s", err, want)
	}
}

func TestQueryCache(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	for query, result := range schematest.Queries() {
		db.AddQuery(query, result)
	}

	firstQuery := "select * from test_table_01"
	secondQuery := "select * from test_table_02"
	db.AddQuery("select * from test_table_01 where 1 != 1", &sqltypes.Result{})
	db.AddQuery("select * from test_table_02 where 1 != 1", &sqltypes.Result{})

	qe := newTestQueryEngine(10, 10*time.Second, true)
	testUtils := newTestUtils()
	dbconfigs := testUtils.newDBConfigs(db)
	qe.se.Open(db.ConnParams())
	qe.Open(dbconfigs)
	defer qe.Close()

	ctx := context.Background()
	logStats := tabletenv.NewLogStats(ctx, "GetPlanStats")
	qe.SetQueryCacheCap(1)
	firstPlan, err := qe.GetPlan(ctx, logStats, firstQuery)
	if err != nil {
		t.Fatal(err)
	}
	if firstPlan == nil {
		t.Fatalf("plan should not be nil")
	}
	secondPlan, err := qe.GetPlan(ctx, logStats, secondQuery)
	if err != nil {
		t.Fatal(err)
	}
	if secondPlan == nil {
		t.Fatalf("plan should not be nil")
	}
	expvar.Do(func(kv expvar.KeyValue) {
		_ = kv.Value.String()
	})
	qe.ClearQueryPlanCache()
}

func TestStatsURL(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	for query, result := range schematest.Queries() {
		db.AddQuery(query, result)
	}
	query := "select * from test_table_01"
	db.AddQuery("select * from test_table_01 where 1 != 1", &sqltypes.Result{})
	qe := newTestQueryEngine(10, 1*time.Second, true)
	testUtils := newTestUtils()
	dbconfigs := testUtils.newDBConfigs(db)
	qe.se.Open(db.ConnParams())
	qe.Open(dbconfigs)
	defer qe.Close()
	// warm up cache
	ctx := context.Background()
	logStats := tabletenv.NewLogStats(ctx, "GetPlanStats")
	qe.GetPlan(ctx, logStats, query)

	request, _ := http.NewRequest("GET", "/debug/tablet_plans", nil)
	response := httptest.NewRecorder()
	qe.ServeHTTP(response, request)

	request, _ = http.NewRequest("GET", "/debug/query_stats", nil)
	response = httptest.NewRecorder()
	qe.ServeHTTP(response, request)

	request, _ = http.NewRequest("GET", "/debug/query_rules", nil)
	response = httptest.NewRecorder()
	qe.ServeHTTP(response, request)

	request, _ = http.NewRequest("GET", "/debug/unknown", nil)
	response = httptest.NewRecorder()
	qe.ServeHTTP(response, request)
}

func newTestQueryEngine(queryCacheSize int, idleTimeout time.Duration, strict bool) *QueryEngine {
	config := tabletenv.DefaultQsConfig
	config.QueryCacheSize = queryCacheSize
	config.IdleTimeout = float64(idleTimeout) / 1e9
	return NewQueryEngine(DummyChecker, schema.NewEngine(DummyChecker, config), config)
}
