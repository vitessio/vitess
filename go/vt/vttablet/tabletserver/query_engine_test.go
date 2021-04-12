/*
Copyright 2019 The Vitess Authors.

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

package tabletserver

import (
	"context"
	"expvar"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"vitess.io/vitess/go/mysql"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/cache"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/streamlog"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/tableacl"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/planbuilder"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema/schematest"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestStrictMode(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	for query, result := range schematest.Queries() {
		db.AddQuery(query, result)
	}

	// Test default behavior.
	config := tabletenv.NewDefaultConfig()
	config.DB = newDBConfigs(db)
	env := tabletenv.NewEnv(config, "TabletServerTest")
	se := schema.NewEngine(env)
	qe := NewQueryEngine(env, se)
	qe.se.InitDBConfig(newDBConfigs(db).DbaWithDB())
	qe.se.Open()
	if err := qe.Open(); err != nil {
		t.Error(err)
	}
	qe.Close()

	// Check that we fail if STRICT_TRANS_TABLES or STRICT_ALL_TABLES is not set.
	db.AddQuery(
		"select @@global.sql_mode",
		&sqltypes.Result{
			Fields: []*querypb.Field{{Type: sqltypes.VarChar}},
			Rows:   [][]sqltypes.Value{{sqltypes.NewVarBinary("")}},
		},
	)
	qe = NewQueryEngine(env, se)
	err := qe.Open()
	wantErr := "require sql_mode to be STRICT_TRANS_TABLES or STRICT_ALL_TABLES: got ''"
	if err == nil || err.Error() != wantErr {
		t.Errorf("Open: %v, want %s", err, wantErr)
	}
	qe.Close()

	// Test that we succeed if the enforcement flag is off.
	config.EnforceStrictTransTables = false
	qe = NewQueryEngine(env, se)
	if err := qe.Open(); err != nil {
		t.Fatal(err)
	}
	qe.Close()
}

func TestGetPlanPanicDuetoEmptyQuery(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	for query, result := range schematest.Queries() {
		db.AddQuery(query, result)
	}
	qe := newTestQueryEngine(10*time.Second, true, newDBConfigs(db))
	qe.se.Open()
	qe.Open()
	defer qe.Close()

	ctx := context.Background()
	logStats := tabletenv.NewLogStats(ctx, "GetPlanStats")
	_, err := qe.GetPlan(ctx, logStats, "", false, false /* inReservedConn */)
	require.EqualError(t, err, "Query was empty")
}

func addSchemaEngineQueries(db *fakesqldb.DB) {
	db.AddQueryPattern(baseShowTablesPattern, &sqltypes.Result{
		Fields: mysql.BaseShowTablesFields,
		Rows: [][]sqltypes.Value{
			mysql.BaseShowTablesRow("test_table_01", false, ""),
			mysql.BaseShowTablesRow("test_table_02", false, ""),
			mysql.BaseShowTablesRow("test_table_03", false, ""),
			mysql.BaseShowTablesRow("seq", false, "vitess_sequence"),
			mysql.BaseShowTablesRow("msg", false, "vitess_message,vt_ack_wait=30,vt_purge_after=120,vt_batch_size=1,vt_cache_size=10,vt_poller_interval=30"),
		}})
	db.AddQuery("show status like 'Innodb_rows_read'", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"Variable_name|Value",
		"varchar|int64"),
		"Innodb_rows_read|0",
	))
}

func TestGetMessageStreamPlan(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	for query, result := range schematest.Queries() {
		db.AddQuery(query, result)
	}

	addSchemaEngineQueries(db)

	qe := newTestQueryEngine(10*time.Second, true, newDBConfigs(db))
	qe.se.Open()
	qe.Open()
	defer qe.Close()

	plan, err := qe.GetMessageStreamPlan("msg")
	if err != nil {
		t.Fatal(err)
	}
	wantPlan := &planbuilder.Plan{
		PlanID: planbuilder.PlanMessageStream,
		Table:  qe.tables["msg"],
		Permissions: []planbuilder.Permission{{
			TableName: "msg",
			Role:      tableacl.WRITER,
		}},
	}
	if !reflect.DeepEqual(plan.Plan, wantPlan) {
		t.Errorf("GetMessageStreamPlan(msg): %v, want %v", plan.Plan, wantPlan)
	}
	if plan.Rules == nil || plan.Authorized == nil {
		t.Errorf("GetMessageStreamPlan(msg): Rules or ACLResult are nil. Rules: %v, Authorized: %v", plan.Rules, plan.Authorized)
	}
}

func assertPlanCacheSize(t *testing.T, qe *QueryEngine, expected int) {
	var size int
	qe.plans.ForEach(func(_ interface{}) bool {
		size++
		return true
	})
	if size != expected {
		t.Fatalf("expected query plan cache to contain %d entries, found %d", expected, size)
	}
}

func TestQueryPlanCache(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	for query, result := range schematest.Queries() {
		db.AddQuery(query, result)
	}

	firstQuery := "select * from test_table_01"
	secondQuery := "select * from test_table_02"
	db.AddQuery("select * from test_table_01 where 1 != 1", &sqltypes.Result{})
	db.AddQuery("select * from test_table_02 where 1 != 1", &sqltypes.Result{})

	qe := newTestQueryEngine(10*time.Second, true, newDBConfigs(db))
	qe.se.Open()
	qe.Open()
	defer qe.Close()

	ctx := context.Background()
	logStats := tabletenv.NewLogStats(ctx, "GetPlanStats")
	if cache.DefaultConfig.LFU {
		qe.SetQueryPlanCacheCap(1024)
	} else {
		qe.SetQueryPlanCacheCap(1)
	}
	firstPlan, err := qe.GetPlan(ctx, logStats, firstQuery, false, false /* inReservedConn */)
	if err != nil {
		t.Fatal(err)
	}
	if firstPlan == nil {
		t.Fatalf("plan should not be nil")
	}
	secondPlan, err := qe.GetPlan(ctx, logStats, secondQuery, false, false /* inReservedConn */)
	if err != nil {
		t.Fatal(err)
	}
	if secondPlan == nil {
		t.Fatalf("plan should not be nil")
	}
	expvar.Do(func(kv expvar.KeyValue) {
		_ = kv.Value.String()
	})
	assertPlanCacheSize(t, qe, 1)
	qe.ClearQueryPlanCache()
}

func TestNoQueryPlanCache(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	for query, result := range schematest.Queries() {
		db.AddQuery(query, result)
	}

	firstQuery := "select * from test_table_01"
	db.AddQuery("select * from test_table_01 where 1 != 1", &sqltypes.Result{})
	db.AddQuery("select * from test_table_02 where 1 != 1", &sqltypes.Result{})

	qe := newTestQueryEngine(10*time.Second, true, newDBConfigs(db))
	qe.se.Open()
	qe.Open()
	defer qe.Close()

	ctx := context.Background()
	logStats := tabletenv.NewLogStats(ctx, "GetPlanStats")
	qe.SetQueryPlanCacheCap(1024)
	firstPlan, err := qe.GetPlan(ctx, logStats, firstQuery, true, false /* inReservedConn */)
	if err != nil {
		t.Fatal(err)
	}
	if firstPlan == nil {
		t.Fatalf("plan should not be nil")
	}
	assertPlanCacheSize(t, qe, 0)
	qe.ClearQueryPlanCache()
}

func TestNoQueryPlanCacheDirective(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	for query, result := range schematest.Queries() {
		db.AddQuery(query, result)
	}

	firstQuery := "select /*vt+ SKIP_QUERY_PLAN_CACHE=1 */ * from test_table_01"
	db.AddQuery("select /*vt+ SKIP_QUERY_PLAN_CACHE=1 */ * from test_table_01 where 1 != 1", &sqltypes.Result{})
	db.AddQuery("select /*vt+ SKIP_QUERY_PLAN_CACHE=1 */ * from test_table_02 where 1 != 1", &sqltypes.Result{})

	qe := newTestQueryEngine(10*time.Second, true, newDBConfigs(db))
	qe.se.Open()
	qe.Open()
	defer qe.Close()

	ctx := context.Background()
	logStats := tabletenv.NewLogStats(ctx, "GetPlanStats")
	qe.SetQueryPlanCacheCap(1024)
	firstPlan, err := qe.GetPlan(ctx, logStats, firstQuery, false, false /* inReservedConn */)
	if err != nil {
		t.Fatal(err)
	}
	if firstPlan == nil {
		t.Fatalf("plan should not be nil")
	}
	assertPlanCacheSize(t, qe, 0)
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
	qe := newTestQueryEngine(1*time.Second, true, newDBConfigs(db))
	qe.se.Open()
	qe.Open()
	defer qe.Close()
	// warm up cache
	ctx := context.Background()
	logStats := tabletenv.NewLogStats(ctx, "GetPlanStats")
	qe.GetPlan(ctx, logStats, query, false, false /* inReservedConn */)

	request, _ := http.NewRequest("GET", "/debug/tablet_plans", nil)
	response := httptest.NewRecorder()
	qe.handleHTTPQueryPlans(response, request)

	request, _ = http.NewRequest("GET", "/debug/query_stats", nil)
	response = httptest.NewRecorder()
	qe.handleHTTPQueryStats(response, request)

	request, _ = http.NewRequest("GET", "/debug/query_rules", nil)
	response = httptest.NewRecorder()
	qe.handleHTTPQueryRules(response, request)
}

func newTestQueryEngine(idleTimeout time.Duration, strict bool, dbcfgs *dbconfigs.DBConfigs) *QueryEngine {
	config := tabletenv.NewDefaultConfig()
	config.DB = dbcfgs
	config.OltpReadPool.IdleTimeoutSeconds.Set(idleTimeout)
	config.OlapReadPool.IdleTimeoutSeconds.Set(idleTimeout)
	config.TxPool.IdleTimeoutSeconds.Set(idleTimeout)
	env := tabletenv.NewEnv(config, "TabletServerTest")
	se := schema.NewEngine(env)
	qe := NewQueryEngine(env, se)
	se.InitDBConfig(dbcfgs.DbaWithDB())
	return qe
}

func runConsolidatedQuery(t *testing.T, sql string) *QueryEngine {
	db := fakesqldb.New(t)
	defer db.Close()

	qe := newTestQueryEngine(1*time.Second, true, newDBConfigs(db))
	qe.se.Open()
	qe.Open()
	defer qe.Close()

	r1, ok := qe.consolidator.Create(sql)
	if !ok {
		t.Errorf("expected first consolidator ok")
	}
	r2, ok := qe.consolidator.Create(sql)
	if ok {
		t.Errorf("expected second consolidator not ok")
	}

	r1.Broadcast()
	r2.Wait()

	return qe
}

func TestConsolidationsUIRedaction(t *testing.T) {
	// Reset to default redaction state.
	defer func() {
		*streamlog.RedactDebugUIQueries = false
	}()

	request, _ := http.NewRequest("GET", "/debug/consolidations", nil)

	sql := "select * from test_db_01 where col = 'secret'"
	redactedSQL := "select * from test_db_01 where col = :redacted1"

	// First with the redaction off
	*streamlog.RedactDebugUIQueries = false
	unRedactedResponse := httptest.NewRecorder()
	qe := runConsolidatedQuery(t, sql)

	qe.handleHTTPConsolidations(unRedactedResponse, request)
	if !strings.Contains(unRedactedResponse.Body.String(), sql) {
		t.Fatalf("Response is missing the consolidated query: %v %v", sql, unRedactedResponse.Body.String())
	}

	// Now with the redaction on
	*streamlog.RedactDebugUIQueries = true
	redactedResponse := httptest.NewRecorder()
	qe.handleHTTPConsolidations(redactedResponse, request)

	if strings.Contains(redactedResponse.Body.String(), "secret") {
		t.Fatalf("Response contains unredacted consolidated query: %v %v", sql, redactedResponse.Body.String())
	}

	if !strings.Contains(redactedResponse.Body.String(), redactedSQL) {
		t.Fatalf("Response missing redacted consolidated query: %v %v", redactedSQL, redactedResponse.Body.String())
	}
}

func BenchmarkPlanCacheThroughput(b *testing.B) {
	db := fakesqldb.New(b)
	defer db.Close()

	for query, result := range schematest.Queries() {
		db.AddQuery(query, result)
	}

	db.AddQueryPattern(".*", &sqltypes.Result{})

	qe := newTestQueryEngine(10*time.Second, true, newDBConfigs(db))
	qe.se.Open()
	qe.Open()
	defer qe.Close()

	ctx := context.Background()
	logStats := tabletenv.NewLogStats(ctx, "GetPlanStats")

	for i := 0; i < b.N; i++ {
		query := fmt.Sprintf("SELECT (a, b, c) FROM test_table_%d", rand.Intn(500))
		_, err := qe.GetPlan(ctx, logStats, query, false, false /* inReservedConn */)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkPlanCache(b *testing.B, db *fakesqldb.DB, lfu bool, par int) {
	b.Helper()

	dbcfgs := newDBConfigs(db)
	config := tabletenv.NewDefaultConfig()
	config.DB = dbcfgs
	config.QueryCacheLFU = lfu

	env := tabletenv.NewEnv(config, "TabletServerTest")
	se := schema.NewEngine(env)
	qe := NewQueryEngine(env, se)

	se.InitDBConfig(dbcfgs.DbaWithDB())
	require.NoError(b, se.Open())
	require.NoError(b, qe.Open())
	defer qe.Close()

	b.SetParallelism(par)
	b.RunParallel(func(pb *testing.PB) {
		ctx := context.Background()
		logStats := tabletenv.NewLogStats(ctx, "GetPlanStats")

		for pb.Next() {
			query := fmt.Sprintf("SELECT (a, b, c) FROM test_table_%d", rand.Intn(500))
			_, err := qe.GetPlan(ctx, logStats, query, false, false /* inReservedConn */)
			require.NoErrorf(b, err, "bad query: %s", query)
		}
	})
}

func BenchmarkPlanCacheContention(b *testing.B) {
	db := fakesqldb.New(b)
	defer db.Close()

	for query, result := range schematest.Queries() {
		db.AddQuery(query, result)
	}

	db.AddQueryPattern(".*", &sqltypes.Result{})

	for par := 1; par <= 8; par *= 2 {
		b.Run(fmt.Sprintf("ContentionLRU-%d", par), func(b *testing.B) {
			benchmarkPlanCache(b, db, false, par)
		})

		b.Run(fmt.Sprintf("ContentionLFU-%d", par), func(b *testing.B) {
			benchmarkPlanCache(b, db, true, par)
		})
	}
}

func TestPlanCachePollution(t *testing.T) {
	plotPath := os.Getenv("CACHE_PLOT_PATH")
	if plotPath == "" {
		t.Skipf("CACHE_PLOT_PATH not set")
	}

	const NormalQueries = 500000
	const PollutingQueries = NormalQueries / 2

	db := fakesqldb.New(t)
	defer db.Close()

	for query, result := range schematest.Queries() {
		db.AddQuery(query, result)
	}

	db.AddQueryPattern(".*", &sqltypes.Result{})

	dbcfgs := newDBConfigs(db)
	config := tabletenv.NewDefaultConfig()
	config.DB = dbcfgs
	// config.LFUQueryCacheSizeBytes = 3 * 1024 * 1024

	env := tabletenv.NewEnv(config, "TabletServerTest")
	se := schema.NewEngine(env)
	qe := NewQueryEngine(env, se)

	se.InitDBConfig(dbcfgs.DbaWithDB())
	se.Open()

	qe.Open()
	defer qe.Close()

	type Stats struct {
		queries  uint64
		cached   uint64
		interval time.Duration
	}

	var stats1, stats2 Stats
	var wg sync.WaitGroup

	go func() {
		cacheMode := "lru"
		if config.QueryCacheLFU {
			cacheMode = "lfu"
		}

		out, err := os.Create(path.Join(plotPath,
			fmt.Sprintf("cache_plot_%d_%d_%s.dat",
				config.QueryCacheSize, config.QueryCacheMemory, cacheMode,
			)),
		)
		require.NoError(t, err)
		defer out.Close()

		var last1 uint64
		var last2 uint64

		for range time.Tick(100 * time.Millisecond) {
			var avg1, avg2 time.Duration

			if stats1.queries-last1 > 0 {
				avg1 = stats1.interval / time.Duration(stats1.queries-last1)
			}
			if stats2.queries-last2 > 0 {
				avg2 = stats2.interval / time.Duration(stats2.queries-last2)
			}

			stats1.interval = 0
			last1 = stats1.queries
			stats2.interval = 0
			last2 = stats2.queries

			cacheUsed, cacheCap := qe.plans.UsedCapacity(), qe.plans.MaxCapacity()

			t.Logf("%d queries (%f hit rate), cache %d / %d (%f usage), %v %v",
				stats1.queries+stats2.queries,
				float64(stats1.cached)/float64(stats1.queries),
				cacheUsed, cacheCap,
				float64(cacheUsed)/float64(cacheCap), avg1, avg2)

			if out != nil {
				fmt.Fprintf(out, "%d %f %f %f %f %d %d\n",
					stats1.queries+stats2.queries,
					float64(stats1.queries)/float64(NormalQueries),
					float64(stats2.queries)/float64(PollutingQueries),
					float64(stats1.cached)/float64(stats1.queries),
					float64(cacheUsed)/float64(cacheCap),
					avg1.Microseconds(),
					avg2.Microseconds(),
				)
			}
		}
	}()

	runner := func(totalQueries uint64, stats *Stats, sample func() string) {
		for i := uint64(0); i < totalQueries; i++ {
			ctx := context.Background()
			logStats := tabletenv.NewLogStats(ctx, "GetPlanStats")
			query := sample()

			start := time.Now()
			_, err := qe.GetPlan(ctx, logStats, query, false, false /* inReservedConn */)
			require.NoErrorf(t, err, "bad query: %s", query)
			stats.interval += time.Since(start)

			atomic.AddUint64(&stats.queries, 1)
			if logStats.CachedPlan {
				atomic.AddUint64(&stats.cached, 1)
			}
		}
	}

	wg.Add(2)

	go func() {
		defer wg.Done()
		runner(NormalQueries, &stats1, func() string {
			return fmt.Sprintf("SELECT (a, b, c) FROM test_table_%d", rand.Intn(5000))
		})
	}()

	go func() {
		defer wg.Done()
		time.Sleep(500 * time.Millisecond)
		runner(PollutingQueries, &stats2, func() string {
			return fmt.Sprintf("INSERT INTO test_table_00 VALUES (1, 2, 3, %d)", rand.Int())
		})
	}()

	wg.Wait()
}
