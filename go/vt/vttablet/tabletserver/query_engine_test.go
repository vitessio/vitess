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
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"vitess.io/vitess/go/cache/theine"
	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vtenv"

	"vitess.io/vitess/go/vt/sqlparser"

	"vitess.io/vitess/go/mysql"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/streamlog"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/tableacl"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/planbuilder"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/rules"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema/schematest"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestStrictMode(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	schematest.AddDefaultQueries(db)

	// Test default behavior.
	cfg := tabletenv.NewDefaultConfig()
	cfg.DB = newDBConfigs(db)
	env := tabletenv.NewEnv(vtenv.NewTestEnv(), cfg, "TabletServerTest")
	se := schema.NewEngine(env)
	qe := NewQueryEngine(env, se)
	qe.se.InitDBConfig(newDBConfigs(db).DbaWithDB())
	qe.se.Open()
	require.NoError(t, qe.Open())
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
	require.EqualError(t, err, "require sql_mode to be STRICT_TRANS_TABLES or STRICT_ALL_TABLES: got ''", "Open")
	qe.Close()

	// Test that we succeed if the enforcement flag is off.
	cfg.EnforceStrictTransTables = false
	qe = NewQueryEngine(env, se)
	require.NoError(t, qe.Open())
	qe.Close()
}

func TestGetPlanPanicDuetoEmptyQuery(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	schematest.AddDefaultQueries(db)
	qe := newTestQueryEngine(10*time.Second, true, newDBConfigs(db))
	qe.se.Open()
	qe.Open()
	defer qe.Close()

	ctx := t.Context()
	logStats := tabletenv.NewLogStats(ctx, "GetPlanStats", streamlog.NewQueryLogConfigForTest())
	_, err := qe.GetPlan(ctx, logStats, "", false, false)
	require.EqualError(t, err, "Query was empty")
}

func addSchemaEngineQueries(db *fakesqldb.DB) {
	db.AddQueryPattern(baseShowTablesWithSizesPattern, &sqltypes.Result{
		Fields: mysql.BaseShowTablesWithSizesFields,
		Rows: [][]sqltypes.Value{
			mysql.BaseShowTablesWithSizesRow("test_table_01", false, ""),
			mysql.BaseShowTablesWithSizesRow("test_table_02", false, ""),
			mysql.BaseShowTablesWithSizesRow("test_table_03", false, ""),
			mysql.BaseShowTablesWithSizesRow("seq", false, "vitess_sequence"),
			mysql.BaseShowTablesWithSizesRow("msg", false, "vitess_message,vt_ack_wait=30,vt_purge_after=120,vt_batch_size=1,vt_cache_size=10,vt_poller_interval=30"),
		},
	})
	db.AddQuery(mysql.BaseShowTables,
		&sqltypes.Result{
			Fields: mysql.BaseShowTablesFields,
			Rows: [][]sqltypes.Value{
				mysql.BaseShowTablesRow("test_table_01", false, ""),
				mysql.BaseShowTablesRow("test_table_02", false, ""),
				mysql.BaseShowTablesRow("test_table_03", false, ""),
				mysql.BaseShowTablesRow("seq", false, "vitess_sequence"),
				mysql.BaseShowTablesRow("msg", false, "vitess_message,vt_ack_wait=30,vt_purge_after=120,vt_batch_size=1,vt_cache_size=10,vt_poller_interval=30"),
			},
		})
	db.AddQuery("show status like 'Innodb_rows_read'", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"Variable_name|Value",
		"varchar|int64"),
		"Innodb_rows_read|0",
	))
}

func TestGetMessageStreamPlan(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	schematest.AddDefaultQueries(db)

	addSchemaEngineQueries(db)

	qe := newTestQueryEngine(10*time.Second, true, newDBConfigs(db))
	qe.se.Open()
	qe.Open()
	defer qe.Close()

	plan, err := qe.GetMessageStreamPlan("msg")
	require.NoError(t, err)
	wantPlan := &planbuilder.Plan{
		PlanID: planbuilder.PlanMessageStream,
		Table:  qe.schema.Load().tables["msg"],
		Permissions: []planbuilder.Permission{{
			TableName: "msg",
			Role:      tableacl.WRITER,
		}},
	}
	assert.Equalf(t, wantPlan, plan.Plan, "GetMessageStreamPlan(msg)")
	assert.NotNilf(t, plan.Rules, "GetMessageStreamPlan(msg): Rules is nil")
	assert.NotNilf(t, plan.Authorized, "GetMessageStreamPlan(msg): Authorized is nil")
}

func assertPlanCacheSize(t *testing.T, qe *QueryEngine, expected int) {
	t.Helper()
	time.Sleep(100 * time.Millisecond)
	size := qe.plans.Len()
	require.Equal(t, expected, size, "expected query plan cache to contain %d entries, found %d", expected, size)
}

func TestQueryPlanCache(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	schematest.AddDefaultQueries(db)

	firstQuery := "select * from test_table_01"
	db.AddQuery("select * from test_table_01 where 1 != 1", &sqltypes.Result{})
	db.AddQuery("select * from test_table_02 where 1 != 1", &sqltypes.Result{})

	qe := newTestQueryEngine(10*time.Second, true, newDBConfigs(db))
	qe.se.Open()
	qe.Open()
	defer qe.Close()

	ctx := t.Context()
	logStats := tabletenv.NewLogStats(ctx, "GetPlanStats", streamlog.NewQueryLogConfigForTest())

	initialHits := qe.queryEnginePlanCacheHits.Get()
	initialMisses := qe.queryEnginePlanCacheMisses.Get()

	firstPlan, err := qe.GetPlan(ctx, logStats, firstQuery, false, false)
	require.NoError(t, err)
	require.NotNil(t, firstPlan, "plan should not be nil")

	assertPlanCacheSize(t, qe, 1)

	require.Equal(t, int64(0), qe.queryEnginePlanCacheHits.Get()-initialHits)
	require.Equal(t, int64(1), qe.queryEnginePlanCacheMisses.Get()-initialMisses)

	secondPlan, err := qe.GetPlan(ctx, logStats, firstQuery, false, false)
	require.NoError(t, err)
	require.NotNil(t, secondPlan, "plan should not be nil")

	assertPlanCacheSize(t, qe, 1)

	require.Equal(t, int64(1), qe.queryEnginePlanCacheHits.Get()-initialHits)
	require.Equal(t, int64(1), qe.queryEnginePlanCacheMisses.Get()-initialMisses)

	qe.ClearQueryPlanCache()
}

func TestNoQueryPlanCache(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	schematest.AddDefaultQueries(db)

	firstQuery := "select * from test_table_01"
	db.AddQuery("select * from test_table_01 where 1 != 1", &sqltypes.Result{})
	db.AddQuery("select * from test_table_02 where 1 != 1", &sqltypes.Result{})

	qe := newTestQueryEngine(10*time.Second, true, newDBConfigs(db))
	qe.se.Open()
	qe.Open()
	defer qe.Close()

	ctx := t.Context()
	logStats := tabletenv.NewLogStats(ctx, "GetPlanStats", streamlog.NewQueryLogConfigForTest())

	firstPlan, err := qe.GetPlan(ctx, logStats, firstQuery, true, false)
	require.NoError(t, err)
	require.NotNil(t, firstPlan, "plan should not be nil")
	assertPlanCacheSize(t, qe, 0)
	qe.ClearQueryPlanCache()
}

func TestNoQueryPlanCacheDirective(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	schematest.AddDefaultQueries(db)

	firstQuery := "select /*vt+ SKIP_QUERY_PLAN_CACHE=1 */ * from test_table_01"
	db.AddQuery("select * from test_table_01 where 1 != 1", &sqltypes.Result{})
	db.AddQuery("select /*vt+ SKIP_QUERY_PLAN_CACHE=1 */ * from test_table_01 where 1 != 1", &sqltypes.Result{})
	db.AddQuery("select /*vt+ SKIP_QUERY_PLAN_CACHE=1 */ * from test_table_02 where 1 != 1", &sqltypes.Result{})

	qe := newTestQueryEngine(10*time.Second, true, newDBConfigs(db))
	qe.se.Open()
	qe.Open()
	defer qe.Close()

	ctx := t.Context()
	logStats := tabletenv.NewLogStats(ctx, "GetPlanStats", streamlog.NewQueryLogConfigForTest())

	firstPlan, err := qe.GetPlan(ctx, logStats, firstQuery, false, false)
	require.NoError(t, err)
	require.NotNil(t, firstPlan, "plan should not be nil")
	assertPlanCacheSize(t, qe, 0)
	qe.ClearQueryPlanCache()
}

func TestStreamQueryPlanCache(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	schematest.AddDefaultQueries(db)

	firstQuery := "select * from test_table_01"
	db.AddQuery("select * from test_table_01 where 1 != 1", &sqltypes.Result{})
	db.AddQuery("select * from test_table_02 where 1 != 1", &sqltypes.Result{})

	qe := newTestQueryEngine(10*time.Second, true, newDBConfigs(db))
	qe.se.Open()
	qe.Open()
	defer qe.Close()

	ctx := t.Context()
	logStats := tabletenv.NewLogStats(ctx, "GetPlanStats", streamlog.NewQueryLogConfigForTest())

	firstPlan, err := qe.GetStreamPlan(ctx, logStats, firstQuery, false)
	require.NoError(t, err)
	require.NotNil(t, firstPlan, "plan should not be nil")
	assertPlanCacheSize(t, qe, 1)
	qe.ClearQueryPlanCache()
}

// TestGetStreamPlanFiltersRulesByAllTables ensures the streaming plan path
// filters query rules using every table the statement touches, matching the
// non-streaming path. A multi-table DML/SELECT has plan.Table == nil, so
// filtering by the single TableName() would silently skip table-scoped rules.
func TestGetStreamPlanFiltersRulesByAllTables(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	schematest.AddDefaultQueries(db)

	qe := newTestQueryEngine(10*time.Second, true, newDBConfigs(db))
	qe.se.Open()
	qe.Open()
	defer qe.Close()

	// A rule scoped to the second table of a multi-table statement. plan.Table
	// is nil for multi-table queries, so a single-table-name filter would drop
	// it.
	const sourceName = "test stream rules"
	qr := rules.NewQueryRule("deny test_table_02", "deny_t2", rules.QRFailRetry)
	qr.AddTableCond("test_table_02")
	qrs := rules.New()
	qrs.Add(qr)
	qe.queryRuleSources.UnRegisterSource(sourceName)
	qe.queryRuleSources.RegisterSource(sourceName)
	defer qe.queryRuleSources.UnRegisterSource(sourceName)
	require.NoError(t, qe.queryRuleSources.SetRules(sourceName, qrs))

	// getStreamPlan is exercised directly with a populated schema so the
	// planbuilder resolves the referenced tables (the schema engine in this
	// test harness does not populate the table map).
	curSchema := &currentSchema{
		tables: map[string]*schema.Table{
			"test_table_01": {Name: sqlparser.NewIdentifierCS("test_table_01")},
			"test_table_02": {Name: sqlparser.NewIdentifierCS("test_table_02")},
		},
	}

	testcases := []struct {
		name string
		sql  string
	}{
		{"multi-table DML", "update test_table_01, test_table_02 set test_table_01.pk = 1"},
		{"multi-table select", "select test_table_01.pk from test_table_01 join test_table_02 on test_table_01.pk = test_table_02.pk"},
	}
	for _, tcase := range testcases {
		t.Run(tcase.name, func(t *testing.T) {
			plan, err := qe.getStreamPlan(curSchema, tcase.sql)
			require.NoError(t, err)
			require.Nil(t, plan.Table, "expected a multi-table plan with no single table")
			require.NotNil(t, plan.Rules.Find("deny_t2"), "table-scoped query rule must apply to multi-table streamed query")
		})
	}
}

// A rule using the deprecated SelectStream plan name applies to streaming-path
// plans for every statement shape the pre-v25 streaming planner labeled
// SelectStream, and never to buffered-execution plans of the same statements.
func TestSelectStreamRuleAppliesOnlyToStreamingPlans(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	schematest.AddDefaultQueries(db)

	qe := newTestQueryEngine(10*time.Second, true, newDBConfigs(db))
	qe.se.Open()
	qe.Open()
	defer qe.Close()

	const sourceName = "test legacy stream rules"
	qr := rules.NewQueryRule("deny streamed reads", "deny_streamed_reads", rules.QRFail)
	qr.AddPlanCond(planbuilder.PlanSelectStream)
	qrs := rules.New()
	qrs.Add(qr)
	qe.queryRuleSources.RegisterSource(sourceName)
	defer qe.queryRuleSources.UnRegisterSource(sourceName)
	require.NoError(t, qe.queryRuleSources.SetRules(sourceName, qrs))

	// getStreamPlan and getPlan are exercised directly with a populated schema
	// so the planbuilder resolves the referenced tables (the schema engine in
	// this test harness does not populate the table map).
	curSchema := &currentSchema{
		tables: map[string]*schema.Table{
			"test_table_01": {Name: sqlparser.NewIdentifierCS("test_table_01")},
			"seq":           {Name: sqlparser.NewIdentifierCS("seq"), Type: schema.Sequence},
		},
	}

	testcases := []struct {
		name   string
		sql    string
		planID planbuilder.PlanType
	}{
		{"select", "select pk from test_table_01", planbuilder.PlanSelect},
		{"impossible-where select", "select pk from test_table_01 where 1 != 1", planbuilder.PlanSelectImpossible},
		{"lock-function select", "select get_lock('foo', 10)", planbuilder.PlanSelectLockFunc},
		{"next-value select", "select next value from seq", planbuilder.PlanNextval},
		{"explain", "explain test_table_01", planbuilder.PlanSelect},
		{"show", "show tables", planbuilder.PlanShow},
		{"show vitess_migrations", "show vitess_migrations", planbuilder.PlanShowMigrations},
		{"show other", "show engine innodb status", planbuilder.PlanOtherRead},
	}
	for _, tcase := range testcases {
		t.Run(tcase.name, func(t *testing.T) {
			// The private helpers return the plan together with the errNoCache
			// sentinel for non-cacheable statements like SHOW; the public
			// GetPlan/GetStreamPlan wrappers strip it the same way.
			streamPlan, err := qe.getStreamPlan(curSchema, tcase.sql)
			if err != nil {
				require.ErrorIs(t, err, errNoCache)
			}
			require.Equal(t, tcase.planID, streamPlan.PlanID)
			require.NotNil(t, streamPlan.Rules.Find("deny_streamed_reads"),
				"SelectStream rule must apply to the streaming-path plan")

			execPlan, err := qe.getPlan(curSchema, tcase.sql, false)
			if err != nil {
				require.ErrorIs(t, err, errNoCache)
			}
			require.Equal(t, tcase.planID, execPlan.PlanID)
			require.Nil(t, execPlan.Rules.Find("deny_streamed_reads"),
				"SelectStream rule must not apply to the buffered-execution plan")
		})
	}
}

// Streamed ANALYZE keeps matching rules keyed on OtherRead — its pre-v25
// streaming plan type — and does not match legacy SelectStream rules. On the
// buffered path, where ANALYZE has always planned as Select, neither rule
// applies.
func TestStreamedAnalyzeLegacyRuleMatching(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	schematest.AddDefaultQueries(db)

	qe := newTestQueryEngine(10*time.Second, true, newDBConfigs(db))
	qe.se.Open()
	qe.Open()
	defer qe.Close()

	const sourceName = "test analyze legacy rules"
	qrs := rules.New()
	otherReadRule := rules.NewQueryRule("deny other reads", "deny_other_read", rules.QRFail)
	otherReadRule.AddPlanCond(planbuilder.PlanOtherRead)
	qrs.Add(otherReadRule)
	selectStreamRule := rules.NewQueryRule("deny streamed reads", "deny_streamed_reads", rules.QRFail)
	selectStreamRule.AddPlanCond(planbuilder.PlanSelectStream)
	qrs.Add(selectStreamRule)
	qe.queryRuleSources.RegisterSource(sourceName)
	defer qe.queryRuleSources.UnRegisterSource(sourceName)
	require.NoError(t, qe.queryRuleSources.SetRules(sourceName, qrs))

	curSchema := &currentSchema{
		tables: map[string]*schema.Table{
			"test_table_01": {Name: sqlparser.NewIdentifierCS("test_table_01")},
		},
	}

	streamPlan, err := qe.getStreamPlan(curSchema, "analyze table test_table_01")
	if err != nil {
		require.ErrorIs(t, err, errNoCache)
	}
	require.Equal(t, planbuilder.PlanSelect, streamPlan.PlanID)
	require.NotNil(t, streamPlan.Rules.Find("deny_other_read"),
		"an OtherRead rule must keep applying to streamed ANALYZE")
	require.Nil(t, streamPlan.Rules.Find("deny_streamed_reads"),
		"a SelectStream rule must not apply to streamed ANALYZE")

	execPlan, err := qe.getPlan(curSchema, "analyze table test_table_01", false)
	if err != nil {
		require.ErrorIs(t, err, errNoCache)
	}
	require.Equal(t, planbuilder.PlanSelect, execPlan.PlanID)
	require.Nil(t, execPlan.Rules.Find("deny_other_read"),
		"an OtherRead rule must not apply to buffered ANALYZE")
	require.Nil(t, execPlan.Rules.Find("deny_streamed_reads"),
		"a SelectStream rule must not apply to buffered ANALYZE")
}

func TestNoStreamQueryPlanCache(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	schematest.AddDefaultQueries(db)

	firstQuery := "select * from test_table_01"
	db.AddQuery("select * from test_table_01 where 1 != 1", &sqltypes.Result{})
	db.AddQuery("select * from test_table_02 where 1 != 1", &sqltypes.Result{})

	qe := newTestQueryEngine(10*time.Second, true, newDBConfigs(db))
	qe.se.Open()
	qe.Open()
	defer qe.Close()

	ctx := t.Context()
	logStats := tabletenv.NewLogStats(ctx, "GetPlanStats", streamlog.NewQueryLogConfigForTest())
	firstPlan, err := qe.GetStreamPlan(ctx, logStats, firstQuery, true)
	require.NoError(t, err)
	require.NotNil(t, firstPlan)
	assertPlanCacheSize(t, qe, 0)
	qe.ClearQueryPlanCache()
}

func TestNoStreamQueryPlanCacheDirective(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	schematest.AddDefaultQueries(db)

	firstQuery := "select /*vt+ SKIP_QUERY_PLAN_CACHE=1 */ * from test_table_01"
	db.AddQuery("select * from test_table_01 where 1 != 1", &sqltypes.Result{})
	db.AddQuery("select /*vt+ SKIP_QUERY_PLAN_CACHE=1 */ * from test_table_01 where 1 != 1", &sqltypes.Result{})
	db.AddQuery("select /*vt+ SKIP_QUERY_PLAN_CACHE=1 */ * from test_table_02 where 1 != 1", &sqltypes.Result{})

	qe := newTestQueryEngine(10*time.Second, true, newDBConfigs(db))
	qe.se.Open()
	qe.Open()
	defer qe.Close()

	ctx := t.Context()
	logStats := tabletenv.NewLogStats(ctx, "GetPlanStats", streamlog.NewQueryLogConfigForTest())
	firstPlan, err := qe.GetStreamPlan(ctx, logStats, firstQuery, false)
	require.NoError(t, err)
	require.NotNil(t, firstPlan)
	assertPlanCacheSize(t, qe, 0)
	qe.ClearQueryPlanCache()
}

func TestStatsURL(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	schematest.AddDefaultQueries(db)
	query := "select * from test_table_01"
	db.AddQuery("select * from test_table_01 where 1 != 1", &sqltypes.Result{})
	qe := newTestQueryEngine(1*time.Second, true, newDBConfigs(db))
	qe.se.Open()
	qe.Open()
	defer qe.Close()
	// warm up cache
	ctx := t.Context()
	logStats := tabletenv.NewLogStats(ctx, "GetPlanStats", streamlog.NewQueryLogConfigForTest())
	qe.GetPlan(ctx, logStats, query, false, false)

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
	cfg := tabletenv.NewDefaultConfig()
	cfg.DB = dbcfgs
	cfg.OltpReadPool.IdleTimeout = idleTimeout
	cfg.OlapReadPool.IdleTimeout = idleTimeout
	cfg.TxPool.IdleTimeout = idleTimeout
	env := tabletenv.NewEnv(vtenv.NewTestEnv(), cfg, "TabletServerTest")
	se := schema.NewEngine(env)
	qe := NewQueryEngine(env, se)
	// the integration tests that check cache behavior do not expect a doorkeeper; disable it
	qe.plans = theine.NewStore[PlanCacheKey, *TabletPlan](4*1024*1024, false)
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
	assert.True(t, ok, "expected first consolidator ok")
	r2, ok := qe.consolidator.Create(sql)
	assert.False(t, ok, "expected second consolidator not ok")

	r1.Broadcast()
	r2.Wait()

	return qe
}

func TestConsolidationsUIRedaction(t *testing.T) {
	request, _ := http.NewRequest("GET", "/debug/consolidations", nil)

	sql := "select * from test_db_01 where col = 'secret'"
	redactedSQL := "select * from test_db_01 where col = :col"

	// First with the redaction off
	unRedactedResponse := httptest.NewRecorder()
	qe := runConsolidatedQuery(t, sql)

	qe.handleHTTPConsolidations(unRedactedResponse, request)
	require.Containsf(t, unRedactedResponse.Body.String(), sql, "Response is missing the consolidated query: %v %v", sql, unRedactedResponse.Body.String())

	// Now with the redaction on
	qe.redactUIQuery = true
	redactedResponse := httptest.NewRecorder()
	qe.handleHTTPConsolidations(redactedResponse, request)

	require.NotContainsf(t, redactedResponse.Body.String(), "secret", "Response contains unredacted consolidated query: %v %v", sql, redactedResponse.Body.String())
	require.Containsf(t, redactedResponse.Body.String(), redactedSQL, "Response missing redacted consolidated query: %v %v", redactedSQL, redactedResponse.Body.String())
}

func TestConsolidationsUITagsStreamingAndNonStreaming(t *testing.T) {
	request, _ := http.NewRequest("GET", "/debug/consolidations", nil)

	const sql = "select * from test_db_01 where id = 1"

	db := fakesqldb.New(t)
	defer db.Close()
	qe := newTestQueryEngine(1*time.Second, true, newDBConfigs(db))
	qe.se.Open()
	qe.Open()
	defer qe.Close()

	require.NotNil(t, qe.streamConsolidator, "stream consolidator should be enabled by the default config")

	// Record the same query on both paths so we can confirm they're labelled
	// separately rather than collapsed onto one ambiguous line.
	qe.consolidator.Record(sql)
	qe.streamConsolidator.consolidations.Record(sql)

	response := httptest.NewRecorder()
	qe.handleHTTPConsolidations(response, request)
	body := response.Body.String()

	require.Containsf(t, body, "non-streaming 1: "+sql, "missing non-streaming entry: %v", body)
	require.Containsf(t, body, "streaming 1: "+sql, "missing streaming entry: %v", body)
}

func BenchmarkPlanCacheThroughput(b *testing.B) {
	db := fakesqldb.New(b)
	defer db.Close()

	schematest.AddDefaultQueries(db)

	db.AddQueryPattern(".*", &sqltypes.Result{})

	qe := newTestQueryEngine(10*time.Second, true, newDBConfigs(db))
	qe.se.Open()
	qe.Open()
	defer qe.Close()

	ctx := context.Background()
	logStats := tabletenv.NewLogStats(ctx, "GetPlanStats", streamlog.NewQueryLogConfigForTest())

	for b.Loop() {
		query := fmt.Sprintf("SELECT (a, b, c) FROM test_table_%d", rand.IntN(500))
		_, err := qe.GetPlan(ctx, logStats, query, false, false)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkPlanCache(b *testing.B, db *fakesqldb.DB, par int) {
	b.Helper()

	dbcfgs := newDBConfigs(db)
	cfg := tabletenv.NewDefaultConfig()
	cfg.DB = dbcfgs

	env := tabletenv.NewEnv(vtenv.NewTestEnv(), cfg, "TabletServerTest")
	se := schema.NewEngine(env)
	qe := NewQueryEngine(env, se)

	se.InitDBConfig(dbcfgs.DbaWithDB())
	require.NoError(b, se.Open())
	require.NoError(b, qe.Open())
	defer qe.Close()

	b.SetParallelism(par)
	b.RunParallel(func(pb *testing.PB) {
		ctx := context.Background()
		logStats := tabletenv.NewLogStats(ctx, "GetPlanStats", streamlog.NewQueryLogConfigForTest())

		for pb.Next() {
			query := fmt.Sprintf("SELECT (a, b, c) FROM test_table_%d", rand.IntN(500))
			_, err := qe.GetPlan(ctx, logStats, query, false, false)
			require.NoErrorf(b, err, "bad query: %s", query)
		}
	})
}

func BenchmarkPlanCacheContention(b *testing.B) {
	db := fakesqldb.New(b)
	defer db.Close()

	schematest.AddDefaultQueries(db)

	db.AddQueryPattern(".*", &sqltypes.Result{})

	for par := 1; par <= 8; par *= 2 {
		b.Run(fmt.Sprintf("ContentionLFU-%d", par), func(b *testing.B) {
			benchmarkPlanCache(b, db, par)
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

	schematest.AddDefaultQueries(db)

	db.AddQueryPattern(".*", &sqltypes.Result{})

	dbcfgs := newDBConfigs(db)
	cfg := tabletenv.NewDefaultConfig()
	cfg.DB = dbcfgs
	// config.LFUQueryCacheSizeBytes = 3 * 1024 * 1024

	env := tabletenv.NewEnv(vtenv.NewTestEnv(), cfg, "TabletServerTest")
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
		cacheMode := "lfu"

		out, err := os.Create(path.Join(plotPath, fmt.Sprintf("cache_plot_%d_%s.dat", cfg.QueryCacheMemory, cacheMode)))
		if !assert.NoError(t, err) {
			return
		}
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
		for range totalQueries {
			ctx := t.Context()
			logStats := tabletenv.NewLogStats(ctx, "GetPlanStats", streamlog.NewQueryLogConfigForTest())
			query := sample()

			start := time.Now()
			_, err := qe.GetPlan(ctx, logStats, query, false, false)
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
			return fmt.Sprintf("SELECT (a, b, c) FROM test_table_%d", rand.IntN(5000))
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

func TestAddQueryStats(t *testing.T) {
	fakeSelectPlan := &TabletPlan{
		Plan: &planbuilder.Plan{
			PlanID:    planbuilder.PlanSelect,
			FullQuery: &sqlparser.ParsedQuery{Query: `select * from something where something=123`}, // 43 length
		},
	}
	fakeInsertPlan := &TabletPlan{
		Plan: &planbuilder.Plan{
			PlanID:    planbuilder.PlanInsert,
			FullQuery: &sqlparser.ParsedQuery{Query: `insert into something (id, msg) values(123, 'hello world!')`}, // 59 length
		},
	}
	testcases := []struct {
		name                             string
		plan                             *TabletPlan
		tableName                        string
		tabletType                       topodata.TabletType
		queryCount                       int64
		duration                         time.Duration
		mysqlTime                        time.Duration
		rowsAffected                     int64
		rowsReturned                     int64
		errorCount                       int64
		errorCode                        string
		enablePerWorkloadTableMetrics    bool
		workload                         string
		expectedQueryCounts              string
		expectedQueryCountsWithTableType string
		expectedQueryTimes               string
		expectedQueryRowsAffected        string
		expectedQueryRowsReturned        string
		expectedQueryTextCharsProcessed  string
		expectedQueryErrorCounts         string
		expectedQueryErrorCountsWithCode string
	}{
		{
			name:                             "select query",
			plan:                             fakeSelectPlan,
			tableName:                        "A",
			tabletType:                       topodata.TabletType_PRIMARY,
			queryCount:                       1,
			duration:                         10,
			rowsAffected:                     0,
			rowsReturned:                     15,
			errorCount:                       0,
			errorCode:                        "OK",
			enablePerWorkloadTableMetrics:    false,
			workload:                         "some-workload",
			expectedQueryCounts:              `{"A.Select": 1}`,
			expectedQueryTimes:               `{"A.Select": 10}`,
			expectedQueryRowsAffected:        `{}`,
			expectedQueryRowsReturned:        `{"A.Select": 15}`,
			expectedQueryTextCharsProcessed:  `{"A.Select": 43}`,
			expectedQueryErrorCounts:         `{"A.Select": 0}`,
			expectedQueryErrorCountsWithCode: `{}`,
			expectedQueryCountsWithTableType: `{"A.Select.PRIMARY": 1}`,
		}, {
			name:                             "select query against a replica",
			plan:                             fakeSelectPlan,
			tableName:                        "A",
			tabletType:                       topodata.TabletType_REPLICA,
			queryCount:                       1,
			duration:                         10,
			rowsAffected:                     0,
			rowsReturned:                     15,
			errorCount:                       0,
			errorCode:                        "OK",
			enablePerWorkloadTableMetrics:    false,
			workload:                         "some-workload",
			expectedQueryCounts:              `{"A.Select": 1}`,
			expectedQueryTimes:               `{"A.Select": 10}`,
			expectedQueryRowsAffected:        `{}`,
			expectedQueryRowsReturned:        `{"A.Select": 15}`,
			expectedQueryTextCharsProcessed:  `{"A.Select": 43}`,
			expectedQueryErrorCounts:         `{"A.Select": 0}`,
			expectedQueryErrorCountsWithCode: `{}`,
			expectedQueryCountsWithTableType: `{"A.Select.REPLICA": 1}`,
		}, {
			name:                             "select into query",
			plan:                             fakeSelectPlan,
			tableName:                        "A",
			tabletType:                       topodata.TabletType_PRIMARY,
			queryCount:                       1,
			duration:                         10,
			rowsAffected:                     15,
			rowsReturned:                     0,
			errorCount:                       0,
			errorCode:                        "OK",
			enablePerWorkloadTableMetrics:    false,
			workload:                         "some-workload",
			expectedQueryCounts:              `{"A.Select": 1}`,
			expectedQueryTimes:               `{"A.Select": 10}`,
			expectedQueryRowsAffected:        `{"A.Select": 15}`,
			expectedQueryRowsReturned:        `{"A.Select": 0}`,
			expectedQueryTextCharsProcessed:  `{"A.Select": 43}`,
			expectedQueryErrorCounts:         `{"A.Select": 0}`,
			expectedQueryErrorCountsWithCode: `{}`,
			expectedQueryCountsWithTableType: `{"A.Select.PRIMARY": 1}`,
		}, {
			name:                             "error",
			plan:                             fakeSelectPlan,
			tableName:                        "A",
			tabletType:                       topodata.TabletType_PRIMARY,
			queryCount:                       1,
			duration:                         10,
			rowsAffected:                     0,
			rowsReturned:                     0,
			errorCount:                       1,
			errorCode:                        "RESOURCE_EXHAUSTED",
			enablePerWorkloadTableMetrics:    false,
			workload:                         "some-workload",
			expectedQueryCounts:              `{"A.Select": 1}`,
			expectedQueryTimes:               `{"A.Select": 10}`,
			expectedQueryRowsAffected:        `{}`,
			expectedQueryRowsReturned:        `{"A.Select": 0}`,
			expectedQueryTextCharsProcessed:  `{"A.Select": 43}`,
			expectedQueryErrorCounts:         `{"A.Select": 1}`,
			expectedQueryErrorCountsWithCode: `{"A.Select.RESOURCE_EXHAUSTED": 1}`,
			expectedQueryCountsWithTableType: `{"A.Select.PRIMARY": 1}`,
		}, {
			name:                             "insert query",
			plan:                             fakeInsertPlan,
			tableName:                        "A",
			tabletType:                       topodata.TabletType_PRIMARY,
			queryCount:                       1,
			duration:                         10,
			rowsAffected:                     15,
			rowsReturned:                     0,
			errorCount:                       0,
			errorCode:                        "OK",
			enablePerWorkloadTableMetrics:    false,
			workload:                         "some-workload",
			expectedQueryCounts:              `{"A.Insert": 1}`,
			expectedQueryTimes:               `{"A.Insert": 10}`,
			expectedQueryRowsAffected:        `{"A.Insert": 15}`,
			expectedQueryRowsReturned:        `{}`,
			expectedQueryTextCharsProcessed:  `{"A.Insert": 59}`,
			expectedQueryErrorCounts:         `{"A.Insert": 0}`,
			expectedQueryErrorCountsWithCode: `{}`,
			expectedQueryCountsWithTableType: `{"A.Insert.PRIMARY": 1}`,
		}, {
			name:                             "select query with per workload metrics",
			plan:                             fakeSelectPlan,
			tableName:                        "A",
			tabletType:                       topodata.TabletType_PRIMARY,
			queryCount:                       1,
			duration:                         10,
			rowsAffected:                     0,
			rowsReturned:                     15,
			errorCount:                       0,
			errorCode:                        "OK",
			enablePerWorkloadTableMetrics:    true,
			workload:                         "some-workload",
			expectedQueryCounts:              `{"A.Select.some-workload": 1}`,
			expectedQueryTimes:               `{"A.Select.some-workload": 10}`,
			expectedQueryRowsAffected:        `{}`,
			expectedQueryRowsReturned:        `{"A.Select.some-workload": 15}`,
			expectedQueryTextCharsProcessed:  `{"A.Select.some-workload": 43}`,
			expectedQueryErrorCounts:         `{"A.Select.some-workload": 0}`,
			expectedQueryErrorCountsWithCode: `{}`,
			expectedQueryCountsWithTableType: `{"A.Select.PRIMARY": 1}`,
		}, {
			name:                             "select into query with per workload metrics",
			plan:                             fakeSelectPlan,
			tableName:                        "A",
			tabletType:                       topodata.TabletType_PRIMARY,
			queryCount:                       1,
			duration:                         10,
			rowsAffected:                     15,
			rowsReturned:                     0,
			errorCount:                       0,
			errorCode:                        "OK",
			enablePerWorkloadTableMetrics:    true,
			workload:                         "some-workload",
			expectedQueryCounts:              `{"A.Select.some-workload": 1}`,
			expectedQueryTimes:               `{"A.Select.some-workload": 10}`,
			expectedQueryRowsAffected:        `{"A.Select.some-workload": 15}`,
			expectedQueryRowsReturned:        `{"A.Select.some-workload": 0}`,
			expectedQueryTextCharsProcessed:  `{"A.Select.some-workload": 43}`,
			expectedQueryErrorCounts:         `{"A.Select.some-workload": 0}`,
			expectedQueryErrorCountsWithCode: `{}`,
			expectedQueryCountsWithTableType: `{"A.Select.PRIMARY": 1}`,
		}, {
			name:                             "error with per workload metrics",
			plan:                             fakeSelectPlan,
			tableName:                        "A",
			tabletType:                       topodata.TabletType_PRIMARY,
			queryCount:                       1,
			duration:                         10,
			rowsAffected:                     0,
			rowsReturned:                     0,
			errorCount:                       1,
			errorCode:                        "RESOURCE_EXHAUSTED",
			enablePerWorkloadTableMetrics:    true,
			workload:                         "some-workload",
			expectedQueryCounts:              `{"A.Select.some-workload": 1}`,
			expectedQueryTimes:               `{"A.Select.some-workload": 10}`,
			expectedQueryRowsAffected:        `{}`,
			expectedQueryRowsReturned:        `{"A.Select.some-workload": 0}`,
			expectedQueryTextCharsProcessed:  `{"A.Select.some-workload": 43}`,
			expectedQueryErrorCounts:         `{"A.Select.some-workload": 1}`,
			expectedQueryErrorCountsWithCode: `{"A.Select.RESOURCE_EXHAUSTED": 1}`,
			expectedQueryCountsWithTableType: `{"A.Select.PRIMARY": 1}`,
		}, {
			name:                             "insert query with per workload metrics",
			plan:                             fakeInsertPlan,
			tableName:                        "A",
			tabletType:                       topodata.TabletType_PRIMARY,
			queryCount:                       1,
			duration:                         10,
			rowsAffected:                     15,
			rowsReturned:                     0,
			errorCount:                       0,
			errorCode:                        "OK",
			enablePerWorkloadTableMetrics:    true,
			workload:                         "some-workload",
			expectedQueryCounts:              `{"A.Insert.some-workload": 1}`,
			expectedQueryTimes:               `{"A.Insert.some-workload": 10}`,
			expectedQueryRowsAffected:        `{"A.Insert.some-workload": 15}`,
			expectedQueryRowsReturned:        `{}`,
			expectedQueryTextCharsProcessed:  `{"A.Insert.some-workload": 59}`,
			expectedQueryErrorCounts:         `{"A.Insert.some-workload": 0}`,
			expectedQueryErrorCountsWithCode: `{}`,
			expectedQueryCountsWithTableType: `{"A.Insert.PRIMARY": 1}`,
		},
	}

	t.Parallel()
	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			cfg := tabletenv.NewDefaultConfig()
			cfg.DB = newDBConfigs(fakesqldb.New(t))
			cfg.EnablePerWorkloadTableMetrics = testcase.enablePerWorkloadTableMetrics
			env := tabletenv.NewEnv(vtenv.NewTestEnv(), cfg, "TestAddQueryStats_"+testcase.name)
			se := schema.NewEngine(env)
			qe := NewQueryEngine(env, se)
			qe.AddStats(testcase.plan, testcase.tableName, testcase.workload, testcase.tabletType, testcase.queryCount, testcase.duration, testcase.mysqlTime, testcase.rowsAffected, testcase.rowsReturned, testcase.errorCount, testcase.errorCode)
			assert.Equal(t, testcase.expectedQueryCounts, qe.queryCounts.String())
			assert.Equal(t, testcase.expectedQueryCountsWithTableType, qe.queryCountsWithTabletType.String())
			assert.Equal(t, testcase.expectedQueryTimes, qe.queryTimes.String())
			assert.Equal(t, testcase.expectedQueryRowsAffected, qe.queryRowsAffected.String())
			assert.Equal(t, testcase.expectedQueryRowsReturned, qe.queryRowsReturned.String())
			assert.Equal(t, testcase.expectedQueryTextCharsProcessed, qe.queryTextCharsProcessed.String())
			assert.Equal(t, testcase.expectedQueryErrorCounts, qe.queryErrorCounts.String())
			assert.Equal(t, testcase.expectedQueryErrorCountsWithCode, qe.queryErrorCountsWithCode.String())
		})
	}
}

func TestPlanPoolUnsafe(t *testing.T) {
	tcases := []struct {
		name, query, err string
	}{
		{
			"get_lock named locks are unsafe with server-side connection pooling",
			"select get_lock('foo', 10) from dual",
			"SelectLockFunc not allowed without reserved connection",
		}, {
			"setting system variables must happen inside reserved connections",
			"set sql_safe_updates = false",
			"Set not allowed without reserved connection",
		}, {
			"setting system variables must happen inside reserved connections",
			"set @@sql_safe_updates = false",
			"Set not allowed without reserved connection",
		}, {
			"setting system variables must happen inside reserved connections",
			"set @udv = false",
			"Set not allowed without reserved connection",
		},
	}
	for _, tcase := range tcases {
		t.Run(tcase.name, func(t *testing.T) {
			statement, err := sqlparser.NewTestParser().Parse(tcase.query)
			require.NoError(t, err)
			plan, err := planbuilder.Build(vtenv.NewTestEnv(), statement, map[string]*schema.Table{}, "dbName", false)
			// Plan building will not fail, but it will mark that reserved connection is needed.
			// checking plan is valid will fail.
			require.NoError(t, err)
			require.True(t, plan.NeedsReservedConn)
			err = isValid(plan.PlanID, false, false)
			require.EqualError(t, err, tcase.err)
		})
	}
}
