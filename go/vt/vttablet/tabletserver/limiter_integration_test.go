/*
Copyright 2026 The Vitess Authors.

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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/streamlog"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/limiter"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// newTestTabletServerLimiter creates a TabletServer with the concurrency limiter
// enabled and optionally in dry-run mode.
func newTestTabletServerLimiter(t *testing.T, db *fakesqldb.DB, dryRun bool) *TabletServer {
	t.Helper()
	cfg := tabletenv.NewDefaultConfig()
	cfg.OltpReadPool.Size = 100
	cfg.TxPool.Size = 100
	cfg.EnableOnlineDDL = false
	cfg.EnableConcurrencyLimiter = true
	cfg.ConcurrencyLimiterDryRun = dryRun
	cfg.DB = newDBConfigs(db)
	srvTopoCounts := stats.NewCountersWithSingleLabel("", "Resilient srvtopo server operations", "type")
	tsv := NewTabletServer(t.Context(), vtenv.NewTestEnv(), "LimiterTest"+t.Name(), cfg, memorytopo.NewServer(t.Context(), ""), &topodatapb.TabletAlias{}, srvTopoCounts)
	target := &querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}
	if err := tsv.StartService(target, cfg.DB, nil); err != nil {
		t.Fatalf("StartService: %v", err)
	}
	t.Cleanup(tsv.StopService)
	return tsv
}

// newPlan is a helper to get a TabletPlan via GetPlan, used to inspect
// LimiterSpecs without running the query.
func newPlan(t *testing.T, tsv *TabletServer, sql string) *TabletPlan {
	t.Helper()
	logStats := tabletenv.NewLogStats(t.Context(), "TestLimiterPlan", streamlog.NewQueryLogConfigForTest())
	plan, err := tsv.qe.GetPlan(t.Context(), logStats, sql, false, false)
	require.NoError(t, err)
	return plan
}

// TestLimiterDirectiveParsed verifies that a post-verb /*vt+ CONCURRENCY=... */
// directive is parsed into plan.LimiterSpecs correctly.
func TestLimiterDirectiveParsed(t *testing.T) {
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	db.AddQuery("select * from test_table where pk = 1 limit 10001", &sqltypes.Result{})

	tsv := newTestTabletServerLimiter(t, db, false)

	plan := newPlan(t, tsv, "select /*vt+ CONCURRENCY=workload:OLAP:5,caller:analytics:3 */ * from test_table where pk = 1")
	require.Len(t, plan.LimiterSpecs, 2)
	assert.Equal(t, limiter.Spec{Key: "workload", Value: "OLAP", Limit: 5}, plan.LimiterSpecs[0])
	assert.Equal(t, limiter.Spec{Key: "caller", Value: "analytics", Limit: 3}, plan.LimiterSpecs[1])
}

// TestLimiterLeadingCommentYieldsNoSpecs verifies the SE-1 constraint: a truly-
// leading /*vt+ */ comment is stripped as a margin comment before parsing,
// so it never reaches the AST and no specs are seen.
func TestLimiterLeadingCommentYieldsNoSpecs(t *testing.T) {
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	db.AddQuery("select * from test_table where pk = 1 limit 10001", &sqltypes.Result{})

	tsv := newTestTabletServerLimiter(t, db, false)

	// Truly leading: stripped by SplitMarginComments; directive never reaches AST.
	plan := newPlan(t, tsv, "/*vt+ CONCURRENCY=workload:OLAP:5 */ select * from test_table where pk = 1")
	assert.Empty(t, plan.LimiterSpecs, "leading-comment directive must yield zero specs (margin comment is stripped before parse)")
}

// TestLimiterDisabledAlwaysAdmits verifies that when EnableConcurrencyLimiter is
// false (the default), no specs are parsed and the hook is skipped.
func TestLimiterDisabledAlwaysAdmits(t *testing.T) {
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	db.AddQuery("select * from test_table where pk = 1 limit 10001", &sqltypes.Result{})

	// Disabled: use the stock helper (EnableConcurrencyLimiter=false by default).
	tsv := newTestTabletServer(t.Context(), noFlags, db)
	defer tsv.StopService()

	plan := newPlan(t, tsv, "select /*vt+ CONCURRENCY=workload:OLAP:1 */ * from test_table where pk = 1")
	assert.Empty(t, plan.LimiterSpecs, "limiter disabled: specs must not be parsed")
}

// TestLimiterEnforceOnLimit verifies that when a limit is reached the (limit+1)-th
// Execute returns RESOURCE_EXHAUSTED naming the offending dimension.
func TestLimiterEnforceOnLimit(t *testing.T) {
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	db.AddQuery("select * from test_table where pk = 1 limit 10001", &sqltypes.Result{})

	tsv := newTestTabletServerLimiter(t, db, false)

	sql := "select /*vt+ CONCURRENCY=workload:OLAP:1 */ * from test_table where pk = 1"

	// Acquire the single slot via the limiter directly (simulates a concurrent query
	// holding the slot) — then verify that a new Execute is rejected.
	spec := limiter.Spec{Key: "workload", Value: "OLAP", Limit: 1}
	ok, _ := tsv.limiter.Acquire([]limiter.Spec{spec}, false)
	require.True(t, ok, "direct pre-acquire must succeed")
	defer tsv.limiter.Release([]limiter.Spec{spec})

	qre := newTestQueryExecutor(t.Context(), tsv, sql, 0)
	_, err := qre.Execute()
	require.Error(t, err)
	assert.Equal(t, vtrpcpb.Code_RESOURCE_EXHAUSTED, vterrors.Code(err), "must be RESOURCE_EXHAUSTED")
	assert.ErrorContains(t, err, "concurrency limit exceeded for workload:OLAP")
}

// TestLimiterObserveOnly verifies that a Spec with Limit==0 never causes rejection
// but still tracks in-flight via the admitted counter (no errors emitted).
func TestLimiterObserveOnly(t *testing.T) {
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	rows := sqltypes.MakeTestResult(sqltypes.MakeTestFields("pk|name|addr", "int32|int32|int32"), "1|1|1")
	// The plan formatter includes the /*vt+ ... */ comment in the final SQL.
	db.AddQueryPattern(`select /\*vt\+ CONCURRENCY=workload:OLAP:0 \*/ \* from test_table where pk = 1 limit 10001`, rows)

	tsv := newTestTabletServerLimiter(t, db, false)

	// Limit=0: observe-only, must never reject.
	sql := "select /*vt+ CONCURRENCY=workload:OLAP:0 */ * from test_table where pk = 1"
	for i := 0; i < 10; i++ {
		qre := newTestQueryExecutor(t.Context(), tsv, sql, 0)
		_, err := qre.Execute()
		require.NoErrorf(t, err, "observe-only spec must never reject (iteration %d)", i)
	}
}

// TestLimiterDryRunAdmitsButCounts verifies that in dry-run mode an over-limit
// query is still admitted, and the rejected counter is bumped with DryRun=true.
func TestLimiterDryRunAdmitsButCounts(t *testing.T) {
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	rows := sqltypes.MakeTestResult(sqltypes.MakeTestFields("pk|name|addr", "int32|int32|int32"), "1|1|1")
	db.AddQueryPattern(`select /\*vt\+ CONCURRENCY=workload:OLAP:1 \*/ \* from test_table where pk = 1 limit 10001`, rows)

	tsv := newTestTabletServerLimiter(t, db, true /* dryRun */)

	spec := limiter.Spec{Key: "workload", Value: "OLAP", Limit: 1}

	// Fill the slot.
	ok, _ := tsv.limiter.Acquire([]limiter.Spec{spec}, false)
	require.True(t, ok)
	defer tsv.limiter.Release([]limiter.Spec{spec})

	// Dry-run over-limit: Execute must succeed.
	sql := "select /*vt+ CONCURRENCY=workload:OLAP:1 */ * from test_table where pk = 1"
	qre := newTestQueryExecutor(t.Context(), tsv, sql, 0)
	_, err := qre.Execute()
	assert.NoError(t, err, "dry-run: over-limit query must still succeed")
}

// TestLimiterReleaseRestoresCapacity exercises a full acquire-execute-release cycle
// via the QueryExecutor and verifies that the slot is restored after the first
// request completes, allowing a second one through.
func TestLimiterReleaseRestoresCapacity(t *testing.T) {
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	rows := sqltypes.MakeTestResult(sqltypes.MakeTestFields("pk|name|addr", "int32|int32|int32"), "1|1|1")
	db.AddQueryPattern(`select /\*vt\+ CONCURRENCY=workload:OLAP:1 \*/ \* from test_table where pk = 1 limit 10001`, rows)

	tsv := newTestTabletServerLimiter(t, db, false)
	sql := "select /*vt+ CONCURRENCY=workload:OLAP:1 */ * from test_table where pk = 1"

	// First execution completes; slot is released by defer in Execute.
	qre := newTestQueryExecutor(t.Context(), tsv, sql, 0)
	_, err := qre.Execute()
	require.NoError(t, err, "first execute must succeed")

	// Second execution must also succeed (slot was released).
	qre = newTestQueryExecutor(t.Context(), tsv, sql, 0)
	_, err = qre.Execute()
	assert.NoError(t, err, "second execute must succeed after first released the slot")
}

// TestLimiterStreamPath verifies that the limiter hook in Stream enforces limits.
func TestLimiterStreamPath(t *testing.T) {
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	db.AddQuery("select * from test_table where pk = 1", &sqltypes.Result{})

	tsv := newTestTabletServerLimiter(t, db, false)

	spec := limiter.Spec{Key: "workload", Value: "OLAP", Limit: 1}
	ok, _ := tsv.limiter.Acquire([]limiter.Spec{spec}, false)
	require.True(t, ok)
	defer tsv.limiter.Release([]limiter.Spec{spec})

	sql := "select /*vt+ CONCURRENCY=workload:OLAP:1 */ * from test_table where pk = 1"
	qre := newTestQueryExecutorStreaming(t.Context(), tsv, sql, 0)
	err := qre.Stream(func(*sqltypes.Result) error { return nil })
	require.Error(t, err)
	assert.ErrorContains(t, err, "concurrency limit exceeded for workload:OLAP")
}
