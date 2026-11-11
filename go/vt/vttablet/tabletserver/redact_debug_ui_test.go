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
	"bufio"
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/streamlog"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/planbuilder"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// Placeholders standing in for a customer literal and a bind value. They are
// not credentials.
const (
	redactStatement = "select email from customer where ssn = 'custvalue991288'"
	redactLiteral   = "custvalue991288"
	redactClause    = "where ssn"
	redactBindValue = "867530901"
	redactRewritten = "rewritten44821"
	tabletQueryLog  = "/debug/querylog"
)

func setRedactDebugUIQueries(t *testing.T) {
	t.Helper()
	fs := servenv.GetFlagSetFor("vttablet")
	require.NotNil(t, fs.Lookup("redact-debug-ui-queries"))
	require.NoError(t, fs.Set("redact-debug-ui-queries", "true"))
	t.Cleanup(func() {
		require.NoError(t, fs.Set("redact-debug-ui-queries", "false"))
	})
}

func requireStatementHidden(t *testing.T, body string) {
	t.Helper()
	require.NotContains(t, body, redactLiteral)
	require.NotContains(t, body, redactClause)
	require.NotContains(t, body, redactBindValue)
	require.NotContains(t, body, redactRewritten)
}

func startDebugHTTP(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- servenv.HTTPServe(ln)
	}()
	t.Cleanup(func() {
		ln.Close()
		<-serveErr
	})
	return ln.Addr().String()
}

var (
	tabletQueryLogOnce sync.Once
	tabletQueryLogger  *streamlog.StreamLogger[*tabletenv.LogStats]
)

func redactTabletQueryLog() *streamlog.StreamLogger[*tabletenv.LogStats] {
	tabletQueryLogOnce.Do(func() {
		tabletQueryLogger = streamlog.New[*tabletenv.LogStats]("TabletServer", 8)
		// Same registration as tabletenv.Init when --query-log-stream-handler
		// is left at its default of /debug/querylog.
		tabletQueryLogger.ServeLogs(tabletQueryLog, streamlog.GetFormatter(tabletQueryLogger))
	})
	return tabletQueryLogger
}

func redactTabletStats(ctx context.Context) *tabletenv.LogStats {
	stats := tabletenv.NewLogStats(ctx, "Execute", streamlog.GetQueryLogConfig())
	stats.OriginalSQL = redactStatement
	stats.BindVariables = map[string]*querypb.BindVariable{
		"ssn": sqltypes.Int64BindVariable(867530901),
	}
	stats.AddRewrittenSQL("select email from customer where ssn = '"+redactRewritten+"'", time.Now())
	stats.EndTime = stats.StartTime.Add(time.Millisecond)
	return stats
}

// TestRedactDebugUIQueriesHidesStatementOnQuerylog is the tablet streaming
// /debug/querylog. The request carries no credentials; the default ACL allows
// DEBUGGING.
func TestRedactDebugUIQueriesHidesStatementOnQuerylog(t *testing.T) {
	setRedactDebugUIQueries(t)
	cfg := streamlog.GetQueryLogConfig()
	require.True(t, cfg.RedactDebugUIQueries)

	addr := startDebugHTTP(t)
	logger := redactTabletQueryLog()

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+tabletQueryLog, nil)
	require.NoError(t, err)

	type got struct {
		line string
		err  error
	}
	ch := make(chan got, 8)
	go func() {
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			ch <- got{err: err}
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			ch <- got{err: fmt.Errorf("status %d", resp.StatusCode)}
			return
		}
		ch <- got{line: "ready"}
		reader := bufio.NewReader(resp.Body)
		for {
			line, err := reader.ReadString('\n')
			ch <- got{line: line, err: err}
			if err != nil {
				return
			}
		}
	}()

	select {
	case g := <-ch:
		require.NoError(t, g.err)
		require.Equal(t, "ready", g.line)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	for _, format := range []string{streamlog.QueryLogFormatText, streamlog.QueryLogFormatJSON} {
		t.Run(format, func(t *testing.T) {
			stats := redactTabletStats(t.Context())
			stats.Config.Format = format
			logger.Send(stats)

			select {
			case g := <-ch:
				require.NoError(t, g.err)
				require.Contains(t, g.line, "Execute")
				requireStatementHidden(t, g.line)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		})
	}
}

// TestRedactDebugUIQueriesHidesStatementOnQuerylogz is the tablet /querylogz page.
func TestRedactDebugUIQueriesHidesStatementOnQuerylogz(t *testing.T) {
	setRedactDebugUIQueries(t)
	require.True(t, streamlog.GetQueryLogConfig().RedactDebugUIQueries)

	stats := redactTabletStats(t.Context())
	ch := make(chan *tabletenv.LogStats, 1)
	ch <- stats

	req := httptest.NewRequest(http.MethodGet, "/querylogz?timeout=1&limit=1", nil)
	rec := httptest.NewRecorder()
	querylogzHandler(ch, rec, req, sqlparser.NewTestParser())

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	require.Contains(t, body, "Execute")
	requireStatementHidden(t, body)
}

func queryEngineWithCachedStatement(t *testing.T, statement string) *QueryEngine {
	t.Helper()
	qe := newTestQueryEngine(10*time.Second, true, &dbconfigs.DBConfigs{})
	plan := &TabletPlan{
		Original: statement,
		Plan: &planbuilder.Plan{
			Table:  &schema.Table{Name: sqlparser.NewIdentifierCS("customer")},
			PlanID: planbuilder.PlanSelect,
		},
	}
	plan.AddStats(1, time.Millisecond, time.Millisecond, 0, 1, 0)
	require.True(t, qe.plans.Set(PlanCacheKey(statement), plan, 1, 0))
	return qe
}

// TestRedactDebugUIQueriesHidesStatementOnQueryz is the tablet /queryz page.
func TestRedactDebugUIQueriesHidesStatementOnQueryz(t *testing.T) {
	setRedactDebugUIQueries(t)
	require.True(t, streamlog.GetQueryLogConfig().RedactDebugUIQueries)

	qe := queryEngineWithCachedStatement(t, redactStatement)
	req := httptest.NewRequest(http.MethodGet, "/queryz", nil)
	rec := httptest.NewRecorder()
	queryzHandler(qe, rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	require.Contains(t, body, `class="low"`)
	requireStatementHidden(t, body)
}

// TestRedactDebugUIQueriesHidesStatementOnTabletPlans is /debug/tablet_plans.
func TestRedactDebugUIQueriesHidesStatementOnTabletPlans(t *testing.T) {
	setRedactDebugUIQueries(t)
	require.True(t, streamlog.GetQueryLogConfig().RedactDebugUIQueries)

	qe := queryEngineWithCachedStatement(t, redactStatement)
	req := httptest.NewRequest(http.MethodGet, "/debug/tablet_plans", nil)
	rec := httptest.NewRecorder()
	qe.handleHTTPQueryPlans(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	require.Contains(t, body, "PlanID")
	requireStatementHidden(t, body)
}

// TestRedactDebugUIQueriesHidesStatementOnQueryStats is /debug/query_stats.
func TestRedactDebugUIQueriesHidesStatementOnQueryStats(t *testing.T) {
	setRedactDebugUIQueries(t)
	require.True(t, streamlog.GetQueryLogConfig().RedactDebugUIQueries)

	qe := queryEngineWithCachedStatement(t, redactStatement)
	req := httptest.NewRequest(http.MethodGet, "/debug/query_stats", nil)
	rec := httptest.NewRecorder()
	qe.handleHTTPQueryStats(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	require.Contains(t, body, "QueryCount")
	requireStatementHidden(t, body)
}

// TestRedactDebugUIQueriesHidesStatementOnConsolidations is /debug/consolidations.
func TestRedactDebugUIQueriesHidesStatementOnConsolidations(t *testing.T) {
	setRedactDebugUIQueries(t)
	require.True(t, streamlog.GetQueryLogConfig().RedactDebugUIQueries)

	qe := newTestQueryEngine(time.Second, true, &dbconfigs.DBConfigs{})
	first, ok := qe.consolidator.Create(redactStatement)
	require.True(t, ok)
	duplicate, ok := qe.consolidator.Create(redactStatement)
	require.False(t, ok)
	first.Broadcast()
	duplicate.Wait()
	require.NotEmpty(t, qe.consolidator.Items())

	req := httptest.NewRequest(http.MethodGet, "/debug/consolidations", nil)
	rec := httptest.NewRecorder()
	qe.handleHTTPConsolidations(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	require.Contains(t, body, "Length:")
	// The consolidations page already strips literals when its redaction bool is set.
	// The operator flag never sets that bool, so the literal is still published.
	require.NotContains(t, body, redactLiteral)
}
