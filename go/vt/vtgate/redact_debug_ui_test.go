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

package vtgate

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

	"vitess.io/vitess/go/cache/theine"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/streamlog"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/logstats"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// Placeholders standing in for a customer literal and a bind value. They are
// not credentials.
const (
	redactStatement = "select email from customer where ssn = 'custvalue991288'"
	redactLiteral   = "custvalue991288"
	redactClause    = "where ssn"
	redactBindValue = "867530901"
)

func setRedactDebugUIQueries(t *testing.T, cmd string) {
	t.Helper()
	fs := servenv.GetFlagSetFor(cmd)
	require.NotNil(t, fs.Lookup("redact-debug-ui-queries"))
	require.NoError(t, fs.Set("redact-debug-ui-queries", "true"))
	t.Cleanup(func() {
		require.NoError(t, fs.Set("redact-debug-ui-queries", "false"))
	})
}

func redactBindVars() map[string]*querypb.BindVariable {
	return map[string]*querypb.BindVariable{
		"ssn": sqltypes.Int64BindVariable(867530901),
	}
}

func requireStatementHidden(t *testing.T, body string) {
	t.Helper()
	require.NotContains(t, body, redactLiteral)
	require.NotContains(t, body, redactClause)
	require.NotContains(t, body, redactBindValue)
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
	gateQueryLogOnce sync.Once
	gateQueryLog     *streamlog.StreamLogger[*logstats.LogStats]
)

func redactGateQueryLog() *streamlog.StreamLogger[*logstats.LogStats] {
	gateQueryLogOnce.Do(func() {
		gateQueryLog = streamlog.New[*logstats.LogStats]("VTGate", 8)
		// Same registration as Executor.defaultQueryLogger: /debug/querylog.
		gateQueryLog.ServeLogs(QueryLogHandler, streamlog.GetFormatter(gateQueryLog))
	})
	return gateQueryLog
}

// TestRedactDebugUIQueriesHidesStatementOnQuerylog is the streaming /debug/querylog
// page. The request carries no credentials; the default ACL allows DEBUGGING.
func TestRedactDebugUIQueriesHidesStatementOnQuerylog(t *testing.T) {
	setRedactDebugUIQueries(t, "vtgate")
	cfg := streamlog.GetQueryLogConfig()
	require.True(t, cfg.RedactDebugUIQueries)

	addr := startDebugHTTP(t)
	logger := redactGateQueryLog()

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+QueryLogHandler, nil)
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
			statsCfg := cfg
			statsCfg.Format = format
			stats := logstats.NewLogStats(t.Context(), "Execute", redactStatement, "suuid", redactBindVars(), statsCfg)
			stats.EndTime = stats.StartTime.Add(time.Millisecond)
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

// TestRedactDebugUIQueriesHidesStatementOnQuerylogz is /debug/querylogz.
func TestRedactDebugUIQueriesHidesStatementOnQuerylogz(t *testing.T) {
	setRedactDebugUIQueries(t, "vtgate")
	cfg := streamlog.GetQueryLogConfig()
	require.True(t, cfg.RedactDebugUIQueries)

	stats := logstats.NewLogStats(t.Context(), "Execute", redactStatement, "suuid", redactBindVars(), cfg)
	stats.EndTime = stats.StartTime.Add(time.Millisecond)
	ch := make(chan *logstats.LogStats, 1)
	ch <- stats

	req := httptest.NewRequest(http.MethodGet, QueryLogzHandler+"?timeout=1&limit=1", nil)
	rec := httptest.NewRecorder()
	querylogzHandler(ch, rec, req, sqlparser.NewTestParser())

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	require.Contains(t, body, "Execute")
	requireStatementHidden(t, body)
}

// TestRedactDebugUIQueriesHidesStatementOnQueryz is /debug/queryz, the plan cache page.
func TestRedactDebugUIQueriesHidesStatementOnQueryz(t *testing.T) {
	setRedactDebugUIQueries(t, "vtgate")
	require.True(t, streamlog.GetQueryLogConfig().RedactDebugUIQueries)

	executor := executorWithCachedStatement(t, redactStatement)
	req := httptest.NewRequest(http.MethodGet, QueryzHandler, nil)
	rec := httptest.NewRecorder()
	queryzHandler(executor, rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	require.Contains(t, body, `class="low"`)
	requireStatementHidden(t, body)
}

// TestRedactDebugUIQueriesHidesStatementOnQueryPlans is /debug/query_plans.
func TestRedactDebugUIQueriesHidesStatementOnQueryPlans(t *testing.T) {
	setRedactDebugUIQueries(t, "vtgate")
	require.True(t, streamlog.GetQueryLogConfig().RedactDebugUIQueries)

	executor := executorWithCachedStatement(t, redactStatement)
	req := httptest.NewRequest(http.MethodGet, pathQueryPlans, nil)
	rec := httptest.NewRecorder()
	executor.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	require.NotEmpty(t, body)
	requireStatementHidden(t, body)
}

func executorWithCachedStatement(t *testing.T, statement string) *Executor {
	t.Helper()
	plans := theine.NewStore[PlanCacheKey, *engine.Plan](4*1024*1024, false)
	t.Cleanup(plans.Close)
	plan := &engine.Plan{Original: statement}
	plan.AddStats(1, time.Millisecond, 1, 0, 1, 0)
	require.True(t, plans.Set(PlanCacheKey{}, plan, 1, 0))
	return &Executor{
		env:   vtenv.NewTestEnv(),
		plans: plans,
	}
}
