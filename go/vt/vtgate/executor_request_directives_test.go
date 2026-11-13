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
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	econtext "vitess.io/vitess/go/vt/vtgate/executorcontext"
	"vitess.io/vitess/go/vt/vtgate/logstats"
	"vitess.io/vitess/go/vt/vttablet/sandboxconn"
)

// sentToTablets is what a statement sent to the sandbox tablets, and how the
// query log recorded it.
type sentToTablets struct {
	queries [][]string
	options [][]string
	logSQL  string
	cached  bool
}

// TestRequestDirectivesSharePlan checks that a request finds the plan that
// another request built when the two differ only in their request-only
// comment directives, and that it sends vttablet the same SQL and execute
// options, and logs the same SQL, as it does when it doesn't use the plan
// cache. The SQL carries the request's own directive block wherever it
// carries one without the plan cache.
func TestRequestDirectivesSharePlan(t *testing.T) {
	executor, sbc1, sbc2, sbclookup, ctx := createExecutorEnvWithConfig(t, createExecutorConfigWithNormalizer())
	tablets := []*sandboxconn.SandboxConn{sbc1, sbc2, sbclookup}
	logChan := executor.queryLogger.Subscribe("Test")
	t.Cleanup(func() { executor.queryLogger.Unsubscribe(logChan) })

	// run runs sql, with the plan cache unless skipCache is set, and returns
	// what it sent. It doesn't fail t, as it runs inside Eventually.
	run := func(sql string, stream, skipCache bool) (sentToTablets, error) {
		for _, tablet := range tablets {
			tablet.ClearQueries()
			tablet.ClearOptions()
		}
		for getQueryLog(logChan) != nil {
		}

		session := econtext.NewAutocommitSession(&vtgatepb.Session{
			TargetString: "@primary",
			Options:      &querypb.ExecuteOptions{SkipQueryPlanCache: skipCache},
		})
		method := "TestExecute"
		var err error
		if stream {
			method = "TestExecuteStream"
			err = executor.StreamExecute(ctx, nil, method, session, sql, nil, false, func(*sqltypes.Result) error { return nil })
		} else {
			_, err = executorExecSession(ctx, executor, session, sql, nil)
		}
		if err != nil {
			return sentToTablets{}, err
		}

		var sent sentToTablets
		for _, tablet := range tablets {
			var queries, options []string
			for _, query := range tablet.GetQueries() {
				queries = append(queries, query.Sql)
			}
			for _, o := range tablet.GetOptions() {
				options = append(options, fmt.Sprintf("workload=%q priority=%q consolidator=%v", o.GetWorkloadName(), o.GetPriority(), o.GetConsolidator()))
			}
			// The queries of a statement can reach a tablet concurrently.
			slices.Sort(queries)
			sent.queries = append(sent.queries, queries)
			sent.options = append(sent.options, options)
		}
		var logStats *logstats.LogStats
		for ls := getQueryLog(logChan); ls != nil; ls = getQueryLog(logChan) {
			if ls.Method == method {
				logStats = ls
			}
		}
		if logStats == nil {
			return sentToTablets{}, fmt.Errorf("no query log for %s", sql)
		}
		sent.logSQL = logStats.SQL
		sent.cached = logStats.CachedPlan
		return sent, nil
	}

	const (
		// SCATTER_ERRORS_AS_WARNINGS stays in the marker blocks, and the two
		// requests share a plan only because they agree on it.
		builder = "/*vt+ WORKLOAD_NAME=builder PRIORITY=10 SCATTER_ERRORS_AS_WARNINGS */"
		request = "/*vt+ PRIORITY=20 SCATTER_ERRORS_AS_WARNINGS WORKLOAD_NAME=request CONSOLIDATOR=disabled IGNORE_MAX_MEMORY_ROWS=1 */"
	)
	tests := []struct {
		sql    string
		stream bool
	}{
		{sql: "select %s id from user where id = 1"},
		{sql: "select %s id, name from user order by id"},
		{sql: "select %s u.id, e.col from user u join user_extra e on u.col = e.col"},
		{sql: "select %s id from user where id = 1 union select id from music where id = 2"},
		{sql: "select %s get_lock('lock name', 10) from dual"},
		{sql: "insert %s into user(id, v, name) values (1, 2, 'myname')"},
		{sql: "insert %s into user(v, name) values (2, 'myname')"},
		{sql: "update %s user set v = 3 where id = 1"},
		{sql: "delete %s from user where id = 1"},
		{sql: "select %s id, name from user", stream: true},
	}
	for _, tc := range tests {
		t.Run(tc.sql, func(t *testing.T) {
			// The plan cache stores a plan in the background, so wait until
			// the builder finds its own plan.
			require.Eventually(t, func() bool {
				sent, err := run(fmt.Sprintf(tc.sql, builder), tc.stream, false)
				return err == nil && sent.cached
			}, 30*time.Second, 10*time.Millisecond)

			sql := fmt.Sprintf(tc.sql, request)
			got, err := run(sql, tc.stream, false)
			require.NoError(t, err)
			require.True(t, got.cached, "the request didn't find the plan that the builder built")

			want, err := run(sql, tc.stream, true)
			require.NoError(t, err)
			assert.False(t, want.cached)
			assert.Equal(t, want.queries, got.queries)
			assert.Equal(t, want.options, got.options)
			assert.Equal(t, want.logSQL, got.logSQL)
			assert.Contains(t, got.logSQL, request)
			assert.NotContains(t, strings.Join(append(flatten(got.queries), got.logSQL), "\n"), "VTGATE_REQUEST_DIRECTIVES")
		})
	}
}

func flatten(queries [][]string) []string {
	var all []string
	for _, q := range queries {
		all = append(all, q...)
	}
	return all
}

// TestPlanWithMarkersIsKeptApart checks that a statement that VTGate plans as
// written never finds a plan that was built with marker blocks, even when its
// text is that plan's text. That plan's queries carry markers that only a
// request that replaced its own directives restores, and a binary protocol
// prepared statement would apply the hints of the request that built it.
func TestPlanWithMarkersIsKeptApart(t *testing.T) {
	executor, sbc1, sbc2, _, ctx := createExecutorEnvWithConfig(t, createExecutorConfigWithNormalizer())
	session := &vtgatepb.Session{TargetString: "@primary", Autocommit: true}

	_, err := executorExec(ctx, executor, session, "select /*vt+ WORKLOAD_NAME=builder */ id from user", nil)
	require.NoError(t, err)
	var withMarkers string
	require.Eventually(t, func() bool {
		executor.ForEachPlan(func(p *engine.Plan) bool {
			if strings.Contains(p.Original, sqlparser.DirectiveRequestDirectives) {
				withMarkers = p.Original
			}
			return true
		})
		return withMarkers != ""
	}, 30*time.Second, 10*time.Millisecond)

	for _, sbc := range []*sandboxconn.SandboxConn{sbc1, sbc2} {
		sbc.ClearQueries()
		sbc.ClearOptions()
	}
	_, _, err = executorPrepare(ctx, executor, session, withMarkers)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, nil, "TestExecute", econtext.NewSafeSession(session), withMarkers, nil, true)
	require.NoError(t, err)

	for _, sbc := range []*sandboxconn.SandboxConn{sbc1, sbc2} {
		require.NotEmpty(t, sbc.GetQueries())
		for _, options := range sbc.GetOptions() {
			assert.Empty(t, options.GetWorkloadName())
		}
	}
}
