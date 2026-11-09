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
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/pools/smartconnpool"
	"vitess.io/vitess/go/sqltypes"

	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
)

const (
	consolidatorTZUTC   = "set @@time_zone = '+00:00'"
	consolidatorTZPlus5 = "set @@time_zone = '+05:00'"
)

// runConcurrentSelects runs the same read twice, the second one starting once the first one is
// blocked in MySQL, and returns how many times MySQL received the read. The second read either
// reaches MySQL, or joins the first one in the consolidator.
func runConcurrentSelects(t *testing.T, stream bool, query, mysqlQuery string, settingA, settingB *smartconnpool.Setting) int32 {
	db := setUpQueryExecutorTest(t)
	t.Cleanup(db.Close)
	tsv := newTestTabletServer(t.Context(), enableConsolidator, db)
	t.Cleanup(tsv.StopService)
	if stream {
		tsv.qe.streamConsolidator = NewStreamConsolidator(128*1024, 2*1024, returnStreamResult)
	}

	db.AddQuery(consolidatorTZUTC, &sqltypes.Result{})
	db.AddQuery(consolidatorTZPlus5, &sqltypes.Result{})
	db.AddQuery(mysqlQuery, &sqltypes.Result{Fields: getTestTableFields()})
	release := make(chan struct{})
	releaseOnce := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseOnce)
	var arrivals atomic.Int32
	db.SetBeforeFunc(mysqlQuery, func() {
		arrivals.Add(1)
		<-release
	})

	run := func(setting *smartconnpool.Setting) error {
		var qre *QueryExecutor
		if stream {
			qre = newTestQueryExecutorStreaming(t.Context(), tsv, query, 0)
		} else {
			qre = newTestQueryExecutor(t.Context(), tsv, query, 0)
		}
		qre.options = &querypb.ExecuteOptions{Consolidator: querypb.ExecuteOptions_CONSOLIDATOR_ENABLED}
		qre.setting = setting
		if stream {
			return qre.Stream(func(*sqltypes.Result) error { return nil })
		}
		_, err := qre.Execute()
		return err
	}

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	start := func(setting *smartconnpool.Setting) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- run(setting)
		}()
	}

	start(settingA)
	require.Eventually(t, func() bool { return arrivals.Load() == 1 }, 30*time.Second, 5*time.Millisecond)
	start(settingB)
	// The second read has either reached MySQL or is waiting on the first one.
	require.Eventually(t, func() bool {
		return arrivals.Load() == 2 || tsv.qe.consolidator.TotalWaiterCount() == 1 || streamFollowers(tsv) == 1
	}, 30*time.Second, 5*time.Millisecond)

	got := arrivals.Load()
	releaseOnce()
	wg.Wait()
	close(errs)
	for err := range errs {
		assert.NoError(t, err)
	}
	return got
}

// streamFollowers returns the number of streaming reads that follow a leader.
func streamFollowers(tsv *TabletServer) int {
	sc := tsv.qe.streamConsolidator
	if sc == nil {
		return 0
	}
	sc.mu.Lock()
	defer sc.mu.Unlock()
	n := 0
	for _, inflight := range sc.inflight {
		inflight.mu.Lock()
		n += len(inflight.fanout)
		inflight.mu.Unlock()
	}
	return n
}

// Concurrent identical reads with different connection settings must not be consolidated:
// the settings are applied only to the connection that runs the query, so a read that joined
// a leader with other settings would get a result computed under the leader's settings.
// Reads with equal settings, including settings that are separate but equal instances, are
// still consolidated.
func TestConsolidatorKeysOnConnectionSettings(t *testing.T) {
	utc := smartconnpool.NewSetting(consolidatorTZUTC, "set @@time_zone = default")
	plus5 := smartconnpool.NewSetting(consolidatorTZPlus5, "set @@time_zone = default")
	utcCopy := smartconnpool.NewSetting(consolidatorTZUTC, "set @@time_zone = default")

	tests := []struct {
		name           string
		a, b           *smartconnpool.Setting
		wantMySQLCalls int32
	}{
		{"different settings", utc, plus5, 2},
		{"setting and no setting", utc, nil, 2},
		{"same setting", utc, utc, 1},
		{"equal settings", utc, utcCopy, 1},
		{"no settings", nil, nil, 1},
	}
	for _, stream := range []bool{false, true} {
		query, mysqlQuery := "select * from t", "select * from t limit 10001"
		name := "execute"
		if stream {
			mysqlQuery = query
			name = "stream"
		}
		for _, tc := range tests {
			t.Run(name+"/"+tc.name, func(t *testing.T) {
				got := runConcurrentSelects(t, stream, query, mysqlQuery, tc.a, tc.b)
				assert.Equal(t, tc.wantMySQLCalls, got)
			})
		}
	}
}

// The debug page shows the connection settings of a consolidation key, and, when it redacts
// queries, leaves out their values along with the query's, whatever the settings and the
// query contain.
func TestConsolidationsUIShowsSettings(t *testing.T) {
	const (
		sql = "select * from t where a = 'secret'"
		// A literal that looks like the start of the settings, or like their end.
		sqlLikeSettings = "select * from t where a = '/* settings 4: x */ secret'"
		sqlLikeEnd      = "select * from t where a = ' */ secret'"
		// A value that looks like the end of the settings.
		tzLikeEnd = "set @@time_zone = 'x */ secret'"
	)
	redacted := func(sql string) string {
		r, err := sqlparser.NewTestParser().RedactSQLQuery(sql)
		require.NoError(t, err)
		return r
	}
	tests := []struct {
		name        string
		sql         string
		applyQuery  string
		redact      bool
		want        string
		notContains []string
	}{
		{
			name:       "settings",
			sql:        sql,
			applyQuery: consolidatorTZPlus5,
			want:       "/* settings 26: " + consolidatorTZPlus5 + " */ " + sql,
		},
		{
			name:        "redacted settings",
			sql:         sql,
			applyQuery:  consolidatorTZPlus5,
			redact:      true,
			want:        "/* settings redacted */ " + redacted(sql),
			notContains: []string{"secret", "+05:00"},
		},
		{
			name:        "redacted settings that look like their end",
			sql:         sqlLikeEnd,
			applyQuery:  tzLikeEnd,
			redact:      true,
			want:        "/* settings redacted */ " + redacted(sqlLikeEnd),
			notContains: []string{"secret"},
		},
		{
			name:        "redacted query that looks like settings",
			sql:         sqlLikeSettings,
			redact:      true,
			want:        redacted(sqlLikeSettings),
			notContains: []string{"secret"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := setUpQueryExecutorTest(t)
			t.Cleanup(db.Close)
			tsv := newTestTabletServer(t.Context(), enableConsolidator, db)
			t.Cleanup(tsv.StopService)

			qre := newTestQueryExecutor(t.Context(), tsv, tc.sql, 0)
			if tc.applyQuery != "" {
				qre.setting = smartconnpool.NewSetting(tc.applyQuery, "set @@time_zone = default")
			}
			key := qre.consolidationKey(tc.sql)
			q, ok := tsv.qe.consolidator.Create(key)
			require.True(t, ok)
			q.Broadcast()
			tsv.qe.consolidator.Record(key)
			tsv.qe.redactUIQuery = tc.redact

			response := httptest.NewRecorder()
			request, err := http.NewRequest("GET", "/debug/consolidations", nil)
			require.NoError(t, err)
			tsv.qe.handleHTTPConsolidations(response, request)
			assert.Equal(t, "Length: 1\n1: "+tc.want+"\n", response.Body.String())
			for _, s := range tc.notContains {
				assert.NotContains(t, response.Body.String(), s)
			}
		})
	}
}
