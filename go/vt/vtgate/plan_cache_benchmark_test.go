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
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/cache/theine"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/streamlog"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vtgate/engine"
	econtext "vitess.io/vitess/go/vt/vtgate/executorcontext"
	"vitess.io/vitess/go/vt/vtgate/logstats"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
)

// BenchmarkFetchOrCreatePlanHit measures request-local setup and plan acquisition,
// without tablet execution. Cache admission and cold planning are excluded.
func BenchmarkFetchOrCreatePlanHit(b *testing.B) {
	// Fixture logs can split benchmark result lines when using -count.
	previousLogger := log.SwapLogger(nil)
	b.Cleanup(func() { log.SwapLogger(previousLogger) })

	for _, tc := range []struct {
		name      string
		queries   []string
		normalize bool
		prepared  bool
	}{
		{
			name:    "text",
			queries: []string{"select id from user where id = 1"},
		},
		{
			name:      "normalized_text",
			queries:   []string{"select id from user where id = 1", "select id from user where id = 2"},
			normalize: true,
		},
		{
			name:      "prepared",
			queries:   []string{"select id from user where id = :id"},
			normalize: true,
			prepared:  true,
		},
	} {
		b.Run(tc.name, func(b *testing.B) {
			ctx := b.Context()
			cell := "aa"
			createSandbox(KsTestSharded).VSchema = executorVSchema
			createSandbox(KsTestUnsharded).VSchema = unshardedVSchema
			serv := newSandboxForCells(ctx, []string{cell})
			resolver := newTestResolver(ctx, discovery.NewFakeHealthCheck(nil), serv, cell)
			// Disable the doorkeeper so every case measures guaranteed cache hits.
			plans := theine.NewStore[PlanCacheKey, *engine.Plan](queryPlanCacheMemory, false)
			executor := NewExecutor(ctx, vtenv.NewTestEnv(), serv, cell, resolver, createExecutorConfig(), false, plans, nil, querypb.ExecuteOptions_Gen4, NewDynamicViperConfig())
			b.Cleanup(executor.Close)
			session := econtext.NewSafeSession(&vtgatepb.Session{
				TargetString: KsTestSharded + "@primary",
				Autocommit:   true,
			})
			logConfig := streamlog.NewQueryLogConfigForTest()
			fetch := func(query string) (*engine.Plan, bool, error) {
				bindVars := make(map[string]*querypb.BindVariable)
				if tc.prepared {
					bindVars["id"] = sqltypes.Int64BindVariable(1)
				}
				stats := logstats.NewLogStats(ctx, "Execute", query, "", bindVars, logConfig)
				plan, _, _, err := executor.fetchOrCreatePlan(ctx, session, query, bindVars, tc.normalize, tc.prepared, stats, true)
				return plan, stats.CachedPlan, err
			}
			want, _, err := fetch(tc.queries[0])
			require.NoError(b, err)
			for _, query := range tc.queries {
				plan, cached, err := fetch(query)
				require.NoError(b, err)
				require.True(b, cached)
				require.Same(b, want, plan)
			}

			var plan *engine.Plan
			var cached bool
			i := 0
			b.ReportAllocs()
			for b.Loop() {
				plan, cached, err = fetch(tc.queries[i%len(tc.queries)])
				if err != nil || !cached {
					b.Fatalf("expected a cache hit: cached=%v, err=%v", cached, err)
				}
				i++
			}
			require.Same(b, want, plan)
		})
	}
}
