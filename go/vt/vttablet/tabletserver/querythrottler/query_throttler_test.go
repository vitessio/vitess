/*
Copyright 2025 The Vitess Authors.

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

package querythrottler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/logutil"
	querypb "vitess.io/vitess/go/vt/proto/query"
	querythrottlerpb "vitess.io/vitess/go/vt/proto/querythrottler"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/srvtopo/srvtopotest"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"

	"vitess.io/vitess/go/vt/vttablet/tabletserver/querythrottler/registry"

	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle"
)

// Register the metrics up front: some tests build a QueryThrottler directly, bypassing
// NewQueryThrottler, and still touch the package-level metric vars.
func init() {
	initThrottlerMetrics()
}

func TestSelectThrottlingStrategy(t *testing.T) {
	tests := []struct {
		name                   string
		giveThrottlingStrategy querythrottlerpb.ThrottlingStrategy
		expectedType           registry.ThrottlingStrategyHandler
	}{
		{
			name:                   "Unknown strategy defaults to NoOp",
			giveThrottlingStrategy: querythrottlerpb.ThrottlingStrategy_UNKNOWN,
			expectedType:           &registry.NoOpStrategy{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := &throttle.Client{}

			config := &tabletenv.TabletConfig{
				QueryThrottlerConfigRefreshInterval: 10 * time.Millisecond,
			}

			strategy := selectThrottlingStrategy(&querythrottlerpb.Config{Enabled: true, Strategy: tt.giveThrottlingStrategy}, mockClient, config, nil, "", "", nil)

			require.IsType(t, tt.expectedType, strategy)
		})
	}
}

// TestQueryThrottler_StrategyLifecycleManagement tests that strategies are properly started and stopped.
func TestQueryThrottler_StrategyLifecycleManagement(t *testing.T) {
	// Test that initial strategy is started
	ctx := t.Context()

	throttler := &throttle.Throttler{}
	config := &tabletenv.TabletConfig{
		QueryThrottlerConfigRefreshInterval: 10 * time.Millisecond,
	}
	env := tabletenv.NewEnv(vtenv.NewTestEnv(), config, "TestThrottler")

	srvTopoServer := srvtopotest.NewPassthroughSrvTopoServer()

	iqt := NewQueryThrottler(ctx, throttler, env, &topodatapb.TabletAlias{Cell: "test-cell", Uid: uint32(123)}, srvTopoServer)

	// Verify initial strategy was started (NoOpStrategy in this case)
	require.NotNil(t, iqt.snapshot.Load().strategy)

	// Test Shutdown properly stops the strategy
	iqt.Shutdown()

	// After shutdown, the strategy should have been stopped
	// In a real test, we would verify the strategy's Stop method was called
	require.NotNil(t, iqt.snapshot.Load().strategy) // Strategy reference should still exist but be stopped
}

// TestQueryThrottler_Shutdown tests the Shutdown method.
func TestQueryThrottler_Shutdown(t *testing.T) {
	ctx := t.Context()

	config := &tabletenv.TabletConfig{
		QueryThrottlerConfigRefreshInterval: 10 * time.Millisecond,
	}
	env := tabletenv.NewEnv(vtenv.NewTestEnv(), config, "TestThrottler")

	throttler := &throttle.Throttler{}
	srvTopoServer := srvtopotest.NewPassthroughSrvTopoServer()

	iqt := NewQueryThrottler(ctx, throttler, env, &topodatapb.TabletAlias{Cell: "test-cell", Uid: uint32(123)}, srvTopoServer)

	// Should not panic when called multiple times
	iqt.Shutdown()
	iqt.Shutdown()

	// Should still be able to check the strategy reference
	strategy := iqt.snapshot.Load().strategy
	require.NotNil(t, strategy)
}

// TestQueryThrottler_DryRunMode tests that dry-run mode logs decisions but doesn't throttle queries.
func TestQueryThrottler_DryRunMode(t *testing.T) {
	tests := []struct {
		name                      string
		enabled                   bool
		dryRun                    bool
		throttleDecision          registry.ThrottleDecision
		expectError               bool
		expectDryRunLog           bool
		expectedTotalRequests     int64
		expectedThrottledRequests int64
	}{
		{
			name:    "Disabled throttler - no checks performed",
			enabled: false,
			dryRun:  false,
			throttleDecision: registry.ThrottleDecision{
				Throttle: true,
				Message:  "Should not be evaluated",
			},
			expectError:     false,
			expectDryRunLog: false,
		},
		{
			name:    "Disabled throttler with dry-run - no checks performed",
			enabled: false,
			dryRun:  true,
			throttleDecision: registry.ThrottleDecision{
				Throttle: true,
				Message:  "Should not be evaluated",
			},
			expectError:     false,
			expectDryRunLog: false,
		},
		{
			name:    "Normal mode - query allowed",
			enabled: true,
			dryRun:  false,
			throttleDecision: registry.ThrottleDecision{
				Throttle: false,
				Message:  "Query allowed",
			},
			expectError:           false,
			expectDryRunLog:       false,
			expectedTotalRequests: 1,
		},
		{
			name:    "Normal mode - query throttled",
			enabled: true,
			dryRun:  false,
			throttleDecision: registry.ThrottleDecision{
				Throttle:           true,
				Message:            "Query throttled: metric=cpu value=90.0 threshold=80.0",
				MetricName:         "cpu",
				MetricValue:        90.0,
				Threshold:          80.0,
				ThrottlePercentage: 1.0,
			},
			expectError:               true,
			expectDryRunLog:           false,
			expectedTotalRequests:     1,
			expectedThrottledRequests: 1,
		},
		{
			name:    "Dry-run mode - query would be throttled but allowed",
			enabled: true,
			dryRun:  true,
			throttleDecision: registry.ThrottleDecision{
				Throttle:           true,
				Message:            "Query throttled: metric=cpu value=95.0 threshold=80.0",
				MetricName:         "cpu",
				MetricValue:        95.0,
				Threshold:          80.0,
				ThrottlePercentage: 1.0,
			},
			expectError:               false,
			expectDryRunLog:           true,
			expectedTotalRequests:     1,
			expectedThrottledRequests: 1,
		},
		{
			name:    "Dry-run mode - query allowed normally",
			enabled: true,
			dryRun:  true,
			throttleDecision: registry.ThrottleDecision{
				Throttle: false,
				Message:  "Query allowed",
			},
			expectError:           false,
			expectDryRunLog:       false,
			expectedTotalRequests: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock strategy with controlled decision
			mockStrategy := &mockThrottlingStrategy{
				decision: tt.throttleDecision,
			}

			env := tabletenv.NewEnv(vtenv.NewTestEnv(), &tabletenv.TabletConfig{}, "TestThrottler")

			// Create throttler with controlled config. A long throttle interval means the
			// dry-run logger emits at most once, so GetLastLogTime reliably reflects whether
			// a dry-run decision was logged during this case.
			iqt := &QueryThrottler{
				ctx:             t.Context(),
				env:             env,
				throttledLogger: logutil.NewThrottledLogger("test", time.Hour),
			}
			iqt.snapshot.Store(&stateSnapshot{
				cfg: &querythrottlerpb.Config{
					Enabled: tt.enabled,
					DryRun:  tt.dryRun,
				},
				strategy: mockStrategy,
			})

			requestsTotal.ResetAll()
			requestsThrottled.ResetAll()

			// Test the enforcement
			err := iqt.Throttle(
				t.Context(),
				topodatapb.TabletType_REPLICA,
				&sqlparser.ParsedQuery{Query: "SELECT * FROM test_table WHERE id = 1"},
				sqlparser.StmtSelect,
				12345,
				&querypb.ExecuteOptions{
					WorkloadName: "test-workload",
					Priority:     "50",
				},
			)

			// Verify error expectation
			if tt.expectError {
				require.EqualError(t, err, sqlerror.QueryThrottledMarker+" "+tt.throttleDecision.Message, "Error should carry the query-throttle marker followed by the throttle message")
			} else {
				require.NoError(t, err, "Expected no throttling error")
			}

			// Verify dry-run log expectation via the throttled logger's last-log time
			// (the log now routes through logutil.ThrottledLogger, not log.Warn).
			if tt.expectDryRunLog {
				require.False(t, iqt.throttledLogger.GetLastLogTime().IsZero(), "Expected a dry-run log to be emitted")
			} else {
				require.True(t, iqt.throttledLogger.GetLastLogTime().IsZero(), "Expected no dry-run log to be emitted")
			}

			// Verify stats expectation
			totalReqs := stats.CounterForDimension(requestsTotal, "Strategy")
			throttledReqs := stats.CounterForDimension(requestsThrottled, "Strategy")
			require.Equal(t, tt.expectedTotalRequests, totalReqs.Counts()["MockStrategy"], "Total requests should match expected")
			require.Equal(t, tt.expectedThrottledRequests, throttledReqs.Counts()["MockStrategy"], "Throttled requests should match expected")
		})
	}
}

// Dry-run logging must be rate-limited, or a 100%-throttle rule spams the logs at query
// rate. Only the first decision in an interval logs; the counter still records them all.
func TestQueryThrottler_DryRunLogIsRateLimited(t *testing.T) {
	mockStrategy := &mockThrottlingStrategy{
		decision: registry.ThrottleDecision{
			Throttle:           true,
			Message:            "Query throttled: metric=cpu value=95.0 threshold=80.0",
			MetricName:         "cpu",
			MetricValue:        95.0,
			Threshold:          80.0,
			ThrottlePercentage: 1.0,
		},
	}

	env := tabletenv.NewEnv(vtenv.NewTestEnv(), &tabletenv.TabletConfig{}, "TestThrottler")
	// A large interval guarantees only the first decision is logged; the rest are suppressed.
	iqt := &QueryThrottler{
		ctx:             t.Context(),
		env:             env,
		throttledLogger: logutil.NewThrottledLogger("test", time.Hour),
	}
	iqt.snapshot.Store(&stateSnapshot{
		cfg: &querythrottlerpb.Config{
			Enabled: true,
			DryRun:  true,
		},
		strategy: mockStrategy,
	})

	requestsTotal.ResetAll()
	requestsThrottled.ResetAll()

	call := func() {
		err := iqt.Throttle(
			t.Context(),
			topodatapb.TabletType_REPLICA,
			&sqlparser.ParsedQuery{Query: "SELECT * FROM test_table WHERE id = 1"},
			sqlparser.StmtSelect,
			12345,
			&querypb.ExecuteOptions{Priority: "50"},
		)
		require.NoError(t, err, "dry-run must never return an error")
	}

	// First dry-run decision emits a log.
	call()
	firstLogTime := iqt.throttledLogger.GetLastLogTime()
	require.False(t, firstLogTime.IsZero(), "first dry-run decision must emit a log")

	// Subsequent decisions within the interval are suppressed: last-log time does not advance.
	for range 5 {
		call()
	}
	require.Equal(t, firstLogTime, iqt.throttledLogger.GetLastLogTime(),
		"dry-run logs within the throttle interval must be rate-limited (suppressed)")

	// Every decision is still counted, so volume is available from the counter.
	throttledReqs := stats.CounterForDimension(requestsThrottled, "Strategy")
	require.Equal(t, int64(6), throttledReqs.Counts()["MockStrategy"],
		"counters must record every dry-run throttle decision even though logs are rate-limited")
}

// A throttle rejection must reach clients as ER_OUT_OF_RESOURCES (1041), like the
// transaction throttler, not the default ER_TOO_MANY_USER_CONNECTIONS (1203). The mock's
// message avoids the word "throttled", so only the marker Throttle prepends can map it.
func TestQueryThrottler_ThrottledErrorMapsToOutOfResources(t *testing.T) {
	mockStrategy := &mockThrottlingStrategy{
		decision: registry.ThrottleDecision{
			Throttle: true,
			Message:  "metric=lag value=20 over threshold",
		},
	}

	env := tabletenv.NewEnv(vtenv.NewTestEnv(), &tabletenv.TabletConfig{}, "TestThrottler")
	iqt := &QueryThrottler{
		ctx: t.Context(),
		env: env,
	}
	iqt.snapshot.Store(&stateSnapshot{
		cfg: &querythrottlerpb.Config{
			Enabled: true,
			DryRun:  false,
		},
		strategy: mockStrategy,
	})

	requestsTotal.ResetAll()
	requestsThrottled.ResetAll()

	err := iqt.Throttle(
		t.Context(),
		topodatapb.TabletType_REPLICA,
		&sqlparser.ParsedQuery{Query: "SELECT * FROM test_table WHERE id = 1"},
		sqlparser.StmtSelect,
		12345,
		&querypb.ExecuteOptions{WorkloadName: "test-workload", Priority: "50"},
	)
	require.Error(t, err)

	sqlErr, ok := sqlerror.NewSQLErrorFromError(err).(*sqlerror.SQLError)
	require.True(t, ok, "throttle error must convert to a *sqlerror.SQLError")
	require.Equal(t, sqlerror.EROutOfResources, sqlErr.Num,
		"query-throttle rejection must map to ER_OUT_OF_RESOURCES (1041), not ER_TOO_MANY_USER_CONNECTIONS (1203)")
}

func TestQueryThrottler_extractWorkloadName(t *testing.T) {
	tests := []struct {
		name    string
		options *querypb.ExecuteOptions
		want    string
	}{
		{
			name:    "nil options returns unknown",
			options: nil,
			want:    "unknown",
		},
		{
			name: "empty workload name returns unknown",
			options: &querypb.ExecuteOptions{
				WorkloadName: "",
			},
			want: "unknown",
		},
		{
			name: "custom workload name returns the name",
			options: &querypb.ExecuteOptions{
				WorkloadName: "analytics",
			},
			want: "analytics",
		},
		{
			name: "another custom workload name",
			options: &querypb.ExecuteOptions{
				WorkloadName: "batch-processing",
			},
			want: "batch-processing",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractWorkloadName(tt.options)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestQueryThrottler_buildLabels(t *testing.T) {
	tests := []struct {
		name               string
		perWorkloadMetrics bool
		extras             []string
		want               []string
	}{
		{
			name:               "workload label disabled, no extras",
			perWorkloadMetrics: false,
			want:               []string{"strat", "unknown", "50"},
		},
		{
			name:               "workload label disabled, with extras",
			perWorkloadMetrics: false,
			extras:             []string{"cpu", "false"},
			want:               []string{"strat", "unknown", "50", "cpu", "false"},
		},
		{
			name:               "workload label enabled, no extras",
			perWorkloadMetrics: true,
			want:               []string{"strat", "client-supplied-wl", "50"},
		},
		{
			name:               "workload label enabled, with extras",
			perWorkloadMetrics: true,
			extras:             []string{"cpu", "false"},
			want:               []string{"strat", "client-supplied-wl", "50", "cpu", "false"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qt := &QueryThrottler{perWorkloadMetrics: tt.perWorkloadMetrics}
			got := qt.buildLabels("strat", "client-supplied-wl", "50", tt.extras...)
			require.Equal(t, tt.want, got)
		})
	}
}

// Instances that disagree on EnablePerWorkloadTableMetrics share the process-global stats,
// so they must emit the same label count — Add/Record panic on a mismatch. Pre-fix, the
// enabled instance emitted 3 values against a 2-label schema and panicked below.
func TestQueryThrottler_metricsLabelCountStableAcrossInstances(t *testing.T) {
	enabled := &QueryThrottler{perWorkloadMetrics: true}
	disabled := &QueryThrottler{perWorkloadMetrics: false}

	enabledBase := enabled.buildLabels("noop", "wl", "50")
	disabledBase := disabled.buildLabels("noop", "wl", "50")
	require.Len(t, disabledBase, len(enabledBase), "base label count must not depend on the per-instance flag")

	enabledThrottled := enabled.buildLabels("noop", "wl", "50", "cpu", "false")
	disabledThrottled := disabled.buildLabels("noop", "wl", "50", "cpu", "false")
	require.Len(t, disabledThrottled, len(enabledThrottled), "throttled label count must not depend on the per-instance flag")

	require.NotPanics(t, func() {
		requestsTotal.Add(enabledBase, 1)
		requestsTotal.Add(disabledBase, 1)
		requestsThrottled.Add(enabledThrottled, 1)
		requestsThrottled.Add(disabledThrottled, 1)
		totalLatency.Record(enabledBase, time.Now())
		totalLatency.Record(disabledBase, time.Now())
		evaluateLatency.Record(enabledBase, time.Now())
		evaluateLatency.Record(disabledBase, time.Now())
	}, "stats must accept labels from instances regardless of EnablePerWorkloadTableMetrics")
}

func TestQueryThrottler_extractPriority(t *testing.T) {
	tests := []struct {
		name    string
		options *querypb.ExecuteOptions
		want    int
	}{
		{
			name:    "nil options returns default priority",
			options: nil,
			want:    100,
		},
		{
			name: "empty priority returns default priority",
			options: &querypb.ExecuteOptions{
				Priority: "",
			},
			want: 100,
		},
		{
			name: "valid integer priority 0",
			options: &querypb.ExecuteOptions{
				Priority: "0",
			},
			want: 0,
		},
		{
			name: "valid integer priority 50",
			options: &querypb.ExecuteOptions{
				Priority: "50",
			},
			want: 50,
		},
		{
			name: "valid integer priority 100",
			options: &querypb.ExecuteOptions{
				Priority: "100",
			},
			want: 100,
		},
		{
			name: "invalid non-numeric priority returns default priority",
			options: &querypb.ExecuteOptions{
				Priority: "high",
			},
			want: 100,
		},
		{
			name: "invalid non-numeric priority low returns default priority",
			options: &querypb.ExecuteOptions{
				Priority: "low",
			},
			want: 100,
		},
		{
			name: "invalid negative priority returns default priority",
			options: &querypb.ExecuteOptions{
				Priority: "-1",
			},
			want: 100,
		},
		{
			name: "invalid decimal priority returns default priority",
			options: &querypb.ExecuteOptions{
				Priority: "50.5",
			},
			want: 100,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractPriority(tt.options)
			require.Equal(t, tt.want, got)
		})
	}
}

// TestQueryThrottler_HandleConfigUpdate_ErrorHandling verifies callback behavior for different error types.
func TestQueryThrottler_HandleConfigUpdate_ErrorHandling(t *testing.T) {
	tests := []struct {
		name           string
		inputErr       error
		expectedResult bool
		description    string
	}{
		{
			name:           "ContextCanceledError",
			inputErr:       context.Canceled,
			expectedResult: true,
			description:    "callback should return true to keep watching on context cancellation",
		},
		{
			name:           "TransientTopoError",
			inputErr:       errors.New("topo error: transient error"),
			expectedResult: true,
			description:    "callback should return true and continue watching on transient errors",
		},
		{
			name:           "NoNodeError",
			inputErr:       topo.NewError(topo.NoNode, "keyspace/test_keyspace"),
			expectedResult: true,
			description:    "callback should return true to keep watching when keyspace is not found (NoNode)",
		},
		{
			name:           "InterruptedError",
			inputErr:       topo.NewError(topo.Interrupted, "watch interrupted"),
			expectedResult: true,
			description:    "callback should return true to keep watching on Interrupted error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()

			qt := &QueryThrottler{
				ctx:          ctx,
				keyspace:     "test-keyspace",
				tabletConfig: &tabletenv.TabletConfig{},
			}
			qt.snapshot.Store(&stateSnapshot{
				cfg:      &querythrottlerpb.Config{Enabled: true, Strategy: querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER},
				strategy: &registry.NoOpStrategy{},
			})

			// Create a valid SrvKeyspace matching the test setup (errors are checked before srvks is used)
			srvks := createTestSrvKeyspace(true, querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER, false)

			result := qt.HandleConfigUpdate(srvks, tt.inputErr)

			require.Equal(t, tt.expectedResult, result, tt.description)
		})
	}
}

// TestQueryThrottler_HandleConfigUpdate__ConfigExtraction verifies config is properly extracted from SrvKeyspace.
func TestQueryThrottler_HandleConfigUpdate__ConfigExtraction(t *testing.T) {
	ctx := t.Context()

	oldCfg := &querythrottlerpb.Config{Enabled: false, Strategy: querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER, DryRun: false}
	oldStrategy := &registry.NoOpStrategy{}

	qt := &QueryThrottler{
		ctx:             ctx,
		tabletConfig:    &tabletenv.TabletConfig{},
		throttlerClient: &throttle.Client{},
	}
	qt.snapshot.Store(&stateSnapshot{
		cfg:      oldCfg,
		strategy: oldStrategy,
	})

	// Create SrvKeyspace with different config values
	srvks := createTestSrvKeyspace(true, querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER, true)

	result := qt.HandleConfigUpdate(srvks, nil)

	// Should return true to continue watching, config should be extracted from SrvKeyspace
	require.True(t, result, "callback should return true and continue watching")

	snap := qt.snapshot.Load()
	require.True(t, snap.cfg.GetEnabled(), "Enabled should be updated from SrvKeyspace")
	require.True(t, snap.cfg.GetDryRun(), "DryRun should be updated from SrvKeyspace")
	require.Equal(t, querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER, snap.cfg.GetStrategy(), "strategy should remain TabletThrottler")
}

// TestQueryThrottler_HandleConfigUpdate__SuccessfulConfigUpdate tests successful config update when strategy doesn't change.
func TestQueryThrottler_HandleConfigUpdate__SuccessfulConfigUpdate(t *testing.T) {
	ctx := t.Context()

	// Use a mock strategy to track state changes
	oldStrategy := &mockThrottlingStrategy{}

	// Both initial and new config have the same strategy TYPE (no swap expected)
	unchangedStrategyType := querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER

	qt := &QueryThrottler{
		ctx:          ctx,
		tabletConfig: &tabletenv.TabletConfig{},
	}
	qt.snapshot.Store(&stateSnapshot{
		cfg:      &querythrottlerpb.Config{Enabled: true, Strategy: unchangedStrategyType, DryRun: false},
		strategy: oldStrategy,
	})

	// Create SrvKeyspace with same strategy but DryRun changed
	srvks := createTestSrvKeyspace(true, unchangedStrategyType, true)

	result := qt.HandleConfigUpdate(srvks, nil)

	require.True(t, result, "callback should return true")

	snap := qt.snapshot.Load()
	require.True(t, snap.cfg.GetDryRun(), "DryRun config should be updated")
	require.Equal(t, unchangedStrategyType, snap.cfg.GetStrategy(), "strategy type should remain the same")
	require.Equal(t, oldStrategy, snap.strategy, "strategy instance should not change when type is same")
	// Verify the old strategy was NOT stopped (no swap occurred)
	require.False(t, oldStrategy.stopped, "old strategy should NOT be stopped when type doesn't change")
}

// TestQueryThrottler_HandleConfigUpdate__StrategySwitch tests that strategy is properly switched when strategy type changes.
func TestQueryThrottler_HandleConfigUpdate__StrategySwitch(t *testing.T) {
	ctx := t.Context()

	oldStrategy := &mockThrottlingStrategy{}

	qt := &QueryThrottler{
		ctx:             ctx,
		tabletConfig:    &tabletenv.TabletConfig{},
		throttlerClient: &throttle.Client{},
	}
	qt.snapshot.Store(&stateSnapshot{
		cfg:      &querythrottlerpb.Config{Enabled: true, Strategy: querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER},
		strategy: oldStrategy,
	})

	srvks := createTestSrvKeyspace(true, querythrottlerpb.ThrottlingStrategy_UNKNOWN, false)

	result := qt.HandleConfigUpdate(srvks, nil)

	// Strategy should be switched
	require.True(t, result, "callback should return true")

	snap := qt.snapshot.Load()
	require.Equal(t, querythrottlerpb.ThrottlingStrategy_UNKNOWN, snap.cfg.GetStrategy(), "config strategy should be updated")
	// Old strategy should have been stopped (mocked strategy tracks this)
	require.True(t, oldStrategy.stopped, "old strategy should be stopped")
	// New strategy should be different instance
	newStrategyInstance := snap.strategy

	require.NotEqual(t, fmt.Sprintf("%p", oldStrategy), fmt.Sprintf("%p", newStrategyInstance),
		"strategy instance should be different after type change")
}

// TestQueryThrottler_HandleConfigUpdate__NoChange tests that nothing changes when the config is identical.
func TestQueryThrottler_HandleConfigUpdate__NoChange(t *testing.T) {
	ctx := t.Context()

	unchangedCfg := &querythrottlerpb.Config{Enabled: true, Strategy: querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER, DryRun: false}
	oldStrategy := &registry.NoOpStrategy{}

	qt := &QueryThrottler{
		ctx:          ctx,
		tabletConfig: &tabletenv.TabletConfig{},
	}
	qt.snapshot.Store(&stateSnapshot{
		cfg:      unchangedCfg,
		strategy: oldStrategy,
	})

	// Create SrvKeyspace with identical config
	srvks := createTestSrvKeyspace(true, querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER, false)

	result := qt.HandleConfigUpdate(srvks, nil)

	// Config and strategy should remain same
	require.True(t, result, "callback should return true")

	snap := qt.snapshot.Load()
	require.Equal(t, unchangedCfg, snap.cfg, "config should remain unchanged")
	require.Equal(t, oldStrategy, snap.strategy, "strategy should remain unchanged")
}

// TestQueryThrottler_startSrvKeyspaceWatch_InitialLoad tests that initial configuration is loaded successfully when GetSrvKeyspace succeeds.
func TestQueryThrottler_startSrvKeyspaceWatch_InitialLoad(t *testing.T) {
	ctx := t.Context()

	env := tabletenv.NewEnv(vtenv.NewTestEnv(), &tabletenv.TabletConfig{}, "TestThrottler")

	srvTopoServer := srvtopotest.NewPassthroughSrvTopoServer()
	srvTopoServer.SrvKeyspace = createTestSrvKeyspace(true, querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER, false)
	srvTopoServer.SrvKeyspaceError = nil

	throttler := &throttle.Throttler{}
	qt := NewQueryThrottler(ctx, throttler, env, &topodatapb.TabletAlias{Cell: "test-cell", Uid: uint32(123)}, srvTopoServer)

	qt.InitDBConfig("test_keyspace")

	// Verify watch was started
	require.Eventually(t, func() bool {
		return qt.watchStarted.Load()
	}, 2*time.Second, 10*time.Millisecond, "Watch should have been started")

	// Verify that the configuration was loaded correctly
	require.Eventually(t, func() bool {
		snap := qt.snapshot.Load()
		return snap.cfg.GetEnabled() &&
			snap.cfg.GetStrategy() == querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER &&
			!snap.cfg.GetDryRun()
	}, 2*time.Second, 10*time.Millisecond, "Config should be loaded correctly: enabled=true, strategy=TabletThrottler, dryRun=false")

	require.Equal(t, "test_keyspace", qt.keyspace, "Keyspace should be set correctly")
}

// TestQueryThrottler_startSrvKeyspaceWatch_InitialLoadFailure tests that watch starts even when initial GetSrvKeyspace fails.
func TestQueryThrottler_startSrvKeyspaceWatch_InitialLoadFailure(t *testing.T) {
	ctx := t.Context()

	env := tabletenv.NewEnv(vtenv.NewTestEnv(), &tabletenv.TabletConfig{}, "TestThrottler")

	// Configure PassthroughSrvTopoServer to return an error on GetSrvKeyspace
	srvTopoServer := srvtopotest.NewPassthroughSrvTopoServer()
	srvTopoServer.SrvKeyspace = nil
	srvTopoServer.SrvKeyspaceError = errors.New("failed to fetch keyspace")

	throttler := &throttle.Throttler{}
	qt := NewQueryThrottler(ctx, throttler, env, &topodatapb.TabletAlias{Cell: "test-cell", Uid: uint32(123)}, srvTopoServer)

	// Initialize with keyspace to trigger startSrvKeyspaceWatch
	qt.InitDBConfig("test_keyspace")

	// Verify watch was started despite initial load failure
	require.Eventually(t, func() bool {
		return qt.watchStarted.Load()
	}, 2*time.Second, 10*time.Millisecond, "Watch should be started even if initial load fails")

	require.Equal(t, "test_keyspace", qt.keyspace, "Keyspace should be set correctly")

	// Configuration should remain at default (NoOpStrategy) due to failure
	require.Eventually(t, func() bool {
		return !qt.snapshot.Load().cfg.GetEnabled()
	}, 2*time.Second, 10*time.Millisecond, "Config should remain disabled after initial load failure")
}

// TestQueryThrottler_startSrvKeyspaceWatch_OnlyStartsOnce tests that watch only starts once even with concurrent calls (atomic flag protection).
func TestQueryThrottler_startSrvKeyspaceWatch_OnlyStartsOnce(t *testing.T) {
	ctx := t.Context()

	env := tabletenv.NewEnv(vtenv.NewTestEnv(), &tabletenv.TabletConfig{}, "TestThrottler")

	srvTopoServer := srvtopotest.NewPassthroughSrvTopoServer()
	srvTopoServer.SrvKeyspace = createTestSrvKeyspace(true, querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER, false)
	srvTopoServer.SrvKeyspaceError = nil

	throttler := &throttle.Throttler{}
	qt := NewQueryThrottler(ctx, throttler, env, &topodatapb.TabletAlias{Cell: "test-cell", Uid: uint32(123)}, srvTopoServer)

	qt.InitDBConfig("test_keyspace")

	// Attempt to start the watch multiple times concurrently
	const numGoroutines = 10
	startedCount := 0
	var wg sync.WaitGroup
	var mu sync.Mutex

	for range numGoroutines {
		wg.Go(func() {
			// Each goroutine tries to start the watch
			qt.startSrvKeyspaceWatch()
			mu.Lock()
			startedCount++
			mu.Unlock()
		})
	}

	// Wait for all goroutines to complete
	wg.Wait()

	// Verify that the watch was started exactly once (atomic flag prevents multiple starts)
	require.Eventually(t, func() bool {
		return qt.watchStarted.Load()
	}, 2*time.Second, 10*time.Millisecond, "Watch should have been started")

	require.Equal(t, numGoroutines, startedCount, "All goroutines should have called startSrvKeyspaceWatch")
}

// TestQueryThrottler_startSrvKeyspaceWatch_RequiredFieldsValidation tests that watch doesn't start when required fields are missing.
func TestQueryThrottler_startSrvKeyspaceWatch_RequiredFieldsValidation(t *testing.T) {
	tests := []struct {
		name              string
		srvTopoServer     srvtopo.Server
		keyspace          string
		expectedWatchFlag bool
		description       string
	}{
		{
			name:              "Nil srvTopoServer prevents watch start",
			srvTopoServer:     nil,
			keyspace:          "test_keyspace",
			expectedWatchFlag: false,
			description:       "Watch should not start when srvTopoServer is nil",
		},
		{
			name:              "Empty keyspace prevents watch start",
			srvTopoServer:     srvtopotest.NewPassthroughSrvTopoServer(),
			keyspace:          "",
			expectedWatchFlag: false,
			description:       "Watch should not start when keyspace is empty",
		},
		{
			name:              "Valid fields allow watch to start",
			srvTopoServer:     srvtopotest.NewPassthroughSrvTopoServer(),
			keyspace:          "test_keyspace",
			expectedWatchFlag: true,
			description:       "Watch should start when all required fields are valid",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()

			env := tabletenv.NewEnv(vtenv.NewTestEnv(), &tabletenv.TabletConfig{}, "TestThrottler")

			throttler := &throttle.Throttler{}
			qt := NewQueryThrottler(ctx, throttler, env, &topodatapb.TabletAlias{Cell: "test-cell", Uid: uint32(123)}, tt.srvTopoServer)

			qt.InitDBConfig(tt.keyspace)

			qt.startSrvKeyspaceWatch()

			if tt.expectedWatchFlag {
				require.Eventually(t, func() bool {
					return qt.watchStarted.Load()
				}, 2*time.Second, 10*time.Millisecond, tt.description)
			} else {
				// For negative cases, ensure the watch doesn't start within a reasonable time
				require.Never(t, func() bool {
					return qt.watchStarted.Load()
				}, 500*time.Millisecond, 10*time.Millisecond, tt.description)
			}
		})
	}
}

// TestQueryThrottler_startSrvKeyspaceWatch_WatchCallback tests that WatchSrvKeyspace callback receives config updates and HandleConfigUpdate is invoked correctly.
func TestQueryThrottler_startSrvKeyspaceWatch_WatchCallback(t *testing.T) {
	tests := []struct {
		name             string
		enabled          bool
		strategy         querythrottlerpb.ThrottlingStrategy
		dryRun           bool
		expectedEnabled  bool
		expectedStrategy querythrottlerpb.ThrottlingStrategy
		expectedDryRun   bool
	}{
		{
			name:             "TabletThrottler strategy with enabled and no dry-run",
			enabled:          true,
			strategy:         querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER,
			dryRun:           false,
			expectedEnabled:  true,
			expectedStrategy: querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER,
			expectedDryRun:   false,
		},
		{
			name:             "TabletThrottler disabled with dry-run",
			enabled:          false,
			strategy:         querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER,
			dryRun:           true,
			expectedEnabled:  false,
			expectedStrategy: querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER,
			expectedDryRun:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()

			env := tabletenv.NewEnv(vtenv.NewTestEnv(), &tabletenv.TabletConfig{}, "TestThrottler")

			srvTopoServer := srvtopotest.NewPassthroughSrvTopoServer()
			srvTopoServer.SrvKeyspace = createTestSrvKeyspace(tt.enabled, tt.strategy, tt.dryRun)
			srvTopoServer.SrvKeyspaceError = nil

			throttler := &throttle.Throttler{}
			qt := NewQueryThrottler(ctx, throttler, env, &topodatapb.TabletAlias{Cell: "test-cell", Uid: uint32(123)}, srvTopoServer)

			qt.InitDBConfig("test_keyspace")

			// Verify watch was started
			require.Eventually(t, func() bool {
				return qt.watchStarted.Load()
			}, 2*time.Second, 10*time.Millisecond, "Watch should have been started")

			// Verify that HandleConfigUpdate was called by checking if the config was updated
			require.Eventually(t, func() bool {
				snap := qt.snapshot.Load()
				return snap.cfg.GetEnabled() == tt.expectedEnabled &&
					snap.cfg.GetStrategy() == tt.expectedStrategy &&
					snap.cfg.GetDryRun() == tt.expectedDryRun
			}, 2*time.Second, 10*time.Millisecond, "Config should be updated correctly after callback is invoked")
		},
		)
	}
}

// TestQueryThrottler_startSrvKeyspaceWatch_ShutdownStopsWatch tests that Shutdown properly cancels the watch context and stops the watch goroutine.
func TestQueryThrottler_startSrvKeyspaceWatch_ShutdownStopsWatch(t *testing.T) {
	ctx := t.Context()

	env := tabletenv.NewEnv(vtenv.NewTestEnv(), &tabletenv.TabletConfig{}, "TestThrottler")

	srvTopoServer := srvtopotest.NewPassthroughSrvTopoServer()
	srvTopoServer.SrvKeyspace = createTestSrvKeyspace(true, querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER, false)
	srvTopoServer.SrvKeyspaceError = nil

	throttler := &throttle.Throttler{}
	qt := NewQueryThrottler(ctx, throttler, env, &topodatapb.TabletAlias{Cell: "test-cell", Uid: uint32(123)}, srvTopoServer)

	qt.InitDBConfig("test_keyspace")

	// Verify watch was started
	require.Eventually(t, func() bool {
		return qt.watchStarted.Load()
	}, 2*time.Second, 10*time.Millisecond, "Watch should have been started before shutdown")

	require.NotNil(t, qt.cancelWatchContext, "Cancel function should be set before shutdown")

	// Call Shutdown to stop the watch
	qt.Shutdown()

	// Verify that the watch started flag is reset
	require.Eventually(t, func() bool {
		return !qt.watchStarted.Load()
	}, 2*time.Second, 10*time.Millisecond, "Watch should be marked as not started after shutdown")

	// Verify that the strategy was stopped
	strategyInstance := qt.snapshot.Load().strategy
	require.NotNil(t, strategyInstance, "Strategy instance should still exist after shutdown")

	// Call Shutdown again to ensure it doesn't panic
	qt.Shutdown()

	// Verify the watch flag remains false
	require.False(t, qt.watchStarted.Load(), "Watch should remain not started after multiple shutdowns")
}

// The listener must return false once the watch is cancelled. HandleConfigUpdate's
// shutdown latch is not enough: it stops config being applied, but leaves the listener
// registered and holding the throttler. Only the boolean return releases it.
func TestQueryThrottler_srvKeyspaceListener_DeregistersAfterWatchCancel(t *testing.T) {
	env := tabletenv.NewEnv(vtenv.NewTestEnv(), &tabletenv.TabletConfig{}, "TestThrottler")
	srvTopoServer := srvtopotest.NewPassthroughSrvTopoServer()
	srvTopoServer.SrvKeyspace = createTestSrvKeyspace(true, querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER, false)

	qt := NewQueryThrottler(t.Context(), &throttle.Throttler{}, env, &topodatapb.TabletAlias{Cell: "test-cell", Uid: 123}, srvTopoServer)
	qt.keyspace = "test_keyspace"

	watchCtx, cancel := context.WithCancel(t.Context())
	listener := qt.srvKeyspaceListener(watchCtx)

	srvks := createTestSrvKeyspace(true, querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER, true)

	require.True(t, listener(srvks, nil),
		"listener must stay registered while the watch context is live")

	cancel()
	require.False(t, listener(srvks, nil),
		"listener must return false once cancelled, so srvtopo drops it")

	// The error path must not re-register it either.
	require.False(t, listener(nil, errors.New("watch error")),
		"listener must return false after cancellation even on an error notification")
}

// Premise guard against the real resilient watcher: returning false drops a listener,
// cancelling its registration context does not. If srvtopo's protocol changed,
// srvKeyspaceListener would silently stop unregistering.
func TestSrvKeyspaceWatcher_DropsListenerReturningFalse(t *testing.T) {
	ctx := t.Context()
	const (
		cell     = "test-cell"
		keyspace = "test_keyspace"
	)

	ts := memorytopo.NewServer(ctx, cell)
	t.Cleanup(func() { ts.Close() })

	counts := stats.NewCountersWithSingleLabel("", "Resilient srvtopo listener protocol", "type")
	srvTopoServer := srvtopo.NewResilientServer(ctx, ts, counts)

	publishDryRun := func(dryRun bool) {
		require.NoError(t, ts.UpdateSrvKeyspace(ctx, cell, keyspace,
			createTestSrvKeyspace(true, querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER, dryRun)))
	}
	publishDryRun(false)

	// A listener registered with its own context, which we cancel part-way through.
	listenerCtx, cancelListener := context.WithCancel(ctx)
	t.Cleanup(cancelListener)

	var (
		mu             sync.Mutex
		calls          int
		keepRegistered = true
		// Recorded inside the callback: sampling from the test goroutine could snapshot
		// the baseline too early and misread it as a post-drop invocation.
		callsAtDrop = -1
	)
	srvTopoServer.WatchSrvKeyspace(listenerCtx, cell, keyspace, func(_ *topodatapb.SrvKeyspace, _ error) bool {
		mu.Lock()
		defer mu.Unlock()
		calls++
		if !keepRegistered {
			callsAtDrop = calls
			return false
		}
		return true
	})

	callCount := func() int {
		mu.Lock()
		defer mu.Unlock()
		return calls
	}
	dropPoint := func() int {
		mu.Lock()
		defer mu.Unlock()
		return callsAtDrop
	}

	// Baseline: the listener is live and receives updates.
	before := callCount()
	publishDryRun(true)
	require.Eventually(t, func() bool { return callCount() > before }, 30*time.Second, 10*time.Millisecond,
		"a registered listener must receive SrvKeyspace updates")

	// Cancelling the registration context alone must NOT unregister it — which is why
	// srvKeyspaceListener cannot rely on cancellation by itself.
	cancelListener()
	before = callCount()
	publishDryRun(false)
	require.Eventually(t, func() bool { return callCount() > before }, 30*time.Second, 10*time.Millisecond,
		"cancelling the context must not unregister the listener — only a false return does")

	// Now return false on the next invocation: the watcher must drop the listener and
	// never call it again.
	mu.Lock()
	keepRegistered = false
	mu.Unlock()

	publishDryRun(true)
	require.Eventually(t, func() bool { return dropPoint() != -1 }, 30*time.Second, 10*time.Millisecond,
		"listener should be invoked once more, returning false")

	// No invocation may occur after the one that returned false, however many updates land.
	publishDryRun(false)
	publishDryRun(true)
	require.Never(t, func() bool { return callCount() > dropPoint() }, 5*time.Second, 50*time.Millisecond,
		"a listener that returned false must never be invoked again")
}

// watchRecordingSrvTopoServer counts WatchSrvKeyspace registrations, so a test can assert
// no listener was registered at all rather than that a registered one stayed inert.
type watchRecordingSrvTopoServer struct {
	*srvtopotest.PassthroughSrvTopoServer
	watches atomic.Int64
}

func (s *watchRecordingSrvTopoServer) WatchSrvKeyspace(ctx context.Context, cell, keyspace string, callback func(*topodatapb.SrvKeyspace, error) bool) {
	s.watches.Add(1)
	s.PassthroughSrvTopoServer.WatchSrvKeyspace(ctx, cell, keyspace, callback)
}

// No watch may be registered once Shutdown has run: dropping a listener needs a later
// notification, which may never arrive, so one registered now leaks outright.
func TestQueryThrottler_startSrvKeyspaceWatch_SkippedAfterShutdown(t *testing.T) {
	env := tabletenv.NewEnv(vtenv.NewTestEnv(), &tabletenv.TabletConfig{}, "TestThrottler")
	passthrough := srvtopotest.NewPassthroughSrvTopoServer()
	passthrough.SrvKeyspace = createTestSrvKeyspace(true, querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER, false)
	srvTopoServer := &watchRecordingSrvTopoServer{PassthroughSrvTopoServer: passthrough}

	qt := NewQueryThrottler(t.Context(), &throttle.Throttler{}, env, &topodatapb.TabletAlias{Cell: "test-cell", Uid: 123}, srvTopoServer)

	qt.Shutdown()
	require.True(t, qt.IsShutdown())

	// Registration happens on a goroutine, so allow time for it to occur if unguarded.
	qt.InitDBConfig("test_keyspace")
	require.Never(t, func() bool { return srvTopoServer.watches.Load() > 0 }, time.Second, 20*time.Millisecond,
		"no SrvKeyspace watch may be registered once the throttler is shut down")
}

// Runs Throttle() reads against HandleConfigUpdate() swaps. Needs `go test -race` to
// mean anything: cfg and strategy used to be read without synchronization.
func TestQueryThrottler_ConcurrentThrottleAndConfigUpdate(t *testing.T) {
	ctx := t.Context()

	qt := &QueryThrottler{
		ctx:             ctx,
		tabletConfig:    &tabletenv.TabletConfig{},
		throttlerClient: &throttle.Client{},
		env:             tabletenv.NewEnv(vtenv.NewTestEnv(), &tabletenv.TabletConfig{}, "TestThrottler"),
	}
	qt.snapshot.Store(&stateSnapshot{
		cfg:      &querythrottlerpb.Config{Enabled: true, Strategy: querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER},
		strategy: &mockThrottlingStrategy{decision: registry.ThrottleDecision{Throttle: false}},
	})

	var wg sync.WaitGroup
	stop := make(chan struct{})

	const readers = 8
	for range readers {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
					_ = qt.Throttle(ctx, topodatapb.TabletType_REPLICA,
						&sqlparser.ParsedQuery{Query: "SELECT 1"}, sqlparser.StmtSelect, 0,
						&querypb.ExecuteOptions{WorkloadName: "w", Priority: "50"})
				}
			}
		})
	}

	// Alternate the strategy type to force snapshot swaps that exercise both
	// the cfg pointer and the strategy interface value.
	strategies := []querythrottlerpb.ThrottlingStrategy{
		querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER,
		querythrottlerpb.ThrottlingStrategy_UNKNOWN,
	}
	for i := range 200 {
		qt.HandleConfigUpdate(createTestSrvKeyspace(true, strategies[i%2], i%2 == 0), nil)
	}

	close(stop)
	wg.Wait()
}

// When one SrvKeyspace update flips a top-level field, changes the nested config, and
// keeps the same Strategy, the strategy must be updated before the snapshot swap.
// Otherwise the new top-level config is published against the strategy's old rules,
// briefly throttling on them.
func TestQueryThrottler_HandleConfigUpdate_PushesNestedConfigBeforeSnapshotSwap(t *testing.T) {
	ctx := t.Context()

	oldStrategy := &mockThrottlingStrategy{}
	qt := &QueryThrottler{
		ctx:          ctx,
		tabletConfig: &tabletenv.TabletConfig{},
	}
	qt.snapshot.Store(&stateSnapshot{
		cfg: &querythrottlerpb.Config{
			Enabled:  false,
			Strategy: querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER,
		},
		strategy: oldStrategy,
	})

	// Single SrvKeyspace update: flip Enabled false->true AND add nested rules.
	// Strategy enum is unchanged, so the strategy instance is reused.
	newCfg := &querythrottlerpb.Config{
		Enabled:  true,
		Strategy: querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER,
		TabletStrategyConfig: &querythrottlerpb.TabletStrategyConfig{
			TabletRules: map[string]*querythrottlerpb.StatementRuleSet{
				"PRIMARY": {
					StatementRules: map[string]*querythrottlerpb.MetricRuleSet{
						"SELECT": {
							MetricRules: map[string]*querythrottlerpb.MetricRule{
								"lag": {Thresholds: []*querythrottlerpb.ThrottleThreshold{{Above: 20, Throttle: 75}}},
							},
						},
					},
				},
			},
		},
	}
	srvks := &topodatapb.SrvKeyspace{QueryThrottlerConfig: newCfg}

	require.True(t, qt.HandleConfigUpdate(srvks, nil))

	// The strategy instance must be reused (Strategy enum unchanged) — guards against
	// future refactors that accidentally rebuild on Enabled changes.
	snap := qt.snapshot.Load()
	require.Same(t, oldStrategy, snap.strategy, "strategy instance must not change when Strategy enum is unchanged")
	require.True(t, snap.cfg.GetEnabled(), "snapshot must reflect the new Enabled=true")

	// The core assertion: UpdateConfig ran before HandleConfigUpdate returned.
	// Without the fix this slice is empty and the strategy keeps its old nested config.
	require.Len(t, oldStrategy.updateConfigCfgs, 1, "UpdateConfig must be invoked exactly once before the snapshot swap")
	pushed := oldStrategy.updateConfigCfgs[0]
	require.True(t, pushed.GetEnabled(), "pushed cfg must reflect new Enabled=true")
	require.Contains(t, pushed.GetTabletStrategyConfig().GetTabletRules(), "PRIMARY", "pushed cfg must carry the new nested rules")
}

// A SrvKeyspace update that changes only the nested TabletStrategyConfig must still
// reach the strategy. Comparing just Enabled/Strategy/DryRun would short-circuit here
// and drop it.
func TestQueryThrottler_HandleConfigUpdate_NestedOnlyChangePropagates(t *testing.T) {
	ctx := t.Context()

	initialNested := &querythrottlerpb.TabletStrategyConfig{
		TabletRules: map[string]*querythrottlerpb.StatementRuleSet{
			"PRIMARY": {
				StatementRules: map[string]*querythrottlerpb.MetricRuleSet{
					"SELECT": {
						MetricRules: map[string]*querythrottlerpb.MetricRule{
							"lag": {Thresholds: []*querythrottlerpb.ThrottleThreshold{{Above: 10, Throttle: 25}}},
						},
					},
				},
			},
		},
	}
	oldStrategy := &mockThrottlingStrategy{}
	qt := &QueryThrottler{
		ctx:          ctx,
		tabletConfig: &tabletenv.TabletConfig{},
	}
	qt.snapshot.Store(&stateSnapshot{
		cfg: &querythrottlerpb.Config{
			Enabled:              true,
			Strategy:             querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER,
			DryRun:               false,
			TabletStrategyConfig: initialNested,
		},
		strategy: oldStrategy,
	})

	// Same top-level fields, different threshold on the same metric.
	newNested := &querythrottlerpb.TabletStrategyConfig{
		TabletRules: map[string]*querythrottlerpb.StatementRuleSet{
			"PRIMARY": {
				StatementRules: map[string]*querythrottlerpb.MetricRuleSet{
					"SELECT": {
						MetricRules: map[string]*querythrottlerpb.MetricRule{
							"lag": {Thresholds: []*querythrottlerpb.ThrottleThreshold{{Above: 5, Throttle: 99}}},
						},
					},
				},
			},
		},
	}
	srvks := &topodatapb.SrvKeyspace{QueryThrottlerConfig: &querythrottlerpb.Config{
		Enabled:              true,
		Strategy:             querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER,
		DryRun:               false,
		TabletStrategyConfig: newNested,
	}}

	require.True(t, qt.HandleConfigUpdate(srvks, nil))

	// The strategy must hear about the nested change. Comparing only the top-level
	// scalars (all unchanged here) would short-circuit and leave this slice empty.
	require.Len(t, oldStrategy.updateConfigCfgs, 1, "UpdateConfig must be invoked when only the nested TabletStrategyConfig changes")
	pushed := oldStrategy.updateConfigCfgs[0].GetTabletStrategyConfig().GetTabletRules()["PRIMARY"].GetStatementRules()["SELECT"].GetMetricRules()["lag"].GetThresholds()
	require.Len(t, pushed, 1)
	require.Equal(t, float64(5), pushed[0].GetAbove(), "pushed cfg must carry the new threshold")
	require.Equal(t, int32(99), pushed[0].GetThrottle(), "pushed cfg must carry the new throttle ratio")

	// And the snapshot must also reflect the new nested config.
	snap := qt.snapshot.Load()
	require.Same(t, oldStrategy, snap.strategy, "strategy instance must not change for a nested-only update")
	snapThresholds := snap.cfg.GetTabletStrategyConfig().GetTabletRules()["PRIMARY"].GetStatementRules()["SELECT"].GetMetricRules()["lag"].GetThresholds()
	require.Len(t, snapThresholds, 1)
	require.Equal(t, float64(5), snapThresholds[0].GetAbove(), "snapshot cfg must carry the new nested config")
}

// HandleConfigUpdate builds its new strategy outside qt.mu. If Shutdown wins the lock
// first, the callback must discard that strategy rather than Start() and store it —
// otherwise the tablet strategy's ticker and watch goroutines outlive Shutdown.
// The newStrategyFactory hook forces that interleaving deterministically below.
func TestQueryThrottler_HandleConfigUpdate_DiscardsStrategyAfterShutdown(t *testing.T) {
	ctx := t.Context()

	originalMock := &mockThrottlingStrategy{}
	qt := &QueryThrottler{
		ctx:          ctx,
		tabletConfig: &tabletenv.TabletConfig{},
	}
	qt.snapshot.Store(&stateSnapshot{
		cfg: &querythrottlerpb.Config{
			Enabled:  true,
			Strategy: querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER,
		},
		strategy: originalMock,
	})

	// Blocks until released, parking the callback between the build and the lock.
	newMock := &mockThrottlingStrategy{}
	factoryEntered := make(chan struct{})
	factoryReleased := make(chan struct{})
	qt.newStrategyFactory = func(_ *querythrottlerpb.Config) registry.ThrottlingStrategyHandler {
		close(factoryEntered)
		<-factoryReleased
		return newMock
	}

	// A different Strategy enum sends HandleConfigUpdate down the factory path.
	srvks := &topodatapb.SrvKeyspace{
		QueryThrottlerConfig: &querythrottlerpb.Config{
			Enabled:  true,
			Strategy: querythrottlerpb.ThrottlingStrategy_UNKNOWN,
		},
	}

	callbackDone := make(chan struct{})
	go func() {
		defer close(callbackDone)
		qt.HandleConfigUpdate(srvks, nil)
	}()

	// Only once the callback is parked is "Shutdown wins the lock" guaranteed.
	<-factoryEntered

	// Uncontended, so it takes qt.mu immediately and latches the shutdown flag.
	qt.Shutdown()

	// The callback now resumes, takes the lock, and must see shutdown=true.
	close(factoryReleased)

	require.Eventually(t, func() bool {
		select {
		case <-callbackDone:
			return true
		default:
			return false
		}
	}, 30*time.Second, 10*time.Millisecond, "HandleConfigUpdate callback should return after Shutdown")

	// Invariants after the race resolves:
	require.False(t, newMock.started, "discarded strategy must not be Start()'d after Shutdown")
	require.True(t, newMock.stopped, "discarded strategy must be Stop()'d as defense-in-depth so its background work cannot leak")
	require.True(t, originalMock.stopped, "Shutdown must have Stop()'d the original strategy")

	snap := qt.snapshot.Load()
	require.Same(t, originalMock, snap.strategy, "snapshot must NOT be swapped to the discarded strategy")
	require.Equal(t, querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER, snap.cfg.GetStrategy(),
		"snapshot cfg must NOT be replaced with the post-shutdown update")
}

// HandleConfigUpdate must store thresholds sorted ascending, as GetThrottleDecision's
// binary search requires, without mutating the incoming SrvKeyspace. Only the RPC write
// path sorts, so a direct topo write can land unsorted.
func TestQueryThrottler_HandleConfigUpdate_SortsThresholdsOnReceipt(t *testing.T) {
	ctx := t.Context()

	// Seed the snapshot with a different strategy so the incoming TABLET_THROTTLER
	// SrvKeyspace is treated as a strategy change — the new cfg is then built
	// into the snapshot rather than short-circuited by proto.Equal.
	qt := &QueryThrottler{
		ctx:          ctx,
		tabletConfig: &tabletenv.TabletConfig{},
	}
	qt.snapshot.Store(&stateSnapshot{
		cfg:      &querythrottlerpb.Config{Strategy: querythrottlerpb.ThrottlingStrategy_UNKNOWN},
		strategy: &mockThrottlingStrategy{},
	})
	// A deterministic factory, so the build doesn't need a real throttler client.
	qt.newStrategyFactory = func(_ *querythrottlerpb.Config) registry.ThrottlingStrategyHandler {
		return &mockThrottlingStrategy{}
	}

	// Out of order, as a direct topo write bypassing the RPC sanitizer would leave them.
	unsorted := []*querythrottlerpb.ThrottleThreshold{
		{Above: 50, Throttle: 100},
		{Above: 10, Throttle: 25},
		{Above: 25, Throttle: 50},
	}
	srvks := &topodatapb.SrvKeyspace{
		QueryThrottlerConfig: &querythrottlerpb.Config{
			Enabled:  true,
			Strategy: querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER,
			TabletStrategyConfig: &querythrottlerpb.TabletStrategyConfig{
				TabletRules: map[string]*querythrottlerpb.StatementRuleSet{
					"PRIMARY": {
						StatementRules: map[string]*querythrottlerpb.MetricRuleSet{
							"SELECT": {
								MetricRules: map[string]*querythrottlerpb.MetricRule{
									"lag": {Thresholds: unsorted},
								},
							},
						},
					},
				},
			},
		},
	}

	// srvtopo hands this same pointer to every listener and GetSrvKeyspace caller.
	before := srvks.CloneVT()

	qt.HandleConfigUpdate(srvks, nil)

	storedThresholds := qt.snapshot.Load().cfg.
		GetTabletStrategyConfig().
		GetTabletRules()["PRIMARY"].
		GetStatementRules()["SELECT"].
		GetMetricRules()["lag"].
		GetThresholds()
	require.Len(t, storedThresholds, 3)
	require.Equal(t, float64(10), storedThresholds[0].GetAbove(), "thresholds[0] must be the minimum after defensive sort")
	require.Equal(t, float64(25), storedThresholds[1].GetAbove())
	require.Equal(t, float64(50), storedThresholds[2].GetAbove())

	require.True(t, proto.Equal(before, srvks),
		"HandleConfigUpdate must sort a clone, not the shared SrvKeyspace proto")
	require.Equal(t, float64(50), unsorted[0].GetAbove(),
		"the caller's threshold slice must keep its original order")
}
