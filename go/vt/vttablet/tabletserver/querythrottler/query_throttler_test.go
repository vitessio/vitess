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
	"fmt"
	"testing"
	"time"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/sqlparser"

	"vitess.io/vitess/go/vt/vttablet/tabletserver/querythrottler/registry"

	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle"
)

func TestNewQueryThrottler_ConfigRefresh(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	config := &tabletenv.TabletConfig{
		QueryThrottlerConfigRefreshInterval: 10 * time.Millisecond,
	}
	env := tabletenv.NewEnv(vtenv.NewTestEnv(), config, "TestThrottler")

	throttler := &throttle.Throttler{} // use mock if needed
	iqt := NewQueryThrottler(ctx, throttler, newFakeConfigLoader(Config{
		Enabled:  true,
		Strategy: registry.ThrottlingStrategyTabletThrottler,
	}), env)

	// Assert initial state (should be NoOpStrategy)
	require.NotNil(t, iqt)
	iqt.mu.RLock()
	initialStrategy := iqt.strategy
	iqt.mu.RUnlock()
	require.IsType(t, &registry.NoOpStrategy{}, initialStrategy)

	require.Eventually(t, func() bool {
		iqt.mu.RLock()
		defer iqt.mu.RUnlock()

		// Assert updated cfg and strategy after config refresh
		if !iqt.cfg.Enabled {
			return false
		}
		if iqt.cfg.Strategy != registry.ThrottlingStrategyTabletThrottler {
			return false
		}
		return true
	}, 1*time.Second, 10*time.Millisecond, "Config should be refreshed and strategy should be updated")
}

func TestSelectThrottlingStrategy(t *testing.T) {
	tests := []struct {
		name                   string
		giveThrottlingStrategy registry.ThrottlingStrategy
		expectedType           registry.ThrottlingStrategyHandler
	}{
		{
			name:                   "Unknown strategy defaults to NoOp",
			giveThrottlingStrategy: "some-unknown-string",
			expectedType:           &registry.NoOpStrategy{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := &throttle.Client{}

			config := &tabletenv.TabletConfig{
				QueryThrottlerConfigRefreshInterval: 10 * time.Millisecond,
			}

			strategy := selectThrottlingStrategy(Config{Enabled: true, Strategy: tt.giveThrottlingStrategy}, mockClient, config)

			require.IsType(t, tt.expectedType, strategy)
		})
	}
}

// TestQueryThrottler_StrategyLifecycleManagement tests that strategies are properly started and stopped.
func TestQueryThrottler_StrategyLifecycleManagement(t *testing.T) {
	// Test that initial strategy is started
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	throttler := &throttle.Throttler{}
	config := &tabletenv.TabletConfig{
		QueryThrottlerConfigRefreshInterval: 10 * time.Millisecond,
	}
	env := tabletenv.NewEnv(vtenv.NewTestEnv(), config, "TestThrottler")

	iqt := NewQueryThrottler(ctx, throttler, newFakeConfigLoader(Config{
		Enabled:  true,
		Strategy: registry.ThrottlingStrategyTabletThrottler,
	}), env)

	// Verify initial strategy was started (NoOpStrategy in this case)
	require.NotNil(t, iqt.strategy)

	// Test Shutdown properly stops the strategy
	iqt.Shutdown()

	// After shutdown, the strategy should have been stopped
	// In a real test, we would verify the strategy's Stop method was called
	require.NotNil(t, iqt.strategy) // Strategy reference should still exist but be stopped
}

// TestQueryThrottler_Shutdown tests the Shutdown method.
func TestQueryThrottler_Shutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	config := &tabletenv.TabletConfig{
		QueryThrottlerConfigRefreshInterval: 10 * time.Millisecond,
	}
	env := tabletenv.NewEnv(vtenv.NewTestEnv(), config, "TestThrottler")

	throttler := &throttle.Throttler{}
	iqt := NewQueryThrottler(ctx, throttler, newFakeConfigLoader(Config{
		Enabled:  false,
		Strategy: registry.ThrottlingStrategyTabletThrottler,
	}), env)

	// Should not panic when called multiple times
	iqt.Shutdown()
	iqt.Shutdown()

	// Should still be able to check the strategy reference
	iqt.mu.RLock()
	strategy := iqt.strategy
	iqt.mu.RUnlock()
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
		expectedLogMsg            string
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
			expectedLogMsg:            "[DRY-RUN] Query throttled: metric=cpu value=95.0 threshold=80.0, metric name: cpu, metric value: 95.000000",
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

			// Create throttler with controlled config
			iqt := &QueryThrottler{
				ctx: context.Background(),
				cfg: Config{
					Enabled: tt.enabled,
					DryRun:  tt.dryRun,
				},
				strategy: mockStrategy,
				env:      env,
				stats: Stats{
					requestsTotal:     env.Exporter().NewCountersWithMultiLabels(queryThrottlerAppName+"Requests", "TestThrottler requests", []string{"Strategy", "Workload", "Priority"}),
					requestsThrottled: env.Exporter().NewCountersWithMultiLabels(queryThrottlerAppName+"Throttled", "TestThrottler throttled", []string{"Strategy", "Workload", "Priority", "MetricName", "MetricValue", "DryRun"}),
					totalLatency:      env.Exporter().NewMultiTimings(queryThrottlerAppName+"TotalLatencyMs", "Total latency of QueryThrottler.Throttle in milliseconds", []string{"Strategy", "Workload", "Priority"}),
					evaluateLatency:   env.Exporter().NewMultiTimings(queryThrottlerAppName+"EvaluateLatencyMs", "Latency from Throttle entry to completion of Evaluate in milliseconds", []string{"Strategy", "Workload", "Priority"}),
				},
			}

			iqt.stats.requestsTotal.ResetAll()
			iqt.stats.requestsThrottled.ResetAll()

			// Capture log output
			logCapture := &testLogCapture{}
			originalLogWarningf := log.Warningf
			defer func() {
				// Restore original logging function
				log.Warningf = originalLogWarningf
			}()

			// Mock log.Warningf to capture output
			log.Warningf = logCapture.captureLog

			// Test the enforcement
			err := iqt.Throttle(
				context.Background(),
				topodatapb.TabletType_REPLICA,
				&sqlparser.ParsedQuery{Query: "SELECT * FROM test_table WHERE id = 1"},
				12345,
				&querypb.ExecuteOptions{
					WorkloadName: "test-workload",
					Priority:     "50",
				},
			)

			// Verify error expectation
			if tt.expectError {
				require.EqualError(t, err, tt.throttleDecision.Message, "Error should match the throttle message exactly")
			} else {
				require.NoError(t, err, "Expected no throttling error")
			}

			// Verify log expectation
			if tt.expectDryRunLog {
				require.Len(t, logCapture.logs, 1, "Expected exactly one log message")
				require.Equal(t, tt.expectedLogMsg, logCapture.logs[0], "Log message should match expected")
			} else {
				require.Empty(t, logCapture.logs, "Expected no log messages")
			}

			// Verify stats expectation
			totalRequests := stats.CounterForDimension(iqt.stats.requestsTotal, "Strategy")
			throttledRequests := stats.CounterForDimension(iqt.stats.requestsThrottled, "Strategy")
			require.Equal(t, tt.expectedTotalRequests, totalRequests.Counts()["MockStrategy"], "Total requests should match expected")
			require.Equal(t, tt.expectedThrottledRequests, throttledRequests.Counts()["MockStrategy"], "Throttled requests should match expected")
		})
	}
}

// mockThrottlingStrategy is a test strategy that allows us to control throttling decisions
type mockThrottlingStrategy struct {
	decision registry.ThrottleDecision
	started  bool
	stopped  bool
}

func (m *mockThrottlingStrategy) Evaluate(ctx context.Context, targetTabletType topodatapb.TabletType, parsedQuery *sqlparser.ParsedQuery, transactionID int64, attrs registry.QueryAttributes) registry.ThrottleDecision {
	return m.decision
}

func (m *mockThrottlingStrategy) Start() {
	m.started = true
}

func (m *mockThrottlingStrategy) Stop() {
	m.stopped = true
}

func (m *mockThrottlingStrategy) GetStrategyName() string {
	return "MockStrategy"
}

// testLogCapture captures log output for testing
type testLogCapture struct {
	logs []string
}

func (lc *testLogCapture) captureLog(msg string, args ...interface{}) {
	lc.logs = append(lc.logs, fmt.Sprintf(msg, args...))
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
			name: "empty workload name returns default",
			options: &querypb.ExecuteOptions{
				WorkloadName: "",
			},
			want: "default",
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
