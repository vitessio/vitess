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

	querythrottlerpb "vitess.io/vitess/go/vt/proto/querythrottler"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/querythrottler/registry"
)

// createTestSrvKeyspace creates a SrvKeyspace with query throttler config for testing
func createTestSrvKeyspace(enabled bool, strategy querythrottlerpb.ThrottlingStrategy, dryRun bool) *topodatapb.SrvKeyspace {
	return &topodatapb.SrvKeyspace{
		QueryThrottlerConfig: &querythrottlerpb.Config{
			Enabled:  enabled,
			Strategy: strategy,
			DryRun:   dryRun,
		},
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

func (lc *testLogCapture) captureLog(msg string, args ...any) {
	lc.logs = append(lc.logs, fmt.Sprintf(msg, args...))
}
