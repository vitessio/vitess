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
package querythrottler

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/topo/topoproto"

	querythrottlerpb "vitess.io/vitess/go/vt/proto/querythrottler"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"

	"vitess.io/vitess/go/test/endtoend/cluster"
)

const (
	waitTimeout = 60 * time.Second
)

// pushQueryThrottlerConfig pushes a QueryThrottlerConfig to the keyspace via gRPC.
func pushQueryThrottlerConfig(t *testing.T, config *querythrottlerpb.Config) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	req := &vtctldatapb.UpdateQueryThrottlerConfigRequest{
		Keyspace:             keyspaceName,
		QueryThrottlerConfig: config,
	}

	var lastErr error
	for {
		_, err := vtctldClient.UpdateQueryThrottlerConfig(ctx, req)
		if err == nil {
			return
		}
		lastErr = err
		t.Logf("pushQueryThrottlerConfig: err=%v", err)
		select {
		case <-ctx.Done():
			require.Failf(t, "timeout", "timed out pushing query throttler config; last err=%v", lastErr)
		case <-ticker.C:
		}
	}
}

// waitForQueryThrottlerStatus polls GetThrottlerStatus via gRPC until
// the throttler reports the expected enabled state.
func waitForQueryThrottlerStatus(t *testing.T, tablet *cluster.Vttablet, wantEnabled bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	tabletAlias, err := topoproto.ParseTabletAlias(tablet.Alias)
	require.NoError(t, err)

	req := &vtctldatapb.GetThrottlerStatusRequest{
		TabletAlias:   tabletAlias,
		ThrottlerType: tabletmanagerdatapb.ThrottlerType_DedicatedQueryThrottler,
	}

	var lastResp *vtctldatapb.GetThrottlerStatusResponse
	for {
		resp, rpcErr := vtctldClient.GetThrottlerStatus(ctx, req)
		lastResp = resp
		if rpcErr == nil && resp.GetStatus() != nil {
			isEnabled := resp.GetStatus().GetIsEnabled()
			if wantEnabled == isEnabled {
				return
			}
		}
		select {
		case <-ctx.Done():
			require.Failf(t, "timeout", "waiting for query throttler enabled=%v on tablet %s; last resp: %v",
				wantEnabled, tablet.Alias, lastResp)
			return
		case <-ticker.C:
		}
	}
}

// waitForQueryThrottlerCheck polls CheckThrottler via gRPC until
// the "lag" metric's response_code matches wantResponseCode.
func waitForQueryThrottlerCheck(t *testing.T, tablet *cluster.Vttablet, wantResponseCode tabletmanagerdatapb.CheckThrottlerResponseCode) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	tabletAlias, err := topoproto.ParseTabletAlias(tablet.Alias)
	require.NoError(t, err)

	req := &vtctldatapb.CheckThrottlerRequest{
		TabletAlias:   tabletAlias,
		ThrottlerType: tabletmanagerdatapb.ThrottlerType_DedicatedQueryThrottler,
	}

	var lastResp *vtctldatapb.CheckThrottlerResponse
	for {
		resp, rpcErr := vtctldClient.CheckThrottler(ctx, req)
		lastResp = resp
		if rpcErr == nil && resp.GetCheck() != nil {
			lagMetric, ok := resp.GetCheck().GetMetrics()["lag"]
			if ok && lagMetric.GetResponseCode() == wantResponseCode {
				t.Logf("waitForQueryThrottlerCheck: lag response_code=%s", wantResponseCode)
				return
			}
		}
		select {
		case <-ctx.Done():
			require.Failf(t, "timeout", "waiting for lag response_code=%s on tablet %s; last resp: %v",
				wantResponseCode, tablet.Alias, lastResp)
			return
		case <-ticker.C:
		}
	}
}

// vtgateExec executes a query via vtgate MySQL connection.
// If expectError is non-empty, the error must contain that substring.
func vtgateExec(t *testing.T, query string, expectError string) *sqltypes.Result {
	t.Helper()
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err, "vtgate connection should succeed")
	defer conn.Close()

	qr, err := conn.ExecuteFetch(query, 1000, true)
	if expectError == "" {
		require.NoError(t, err, "query should succeed: %s", query)
	} else {
		require.Error(t, err, "query should have failed: %s", query)
		require.Contains(t, err.Error(), expectError, "unexpected error for query: %s", query)
	}
	return qr
}

// stopReplication stops replication on the replica tablet.
func stopReplication(t *testing.T) {
	t.Helper()
	_, err := clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput("StopReplication", replicaTablet.Alias)
	require.NoError(t, err)
	clusterInstance.DisableVTOrcRecoveries(t)
}

// startReplication starts replication on the replica tablet and verifies it is running.
func startReplication(t *testing.T) {
	t.Helper()
	clusterInstance.EnableVTOrcRecoveries(t)
	_, err := clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput("StartReplication", replicaTablet.Alias)
	require.NoError(t, err)

	// Verify replication is actually running
	_, err = clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput("GetFullStatus", replicaTablet.Alias)
	require.NoError(t, err)
}

// lagThrottleConfig returns a Config proto that throttles the given statement type on PRIMARY
// when shard/lag exceeds the threshold.
func lagThrottleConfig(stmtType string, thresholdSeconds float64, dryRun bool) *querythrottlerpb.Config {
	return lagThrottleConfigForTabletType("PRIMARY", stmtType, thresholdSeconds, dryRun)
}

// lagThrottleConfigForTabletType returns a Config proto that throttles the given
// statement type on the specified tablet type when lag exceeds the threshold.
func lagThrottleConfigForTabletType(tabletType, stmtType string, thresholdSeconds float64, dryRun bool) *querythrottlerpb.Config {
	return &querythrottlerpb.Config{
		Enabled:  true,
		Strategy: querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER,
		DryRun:   dryRun,
		TabletStrategyConfig: &querythrottlerpb.TabletStrategyConfig{
			TabletRules: map[string]*querythrottlerpb.StatementRuleSet{
				tabletType: {
					StatementRules: map[string]*querythrottlerpb.MetricRuleSet{
						stmtType: {
							MetricRules: map[string]*querythrottlerpb.MetricRule{
								"lag": {
									Thresholds: []*querythrottlerpb.ThrottleThreshold{
										{Above: thresholdSeconds, Throttle: 100},
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

// resetConfig returns a zero-value Config proto that resets the query throttler.
func resetConfig() *querythrottlerpb.Config {
	return &querythrottlerpb.Config{}
}

// disabledConfig returns a Config proto that disables the query throttler.
func disabledConfig() *querythrottlerpb.Config {
	return &querythrottlerpb.Config{Enabled: false}
}

// waitForThrottledQuery polls vtgate until the given query returns an error containing expectError.
func waitForThrottledQuery(t *testing.T, query string, expectError string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		conn, err := mysql.Connect(ctx, &vtParams)
		if err != nil {
			select {
			case <-ctx.Done():
				require.NoError(t, err, "timed out waiting for throttled query, last error was connection failure")
			case <-ticker.C:
				continue
			}
		}
		_, err = conn.ExecuteFetch(query, 1000, true)
		conn.Close()
		if err != nil && strings.Contains(err.Error(), expectError) {
			t.Logf("[SUCCESS] waitForThrottledQuery: query returned error: %v", err)
			return
		}
		select {
		case <-ctx.Done():
			if err != nil {
				require.Failf(t, "timeout", "query error does not contain %q; got: %v", expectError, err)
			} else {
				require.Failf(t, "timeout", "query %q still succeeding, expected error containing %q", query, expectError)
			}
			return
		case <-ticker.C:
		}
	}
}

// waitForUnthrottledQuery polls vtgate until the given query succeeds without error.
func waitForUnthrottledQuery(t *testing.T, query string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	attempt := 0
	var lastErr error
	for {
		attempt++
		conn, err := mysql.Connect(ctx, &vtParams)
		if err != nil {
			t.Logf("waitForUnthrottledQuery attempt %d: connect error: %v", attempt, err)
			lastErr = err
			select {
			case <-ctx.Done():
				require.NoError(t, lastErr, "timed out waiting for query to succeed")
			case <-ticker.C:
				continue
			}
		}
		_, err = conn.ExecuteFetch(query, 1000, true)
		conn.Close()
		if err == nil {
			t.Logf("waitForUnthrottledQuery attempt %d: query succeeded", attempt)
			return
		}
		t.Logf("waitForUnthrottledQuery attempt %d: query error: %v", attempt, err)
		if attempt%5 == 0 {
			tabletAlias, parseErr := topoproto.ParseTabletAlias(primaryTablet.Alias)
			if parseErr == nil {
				checkResp, checkErr := vtctldClient.CheckThrottler(ctx, &vtctldatapb.CheckThrottlerRequest{
					TabletAlias:   tabletAlias,
					ThrottlerType: tabletmanagerdatapb.ThrottlerType_DedicatedQueryThrottler,
				})
				t.Logf("waitForUnthrottledQuery attempt %d: CheckThrottler resp (err=%v): %v", attempt, checkErr, checkResp)
			}
		}
		lastErr = err
		select {
		case <-ctx.Done():
			require.NoError(t, lastErr, "timed out waiting for query %q to stop being throttled", query)
			return
		case <-ticker.C:
		}
	}
}

// TestDisabledByDefault validates that with no QueryThrottlerConfig pushed,
// queries flow through normally (NoOpStrategy active, no throttling).
func TestDisabledByDefault(t *testing.T) {
	t.Run("queries succeed with no throttler config", func(t *testing.T) {
		vtgateExec(t, "select id from t1 limit 1", "")
	})

	t.Run("insert succeeds with no throttler config", func(t *testing.T) {
		vtgateExec(t, "insert into t1 (id, value) values (1000, 'default_test')", "")
	})

	t.Run("cleanup", func(t *testing.T) {
		vtgateExec(t, "delete from t1 where id = 1000", "")
	})
}

// TestEnableAndThrottleWithLag validates the core throttling lifecycle:
// push a config with a very low lag threshold, stop replication to induce lag,
// observe that queries get rejected with ResourceExhausted, then restore
// replication and confirm queries flow again.
func TestEnableAndThrottleWithLag(t *testing.T) {
	defer func() {
		clusterInstance.EnableVTOrcRecoveries(t)
		clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput("StartReplication", replicaTablet.Alias)
		vtctldClient.UpdateQueryThrottlerConfig(context.Background(), &vtctldatapb.UpdateQueryThrottlerConfigRequest{
			Keyspace:             keyspaceName,
			QueryThrottlerConfig: disabledConfig(),
		})
	}()

	t.Run("push config with lag threshold for SELECT", func(t *testing.T) {
		pushQueryThrottlerConfig(t, lagThrottleConfig("SELECT", 5.0, false))
	})

	t.Run("wait for query throttler to be enabled", func(t *testing.T) {
		waitForQueryThrottlerStatus(t, primaryTablet, true)
	})

	t.Run("verify lag metric evaluated by throttler is healthy", func(t *testing.T) {
		waitForQueryThrottlerCheck(t, primaryTablet, tabletmanagerdatapb.CheckThrottlerResponseCode_OK)
	})

	t.Run("queries succeed while replication is healthy", func(t *testing.T) {
		vtgateExec(t, "select id from t1 limit 1", "")
	})

	t.Run("stop replication and wait for lag to exceed threshold", func(t *testing.T) {
		stopReplication(t)
		time.Sleep(6 * time.Second)
	})

	t.Run("wait for underlying throttler to detect lag breach", func(t *testing.T) {
		waitForQueryThrottlerCheck(t, primaryTablet, tabletmanagerdatapb.CheckThrottlerResponseCode_THRESHOLD_EXCEEDED)
	})

	t.Run("SELECT is throttled with ResourceExhausted", func(t *testing.T) {
		waitForThrottledQuery(t, "select id from t1 limit 1", "ResourceExhausted")
	})

	t.Run("start replication to clear lag", func(t *testing.T) {
		startReplication(t)
	})

	t.Run("wait for underlying throttler to report OK after recovery", func(t *testing.T) {
		waitForQueryThrottlerCheck(t, primaryTablet, tabletmanagerdatapb.CheckThrottlerResponseCode_OK)
	})

	t.Run("SELECT succeeds after lag recovers", func(t *testing.T) {
		waitForUnthrottledQuery(t, "select id from t1 limit 1")
	})
}

// TestDisableAfterEnable verifies that disabling the query throttler via config
// stops enforcement immediately, even when the underlying metric is still breached.
func TestDisableAfterEnable(t *testing.T) {
	defer func() {
		clusterInstance.EnableVTOrcRecoveries(t)
		clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput("StartReplication", replicaTablet.Alias)
		vtctldClient.UpdateQueryThrottlerConfig(context.Background(), &vtctldatapb.UpdateQueryThrottlerConfigRequest{
			Keyspace:             keyspaceName,
			QueryThrottlerConfig: disabledConfig(),
		})
	}()

	t.Run("push config to enable throttling", func(t *testing.T) {
		pushQueryThrottlerConfig(t, lagThrottleConfig("SELECT", 5.0, false))
	})

	t.Run("wait for throttler to be enabled", func(t *testing.T) {
		waitForQueryThrottlerStatus(t, primaryTablet, true)
	})

	t.Run("stop replication and wait for lag to build", func(t *testing.T) {
		stopReplication(t)
		time.Sleep(6 * time.Second)
	})

	t.Run("verify queries are throttled", func(t *testing.T) {
		waitForThrottledQuery(t, "select id from t1 limit 1", "ResourceExhausted")
	})

	t.Run("push disabled config", func(t *testing.T) {
		pushQueryThrottlerConfig(t, disabledConfig())
	})

	t.Run("wait for throttler to be disabled", func(t *testing.T) {
		waitForQueryThrottlerStatus(t, primaryTablet, false)
	})

	t.Run("queries succeed even though lag is still high", func(t *testing.T) {
		waitForUnthrottledQuery(t, "select id from t1 limit 1")
	})

	t.Run("start replication to restore healthy state", func(t *testing.T) {
		startReplication(t)
	})
}

// TestDryRunMode verifies that dry_run=true logs throttle decisions without
// actually rejecting queries, and switching to dry_run=false enforces throttling.
func TestDryRunMode(t *testing.T) {
	defer func() {
		clusterInstance.EnableVTOrcRecoveries(t)
		clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput("StartReplication", replicaTablet.Alias)
		vtctldClient.UpdateQueryThrottlerConfig(context.Background(), &vtctldatapb.UpdateQueryThrottlerConfigRequest{
			Keyspace:             keyspaceName,
			QueryThrottlerConfig: disabledConfig(),
		})
	}()

	t.Run("push dry-run config with lag threshold", func(t *testing.T) {
		pushQueryThrottlerConfig(t, lagThrottleConfig("SELECT", 5.0, true))
	})

	t.Run("wait for throttler to be enabled", func(t *testing.T) {
		waitForQueryThrottlerStatus(t, primaryTablet, true)
	})

	t.Run("stop replication and wait for lag to build", func(t *testing.T) {
		stopReplication(t)
		time.Sleep(6 * time.Second)
	})

	t.Run("wait for underlying throttler to detect lag breach", func(t *testing.T) {
		waitForQueryThrottlerCheck(t, primaryTablet, tabletmanagerdatapb.CheckThrottlerResponseCode_THRESHOLD_EXCEEDED)
	})

	t.Run("queries still succeed in dry-run mode", func(t *testing.T) {
		vtgateExec(t, "select id from t1 limit 1", "")
	})

	t.Run("switch to enforcing mode with same threshold", func(t *testing.T) {
		pushQueryThrottlerConfig(t, lagThrottleConfig("SELECT", 5.0, false))
	})

	t.Run("queries are now throttled", func(t *testing.T) {
		waitForThrottledQuery(t, "select id from t1 limit 1", "ResourceExhausted")
	})

	t.Run("start replication to restore healthy state", func(t *testing.T) {
		startReplication(t)
	})
}

// TestThrottleByStatementType verifies that statement_rules correctly scope
// throttling to a specific statement type — SELECT is throttled while INSERT
// passes through unaffected.
func TestThrottleByStatementType(t *testing.T) {
	defer func() {
		clusterInstance.EnableVTOrcRecoveries(t)
		clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput("StartReplication", replicaTablet.Alias)
		vtctldClient.UpdateQueryThrottlerConfig(context.Background(), &vtctldatapb.UpdateQueryThrottlerConfigRequest{
			Keyspace:             keyspaceName,
			QueryThrottlerConfig: disabledConfig(),
		})
	}()

	t.Run("push config that only throttles SELECT", func(t *testing.T) {
		pushQueryThrottlerConfig(t, lagThrottleConfig("SELECT", 5.0, false))
	})

	t.Run("wait for throttler to be enabled", func(t *testing.T) {
		waitForQueryThrottlerStatus(t, primaryTablet, true)
	})

	t.Run("stop replication and wait for lag to build", func(t *testing.T) {
		stopReplication(t)
		time.Sleep(6 * time.Second)
	})

	t.Run("wait for underlying throttler to detect lag breach", func(t *testing.T) {
		waitForQueryThrottlerCheck(t, primaryTablet, tabletmanagerdatapb.CheckThrottlerResponseCode_THRESHOLD_EXCEEDED)
	})

	t.Run("SELECT is throttled", func(t *testing.T) {
		waitForThrottledQuery(t, "select id from t1 limit 1", "ResourceExhausted")
	})

	t.Run("INSERT succeeds despite lag", func(t *testing.T) {
		vtgateExec(t, "insert into t1 (id, value) values (2000, 'stmt_type_test')", "")
	})

	t.Run("cleanup inserted row and start replication", func(t *testing.T) {
		startReplication(t)
		time.Sleep(2 * time.Second)
		vtgateExec(t, "delete from t1 where id = 2000", "")
	})
}

// TestConfigUpdateWithoutRestart verifies that pushing a new config (e.g. changing
// the throttled statement type) takes effect live without requiring a tablet restart.
func TestConfigUpdateWithoutRestart(t *testing.T) {
	defer func() {
		clusterInstance.EnableVTOrcRecoveries(t)
		clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput("StartReplication", replicaTablet.Alias)
		vtctldClient.UpdateQueryThrottlerConfig(context.Background(), &vtctldatapb.UpdateQueryThrottlerConfigRequest{
			Keyspace:             keyspaceName,
			QueryThrottlerConfig: disabledConfig(),
		})
	}()

	t.Run("push config throttling SELECT", func(t *testing.T) {
		pushQueryThrottlerConfig(t, lagThrottleConfig("SELECT", 5.0, false))
	})

	t.Run("wait for throttler to be enabled", func(t *testing.T) {
		waitForQueryThrottlerStatus(t, primaryTablet, true)
	})

	t.Run("stop replication and wait for lag to build", func(t *testing.T) {
		stopReplication(t)
		time.Sleep(6 * time.Second)
	})

	t.Run("wait for underlying throttler to detect lag breach", func(t *testing.T) {
		waitForQueryThrottlerCheck(t, primaryTablet, tabletmanagerdatapb.CheckThrottlerResponseCode_THRESHOLD_EXCEEDED)
	})

	t.Run("SELECT is throttled", func(t *testing.T) {
		waitForThrottledQuery(t, "select id from t1 limit 1", "ResourceExhausted")
	})

	t.Run("live-update config to throttle INSERT instead", func(t *testing.T) {
		pushQueryThrottlerConfig(t, lagThrottleConfig("INSERT", 5.0, false))
	})

	t.Run("SELECT now succeeds after config change", func(t *testing.T) {
		waitForUnthrottledQuery(t, "select id from t1 limit 1")
	})

	t.Run("INSERT is now throttled", func(t *testing.T) {
		waitForThrottledQuery(t, "insert into t1 (id, value) values (3000, 'cfg_update')", "ResourceExhausted")
	})

	t.Run("start replication to restore healthy state", func(t *testing.T) {
		startReplication(t)
	})
}

// TestPriorityZeroNeverThrottled verifies that queries with priority=0 (highest
// priority) bypass throttling entirely, while default priority queries are throttled.
func TestPriorityZeroNeverThrottled(t *testing.T) {
	defer func() {
		clusterInstance.EnableVTOrcRecoveries(t)
		clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput("StartReplication", replicaTablet.Alias)
		vtctldClient.UpdateQueryThrottlerConfig(context.Background(), &vtctldatapb.UpdateQueryThrottlerConfigRequest{
			Keyspace:             keyspaceName,
			QueryThrottlerConfig: disabledConfig(),
		})
	}()

	t.Run("push config throttling SELECT", func(t *testing.T) {
		pushQueryThrottlerConfig(t, lagThrottleConfig("SELECT", 5.0, false))
	})

	t.Run("wait for throttler to be enabled", func(t *testing.T) {
		waitForQueryThrottlerStatus(t, primaryTablet, true)
	})

	t.Run("stop replication and wait for lag to build", func(t *testing.T) {
		stopReplication(t)
		time.Sleep(6 * time.Second)
	})

	t.Run("wait for underlying throttler to detect lag breach", func(t *testing.T) {
		waitForQueryThrottlerCheck(t, primaryTablet, tabletmanagerdatapb.CheckThrottlerResponseCode_THRESHOLD_EXCEEDED)
	})

	t.Run("default priority SELECT is throttled", func(t *testing.T) {
		waitForThrottledQuery(t, "select id from t1 limit 1", "ResourceExhausted")
	})

	t.Run("priority=0 SELECT bypasses throttling", func(t *testing.T) {
		waitForUnthrottledQuery(t, "select /*vt+ PRIORITY=0 */ id from t1 limit 1")
	})

	t.Run("start replication to restore healthy state", func(t *testing.T) {
		startReplication(t)
	})
}

// TestNoRulesForTabletType verifies that when throttle rules are configured
// only for REPLICA, queries routed to the PRIMARY tablet are not throttled.
func TestNoRulesForTabletType(t *testing.T) {
	defer func() {
		clusterInstance.EnableVTOrcRecoveries(t)
		clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput("StartReplication", replicaTablet.Alias)
		vtctldClient.UpdateQueryThrottlerConfig(context.Background(), &vtctldatapb.UpdateQueryThrottlerConfigRequest{
			Keyspace:             keyspaceName,
			QueryThrottlerConfig: disabledConfig(),
		})
	}()

	t.Run("push config with REPLICA-only rules", func(t *testing.T) {
		pushQueryThrottlerConfig(t, lagThrottleConfigForTabletType("REPLICA", "SELECT", 5.0, false))
	})

	t.Run("wait for throttler to be enabled", func(t *testing.T) {
		waitForQueryThrottlerStatus(t, primaryTablet, true)
	})

	t.Run("stop replication and wait for lag to build", func(t *testing.T) {
		stopReplication(t)
		time.Sleep(6 * time.Second)
	})

	t.Run("wait for underlying throttler to detect lag breach", func(t *testing.T) {
		waitForQueryThrottlerCheck(t, primaryTablet, tabletmanagerdatapb.CheckThrottlerResponseCode_THRESHOLD_EXCEEDED)
	})

	t.Run("PRIMARY SELECT succeeds despite lag breach (no PRIMARY rules)", func(t *testing.T) {
		waitForUnthrottledQuery(t, "select id from t1 limit 1")
	})

	t.Run("start replication to restore healthy state", func(t *testing.T) {
		startReplication(t)
	})
}

// TestResetConfigClearsThrottling validates the full lifecycle of enabling,
// resetting, re-enabling, and disabling the query throttler.
func TestResetConfigClearsThrottling(t *testing.T) {
	defer func() {
		clusterInstance.EnableVTOrcRecoveries(t)
		clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput("StartReplication", replicaTablet.Alias)
		vtctldClient.UpdateQueryThrottlerConfig(context.Background(), &vtctldatapb.UpdateQueryThrottlerConfigRequest{
			Keyspace:             keyspaceName,
			QueryThrottlerConfig: disabledConfig(),
		})
	}()

	// --- Step 1: Enable throttler and confirm throttling ---

	t.Run("push config with lag threshold for SELECT", func(t *testing.T) {
		pushQueryThrottlerConfig(t, lagThrottleConfig("SELECT", 5.0, false))
	})

	t.Run("wait for throttler to be enabled", func(t *testing.T) {
		waitForQueryThrottlerStatus(t, primaryTablet, true)
	})

	t.Run("stop replication and wait for lag to build", func(t *testing.T) {
		stopReplication(t)
		time.Sleep(6 * time.Second)
	})

	t.Run("wait for underlying throttler to detect lag breach", func(t *testing.T) {
		waitForQueryThrottlerCheck(t, primaryTablet, tabletmanagerdatapb.CheckThrottlerResponseCode_THRESHOLD_EXCEEDED)
	})

	t.Run("SELECT is throttled with ResourceExhausted", func(t *testing.T) {
		waitForThrottledQuery(t, "select id from t1 limit 1", "ResourceExhausted")
	})

	// --- Step 2: Reset config with {} and confirm no throttling ---

	t.Run("push reset config (zero-value proto)", func(t *testing.T) {
		pushQueryThrottlerConfig(t, resetConfig())
	})

	t.Run("wait for throttler to be disabled after reset", func(t *testing.T) {
		waitForQueryThrottlerStatus(t, primaryTablet, false)
	})

	t.Run("SELECT succeeds after config reset despite lag still being high", func(t *testing.T) {
		waitForUnthrottledQuery(t, "select id from t1 limit 1")
	})

	// --- Step 3: Re-enable and confirm throttling resumes ---

	t.Run("re-push config with lag threshold for SELECT", func(t *testing.T) {
		pushQueryThrottlerConfig(t, lagThrottleConfig("SELECT", 5.0, false))
	})

	t.Run("wait for throttler to be re-enabled", func(t *testing.T) {
		waitForQueryThrottlerStatus(t, primaryTablet, true)
	})

	t.Run("wait for underlying throttler to detect lag breach again", func(t *testing.T) {
		waitForQueryThrottlerCheck(t, primaryTablet, tabletmanagerdatapb.CheckThrottlerResponseCode_THRESHOLD_EXCEEDED)
	})

	t.Run("SELECT is throttled again after re-enable", func(t *testing.T) {
		waitForThrottledQuery(t, "select id from t1 limit 1", "ResourceExhausted")
	})

	// --- Step 4: Disable and confirm no throttling ---

	t.Run("push disabled config", func(t *testing.T) {
		pushQueryThrottlerConfig(t, disabledConfig())
	})

	t.Run("wait for throttler to be disabled", func(t *testing.T) {
		waitForQueryThrottlerStatus(t, primaryTablet, false)
	})

	t.Run("SELECT succeeds after explicit disable despite lag still being high", func(t *testing.T) {
		waitForUnthrottledQuery(t, "select id from t1 limit 1")
	})

	// --- Cleanup ---

	t.Run("start replication to restore healthy state", func(t *testing.T) {
		startReplication(t)
	})
}
