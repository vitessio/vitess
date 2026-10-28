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
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"

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
	req := &vtctldatapb.UpdateQueryThrottlerConfigRequest{
		Keyspace:             keyspaceName,
		QueryThrottlerConfig: config,
	}
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		_, err := vtctldClient.UpdateQueryThrottlerConfig(t.Context(), req)
		assert.NoError(c, err, "pushing query throttler config")
	}, waitTimeout, time.Second)
}

// requireKeyspaceQueryThrottlerConfig requires the keyspace to store want as its query throttler config.
func requireKeyspaceQueryThrottlerConfig(t *testing.T, want *querythrottlerpb.Config) {
	t.Helper()
	resp, err := vtctldClient.GetKeyspace(t.Context(), &vtctldatapb.GetKeyspaceRequest{Keyspace: keyspaceName})
	require.NoError(t, err)
	got := resp.GetKeyspace().GetKeyspace().GetQueryThrottlerConfig()
	require.True(t, proto.Equal(want, got), "keyspace query throttler config: want %v, got %v", want, got)
}

// waitForQueryThrottlerStatus polls GetThrottlerStatus via gRPC until
// the throttler reports the expected enabled state.
func waitForQueryThrottlerStatus(t *testing.T, tablet *cluster.Vttablet, wantEnabled bool) {
	t.Helper()
	tabletAlias, err := topoproto.ParseTabletAlias(tablet.Alias)
	require.NoError(t, err)

	req := &vtctldatapb.GetThrottlerStatusRequest{
		TabletAlias:   tabletAlias,
		ThrottlerType: tabletmanagerdatapb.ThrottlerType_DEDICATED_QUERY_THROTTLER,
	}
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		resp, err := vtctldClient.GetThrottlerStatus(t.Context(), req)
		require.NoError(c, err)
		require.NotNil(c, resp.GetStatus())
		assert.Equal(c, tabletmanagerdatapb.ThrottlerType_DEDICATED_QUERY_THROTTLER, resp.GetStatus().GetThrottlerType(), "throttler that answered on tablet %s", tablet.Alias)
		assert.Equal(c, wantEnabled, resp.GetStatus().GetIsEnabled(), "query throttler enabled state on tablet %s", tablet.Alias)
	}, waitTimeout, time.Second)
}

// waitForQueryThrottlerCheck polls CheckThrottler via gRPC until
// the "lag" metric's response_code matches wantResponseCode.
func waitForQueryThrottlerCheck(t *testing.T, tablet *cluster.Vttablet, wantResponseCode tabletmanagerdatapb.CheckThrottlerResponseCode) {
	t.Helper()
	tabletAlias, err := topoproto.ParseTabletAlias(tablet.Alias)
	require.NoError(t, err)

	req := &vtctldatapb.CheckThrottlerRequest{
		TabletAlias:   tabletAlias,
		ThrottlerType: tabletmanagerdatapb.ThrottlerType_DEDICATED_QUERY_THROTTLER,
	}
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		resp, err := vtctldClient.CheckThrottler(t.Context(), req)
		require.NoError(c, err)
		assert.Equal(c, tabletmanagerdatapb.ThrottlerType_DEDICATED_QUERY_THROTTLER, resp.GetCheck().GetThrottlerType(), "throttler that answered on tablet %s", tablet.Alias)
		lagMetric, ok := resp.GetCheck().GetMetrics()["lag"]
		require.True(c, ok, "lag metric missing from check on tablet %s: %v", tablet.Alias, resp)
		assert.Equal(c, wantResponseCode, lagMetric.GetResponseCode(), "lag response_code on tablet %s", tablet.Alias)
	}, waitTimeout, 2*time.Second)
}

// vtgateExec executes a query via vtgate MySQL connection.
// If expectError is non-empty, the error must contain that substring.
func vtgateExec(t *testing.T, query string, expectError string) *sqltypes.Result {
	t.Helper()
	conn, err := mysql.Connect(t.Context(), &vtParams)
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

// lagRule names a statement type to throttle on a tablet type.
type lagRule struct {
	tabletType string
	stmtType   string
}

// multiLagThrottleConfig returns an enforcing Config proto that throttles every rule's
// statement type on its tablet type when lag exceeds the threshold.
func multiLagThrottleConfig(thresholdSeconds float64, rules ...lagRule) *querythrottlerpb.Config {
	tabletRules := make(map[string]*querythrottlerpb.StatementRuleSet)
	for _, r := range rules {
		if tabletRules[r.tabletType] == nil {
			tabletRules[r.tabletType] = &querythrottlerpb.StatementRuleSet{StatementRules: map[string]*querythrottlerpb.MetricRuleSet{}}
		}
		tabletRules[r.tabletType].StatementRules[r.stmtType] = &querythrottlerpb.MetricRuleSet{
			MetricRules: map[string]*querythrottlerpb.MetricRule{
				"lag": {
					Thresholds: []*querythrottlerpb.ThrottleThreshold{
						{Above: thresholdSeconds, Throttle: 100},
					},
				},
			},
		}
	}
	return &querythrottlerpb.Config{
		Enabled:              true,
		Strategy:             querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER,
		TabletStrategyConfig: &querythrottlerpb.TabletStrategyConfig{TabletRules: tabletRules},
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

// restoreAfterTest registers a cleanup that restarts replication and disables the query
// throttler, so a failed test does not leave lag or throttling behind for the next one.
func restoreAfterTest(t *testing.T) {
	t.Helper()
	t.Cleanup(func() {
		// t.Context() is canceled before cleanups run.
		ctx := context.WithoutCancel(t.Context())
		clusterInstance.EnableVTOrcRecoveries(t)
		_, err := clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput("StartReplication", replicaTablet.Alias)
		assert.NoError(t, err, "restarting replication")
		_, err = vtctldClient.UpdateQueryThrottlerConfig(ctx, &vtctldatapb.UpdateQueryThrottlerConfigRequest{
			Keyspace:             keyspaceName,
			QueryThrottlerConfig: disabledConfig(),
		})
		assert.NoError(t, err, "disabling the query throttler")
	})
}

// throttledCount returns the tablet's QueryThrottlerThrottled count, summed over every
// label set with the given DryRun value.
func throttledCount(tablet *cluster.Vttablet, dryRun bool) float64 {
	counts, _ := tablet.VttabletProcess.GetVars()["QueryThrottlerThrottled"].(map[string]any)
	// Keys are the label values joined with '.', and DryRun is the last label.
	suffix := "." + strconv.FormatBool(dryRun)
	var total float64
	for key, value := range counts {
		if n, ok := value.(float64); ok && strings.HasSuffix(key, suffix) {
			total += n
		}
	}
	return total
}

// waitForDryRunDecision polls vtgate with query until the primary records a dry-run
// throttle decision for it. Every attempt must succeed, since dry_run never rejects.
func waitForDryRunDecision(t *testing.T, query string) {
	t.Helper()
	baseline := throttledCount(primaryTablet, true)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		conn, err := mysql.Connect(t.Context(), &vtParams)
		require.NoError(c, err)
		defer conn.Close()

		// A failure on c would only retry, so a rejected query fails the test itself, and
		// returning without a failure on c ends the wait.
		_, err = conn.ExecuteFetch(query, 1000, true)
		if !assert.NoError(t, err, "query %q must succeed in dry-run mode", query) {
			return
		}
		assert.Greater(c, throttledCount(primaryTablet, true), baseline, "no dry-run throttle decision recorded for %q", query)
	}, waitTimeout, time.Second)
}

// waitForThrottledQuery polls vtgate until the given query returns an error containing
// expectError. Any setup statements run first on the same connection. A query throttler rejection must reach the client as ER_OUT_OF_RESOURCES
// (1041), so the errno is checked as well.
func waitForThrottledQuery(t *testing.T, query string, expectError string, setup ...string) {
	t.Helper()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		conn, err := mysql.Connect(t.Context(), &vtParams)
		require.NoError(c, err)
		defer conn.Close()

		for _, stmt := range setup {
			_, err = conn.ExecuteFetch(stmt, 1000, true)
			require.NoError(c, err, "setup statement %q", stmt)
		}
		_, err = conn.ExecuteFetch(query, 1000, true)
		require.Error(c, err, "query %q still succeeding, expected error containing %q", query, expectError)
		assert.Contains(c, err.Error(), expectError)
		var sqlErr *sqlerror.SQLError
		if assert.ErrorAs(c, err, &sqlErr, "expected a MySQL error") {
			assert.Equal(c, sqlerror.EROutOfResources, sqlErr.Number(), "unexpected errno for throttled query: %v", err)
		}
	}, waitTimeout, 2*time.Second)
}

// waitForUnthrottledQuery polls vtgate until the given query succeeds without error.
func waitForUnthrottledQuery(t *testing.T, query string) {
	t.Helper()
	attempt := 0
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		attempt++
		conn, err := mysql.Connect(t.Context(), &vtParams)
		require.NoError(c, err)
		defer conn.Close()

		_, err = conn.ExecuteFetch(query, 1000, true)
		if err != nil && attempt%5 == 0 {
			// Log what the throttler reports, to help diagnose a query that stays throttled.
			if tabletAlias, parseErr := topoproto.ParseTabletAlias(primaryTablet.Alias); parseErr == nil {
				checkResp, checkErr := vtctldClient.CheckThrottler(t.Context(), &vtctldatapb.CheckThrottlerRequest{
					TabletAlias:   tabletAlias,
					ThrottlerType: tabletmanagerdatapb.ThrottlerType_DEDICATED_QUERY_THROTTLER,
				})
				t.Logf("waitForUnthrottledQuery attempt %d: CheckThrottler resp (err=%v): %v", attempt, checkErr, checkResp)
			}
		}
		assert.NoError(c, err, "query %q is still throttled", query)
	}, waitTimeout, 2*time.Second)
}

// TestDisabledByDefault validates that with no QueryThrottlerConfig pushed, queries are never
// throttled, even while the replica lags and the lag throttler, which follows the keyspace's
// ThrottlerConfig, reports that lag.
func TestDisabledByDefault(t *testing.T) {
	requireKeyspaceQueryThrottlerConfig(t, nil)
	t.Cleanup(func() {
		// t.Context() is canceled before cleanups run.
		ctx := context.WithoutCancel(t.Context())
		clusterInstance.EnableVTOrcRecoveries(t)
		_, err := clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput("StartReplication", replicaTablet.Alias)
		assert.NoError(t, err, "restarting replication")
		_, err = vtctldClient.UpdateThrottlerConfig(ctx, &vtctldatapb.UpdateThrottlerConfigRequest{Keyspace: keyspaceName, Disable: true})
		assert.NoError(t, err, "disabling the lag throttler")
	})

	t.Run("enable the lag throttler", func(t *testing.T) {
		req := &vtctldatapb.UpdateThrottlerConfigRequest{Keyspace: keyspaceName, Enable: true, Threshold: 1}
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			_, err := vtctldClient.UpdateThrottlerConfig(t.Context(), req)
			assert.NoError(c, err, "enabling the lag throttler")
		}, waitTimeout, time.Second)
	})

	t.Run("stop replication to induce lag", func(t *testing.T) {
		stopReplication(t)
	})

	t.Run("lag throttler reports the lag", func(t *testing.T) {
		tabletAlias, err := topoproto.ParseTabletAlias(primaryTablet.Alias)
		require.NoError(t, err)
		// Unlike the "vitess" app's, the "test" app's checks renew the heartbeat lease.
		req := &vtctldatapb.CheckThrottlerRequest{TabletAlias: tabletAlias, AppName: throttlerapp.TestingName.String()}
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			resp, err := vtctldClient.CheckThrottler(t.Context(), req)
			require.NoError(c, err)
			assert.Equal(c, tabletmanagerdatapb.ThrottlerType_DEFAULT_LAG_THROTTLER, resp.GetCheck().GetThrottlerType())
			assert.Equal(c, tabletmanagerdatapb.CheckThrottlerResponseCode_THRESHOLD_EXCEEDED, resp.GetCheck().GetResponseCode())
		}, waitTimeout, time.Second)
	})

	t.Run("query throttler is still disabled", func(t *testing.T) {
		tabletAlias, err := topoproto.ParseTabletAlias(primaryTablet.Alias)
		require.NoError(t, err)
		resp, err := vtctldClient.GetThrottlerStatus(t.Context(), &vtctldatapb.GetThrottlerStatusRequest{
			TabletAlias:   tabletAlias,
			ThrottlerType: tabletmanagerdatapb.ThrottlerType_DEDICATED_QUERY_THROTTLER,
		})
		require.NoError(t, err)
		assert.Equal(t, tabletmanagerdatapb.ThrottlerType_DEDICATED_QUERY_THROTTLER, resp.GetStatus().GetThrottlerType())
		assert.False(t, resp.GetStatus().GetIsEnabled(), "the lag throttler's config must not enable the query throttler")
	})

	t.Run("SELECT and INSERT succeed under lag", func(t *testing.T) {
		vtgateExec(t, "select id from t1 limit 1", "")
		vtgateExec(t, "insert into t1 (id, value) values (1000, 'default_test')", "")
		vtgateExec(t, "delete from t1 where id = 1000", "")
	})
}

// TestEnableAndThrottleWithLag validates the core throttling lifecycle:
// push a config with a very low lag threshold, stop replication to induce lag,
// observe that queries get rejected with ResourceExhausted, then restore
// replication and confirm queries flow again.
func TestEnableAndThrottleWithLag(t *testing.T) {
	restoreAfterTest(t)

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

	t.Run("stop replication to induce lag", func(t *testing.T) {
		stopReplication(t)
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
	restoreAfterTest(t)

	t.Run("push config to enable throttling", func(t *testing.T) {
		pushQueryThrottlerConfig(t, lagThrottleConfig("SELECT", 5.0, false))
	})

	t.Run("wait for throttler to be enabled", func(t *testing.T) {
		waitForQueryThrottlerStatus(t, primaryTablet, true)
	})

	t.Run("stop replication to induce lag", func(t *testing.T) {
		stopReplication(t)
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
	restoreAfterTest(t)

	// Push the config through vtctldclient, as an operator would, rather than over gRPC like the
	// other tests, so the CLI's parsing is covered end to end.
	const dryRunConfigJSON = `{"enabled":true,"strategy":"TABLET_THROTTLER","dry_run":true,"tablet_strategy_config":{"tablet_rules":{"PRIMARY":{"statement_rules":{"SELECT":{"metric_rules":{"lag":{"thresholds":[{"above":5,"throttle":100}]}}}}}}}}`

	t.Run("push dry-run config with lag threshold through vtctldclient", func(t *testing.T) {
		out, err := clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput("UpdateQueryThrottlerConfig", "--config", dryRunConfigJSON, keyspaceName)
		require.NoError(t, err, out)
		requireKeyspaceQueryThrottlerConfig(t, lagThrottleConfig("SELECT", 5.0, true))
	})

	t.Run("vtctldclient refuses a misspelled key and leaves the config unchanged", func(t *testing.T) {
		// Dropping the unknown "dry-run" key would push an enforcing config.
		misspelled := strings.Replace(dryRunConfigJSON, `"dry_run":true`, `"dry-run":true`, 1)
		require.NotEqual(t, dryRunConfigJSON, misspelled)
		out, err := clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput("UpdateQueryThrottlerConfig", "--config", misspelled, keyspaceName)
		require.Error(t, err)
		// vtctldclient logs the error as JSON, which escapes the quotes around the field name.
		assert.Contains(t, out, "unknown field")
		assert.Contains(t, out, "dry-run")
		requireKeyspaceQueryThrottlerConfig(t, lagThrottleConfig("SELECT", 5.0, true))
	})

	t.Run("wait for throttler to be enabled", func(t *testing.T) {
		waitForQueryThrottlerStatus(t, primaryTablet, true)
	})

	t.Run("stop replication to induce lag", func(t *testing.T) {
		stopReplication(t)
	})

	t.Run("wait for underlying throttler to detect lag breach", func(t *testing.T) {
		waitForQueryThrottlerCheck(t, primaryTablet, tabletmanagerdatapb.CheckThrottlerResponseCode_THRESHOLD_EXCEEDED)
	})

	t.Run("queries still succeed in dry-run mode while the throttler decides to throttle them", func(t *testing.T) {
		waitForDryRunDecision(t, "select id from t1 limit 1")
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
	restoreAfterTest(t)

	t.Run("push config that only throttles SELECT", func(t *testing.T) {
		pushQueryThrottlerConfig(t, lagThrottleConfig("SELECT", 5.0, false))
	})

	t.Run("wait for throttler to be enabled", func(t *testing.T) {
		waitForQueryThrottlerStatus(t, primaryTablet, true)
	})

	t.Run("stop replication to induce lag", func(t *testing.T) {
		stopReplication(t)
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
		vtgateExec(t, "delete from t1 where id = 2000", "")
	})
}

// TestConfigUpdateWithoutRestart verifies that pushing a new config (e.g. changing
// the throttled statement type) takes effect live without requiring a tablet restart.
func TestConfigUpdateWithoutRestart(t *testing.T) {
	restoreAfterTest(t)

	t.Run("push config throttling SELECT", func(t *testing.T) {
		pushQueryThrottlerConfig(t, lagThrottleConfig("SELECT", 5.0, false))
	})

	t.Run("wait for throttler to be enabled", func(t *testing.T) {
		waitForQueryThrottlerStatus(t, primaryTablet, true)
	})

	t.Run("stop replication to induce lag", func(t *testing.T) {
		stopReplication(t)
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
	restoreAfterTest(t)

	t.Run("push config throttling SELECT", func(t *testing.T) {
		pushQueryThrottlerConfig(t, lagThrottleConfig("SELECT", 5.0, false))
	})

	t.Run("wait for throttler to be enabled", func(t *testing.T) {
		waitForQueryThrottlerStatus(t, primaryTablet, true)
	})

	t.Run("stop replication to induce lag", func(t *testing.T) {
		stopReplication(t)
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

// TestNoRulesForTabletType verifies that a SELECT rule configured only for REPLICA
// throttles SELECTs routed to the REPLICA tablet but not those routed to the PRIMARY.
func TestNoRulesForTabletType(t *testing.T) {
	restoreAfterTest(t)

	t.Run("push config with a REPLICA SELECT rule and a PRIMARY INSERT rule", func(t *testing.T) {
		pushQueryThrottlerConfig(t, multiLagThrottleConfig(5.0,
			lagRule{tabletType: "REPLICA", stmtType: "SELECT"},
			lagRule{tabletType: "PRIMARY", stmtType: "INSERT"},
		))
	})

	t.Run("wait for throttler to be enabled", func(t *testing.T) {
		waitForQueryThrottlerStatus(t, primaryTablet, true)
		waitForQueryThrottlerStatus(t, replicaTablet, true)
	})

	t.Run("stop replication to induce lag", func(t *testing.T) {
		stopReplication(t)
	})

	t.Run("wait for underlying throttler to detect lag breach", func(t *testing.T) {
		waitForQueryThrottlerCheck(t, primaryTablet, tabletmanagerdatapb.CheckThrottlerResponseCode_THRESHOLD_EXCEEDED)
	})

	t.Run("PRIMARY INSERT is throttled", func(t *testing.T) {
		// Positive control: the primary's throttle decisions see the lag breach.
		waitForThrottledQuery(t, "insert into t1 (id, value) values (4000, 'no_rules')", "ResourceExhausted")
	})

	t.Run("PRIMARY SELECT succeeds despite lag breach (no PRIMARY SELECT rule)", func(t *testing.T) {
		vtgateExec(t, "select id from t1 limit 1", "")
	})

	t.Run("REPLICA SELECT is throttled", func(t *testing.T) {
		waitForThrottledQuery(t, "select id from t1 limit 1", "ResourceExhausted", "use `"+keyspaceName+"@replica`")
	})

	t.Run("start replication to restore healthy state", func(t *testing.T) {
		startReplication(t)
	})
}

// TestResetConfigClearsThrottling validates the full lifecycle of enabling,
// resetting, re-enabling, and disabling the query throttler.
func TestResetConfigClearsThrottling(t *testing.T) {
	restoreAfterTest(t)

	// --- Step 1: Enable throttler and confirm throttling ---

	t.Run("push config with lag threshold for SELECT", func(t *testing.T) {
		pushQueryThrottlerConfig(t, lagThrottleConfig("SELECT", 5.0, false))
	})

	t.Run("wait for throttler to be enabled", func(t *testing.T) {
		waitForQueryThrottlerStatus(t, primaryTablet, true)
	})

	t.Run("stop replication to induce lag", func(t *testing.T) {
		stopReplication(t)
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

// TestThrottlePathsUnderLag covers, under a single lag breach, the statement types and
// execution paths the other tests do not: REPLACE, UPDATE and DELETE, OLAP streaming,
// statements inside a transaction, and a RebuildKeyspaceGraph while throttling.
func TestThrottlePathsUnderLag(t *testing.T) {
	restoreAfterTest(t)

	config := multiLagThrottleConfig(5.0,
		lagRule{tabletType: "PRIMARY", stmtType: "SELECT"},
		lagRule{tabletType: "PRIMARY", stmtType: "REPLACE"},
		lagRule{tabletType: "PRIMARY", stmtType: "UPDATE"},
		lagRule{tabletType: "PRIMARY", stmtType: "DELETE"},
	)

	t.Run("push config throttling SELECT, REPLACE, UPDATE and DELETE", func(t *testing.T) {
		pushQueryThrottlerConfig(t, config)
	})

	t.Run("wait for throttler to be enabled", func(t *testing.T) {
		waitForQueryThrottlerStatus(t, primaryTablet, true)
	})

	t.Run("stop replication to induce lag", func(t *testing.T) {
		stopReplication(t)
	})

	t.Run("wait for underlying throttler to detect lag breach", func(t *testing.T) {
		waitForQueryThrottlerCheck(t, primaryTablet, tabletmanagerdatapb.CheckThrottlerResponseCode_THRESHOLD_EXCEEDED)
	})

	// vtgate rejects REPLACE in a sharded keyspace, so send it straight to the shard.
	shardTarget := fmt.Sprintf("use `%s:%s`", keyspaceName, clusterInstance.Keyspaces[0].Shards[0].Name)
	for _, tc := range []struct {
		stmtType string
		query    string
		setup    []string
	}{
		{stmtType: "REPLACE", query: "replace into t1 (id, value) values (5000, 'replace')", setup: []string{shardTarget}},
		{stmtType: "UPDATE", query: "update t1 set value = 'update' where id = 5000"},
		{stmtType: "DELETE", query: "delete from t1 where id = 5000"},
	} {
		t.Run(tc.stmtType+" is throttled", func(t *testing.T) {
			waitForThrottledQuery(t, tc.query, "ResourceExhausted", tc.setup...)
		})
	}

	t.Run("OLAP streaming SELECT is throttled", func(t *testing.T) {
		waitForThrottledQuery(t, "select id from t1 limit 1", "ResourceExhausted", "set workload = olap")
	})

	t.Run("statements inside a transaction are not throttled", func(t *testing.T) {
		// Autocommit statements are throttled, so the transaction below runs during the breach.
		waitForThrottledQuery(t, "select id from t1 limit 1", "ResourceExhausted")

		conn, err := mysql.Connect(t.Context(), &vtParams)
		require.NoError(t, err)
		t.Cleanup(conn.Close)

		// The first statement opens the transaction on the tablet (BeginExecute), and
		// the rest run in it (Execute with a transaction ID); cover both.
		for _, query := range []string{
			"begin",
			"select id from t1 limit 1",
			"update t1 set value = 'in_txn' where id = 5000",
			"select id from t1 limit 1",
			"commit",
		} {
			_, err := conn.ExecuteFetch(query, 1000, true)
			require.NoError(t, err, "%q must not be throttled inside a transaction", query)
		}
	})

	t.Run("RebuildKeyspaceGraph keeps the config and throttling", func(t *testing.T) {
		_, err := clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput("RebuildKeyspaceGraph", keyspaceName)
		require.NoError(t, err)

		resp, err := vtctldClient.GetSrvKeyspaces(t.Context(), &vtctldatapb.GetSrvKeyspacesRequest{
			Keyspace: keyspaceName,
			Cells:    []string{cell},
		})
		require.NoError(t, err)
		got := resp.GetSrvKeyspaces()[cell].GetQueryThrottlerConfig()
		require.NotNil(t, got, "RebuildKeyspaceGraph dropped the query throttler config")
		assert.True(t, got.GetEnabled())
		assert.Len(t, got.GetTabletStrategyConfig().GetTabletRules()["PRIMARY"].GetStatementRules(), 4)

		waitForQueryThrottlerStatus(t, primaryTablet, true)
		waitForThrottledQuery(t, "select id from t1 limit 1", "ResourceExhausted")
	})

	t.Run("start replication to restore healthy state", func(t *testing.T) {
		startReplication(t)
	})
}
