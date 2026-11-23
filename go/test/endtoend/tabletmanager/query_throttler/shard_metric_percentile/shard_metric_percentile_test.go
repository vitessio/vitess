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

package shardmetricpercentile

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/topo/topoproto"

	querythrottlerpb "vitess.io/vitess/go/vt/proto/querythrottler"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

const (
	// With four replicas, p75 selects the third lowest lag, so it tolerates one lagging replica but not two.
	replicaCount = 4
	waitTimeout  = 60 * time.Second
	lagThreshold = 5.0
	selectQuery  = "select id from t1 limit 1"
)

// lagThrottleConfig returns a Config proto that rejects every SELECT on PRIMARY while the shard's
// lag, at the given shard percentile, is above lagThreshold.
func lagThrottleConfig(shardPercentile float64) *querythrottlerpb.Config {
	return &querythrottlerpb.Config{
		Enabled:  true,
		Strategy: querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER,
		TabletStrategyConfig: &querythrottlerpb.TabletStrategyConfig{
			MetricShardPercentiles: map[string]float64{"lag": shardPercentile},
			TabletRules: map[string]*querythrottlerpb.StatementRuleSet{
				"PRIMARY": {StatementRules: map[string]*querythrottlerpb.MetricRuleSet{
					"SELECT": {MetricRules: map[string]*querythrottlerpb.MetricRule{
						"lag": {Thresholds: []*querythrottlerpb.ThrottleThreshold{{Above: lagThreshold, Throttle: 100}}},
					}},
				}},
			},
		},
	}
}

// disabledConfig returns a Config proto that disables the query throttler.
func disabledConfig() *querythrottlerpb.Config {
	return &querythrottlerpb.Config{Enabled: false}
}

// pushQueryThrottlerConfig pushes a QueryThrottlerConfig to the keyspace via gRPC.
func pushQueryThrottlerConfig(t *testing.T, config *querythrottlerpb.Config) {
	t.Helper()
	req := &vtctldatapb.UpdateQueryThrottlerConfigRequest{Keyspace: keyspaceName, QueryThrottlerConfig: config}
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		_, err := vtctldClient.UpdateQueryThrottlerConfig(t.Context(), req)
		assert.NoError(c, err, "pushing query throttler config")
	}, waitTimeout, time.Second)
}

// waitForPrimaryLagCheck waits until the primary's dedicated query throttler reports wantResponseCode for "lag".
func waitForPrimaryLagCheck(t *testing.T, wantResponseCode tabletmanagerdatapb.CheckThrottlerResponseCode) {
	t.Helper()
	tabletAlias, err := topoproto.ParseTabletAlias(primaryTablet.Alias)
	require.NoError(t, err)
	req := &vtctldatapb.CheckThrottlerRequest{
		TabletAlias:   tabletAlias,
		ThrottlerType: tabletmanagerdatapb.ThrottlerType_DEDICATED_QUERY_THROTTLER,
	}
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		resp, err := vtctldClient.CheckThrottler(t.Context(), req)
		require.NoError(c, err)
		assert.Equal(c, tabletmanagerdatapb.ThrottlerType_DEDICATED_QUERY_THROTTLER, resp.GetCheck().GetThrottlerType(), "throttler that answered")
		lagMetric, ok := resp.GetCheck().GetMetrics()["lag"]
		require.True(c, ok, "lag metric missing from check: %v", resp)
		assert.Equal(c, wantResponseCode, lagMetric.GetResponseCode(), "lag value %v", lagMetric.GetValue())
	}, waitTimeout, time.Second)
}

// waitForSelect polls vtgate until selectQuery succeeds, or, if wantThrottled, until the query
// throttler rejects it with ER_OUT_OF_RESOURCES (1041).
func waitForSelect(t *testing.T, wantThrottled bool) {
	t.Helper()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		conn, err := mysql.Connect(t.Context(), &vtParams)
		require.NoError(c, err)
		defer conn.Close()

		_, err = conn.ExecuteFetch(selectQuery, 1000, true)
		if !wantThrottled {
			assert.NoError(c, err, "query %q is still throttled", selectQuery)
			return
		}
		require.Error(c, err, "query %q still succeeding", selectQuery)
		assert.Contains(c, err.Error(), "ResourceExhausted")
		var sqlErr *sqlerror.SQLError
		if assert.ErrorAs(c, err, &sqlErr, "expected a MySQL error") {
			assert.Equal(c, sqlerror.EROutOfResources, sqlErr.Number(), "unexpected errno for throttled query: %v", err)
		}
	}, waitTimeout, time.Second)
}

// stopReplication stops replication on the replica, and stops VTOrc from restarting it.
func stopReplication(t *testing.T, replica *cluster.Vttablet) {
	t.Helper()
	_, err := clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput("StopReplication", replica.Alias)
	require.NoError(t, err)
	clusterInstance.DisableVTOrcRecoveries(t)
}

// restoreAfterTest registers a cleanup that restarts replication on every replica and disables the
// query throttler, so a failed test does not leave lag or throttling behind for the next one.
func restoreAfterTest(t *testing.T) {
	t.Helper()
	t.Cleanup(func() {
		// t.Context() is canceled before cleanups run.
		ctx := context.WithoutCancel(t.Context())
		clusterInstance.EnableVTOrcRecoveries(t)
		for _, replica := range replicaTablets {
			_, err := clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput("StartReplication", replica.Alias)
			assert.NoError(t, err, "restarting replication on %s", replica.Alias)
		}
		_, err := vtctldClient.UpdateQueryThrottlerConfig(ctx, &vtctldatapb.UpdateQueryThrottlerConfigRequest{
			Keyspace:             keyspaceName,
			QueryThrottlerConfig: disabledConfig(),
		})
		assert.NoError(t, err, "disabling the query throttler")
	})
}

// TestShardMetricPercentile stops replication on one replica of four, then on a second one. With
// the percentile unset, the primary throttles on the lagging replica. At p75, it tolerates one
// lagging replica, until a second one lags.
func TestShardMetricPercentile(t *testing.T) {
	restoreAfterTest(t)

	pushQueryThrottlerConfig(t, lagThrottleConfig(0))
	waitForPrimaryLagCheck(t, tabletmanagerdatapb.CheckThrottlerResponseCode_OK)
	waitForSelect(t, false)

	t.Run("unset throttles on one lagging replica", func(t *testing.T) {
		stopReplication(t, replicaTablets[3])
		waitForPrimaryLagCheck(t, tabletmanagerdatapb.CheckThrottlerResponseCode_THRESHOLD_EXCEEDED)
		waitForSelect(t, true)
	})

	t.Run("p75 tolerates one lagging replica", func(t *testing.T) {
		pushQueryThrottlerConfig(t, lagThrottleConfig(75))
		waitForPrimaryLagCheck(t, tabletmanagerdatapb.CheckThrottlerResponseCode_OK)
		waitForSelect(t, false)
	})

	t.Run("p75 throttles on two lagging replicas", func(t *testing.T) {
		stopReplication(t, replicaTablets[2])
		waitForPrimaryLagCheck(t, tabletmanagerdatapb.CheckThrottlerResponseCode_THRESHOLD_EXCEEDED)
		waitForSelect(t, true)
	})
}
