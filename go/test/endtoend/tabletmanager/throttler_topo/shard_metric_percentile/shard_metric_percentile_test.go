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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/throttler"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/base"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
)

const (
	// With four replicas, p75 selects the third lowest lag, so it tolerates one lagging replica but not two.
	replicaCount = 4
	waitTimeout  = 60 * time.Second
)

// updateThrottlerConfig runs vtctldclient UpdateThrottlerConfig on the keyspace with the given flags.
func updateThrottlerConfig(t *testing.T, args ...string) {
	t.Helper()
	_, err := clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput(append([]string{"UpdateThrottlerConfig"}, append(args, keyspaceName)...)...)
	require.NoError(t, err)
}

// waitForShardLagCheck waits until the primary's tablet throttler reports wantCode for the shard's lag.
// It checks as the "test" app, whose checks renew the heartbeat lease.
func waitForShardLagCheck(t *testing.T, wantCode tabletmanagerdatapb.CheckThrottlerResponseCode) {
	t.Helper()
	flags := &throttle.CheckFlags{Scope: base.ShardScope}
	resp, ok := throttler.WaitForCheckThrottlerResult(t, &clusterInstance.VtctldClientProcess, primaryTablet, throttlerapp.TestingName, flags, wantCode, waitTimeout)
	require.True(t, ok, "check: %v", resp)
	assert.Equal(t, tabletmanagerdatapb.ThrottlerType_DEFAULT_LAG_THROTTLER, resp.GetCheck().GetThrottlerType())
}

// stopReplication stops replication on the replica, and stops VTOrc from restarting it.
func stopReplication(t *testing.T, replica *cluster.Vttablet) {
	t.Helper()
	_, err := clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput("StopReplication", replica.Alias)
	require.NoError(t, err)
	clusterInstance.DisableVTOrcRecoveries(t)
}

// restoreAfterTest registers a cleanup that restarts replication on every replica and disables the
// throttler without a shard percentile, so a failed test does not leave lag behind for the next one.
func restoreAfterTest(t *testing.T) {
	t.Helper()
	t.Cleanup(func() {
		clusterInstance.EnableVTOrcRecoveries(t)
		for _, replica := range replicaTablets {
			_, err := clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput("StartReplication", replica.Alias)
			assert.NoError(t, err, "restarting replication on %s", replica.Alias)
		}
		_, err := clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput("UpdateThrottlerConfig", "--disable", "--metric-name", "lag", "--shard-percentile", "0", keyspaceName)
		assert.NoError(t, err, "disabling the tablet throttler")
	})
}

// TestShardMetricPercentile stops replication on one replica of four, then on a second one, and
// checks the primary's tablet throttler as VReplication and Online DDL do. With the percentile
// unset, one lagging replica throttles. At p75, one is tolerated and two throttle. At p50, two are tolerated.
func TestShardMetricPercentile(t *testing.T) {
	restoreAfterTest(t)

	throttler.EnableLagThrottlerAndWaitForStatus(t, clusterInstance)
	waitForShardLagCheck(t, tabletmanagerdatapb.CheckThrottlerResponseCode_OK)

	t.Run("unset throttles on one lagging replica", func(t *testing.T) {
		stopReplication(t, replicaTablets[3])
		waitForShardLagCheck(t, tabletmanagerdatapb.CheckThrottlerResponseCode_THRESHOLD_EXCEEDED)
	})

	t.Run("p75 tolerates one lagging replica", func(t *testing.T) {
		updateThrottlerConfig(t, "--metric-name", "lag", "--shard-percentile", "75")
		waitForShardLagCheck(t, tabletmanagerdatapb.CheckThrottlerResponseCode_OK)
	})

	t.Run("zero restores the worst replica", func(t *testing.T) {
		updateThrottlerConfig(t, "--metric-name", "lag", "--shard-percentile", "0")
		waitForShardLagCheck(t, tabletmanagerdatapb.CheckThrottlerResponseCode_THRESHOLD_EXCEEDED)
		updateThrottlerConfig(t, "--metric-name", "lag", "--shard-percentile", "75")
		waitForShardLagCheck(t, tabletmanagerdatapb.CheckThrottlerResponseCode_OK)
	})

	t.Run("invalid percentile is refused", func(t *testing.T) {
		output, err := clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput("UpdateThrottlerConfig", "--metric-name", "lag", "--shard-percentile", "150", keyspaceName)
		require.Error(t, err)
		require.Contains(t, output, "shard percentile of lag must be between 0 and 100, got 150")
	})

	t.Run("p75 throttles on two lagging replicas", func(t *testing.T) {
		stopReplication(t, replicaTablets[2])
		waitForShardLagCheck(t, tabletmanagerdatapb.CheckThrottlerResponseCode_THRESHOLD_EXCEEDED)
	})

	t.Run("p50 tolerates two lagging replicas", func(t *testing.T) {
		updateThrottlerConfig(t, "--metric-name", "lag", "--shard-percentile", "50")
		waitForShardLagCheck(t, tabletmanagerdatapb.CheckThrottlerResponseCode_OK)
	})
}
