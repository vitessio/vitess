/*
Copyright 2022 The Vitess Authors.

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

package api

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/vtorc/utils"
)

// TestAPIEndpoints tests the various API endpoints that VTOrc offers.
func TestAPIEndpoints(t *testing.T) {
	utils.SetupVttabletsAndVTOrcs(t, clusterInfo, 2, 1, nil, cluster.VTOrcConfiguration{
		PreventCrossCellFailover: true,
	}, 1, "")
	keyspace := &clusterInfo.ClusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]
	vtorc := clusterInfo.ClusterInstance.VTOrcProcesses[0]
	// Call API with retry to ensure VTOrc is up
	status, resp := utils.MakeAPICallRetry(t, vtorc, "/debug/health", func(code int, response string) bool {
		return code != 200
	})
	// Verify when VTOrc is healthy, it has also run the first discovery.
	assert.Equal(t, 200, status)
	assert.Contains(t, resp, `"Healthy": true,`)

	// find primary from topo
	primary := utils.ShardPrimaryTablet(t, clusterInfo, keyspace, shard0)
	assert.NotNil(t, primary, "should have elected a primary")

	// find the replica and rdonly tablet
	var replica, rdonly *cluster.Vttablet
	for _, tablet := range shard0.Vttablets {
		// we know we have only two replica type tablets, so the one not the primary must be the replica
		if tablet.Alias != primary.Alias && tablet.Type == "replica" {
			replica = tablet
		}
		if tablet.Type == "rdonly" {
			rdonly = tablet
		}
	}
	assert.NotNil(t, replica, "could not find replica tablet")
	assert.NotNil(t, rdonly, "could not find rdonly tablet")

	// check that the replication is setup correctly before we set read-only
	utils.CheckReplication(t, clusterInfo, primary, []*cluster.Vttablet{replica, rdonly}, 10*time.Second)

	t.Run("Health API", func(t *testing.T) {
		// Check that VTOrc is healthy
		status, resp, err := utils.MakeAPICall(t, vtorc, "/debug/health")
		require.NoError(t, err)
		assert.Equal(t, 200, status)
		assert.Contains(t, resp, `"Healthy": true,`)
	})

	t.Run("Liveness API", func(t *testing.T) {
		// Check that VTOrc is live
		status, resp, err := utils.MakeAPICall(t, vtorc, "/debug/liveness")
		require.NoError(t, err)
		assert.Equal(t, 200, status)
		assert.Empty(t, resp)
	})

	// Before we disable recoveries, let us wait until VTOrc has fixed all the issues (if any).
	_, _ = utils.MakeAPICallRetry(t, vtorc, "/api/replication-analysis", func(_ int, response string) bool {
		return response != "null"
	})

	t.Run("Database State", func(t *testing.T) {
		// Get database state
		status, resp, err := utils.MakeAPICall(t, vtorc, "/api/database-state")
		require.NoError(t, err)
		assert.Equal(t, 200, status)
		assert.Contains(t, resp, `"alias": "zone1-0000000101"`)
		assert.Contains(t, resp, `{
		"TableName": "vitess_keyspace",
		"Rows": [
			{
				"disable_emergency_reparent": "0",
				"durability_policy": "none",
				"keyspace": "ks",
				"keyspace_type": "0"
			}
		]
	},`)
	})

	t.Run("Check Vars and Metrics", func(t *testing.T) {
		utils.CheckVarExists(t, vtorc, "AnalysisChangeWrite")
		utils.CheckVarExists(t, vtorc, "AuditWrite")
		utils.CheckVarExists(t, vtorc, "CurrentErrantGTIDCount")
		utils.CheckVarExists(t, vtorc, "DetectedProblems")
		utils.CheckVarExists(t, vtorc, "DiscoveriesAttempt")
		utils.CheckVarExists(t, vtorc, "DiscoveriesFail")
		utils.CheckVarExists(t, vtorc, "DiscoveriesInstancePollSecondsExceeded")
		utils.CheckVarExists(t, vtorc, "DiscoveriesQueueLength")
		utils.CheckVarExists(t, vtorc, "DiscoveriesRecentCount")
		utils.CheckVarExists(t, vtorc, "DiscoveryWorkers")
		utils.CheckVarExists(t, vtorc, "DiscoveryWorkersActive")
		utils.CheckVarExists(t, vtorc, "DiscoveryInstanceTimings")
		utils.CheckVarExists(t, vtorc, "FailedRecoveries")
		utils.CheckVarExists(t, vtorc, "InstanceRead")
		utils.CheckVarExists(t, vtorc, "InstanceReadTopology")
		utils.CheckVarExists(t, vtorc, "PendingRecoveries")
		utils.CheckVarExists(t, vtorc, "RecoveriesCount")
		utils.CheckVarExists(t, vtorc, "ShardLocksActive")
		utils.CheckVarExists(t, vtorc, "ShardLockTimings")
		utils.CheckVarExists(t, vtorc, "SuccessfulRecoveries")
		utils.CheckVarExists(t, vtorc, "TabletsWatchedByCell")
		utils.CheckVarExists(t, vtorc, "TabletsWatchedByShard")

		// Metrics registered in prometheus
		utils.CheckMetricExists(t, vtorc, "vtorc_analysis_change_write")
		utils.CheckMetricExists(t, vtorc, "vtorc_audit_write")
		utils.CheckMetricExists(t, vtorc, "vtorc_detected_problems")
		utils.CheckMetricExists(t, vtorc, "vtorc_discoveries_attempt")
		utils.CheckMetricExists(t, vtorc, "vtorc_discoveries_fail")
		utils.CheckMetricExists(t, vtorc, "vtorc_discoveries_instance_poll_seconds_exceeded")
		utils.CheckMetricExists(t, vtorc, "vtorc_discoveries_queue_length")
		utils.CheckMetricExists(t, vtorc, "vtorc_discoveries_recent_count")
		utils.CheckMetricExists(t, vtorc, "vtorc_discovery_instance_timings_bucket")
		utils.CheckMetricExists(t, vtorc, "vtorc_discovery_workers")
		utils.CheckMetricExists(t, vtorc, "vtorc_discovery_workers_active")
		utils.CheckMetricExists(t, vtorc, "vtorc_errant_gtid_tablet_count")
		utils.CheckMetricExists(t, vtorc, "vtorc_instance_read")
		utils.CheckMetricExists(t, vtorc, "vtorc_instance_read_topology")
		utils.CheckMetricExists(t, vtorc, "vtorc_pending_recoveries")
		utils.CheckMetricExists(t, vtorc, "vtorc_recoveries_count")
		utils.CheckMetricExists(t, vtorc, "vtorc_shard_locks_active")
		utils.CheckMetricExists(t, vtorc, "vtorc_shard_lock_timings_bucket")
		utils.CheckMetricExists(t, vtorc, "vtorc_successful_recoveries")
		utils.CheckMetricExists(t, vtorc, "vtorc_tablets_watched_by_cell")
		utils.CheckMetricExists(t, vtorc, "vtorc_tablets_watched_by_shard")
	})

	t.Run("Disable Recoveries API", func(t *testing.T) {
		// Disable recoveries of VTOrc
		status, resp, err := utils.MakeAPICall(t, vtorc, "/api/disable-global-recoveries")
		require.NoError(t, err)
		assert.Equal(t, 200, status)
		assert.Equal(t, "Global recoveries disabled\n", resp)
	})

	t.Run("Replication Analysis API", func(t *testing.T) {
		// use vtctldclient to stop replication
		_, err := clusterInfo.ClusterInstance.VtctldClientProcess.ExecuteCommandWithOutput("StopReplication", replica.Alias)
		require.NoError(t, err)

		// We know VTOrc won't fix this since we disabled global recoveries!
		// Wait until VTOrc picks up on this issue and verify
		// that we see a not null result on the api/replication-analysis page
		status, resp := utils.MakeAPICallRetry(t, vtorc, "/api/replication-analysis", func(_ int, response string) bool {
			return response == "null"
		})
		assert.Equal(t, 200, status, resp)
		assert.Contains(t, resp, fmt.Sprintf(`"AnalyzedInstanceAlias": "%s"`, replica.Alias))
		assert.Contains(t, resp, `"Analysis": "ReplicationStopped"`)

		// Verify that filtering also works in the API as intended
		status, resp, err = utils.MakeAPICall(t, vtorc, "/api/replication-analysis?keyspace=ks&shard=0")
		require.NoError(t, err)
		assert.Equal(t, 200, status, resp)
		assert.Contains(t, resp, fmt.Sprintf(`"AnalyzedInstanceAlias": "%s"`, replica.Alias))

		// Verify that filtering by keyspace also works in the API as intended
		status, resp, err = utils.MakeAPICall(t, vtorc, "/api/replication-analysis?keyspace=ks")
		require.NoError(t, err)
		assert.Equal(t, 200, status, resp)
		assert.Contains(t, resp, fmt.Sprintf(`"AnalyzedInstanceAlias": "%s"`, replica.Alias))

		// Check that filtering using keyspace and shard works
		status, resp, err = utils.MakeAPICall(t, vtorc, "/api/replication-analysis?keyspace=ks&shard=80-")
		require.NoError(t, err)
		assert.Equal(t, 200, status, resp)
		assert.Equal(t, "null", resp)

		// Check that filtering using just the shard fails
		status, resp, err = utils.MakeAPICall(t, vtorc, "/api/replication-analysis?shard=0")
		require.NoError(t, err)
		assert.Equal(t, 400, status, resp)
		assert.Equal(t, "Filtering by shard without keyspace isn't supported\n", resp)
	})

	t.Run("Enable Recoveries API", func(t *testing.T) {
		// Enable recoveries of VTOrc
		status, resp, err := utils.MakeAPICall(t, vtorc, "/api/enable-global-recoveries")
		require.NoError(t, err)
		assert.Equal(t, 200, status)
		assert.Equal(t, "Global recoveries enabled\n", resp)

		// Check that replication is indeed repaired by VTOrc, right after we enable the recoveries
		utils.CheckReplication(t, clusterInfo, primary, []*cluster.Vttablet{replica}, 10*time.Second)
	})

	t.Run("Problems API", func(t *testing.T) {
		// Wait until there are no problems and the api endpoint returns null
		// We need this because we just recovered from a recovery, and it races with this API call
		status, resp := utils.MakeAPICallRetry(t, vtorc, "/api/problems", func(_ int, response string) bool {
			return response != "null"
		})
		assert.Equal(t, 200, status)
		assert.Equal(t, "null", resp)

		// insert an errant GTID in the replica
		_, err := utils.RunSQL(t, "insert into vt_insert_test(id, msg) values (10173, 'test 178342')", replica, "vt_ks")
		require.NoError(t, err)

		// Wait until VTOrc picks up on this errant GTID and verify
		// that we see a not null result on the api/problems page
		// and the replica instance is marked as one of the problems
		status, resp = utils.MakeAPICallRetry(t, vtorc, "/api/problems", func(_ int, response string) bool {
			return response == "null"
		})
		assert.Equal(t, 200, status, resp)
		assert.Contains(t, resp, fmt.Sprintf(`"InstanceAlias": "%v"`, replica.Alias))

		// Check that filtering using keyspace and shard works
		status, resp, err = utils.MakeAPICall(t, vtorc, "/api/problems?keyspace=ks&shard=0")
		require.NoError(t, err)
		assert.Equal(t, 200, status, resp)
		assert.Contains(t, resp, fmt.Sprintf(`"InstanceAlias": "%v"`, replica.Alias))

		// Check that filtering using keyspace works
		status, resp, err = utils.MakeAPICall(t, vtorc, "/api/problems?keyspace=ks")
		require.NoError(t, err)
		assert.Equal(t, 200, status, resp)
		assert.Contains(t, resp, fmt.Sprintf(`"InstanceAlias": "%v"`, replica.Alias))

		// Check that filtering using keyspace and shard works
		status, resp, err = utils.MakeAPICall(t, vtorc, "/api/problems?keyspace=ks&shard=80-")
		require.NoError(t, err)
		assert.Equal(t, 200, status, resp)
		assert.Equal(t, "null", resp)

		// Check that filtering using just the shard fails
		status, resp, err = utils.MakeAPICall(t, vtorc, "/api/problems?shard=0")
		require.NoError(t, err)
		assert.Equal(t, 400, status, resp)
		assert.Equal(t, "Filtering by shard without keyspace isn't supported\n", resp)

		// Also verify that we see the tablet in the errant GTIDs API call
		status, resp, err = utils.MakeAPICall(t, vtorc, "/api/errant-gtids")
		require.NoError(t, err)
		assert.Equal(t, 200, status, resp)
		assert.Contains(t, resp, fmt.Sprintf(`"InstanceAlias": "%v"`, replica.Alias))

		// Check that filtering using keyspace and shard works
		status, resp, err = utils.MakeAPICall(t, vtorc, "/api/errant-gtids?keyspace=ks&shard=0")
		require.NoError(t, err)
		assert.Equal(t, 200, status, resp)
		assert.Contains(t, resp, fmt.Sprintf(`"InstanceAlias": "%v"`, replica.Alias))

		// Check that filtering using keyspace works
		status, resp, err = utils.MakeAPICall(t, vtorc, "/api/errant-gtids?keyspace=ks")
		require.NoError(t, err)
		assert.Equal(t, 200, status, resp)
		assert.Contains(t, resp, fmt.Sprintf(`"InstanceAlias": "%v"`, replica.Alias))

		// Check that filtering using keyspace and shard works
		status, resp, err = utils.MakeAPICall(t, vtorc, "/api/errant-gtids?keyspace=ks&shard=80-")
		require.NoError(t, err)
		assert.Equal(t, 200, status, resp)
		assert.Equal(t, "null", resp)

		// Check that filtering using just the shard fails
		status, resp, err = utils.MakeAPICall(t, vtorc, "/api/errant-gtids?shard=0")
		require.NoError(t, err)
		assert.Equal(t, 400, status, resp)
		assert.Equal(t, "Filtering by shard without keyspace isn't supported\n", resp)

		// Also verify that the metric for errant GTIDs is reporting the correct count.
		waitForErrantGTIDTabletCount(t, vtorc, 1)
		// Now we check the errant GTID count for the tablet
		verifyErrantGTIDCount(t, vtorc, replica.Alias, 1)
	})
}

func waitForErrantGTIDTabletCount(t *testing.T, vtorc *cluster.VTOrcProcess, errantGTIDCountWanted int) {
	timeout := time.After(15 * time.Second)
	for {
		select {
		case <-timeout:
			t.Fatalf("Timed out waiting for errant gtid count in the metrics to be %v", errantGTIDCountWanted)
			return
		default:
			_, resp, err := utils.MakeAPICall(t, vtorc, "/debug/vars")
			require.NoError(t, err)
			resultMap := make(map[string]any)
			err = json.Unmarshal([]byte(resp), &resultMap)
			require.NoError(t, err)
			errantGTIDTabletsCount := reflect.ValueOf(resultMap["ErrantGtidTabletCount"])
			if int(math.Round(errantGTIDTabletsCount.Float())) == errantGTIDCountWanted {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func verifyErrantGTIDCount(t *testing.T, vtorc *cluster.VTOrcProcess, tabletAlias string, countWanted int) {
	vars := vtorc.GetVars()
	errantGTIDCounts := vars["CurrentErrantGTIDCount"].(map[string]interface{})
	gtidCountVal, isPresent := errantGTIDCounts[tabletAlias]
	require.True(t, isPresent, "Tablet %s not found in errant GTID counts", tabletAlias)
	gtidCount := utils.GetIntFromValue(gtidCountVal)
	require.EqualValues(t, countWanted, gtidCount, "Tablet %s has %d errant GTIDs, wanted %d", tabletAlias, gtidCount, countWanted)
}
