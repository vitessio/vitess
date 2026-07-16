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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vitesst"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/topoproto"
)

var lastUsedValue int

// TestAPIEndpoints tests the various API endpoints that VTOrc offers.
func TestAPIEndpoints(t *testing.T) {
	clusterInstance = setupCluster(t)
	ctx := t.Context()

	keyspace := clusterInstance.Keyspace(keyspaceName)
	shard0 := keyspace.Shard(shardName)
	vtorc := clusterInstance.VTOrc()

	// Apply the cross-cell failover guard through the watched config file.
	require.NoError(t, vtorc.WriteConfig(ctx, `{"prevent-cross-cell-failover": true}`))
	waitForConfig(t, vtorc, `"prevent-cross-cell-failover": true`)

	// Call API with retry to ensure VTOrc is up
	status, resp := makeAPICallRetry(ctx, t, vtorc, "/debug/health", func(code int, response string) bool {
		return code != 200
	})
	// Verify when VTOrc is healthy, it has also run the first discovery.
	assert.Equal(t, 200, status)
	assert.Contains(t, resp, `"Healthy": true,`)

	// find primary from topo
	primary := shardPrimaryTablet(ctx, t, shard0)
	assert.NotNil(t, primary, "should have elected a primary")

	// find the replica and rdonly tablet
	var replica, rdonly *vitesst.Tablet
	for _, tablet := range shard0.Tablets() {
		// we know we have only two replica type tablets, so the one not the primary must be the replica
		if tablet.Type() == "rdonly" {
			rdonly = tablet
		} else if tablet.Alias() != primary.Alias() {
			replica = tablet
		}
	}
	assert.NotNil(t, replica, "could not find replica tablet")
	assert.NotNil(t, rdonly, "could not find rdonly tablet")

	// check that the replication is setup correctly before we set read-only
	checkReplication(ctx, t, primary, []*vitesst.Tablet{replica, rdonly}, 10*time.Second)

	t.Run("Health API", func(t *testing.T) {
		// Check that VTOrc is healthy
		status, resp, err := vtorc.MakeAPICall(ctx, "/debug/health")
		require.NoError(t, err)
		assert.Equal(t, 200, status)
		assert.Contains(t, resp, `"Healthy": true,`)
	})

	t.Run("Liveness API", func(t *testing.T) {
		// Check that VTOrc is live
		status, resp, err := vtorc.MakeAPICall(ctx, "/debug/liveness")
		require.NoError(t, err)
		assert.Equal(t, 200, status)
		assert.Empty(t, resp)
	})

	// Before we disable recoveries, let us wait until VTOrc has fixed all the issues (if any).
	_, _ = makeAPICallRetry(ctx, t, vtorc, "/api/detection-analysis", func(i int, response string) bool {
		return response != "null"
	})

	t.Run("Database State", func(t *testing.T) {
		// Get database state
		status, resp, err := vtorc.MakeAPICall(ctx, "/api/database-state")
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
		checkVarExists(ctx, t, vtorc, "AnalysisChangeWrite")
		checkVarExists(ctx, t, vtorc, "AuditWrite")
		checkVarExists(ctx, t, vtorc, "CurrentErrantGTIDCount")
		checkVarExists(ctx, t, vtorc, "DetectedProblems")
		checkVarExists(ctx, t, vtorc, "DiscoveriesAttempt")
		checkVarExists(ctx, t, vtorc, "DiscoveriesFail")
		checkVarExists(ctx, t, vtorc, "DiscoveriesInstancePollSecondsExceeded")
		checkVarExists(ctx, t, vtorc, "DiscoveriesQueueLength")
		checkVarExists(ctx, t, vtorc, "DiscoveriesRecentCount")
		checkVarExists(ctx, t, vtorc, "DiscoveryWorkers")
		checkVarExists(ctx, t, vtorc, "DiscoveryWorkersActive")
		checkVarExists(ctx, t, vtorc, "DiscoveryInstanceTimings")
		checkVarExists(ctx, t, vtorc, "FailedRecoveries")
		checkVarExists(ctx, t, vtorc, "InstanceRead")
		checkVarExists(ctx, t, vtorc, "InstanceReadTopology")
		checkVarExists(ctx, t, vtorc, "PendingRecoveries")
		checkVarExists(ctx, t, vtorc, "RecoveriesCount")
		checkVarExists(ctx, t, vtorc, "ShardLocksActive")
		checkVarExists(ctx, t, vtorc, "ShardLockTimings")
		checkVarExists(ctx, t, vtorc, "SuccessfulRecoveries")
		checkVarExists(ctx, t, vtorc, "TabletsWatchedByCell")
		checkVarExists(ctx, t, vtorc, "TabletsWatchedByShard")

		// Metrics registered in prometheus
		checkMetricExists(ctx, t, vtorc, "vtorc_analysis_change_write")
		checkMetricExists(ctx, t, vtorc, "vtorc_audit_write")
		checkMetricExists(ctx, t, vtorc, "vtorc_detected_problems")
		checkMetricExists(ctx, t, vtorc, "vtorc_discoveries_attempt")
		checkMetricExists(ctx, t, vtorc, "vtorc_discoveries_fail")
		checkMetricExists(ctx, t, vtorc, "vtorc_discoveries_instance_poll_seconds_exceeded")
		checkMetricExists(ctx, t, vtorc, "vtorc_discoveries_queue_length")
		checkMetricExists(ctx, t, vtorc, "vtorc_discoveries_recent_count")
		checkMetricExists(ctx, t, vtorc, "vtorc_discovery_instance_timings_bucket")
		checkMetricExists(ctx, t, vtorc, "vtorc_discovery_workers")
		checkMetricExists(ctx, t, vtorc, "vtorc_discovery_workers_active")
		checkMetricExists(ctx, t, vtorc, "vtorc_errant_gtid_tablet_count")
		checkMetricExists(ctx, t, vtorc, "vtorc_instance_read")
		checkMetricExists(ctx, t, vtorc, "vtorc_instance_read_topology")
		checkMetricExists(ctx, t, vtorc, "vtorc_pending_recoveries")
		checkMetricExists(ctx, t, vtorc, "vtorc_recoveries_count")
		checkMetricExists(ctx, t, vtorc, "vtorc_shard_locks_active")
		checkMetricExists(ctx, t, vtorc, "vtorc_shard_lock_timings_bucket")
		checkMetricExists(ctx, t, vtorc, "vtorc_successful_recoveries")
		checkMetricExists(ctx, t, vtorc, "vtorc_tablets_watched_by_cell")
		checkMetricExists(ctx, t, vtorc, "vtorc_tablets_watched_by_shard")
	})

	t.Run("Disable Recoveries API", func(t *testing.T) {
		// Disable recoveries of VTOrc
		status, resp, err := vtorc.MakeAPICall(ctx, "/api/disable-global-recoveries")
		require.NoError(t, err)
		assert.Equal(t, 200, status)
		assert.Equal(t, "Global recoveries disabled\n", resp)
	})

	t.Run("Detection Analysis API", func(t *testing.T) {
		// use vtctldclient to stop replication
		_, err := clusterInstance.Vtctld().ExecuteCommandWithOutput(ctx, "StopReplication", replica.Alias())
		require.NoError(t, err)

		// We know VTOrc won't fix this since we disabled global recoveries!
		// Wait until VTOrc picks up on this issue and verify
		// that we see a not null result on the api/detection-analysis page
		status, resp := makeAPICallRetry(ctx, t, vtorc, "/api/detection-analysis", func(_ int, response string) bool {
			return response == "null"
		})
		assert.Equal(t, 200, status, resp)
		assert.Contains(t, resp, fmt.Sprintf(`"AnalyzedInstanceAlias": "%s"`, vtorcAlias(replica)))
		assert.Contains(t, resp, `"Analysis": "ReplicationStopped"`)

		// Verify that filtering also works in the API as intended
		status, resp, err = vtorc.MakeAPICall(ctx, "/api/detection-analysis?keyspace=ks&shard=0")
		require.NoError(t, err)
		assert.Equal(t, 200, status, resp)
		assert.Contains(t, resp, fmt.Sprintf(`"AnalyzedInstanceAlias": "%s"`, vtorcAlias(replica)))

		// Verify that filtering by keyspace also works in the API as intended
		status, resp, err = vtorc.MakeAPICall(ctx, "/api/detection-analysis?keyspace=ks")
		require.NoError(t, err)
		assert.Equal(t, 200, status, resp)
		assert.Contains(t, resp, fmt.Sprintf(`"AnalyzedInstanceAlias": "%s"`, vtorcAlias(replica)))

		// Check that filtering using keyspace and shard works
		status, resp, err = vtorc.MakeAPICall(ctx, "/api/detection-analysis?keyspace=ks&shard=80-")
		require.NoError(t, err)
		assert.Equal(t, 200, status, resp)
		assert.Equal(t, "null", resp)

		// Check that filtering using just the shard fails
		status, resp, err = vtorc.MakeAPICall(ctx, "/api/detection-analysis?shard=0")
		require.NoError(t, err)
		assert.Equal(t, 400, status, resp)
		assert.Equal(t, "Filtering by shard without keyspace isn't supported\n", resp)
	})

	t.Run("Enable Recoveries API", func(t *testing.T) {
		// Enable recoveries of VTOrc
		status, resp, err := vtorc.MakeAPICall(ctx, "/api/enable-global-recoveries")
		require.NoError(t, err)
		assert.Equal(t, 200, status)
		assert.Equal(t, "Global recoveries enabled\n", resp)

		// Check that replication is indeed repaired by VTOrc, right after we enable the recoveries
		checkReplication(ctx, t, primary, []*vitesst.Tablet{replica}, 10*time.Second)
	})

	t.Run("Problems API", func(t *testing.T) {
		// Wait until there are no problems and the api endpoint returns null
		// We need this because we just recovered from a recovery, and it races with this API call
		status, resp := makeAPICallRetry(ctx, t, vtorc, "/api/problems", func(_ int, response string) bool {
			return response != "null"
		})
		assert.Equal(t, 200, status)
		assert.Equal(t, "null", resp)

		// insert an errant GTID in the replica
		_, err := replica.QueryTabletWithDB(ctx, "insert into vt_insert_test(id, msg) values (10173, 'test 178342')", "vt_ks")
		require.NoError(t, err)

		// Wait until VTOrc picks up on this errant GTID and verify
		// that we see a not null result on the api/problems page
		// and the replica instance is marked as one of the problems
		status, resp = makeAPICallRetry(ctx, t, vtorc, "/api/problems", func(_ int, response string) bool {
			return response == "null"
		})
		assert.Equal(t, 200, status, resp)
		assert.Contains(t, resp, fmt.Sprintf(`"InstanceAlias": "%v"`, vtorcAlias(replica)))

		// Check that filtering using keyspace and shard works
		status, resp, err = vtorc.MakeAPICall(ctx, "/api/problems?keyspace=ks&shard=0")
		require.NoError(t, err)
		assert.Equal(t, 200, status, resp)
		assert.Contains(t, resp, fmt.Sprintf(`"InstanceAlias": "%v"`, vtorcAlias(replica)))

		// Check that filtering using keyspace works
		status, resp, err = vtorc.MakeAPICall(ctx, "/api/problems?keyspace=ks")
		require.NoError(t, err)
		assert.Equal(t, 200, status, resp)
		assert.Contains(t, resp, fmt.Sprintf(`"InstanceAlias": "%v"`, vtorcAlias(replica)))

		// Check that filtering using keyspace and shard works
		status, resp, err = vtorc.MakeAPICall(ctx, "/api/problems?keyspace=ks&shard=80-")
		require.NoError(t, err)
		assert.Equal(t, 200, status, resp)
		assert.Equal(t, "null", resp)

		// Check that filtering using just the shard fails
		status, resp, err = vtorc.MakeAPICall(ctx, "/api/problems?shard=0")
		require.NoError(t, err)
		assert.Equal(t, 400, status, resp)
		assert.Equal(t, "Filtering by shard without keyspace isn't supported\n", resp)

		// Also verify that we see the tablet in the errant GTIDs API call
		status, resp, err = vtorc.MakeAPICall(ctx, "/api/errant-gtids")
		require.NoError(t, err)
		assert.Equal(t, 200, status, resp)
		assert.Contains(t, resp, fmt.Sprintf(`"InstanceAlias": "%v"`, vtorcAlias(replica)))

		// Check that filtering using keyspace and shard works
		status, resp, err = vtorc.MakeAPICall(ctx, "/api/errant-gtids?keyspace=ks&shard=0")
		require.NoError(t, err)
		assert.Equal(t, 200, status, resp)
		assert.Contains(t, resp, fmt.Sprintf(`"InstanceAlias": "%v"`, vtorcAlias(replica)))

		// Check that filtering using keyspace works
		status, resp, err = vtorc.MakeAPICall(ctx, "/api/errant-gtids?keyspace=ks")
		require.NoError(t, err)
		assert.Equal(t, 200, status, resp)
		assert.Contains(t, resp, fmt.Sprintf(`"InstanceAlias": "%v"`, vtorcAlias(replica)))

		// Check that filtering using keyspace and shard works
		status, resp, err = vtorc.MakeAPICall(ctx, "/api/errant-gtids?keyspace=ks&shard=80-")
		require.NoError(t, err)
		assert.Equal(t, 200, status, resp)
		assert.Equal(t, "null", resp)

		// Check that filtering using just the shard fails
		status, resp, err = vtorc.MakeAPICall(ctx, "/api/errant-gtids?shard=0")
		require.NoError(t, err)
		assert.Equal(t, 400, status, resp)
		assert.Equal(t, "Filtering by shard without keyspace isn't supported\n", resp)

		// Also verify that the metric for errant GTIDs is reporting the correct count.
		waitForErrantGTIDTabletCount(ctx, t, vtorc, 1)
		// Now we check the errant GTID count for the tablet
		verifyErrantGTIDCount(ctx, t, vtorc, vtorcAlias(replica), 1)
	})
}

// vtorcAlias formats a tablet's alias the way VTOrc reports it in its API responses.
func vtorcAlias(tablet *vitesst.Tablet) string {
	return topoproto.TabletAliasString(&topodatapb.TabletAlias{Cell: tablet.Cell, Uid: uint32(tablet.UID)})
}

// shardPrimaryTablet waits until a primary tablet has been elected for the given shard and returns it.
func shardPrimaryTablet(ctx context.Context, t *testing.T, shard *vitesst.Shard) *vitesst.Tablet {
	t.Helper()
	start := time.Now()
	for {
		require.False(t, time.Since(start) > time.Second*60, "failed to elect primary before timeout")

		out, err := clusterInstance.Vtctld().ExecuteCommandWithOutput(ctx, "GetShard", shard.Keyspace.Name+"/"+shard.Name)
		require.NoError(t, err)

		var record struct {
			Shard struct {
				PrimaryAlias *struct {
					Cell string `json:"cell"`
					UID  uint32 `json:"uid"`
				} `json:"primary_alias"`
			} `json:"shard"`
		}
		require.NoError(t, json.Unmarshal([]byte(out), &record))

		if record.Shard.PrimaryAlias != nil {
			for _, tablet := range shard.Tablets() {
				if tablet.Cell == record.Shard.PrimaryAlias.Cell && uint32(tablet.UID) == record.Shard.PrimaryAlias.UID {
					return tablet
				}
			}
		}
		time.Sleep(time.Second)
	}
}

// makeAPICallRetry calls a VTOrc API endpoint, retrying while the given predicate returns true,
// and returns the status and body of the first response the predicate accepts.
func makeAPICallRetry(ctx context.Context, t *testing.T, vtorc *vitesst.VTOrc, path string, retry func(int, string) bool) (int, string) {
	t.Helper()
	status, resp, err := vtorc.MakeAPICallRetry(ctx, path, 30*time.Second, func(code int, response string) bool {
		return !retry(code, response)
	})
	require.NoError(t, err)
	return status, resp
}

// checkVarExists checks whether the given variable exists in /debug/vars.
func checkVarExists(ctx context.Context, t *testing.T, vtorc *vitesst.VTOrc, name string) {
	t.Helper()
	vars, err := vtorc.GetVars(ctx)
	require.NoError(t, err)
	_, exists := vars[name]
	assert.True(t, exists, vars)
}

// checkMetricExists checks whether the given metric exists in /metrics.
func checkMetricExists(ctx context.Context, t *testing.T, vtorc *vitesst.VTOrc, name string) {
	t.Helper()
	metrics, err := vtorc.GetMetrics(ctx)
	require.NoError(t, err)
	assert.Contains(t, metrics, name)
}

// checkReplication checks that replication is setup correctly and that writes succeed and are
// replicated on all the replicas.
func checkReplication(ctx context.Context, t *testing.T, primary *vitesst.Tablet, replicas []*vitesst.Tablet, timeToWait time.Duration) {
	t.Helper()
	endTime := time.Now().Add(timeToWait)
	sqlSchema := `create table if not exists vt_ks.vt_insert_test (
		id bigint,
		msg varchar(64),
		primary key (id)
		) Engine=InnoDB`
	for {
		_, err := primary.QueryTabletWithDB(ctx, sqlSchema, "vt_ks")
		if err == nil {
			break
		}
		require.False(t, time.Now().After(endTime), "timed out waiting for keyspace vt_ks table creation: %v", err)
		time.Sleep(100 * time.Millisecond)
	}
	lastUsedValue++
	confirmReplication(ctx, t, primary, replicas, time.Until(endTime), lastUsedValue)
}

// confirmReplication inserts a value into the primary and confirms it is replicated on every replica.
func confirmReplication(ctx context.Context, t *testing.T, primary *vitesst.Tablet, replicas []*vitesst.Tablet, timeToWait time.Duration, valueToInsert int) {
	t.Helper()
	insertSQL := fmt.Sprintf("insert into vt_insert_test(id, msg) values (%d, 'test %d')", valueToInsert, valueToInsert)
	_, err := primary.QueryTabletWithDB(ctx, insertSQL, "vt_ks")
	require.NoError(t, err)
	endTime := time.Now().Add(timeToWait)
	for {
		var lastErr error
		for _, tab := range replicas {
			if errInReplication := checkInsertedValue(ctx, tab, valueToInsert); errInReplication != nil {
				lastErr = errInReplication
			}
		}
		if lastErr == nil {
			return
		}
		require.False(t, time.Now().After(endTime), "timed out waiting for replication: %v", lastErr)
		time.Sleep(300 * time.Millisecond)
	}
}

func checkInsertedValue(ctx context.Context, tablet *vitesst.Tablet, index int) error {
	selectSQL := fmt.Sprintf("select msg from vt_ks.vt_insert_test where id=%d", index)
	qr, err := tablet.QueryTabletWithDB(ctx, selectSQL, "vt_ks")
	// The error may be not nil, if the replication has not caught upto the point where the table exists.
	// We can safely skip this error and retry reading after wait
	if err == nil && len(qr.Rows) == 1 {
		return nil
	}
	return errors.New("data is not yet replicated")
}

func waitForErrantGTIDTabletCount(ctx context.Context, t *testing.T, vtorc *vitesst.VTOrc, errantGTIDCountWanted int) {
	timeout := time.After(15 * time.Second)
	for {
		select {
		case <-timeout:
			require.Failf(t, "errant gtid count timeout", "Timed out waiting for errant gtid count in the metrics to be %v", errantGTIDCountWanted)
			return
		default:
			_, resp, err := vtorc.MakeAPICall(ctx, "/debug/vars")
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

func verifyErrantGTIDCount(ctx context.Context, t *testing.T, vtorc *vitesst.VTOrc, tabletAlias string, countWanted int) {
	vars, err := vtorc.GetVars(ctx)
	require.NoError(t, err)
	errantGTIDCounts := vars["CurrentErrantGTIDCount"].(map[string]any)
	gtidCountVal, isPresent := errantGTIDCounts[tabletAlias]
	require.True(t, isPresent, "Tablet %s not found in errant GTID counts", tabletAlias)
	gtidCount := getIntFromValue(gtidCountVal)
	require.EqualValues(t, countWanted, gtidCount, "Tablet %s has %d errant GTIDs, wanted %d", tabletAlias, gtidCount, countWanted)
}

// getIntFromValue gets an integer from the given value. If it is convertible to a float, then we
// round the number to the nearest integer. If the value is not numeric at all, we return 0.
func getIntFromValue(val any) int {
	value := reflect.ValueOf(val)
	if value.CanFloat() {
		return int(math.Round(value.Float()))
	}
	if value.CanInt() {
		return int(value.Int())
	}
	return 0
}
