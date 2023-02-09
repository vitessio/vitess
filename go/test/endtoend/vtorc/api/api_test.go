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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/vtorc/utils"
)

// TestAPIEndpoints tests the various API endpoints that VTOrc offers.
// It also tests the functioning of the `disable-global-recoveries` flag.
func TestAPIEndpoints(t *testing.T) {
	defer cluster.PanicHandler(t)
	utils.SetupVttabletsAndVTOrcs(t, clusterInfo, 2, 1, []string{"--disable-global-recoveries"}, cluster.VTOrcConfiguration{
		PreventCrossDataCenterPrimaryFailover: true,
		RecoveryPeriodBlockSeconds:            5,
	}, 1, "")
	keyspace := &clusterInfo.ClusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]
	vtorc := clusterInfo.ClusterInstance.VTOrcProcesses[0]

	t.Run("Replication Analysis API - 1", func(t *testing.T) {
		// Wait until VTOrc picks up on there being no primary and verify
		// that we see a not null result on the api/replication-analysis end-point
		timeout := time.After(30 * time.Second)
		for {
			select {
			case <-timeout:
				t.Fatal("timed out waiting for VTOrc to see ClusterHasNoPrimary in replication analysis")
				return
			default:
				status, response, err := vtorc.MakeAPICall("/api/replication-analysis")
				if err == nil && status == 200 && strings.Contains(response, "ClusterHasNoPrimary") {
					return
				}
				time.Sleep(1 * time.Second)
			}
		}
	})

	// Enable global recoveries on the VTOrc instance and wait for primary to be elected
	vtorc.EnableGlobalRecoveries(t)

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
		status, resp := utils.MakeAPICall(t, vtorc, "/debug/health")
		assert.Equal(t, 200, status)
		assert.Contains(t, resp, `"Healthy": true,`)
	})

	t.Run("Liveness API", func(t *testing.T) {
		// Check that VTOrc is live
		status, resp := utils.MakeAPICall(t, vtorc, "/debug/liveness")
		assert.Equal(t, 200, status)
		assert.Empty(t, resp)
	})

	// Before we disable recoveries, let us wait until VTOrc has fixed all the issues (if any).
	_, _ = utils.MakeAPICallRetry(t, vtorc, "/api/replication-analysis", func(_ int, response string) bool {
		return response != "[]"
	})

	t.Run("Disable Recoveries API", func(t *testing.T) {
		// Disable recoveries of VTOrc
		status, resp := utils.MakeAPICall(t, vtorc, "/api/disable-global-recoveries")
		assert.Equal(t, 200, status)
		assert.Equal(t, "Global recoveries disabled\n", resp)
	})

	t.Run("Replication Analysis API - 2", func(t *testing.T) {
		// use vtctlclient to stop replication
		_, err := clusterInfo.ClusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("StopReplication", replica.Alias)
		require.NoError(t, err)

		// We know VTOrc won't fix this since we disabled global recoveries!
		// Wait until VTOrc picks up on this issue and verify
		// that we see a not null result on the api/replication-analysis page
		status, resp := utils.MakeAPICallRetry(t, vtorc, "/api/replication-analysis", func(_ int, response string) bool {
			return response == "[]"
		})
		assert.Equal(t, 200, status, resp)
		assert.Contains(t, resp, fmt.Sprintf(`"Port": %d`, replica.MySQLPort))
		assert.Contains(t, resp, `"Analysis": "ReplicationStopped"`)

		// Verify that filtering also works in the API as intended
		status, resp = utils.MakeAPICall(t, vtorc, "/api/replication-analysis?keyspace=ks&shard=0")
		assert.Equal(t, 200, status, resp)
		assert.Contains(t, resp, fmt.Sprintf(`"Port": %d`, replica.MySQLPort))

		// Verify that filtering by keyspace also works in the API as intended
		status, resp = utils.MakeAPICall(t, vtorc, "/api/replication-analysis?keyspace=ks")
		assert.Equal(t, 200, status, resp)
		assert.Contains(t, resp, fmt.Sprintf(`"Port": %d`, replica.MySQLPort))

		// Check that filtering using keyspace and shard works
		status, resp = utils.MakeAPICall(t, vtorc, "/api/replication-analysis?keyspace=ks&shard=80-")
		assert.Equal(t, 200, status, resp)
		assert.Equal(t, "[]", resp)

		// Check that filtering using just the shard fails
		status, resp = utils.MakeAPICall(t, vtorc, "/api/replication-analysis?shard=0")
		assert.Equal(t, 400, status, resp)
		assert.Equal(t, "Filtering by shard without keyspace isn't supported\n", resp)
	})

	t.Run("Enable Recoveries API", func(t *testing.T) {
		// Enable recoveries of VTOrc
		status, resp := utils.MakeAPICall(t, vtorc, "/api/enable-global-recoveries")
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
		status, resp = utils.MakeAPICall(t, vtorc, "/api/problems?keyspace=ks&shard=0")
		assert.Equal(t, 200, status, resp)
		assert.Contains(t, resp, fmt.Sprintf(`"InstanceAlias": "%v"`, replica.Alias))

		// Check that filtering using keyspace works
		status, resp = utils.MakeAPICall(t, vtorc, "/api/problems?keyspace=ks")
		assert.Equal(t, 200, status, resp)
		assert.Contains(t, resp, fmt.Sprintf(`"InstanceAlias": "%v"`, replica.Alias))

		// Check that filtering using keyspace and shard works
		status, resp = utils.MakeAPICall(t, vtorc, "/api/problems?keyspace=ks&shard=80-")
		assert.Equal(t, 200, status, resp)
		assert.Equal(t, "null", resp)

		// Check that filtering using just the shard fails
		status, resp = utils.MakeAPICall(t, vtorc, "/api/problems?shard=0")
		assert.Equal(t, 400, status, resp)
		assert.Equal(t, "Filtering by shard without keyspace isn't supported\n", resp)
	})
}
