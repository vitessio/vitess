/*
Copyright 2021 The Vitess Authors.

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

package gracefultakeover

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/vtorc/utils"
)

// make an api call to graceful primary takeover and let vtorc fix it
// covers the test case graceful-master-takeover from orchestrator
func TestGracefulPrimaryTakeover(t *testing.T) {
	defer cluster.PanicHandler(t)
	utils.SetupVttabletsAndVTOrcs(t, clusterInfo, 2, 0, nil, cluster.VTOrcConfiguration{
		PreventCrossDataCenterPrimaryFailover: true,
		RecoveryPeriodBlockSeconds:            5,
	}, 1, "")
	keyspace := &clusterInfo.ClusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]

	// find primary from topo
	curPrimary := utils.ShardPrimaryTablet(t, clusterInfo, keyspace, shard0)
	assert.NotNil(t, curPrimary, "should have elected a primary")

	// find the replica tablet
	var replica *cluster.Vttablet
	for _, tablet := range shard0.Vttablets {
		// we know we have only two tablets, so the one not the primary must be the replica
		if tablet.Alias != curPrimary.Alias {
			replica = tablet
		}
	}
	assert.NotNil(t, replica, "could not find replica tablet")

	// check that the replication is setup correctly before we set read-only
	utils.CheckReplication(t, clusterInfo, curPrimary, []*cluster.Vttablet{replica}, 10*time.Second)

	// Run a failure on the current primary before, just to check that we are able to
	// register another recovery right after fixing this first
	// Make the current primary database read-only.
	_, err := utils.RunSQL(t, "set global read_only=ON", curPrimary, "")
	require.NoError(t, err)

	// wait for repair
	match := utils.WaitForReadOnlyValue(t, curPrimary, 0)
	require.True(t, match)

	// this is added to reduce flakiness. We need to wait for 1 second before calling
	// Graceful primray takeover to ensure that the recovery is registered. If the second recovery
	// ran in less than 1 second, then trying to acknowledge completed recoveries fails because
	// we try to set the same end timestamp on the recovery of first 2 failures which fails the unique constraint
	time.Sleep(1 * time.Second)

	status, resp := utils.MakeAPICallRetry(t, fmt.Sprintf("http://localhost:%d/api/graceful-primary-takeover/localhost/%d/localhost/%d", clusterInfo.ClusterInstance.VTOrcProcesses[0].WebPort, curPrimary.MySQLPort, replica.MySQLPort))
	assert.Equal(t, 200, status, resp)

	// check that the replica gets promoted
	utils.CheckPrimaryTablet(t, clusterInfo, replica, true)
	utils.VerifyWritesSucceed(t, clusterInfo, replica, []*cluster.Vttablet{curPrimary}, 10*time.Second)
}

// make an api call to graceful primary takeover without specifying the primary tablet to promote
// covers the test case graceful-master-takeover-fail-no-target from orchestrator
// orchestrator used to fail in this case, but for VtOrc, specifying no target makes it choose one on its own
func TestGracefulPrimaryTakeoverNoTarget(t *testing.T) {
	defer cluster.PanicHandler(t)
	utils.SetupVttabletsAndVTOrcs(t, clusterInfo, 2, 0, nil, cluster.VTOrcConfiguration{
		PreventCrossDataCenterPrimaryFailover: true,
	}, 1, "")
	keyspace := &clusterInfo.ClusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]

	// find primary from topo
	curPrimary := utils.ShardPrimaryTablet(t, clusterInfo, keyspace, shard0)
	assert.NotNil(t, curPrimary, "should have elected a primary")

	// find the replica tablet
	var replica *cluster.Vttablet
	for _, tablet := range shard0.Vttablets {
		// we know we have only two tablets, so the one not the primary must be the replica
		if tablet.Alias != curPrimary.Alias {
			replica = tablet
		}
	}
	assert.NotNil(t, replica, "could not find the replica tablet")

	// check that the replication is setup correctly before we failover
	utils.CheckReplication(t, clusterInfo, curPrimary, []*cluster.Vttablet{replica}, 10*time.Second)

	status, resp := utils.MakeAPICallRetry(t, fmt.Sprintf("http://localhost:%d/api/graceful-primary-takeover/localhost/%d/", clusterInfo.ClusterInstance.VTOrcProcesses[0].WebPort, curPrimary.MySQLPort))
	assert.Equal(t, 200, status, resp)

	// check that the replica gets promoted
	utils.CheckPrimaryTablet(t, clusterInfo, replica, true)
	utils.VerifyWritesSucceed(t, clusterInfo, replica, []*cluster.Vttablet{curPrimary}, 10*time.Second)
}

// make an api call to graceful primary takeover auto and let vtorc fix it
// covers the test case graceful-master-takeover-auto from orchestrator
func TestGracefulPrimaryTakeoverAuto(t *testing.T) {
	defer cluster.PanicHandler(t)
	utils.SetupVttabletsAndVTOrcs(t, clusterInfo, 2, 1, nil, cluster.VTOrcConfiguration{
		PreventCrossDataCenterPrimaryFailover: true,
	}, 1, "")
	keyspace := &clusterInfo.ClusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]

	// find primary from topo
	primary := utils.ShardPrimaryTablet(t, clusterInfo, keyspace, shard0)
	assert.NotNil(t, primary, "should have elected a primary")

	// find the replica tablet and the rdonly tablet
	var replica, rdonly *cluster.Vttablet
	for _, tablet := range shard0.Vttablets {
		// we know we have only two replcia tablets, so the one not the primary must be the other replica
		if tablet.Alias != primary.Alias && tablet.Type == "replica" {
			replica = tablet
		}
		if tablet.Type == "rdonly" {
			rdonly = tablet
		}
	}
	assert.NotNil(t, replica, "could not find replica tablet")
	assert.NotNil(t, rdonly, "could not find rdonly tablet")

	// check that the replication is setup correctly before we failover
	utils.CheckReplication(t, clusterInfo, primary, []*cluster.Vttablet{replica, rdonly}, 10*time.Second)

	status, resp := utils.MakeAPICallRetry(t, fmt.Sprintf("http://localhost:%d/api/graceful-primary-takeover-auto/localhost/%d/localhost/%d", clusterInfo.ClusterInstance.VTOrcProcesses[0].WebPort, primary.MySQLPort, replica.MySQLPort))
	assert.Equal(t, 200, status, resp)

	// check that the replica gets promoted
	utils.CheckPrimaryTablet(t, clusterInfo, replica, true)
	utils.VerifyWritesSucceed(t, clusterInfo, replica, []*cluster.Vttablet{primary, rdonly}, 10*time.Second)

	status, resp = utils.MakeAPICallRetry(t, fmt.Sprintf("http://localhost:%d/api/graceful-primary-takeover-auto/localhost/%d/", clusterInfo.ClusterInstance.VTOrcProcesses[0].WebPort, replica.MySQLPort))
	assert.Equal(t, 200, status, resp)

	// check that the primary gets promoted back
	utils.CheckPrimaryTablet(t, clusterInfo, primary, true)
	utils.VerifyWritesSucceed(t, clusterInfo, primary, []*cluster.Vttablet{replica, rdonly}, 10*time.Second)
}

// make an api call to graceful primary takeover with a cross-cell replica and check that it errors out
// covers the test case graceful-master-takeover-fail-cross-region from orchestrator
func TestGracefulPrimaryTakeoverFailCrossCell(t *testing.T) {
	defer cluster.PanicHandler(t)
	utils.SetupVttabletsAndVTOrcs(t, clusterInfo, 1, 1, nil, cluster.VTOrcConfiguration{
		PreventCrossDataCenterPrimaryFailover: true,
	}, 1, "")
	keyspace := &clusterInfo.ClusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]

	// find primary from topo
	primary := utils.ShardPrimaryTablet(t, clusterInfo, keyspace, shard0)
	assert.NotNil(t, primary, "should have elected a primary")

	// find the rdonly tablet
	var rdonly *cluster.Vttablet
	for _, tablet := range shard0.Vttablets {
		if tablet.Type == "rdonly" {
			rdonly = tablet
		}
	}
	assert.NotNil(t, rdonly, "could not find rdonly tablet")

	crossCellReplica1 := utils.StartVttablet(t, clusterInfo, utils.Cell2, false)
	// newly started tablet does not replicate from anyone yet, we will allow vtorc to fix this too
	utils.CheckReplication(t, clusterInfo, primary, []*cluster.Vttablet{crossCellReplica1, rdonly}, 25*time.Second)

	status, response := utils.MakeAPICallRetry(t, fmt.Sprintf("http://localhost:%d/api/graceful-primary-takeover/localhost/%d/localhost/%d", clusterInfo.ClusterInstance.VTOrcProcesses[0].WebPort, primary.MySQLPort, crossCellReplica1.MySQLPort))
	assert.Equal(t, 500, status, response)
	assert.Contains(t, response, "GracefulPrimaryTakeover: constraint failure")

	// check that the cross-cell replica doesn't get promoted and the previous primary is still the primary
	utils.CheckPrimaryTablet(t, clusterInfo, primary, true)
	utils.VerifyWritesSucceed(t, clusterInfo, primary, []*cluster.Vttablet{crossCellReplica1, rdonly}, 10*time.Second)
}
