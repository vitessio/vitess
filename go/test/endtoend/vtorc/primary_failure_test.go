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

package vtorc

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
)

// 2. bring down primary, let orc promote replica
// covers the test case master-failover from orchestrator
func TestDownPrimary(t *testing.T) {
	defer cluster.PanicHandler(t)
	setupVttabletsAndVtorc(t, 2, 1, nil, "test_config.json")
	keyspace := &clusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]
	// find primary from topo
	curPrimary := shardPrimaryTablet(t, clusterInstance, keyspace, shard0)
	assert.NotNil(t, curPrimary, "should have elected a primary")

	// find the replica and rdonly tablets
	var replica, rdonly *cluster.Vttablet
	for _, tablet := range shard0.Vttablets {
		// we know we have only two replcia tablets, so the one not the primary must be the other replica
		if tablet.Alias != curPrimary.Alias && tablet.Type == "replica" {
			replica = tablet
		}
		if tablet.Type == "rdonly" {
			rdonly = tablet
		}
	}
	assert.NotNil(t, replica, "could not find replica tablet")
	assert.NotNil(t, rdonly, "could not find rdonly tablet")

	// check that the replication is setup correctly before we failover
	checkReplication(t, clusterInstance, curPrimary, []*cluster.Vttablet{rdonly, replica}, 10*time.Second)

	// Make the current primary database unavailable.
	err := curPrimary.MysqlctlProcess.Stop()
	require.NoError(t, err)
	defer func() {
		// we remove the tablet from our global list since its mysqlctl process has stopped and cannot be reused for other tests
		permanentlyRemoveVttablet(curPrimary)
	}()

	// check that the replica gets promoted
	checkPrimaryTablet(t, clusterInstance, replica, true)
	// also check that the replication is working correctly after failover
	runAdditionalCommands(t, replica, []*cluster.Vttablet{rdonly}, 10*time.Second)
}

// Failover should not be cross data centers, according to the configuration file
// covers part of the test case master-failover-lost-replicas from orchestrator
func TestCrossDataCenterFailure(t *testing.T) {
	defer cluster.PanicHandler(t)
	setupVttabletsAndVtorc(t, 2, 1, nil, "test_config.json")
	keyspace := &clusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]
	// find primary from topo
	curPrimary := shardPrimaryTablet(t, clusterInstance, keyspace, shard0)
	assert.NotNil(t, curPrimary, "should have elected a primary")

	// find the replica and rdonly tablets
	var replicaInSameCell, rdonly *cluster.Vttablet
	for _, tablet := range shard0.Vttablets {
		// we know we have only two replcia tablets, so the one not the primary must be the other replica
		if tablet.Alias != curPrimary.Alias && tablet.Type == "replica" {
			replicaInSameCell = tablet
		}
		if tablet.Type == "rdonly" {
			rdonly = tablet
		}
	}
	assert.NotNil(t, replicaInSameCell, "could not find replica tablet")
	assert.NotNil(t, rdonly, "could not find rdonly tablet")

	crossCellReplica := startVttablet(t, cell2, false)
	// newly started tablet does not replicate from anyone yet, we will allow orchestrator to fix this too
	checkReplication(t, clusterInstance, curPrimary, []*cluster.Vttablet{crossCellReplica, replicaInSameCell, rdonly}, 25*time.Second)

	// Make the current primary database unavailable.
	err := curPrimary.MysqlctlProcess.Stop()
	require.NoError(t, err)
	defer func() {
		// we remove the tablet from our global list since its mysqlctl process has stopped and cannot be reused for other tests
		permanentlyRemoveVttablet(curPrimary)
	}()

	// we have a replica in the same cell, so that is the one which should be promoted and not the one from another cell
	checkPrimaryTablet(t, clusterInstance, replicaInSameCell, true)
	// also check that the replication is working correctly after failover
	runAdditionalCommands(t, replicaInSameCell, []*cluster.Vttablet{crossCellReplica, rdonly}, 10*time.Second)
}

// Failover should not be cross data centers, according to the configuration file
// In case of no viable candidates, we should error out
func TestCrossDataCenterFailureError(t *testing.T) {
	defer cluster.PanicHandler(t)
	setupVttabletsAndVtorc(t, 1, 1, nil, "test_config.json")
	keyspace := &clusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]
	// find primary from topo
	curPrimary := shardPrimaryTablet(t, clusterInstance, keyspace, shard0)
	assert.NotNil(t, curPrimary, "should have elected a primary")

	// find the rdonly tablet
	var rdonly *cluster.Vttablet
	for _, tablet := range shard0.Vttablets {
		if tablet.Type == "rdonly" {
			rdonly = tablet
		}
	}
	assert.NotNil(t, rdonly, "could not find rdonly tablet")

	crossCellReplica1 := startVttablet(t, cell2, false)
	crossCellReplica2 := startVttablet(t, cell2, false)
	// newly started tablet does not replicate from anyone yet, we will allow orchestrator to fix this too
	checkReplication(t, clusterInstance, curPrimary, []*cluster.Vttablet{crossCellReplica1, crossCellReplica2, rdonly}, 25*time.Second)

	// Make the current primary database unavailable.
	err := curPrimary.MysqlctlProcess.Stop()
	require.NoError(t, err)
	defer func() {
		// we remove the tablet from our global list since its mysqlctl process has stopped and cannot be reused for other tests
		permanentlyRemoveVttablet(curPrimary)
	}()

	// wait for 20 seconds
	time.Sleep(20 * time.Second)

	// the previous primary should still be the primary since recovery of dead primary should fail
	checkPrimaryTablet(t, clusterInstance, curPrimary, false)
}

// Failover will sometimes lead to a rdonly which can no longer replicate.
// covers part of the test case master-failover-lost-replicas from orchestrator
func TestLostRdonlyOnPrimaryFailure(t *testing.T) {
	// new version of ERS does not check for lost replicas yet
	t.Skip()
	defer cluster.PanicHandler(t)
	setupVttabletsAndVtorc(t, 2, 2, nil, "test_config.json")
	keyspace := &clusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]
	// find primary from topo
	curPrimary := shardPrimaryTablet(t, clusterInstance, keyspace, shard0)
	assert.NotNil(t, curPrimary, "should have elected a primary")

	// get the tablets
	var replica, rdonly, aheadRdonly *cluster.Vttablet
	for _, tablet := range shard0.Vttablets {
		// find tablets which are not the primary
		if tablet.Alias != curPrimary.Alias {
			if tablet.Type == "replica" {
				replica = tablet
			} else {
				if rdonly == nil {
					rdonly = tablet
				} else {
					aheadRdonly = tablet
				}
			}
		}
	}
	assert.NotNil(t, replica, "could not find replica tablet")
	assert.NotNil(t, rdonly, "could not find any rdonly tablet")
	assert.NotNil(t, aheadRdonly, "could not find both rdonly tablet")

	// check that replication is setup correctly
	checkReplication(t, clusterInstance, curPrimary, []*cluster.Vttablet{rdonly, aheadRdonly, replica}, 15*time.Second)

	// revoke super privileges from vtorc on replica and rdonly so that it is unable to repair the replication
	changePrivileges(t, `REVOKE SUPER ON *.* FROM 'orc_client_user'@'%'`, replica, "orc_client_user")
	changePrivileges(t, `REVOKE SUPER ON *.* FROM 'orc_client_user'@'%'`, rdonly, "orc_client_user")

	// stop replication on the replica and rdonly.
	err := clusterInstance.VtctlclientProcess.ExecuteCommand("StopReplication", replica.Alias)
	require.NoError(t, err)
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("StopReplication", rdonly.Alias)
	require.NoError(t, err)

	// check that aheadRdonly is able to replicate. We also want to add some queries to aheadRdonly which will not be there in replica and rdonly
	runAdditionalCommands(t, curPrimary, []*cluster.Vttablet{aheadRdonly}, 15*time.Second)

	// assert that the replica and rdonly are indeed lagging and do not have the new insertion by checking the count of rows in the tables
	out, err := runSQL(t, "SELECT * FROM vt_insert_test", replica, "vt_ks")
	require.NoError(t, err)
	require.Equal(t, 1, len(out.Rows))
	out, err = runSQL(t, "SELECT * FROM vt_insert_test", rdonly, "vt_ks")
	require.NoError(t, err)
	require.Equal(t, 1, len(out.Rows))

	// Make the current primary database unavailable.
	err = curPrimary.MysqlctlProcess.Stop()
	require.NoError(t, err)
	defer func() {
		// we remove the tablet from our global list since its mysqlctl process has stopped and cannot be reused for other tests
		permanentlyRemoveVttablet(curPrimary)
	}()

	// grant super privileges back to vtorc on replica and rdonly so that it can repair
	changePrivileges(t, `GRANT SUPER ON *.* TO 'orc_client_user'@'%'`, replica, "orc_client_user")
	changePrivileges(t, `GRANT SUPER ON *.* TO 'orc_client_user'@'%'`, rdonly, "orc_client_user")

	// vtorc must promote the lagging replica and not the rdonly, since it has a MustNotPromoteRule promotion rule
	checkPrimaryTablet(t, clusterInstance, replica, true)

	// also check that the replication is setup correctly
	runAdditionalCommands(t, replica, []*cluster.Vttablet{rdonly}, 15*time.Second)

	// check that the rdonly is lost. The lost replica has is detached and its host is prepended with `//`
	out, err = runSQL(t, "SELECT HOST FROM performance_schema.replication_connection_configuration", aheadRdonly, "")
	require.NoError(t, err)
	require.Equal(t, "//localhost", out.Rows[0][0].ToString())
}

// This test checks that the promotion of a tablet succeeds if it passes the promotion lag test
// covers the test case master-failover-fail-promotion-lag-minutes-success from orchestrator
func TestPromotionLagSuccess(t *testing.T) {
	defer cluster.PanicHandler(t)
	setupVttabletsAndVtorc(t, 2, 1, nil, "test_config_promotion_success.json")
	keyspace := &clusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]
	// find primary from topo
	curPrimary := shardPrimaryTablet(t, clusterInstance, keyspace, shard0)
	assert.NotNil(t, curPrimary, "should have elected a primary")

	// find the replica and rdonly tablets
	var replica, rdonly *cluster.Vttablet
	for _, tablet := range shard0.Vttablets {
		// we know we have only two replcia tablets, so the one not the primary must be the other replica
		if tablet.Alias != curPrimary.Alias && tablet.Type == "replica" {
			replica = tablet
		}
		if tablet.Type == "rdonly" {
			rdonly = tablet
		}
	}
	assert.NotNil(t, replica, "could not find replica tablet")
	assert.NotNil(t, rdonly, "could not find rdonly tablet")

	// check that the replication is setup correctly before we failover
	checkReplication(t, clusterInstance, curPrimary, []*cluster.Vttablet{rdonly, replica}, 10*time.Second)

	// Make the current primary database unavailable.
	err := curPrimary.MysqlctlProcess.Stop()
	require.NoError(t, err)
	defer func() {
		// we remove the tablet from our global list since its mysqlctl process has stopped and cannot be reused for other tests
		permanentlyRemoveVttablet(curPrimary)
	}()

	// check that the replica gets promoted
	checkPrimaryTablet(t, clusterInstance, replica, true)
	// also check that the replication is working correctly after failover
	runAdditionalCommands(t, replica, []*cluster.Vttablet{rdonly}, 10*time.Second)
}

// This test checks that the promotion of a tablet succeeds if it passes the promotion lag test
// covers the test case master-failover-fail-promotion-lag-minutes-failure from orchestrator
func TestPromotionLagFailure(t *testing.T) {
	// new version of ERS does not check for promotion lag yet
	t.Skip()
	defer cluster.PanicHandler(t)
	setupVttabletsAndVtorc(t, 3, 1, nil, "test_config_promotion_failure.json")
	keyspace := &clusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]
	// find primary from topo
	curPrimary := shardPrimaryTablet(t, clusterInstance, keyspace, shard0)
	assert.NotNil(t, curPrimary, "should have elected a primary")

	// find the replica and rdonly tablets
	var replica1, replica2, rdonly *cluster.Vttablet
	for _, tablet := range shard0.Vttablets {
		// we know we have only two replcia tablets, so the one not the primary must be the other replica
		if tablet.Alias != curPrimary.Alias && tablet.Type == "replica" {
			if replica1 == nil {
				replica1 = tablet
			} else {
				replica2 = tablet
			}
		}
		if tablet.Type == "rdonly" {
			rdonly = tablet
		}
	}
	assert.NotNil(t, replica1, "could not find replica tablet")
	assert.NotNil(t, replica2, "could not find second replica tablet")
	assert.NotNil(t, rdonly, "could not find rdonly tablet")

	// check that the replication is setup correctly before we failover
	checkReplication(t, clusterInstance, curPrimary, []*cluster.Vttablet{rdonly, replica1, replica2}, 10*time.Second)

	// Make the current primary database unavailable.
	err := curPrimary.MysqlctlProcess.Stop()
	require.NoError(t, err)
	defer func() {
		// we remove the tablet from our global list since its mysqlctl process has stopped and cannot be reused for other tests
		permanentlyRemoveVttablet(curPrimary)
	}()

	// wait for 20 seconds
	time.Sleep(20 * time.Second)

	// the previous primary should still be the primary since recovery of dead primary should fail
	checkPrimaryTablet(t, clusterInstance, curPrimary, false)
}

// covers the test case master-failover-candidate from orchestrator
// We explicitly set one of the replicas to Prefer promotion rule.
// That is the replica which should be promoted in case of primary failure
func TestDownPrimaryPromotionRule(t *testing.T) {
	defer cluster.PanicHandler(t)
	setupVttabletsAndVtorc(t, 2, 1, nil, "test_config_crosscenter_prefer.json")
	keyspace := &clusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]
	// find primary from topo
	curPrimary := shardPrimaryTablet(t, clusterInstance, keyspace, shard0)
	assert.NotNil(t, curPrimary, "should have elected a primary")

	// find the replica and rdonly tablets
	var replica, rdonly *cluster.Vttablet
	for _, tablet := range shard0.Vttablets {
		// we know we have only two replcia tablets, so the one not the primary must be the other replica
		if tablet.Alias != curPrimary.Alias && tablet.Type == "replica" {
			replica = tablet
		}
		if tablet.Type == "rdonly" {
			rdonly = tablet
		}
	}
	assert.NotNil(t, replica, "could not find replica tablet")
	assert.NotNil(t, rdonly, "could not find rdonly tablet")

	crossCellReplica := startVttablet(t, cell2, false)
	// newly started tablet does not replicate from anyone yet, we will allow orchestrator to fix this too
	checkReplication(t, clusterInstance, curPrimary, []*cluster.Vttablet{crossCellReplica, rdonly, replica}, 25*time.Second)

	// Make the current primary database unavailable.
	err := curPrimary.MysqlctlProcess.Stop()
	require.NoError(t, err)
	defer func() {
		// we remove the tablet from our global list since its mysqlctl process has stopped and cannot be reused for other tests
		permanentlyRemoveVttablet(curPrimary)
	}()

	// we have a replica in the same cell, so that is the one which should be promoted and not the one from another cell
	checkPrimaryTablet(t, clusterInstance, crossCellReplica, true)
	// also check that the replication is working correctly after failover
	runAdditionalCommands(t, crossCellReplica, []*cluster.Vttablet{rdonly, replica}, 10*time.Second)
}

// covers the test case master-failover-candidate-lag from orchestrator
// We explicitly set one of the replicas to Prefer promotion rule and make it lag with respect to other replicas.
// That is the replica which should be promoted in case of primary failure
// It should also be caught up when it is promoted
func TestDownPrimaryPromotionRuleWithLag(t *testing.T) {
	defer cluster.PanicHandler(t)
	setupVttabletsAndVtorc(t, 2, 1, nil, "test_config_crosscenter_prefer.json")
	keyspace := &clusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]
	// find primary from topo
	curPrimary := shardPrimaryTablet(t, clusterInstance, keyspace, shard0)
	assert.NotNil(t, curPrimary, "should have elected a primary")

	// get the replicas in the same cell
	var replica, rdonly *cluster.Vttablet
	for _, tablet := range shard0.Vttablets {
		// find tablets which are not the primary
		if tablet.Alias != curPrimary.Alias {
			if tablet.Type == "replica" {
				replica = tablet
			} else {
				rdonly = tablet
			}
		}
	}
	assert.NotNil(t, replica, "could not find replica tablet")
	assert.NotNil(t, rdonly, "could not find rdonly tablet")

	crossCellReplica := startVttablet(t, cell2, false)
	// newly started tablet does not replicate from anyone yet, we will allow orchestrator to fix this too
	checkReplication(t, clusterInstance, curPrimary, []*cluster.Vttablet{crossCellReplica, replica, rdonly}, 25*time.Second)

	// revoke super privileges from vtorc on crossCellReplica so that it is unable to repair the replication
	changePrivileges(t, `REVOKE SUPER ON *.* FROM 'orc_client_user'@'%'`, crossCellReplica, "orc_client_user")

	// stop replication on the crossCellReplica.
	err := clusterInstance.VtctlclientProcess.ExecuteCommand("StopReplication", crossCellReplica.Alias)
	require.NoError(t, err)

	// check that rdonly and replica are able to replicate. We also want to add some queries to replica which will not be there in crossCellReplica
	runAdditionalCommands(t, curPrimary, []*cluster.Vttablet{replica, rdonly}, 15*time.Second)

	// reset the primary logs so that crossCellReplica can never catch up
	resetPrimaryLogs(t, curPrimary)

	// start replication back on the crossCellReplica.
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("StartReplication", crossCellReplica.Alias)
	require.NoError(t, err)

	// grant super privileges back to vtorc on crossCellReplica so that it can repair
	changePrivileges(t, `GRANT SUPER ON *.* TO 'orc_client_user'@'%'`, crossCellReplica, "orc_client_user")

	// assert that the crossCellReplica is indeed lagging and does not have the new insertion by checking the count of rows in the table
	out, err := runSQL(t, "SELECT * FROM vt_insert_test", crossCellReplica, "vt_ks")
	require.NoError(t, err)
	require.Equal(t, 1, len(out.Rows))

	// Make the current primary database unavailable.
	err = curPrimary.MysqlctlProcess.Stop()
	require.NoError(t, err)
	defer func() {
		// we remove the tablet from our global list since its mysqlctl process has stopped and cannot be reused for other tests
		permanentlyRemoveVttablet(curPrimary)
	}()

	// the crossCellReplica is set to be preferred according to the durability requirements. So it must be promoted
	checkPrimaryTablet(t, clusterInstance, crossCellReplica, true)

	// assert that the crossCellReplica has indeed caught up
	out, err = runSQL(t, "SELECT * FROM vt_insert_test", crossCellReplica, "vt_ks")
	require.NoError(t, err)
	require.Equal(t, 2, len(out.Rows))

	// check that rdonly and replica are able to replicate from the crossCellReplica
	runAdditionalCommands(t, crossCellReplica, []*cluster.Vttablet{replica, rdonly}, 15*time.Second)
}

// covers the test case master-failover-candidate-lag-cross-datacenter from orchestrator
// We explicitly set one of the cross-cell replicas to Prefer promotion rule, but we prevent cross data center promotions.
// We let a replica in our own cell lag. That is the replica which should be promoted in case of primary failure
// It should also be caught up when it is promoted
func TestDownPrimaryPromotionRuleWithLagCrossCenter(t *testing.T) {
	defer cluster.PanicHandler(t)
	setupVttabletsAndVtorc(t, 2, 1, nil, "test_config_crosscenter_prefer_prevent.json")
	keyspace := &clusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]
	// find primary from topo
	curPrimary := shardPrimaryTablet(t, clusterInstance, keyspace, shard0)
	assert.NotNil(t, curPrimary, "should have elected a primary")

	// get the replicas in the same cell
	var replica, rdonly *cluster.Vttablet
	for _, tablet := range shard0.Vttablets {
		// find tablets which are not the primary
		if tablet.Alias != curPrimary.Alias {
			if tablet.Type == "replica" {
				replica = tablet
			} else {
				rdonly = tablet
			}
		}
	}
	assert.NotNil(t, replica, "could not find replica tablet")
	assert.NotNil(t, rdonly, "could not find rdonly tablet")

	crossCellReplica := startVttablet(t, cell2, false)
	// newly started tablet does not replicate from anyone yet, we will allow orchestrator to fix this too
	checkReplication(t, clusterInstance, curPrimary, []*cluster.Vttablet{crossCellReplica, replica, rdonly}, 25*time.Second)

	// revoke super privileges from vtorc on replica so that it is unable to repair the replication
	changePrivileges(t, `REVOKE SUPER ON *.* FROM 'orc_client_user'@'%'`, replica, "orc_client_user")

	// stop replication on the replica.
	err := clusterInstance.VtctlclientProcess.ExecuteCommand("StopReplication", replica.Alias)
	require.NoError(t, err)

	// check that rdonly and crossCellReplica are able to replicate. We also want to add some queries to crossCenterReplica which will not be there in replica
	runAdditionalCommands(t, curPrimary, []*cluster.Vttablet{rdonly, crossCellReplica}, 15*time.Second)

	// reset the primary logs so that crossCellReplica can never catch up
	resetPrimaryLogs(t, curPrimary)

	// start replication back on the replica.
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("StartReplication", replica.Alias)
	require.NoError(t, err)

	// grant super privileges back to vtorc on replica so that it can repair
	changePrivileges(t, `GRANT SUPER ON *.* TO 'orc_client_user'@'%'`, replica, "orc_client_user")

	// assert that the replica is indeed lagging and does not have the new insertion by checking the count of rows in the table
	out, err := runSQL(t, "SELECT * FROM vt_insert_test", replica, "vt_ks")
	require.NoError(t, err)
	require.Equal(t, 1, len(out.Rows))

	// Make the current primary database unavailable.
	err = curPrimary.MysqlctlProcess.Stop()
	require.NoError(t, err)
	defer func() {
		// we remove the tablet from our global list since its mysqlctl process has stopped and cannot be reused for other tests
		permanentlyRemoveVttablet(curPrimary)
	}()

	// the replica should be promoted since we have prevented cross cell promotions
	checkPrimaryTablet(t, clusterInstance, replica, true)

	// assert that the replica has indeed caught up
	out, err = runSQL(t, "SELECT * FROM vt_insert_test", replica, "vt_ks")
	require.NoError(t, err)
	require.Equal(t, 2, len(out.Rows))

	// check that rdonly and crossCellReplica are able to replicate from the replica
	runAdditionalCommands(t, replica, []*cluster.Vttablet{crossCellReplica, rdonly}, 15*time.Second)
}
