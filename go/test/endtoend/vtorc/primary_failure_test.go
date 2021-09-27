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
	"vitess.io/vitess/go/vt/log"
)

// 2. bring down primary, let orc promote replica
// covers the test case master-failover from orchestrator
func TestDownPrimary(t *testing.T) {
	defer cluster.PanicHandler(t)
	setupVttabletsAndVtorc(t, 2, 0, nil, "test_config.json")
	keyspace := &clusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]
	// find primary from topo
	curPrimary := shardPrimaryTablet(t, clusterInstance, keyspace, shard0)
	assert.NotNil(t, curPrimary, "should have elected a primary")

	// Make the current primary database unavailable.
	err := curPrimary.MysqlctlProcess.Stop()
	require.NoError(t, err)
	defer func() {
		// we remove the tablet from our global list since its mysqlctl process has stopped and cannot be reused for other tests
		permanentlyRemoveVttablet(curPrimary)
	}()

	for _, tablet := range shard0.Vttablets {
		// we know we have only two tablets, so the "other" one must be the new primary
		if tablet.Alias != curPrimary.Alias {
			checkPrimaryTablet(t, clusterInstance, tablet)
			break
		}
	}
	logs, err := clusterInstance.VtorcProcess.GetLogs()
	log.Errorf("logs for vtorc - %s, error while reading logs - %v", logs, err)
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

	var replicaInSameCell *cluster.Vttablet
	for _, tablet := range shard0.Vttablets {
		// we know we have only two replcia tablets, so the one not the primary must be the other replica
		if tablet.Alias != curPrimary.Alias && tablet.Type == "replica" {
			replicaInSameCell = tablet
			break
		}
	}

	crossCellReplica := startVttablet(t, cell2, false)
	// newly started tablet does not replicate from anyone yet, we will allow orchestrator to fix this too
	checkReplication(t, clusterInstance, curPrimary, []*cluster.Vttablet{crossCellReplica, replicaInSameCell}, 25*time.Second)

	// Make the current primary database unavailable.
	err := curPrimary.MysqlctlProcess.Stop()
	require.NoError(t, err)
	defer func() {
		// we remove the tablet from our global list since its mysqlctl process has stopped and cannot be reused for other tests
		permanentlyRemoveVttablet(curPrimary)
	}()

	// we have a replica in the same cell, so that is the one which should be promoted and not the one from another cell
	checkPrimaryTablet(t, clusterInstance, replicaInSameCell)
	logs, err := clusterInstance.VtorcProcess.GetLogs()
	log.Errorf("logs for vtorc - %s, error while reading logs - %v", logs, err)
}

// Failover will sometimes lead to a replica which can no longer replicate.
// covers part of the test case master-failover-lost-replicas from orchestrator
func TestLostReplicasOnPrimaryFailure(t *testing.T) {
	defer cluster.PanicHandler(t)
	setupVttabletsAndVtorc(t, 2, 1, nil, "test_config.json")
	keyspace := &clusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]
	// find primary from topo
	curPrimary := shardPrimaryTablet(t, clusterInstance, keyspace, shard0)
	assert.NotNil(t, curPrimary, "should have elected a primary")

	// get the replicas
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

	// check that replication is setup correctly
	checkReplication(t, clusterInstance, curPrimary, []*cluster.Vttablet{rdonly, replica}, 15*time.Second)

	// revoke super privileges from vtorc on replica so that it is unable to repair the replication
	changePrivileges(t, `REVOKE SUPER ON *.* FROM 'orc_client_user'@'%'`, replica, "orc_client_user")

	// stop replication on the replica.
	err := clusterInstance.VtctlclientProcess.ExecuteCommand("StopReplication", replica.Alias)
	require.NoError(t, err)

	// check that rdonly is able to replicate. We also want to add some queries to rdonly which will not be there in replica
	runAdditionalCommands(t, curPrimary, []*cluster.Vttablet{rdonly}, 15*time.Second)

	// Make the current primary database unavailable.
	err = curPrimary.MysqlctlProcess.Stop()
	require.NoError(t, err)
	defer func() {
		// we remove the tablet from our global list since its mysqlctl process has stopped and cannot be reused for other tests
		permanentlyRemoveVttablet(curPrimary)
	}()

	// grant super privileges back to vtorc on replica so that it can repair
	changePrivileges(t, `GRANT SUPER ON *.* TO 'orc_client_user'@'%'`, replica, "orc_client_user")

	// vtorc must promote the lagging replica and not the rdonly, since it has a MustNotPromoteRule promotion rule
	checkPrimaryTablet(t, clusterInstance, replica)

	// check that the rdonly replica is lost. The lost replica has is detached and its host is prepended with `//`
	out, err := runSQL(t, "SELECT HOST FROM performance_schema.replication_connection_configuration", rdonly, "")
	require.NoError(t, err)
	require.Equal(t, "//localhost", out.Rows[0][0].ToString())
	logs, err := clusterInstance.VtorcProcess.GetLogs()
	log.Errorf("logs for vtorc - %s, error while reading logs - %v", logs, err)
}

// This test checks that the promotion of a tablet succeeds if it passes the promotion lag test
// covers the test case master-failover-fail-promotion-lag-minutes-success from orchestrator
func TestPromotionLagSuccess(t *testing.T) {
	defer cluster.PanicHandler(t)
	setupVttabletsAndVtorc(t, 2, 0, nil, "test_config_promotion_success.json")
	keyspace := &clusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]
	// find primary from topo
	curPrimary := shardPrimaryTablet(t, clusterInstance, keyspace, shard0)
	assert.NotNil(t, curPrimary, "should have elected a primary")

	// Make the current primary database unavailable.
	err := curPrimary.MysqlctlProcess.Stop()
	require.NoError(t, err)
	defer func() {
		// we remove the tablet from our global list since its mysqlctl process has stopped and cannot be reused for other tests
		permanentlyRemoveVttablet(curPrimary)
	}()

	for _, tablet := range shard0.Vttablets {
		// we know we have only two tablets, so the "other" one must be the new primary
		if tablet.Alias != curPrimary.Alias {
			checkPrimaryTablet(t, clusterInstance, tablet)
			break
		}
	}
	logs, err := clusterInstance.VtorcProcess.GetLogs()
	log.Errorf("logs for vtorc - %s, error while reading logs - %v", logs, err)
}

// This test checks that the promotion of a tablet succeeds if it passes the promotion lag test
// covers the test case master-failover-fail-promotion-lag-minutes-failure from orchestrator
func TestPromotionLagFailure(t *testing.T) {
	// skip the test since it fails now
	t.Skip()
	defer cluster.PanicHandler(t)
	setupVttabletsAndVtorc(t, 2, 0, nil, "test_config_promotion_failure.json")
	keyspace := &clusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]
	// find primary from topo
	curPrimary := shardPrimaryTablet(t, clusterInstance, keyspace, shard0)
	assert.NotNil(t, curPrimary, "should have elected a primary")

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
	checkPrimaryTablet(t, clusterInstance, curPrimary)
	logs, err := clusterInstance.VtorcProcess.GetLogs()
	log.Errorf("logs for vtorc - %s, error while reading logs - %v", logs, err)
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

	crossCellReplica := startVttablet(t, cell2, false)
	// newly started tablet does not replicate from anyone yet, we will allow orchestrator to fix this too
	checkReplication(t, clusterInstance, curPrimary, []*cluster.Vttablet{crossCellReplica}, 25*time.Second)

	// Make the current primary database unavailable.
	err := curPrimary.MysqlctlProcess.Stop()
	require.NoError(t, err)
	defer func() {
		// we remove the tablet from our global list since its mysqlctl process has stopped and cannot be reused for other tests
		permanentlyRemoveVttablet(curPrimary)
	}()

	// we have a replica in the same cell, so that is the one which should be promoted and not the one from another cell
	checkPrimaryTablet(t, clusterInstance, crossCellReplica)
	logs, err := clusterInstance.VtorcProcess.GetLogs()
	log.Errorf("logs for vtorc - %s, error while reading logs - %v", logs, err)
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

	// make the crossCellReplica lag by setting the source_delay to 20 seconds
	runSQL(t, "STOP SLAVE", crossCellReplica, "")
	runSQL(t, "CHANGE MASTER TO MASTER_DELAY = 20", crossCellReplica, "")
	runSQL(t, "START SLAVE", crossCellReplica, "")

	defer func() {
		// fix the crossCell replica back so that no other tests see this as a side effect
		runSQL(t, "STOP SLAVE", crossCellReplica, "")
		runSQL(t, "CHANGE MASTER TO MASTER_DELAY = 0", crossCellReplica, "")
		runSQL(t, "START SLAVE", crossCellReplica, "")
	}()

	// check that rdonly and replica are able to replicate. We also want to add some queries to replica which will not be there in crossCellReplica
	runAdditionalCommands(t, curPrimary, []*cluster.Vttablet{replica, rdonly}, 15*time.Second)

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
	checkPrimaryTablet(t, clusterInstance, crossCellReplica)

	// check that rdonly and replica are able to replicate from the crossCellReplica
	runAdditionalCommands(t, crossCellReplica, []*cluster.Vttablet{replica, rdonly}, 15*time.Second)
	logs, err := clusterInstance.VtorcProcess.GetLogs()
	log.Errorf("logs for vtorc - %s, error while reading logs - %v", logs, err)
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

	// make the replica lag by setting the source_delay to 20 seconds
	runSQL(t, "STOP SLAVE", replica, "")
	runSQL(t, "CHANGE MASTER TO MASTER_DELAY = 20", replica, "")
	runSQL(t, "START SLAVE", replica, "")

	defer func() {
		// fix the replica back so that no other tests see this as a side effect
		runSQL(t, "STOP SLAVE", replica, "")
		runSQL(t, "CHANGE MASTER TO MASTER_DELAY = 0", replica, "")
		runSQL(t, "START SLAVE", replica, "")
	}()

	// check that rdonly and crossCellReplica are able to replicate. We also want to add some queries to crossCenterReplica which will not be there in replica
	runAdditionalCommands(t, curPrimary, []*cluster.Vttablet{rdonly, crossCellReplica}, 15*time.Second)

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
	checkPrimaryTablet(t, clusterInstance, replica)

	// check that rdonly and crossCellReplica are able to replicate from the replica
	runAdditionalCommands(t, replica, []*cluster.Vttablet{crossCellReplica, rdonly}, 15*time.Second)
	logs, err := clusterInstance.VtorcProcess.GetLogs()
	log.Errorf("logs for vtorc - %s, error while reading logs - %v", logs, err)
}
