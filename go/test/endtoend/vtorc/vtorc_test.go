/*
Copyright 2020 The Vitess Authors.

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
	"fmt"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/log"

	_ "vitess.io/vitess/go/vt/topo/consultopo"
	_ "vitess.io/vitess/go/vt/topo/etcd2topo"
	_ "vitess.io/vitess/go/vt/topo/k8stopo"
	_ "vitess.io/vitess/go/vt/topo/zk2topo"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
)

// Cases to test:
// 1. create cluster with 1 replica and 1 rdonly, let orc choose primary
// verify rdonly is not elected, only replica
// verify replication is setup
func TestPrimaryElection(t *testing.T) {
	defer func() {
		logs, err := clusterInstance.VtorcProcess.GetLogs()
		log.Errorf("logs for vtorc - %s, error while reading logs - %v", logs, err)
	}()
	defer cluster.PanicHandler(t)
	setupVttabletsAndVtorc(t, 1, 1, nil, "test_config.json")
	keyspace := &clusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]

	checkPrimaryTablet(t, clusterInstance, shard0.Vttablets[0])
	checkReplication(t, clusterInstance, shard0.Vttablets[0], shard0.Vttablets[1:], 10*time.Second)
}

// Cases to test:
// 1. create cluster with 1 replica and 1 rdonly, let orc choose primary
// verify rdonly is not elected, only replica
// verify replication is setup
func TestSingleKeyspace(t *testing.T) {
	defer func() {
		logs, err := clusterInstance.VtorcProcess.GetLogs()
		log.Errorf("logs for vtorc - %s, error while reading logs - %v", logs, err)
	}()
	defer cluster.PanicHandler(t)
	setupVttabletsAndVtorc(t, 1, 1, []string{"-clusters_to_watch", "ks"}, "test_config.json")
	keyspace := &clusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]

	checkPrimaryTablet(t, clusterInstance, shard0.Vttablets[0])
	checkReplication(t, clusterInstance, shard0.Vttablets[0], shard0.Vttablets[1:], 10*time.Second)
}

// Cases to test:
// 1. create cluster with 1 replica and 1 rdonly, let orc choose primary
// verify rdonly is not elected, only replica
// verify replication is setup
func TestKeyspaceShard(t *testing.T) {
	defer func() {
		logs, err := clusterInstance.VtorcProcess.GetLogs()
		log.Errorf("logs for vtorc - %s, error while reading logs - %v", logs, err)
	}()
	defer cluster.PanicHandler(t)
	setupVttabletsAndVtorc(t, 1, 1, []string{"-clusters_to_watch", "ks/0"}, "test_config.json")
	keyspace := &clusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]

	checkPrimaryTablet(t, clusterInstance, shard0.Vttablets[0])
	checkReplication(t, clusterInstance, shard0.Vttablets[0], shard0.Vttablets[1:], 10*time.Second)
}

func waitForReadOnlyValue(t *testing.T, curPrimary *cluster.Vttablet, expectValue int64) (match bool) {
	timeout := 15 * time.Second
	startTime := time.Now()
	for time.Since(startTime) < timeout {
		qr, err := runSQL(t, "select @@global.read_only as read_only", curPrimary, "")
		require.NoError(t, err)
		require.NotNil(t, qr)
		row := qr.Named().Row()
		require.NotNil(t, row)
		readOnly, err := row.ToInt64("read_only")
		require.NoError(t, err)
		if readOnly == expectValue {
			return true
		}
		time.Sleep(time.Second)
	}
	return false
}

// 3. make primary readonly, let orc repair
func TestPrimaryReadOnly(t *testing.T) {
	defer func() {
		logs, err := clusterInstance.VtorcProcess.GetLogs()
		log.Errorf("logs for vtorc - %s, error while reading logs - %v", logs, err)
	}()
	defer cluster.PanicHandler(t)
	setupVttabletsAndVtorc(t, 2, 0, nil, "test_config.json")
	keyspace := &clusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]

	// find primary from topo
	curPrimary := shardPrimaryTablet(t, clusterInstance, keyspace, shard0)
	assert.NotNil(t, curPrimary, "should have elected a primary")

	// Make the current primary database read-only.
	_, err := runSQL(t, "set global read_only=ON", curPrimary, "")
	require.NoError(t, err)

	// wait for repair
	match := waitForReadOnlyValue(t, curPrimary, 0)
	require.True(t, match)
}

// 4. make replica ReadWrite, let orc repair
func TestReplicaReadWrite(t *testing.T) {
	defer func() {
		logs, err := clusterInstance.VtorcProcess.GetLogs()
		log.Errorf("logs for vtorc - %s, error while reading logs - %v", logs, err)
	}()
	defer cluster.PanicHandler(t)
	setupVttabletsAndVtorc(t, 2, 0, nil, "test_config.json")
	keyspace := &clusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]

	// find primary from topo
	curPrimary := shardPrimaryTablet(t, clusterInstance, keyspace, shard0)
	assert.NotNil(t, curPrimary, "should have elected a primary")

	var replica *cluster.Vttablet
	for _, tablet := range shard0.Vttablets {
		// we know we have only two tablets, so the "other" one must be the new primary
		if tablet.Alias != curPrimary.Alias {
			replica = tablet
			break
		}
	}
	// Make the replica database read-write.
	_, err := runSQL(t, "set global read_only=OFF", replica, "")
	require.NoError(t, err)

	// wait for repair
	match := waitForReadOnlyValue(t, replica, 1)
	require.True(t, match)
}

// 5. stop replication, let orc repair
func TestStopReplication(t *testing.T) {
	defer func() {
		logs, err := clusterInstance.VtorcProcess.GetLogs()
		log.Errorf("logs for vtorc - %s, error while reading logs - %v", logs, err)
	}()
	defer cluster.PanicHandler(t)
	setupVttabletsAndVtorc(t, 2, 0, nil, "test_config.json")
	keyspace := &clusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]

	// find primary from topo
	curPrimary := shardPrimaryTablet(t, clusterInstance, keyspace, shard0)
	assert.NotNil(t, curPrimary, "should have elected a primary")

	// TODO(deepthi): we should not need to do this, the DB should be created automatically
	_, err := curPrimary.VttabletProcess.QueryTablet(fmt.Sprintf("create database IF NOT EXISTS vt_%s", keyspace.Name), keyspace.Name, false)
	require.NoError(t, err)

	var replica *cluster.Vttablet
	for _, tablet := range shard0.Vttablets {
		// we know we have only two tablets, so the "other" one must be the new primary
		if tablet.Alias != curPrimary.Alias {
			replica = tablet
			break
		}
	}
	require.NotNil(t, replica, "should be able to find a replica")
	// use vtctlclient to stop replication
	_, err = clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("StopReplication", replica.Alias)
	require.NoError(t, err)

	// check replication is setup correctly
	checkReplication(t, clusterInstance, curPrimary, []*cluster.Vttablet{replica}, 15*time.Second)
}

// 6. setup replication from non-primary, let orc repair
func TestReplicationFromOtherReplica(t *testing.T) {
	defer func() {
		logs, err := clusterInstance.VtorcProcess.GetLogs()
		log.Errorf("logs for vtorc - %s, error while reading logs - %v", logs, err)
	}()
	defer cluster.PanicHandler(t)
	setupVttabletsAndVtorc(t, 3, 0, nil, "test_config.json")
	keyspace := &clusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]

	// find primary from topo
	curPrimary := shardPrimaryTablet(t, clusterInstance, keyspace, shard0)
	assert.NotNil(t, curPrimary, "should have elected a primary")

	// TODO(deepthi): we should not need to do this, the DB should be created automatically
	_, err := curPrimary.VttabletProcess.QueryTablet(fmt.Sprintf("create database IF NOT EXISTS vt_%s", keyspace.Name), keyspace.Name, false)
	require.NoError(t, err)

	var replica, otherReplica *cluster.Vttablet
	for _, tablet := range shard0.Vttablets {
		// we know we have only two tablets, so the "other" one must be the new primary
		if tablet.Alias != curPrimary.Alias {
			if replica == nil {
				replica = tablet
			} else {
				otherReplica = tablet
			}
		}
	}
	require.NotNil(t, replica, "should be able to find a replica")
	require.NotNil(t, otherReplica, "should be able to find 2nd replica")

	// point replica at otherReplica
	// Get primary position
	hostname := "localhost"
	_, gtid := cluster.GetPrimaryPosition(t, *otherReplica, hostname)

	changeReplicationSourceCommand := fmt.Sprintf("STOP SLAVE; RESET MASTER; SET GLOBAL gtid_purged = '%s';"+
		"CHANGE MASTER TO MASTER_HOST='%s', MASTER_PORT=%d, MASTER_USER='vt_repl', MASTER_AUTO_POSITION = 1; START SLAVE", gtid, hostname, otherReplica.MySQLPort)
	result, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("ExecuteFetchAsDba", replica.Alias, changeReplicationSourceCommand)
	require.NoError(t, err, result)

	// check replication is setup correctly
	checkReplication(t, clusterInstance, curPrimary, []*cluster.Vttablet{replica, otherReplica}, 15*time.Second)
}

func TestRepairAfterTER(t *testing.T) {
	defer func() {
		logs, err := clusterInstance.VtorcProcess.GetLogs()
		log.Errorf("logs for vtorc - %s, error while reading logs - %v", logs, err)
	}()
	// test fails intermittently on CI, skip until it can be fixed.
	t.SkipNow()
	defer cluster.PanicHandler(t)
	setupVttabletsAndVtorc(t, 2, 0, nil, "test_config.json")
	keyspace := &clusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]

	// find primary from topo
	curPrimary := shardPrimaryTablet(t, clusterInstance, keyspace, shard0)
	assert.NotNil(t, curPrimary, "should have elected a primary")

	// TODO(deepthi): we should not need to do this, the DB should be created automatically
	_, err := curPrimary.VttabletProcess.QueryTablet(fmt.Sprintf("create database IF NOT EXISTS vt_%s", keyspace.Name), keyspace.Name, false)
	require.NoError(t, err)

	var newPrimary *cluster.Vttablet
	for _, tablet := range shard0.Vttablets {
		// we know we have only two tablets, so the "other" one must be the new primary
		if tablet.Alias != curPrimary.Alias {
			newPrimary = tablet
			break
		}
	}

	// TER to other tablet
	_, err = clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("TabletExternallyReparented", newPrimary.Alias)
	require.NoError(t, err)

	checkReplication(t, clusterInstance, newPrimary, []*cluster.Vttablet{curPrimary}, 15*time.Second)
}

// 7. make instance A replicates from B and B from A, wait for repair
func TestCircularReplication(t *testing.T) {
	defer func() {
		logs, err := clusterInstance.VtorcProcess.GetLogs()
		log.Errorf("logs for vtorc - %s, error while reading logs - %v", logs, err)
	}()
	defer cluster.PanicHandler(t)
	setupVttabletsAndVtorc(t, 2, 0, nil, "test_config.json")
	keyspace := &clusterInstance.Keyspaces[0]
	shard0 := &keyspace.Shards[0]

	// find primary from topo
	primary := shardPrimaryTablet(t, clusterInstance, keyspace, shard0)
	assert.NotNil(t, primary, "should have elected a primary")

	var replica *cluster.Vttablet
	for _, tablet := range shard0.Vttablets {
		// we know we have only two tablets, so the "other" one must be the new primary
		if tablet.Alias != primary.Alias {
			replica = tablet
			break
		}
	}

	changeReplicationSourceCommands := fmt.Sprintf("RESET SLAVE;"+
		"CHANGE MASTER TO MASTER_HOST='%s', MASTER_PORT=%d, MASTER_USER='vt_repl', MASTER_AUTO_POSITION = 1;"+
		"START SLAVE;", replica.VttabletProcess.TabletHostname, replica.MySQLPort)

	_, err := runSQL(t, changeReplicationSourceCommands, primary, "")
	require.NoError(t, err)

	// wait for repair
	err = waitForReplicationToStop(t, primary)
	require.NoError(t, err)
	// check replication is setup correctly
	checkReplication(t, clusterInstance, primary, []*cluster.Vttablet{replica}, 10*time.Second)
}
