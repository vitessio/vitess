/*
Copyright 2019 The Vitess Authors.

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

package shardedrecovery

import (
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"testing"

	"vitess.io/vitess/go/test/endtoend/recovery"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	_ "vitess.io/vitess/go/vt/vtgate/grpcvtgateconn"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"
)

var (
	master   *cluster.Vttablet
	replica1 *cluster.Vttablet
	rdOnly   *cluster.Vttablet
	replica2 *cluster.Vttablet
	replica3 *cluster.Vttablet

	shard0Master  *cluster.Vttablet
	shard0Replica *cluster.Vttablet
	shard0RdOnly  *cluster.Vttablet

	shard1Master  *cluster.Vttablet
	shard1Replica *cluster.Vttablet
	shard1RdOnly  *cluster.Vttablet

	localCluster    *cluster.LocalProcessCluster
	cell            = cluster.DefaultCell
	hostname        = "localhost"
	keyspaceName    = "test_keyspace"
	shardName       = "0"
	shard0Name      = "-80"
	shard1Name      = "80-"
	commonTabletArg = []string{
		"-vreplication_healthcheck_topology_refresh", "1s",
		"-vreplication_healthcheck_retry_delay", "1s",
		"-vreplication_retry_delay", "1s",
		"-degraded_threshold", "5s",
		"-lock_tables_timeout", "5s",
		"-watch_replication_stream",
		"-serving_state_grace_period", "1s"}
	recoveryKS   = "recovery_keyspace"
	vtInsertTest = `create table vt_insert_test (
					  id bigint auto_increment,
					  msg varchar(64),
					  primary key (id)
					  ) Engine=InnoDB`
	vSchema = `{
    "sharded": true,
    "vindexes": {
      "hash": {
        "type": "hash"
      }
    },
    "tables": {
        "vt_insert_test": {
        "column_vindexes": [
          {
            "column": "id",
            "name": "hash"
          }
        ]
      }
    }
}`
)

// Test recovery from backup flow.

// test_recovery will:
// - create a shard with master and replica1 only
// - run InitShardMaster
// - insert some data
// - take a backup
// - insert more data on the master
// - perform a resharding
// - create a recovery keyspace
// - bring up tablet_replica2 in the new keyspace
// - check that new tablet does not have data created after backup
// - check that vtgate queries work correctly

func TestUnShardedRecoveryAfterSharding(t *testing.T) {
	defer cluster.PanicHandler(t)
	_, err := initializeCluster(t)
	defer localCluster.Teardown()
	require.NoError(t, err)
	err = localCluster.VtctlclientProcess.ApplySchema(keyspaceName, vtInsertTest)
	require.NoError(t, err)
	recovery.InsertData(t, master, 1, keyspaceName)
	cluster.VerifyRowsInTablet(t, replica1, keyspaceName, 1)

	// insert more data on the master
	recovery.InsertData(t, master, 2, keyspaceName)

	// backup the replica
	err = localCluster.VtctlclientProcess.ExecuteCommand("Backup", replica1.Alias)
	require.NoError(t, err)

	// check that the backup shows up in the listing
	output, err := localCluster.ListBackups("test_keyspace/0")
	require.NoError(t, err)
	assert.Equal(t, 1, len(output))
	assert.True(t, strings.HasSuffix(output[0], replica1.Alias))

	// insert more data on the master
	recovery.InsertData(t, master, 3, keyspaceName)

	err = localCluster.VtctlclientProcess.ApplyVSchema(keyspaceName, vSchema)
	require.NoError(t, err)

	// create the split shards
	for _, tablet := range []*cluster.Vttablet{shard0Master, shard0Replica, shard0RdOnly, shard1Master, shard1Replica, shard1RdOnly} {
		tablet.VttabletProcess.ExtraArgs = []string{"-binlog_use_v3_resharding_mode=true"}
		err = tablet.VttabletProcess.Setup()
		require.NoError(t, err)
	}

	err = localCluster.VtctlProcess.ExecuteCommand("InitShardMaster", "-force", "test_keyspace/-80", shard0Master.Alias)
	require.NoError(t, err)

	err = localCluster.VtctlProcess.ExecuteCommand("InitShardMaster", "-force", "test_keyspace/80-", shard1Master.Alias)
	require.NoError(t, err)

	shardedTablets := []*cluster.Vttablet{shard0Master, shard0Replica, shard0RdOnly, shard1Master, shard1Replica, shard1RdOnly}

	for _, tablet := range shardedTablets {
		_ = tablet.VttabletProcess.WaitForTabletType("SERVING")
		require.NoError(t, err)
	}

	for _, tablet := range shardedTablets {
		assert.Equal(t, tablet.VttabletProcess.GetTabletStatus(), "SERVING")
	}

	// we need to create the schema, and the worker will do data copying
	for _, keyspaceShard := range []string{"test_keyspace/-80", "test_keyspace/80-"} {
		err = localCluster.VtctlclientProcess.ExecuteCommand("CopySchemaShard", "test_keyspace/0", keyspaceShard)
		require.NoError(t, err)
	}

	err = localCluster.VtctlclientProcess.ExecuteCommand("SplitClone", "test_keyspace", "0", "-80,80-")
	require.NoError(t, err)

	err = localCluster.VtctlclientProcess.ExecuteCommand("MigrateServedTypes", "test_keyspace/0", "rdonly")
	require.NoError(t, err)

	err = localCluster.VtctlclientProcess.ExecuteCommand("MigrateServedTypes", "test_keyspace/0", "replica")
	require.NoError(t, err)

	// then serve master from the split shards
	err = localCluster.VtctlclientProcess.ExecuteCommand("MigrateServedTypes", "test_keyspace/0", "master")
	require.NoError(t, err)

	// remove the original tablets in the original shard
	removeTablets(t, []*cluster.Vttablet{master, replica1, rdOnly})

	for _, tablet := range []*cluster.Vttablet{replica1, rdOnly} {
		err = localCluster.VtctlclientProcess.ExecuteCommand("DeleteTablet", tablet.Alias)
		require.NoError(t, err)
	}
	err = localCluster.VtctlclientProcess.ExecuteCommand("DeleteTablet", "-allow_master", master.Alias)
	require.NoError(t, err)

	// rebuild the serving graph, all mentions of the old shards should be gone
	err = localCluster.VtctlclientProcess.ExecuteCommand("RebuildKeyspaceGraph", "test_keyspace")
	require.NoError(t, err)

	// delete the original shard
	err = localCluster.VtctlclientProcess.ExecuteCommand("DeleteShard", "test_keyspace/0")
	require.NoError(t, err)

	// now bring up the recovery keyspace and a tablet, letting it restore from backup.
	recovery.RestoreTablet(t, localCluster, replica2, recoveryKS, "0", keyspaceName, commonTabletArg)

	// check the new replica does not have the data
	cluster.VerifyRowsInTablet(t, replica2, keyspaceName, 2)

	vtgateInstance := localCluster.NewVtgateInstance()
	vtgateInstance.TabletTypesToWait = "REPLICA"
	err = vtgateInstance.Setup()
	localCluster.VtgateGrpcPort = vtgateInstance.GrpcPort
	require.NoError(t, err)
	err = vtgateInstance.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", keyspaceName, shard0Name), 1)
	require.NoError(t, err)
	err = vtgateInstance.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.replica", keyspaceName, shard1Name), 1)
	require.NoError(t, err)
	err = vtgateInstance.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", keyspaceName, shard0Name), 1)
	require.NoError(t, err)
	err = vtgateInstance.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.replica", keyspaceName, shard1Name), 1)
	require.NoError(t, err)
	err = vtgateInstance.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.replica", recoveryKS, shardName), 1)
	require.NoError(t, err)

	// Build vtgate grpc connection
	// check that vtgate doesn't route queries to new tablet
	grpcAddress := fmt.Sprintf("%s:%d", localCluster.Hostname, localCluster.VtgateGrpcPort)
	vtgateConn, err := vtgateconn.Dial(context.Background(), grpcAddress)
	require.NoError(t, err)

	session := vtgateConn.Session("@replica", nil)
	recovery.VerifyQueriesUsingVtgate(t, session, "select count(*) from vt_insert_test", "INT64(3)")

	// check that new tablet is accessible by using ks.table
	recovery.VerifyQueriesUsingVtgate(t, session, "select count(*) from recovery_keyspace.vt_insert_test", "INT64(2)")

	// check that new tablet is accessible with 'use ks'
	cluster.ExecuteQueriesUsingVtgate(t, session, "use `recovery_keyspace@replica`")
	recovery.VerifyQueriesUsingVtgate(t, session, "select count(*) from vt_insert_test", "INT64(2)")

	// check that new tablet is accessible with `use ks:shard`
	cluster.ExecuteQueriesUsingVtgate(t, session, "use `recovery_keyspace:0@replica`")
	recovery.VerifyQueriesUsingVtgate(t, session, "select count(*) from vt_insert_test", "INT64(2)")

	vtgateConn.Close()
	err = vtgateInstance.TearDown()
	require.NoError(t, err)
}

// Test recovery from backup flow.

// test_recovery will:
// - create a shard with master and replica1 only
// - run InitShardMaster
// - insert some data
// - perform a resharding
// - take a backup of both new shards
// - insert more data on the masters of both shards
// - create a recovery keyspace
// - bring up tablet_replica2 and tablet_replica3 in the new keyspace
// - check that new tablets do not have data created after backup
// - check that vtgate queries work correctly

func TestShardedRecovery(t *testing.T) {

	defer cluster.PanicHandler(t)
	_, err := initializeCluster(t)
	defer localCluster.Teardown()
	require.NoError(t, err)
	err = localCluster.VtctlclientProcess.ApplySchema(keyspaceName, vtInsertTest)
	require.NoError(t, err)
	recovery.InsertData(t, master, 1, keyspaceName)

	cluster.VerifyRowsInTablet(t, replica1, keyspaceName, 1)

	// insert more data on the master
	recovery.InsertData(t, master, 4, keyspaceName)

	err = localCluster.VtctlclientProcess.ApplyVSchema(keyspaceName, vSchema)
	require.NoError(t, err)

	// create the split shards
	shardedTablets := []*cluster.Vttablet{shard0Master, shard0Replica, shard0RdOnly, shard1Master, shard1Replica, shard1RdOnly}
	for _, tablet := range shardedTablets {
		tablet.VttabletProcess.ExtraArgs = []string{"-binlog_use_v3_resharding_mode=true"}
		err = tablet.VttabletProcess.Setup()
		require.NoError(t, err)
	}

	err = localCluster.VtctlProcess.ExecuteCommand("InitShardMaster", "-force", "test_keyspace/-80", shard0Master.Alias)
	require.NoError(t, err)

	err = localCluster.VtctlProcess.ExecuteCommand("InitShardMaster", "-force", "test_keyspace/80-", shard1Master.Alias)
	require.NoError(t, err)

	for _, tablet := range shardedTablets {
		_ = tablet.VttabletProcess.WaitForTabletType("SERVING")
		require.NoError(t, err)
	}

	// we need to create the schema, and the worker will do data copying
	for _, keyspaceShard := range []string{"test_keyspace/-80", "test_keyspace/80-"} {
		err = localCluster.VtctlclientProcess.ExecuteCommand("CopySchemaShard", "test_keyspace/0", keyspaceShard)
		require.NoError(t, err)
	}

	err = localCluster.VtctlclientProcess.ExecuteCommand("SplitClone", "test_keyspace", "0", "-80,80-")
	require.NoError(t, err)

	err = localCluster.VtctlclientProcess.ExecuteCommand("MigrateServedTypes", "test_keyspace/0", "rdonly")
	require.NoError(t, err)

	err = localCluster.VtctlclientProcess.ExecuteCommand("MigrateServedTypes", "test_keyspace/0", "replica")
	require.NoError(t, err)

	// then serve master from the split shards
	err = localCluster.VtctlclientProcess.ExecuteCommand("MigrateServedTypes", "test_keyspace/0", "master")
	require.NoError(t, err)

	// remove the original tablets in the original shard
	removeTablets(t, []*cluster.Vttablet{master, replica1, rdOnly})

	for _, tablet := range []*cluster.Vttablet{replica1, rdOnly} {
		err = localCluster.VtctlclientProcess.ExecuteCommand("DeleteTablet", tablet.Alias)
		require.NoError(t, err)
	}
	err = localCluster.VtctlclientProcess.ExecuteCommand("DeleteTablet", "-allow_master", master.Alias)
	require.NoError(t, err)

	// rebuild the serving graph, all mentions of the old shards should be gone
	err = localCluster.VtctlclientProcess.ExecuteCommand("RebuildKeyspaceGraph", "test_keyspace")
	require.NoError(t, err)

	// delete the original shard
	err = localCluster.VtctlclientProcess.ExecuteCommand("DeleteShard", "test_keyspace/0")
	require.NoError(t, err)

	qr, err := shard0Master.VttabletProcess.QueryTablet("select count(*) from vt_insert_test", keyspaceName, true)
	require.NoError(t, err)
	shard0CountStr := fmt.Sprintf("%s", qr.Rows[0][0].ToBytes())
	shard0Count, err := strconv.Atoi(shard0CountStr)
	require.NoError(t, err)
	var shard0TestID string
	if shard0Count > 0 {
		qr, err := shard0Master.VttabletProcess.QueryTablet("select id from vt_insert_test", keyspaceName, true)
		require.NoError(t, err)
		shard0TestID = fmt.Sprintf("%s", qr.Rows[0][0].ToBytes())
		require.NoError(t, err)
	}

	qr, err = shard1Master.VttabletProcess.QueryTablet("select count(*) from vt_insert_test", keyspaceName, true)
	require.NoError(t, err)
	shard1CountStr := fmt.Sprintf("%s", qr.Rows[0][0].ToBytes())
	shard1Count, err := strconv.Atoi(shard1CountStr)
	require.NoError(t, err)
	var shard1TestID string
	if shard1Count > 0 {
		qr, err := shard1Master.VttabletProcess.QueryTablet("select id from vt_insert_test", keyspaceName, true)
		require.NoError(t, err)
		shard1TestID = fmt.Sprintf("%s", qr.Rows[0][0].ToBytes())
		require.NoError(t, err)
	}

	// backup the new shards
	err = localCluster.VtctlclientProcess.ExecuteCommand("Backup", shard0Replica.Alias)
	require.NoError(t, err)
	err = localCluster.VtctlclientProcess.ExecuteCommand("Backup", shard1Replica.Alias)
	require.NoError(t, err)

	// check that the backup shows up in the listing
	output, err := localCluster.ListBackups("test_keyspace/-80")
	require.NoError(t, err)
	assert.Equal(t, 1, len(output))
	assert.True(t, strings.HasSuffix(output[0], shard0Replica.Alias))

	output, err = localCluster.ListBackups("test_keyspace/80-")
	require.NoError(t, err)
	assert.Equal(t, 1, len(output))
	assert.True(t, strings.HasSuffix(output[0], shard1Replica.Alias))

	vtgateInstance := localCluster.NewVtgateInstance()
	vtgateInstance.TabletTypesToWait = "MASTER"
	err = vtgateInstance.Setup()
	localCluster.VtgateGrpcPort = vtgateInstance.GrpcPort
	require.NoError(t, err)
	err = vtgateInstance.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", keyspaceName, shard0Name), 1)
	require.NoError(t, err)
	err = vtgateInstance.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", keyspaceName, shard1Name), 1)
	require.NoError(t, err)

	// Build vtgate grpc connection
	// check that vtgate doesn't route queries to new tablet
	grpcAddress := fmt.Sprintf("%s:%d", localCluster.Hostname, localCluster.VtgateGrpcPort)
	vtgateConn, err := vtgateconn.Dial(context.Background(), grpcAddress)
	require.NoError(t, err)
	session := vtgateConn.Session("@master", nil)
	cluster.ExecuteQueriesUsingVtgate(t, session, "insert into vt_insert_test (id, msg) values (2,'test 2')")
	cluster.ExecuteQueriesUsingVtgate(t, session, "insert into vt_insert_test (id, msg) values (3,'test 3')")

	vtgateConn.Close()
	err = vtgateInstance.TearDown()
	require.NoError(t, err)

	// now bring up the recovery keyspace and 2 tablets, letting it restore from backup.
	recovery.RestoreTablet(t, localCluster, replica2, recoveryKS, "-80", keyspaceName, commonTabletArg)
	recovery.RestoreTablet(t, localCluster, replica3, recoveryKS, "80-", keyspaceName, commonTabletArg)

	// check the new replicas have the correct number of rows
	cluster.VerifyRowsInTablet(t, replica2, keyspaceName, shard0Count)
	cluster.VerifyRowsInTablet(t, replica3, keyspaceName, shard1Count)

	// start vtgate
	vtgateInstance = localCluster.NewVtgateInstance()
	vtgateInstance.TabletTypesToWait = "REPLICA"
	err = vtgateInstance.Setup()
	localCluster.VtgateGrpcPort = vtgateInstance.GrpcPort
	require.NoError(t, err)
	err = vtgateInstance.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", keyspaceName, shard0Name), 1)
	require.NoError(t, err)
	err = vtgateInstance.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.replica", keyspaceName, shard1Name), 1)
	require.NoError(t, err)
	err = vtgateInstance.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", keyspaceName, shard0Name), 1)
	require.NoError(t, err)
	err = vtgateInstance.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.replica", keyspaceName, shard1Name), 1)
	require.NoError(t, err)
	err = vtgateInstance.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.replica", recoveryKS, shard0Name), 1)
	require.NoError(t, err)
	err = vtgateInstance.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.replica", recoveryKS, shard1Name), 1)
	require.NoError(t, err)

	// check that vtgate doesn't route queries to new tablet
	grpcAddress = fmt.Sprintf("%s:%d", localCluster.Hostname, localCluster.VtgateGrpcPort)
	vtgateConn, err = vtgateconn.Dial(context.Background(), grpcAddress)
	require.NoError(t, err)
	session = vtgateConn.Session("@replica", nil)
	recovery.VerifyQueriesUsingVtgate(t, session, "select count(*) from vt_insert_test", "INT64(4)")

	// check that new keyspace is accessible by using ks.table
	recovery.VerifyQueriesUsingVtgate(t, session, "select count(*) from recovery_keyspace.vt_insert_test", "INT64(2)")

	// check that new keyspace is accessible with 'use ks'
	cluster.ExecuteQueriesUsingVtgate(t, session, "use recovery_keyspace@replica")
	recovery.VerifyQueriesUsingVtgate(t, session, "select count(*) from vt_insert_test", "INT64(2)")

	// check that new tablet is accessible with use `ks:shard`
	cluster.ExecuteQueriesUsingVtgate(t, session, "use `recovery_keyspace:-80@replica`")
	recovery.VerifyQueriesUsingVtgate(t, session, "select count(*) from vt_insert_test", "INT64("+shard0CountStr+")")
	recovery.VerifyQueriesUsingVtgate(t, session, "select id from vt_insert_test", "INT64("+shard0TestID+")")

	cluster.ExecuteQueriesUsingVtgate(t, session, "use `recovery_keyspace:80-@replica`")
	recovery.VerifyQueriesUsingVtgate(t, session, "select count(*) from vt_insert_test", "INT64("+shard1CountStr+")")
	recovery.VerifyQueriesUsingVtgate(t, session, "select id from vt_insert_test", "INT64("+shard1TestID+")")

	vtgateConn.Close()
	err = vtgateInstance.TearDown()
	require.NoError(t, err)
}

func removeTablets(t *testing.T, tablets []*cluster.Vttablet) {
	var mysqlProcs []*exec.Cmd
	for _, tablet := range tablets {
		proc, _ := tablet.MysqlctlProcess.StopProcess()
		mysqlProcs = append(mysqlProcs, proc)
		tablet.VttabletProcess.TearDown()
	}
	for _, proc := range mysqlProcs {
		err := proc.Wait()
		require.NoError(t, err)
	}
}

func initializeCluster(t *testing.T) (int, error) {

	localCluster = cluster.NewCluster(cell, hostname)

	// Start topo server
	err := localCluster.StartTopo()
	if err != nil {
		return 1, err
	}
	// Start keyspace
	keyspace := &cluster.Keyspace{
		Name: keyspaceName,
	}
	localCluster.Keyspaces = append(localCluster.Keyspaces, *keyspace)

	// TODO : handle shards properly
	shard := &cluster.Shard{
		Name: shardName,
	}
	shard0 := &cluster.Shard{
		Name: shard0Name,
	}
	shard1 := &cluster.Shard{
		Name: shard1Name,
	}

	// Defining all the tablets
	master = localCluster.NewVttabletInstance("replica", 0, "")
	replica1 = localCluster.NewVttabletInstance("replica", 0, "")
	rdOnly = localCluster.NewVttabletInstance("rdonly", 0, "")
	replica2 = localCluster.NewVttabletInstance("replica", 0, "")
	replica3 = localCluster.NewVttabletInstance("replica", 0, "")
	shard0Master = localCluster.NewVttabletInstance("replica", 0, "")
	shard0Replica = localCluster.NewVttabletInstance("replica", 0, "")
	shard0RdOnly = localCluster.NewVttabletInstance("rdonly", 0, "")
	shard1Master = localCluster.NewVttabletInstance("replica", 0, "")
	shard1Replica = localCluster.NewVttabletInstance("replica", 0, "")
	shard1RdOnly = localCluster.NewVttabletInstance("rdonly", 0, "")

	shard.Vttablets = []*cluster.Vttablet{master, replica1, rdOnly, replica2, replica3}
	shard0.Vttablets = []*cluster.Vttablet{shard0Master, shard0Replica, shard0RdOnly}
	shard1.Vttablets = []*cluster.Vttablet{shard1Master, shard1Replica, shard1RdOnly}

	localCluster.VtTabletExtraArgs = append(localCluster.VtTabletExtraArgs, commonTabletArg...)
	localCluster.VtTabletExtraArgs = append(localCluster.VtTabletExtraArgs, "-restore_from_backup", "-enable_semi_sync")

	err = localCluster.SetupCluster(keyspace, []cluster.Shard{*shard, *shard0, *shard1})
	require.NoError(t, err)
	// Start MySql
	var mysqlCtlProcessList []*exec.Cmd
	for _, shard := range localCluster.Keyspaces[0].Shards {
		for _, tablet := range shard.Vttablets {
			if proc, err := tablet.MysqlctlProcess.StartProcess(); err != nil {
				t.Fatal(err)
			} else {
				mysqlCtlProcessList = append(mysqlCtlProcessList, proc)
			}
		}
	}

	// Wait for mysql processes to start
	for _, proc := range mysqlCtlProcessList {
		if err := proc.Wait(); err != nil {
			t.Fatal(err)
		}
	}

	for _, tablet := range []cluster.Vttablet{*master, *replica1, *rdOnly} {
		if err = tablet.VttabletProcess.CreateDB(keyspaceName); err != nil {
			return 1, err
		}
		if err = tablet.VttabletProcess.Setup(); err != nil {
			return 1, err
		}
	}

	if err = localCluster.VtctlclientProcess.InitShardMaster(keyspaceName, shard.Name, cell, master.TabletUID); err != nil {
		return 1, err
	}

	return 0, nil
}
