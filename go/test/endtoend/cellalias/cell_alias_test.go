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

This test cell aliases feature

We start with no aliases and assert that vtgates can't route to replicas/rondly tablets.
Then we add an alias, and these tablets should be routable
*/

package binlog

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/sharding"
	"vitess.io/vitess/go/vt/proto/topodata"
)

var (
	localCluster *cluster.LocalProcessCluster
	cell1        = "zone1"
	cell2        = "zone2"
	hostname     = "localhost"
	keyspaceName = "ks"
	tableName    = "test_table"
	sqlSchema    = `
					create table %s(
					id bigint(20) unsigned auto_increment,
					msg varchar(64),
					primary key (id),
					index by_msg (msg)
					) Engine=InnoDB
`
	commonTabletArg = []string{
		"-vreplication_healthcheck_topology_refresh", "1s",
		"-vreplication_healthcheck_retry_delay", "1s",
		"-vreplication_retry_delay", "1s",
		"-degraded_threshold", "5s",
		"-lock_tables_timeout", "5s",
		"-watch_replication_stream",
		"-enable_replication_reporter",
		"-serving_state_grace_period", "1s",
		"-binlog_player_protocol", "grpc",
		"-enable-autocommit",
	}
	vSchema = `
		{
		  "sharded": true,
		  "vindexes": {
			"hash_index": {
			  "type": "hash"
			}
		  },
		  "tables": {
			"%s": {
			   "column_vindexes": [
				{
				  "column": "id",
				  "name": "hash_index"
				}
			  ] 
			}
		  }
		}
`
	shard1Master  *cluster.Vttablet
	shard1Replica *cluster.Vttablet
	shard1Rdonly  *cluster.Vttablet
	shard2Master  *cluster.Vttablet
	shard2Replica *cluster.Vttablet
	shard2Rdonly  *cluster.Vttablet
)

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitcode, err := func() (int, error) {
		localCluster = cluster.NewCluster(cell1, hostname)
		defer localCluster.Teardown()
		localCluster.Keyspaces = append(localCluster.Keyspaces, cluster.Keyspace{
			Name: keyspaceName,
		})

		// Start topo server
		if err := localCluster.StartTopo(); err != nil {
			return 1, err
		}

		// Adding another cell in the same cluster
		err := localCluster.TopoProcess.ManageTopoDir("mkdir", "/vitess/"+cell2)
		if err != nil {
			return 1, err
		}
		err = localCluster.VtctlProcess.AddCellInfo(cell2)
		if err != nil {
			return 1, err
		}

		shard1Master = localCluster.NewVttabletInstance("master", 0, cell1)
		shard1Replica = localCluster.NewVttabletInstance("replica", 0, cell2)
		shard1Rdonly = localCluster.NewVttabletInstance("rdonly", 0, cell2)

		shard2Master = localCluster.NewVttabletInstance("master", 0, cell1)
		shard2Replica = localCluster.NewVttabletInstance("replica", 0, cell2)
		shard2Rdonly = localCluster.NewVttabletInstance("rdonly", 0, cell2)

		var mysqlProcs []*exec.Cmd
		for _, tablet := range []*cluster.Vttablet{shard1Master, shard1Replica, shard1Rdonly, shard2Master, shard2Replica, shard2Rdonly} {
			tablet.MysqlctlProcess = *cluster.MysqlCtlProcessInstance(tablet.TabletUID, tablet.MySQLPort, localCluster.TmpDirectory)
			tablet.VttabletProcess = cluster.VttabletProcessInstance(tablet.HTTPPort,
				tablet.GrpcPort,
				tablet.TabletUID,
				tablet.Cell,
				"",
				keyspaceName,
				localCluster.VtctldProcess.Port,
				tablet.Type,
				localCluster.TopoPort,
				hostname,
				localCluster.TmpDirectory,
				commonTabletArg,
				true,
			)
			tablet.VttabletProcess.SupportsBackup = true
			proc, err := tablet.MysqlctlProcess.StartProcess()
			if err != nil {
				return 1, err
			}
			mysqlProcs = append(mysqlProcs, proc)
		}
		for _, proc := range mysqlProcs {
			if err := proc.Wait(); err != nil {
				return 1, err
			}
		}

		if err := localCluster.VtctlProcess.CreateKeyspace(keyspaceName); err != nil {
			return 1, err
		}

		shard1 := cluster.Shard{
			Name:      "-80",
			Vttablets: []*cluster.Vttablet{shard1Master, shard1Replica, shard1Rdonly},
		}
		for idx := range shard1.Vttablets {
			shard1.Vttablets[idx].VttabletProcess.Shard = shard1.Name
		}
		localCluster.Keyspaces[0].Shards = append(localCluster.Keyspaces[0].Shards, shard1)

		shard2 := cluster.Shard{
			Name:      "80-",
			Vttablets: []*cluster.Vttablet{shard2Master, shard2Replica, shard2Rdonly},
		}
		for idx := range shard2.Vttablets {
			shard2.Vttablets[idx].VttabletProcess.Shard = shard2.Name
		}
		localCluster.Keyspaces[0].Shards = append(localCluster.Keyspaces[0].Shards, shard2)

		for _, tablet := range shard1.Vttablets {
			if err := tablet.VttabletProcess.Setup(); err != nil {
				return 1, err
			}
		}
		if err := localCluster.VtctlclientProcess.InitShardMaster(keyspaceName, shard1.Name, shard1Master.Cell, shard1Master.TabletUID); err != nil {
			return 1, err
		}

		// run a health check on source replica so it responds to discovery
		// (for binlog players) and on the source rdonlys (for workers)
		for _, tablet := range []string{shard1Replica.Alias, shard1Rdonly.Alias} {
			if err := localCluster.VtctlclientProcess.ExecuteCommand("RunHealthCheck", tablet); err != nil {
				return 1, err
			}
		}

		for _, tablet := range shard2.Vttablets {
			if err := tablet.VttabletProcess.Setup(); err != nil {
				return 1, err
			}
		}

		if err := localCluster.VtctlclientProcess.InitShardMaster(keyspaceName, shard2.Name, shard2Master.Cell, shard2Master.TabletUID); err != nil {
			return 1, err
		}

		if err := localCluster.VtctlclientProcess.ApplySchema(keyspaceName, fmt.Sprintf(sqlSchema, tableName)); err != nil {
			return 1, err
		}
		if err := localCluster.VtctlclientProcess.ApplyVSchema(keyspaceName, fmt.Sprintf(vSchema, tableName)); err != nil {
			return 1, err
		}

		_ = localCluster.VtctlclientProcess.ExecuteCommand("RebuildKeyspaceGraph", keyspaceName)

		return m.Run(), nil
	}()
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	} else {
		os.Exit(exitcode)
	}
}

func TestAlias(t *testing.T) {
	defer cluster.PanicHandler(t)

	insertInitialValues(t)
	defer deleteInitialValues(t)

	err := localCluster.VtctlclientProcess.ExecuteCommand("RebuildKeyspaceGraph", keyspaceName)
	require.NoError(t, err)
	shard1 := localCluster.Keyspaces[0].Shards[0]
	shard2 := localCluster.Keyspaces[0].Shards[1]
	allCells := fmt.Sprintf("%s,%s", cell1, cell2)

	expectedPartitions := map[topodata.TabletType][]string{}
	expectedPartitions[topodata.TabletType_MASTER] = []string{shard1.Name, shard2.Name}
	expectedPartitions[topodata.TabletType_REPLICA] = []string{shard1.Name, shard2.Name}
	expectedPartitions[topodata.TabletType_RDONLY] = []string{shard1.Name, shard2.Name}
	sharding.CheckSrvKeyspace(t, cell1, keyspaceName, "", 0, expectedPartitions, *localCluster)
	sharding.CheckSrvKeyspace(t, cell2, keyspaceName, "", 0, expectedPartitions, *localCluster)

	// Adds alias so vtgate can route to replica/rdonly tablets that are not in the same cell, but same alias
	err = localCluster.VtctlclientProcess.ExecuteCommand("AddCellsAlias",
		"-cells", allCells,
		"region_east_coast")
	require.NoError(t, err)
	err = localCluster.VtctlclientProcess.ExecuteCommand("UpdateCellsAlias",
		"-cells", allCells,
		"region_east_coast")
	require.NoError(t, err)

	vtgateInstance := localCluster.NewVtgateInstance()
	vtgateInstance.CellsToWatch = allCells
	vtgateInstance.TabletTypesToWait = "MASTER,REPLICA"
	// Use legacy gateway. There's a separate test for tabletgateway in go/test/endtoend/tabletgateway/cellalias/cell_alias_test.go
	vtgateInstance.GatewayImplementation = "discoverygateway"
	err = vtgateInstance.Setup()
	require.NoError(t, err)

	// Cluster teardown will not teardown vtgate because we are not
	// actually setting this on localCluster.VtgateInstance
	defer vtgateInstance.TearDown()

	waitTillAllTabletsAreHealthyInVtgate(t, *vtgateInstance, shard1.Name, shard2.Name)

	testQueriesOnTabletType(t, "master", vtgateInstance.GrpcPort, false)
	testQueriesOnTabletType(t, "replica", vtgateInstance.GrpcPort, false)
	testQueriesOnTabletType(t, "rdonly", vtgateInstance.GrpcPort, false)

	// now, delete the alias, so that if we run above assertions again, it will fail for replica,rdonly target type
	err = localCluster.VtctlclientProcess.ExecuteCommand("DeleteCellsAlias",
		"region_east_coast")
	require.NoError(t, err)

	// restarts the vtgate process
	vtgateInstance.TabletTypesToWait = "MASTER"
	err = vtgateInstance.TearDown()
	require.NoError(t, err)
	err = vtgateInstance.Setup()
	require.NoError(t, err)

	// since replica and rdonly tablets of all shards in cell2, the last 2 assertion is expected to fail
	testQueriesOnTabletType(t, "master", vtgateInstance.GrpcPort, false)
	testQueriesOnTabletType(t, "replica", vtgateInstance.GrpcPort, true)
	testQueriesOnTabletType(t, "rdonly", vtgateInstance.GrpcPort, true)

}

func TestAddAliasWhileVtgateUp(t *testing.T) {
	defer cluster.PanicHandler(t)

	insertInitialValues(t)
	defer deleteInitialValues(t)

	err := localCluster.VtctlclientProcess.ExecuteCommand("RebuildKeyspaceGraph", keyspaceName)
	require.NoError(t, err)
	shard1 := localCluster.Keyspaces[0].Shards[0]
	shard2 := localCluster.Keyspaces[0].Shards[1]
	allCells := fmt.Sprintf("%s,%s", cell1, cell2)

	expectedPartitions := map[topodata.TabletType][]string{}
	expectedPartitions[topodata.TabletType_MASTER] = []string{shard1.Name, shard2.Name}
	expectedPartitions[topodata.TabletType_REPLICA] = []string{shard1.Name, shard2.Name}
	expectedPartitions[topodata.TabletType_RDONLY] = []string{shard1.Name, shard2.Name}
	sharding.CheckSrvKeyspace(t, cell1, keyspaceName, "", 0, expectedPartitions, *localCluster)
	sharding.CheckSrvKeyspace(t, cell2, keyspaceName, "", 0, expectedPartitions, *localCluster)

	vtgateInstance := localCluster.NewVtgateInstance()
	vtgateInstance.CellsToWatch = allCells
	// only MASTER is in vtgate's "cell", other tablet types are not visible because they are in the other cell
	vtgateInstance.TabletTypesToWait = "MASTER"
	err = vtgateInstance.Setup()
	require.NoError(t, err)
	defer vtgateInstance.TearDown()

	// since replica and rdonly tablets of all shards in cell2, the last 2 assertion is expected to fail
	testQueriesOnTabletType(t, "master", vtgateInstance.GrpcPort, false)
	testQueriesOnTabletType(t, "replica", vtgateInstance.GrpcPort, true)
	testQueriesOnTabletType(t, "rdonly", vtgateInstance.GrpcPort, true)

	// Adds alias so vtgate can route to replica/rdonly tablets that are not in the same cell, but same alias
	err = localCluster.VtctlclientProcess.ExecuteCommand("AddCellsAlias",
		"-cells", allCells,
		"region_east_coast")
	require.NoError(t, err)

	testQueriesOnTabletType(t, "master", vtgateInstance.GrpcPort, false)
	// TODO(deepthi) change the following to shouldFail:false when fixing https://github.com/vitessio/vitess/issues/5911
	testQueriesOnTabletType(t, "replica", vtgateInstance.GrpcPort, true)
	testQueriesOnTabletType(t, "rdonly", vtgateInstance.GrpcPort, true)

}

func waitTillAllTabletsAreHealthyInVtgate(t *testing.T, vtgateInstance cluster.VtgateProcess, shards ...string) {
	for _, shard := range shards {
		err := vtgateInstance.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", keyspaceName, shard), 1)
		require.NoError(t, err)
		err = vtgateInstance.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.replica", keyspaceName, shard), 1)
		require.NoError(t, err)
		err = vtgateInstance.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.rdonly", keyspaceName, shard), 1)
		require.NoError(t, err)
	}
}

func testQueriesOnTabletType(t *testing.T, tabletType string, vtgateGrpcPort int, shouldFail bool) {
	output, err := localCluster.VtctlProcess.ExecuteCommandWithOutput("VtGateExecute", "-json",
		"-server", fmt.Sprintf("%s:%d", localCluster.Hostname, vtgateGrpcPort),
		"-target", "@"+tabletType,
		fmt.Sprintf(`select * from %s`, tableName))
	if shouldFail {
		require.Error(t, err)
		return
	}
	require.NoError(t, err)
	var result sqltypes.Result

	err = json.Unmarshal([]byte(output), &result)
	require.NoError(t, err)
	assert.Equal(t, len(result.Rows), 3)
}

func insertInitialValues(t *testing.T) {
	sharding.ExecuteOnTablet(t,
		fmt.Sprintf(sharding.InsertTabletTemplateKsID, tableName, 1, "msg1", 1),
		*shard1Master,
		keyspaceName,
		false)

	sharding.ExecuteOnTablet(t,
		fmt.Sprintf(sharding.InsertTabletTemplateKsID, tableName, 2, "msg2", 2),
		*shard1Master,
		keyspaceName,
		false)

	sharding.ExecuteOnTablet(t,
		fmt.Sprintf(sharding.InsertTabletTemplateKsID, tableName, 4, "msg4", 4),
		*shard2Master,
		keyspaceName,
		false)
}

func deleteInitialValues(t *testing.T) {
	sharding.ExecuteOnTablet(t,
		fmt.Sprintf("delete from %s where id = %v", tableName, 1),
		*shard1Master,
		keyspaceName,
		false)

	sharding.ExecuteOnTablet(t,
		fmt.Sprintf("delete from %s where id = %v", tableName, 2),
		*shard1Master,
		keyspaceName,
		false)

	sharding.ExecuteOnTablet(t,
		fmt.Sprintf("delete from %s where id = %v", tableName, 4),
		*shard2Master,
		keyspaceName,
		false)
}
