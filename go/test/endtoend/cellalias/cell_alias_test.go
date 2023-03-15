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
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
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
		"--vreplication_healthcheck_topology_refresh", "1s",
		"--vreplication_healthcheck_retry_delay", "1s",
		"--vreplication_retry_delay", "1s",
		"--degraded_threshold", "5s",
		"--lock_tables_timeout", "5s",
		"--watch_replication_stream",
		"--enable_replication_reporter",
		"--serving_state_grace_period", "1s",
		"--binlog_player_protocol", "grpc",
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
	shard1Primary *cluster.Vttablet
	shard1Replica *cluster.Vttablet
	shard1Rdonly  *cluster.Vttablet
	shard2Primary *cluster.Vttablet
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

		vtctldClientProcess := cluster.VtctldClientProcessInstance("localhost", localCluster.VtctldProcess.GrpcPort, localCluster.TmpDirectory)
		_, err = vtctldClientProcess.ExecuteCommandWithOutput("CreateKeyspace", keyspaceName, "--durability-policy=semi_sync")
		if err != nil {
			return 1, err
		}

		shard1Primary = localCluster.NewVttabletInstance("primary", 0, cell1)
		shard1Replica = localCluster.NewVttabletInstance("replica", 0, cell2)
		shard1Rdonly = localCluster.NewVttabletInstance("rdonly", 0, cell2)

		shard2Primary = localCluster.NewVttabletInstance("primary", 0, cell1)
		shard2Replica = localCluster.NewVttabletInstance("replica", 0, cell2)
		shard2Rdonly = localCluster.NewVttabletInstance("rdonly", 0, cell2)

		var mysqlProcs []*exec.Cmd
		for _, tablet := range []*cluster.Vttablet{shard1Primary, shard1Replica, shard1Rdonly, shard2Primary, shard2Replica, shard2Rdonly} {
			mysqlctlProcess, err := cluster.MysqlCtlProcessInstance(tablet.TabletUID, tablet.MySQLPort, localCluster.TmpDirectory)
			if err != nil {
				return 1, err
			}
			tablet.MysqlctlProcess = *mysqlctlProcess
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
				localCluster.DefaultCharset,
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

		shard1 := cluster.Shard{
			Name:      "-80",
			Vttablets: []*cluster.Vttablet{shard1Primary, shard1Replica, shard1Rdonly},
		}
		for idx := range shard1.Vttablets {
			shard1.Vttablets[idx].VttabletProcess.Shard = shard1.Name
		}
		localCluster.Keyspaces[0].Shards = append(localCluster.Keyspaces[0].Shards, shard1)

		shard2 := cluster.Shard{
			Name:      "80-",
			Vttablets: []*cluster.Vttablet{shard2Primary, shard2Replica, shard2Rdonly},
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
		if err := localCluster.VtctlclientProcess.InitializeShard(keyspaceName, shard1.Name, shard1Primary.Cell, shard1Primary.TabletUID); err != nil {
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

		if err := localCluster.VtctlclientProcess.InitializeShard(keyspaceName, shard2.Name, shard2Primary.Cell, shard2Primary.TabletUID); err != nil {
			return 1, err
		}

		if err := localCluster.StartVTOrc(keyspaceName); err != nil {
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
	expectedPartitions[topodata.TabletType_PRIMARY] = []string{shard1.Name, shard2.Name}
	expectedPartitions[topodata.TabletType_REPLICA] = []string{shard1.Name, shard2.Name}
	expectedPartitions[topodata.TabletType_RDONLY] = []string{shard1.Name, shard2.Name}
	cluster.CheckSrvKeyspace(t, cell1, keyspaceName, expectedPartitions, *localCluster)
	cluster.CheckSrvKeyspace(t, cell2, keyspaceName, expectedPartitions, *localCluster)

	// Adds alias so vtgate can route to replica/rdonly tablets that are not in the same cell, but same alias
	err = localCluster.VtctlclientProcess.ExecuteCommand("AddCellsAlias", "--",
		"--cells", allCells,
		"region_east_coast")
	require.NoError(t, err)
	err = localCluster.VtctlclientProcess.ExecuteCommand("UpdateCellsAlias", "--",
		"--cells", allCells,
		"region_east_coast")
	require.NoError(t, err)

	vtgateInstance := localCluster.NewVtgateInstance()
	vtgateInstance.CellsToWatch = allCells
	vtgateInstance.TabletTypesToWait = "PRIMARY,REPLICA"
	err = vtgateInstance.Setup()
	require.NoError(t, err)

	// Cluster teardown will not teardown vtgate because we are not
	// actually setting this on localCluster.VtgateInstance
	defer vtgateInstance.TearDown()

	waitTillAllTabletsAreHealthyInVtgate(t, *vtgateInstance, shard1.Name, shard2.Name)

	testQueriesOnTabletType(t, "primary", vtgateInstance.GrpcPort, false)
	testQueriesOnTabletType(t, "replica", vtgateInstance.GrpcPort, false)
	testQueriesOnTabletType(t, "rdonly", vtgateInstance.GrpcPort, false)

	// now, delete the alias, so that if we run above assertions again, it will fail for replica,rdonly target type
	err = localCluster.VtctlclientProcess.ExecuteCommand("DeleteCellsAlias",
		"region_east_coast")
	require.NoError(t, err)

	// restarts the vtgate process
	vtgateInstance.TabletTypesToWait = "PRIMARY"
	err = vtgateInstance.TearDown()
	require.NoError(t, err)
	err = vtgateInstance.Setup()
	require.NoError(t, err)

	// since replica and rdonly tablets of all shards in cell2, the last 2 assertion is expected to fail
	testQueriesOnTabletType(t, "primary", vtgateInstance.GrpcPort, false)
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
	expectedPartitions[topodata.TabletType_PRIMARY] = []string{shard1.Name, shard2.Name}
	expectedPartitions[topodata.TabletType_REPLICA] = []string{shard1.Name, shard2.Name}
	expectedPartitions[topodata.TabletType_RDONLY] = []string{shard1.Name, shard2.Name}
	cluster.CheckSrvKeyspace(t, cell1, keyspaceName, expectedPartitions, *localCluster)
	cluster.CheckSrvKeyspace(t, cell2, keyspaceName, expectedPartitions, *localCluster)

	vtgateInstance := localCluster.NewVtgateInstance()
	vtgateInstance.CellsToWatch = allCells
	// only primary is in vtgate's "cell", other tablet types are not visible because they are in the other cell
	vtgateInstance.TabletTypesToWait = "PRIMARY"
	err = vtgateInstance.Setup()
	require.NoError(t, err)
	defer vtgateInstance.TearDown()

	// since replica and rdonly tablets of all shards in cell2, the last 2 assertion is expected to fail
	testQueriesOnTabletType(t, "primary", vtgateInstance.GrpcPort, false)
	testQueriesOnTabletType(t, "replica", vtgateInstance.GrpcPort, true)
	testQueriesOnTabletType(t, "rdonly", vtgateInstance.GrpcPort, true)

	// Adds alias so vtgate can route to replica/rdonly tablets that are not in the same cell, but same alias
	err = localCluster.VtctlclientProcess.ExecuteCommand("AddCellsAlias", "--",
		"--cells", allCells,
		"region_east_coast")
	require.NoError(t, err)

	testQueriesOnTabletType(t, "primary", vtgateInstance.GrpcPort, false)
	// TODO(deepthi) change the following to shouldFail:false when fixing https://github.com/vitessio/vitess/issues/5911
	testQueriesOnTabletType(t, "replica", vtgateInstance.GrpcPort, true)
	testQueriesOnTabletType(t, "rdonly", vtgateInstance.GrpcPort, true)

}

func waitTillAllTabletsAreHealthyInVtgate(t *testing.T, vtgateInstance cluster.VtgateProcess, shards ...string) {
	for _, shard := range shards {
		err := vtgateInstance.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.primary", keyspaceName, shard), 1)
		require.Nil(t, err)
		err = vtgateInstance.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.replica", keyspaceName, shard), 1)
		require.Nil(t, err)
		err = vtgateInstance.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.rdonly", keyspaceName, shard), 1)
		require.Nil(t, err)
	}
}

func testQueriesOnTabletType(t *testing.T, tabletType string, vtgateGrpcPort int, shouldFail bool) {
	qr, err := localCluster.ExecOnVTGate(context.Background(),
		fmt.Sprintf("%s:%d", localCluster.Hostname, vtgateGrpcPort),
		"@"+tabletType,
		fmt.Sprintf(`select * from %s`, tableName), nil, nil,
	)
	if shouldFail {
		require.Error(t, err)
		return
	}
	assert.Equal(t, len(qr.Rows), 3)
}

func insertInitialValues(t *testing.T) {
	cluster.ExecuteOnTablet(t,
		fmt.Sprintf(cluster.InsertTabletTemplateKsID, tableName, 1, "msg1", 1),
		*shard1Primary,
		keyspaceName,
		false)

	cluster.ExecuteOnTablet(t,
		fmt.Sprintf(cluster.InsertTabletTemplateKsID, tableName, 2, "msg2", 2),
		*shard1Primary,
		keyspaceName,
		false)

	cluster.ExecuteOnTablet(t,
		fmt.Sprintf(cluster.InsertTabletTemplateKsID, tableName, 4, "msg4", 4),
		*shard2Primary,
		keyspaceName,
		false)
}

func deleteInitialValues(t *testing.T) {
	cluster.ExecuteOnTablet(t,
		fmt.Sprintf("delete from %s where id = %v", tableName, 1),
		*shard1Primary,
		keyspaceName,
		false)

	cluster.ExecuteOnTablet(t,
		fmt.Sprintf("delete from %s where id = %v", tableName, 2),
		*shard1Primary,
		keyspaceName,
		false)

	cluster.ExecuteOnTablet(t,
		fmt.Sprintf("delete from %s where id = %v", tableName, 4),
		*shard2Primary,
		keyspaceName,
		false)
}
