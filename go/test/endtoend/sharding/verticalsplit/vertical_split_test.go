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

package verticalsplit

import (
	"context"
	"flag"
	"fmt"
	"os/exec"
	"path"
	"reflect"
	"strings"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"vitess.io/vitess/go/json2"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/sharding"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/proto/topodata"
	_ "vitess.io/vitess/go/vt/vtgate/grpcvtgateconn"
)

var (
	clusterInstance      *cluster.LocalProcessCluster
	sourceKeyspace       = "source_keyspace"
	destinationKeyspace  = "destination_keyspace"
	hostname             = "localhost"
	cellj                = "test_nj"
	shardName            = "0"
	createTabletTemplate = `
		create table %s(
			id bigint not null,
			msg varchar(64),
			primary key (id),
			index by_msg (msg)
		) Engine=InnoDB;`
	createViewTemplate     = "create view %s(id, msg) as select id, msg from %s;"
	createMoving3NoPkTable = `
		create table moving3_no_pk (
			id bigint not null,
			msg varchar(64)
		) Engine=InnoDB;`
	tableArr         = []string{"moving1", "moving2", "staying1", "staying2"}
	insertIndex      = 0
	moving1First     int
	moving2First     int
	staying1First    int
	staying2First    int
	moving3NoPkFirst int
)

func TestVerticalSplit(t *testing.T) {
	defer cluster.PanicHandler(t)
	flag.Parse()
	code, err := initializeCluster()
	if err != nil {
		t.Errorf("setup failed with status code %d", code)
	}
	defer teardownCluster()

	// Adding another cell in the same cluster
	err = clusterInstance.TopoProcess.ManageTopoDir("mkdir", "/vitess/"+"test_ca")
	require.NoError(t, err)
	err = clusterInstance.VtctlProcess.AddCellInfo("test_ca")
	require.NoError(t, err)
	err = clusterInstance.TopoProcess.ManageTopoDir("mkdir", "/vitess/"+"test_ny")
	require.NoError(t, err)
	err = clusterInstance.VtctlProcess.AddCellInfo("test_ny")
	require.NoError(t, err)

	require.NoError(t, err, "error should be Nil")

	// source keyspace, with 4 tables
	sourceShard := clusterInstance.Keyspaces[0].Shards[0]
	sourceMasterTablet := *sourceShard.Vttablets[0]
	sourceReplicaTablet := *sourceShard.Vttablets[1]
	sourceRdOnlyTablet1 := *sourceShard.Vttablets[2]
	sourceRdOnlyTablet2 := *sourceShard.Vttablets[3]
	sourceKs := fmt.Sprintf("%s/%s", sourceKeyspace, shardName)

	// destination keyspace, with just two tables
	destinationShard := clusterInstance.Keyspaces[1].Shards[0]
	destinationMasterTablet := *destinationShard.Vttablets[0]
	destinationReplicaTablet := *destinationShard.Vttablets[1]
	destinationRdOnlyTablet1 := *destinationShard.Vttablets[2]
	destinationRdOnlyTablet2 := *destinationShard.Vttablets[3]

	// source and destination master and replica tablets will be started
	for _, tablet := range []cluster.Vttablet{sourceMasterTablet, sourceReplicaTablet, destinationMasterTablet, destinationReplicaTablet} {
		_ = tablet.VttabletProcess.Setup()
	}

	// rdonly tablets will be started
	for _, tablet := range []cluster.Vttablet{sourceRdOnlyTablet1, sourceRdOnlyTablet2, destinationRdOnlyTablet1, destinationRdOnlyTablet2} {
		_ = tablet.VttabletProcess.Setup()
	}

	// RebuildKeyspaceGraph source keyspace
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RebuildKeyspaceGraph", sourceKeyspace)
	require.NoError(t, err)

	// RebuildKeyspaceGraph destination keyspace
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RebuildKeyspaceGraph", destinationKeyspace)
	require.NoError(t, err)

	// check SrvKeyspace
	ksServedFrom := "ServedFrom(master): source_keyspace\nServedFrom(rdonly): source_keyspace\nServedFrom(replica): source_keyspace\n"
	checkSrvKeyspaceServedFrom(t, cellj, destinationKeyspace, ksServedFrom, *clusterInstance)

	// reparent to make the tablets work (we use health check, fix their types)
	err = clusterInstance.VtctlclientProcess.InitShardMaster(sourceKeyspace, shardName, cellj, sourceMasterTablet.TabletUID)
	require.NoError(t, err)
	err = clusterInstance.VtctlclientProcess.InitShardMaster(destinationKeyspace, shardName, cellj, destinationMasterTablet.TabletUID)
	require.NoError(t, err)

	sourceMasterTablet.Type = "master"
	destinationMasterTablet.Type = "master"

	for _, tablet := range []cluster.Vttablet{sourceReplicaTablet, destinationReplicaTablet} {
		_ = tablet.VttabletProcess.WaitForTabletType("SERVING")
		require.NoError(t, err)
	}
	for _, tablet := range []cluster.Vttablet{sourceRdOnlyTablet1, sourceRdOnlyTablet2, destinationRdOnlyTablet1, destinationRdOnlyTablet2} {
		_ = tablet.VttabletProcess.WaitForTabletType("SERVING")
		require.NoError(t, err)
	}

	for _, tablet := range []cluster.Vttablet{sourceMasterTablet, destinationMasterTablet, sourceReplicaTablet, destinationReplicaTablet, sourceRdOnlyTablet1, sourceRdOnlyTablet2, destinationRdOnlyTablet1, destinationRdOnlyTablet2} {
		assert.Equal(t, tablet.VttabletProcess.GetTabletStatus(), "SERVING")
	}

	// Apply schema on source.
	for _, tableName := range tableArr {
		_, err := sourceMasterTablet.VttabletProcess.QueryTablet(fmt.Sprintf(createTabletTemplate, tableName), sourceKeyspace, true)
		require.NoError(t, err)
	}
	_, err = sourceMasterTablet.VttabletProcess.QueryTablet(fmt.Sprintf(createViewTemplate, "view1", "moving1"), sourceKeyspace, true)
	require.NoError(t, err)
	_, err = sourceMasterTablet.VttabletProcess.QueryTablet(createMoving3NoPkTable, sourceKeyspace, true)
	require.NoError(t, err)
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ReloadSchemaShard", "source_keyspace/0")
	require.NoError(t, err)

	// Alloy schema on destination.
	_, err = destinationMasterTablet.VttabletProcess.QueryTablet(fmt.Sprintf(createTabletTemplate, "extra1"), destinationKeyspace, true)
	require.NoError(t, err)
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ReloadSchemaShard", "destination_keyspace/0")
	require.NoError(t, err)

	err = clusterInstance.StartVtgate()
	require.NoError(t, err)

	vtParams := mysql.ConnParams{
		Host: clusterInstance.Hostname,
		Port: clusterInstance.VtgateMySQLPort,
	}

	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	err = clusterInstance.VtgateProcess.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", sourceKeyspace, shardName), 1)
	require.NoError(t, err)
	err = clusterInstance.VtgateProcess.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.replica", sourceKeyspace, shardName), 1)
	require.NoError(t, err)
	err = clusterInstance.VtgateProcess.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.rdonly", sourceKeyspace, shardName), 2)
	require.NoError(t, err)
	err = clusterInstance.VtgateProcess.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", destinationKeyspace, shardName), 1)
	require.NoError(t, err)
	err = clusterInstance.VtgateProcess.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.replica", destinationKeyspace, shardName), 1)
	require.NoError(t, err)
	err = clusterInstance.VtgateProcess.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.rdonly", destinationKeyspace, shardName), 2)
	require.NoError(t, err)

	// create the schema on the source keyspace, add some values
	insertInitialValues(t, conn, sourceMasterTablet, destinationMasterTablet)

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("CopySchemaShard", "--tables", "/moving/,view1", sourceRdOnlyTablet1.Alias, "destination_keyspace/0")
	require.NoError(t, err, "CopySchemaShard failed")

	// starting vtworker
	httpPort := clusterInstance.GetAndReservePort()
	grpcPort := clusterInstance.GetAndReservePort()
	clusterInstance.VtworkerProcess = *cluster.VtworkerProcessInstance(
		httpPort,
		grpcPort,
		clusterInstance.TopoPort,
		clusterInstance.Hostname,
		clusterInstance.TmpDirectory)

	err = clusterInstance.VtworkerProcess.ExecuteVtworkerCommand(httpPort, grpcPort,
		"--cell", cellj,
		"--command_display_interval", "10ms",
		"--use_v3_resharding_mode=true",
		"VerticalSplitClone",
		"--tables", "/moving/,view1",
		"--chunk_count", "10",
		"--min_rows_per_chunk", "1",
		"--min_healthy_tablets", "1", "destination_keyspace/0")
	require.NoError(t, err)

	// test Cancel first
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("CancelResharding", "destination_keyspace/0")
	require.NoError(t, err)
	err = destinationMasterTablet.VttabletProcess.WaitForBinLogPlayerCount(0)
	require.NoError(t, err)
	// master should be in serving state after cancel
	sharding.CheckTabletQueryServices(t, []cluster.Vttablet{destinationMasterTablet}, "SERVING", false, *clusterInstance)

	// redo VerticalSplitClone
	err = clusterInstance.VtworkerProcess.ExecuteVtworkerCommand(clusterInstance.GetAndReservePort(), clusterInstance.GetAndReservePort(),
		"--cell", cellj,
		"--command_display_interval", "10ms",
		"--use_v3_resharding_mode=true",
		"VerticalSplitClone",
		"--tables", "/moving/,view1",
		"--chunk_count", "10",
		"--min_rows_per_chunk", "1",
		"--min_healthy_tablets", "1", "destination_keyspace/0")
	require.NoError(t, err)

	// check values are present
	checkValues(t, &destinationMasterTablet, destinationKeyspace, "vt_destination_keyspace", "moving1", moving1First, 100)
	checkValues(t, &destinationMasterTablet, destinationKeyspace, "vt_destination_keyspace", "moving2", moving2First, 100)
	checkValues(t, &destinationMasterTablet, destinationKeyspace, "vt_destination_keyspace", "view1", moving1First, 100)
	checkValues(t, &destinationMasterTablet, destinationKeyspace, "vt_destination_keyspace", "moving3_no_pk", moving3NoPkFirst, 100)

	// Verify vreplication table entries
	dbParams := mysql.ConnParams{
		Uname:      "vt_dba",
		UnixSocket: path.Join(destinationMasterTablet.VttabletProcess.Directory, "mysql.sock"),
	}
	dbParams.DbName = "_vt"
	dbConn, err := mysql.Connect(ctx, &dbParams)
	require.NoError(t, err)
	qr, err := dbConn.ExecuteFetch("select * from vreplication", 1000, true)
	require.NoError(t, err, "error should be Nil")
	assert.Equal(t, 1, len(qr.Rows))
	assert.Contains(t, fmt.Sprintf("%v", qr.Rows), "SplitClone")
	assert.Contains(t, fmt.Sprintf("%v", qr.Rows), `keyspace:\"source_keyspace\" shard:\"0\" tables:\"/moving/\" tables:\"view1\`)
	dbConn.Close()

	// check the binlog player is running and exporting vars
	sharding.CheckDestinationMaster(t, destinationMasterTablet, []string{sourceKs}, *clusterInstance)

	// check that binlog server exported the stats vars
	sharding.CheckBinlogServerVars(t, sourceReplicaTablet, 0, 0, true)

	// add values to source, make sure they're replicated
	moving1FirstAdd1 := insertValues(t, conn, sourceKeyspace, "moving1", 100)
	_ = insertValues(t, conn, sourceKeyspace, "staying1", 100)
	moving2FirstAdd1 := insertValues(t, conn, sourceKeyspace, "moving2", 100)
	checkValuesTimeout(t, destinationMasterTablet, "moving1", moving1FirstAdd1, 100, 30)
	checkValuesTimeout(t, destinationMasterTablet, "moving2", moving2FirstAdd1, 100, 30)
	sharding.CheckBinlogPlayerVars(t, destinationMasterTablet, []string{sourceKs}, 30)
	sharding.CheckBinlogServerVars(t, sourceReplicaTablet, 100, 100, true)

	// use vtworker to compare the data
	log.Info("Running vtworker VerticalSplitDiff")
	err = clusterInstance.VtworkerProcess.ExecuteVtworkerCommand(clusterInstance.GetAndReservePort(),
		clusterInstance.GetAndReservePort(),
		"--use_v3_resharding_mode=true",
		"--cell", "test_nj",
		"VerticalSplitDiff",
		"--min_healthy_rdonly_tablets", "1",
		"destination_keyspace/0")
	require.NoError(t, err)

	// check query service is off on destination master, as filtered
	// replication is enabled. Even health check should not interfere.
	destinationMasterTabletVars := destinationMasterTablet.VttabletProcess.GetVars()
	assert.NotNil(t, destinationMasterTabletVars)
	assert.Contains(t, reflect.ValueOf(destinationMasterTabletVars["TabletStateName"]).String(), "NOT_SERVING")

	// check we can't migrate the master just yet
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("MigrateServedFrom", "destination_keyspace/0", "master")
	require.Error(t, err)

	// migrate rdonly only in test_ny cell, make sure nothing is migrated
	// in test_nj
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("MigrateServedFrom", "--cells=test_ny", "destination_keyspace/0", "rdonly")
	require.NoError(t, err)

	// check SrvKeyspace
	checkSrvKeyspaceServedFrom(t, cellj, destinationKeyspace, ksServedFrom, *clusterInstance)
	checkBlacklistedTables(t, sourceMasterTablet, sourceKeyspace, nil)
	checkBlacklistedTables(t, sourceReplicaTablet, sourceKeyspace, nil)
	checkBlacklistedTables(t, sourceRdOnlyTablet1, sourceKeyspace, nil)
	checkBlacklistedTables(t, sourceRdOnlyTablet2, sourceKeyspace, nil)

	// migrate test_nj only, using command line manual fix command,
	// and restore it back.
	keyspaceJSON, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("GetKeyspace", "destination_keyspace")
	require.NoError(t, err)

	validateKeyspaceJSON(t, keyspaceJSON, []string{"test_ca", "test_nj"})

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("SetKeyspaceServedFrom", "-source=source_keyspace", "-remove", "-cells=test_nj,test_ca", "destination_keyspace", "rdonly")
	require.NoError(t, err)

	// again validating keyspaceJSON
	keyspaceJSON, err = clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("GetKeyspace", "destination_keyspace")
	require.NoError(t, err)

	validateKeyspaceJSON(t, keyspaceJSON, nil)

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("SetKeyspaceServedFrom", "-source=source_keyspace", "destination_keyspace", "rdonly")
	require.NoError(t, err)

	keyspaceJSON, err = clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("GetKeyspace", "destination_keyspace")
	require.NoError(t, err)

	validateKeyspaceJSON(t, keyspaceJSON, []string{})

	// now serve rdonly from the destination shards
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("MigrateServedFrom", "destination_keyspace/0", "rdonly")
	require.NoError(t, err)
	checkSrvKeyspaceServedFrom(t, cellj, destinationKeyspace, "ServedFrom(master): source_keyspace\nServedFrom(replica): source_keyspace\n", *clusterInstance)
	checkBlacklistedTables(t, sourceMasterTablet, sourceKeyspace, nil)
	checkBlacklistedTables(t, sourceReplicaTablet, sourceKeyspace, nil)
	checkBlacklistedTables(t, sourceRdOnlyTablet1, sourceKeyspace, []string{"/moving/", "view1"})
	checkBlacklistedTables(t, sourceRdOnlyTablet2, sourceKeyspace, []string{"/moving/", "view1"})

	grpcAddress := fmt.Sprintf("%s:%d", "localhost", clusterInstance.VtgateProcess.GrpcPort)
	gconn, err := vtgateconn.Dial(ctx, grpcAddress)
	require.NoError(t, err)
	defer gconn.Close()

	checkClientConnRedirectionExecuteKeyrange(ctx, t, gconn, destinationKeyspace, []topodata.TabletType{topodata.TabletType_MASTER, topodata.TabletType_REPLICA}, []string{"moving1", "moving2"})

	// then serve replica from the destination shards
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("MigrateServedFrom", "destination_keyspace/0", "replica")
	require.NoError(t, err)
	checkSrvKeyspaceServedFrom(t, cellj, destinationKeyspace, "ServedFrom(master): source_keyspace\n", *clusterInstance)
	checkBlacklistedTables(t, sourceMasterTablet, sourceKeyspace, nil)
	checkBlacklistedTables(t, sourceReplicaTablet, sourceKeyspace, []string{"/moving/", "view1"})
	checkBlacklistedTables(t, sourceRdOnlyTablet1, sourceKeyspace, []string{"/moving/", "view1"})
	checkBlacklistedTables(t, sourceRdOnlyTablet2, sourceKeyspace, []string{"/moving/", "view1"})
	checkClientConnRedirectionExecuteKeyrange(ctx, t, gconn, destinationKeyspace, []topodata.TabletType{topodata.TabletType_MASTER}, []string{"moving1", "moving2"})

	// move replica back and forth
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("MigrateServedFrom", "-reverse", "destination_keyspace/0", "replica")
	require.NoError(t, err)
	checkSrvKeyspaceServedFrom(t, cellj, destinationKeyspace, "ServedFrom(master): source_keyspace\nServedFrom(replica): source_keyspace\n", *clusterInstance)
	checkBlacklistedTables(t, sourceMasterTablet, sourceKeyspace, nil)
	checkBlacklistedTables(t, sourceReplicaTablet, sourceKeyspace, nil)
	checkBlacklistedTables(t, sourceRdOnlyTablet1, sourceKeyspace, []string{"/moving/", "view1"})
	checkBlacklistedTables(t, sourceRdOnlyTablet2, sourceKeyspace, []string{"/moving/", "view1"})

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("MigrateServedFrom", "destination_keyspace/0", "replica")
	require.NoError(t, err)
	checkSrvKeyspaceServedFrom(t, cellj, destinationKeyspace, "ServedFrom(master): source_keyspace\n", *clusterInstance)
	checkBlacklistedTables(t, sourceMasterTablet, sourceKeyspace, nil)
	checkBlacklistedTables(t, sourceReplicaTablet, sourceKeyspace, []string{"/moving/", "view1"})
	checkBlacklistedTables(t, sourceRdOnlyTablet1, sourceKeyspace, []string{"/moving/", "view1"})
	checkBlacklistedTables(t, sourceRdOnlyTablet2, sourceKeyspace, []string{"/moving/", "view1"})
	checkClientConnRedirectionExecuteKeyrange(ctx, t, gconn, destinationKeyspace, []topodata.TabletType{topodata.TabletType_MASTER}, []string{"moving1", "moving2"})

	// Cancel should fail now
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("CancelResharding", "destination_keyspace/0")
	require.Error(t, err)

	// then serve master from the destination shards
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("MigrateServedFrom", "destination_keyspace/0", "master")
	require.NoError(t, err)
	checkSrvKeyspaceServedFrom(t, cellj, destinationKeyspace, "", *clusterInstance)
	checkBlacklistedTables(t, sourceMasterTablet, sourceKeyspace, []string{"/moving/", "view1"})
	checkBlacklistedTables(t, sourceReplicaTablet, sourceKeyspace, []string{"/moving/", "view1"})
	checkBlacklistedTables(t, sourceRdOnlyTablet1, sourceKeyspace, []string{"/moving/", "view1"})
	checkBlacklistedTables(t, sourceRdOnlyTablet2, sourceKeyspace, []string{"/moving/", "view1"})

	// check the binlog player is gone now
	_ = destinationMasterTablet.VttabletProcess.WaitForBinLogPlayerCount(0)

	// check the stats are correct
	checkStats(t)

	// now remove the tables on the source shard. The blacklisted tables
	// in the source shard won't match any table, make sure that works.
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ApplySchema", "-sql=drop view view1", "source_keyspace")
	require.NoError(t, err)

	for _, table := range []string{"moving1", "moving2"} {
		err = clusterInstance.VtctlclientProcess.ExecuteCommand("ApplySchema", "--sql=drop table "+table, "source_keyspace")
		require.NoError(t, err)
	}
	for _, tablet := range []cluster.Vttablet{sourceMasterTablet, sourceReplicaTablet, sourceRdOnlyTablet1, sourceRdOnlyTablet2} {
		err = clusterInstance.VtctlclientProcess.ExecuteCommand("ReloadSchema", tablet.Alias)
		require.NoError(t, err)
	}
	qr, _ = sourceMasterTablet.VttabletProcess.QueryTablet("select count(1) from staying1", sourceKeyspace, true)
	assert.Equal(t, 1, len(qr.Rows), fmt.Sprintf("cannot read staying1: got %d", len(qr.Rows)))

	// test SetShardTabletControl
	verifyVtctlSetShardTabletControl(t)

}

func verifyVtctlSetShardTabletControl(t *testing.T) {
	// clear the rdonly entry:
	err := clusterInstance.VtctlclientProcess.ExecuteCommand("SetShardTabletControl", "--remove", "source_keyspace/0", "rdonly")
	require.NoError(t, err)
	assertTabletControls(t, clusterInstance, []topodata.TabletType{topodata.TabletType_MASTER, topodata.TabletType_REPLICA})

	// re-add rdonly:
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("SetShardTabletControl", "--blacklisted_tables=/moving/,view1", "source_keyspace/0", "rdonly")
	require.NoError(t, err)
	assertTabletControls(t, clusterInstance, []topodata.TabletType{topodata.TabletType_MASTER, topodata.TabletType_REPLICA, topodata.TabletType_RDONLY})

	//and then clear all entries:
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("SetShardTabletControl", "--remove", "source_keyspace/0", "rdonly")
	require.NoError(t, err)
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("SetShardTabletControl", "--remove", "source_keyspace/0", "replica")
	require.NoError(t, err)
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("SetShardTabletControl", "--remove", "source_keyspace/0", "master")
	require.NoError(t, err)

	shardJSON, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("GetShard", "source_keyspace/0")
	require.NoError(t, err)
	var shardJSONData topodata.Shard
	err = json2.Unmarshal([]byte(shardJSON), &shardJSONData)
	require.NoError(t, err)
	assert.Empty(t, shardJSONData.TabletControls)

}

func assertTabletControls(t *testing.T, clusterInstance *cluster.LocalProcessCluster, aliases []topodata.TabletType) {
	shardJSON, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("GetShard", "source_keyspace/0")
	require.NoError(t, err)
	var shardJSONData topodata.Shard
	err = json2.Unmarshal([]byte(shardJSON), &shardJSONData)
	require.NoError(t, err)
	assert.Equal(t, len(shardJSONData.TabletControls), len(aliases))
	for _, tc := range shardJSONData.TabletControls {
		assert.Contains(t, aliases, tc.TabletType)
		assert.Equal(t, []string{"/moving/", "view1"}, tc.BlacklistedTables)
	}
}

func checkStats(t *testing.T) {

	resultMap, err := clusterInstance.VtgateProcess.GetVars()
	require.NoError(t, err)
	resultVtTabletCall := resultMap["VttabletCall"]
	resultVtTabletCallMap := resultVtTabletCall.(map[string]interface{})
	resultHistograms := resultVtTabletCallMap["Histograms"]
	resultHistogramsMap := resultHistograms.(map[string]interface{})
	resultTablet := resultHistogramsMap["Execute.source_keyspace.0.replica"]
	resultTableMap := resultTablet.(map[string]interface{})
	resultCountStr := fmt.Sprintf("%v", reflect.ValueOf(resultTableMap["Count"]))
	assert.Equal(t, "2", resultCountStr, fmt.Sprintf("unexpected value for VttabletCall(Execute.source_keyspace.0.replica) inside %s", resultCountStr))

	// Verify master reads done by self._check_client_conn_redirection().
	resultVtgateAPI := resultMap["VtgateApi"]
	resultVtgateAPIMap := resultVtgateAPI.(map[string]interface{})
	resultAPIHistograms := resultVtgateAPIMap["Histograms"]
	resultAPIHistogramsMap := resultAPIHistograms.(map[string]interface{})
	resultTabletDestination := resultAPIHistogramsMap["Execute.destination_keyspace.master"]
	resultTabletDestinationMap := resultTabletDestination.(map[string]interface{})
	resultCountStrDestination := fmt.Sprintf("%v", reflect.ValueOf(resultTabletDestinationMap["Count"]))
	assert.Equal(t, "6", resultCountStrDestination, fmt.Sprintf("unexpected value for VtgateApi(Execute.destination_keyspace.master) inside %s)", resultCountStrDestination))

	assert.Empty(t, resultMap["VtgateApiErrorCounts"])

}

func insertInitialValues(t *testing.T, conn *mysql.Conn, sourceMasterTablet cluster.Vttablet, destinationMasterTablet cluster.Vttablet) {
	moving1First = insertValues(t, conn, sourceKeyspace, "moving1", 100)
	moving2First = insertValues(t, conn, sourceKeyspace, "moving2", 100)
	staying1First = insertValues(t, conn, sourceKeyspace, "staying1", 100)
	staying2First = insertValues(t, conn, sourceKeyspace, "staying2", 100)
	checkValues(t, &sourceMasterTablet, sourceKeyspace, "vt_source_keyspace", "moving1", moving1First, 100)
	checkValues(t, &sourceMasterTablet, sourceKeyspace, "vt_source_keyspace", "moving2", moving2First, 100)
	checkValues(t, &sourceMasterTablet, sourceKeyspace, "vt_source_keyspace", "staying1", staying1First, 100)
	checkValues(t, &sourceMasterTablet, sourceKeyspace, "vt_source_keyspace", "staying2", staying2First, 100)
	checkValues(t, &sourceMasterTablet, sourceKeyspace, "vt_source_keyspace", "view1", moving1First, 100)

	moving3NoPkFirst = insertValues(t, conn, sourceKeyspace, "moving3_no_pk", 100)
	checkValues(t, &sourceMasterTablet, sourceKeyspace, "vt_source_keyspace", "moving3_no_pk", moving3NoPkFirst, 100)

	// Insert data directly because vtgate would redirect us.
	_, err := destinationMasterTablet.VttabletProcess.QueryTablet(fmt.Sprintf("insert into %s (id, msg) values(%d, 'value %d')", "extra1", 1, 1), destinationKeyspace, true)
	require.NoError(t, err)
	checkValues(t, &destinationMasterTablet, destinationKeyspace, "vt_destination_keyspace", "extra1", 1, 1)
}

func checkClientConnRedirectionExecuteKeyrange(ctx context.Context, t *testing.T, conn *vtgateconn.VTGateConn, keyspace string, servedFromDbTypes []topodata.TabletType, movedTables []string) {
	// check that the ServedFrom indirection worked correctly.
	for _, tabletType := range servedFromDbTypes {
		session := conn.Session(fmt.Sprintf("%s@%v", keyspace, tabletType), nil)
		for _, table := range movedTables {
			_, err := session.Execute(ctx, fmt.Sprintf("select * from %s", table), nil)
			require.NoError(t, err)
		}
	}
}

func checkValues(t *testing.T, tablet *cluster.Vttablet, keyspace string, dbname string, table string, first int, count int) {
	t.Helper()
	log.Infof("Checking %d values from %s/%s starting at %d", count, dbname, table, first)
	qr, _ := tablet.VttabletProcess.QueryTablet(fmt.Sprintf("select id, msg from %s where id>=%d order by id limit %d", table, first, count), keyspace, true)
	assert.Equal(t, count, len(qr.Rows), fmt.Sprintf("got wrong number of rows: %d != %d", len(qr.Rows), count))
	i := 0
	for i < count {
		result, _ := evalengine.ToInt64(qr.Rows[i][0])
		assert.Equal(t, int64(first+i), result, fmt.Sprintf("got wrong number of rows: %d != %d", len(qr.Rows), first+i))
		assert.Contains(t, qr.Rows[i][1].String(), fmt.Sprintf("value %d", first+i), fmt.Sprintf("invalid msg[%d]: 'value %d' != '%s'", i, first+i, qr.Rows[i][1].String()))
		i++
	}
}

func checkValuesTimeout(t *testing.T, tablet cluster.Vttablet, table string, first int, count int, timeoutInSec int) bool {
	t.Helper()
	for i := 0; i < timeoutInSec; i-- {
		qr, err := tablet.VttabletProcess.QueryTablet(fmt.Sprintf("select id, msg from %s where id>=%d order by id limit %d", table, first, count), destinationKeyspace, true)
		if err != nil {
			require.NoError(t, err, "select failed on master tablet")
		}
		if len(qr.Rows) == count {
			return true
		}
		time.Sleep(1 * time.Second)
	}
	return true
}

func checkBlacklistedTables(t *testing.T, tablet cluster.Vttablet, keyspace string, expected []string) {
	t.Helper()
	tabletStatus := tablet.VttabletProcess.GetStatus()
	if expected != nil {
		assert.Contains(t, tabletStatus, fmt.Sprintf("BlacklistedTables: %s", strings.Join(expected, " ")))
	} else {
		assert.NotContains(t, tabletStatus, "BlacklistedTables")
	}

	// check we can or cannot access the tables
	for _, table := range []string{"moving1", "moving2"} {
		if expected != nil && strings.Contains(strings.Join(expected, " "), "moving") {
			// table is blacklisted, should get error
			err := clusterInstance.VtctlclientProcess.ExecuteCommand("VtTabletExecute", "-json", tablet.Alias, fmt.Sprintf("select count(1) from %s", table))
			require.Error(t, err, "disallowed due to rule: enforce blacklisted tables")
		} else {
			// table is not blacklisted, should just work
			_, err := tablet.VttabletProcess.QueryTablet(fmt.Sprintf("select count(1) from %s", table), keyspace, true)
			require.NoError(t, err)
		}
	}
}

func insertValues(t *testing.T, conn *mysql.Conn, keyspace string, table string, count int) int {
	result := insertIndex
	i := 0
	for i < count {
		execQuery(t, conn, "begin")
		execQuery(t, conn, "use `"+keyspace+":0`")
		execQuery(t, conn, fmt.Sprintf("insert into %s (id, msg) values(%d, 'value %d')", table, insertIndex, insertIndex))
		execQuery(t, conn, "commit")
		insertIndex++
		i++
	}
	return result
}

func teardownCluster() {
	clusterInstance.Teardown()
}

func execQuery(t *testing.T, conn *mysql.Conn, query string) *sqltypes.Result {
	t.Helper()
	qr, err := conn.ExecuteFetch(query, 1000, true)
	require.NoError(t, err)
	return qr
}

// CheckSrvKeyspaceServedFrom verifies the servedFrom with expected
func checkSrvKeyspaceServedFrom(t *testing.T, cell string, ksname string, expected string, ci cluster.LocalProcessCluster) {
	srvKeyspace := sharding.GetSrvKeyspace(t, cell, ksname, ci)
	tabletTypeKeyspaceMap := make(map[string]string)
	result := ""
	if srvKeyspace.GetServedFrom() != nil {
		for _, servedFrom := range srvKeyspace.GetServedFrom() {
			tabletTy := strings.ToLower(servedFrom.GetTabletType().String())
			tabletTypeKeyspaceMap[tabletTy] = servedFrom.GetKeyspace()
		}
	}
	if tabletTypeKeyspaceMap["master"] != "" {
		result = result + fmt.Sprintf("ServedFrom(%s): %s\n", "master", tabletTypeKeyspaceMap["master"])
	}
	if tabletTypeKeyspaceMap["rdonly"] != "" {
		result = result + fmt.Sprintf("ServedFrom(%s): %s\n", "rdonly", tabletTypeKeyspaceMap["rdonly"])
	}
	if tabletTypeKeyspaceMap["replica"] != "" {
		result = result + fmt.Sprintf("ServedFrom(%s): %s\n", "replica", tabletTypeKeyspaceMap["replica"])
	}
	assert.Equal(t, expected, result, fmt.Sprintf("Mismatch in srv keyspace for cell %s keyspace %s, expected:\n %s\ngot:\n%s", cell, ksname, expected, result))
	assert.Equal(t, "", srvKeyspace.GetShardingColumnName(), fmt.Sprintf("Got a sharding_column_name in SrvKeyspace: %s", srvKeyspace.GetShardingColumnName()))
	assert.Equal(t, "UNSET", srvKeyspace.GetShardingColumnType().String(), fmt.Sprintf("Got a sharding_column_type in SrvKeyspace: %s", srvKeyspace.GetShardingColumnType().String()))
}

func initializeCluster() (int, error) {
	var mysqlProcesses []*exec.Cmd
	clusterInstance = cluster.NewCluster(cellj, hostname)

	// Start topo server
	if err := clusterInstance.StartTopo(); err != nil {
		return 1, err
	}

	for _, keyspaceStr := range []string{sourceKeyspace, destinationKeyspace} {
		KeyspacePtr := &cluster.Keyspace{Name: keyspaceStr}
		keyspace := *KeyspacePtr
		if keyspaceStr == sourceKeyspace {
			if err := clusterInstance.VtctlProcess.CreateKeyspace(keyspace.Name); err != nil {
				return 1, err
			}
		} else {
			if err := clusterInstance.VtctlclientProcess.ExecuteCommand("CreateKeyspace", "--served_from", "master:source_keyspace,replica:source_keyspace,rdonly:source_keyspace", "destination_keyspace"); err != nil {
				return 1, err
			}
		}
		shard := &cluster.Shard{
			Name: shardName,
		}
		for i := 0; i < 4; i++ {
			// instantiate vttablet object with reserved ports
			tabletUID := clusterInstance.GetAndReserveTabletUID()
			var tablet *cluster.Vttablet
			if i == 0 {
				tablet = clusterInstance.NewVttabletInstance("replica", tabletUID, cellj)
			} else if i == 1 {
				tablet = clusterInstance.NewVttabletInstance("replica", tabletUID, cellj)
			} else {
				tablet = clusterInstance.NewVttabletInstance("rdonly", tabletUID, cellj)
			}
			// Start Mysqlctl process
			tablet.MysqlctlProcess = *cluster.MysqlCtlProcessInstance(tablet.TabletUID, tablet.MySQLPort, clusterInstance.TmpDirectory)
			proc, err := tablet.MysqlctlProcess.StartProcess()
			if err != nil {
				return 1, err
			}
			mysqlProcesses = append(mysqlProcesses, proc)
			// start vttablet process
			tablet.VttabletProcess = cluster.VttabletProcessInstance(tablet.HTTPPort,
				tablet.GrpcPort,
				tablet.TabletUID,
				clusterInstance.Cell,
				shardName,
				keyspace.Name,
				clusterInstance.VtctldProcess.Port,
				tablet.Type,
				clusterInstance.TopoProcess.Port,
				clusterInstance.Hostname,
				clusterInstance.TmpDirectory,
				clusterInstance.VtTabletExtraArgs,
				clusterInstance.EnableSemiSync)
			tablet.Alias = tablet.VttabletProcess.TabletPath

			shard.Vttablets = append(shard.Vttablets, tablet)
		}
		keyspace.Shards = append(keyspace.Shards, *shard)
		clusterInstance.Keyspaces = append(clusterInstance.Keyspaces, keyspace)
	}
	for _, proc := range mysqlProcesses {
		err := proc.Wait()
		if err != nil {
			return 1, err
		}
	}
	return 0, nil
}

func validateKeyspaceJSON(t *testing.T, keyspaceJSON string, cellsArr []string) {
	var keyspace topodata.Keyspace
	err := json2.Unmarshal([]byte(keyspaceJSON), &keyspace)
	require.NoError(t, err)
	found := false
	for _, servedFrom := range keyspace.GetServedFroms() {
		if strings.ToLower(servedFrom.GetTabletType().String()) == "rdonly" {
			found = true
			if cellsArr != nil {
				if len(cellsArr) > 0 {
					for _, eachCell := range cellsArr {
						assert.Contains(t, strings.Join(servedFrom.GetCells(), " "), eachCell)
					}
				} else {
					assert.Equal(t, []string{}, servedFrom.GetCells())
				}
			}
		}
	}
	if cellsArr != nil {
		assert.Equal(t, true, found)
	} else {
		assert.Equal(t, false, found)
	}
}
