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

package mergesharding

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"vitess.io/vitess/go/sqltypes"

	"vitess.io/vitess/go/mysql"

	"github.com/prometheus/common/log"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/sharding"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/topodata"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	// ClusterInstance instance to be used for test with different params
	clusterInstance      *cluster.LocalProcessCluster
	hostname             = "localhost"
	keyspaceName         = "ks"
	cell                 = "zone1"
	createTabletTemplate = `
							create table %s(
							custom_ksid_col %s not null,
							msg varchar(64),
							id bigint not null,
							parent_id bigint not null,
							primary key (parent_id, id),
							index by_msg (msg)
							) Engine=InnoDB;
							`
	fixedParentID = 86
	tableName     = "resharding1"
	vSchema       = `
							{
							  "sharded": true,
							  "vindexes": {
								"hash_index": {
								  "type": "hash"
								}
							  },
							  "tables": {
								"resharding1": {
								   "column_vindexes": [
									{
									  "column": "custom_ksid_col",
									  "name": "hash_index"
									}
								  ] 
								}
							  }
							}
						`
	// insertTabletTemplateKsID common insert format
	insertTabletTemplateKsID = `insert into %s (parent_id, id, msg, custom_ksid_col) values (%d, %d, '%s', %d) /* vtgate:: keyspace_id:%d */ /* id:%d */`

	// initial shards
	// range -40, 40-80 & 80-
	shard0 = &cluster.Shard{Name: "-40"}
	shard1 = &cluster.Shard{Name: "40-80"}
	shard2 = &cluster.Shard{Name: "80-"}

	// merge shard
	// merging -40 & 40-80 to -80
	shard3 = &cluster.Shard{Name: "-80"}

	// Sharding keys
	key1 uint64 = 1 // Key redirect to shard 0 [-40]
	key2 uint64 = 3 // key redirect to shard 1 [40-80]
	key3 uint64 = 4 // Key redirect to shard 2 [80-]
)

// TestMergesharding covers the workflow for a sharding merge.
// We start with 3 shards: -40, 40-80, and 80-. We then merge -40 and 40-80 into -80.
// Note this test is just testing the full workflow, not corner cases or error
// cases. These are mostly done by the other resharding tests.
func TestMergesharding(t *testing.T, useVarbinaryShardingKeyType bool) {
	defer cluster.PanicHandler(t)
	clusterInstance = cluster.NewCluster(cell, hostname)
	defer clusterInstance.Teardown()

	// Launch keyspace
	keyspace := &cluster.Keyspace{Name: keyspaceName}

	// Start topo server
	err := clusterInstance.StartTopo()
	require.NoError(t, err)

	// Defining all the tablets
	shard0Master := clusterInstance.NewVttabletInstance("replica", 0, "")
	shard0Replica := clusterInstance.NewVttabletInstance("replica", 0, "")
	shard0Rdonly := clusterInstance.NewVttabletInstance("rdonly", 0, "")

	shard1Master := clusterInstance.NewVttabletInstance("replica", 0, "")
	shard1Replica := clusterInstance.NewVttabletInstance("replica", 0, "")
	shard1Rdonly := clusterInstance.NewVttabletInstance("rdonly", 0, "")

	shard2Master := clusterInstance.NewVttabletInstance("replica", 0, "")
	shard2Replica := clusterInstance.NewVttabletInstance("replica", 0, "")
	shard2Rdonly := clusterInstance.NewVttabletInstance("rdonly", 0, "")

	shard3Master := clusterInstance.NewVttabletInstance("replica", 0, "")
	shard3Replica := clusterInstance.NewVttabletInstance("replica", 0, "")
	shard3Rdonly := clusterInstance.NewVttabletInstance("rdonly", 0, "")

	shard0.Vttablets = []*cluster.Vttablet{shard0Master, shard0Replica, shard0Rdonly}
	shard1.Vttablets = []*cluster.Vttablet{shard1Master, shard1Replica, shard1Rdonly}
	shard2.Vttablets = []*cluster.Vttablet{shard2Master, shard2Replica, shard2Rdonly}
	shard3.Vttablets = []*cluster.Vttablet{shard3Master, shard3Replica, shard3Rdonly}

	clusterInstance.VtTabletExtraArgs = []string{
		"-vreplication_healthcheck_topology_refresh", "1s",
		"-vreplication_healthcheck_retry_delay", "1s",
		"-vreplication_retry_delay", "1s",
		"-degraded_threshold", "5s",
		"-lock_tables_timeout", "5s",
		"-watch_replication_stream",
		"-enable_semi_sync",
		"-enable_replication_reporter",
		"-enable-tx-throttler",
		"-binlog_use_v3_resharding_mode=true",
	}

	shardingColumnType := "bigint(20) unsigned"
	shardingKeyType := querypb.Type_UINT64

	if useVarbinaryShardingKeyType {
		shardingColumnType = "varbinary(64)"
		shardingKeyType = querypb.Type_VARBINARY
	}

	// Initialize Cluster
	err = clusterInstance.SetupCluster(keyspace, []cluster.Shard{*shard0, *shard1, *shard2, *shard3})
	require.NoError(t, err)
	assert.Equal(t, len(clusterInstance.Keyspaces[0].Shards), 4)

	//Start MySql
	var mysqlCtlProcessList []*exec.Cmd
	for _, shard := range clusterInstance.Keyspaces[0].Shards {
		for _, tablet := range shard.Vttablets {
			log.Infof("Starting MySql for tablet %v", tablet.Alias)
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

	// Rebuild keyspace Graph
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RebuildKeyspaceGraph", keyspaceName)
	require.NoError(t, err)

	// Get Keyspace and verify the structure
	srvKeyspace := sharding.GetSrvKeyspace(t, cell, keyspaceName, *clusterInstance)
	assert.Equal(t, "", srvKeyspace.GetShardingColumnName())

	//Start Tablets and Wait for the Process
	for _, shard := range clusterInstance.Keyspaces[0].Shards {
		for _, tablet := range shard.Vttablets {
			err = tablet.VttabletProcess.Setup()
			require.NoError(t, err)
		}
	}

	// Init Shard Master
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("InitShardMaster",
		"-force", fmt.Sprintf("%s/%s", keyspaceName, shard0.Name), shard0Master.Alias)
	require.NoError(t, err)
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("InitShardMaster",
		"-force", fmt.Sprintf("%s/%s", keyspaceName, shard1.Name), shard1Master.Alias)
	require.NoError(t, err)

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("InitShardMaster",
		"-force", fmt.Sprintf("%s/%s", keyspaceName, shard2.Name), shard2Master.Alias)
	require.NoError(t, err)

	// Init Shard Master on Merge Shard
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("InitShardMaster",
		"-force", fmt.Sprintf("%s/%s", keyspaceName, shard3.Name), shard3Master.Alias)
	require.NoError(t, err)

	// Wait for tablets to come in Service state
	err = shard0Master.VttabletProcess.WaitForTabletType("SERVING")
	require.NoError(t, err)
	err = shard1Master.VttabletProcess.WaitForTabletType("SERVING")
	require.NoError(t, err)
	err = shard2Master.VttabletProcess.WaitForTabletType("SERVING")
	require.NoError(t, err)
	err = shard3Master.VttabletProcess.WaitForTabletType("SERVING")
	require.NoError(t, err)

	// keyspace/shard name fields
	shard0Ks := fmt.Sprintf("%s/%s", keyspaceName, shard0.Name)
	shard1Ks := fmt.Sprintf("%s/%s", keyspaceName, shard1.Name)
	shard3Ks := fmt.Sprintf("%s/%s", keyspaceName, shard3.Name)

	// check for shards
	result, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("FindAllShardsInKeyspace", keyspaceName)
	require.NoError(t, err)
	resultMap := make(map[string]interface{})
	err = json.Unmarshal([]byte(result), &resultMap)
	require.NoError(t, err)
	assert.Equal(t, 4, len(resultMap), "No of shards should be 4")

	// Apply Schema
	err = clusterInstance.VtctlclientProcess.ApplySchema(keyspaceName, fmt.Sprintf(createTabletTemplate, "resharding1", shardingColumnType))
	require.NoError(t, err)

	// Apply VSchema
	err = clusterInstance.VtctlclientProcess.ApplyVSchema(keyspaceName, vSchema)
	require.NoError(t, err)

	// Insert Data
	insertStartupValues(t)

	// run a health check on source replicas so they respond to discovery
	// (for binlog players) and on the source rdonlys (for workers)
	for _, shard := range keyspace.Shards {
		for _, tablet := range shard.Vttablets {
			err = clusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", tablet.Alias)
			require.NoError(t, err)
		}
	}

	// Rebuild keyspace Graph
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RebuildKeyspaceGraph", keyspaceName)
	require.NoError(t, err)

	// check srv keyspace
	expectedPartitions := map[topodata.TabletType][]string{}
	expectedPartitions[topodata.TabletType_MASTER] = []string{shard0.Name, shard1.Name, shard2.Name}
	expectedPartitions[topodata.TabletType_REPLICA] = []string{shard0.Name, shard1.Name, shard2.Name}
	expectedPartitions[topodata.TabletType_RDONLY] = []string{shard0.Name, shard1.Name, shard2.Name}
	sharding.CheckSrvKeyspace(t, cell, keyspaceName, "", 0, expectedPartitions, *clusterInstance)

	// we need to create the schema, and the worker will do data copying
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("CopySchemaShard",
		shard0.Rdonly().Alias, fmt.Sprintf("%s/%s", keyspaceName, shard3.Name))
	require.NoError(t, err)

	// Run vtworker as daemon for the following SplitClone commands. -use_v3_resharding_mode default is true
	err = clusterInstance.StartVtworker(cell, "--command_display_interval", "10ms")
	require.NoError(t, err)

	// Initial clone (online).
	err = clusterInstance.VtworkerProcess.ExecuteCommand("SplitClone",
		"--offline=false",
		"--chunk_count", "10",
		"--min_rows_per_chunk", "1",
		"--min_healthy_rdonly_tablets", "1",
		"--max_tps", "9999",
		shard3Ks)
	require.NoError(t, err)

	// Check values in the merge shard
	checkValues(t, *shard3.MasterTablet(), []string{"INT64(86)", "INT64(1)", `VARCHAR("msg1")`, fmt.Sprintf("UINT64(%d)", key1)},
		1, true, tableName, fixedParentID, keyspaceName, shardingKeyType, nil)
	checkValues(t, *shard3.MasterTablet(), []string{"INT64(86)", "INT64(2)", `VARCHAR("msg2")`, fmt.Sprintf("UINT64(%d)", key2)},
		2, true, tableName, fixedParentID, keyspaceName, shardingKeyType, nil)

	// Reset vtworker such that we can run the next command.
	err = clusterInstance.VtworkerProcess.ExecuteCommand("Reset")
	require.NoError(t, err)

	// Delete row 2 (provokes an insert).
	_, err = shard3Master.VttabletProcess.QueryTablet("delete from resharding1 where id=2", keyspaceName, true)
	require.NoError(t, err)
	// Update row 3 (provokes an update).
	_, err = shard3Master.VttabletProcess.QueryTablet("update resharding1 set msg='msg-not-1' where id=1", keyspaceName, true)
	require.NoError(t, err)

	// Insert row 4  (provokes a delete).
	insertValue(t, shard3.MasterTablet(), keyspaceName, tableName, 4, "msg4", key3)

	err = clusterInstance.VtworkerProcess.ExecuteCommand(
		"SplitClone",
		"--chunk_count", "10",
		"--min_rows_per_chunk", "1",
		"--min_healthy_rdonly_tablets", "1",
		"--max_tps", "9999",
		shard3Ks)
	require.NoError(t, err)

	// Change tablet, which was taken offline, back to rdonly.
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeTabletType", shard0Rdonly.Alias, "rdonly")
	require.NoError(t, err)
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeTabletType", shard1Rdonly.Alias, "rdonly")
	require.NoError(t, err)

	// Terminate worker daemon because it is no longer needed.
	err = clusterInstance.VtworkerProcess.TearDown()
	require.NoError(t, err)

	// Check startup values
	checkStartupValues(t, shardingKeyType)

	// check the schema too
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ValidateSchemaKeyspace", keyspaceName)
	require.NoError(t, err)

	// Verify vreplication table entries
	qr, err := shard3.MasterTablet().VttabletProcess.QueryTabletWithDB("select * from vreplication", "_vt")
	require.NoError(t, err)
	assert.Equal(t, 2, len(qr.Rows))
	assert.Contains(t, fmt.Sprintf("%v", qr.Rows), "SplitClone")
	assert.Contains(t, fmt.Sprintf("%v", qr.Rows), `"keyspace:\"ks\" shard:\"-40\" key_range:<end:\"\\200\" > "`)
	assert.Contains(t, fmt.Sprintf("%v", qr.Rows), `"keyspace:\"ks\" shard:\"40-80\" key_range:<end:\"\\200\" > "`)

	// check the binlog players are running and exporting vars
	sharding.CheckDestinationMaster(t, *shard3Master, []string{shard1Ks, shard0Ks}, *clusterInstance)

	// When the binlog players/filtered replication is turned on, the query
	// service must be turned off on the destination masters.
	// The tested behavior is a safeguard to prevent that somebody can
	// accidentally modify data on the destination masters while they are not
	// migrated yet and the source shards are still the source of truth.
	err = shard3Master.VttabletProcess.WaitForTabletType("NOT_SERVING")
	require.NoError(t, err)

	// check that binlog server exported the stats vars
	sharding.CheckBinlogServerVars(t, *shard0Replica, 0, 0, false)
	sharding.CheckBinlogServerVars(t, *shard1Replica, 0, 0, false)

	// testing filtered replication: insert a bunch of data on shard 1, check we get most of it after a few seconds,
	// wait for binlog server timeout, check we get all of it.
	log.Debug("Inserting lots of data on source shard")
	insertLots(t, 100, 0, tableName, fixedParentID, keyspaceName)

	//Checking 100 percent of data is sent quickly
	assert.True(t, checkLotsTimeout(t, 100, 0, tableName, keyspaceName, shardingKeyType))

	sharding.CheckBinlogPlayerVars(t, *shard3Master, []string{shard1Ks, shard0Ks}, 30)

	sharding.CheckBinlogServerVars(t, *shard0Replica, 100, 100, false)
	sharding.CheckBinlogServerVars(t, *shard1Replica, 100, 100, false)

	// use vtworker to compare the data (after health-checking the destination
	// rdonly tablets so discovery works)
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", shard3Rdonly.Alias)
	require.NoError(t, err)

	// use vtworker to compare the data
	clusterInstance.VtworkerProcess.Cell = cell

	// Compare using SplitDiff
	log.Debug("Running vtworker SplitDiff")
	err = clusterInstance.VtworkerProcess.ExecuteVtworkerCommand(clusterInstance.GetAndReservePort(),
		clusterInstance.GetAndReservePort(),
		"--use_v3_resharding_mode=true",
		"SplitDiff",
		"--exclude_tables", "unrelated",
		"--min_healthy_rdonly_tablets", "1",
		"--source_uid", "1",
		shard3Ks)
	require.NoError(t, err)

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeTabletType", shard0Rdonly.Alias, "rdonly")
	require.NoError(t, err)
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeTabletType", shard3Rdonly.Alias, "rdonly")
	require.NoError(t, err)

	log.Debug("Running vtworker SplitDiff on second half")

	err = clusterInstance.VtworkerProcess.ExecuteVtworkerCommand(clusterInstance.GetAndReservePort(),
		clusterInstance.GetAndReservePort(),
		"--use_v3_resharding_mode=true",
		"SplitDiff",
		"--exclude_tables", "unrelated",
		"--min_healthy_rdonly_tablets", "1",
		"--source_uid", "2",
		shard3Ks)
	require.NoError(t, err)

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeTabletType", shard1Rdonly.Alias, "rdonly")
	require.NoError(t, err)
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeTabletType", shard3Rdonly.Alias, "rdonly")
	require.NoError(t, err)

	sharding.CheckTabletQueryService(t, *shard3Master, "NOT_SERVING", false, *clusterInstance)
	streamHealth, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput(
		"VtTabletStreamHealth",
		"-count", "1", shard3Master.Alias)
	require.NoError(t, err)
	log.Debug("Got health: ", streamHealth)

	var streamHealthResponse querypb.StreamHealthResponse
	err = json.Unmarshal([]byte(streamHealth), &streamHealthResponse)
	require.NoError(t, err)
	assert.Equal(t, streamHealthResponse.Serving, false)
	assert.NotNil(t, streamHealthResponse.RealtimeStats)

	// now serve rdonly from the split shards, in cell1 only
	err = clusterInstance.VtctlclientProcess.ExecuteCommand(
		"MigrateServedTypes", shard3Ks, "rdonly")
	require.NoError(t, err)

	// check srv keyspace
	expectedPartitions = map[topodata.TabletType][]string{}
	expectedPartitions[topodata.TabletType_MASTER] = []string{shard0.Name, shard1.Name, shard2.Name}
	expectedPartitions[topodata.TabletType_RDONLY] = []string{shard3.Name, shard2.Name}
	expectedPartitions[topodata.TabletType_REPLICA] = []string{shard0.Name, shard1.Name, shard2.Name}
	sharding.CheckSrvKeyspace(t, cell, keyspaceName, "", 0, expectedPartitions, *clusterInstance)

	sharding.CheckTabletQueryService(t, *shard0Rdonly, "NOT_SERVING", true, *clusterInstance)
	sharding.CheckTabletQueryService(t, *shard1Rdonly, "NOT_SERVING", true, *clusterInstance)

	// Now serve replica from the split shards
	err = clusterInstance.VtctlclientProcess.ExecuteCommand(
		"MigrateServedTypes", shard3Ks, "replica")
	require.NoError(t, err)

	expectedPartitions = map[topodata.TabletType][]string{}
	expectedPartitions[topodata.TabletType_MASTER] = []string{shard0.Name, shard1.Name, shard2.Name}
	expectedPartitions[topodata.TabletType_RDONLY] = []string{shard3.Name, shard2.Name}
	expectedPartitions[topodata.TabletType_REPLICA] = []string{shard3.Name, shard2.Name}
	sharding.CheckSrvKeyspace(t, cell, keyspaceName, "", 0, expectedPartitions, *clusterInstance)

	// now serve master from the split shards
	err = clusterInstance.VtctlclientProcess.ExecuteCommand(
		"MigrateServedTypes", shard3Ks, "master")
	require.NoError(t, err)

	expectedPartitions = map[topodata.TabletType][]string{}
	expectedPartitions[topodata.TabletType_MASTER] = []string{shard3.Name, shard2.Name}
	expectedPartitions[topodata.TabletType_RDONLY] = []string{shard3.Name, shard2.Name}
	expectedPartitions[topodata.TabletType_REPLICA] = []string{shard3.Name, shard2.Name}
	sharding.CheckSrvKeyspace(t, cell, keyspaceName, "", 0, expectedPartitions, *clusterInstance)

	sharding.CheckTabletQueryService(t, *shard0Master, "NOT_SERVING", true, *clusterInstance)
	sharding.CheckTabletQueryService(t, *shard1Master, "NOT_SERVING", true, *clusterInstance)

	// check destination shards are serving
	sharding.CheckTabletQueryService(t, *shard3Master, "SERVING", false, *clusterInstance)

	// check the binlog players are gone now
	err = shard3Master.VttabletProcess.WaitForBinLogPlayerCount(0)
	require.NoError(t, err)

	// delete the original tablets in the original shard
	var wg sync.WaitGroup
	for _, shard := range []cluster.Shard{*shard0, *shard1} {
		for _, tablet := range shard.Vttablets {
			wg.Add(1)
			go func(tablet *cluster.Vttablet) {
				defer wg.Done()
				_ = tablet.VttabletProcess.TearDown()
				_ = tablet.MysqlctlProcess.Stop()
			}(tablet)
		}
	}
	wg.Wait()

	for _, tablet := range []cluster.Vttablet{*shard0Replica, *shard1Replica, *shard0Rdonly, *shard1Rdonly} {
		err = clusterInstance.VtctlclientProcess.ExecuteCommand("DeleteTablet", tablet.Alias)
		require.NoError(t, err)
	}

	for _, tablet := range []cluster.Vttablet{*shard0Master, *shard1Master} {
		err = clusterInstance.VtctlclientProcess.ExecuteCommand("DeleteTablet", "-allow_master", tablet.Alias)
		require.NoError(t, err)
	}

	// rebuild the serving graph, all mentions of the old shards should be gone
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RebuildKeyspaceGraph", keyspaceName)
	require.NoError(t, err)

}

func insertStartupValues(t *testing.T) {
	insertSQL := fmt.Sprintf(insertTabletTemplateKsID, "resharding1", fixedParentID, 1, "msg1", key1, key1, 1)
	sharding.ExecuteOnTablet(t, insertSQL, *shard0.MasterTablet(), keyspaceName, false)

	insertSQL = fmt.Sprintf(insertTabletTemplateKsID, "resharding1", fixedParentID, 2, "msg2", key2, key2, 2)
	sharding.ExecuteOnTablet(t, insertSQL, *shard1.MasterTablet(), keyspaceName, false)

	insertSQL = fmt.Sprintf(insertTabletTemplateKsID, "resharding1", fixedParentID, 3, "msg3", key3, key3, 3)
	sharding.ExecuteOnTablet(t, insertSQL, *shard2.MasterTablet(), keyspaceName, false)
}

func insertValue(t *testing.T, tablet *cluster.Vttablet, keyspaceName string, tableName string, id int, msg string, ksID uint64) {
	insertSQL := fmt.Sprintf(insertTabletTemplateKsID, tableName, fixedParentID, id, msg, ksID, ksID, id)
	sharding.ExecuteOnTablet(t, insertSQL, *tablet, keyspaceName, false)
}

func checkStartupValues(t *testing.T, shardingKeyType querypb.Type) {
	for _, tablet := range shard3.Vttablets {
		checkValues(t, *tablet, []string{"INT64(86)", "INT64(1)", `VARCHAR("msg1")`, fmt.Sprintf("UINT64(%d)", key1)},
			1, true, "resharding1", fixedParentID, keyspaceName, shardingKeyType, nil)

		checkValues(t, *tablet, []string{"INT64(86)", "INT64(2)", `VARCHAR("msg2")`, fmt.Sprintf("UINT64(%d)", key2)},
			2, true, "resharding1", fixedParentID, keyspaceName, shardingKeyType, nil)
	}
}

// checkLotsTimeout waits till all values are inserted
func checkLotsTimeout(t *testing.T, count uint64, base uint64, table string, keyspaceName string, keyType querypb.Type) bool {
	timeout := time.Now().Add(10 * time.Second)
	for time.Now().Before(timeout) {
		percentFound := checkLots(t, count, base, table, keyspaceName, keyType)
		if percentFound == 100 {
			return true
		}
		time.Sleep(300 * time.Millisecond)
	}
	return false
}

func checkLots(t *testing.T, count uint64, base uint64, table string, keyspaceName string, keyType querypb.Type) float32 {
	shard3Replica := *shard3.Vttablets[1]

	ctx := context.Background()
	dbParams := getDBparams(shard3Replica, keyspaceName)
	dbConn, _ := mysql.Connect(ctx, &dbParams)
	defer dbConn.Close()

	var isFound bool
	var totalFound int
	var i uint64
	for i = 0; i < count; i++ {
		isFound = checkValues(t, shard3Replica, []string{"INT64(86)",
			fmt.Sprintf("INT64(%d)", 10000+base+i),
			fmt.Sprintf(`VARCHAR("msg-range0-%d")`, 10000+base+i),
			fmt.Sprintf("UINT64(%d)", key1)},
			10000+base+i, true, table, fixedParentID, keyspaceName, keyType, dbConn)
		if isFound {
			totalFound++
		}

		isFound = checkValues(t, shard3Replica, []string{"INT64(86)",
			fmt.Sprintf("INT64(%d)", 20000+base+i),
			fmt.Sprintf(`VARCHAR("msg-range1-%d")`, 20000+base+i),
			fmt.Sprintf("UINT64(%d)", key2)},
			20000+base+i, true, table, fixedParentID, keyspaceName, keyType, dbConn)
		if isFound {
			totalFound++
		}
	}
	return float32(totalFound * 100 / int(count) / 2)
}

func checkValues(t *testing.T, vttablet cluster.Vttablet, values []string, id uint64, exists bool, tableName string,
	parentID int, ks string, keyType querypb.Type, dbConn *mysql.Conn) bool {
	query := fmt.Sprintf("select parent_id, id, msg, custom_ksid_col from %s where parent_id = %d and id = %d", tableName, parentID, id)
	var result *sqltypes.Result
	var err error
	if dbConn != nil {
		result, err = dbConn.ExecuteFetch(query, 1000, true)
		require.NoError(t, err)
	} else {
		result, err = vttablet.VttabletProcess.QueryTablet(query, ks, true)
		require.NoError(t, err)
	}

	isFound := false
	if exists && len(result.Rows) > 0 {
		isFound = assert.Equal(t, result.Rows[0][0].String(), values[0])
		isFound = isFound && assert.Equal(t, result.Rows[0][1].String(), values[1])
		isFound = isFound && assert.Equal(t, result.Rows[0][2].String(), values[2])
		if keyType == querypb.Type_VARBINARY {
			r := strings.NewReplacer("UINT64(", "VARBINARY(\"", ")", "\")")
			expected := r.Replace(values[3])
			isFound = isFound && assert.Equal(t, result.Rows[0][3].String(), expected)
		} else {
			isFound = isFound && assert.Equal(t, result.Rows[0][3].String(), values[3])
		}

	} else {
		assert.Equal(t, len(result.Rows), 0)
	}
	return isFound
}

// insertLots inserts multiple values to vttablet
func insertLots(t *testing.T, count uint64, base uint64, table string, parentID int, ks string) {
	var query1, query2 string
	var i uint64
	for i = 0; i < count; i++ {
		query1 = fmt.Sprintf(insertTabletTemplateKsID, table, parentID, 10000+base+i,
			fmt.Sprintf("msg-range0-%d", 10000+base+i), key1, key1, 10000+base+i)
		query2 = fmt.Sprintf(insertTabletTemplateKsID, table, parentID, 20000+base+i,
			fmt.Sprintf("msg-range1-%d", 20000+base+i), key2, key2, 20000+base+i)

		sharding.ExecuteOnTablet(t, query1, *shard0.MasterTablet(), ks, false)
		sharding.ExecuteOnTablet(t, query2, *shard1.MasterTablet(), ks, false)
	}
}

func getDBparams(vttablet cluster.Vttablet, ks string) mysql.ConnParams {
	dbParams := mysql.ConnParams{
		Uname:      "vt_dba",
		UnixSocket: path.Join(vttablet.VttabletProcess.Directory, "mysql.sock"),
		DbName:     "vt_" + ks,
	}
	return dbParams
}
