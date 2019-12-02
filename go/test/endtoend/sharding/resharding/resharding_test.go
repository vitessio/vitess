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

package resharding

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/common/log"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/sharding"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/topodata"

	"github.com/stretchr/testify/assert"
)

var (
	// ClusterInstance instance to be used for test with different params
	clusterInstance      *cluster.LocalProcessCluster
	hostname             = "localhost"
	keyspaceName         = "ks"
	cell1                = "zone1"
	cell2                = "zone2"
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
	createTableBindataTemplate = `
							create table %s(
							custom_ksid_col %s not null,
							id bigint not null,
							parent_id bigint not null,
							msg bit(8),
							primary key (parent_id, id),
							index by_msg (msg)
							) Engine=InnoDB;
							`
	createViewTemplate = `
		create view %s (parent_id, id, msg, custom_ksid_col) as select parent_id, id, msg, custom_ksid_col from %s;
		`
	createTimestampTable = `
							create table timestamps(
							id int not null,
							time_milli bigint(20) unsigned not null,
							custom_ksid_col %s not null,
							primary key (id)
							) Engine=InnoDB;
							`
	// Make sure that clone and diff work with tables which have no primary key.
	// RBR only because Vitess requires the primary key for query rewrites if
	// it is running with statement based replication.
	createNoPkTable = `
							create table no_pk(
							custom_ksid_col %s not null,
							msg varchar(64),
							id bigint not null,
							parent_id bigint not null
							) Engine=InnoDB;
							`
	createUnrelatedTable = `
							create table unrelated(
							name varchar(64),
							primary key (name)
							) Engine=InnoDB;
							`

	insertTabletTemplate = `insert into %s(parent_id, id, msg, custom_ksid_col) values(%d, %d, "%s", "%s")`
	fixedParentID        = 86
	tableName            = "resharding1"
	vSchema              = `
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
										  "column": "%s",
										  "name": "hash_index"
										}
									  ] 
									}
								  }
								}
							`

	// initial shards
	// range '' - 80  &  range 80 - ''
	shard0 = &cluster.Shard{Name: "-80"}
	shard1 = &cluster.Shard{Name: "80-"}

	// split shards
	// range 80 - c0    &   range c0 - ''
	shard2 = &cluster.Shard{Name: "80-c0"}
	shard3 = &cluster.Shard{Name: "c0-"}
)

// TestReSharding - main test with accepts different params for various test
func TestReSharding(t *testing.T) {
	clusterInstance = &cluster.LocalProcessCluster{Cell: cell1, Hostname: hostname}
	defer clusterInstance.Teardown()

	// Launch keyspace
	keyspace := &cluster.Keyspace{Name: keyspaceName}

	// Start topo server
	err := clusterInstance.StartTopo()
	assert.Nil(t, err, "error should be Nil")

	// Adding another cell in the same cluster
	err = clusterInstance.TopoProcess.ManageTopoDir("mkdir", "/vitess/"+cell2)
	err = clusterInstance.VtctlProcess.AddCellInfo(cell2)
	assert.Nil(t, err, "error should be Nil")

	// Defining all the tablets
	shard0Master := clusterInstance.GetVttabletInstance("replica", 0, "")
	shard0Replica := clusterInstance.GetVttabletInstance("replica", 0, "")
	shard0RdonlyZ2 := clusterInstance.GetVttabletInstance("rdonly", 0, cell2)

	shard1Master := clusterInstance.GetVttabletInstance("replica", 0, "")
	shard1Replica1 := clusterInstance.GetVttabletInstance("replica", 0, "")
	shard1Replica2 := clusterInstance.GetVttabletInstance("replica", 0, "")
	shard1Rdonly := clusterInstance.GetVttabletInstance("rdonly", 0, "")
	shard1RdonlyZ2 := clusterInstance.GetVttabletInstance("rdonly", 0, cell2)

	shard2Master := clusterInstance.GetVttabletInstance("replica", 0, "")
	shard2Replica1 := clusterInstance.GetVttabletInstance("replica", 0, "")
	shard2Replica2 := clusterInstance.GetVttabletInstance("replica", 0, "")
	shard2Rdonly := clusterInstance.GetVttabletInstance("rdonly", 0, "")

	shard3Master := clusterInstance.GetVttabletInstance("replica", 0, "")
	shard3Replica := clusterInstance.GetVttabletInstance("replica", 0, "")
	shard3Rdonly := clusterInstance.GetVttabletInstance("rdonly", 0, "")

	shard0.Vttablets = []*cluster.Vttablet{shard0Master, shard0Replica, shard0RdonlyZ2}
	shard1.Vttablets = []*cluster.Vttablet{shard1Master, shard1Replica1, shard1Replica2, shard1Rdonly, shard1RdonlyZ2}
	shard2.Vttablets = []*cluster.Vttablet{shard2Master, shard2Replica1, shard2Replica2, shard2Rdonly}
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
	}

	shardingColumnType := "bigint(20) unsigned"
	shardingKeyIdType := "uint64"
	shardingKeyType := topodata.KeyspaceIdType_UINT64

	// Initialize Cluster
	err = clusterInstance.LaunchCluster(keyspace, []cluster.Shard{*shard0, *shard1, *shard2, *shard3})
	assert.Nil(t, err, "error should be Nil")
	assert.Equal(t, len(clusterInstance.Keyspaces[0].Shards), 4)

	fmt.Println(clusterInstance.TmpDirectory)

	// Set Sharding column
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("SetKeyspaceShardingInfo",
		"-force", keyspaceName, "custom_ksid_col", shardingKeyIdType)
	assert.Nil(t, err, "error should be Nil")

	//Start MySql
	for _, shard := range clusterInstance.Keyspaces[0].Shards {
		for _, tablet := range shard.Vttablets {
			fmt.Println("Starting MySql for tablet ", tablet.Alias)
			if err = tablet.MysqlctlProcess.Start(); err != nil {
				t.Fatal(err)
			}
		}
	}

	// Rebuild keyspace Graph
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RebuildKeyspaceGraph", keyspaceName)
	assert.Nil(t, err, "error should be Nil")
	// Get Keyspace and verify the structure
	srvKeyspace := sharding.GetSrvKeyspace(t, cell1, keyspaceName, *clusterInstance)
	fmt.Println(srvKeyspace)
	assert.Equal(t, "custom_ksid_col", srvKeyspace.GetShardingColumnName())

	//Start Tablets and Wait for the Process
	for _, shard := range clusterInstance.Keyspaces[0].Shards {
		for _, tablet := range shard.Vttablets {
			err = tablet.VttabletProcess.Setup()
			assert.Nil(t, err, "error should be Nil")
			// Create Database
			tablet.VttabletProcess.QueryTablet(fmt.Sprintf("create database vt_%s", keyspace.Name), keyspace.Name, false)
		}
	}

	// Init Shard Master
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("InitShardMaster",
		"-force", fmt.Sprintf("%s/%s", keyspaceName, shard0.Name), shard0.MasterTablet().Alias)

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("InitShardMaster",
		"-force", fmt.Sprintf("%s/%s", keyspaceName, shard1.Name), shard1.MasterTablet().Alias)

	// Init Shard Master on Split Shards
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("InitShardMaster",
		"-force", fmt.Sprintf("%s/%s", keyspaceName, shard2.Name), shard2.MasterTablet().Alias)

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("InitShardMaster",
		"-force", fmt.Sprintf("%s/%s", keyspaceName, shard3.Name), shard3.MasterTablet().Alias)

	shard0.MasterTablet().VttabletProcess.WaitForTabletType("SERVING")
	shard1.MasterTablet().VttabletProcess.WaitForTabletType("SERVING")
	shard2.MasterTablet().VttabletProcess.WaitForTabletType("SERVING")
	shard3.MasterTablet().VttabletProcess.WaitForTabletType("SERVING")

	// check for shards
	result, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("FindAllShardsInKeyspace", keyspaceName)
	fmt.Println(result)
	resultMap := make(map[string]interface{})
	err = json.Unmarshal([]byte(result), &resultMap)
	assert.Nil(t, err, "error should be Nil")
	assert.Equal(t, 4, len(resultMap), "No of shards should be 4")

	// Apply Schema
	_ = clusterInstance.VtctlclientProcess.ApplySchema(keyspaceName, fmt.Sprintf(createTabletTemplate, "resharding1", shardingColumnType))
	_ = clusterInstance.VtctlclientProcess.ApplySchema(keyspaceName, fmt.Sprintf(createTabletTemplate, "resharding2", shardingColumnType))
	_ = clusterInstance.VtctlclientProcess.ApplySchema(keyspaceName, fmt.Sprintf(createTableBindataTemplate, "resharding3", shardingColumnType))
	_ = clusterInstance.VtctlclientProcess.ApplySchema(keyspaceName, fmt.Sprintf(createViewTemplate, "view1", "resharding3"))
	_ = clusterInstance.VtctlclientProcess.ApplySchema(keyspaceName, fmt.Sprintf(createTimestampTable, shardingColumnType))
	_ = clusterInstance.VtctlclientProcess.ApplySchema(keyspaceName, fmt.Sprintf(createUnrelatedTable))

	// Insert Data
	insertStartupValues()

	// run a health check on source replicas so they respond to discovery
	// (for binlog players) and on the source rdonlys (for workers)
	for _, shard := range keyspace.Shards {
		for _, tablet := range shard.Vttablets {
			err = clusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", tablet.Alias)
			assert.Nil(t, err, "error should be Nil")
		}
	}

	// Rebuild keyspace Graph
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RebuildKeyspaceGraph", keyspaceName)
	assert.Nil(t, err, "error should be Nil")

	// check srv keyspace
	expectedPartitions := map[topodata.TabletType][]string{}
	expectedPartitions[topodata.TabletType_MASTER] = []string{shard0.Name, shard1.Name}
	expectedPartitions[topodata.TabletType_REPLICA] = []string{shard0.Name, shard1.Name}
	expectedPartitions[topodata.TabletType_RDONLY] = []string{shard0.Name, shard1.Name}
	checkSrvKeyspaceForSharding(t, cell1, expectedPartitions, shardingKeyType)

	// disable shard1.replica2, so we're sure filtered replication will go from shard1.replica1
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeSlaveType", shard1Replica2.Alias, "spare")
	assert.Nil(t, err, "error should be Nil")
	shard1Replica2.VttabletProcess.WaitForTabletType("NOT_SERVING")

	// we need to create the schema, and the worker will do data copying
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("CopySchemaShard", "--exclude_tables", "unrelated",
		shard1.Rdonly().Alias, fmt.Sprintf("%s/%s", keyspaceName, shard2.Name))
	assert.Nil(t, err, "error should be Nil")
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("CopySchemaShard", "--exclude_tables", "unrelated",
		shard1.Rdonly().Alias, fmt.Sprintf("%s/%s", keyspaceName, shard3.Name))
	assert.Nil(t, err, "error should be Nil")

	// Run vtworker as daemon for the following SplitClone commands.
	err = clusterInstance.StartVtworker(cell1, "--command_display_interval", "10ms", "--use_v3_resharding_mode=false")
	assert.Nil(t, err, "error should be Nil")

	// Copy the data from the source to the destination shards.
	// --max_tps is only specified to enable the throttler and ensure that the
	// code is executed. But the intent here is not to throttle the test, hence
	// the rate limit is set very high.

	// Initial clone (online).
	_ = clusterInstance.VtworkerProcess.ExecuteCommand("SplitClone",
		"--offline=false",
		"--exclude_tables", "unrelated",
		"--chunk_count", "10",
		"--min_rows_per_chunk", "1",
		"--min_healthy_rdonly_tablets", "1",
		"--max_tps", "9999",
		fmt.Sprintf("%s/%s", keyspaceName, shard1.Name))

	vtworkerURL := fmt.Sprintf("http://localhost:%d/debug/vars", clusterInstance.VtworkerProcess.Port)
	sharding.VerifyReconciliationCounters(t, vtworkerURL, "Online", tableName, 2, 0, 0, 0)

	// Reset vtworker such that we can run the next command.
	err = clusterInstance.VtworkerProcess.ExecuteCommand("Reset")

	// Test the correct handling of keyspace_id changes which happen after the first clone.
	// Let row 2 go to shard 3 instead of shard 2.
	sql := "update resharding1 set custom_ksid_col=0xD000000000000000 WHERE id=2"
	_, _ = shard1.MasterTablet().VttabletProcess.QueryTablet(sql, keyspaceName, true)

	_ = clusterInstance.VtworkerProcess.ExecuteCommand("SplitClone",
		"--offline=false",
		"--exclude_tables", "unrelated",
		"--chunk_count", "10",
		"--min_rows_per_chunk", "1",
		"--min_healthy_rdonly_tablets", "1",
		"--max_tps", "9999",
		fmt.Sprintf("%s/%s", keyspaceName, shard1.Name))

	sharding.VerifyReconciliationCounters(t, vtworkerURL, "Online", tableName, 1, 0, 1, 1)

	sharding.CheckValues(t, *shard2.MasterTablet(), []string{"INT64(86)", "INT64(2)", `VARCHAR("msg2")`, sharding.HexToDbStr(0xD000000000000000, shardingKeyType)},
		2, false, tableName, fixedParentID, keyspaceName, shardingKeyType)
	sharding.CheckValues(t, *shard3.MasterTablet(), []string{"INT64(86)", "INT64(2)", `VARCHAR("msg2")`, sharding.HexToDbStr(0xD000000000000000, shardingKeyType)},
		2, true, tableName, fixedParentID, keyspaceName, shardingKeyType)

	err = clusterInstance.VtworkerProcess.ExecuteCommand("Reset")

	// Move row 2 back to shard 2 from shard 3 by changing the keyspace_id again
	sql = "update resharding1 set custom_ksid_col=0x9000000000000000 WHERE id=2"
	_, _ = shard1.MasterTablet().VttabletProcess.QueryTablet(sql, keyspaceName, true)

	_ = clusterInstance.VtworkerProcess.ExecuteCommand("SplitClone",
		"--offline=false",
		"--exclude_tables", "unrelated",
		"--chunk_count", "10",
		"--min_rows_per_chunk", "1",
		"--min_healthy_rdonly_tablets", "1",
		"--max_tps", "9999",
		fmt.Sprintf("%s/%s", keyspaceName, shard1.Name))

	sharding.VerifyReconciliationCounters(t, vtworkerURL, "Online", tableName, 1, 0, 1, 1)

	sharding.CheckValues(t, *shard2.MasterTablet(), []string{"INT64(86)", "INT64(2)", `VARCHAR("msg2")`, sharding.HexToDbStr(0x9000000000000000, shardingKeyType)},
		2, true, tableName, fixedParentID, keyspaceName, shardingKeyType)
	sharding.CheckValues(t, *shard3.MasterTablet(), []string{"INT64(86)", "INT64(2)", `VARCHAR("msg2")`, sharding.HexToDbStr(0x9000000000000000, shardingKeyType)},
		2, false, tableName, fixedParentID, keyspaceName, shardingKeyType)

	// Reset vtworker such that we can run the next command.
	err = clusterInstance.VtworkerProcess.ExecuteCommand("Reset")

	// Modify the destination shard. SplitClone will revert the changes.

	// Delete row 2 (provokes an insert).
	_, _ = shard2.MasterTablet().VttabletProcess.QueryTablet("delete from resharding1 where id=2", keyspaceName, true)
	// Update row 3 (provokes an update).
	_, _ = shard3.MasterTablet().VttabletProcess.QueryTablet("update resharding1 set msg='msg-not-3' where id=3", keyspaceName, true)

	// Insert row 4 and 5 (provokes a delete).
	insertValue(shard3.MasterTablet(), keyspaceName, tableName, 4, "msg4", 0xD000000000000000)
	insertValue(shard3.MasterTablet(), keyspaceName, tableName, 5, "msg5", 0xD000000000000000)

	_ = clusterInstance.VtworkerProcess.ExecuteCommand("SplitClone",
		"--exclude_tables", "unrelated",
		"--chunk_count", "10",
		"--min_rows_per_chunk", "1",
		"--min_healthy_rdonly_tablets", "1",
		"--max_tps", "9999",
		fmt.Sprintf("%s/%s", keyspaceName, shard1.Name))

	// Change tablet, which was taken offline, back to rdonly.
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeSlaveType", shard1Rdonly.Alias, "rdonly")
	assert.Nil(t, err, "error should be Nil")

	sharding.VerifyReconciliationCounters(t, vtworkerURL, "Online", tableName, 1, 1, 2, 0)
	// TODO: check back
	sharding.VerifyReconciliationCounters(t, vtworkerURL, "Offline", tableName, 0, 0, 0, 2)

	// Terminate worker daemon because it is no longer needed.
	_ = clusterInstance.VtworkerProcess.TearDown()

	// Check startup values
	checkStartupValues(t, shardingKeyType)

	// check the schema too
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ValidateSchemaKeyspace", "--exclude_tables=unrelated", keyspaceName)
	assert.Nil(t, err, "error should be Nil")

	// Verify vreplication table entries
	qr, err := shard2.MasterTablet().VttabletProcess.QueryTabletWithDB("select * from vreplication", "_vt")
	assert.Nil(t, err, "error should be Nil")
	fmt.Println(qr)
	assert.Equal(t, 1, len(qr.Rows))
	assert.Contains(t, fmt.Sprintf("%v", qr.Rows), "SplitClone")
	assert.Contains(t, fmt.Sprintf("%v", qr.Rows), `"keyspace:\"ks\" shard:\"80-\" key_range:<start:\"\\200\" end:\"\\300\" > "`)

	qr, err = shard3.MasterTablet().VttabletProcess.QueryTabletWithDB("select * from vreplication", "_vt")
	assert.Nil(t, err, "error should be Nil")
	assert.Equal(t, 1, len(qr.Rows))
	assert.Contains(t, fmt.Sprintf("%v", qr.Rows), "SplitClone")
	assert.Contains(t, fmt.Sprintf("%v", qr.Rows), `"keyspace:\"ks\" shard:\"80-\" key_range:<start:\"\\300\" > "`)

	// check the binlog players are running and exporting vars
	shard1Ks := fmt.Sprintf("%s/%s", keyspaceName, shard1.Name)
	sharding.CheckDestinationMaster(t, *shard2.MasterTablet(), []string{shard1Ks}, *clusterInstance)
	sharding.CheckDestinationMaster(t, *shard3.MasterTablet(), []string{shard1Ks}, *clusterInstance)

	// When the binlog players/filtered replication is turned on, the query
	// service must be turned off on the destination masters.
	// The tested behavior is a safeguard to prevent that somebody can
	// accidentally modify data on the destination masters while they are not
	// migrated yet and the source shards are still the source of truth.
	shard2.MasterTablet().VttabletProcess.WaitForTabletType("NOT_SERVING")
	shard3.MasterTablet().VttabletProcess.WaitForTabletType("NOT_SERVING")

	// check that binlog server exported the stats vars
	sharding.CheckBinlogServerVars(t, *shard1.Replica(), 0, 0)

	// Check that the throttler was enabled.
	// The stream id is hard-coded as 1, which is the first id generated
	// through auto-inc.
	//TODO: to complete
	//sharding.CheckThrottlerService(t, *shard2.MasterTablet(), ['BinlogPlayer/1'], 9999)
	//sharding.CheckThrottlerService(t, *shard3.MasterTablet(), ['BinlogPlayer/1'], 9999)
	//self.check_throttler_service(shard_2_master.rpc_endpoint(),
	//['BinlogPlayer/1'], 9999)

	// testing filtered replication: insert a bunch of data on shard 1,
	// check we get most of it after a few seconds, wait for binlog server
	// timeout, check we get all of it.
	//logging.debug('Inserting lots of data on source shard')
	sharding.InsertLots(1000, 0, *shard1.MasterTablet(), tableName, fixedParentID, keyspaceName)
	//logging.debug('Executing MultiValue Insert Queries')
	execMultiShardDmls(keyspaceName)

	// Checking 80 percent of data is sent quickly
	assert.True(t, checkLotsTimeout(t, 1000, 0, tableName, keyspaceName, shardingKeyType))
	// Checking no data was sent the wrong way
	checkLotsNotPresent(t, 1000, 0, tableName, keyspaceName, shardingKeyType)

	// Checking MultiValue Insert Queries
	checkMultiShardValues(t, keyspaceName, shardingKeyType)
	sharding.CheckBinlogPlayerVars(t, *shard2.MasterTablet(), []string{shard1Ks}, 30)
	sharding.CheckBinlogPlayerVars(t, *shard3.MasterTablet(), []string{shard1Ks}, 30)
	//TODO: verify again
	//sharding.CheckBinlogServerVars(t,*shard1.Replica(),1000,1000)

	// use vtworker to compare the data (after health-checking the destination
	// rdonly tablets so discovery works)
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", shard3Rdonly.Alias)
	assert.Nil(t, err, "error should be Nil")

	// use vtworker to compare the data
	useMultiSplitDiff := false
	clusterInstance.VtworkerProcess.Cell = cell1
	if useMultiSplitDiff {
		log.Debug("Running vtworker MultiSplitDiff")
		err = clusterInstance.VtworkerProcess.ExecuteVtworkerCommand(clusterInstance.GetAndReservePort(),
			clusterInstance.GetAndReservePort(),
			"--use_v3_resharding_mode=false",
			"MultiSplitDiff",
			"--exclude_tables", "unrelated",
			"--min_healthy_rdonly_tablets", "1",
			fmt.Sprintf("%s/%s", keyspaceName, shard3.Name))
		assert.Nil(t, err, "error should be Nil")
	} else {
		log.Debug("Running vtworker SplitDiff")
		err = clusterInstance.VtworkerProcess.ExecuteVtworkerCommand(clusterInstance.GetAndReservePort(),
			clusterInstance.GetAndReservePort(),
			"--use_v3_resharding_mode=false",
			"SplitDiff",
			"--exclude_tables", "unrelated",
			"--min_healthy_rdonly_tablets", "1",
			fmt.Sprintf("%s/%s", keyspaceName, shard3.Name))
		assert.Nil(t, err, "error should be Nil")
	}

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeSlaveType", shard1Rdonly.Alias, "rdonly")
	assert.Nil(t, err, "error should be Nil")
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeSlaveType", shard3Rdonly.Alias, "rdonly")
	assert.Nil(t, err, "error should be Nil")

	// get status for destination master tablets, make sure we have it all
	useRBR := false
	if useRBR {
		// We submitted non-annotated DMLs, that are properly routed
		// with RBR, but not with SBR. So the first shard counts
		// are smaller. In the second shard, we submitted statements
		// that affect more than one keyspace id. These will result
		// in two queries with RBR. So the count there is higher.
		sharding.CheckRunningBinlogPlayer(t, *shard2.MasterTablet(), 4036, 2016)
		sharding.CheckRunningBinlogPlayer(t, *shard3.MasterTablet(), 4056, 2016)
	} else {
		sharding.CheckRunningBinlogPlayer(t, *shard2.MasterTablet(), 4044, 2016)
		sharding.CheckRunningBinlogPlayer(t, *shard3.MasterTablet(), 4048, 2016)
	}

	// start a thread to insert data into shard_1 in the background
	// with current time, and monitor the delay
	// TODO: revist
	//insert_thread_1 = InsertThread(shard_1_master, 'insert_low', 1, 10000,
	//	0x9000000000000000)
	//insert_thread_2 = InsertThread(shard_1_master, 'insert_high', 2, 10001,
	//	0xD000000000000000)
	//monitor_thread_1 = MonitorLagThread(shard_2_replica2, 'insert_low', 1)
	//monitor_thread_2 = MonitorLagThread(shard_3_replica, 'insert_high', 2)

	// tests a failover switching serving to a different replica
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeSlaveType", shard1Replica2.Alias, "replica")
	assert.Nil(t, err, "error should be Nil")
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeSlaveType", shard1Replica1.Alias, "spare")
	assert.Nil(t, err, "error should be Nil")
	shard1Replica2.VttabletProcess.WaitForTabletType("SERVING")
	shard1Replica1.VttabletProcess.WaitForTabletType("NOT_SERVING")

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", shard1Replica2.Alias)
	assert.Nil(t, err, "error should be Nil")

	// test data goes through again
	log.Debug("Inserting lots of data on source shard")
	sharding.InsertLots(1000, 1000, *shard1.MasterTablet(), tableName, fixedParentID, keyspaceName)
	log.Debug("Checking 80 percent of data was sent quickly")
	assert.True(t, checkLotsTimeout(t, 1000, 1000, tableName, keyspaceName, shardingKeyType))
	sharding.CheckBinlogServerVars(t, *shard1Replica2, 800, 800)

	// check we can't migrate the master just yet
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("MigrateServedTypes", shard1Ks, "master")
	assert.NotNil(t, err, "MigrateServedTypes should fail")

	// check query service is off on master 2 and master 3, as filtered
	// replication is enabled. Even health check that is enabled on
	// master 3 should not interfere (we run it to be sure).
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", shard3Master.Alias)
	assert.Nil(t, err, "error should be Nil")

	for _, master := range []cluster.Vttablet{*shard2Master, *shard3Master} {
		sharding.CheckTabletQueryService(t, master, "NOT_SERVING", false, *clusterInstance)
		streamHealth, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("VtTabletStreamHealth", "-count", "1", master.Alias)
		assert.Nil(t, err)
		log.Debug("Got health: ", streamHealth)

		var streamHealthResponse querypb.StreamHealthResponse
		err = json.Unmarshal([]byte(streamHealth), &streamHealthResponse)
		assert.Nil(t, err, "error should be Nil")
		assert.Equal(t, streamHealthResponse.Serving, false)
		assert.NotNil(t, streamHealthResponse.RealtimeStats)

	}

	// check the destination master 3 is healthy, even though its query
	// service is not running (if not healthy this would exception out)
	//shard3Master.get_healthz() //TODO: check

	// now serve rdonly from the split shards, in cell1 only
	err = clusterInstance.VtctlclientProcess.ExecuteCommand(
		"MigrateServedTypes", fmt.Sprintf("--cells=%s", cell1),
		shard1Ks, "rdonly")
	assert.Nil(t, err, "error should be Nil")

	// check srv keyspace
	expectedPartitions = map[topodata.TabletType][]string{}
	expectedPartitions[topodata.TabletType_MASTER] = []string{shard0.Name, shard1.Name}
	expectedPartitions[topodata.TabletType_RDONLY] = []string{shard0.Name, shard2.Name, shard3.Name}
	expectedPartitions[topodata.TabletType_REPLICA] = []string{shard0.Name, shard1.Name}
	checkSrvKeyspaceForSharding(t, cell1, expectedPartitions, shardingKeyType)

	// Cell 2 is not affected
	expectedPartitions = map[topodata.TabletType][]string{}
	expectedPartitions[topodata.TabletType_MASTER] = []string{shard0.Name, shard1.Name}
	expectedPartitions[topodata.TabletType_RDONLY] = []string{shard0.Name, shard1.Name}
	expectedPartitions[topodata.TabletType_REPLICA] = []string{shard0.Name, shard1.Name}
	checkSrvKeyspaceForSharding(t, cell2, expectedPartitions, shardingKeyType)

	sharding.CheckTabletQueryService(t, *shard0RdonlyZ2, "SERVING", false, *clusterInstance)
	sharding.CheckTabletQueryService(t, *shard1RdonlyZ2, "SERVING", false, *clusterInstance)
	sharding.CheckTabletQueryService(t, *shard1Rdonly, "NOT_SERVING", true, *clusterInstance)

	// Shouldn't be able to rebuild keyspace graph while migration is on going
	// (i.e there are records that have tablet controls set)
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RebuildKeyspaceGraph", keyspaceName)
	assert.NotNil(t, err, "Error expected")

	// rerun migrate to ensure it doesn't fail
	// skip refresh to make it go faster
	err = clusterInstance.VtctlclientProcess.ExecuteCommand(
		"MigrateServedTypes",
		fmt.Sprintf("--cells=%s", cell1),
		"-skip-refresh-state=true",
		shard1Ks, "rdonly")
	assert.Nil(t, err, "error should be Nil")

	// now serve rdonly from the split shards, everywhere
	err = clusterInstance.VtctlclientProcess.ExecuteCommand(
		"MigrateServedTypes",
		shard1Ks, "rdonly")
	assert.Nil(t, err, "error should be Nil")

	expectedPartitions = map[topodata.TabletType][]string{}
	expectedPartitions[topodata.TabletType_MASTER] = []string{shard0.Name, shard1.Name}
	expectedPartitions[topodata.TabletType_RDONLY] = []string{shard0.Name, shard2.Name, shard3.Name}
	expectedPartitions[topodata.TabletType_REPLICA] = []string{shard0.Name, shard1.Name}
	checkSrvKeyspaceForSharding(t, cell1, expectedPartitions, shardingKeyType)
	// Cell 2 is also changed
	checkSrvKeyspaceForSharding(t, cell2, expectedPartitions, shardingKeyType)

	sharding.CheckTabletQueryService(t, *shard0RdonlyZ2, "SERVING", false, *clusterInstance)
	sharding.CheckTabletQueryService(t, *shard1RdonlyZ2, "NOT_SERVING", true, *clusterInstance)
	sharding.CheckTabletQueryService(t, *shard1Rdonly, "NOT_SERVING", true, *clusterInstance)

	// rerun migrate to ensure it doesn't fail
	// skip refresh to make it go faster
	err = clusterInstance.VtctlclientProcess.ExecuteCommand(
		"MigrateServedTypes",
		"-skip-refresh-state=true",
		shard1Ks, "rdonly")
	assert.Nil(t, err, "error should be Nil")

	// then serve replica from the split shards
	destinationShards := []cluster.Shard{*shard2, *shard3}
	err = clusterInstance.VtctlclientProcess.ExecuteCommand(
		"MigrateServedTypes", shard1Ks, "replica")
	assert.Nil(t, err, "error should be Nil")

	expectedPartitions = map[topodata.TabletType][]string{}
	expectedPartitions[topodata.TabletType_MASTER] = []string{shard0.Name, shard1.Name}
	expectedPartitions[topodata.TabletType_RDONLY] = []string{shard0.Name, shard2.Name, shard3.Name}
	expectedPartitions[topodata.TabletType_REPLICA] = []string{shard0.Name, shard2.Name, shard3.Name}
	checkSrvKeyspaceForSharding(t, cell1, expectedPartitions, shardingKeyType)

	// move replica back and forth
	err = clusterInstance.VtctlclientProcess.ExecuteCommand(
		"MigrateServedTypes", "-reverse",
		shard1Ks, "replica")
	assert.Nil(t, err, "error should be Nil")

	// After a backwards migration, queryservice should be enabled on
	// source and disabled on destinations
	sharding.CheckTabletQueryService(t, *shard1Replica2, "SERVING", false, *clusterInstance)

	// Destination tablets would have query service disabled for other reasons than the migration,
	// so check the shard record instead of the tablets directly.
	sharding.CheckShardQueryServices(t, *clusterInstance, destinationShards, cell1, keyspaceName,
		topodata.TabletType_REPLICA, false)
	sharding.CheckShardQueryServices(t, *clusterInstance, destinationShards, cell2, keyspaceName,
		topodata.TabletType_REPLICA, false)

	expectedPartitions = map[topodata.TabletType][]string{}
	expectedPartitions[topodata.TabletType_MASTER] = []string{shard0.Name, shard1.Name}
	expectedPartitions[topodata.TabletType_RDONLY] = []string{shard0.Name, shard2.Name, shard3.Name}
	expectedPartitions[topodata.TabletType_REPLICA] = []string{shard0.Name, shard1.Name}
	checkSrvKeyspaceForSharding(t, cell1, expectedPartitions, shardingKeyType)

	err = clusterInstance.VtctlclientProcess.ExecuteCommand(
		"MigrateServedTypes", shard1Ks, "replica")
	assert.Nil(t, err, "error should be Nil")
	// After a forwards migration, queryservice should be disabled on
	// source and enabled on destinations
	sharding.CheckTabletQueryService(t, *shard1Replica2, "NOT_SERVING", true, *clusterInstance)
	// Destination tablets would have query service disabled for other reasons than the migration,
	// so check the shard record instead of the tablets directly
	sharding.CheckShardQueryServices(t, *clusterInstance, destinationShards, cell1, keyspaceName,
		topodata.TabletType_REPLICA, true)
	sharding.CheckShardQueryServices(t, *clusterInstance, destinationShards, cell2, keyspaceName,
		topodata.TabletType_REPLICA, true)

	expectedPartitions = map[topodata.TabletType][]string{}
	expectedPartitions[topodata.TabletType_MASTER] = []string{shard0.Name, shard1.Name}
	expectedPartitions[topodata.TabletType_RDONLY] = []string{shard0.Name, shard2.Name, shard3.Name}
	expectedPartitions[topodata.TabletType_REPLICA] = []string{shard0.Name, shard2.Name, shard3.Name}
	checkSrvKeyspaceForSharding(t, cell1, expectedPartitions, shardingKeyType)

	//
	//// reparent shard_2 to shard_2_replica1, then insert more data and
	//// see it flow through still
	//shard2Ks := fmt.Sprintf("%s/%s", keyspaceName, shard2.Name)
	//err = clusterInstance.VtctlclientProcess.ExecuteCommand("PlannedReparentShard", "-keyspace_shard", shard2Ks,
	//	"-new_master", shard2Replica1.Alias)
	//assert.Nil(t, err, "error should be Nil")
	//
	////update our test variables to point at the new master
	//tmp := shard2Master
	//shard2Master  = shard2Replica1
	//shard2Replica1 = tmp
	//
	//log.Debug("Inserting lots of data on source shard after reparenting")
	//sharding.InsertLots(3000, 2000, *shard1.MasterTablet(), tableName, fixedParentID, keyspaceName)
	//// Checking 80 percent of data is sent fairly quickly
	//assert.True(t, checkLotsTimeout(t, 3000, 2000, tableName, keyspaceName, shardingKeyType))
	//
	//if useMultiSplitDiff {
	//	log.Debug("Running vtworker MultiSplitDiff")
	//	err = clusterInstance.VtworkerProcess.ExecuteVtworkerCommand(clusterInstance.GetAndReservePort(),
	//		clusterInstance.GetAndReservePort(),
	//		"--use_v3_resharding_mode=false",
	//		"MultiSplitDiff",
	//		"--exclude_tables", "unrelated",
	//		"--min_healthy_rdonly_tablets", "1",
	//		fmt.Sprintf("%s/%s", keyspaceName, shard1.Name))
	//	assert.Nil(t, err, "error should be Nil")
	//} else {
	//	log.Debug("Running vtworker SplitDiff")
	//	err = clusterInstance.VtworkerProcess.ExecuteVtworkerCommand(clusterInstance.GetAndReservePort(),
	//		clusterInstance.GetAndReservePort(),
	//		"--use_v3_resharding_mode=false",
	//		"SplitDiff",
	//		"--exclude_tables", "unrelated",
	//		"--min_healthy_rdonly_tablets", "1",
	//		fmt.Sprintf("%s/%s", keyspaceName, shard3.Name))
	//	assert.Nil(t, err, "error should be Nil")
	//}
	//
	//err = clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeSlaveType", shard1Rdonly.Alias, "rdonly")
	//assert.Nil(t, err, "error should be Nil")
	//err = clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeSlaveType", shard3Rdonly.Alias, "rdonly")
	//assert.Nil(t, err, "error should be Nil")

	/*



	 */
	fmt.Println("DONE.......")
	time.Sleep(10 * time.Millisecond)

	//

	//
	//// check we can't migrate the master just yet
	//err = ClusterInstance.VtctlclientProcess.ExecuteCommand("MigrateServedTypes", shard1Ks, "master")
	//assert.NotNil(t, err)
	//
	//// now serve rdonly from the split shards
	//_ = ClusterInstance.VtctlclientProcess.ExecuteCommand("MigrateServedTypes", shard1Ks, "rdonly")
	//expectedPartitions = map[topodata.TabletType][]string{}
	//expectedPartitions[topodata.TabletType_MASTER] = []string{shard1.Name}
	//expectedPartitions[topodata.TabletType_REPLICA] = []string{shard1.Name}
	//expectedPartitions[topodata.TabletType_RDONLY] = []string{shard21.Name, shard22.Name}
	//checkSrvKeyspaceForSharding(t, expectedPartitions, version, shardingKeyType)
	//
	//_ = shard21.Rdonly().VttabletProcess.WaitForTabletType("SERVING")
	//_ = shard22.Rdonly().VttabletProcess.WaitForTabletType("SERVING")
	//
	//_ = ClusterInstance.VtgateProcess.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.rdonly", keyspaceName, shard21.Name))
	//_ = ClusterInstance.VtgateProcess.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.rdonly", keyspaceName, shard22.Name))
	//
	//sql = fmt.Sprintf("select id, msg from %s", tableName)
	//output, _ = ClusterInstance.VtctlProcess.ExecuteCommandWithOutput("VtGateSplitQuery",
	//	"-server", fmt.Sprintf("%s:%d", ClusterInstance.Hostname, ClusterInstance.VtgateProcess.GrpcPort),
	//	"-keyspace", keyspaceName,
	//	"-split_count", "2",
	//	sql)
	//
	//splitqueryresponse = nil
	//err = json.Unmarshal([]byte(output), &splitqueryresponse)
	//assert.Nil(t, err)
	//assert.Equal(t, len(splitqueryresponse), 2)
	//assert.Equal(t, splitqueryresponse[0].KeyRangePart.Keyspace, keyspaceName)
	//assert.Equal(t, splitqueryresponse[1].KeyRangePart.Keyspace, keyspaceName)
	//assert.Equal(t, len(splitqueryresponse[0].KeyRangePart.KeyRanges), 1)
	//assert.Equal(t, len(splitqueryresponse[1].KeyRangePart.KeyRanges), 1)
	//
	////then serve replica from the split shards
	//
	//sourceTablet := shard1.Replica()
	//destinationTablets := []cluster.Vttablet{*shard21.Replica(), *shard22.Replica()}
	//
	//_ = ClusterInstance.VtctlclientProcess.ExecuteCommand("MigrateServedTypes", shard1Ks, "replica")
	//expectedPartitions = map[topodata.TabletType][]string{}
	//expectedPartitions[topodata.TabletType_MASTER] = []string{shard1.Name}
	//expectedPartitions[topodata.TabletType_REPLICA] = []string{shard21.Name, shard22.Name}
	//expectedPartitions[topodata.TabletType_RDONLY] = []string{shard21.Name, shard22.Name}
	//checkSrvKeyspaceForSharding(t, expectedPartitions, version, shardingKeyType)
	//
	////move replica back and forth
	//_ = ClusterInstance.VtctlclientProcess.ExecuteCommand("MigrateServedTypes", "-reverse", shard1Ks, "replica")
	//
	//// After a backwards migration, queryservice should be enabled on source and disabled on destinations
	//sharding.CheckTabletQueryService(t, *sourceTablet, "SERVING", false, *ClusterInstance)
	//sharding.CheckTabletQueryServices(t, destinationTablets, "NOT_SERVING", true, *ClusterInstance)
	//
	//expectedPartitions = map[topodata.TabletType][]string{}
	//expectedPartitions[topodata.TabletType_MASTER] = []string{shard1.Name}
	//expectedPartitions[topodata.TabletType_REPLICA] = []string{shard1.Name}
	//expectedPartitions[topodata.TabletType_RDONLY] = []string{shard21.Name, shard22.Name}
	//checkSrvKeyspaceForSharding(t, expectedPartitions, version, shardingKeyType)
	//
	//_ = ClusterInstance.VtctlclientProcess.ExecuteCommand("MigrateServedTypes", shard1Ks, "replica")
	//
	//// After a forwards migration, queryservice should be disabled on source and enabled on destinations
	//sharding.CheckTabletQueryService(t, *sourceTablet, "NOT_SERVING", true, *ClusterInstance)
	//sharding.CheckTabletQueryServices(t, destinationTablets, "SERVING", false, *ClusterInstance)
	//expectedPartitions = map[topodata.TabletType][]string{}
	//expectedPartitions[topodata.TabletType_MASTER] = []string{shard1.Name}
	//expectedPartitions[topodata.TabletType_REPLICA] = []string{shard21.Name, shard22.Name}
	//expectedPartitions[topodata.TabletType_RDONLY] = []string{shard21.Name, shard22.Name}
	//checkSrvKeyspaceForSharding(t, expectedPartitions, version, shardingKeyType)
	//
	//// then serve master from the split shards
	//_ = ClusterInstance.VtctlclientProcess.ExecuteCommand("MigrateServedTypes", shard1Ks, "master")
	//expectedPartitions = map[topodata.TabletType][]string{}
	//expectedPartitions[topodata.TabletType_MASTER] = []string{shard21.Name, shard22.Name}
	//expectedPartitions[topodata.TabletType_REPLICA] = []string{shard21.Name, shard22.Name}
	//expectedPartitions[topodata.TabletType_RDONLY] = []string{shard21.Name, shard22.Name}
	//checkSrvKeyspaceForSharding(t, expectedPartitions, version, shardingKeyType)
	//
	//// check the binlog players are gone now
	//_ = shard21.MasterTablet().VttabletProcess.WaitForBinLogPlayerCount(0)
	//_ = shard22.MasterTablet().VttabletProcess.WaitForBinLogPlayerCount(0)
	//
	//// make sure we can't delete a shard with tablets
	//err = ClusterInstance.VtctlclientProcess.ExecuteCommand("DeleteShard", shard1Ks)
	//assert.NotNil(t, err)
	//
	//// Teardown
	//for _, tablet := range []cluster.Vttablet{shard1MasterTablet, *shard1.Replica(), *shard1.Rdonly()} {
	//	_ = tablet.MysqlctlProcess.Stop()
	//	_ = tablet.VttabletProcess.TearDown(true)
	//}
	//_ = ClusterInstance.VtctlclientProcess.ExecuteCommand("DeleteTablet", shard1.Replica().Alias)
	//_ = ClusterInstance.VtctlclientProcess.ExecuteCommand("DeleteTablet", shard1.Rdonly().Alias)
	//_ = ClusterInstance.VtctlclientProcess.ExecuteCommand("DeleteTablet", "-allow_master", shard1MasterTablet.Alias)
	//
	//_ = ClusterInstance.VtctlclientProcess.ExecuteCommand("RebuildKeyspaceGraph", keyspaceName)
	//_ = ClusterInstance.VtctlclientProcess.ExecuteCommand("DeleteShard", keyspaceName+"/"+shard1.Name)

	// Kill Tablets and Wait for the Process
	//for _, shard := range clusterInstance.Keyspaces[0].Shards {
	//	for _, tablet := range shard.Vttablets {
	//		_ = tablet.VttabletProcess.TearDown(false)
	//	}
	//}

}

func checkSrvKeyspaceForSharding(t *testing.T, cell string, expectedPartitions map[topodata.TabletType][]string, keyspaceIDType topodata.KeyspaceIdType) {
	//if version == 3 {
	//	sharding.CheckSrvKeyspace(t, cell, keyspaceName, "", 0, expectedPartitions, *clusterInstance)
	//} else {
	sharding.CheckSrvKeyspace(t, cell, keyspaceName, "custom_ksid_col", keyspaceIDType, expectedPartitions, *clusterInstance)
	//}

}

func insertStartupValues() {
	var ksID uint64 = 0x1000000000000000
	insertSQL := fmt.Sprintf(sharding.InsertTabletTemplateKsID, "resharding1", fixedParentID, 1, "msg1", ksID, ksID, 1)
	sharding.InsertToTablet(insertSQL, *shard0.MasterTablet(), keyspaceName)

	var ksID2 uint64 = 0x9000000000000000
	insertSQL = fmt.Sprintf(sharding.InsertTabletTemplateKsID, "resharding1", fixedParentID, 2, "msg2", ksID2, ksID2, 2)
	sharding.InsertToTablet(insertSQL, *shard1.MasterTablet(), keyspaceName)

	var ksID3 uint64 = 0xD000000000000000
	insertSQL = fmt.Sprintf(sharding.InsertTabletTemplateKsID, "resharding1", fixedParentID, 3, "msg3", ksID3, ksID3, 3)
	sharding.InsertToTablet(insertSQL, *shard1.MasterTablet(), keyspaceName)

	insertSQL = fmt.Sprintf(sharding.InsertTabletTemplateKsID, "resharding3", fixedParentID, 1, "a", ksID, ksID, 1)
	sharding.InsertToTablet(insertSQL, *shard0.MasterTablet(), keyspaceName)

	insertSQL = fmt.Sprintf(sharding.InsertTabletTemplateKsID, "resharding3", fixedParentID, 2, "b", ksID2, ksID2, 1)
	sharding.InsertToTablet(insertSQL, *shard1.MasterTablet(), keyspaceName)

	insertSQL = fmt.Sprintf(sharding.InsertTabletTemplateKsID, "resharding3", fixedParentID, 3, "c", ksID3, ksID3, 1)
	sharding.InsertToTablet(insertSQL, *shard1.MasterTablet(), keyspaceName)
}

func insertValue(tablet *cluster.Vttablet, keyspaceName string, tableName string, id int, msg string, ksID uint64) {
	insertSQL := fmt.Sprintf(sharding.InsertTabletTemplateKsID, tableName, fixedParentID, id, msg, ksID, ksID, id)
	sharding.InsertToTablet(insertSQL, *tablet, keyspaceName)
}

func execMultiShardDmls(keyspaceName string) {
	ids := []int{10000001, 10000002, 10000003}
	msgs := []string{"msg-id10000001", "msg-id10000002", "msg-id10000003"}
	ksIds := []uint64{0x9000000000000000, 0xD000000000000000, 0xE000000000000000}
	sharding.InsertMultiValueToTablet(*shard1.MasterTablet(), keyspaceName, "resharding1", fixedParentID, ids, msgs, ksIds)

	ids = []int{10000004, 10000005}
	msgs = []string{"msg-id10000004", "msg-id10000005"}
	ksIds = []uint64{0xD000000000000000, 0xE000000000000000}
	sharding.InsertMultiValueToTablet(*shard1.MasterTablet(), keyspaceName, "resharding1", fixedParentID, ids, msgs, ksIds)

	ids = []int{10000011, 10000012, 10000013}
	msgs = []string{"msg-id10000011", "msg-id10000012", "msg-id10000013"}
	ksIds = []uint64{0x9000000000000000, 0xD000000000000000, 0xE000000000000000}
	sharding.InsertMultiValueToTablet(*shard1.MasterTablet(), keyspaceName, "resharding1", fixedParentID, ids, msgs, ksIds)

	// This update targets two shards.
	sql := `update resharding1 set msg="update1" where parent_id=86 and id in (10000011,10000012)`
	_, _ = shard1.MasterTablet().VttabletProcess.QueryTablet(sql, keyspaceName, true)

	// This update targets one shard.
	sql = `update resharding1 set msg="update1" where parent_id=86 and id in (10000013)`
	_, _ = shard1.MasterTablet().VttabletProcess.QueryTablet(sql, keyspaceName, true)

	ids = []int{10000014, 10000015, 10000016}
	msgs = []string{"msg-id10000014", "msg-id10000015", "msg-id10000016"}
	ksIds = []uint64{0x9000000000000000, 0xD000000000000000, 0xE000000000000000}
	sharding.InsertMultiValueToTablet(*shard1.MasterTablet(), keyspaceName, "resharding1", fixedParentID, ids, msgs, ksIds)

	// This delete targets two shards.
	sql = `delete from resharding1 where parent_id =86 and id in (10000014, 10000015)`
	_, _ = shard1.MasterTablet().VttabletProcess.QueryTablet(sql, keyspaceName, true)
	// This delete targets one shard.
	sql = `delete from resharding1 where parent_id =86 and id in (10000016)`
	_, _ = shard1.MasterTablet().VttabletProcess.QueryTablet(sql, keyspaceName, true)

	// repeat DMLs for table with msg as bit(8)
	ids = []int{10000001, 10000002, 10000003}
	msgs = []string{"a", "b", "c"}
	ksIds = []uint64{0x9000000000000000, 0xD000000000000000, 0xE000000000000000}
	sharding.InsertMultiValueToTablet(*shard1.MasterTablet(), keyspaceName, "resharding3", fixedParentID, ids, msgs, ksIds)

	ids = []int{10000004, 10000005}
	msgs = []string{"d", "e"}
	ksIds = []uint64{0xD000000000000000, 0xE000000000000000}
	sharding.InsertMultiValueToTablet(*shard1.MasterTablet(), keyspaceName, "resharding3", fixedParentID, ids, msgs, ksIds)

	ids = []int{10000011, 10000012, 10000013}
	msgs = []string{"k", "l", "m"}
	ksIds = []uint64{0x9000000000000000, 0xD000000000000000, 0xE000000000000000}
	sharding.InsertMultiValueToTablet(*shard1.MasterTablet(), keyspaceName, "resharding3", fixedParentID, ids, msgs, ksIds)

	// This update targets two shards.
	sql = `update resharding3 set msg="g" where parent_id=86 and id in (10000011, 10000012)`
	_, _ = shard1.MasterTablet().VttabletProcess.QueryTablet(sql, keyspaceName, true)

	// This update targets one shard.
	sql = `update resharding3 set msg="h" where parent_id=86 and id in (10000013)`
	_, _ = shard1.MasterTablet().VttabletProcess.QueryTablet(sql, keyspaceName, true)

	ids = []int{10000014, 10000015, 10000016}
	msgs = []string{"n", "o", "p"}
	ksIds = []uint64{0x9000000000000000, 0xD000000000000000, 0xE000000000000000}
	sharding.InsertMultiValueToTablet(*shard1.MasterTablet(), keyspaceName, "resharding3", fixedParentID, ids, msgs, ksIds)

	// This delete targets two shards.
	sql = `delete from resharding3 where parent_id =86 and id in (10000014, 10000015)`
	_, _ = shard1.MasterTablet().VttabletProcess.QueryTablet(sql, keyspaceName, true)
	// This delete targets one shard.
	sql = `delete from resharding3 where parent_id =86 and id in (10000016)`
	_, _ = shard1.MasterTablet().VttabletProcess.QueryTablet(sql, keyspaceName, true)

}

func checkStartupValues(t *testing.T, shardingKeyType topodata.KeyspaceIdType) {
	// check first value is in the right shard
	for _, tablet := range shard2.Vttablets {
		sharding.CheckValues(t, *tablet, []string{"INT64(86)", "INT64(2)", `VARCHAR("msg2")`, sharding.HexToDbStr(0x9000000000000000, shardingKeyType)},
			2, true, "resharding1", fixedParentID, keyspaceName, shardingKeyType)
		sharding.CheckValues(t, *tablet, []string{"INT64(86)", "INT64(2)", `BIT("b")`, sharding.HexToDbStr(0x9000000000000000, shardingKeyType)},
			2, true, "resharding3", fixedParentID, keyspaceName, shardingKeyType)
	}
	for _, tablet := range shard3.Vttablets {
		sharding.CheckValues(t, *tablet, []string{"INT64(86)", "INT64(2)", `VARCHAR("msg2")`, sharding.HexToDbStr(0x9000000000000000, shardingKeyType)},
			2, false, "resharding1", fixedParentID, keyspaceName, shardingKeyType)
		sharding.CheckValues(t, *tablet, []string{"INT64(86)", "INT64(2)", `BIT("b")`, sharding.HexToDbStr(0x9000000000000000, shardingKeyType)},
			2, false, "resharding3", fixedParentID, keyspaceName, shardingKeyType)
	}
	// check first value is in the right shard
	for _, tablet := range shard2.Vttablets {
		sharding.CheckValues(t, *tablet, []string{"INT64(86)", "INT64(3)", `VARCHAR("msg3")`, sharding.HexToDbStr(0xD000000000000000, shardingKeyType)},
			3, false, "resharding1", fixedParentID, keyspaceName, shardingKeyType)
		sharding.CheckValues(t, *tablet, []string{"INT64(86)", "INT64(3)", `BIT("c")`, sharding.HexToDbStr(0xD000000000000000, shardingKeyType)},
			3, false, "resharding3", fixedParentID, keyspaceName, shardingKeyType)
	}
	for _, tablet := range shard3.Vttablets {
		sharding.CheckValues(t, *tablet, []string{"INT64(86)", "INT64(3)", `VARCHAR("msg3")`, sharding.HexToDbStr(0xD000000000000000, shardingKeyType)},
			3, true, "resharding1", fixedParentID, keyspaceName, shardingKeyType)
		sharding.CheckValues(t, *tablet, []string{"INT64(86)", "INT64(3)", `BIT("c")`, sharding.HexToDbStr(0xD000000000000000, shardingKeyType)},
			3, true, "resharding3", fixedParentID, keyspaceName, shardingKeyType)
	}
}

// checkLotsNotPresent verifies that no rows should be present in vttablet
func checkLotsNotPresent(t *testing.T, count uint64, base uint64, table string, keyspaceName string, keyType topodata.KeyspaceIdType) {
	var i uint64
	//var count uint64 = 1000
	for i = 0; i < count; i++ {
		assert.False(t, sharding.CheckValues(t, *shard3.Vttablets[1], []string{"INT64(86)",
			fmt.Sprintf("INT64(%d)", 10000+base+i),
			fmt.Sprintf(`VARCHAR("msg-range1-%d")`, 10000+base+i),
			sharding.HexToDbStr(0xA000000000000000+base+i, keyType)},
			10000+base+i, true, table, fixedParentID, keyspaceName, keyType))

		assert.False(t, sharding.CheckValues(t, *shard2.Vttablets[2], []string{"INT64(86)",
			fmt.Sprintf("INT64(%d)", 20000+base+i),
			fmt.Sprintf(`VARCHAR("msg-range2-%d")`, 20000+base+i),
			sharding.HexToDbStr(0xE000000000000000+base+i, keyType)},
			20000+base+i, true, table, fixedParentID, keyspaceName, keyType))
	}
}

// checkLotsTimeout waits till all values are inserted
func checkLotsTimeout(t *testing.T, count uint64, base uint64, table string, keyspaceName string, keyType topodata.KeyspaceIdType) bool {
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

func checkLots(t *testing.T, count uint64, base uint64, table string, keyspaceName string, keyType topodata.KeyspaceIdType) float32 {
	var isFound bool
	var totalFound int
	var i uint64
	//var count uint64 = 1000
	for i = 0; i < count; i++ {
		isFound = sharding.CheckValues(t, *shard2.Vttablets[2], []string{"INT64(86)",
			fmt.Sprintf("INT64(%d)", 10000+base+i),
			fmt.Sprintf(`VARCHAR("msg-range1-%d")`, 10000+base+i),
			sharding.HexToDbStr(0xA000000000000000+base+i, keyType)},
			10000+base+i, true, table, fixedParentID, keyspaceName, keyType)
		if isFound {
			totalFound++
		}

		isFound = sharding.CheckValues(t, *shard3.Vttablets[1], []string{"INT64(86)",
			fmt.Sprintf("INT64(%d)", 20000+base+i),
			fmt.Sprintf(`VARCHAR("msg-range2-%d")`, 20000+base+i),
			sharding.HexToDbStr(0xE000000000000000+base+i, keyType)},
			20000+base+i, true, table, fixedParentID, keyspaceName, keyType)
		if isFound {
			totalFound++
		}
	}
	return float32(totalFound * 100 / int(count) / 2)
}

func checkMultiShardValues(t *testing.T, keyspaceName string, keyType topodata.KeyspaceIdType) {
	//Shard2 master, replica1 and replica 2
	shard2Tablets := []cluster.Vttablet{*shard2.Vttablets[0], *shard2.Vttablets[1], *shard2.Vttablets[2]}
	checkMultiDbs(t, shard2Tablets, "resharding1", keyspaceName, keyType, 10000001, "msg-id10000001", 0x9000000000000000, true)
	checkMultiDbs(t, shard2Tablets, "resharding1", keyspaceName, keyType, 10000002, "msg-id10000002", 0xD000000000000000, false)
	checkMultiDbs(t, shard2Tablets, "resharding1", keyspaceName, keyType, 10000003, "msg-id10000003", 0xE000000000000000, false)
	checkMultiDbs(t, shard2Tablets, "resharding1", keyspaceName, keyType, 10000004, "msg-id10000004", 0xD000000000000000, false)
	checkMultiDbs(t, shard2Tablets, "resharding1", keyspaceName, keyType, 10000005, "msg-id10000005", 0xE000000000000000, false)

	//Shard 3 master and replica
	shard3Tablets := []cluster.Vttablet{*shard3.Vttablets[0], *shard3.Vttablets[1]}
	checkMultiDbs(t, shard3Tablets, "resharding1", keyspaceName, keyType, 10000001, "msg-id10000001", 0x9000000000000000, false)
	checkMultiDbs(t, shard3Tablets, "resharding1", keyspaceName, keyType, 10000002, "msg-id10000002", 0xD000000000000000, true)
	checkMultiDbs(t, shard3Tablets, "resharding1", keyspaceName, keyType, 10000003, "msg-id10000003", 0xE000000000000000, true)
	checkMultiDbs(t, shard3Tablets, "resharding1", keyspaceName, keyType, 10000004, "msg-id10000004", 0xD000000000000000, true)
	checkMultiDbs(t, shard3Tablets, "resharding1", keyspaceName, keyType, 10000005, "msg-id10000005", 0xE000000000000000, true)

	// Updated values
	checkMultiDbs(t, shard2Tablets, "resharding1", keyspaceName, keyType, 10000011, "update1", 0x9000000000000000, true)
	checkMultiDbs(t, shard3Tablets, "resharding1", keyspaceName, keyType, 10000012, "update1", 0xD000000000000000, true)
	checkMultiDbs(t, shard3Tablets, "resharding1", keyspaceName, keyType, 10000013, "update2", 0xE000000000000000, true)

	allTablets := []cluster.Vttablet{*shard2.Vttablets[0], *shard2.Vttablets[1], *shard2.Vttablets[2], *shard3.Vttablets[0], *shard3.Vttablets[1]}
	checkMultiDbs(t, allTablets, "resharding1", keyspaceName, keyType, 10000014, "msg-id10000014", 0x9000000000000000, false)
	checkMultiDbs(t, allTablets, "resharding1", keyspaceName, keyType, 10000015, "msg-id10000015", 0xD000000000000000, false)
	checkMultiDbs(t, allTablets, "resharding1", keyspaceName, keyType, 10000016, "msg-id10000016", 0xF000000000000000, false)

	// checks for bit(8) table
	checkMultiDbs(t, shard2Tablets, "resharding3", keyspaceName, keyType, 10000001, "a", 0x9000000000000000, true)
	checkMultiDbs(t, shard2Tablets, "resharding3", keyspaceName, keyType, 10000002, "b", 0xD000000000000000, false)
	checkMultiDbs(t, shard2Tablets, "resharding3", keyspaceName, keyType, 10000003, "c", 0xE000000000000000, false)
	checkMultiDbs(t, shard2Tablets, "resharding3", keyspaceName, keyType, 10000004, "d", 0xD000000000000000, false)
	checkMultiDbs(t, shard2Tablets, "resharding3", keyspaceName, keyType, 10000005, "e", 0xE000000000000000, false)

	checkMultiDbs(t, shard3Tablets, "resharding3", keyspaceName, keyType, 10000001, "a", 0x9000000000000000, false)
	checkMultiDbs(t, shard3Tablets, "resharding3", keyspaceName, keyType, 10000002, "b", 0xD000000000000000, true)
	checkMultiDbs(t, shard3Tablets, "resharding3", keyspaceName, keyType, 10000003, "c", 0xE000000000000000, true)
	checkMultiDbs(t, shard3Tablets, "resharding3", keyspaceName, keyType, 10000004, "d", 0xD000000000000000, true)
	checkMultiDbs(t, shard3Tablets, "resharding3", keyspaceName, keyType, 10000005, "e", 0xE000000000000000, true)

	// updated values
	checkMultiDbs(t, shard2Tablets, "resharding3", keyspaceName, keyType, 10000011, "g", 0x9000000000000000, true)
	checkMultiDbs(t, shard3Tablets, "resharding3", keyspaceName, keyType, 10000012, "g", 0xD000000000000000, true)
	checkMultiDbs(t, shard3Tablets, "resharding3", keyspaceName, keyType, 10000013, "h", 0xE000000000000000, true)

	checkMultiDbs(t, allTablets, "resharding3", keyspaceName, keyType, 10000014, "n", 0x9000000000000000, false)
	checkMultiDbs(t, allTablets, "resharding3", keyspaceName, keyType, 10000015, "o", 0xD000000000000000, false)
	checkMultiDbs(t, allTablets, "resharding3", keyspaceName, keyType, 10000016, "p", 0xF000000000000000, false)

}

// checkMultiDbs checks the row in multiple dbs
func checkMultiDbs(t *testing.T, vttablets []cluster.Vttablet, tableName string, keyspaceName string,
	keyType topodata.KeyspaceIdType, id int, msg string, ksId uint64, presentInDb bool) {
	for _, tablet := range vttablets {
		sharding.CheckValues(t, tablet, []string{"INT64(86)",
			fmt.Sprintf("INT64(%d)", id),
			fmt.Sprintf(`VARCHAR("%s")`, msg),
			sharding.HexToDbStr(ksId, keyType)},
			ksId, presentInDb, tableName, fixedParentID, keyspaceName, keyType)
	}
}
