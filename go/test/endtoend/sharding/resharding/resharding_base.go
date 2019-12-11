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
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"path"
	"strings"
	"testing"
	"time"

	"vitess.io/vitess/go/mysql"

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
							custom_ksid_col bigint not null,
							name varchar(64),
							primary key (name)
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
								},
                                "resharding2": {
								   "column_vindexes": [
									{
									  "column": "custom_ksid_col",
									  "name": "hash_index"
									}
								  ] 
								},
                                "resharding3": {
								   "column_vindexes": [
									{
									  "column": "custom_ksid_col",
									  "name": "hash_index"
									}
								  ] 
								},
								"no_pk": {
								   "column_vindexes": [
									{
									  "column": "custom_ksid_col",
									  "name": "hash_index"
									}
								  ] 
								},
								"timestamps": {
								   "column_vindexes": [
									{
									  "column": "custom_ksid_col",
									  "name": "hash_index"
									}
								  ] 
								},
								"unrelated": {
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

	// initial shards
	// range '' - 80  &  range 80 - ''
	shard0 = &cluster.Shard{Name: "-80"}
	shard1 = &cluster.Shard{Name: "80-"}

	// split shards
	// range 80 - c0    &   range c0 - ''
	shard2 = &cluster.Shard{Name: "80-c0"}
	shard3 = &cluster.Shard{Name: "c0-"}

	// Sharding keys
	key1 uint64 = 1152921504606846976  // decimal of 0x1000000000000000  // redirect to shard 0
	key2 uint64 = 14987979559889010688 // decimal of 0xD000000000000000  // redirect to shard 1,2
	key3 uint64 = 10376293541461622784 // decimal of 0x9000000000000000  // redirect to shard 1,3
	key4 uint64 = 10376293541461622789 // decimal of 0xE000000000000000  // redirect to shard 3
	key5 uint64 = 14987979559889010670 // decimal of 0xA000000000000000  // redirect to shard 2
	key6 uint64 = 17293822569102704640 // decimal of 0xF000000000000000

	// insertTabletTemplateKsID common insert format
	insertTabletTemplateKsID = `insert into %s (parent_id, id, msg, custom_ksid_col) values (%d, %d, '%s', %d) /* vtgate:: keyspace_id:%d */ /* id:%d */`
)

// TestReSharding - main test with accepts different params for various test
func TestReSharding(t *testing.T, useByteShardingKeyType bool) {
	clusterInstance = cluster.NewCluster(cell1, hostname)
	defer clusterInstance.Teardown()

	// Launch keyspace
	keyspace := &cluster.Keyspace{Name: keyspaceName}

	// Start topo server
	err := clusterInstance.StartTopo()
	assert.Nil(t, err)

	// Adding another cell in the same cluster
	err = clusterInstance.TopoProcess.ManageTopoDir("mkdir", "/vitess/"+cell2)
	assert.Nil(t, err)
	err = clusterInstance.VtctlProcess.AddCellInfo(cell2)
	assert.Nil(t, err)

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
		"-enable-tx-throttler",
		"-binlog_use_v3_resharding_mode=true",
	}

	shardingColumnType := "bigint(20) unsigned"
	shardingKeyType := querypb.Type_UINT64

	if useByteShardingKeyType {
		shardingColumnType = "varbinary(64)"
		shardingKeyType = querypb.Type_VARBINARY
	}

	// Initialize Cluster
	err = clusterInstance.LaunchCluster(keyspace, []cluster.Shard{*shard0, *shard1, *shard2, *shard3})
	assert.Nil(t, err)
	assert.Equal(t, len(clusterInstance.Keyspaces[0].Shards), 4)

	//Start MySql
	var mysqlCtlProcessList []*exec.Cmd
	for _, shard := range clusterInstance.Keyspaces[0].Shards {
		for _, tablet := range shard.Vttablets {
			fmt.Println("Starting MySql for tablet ", tablet.Alias)
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
	assert.Nil(t, err)

	// Get Keyspace and verify the structure
	srvKeyspace := sharding.GetSrvKeyspace(t, cell1, keyspaceName, *clusterInstance)
	assert.Equal(t, "", srvKeyspace.GetShardingColumnName())

	//Start Tablets and Wait for the Process
	for _, shard := range clusterInstance.Keyspaces[0].Shards {
		for _, tablet := range shard.Vttablets {
			// Start the tablet
			err = tablet.VttabletProcess.Setup()
			assert.Nil(t, err)

			// Create Database
			_, err = tablet.VttabletProcess.QueryTablet(fmt.Sprintf("create database vt_%s",
				keyspace.Name), keyspace.Name, false)
			assert.Nil(t, err)
		}
	}

	// Init Shard Master
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("InitShardMaster",
		"-force", fmt.Sprintf("%s/%s", keyspaceName, shard0.Name), shard0Master.Alias)
	assert.Nil(t, err)
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("InitShardMaster",
		"-force", fmt.Sprintf("%s/%s", keyspaceName, shard1.Name), shard1Master.Alias)
	assert.Nil(t, err)

	// Init Shard Master on Split Shards
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("InitShardMaster",
		"-force", fmt.Sprintf("%s/%s", keyspaceName, shard2.Name), shard2Master.Alias)
	assert.Nil(t, err)
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("InitShardMaster",
		"-force", fmt.Sprintf("%s/%s", keyspaceName, shard3.Name), shard3Master.Alias)
	assert.Nil(t, err)

	// Wait for tablets to come in Service state
	err = shard0Master.VttabletProcess.WaitForTabletType("SERVING")
	assert.Nil(t, err)
	err = shard1Master.VttabletProcess.WaitForTabletType("SERVING")
	assert.Nil(t, err)
	err = shard2Master.VttabletProcess.WaitForTabletType("SERVING")
	assert.Nil(t, err)
	err = shard3Master.VttabletProcess.WaitForTabletType("SERVING")
	assert.Nil(t, err)

	// keyspace/shard name fields
	shard0Ks := fmt.Sprintf("%s/%s", keyspaceName, shard0.Name)
	shard1Ks := fmt.Sprintf("%s/%s", keyspaceName, shard1.Name)
	shard2Ks := fmt.Sprintf("%s/%s", keyspaceName, shard2.Name)
	shard3Ks := fmt.Sprintf("%s/%s", keyspaceName, shard3.Name)

	// check for shards
	result, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("FindAllShardsInKeyspace", keyspaceName)
	assert.Nil(t, err)
	resultMap := make(map[string]interface{})
	err = json.Unmarshal([]byte(result), &resultMap)
	assert.Nil(t, err)
	assert.Equal(t, 4, len(resultMap), "No of shards should be 4")

	// Apply Schema
	err = clusterInstance.VtctlclientProcess.ApplySchema(keyspaceName, fmt.Sprintf(createTabletTemplate, "resharding1", shardingColumnType))
	assert.Nil(t, err)
	err = clusterInstance.VtctlclientProcess.ApplySchema(keyspaceName, fmt.Sprintf(createTabletTemplate, "resharding2", shardingColumnType))
	assert.Nil(t, err)
	err = clusterInstance.VtctlclientProcess.ApplySchema(keyspaceName, fmt.Sprintf(createTableBindataTemplate, "resharding3", shardingColumnType))
	assert.Nil(t, err)
	err = clusterInstance.VtctlclientProcess.ApplySchema(keyspaceName, fmt.Sprintf(createViewTemplate, "view1", "resharding3"))
	assert.Nil(t, err)
	err = clusterInstance.VtctlclientProcess.ApplySchema(keyspaceName, fmt.Sprintf(createNoPkTable, shardingColumnType))
	assert.Nil(t, err)
	err = clusterInstance.VtctlclientProcess.ApplySchema(keyspaceName, fmt.Sprintf(createTimestampTable, shardingColumnType))
	assert.Nil(t, err)
	err = clusterInstance.VtctlclientProcess.ApplySchema(keyspaceName, createUnrelatedTable)
	assert.Nil(t, err)

	// Apply VSchema
	err = clusterInstance.VtctlclientProcess.ApplyVSchema(keyspaceName, vSchema)
	assert.Nil(t, err)

	// Insert Data
	insertStartupValues(t)

	// run a health check on source replicas so they respond to discovery
	// (for binlog players) and on the source rdonlys (for workers)
	for _, shard := range keyspace.Shards {
		for _, tablet := range shard.Vttablets {
			err = clusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", tablet.Alias)
			assert.Nil(t, err)
		}
	}

	// Rebuild keyspace Graph
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RebuildKeyspaceGraph", keyspaceName)
	assert.Nil(t, err)

	// check srv keyspace
	expectedPartitions := map[topodata.TabletType][]string{}
	expectedPartitions[topodata.TabletType_MASTER] = []string{shard0.Name, shard1.Name}
	expectedPartitions[topodata.TabletType_REPLICA] = []string{shard0.Name, shard1.Name}
	expectedPartitions[topodata.TabletType_RDONLY] = []string{shard0.Name, shard1.Name}
	sharding.CheckSrvKeyspace(t, cell1, keyspaceName, "", 0, expectedPartitions, *clusterInstance)

	// disable shard1Replica2, so we're sure filtered replication will go from shard1Replica1
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeSlaveType", shard1Replica2.Alias, "spare")
	assert.Nil(t, err)

	err = shard1Replica2.VttabletProcess.WaitForTabletType("NOT_SERVING")
	assert.Nil(t, err)

	// we need to create the schema, and the worker will do data copying
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("CopySchemaShard", "--exclude_tables", "unrelated",
		shard1.Rdonly().Alias, fmt.Sprintf("%s/%s", keyspaceName, shard2.Name))
	assert.Nil(t, err)
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("CopySchemaShard", "--exclude_tables", "unrelated",
		shard1.Rdonly().Alias, fmt.Sprintf("%s/%s", keyspaceName, shard3.Name))
	assert.Nil(t, err)

	// Run vtworker as daemon for the following SplitClone commands. -use_v3_resharding_mode default is true
	err = clusterInstance.StartVtworker(cell1, "--command_display_interval", "10ms")
	assert.Nil(t, err)

	// Copy the data from the source to the destination shards.
	// --max_tps is only specified to enable the throttler and ensure that the
	// code is executed. But the intent here is not to throttle the test, hence
	// the rate limit is set very high.

	// Initial clone (online).
	err = clusterInstance.VtworkerProcess.ExecuteCommand("SplitClone",
		"--offline=false",
		"--exclude_tables", "unrelated",
		"--chunk_count", "10",
		"--min_rows_per_chunk", "1",
		"--min_healthy_rdonly_tablets", "1",
		"--max_tps", "9999",
		shard1Ks)
	assert.Nil(t, err)

	// Check values in the split shard
	checkValues(t, *shard2.MasterTablet(), []string{"INT64(86)", "INT64(2)", `VARCHAR("msg2")`, fmt.Sprintf("UINT64(%d)", key2)},
		2, true, tableName, fixedParentID, keyspaceName, shardingKeyType)
	checkValues(t, *shard3.MasterTablet(), []string{"INT64(86)", "INT64(2)", `VARCHAR("msg2")`, fmt.Sprintf("UINT64(%d)", key2)},
		2, false, tableName, fixedParentID, keyspaceName, shardingKeyType)

	// Reset vtworker such that we can run the next command.
	err = clusterInstance.VtworkerProcess.ExecuteCommand("Reset")
	assert.Nil(t, err)

	// Test the correct handling of keyspace_id changes which happen after the first clone.
	sql := fmt.Sprintf("update resharding1 set custom_ksid_col=%d WHERE id=2", key3)
	_, err = shard1Master.VttabletProcess.QueryTablet(sql, keyspaceName, true)
	assert.Nil(t, err)

	err = clusterInstance.VtworkerProcess.ExecuteCommand("SplitClone",
		"--offline=false",
		"--exclude_tables", "unrelated",
		"--chunk_count", "10",
		"--min_rows_per_chunk", "1",
		"--min_healthy_rdonly_tablets", "1",
		"--max_tps", "9999",
		shard1Ks)
	assert.Nil(t, err)

	checkValues(t, *shard2.MasterTablet(), []string{"INT64(86)", "INT64(2)", `VARCHAR("msg2")`, fmt.Sprintf("UINT64(%d)", key3)},
		2, false, tableName, fixedParentID, keyspaceName, shardingKeyType)
	checkValues(t, *shard3.MasterTablet(), []string{"INT64(86)", "INT64(2)", `VARCHAR("msg2")`, fmt.Sprintf("UINT64(%d)", key3)},
		2, true, tableName, fixedParentID, keyspaceName, shardingKeyType)

	err = clusterInstance.VtworkerProcess.ExecuteCommand("Reset")
	assert.Nil(t, err)

	// Move row 2 back to shard 2 from shard 3 by changing the keyspace_id again
	sql = fmt.Sprintf("update resharding1 set custom_ksid_col=%d WHERE id=2", key2)
	_, err = shard1Master.VttabletProcess.QueryTablet(sql, keyspaceName, true)
	assert.Nil(t, err)

	err = clusterInstance.VtworkerProcess.ExecuteCommand("SplitClone",
		"--offline=false",
		"--exclude_tables", "unrelated",
		"--chunk_count", "10",
		"--min_rows_per_chunk", "1",
		"--min_healthy_rdonly_tablets", "1",
		"--max_tps", "9999",
		shard1Ks)
	assert.Nil(t, err)

	checkValues(t, *shard2.MasterTablet(), []string{"INT64(86)", "INT64(2)", `VARCHAR("msg2")`, fmt.Sprintf("UINT64(%d)", key2)},
		2, true, tableName, fixedParentID, keyspaceName, shardingKeyType)
	checkValues(t, *shard3.MasterTablet(), []string{"INT64(86)", "INT64(2)", `VARCHAR("msg2")`, fmt.Sprintf("UINT64(%d)", key2)},
		2, false, tableName, fixedParentID, keyspaceName, shardingKeyType)

	// Reset vtworker such that we can run the next command.
	err = clusterInstance.VtworkerProcess.ExecuteCommand("Reset")
	assert.Nil(t, err)

	// Modify the destination shard. SplitClone will revert the changes.

	// Delete row 2 (provokes an insert).
	_, err = shard2Master.VttabletProcess.QueryTablet("delete from resharding1 where id=2", keyspaceName, true)
	assert.Nil(t, err)
	// Update row 3 (provokes an update).
	_, err = shard3Master.VttabletProcess.QueryTablet("update resharding1 set msg='msg-not-3' where id=3", keyspaceName, true)
	assert.Nil(t, err)

	// Insert row 4 and 5 (provokes a delete).
	insertValue(t, shard3.MasterTablet(), keyspaceName, tableName, 4, "msg4", key3)
	insertValue(t, shard3.MasterTablet(), keyspaceName, tableName, 5, "msg5", key3)

	err = clusterInstance.VtworkerProcess.ExecuteCommand("SplitClone",
		"--exclude_tables", "unrelated",
		"--chunk_count", "10",
		"--min_rows_per_chunk", "1",
		"--min_healthy_rdonly_tablets", "1",
		"--max_tps", "9999",
		shard1Ks)
	assert.Nil(t, err)

	// Change tablet, which was taken offline, back to rdonly.
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeSlaveType", shard1Rdonly.Alias, "rdonly")
	assert.Nil(t, err)

	// Terminate worker daemon because it is no longer needed.
	err = clusterInstance.VtworkerProcess.TearDown()
	assert.Nil(t, err)

	// Check startup values
	checkStartupValues(t, shardingKeyType)

	// check the schema too
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ValidateSchemaKeyspace", "--exclude_tables=unrelated", keyspaceName)
	assert.Nil(t, err)

	// Verify vreplication table entries
	qr, err := shard2Master.VttabletProcess.QueryTabletWithDB("select * from vreplication", "_vt")
	assert.Nil(t, err)
	assert.Equal(t, 1, len(qr.Rows))
	assert.Contains(t, fmt.Sprintf("%v", qr.Rows), "SplitClone")
	assert.Contains(t, fmt.Sprintf("%v", qr.Rows), `"keyspace:\"ks\" shard:\"80-\" key_range:<start:\"\\200\" end:\"\\300\" > "`)

	qr, err = shard3.MasterTablet().VttabletProcess.QueryTabletWithDB("select * from vreplication", "_vt")
	assert.Nil(t, err)
	assert.Equal(t, 1, len(qr.Rows))
	assert.Contains(t, fmt.Sprintf("%v", qr.Rows), "SplitClone")
	assert.Contains(t, fmt.Sprintf("%v", qr.Rows), `"keyspace:\"ks\" shard:\"80-\" key_range:<start:\"\\300\" > "`)

	// check the binlog players are running and exporting vars
	sharding.CheckDestinationMaster(t, *shard2Master, []string{shard1Ks}, *clusterInstance)
	sharding.CheckDestinationMaster(t, *shard3Master, []string{shard1Ks}, *clusterInstance)

	// When the binlog players/filtered replication is turned on, the query
	// service must be turned off on the destination masters.
	// The tested behavior is a safeguard to prevent that somebody can
	// accidentally modify data on the destination masters while they are not
	// migrated yet and the source shards are still the source of truth.
	err = shard2Master.VttabletProcess.WaitForTabletType("NOT_SERVING")
	assert.Nil(t, err)
	err = shard3Master.VttabletProcess.WaitForTabletType("NOT_SERVING")
	assert.Nil(t, err)

	// check that binlog server exported the stats vars
	sharding.CheckBinlogServerVars(t, *shard1Replica1, 0, 0)

	// Check that the throttler was enabled.
	// The stream id is hard-coded as 1, which is the first id generated through auto-inc.
	sharding.CheckThrottlerService(t, fmt.Sprintf("%s:%d", hostname, shard2Master.GrpcPort),
		[]string{"BinlogPlayer/1"}, 9999, *clusterInstance)
	sharding.CheckThrottlerService(t, fmt.Sprintf("%s:%d", hostname, shard3Master.GrpcPort),
		[]string{"BinlogPlayer/1"}, 9999, *clusterInstance)

	// testing filtered replication: insert a bunch of data on shard 1, check we get most of it after a few seconds,
	// wait for binlog server timeout, check we get all of it.
	log.Debug("Inserting lots of data on source shard")
	insertLots(100, 0, *shard1Master, tableName, fixedParentID, keyspaceName)
	log.Debug("Executing MultiValue Insert Queries")
	execMultiShardDmls(t, keyspaceName)

	// Checking 100 percent of data is sent quickly
	assert.True(t, checkLotsTimeout(t, 100, 0, tableName, keyspaceName, shardingKeyType))
	// Checking no data was sent the wrong way
	checkLotsNotPresent(t, 100, 0, tableName, keyspaceName, shardingKeyType)

	// Checking MultiValue Insert Queries
	checkMultiShardValues(t, keyspaceName, shardingKeyType)
	sharding.CheckBinlogPlayerVars(t, *shard2Master, []string{shard1Ks}, 30)
	sharding.CheckBinlogPlayerVars(t, *shard3Master, []string{shard1Ks}, 30)
	sharding.CheckBinlogServerVars(t, *shard1Replica1, 100, 100)

	// use vtworker to compare the data (after health-checking the destination
	// rdonly tablets so discovery works)
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", shard3Rdonly.Alias)
	assert.Nil(t, err)

	// use vtworker to compare the data
	clusterInstance.VtworkerProcess.Cell = cell1

	// Compare using SplitDiff
	log.Debug("Running vtworker SplitDiff")
	err = clusterInstance.VtworkerProcess.ExecuteVtworkerCommand(clusterInstance.GetAndReservePort(),
		clusterInstance.GetAndReservePort(),
		"--use_v3_resharding_mode=true",
		"SplitDiff",
		"--exclude_tables", "unrelated",
		"--min_healthy_rdonly_tablets", "1",
		shard3Ks)
	assert.Nil(t, err)

	// Compare using MultiSplitDiff
	log.Debug("Running vtworker MultiSplitDiff")
	err = clusterInstance.VtworkerProcess.ExecuteVtworkerCommand(clusterInstance.GetAndReservePort(),
		clusterInstance.GetAndReservePort(),
		"--use_v3_resharding_mode=true",
		"MultiSplitDiff",
		"--exclude_tables", "unrelated",
		shard1Ks)
	assert.Nil(t, err)

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeSlaveType", shard1Rdonly.Alias, "rdonly")
	assert.Nil(t, err)
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeSlaveType", shard3Rdonly.Alias, "rdonly")
	assert.Nil(t, err)

	// get status for destination master tablets, make sure we have it all
	sharding.CheckRunningBinlogPlayer(t, *shard2Master, 436, 216)
	sharding.CheckRunningBinlogPlayer(t, *shard3Master, 456, 216)

	// tests a failover switching serving to a different replica
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeSlaveType", shard1Replica2.Alias, "replica")
	assert.Nil(t, err)
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeSlaveType", shard1Replica1.Alias, "spare")
	assert.Nil(t, err)

	err = shard1Replica2.VttabletProcess.WaitForTabletType("SERVING")
	assert.Nil(t, err)
	err = shard1Replica1.VttabletProcess.WaitForTabletType("NOT_SERVING")
	assert.Nil(t, err)

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", shard1Replica2.Alias)
	assert.Nil(t, err)

	// test data goes through again
	log.Debug("Inserting lots of data on source shard")
	insertLots(100, 100, *shard1Master, tableName, fixedParentID, keyspaceName)
	log.Debug("Checking 100 percent of data was sent quickly")
	assert.True(t, checkLotsTimeout(t, 100, 100, tableName, keyspaceName, shardingKeyType))
	sharding.CheckBinlogServerVars(t, *shard1Replica2, 80, 80)

	// check we can't migrate the master just yet
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("MigrateServedTypes", shard1Ks, "master")
	assert.NotNil(t, err, "MigrateServedTypes should fail")

	// check query service is off on master 2 and master 3, as filtered replication is enabled.
	// Even health check that is enabled on master 3 should not interfere (we run it to be sure).
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", shard3Master.Alias)
	assert.Nil(t, err)

	for _, master := range []cluster.Vttablet{*shard2Master, *shard3Master} {
		sharding.CheckTabletQueryService(t, master, "NOT_SERVING", false, *clusterInstance)
		streamHealth, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput(
			"VtTabletStreamHealth",
			"-count", "1", master.Alias)
		assert.Nil(t, err)
		log.Debug("Got health: ", streamHealth)

		var streamHealthResponse querypb.StreamHealthResponse
		err = json.Unmarshal([]byte(streamHealth), &streamHealthResponse)
		assert.Nil(t, err)
		assert.Equal(t, streamHealthResponse.Serving, false)
		assert.NotNil(t, streamHealthResponse.RealtimeStats)

	}

	// check the destination master 3 is healthy, even though its query
	// service is not running (if not healthy this would exception out)
	sharding.VerifyTabletHealth(t, *shard3Master, hostname)

	// now serve rdonly from the split shards, in cell1 only
	err = clusterInstance.VtctlclientProcess.ExecuteCommand(
		"MigrateServedTypes", fmt.Sprintf("--cells=%s", cell1),
		shard1Ks, "rdonly")
	assert.Nil(t, err)

	// check srv keyspace
	expectedPartitions = map[topodata.TabletType][]string{}
	expectedPartitions[topodata.TabletType_MASTER] = []string{shard0.Name, shard1.Name}
	expectedPartitions[topodata.TabletType_RDONLY] = []string{shard0.Name, shard2.Name, shard3.Name}
	expectedPartitions[topodata.TabletType_REPLICA] = []string{shard0.Name, shard1.Name}
	sharding.CheckSrvKeyspace(t, cell1, keyspaceName, "", 0, expectedPartitions, *clusterInstance)

	// Cell 2 is not affected
	expectedPartitions = map[topodata.TabletType][]string{}
	expectedPartitions[topodata.TabletType_MASTER] = []string{shard0.Name, shard1.Name}
	expectedPartitions[topodata.TabletType_RDONLY] = []string{shard0.Name, shard1.Name}
	expectedPartitions[topodata.TabletType_REPLICA] = []string{shard0.Name, shard1.Name}
	sharding.CheckSrvKeyspace(t, cell2, keyspaceName, "", 0, expectedPartitions, *clusterInstance)

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
	assert.Nil(t, err)

	// now serve rdonly from the split shards, everywhere
	err = clusterInstance.VtctlclientProcess.ExecuteCommand(
		"MigrateServedTypes",
		shard1Ks, "rdonly")
	assert.Nil(t, err)

	expectedPartitions = map[topodata.TabletType][]string{}
	expectedPartitions[topodata.TabletType_MASTER] = []string{shard0.Name, shard1.Name}
	expectedPartitions[topodata.TabletType_RDONLY] = []string{shard0.Name, shard2.Name, shard3.Name}
	expectedPartitions[topodata.TabletType_REPLICA] = []string{shard0.Name, shard1.Name}
	sharding.CheckSrvKeyspace(t, cell1, keyspaceName, "", 0, expectedPartitions, *clusterInstance)
	// Cell 2 is also changed
	sharding.CheckSrvKeyspace(t, cell2, keyspaceName, "", 0, expectedPartitions, *clusterInstance)

	sharding.CheckTabletQueryService(t, *shard0RdonlyZ2, "SERVING", false, *clusterInstance)
	sharding.CheckTabletQueryService(t, *shard1RdonlyZ2, "NOT_SERVING", true, *clusterInstance)
	sharding.CheckTabletQueryService(t, *shard1Rdonly, "NOT_SERVING", true, *clusterInstance)

	// rerun migrate to ensure it doesn't fail
	// skip refresh to make it go faster
	err = clusterInstance.VtctlclientProcess.ExecuteCommand(
		"MigrateServedTypes",
		"-skip-refresh-state=true",
		shard1Ks, "rdonly")
	assert.Nil(t, err)

	// then serve replica from the split shards
	destinationShards := []cluster.Shard{*shard2, *shard3}
	err = clusterInstance.VtctlclientProcess.ExecuteCommand(
		"MigrateServedTypes", shard1Ks, "replica")
	assert.Nil(t, err)

	expectedPartitions = map[topodata.TabletType][]string{}
	expectedPartitions[topodata.TabletType_MASTER] = []string{shard0.Name, shard1.Name}
	expectedPartitions[topodata.TabletType_RDONLY] = []string{shard0.Name, shard2.Name, shard3.Name}
	expectedPartitions[topodata.TabletType_REPLICA] = []string{shard0.Name, shard2.Name, shard3.Name}
	sharding.CheckSrvKeyspace(t, cell1, keyspaceName, "", 0, expectedPartitions, *clusterInstance)

	// move replica back and forth
	err = clusterInstance.VtctlclientProcess.ExecuteCommand(
		"MigrateServedTypes", "-reverse",
		shard1Ks, "replica")
	assert.Nil(t, err)

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
	sharding.CheckSrvKeyspace(t, cell1, keyspaceName, "", 0, expectedPartitions, *clusterInstance)

	err = clusterInstance.VtctlclientProcess.ExecuteCommand(
		"MigrateServedTypes", shard1Ks, "replica")
	assert.Nil(t, err)
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
	sharding.CheckSrvKeyspace(t, cell1, keyspaceName, "", 0, expectedPartitions, *clusterInstance)

	// reparent shard2 to shard2Replica1, then insert more data and see it flow through still
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("PlannedReparentShard", "-keyspace_shard", shard2Ks,
		"-new_master", shard2Replica1.Alias)
	assert.Nil(t, err)

	// update our test variables to point at the new master
	tmp := shard2Master
	shard2Master = shard2Replica1
	shard2Replica1 = tmp

	// Insert another set of the data and see it flow through
	insertLots(100, 200, *shard1.MasterTablet(), tableName, fixedParentID, keyspaceName)
	// Checking 100 percent of data is sent fairly quickly
	assert.True(t, checkLotsTimeout(t, 100, 200, tableName, keyspaceName, shardingKeyType))

	// Compare using SplitDiff
	log.Debug("Running vtworker SplitDiff")
	err = clusterInstance.VtworkerProcess.ExecuteVtworkerCommand(clusterInstance.GetAndReservePort(),
		clusterInstance.GetAndReservePort(),
		"--use_v3_resharding_mode=true",
		"SplitDiff",
		"--exclude_tables", "unrelated",
		"--min_healthy_rdonly_tablets", "1",
		shard3Ks)
	assert.Nil(t, err)

	// Compare using MultiSplitDiff
	log.Debug("Running vtworker MultiSplitDiff")
	err = clusterInstance.VtworkerProcess.ExecuteVtworkerCommand(clusterInstance.GetAndReservePort(),
		clusterInstance.GetAndReservePort(),
		"--use_v3_resharding_mode=true",
		"MultiSplitDiff",
		"--exclude_tables", "unrelated",
		shard1Ks)
	assert.Nil(t, err)

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeSlaveType", shard1Rdonly.Alias, "rdonly")
	assert.Nil(t, err)
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeSlaveType", shard3Rdonly.Alias, "rdonly")
	assert.Nil(t, err)

	// going to migrate the master now

	// mock with the SourceShard records to test 'vtctl SourceShardDelete'  and 'vtctl SourceShardAdd'
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("SourceShardDelete", shard3Ks, "1")
	assert.Nil(t, err)
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("SourceShardAdd", "--key_range=80-",
		shard3Ks, "1", shard1Ks)
	assert.Nil(t, err)

	// CancelResharding should fail because migration has started.
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("CancelResharding", shard1Ks, "1")
	assert.NotNil(t, err)

	// do a Migrate that will fail waiting for replication
	// which should cause the Migrate to be canceled and the source
	// master to be serving again.
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("MigrateServedTypes",
		"-filtered_replication_wait_time", "0s", shard1Ks, "master")
	assert.NotNil(t, err)

	expectedPartitions = map[topodata.TabletType][]string{}
	expectedPartitions[topodata.TabletType_MASTER] = []string{shard0.Name, shard1.Name}
	expectedPartitions[topodata.TabletType_RDONLY] = []string{shard0.Name, shard2.Name, shard3.Name}
	expectedPartitions[topodata.TabletType_REPLICA] = []string{shard0.Name, shard2.Name, shard3.Name}
	sharding.CheckSrvKeyspace(t, cell1, keyspaceName, "", 0, expectedPartitions, *clusterInstance)

	sharding.CheckTabletQueryService(t, *shard1Master, "SERVING", false, *clusterInstance)

	// sabotage master migration and make it fail in an unfinished state.
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("SetShardTabletControl",
		"-blacklisted_tables=t",
		shard3Ks, "master")
	assert.Nil(t, err)

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("MigrateServedTypes",
		shard1Ks, "master")
	assert.NotNil(t, err)

	// Query service is disabled in source shard as failure occurred after point of no return
	sharding.CheckTabletQueryService(t, *shard1Master, "NOT_SERVING", true, *clusterInstance)

	// Global topology records should not change as migration did not succeed
	shardInfo := sharding.GetShardInfo(t, shard1Ks, *clusterInstance)
	assert.True(t, shardInfo.GetIsMasterServing(), "source shards should be set in destination shard")

	shardInfo = sharding.GetShardInfo(t, shard3Ks, *clusterInstance)
	assert.Equal(t, 1, len(shardInfo.GetSourceShards()), "source shards should be set in destination shard")
	assert.False(t, shardInfo.GetIsMasterServing(), "source shards should be set in destination shard")

	shardInfo = sharding.GetShardInfo(t, shard2Ks, *clusterInstance)
	assert.Equal(t, 1, len(shardInfo.GetSourceShards()), "source shards should be set in destination shard")
	assert.False(t, shardInfo.GetIsMasterServing(), "source shards should be set in destination shard")

	// remove sabotage, but make it fail early. This should not result in the source master serving,
	// because this failure is past the point of no return.
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("SetShardTabletControl", "-blacklisted_tables=t",
		"-remove", shard3Ks, "master")
	assert.Nil(t, err)
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("MigrateServedTypes",
		"-filtered_replication_wait_time", "0s", shard1Ks, "master")
	assert.NotNil(t, err)

	sharding.CheckTabletQueryService(t, *shard1Master, "NOT_SERVING", true, *clusterInstance)

	// do the migration that's expected to succeed
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("MigrateServedTypes",
		shard1Ks, "master")
	assert.Nil(t, err)
	expectedPartitions = map[topodata.TabletType][]string{}
	expectedPartitions[topodata.TabletType_MASTER] = []string{shard0.Name, shard2.Name, shard3.Name}
	expectedPartitions[topodata.TabletType_RDONLY] = []string{shard0.Name, shard2.Name, shard3.Name}
	expectedPartitions[topodata.TabletType_REPLICA] = []string{shard0.Name, shard2.Name, shard3.Name}
	sharding.CheckSrvKeyspace(t, cell1, keyspaceName, "", 0, expectedPartitions, *clusterInstance)

	sharding.CheckTabletQueryService(t, *shard1Master, "NOT_SERVING", true, *clusterInstance)

	// check destination shards are serving
	sharding.CheckTabletQueryService(t, *shard2Master, "SERVING", false, *clusterInstance)
	sharding.CheckTabletQueryService(t, *shard3Master, "SERVING", false, *clusterInstance)

	// check the binlog players are gone now
	err = shard2Master.VttabletProcess.WaitForBinLogPlayerCount(0)
	assert.Nil(t, err)
	err = shard3Master.VttabletProcess.WaitForBinLogPlayerCount(0)
	assert.Nil(t, err)

	// test reverse_replication
	// start with inserting a row in each destination shard
	insertValue(t, shard2Master, keyspaceName, "resharding2", 2, "msg2", key2)
	insertValue(t, shard3Master, keyspaceName, "resharding2", 3, "msg3", key3)

	// ensure the rows are not present yet
	checkValues(t, *shard1Master, []string{"INT64(86)", "INT64(2)", `VARCHAR("msg2")`, fmt.Sprintf("UINT64(%d)", key2)},
		2, false, "resharding2", fixedParentID, keyspaceName, shardingKeyType)
	checkValues(t, *shard1Master, []string{"INT64(86)", "INT64(3)", `VARCHAR("msg3")`, fmt.Sprintf("UINT64(%d)", key3)},
		3, false, "resharding2", fixedParentID, keyspaceName, shardingKeyType)

	// repeat the migration with reverse_replication
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("MigrateServedTypes", "-reverse_replication=true",
		shard1Ks, "master")
	assert.Nil(t, err)
	// look for the rows in the original master after a short wait
	time.Sleep(1 * time.Second)
	checkValues(t, *shard1Master, []string{"INT64(86)", "INT64(2)", `VARCHAR("msg2")`, fmt.Sprintf("UINT64(%d)", key2)},
		2, true, "resharding2", fixedParentID, keyspaceName, shardingKeyType)
	checkValues(t, *shard1Master, []string{"INT64(86)", "INT64(3)", `VARCHAR("msg3")`, fmt.Sprintf("UINT64(%d)", key3)},
		3, true, "resharding2", fixedParentID, keyspaceName, shardingKeyType)

	// retry the migration to ensure it now fails
	err = clusterInstance.VtctlclientProcess.ExecuteCommand(
		"MigrateServedTypes",
		"-reverse_replication=true",
		shard1Ks, "master")
	assert.NotNil(t, err)

	// CancelResharding should now succeed
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("CancelResharding", shard1Ks)
	assert.Nil(t, err)
	err = shard1Master.VttabletProcess.WaitForBinLogPlayerCount(0)
	assert.Nil(t, err)

	// delete the original tablets in the original shard
	for _, tablet := range []cluster.Vttablet{*shard1Master, *shard1Replica1, *shard1Replica2, *shard1Rdonly, *shard1RdonlyZ2} {
		_ = tablet.MysqlctlProcess.Stop()
		_ = tablet.VttabletProcess.TearDown()
	}

	for _, tablet := range []cluster.Vttablet{*shard1Replica1, *shard1Replica2, *shard1Rdonly, *shard1RdonlyZ2} {
		err = clusterInstance.VtctlclientProcess.ExecuteCommand("DeleteTablet", tablet.Alias)
		assert.Nil(t, err)
	}
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("DeleteTablet", "-allow_master", shard1Master.Alias)
	assert.Nil(t, err)

	// rebuild the serving graph, all mentions of the old shards should be gone
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RebuildKeyspaceGraph", keyspaceName)
	assert.Nil(t, err)

	// test RemoveShardCell
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RemoveShardCell", shard0Ks, cell1)
	assert.NotNil(t, err)
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RemoveShardCell", shard1Ks, cell1)
	assert.Nil(t, err)
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RemoveShardCell", shard1Ks, cell2)
	assert.Nil(t, err)

	shardInfo = sharding.GetShardInfo(t, shard1Ks, *clusterInstance)
	assert.Empty(t, shardInfo.GetTabletControls(), "cells not in shard ")

	// delete the original shard
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("DeleteShard", shard1Ks)
	assert.Nil(t, err)

	// make sure we can't delete the destination shard now that it's serving
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("DeleteShard", shard2Ks)
	assert.NotNil(t, err)

}

func insertStartupValues(t *testing.T) {
	insertSQL := fmt.Sprintf(insertTabletTemplateKsID, "resharding1", fixedParentID, 1, "msg1", key1, key1, 1)
	sharding.InsertToTablet(t, insertSQL, *shard0.MasterTablet(), keyspaceName, false)

	insertSQL = fmt.Sprintf(insertTabletTemplateKsID, "resharding1", fixedParentID, 2, "msg2", key2, key2, 2)
	sharding.InsertToTablet(t, insertSQL, *shard1.MasterTablet(), keyspaceName, false)

	insertSQL = fmt.Sprintf(insertTabletTemplateKsID, "resharding1", fixedParentID, 3, "msg3", key3, key3, 3)
	sharding.InsertToTablet(t, insertSQL, *shard1.MasterTablet(), keyspaceName, false)

	insertSQL = fmt.Sprintf(insertTabletTemplateKsID, "resharding3", fixedParentID, 1, "a", key1, key1, 1)
	sharding.InsertToTablet(t, insertSQL, *shard0.MasterTablet(), keyspaceName, false)

	insertSQL = fmt.Sprintf(insertTabletTemplateKsID, "resharding3", fixedParentID, 2, "b", key2, key2, 2)
	sharding.InsertToTablet(t, insertSQL, *shard1.MasterTablet(), keyspaceName, false)

	insertSQL = fmt.Sprintf(insertTabletTemplateKsID, "resharding3", fixedParentID, 3, "c", key3, key3, 3)
	sharding.InsertToTablet(t, insertSQL, *shard1.MasterTablet(), keyspaceName, false)

	insertSQL = fmt.Sprintf(insertTabletTemplateKsID, "no_pk", fixedParentID, 1, "msg1", key5, key5, 1)
	sharding.InsertToTablet(t, insertSQL, *shard1.MasterTablet(), keyspaceName, false)
}

func insertValue(t *testing.T, tablet *cluster.Vttablet, keyspaceName string, tableName string, id int, msg string, ksID uint64) {
	insertSQL := fmt.Sprintf(insertTabletTemplateKsID, tableName, fixedParentID, id, msg, ksID, ksID, id)
	sharding.InsertToTablet(t, insertSQL, *tablet, keyspaceName, false)
}

func execMultiShardDmls(t *testing.T, keyspaceName string) {
	ids := []int{10000001, 10000002, 10000003}
	msgs := []string{"msg-id10000001", "msg-id10000002", "msg-id10000003"}
	ksIds := []uint64{key2, key3, key4}
	sharding.InsertMultiValueToTablet(t, *shard1.MasterTablet(), keyspaceName, "resharding1", fixedParentID, ids, msgs, ksIds)

	ids = []int{10000004, 10000005}
	msgs = []string{"msg-id10000004", "msg-id10000005"}
	ksIds = []uint64{key3, key4}
	sharding.InsertMultiValueToTablet(t, *shard1.MasterTablet(), keyspaceName, "resharding1", fixedParentID, ids, msgs, ksIds)

	ids = []int{10000011, 10000012, 10000013}
	msgs = []string{"msg-id10000011", "msg-id10000012", "msg-id10000013"}
	ksIds = []uint64{key2, key3, key4}
	sharding.InsertMultiValueToTablet(t, *shard1.MasterTablet(), keyspaceName, "resharding1", fixedParentID, ids, msgs, ksIds)

	// This update targets two shards.
	sql := `update resharding1 set msg="update1" where parent_id=86 and id in (10000011,10000012)`
	_, _ = shard1.MasterTablet().VttabletProcess.QueryTablet(sql, keyspaceName, true)

	// This update targets one shard.
	sql = `update resharding1 set msg="update1" where parent_id=86 and id in (10000013)`
	_, _ = shard1.MasterTablet().VttabletProcess.QueryTablet(sql, keyspaceName, true)

	ids = []int{10000014, 10000015, 10000016}
	msgs = []string{"msg-id10000014", "msg-id10000015", "msg-id10000016"}
	ksIds = []uint64{key2, key3, key4}
	sharding.InsertMultiValueToTablet(t, *shard1.MasterTablet(), keyspaceName, "resharding1", fixedParentID, ids, msgs, ksIds)

	// This delete targets two shards.
	sql = `delete from resharding1 where parent_id =86 and id in (10000014, 10000015)`
	_, _ = shard1.MasterTablet().VttabletProcess.QueryTablet(sql, keyspaceName, true)
	// This delete targets one shard.
	sql = `delete from resharding1 where parent_id =86 and id in (10000016)`
	_, _ = shard1.MasterTablet().VttabletProcess.QueryTablet(sql, keyspaceName, true)

	// repeat DMLs for table with msg as bit(8)
	ids = []int{10000001, 10000002, 10000003}
	msgs = []string{"a", "b", "c"}
	ksIds = []uint64{key2, key3, key4}
	sharding.InsertMultiValueToTablet(t, *shard1.MasterTablet(), keyspaceName, "resharding3", fixedParentID, ids, msgs, ksIds)

	ids = []int{10000004, 10000005}
	msgs = []string{"d", "e"}
	ksIds = []uint64{key3, key4}
	sharding.InsertMultiValueToTablet(t, *shard1.MasterTablet(), keyspaceName, "resharding3", fixedParentID, ids, msgs, ksIds)

	ids = []int{10000011, 10000012, 10000013}
	msgs = []string{"k", "l", "m"}
	ksIds = []uint64{key2, key3, key4}
	sharding.InsertMultiValueToTablet(t, *shard1.MasterTablet(), keyspaceName, "resharding3", fixedParentID, ids, msgs, ksIds)

	// This update targets two shards.
	sql = `update resharding3 set msg="g" where parent_id=86 and id in (10000011, 10000012)`
	_, _ = shard1.MasterTablet().VttabletProcess.QueryTablet(sql, keyspaceName, true)

	// This update targets one shard.
	sql = `update resharding3 set msg="h" where parent_id=86 and id in (10000013)`
	_, _ = shard1.MasterTablet().VttabletProcess.QueryTablet(sql, keyspaceName, true)

	ids = []int{10000014, 10000015, 10000016}
	msgs = []string{"n", "o", "p"}
	ksIds = []uint64{key2, key3, key4}
	sharding.InsertMultiValueToTablet(t, *shard1.MasterTablet(), keyspaceName, "resharding3", fixedParentID, ids, msgs, ksIds)

	// This delete targets two shards.
	sql = `delete from resharding3 where parent_id =86 and id in (10000014, 10000015)`
	_, _ = shard1.MasterTablet().VttabletProcess.QueryTablet(sql, keyspaceName, true)
	// This delete targets one shard.
	sql = `delete from resharding3 where parent_id =86 and id in (10000016)`
	_, _ = shard1.MasterTablet().VttabletProcess.QueryTablet(sql, keyspaceName, true)

}

func checkStartupValues(t *testing.T, shardingKeyType querypb.Type) {
	// check first value is in the right shard
	for _, tablet := range shard2.Vttablets {
		checkValues(t, *tablet, []string{"INT64(86)", "INT64(2)", `VARCHAR("msg2")`, fmt.Sprintf("UINT64(%d)", key2)},
			2, true, "resharding1", fixedParentID, keyspaceName, shardingKeyType)
		checkValues(t, *tablet, []string{"INT64(86)", "INT64(2)", `BIT("b")`, fmt.Sprintf("UINT64(%d)", key2)},
			2, true, "resharding3", fixedParentID, keyspaceName, shardingKeyType)
	}
	for _, tablet := range shard3.Vttablets {
		checkValues(t, *tablet, []string{"INT64(86)", "INT64(2)", `VARCHAR("msg2")`, fmt.Sprintf("UINT64(%d)", key2)},
			2, false, "resharding1", fixedParentID, keyspaceName, shardingKeyType)
		checkValues(t, *tablet, []string{"INT64(86)", "INT64(2)", `BIT("b")`, fmt.Sprintf("UINT64(%d)", key2)},
			2, false, "resharding3", fixedParentID, keyspaceName, shardingKeyType)
	}
	// check first value is in the right shard
	for _, tablet := range shard2.Vttablets {
		checkValues(t, *tablet, []string{"INT64(86)", "INT64(3)", `VARCHAR("msg3")`, fmt.Sprintf("UINT64(%d)", key3)},
			3, false, "resharding1", fixedParentID, keyspaceName, shardingKeyType)
		checkValues(t, *tablet, []string{"INT64(86)", "INT64(3)", `BIT("c")`, fmt.Sprintf("UINT64(%d)", key3)},
			3, false, "resharding3", fixedParentID, keyspaceName, shardingKeyType)
	}
	for _, tablet := range shard3.Vttablets {
		checkValues(t, *tablet, []string{"INT64(86)", "INT64(3)", `VARCHAR("msg3")`, fmt.Sprintf("UINT64(%d)", key3)},
			3, true, "resharding1", fixedParentID, keyspaceName, shardingKeyType)
		checkValues(t, *tablet, []string{"INT64(86)", "INT64(3)", `BIT("c")`, fmt.Sprintf("UINT64(%d)", key3)},
			3, true, "resharding3", fixedParentID, keyspaceName, shardingKeyType)
	}

	// Check for no_pk table
	for _, tablet := range shard2.Vttablets {
		checkValues(t, *tablet, []string{"INT64(86)", "INT64(1)", `VARCHAR("msg1")`, fmt.Sprintf("UINT64(%d)", key5)},
			1, true, "no_pk", fixedParentID, keyspaceName, shardingKeyType)
	}
	for _, tablet := range shard3.Vttablets {
		checkValues(t, *tablet, []string{"INT64(86)", "INT64(1)", `BIT("msg1")`, fmt.Sprintf("UINT64(%d)", key5)},
			1, false, "no_pk", fixedParentID, keyspaceName, shardingKeyType)
	}
}

// checkLotsNotPresent verifies that no rows should be present in vttablet
func checkLotsNotPresent(t *testing.T, count uint64, base uint64, table string, keyspaceName string, keyType querypb.Type) {
	var i uint64
	//var count uint64 = 1000
	for i = 0; i < count; i++ {
		assert.False(t, checkValues(t, *shard3.Vttablets[1], []string{"INT64(86)",
			fmt.Sprintf("INT64(%d)", 10000+base+i),
			fmt.Sprintf(`VARCHAR("msg-range1-%d")`, 10000+base+i),
			fmt.Sprintf("UINT64(%d)", key5)},
			10000+base+i, false, table, fixedParentID, keyspaceName, keyType))

		assert.False(t, checkValues(t, *shard2.Vttablets[2], []string{"INT64(86)",
			fmt.Sprintf("INT64(%d)", 20000+base+i),
			fmt.Sprintf(`VARCHAR("msg-range2-%d")`, 20000+base+i),
			fmt.Sprintf("UINT64(%d)", key4)},
			20000+base+i, false, table, fixedParentID, keyspaceName, keyType))
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
	var isFound bool
	var totalFound int
	var i uint64
	for i = 0; i < count; i++ {
		isFound = checkValues(t, *shard2.Vttablets[2], []string{"INT64(86)",
			fmt.Sprintf("INT64(%d)", 10000+base+i),
			fmt.Sprintf(`VARCHAR("msg-range1-%d")`, 10000+base+i),
			fmt.Sprintf("UINT64(%d)", key5)},
			10000+base+i, true, table, fixedParentID, keyspaceName, keyType)
		if isFound {
			totalFound++
		}

		isFound = checkValues(t, *shard3.Vttablets[1], []string{"INT64(86)",
			fmt.Sprintf("INT64(%d)", 20000+base+i),
			fmt.Sprintf(`VARCHAR("msg-range2-%d")`, 20000+base+i),
			fmt.Sprintf("UINT64(%d)", key4)},
			20000+base+i, true, table, fixedParentID, keyspaceName, keyType)
		if isFound {
			totalFound++
		}
	}
	return float32(totalFound * 100 / int(count) / 2)
}

func checkValues(t *testing.T, vttablet cluster.Vttablet, values []string, id uint64, exists bool, tableName string, parentID int, ks string, keyType querypb.Type) bool {
	query := fmt.Sprintf("select parent_id, id, msg, custom_ksid_col from %s where parent_id = %d and id = %d", tableName, parentID, id)
	result, err := vttablet.VttabletProcess.QueryTablet(query, ks, true)
	assert.Nil(t, err)
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

func checkMultiShardValues(t *testing.T, keyspaceName string, keyType querypb.Type) {
	//Shard2 master, replica1 and replica 2
	shard2Tablets := []cluster.Vttablet{*shard2.Vttablets[0], *shard2.Vttablets[1], *shard2.Vttablets[2]}
	checkMultiDbs(t, shard2Tablets, "resharding1", keyspaceName, keyType, 10000001, "msg-id10000001", key2, true)
	checkMultiDbs(t, shard2Tablets, "resharding1", keyspaceName, keyType, 10000002, "msg-id10000002", key3, false)
	checkMultiDbs(t, shard2Tablets, "resharding1", keyspaceName, keyType, 10000003, "msg-id10000003", key4, false)
	checkMultiDbs(t, shard2Tablets, "resharding1", keyspaceName, keyType, 10000004, "msg-id10000004", key3, false)
	checkMultiDbs(t, shard2Tablets, "resharding1", keyspaceName, keyType, 10000005, "msg-id10000005", key4, false)

	//Shard 3 master and replica
	shard3Tablets := []cluster.Vttablet{*shard3.Vttablets[0], *shard3.Vttablets[1]}
	checkMultiDbs(t, shard3Tablets, "resharding1", keyspaceName, keyType, 10000001, "msg-id10000001", key2, false)
	checkMultiDbs(t, shard3Tablets, "resharding1", keyspaceName, keyType, 10000002, "msg-id10000002", key3, true)
	checkMultiDbs(t, shard3Tablets, "resharding1", keyspaceName, keyType, 10000003, "msg-id10000003", key4, true)
	checkMultiDbs(t, shard3Tablets, "resharding1", keyspaceName, keyType, 10000004, "msg-id10000004", key3, true)
	checkMultiDbs(t, shard3Tablets, "resharding1", keyspaceName, keyType, 10000005, "msg-id10000005", key4, true)

	// Updated values
	checkMultiDbs(t, shard2Tablets, "resharding1", keyspaceName, keyType, 10000011, "update1", key2, true)
	checkMultiDbs(t, shard3Tablets, "resharding1", keyspaceName, keyType, 10000012, "update1", key3, true)
	checkMultiDbs(t, shard3Tablets, "resharding1", keyspaceName, keyType, 10000013, "update2", key4, true)

	allTablets := []cluster.Vttablet{*shard2.Vttablets[0], *shard2.Vttablets[1], *shard2.Vttablets[2], *shard3.Vttablets[0], *shard3.Vttablets[1]}
	checkMultiDbs(t, allTablets, "resharding1", keyspaceName, keyType, 10000014, "msg-id10000014", key2, false)
	checkMultiDbs(t, allTablets, "resharding1", keyspaceName, keyType, 10000015, "msg-id10000015", key3, false)
	checkMultiDbs(t, allTablets, "resharding1", keyspaceName, keyType, 10000016, "msg-id10000016", key6, false)

	// checks for bit(8) table
	checkMultiDbs(t, shard2Tablets, "resharding3", keyspaceName, keyType, 10000001, "a", key2, true)
	checkMultiDbs(t, shard2Tablets, "resharding3", keyspaceName, keyType, 10000002, "b", key3, false)
	checkMultiDbs(t, shard2Tablets, "resharding3", keyspaceName, keyType, 10000003, "c", key4, false)
	checkMultiDbs(t, shard2Tablets, "resharding3", keyspaceName, keyType, 10000004, "d", key3, false)
	checkMultiDbs(t, shard2Tablets, "resharding3", keyspaceName, keyType, 10000005, "e", key4, false)

	checkMultiDbs(t, shard3Tablets, "resharding3", keyspaceName, keyType, 10000001, "a", key2, false)
	checkMultiDbs(t, shard3Tablets, "resharding3", keyspaceName, keyType, 10000002, "b", key3, true)
	checkMultiDbs(t, shard3Tablets, "resharding3", keyspaceName, keyType, 10000003, "c", key4, true)
	checkMultiDbs(t, shard3Tablets, "resharding3", keyspaceName, keyType, 10000004, "d", key3, true)
	checkMultiDbs(t, shard3Tablets, "resharding3", keyspaceName, keyType, 10000005, "e", key4, true)

	// updated values
	checkMultiDbs(t, shard2Tablets, "resharding3", keyspaceName, keyType, 10000011, "g", key2, true)
	checkMultiDbs(t, shard3Tablets, "resharding3", keyspaceName, keyType, 10000012, "g", key3, true)
	checkMultiDbs(t, shard3Tablets, "resharding3", keyspaceName, keyType, 10000013, "h", key4, true)

	checkMultiDbs(t, allTablets, "resharding3", keyspaceName, keyType, 10000014, "n", key2, false)
	checkMultiDbs(t, allTablets, "resharding3", keyspaceName, keyType, 10000015, "o", key3, false)
	checkMultiDbs(t, allTablets, "resharding3", keyspaceName, keyType, 10000016, "p", key6, false)

}

// checkMultiDbs checks the row in multiple dbs
func checkMultiDbs(t *testing.T, vttablets []cluster.Vttablet, tableName string, keyspaceName string,
	keyType querypb.Type, id int, msg string, ksID uint64, presentInDb bool) {
	for _, tablet := range vttablets {
		checkValues(t, tablet, []string{"INT64(86)",
			fmt.Sprintf("INT64(%d)", id),
			fmt.Sprintf(`VARCHAR("%s")`, msg),
			fmt.Sprintf("UINT64(%d)", ksID)},
			ksID, presentInDb, tableName, fixedParentID, keyspaceName, keyType)
	}
}

// insertLots inserts multiple values to vttablet
func insertLots(count uint64, base uint64, vttablet cluster.Vttablet, table string, parentID int, ks string) {
	ctx := context.Background()
	dbParams := mysql.ConnParams{
		Uname:      "vt_dba",
		UnixSocket: path.Join(vttablet.VttabletProcess.Directory, "mysql.sock"),
		DbName:     "vt_" + ks,
	}
	dbConn, _ := mysql.Connect(ctx, &dbParams)
	defer dbConn.Close()

	var query1, query2 string
	var i uint64
	for i = 0; i < count; i++ {
		query1 = fmt.Sprintf(insertTabletTemplateKsID, table, parentID, 10000+base+i,
			fmt.Sprintf("msg-range1-%d", 10000+base+i), key5, key5, 10000+base+i)
		query2 = fmt.Sprintf(insertTabletTemplateKsID, table, parentID, 20000+base+i,
			fmt.Sprintf("msg-range2-%d", 20000+base+i), key4, key4, 20000+base+i)

		insertToTabletUsingSameConn(query1, vttablet, ks, dbConn)
		insertToTabletUsingSameConn(query2, vttablet, ks, dbConn)
	}
}

// insertToTabletUsingSameConn inserts a single row to vttablet using existing connection
func insertToTabletUsingSameConn(query string, vttablet cluster.Vttablet, ks string, dbConn *mysql.Conn) {
	_, err := dbConn.ExecuteFetch(query, 1000, true)
	if err != nil {
		fmt.Println(err)
	}
}
