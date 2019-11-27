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
	"fmt"
	"testing"

	"vitess.io/vitess/go/test/endtoend/sharding"

	"github.com/stretchr/testify/assert"
	"vitess.io/vitess/go/test/endtoend/cluster"
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
							create view %s (parent_id, id, msg, custom_ksid_col)
							as select parent_id, id, msg, custom_ksid_col from %s);
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
)

// TestReSharding - main test with accepts different params for various test
func TestReSharding(t *testing.T) {
	clusterInstance = &cluster.LocalProcessCluster{Cell: cell, Hostname: hostname}
	defer clusterInstance.Teardown()

	clusterInstance.VtTabletExtraArgs = []string{
		"-vreplication_healthcheck_topology_refresh", "1s",
		"-vreplication_healthcheck_retry_delay", "1s",
		"-vreplication_retry_delay", "1s",
		"-degraded_threshold", "5s",
		"-lock_tables_timeout", "5s",
		"-watch_replication_stream",
		"-enable_semi_sync",
	}

	ksIdType := "bigint(20) unsigned"

	// Launch keyspace
	keyspace := &cluster.Keyspace{Name: keyspaceName}

	// initial shards
	// range '' - 80  &  range 80 - ''
	shard0 := &cluster.Shard{Name: "-80", ReplicaCount: 1, RdonlyCount: 1}
	shard1 := &cluster.Shard{Name: "80-", ReplicaCount: 2, RdonlyCount: 1}

	// split shards
	// range 80 - c0    &   range c0 - ''
	shard2 := &cluster.Shard{Name: "80-c0", ReplicaCount: 2, RdonlyCount: 1}
	shard3 := &cluster.Shard{Name: "c0-", ReplicaCount: 1, RdonlyCount: 1}

	// Initialize Cluster
	err := clusterInstance.LaunchCluster(keyspace, []*cluster.Shard{shard0, shard1, shard2, shard3})
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, len(clusterInstance.Keyspaces[0].Shards), 4)

	// Write error test case
	//
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("SetKeyspaceShardingInfo",
		"-force", keyspaceName, "custom_ksid_col", ksIdType)

	//Start MySql
	for _, shard := range clusterInstance.Keyspaces[0].Shards {
		for _, tablet := range shard.Vttablets {
			fmt.Println("Starting MySql for tablet ", tablet.Alias)
			tablet.MysqlctlProcess = *cluster.MysqlCtlProcessInstance(tablet.TabletUID, tablet.MySQLPort, clusterInstance.TmpDirectory)
			if err = tablet.MysqlctlProcess.Start(); err != nil {
				t.Fatal(err)
			}
		}
	}

	// Rebuild keyspace Graph
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RebuildKeyspaceGraph", keyspaceName)
	output, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("GetKeyspace", keyspaceName)
	// assert self.assertEqual(ks['sharding_column_name'], 'custom_ksid_col')
	fmt.Println(output)

	//Start Tablets and Wait for the Process
	for _, shard := range clusterInstance.Keyspaces[0].Shards {
		for _, tablet := range shard.Vttablets {
			err = clusterInstance.StartVttablet(&tablet, "NOT_SERVING", false, cell, keyspaceName, hostname, shard.Name)
			assert.Nil(t, err, "error should be Nil")
			//Create DB
			tablet.VttabletProcess.QueryTablet(fmt.Sprintf("create database vt_%s", keyspace.Name), keyspace.Name, false)
		}
	}
	fmt.Println("*******", clusterInstance.Keyspaces[0].Shards[0].Vttablets)
	fmt.Println("*******", shard0.Vttablets)
	// Init Shard Master
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("InitShardMaster",
		"-force", fmt.Sprintf("%s/%s", keyspaceName, shard0.Name), shard0.MasterTablet().Alias)

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("InitShardMaster",
		"-force", fmt.Sprintf("%s/%s", keyspaceName, shard1.Name), shard1.MasterTablet().Alias)

	//Apply Schema
	_ = clusterInstance.VtctlclientProcess.ApplySchema(keyspaceName, fmt.Sprintf(createTabletTemplate, "resharding1", ksIdType))
	_ = clusterInstance.VtctlclientProcess.ApplySchema(keyspaceName, fmt.Sprintf(createTabletTemplate, "resharding2", ksIdType))
	_ = clusterInstance.VtctlclientProcess.ApplySchema(keyspaceName, fmt.Sprintf(createTableBindataTemplate, "resharding3", ksIdType))
	_ = clusterInstance.VtctlclientProcess.ApplySchema(keyspaceName, fmt.Sprintf(createViewTemplate, "view1", "resharding3"))
	_ = clusterInstance.VtctlclientProcess.ApplySchema(keyspaceName, fmt.Sprintf(createTimestampTable, ksIdType))
	_ = clusterInstance.VtctlclientProcess.ApplySchema(keyspaceName, fmt.Sprintf(createUnrelatedTable))

	// Insert Data
	var ksID uint64 = 0x1000000000000000
	insertSQL := fmt.Sprintf(sharding.InsertTabletTemplateKsID, "resharding1", fixedParentID, 1, "msg1", ksID, ksID, 1)
	sharding.InsertToTablet(insertSQL, *shard0.MasterTablet(), keyspaceName)

	var ksID2 uint64 = 0x9000000000000000
	insertSQL = fmt.Sprintf(sharding.InsertTabletTemplateKsID, "resharding1", fixedParentID, 2, "msg2", ksID2, ksID2, 2)
	sharding.InsertToTablet(insertSQL, *shard1.MasterTablet(), keyspaceName)

	var ksID3 uint64 = 0xD000000000000000
	insertSQL = fmt.Sprintf(sharding.InsertTabletTemplateKsID, "resharding1", fixedParentID, 3, "msg3", ksID3, ksID3, 3)
	sharding.InsertToTablet(insertSQL, *shard1.MasterTablet(), keyspaceName)

	insertSQL = fmt.Sprintf(sharding.InsertTabletTemplateKsID, "resharding1", fixedParentID, 1, "a", ksID, ksID, 1)
	sharding.InsertToTablet(insertSQL, *shard0.MasterTablet(), keyspaceName)

	insertSQL = fmt.Sprintf(sharding.InsertTabletTemplateKsID, "resharding1", fixedParentID, 2, "b", ksID2, ksID2, 1)
	sharding.InsertToTablet(insertSQL, *shard1.MasterTablet(), keyspaceName)

	insertSQL = fmt.Sprintf(sharding.InsertTabletTemplateKsID, "resharding1", fixedParentID, 3, "c", ksID3, ksID3, 1)
	sharding.InsertToTablet(insertSQL, *shard1.MasterTablet(), keyspaceName)

	// run a health check on source replicas so they respond to discovery
	// (for binlog players) and on the source rdonlys (for workers)
	for _, shard := range keyspace.Shards {
		for _, tablet := range shard.Vttablets {
			err = clusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", tablet.Alias)
			assert.Nil(t, err, "error should be Nil")
		}
	}

	//
	//// start replica
	//err = shard1.Replica().VttabletProcess.Setup()
	//assert.Nil(t, err)
	//
	//// reparent to make the tablets work
	//err = ClusterInstance.VtctlclientProcess.InitShardMaster(keyspaceName, shard1.Name, cell, shard1MasterTablet.TabletUID)
	//assert.Nil(t, err)
	//
	//_ = shard1.Replica().VttabletProcess.WaitForTabletType("SERVING")
	//_ = shard1.Rdonly().VttabletProcess.WaitForTabletType("SERVING")
	//for _, vttablet := range shard1.Vttablets {
	//	assert.Equal(t, vttablet.VttabletProcess.GetTabletStatus(), "SERVING")
	//}
	//// create the tables and add startup values
	//_ = ClusterInstance.VtctlclientProcess.ApplySchema(keyspaceName, fmt.Sprintf(createTabletTemplate, tableName))
	//_, _ = shard1MasterTablet.VttabletProcess.QueryTablet(fmt.Sprintf(insertTabletTemplate, "resharding1", fixedParentID, 1, "msg1"), keyspaceName, true)
	//_, _ = shard1MasterTablet.VttabletProcess.QueryTablet(fmt.Sprintf(insertTabletTemplate, "resharding1", fixedParentID, 2, "msg2"), keyspaceName, true)
	//_, _ = shard1MasterTablet.VttabletProcess.QueryTablet(fmt.Sprintf(insertTabletTemplate, "resharding1", fixedParentID, 3, "msg3"), keyspaceName, true)
	//
	//// reload schema on all tablets so we can query them
	//for _, vttablet := range shard1.Vttablets {
	//	_ = ClusterInstance.VtctlclientProcess.ExecuteCommand("ReloadSchema", vttablet.Alias)
	//}
	//err = ClusterInstance.StartVtgate()
	//assert.Nil(t, err)
	//
	//_ = ClusterInstance.VtgateProcess.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", keyspaceName, shard1.Name))
	//_ = ClusterInstance.VtgateProcess.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.replica", keyspaceName, shard1.Name))
	//_ = ClusterInstance.VtgateProcess.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.rdonly", keyspaceName, shard1.Name))
	//
	//// check the Map Reduce API works correctly, should use ExecuteShards,
	//// as we're not sharded yet.
	//// we have 3 values in the database, asking for 4 splits will get us
	//// a single query.
	//sql := `select id, msg from resharding1`
	//output, _ = ClusterInstance.VtctlProcess.ExecuteCommandWithOutput("VtGateSplitQuery",
	//	"-server", fmt.Sprintf("%s:%d", ClusterInstance.Hostname, ClusterInstance.VtgateProcess.GrpcPort),
	//	"-keyspace", keyspaceName,
	//	"-split_count", "4",
	//	sql)
	//
	//var splitqueryresponse []vtgatepb.SplitQueryResponse_Part
	//err = json.Unmarshal([]byte(output), &splitqueryresponse)
	//assert.Nil(t, err)
	//assert.Equal(t, len(splitqueryresponse), 1)
	//assert.Equal(t, splitqueryresponse[0].ShardPart.Shards[0], "0")
	//
	//// change the schema, backfill keyspace_id, and change schema again
	//if shardingKeyType == topodata.KeyspaceIdType_BYTES {
	//	sql = fmt.Sprintf("alter table %s add custom_ksid_col varbinary(64)", tableName)
	//} else {
	//	sql = fmt.Sprintf("alter table %s add custom_ksid_col bigint(20) unsigned", tableName)
	//}
	//
	//_ = ClusterInstance.VtctlclientProcess.ApplySchema(keyspaceName, sql)
	//
	//sql = "update resharding1 set custom_ksid_col=0x1000000000000000 where id=1;"
	//sql = sql + "update resharding1 set custom_ksid_col=0x9000000000000000 where id=2;"
	//sql = sql + "update resharding1 set custom_ksid_col=0xD000000000000000 where id=3;"
	//_, _ = shard1MasterTablet.VttabletProcess.QueryTablet(sql, keyspaceName, true)
	//
	//if shardingKeyType == topodata.KeyspaceIdType_BYTES {
	//	sql = fmt.Sprintf("alter table %s modify custom_ksid_col varbinary(64) not null", tableName)
	//} else {
	//	sql = fmt.Sprintf("alter table %s modify custom_ksid_col bigint(20) unsigned not null", tableName)
	//}
	//_ = ClusterInstance.VtctlclientProcess.ApplySchema(keyspaceName, sql)
	//
	//// now we can be a sharded keyspace (and propagate to SrvKeyspace)
	//if version == 3 {
	//	_ = ClusterInstance.VtctlclientProcess.ApplyVSchema(keyspaceName, fmt.Sprintf(vSchema, tableName, "custom_ksid_col"))
	//} else if version == 2 && shardingKeyType == topodata.KeyspaceIdType_UINT64 {
	//	_ = ClusterInstance.VtctlclientProcess.ExecuteCommand("SetKeyspaceShardingInfo", keyspaceName, "custom_ksid_col", "uint64")
	//} else if version == 2 && shardingKeyType == topodata.KeyspaceIdType_BYTES {
	//	_ = ClusterInstance.VtctlclientProcess.ExecuteCommand("SetKeyspaceShardingInfo", keyspaceName, "custom_ksid_col", "bytes")
	//}
	//
	//_ = ClusterInstance.VtctlclientProcess.ExecuteCommand("RebuildKeyspaceGraph", keyspaceName)
	//
	//// run a health check on source replica so it responds to discovery
	//err = ClusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", shard1.Replica().Alias)
	//assert.Nil(t, err)
	//
	//// create the split shards
	//shard21 := ClusterInstance.Keyspaces[0].Shards[1]
	//shard22 := ClusterInstance.Keyspaces[0].Shards[2]
	//
	//for _, shard := range []cluster.Shard{shard21, shard22} {
	//	for _, vttablet := range shard.Vttablets {
	//		vttablet.VttabletProcess.ExtraArgs = commonTabletArg
	//		_ = ClusterInstance.VtctlclientProcess.InitTablet(&vttablet, cell, keyspaceName, hostname, shard.Name)
	//		_ = vttablet.VttabletProcess.CreateDB(keyspaceName)
	//		err = vttablet.VttabletProcess.Setup()
	//		assert.Nil(t, err)
	//	}
	//}
	//
	//_ = ClusterInstance.VtctlclientProcess.InitShardMaster(keyspaceName, shard21.Name, cell, shard21.MasterTablet().TabletUID)
	//_ = ClusterInstance.VtctlclientProcess.InitShardMaster(keyspaceName, shard22.Name, cell, shard22.MasterTablet().TabletUID)
	//
	//for _, shard := range []cluster.Shard{shard21, shard22} {
	//	_ = shard.Replica().VttabletProcess.WaitForTabletType("SERVING")
	//	_ = shard.Rdonly().VttabletProcess.WaitForTabletType("SERVING")
	//}
	//
	//for _, shard := range []cluster.Shard{shard21, shard22} {
	//	for _, vttablet := range shard.Vttablets {
	//		assert.Equal(t, vttablet.VttabletProcess.GetTabletStatus(), "SERVING")
	//	}
	//}
	//
	//// must restart vtgate after tablets are up, or else wait until 1min refresh
	//// we want cache_ttl at zero so we re-read the topology for every test query.
	//
	//_ = ClusterInstance.VtgateProcess.TearDown()
	//_ = ClusterInstance.VtgateProcess.Setup()
	//
	//// Wait for the endpoints, either local or remote.
	//for _, shard := range []cluster.Shard{shard1, shard21, shard22} {
	//	_ = ClusterInstance.VtgateProcess.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", keyspaceName, shard.Name))
	//	_ = ClusterInstance.VtgateProcess.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.replica", keyspaceName, shard.Name))
	//	_ = ClusterInstance.VtgateProcess.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.rdonly", keyspaceName, shard.Name))
	//}
	//
	//status := ClusterInstance.VtgateProcess.GetStatusForTabletOfShard(keyspaceName + ".80-.master")
	//assert.True(t, status)
	//
	//// check the Map Reduce API works correctly, should use ExecuteKeyRanges now,
	//// as we are sharded (with just one shard).
	//// again, we have 3 values in the database, asking for 4 splits will get us
	//// a single query.
	//
	//sql = `select id, msg from resharding1`
	//output, _ = ClusterInstance.VtctlProcess.ExecuteCommandWithOutput("VtGateSplitQuery",
	//	"-server", fmt.Sprintf("%s:%d", ClusterInstance.Hostname, ClusterInstance.VtgateProcess.GrpcPort),
	//	"-keyspace", keyspaceName,
	//	"-split_count", "4",
	//	sql)
	//splitqueryresponse = nil
	//err = json.Unmarshal([]byte(output), &splitqueryresponse)
	//assert.Nil(t, err)
	//assert.Equal(t, len(splitqueryresponse), 1)
	//assert.Equal(t, splitqueryresponse[0].KeyRangePart.Keyspace, keyspaceName)
	//assert.Equal(t, len(splitqueryresponse[0].KeyRangePart.KeyRanges), 1)
	//assert.Empty(t, splitqueryresponse[0].KeyRangePart.KeyRanges[0])
	//
	//// Check srv keyspace
	//expectedPartitions := map[topodata.TabletType][]string{}
	//expectedPartitions[topodata.TabletType_MASTER] = []string{shard1.Name}
	//expectedPartitions[topodata.TabletType_REPLICA] = []string{shard1.Name}
	//expectedPartitions[topodata.TabletType_RDONLY] = []string{shard1.Name}
	//checkSrvKeyspaceForSharding(t, expectedPartitions, version, shardingKeyType)
	//
	//_ = ClusterInstance.VtctlclientProcess.ExecuteCommand("CopySchemaShard",
	//	"--exclude_tables", "unrelated",
	//	shard1.Rdonly().Alias, fmt.Sprintf("%s/%s", keyspaceName, shard21.Name))
	//
	//_ = ClusterInstance.VtctlclientProcess.ExecuteCommand("CopySchemaShard",
	//	"--exclude_tables", "unrelated",
	//	shard1.Rdonly().Alias, fmt.Sprintf("%s/%s", keyspaceName, shard22.Name))
	//
	//if version == 3 {
	//	_ = ClusterInstance.StartVtworker(cell, "--use_v3_resharding_mode=true")
	//} else if version == 2 {
	//	_ = ClusterInstance.StartVtworker(cell, "--use_v3_resharding_mode=false")
	//}
	//
	//// Initial clone (online).
	//_ = ClusterInstance.VtworkerProcess.ExecuteCommand("SplitClone",
	//	"--offline=false",
	//	"--exclude_tables", "unrelated",
	//	"--chunk_count", "10",
	//	"--min_rows_per_chunk", "1",
	//	"--min_healthy_rdonly_tablets", "1",
	//	fmt.Sprintf("%s/%s", keyspaceName, shard1.Name))
	//
	//vtworkerURL := fmt.Sprintf("http://localhost:%d/debug/vars", ClusterInstance.VtworkerProcess.Port)
	//sharding.VerifyReconciliationCounters(t, vtworkerURL, "Online", tableName, 3, 0, 0, 0)
	//
	//// Reset vtworker such that we can run the next command.
	//_ = ClusterInstance.VtworkerProcess.ExecuteCommand("Reset")
	//
	//// Modify the destination shard. SplitClone will revert the changes.
	//// Delete row 1 (provokes an insert).
	//_, _ = shard21.MasterTablet().VttabletProcess.QueryTablet(fmt.Sprintf("delete from %s where id=1", tableName), keyspaceName, true)
	//// Delete row 2 (provokes an insert).
	//_, _ = shard22.MasterTablet().VttabletProcess.QueryTablet(fmt.Sprintf("delete from %s where id=2", tableName), keyspaceName, true)
	////  Update row 3 (provokes an update).
	//_, _ = shard22.MasterTablet().VttabletProcess.QueryTablet(fmt.Sprintf("update %s set msg='msg-not-3' where id=3", tableName), keyspaceName, true)
	//// Insert row 4 (provokes a delete).
	//var ksid uint64 = 0xD000000000000000
	//insertSQL := fmt.Sprintf(sharding.InsertTabletTemplateKsID, tableName, fixedParentID, 4, "msg4", ksid, ksid, 4)
	//sharding.InsertToTablet(insertSQL, *shard22.MasterTablet(), keyspaceName)
	//
	//_ = ClusterInstance.VtworkerProcess.ExecuteCommand("SplitClone",
	//	"--exclude_tables", "unrelated",
	//	"--chunk_count", "10",
	//	"--min_rows_per_chunk", "1",
	//	"--min_healthy_rdonly_tablets", "1",
	//	fmt.Sprintf("%s/%s", keyspaceName, shard1.Name))
	//
	//sharding.VerifyReconciliationCounters(t, vtworkerURL, "Online", tableName, 2, 1, 1, 0)
	//sharding.VerifyReconciliationCounters(t, vtworkerURL, "Offline", tableName, 0, 0, 0, 3)
	//
	//// check first value is in the left shard
	//for _, tablet := range shard21.Vttablets {
	//	sharding.CheckValues(t, tablet, []string{"INT64(86)", "INT64(1)", `VARCHAR("msg1")`, sharding.HexToDbStr(0x1000000000000000, shardingKeyType)},
	//		1, true, tableName, fixedParentID, keyspaceName, shardingKeyType)
	//}
	//
	//for _, tablet := range shard22.Vttablets {
	//	sharding.CheckValues(t, tablet, []string{"INT64(86)", "INT64(1)", `VARCHAR("msg1")`, sharding.HexToDbStr(0x1000000000000000, shardingKeyType)},
	//		1, false, tableName, fixedParentID, keyspaceName, shardingKeyType)
	//}
	//
	//for _, tablet := range shard21.Vttablets {
	//	sharding.CheckValues(t, tablet, []string{"INT64(86)", "INT64(2)", `VARCHAR("msg2")`, sharding.HexToDbStr(0x9000000000000000, shardingKeyType)},
	//		2, false, tableName, fixedParentID, keyspaceName, shardingKeyType)
	//}
	//
	//for _, tablet := range shard22.Vttablets {
	//	sharding.CheckValues(t, tablet, []string{"INT64(86)", "INT64(2)", `VARCHAR("msg2")`, sharding.HexToDbStr(0x9000000000000000, shardingKeyType)},
	//		2, true, tableName, fixedParentID, keyspaceName, shardingKeyType)
	//}
	//
	//for _, tablet := range shard21.Vttablets {
	//	sharding.CheckValues(t, tablet, []string{"INT64(86)", "INT64(3)", `VARCHAR("msg3")`, sharding.HexToDbStr(0xD000000000000000, shardingKeyType)},
	//		3, false, tableName, fixedParentID, keyspaceName, shardingKeyType)
	//}
	//
	//for _, tablet := range shard22.Vttablets {
	//	sharding.CheckValues(t, tablet, []string{"INT64(86)", "INT64(3)", `VARCHAR("msg3")`, sharding.HexToDbStr(0xD000000000000000, shardingKeyType)},
	//		3, true, tableName, fixedParentID, keyspaceName, shardingKeyType)
	//}
	//
	//err = ClusterInstance.VtctlclientProcess.ExecuteCommand("ValidateSchemaKeyspace", keyspaceName)
	//assert.Nil(t, err)
	//
	//// check the binlog players are running
	//sharding.CheckDestinationMaster(t, *shard21.MasterTablet(), []string{shard1Ks}, *ClusterInstance)
	//sharding.CheckDestinationMaster(t, *shard22.MasterTablet(), []string{shard1Ks}, *ClusterInstance)
	//
	////  check that binlog server exported the stats vars
	//sharding.CheckBinlogServerVars(t, *shard1.Replica(), 0, 0)
	//
	//for _, tablet := range []cluster.Vttablet{*shard21.Rdonly(), *shard22.Rdonly()} {
	//	err = ClusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", tablet.Alias)
	//	assert.Nil(t, err)
	//}
	//
	//// testing filtered replication: insert a bunch of data on shard 1,
	//// check we get most of it after a few seconds, wait for binlog server
	//// timeout, check we get all of it.
	//sharding.InsertLots(1000, shard1MasterTablet, tableName, fixedParentID, keyspaceName)
	//
	//assert.True(t, sharding.CheckLotsTimeout(t, *shard22.Replica(), 1000, tableName, fixedParentID, keyspaceName, shardingKeyType))
	//sharding.CheckLotsNotPresent(t, *shard21.Replica(), 1000, tableName, fixedParentID, keyspaceName, shardingKeyType)
	//
	//sharding.CheckDestinationMaster(t, *shard21.MasterTablet(), []string{shard1Ks}, *ClusterInstance)
	//sharding.CheckDestinationMaster(t, *shard22.MasterTablet(), []string{shard1Ks}, *ClusterInstance)
	//sharding.CheckBinlogServerVars(t, *shard1.Replica(), 1000, 1000)
	//
	//err = ClusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", shard21.Rdonly().Alias)
	//assert.Nil(t, err)
	//err = ClusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", shard22.Rdonly().Alias)
	//assert.Nil(t, err)
	//
	////use vtworker to compare the data
	//ClusterInstance.VtworkerProcess.Cell = cell
	//if version == 2 {
	//	err = ClusterInstance.VtworkerProcess.ExecuteVtworkerCommand(ClusterInstance.GetAndReservePort(),
	//		ClusterInstance.GetAndReservePort(),
	//		"--use_v3_resharding_mode=false",
	//		"SplitDiff",
	//		"--min_healthy_rdonly_tablets", "1",
	//		fmt.Sprintf("%s/%s", keyspaceName, shard21.Name))
	//	assert.Nil(t, err)
	//
	//	err = ClusterInstance.VtworkerProcess.ExecuteVtworkerCommand(ClusterInstance.GetAndReservePort(),
	//		ClusterInstance.GetAndReservePort(),
	//		"--use_v3_resharding_mode=false",
	//		"SplitDiff",
	//		"--min_healthy_rdonly_tablets", "1",
	//		fmt.Sprintf("%s/%s", keyspaceName, shard22.Name))
	//	assert.Nil(t, err)
	//}
	//
	//// get status for the destination master tablet, make sure we have it all
	//sharding.CheckRunningBinlogPlayer(t, *shard21.MasterTablet(), 2000, 2000)
	//sharding.CheckRunningBinlogPlayer(t, *shard22.MasterTablet(), 6000, 2000)
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
	//ClusterInstance.Keyspaces[0].Shards = []cluster.Shard{shard21, shard22}
}

//func checkSrvKeyspaceForSharding(t *testing.T, expectedPartitions map[topodata.TabletType][]string, version int, keyspaceIDType topodata.KeyspaceIdType) {
//	if version == 3 {
//		sharding.CheckSrvKeyspace(t, cell, keyspaceName, "", 0, expectedPartitions, *ClusterInstance)
//	} else {
//		sharding.CheckSrvKeyspace(t, cell, keyspaceName, "custom_ksid_col", keyspaceIDType, expectedPartitions, *ClusterInstance)
//	}
//
//}
