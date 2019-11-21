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

This test simulates the first time a database has to be split.

- we start with a keyspace with a single shard and a single table
- we add and populate the sharding key
- we set the sharding key in the topology
- we clone into 2 instances
- we enable filtered replication
- we move all serving types
- we remove the source tablets
- we remove the original shard

*/

package initialsharding

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	hostname        = "localhost"
	keyspaceName    = "ks"
	cell            = "zone1"
	commonTabletArg = []string{"-binlog_use_v3_resharding_mode=false",
		"-vreplication_healthcheck_topology_refresh", "1s",
		"-vreplication_healthcheck_retry_delay", "1s",
		"-vreplication_retry_delay", "1s",
		"-degraded_threshold", "5s",
		"-lock_tables_timeout", "5s",
		"-watch_replication_stream",
		"-enable_semi_sync"}
	createTabletTemplate = `
							create table %s(
							msg varchar(64),
							id bigint not null,
							parent_id bigint not null,
							primary key (parent_id, id),
							index by_msg (msg)
							) Engine=InnoDB;
`
	insertTabletTemplate = `insert into %s(parent_id, id, msg) values(%d, %d, "%s")`
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

func TestMain(m *testing.M) {
	flag.Parse()

	exitcode, err := func() (int, error) {
		clusterInstance = &cluster.LocalProcessCluster{Cell: cell, Hostname: hostname}
		defer clusterInstance.Teardown()

		// Start topo server
		if err := clusterInstance.StartTopo(); err != nil {
			return 1, err
		}
		if err := clusterInstance.VtctlProcess.CreateKeyspace(keyspaceName); err != nil {
			return 1, err
		}

		initCluster([]string{"0"}, 3, true)
		initCluster([]string{"-80", "80-"}, 3, true)

		return m.Run(), nil
	}()
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	} else {
		os.Exit(exitcode)
	}
}

func initCluster(shardNames []string, totalTabletsRequired int, rdonly bool) {
	var keyspace cluster.Keyspace
	var ksExists bool
	if clusterInstance.Keyspaces == nil {
		keyspace = cluster.Keyspace{
			Name: keyspaceName,
		}
	} else {
		// this is single keyspace test, so picking up 0th index
		ksExists = true
		keyspace = clusterInstance.Keyspaces[0]
	}

	for _, shardName := range shardNames {
		shard := &cluster.Shard{
			Name: shardName,
		}

		for i := 0; i < totalTabletsRequired; i++ {
			// instantiate vttablet object with reserved ports
			tabletUID := clusterInstance.GetAndReserveTabletUID()
			tablet := &cluster.Vttablet{
				TabletUID: tabletUID,
				HTTPPort:  clusterInstance.GetAndReservePort(),
				GrpcPort:  clusterInstance.GetAndReservePort(),
				MySQLPort: clusterInstance.GetAndReservePort(),
				Alias:     fmt.Sprintf("%s-%010d", clusterInstance.Cell, tabletUID),
			}
			if i == 0 { // Make the first one as master
				tablet.Type = "master"
			} else if i == totalTabletsRequired-1 && rdonly { // Make the last one as rdonly if rdonly flag is passed
				tablet.Type = "rdonly"
			}
			// Start Mysqlctl process
			tablet.MysqlctlProcess = *cluster.MysqlCtlProcessInstance(tablet.TabletUID, tablet.MySQLPort, clusterInstance.TmpDirectory)
			if err := tablet.MysqlctlProcess.Start(); err != nil {
				return
			}

			// start vttablet process
			tablet.VttabletProcess = *cluster.VttabletProcessInstance(tablet.HTTPPort,
				tablet.GrpcPort,
				tablet.TabletUID,
				clusterInstance.Cell,
				shardName,
				keyspaceName,
				clusterInstance.VtctldProcess.Port,
				tablet.Type,
				clusterInstance.TopoProcess.Port,
				clusterInstance.Hostname,
				clusterInstance.TmpDirectory,
				clusterInstance.VtTabletExtraArgs,
				clusterInstance.EnableSemiSync)
			tablet.Alias = tablet.VttabletProcess.TabletPath

			shard.Vttablets = append(shard.Vttablets, *tablet)
		}
		if ksExists {
			clusterInstance.Keyspaces[0].Shards = append(clusterInstance.Keyspaces[0].Shards, *shard)
		} else {
			keyspace.Shards = append(keyspace.Shards, *shard)
		}
	}
	if len(clusterInstance.Keyspaces) == 0 {
		clusterInstance.Keyspaces = append(clusterInstance.Keyspaces, keyspace)
	}
}

func TestResharding(t *testing.T) {
	assert.Equal(t, len(clusterInstance.Keyspaces[0].Shards), 3)
	// Start the master and rdonly of 1st shard
	shard1 := clusterInstance.Keyspaces[0].Shards[0]
	shard1Ks := fmt.Sprintf("%s/%s", keyspaceName, shard1.Name)
	shard1MasterTablet := *shard1.MasterTablet()

	// master tablet start
	shard1MasterTablet.VttabletProcess.ExtraArgs = commonTabletArg
	_ = clusterInstance.VtctlclientProcess.InitTablet(&shard1MasterTablet, cell, keyspaceName, hostname, shard1.Name)
	_ = shard1MasterTablet.VttabletProcess.CreateDB(keyspaceName)
	err := shard1MasterTablet.VttabletProcess.Setup()
	assert.Nil(t, err)

	// replica tablet init
	shard1.Replica().VttabletProcess.ExtraArgs = commonTabletArg
	_ = clusterInstance.VtctlclientProcess.InitTablet(shard1.Replica(), cell, keyspaceName, hostname, shard1.Name)
	_ = shard1.Replica().VttabletProcess.CreateDB(keyspaceName)

	// rdonly tablet start
	shard1.Rdonly().VttabletProcess.ExtraArgs = commonTabletArg
	_ = clusterInstance.VtctlclientProcess.InitTablet(shard1.Rdonly(), cell, keyspaceName, hostname, shard1.Name)
	_ = shard1.Rdonly().VttabletProcess.CreateDB(keyspaceName)
	err = shard1.Rdonly().VttabletProcess.Setup()
	assert.Nil(t, err)

	output, _ := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("InitShardMaster",
		"-force", fmt.Sprintf("%s/%s", keyspaceName, shard1.Name), shard1MasterTablet.Alias)
	assert.Contains(t, output, fmt.Sprintf("tablet %s ResetReplication failed", shard1.Replica().Alias))

	// start replica
	err = shard1.Replica().VttabletProcess.Setup()
	assert.Nil(t, err)

	// reparent to make the tablets work
	err = clusterInstance.VtctlclientProcess.InitShardMaster(keyspaceName, shard1.Name, cell, shard1MasterTablet.TabletUID)
	assert.Nil(t, err)

	_ = shard1.Replica().VttabletProcess.WaitForTabletType("SERVING")
	_ = shard1.Rdonly().VttabletProcess.WaitForTabletType("SERVING")
	for _, vttablet := range shard1.Vttablets {
		assert.Equal(t, vttablet.VttabletProcess.GetTabletStatus(), "SERVING")
	}
	// create the tables and add startup values
	_ = clusterInstance.VtctlclientProcess.ApplySchema(keyspaceName, fmt.Sprintf(createTabletTemplate, tableName))
	_, _ = shard1MasterTablet.VttabletProcess.QueryTablet(fmt.Sprintf(insertTabletTemplate, "resharding1", fixedParentID, 1, "msg1"), keyspaceName, true)
	_, _ = shard1MasterTablet.VttabletProcess.QueryTablet(fmt.Sprintf(insertTabletTemplate, "resharding1", fixedParentID, 2, "msg2"), keyspaceName, true)
	_, _ = shard1MasterTablet.VttabletProcess.QueryTablet(fmt.Sprintf(insertTabletTemplate, "resharding1", fixedParentID, 3, "msg3"), keyspaceName, true)

	// reload schema on all tablets so we can query them
	for _, vttablet := range shard1.Vttablets {
		_ = clusterInstance.VtctlclientProcess.ExecuteCommand("ReloadSchema", vttablet.Alias)
	}
	err = clusterInstance.StartVtgate()
	assert.Nil(t, err)

	_ = clusterInstance.VtgateProcess.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", keyspaceName, shard1.Name))
	_ = clusterInstance.VtgateProcess.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.replica", keyspaceName, shard1.Name))
	_ = clusterInstance.VtgateProcess.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.rdonly", keyspaceName, shard1.Name))

	// check the Map Reduce API works correctly, should use ExecuteShards,
	// as we're not sharded yet.
	// we have 3 values in the database, asking for 4 splits will get us
	// a single query.
	sql := `select id, msg from resharding1`
	output, _ = clusterInstance.VtctlProcess.ExecuteCommandWithOutput("VtGateSplitQuery",
		"-server", fmt.Sprintf("%s:%d", clusterInstance.Hostname, clusterInstance.VtgateProcess.GrpcPort),
		"-keyspace", keyspaceName,
		"-split_count", "4",
		sql)

	var splitqueryresponse []vtgatepb.SplitQueryResponse_Part
	err = json.Unmarshal([]byte(output), &splitqueryresponse)
	assert.Nil(t, err)
	assert.Equal(t, len(splitqueryresponse), 1)
	assert.Equal(t, splitqueryresponse[0].ShardPart.Shards[0], "0")

	// change the schema, backfill keyspace_id, and change schema again
	sql = fmt.Sprintf("alter table %s add custom_ksid_col bigint(20) unsigned", tableName)
	_ = clusterInstance.VtctlclientProcess.ApplySchema(keyspaceName, sql)

	sql = "update resharding1 set custom_ksid_col=0x1000000000000000 where id=1;"
	sql = sql + "update resharding1 set custom_ksid_col=0x9000000000000000 where id=2;"
	sql = sql + "update resharding1 set custom_ksid_col=0xD000000000000000 where id=3;"
	_, _ = shard1MasterTablet.VttabletProcess.QueryTablet(sql, keyspaceName, true)

	sql = fmt.Sprintf("alter table %s modify custom_ksid_col bigint(20) unsigned not null", tableName)
	_ = clusterInstance.VtctlclientProcess.ApplySchema(keyspaceName, sql)

	// now we can be a sharded keyspace (and propagate to SrvKeyspace)
	// TODO: migration this to v3 way
	//_ = clusterInstance.VtctlclientProcess.ApplyVSchema(keyspaceName, fmt.Sprintf(vSchema, tableName, "custom_ksid_col"))
	_ = clusterInstance.VtctlclientProcess.ExecuteCommand("SetKeyspaceShardingInfo", keyspaceName, "custom_ksid_col", "uint64")
	_ = clusterInstance.VtctlclientProcess.ExecuteCommand("RebuildKeyspaceGraph", keyspaceName)

	// run a health check on source replica so it responds to discovery
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", shard1.Replica().Alias)
	assert.Nil(t, err)

	// create the split shards
	shard21 := clusterInstance.Keyspaces[0].Shards[1]
	shard22 := clusterInstance.Keyspaces[0].Shards[2]

	for _, shard := range []cluster.Shard{shard21, shard22} {
		for _, vttablet := range shard.Vttablets {
			vttablet.VttabletProcess.ExtraArgs = commonTabletArg
			_ = clusterInstance.VtctlclientProcess.InitTablet(&vttablet, cell, keyspaceName, hostname, shard.Name)
			_ = vttablet.VttabletProcess.CreateDB(keyspaceName)
			err = vttablet.VttabletProcess.Setup()
			assert.Nil(t, err)
		}
	}

	_ = clusterInstance.VtctlclientProcess.InitShardMaster(keyspaceName, shard21.Name, cell, shard21.MasterTablet().TabletUID)
	_ = clusterInstance.VtctlclientProcess.InitShardMaster(keyspaceName, shard22.Name, cell, shard22.MasterTablet().TabletUID)

	for _, shard := range []cluster.Shard{shard21, shard22} {
		_ = shard.Replica().VttabletProcess.WaitForTabletType("SERVING")
		_ = shard.Rdonly().VttabletProcess.WaitForTabletType("SERVING")
	}

	for _, shard := range []cluster.Shard{shard21, shard22} {
		for _, vttablet := range shard.Vttablets {
			assert.Equal(t, vttablet.VttabletProcess.GetTabletStatus(), "SERVING")
		}
	}

	// must restart vtgate after tablets are up, or else wait until 1min refresh
	// we want cache_ttl at zero so we re-read the topology for every test query.

	_ = clusterInstance.ReStartVtgate()

	// Wait for the endpoints, either local or remote.
	for _, shard := range []cluster.Shard{shard1, shard21, shard22} {
		_ = clusterInstance.VtgateProcess.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", keyspaceName, shard.Name))
		_ = clusterInstance.VtgateProcess.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.replica", keyspaceName, shard.Name))
		_ = clusterInstance.VtgateProcess.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.rdonly", keyspaceName, shard.Name))
	}

	status := clusterInstance.VtgateProcess.GetStatusForTabletOfShard(keyspaceName + ".80-.master")
	assert.True(t, status)

	// check the Map Reduce API works correctly, should use ExecuteKeyRanges now,
	// as we are sharded (with just one shard).
	// again, we have 3 values in the database, asking for 4 splits will get us
	// a single query.

	sql = `select id, msg from resharding1`
	output, _ = clusterInstance.VtctlProcess.ExecuteCommandWithOutput("VtGateSplitQuery",
		"-server", fmt.Sprintf("%s:%d", clusterInstance.Hostname, clusterInstance.VtgateProcess.GrpcPort),
		"-keyspace", keyspaceName,
		"-split_count", "4",
		sql)
	splitqueryresponse = nil
	err = json.Unmarshal([]byte(output), &splitqueryresponse)
	assert.Nil(t, err)
	assert.Equal(t, len(splitqueryresponse), 1)
	assert.Equal(t, splitqueryresponse[0].KeyRangePart.Keyspace, keyspaceName)
	assert.Equal(t, len(splitqueryresponse[0].KeyRangePart.KeyRanges), 1)
	assert.Empty(t, splitqueryresponse[0].KeyRangePart.KeyRanges[0])

	// Check srv keyspace
	expectedPartitions := map[topodata.TabletType][]string{}
	expectedPartitions[topodata.TabletType_MASTER] = []string{shard1.Name}
	expectedPartitions[topodata.TabletType_REPLICA] = []string{shard1.Name}
	expectedPartitions[topodata.TabletType_RDONLY] = []string{shard1.Name}
	checkSrvKeyspace(t, cell, keyspaceName, "custom_ksid_col", topodata.KeyspaceIdType_UINT64, expectedPartitions, *clusterInstance)

	_ = clusterInstance.VtctlclientProcess.ExecuteCommand("CopySchemaShard",
		"--exclude_tables", "unrelated",
		shard1.Rdonly().Alias, fmt.Sprintf("%s/%s", keyspaceName, shard21.Name))

	_ = clusterInstance.VtctlclientProcess.ExecuteCommand("CopySchemaShard",
		"--exclude_tables", "unrelated",
		shard1.Rdonly().Alias, fmt.Sprintf("%s/%s", keyspaceName, shard22.Name))

	_ = clusterInstance.StartVtworker(cell)
	// Initial clone (online).
	_ = clusterInstance.VtworkerProcess.ExecuteCommand("SplitClone",
		"--offline=false",
		"--exclude_tables", "unrelated",
		"--chunk_count", "10",
		"--min_rows_per_chunk", "1",
		"--min_healthy_rdonly_tablets", "1",
		fmt.Sprintf("%s/%s", keyspaceName, shard1.Name))

	vtworkerURL := fmt.Sprintf("http://localhost:%d/debug/vars", clusterInstance.VtworkerProcess.Port)
	verifyReconciliationCounters(t, vtworkerURL, "Online", tableName, 3, 0, 0, 0)

	// Reset vtworker such that we can run the next command.
	_ = clusterInstance.VtworkerProcess.ExecuteCommand("Reset")

	// Modify the destination shard. SplitClone will revert the changes.
	// Delete row 1 (provokes an insert).
	_, _ = shard21.MasterTablet().VttabletProcess.QueryTablet(fmt.Sprintf("delete from %s where id=1", tableName), keyspaceName, true)
	// Delete row 2 (provokes an insert).
	_, _ = shard22.MasterTablet().VttabletProcess.QueryTablet(fmt.Sprintf("delete from %s where id=2", tableName), keyspaceName, true)
	//  Update row 3 (provokes an update).
	_, _ = shard22.MasterTablet().VttabletProcess.QueryTablet(fmt.Sprintf("update %s set msg='msg-not-3' where id=3", tableName), keyspaceName, true)
	// Insert row 4 (provokes a delete).
	var ksid uint64 = 0xD000000000000000
	insertSQL := fmt.Sprintf(insertTabletTemplateKsID, tableName, fixedParentID, 4, "msg4", ksid, ksid, 4)
	insertToTablet(insertSQL, *shard22.MasterTablet(), keyspaceName)

	_ = clusterInstance.VtworkerProcess.ExecuteCommand("SplitClone",
		"--exclude_tables", "unrelated",
		"--chunk_count", "10",
		"--min_rows_per_chunk", "1",
		"--min_healthy_rdonly_tablets", "1",
		fmt.Sprintf("%s/%s", keyspaceName, shard1.Name))

	verifyReconciliationCounters(t, vtworkerURL, "Online", tableName, 2, 1, 1, 0)
	verifyReconciliationCounters(t, vtworkerURL, "Offline", tableName, 0, 0, 0, 3)

	// check first value is in the left shard
	for _, tablet := range shard21.Vttablets {
		checkValues(t, tablet, []string{"INT64(86)", "INT64(1)", `VARCHAR("msg1")`, hexToDbStr(0x1000000000000000)}, 1, true, tableName, fixedParentID, keyspaceName)
	}

	for _, tablet := range shard22.Vttablets {
		checkValues(t, tablet, []string{"INT64(86)", "INT64(1)", `VARCHAR("msg1")`, hexToDbStr(0x1000000000000000)}, 1, false, tableName, fixedParentID, keyspaceName)
	}

	for _, tablet := range shard21.Vttablets {
		checkValues(t, tablet, []string{"INT64(86)", "INT64(2)", `VARCHAR("msg2")`, hexToDbStr(0x9000000000000000)}, 2, false, tableName, fixedParentID, keyspaceName)
	}

	for _, tablet := range shard22.Vttablets {
		checkValues(t, tablet, []string{"INT64(86)", "INT64(2)", `VARCHAR("msg2")`, hexToDbStr(0x9000000000000000)}, 2, true, tableName, fixedParentID, keyspaceName)
	}

	for _, tablet := range shard21.Vttablets {
		checkValues(t, tablet, []string{"INT64(86)", "INT64(3)", `VARCHAR("msg3")`, hexToDbStr(0xD000000000000000)}, 3, false, tableName, fixedParentID, keyspaceName)
	}

	for _, tablet := range shard22.Vttablets {
		checkValues(t, tablet, []string{"INT64(86)", "INT64(3)", `VARCHAR("msg3")`, hexToDbStr(0xD000000000000000)}, 3, true, tableName, fixedParentID, keyspaceName)
	}

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ValidateSchemaKeyspace", keyspaceName)
	assert.Nil(t, err)

	// check the binlog players are running
	checkDestinationMaster(t, *shard21.MasterTablet(), []string{shard1Ks}, *clusterInstance)
	checkDestinationMaster(t, *shard22.MasterTablet(), []string{shard1Ks}, *clusterInstance)

	//  check that binlog server exported the stats vars
	checkBinlogServerVars(t, *shard1.Replica(), 0, 0)

	for _, tablet := range []cluster.Vttablet{*shard21.Rdonly(), *shard22.Rdonly()} {
		err = clusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", tablet.Alias)
		assert.Nil(t, err)
	}

	// testing filtered replication: insert a bunch of data on shard 1,
	// check we get most of it after a few seconds, wait for binlog server
	// timeout, check we get all of it.
	insertLots(1000, shard1MasterTablet, tableName, fixedParentID, keyspaceName)

	assert.True(t, checkLotsTimeout(t, *shard22.Replica(), 1000, tableName, fixedParentID, keyspaceName))
	checkLotsNotPresent(t, *shard21.Replica(), 1000, tableName, fixedParentID, keyspaceName)

	checkDestinationMaster(t, *shard21.MasterTablet(), []string{shard1Ks}, *clusterInstance)
	checkDestinationMaster(t, *shard22.MasterTablet(), []string{shard1Ks}, *clusterInstance)
	checkBinlogServerVars(t, *shard1.Replica(), 1000, 1000)

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", shard21.Rdonly().Alias)
	assert.Nil(t, err)
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", shard22.Rdonly().Alias)
	assert.Nil(t, err)

	//use vtworker to compare the data
	clusterInstance.VtworkerProcess.Cell = cell
	err = clusterInstance.VtworkerProcess.ExecuteVtworkerCommand(clusterInstance.GetAndReservePort(),
		clusterInstance.GetAndReservePort(), "SplitDiff",
		"--min_healthy_rdonly_tablets", "1",
		fmt.Sprintf("%s/%s", keyspaceName, shard21.Name))
	assert.Nil(t, err)

	err = clusterInstance.VtworkerProcess.ExecuteVtworkerCommand(clusterInstance.GetAndReservePort(),
		clusterInstance.GetAndReservePort(), "SplitDiff",
		"--min_healthy_rdonly_tablets", "1",
		fmt.Sprintf("%s/%s", keyspaceName, shard22.Name))
	assert.Nil(t, err)

	// get status for the destination master tablet, make sure we have it all
	checkRunningBinlogPlayer(t, *shard21.MasterTablet(), 2000, 2000)
	checkRunningBinlogPlayer(t, *shard22.MasterTablet(), 6000, 2000)

	// check we can't migrate the master just yet
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("MigrateServedTypes", shard1Ks, "master")
	assert.NotNil(t, err)

	// now serve rdonly from the split shards
	_ = clusterInstance.VtctlclientProcess.ExecuteCommand("MigrateServedTypes", shard1Ks, "rdonly")
	expectedPartitions = map[topodata.TabletType][]string{}
	expectedPartitions[topodata.TabletType_MASTER] = []string{shard1.Name}
	expectedPartitions[topodata.TabletType_REPLICA] = []string{shard1.Name}
	expectedPartitions[topodata.TabletType_RDONLY] = []string{shard21.Name, shard22.Name}
	checkSrvKeyspace(t, cell, keyspaceName, "custom_ksid_col", topodata.KeyspaceIdType_UINT64, expectedPartitions, *clusterInstance)

	_ = shard21.Rdonly().VttabletProcess.WaitForTabletType("SERVING")
	_ = shard22.Rdonly().VttabletProcess.WaitForTabletType("SERVING")

	_ = clusterInstance.VtgateProcess.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.rdonly", keyspaceName, shard21.Name))
	_ = clusterInstance.VtgateProcess.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.rdonly", keyspaceName, shard22.Name))

	sql = fmt.Sprintf("select id, msg from %s", tableName)
	output, _ = clusterInstance.VtctlProcess.ExecuteCommandWithOutput("VtGateSplitQuery",
		"-server", fmt.Sprintf("%s:%d", clusterInstance.Hostname, clusterInstance.VtgateProcess.GrpcPort),
		"-keyspace", keyspaceName,
		"-split_count", "2",
		sql)

	splitqueryresponse = nil
	err = json.Unmarshal([]byte(output), &splitqueryresponse)
	assert.Nil(t, err)
	assert.Equal(t, len(splitqueryresponse), 2)
	assert.Equal(t, splitqueryresponse[0].KeyRangePart.Keyspace, keyspaceName)
	assert.Equal(t, splitqueryresponse[1].KeyRangePart.Keyspace, keyspaceName)
	assert.Equal(t, len(splitqueryresponse[0].KeyRangePart.KeyRanges), 1)
	assert.Equal(t, len(splitqueryresponse[1].KeyRangePart.KeyRanges), 1)

	//then serve replica from the split shards

	sourceTablet := shard1.Replica()
	destinationTablets := []cluster.Vttablet{*shard21.Replica(), *shard22.Replica()}

	_ = clusterInstance.VtctlclientProcess.ExecuteCommand("MigrateServedTypes", shard1Ks, "replica")
	expectedPartitions = map[topodata.TabletType][]string{}
	expectedPartitions[topodata.TabletType_MASTER] = []string{shard1.Name}
	expectedPartitions[topodata.TabletType_REPLICA] = []string{shard21.Name, shard22.Name}
	expectedPartitions[topodata.TabletType_RDONLY] = []string{shard21.Name, shard22.Name}
	checkSrvKeyspace(t, cell, keyspaceName, "custom_ksid_col", topodata.KeyspaceIdType_UINT64, expectedPartitions, *clusterInstance)

	//move replica back and forth
	_ = clusterInstance.VtctlclientProcess.ExecuteCommand("MigrateServedTypes", "-reverse", shard1Ks, "replica")

	// After a backwards migration, queryservice should be enabled on source and disabled on destinations
	checkTabletQueryService(t, *sourceTablet, "SERVING", false, *clusterInstance)
	checkTabletQueryServices(t, destinationTablets, "NOT_SERVING", true, *clusterInstance)

	expectedPartitions = map[topodata.TabletType][]string{}
	expectedPartitions[topodata.TabletType_MASTER] = []string{shard1.Name}
	expectedPartitions[topodata.TabletType_REPLICA] = []string{shard1.Name}
	expectedPartitions[topodata.TabletType_RDONLY] = []string{shard21.Name, shard22.Name}
	checkSrvKeyspace(t, cell, keyspaceName, "custom_ksid_col", topodata.KeyspaceIdType_UINT64, expectedPartitions, *clusterInstance)

	_ = clusterInstance.VtctlclientProcess.ExecuteCommand("MigrateServedTypes", shard1Ks, "replica")

	// After a forwards migration, queryservice should be disabled on source and enabled on destinations
	checkTabletQueryService(t, *sourceTablet, "NOT_SERVING", true, *clusterInstance)
	checkTabletQueryServices(t, destinationTablets, "SERVING", false, *clusterInstance)
	expectedPartitions = map[topodata.TabletType][]string{}
	expectedPartitions[topodata.TabletType_MASTER] = []string{shard1.Name}
	expectedPartitions[topodata.TabletType_REPLICA] = []string{shard21.Name, shard22.Name}
	expectedPartitions[topodata.TabletType_RDONLY] = []string{shard21.Name, shard22.Name}
	checkSrvKeyspace(t, cell, keyspaceName, "custom_ksid_col", topodata.KeyspaceIdType_UINT64, expectedPartitions, *clusterInstance)

	// then serve master from the split shards
	_ = clusterInstance.VtctlclientProcess.ExecuteCommand("MigrateServedTypes", shard1Ks, "master")
	expectedPartitions = map[topodata.TabletType][]string{}
	expectedPartitions[topodata.TabletType_MASTER] = []string{shard21.Name, shard22.Name}
	expectedPartitions[topodata.TabletType_REPLICA] = []string{shard21.Name, shard22.Name}
	expectedPartitions[topodata.TabletType_RDONLY] = []string{shard21.Name, shard22.Name}
	checkSrvKeyspace(t, cell, keyspaceName, "custom_ksid_col", topodata.KeyspaceIdType_UINT64, expectedPartitions, *clusterInstance)

	// check the binlog players are gone now
	_ = shard21.MasterTablet().VttabletProcess.WaitForBinLogPlayerCount(0)
	_ = shard22.MasterTablet().VttabletProcess.WaitForBinLogPlayerCount(0)

	// make sure we can't delete a shard with tablets
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("DeleteShard", shard1Ks)
	assert.NotNil(t, err)

	// Teardown
	for _, tablet := range shard1.Vttablets {
		_ = tablet.MysqlctlProcess.Stop()
		_ = tablet.VttabletProcess.TearDown(true)
	}
	_ = clusterInstance.VtctlclientProcess.ExecuteCommand("DeleteTablet", shard1.Replica().Alias)
	_ = clusterInstance.VtctlclientProcess.ExecuteCommand("DeleteTablet", shard1.Rdonly().Alias)
	_ = clusterInstance.VtctlclientProcess.ExecuteCommand("DeleteTablet", "-allow_master", shard1MasterTablet.Alias)

	_ = clusterInstance.VtctlclientProcess.ExecuteCommand("RebuildKeyspaceGraph", keyspaceName)
	_ = clusterInstance.VtctlclientProcess.ExecuteCommand("DeleteShard", keyspaceName+"/"+shard1.Name)
}
