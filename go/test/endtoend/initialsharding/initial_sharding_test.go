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
	"io/ioutil"
	"net/http"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"vitess.io/vitess/go/test/endtoend/cluster"
	querypb "vitess.io/vitess/go/vt/proto/query"
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
	insertTabletTemplate     = `insert into %s(parent_id, id, msg) values(%d, %d, "%s")`
	insertTabletTemplateKsId = `insert into %s (parent_id, id, msg, custom_ksid_col) values (%d, %d, '%s', 0x%x) /* vtgate:: keyspace_id:%016X */ /* id:%d */`
	fixedParentId            = 86
	tableName                = "resharding1"
	vSchema                  = `
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

	output, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("InitShardMaster",
		"-force", fmt.Sprintf("%s/%s", keyspaceName, shard1.Name), shard1MasterTablet.Alias)
	assert.Contains(t, output, fmt.Sprintf("tablet %s ResetReplication failed", shard1.Replica().Alias))
	println(output)

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
	_, _ = shard1MasterTablet.VttabletProcess.QueryTablet(fmt.Sprintf(insertTabletTemplate, "resharding1", fixedParentId, 1, "msg1"), keyspaceName, true)
	_, _ = shard1MasterTablet.VttabletProcess.QueryTablet(fmt.Sprintf(insertTabletTemplate, "resharding1", fixedParentId, 2, "msg2"), keyspaceName, true)
	_, _ = shard1MasterTablet.VttabletProcess.QueryTablet(fmt.Sprintf(insertTabletTemplate, "resharding1", fixedParentId, 3, "msg3"), keyspaceName, true)

	// reload schema on all tablets so we can query them
	for _, vttablet := range shard1.Vttablets {
		_ = clusterInstance.VtctlclientProcess.ExecuteCommand("ReloadSchema", vttablet.Alias)
	}
	err = clusterInstance.StartVtgate()
	assert.Nil(t, err)
	println("vtgate started on port " + fmt.Sprintf("%d", clusterInstance.VtgateProcess.Port))

	_ = clusterInstance.VtgateProcess.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", keyspaceName, shard1.Name))
	_ = clusterInstance.VtgateProcess.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.replica", keyspaceName, shard1.Name))
	_ = clusterInstance.VtgateProcess.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.rdonly", keyspaceName, shard1.Name))

	// check the Map Reduce API works correctly, should use ExecuteShards,
	// as we're not sharded yet.
	// we have 3 values in the database, asking for 4 splits will get us
	// a single query.
	sql := `select id, msg from resharding1`
	output, err = clusterInstance.VtctlProcess.ExecuteCommandWithOutput("VtGateSplitQuery",
		"-server", fmt.Sprintf("%s:%d", clusterInstance.Hostname, clusterInstance.VtgateProcess.GrpcPort),
		"-keyspace", keyspaceName,
		"-split_count", "4",
		sql)

	var splitqueryresponse []vtgatepb.SplitQueryResponse_Part
	err = json.Unmarshal([]byte(output), &splitqueryresponse)
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
	output, err = clusterInstance.VtctlProcess.ExecuteCommandWithOutput("VtGateSplitQuery",
		"-server", fmt.Sprintf("%s:%d", clusterInstance.Hostname, clusterInstance.VtgateProcess.GrpcPort),
		"-keyspace", keyspaceName,
		"-split_count", "4",
		sql)
	splitqueryresponse = nil
	err = json.Unmarshal([]byte(output), &splitqueryresponse)
	assert.Equal(t, len(splitqueryresponse), 1)
	assert.Equal(t, splitqueryresponse[0].KeyRangePart.Keyspace, keyspaceName)
	assert.Equal(t, len(splitqueryresponse[0].KeyRangePart.KeyRanges), 1)
	assert.Empty(t, splitqueryresponse[0].KeyRangePart.KeyRanges[0])

	// Check srv keyspace
	expectedPartitions := map[topodata.TabletType][]string{}
	expectedPartitions[topodata.TabletType_MASTER] = []string{shard1.Name}
	expectedPartitions[topodata.TabletType_REPLICA] = []string{shard1.Name}
	expectedPartitions[topodata.TabletType_RDONLY] = []string{shard1.Name}
	checkSrvKeyspace(t, cell, keyspaceName, "custom_ksid_col", topodata.KeyspaceIdType_UINT64, expectedPartitions)

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

	println("vtworker -- " + fmt.Sprintf("http://localhost:%d/debug/vars", clusterInstance.VtworkerProcess.Port))
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
	var ksid uint64
	ksid = 0xD000000000000000
	insertSQL := fmt.Sprintf(insertTabletTemplateKsId, tableName, fixedParentId, 4, "msg4", ksid, ksid, 4)
	insertToTablet(insertSQL, *shard22.MasterTablet())

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
		checkValues(t, tablet, "INT64(86)", "INT64(1)", `VARCHAR("msg1")`, 1, true)
	}

	for _, tablet := range shard22.Vttablets {
		checkValues(t, tablet, "INT64(86)", "INT64(1)", `VARCHAR("msg1")`, 1, false)
	}

	for _, tablet := range shard21.Vttablets {
		checkValues(t, tablet, "INT64(86)", "INT64(2)", `VARCHAR("msg2")`, 2, false)
	}

	for _, tablet := range shard22.Vttablets {
		checkValues(t, tablet, "INT64(86)", "INT64(2)", `VARCHAR("msg2")`, 2, true)
	}

	for _, tablet := range shard21.Vttablets {
		checkValues(t, tablet, "INT64(86)", "INT64(3)", `VARCHAR("msg3")`, 3, false)
	}

	for _, tablet := range shard22.Vttablets {
		checkValues(t, tablet, "INT64(86)", "INT64(3)", `VARCHAR("msg3")`, 3, true)
	}

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ValidateSchemaKeyspace", keyspaceName)
	assert.Nil(t, err)

	// check the binlog players are running
	checkDestinationMaster(t, *shard21.MasterTablet(), []string{fmt.Sprintf("%s/%s", keyspaceName, shard1.Name)})
	checkDestinationMaster(t, *shard22.MasterTablet(), []string{fmt.Sprintf("%s/%s", keyspaceName, shard1.Name)})

	//  check that binlog server exported the stats vars
	checkBinlogServerVars(t, *shard1.Replica(), 0, 0)

	for _, tablet := range []cluster.Vttablet{*shard21.Rdonly(), *shard22.Rdonly()} {
		err = clusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", tablet.Alias)
		assert.Nil(t, err)
	}

	// testing filtered replication: insert a bunch of data on shard 1,
	// check we get most of it after a few seconds, wait for binlog server
	// timeout, check we get all of it.
	insertLots(1000, shard1MasterTablet)

	assert.True(t, checkLotsTimeout(t, *shard22.Replica(), 1000))
	checkLotsNotPresent(t, *shard21.Replica(), 1000)

	checkDestinationMaster(t, *shard21.MasterTablet(), []string{fmt.Sprintf("%s/%s", keyspaceName, shard1.Name)})
	checkDestinationMaster(t, *shard22.MasterTablet(), []string{fmt.Sprintf("%s/%s", keyspaceName, shard1.Name)})
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
	checkSrvKeyspace(t, cell, keyspaceName, "custom_ksid_col", topodata.KeyspaceIdType_UINT64, expectedPartitions)

	_ = shard21.Rdonly().VttabletProcess.WaitForTabletType("SERVING")
	_ = shard22.Rdonly().VttabletProcess.WaitForTabletType("SERVING")

	_ = clusterInstance.VtgateProcess.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.rdonly", keyspaceName, shard21.Name))
	_ = clusterInstance.VtgateProcess.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.rdonly", keyspaceName, shard22.Name))

	sql = fmt.Sprintf("select id, msg from %s", tableName)
	output, err = clusterInstance.VtctlProcess.ExecuteCommandWithOutput("VtGateSplitQuery",
		"-server", fmt.Sprintf("%s:%d", clusterInstance.Hostname, clusterInstance.VtgateProcess.GrpcPort),
		"-keyspace", keyspaceName,
		"-split_count", "2",
		sql)

	splitqueryresponse = nil
	err = json.Unmarshal([]byte(output), &splitqueryresponse)
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
	checkSrvKeyspace(t, cell, keyspaceName, "custom_ksid_col", topodata.KeyspaceIdType_UINT64, expectedPartitions)

	//move replica back and forth
	_ = clusterInstance.VtctlclientProcess.ExecuteCommand("MigrateServedTypes", "-reverse", shard1Ks, "replica")

	// After a backwards migration, queryservice should be enabled on source and disabled on destinations
	checkTabletQueryService(t, *sourceTablet, "SERVING", false)
	checkTabletQueryServices(t, destinationTablets, "NOT_SERVING", true)

	expectedPartitions = map[topodata.TabletType][]string{}
	expectedPartitions[topodata.TabletType_MASTER] = []string{shard1.Name}
	expectedPartitions[topodata.TabletType_REPLICA] = []string{shard1.Name}
	expectedPartitions[topodata.TabletType_RDONLY] = []string{shard21.Name, shard22.Name}
	checkSrvKeyspace(t, cell, keyspaceName, "custom_ksid_col", topodata.KeyspaceIdType_UINT64, expectedPartitions)

	_ = clusterInstance.VtctlclientProcess.ExecuteCommand("MigrateServedTypes", shard1Ks, "replica")

	// After a forwards migration, queryservice should be disabled on source and enabled on destinations
	checkTabletQueryService(t, *sourceTablet, "NOT_SERVING", true)
	checkTabletQueryServices(t, destinationTablets, "SERVING", false)
	expectedPartitions = map[topodata.TabletType][]string{}
	expectedPartitions[topodata.TabletType_MASTER] = []string{shard1.Name}
	expectedPartitions[topodata.TabletType_REPLICA] = []string{shard21.Name, shard22.Name}
	expectedPartitions[topodata.TabletType_RDONLY] = []string{shard21.Name, shard22.Name}
	checkSrvKeyspace(t, cell, keyspaceName, "custom_ksid_col", topodata.KeyspaceIdType_UINT64, expectedPartitions)

	// then serve master from the split shards
	_ = clusterInstance.VtctlclientProcess.ExecuteCommand("MigrateServedTypes", shard1Ks, "master")
	expectedPartitions = map[topodata.TabletType][]string{}
	expectedPartitions[topodata.TabletType_MASTER] = []string{shard21.Name, shard22.Name}
	expectedPartitions[topodata.TabletType_REPLICA] = []string{shard21.Name, shard22.Name}
	expectedPartitions[topodata.TabletType_RDONLY] = []string{shard21.Name, shard22.Name}
	checkSrvKeyspace(t, cell, keyspaceName, "custom_ksid_col", topodata.KeyspaceIdType_UINT64, expectedPartitions)

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

	println("Add done")

}

func checkSrvKeyspace(t *testing.T, cell string, ksname string, shardingCol string, colType topodata.KeyspaceIdType, expectedPartition map[topodata.TabletType][]string) {
	srvKeyspace := getSrvKeyspace(t, cell, ksname)
	assert.Equal(t, srvKeyspace.ShardingColumnName, shardingCol)
	assert.Equal(t, srvKeyspace.ShardingColumnType, colType)

	currentPartition := map[topodata.TabletType][]string{}

	for _, partition := range srvKeyspace.Partitions {
		currentPartition[partition.ServedType] = []string{}
		for _, shardRef := range partition.ShardReferences {
			currentPartition[partition.ServedType] = append(currentPartition[partition.ServedType], shardRef.Name)
		}
	}

	assert.True(t, reflect.DeepEqual(currentPartition, expectedPartition))
}

func getSrvKeyspace(t *testing.T, cell string, ksname string) *topodata.SrvKeyspace {
	output, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("GetSrvKeyspace", cell, ksname)
	assert.Nil(t, err)
	var srvKeyspace topodata.SrvKeyspace

	err = json.Unmarshal([]byte(output), &srvKeyspace)
	assert.Nil(t, err)
	return &srvKeyspace
}

func verifyReconciliationCounters(t *testing.T, vtworkerURL string, availabilityType string, table string,
	inserts int, updates int, deletes int, equals int) {
	resp, err := http.Get(vtworkerURL)
	assert.Nil(t, err)
	assert.Equal(t, resp.StatusCode, 200)

	resultMap := make(map[string]interface{})
	respByte, _ := ioutil.ReadAll(resp.Body)
	err = json.Unmarshal(respByte, &resultMap)
	assert.Nil(t, err)

	value := getValueFromJSON(resultMap, "Worker"+availabilityType+"InsertsCounters", table)
	if inserts == 0 {
		assert.Equal(t, value, "")
	} else {
		assert.Equal(t, value, fmt.Sprintf("%d", inserts))
	}

	value = getValueFromJSON(resultMap, "Worker"+availabilityType+"UpdatesCounters", table)
	if updates == 0 {
		assert.Equal(t, value, "")
	} else {
		assert.Equal(t, value, fmt.Sprintf("%d", updates))
	}

	value = getValueFromJSON(resultMap, "Worker"+availabilityType+"DeletesCounters", table)
	if deletes == 0 {
		assert.Equal(t, value, "")
	} else {
		assert.Equal(t, value, fmt.Sprintf("%d", deletes))
	}

	value = getValueFromJSON(resultMap, "Worker"+availabilityType+"EqualRowsCounters", table)
	if equals == 0 {
		assert.Equal(t, value, "")
	} else {
		assert.Equal(t, value, fmt.Sprintf("%d", equals))
	}
}

func getValueFromJSON(jsonMap map[string]interface{}, keyname string, tableName string) string {
	object := reflect.ValueOf(jsonMap[keyname])
	if object.Kind() == reflect.Map {
		for _, key := range object.MapKeys() {
			if key.String() == tableName {
				return fmt.Sprintf("%v", object.MapIndex(key))
			}
		}
	}
	return ""
}

// checkValues check value from sql query to table with expected values
// TODO: compare the custom_ksid_col value
func checkValues(t *testing.T, vttablet cluster.Vttablet, col1 string, col2 string, col3 string, id int, exists bool) bool {
	query := fmt.Sprintf("select parent_id, id, msg, custom_ksid_col from %s where parent_id = %d and id = %d", tableName, fixedParentId, id)
	result, err := vttablet.VttabletProcess.QueryTablet(query, keyspaceName, true)
	assert.Nil(t, err)
	isFound := false
	if exists && len(result.Rows) > 0 {
		isFound = assert.Equal(t, result.Rows[0][0].String(), col1)
		isFound = assert.Equal(t, result.Rows[0][1].String(), col2)
		isFound = assert.Equal(t, result.Rows[0][2].String(), col3)
	} else {
		assert.Equal(t, len(result.Rows), 0)
	}
	return isFound
}

func checkDestinationMaster(t *testing.T, vttablet cluster.Vttablet, sourceShards []string) {
	_ = vttablet.VttabletProcess.WaitForBinLogPlayerCount(len(sourceShards))
	checkBinlogVars(t, vttablet)
	checkStreamHealthEqualsBinlogPlayerVars(t, vttablet, len(sourceShards))
}

// checkStreamHealthEqualsBinlogPlayerVars - Checks the variables exported by streaming health check match vars.
func checkStreamHealthEqualsBinlogPlayerVars(t *testing.T, vttablet cluster.Vttablet, count int) {
	tabletVars := vttablet.VttabletProcess.GetVars()

	streamCountStr := fmt.Sprintf("%v", reflect.ValueOf(tabletVars["VReplicationStreamCount"]))
	streamCount, _ := strconv.Atoi(streamCountStr)

	//secondBehindMaserMaxStr := fmt.Sprintf("%v", reflect.ValueOf(tabletVars["VReplicationSecondsBehindMasterMax"]))
	//secondBehindMaserMax, _ := strconv.ParseInt(secondBehindMaserMaxStr, 10, 64)

	assert.Equal(t, streamCount, count)
	// Enforce health check because it's not running by default as
	// tablets may not be started with it, or may not run it in time.
	_ = clusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", vttablet.Alias)
	streamHealth, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("VtTabletStreamHealth", "-count", "1", vttablet.Alias)
	assert.Nil(t, err)

	var streamHealthResponse querypb.StreamHealthResponse
	err = json.Unmarshal([]byte(streamHealth), &streamHealthResponse)
	assert.Nil(t, err, "error should be Nil")
	assert.Equal(t, streamHealthResponse.Serving, false)
	assert.NotNil(t, streamHealthResponse.RealtimeStats)
	assert.Equal(t, streamHealthResponse.RealtimeStats.HealthError, "")
	assert.NotNil(t, streamHealthResponse.RealtimeStats.BinlogPlayersCount)

	assert.Equal(t, streamCount, int(streamHealthResponse.RealtimeStats.BinlogPlayersCount))
	// TODO: fix this assertions , issue with type check
	//assert.Equal(t, secondBehindMaserMax, streamHealthResponse.RealtimeStats.SecondsBehindMasterFilteredReplication)
}

func checkBinlogVars(t *testing.T, vttablet cluster.Vttablet) {
	resultMap := vttablet.VttabletProcess.GetVars()
	assert.Contains(t, resultMap, "VReplicationStreamCount")
	assert.Contains(t, resultMap, "VReplicationSecondsBehindMasterMax")
	assert.Contains(t, resultMap, "VReplicationSecondsBehindMaster")
	assert.Contains(t, resultMap, "VReplicationSource")
	// TODO: complete all assertion from base_sharding
}

func checkBinlogServerVars(t *testing.T, vttablet cluster.Vttablet, minStatement int, minTxn int) {
	resultMap := vttablet.VttabletProcess.GetVars()
	assert.Contains(t, resultMap, "UpdateStreamKeyRangeStatements")
	assert.Contains(t, resultMap, "UpdateStreamKeyRangeTransactions")
	if minStatement > 0 {
		value := fmt.Sprintf("%v", reflect.ValueOf(resultMap["UpdateStreamKeyRangeStatements"]))
		println(" stmt value - " + value)
		iValue, _ := strconv.Atoi(value)
		assert.True(t, iValue >= minStatement)
	}

	if minTxn > 0 {
		value := fmt.Sprintf("%v", reflect.ValueOf(resultMap["UpdateStreamKeyRangeStatements"]))
		println("txn value - " + value)
		iValue, _ := strconv.Atoi(value)
		assert.True(t, iValue >= minTxn)
	}
}

func insertLots(count uint64, vttablet cluster.Vttablet) {
	var query1, query2 string
	var base1, base2, i uint64
	base1 = 0xA000000000000000
	base2 = 0xE000000000000000
	for i = 0; i < count; i++ {
		query1 = fmt.Sprintf(insertTabletTemplateKsId, tableName, fixedParentId, 10000+i, fmt.Sprintf("msg-range1-%d", 10000+i), base1+i, base1+i, 10000+i)
		query2 = fmt.Sprintf(insertTabletTemplateKsId, tableName, fixedParentId, 20000+i, fmt.Sprintf("msg-range2-%d", 20000+i), base2+i, base2+i, 20000+i)

		insertToTablet(query1, vttablet)
		insertToTablet(query2, vttablet)
	}
}

func insertToTablet(query string, vttablet cluster.Vttablet) {
	_, _ = vttablet.VttabletProcess.QueryTablet("begin", keyspaceName, true)
	_, _ = vttablet.VttabletProcess.QueryTablet(query, keyspaceName, true)
	_, _ = vttablet.VttabletProcess.QueryTablet("commit", keyspaceName, true)
}

func checkLotsTimeout(t *testing.T, vttablet cluster.Vttablet, count int) bool {
	timeout := time.Now().Add(10 * time.Second)
	for time.Now().Before(timeout) {
		percentFound := checkLots(t, vttablet, count)
		fmt.Println(fmt.Sprintf("Total rows found %f", percentFound))
		if percentFound == 100 {
			return true
		}
		time.Sleep(300 * time.Millisecond)
	}
	return false
}

func checkLotsNotPresent(t *testing.T, vttablet cluster.Vttablet, count int) {
	for i := 0; i < count; i++ {
		assert.False(t, checkValues(t, vttablet, "INT64(86)",
			fmt.Sprintf("INT64(%d)", 10000+i),
			fmt.Sprintf(`VARCHAR("msg-range1-%d")`, 10000+i),
			10000+i, true))

		assert.False(t, checkValues(t, vttablet, "INT64(86)",
			fmt.Sprintf("INT64(%d)", 20000+i),
			fmt.Sprintf(`VARCHAR("msg-range2-%d")`, 20000+i),
			20000+i, true))
	}
}

func checkLots(t *testing.T, vttablet cluster.Vttablet, count int) float32 {
	var isFound bool
	var totalFound int
	for i := 0; i < count; i++ {
		// "INT64(1)" `VARCHAR("msg1")`,
		isFound = checkValues(t, vttablet, "INT64(86)",
			fmt.Sprintf("INT64(%d)", 10000+i),
			fmt.Sprintf(`VARCHAR("msg-range1-%d")`, 10000+i),
			10000+i, true)
		if isFound {
			totalFound++
		} else {
			println(fmt.Sprintf("Row not found for %d", 10000+i))
		}

		isFound = checkValues(t, vttablet, "INT64(86)",
			fmt.Sprintf("INT64(%d)", 20000+i),
			fmt.Sprintf(`VARCHAR("msg-range2-%d")`, 20000+i),
			20000+i, true)
		if isFound {
			totalFound++
		} else {
			println(fmt.Sprintf("Row not found for %d", 20000+i))
		}
	}
	println(fmt.Sprintf("found %d", totalFound))
	return float32(totalFound * 100 / count / 2)
}

func checkRunningBinlogPlayer(t *testing.T, vttablet cluster.Vttablet, numberOfQueries int, numberOfTxns int) {
	status := vttablet.VttabletProcess.GetStatus()
	println(status)
	assert.Contains(t, status, "VReplication state: Open")
	assert.Contains(t, status, fmt.Sprintf("<td><b>All</b>: %d<br><b>Query</b>: %d<br><b>Transaction</b>: %d<br></td>", numberOfQueries+numberOfTxns, numberOfQueries, numberOfTxns))
	assert.Contains(t, status, "</html>")
}

func checkTabletQueryServices(t *testing.T, vttablets []cluster.Vttablet, expectedStatus string, tabletControlEnabled bool) {
	for _, tablet := range vttablets {
		checkTabletQueryService(t, tablet, expectedStatus, tabletControlEnabled)
	}
}

func checkTabletQueryService(t *testing.T, vttablet cluster.Vttablet, expectedStatus string, tabletControlEnabled bool) {
	tabletStatus := vttablet.VttabletProcess.GetTabletStatus()
	assert.Equal(t, tabletStatus, expectedStatus)

	queryServiceDisabled := "Query Service disabled: TabletControl.DisableQueryService set"
	status := vttablet.VttabletProcess.GetStatus()
	if tabletControlEnabled {
		assert.Contains(t, status, queryServiceDisabled)
	} else {
		assert.NotContains(t, status, queryServiceDisabled)
	}

	if vttablet.Type == "rdonly" {
		// Run RunHealthCheck to be sure the tablet doesn't change its serving state.
		_ = clusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", vttablet.Alias)
		tabletStatus = vttablet.VttabletProcess.GetTabletStatus()
		assert.Equal(t, tabletStatus, expectedStatus)
	}
}
