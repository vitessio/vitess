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

package sharded

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/log"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	hostname        = "localhost"
	keyspaceName    = "ks"
	cell            = "zone1"
	sqlSchema       = `
		create table vt_select_test (
		id bigint not null,
		msg varchar(64),
		primary key (id)
		) Engine=InnoDB
		`
	sqlSchemaReverse = `
		create table vt_select_test (
		msg varchar(64),
		id bigint not null,
		primary key (id)
		) Engine=InnoDB
		`
	vSchema = `
		{
		  "sharded": true,
		  "vindexes": {
			"hash_index": {
			  "type": "hash"
			}
		  },
		  "tables": {
			"vt_select_test": {
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
)

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitcode, err := func() (int, error) {
		clusterInstance = cluster.NewCluster(cell, hostname)
		defer clusterInstance.Teardown()

		// Start topo server
		if err := clusterInstance.StartTopo(); err != nil {
			return 1, err
		}
		if err := clusterInstance.VtctlProcess.CreateKeyspace(keyspaceName); err != nil {
			return 1, err
		}

		initCluster([]string{"-80", "80-"}, 2)

		return m.Run(), nil
	}()
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	} else {
		os.Exit(exitcode)
	}

}

func TestShardedKeyspace(t *testing.T) {
	defer cluster.PanicHandler(t)
	shard1 := clusterInstance.Keyspaces[0].Shards[0]
	shard2 := clusterInstance.Keyspaces[0].Shards[1]

	shard1Primary := shard1.Vttablets[0]
	shard2Primary := shard2.Vttablets[0]
	err := clusterInstance.VtctlclientProcess.InitializeShard(keyspaceName, shard1.Name, cell, shard1Primary.TabletUID)
	require.Nil(t, err)
	err = clusterInstance.VtctlclientProcess.InitializeShard(keyspaceName, shard2.Name, cell, shard2Primary.TabletUID)
	require.Nil(t, err)

	err = clusterInstance.StartVTOrc(keyspaceName)
	require.NoError(t, err)

	// apply the schema on the first shard through vtctl, so all tablets
	// are the same.
	//apply the schema on the second shard.
	_, err = shard1Primary.VttabletProcess.QueryTablet(sqlSchema, keyspaceName, true)
	require.Nil(t, err)

	_, err = shard2Primary.VttabletProcess.QueryTablet(sqlSchemaReverse, keyspaceName, true)
	require.Nil(t, err)

	if err = clusterInstance.VtctlclientProcess.ApplyVSchema(keyspaceName, vSchema); err != nil {
		log.Error(err.Error())
		return
	}

	reloadSchemas(t,
		shard1Primary.Alias,
		shard1.Vttablets[1].Alias,
		shard2Primary.Alias,
		shard2.Vttablets[1].Alias)

	_ = clusterInstance.VtctlclientProcess.ExecuteCommand("SetReadWrite", shard1Primary.Alias)
	_ = clusterInstance.VtctlclientProcess.ExecuteCommand("SetReadWrite", shard2Primary.Alias)

	_, _ = shard1Primary.VttabletProcess.QueryTablet("insert into vt_select_test (id, msg) values (1, 'test 1')", keyspaceName, true)
	_, _ = shard2Primary.VttabletProcess.QueryTablet("insert into vt_select_test (id, msg) values (10, 'test 10')", keyspaceName, true)

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("Validate", "--", "--ping-tablets")
	require.Nil(t, err)

	rows, err := shard1Primary.VttabletProcess.QueryTablet("select id, msg from vt_select_test order by id", keyspaceName, true)
	require.Nil(t, err)
	assert.Equal(t, `[[INT64(1) VARCHAR("test 1")]]`, fmt.Sprintf("%v", rows.Rows))

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ValidateSchemaShard", fmt.Sprintf("%s/%s", keyspaceName, shard1.Name))
	require.Nil(t, err)

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ValidateSchemaShard", fmt.Sprintf("%s/%s", keyspaceName, shard1.Name))
	require.Nil(t, err)

	output, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("ValidateSchemaKeyspace", keyspaceName)
	require.Error(t, err)
	// We should assert that there is a schema difference and that both the shard primaries are involved in it.
	// However, we cannot assert in which order the two primaries will occur since the underlying function does not guarantee that
	// We could have an output here like `schemas differ ... shard1Primary ... differs from: shard2Primary ...` or `schemas differ ... shard2Primary ... differs from: shard1Primary ...`
	assert.Contains(t, output, "schemas differ on table vt_select_test:")
	assert.Contains(t, output, shard1Primary.Alias+": CREATE TABLE")
	assert.Contains(t, output, shard2Primary.Alias+": CREATE TABLE")

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ValidateVersionShard", fmt.Sprintf("%s/%s", keyspaceName, shard1.Name))
	require.Nil(t, err)
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("GetPermissions", shard1.Vttablets[1].Alias)
	require.Nil(t, err)
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ValidatePermissionsShard", fmt.Sprintf("%s/%s", keyspaceName, shard1.Name))
	require.Nil(t, err)
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ValidatePermissionsKeyspace", keyspaceName)
	require.Nil(t, err)

	rows, err = shard1Primary.VttabletProcess.QueryTablet("select id, msg from vt_select_test order by id", keyspaceName, true)
	require.Nil(t, err)
	assert.Equal(t, `[[INT64(1) VARCHAR("test 1")]]`, fmt.Sprintf("%v", rows.Rows))

	rows, err = shard2Primary.VttabletProcess.QueryTablet("select id, msg from vt_select_test order by id", keyspaceName, true)
	require.Nil(t, err)
	assert.Equal(t, `[[INT64(10) VARCHAR("test 10")]]`, fmt.Sprintf("%v", rows.Rows))
}

func reloadSchemas(t *testing.T, aliases ...string) {
	for _, alias := range aliases {
		if err := clusterInstance.VtctlclientProcess.ExecuteCommand("ReloadSchema", alias); err != nil {
			assert.Fail(t, "Unable to reload schema")
		}

	}
}

func initCluster(shardNames []string, totalTabletsRequired int) {
	keyspace := cluster.Keyspace{
		Name: keyspaceName,
	}
	for _, shardName := range shardNames {
		shard := &cluster.Shard{
			Name: shardName,
		}

		var mysqlCtlProcessList []*exec.Cmd

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
			if i == 0 { // Make the first one as primary
				tablet.Type = "primary"
			}
			// Start Mysqlctl process
			tablet.MysqlctlProcess = *cluster.MysqlCtlProcessInstance(tablet.TabletUID, tablet.MySQLPort, clusterInstance.TmpDirectory)
			proc, err := tablet.MysqlctlProcess.StartProcess()
			if err != nil {
				return
			}
			mysqlCtlProcessList = append(mysqlCtlProcessList, proc)

			// start vttablet process
			tablet.VttabletProcess = cluster.VttabletProcessInstance(
				tablet.HTTPPort,
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
				clusterInstance.DefaultCharset)
			tablet.Alias = tablet.VttabletProcess.TabletPath

			shard.Vttablets = append(shard.Vttablets, tablet)
		}
		for _, proc := range mysqlCtlProcessList {
			if err := proc.Wait(); err != nil {
				return
			}
		}

		for _, tablet := range shard.Vttablets {
			log.Info(fmt.Sprintf("Starting vttablet for tablet uid %d, grpc port %d", tablet.TabletUID, tablet.GrpcPort))

			if err := tablet.VttabletProcess.Setup(); err != nil {
				log.Error(err.Error())
				return
			}
		}

		keyspace.Shards = append(keyspace.Shards, *shard)
	}
	clusterInstance.Keyspaces = append(clusterInstance.Keyspaces, keyspace)
}
