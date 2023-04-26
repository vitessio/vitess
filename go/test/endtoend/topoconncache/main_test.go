/*
Copyright 2022 The Vitess Authors.

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

package topoconncache

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"testing"

	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	cell1           = "zone1"
	cell2           = "zone2"
	hostname        = "localhost"
	keyspaceName    = "ks"
	tableName       = "test_table"
	sqlSchema       = `
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

	shard1 cluster.Shard
	shard2 cluster.Shard
)

/*
	This end-to-end test validates the cache fix in topo/server.go inside ConnForCell.

The issue was, if we delete and add back a cell with same name but at different path, the map of cells
in server.go returned the connection object of the previous cell instead of newly-created one
with the same name.
Topology: We create a keyspace with two shards , having 3 tablets each. Primaries belong
to 'zone1' and replicas/rdonly belongs to cell2.
*/
func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitcode, err := func() (int, error) {
		clusterInstance = cluster.NewCluster(cell1, hostname)
		defer clusterInstance.Teardown()
		clusterInstance.Keyspaces = append(clusterInstance.Keyspaces, cluster.Keyspace{
			Name: keyspaceName,
		})

		// Start topo server
		if err := clusterInstance.StartTopo(); err != nil {
			return 1, err
		}

		// Adding another cell in the same cluster
		err := clusterInstance.TopoProcess.ManageTopoDir("mkdir", "/vitess/"+cell2)
		if err != nil {
			return 1, err
		}
		err = clusterInstance.VtctlProcess.AddCellInfo(cell2)
		if err != nil {
			return 1, err
		}

		shard1Primary = clusterInstance.NewVttabletInstance("primary", 0, cell1)
		shard1Replica = clusterInstance.NewVttabletInstance("replica", 0, cell2)
		shard1Rdonly = clusterInstance.NewVttabletInstance("rdonly", 0, cell2)

		shard2Primary = clusterInstance.NewVttabletInstance("primary", 0, cell1)
		shard2Replica = clusterInstance.NewVttabletInstance("replica", 0, cell2)
		shard2Rdonly = clusterInstance.NewVttabletInstance("rdonly", 0, cell2)

		var mysqlProcs []*exec.Cmd
		for _, tablet := range []*cluster.Vttablet{shard1Primary, shard1Replica, shard1Rdonly, shard2Primary, shard2Replica, shard2Rdonly} {
			tablet.MysqlctlProcess = *cluster.MysqlCtlProcessInstance(tablet.TabletUID, tablet.MySQLPort, clusterInstance.TmpDirectory)
			tablet.VttabletProcess = cluster.VttabletProcessInstance(tablet.HTTPPort,
				tablet.GrpcPort,
				tablet.TabletUID,
				tablet.Cell,
				"",
				keyspaceName,
				clusterInstance.VtctldProcess.Port,
				tablet.Type,
				clusterInstance.TopoPort,
				hostname,
				clusterInstance.TmpDirectory,
				commonTabletArg,
				true,
				clusterInstance.DefaultCharset,
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

		if err := clusterInstance.VtctlProcess.CreateKeyspace(keyspaceName); err != nil {
			return 1, err
		}

		shard1 = cluster.Shard{
			Name:      "-80",
			Vttablets: []*cluster.Vttablet{shard1Primary, shard1Replica, shard1Rdonly},
		}
		for idx := range shard1.Vttablets {
			shard1.Vttablets[idx].VttabletProcess.Shard = shard1.Name
		}
		clusterInstance.Keyspaces[0].Shards = append(clusterInstance.Keyspaces[0].Shards, shard1)

		shard2 = cluster.Shard{
			Name:      "80-",
			Vttablets: []*cluster.Vttablet{shard2Primary, shard2Replica, shard2Rdonly},
		}
		for idx := range shard2.Vttablets {
			shard2.Vttablets[idx].VttabletProcess.Shard = shard2.Name
		}
		clusterInstance.Keyspaces[0].Shards = append(clusterInstance.Keyspaces[0].Shards, shard2)

		for _, tablet := range shard1.Vttablets {
			if err := tablet.VttabletProcess.Setup(); err != nil {
				return 1, err
			}
		}
		if err := clusterInstance.VtctlclientProcess.InitializeShard(keyspaceName, shard1.Name, shard1Primary.Cell, shard1Primary.TabletUID); err != nil {
			return 1, err
		}

		// run a health check on source replica so it responds to discovery
		// (for binlog players) and on the source rdonlys (for workers)
		for _, tablet := range []string{shard1Replica.Alias, shard1Rdonly.Alias} {
			if err := clusterInstance.VtctlclientProcess.ExecuteCommand("RunHealthCheck", tablet); err != nil {
				return 1, err
			}
		}

		for _, tablet := range shard2.Vttablets {
			if err := tablet.VttabletProcess.Setup(); err != nil {
				return 1, err
			}
		}

		if err := clusterInstance.VtctlclientProcess.InitializeShard(keyspaceName, shard2.Name, shard2Primary.Cell, shard2Primary.TabletUID); err != nil {
			return 1, err
		}

		if err := clusterInstance.VtctlclientProcess.ApplySchema(keyspaceName, fmt.Sprintf(sqlSchema, tableName)); err != nil {
			return 1, err
		}
		if err := clusterInstance.VtctlclientProcess.ApplyVSchema(keyspaceName, fmt.Sprintf(vSchema, tableName)); err != nil {
			return 1, err
		}

		_ = clusterInstance.VtctlclientProcess.ExecuteCommand("RebuildKeyspaceGraph", keyspaceName)

		return m.Run(), nil
	}()
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	} else {
		os.Exit(exitcode)
	}
}

func testURL(t *testing.T, url string, testCaseName string) {
	statusCode := getStatusForURL(url)
	if got, want := statusCode, 200; got != want {
		t.Errorf("\nurl: %v\nstatus code: %v \nwant %v for %s", url, got, want, testCaseName)
	}
}

// getStatusForUrl returns the status code for the URL
func getStatusForURL(url string) int {
	resp, err := http.Get(url)
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	return resp.StatusCode
}
