/*
Copyright 2025 The Vitess Authors.

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

package tabletbalancer

import (
	_ "embed"
	"flag"
	"fmt"
	"os"
	"testing"

	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	cell1           = "zone1"
	cell2           = "zone2"
	keyspaceName    = "ks"

	//go:embed schema.sql
	schemaSQL string

	vSchema = `{
		"sharded": false,
		"tables": {
			"balancer_test": {}
		}
	}`
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(cell1, "localhost")
		defer clusterInstance.Teardown()

		// Start topo server
		if err := clusterInstance.StartTopo(); err != nil {
			return 1
		}

		// Add second cell
		if err := clusterInstance.TopoProcess.ManageTopoDir("mkdir", "/vitess/"+cell2); err != nil {
			return 1
		}
		if err := clusterInstance.VtctldClientProcess.AddCellInfo(cell2); err != nil {
			return 1
		}

		// Start keyspace with tablets in cell1 (using standard method)
		keyspace := &cluster.Keyspace{
			Name:      keyspaceName,
			SchemaSQL: schemaSQL,
			VSchema:   vSchema,
		}

		// Start with 2 replicas (3 total tablets including primary)
		if err := clusterInstance.StartUnshardedKeyspace(*keyspace, 2, false, clusterInstance.Cell); err != nil {
			return 1
		}

		// Now manually add tablets to cell2 to simulate multi-cell setup
		shard := clusterInstance.Keyspaces[0].Shards[0]
		for i := 0; i < 3; i++ {
			tabletUID := clusterInstance.GetAndReserveTabletUID()
			tablet := &cluster.Vttablet{
				TabletUID: tabletUID,
				Type:      "replica",
				HTTPPort:  clusterInstance.GetAndReservePort(),
				GrpcPort:  clusterInstance.GetAndReservePort(),
				MySQLPort: clusterInstance.GetAndReservePort(),
				Cell:      cell2,
				Alias:     fmt.Sprintf("%s-%010d", cell2, tabletUID),
			}

			// Start Mysqlctl process for this tablet
			mysqlctlProcess, err := cluster.MysqlCtlProcessInstance(tablet.TabletUID, tablet.MySQLPort, clusterInstance.TmpDirectory)
			if err != nil {
				return 1
			}
			tablet.MysqlctlProcess = *mysqlctlProcess
			if err := tablet.MysqlctlProcess.Start(); err != nil {
				return 1
			}

			// Initialize vttablet
			tablet.VttabletProcess = cluster.VttabletProcessInstance(
				tablet.HTTPPort,
				tablet.GrpcPort,
				tablet.TabletUID,
				cell2,
				shard.Name,
				keyspaceName,
				clusterInstance.VtctldProcess.Port,
				tablet.Type,
				clusterInstance.TopoProcess.Port,
				clusterInstance.Hostname,
				clusterInstance.TmpDirectory,
				clusterInstance.VtTabletExtraArgs,
				clusterInstance.DefaultCharset)

			// Start vttablet
			if err := tablet.VttabletProcess.Setup(); err != nil {
				return 1
			}

			shard.Vttablets = append(shard.Vttablets, tablet)
		}

		clusterInstance.Keyspaces[0].Shards[0] = shard

		// we need to create an alias to make sure that we'll use both cells at all, even with cells_to_watch
		allCells := fmt.Sprintf("%s,%s", cell1, cell2)
		clusterInstance.VtctldClientProcess.ExecuteCommand("AddCellsAlias",
			"--cells", allCells,
			"combined_cells")

		return m.Run()
	}()
	os.Exit(exitCode)
}
