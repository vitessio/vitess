/*
Copyright 2024 The Vitess Authors.

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

package warming

import (
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	keyspaceName    = "commerce"
	cell            = "zone1"

	// Track tablets by their start time category for test verification
	oldReplica *cluster.Vttablet
	newReplica *cluster.Vttablet

	sqlSchema = `
		create table test_table (
			id bigint not null auto_increment,
			val varchar(128),
			primary key(id)
		) ENGINE=InnoDB;
	`

	vSchema = `{
		"tables": {
			"test_table": {}
		}
	}`
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(cell, "localhost")
		defer clusterInstance.Teardown()

		// Start topo server
		if err := clusterInstance.StartTopo(); err != nil {
			fmt.Printf("Failed to start topo: %v\n", err)
			return 1
		}

		// Start keyspace with standard setup (1 primary + 2 replicas)
		// All tablets will initially have "now" as their start time
		keyspace := &cluster.Keyspace{
			Name:      keyspaceName,
			SchemaSQL: sqlSchema,
			VSchema:   vSchema,
		}
		if err := clusterInstance.StartUnshardedKeyspace(*keyspace, 2, false); err != nil {
			fmt.Printf("Failed to start keyspace: %v\n", err)
			return 1
		}

		// Find the replica tablets
		shard := &clusterInstance.Keyspaces[0].Shards[0]
		var replicas []*cluster.Vttablet
		for _, tablet := range shard.Vttablets {
			if tablet.Type == "replica" {
				replicas = append(replicas, tablet)
			}
		}
		if len(replicas) < 2 {
			fmt.Printf("Expected 2 replicas, got %d\n", len(replicas))
			return 1
		}

		// Keep first replica as "new" (current time)
		newReplica = replicas[0]

		// Restart second replica with "old" start time (1 hour ago)
		oldReplica = replicas[1]
		if err := oldReplica.VttabletProcess.TearDown(); err != nil {
			fmt.Printf("Failed to stop old replica: %v\n", err)
			return 1
		}

		// Recreate the vttablet process with ExtraEnv for old start time
		oldStartTime := time.Now().Add(-1 * time.Hour).Unix()
		oldReplica.VttabletProcess = cluster.VttabletProcessInstance(
			oldReplica.HTTPPort,
			oldReplica.GrpcPort,
			oldReplica.TabletUID,
			cell,
			shard.Name,
			keyspaceName,
			clusterInstance.VtctldProcess.Port,
			oldReplica.Type,
			clusterInstance.TopoProcess.Port,
			clusterInstance.Hostname,
			clusterInstance.TmpDirectory,
			clusterInstance.VtTabletExtraArgs,
			clusterInstance.DefaultCharset)
		oldReplica.VttabletProcess.ExtraEnv = []string{
			fmt.Sprintf("VTTEST_TABLET_START_TIME=%d", oldStartTime),
		}
		oldReplica.VttabletProcess.ServingStatus = "SERVING"
		if err := oldReplica.VttabletProcess.Setup(); err != nil {
			fmt.Printf("Failed to restart old replica: %v\n", err)
			return 1
		}

		// Start vtgate with warming balancer mode
		vtgateInstance := clusterInstance.NewVtgateInstance()
		vtgateInstance.ExtraArgs = append(vtgateInstance.ExtraArgs,
			"--vtgate-balancer-mode", "warming",
			"--balancer-warming-period", "30m",
			"--balancer-warming-traffic-percent", "10",
		)
		if err := vtgateInstance.Setup(); err != nil {
			fmt.Printf("Failed to start vtgate: %v\n", err)
			return 1
		}
		clusterInstance.VtgateProcess = *vtgateInstance

		return m.Run()
	}()
	os.Exit(exitCode)
}
