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
	"context"
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"

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

		// Start keyspace with 1 primary + 2 replicas
		keyspace := &cluster.Keyspace{
			Name:      keyspaceName,
			SchemaSQL: sqlSchema,
			VSchema:   vSchema,
		}
		if err := clusterInstance.StartKeyspace(*keyspace, []string{"0"}, 2, false); err != nil {
			fmt.Printf("Failed to start keyspace: %v\n", err)
			return 1
		}

		// Find the replica tablets and assign one as "old" and one as "new"
		shard := &clusterInstance.Keyspaces[0].Shards[0]
		replicaCount := 0
		for _, tablet := range shard.Vttablets {
			if tablet.Type == "replica" {
				replicaCount++
				if replicaCount == 1 {
					newReplica = tablet
				} else {
					oldReplica = tablet
				}
			}
		}
		if oldReplica == nil || newReplica == nil {
			fmt.Printf("Failed to identify old and new replicas\n")
			return 1
		}

		// Set the "old" replica's start time to 1 hour ago via topo
		oldStartTime := time.Now().Add(-1 * time.Hour).Unix()
		if err := updateTabletStartTime(oldReplica, oldStartTime); err != nil {
			fmt.Printf("Failed to update old replica start time: %v\n", err)
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

// updateTabletStartTime updates the TabletStartTime in topo for a tablet.
func updateTabletStartTime(tablet *cluster.Vttablet, startTime int64) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ts := clusterInstance.TopoProcess.Server
	_, err := ts.UpdateTabletFields(ctx, tablet.GetAlias(), func(t *topodatapb.Tablet) error {
		t.TabletStartTime = startTime
		return nil
	})
	return err
}
