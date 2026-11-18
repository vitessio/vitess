/*
Copyright 2026 The Vitess Authors.

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

package shardmetricpercentile

import (
	"flag"
	"fmt"
	"os"
	"testing"

	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	primaryTablet   *cluster.Vttablet
	replicaTablets  []*cluster.Vttablet
	hostname        = "localhost"
	keyspaceName    = "ks"
	cell            = "zone1"
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(cell, hostname)
		defer clusterInstance.Teardown()

		if err := clusterInstance.StartTopo(); err != nil {
			fmt.Fprintf(os.Stderr, "starting topo: %v\n", err)
			return 1
		}

		clusterInstance.VtTabletExtraArgs = []string{
			"--lock-tables-timeout", "5s",
			"--enable-replication-reporter",
			"--heartbeat-interval", "250ms",
			"--heartbeat-on-demand-duration", "5s",
		}

		keyspace := &cluster.Keyspace{Name: keyspaceName}
		if err := clusterInstance.StartUnshardedKeyspace(*keyspace, replicaCount, false, clusterInstance.Cell); err != nil {
			fmt.Fprintf(os.Stderr, "starting keyspace %s: %v\n", keyspaceName, err)
			return 1
		}

		for _, tablet := range clusterInstance.Keyspaces[0].Shards[0].Vttablets {
			switch tablet.Type {
			case "primary":
				primaryTablet = tablet
			case "replica":
				replicaTablets = append(replicaTablets, tablet)
			}
		}
		if primaryTablet == nil || len(replicaTablets) != replicaCount {
			fmt.Fprintf(os.Stderr, "expected a primary and %d replicas, got primary=%v and %d replicas\n", replicaCount, primaryTablet != nil, len(replicaTablets))
			return 1
		}

		return m.Run()
	}()
	os.Exit(exitCode)
}
