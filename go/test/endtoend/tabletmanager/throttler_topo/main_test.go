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
package throttler

import (
	"context"
	"flag"
	"fmt"
	"os"
	"testing"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/vitesst"
)

var (
	clusterInstance *vitesst.Cluster
	primaryTablet   *vitesst.Tablet
	replicaTablet   *vitesst.Tablet
	vtParams        mysql.ConnParams
	keyspaceName    = "ks"
	shardName       = "0"
	cell            = "zone1"
	sqlSchema       = `
	create table t1(
		id bigint,
		value varchar(16),
		primary key(id)
	) Engine=InnoDB;
`

	vSchema = `
	{
    "sharded": true,
    "vindexes": {
      "hash": {
        "type": "hash"
      }
    },
    "tables": {
      "t1": {
        "column_vindexes": [
          {
            "column": "id",
            "name": "hash"
          }
        ]
      }
    }
	}`
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitCode := func() int {
		ctx := context.Background()

		cluster, err := vitesst.NewCluster(
			vitesst.WithCells(cell),
			// Set extra tablet args for lock timeout
			vitesst.WithVTTabletArgs(
				"--lock-tables-timeout", "5s",
				"--enable-replication-reporter",
				"--heartbeat-interval", "250ms",
				"--heartbeat-on-demand-duration", onDemandHeartbeatDuration.String(),
			),
			vitesst.WithVTOrc(),
			vitesst.WithKeyspace(keyspaceName).
				WithShardNames(shardName).
				WithReplicas(1).
				WithSchema(sqlSchema).
				WithVSchema(vSchema),
		)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}

		cleanup, err := cluster.Start(ctx)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		defer func() {
			if err := cleanup(ctx); err != nil {
				fmt.Fprintln(os.Stderr, "cluster teardown:", err)
			}
		}()

		clusterInstance = cluster

		// Collect the tablets of the single shard.
		shard := cluster.Keyspace(keyspaceName).Shard(shardName)
		primaryTablet = shard.Primary()
		replicaTablet = shard.Replicas()[0]

		vtParams = cluster.VTParams(ctx, "")

		return m.Run()
	}()
	os.Exit(exitCode)
}
