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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vitesst"
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

func setup(t *testing.T) {
	t.Helper()
	ctx := t.Context()

	cluster, err := vitesst.NewCluster(t,
		vitesst.WithCells(cell),
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
	require.NoError(t, err)

	cleanup, err := cluster.Start(t, ctx)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
		defer cancel()
		if cleanupErr := cleanup(cleanupCtx); cleanupErr != nil {
			t.Logf("cluster teardown: %v", cleanupErr)
		}
	})
	require.NoError(t, err)

	clusterInstance = cluster
	shard := cluster.Keyspace(keyspaceName).Shard(shardName)
	primaryTablet = shard.Primary()
	replicaTablet = shard.Replicas()[0]
	vtParams = cluster.VTParams(ctx, "")
}
