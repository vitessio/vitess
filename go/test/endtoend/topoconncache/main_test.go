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
*/

package topoconncache

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vitesst"
)

var (
	clusterInstance *vitesst.Cluster
	cell1           = "zone1"
	cell2           = "zone2"
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
		"--vreplication-retry-delay", "1s",
		"--degraded-threshold", "5s",
		"--lock-tables-timeout", "5s",
		"--enable-replication-reporter",
		"--serving-state-grace-period", "1s",
		"--binlog-player-protocol", "grpc",
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
)

/*
This end-to-end test validates the cache fix in topo/server.go inside ConnForCell.

The issue was, if we delete and add back a cell with same name but at different path, the map of cells
in server.go returned the connection object of the previous cell instead of newly-created one
with the same name.
Topology: We create a keyspace with two shards , having 3 tablets each. Primaries belong
to 'zone1' and replicas/rdonly belongs to cell2.
*/
func setupCluster(t *testing.T) {
	t.Helper()
	ctx := t.Context()

	cluster, err := vitesst.NewCluster(t,
		vitesst.WithCells(cell1, cell2),
		vitesst.WithoutVTGate(),
		vitesst.WithVTOrc(),
		vitesst.WithVTTabletArgs(commonTabletArg...),
		vitesst.WithKeyspace(keyspaceName).
			WithShardNames("-80", "80-").
			WithReplicas(1).
			WithRDOnly(1).
			WithDurabilityPolicy("semi_sync").
			WithSchema(fmt.Sprintf(sqlSchema, tableName)).
			WithVSchema(fmt.Sprintf(vSchema, tableName)).
			WithTabletSpec(func(spec *vitesst.TabletSpec) {
				if spec.Type == "primary" {
					spec.Cell = cell1
				} else {
					spec.Cell = cell2
				}
			}),
	)
	require.NoError(t, err)
	cleanup, err := cluster.Start(t, ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
		defer cancel()
		if cleanupErr := cleanup(cleanupCtx); cleanupErr != nil {
			t.Logf("cluster teardown: %v", cleanupErr)
		}
	})

	clusterInstance = cluster
	shard1 := cluster.Keyspace(keyspaceName).Shard("-80")

	// run a health check on source replica so it responds to discovery
	// (for binlog players) and on the source rdonlys (for workers)
	for _, tablet := range []*vitesst.Tablet{shard1.Replicas()[0], shard1.RDOnly()[0]} {
		require.NoError(t, cluster.Vtctld().ExecuteCommand(ctx, "RunHealthCheck", tablet.Alias()))
	}
	require.NoError(t, cluster.Vtctld().ExecuteCommand(ctx, "RebuildKeyspaceGraph", keyspaceName))
}

func testURL(t *testing.T, path string, testCaseName string) {
	statusCode := getStatusForURL(t, path)
	if got, want := statusCode, 200; got != want {
		assert.Equalf(t, want, got, "\npath: %v\nstatus code: %v \nwant %v for %s", path, got, want, testCaseName)
	}
}

// getStatusForURL returns the status code for the vtctld HTTP path
func getStatusForURL(t *testing.T, path string) int {
	status, _, err := clusterInstance.Vtctld().MakeAPICall(t.Context(), path)
	if err != nil {
		return 0
	}
	return status
}
