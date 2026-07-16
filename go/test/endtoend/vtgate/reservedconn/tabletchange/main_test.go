/*
Copyright 2021 The Vitess Authors.

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

package tabletchange

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/vitesst"
)

var (
	clusterInstance *vitesst.Cluster
	vtParams        mysql.ConnParams
	keyspaceName    = "ks"
	sqlSchema       = `create table test(id bigint primary key)Engine=InnoDB;`

	vSchema = `
		{
			"sharded":true,
			"vindexes": {
				"hash_index": {
					"type": "hash"
				}
			},
			"tables": {
				"test":{
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

func setup(t *testing.T) {
	t.Helper()
	ctx := t.Context()

	cluster, err := vitesst.NewCluster(
		vitesst.WithKeyspace(keyspaceName).
			WithShardNames("-80", "80-").
			WithReplicas(1).
			WithRDOnly(1).
			WithSchema(sqlSchema).
			WithVSchema(vSchema),
		vitesst.WithVTTabletArgs("--queryserver-config-transaction-timeout", "5s"),
		vitesst.WithVTGateArgs("--lock-heartbeat-time", "2s"),
	)
	require.NoError(t, err)

	cleanup, err := cluster.Start(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
		defer cancel()
		if t.Failed() {
			cluster.DumpDiagnostics(cleanupCtx, t.Logf)
		}
		if err := cleanup(cleanupCtx); err != nil {
			t.Logf("cluster teardown: %v", err)
		}
	})

	clusterInstance = cluster
	vtParams = cluster.VTParams(ctx, "")
}

func TestTabletChange(t *testing.T) {
	setup(t)
	conn, err := mysql.Connect(t.Context(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	vitesst.Exec(t, conn, "use @primary")
	vitesst.Exec(t, conn, "set sql_mode = ''")

	// this will create reserved connection on primary on -80 and 80- shards.
	vitesst.Exec(t, conn, "select * from test")

	// Change Primary
	err = clusterInstance.Vtctld().ExecuteCommand(t.Context(), "PlannedReparentShard", fmt.Sprintf("%s/%s", keyspaceName, "-80"))
	require.NoError(t, err)

	// this should pass as there is a new primary tablet and is serving.
	_, err = vitesst.ExecAllowError(t, conn, "select * from test")
	assert.NoError(t, err)
}

func TestTabletChangeStreaming(t *testing.T) {
	setup(t)
	conn, err := mysql.Connect(t.Context(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	vitesst.Exec(t, conn, "set workload = olap")
	vitesst.Exec(t, conn, "use @primary")
	vitesst.Exec(t, conn, "set sql_mode = ''")

	// this will create reserved connection on primary on -80 and 80- shards.
	vitesst.Exec(t, conn, "select * from test")

	// Change Primary
	err = clusterInstance.Vtctld().ExecuteCommand(t.Context(), "PlannedReparentShard", fmt.Sprintf("%s/%s", keyspaceName, "-80"))
	require.NoError(t, err)

	// this should pass as there is a new primary tablet and is serving.
	_, err = vitesst.ExecAllowError(t, conn, "select * from test")
	assert.NoError(t, err)
}
