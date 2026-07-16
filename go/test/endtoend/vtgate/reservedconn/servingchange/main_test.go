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

package servingchange

import (
	"context"
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

// tabletOfType returns the shard's non-primary tablet currently reporting the
// wanted type ("replica" or "rdonly").
func tabletOfType(t *testing.T, ctx context.Context, shard *vitesst.Shard, wantType string) *vitesst.Tablet {
	candidates := append(shard.Replicas(), shard.RDOnly()...)
	for _, tablet := range candidates {
		vars, err := tablet.GetVars(ctx)
		require.NoError(t, err)
		if typ, ok := vars["TabletType"].(string); ok && typ == wantType {
			return tablet
		}
	}
	require.Failf(t, "no tablet found", "shard %s has no %s tablet", shard.Name, wantType)
	return nil
}

func TestServingChange(t *testing.T) {
	setup(t)
	ctx := t.Context()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	vitesst.Exec(t, conn, "use @rdonly")
	vitesst.Exec(t, conn, "set sql_mode = ''")

	// to see rdonly is available and
	// also this will create reserved connection on rdonly on -80 and 80- shards.
	_, err = vitesst.ExecAllowError(t, conn, "select * from test")
	for err != nil {
		_, err = vitesst.ExecAllowError(t, conn, "select * from test")
	}

	shard := clusterInstance.Keyspace(keyspaceName).Shard("-80")
	rdonlyTablet := tabletOfType(t, ctx, shard, "rdonly")
	replicaTablet := tabletOfType(t, ctx, shard, "replica")

	// changing rdonly tablet to spare (non serving).
	err = clusterInstance.Vtctld().ExecuteCommand(ctx, "ChangeTabletType", rdonlyTablet.Alias(), "replica")
	require.NoError(t, err)

	// this should fail as there is no rdonly present
	_, err = vitesst.ExecAllowError(t, conn, "select * from test")
	require.Error(t, err)

	// changing replica tablet to rdonly to make rdonly available for serving.
	err = clusterInstance.Vtctld().ExecuteCommand(ctx, "ChangeTabletType", replicaTablet.Alias(), "rdonly")
	require.NoError(t, err)

	// to see/make the new rdonly available
	err = clusterInstance.Vtctld().ExecuteCommand(ctx, "PingTablet", replicaTablet.Alias())
	require.NoError(t, err)

	// this should pass now as there is rdonly present
	_, err = vitesst.ExecAllowError(t, conn, "select * from test")
	assert.NoError(t, err)
}

func TestServingChangeStreaming(t *testing.T) {
	setup(t)
	ctx := t.Context()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	vitesst.Exec(t, conn, "set workload = olap")
	vitesst.Exec(t, conn, "use @rdonly")
	vitesst.Exec(t, conn, "set sql_mode = ''")

	// to see rdonly is available and
	// also this will create reserved connection on rdonly on -80 and 80- shards.
	_, err = vitesst.ExecAllowError(t, conn, "select * from test")
	for err != nil {
		_, err = vitesst.ExecAllowError(t, conn, "select * from test")
	}

	shard := clusterInstance.Keyspace(keyspaceName).Shard("-80")
	rdonlyTablet := tabletOfType(t, ctx, shard, "rdonly")
	replicaTablet := tabletOfType(t, ctx, shard, "replica")

	// changing rdonly tablet to spare (non serving).
	err = clusterInstance.Vtctld().ExecuteCommand(ctx, "ChangeTabletType", rdonlyTablet.Alias(), "replica")
	require.NoError(t, err)

	// this should fail as there is no rdonly present
	_, err = vitesst.ExecAllowError(t, conn, "select * from test")
	require.Error(t, err)

	// The mid-stream error must surface to the client via an ERR packet without
	// tearing down the connection — a subsequent query on the same conn must succeed.
	_, err = vitesst.ExecAllowError(t, conn, "select 1")
	require.NoError(t, err, "streaming connection must survive a mid-stream error")

	// changing replica tablet to rdonly to make rdonly available for serving.
	err = clusterInstance.Vtctld().ExecuteCommand(ctx, "ChangeTabletType", replicaTablet.Alias(), "rdonly")
	require.NoError(t, err)

	// to see/make the new rdonly available
	err = clusterInstance.Vtctld().ExecuteCommand(ctx, "PingTablet", replicaTablet.Alias())
	require.NoError(t, err)

	// this should pass now as there is rdonly present
	_, err = vitesst.ExecAllowError(t, conn, "select * from test")
	assert.NoError(t, err)
}
