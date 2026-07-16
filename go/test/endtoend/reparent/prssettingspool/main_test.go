/*
Copyright 2023 The Vitess Authors.

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

package misc

import (
	"context"
	_ "embed"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/vitesst"
)

var (
	keyspaceName = "ks"

	//go:embed schema.sql
	schemaSQL string
)

func setup(t *testing.T) (*vitesst.Cluster, mysql.ConnParams) {
	t.Helper()
	ctx := t.Context()

	cluster, err := vitesst.NewCluster(
		vitesst.WithKeyspace(keyspaceName).
			WithReplicas(2).
			WithSchema(schemaSQL),
		vitesst.WithVTGateArgs("--planner-version", "gen4"),
	)
	require.NoError(t, err)

	cleanup, err := cluster.Start(ctx)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), time.Minute)
		defer cancel()
		if t.Failed() {
			cluster.DumpDiagnostics(cleanupCtx, t.Logf)
		}
		if err := cleanup(cleanupCtx); err != nil {
			t.Logf("cluster teardown: %v", err)
		}
	})
	require.NoError(t, err)

	return cluster, cluster.VTParams(ctx, "")
}

// prs runs a PlannedReparentShard to make tab the new primary of the shard.
func prs(ctx context.Context, cluster *vitesst.Cluster, shard *vitesst.Shard, tab *vitesst.Tablet) (string, error) {
	return cluster.Vtctld().ExecuteCommandWithOutput(ctx,
		"PlannedReparentShard", shard.Ref(), "--new-primary", tab.Alias())
}

func TestSettingsPoolWithTXAndPRS(t *testing.T) {
	ctx := t.Context()
	clusterInstance, vtParams := setup(t)
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// set a system settings that will trigger reserved connection usage.
	vitesst.Exec(t, conn, "set default_week_format = 5")

	// have transaction on the session
	vitesst.Exec(t, conn, "begin")
	vitesst.Exec(t, conn, "select id1, id2 from t1")
	vitesst.Exec(t, conn, "commit")

	shard := clusterInstance.Keyspace(keyspaceName).Shards()[0]
	primary := shard.Primary()
	replica := shard.Replicas()[0]

	// prs should happen without any error.
	text, err := prs(ctx, clusterInstance, shard, replica)
	require.NoError(t, err, text)
	require.NoError(t, primary.WaitForTabletStatus(ctx, 1*time.Minute, "SERVING"))

	defer func() {
		// reset state
		text, err = prs(ctx, clusterInstance, shard, primary)
		require.NoError(t, err, text)
		require.NoError(t, replica.WaitForTabletStatus(ctx, 1*time.Minute, "SERVING"))
	}()

	// no error should occur and it should go to the right tablet.
	vitesst.Exec(t, conn, "select id1, id2 from t1")
}

func TestSettingsPoolWithoutTXAndPRS(t *testing.T) {
	ctx := t.Context()
	clusterInstance, vtParams := setup(t)
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// set a system settings that will trigger reserved connection usage.
	vitesst.Exec(t, conn, "set default_week_format = 5")

	// execute non-tx query
	vitesst.Exec(t, conn, "select id1, id2 from t1")

	shard := clusterInstance.Keyspace(keyspaceName).Shards()[0]
	primary := shard.Primary()
	replica := shard.Replicas()[0]

	// prs should happen without any error.
	text, err := prs(ctx, clusterInstance, shard, replica)
	require.NoError(t, err, text)
	require.NoError(t, primary.WaitForTabletStatus(ctx, 1*time.Minute, "SERVING"))
	defer func() {
		// reset state
		text, err = prs(ctx, clusterInstance, shard, primary)
		require.NoError(t, err, text)
		require.NoError(t, replica.WaitForTabletStatus(ctx, 1*time.Minute, "SERVING"))
	}()

	// no error should occur and it should go to the right tablet.
	vitesst.Exec(t, conn, "select id1, id2 from t1")
}
