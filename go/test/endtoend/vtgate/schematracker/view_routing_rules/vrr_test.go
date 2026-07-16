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

package viewroutingrules

import (
	"context"
	_ "embed"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vitesst"
)

var (
	sourceKs = "source_ks"
	targetKs = "target_ks"

	//go:embed source_schema.sql
	sourceSchema string

	//go:embed target_schema.sql
	targetSchema string

	//go:embed source_vschema.json
	sourceVSchema string

	//go:embed target_vschema.json
	targetVSchema string
)

func setup(t *testing.T) (*vitesst.Cluster, mysql.ConnParams) {
	t.Helper()
	ctx := t.Context()

	cluster, err := vitesst.NewCluster(t,
		vitesst.WithVTGateArgs("--enable-views"),
		vitesst.WithVTTabletArgs("--queryserver-enable-views"),
		vitesst.WithKeyspace(sourceKs).
			WithReplicas(1).
			WithSchema(sourceSchema).
			WithVSchema(sourceVSchema),
		vitesst.WithKeyspace(targetKs).
			WithShardNames("-80", "80-").
			WithReplicas(1).
			WithSchema(targetSchema).
			WithVSchema(targetVSchema),
	)
	require.NoError(t, err)

	cleanup, err := cluster.Start(t, ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), time.Minute)
		defer cancel()
		if err := cleanup(cleanupCtx); err != nil {
			t.Logf("cluster teardown: %v", err)
		}
	})

	return cluster, cluster.VTParams(ctx, "")
}

func TestViewRoutingRules(t *testing.T) {
	ctx := t.Context()
	cluster, vtParams := setup(t)
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// Wait for views to be tracked by the schema tracker.
	viewExists := func(t *testing.T, ksMap map[string]any) bool {
		views, ok := ksMap["views"]
		if !ok {
			return false
		}
		viewsMap := views.(map[string]any)
		_, ok = viewsMap["view1"]
		return ok
	}
	vitesst.WaitForVschemaCondition(t, cluster.VTGate(), sourceKs, viewExists, "source view1 not found")
	vitesst.WaitForVschemaCondition(t, cluster.VTGate(), targetKs, viewExists, "target view1 not found")

	// Insert different data in each keyspace so we can distinguish which one is being queried.
	vitesst.Exec(t, conn, "insert into source_ks.t1(id, val) values(1, 'source_data')")
	vitesst.Exec(t, conn, "insert into target_ks.t1(id, val) values(1, 'target_data')")

	vitesst.AssertMatches(t, conn, "select * from source_ks.view1", `[[INT64(1) VARCHAR("source_data")]]`)
	vitesst.AssertMatches(t, conn, "select * from target_ks.view1", `[[INT64(1) VARCHAR("target_data")]]`)

	// Unqualified query should fail with ambiguous table error since view1 exists in both keyspaces.
	_, err = vitesst.ExecAllowError(t, conn, "select * from view1")
	require.Error(t, err)
	require.ErrorContains(t, err, "ambiguous")

	// Apply routing rules to route to target
	routingRules := `{"rules": [
		{"from_table": "view1", "to_tables": ["target_ks.view1"]},
		{"from_table": "source_ks.view1", "to_tables": ["target_ks.view1"]},
		{"from_table": "source_ks.view1@replica", "to_tables": ["target_ks.view1"]}
	]}`
	err = cluster.Vtctld().ExecuteCommand(ctx, "ApplyRoutingRules", "--rules", routingRules)
	require.NoError(t, err)
	defer func() {
		err := cluster.Vtctld().ExecuteCommand(ctx, "ApplyRoutingRules", "--rules", "{}")
		require.NoError(t, err)
	}()

	// After routing rules, all queries should return target data.

	vitesst.AssertMatches(t, conn, "select * from view1", `[[INT64(1) VARCHAR("target_data")]]`)
	vitesst.AssertMatches(t, conn, "select * from source_ks.view1", `[[INT64(1) VARCHAR("target_data")]]`)

	vitesst.Exec(t, conn, "use @replica")
	vitesst.AssertMatches(t, conn, "select * from source_ks.view1", `[[INT64(1) VARCHAR("target_data")]]`)

	vitesst.Exec(t, conn, "use @primary")
	vitesst.AssertMatches(t, conn, "select * from target_ks.view1", `[[INT64(1) VARCHAR("target_data")]]`)
}
