/*
Copyright 2025 The Vitess Authors.

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

package tabletbalancer

import (
	"context"
	_ "embed"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vitesst"
)

var (
	cell1        = "zone1"
	cell2        = "zone2"
	keyspaceName = "ks"

	//go:embed schema.sql
	schemaSQL string

	vSchema = `{
		"sharded": false,
		"tables": {
			"balancer_test": {}
		}
	}`
)

func setupCluster(t *testing.T) *vitesst.Cluster {
	t.Helper()

	ctx := t.Context()
	cluster, err := vitesst.NewCluster(
		vitesst.WithCells(cell1, cell2),
		vitesst.WithKeyspace(keyspaceName).
			WithReplicas(5).
			WithSchema(schemaSQL).
			WithVSchema(vSchema),
	)
	require.NoError(t, err)
	cleanup, err := cluster.Start(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		require.NoError(t, cleanup(cleanupCtx))
	})

	allCells := fmt.Sprintf("%s,%s", cell1, cell2)
	require.NoError(t, cluster.Vtctld().ExecuteCommand(ctx, "AddCellsAlias",
		"--cells", allCells,
		"combined_cells"))

	return cluster
}
