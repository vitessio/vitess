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

package api

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/vitesst"
)

const (
	keyspaceName = "ks"
	shardName    = "0"
	cell1        = "zone1"
	cell2        = "zone2"
)

var clusterInstance *vitesst.Cluster

func setupCluster(t *testing.T) *vitesst.Cluster {
	t.Helper()
	ctx := t.Context()
	lastUsedValue = 0
	dynamicConfig = make(map[string]any)

	cluster, err := vitesst.NewCluster(
		vitesst.WithCells(cell1, cell2),
		vitesst.WithoutVTGate(),
		vitesst.WithVTOrc(),
		vitesst.WithKeyspace(keyspaceName).
			WithShardNames(shardName).
			WithReplicas(1).
			WithRDOnly(1).
			WithoutPrimaryElection().
			WithTabletSpec(func(spec *vitesst.TabletSpec) {
				spec.Cell = cell1
			}),
	)
	require.NoError(t, err)

	cleanup, err := cluster.Start(ctx)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
		defer cancel()
		if t.Failed() {
			cluster.DumpDiagnostics(cleanupCtx, t.Logf)
		}
		if cleanupErr := cleanup(cleanupCtx); cleanupErr != nil {
			t.Logf("cluster teardown: %v", cleanupErr)
		}
	})
	require.NoError(t, err)

	for _, tablet := range cluster.Tablets() {
		_, err := tablet.QueryTabletWithDB(ctx, "SET GLOBAL super_read_only = OFF", "")
		require.NoError(t, err)
	}
	require.NoError(t, cluster.WaitForHealthyShard(ctx, keyspaceName, shardName, 60*time.Second))

	return cluster
}
