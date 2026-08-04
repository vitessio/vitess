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

package wrangler

import (
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtenv"
)

func TestVitessCluster(t *testing.T) {
	ctx := t.Context()
	ts := memorytopo.NewServer(ctx, "zone1")
	tmc := newTestWranglerTMClient()
	wr := New(vtenv.NewTestEnv(), logutil.NewConsoleLogger(), ts, tmc)
	name, topoType, topoServer, topoRoot := "c1", "x", "y", "z"

	t.Run("Zero clusters to start", func(t *testing.T) {
		clusters, err := ts.GetExternalVitessClusters(ctx)
		require.NoError(t, err)
		require.Empty(t, clusters)
	})
	t.Run("Mount first cluster", func(t *testing.T) {
		err := wr.MountExternalVitessCluster(ctx, name, topoType, topoServer, topoRoot)
		require.NoError(t, err)
		vci, err := ts.GetExternalVitessCluster(ctx, name)
		require.NoError(t, err)
		require.Equal(t, vci.ClusterName, name)
		expectedVc := &topodata.ExternalVitessCluster{
			TopoConfig: &topodata.TopoConfig{
				TopoType: topoType,
				Server:   topoServer,
				Root:     topoRoot,
			},
		}
		utils.MustMatch(t, expectedVc, vci.ExternalVitessCluster)
	})

	t.Run("Mount second cluster", func(t *testing.T) {
		name2 := "c2"
		err := wr.MountExternalVitessCluster(ctx, name2, topoType, topoServer, topoRoot)
		require.NoError(t, err)
	})

	t.Run("List clusters should return c1,c2", func(t *testing.T) {
		clusters, err := ts.GetExternalVitessClusters(ctx)
		require.NoError(t, err)
		require.Len(t, clusters, 2)
		require.Equal(t, []string{"c1", "c2"}, clusters)
	})
	t.Run("Unmount first cluster", func(t *testing.T) {
		err := wr.UnmountExternalVitessCluster(ctx, name)
		require.NoError(t, err)
		vci, err := ts.GetExternalVitessCluster(ctx, name)
		require.NoError(t, err)
		require.Nil(t, vci)
	})
	t.Run("List clusters should return c2", func(t *testing.T) {
		clusters, err := ts.GetExternalVitessClusters(ctx)
		require.NoError(t, err)
		require.Len(t, clusters, 1)
		require.Equal(t, []string{"c2"}, clusters)
	})
}
