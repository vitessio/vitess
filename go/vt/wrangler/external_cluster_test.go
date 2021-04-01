package wrangler

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/memorytopo"
)

func TestVitessCluster(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("zone1")
	tmc := newTestWranglerTMClient()
	wr := New(logutil.NewConsoleLogger(), ts, tmc)
	name, topoType, topoServer, topoRoot := "c1", "x", "y", "z"

	t.Run("Zero clusters to start", func(t *testing.T) {
		clusters, err := ts.GetExternalVitessClusters(ctx)
		require.NoError(t, err)
		require.Equal(t, 0, len(clusters))
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
		require.Equal(t, expectedVc, vci.ExternalVitessCluster)
	})

	t.Run("Mount second cluster", func(t *testing.T) {
		name2 := "c2"
		err := wr.MountExternalVitessCluster(ctx, name2, topoType, topoServer, topoRoot)
		require.NoError(t, err)
	})

	t.Run("List clusters should return c1,c2", func(t *testing.T) {
		clusters, err := ts.GetExternalVitessClusters(ctx)
		require.NoError(t, err)
		require.Equal(t, 2, len(clusters))
		require.EqualValues(t, []string{"c1", "c2"}, clusters)
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
		require.Equal(t, 1, len(clusters))
		require.EqualValues(t, []string{"c2"}, clusters)
	})
}
