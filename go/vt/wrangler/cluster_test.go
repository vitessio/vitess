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

	err := wr.MountVitessCluster(ctx, name, topoType, topoServer, topoRoot)
	require.NoError(t, err)
	vci, err := ts.GetVitessCluster(ctx, name)
	require.NoError(t, err)
	require.Equal(t, vci.ClusterName, name)
	expectedVc := &topodata.VitessCluster{
		TopoConfig: &topodata.TopoConfig{
			TopoType: topoType,
			Server:   topoServer,
			Root:     topoRoot,
		},
	}
	require.Equal(t, expectedVc, vci.VitessCluster)

	err = wr.UnmountVitessCluster(ctx, name)
	require.NoError(t, err)
	vci, err = ts.GetVitessCluster(ctx, name)
	require.NoError(t, err)
	require.Nil(t, vci)
}
