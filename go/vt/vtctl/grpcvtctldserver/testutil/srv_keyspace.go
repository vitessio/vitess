package testutil

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/topo"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// SrvKeyspace groups a topodatapb.SrvKeyspace together with a keyspace and
// cell.
type SrvKeyspace struct {
	Keyspace    string
	Cell        string
	SrvKeyspace *topodatapb.SrvKeyspace
}

// AddSrvKeyspaces adds one or more SrvKeyspace objects to the topology. It
// fails the calling test if any of the objects fail to update.
func AddSrvKeyspaces(t *testing.T, ts *topo.Server, srvKeyspaces ...*SrvKeyspace) {
	t.Helper()

	ctx := context.Background()

	for _, sk := range srvKeyspaces {
		err := ts.UpdateSrvKeyspace(ctx, sk.Cell, sk.Keyspace, sk.SrvKeyspace)
		require.NoError(t, err, "UpdateSrvKeyspace(cell = %v, keyspace = %v, srv_keyspace = %v", sk.Cell, sk.Keyspace, sk.SrvKeyspace)
	}
}
