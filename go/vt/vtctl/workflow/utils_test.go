package workflow

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/topo/memorytopo"
)

func TestUpdateKeyspaceRoutingRule(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ts := memorytopo.NewServer(ctx, "zone1")
	defer ts.Close()
	routes := make(map[string]string)
	routes["from"] = "to"
	err := updateKeyspaceRoutingRule(ctx, ts, "ks", routes)
	require.NoError(t, err)
}
