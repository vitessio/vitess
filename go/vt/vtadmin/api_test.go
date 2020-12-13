package vtadmin

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"vitess.io/vitess/go/vt/vtadmin/cluster"
	"vitess.io/vitess/go/vt/vtadmin/cluster/discovery/fakediscovery"
	"vitess.io/vitess/go/vt/vtadmin/grpcserver"
	"vitess.io/vitess/go/vt/vtadmin/http"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

func TestGetGates(t *testing.T) {
	fakedisco1 := fakediscovery.New()
	cluster1 := &cluster.Cluster{
		ID:        "c1",
		Name:      "cluster1",
		Discovery: fakedisco1,
	}
	cluster1Gates := []*vtadminpb.VTGate{
		{
			Hostname: "cluster1-gate1",
		},
		{
			Hostname: "cluster1-gate2",
		},
		{
			Hostname: "cluster1-gate3",
		},
	}

	fakedisco1.AddTaggedGates(nil, cluster1Gates...)

	fakedisco2 := fakediscovery.New()
	cluster2 := &cluster.Cluster{
		ID:        "c2",
		Name:      "cluster2",
		Discovery: fakedisco2,
	}
	cluster2Gates := []*vtadminpb.VTGate{
		{
			Hostname: "cluster2-gate1",
		},
	}

	fakedisco2.AddTaggedGates(nil, cluster2Gates...)

	api := NewAPI([]*cluster.Cluster{cluster1, cluster2}, grpcserver.Options{}, http.Options{})

	resp, err := api.GetGates(context.Background(), &vtadminpb.GetGatesRequest{})
	assert.NoError(t, err)
	assert.ElementsMatch(t, append(cluster1Gates, cluster2Gates...), resp.Gates)

	resp, err = api.GetGates(context.Background(), &vtadminpb.GetGatesRequest{ClusterIds: []string{cluster1.ID}})
	assert.NoError(t, err)
	assert.ElementsMatch(t, cluster1Gates, resp.Gates)

	fakedisco1.SetGatesError(true)

	resp, err = api.GetGates(context.Background(), &vtadminpb.GetGatesRequest{})
	assert.Error(t, err)
	assert.Nil(t, resp)
}
