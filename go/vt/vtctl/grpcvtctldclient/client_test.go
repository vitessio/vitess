package grpcvtctldclient_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/nettest"
	"google.golang.org/grpc"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtctl/grpcvtctldserver"
	"vitess.io/vitess/go/vt/vtctl/vtctldclient"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtctlservicepb "vitess.io/vitess/go/vt/proto/vtctlservice"
)

func withTestServer(
	t *testing.T,
	server vtctlservicepb.VtctldServer,
	test func(t *testing.T, addr string),
) {
	lis, err := nettest.NewLocalListener("tcp")
	require.NoError(t, err, "cannot create nettest listener")
	defer lis.Close()

	s := grpc.NewServer()
	vtctlservicepb.RegisterVtctldServer(s, server)

	go s.Serve(lis)
	defer s.Stop()

	test(t, lis.Addr().String())
}

func TestGetKeyspace(t *testing.T) {
	ctx := context.Background()

	ts := memorytopo.NewServer("cell1")
	vtctld := grpcvtctldserver.NewVtctldServer(ts)

	withTestServer(t, vtctld, func(t *testing.T, addr string) {
		client, err := vtctldclient.New("grpc", addr)
		require.NoError(t, err)

		expected := &vtctldatapb.GetKeyspaceResponse{
			Keyspace: &vtctldatapb.Keyspace{
				Name: "testkeyspace",
				Keyspace: &topodatapb.Keyspace{
					ShardingColumnName: "col1",
				},
			},
		}
		in := *expected.Keyspace.Keyspace

		err = ts.CreateKeyspace(ctx, expected.Keyspace.Name, &in)
		require.NoError(t, err)

		resp, err := client.GetKeyspace(ctx, &vtctldatapb.GetKeyspaceRequest{Keyspace: expected.Keyspace.Name})
		assert.NoError(t, err)
		assert.Equal(t, expected, resp)

		client.Close()
		_, err = client.GetKeyspace(ctx, &vtctldatapb.GetKeyspaceRequest{})
		assert.Error(t, err)
	})
}

func TestGetKeyspaces(t *testing.T) {
	ctx := context.Background()

	ts := memorytopo.NewServer("cell1")
	vtctld := grpcvtctldserver.NewVtctldServer(ts)

	withTestServer(t, vtctld, func(t *testing.T, addr string) {
		client, err := vtctldclient.New("grpc", addr)
		require.NoError(t, err)

		resp, err := client.GetKeyspaces(ctx, &vtctldatapb.GetKeyspacesRequest{})
		assert.NoError(t, err)
		assert.Empty(t, resp.Keyspaces)

		expected := &vtctldatapb.Keyspace{
			Name:     "testkeyspace",
			Keyspace: &topodatapb.Keyspace{},
		}
		in := *expected.Keyspace

		err = ts.CreateKeyspace(ctx, expected.Name, &in)
		require.NoError(t, err)

		resp, err = client.GetKeyspaces(ctx, &vtctldatapb.GetKeyspacesRequest{})
		assert.NoError(t, err)
		assert.Equal(t, []*vtctldatapb.Keyspace{expected}, resp.Keyspaces)

		client.Close()
		_, err = client.GetKeyspaces(ctx, &vtctldatapb.GetKeyspacesRequest{})
		assert.Error(t, err)
	})
}
