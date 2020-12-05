package grpcvtctldclient_test

import (
	"context"
	"io"
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

		expected := &vtctldatapb.Keyspace{
			Name: "testkeyspace",
			Keyspace: &topodatapb.Keyspace{
				ShardingColumnName: "col1",
			},
		}
		in := *expected.Keyspace

		ts.CreateKeyspace(ctx, expected.Name, &in)

		resp, err := client.GetKeyspace(ctx, &vtctldatapb.GetKeyspaceRequest{Keyspace: expected.Name})
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

		err = ts.CreateKeyspace(ctx, "testkeyspace", &topodatapb.Keyspace{})
		require.NoError(t, err)

		resp, err = client.GetKeyspaces(ctx, &vtctldatapb.GetKeyspacesRequest{})
		assert.NoError(t, err)
		assert.Equal(t, []string{"testkeyspace"}, resp.Keyspaces)

		client.Close()
		_, err = client.GetKeyspaces(ctx, &vtctldatapb.GetKeyspacesRequest{})
		assert.Error(t, err)
	})
}

func consumeShowAllKeyspaces(
	t *testing.T,
	stream vtctlservicepb.Vtctld_ShowAllKeyspacesClient,
	f func(t *testing.T, keyspace *vtctldatapb.Keyspace, err error),
) {
	for {
		ks, err := stream.Recv()
		if err == io.EOF {
			return
		}

		f(t, ks, err)
	}
}

func TestShowAllKeyspaces(t *testing.T) {
	ctx := context.Background()

	ts := memorytopo.NewServer("cell1")
	vtctld := grpcvtctldserver.NewVtctldServer(ts)

	withTestServer(t, vtctld, func(t *testing.T, addr string) {
		client, err := vtctldclient.New("grpc", addr)
		require.NoError(t, err)

		var actual []*vtctldatapb.Keyspace

		stream, err := client.ShowAllKeyspaces(ctx, &vtctldatapb.ShowAllKeyspacesRequest{})
		assert.NoError(t, err)
		consumeShowAllKeyspaces(t, stream, func(t *testing.T, keyspace *vtctldatapb.Keyspace, err error) {
			assert.NoError(t, err)
			actual = append(actual, keyspace)
		})
		assert.Empty(t, actual)

		expected := []*vtctldatapb.Keyspace{
			{
				Name: "ks1",
				Keyspace: &topodatapb.Keyspace{
					ShardingColumnName: "col1",
				},
			},
			{
				Name: "ks2",
				Keyspace: &topodatapb.Keyspace{
					ShardingColumnName: "col2",
				},
			},
		}

		for _, ks := range expected {
			keyspace := *ks.Keyspace // value copy
			err := ts.CreateKeyspace(ctx, ks.Name, &keyspace)
			require.NoError(t, err)
		}

		// reset our tracking slice
		actual = nil
		stream, err = client.ShowAllKeyspaces(ctx, &vtctldatapb.ShowAllKeyspacesRequest{})
		assert.NoError(t, err)
		consumeShowAllKeyspaces(t, stream, func(t *testing.T, keyspace *vtctldatapb.Keyspace, err error) {
			assert.NoError(t, err)
			actual = append(actual, keyspace)
		})
		assert.Equal(t, expected, actual)

		client.Close()
		_, err = client.ShowAllKeyspaces(ctx, &vtctldatapb.ShowAllKeyspacesRequest{})
		assert.Error(t, err)
	})
}
