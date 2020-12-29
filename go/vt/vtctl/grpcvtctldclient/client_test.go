/*
Copyright 2020 The Vitess Authors.

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

package grpcvtctldclient_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/nettest"
	"google.golang.org/grpc"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtctl/grpcvtctldserver"
	"vitess.io/vitess/go/vt/vtctl/vtctldclient"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vtctldata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtctlservicepb "vitess.io/vitess/go/vt/proto/vtctlservice"
)

// annoyingly, this is duplicated with theu tests in package grpcvtctldserver.
// fine for now, I suppose.
func addKeyspace(ctx context.Context, t *testing.T, ts *topo.Server, ks *vtctldatapb.Keyspace) {
	in := *ks.Keyspace // take a copy to avoid the XXX_ fields changing

	err := ts.CreateKeyspace(ctx, ks.Name, &in)
	require.NoError(t, err)
}

func withTestServer(
	t *testing.T,
	server vtctlservicepb.VtctldServer,
	test func(t *testing.T, client vtctldclient.VtctldClient),
) {
	lis, err := nettest.NewLocalListener("tcp")
	require.NoError(t, err, "cannot create nettest listener")

	defer lis.Close()

	s := grpc.NewServer()
	vtctlservicepb.RegisterVtctldServer(s, server)

	go s.Serve(lis)
	defer s.Stop()

	client, err := vtctldclient.New("grpc", lis.Addr().String())
	require.NoError(t, err, "cannot create vtctld client")

	test(t, client)
}

func TestFindAllShardsInKeyspace(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell1")
	vtctld := grpcvtctldserver.NewVtctldServer(ts)

	withTestServer(t, vtctld, func(t *testing.T, client vtctldclient.VtctldClient) {
		ks := &vtctldatapb.Keyspace{
			Name:     "testkeyspace",
			Keyspace: &topodatapb.Keyspace{},
		}
		addKeyspace(ctx, t, ts, ks)

		si1, err := ts.GetOrCreateShard(ctx, ks.Name, "-80")
		require.NoError(t, err)
		si2, err := ts.GetOrCreateShard(ctx, ks.Name, "80-")
		require.NoError(t, err)

		resp, err := client.FindAllShardsInKeyspace(ctx, &vtctldatapb.FindAllShardsInKeyspaceRequest{Keyspace: ks.Name})
		assert.NoError(t, err)
		assert.NotNil(t, resp)

		expected := map[string]*vtctldatapb.Shard{
			"-80": {
				Keyspace: ks.Name,
				Name:     "-80",
				Shard:    si1.Shard,
			},
			"80-": {
				Keyspace: ks.Name,
				Name:     "80-",
				Shard:    si2.Shard,
			},
		}

		assert.Equal(t, expected, resp.Shards)

		client.Close()
		_, err = client.FindAllShardsInKeyspace(ctx, &vtctldatapb.FindAllShardsInKeyspaceRequest{Keyspace: ks.Name})
		assert.Error(t, err)
	})
}

func TestGetKeyspace(t *testing.T) {
	ctx := context.Background()

	ts := memorytopo.NewServer("cell1")
	vtctld := grpcvtctldserver.NewVtctldServer(ts)

	withTestServer(t, vtctld, func(t *testing.T, client vtctldclient.VtctldClient) {
		expected := &vtctldatapb.GetKeyspaceResponse{
			Keyspace: &vtctldata.Keyspace{
				Name: "testkeyspace",
				Keyspace: &topodatapb.Keyspace{
					ShardingColumnName: "col1",
				},
			},
		}
		addKeyspace(ctx, t, ts, expected.Keyspace)

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

	withTestServer(t, vtctld, func(t *testing.T, client vtctldclient.VtctldClient) {
		resp, err := client.GetKeyspaces(ctx, &vtctldatapb.GetKeyspacesRequest{})
		assert.NoError(t, err)
		assert.Empty(t, resp.Keyspaces)

		expected := &vtctldatapb.Keyspace{
			Name:     "testkeyspace",
			Keyspace: &topodatapb.Keyspace{},
		}
		addKeyspace(ctx, t, ts, expected)

		resp, err = client.GetKeyspaces(ctx, &vtctldatapb.GetKeyspacesRequest{})
		assert.NoError(t, err)
		assert.Equal(t, []*vtctldatapb.Keyspace{expected}, resp.Keyspaces)

		client.Close()
		_, err = client.GetKeyspaces(ctx, &vtctldatapb.GetKeyspacesRequest{})
		assert.Error(t, err)
	})
}
