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

package grpcvtctldclient

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtctl/grpcvtctldserver"
	"vitess.io/vitess/go/vt/vtctl/grpcvtctldserver/testutil"
	"vitess.io/vitess/go/vt/vtctl/vtctldclient"
	"vitess.io/vitess/go/vt/vtenv"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtctlservicepb "vitess.io/vitess/go/vt/proto/vtctlservice"
)

func TestFindAllShardsInKeyspace(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ts := memorytopo.NewServer(ctx, "cell1")
	defer ts.Close()
	vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
		return grpcvtctldserver.NewVtctldServer(vtenv.NewTestEnv(), ts)
	})

	testutil.WithTestServer(t, vtctld, func(t *testing.T, client vtctldclient.VtctldClient) {
		ks := &vtctldatapb.Keyspace{
			Name:     "testkeyspace",
			Keyspace: &topodatapb.Keyspace{},
		}
		testutil.AddKeyspace(ctx, t, ts, ks)

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

		utils.MustMatch(t, expected, resp.Shards)

		client.Close()
		_, err = client.FindAllShardsInKeyspace(ctx, &vtctldatapb.FindAllShardsInKeyspaceRequest{Keyspace: ks.Name})
		assert.Error(t, err)
	})
}

func TestGetKeyspace(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ts := memorytopo.NewServer(ctx, "cell1")
	defer ts.Close()
	vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
		return grpcvtctldserver.NewVtctldServer(vtenv.NewTestEnv(), ts)
	})

	testutil.WithTestServer(t, vtctld, func(t *testing.T, client vtctldclient.VtctldClient) {
		expected := &vtctldatapb.GetKeyspaceResponse{
			Keyspace: &vtctldatapb.Keyspace{
				Name:     "testkeyspace",
				Keyspace: &topodatapb.Keyspace{},
			},
		}
		testutil.AddKeyspace(ctx, t, ts, expected.Keyspace)

		resp, err := client.GetKeyspace(ctx, &vtctldatapb.GetKeyspaceRequest{Keyspace: expected.Keyspace.Name})
		assert.NoError(t, err)
		utils.MustMatch(t, expected, resp)

		client.Close()
		_, err = client.GetKeyspace(ctx, &vtctldatapb.GetKeyspaceRequest{})
		assert.Error(t, err)
	})
}

func TestGetKeyspaces(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ts := memorytopo.NewServer(ctx, "cell1")
	defer ts.Close()
	vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
		return grpcvtctldserver.NewVtctldServer(vtenv.NewTestEnv(), ts)
	})

	testutil.WithTestServer(t, vtctld, func(t *testing.T, client vtctldclient.VtctldClient) {
		resp, err := client.GetKeyspaces(ctx, &vtctldatapb.GetKeyspacesRequest{})
		assert.NoError(t, err)
		assert.Empty(t, resp.Keyspaces)

		expected := &vtctldatapb.Keyspace{
			Name:     "testkeyspace",
			Keyspace: &topodatapb.Keyspace{},
		}
		testutil.AddKeyspace(ctx, t, ts, expected)

		resp, err = client.GetKeyspaces(ctx, &vtctldatapb.GetKeyspacesRequest{})
		assert.NoError(t, err)
		utils.MustMatch(t, []*vtctldatapb.Keyspace{expected}, resp.Keyspaces)

		client.Close()
		_, err = client.GetKeyspaces(ctx, &vtctldatapb.GetKeyspacesRequest{})
		assert.Error(t, err)
	})
}
