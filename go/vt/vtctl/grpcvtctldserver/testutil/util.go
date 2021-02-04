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

// Package testutil contains utility functions for writing tests for the
// grpcvtctldserver.
package testutil

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/net/nettest"
	"google.golang.org/grpc"

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtctl/vtctldclient"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtctlservicepb "vitess.io/vitess/go/vt/proto/vtctlservice"
)

// WithTestServer creates a gRPC server listening locally with the given RPC
// implementation, then runs the test func with a client created to point at
// that server.
func WithTestServer(
	t *testing.T,
	server vtctlservicepb.VtctldServer,
	test func(t *testing.T, client vtctldclient.VtctldClient),
) {
	lis, err := nettest.NewLocalListener("tcp")
	require.NoError(t, err, "cannot create local listener")

	defer lis.Close()

	s := grpc.NewServer()
	vtctlservicepb.RegisterVtctldServer(s, server)

	go s.Serve(lis)
	defer s.Stop()

	client, err := vtctldclient.New("grpc", lis.Addr().String())
	require.NoError(t, err, "cannot create vtctld client")

	test(t, client)
}

// AddKeyspace adds a keyspace to a topology, failing a test if that keyspace
// could not be added. It shallow copies the proto struct to prevent XXX_ fields
// from changing in the marshalling.
func AddKeyspace(ctx context.Context, t *testing.T, ts *topo.Server, ks *vtctldatapb.Keyspace) {
	in := *ks.Keyspace // take a copy to avoid XXX_ fields changing.

	err := ts.CreateKeyspace(ctx, ks.Name, &in)
	require.NoError(t, err)
}

// AddKeyspaces adds a list of keyspaces to the topology, failing a test if any
// of those keyspaces cannot be added. See AddKeyspace for details.
func AddKeyspaces(ctx context.Context, t *testing.T, ts *topo.Server, keyspaces ...*vtctldatapb.Keyspace) {
	for _, keyspace := range keyspaces {
		AddKeyspace(ctx, t, ts, keyspace)
	}
}

// AddTablet adds a tablet to the topology, failing a test if that tablet record
// could not be created. It shallow copies to prevent XXX_ fields from changing,
// including nested proto message fields.
//
// AddTablet also optionally adds empty keyspace and shard records to the
// topology, if they are set on the tablet record and they cannot be retrieved
// from the topo server without error.
func AddTablet(ctx context.Context, t *testing.T, ts *topo.Server, tablet *topodatapb.Tablet) {
	in := *tablet
	alias := *tablet.Alias
	in.Alias = &alias

	err := ts.CreateTablet(ctx, &in)
	require.NoError(t, err, "CreateTablet(%+v)", &in)

	if tablet.Keyspace != "" {
		if _, err := ts.GetKeyspace(ctx, tablet.Keyspace); err != nil {
			err := ts.CreateKeyspace(ctx, tablet.Keyspace, &topodatapb.Keyspace{})
			require.NoError(t, err, "CreateKeyspace(%s)", tablet.Keyspace)
		}

		if tablet.Shard != "" {
			if _, err := ts.GetShard(ctx, tablet.Keyspace, tablet.Shard); err != nil {
				err := ts.CreateShard(ctx, tablet.Keyspace, tablet.Shard)
				require.NoError(t, err, "CreateShard(%s, %s)", tablet.Keyspace, tablet.Shard)
			}
		}
	}
}

// AddTablets adds a list of tablets to the topology. See AddTablet for more
// details.
func AddTablets(ctx context.Context, t *testing.T, ts *topo.Server, tablets ...*topodatapb.Tablet) {
	for _, tablet := range tablets {
		AddTablet(ctx, t, ts, tablet)
	}
}

// AddShards adds a list of shards to the topology, failing a test if any of the
// shard records could not be created. It also ensures that every shard's
// keyspace exists, or creates an empty keyspace if that shard's keyspace does
// not exist.
func AddShards(ctx context.Context, t *testing.T, ts *topo.Server, shards ...*vtctldatapb.Shard) {
	for _, shard := range shards {
		if shard.Keyspace != "" {
			if _, err := ts.GetKeyspace(ctx, shard.Keyspace); err != nil {
				err := ts.CreateKeyspace(ctx, shard.Keyspace, &topodatapb.Keyspace{})
				require.NoError(t, err, "CreateKeyspace(%s)", shard.Keyspace)
			}
		}

		err := ts.CreateShard(ctx, shard.Keyspace, shard.Name)
		require.NoError(t, err, "CreateShard(%s/%s)", shard.Keyspace, shard.Name)
	}
}

// SetupReplicationGraphs creates a set of ShardReplication objects in the topo,
// failing the test if any of the records could not be created.
func SetupReplicationGraphs(ctx context.Context, t *testing.T, ts *topo.Server, replicationGraphs ...*topo.ShardReplicationInfo) {
	for _, graph := range replicationGraphs {
		err := ts.UpdateShardReplicationFields(ctx, graph.Cell(), graph.Keyspace(), graph.Shard(), func(sr *topodatapb.ShardReplication) error {
			sr.Nodes = graph.Nodes
			return nil
		})
		require.NoError(t, err, "could not save replication graph for %s/%s in cell %v", graph.Keyspace(), graph.Shard(), graph.Cell())
	}
}

// UpdateSrvKeyspaces updates a set of SrvKeyspace records, grouped by cell and
// then by keyspace. It fails the test if any records cannot be updated.
func UpdateSrvKeyspaces(ctx context.Context, t *testing.T, ts *topo.Server, srvkeyspacesByCellByKeyspace map[string]map[string]*topodatapb.SrvKeyspace) {
	for cell, srvKeyspacesByKeyspace := range srvkeyspacesByCellByKeyspace {
		for keyspace, srvKeyspace := range srvKeyspacesByKeyspace {
			err := ts.UpdateSrvKeyspace(ctx, cell, keyspace, srvKeyspace)
			require.NoError(t, err, "UpdateSrvKeyspace(%v, %v, %v)", cell, keyspace, srvKeyspace)
		}
	}
}
