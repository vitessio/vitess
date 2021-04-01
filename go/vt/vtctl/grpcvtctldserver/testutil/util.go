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
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/net/nettest"
	"google.golang.org/grpc"

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
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

// WithTestServers creates N gRPC servers listening locally with the given RPC
// implementations, and then runs the test func with N clients created, where
// clients[i] points at servers[i].
func WithTestServers(
	t *testing.T,
	test func(t *testing.T, clients ...vtctldclient.VtctldClient),
	servers ...vtctlservicepb.VtctldServer,
) {
	// Declare our recursive helper function so it can refer to itself.
	var withTestServers func(t *testing.T, servers ...vtctlservicepb.VtctldServer)

	// Preallocate a slice of clients we're eventually going to call the test
	// function with.
	clients := make([]vtctldclient.VtctldClient, 0, len(servers))

	withTestServers = func(t *testing.T, servers ...vtctlservicepb.VtctldServer) {
		if len(servers) == 0 {
			// We've started up all the test servers and accumulated clients for
			// each of them (or there were no test servers to start, and we've
			// accumulated no clients), so finally we run the test and stop
			// recursing.
			test(t, clients...)

			return
		}

		// Start up a test server for the head of our server slice, accumulate
		// the resulting client, and recurse on the tail of our server slice.
		WithTestServer(t, servers[0], func(t *testing.T, client vtctldclient.VtctldClient) {
			clients = append(clients, client)
			withTestServers(t, servers[1:]...)
		})
	}

	withTestServers(t, servers...)
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

// AddTabletOptions is a container for different behaviors tests need from
// AddTablet.
type AddTabletOptions struct {
	// AlsoSetShardMaster is an option to control additional setup to take when
	// AddTablet receives a tablet of type MASTER. When set, AddTablet will also
	// update the shard record to make that tablet the primary, and fail the
	// test if the shard record has a serving primary already.
	AlsoSetShardMaster bool
	// ForceSetShardMaster, when combined with AlsoSetShardMaster, will ignore
	// any existing primary in the shard, making the current tablet the serving
	// primary (given it is type MASTER), and log that it has done so.
	ForceSetShardMaster bool
	// SkipShardCreation, when set, makes AddTablet never attempt to create a
	// shard record in the topo under any circumstances.
	SkipShardCreation bool
}

// AddTablet adds a tablet to the topology, failing a test if that tablet record
// could not be created. It shallow copies to prevent XXX_ fields from changing,
// including nested proto message fields.
//
// AddTablet also optionally adds empty keyspace and shard records to the
// topology, if they are set on the tablet record and they cannot be retrieved
// from the topo server without error.
//
// If AddTablet receives a tablet record with a keyspace and shard set, and that
// tablet's type is MASTER, and opts.AlsoSetShardMaster is set, then AddTablet
// will update the shard record to make that tablet the shard master and set the
// shard to serving. If that shard record already has a serving primary, then
// AddTablet will fail the test.
func AddTablet(ctx context.Context, t *testing.T, ts *topo.Server, tablet *topodatapb.Tablet, opts *AddTabletOptions) {
	in := *tablet
	alias := *tablet.Alias
	in.Alias = &alias

	if opts == nil {
		opts = &AddTabletOptions{}
	}

	err := ts.CreateTablet(ctx, &in)
	require.NoError(t, err, "CreateTablet(%+v)", &in)

	if opts.SkipShardCreation {
		return
	}

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

			if tablet.Type == topodatapb.TabletType_MASTER && opts.AlsoSetShardMaster {
				_, err := ts.UpdateShardFields(ctx, tablet.Keyspace, tablet.Shard, func(si *topo.ShardInfo) error {
					if si.IsMasterServing && si.MasterAlias != nil {
						msg := fmt.Sprintf("shard %v/%v already has a serving master (%v)", tablet.Keyspace, tablet.Shard, topoproto.TabletAliasString(si.MasterAlias))

						if !opts.ForceSetShardMaster {
							return errors.New(msg)
						}

						t.Logf("%s; replacing with %v because ForceSetShardMaster = true", msg, topoproto.TabletAliasString(tablet.Alias))
					}

					si.MasterAlias = tablet.Alias
					si.IsMasterServing = true
					si.MasterTermStartTime = tablet.MasterTermStartTime

					return nil
				})
				require.NoError(t, err, "UpdateShardFields(%s, %s) to set %s as serving primary failed", tablet.Keyspace, tablet.Shard, topoproto.TabletAliasString(tablet.Alias))
			}
		}
	}
}

// AddTablets adds a list of tablets to the topology. See AddTablet for more
// details.
func AddTablets(ctx context.Context, t *testing.T, ts *topo.Server, opts *AddTabletOptions, tablets ...*topodatapb.Tablet) {
	for _, tablet := range tablets {
		AddTablet(ctx, t, ts, tablet, opts)
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

		if shard.Shard != nil {
			_, err := ts.UpdateShardFields(ctx, shard.Keyspace, shard.Name, func(si *topo.ShardInfo) error {
				si.Shard = shard.Shard

				return nil
			})
			require.NoError(t, err, "UpdateShardFields(%s/%s, %v)", shard.Keyspace, shard.Name, shard.Shard)
		}
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
