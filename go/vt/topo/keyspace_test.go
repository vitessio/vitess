/*
Copyright 2025 The Vitess Authors.

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

package topo_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
)

func TestDeleteOrphanedKeyspaceFiles(t *testing.T) {
	cell := "zone-1"
	cell2 := "zone-2"
	keyspace := "ks"
	keyspace2 := "ks2"
	ctx := context.Background()
	tests := []struct {
		name   string
		setup  func(t *testing.T, ts *topo.Server, mtf *memorytopo.Factory)
		verify func(t *testing.T, ts *topo.Server, mtf *memorytopo.Factory)
	}{
		{
			name: "No orphaned files to delete",
			setup: func(t *testing.T, ts *topo.Server, mtf *memorytopo.Factory) {
				// We don't create anything that needs to be cleared.
			},
			verify: func(t *testing.T, ts *topo.Server, mtf *memorytopo.Factory) {
				// We check that we only get one ListDir call.
				require.EqualValues(t, 1, mtf.GetCallStats().Counts()["ListDir"])
			},
		}, {
			name: "Single orphaned file to delete",
			setup: func(t *testing.T, ts *topo.Server, mtf *memorytopo.Factory) {
				err := ts.UpdateShardReplicationFields(ctx, cell, keyspace, "0", func(sr *topodatapb.ShardReplication) error {
					sr.Nodes = append(sr.Nodes, &topodatapb.ShardReplication_Node{TabletAlias: &topodatapb.TabletAlias{Cell: cell, Uid: 0}})
					return nil
				})
				require.NoError(t, err)
				// Verify that getting srvKeyspaceNames now gives us this keyspace.
				keyspaces, err := ts.GetSrvKeyspaceNames(ctx, cell)
				require.NoError(t, err)
				require.EqualValues(t, 1, len(keyspaces))
				require.EqualValues(t, keyspace, keyspaces[0])
				// Also verify that we can't get the SrvKeyspace.
				_, err = ts.GetSrvKeyspace(ctx, cell, keyspace)
				require.Error(t, err)
				require.True(t, topo.IsErrType(err, topo.NoNode))
				mtf.GetCallStats().ResetAll()
			},
			verify: func(t *testing.T, ts *topo.Server, mtf *memorytopo.Factory) {
				// We check that we only get 3 ListDir calls -
				// - keyspaces/ks
				// - keyspaces/ks/shards
				// - keyspaces/ks/shards/0
				require.EqualValues(t, 3, mtf.GetCallStats().Counts()["ListDir"])
				// We check that we delete the shard replication file
				// - keyspaces/ks/shards/0/ShardReplication
				require.EqualValues(t, 1, mtf.GetCallStats().Counts()["Delete"])
				// Verify that getting srvKeyspaceNames is now empty.
				keyspaces, err := ts.GetSrvKeyspaceNames(ctx, cell)
				require.NoError(t, err)
				require.Len(t, keyspaces, 0)
			},
		}, {
			name: "Ensure we don't delete extra files",
			setup: func(t *testing.T, ts *topo.Server, mtf *memorytopo.Factory) {
				err := ts.UpdateShardReplicationFields(ctx, cell, keyspace, "0", func(sr *topodatapb.ShardReplication) error {
					sr.Nodes = append(sr.Nodes, &topodatapb.ShardReplication_Node{TabletAlias: &topodatapb.TabletAlias{Cell: cell, Uid: 0}})
					return nil
				})
				require.NoError(t, err)
				err = ts.CreateKeyspace(ctx, keyspace2, &topodatapb.Keyspace{KeyspaceType: topodatapb.KeyspaceType_NORMAL})
				require.NoError(t, err)
				err = ts.UpdateShardReplicationFields(ctx, cell, keyspace2, "0", func(sr *topodatapb.ShardReplication) error {
					sr.Nodes = append(sr.Nodes, &topodatapb.ShardReplication_Node{TabletAlias: &topodatapb.TabletAlias{Cell: cell, Uid: 0}})
					return nil
				})
				require.NoError(t, err)
			},
			verify: func(t *testing.T, ts *topo.Server, mtf *memorytopo.Factory) {
				// Verify that we don't delete the files for the other keyspace.
				_, err := ts.GetKeyspace(ctx, keyspace2)
				require.NoError(t, err)
				_, err = ts.GetShardReplication(ctx, cell, keyspace2, "0")
				require.NoError(t, err)
			},
		}, {
			name: "Multiple orphaned files to delete",
			setup: func(t *testing.T, ts *topo.Server, mtf *memorytopo.Factory) {
				err := ts.UpdateShardReplicationFields(ctx, cell, keyspace, "0", func(sr *topodatapb.ShardReplication) error {
					sr.Nodes = append(sr.Nodes, &topodatapb.ShardReplication_Node{TabletAlias: &topodatapb.TabletAlias{Cell: cell, Uid: 0}})
					return nil
				})
				require.NoError(t, err)
				err = ts.UpdateSrvKeyspace(ctx, cell, keyspace, &topodatapb.SrvKeyspace{
					Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
						{
							ServedType: topodatapb.TabletType_PRIMARY,
						},
					},
				})
				require.NoError(t, err)
				err = ts.UpdateSrvKeyspace(ctx, cell2, keyspace, &topodatapb.SrvKeyspace{
					Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
						{
							ServedType: topodatapb.TabletType_PRIMARY,
						},
					},
				})
				require.NoError(t, err)
				// Verify that getting srvKeyspaceNames now gives us this keyspace.
				keyspaces, err := ts.GetSrvKeyspaceNames(ctx, cell)
				require.NoError(t, err)
				require.EqualValues(t, 1, len(keyspaces))
				require.EqualValues(t, keyspace, keyspaces[0])
				// Also verify that we can't get the SrvKeyspace.
				_, err = ts.GetSrvKeyspace(ctx, cell, keyspace)
				require.NoError(t, err)
				mtf.GetCallStats().ResetAll()
			},
			verify: func(t *testing.T, ts *topo.Server, mtf *memorytopo.Factory) {
				// We check that we only get 3 ListDir calls -
				// - keyspaces/ks
				// - keyspaces/ks/shards
				// - keyspaces/ks/shards/0
				require.EqualValues(t, 3, mtf.GetCallStats().Counts()["ListDir"])
				// We check that we delete the shard replication file
				// - keyspaces/ks/shards/0/ShardReplication
				// - keyspaces/ks/SrvKeyspace
				require.EqualValues(t, 2, mtf.GetCallStats().Counts()["Delete"])
				// Verify that getting srvKeyspaceNames is now empty.
				keyspaces, err := ts.GetSrvKeyspaceNames(ctx, cell)
				require.NoError(t, err)
				require.Len(t, keyspaces, 0)
				// Since we only clear orphaned files for one cell, we should still see
				// they srvKeyspace in the other cell.
				keyspaces, err = ts.GetSrvKeyspaceNames(ctx, cell2)
				require.NoError(t, err)
				require.Len(t, keyspaces, 1)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts, mtf := memorytopo.NewServerAndFactory(ctx, cell, cell2)
			tt.setup(t, ts, mtf)
			err := ts.DeleteOrphanedKeyspaceFiles(ctx, cell, keyspace)
			require.NoError(t, err)
			tt.verify(t, ts, mtf)
		})
	}
}
