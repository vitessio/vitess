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

package grpcvtctldserver

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl/backupstorage"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/grpcvtctldserver/testutil"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	mysqlctlpb "vitess.io/vitess/go/vt/proto/mysqlctl"
	querypb "vitess.io/vitess/go/vt/proto/query"
	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtctlservicepb "vitess.io/vitess/go/vt/proto/vtctlservice"
	"vitess.io/vitess/go/vt/proto/vttime"
)

func init() {
	*backupstorage.BackupStorageImplementation = testutil.BackupStorageImplementation

	// For tests that don't actually care about mocking the tmclient (i.e. they
	// call NewVtctldServer to initialize the unit under test), this needs to be
	// set.
	//
	// Tests that do care about the tmclient should use
	// testutil.NewVtctldServerWithTabletManagerClient to initialize their
	// VtctldServer.
	*tmclient.TabletManagerProtocol = "grpcvtctldserver.test"
	tmclient.RegisterTabletManagerClientFactory("grpcvtctldserver.test", func() tmclient.TabletManagerClient {
		return nil
	})
}

func TestChangeTabletType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		cells     []string
		tablets   []*topodatapb.Tablet
		req       *vtctldatapb.ChangeTabletTypeRequest
		expected  *vtctldatapb.ChangeTabletTypeResponse
		shouldErr bool
	}{
		{
			name:  "success",
			cells: []string{"zone1"},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type: topodatapb.TabletType_REPLICA,
				},
			},
			req: &vtctldatapb.ChangeTabletTypeRequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
				DbType: topodatapb.TabletType_RDONLY,
			},
			expected: &vtctldatapb.ChangeTabletTypeResponse{
				BeforeTablet: &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type: topodatapb.TabletType_REPLICA,
				},
				AfterTablet: &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type: topodatapb.TabletType_RDONLY,
				},
				WasDryRun: false,
			},
			shouldErr: false,
		},
		{
			name:  "dry run",
			cells: []string{"zone1"},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type: topodatapb.TabletType_REPLICA,
				},
			},
			req: &vtctldatapb.ChangeTabletTypeRequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
				DbType: topodatapb.TabletType_RDONLY,
				DryRun: true,
			},
			expected: &vtctldatapb.ChangeTabletTypeResponse{
				BeforeTablet: &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type: topodatapb.TabletType_REPLICA,
				},
				AfterTablet: &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type: topodatapb.TabletType_RDONLY,
				},
				WasDryRun: true,
			},
			shouldErr: false,
		},
		{
			name:  "tablet not found",
			cells: []string{"zone1"},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  200,
					},
					Type: topodatapb.TabletType_REPLICA,
				},
			},
			req: &vtctldatapb.ChangeTabletTypeRequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
				DbType: topodatapb.TabletType_RDONLY,
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name:  "master promotions not allowed",
			cells: []string{"zone1"},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type: topodatapb.TabletType_REPLICA,
				},
			},
			req: &vtctldatapb.ChangeTabletTypeRequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
				DbType: topodatapb.TabletType_MASTER,
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name:  "master demotions not allowed",
			cells: []string{"zone1"},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type: topodatapb.TabletType_MASTER,
				},
			},
			req: &vtctldatapb.ChangeTabletTypeRequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
				DbType: topodatapb.TabletType_REPLICA,
			},
			expected:  nil,
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			ts := memorytopo.NewServer(tt.cells...)
			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, &testutil.TabletManagerClient{
				TopoServer: ts,
			}, func(ts *topo.Server) vtctlservicepb.VtctldServer { return NewVtctldServer(ts) })

			testutil.AddTablets(ctx, t, ts, nil, tt.tablets...)

			resp, err := vtctld.ChangeTabletType(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.expected, resp)

			// If we are testing a dry-run, then the tablet in the actual
			// topo should match the BeforeTablet in the response. Otherwise,
			// the tablet in the actual topo should match the AfterTablet in
			// the response.
			expectedRealType := resp.AfterTablet.Type
			msg := "ChangeTabletType did not cause topo update"
			if tt.req.DryRun {
				expectedRealType = resp.BeforeTablet.Type
				msg = "dryrun type change resulted in real type change"
			}

			tablet, err := ts.GetTablet(ctx, tt.req.TabletAlias)
			assert.NoError(t, err,
				"could not load tablet %s from topo after type change %v -> %v [dryrun=%t]",
				topoproto.TabletAliasString(tt.req.TabletAlias),
				resp.BeforeTablet.Type,
				resp.AfterTablet.Type,
				resp.WasDryRun,
			)
			assert.Equal(t, expectedRealType, tablet.Type, msg)
		})
	}

	t.Run("tabletmanager failure", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		ts := memorytopo.NewServer("zone1")
		vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, &testutil.TabletManagerClient{
			TopoServer: nil,
		}, func(ts *topo.Server) vtctlservicepb.VtctldServer { return NewVtctldServer(ts) })

		testutil.AddTablet(ctx, t, ts, &topodatapb.Tablet{
			Alias: &topodatapb.TabletAlias{
				Cell: "zone1",
				Uid:  100,
			},
			Type: topodatapb.TabletType_REPLICA,
		}, nil)

		_, err := vtctld.ChangeTabletType(ctx, &vtctldatapb.ChangeTabletTypeRequest{
			TabletAlias: &topodatapb.TabletAlias{
				Cell: "zone1",
				Uid:  100,
			},
			DbType: topodatapb.TabletType_RDONLY,
		})
		assert.Error(t, err)
	})
}

func TestCreateKeyspace(t *testing.T) {
	t.Parallel()

	cells := []string{"zone1", "zone2", "zone3"}
	tests := []struct {
		name               string
		topo               map[string]*topodatapb.Keyspace
		vschemas           map[string]*vschemapb.Keyspace
		req                *vtctldatapb.CreateKeyspaceRequest
		expected           *vtctldatapb.CreateKeyspaceResponse
		shouldErr          bool
		vschemaShouldExist bool
		expectedVSchema    *vschemapb.Keyspace
	}{
		{
			name: "normal keyspace",
			topo: nil,
			req: &vtctldatapb.CreateKeyspaceRequest{
				Name: "testkeyspace",
				Type: topodatapb.KeyspaceType_NORMAL,
			},
			expected: &vtctldatapb.CreateKeyspaceResponse{
				Keyspace: &vtctldatapb.Keyspace{
					Name: "testkeyspace",
					Keyspace: &topodatapb.Keyspace{
						KeyspaceType: topodatapb.KeyspaceType_NORMAL,
					},
				},
			},
			vschemaShouldExist: true,
			expectedVSchema: &vschemapb.Keyspace{
				Sharded: false,
			},
			shouldErr: false,
		},
		{
			name: "snapshot keyspace",
			topo: map[string]*topodatapb.Keyspace{
				"testkeyspace": {
					KeyspaceType: topodatapb.KeyspaceType_NORMAL,
				},
			},
			vschemas: map[string]*vschemapb.Keyspace{
				"testkeyspace": {
					Sharded: true,
					Vindexes: map[string]*vschemapb.Vindex{
						"h1": {
							Type: "hash",
						},
					},
				},
			},
			req: &vtctldatapb.CreateKeyspaceRequest{
				Name:         "testsnapshot",
				Type:         topodatapb.KeyspaceType_SNAPSHOT,
				BaseKeyspace: "testkeyspace",
				SnapshotTime: &vttime.Time{
					Seconds: 1,
				},
			},
			expected: &vtctldatapb.CreateKeyspaceResponse{
				Keyspace: &vtctldatapb.Keyspace{
					Name: "testsnapshot",
					Keyspace: &topodatapb.Keyspace{
						KeyspaceType: topodatapb.KeyspaceType_SNAPSHOT,
						BaseKeyspace: "testkeyspace",
						SnapshotTime: &vttime.Time{
							Seconds: 1,
						},
					},
				},
			},
			vschemaShouldExist: true,
			expectedVSchema: &vschemapb.Keyspace{
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"h1": {
						Type: "hash",
					},
				},
				RequireExplicitRouting: true,
			},
			shouldErr: false,
		},
		{
			name: "snapshot keyspace with no base keyspace specified",
			topo: nil,
			req: &vtctldatapb.CreateKeyspaceRequest{
				Name:         "testsnapshot",
				Type:         topodatapb.KeyspaceType_SNAPSHOT,
				SnapshotTime: &vttime.Time{},
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "snapshot keyspace with no snapshot time",
			topo: nil,
			req: &vtctldatapb.CreateKeyspaceRequest{
				Name:         "testsnapshot",
				Type:         topodatapb.KeyspaceType_SNAPSHOT,
				BaseKeyspace: "testkeyspace",
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "snapshot keyspace with nonexistent base keyspace",
			topo: nil,
			req: &vtctldatapb.CreateKeyspaceRequest{
				Name:         "testsnapshot",
				Type:         topodatapb.KeyspaceType_SNAPSHOT,
				BaseKeyspace: "testkeyspace",
				SnapshotTime: &vttime.Time{Seconds: 100},
			},
			expected: &vtctldatapb.CreateKeyspaceResponse{
				Keyspace: &vtctldatapb.Keyspace{
					Name: "testsnapshot",
					Keyspace: &topodatapb.Keyspace{
						KeyspaceType: topodatapb.KeyspaceType_SNAPSHOT,
						BaseKeyspace: "testkeyspace",
						SnapshotTime: &vttime.Time{Seconds: 100},
					},
				},
			},
			vschemaShouldExist: true,
			expectedVSchema: &vschemapb.Keyspace{
				Sharded:                false,
				RequireExplicitRouting: true,
			},
			shouldErr: false,
		},
		{
			name: "invalid keyspace type",
			topo: nil,
			req: &vtctldatapb.CreateKeyspaceRequest{
				Name: "badkeyspacetype",
				Type: 10000000,
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "keyspace exists/no force",
			topo: map[string]*topodatapb.Keyspace{
				"testkeyspace": {
					KeyspaceType:       topodatapb.KeyspaceType_NORMAL,
					ShardingColumnName: "col1",
					ShardingColumnType: topodatapb.KeyspaceIdType_UINT64,
				},
			},
			req: &vtctldatapb.CreateKeyspaceRequest{
				Name:  "testkeyspace",
				Type:  topodatapb.KeyspaceType_NORMAL,
				Force: false,
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "keyspace exists/force",
			topo: map[string]*topodatapb.Keyspace{
				"testkeyspace": {
					KeyspaceType:       topodatapb.KeyspaceType_NORMAL,
					ShardingColumnName: "col1",
					ShardingColumnType: topodatapb.KeyspaceIdType_UINT64,
				},
			},
			req: &vtctldatapb.CreateKeyspaceRequest{
				Name:  "testkeyspace",
				Type:  topodatapb.KeyspaceType_NORMAL,
				Force: true,
			},
			expected: &vtctldatapb.CreateKeyspaceResponse{
				Keyspace: &vtctldatapb.Keyspace{
					Name: "testkeyspace",
					Keyspace: &topodatapb.Keyspace{
						KeyspaceType:       topodatapb.KeyspaceType_NORMAL,
						ShardingColumnName: "col1",
						ShardingColumnType: topodatapb.KeyspaceIdType_UINT64,
					},
				},
			},
			vschemaShouldExist: true,
			expectedVSchema: &vschemapb.Keyspace{
				Sharded: false,
			},
			shouldErr: false,
		},
		{
			name: "allow empty vschema",
			topo: nil,
			req: &vtctldatapb.CreateKeyspaceRequest{
				Name:              "testkeyspace",
				Type:              topodatapb.KeyspaceType_NORMAL,
				AllowEmptyVSchema: true,
			},
			expected: &vtctldatapb.CreateKeyspaceResponse{
				Keyspace: &vtctldatapb.Keyspace{
					Name: "testkeyspace",
					Keyspace: &topodatapb.Keyspace{
						KeyspaceType: topodatapb.KeyspaceType_NORMAL,
					},
				},
			},
			vschemaShouldExist: false,
			expectedVSchema:    nil,
			shouldErr:          false,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if tt.req == nil {
				t.Skip("test not yet implemented")
			}

			ctx := context.Background()
			ts := memorytopo.NewServer(cells...)
			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})

			for name, ks := range tt.topo {
				testutil.AddKeyspace(ctx, t, ts, &vtctldatapb.Keyspace{
					Name:     name,
					Keyspace: ks,
				})
			}

			for name, vs := range tt.vschemas {
				require.NoError(t, ts.SaveVSchema(ctx, name, vs), "error in SaveVSchema(%v, %+v)", name, vs)
			}

			// Create the keyspace and make some assertions
			resp, err := vtctld.CreateKeyspace(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			testutil.AssertKeyspacesEqual(t, tt.expected.Keyspace, resp.Keyspace, "%+v\n%+v\n", tt.expected.Keyspace, resp.Keyspace)

			// Fetch the newly-created keyspace out of the topo and assert on it
			ks, err := ts.GetKeyspace(ctx, tt.req.Name)
			assert.NoError(t, err, "cannot get keyspace %v after creating", tt.req.Name)
			require.NotNil(t, ks.Keyspace)

			actualKs := &vtctldatapb.Keyspace{
				Name:     tt.req.Name,
				Keyspace: ks.Keyspace,
			}
			testutil.AssertKeyspacesEqual(
				t,
				tt.expected.Keyspace,
				actualKs,
				"created keyspace %v does not match requested keyspace (name = %v) %v",
				actualKs,
				tt.expected.Keyspace,
			)

			// Finally, check the VSchema
			vs, err := ts.GetVSchema(ctx, tt.req.Name)
			if !tt.vschemaShouldExist {
				assert.True(t, topo.IsErrType(err, topo.NoNode), "vschema should not exist, but got other error = %v", err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.expectedVSchema, vs)
		})
	}
}

func TestCreateShard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		keyspaces []*vtctldatapb.Keyspace
		shards    []*vtctldatapb.Shard
		topoErr   error
		req       *vtctldatapb.CreateShardRequest
		expected  *vtctldatapb.CreateShardResponse
		shouldErr bool
	}{
		{
			name: "success",
			keyspaces: []*vtctldatapb.Keyspace{
				{
					Name:     "testkeyspace",
					Keyspace: &topodatapb.Keyspace{},
				},
			},
			shards:  nil,
			topoErr: nil,
			req: &vtctldatapb.CreateShardRequest{
				Keyspace:  "testkeyspace",
				ShardName: "-",
			},
			expected: &vtctldatapb.CreateShardResponse{
				Keyspace: &vtctldatapb.Keyspace{
					Name:     "testkeyspace",
					Keyspace: &topodatapb.Keyspace{},
				},
				Shard: &vtctldatapb.Shard{
					Keyspace: "testkeyspace",
					Name:     "-",
					Shard: &topodatapb.Shard{
						KeyRange:        &topodatapb.KeyRange{},
						IsMasterServing: true,
					},
				},
				ShardAlreadyExists: false,
			},
			shouldErr: false,
		},
		{
			name:      "include parent",
			keyspaces: nil,
			shards:    nil,
			topoErr:   nil,
			req: &vtctldatapb.CreateShardRequest{
				Keyspace:      "testkeyspace",
				ShardName:     "-",
				IncludeParent: true,
			},
			expected: &vtctldatapb.CreateShardResponse{
				Keyspace: &vtctldatapb.Keyspace{
					Name:     "testkeyspace",
					Keyspace: &topodatapb.Keyspace{},
				},
				Shard: &vtctldatapb.Shard{
					Keyspace: "testkeyspace",
					Name:     "-",
					Shard: &topodatapb.Shard{
						KeyRange:        &topodatapb.KeyRange{},
						IsMasterServing: true,
					},
				},
				ShardAlreadyExists: false,
			},
			shouldErr: false,
		},
		{
			name:      "keyspace does not exist",
			keyspaces: nil,
			shards:    nil,
			topoErr:   nil,
			req: &vtctldatapb.CreateShardRequest{
				Keyspace:  "testkeyspace",
				ShardName: "-",
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "include parent/keyspace exists/no force",
			keyspaces: []*vtctldatapb.Keyspace{
				{
					Name:     "testkeyspace",
					Keyspace: &topodatapb.Keyspace{},
				},
			},
			shards:  nil,
			topoErr: nil,
			req: &vtctldatapb.CreateShardRequest{
				Keyspace:      "testkeyspace",
				ShardName:     "-",
				IncludeParent: true,
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "include parent/keyspace exists/force",
			keyspaces: []*vtctldatapb.Keyspace{
				{
					Name:     "testkeyspace",
					Keyspace: &topodatapb.Keyspace{},
				},
			},
			shards:  nil,
			topoErr: nil,
			req: &vtctldatapb.CreateShardRequest{
				Keyspace:      "testkeyspace",
				ShardName:     "-",
				IncludeParent: true,
				Force:         true,
			},
			expected: &vtctldatapb.CreateShardResponse{
				Keyspace: &vtctldatapb.Keyspace{
					Name:     "testkeyspace",
					Keyspace: &topodatapb.Keyspace{},
				},
				Shard: &vtctldatapb.Shard{
					Keyspace: "testkeyspace",
					Name:     "-",
					Shard: &topodatapb.Shard{
						KeyRange:        &topodatapb.KeyRange{},
						IsMasterServing: true,
					},
				},
				ShardAlreadyExists: false,
			},
			shouldErr: false,
		},
		{
			name: "shard exists/no force",
			keyspaces: []*vtctldatapb.Keyspace{
				{
					Name:     "testkeyspace",
					Keyspace: &topodatapb.Keyspace{},
				},
			},
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			topoErr: nil,
			req: &vtctldatapb.CreateShardRequest{
				Keyspace:  "testkeyspace",
				ShardName: "-",
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "shard exists/force",
			keyspaces: []*vtctldatapb.Keyspace{
				{
					Name:     "testkeyspace",
					Keyspace: &topodatapb.Keyspace{},
				},
			},
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			topoErr: nil,
			req: &vtctldatapb.CreateShardRequest{
				Keyspace:  "testkeyspace",
				ShardName: "-",
				Force:     true,
			},
			expected: &vtctldatapb.CreateShardResponse{
				Keyspace: &vtctldatapb.Keyspace{
					Name:     "testkeyspace",
					Keyspace: &topodatapb.Keyspace{},
				},
				Shard: &vtctldatapb.Shard{
					Keyspace: "testkeyspace",
					Name:     "-",
					Shard: &topodatapb.Shard{
						KeyRange:        &topodatapb.KeyRange{},
						IsMasterServing: true,
					},
				},
				ShardAlreadyExists: true,
			},
			shouldErr: false,
		},
		{
			name: "topo is down",
			keyspaces: []*vtctldatapb.Keyspace{
				{
					Name:     "testkeyspace",
					Keyspace: &topodatapb.Keyspace{},
				},
			},
			shards:  nil,
			topoErr: assert.AnError,
			req: &vtctldatapb.CreateShardRequest{
				Keyspace:  "testkeyspace",
				ShardName: "-",
			},
			expected:  nil,
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if tt.req == nil {
				t.Skip("focusing on other tests")
			}

			ctx := context.Background()
			ts, topofactory := memorytopo.NewServerAndFactory("zone1")
			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})

			for _, ks := range tt.keyspaces {
				testutil.AddKeyspace(ctx, t, ts, ks)
			}

			testutil.AddShards(ctx, t, ts, tt.shards...)

			if tt.topoErr != nil {
				topofactory.SetError(tt.topoErr)
			}

			resp, err := vtctld.CreateShard(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.expected, resp)
		})
	}
}

func TestDeleteKeyspace(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                       string
		keyspaces                  []*vtctldatapb.Keyspace
		shards                     []*vtctldatapb.Shard
		srvKeyspaces               map[string]map[string]*topodatapb.SrvKeyspace
		topoErr                    error
		req                        *vtctldatapb.DeleteKeyspaceRequest
		expected                   *vtctldatapb.DeleteKeyspaceResponse
		expectedRemainingKeyspaces []string
		expectedRemainingShards    map[string][]string
		shouldErr                  bool
	}{
		{
			name: "success",
			keyspaces: []*vtctldatapb.Keyspace{
				{
					Name:     "testkeyspace",
					Keyspace: &topodatapb.Keyspace{},
				},
			},
			shards:       nil,
			srvKeyspaces: nil,
			topoErr:      nil,
			req: &vtctldatapb.DeleteKeyspaceRequest{
				Keyspace: "testkeyspace",
			},
			expected:                   &vtctldatapb.DeleteKeyspaceResponse{},
			expectedRemainingKeyspaces: []string{},
			expectedRemainingShards:    map[string][]string{},
			shouldErr:                  false,
		},
		{
			name: "keyspace does not exist",
			keyspaces: []*vtctldatapb.Keyspace{
				{
					Name:     "otherkeyspace",
					Keyspace: &topodatapb.Keyspace{},
				},
			},
			shards:       nil,
			srvKeyspaces: nil,
			topoErr:      nil,
			req: &vtctldatapb.DeleteKeyspaceRequest{
				Keyspace: "testkeyspace",
			},
			expected:                   nil,
			expectedRemainingKeyspaces: []string{"otherkeyspace"},
			expectedRemainingShards: map[string][]string{
				"otherkeyspace": nil,
			},
			shouldErr: true,
		},
		{
			name: "keyspace has shards/Recursive=false",
			keyspaces: []*vtctldatapb.Keyspace{
				{
					Name:     "testkeyspace",
					Keyspace: &topodatapb.Keyspace{},
				},
			},
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-80",
				},
				{
					Keyspace: "testkeyspace",
					Name:     "80-",
				},
			},
			srvKeyspaces: nil,
			topoErr:      nil,
			req: &vtctldatapb.DeleteKeyspaceRequest{
				Keyspace: "testkeyspace",
			},
			expected:                   nil,
			expectedRemainingKeyspaces: []string{"testkeyspace"},
			expectedRemainingShards: map[string][]string{
				"testkeyspace": {"-80", "80-"},
			},
			shouldErr: true,
		},
		{
			name: "keyspace has shards/Recursive=true",
			keyspaces: []*vtctldatapb.Keyspace{
				{
					Name:     "testkeyspace",
					Keyspace: &topodatapb.Keyspace{},
				},
			},
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-80",
				},
				{
					Keyspace: "testkeyspace",
					Name:     "80-",
				},
			},
			srvKeyspaces: nil,
			topoErr:      nil,
			req: &vtctldatapb.DeleteKeyspaceRequest{
				Keyspace:  "testkeyspace",
				Recursive: true,
			},
			expected:                   &vtctldatapb.DeleteKeyspaceResponse{},
			expectedRemainingKeyspaces: []string{},
			expectedRemainingShards:    map[string][]string{},
			shouldErr:                  false,
		},
		// Not sure how to force this case because we always pass
		// (Recursive=true, EvenIfServing=true) so anything short of "topo
		// server is down" won't fail, and "topo server is down" will cause us
		// to error before we even reach this point in the code, so, ¯\_(ツ)_/¯.
		// {
		// 	name: "recursive/cannot delete shard",
		// },
		{
			name: "topo error",
			keyspaces: []*vtctldatapb.Keyspace{
				{
					Name:     "testkeyspace",
					Keyspace: &topodatapb.Keyspace{},
				},
			},
			shards:       nil,
			srvKeyspaces: nil,
			topoErr:      assert.AnError,
			req: &vtctldatapb.DeleteKeyspaceRequest{
				Keyspace: "testkeyspace",
			},
			expected:                   nil,
			expectedRemainingKeyspaces: []string{"testkeyspace"},
			expectedRemainingShards: map[string][]string{
				"testkeyspace": nil,
			},
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cells := []string{"zone1", "zone2", "zone3"}

			ctx := context.Background()
			ts, topofactory := memorytopo.NewServerAndFactory(cells...)
			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})

			testutil.AddKeyspaces(ctx, t, ts, tt.keyspaces...)
			testutil.AddShards(ctx, t, ts, tt.shards...)
			testutil.UpdateSrvKeyspaces(ctx, t, ts, tt.srvKeyspaces)

			if tt.topoErr != nil {
				topofactory.SetError(tt.topoErr)
			}

			defer func() {
				if tt.expectedRemainingKeyspaces == nil {
					return
				}

				topofactory.SetError(nil)

				keyspaces, err := ts.GetKeyspaces(ctx)
				require.NoError(t, err, "cannot get keyspaces names after DeleteKeyspace call")
				assert.ElementsMatch(t, tt.expectedRemainingKeyspaces, keyspaces)

				if tt.expectedRemainingShards == nil {
					return
				}

				remainingShards := make(map[string][]string, len(keyspaces))

				for _, ks := range keyspaces {
					shards, err := ts.GetShardNames(ctx, ks)
					require.NoError(t, err, "cannot get shard names for keyspace %s", ks)

					remainingShards[ks] = shards
				}

				assert.Equal(t, tt.expectedRemainingShards, remainingShards)
			}()

			resp, err := vtctld.DeleteKeyspace(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)

				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.expected, resp)
		})
	}
}

func TestDeleteShards(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                    string
		shards                  []*vtctldatapb.Shard
		tablets                 []*topodatapb.Tablet
		replicationGraphs       []*topo.ShardReplicationInfo
		srvKeyspaces            map[string]map[string]*topodatapb.SrvKeyspace
		topoErr                 error
		req                     *vtctldatapb.DeleteShardsRequest
		expected                *vtctldatapb.DeleteShardsResponse
		expectedRemainingShards []*vtctldatapb.Shard
		shouldErr               bool
	}{
		{
			name: "success",
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			tablets: nil,
			topoErr: nil,
			req: &vtctldatapb.DeleteShardsRequest{
				Shards: []*vtctldatapb.Shard{
					{
						Keyspace: "testkeyspace",
						Name:     "-",
					},
				},
			},
			expected:                &vtctldatapb.DeleteShardsResponse{},
			expectedRemainingShards: []*vtctldatapb.Shard{},
			shouldErr:               false,
		},
		{
			name:    "shard not found",
			shards:  nil,
			tablets: nil,
			topoErr: nil,
			req: &vtctldatapb.DeleteShardsRequest{
				Shards: []*vtctldatapb.Shard{
					{
						Keyspace: "testkeyspace",
						Name:     "-",
					},
				},
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "multiple shards",
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
				{
					Keyspace: "otherkeyspace",
					Name:     "-80",
				},
				{
					Keyspace: "otherkeyspace",
					Name:     "80-",
				},
			},
			tablets: nil,
			topoErr: nil,
			req: &vtctldatapb.DeleteShardsRequest{
				Shards: []*vtctldatapb.Shard{
					{
						Keyspace: "testkeyspace",
						Name:     "-",
					},
					{
						Keyspace: "otherkeyspace",
						Name:     "-80",
					},
				},
			},
			expected: &vtctldatapb.DeleteShardsResponse{},
			expectedRemainingShards: []*vtctldatapb.Shard{
				{
					Keyspace: "otherkeyspace",
					Name:     "80-",
				},
			},
			shouldErr: false,
		},
		{
			name: "topo is down",
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			tablets: nil,
			topoErr: assert.AnError,
			req: &vtctldatapb.DeleteShardsRequest{
				Shards: []*vtctldatapb.Shard{
					{
						Keyspace: "testkeyspace",
						Name:     "-",
					},
				},
			},
			expected: nil,
			expectedRemainingShards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			shouldErr: true,
		},
		{
			name: "shard is serving/EvenIfServing=false",
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			tablets: nil,
			srvKeyspaces: map[string]map[string]*topodatapb.SrvKeyspace{
				"zone1": {
					"testkeyspace": &topodatapb.SrvKeyspace{
						Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
							{
								ServedType: topodatapb.TabletType_MASTER,
								ShardReferences: []*topodatapb.ShardReference{
									{
										Name:     "-",
										KeyRange: &topodatapb.KeyRange{},
									},
								},
							},
						},
					},
				},
			},
			topoErr: nil,
			req: &vtctldatapb.DeleteShardsRequest{
				Shards: []*vtctldatapb.Shard{
					{
						Keyspace: "testkeyspace",
						Name:     "-",
					},
				},
			},
			expected: nil,
			expectedRemainingShards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			shouldErr: true,
		},
		{
			name: "shard is serving/EvenIfServing=true",
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			tablets: nil,
			srvKeyspaces: map[string]map[string]*topodatapb.SrvKeyspace{
				"zone1": {
					"testkeyspace": &topodatapb.SrvKeyspace{
						Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
							{
								ServedType: topodatapb.TabletType_MASTER,
								ShardReferences: []*topodatapb.ShardReference{
									{
										Name:     "-",
										KeyRange: &topodatapb.KeyRange{},
									},
								},
							},
						},
					},
				},
			},
			topoErr: nil,
			req: &vtctldatapb.DeleteShardsRequest{
				Shards: []*vtctldatapb.Shard{
					{
						Keyspace: "testkeyspace",
						Name:     "-",
					},
				},
				EvenIfServing: true,
			},
			expected:                &vtctldatapb.DeleteShardsResponse{},
			expectedRemainingShards: []*vtctldatapb.Shard{},
			shouldErr:               false,
		},
		{
			name: "ShardReplication in topo",
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			tablets: nil,
			replicationGraphs: []*topo.ShardReplicationInfo{
				topo.NewShardReplicationInfo(&topodatapb.ShardReplication{
					Nodes: []*topodatapb.ShardReplication_Node{
						{
							TabletAlias: &topodatapb.TabletAlias{
								Cell: "zone1",
								Uid:  100,
							},
						},
					},
				}, "zone1", "testkeyspace", "-"),
				topo.NewShardReplicationInfo(&topodatapb.ShardReplication{
					Nodes: []*topodatapb.ShardReplication_Node{
						{
							TabletAlias: &topodatapb.TabletAlias{
								Cell: "zone2",
								Uid:  200,
							},
						},
					},
				}, "zone2", "testkeyspace", "-"),
				topo.NewShardReplicationInfo(&topodatapb.ShardReplication{
					Nodes: []*topodatapb.ShardReplication_Node{
						{
							TabletAlias: &topodatapb.TabletAlias{
								Cell: "zone3",
								Uid:  300,
							},
						},
					},
				}, "zone3", "testkeyspace", "-"),
			},
			topoErr: nil,
			req: &vtctldatapb.DeleteShardsRequest{
				Shards: []*vtctldatapb.Shard{
					{
						Keyspace: "testkeyspace",
						Name:     "-",
					},
				},
			},
			expected:                &vtctldatapb.DeleteShardsResponse{},
			expectedRemainingShards: []*vtctldatapb.Shard{},
			shouldErr:               false,
		},
		{
			name: "shard has tablets/Recursive=false",
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},
			topoErr: nil,
			req: &vtctldatapb.DeleteShardsRequest{
				Shards: []*vtctldatapb.Shard{
					{
						Keyspace: "testkeyspace",
						Name:     "-",
					},
				},
			},
			expected: nil,
			expectedRemainingShards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			shouldErr: true,
		},
		{
			name: "shard has tablets/Recursive=true",
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},
			topoErr: nil,
			req: &vtctldatapb.DeleteShardsRequest{
				Shards: []*vtctldatapb.Shard{
					{
						Keyspace: "testkeyspace",
						Name:     "-",
					},
				},
				Recursive: true,
			},
			expected:                &vtctldatapb.DeleteShardsResponse{},
			expectedRemainingShards: []*vtctldatapb.Shard{},
			shouldErr:               false,
		},
		{
			name: "tablets in topo belonging to other shard",
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-80",
				},
				{
					Keyspace: "testkeyspace",
					Name:     "80-",
				},
			},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "testkeyspace",
					Shard:    "80-",
				},
			},
			topoErr: nil,
			req: &vtctldatapb.DeleteShardsRequest{
				Shards: []*vtctldatapb.Shard{
					{
						Keyspace: "testkeyspace",
						Name:     "-80",
					},
				},
			},
			expected: &vtctldatapb.DeleteShardsResponse{},
			expectedRemainingShards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "80-",
				},
			},
			shouldErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cells := []string{"zone1", "zone2", "zone3"}

			ctx := context.Background()
			ts, topofactory := memorytopo.NewServerAndFactory(cells...)
			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})

			testutil.AddShards(ctx, t, ts, tt.shards...)
			testutil.AddTablets(ctx, t, ts, nil, tt.tablets...)
			testutil.SetupReplicationGraphs(ctx, t, ts, tt.replicationGraphs...)
			testutil.UpdateSrvKeyspaces(ctx, t, ts, tt.srvKeyspaces)

			if tt.topoErr != nil {
				topofactory.SetError(tt.topoErr)
			}

			if tt.expectedRemainingShards != nil {
				defer func() {
					topofactory.SetError(nil)

					actualShards := []*vtctldatapb.Shard{}

					keyspaces, err := ts.GetKeyspaces(ctx)
					require.NoError(t, err, "cannot get keyspace names to check remaining shards")

					for _, ks := range keyspaces {
						shards, err := ts.GetShardNames(ctx, ks)
						require.NoError(t, err, "cannot get shard names for keyspace %s", ks)

						for _, shard := range shards {
							actualShards = append(actualShards, &vtctldatapb.Shard{
								Keyspace: ks,
								Name:     shard,
							})
						}
					}

					assert.ElementsMatch(t, tt.expectedRemainingShards, actualShards)
				}()
			}

			resp, err := vtctld.DeleteShards(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)

				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.expected, resp)
		})
	}
}

func TestDeleteTablets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                     string
		tablets                  []*topodatapb.Tablet
		shardFieldUpdates        map[string]func(*topo.ShardInfo) error
		lockedShards             []*vtctldatapb.Shard
		topoError                error
		req                      *vtctldatapb.DeleteTabletsRequest
		expected                 *vtctldatapb.DeleteTabletsResponse
		expectedRemainingTablets []*topodatapb.Tablet
		shouldErr                bool
	}{
		{
			name: "single replica",
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},
			lockedShards: nil,
			topoError:    nil,
			req: &vtctldatapb.DeleteTabletsRequest{
				TabletAliases: []*topodatapb.TabletAlias{
					{
						Cell: "zone1",
						Uid:  100,
					},
				},
			},
			expected:                 &vtctldatapb.DeleteTabletsResponse{},
			expectedRemainingTablets: []*topodatapb.Tablet{},
			shouldErr:                false,
		},
		{
			name: "single primary/no AllowPrimary",
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_MASTER,
					Keyspace: "testkeyspace",
					Shard:    "-",
					MasterTermStartTime: &vttime.Time{
						Seconds:     100,
						Nanoseconds: 10,
					},
				},
			},
			lockedShards: nil,
			topoError:    nil,
			req: &vtctldatapb.DeleteTabletsRequest{
				TabletAliases: []*topodatapb.TabletAlias{
					{
						Cell: "zone1",
						Uid:  100,
					},
				},
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "single primary/with AllowPrimary",
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_MASTER,
					Keyspace: "testkeyspace",
					Shard:    "-",
					MasterTermStartTime: &vttime.Time{
						Seconds:     100,
						Nanoseconds: 10,
					},
				},
			},
			lockedShards: nil,
			topoError:    nil,
			req: &vtctldatapb.DeleteTabletsRequest{
				TabletAliases: []*topodatapb.TabletAlias{
					{
						Cell: "zone1",
						Uid:  100,
					},
				},
				AllowPrimary: true,
			},
			expected:                 &vtctldatapb.DeleteTabletsResponse{},
			expectedRemainingTablets: []*topodatapb.Tablet{},
			shouldErr:                false,
		},
		{
			name: "multiple tablets",
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  102,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},
			lockedShards: nil,
			topoError:    nil,
			req: &vtctldatapb.DeleteTabletsRequest{
				TabletAliases: []*topodatapb.TabletAlias{
					{
						Cell: "zone1",
						Uid:  100,
					},
					{
						Cell: "zone1",
						Uid:  102,
					},
				},
			},
			expected: &vtctldatapb.DeleteTabletsResponse{},
			expectedRemainingTablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},
			shouldErr: false,
		},
		{
			name: "stale primary record",
			tablets: []*topodatapb.Tablet{
				{
					// The stale primary we're going to delete.
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_MASTER,
					Keyspace: "testkeyspace",
					Shard:    "-",
					MasterTermStartTime: &vttime.Time{
						Seconds:     100,
						Nanoseconds: 10,
					},
				},
				{
					// The real shard primary, which we'll update in the shard
					// record below.
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
					Type:     topodatapb.TabletType_MASTER,
					Keyspace: "testkeyspace",
					Shard:    "-",
					MasterTermStartTime: &vttime.Time{
						Seconds:     1001,
						Nanoseconds: 101,
					},
				},
			},
			shardFieldUpdates: map[string]func(*topo.ShardInfo) error{
				"testkeyspace/-": func(si *topo.ShardInfo) error {
					si.MasterAlias = &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					}
					si.MasterTermStartTime = &vttime.Time{
						Seconds:     1001,
						Nanoseconds: 101,
					}

					return nil
				},
			},
			lockedShards: nil,
			topoError:    nil,
			req: &vtctldatapb.DeleteTabletsRequest{
				TabletAliases: []*topodatapb.TabletAlias{
					{
						Cell: "zone1",
						Uid:  100,
					},
				},
			},
			expected: &vtctldatapb.DeleteTabletsResponse{},
			expectedRemainingTablets: []*topodatapb.Tablet{
				{
					// The true shard primary still exists (phew!)
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
					Type:     topodatapb.TabletType_MASTER,
					Keyspace: "testkeyspace",
					Shard:    "-",
					MasterTermStartTime: &vttime.Time{
						Seconds:     1001,
						Nanoseconds: 101,
					},
				},
			},
			shouldErr: false,
		},
		{
			name: "tablet not found",
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},
			lockedShards: nil,
			topoError:    nil,
			req: &vtctldatapb.DeleteTabletsRequest{
				TabletAliases: []*topodatapb.TabletAlias{
					{
						Cell: "zone1",
						Uid:  200,
					},
				},
			},
			expected: nil,
			expectedRemainingTablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},
			shouldErr: true,
		},
		{
			name: "shard is locked",
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_MASTER,
					Keyspace: "testkeyspace",
					Shard:    "-",
					MasterTermStartTime: &vttime.Time{
						Seconds:     100,
						Nanoseconds: 10,
					},
				},
			},
			lockedShards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			topoError: nil,
			req: &vtctldatapb.DeleteTabletsRequest{
				TabletAliases: []*topodatapb.TabletAlias{
					{
						Cell: "zone1",
						Uid:  100,
					},
				},
				AllowPrimary: true,
			},
			expected: nil,
			expectedRemainingTablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_MASTER,
					Keyspace: "testkeyspace",
					Shard:    "-",
					MasterTermStartTime: &vttime.Time{
						Seconds:     100,
						Nanoseconds: 10,
					},
				},
			},
			shouldErr: true,
		},
		{
			name: "another shard is locked",
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_MASTER,
					Keyspace: "testkeyspace",
					Shard:    "-80",
					MasterTermStartTime: &vttime.Time{
						Seconds:     100,
						Nanoseconds: 10,
					},
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  200,
					},
					Type:     topodatapb.TabletType_MASTER,
					Keyspace: "testkeyspace",
					Shard:    "80-",
					MasterTermStartTime: &vttime.Time{
						Seconds:     200,
						Nanoseconds: 20,
					},
				},
			},
			lockedShards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "80-",
				},
			},
			topoError: nil,
			req: &vtctldatapb.DeleteTabletsRequest{
				TabletAliases: []*topodatapb.TabletAlias{
					{
						// testkeyspace/-80
						Cell: "zone1",
						Uid:  100,
					},
				},
				AllowPrimary: true,
			},
			expected: &vtctldatapb.DeleteTabletsResponse{},
			expectedRemainingTablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  200,
					},
					Type:     topodatapb.TabletType_MASTER,
					Keyspace: "testkeyspace",
					Shard:    "80-",
					MasterTermStartTime: &vttime.Time{
						Seconds:     200,
						Nanoseconds: 20,
					},
				},
			},
			shouldErr: false,
		},
		{
			name: "topo server is down",
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},
			lockedShards: nil,
			topoError:    assert.AnError,
			req: &vtctldatapb.DeleteTabletsRequest{
				TabletAliases: []*topodatapb.TabletAlias{
					{
						Cell: "zone1",
						Uid:  200,
					},
				},
			},
			expected: nil,
			expectedRemainingTablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if tt.req == nil {
				t.Skip("focusing on other tests")
			}

			ctx := context.Background()
			ts, topofactory := memorytopo.NewServerAndFactory("zone1")
			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})

			// Setup tablets and shards
			testutil.AddTablets(ctx, t, ts, nil, tt.tablets...)

			for key, updateFn := range tt.shardFieldUpdates {
				ks, shard, err := topoproto.ParseKeyspaceShard(key)
				require.NoError(t, err, "bad keyspace/shard provided in shardFieldUpdates: %s", key)

				_, err = ts.UpdateShardFields(ctx, ks, shard, updateFn)
				require.NoError(t, err, "failed to update shard fields for %s", key)
			}

			// Set locks
			for _, shard := range tt.lockedShards {
				lctx, unlock, lerr := ts.LockShard(ctx, shard.Keyspace, shard.Name, "testing locked shard")
				require.NoError(t, lerr, "cannot lock shard %s/%s", shard.Keyspace, shard.Name)
				// unlock at the end of the test, we don't care about this error
				// value anymore
				defer unlock(&lerr)

				// we do, however, care that the lock context gets propogated
				// both to additional calls to lock, and to the actual RPC call.
				ctx = lctx
			}

			// Set errors
			if tt.topoError != nil {
				topofactory.SetError(tt.topoError)
			}

			checkRemainingTablets := func() {
				topofactory.SetError(nil)

				resp, err := vtctld.GetTablets(ctx, &vtctldatapb.GetTabletsRequest{})
				assert.NoError(t, err, "cannot look up tablets from topo after issuing DeleteTablets request")

				assert.ElementsMatch(t, tt.expectedRemainingTablets, resp.Tablets)
			}

			// Run the test
			resp, err := vtctld.DeleteTablets(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)

				if tt.expectedRemainingTablets != nil {
					checkRemainingTablets()
				}

				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.expected, resp)
			checkRemainingTablets()
		})
	}
}

func TestEmergencyReparentShard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		ts      *topo.Server
		tmc     tmclient.TabletManagerClient
		tablets []*topodatapb.Tablet

		req                 *vtctldatapb.EmergencyReparentShardRequest
		expected            *vtctldatapb.EmergencyReparentShardResponse
		expectEventsToOccur bool
		shouldErr           bool
	}{
		{
			name: "successful reparent",
			ts:   memorytopo.NewServer("zone1"),
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type: topodatapb.TabletType_MASTER,
					MasterTermStartTime: &vttime.Time{
						Seconds: 100,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  200,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
					Type:     topodatapb.TabletType_RDONLY,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},
			tmc: &testutil.TabletManagerClient{
				DemoteMasterResults: map[string]struct {
					Status *replicationdatapb.MasterStatus
					Error  error
				}{
					"zone1-0000000100": {
						Status: &replicationdatapb.MasterStatus{
							Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5",
						},
					},
				},
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000200": nil,
				},
				PromoteReplicaResults: map[string]struct {
					Result string
					Error  error
				}{
					"zone1-0000000200": {},
				},
				SetMasterResults: map[string]error{
					"zone1-0000000100": nil,
					"zone1-0000000101": nil,
				},
				StopReplicationAndGetStatusResults: map[string]struct {
					Status     *replicationdatapb.Status
					StopStatus *replicationdatapb.StopReplicationStatus
					Error      error
				}{
					"zone1-0000000100": {
						Error: mysql.ErrNotReplica,
					},
					"zone1-0000000101": {
						Error: assert.AnError,
					},
					"zone1-0000000200": {
						StopStatus: &replicationdatapb.StopReplicationStatus{
							Before: &replicationdatapb.Status{},
							After: &replicationdatapb.Status{
								MasterUuid:       "3E11FA47-71CA-11E1-9E33-C80AA9429562",
								RelayLogPosition: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5",
								Position:         "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5",
							},
						},
					},
				},
				WaitForPositionResults: map[string]map[string]error{
					"zone1-0000000100": {
						"MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5": nil,
					},
					"zone1-0000000200": {
						"MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5": nil,
					},
				},
			},
			req: &vtctldatapb.EmergencyReparentShardRequest{
				Keyspace: "testkeyspace",
				Shard:    "-",
				NewPrimary: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  200,
				},
				WaitReplicasTimeout: protoutil.DurationToProto(time.Millisecond * 10),
			},
			expected: &vtctldatapb.EmergencyReparentShardResponse{
				Keyspace: "testkeyspace",
				Shard:    "-",
				PromotedPrimary: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  200,
				},
			},
			expectEventsToOccur: true,
			shouldErr:           false,
		},
		{
			// Note: this is testing the error-handling done in
			// (*VtctldServer).EmergencyReparentShard, not the logic of an ERS.
			// That logic is tested in reparentutil, and not here. Therefore,
			// the simplest way to trigger a failure is to attempt an ERS on a
			// shard that does not exist.
			name:    "failed reparent",
			ts:      memorytopo.NewServer("zone1"),
			tablets: nil,

			req: &vtctldatapb.EmergencyReparentShardRequest{
				Keyspace: "testkeyspace",
				Shard:    "-",
			},
			expectEventsToOccur: false,
			shouldErr:           true,
		},
		{
			name: "invalid WaitReplicasTimeout",
			req: &vtctldatapb.EmergencyReparentShardRequest{
				WaitReplicasTimeout: &vttime.Duration{
					Seconds: -1,
					Nanos:   1,
				},
			},
			shouldErr: true,
		},
	}

	ctx := context.Background()

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			testutil.AddTablets(ctx, t, tt.ts, &testutil.AddTabletOptions{
				AlsoSetShardMaster:  true,
				ForceSetShardMaster: true,
				SkipShardCreation:   false,
			}, tt.tablets...)

			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, tt.ts, tt.tmc, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})
			resp, err := vtctld.EmergencyReparentShard(ctx, tt.req)

			// We defer this because we want to check in both error and non-
			// error cases, but after the main set of assertions for those
			// cases.
			defer func() {
				if !tt.expectEventsToOccur {
					testutil.AssertNoLogutilEventsOccurred(t, resp, "expected no events to occur during ERS")

					return
				}

				testutil.AssertLogutilEventsOccurred(t, resp, "expected events to occur during ERS")
			}()

			if tt.shouldErr {
				assert.Error(t, err)

				return
			}

			assert.NoError(t, err)
			testutil.AssertEmergencyReparentShardResponsesEqual(t, *tt.expected, *resp)
		})
	}
}

func TestFindAllShardsInKeyspace(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ts := memorytopo.NewServer("cell1")
	vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
		return NewVtctldServer(ts)
	})

	ks := &vtctldatapb.Keyspace{
		Name:     "testkeyspace",
		Keyspace: &topodatapb.Keyspace{},
	}
	testutil.AddKeyspace(ctx, t, ts, ks)

	si1, err := ts.GetOrCreateShard(ctx, ks.Name, "-80")
	require.NoError(t, err)
	si2, err := ts.GetOrCreateShard(ctx, ks.Name, "80-")
	require.NoError(t, err)

	resp, err := vtctld.FindAllShardsInKeyspace(ctx, &vtctldatapb.FindAllShardsInKeyspaceRequest{Keyspace: ks.Name})
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

	_, err = vtctld.FindAllShardsInKeyspace(ctx, &vtctldatapb.FindAllShardsInKeyspaceRequest{Keyspace: "nothing"})
	assert.Error(t, err)
}

func TestGetBackups(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer()
	vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
		return NewVtctldServer(ts)
	})

	testutil.BackupStorage.Backups = map[string][]string{
		"testkeyspace/-": {"backup1", "backup2"},
	}

	expected := &vtctldatapb.GetBackupsResponse{
		Backups: []*mysqlctlpb.BackupInfo{
			{
				Directory: "testkeyspace/-",
				Name:      "backup1",
			},
			{
				Directory: "testkeyspace/-",
				Name:      "backup2",
			},
		},
	}

	resp, err := vtctld.GetBackups(ctx, &vtctldatapb.GetBackupsRequest{
		Keyspace: "testkeyspace",
		Shard:    "-",
	})
	assert.NoError(t, err)
	assert.Equal(t, expected, resp)

	t.Run("no backupstorage", func(t *testing.T) {
		*backupstorage.BackupStorageImplementation = "doesnotexist"
		defer func() { *backupstorage.BackupStorageImplementation = testutil.BackupStorageImplementation }()

		_, err := vtctld.GetBackups(ctx, &vtctldatapb.GetBackupsRequest{
			Keyspace: "testkeyspace",
			Shard:    "-",
		})
		assert.Error(t, err)
	})

	t.Run("listbackups error", func(t *testing.T) {
		testutil.BackupStorage.ListBackupsError = assert.AnError
		defer func() { testutil.BackupStorage.ListBackupsError = nil }()

		_, err := vtctld.GetBackups(ctx, &vtctldatapb.GetBackupsRequest{
			Keyspace: "testkeyspace",
			Shard:    "-",
		})
		assert.Error(t, err)
	})
}

func TestGetKeyspace(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ts := memorytopo.NewServer("cell1")
	vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
		return NewVtctldServer(ts)
	})

	expected := &vtctldatapb.GetKeyspaceResponse{
		Keyspace: &vtctldatapb.Keyspace{
			Name: "testkeyspace",
			Keyspace: &topodatapb.Keyspace{
				ShardingColumnName: "col1",
			},
		},
	}
	testutil.AddKeyspace(ctx, t, ts, expected.Keyspace)

	ks, err := vtctld.GetKeyspace(ctx, &vtctldatapb.GetKeyspaceRequest{Keyspace: expected.Keyspace.Name})
	assert.NoError(t, err)
	assert.Equal(t, expected, ks)

	_, err = vtctld.GetKeyspace(ctx, &vtctldatapb.GetKeyspaceRequest{Keyspace: "notfound"})
	assert.Error(t, err)
}

func TestGetCellInfoNames(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ts := memorytopo.NewServer("cell1", "cell2", "cell3")
	vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
		return NewVtctldServer(ts)
	})

	resp, err := vtctld.GetCellInfoNames(ctx, &vtctldatapb.GetCellInfoNamesRequest{})
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"cell1", "cell2", "cell3"}, resp.Names)

	ts = memorytopo.NewServer()
	vtctld = testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
		return NewVtctldServer(ts)
	})

	resp, err = vtctld.GetCellInfoNames(ctx, &vtctldatapb.GetCellInfoNamesRequest{})
	assert.NoError(t, err)
	assert.Empty(t, resp.Names)

	ts, topofactory := memorytopo.NewServerAndFactory("cell1")
	vtctld = testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
		return NewVtctldServer(ts)
	})

	topofactory.SetError(assert.AnError)
	_, err = vtctld.GetCellInfoNames(ctx, &vtctldatapb.GetCellInfoNamesRequest{})
	assert.Error(t, err)
}

func TestGetCellInfo(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ts := memorytopo.NewServer()
	vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
		return NewVtctldServer(ts)
	})

	expected := &topodatapb.CellInfo{
		ServerAddress: "example.com",
		Root:          "vitess",
	}
	input := *expected // shallow copy
	require.NoError(t, ts.CreateCellInfo(ctx, "cell1", &input))

	resp, err := vtctld.GetCellInfo(ctx, &vtctldatapb.GetCellInfoRequest{Cell: "cell1"})
	assert.NoError(t, err)
	assert.Equal(t, expected, resp.CellInfo)

	_, err = vtctld.GetCellInfo(ctx, &vtctldatapb.GetCellInfoRequest{Cell: "does_not_exist"})
	assert.Error(t, err)

	_, err = vtctld.GetCellInfo(ctx, &vtctldatapb.GetCellInfoRequest{})
	assert.Error(t, err)
}

func TestGetCellsAliases(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ts := memorytopo.NewServer("c11", "c12", "c13", "c21", "c22")
	vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
		return NewVtctldServer(ts)
	})

	alias1 := &topodatapb.CellsAlias{
		Cells: []string{"c11", "c12", "c13"},
	}
	alias2 := &topodatapb.CellsAlias{
		Cells: []string{"c21", "c22"},
	}

	for i, alias := range []*topodatapb.CellsAlias{alias1, alias2} {
		input := *alias // shallow copy
		name := fmt.Sprintf("a%d", i+1)

		require.NoError(t, ts.CreateCellsAlias(ctx, name, &input), "cannot create cells alias %d (idx = %d) = %+v", i+1, i, &input)
	}

	expected := map[string]*topodatapb.CellsAlias{
		"a1": alias1,
		"a2": alias2,
	}

	resp, err := vtctld.GetCellsAliases(ctx, &vtctldatapb.GetCellsAliasesRequest{})
	assert.NoError(t, err)
	assert.Equal(t, expected, resp.Aliases)

	ts, topofactory := memorytopo.NewServerAndFactory()
	vtctld = testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
		return NewVtctldServer(ts)
	})

	topofactory.SetError(assert.AnError)
	_, err = vtctld.GetCellsAliases(ctx, &vtctldatapb.GetCellsAliasesRequest{})
	assert.Error(t, err)
}

func TestGetKeyspaces(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ts, topofactory := memorytopo.NewServerAndFactory("cell1")
	vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
		return NewVtctldServer(ts)
	})

	resp, err := vtctld.GetKeyspaces(ctx, &vtctldatapb.GetKeyspacesRequest{})
	assert.NoError(t, err)
	assert.Empty(t, resp.Keyspaces)

	expected := []*vtctldatapb.Keyspace{
		{
			Name: "ks1",
			Keyspace: &topodatapb.Keyspace{
				ShardingColumnName: "ks1_col1",
			},
		},
		{
			Name: "ks2",
			Keyspace: &topodatapb.Keyspace{
				ShardingColumnName: "ks2_col1",
			},
		},
		{
			Name: "ks3",
			Keyspace: &topodatapb.Keyspace{
				ShardingColumnName: "ks3_col1",
			},
		},
	}
	for _, ks := range expected {
		testutil.AddKeyspace(ctx, t, ts, ks)
	}

	resp, err = vtctld.GetKeyspaces(ctx, &vtctldatapb.GetKeyspacesRequest{})
	assert.NoError(t, err)
	assert.Equal(t, expected, resp.Keyspaces)

	topofactory.SetError(errors.New("error from toposerver"))

	_, err = vtctld.GetKeyspaces(ctx, &vtctldatapb.GetKeyspacesRequest{})
	assert.Error(t, err)
}

func TestGetTablet(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ts := memorytopo.NewServer("cell1")
	vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
		return NewVtctldServer(ts)
	})

	tablet := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "cell1",
			Uid:  100,
		},
		Hostname: "localhost",
		Keyspace: "testkeyspace",
		Shard:    "-",
		Type:     topodatapb.TabletType_REPLICA,
	}

	testutil.AddTablet(ctx, t, ts, tablet, nil)

	resp, err := vtctld.GetTablet(ctx, &vtctldatapb.GetTabletRequest{
		TabletAlias: &topodatapb.TabletAlias{
			Cell: "cell1",
			Uid:  100,
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, resp.Tablet, tablet)

	// not found
	_, err = vtctld.GetTablet(ctx, &vtctldatapb.GetTabletRequest{
		TabletAlias: &topodatapb.TabletAlias{
			Cell: "cell1",
			Uid:  101,
		},
	})
	assert.Error(t, err)
}

func TestGetSchema(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("zone1")
	tmc := testutil.TabletManagerClient{
		GetSchemaResults: map[string]struct {
			Schema *tabletmanagerdatapb.SchemaDefinition
			Error  error
		}{},
	}
	vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, &tmc, func(ts *topo.Server) vtctlservicepb.VtctldServer {
		return NewVtctldServer(ts)
	})

	validAlias := &topodatapb.TabletAlias{
		Cell: "zone1",
		Uid:  100,
	}
	testutil.AddTablet(ctx, t, ts, &topodatapb.Tablet{
		Alias: validAlias,
	}, nil)
	otherAlias := &topodatapb.TabletAlias{
		Cell: "zone1",
		Uid:  101,
	}
	testutil.AddTablet(ctx, t, ts, &topodatapb.Tablet{
		Alias: otherAlias,
	}, nil)

	// we need to run this on each test case or they will pollute each other
	setupSchema := func() {
		tmc.GetSchemaResults[topoproto.TabletAliasString(validAlias)] = struct {
			Schema *tabletmanagerdatapb.SchemaDefinition
			Error  error
		}{
			Schema: &tabletmanagerdatapb.SchemaDefinition{
				DatabaseSchema: "CREATE DATABASE vt_testkeyspace",
				TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
					{
						Name: "t1",
						Schema: `CREATE TABLE t1 (
	id int(11) not null,
	PRIMARY KEY (id)
);`,
						Type:       "BASE",
						Columns:    []string{"id"},
						DataLength: 100,
						RowCount:   50,
						Fields: []*querypb.Field{
							{
								Name: "id",
								Type: querypb.Type_INT32,
							},
						},
					},
				},
			},
			Error: nil,
		}
	}

	tests := []*struct {
		name      string
		req       *vtctldatapb.GetSchemaRequest
		expected  *vtctldatapb.GetSchemaResponse
		shouldErr bool
	}{
		{
			name: "normal path",
			req: &vtctldatapb.GetSchemaRequest{
				TabletAlias: validAlias,
			},
			expected: &vtctldatapb.GetSchemaResponse{
				Schema: &tabletmanagerdatapb.SchemaDefinition{
					DatabaseSchema: "CREATE DATABASE vt_testkeyspace",
					TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
						{
							Name: "t1",
							Schema: `CREATE TABLE t1 (
	id int(11) not null,
	PRIMARY KEY (id)
);`,
							Type:       "BASE",
							Columns:    []string{"id"},
							DataLength: 100,
							RowCount:   50,
							Fields: []*querypb.Field{
								{
									Name: "id",
									Type: querypb.Type_INT32,
								},
							},
						},
					},
				},
			},
			shouldErr: false,
		},
		{
			name: "table names only",
			req: &vtctldatapb.GetSchemaRequest{
				TabletAlias:    validAlias,
				TableNamesOnly: true,
			},
			expected: &vtctldatapb.GetSchemaResponse{
				Schema: &tabletmanagerdatapb.SchemaDefinition{
					DatabaseSchema: "CREATE DATABASE vt_testkeyspace",
					TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
						{
							Name: "t1",
						},
					},
				},
			},
			shouldErr: false,
		},
		{
			name: "table sizes only",
			req: &vtctldatapb.GetSchemaRequest{
				TabletAlias:    validAlias,
				TableSizesOnly: true,
			},
			expected: &vtctldatapb.GetSchemaResponse{
				Schema: &tabletmanagerdatapb.SchemaDefinition{
					DatabaseSchema: "CREATE DATABASE vt_testkeyspace",
					TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
						{
							Name:       "t1",
							Type:       "BASE",
							DataLength: 100,
							RowCount:   50,
						},
					},
				},
			},
			shouldErr: false,
		},
		{
			name: "table names take precedence over table sizes",
			req: &vtctldatapb.GetSchemaRequest{
				TabletAlias:    validAlias,
				TableNamesOnly: true,
				TableSizesOnly: true,
			},
			expected: &vtctldatapb.GetSchemaResponse{
				Schema: &tabletmanagerdatapb.SchemaDefinition{
					DatabaseSchema: "CREATE DATABASE vt_testkeyspace",
					TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
						{
							Name: "t1",
						},
					},
				},
			},
			shouldErr: false,
		},
		// error cases
		{
			name: "no tablet",
			req: &vtctldatapb.GetSchemaRequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "notfound",
					Uid:  100,
				},
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "no schema",
			req: &vtctldatapb.GetSchemaRequest{
				TabletAlias: otherAlias,
			},
			expected:  nil,
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setupSchema()

			resp, err := vtctld.GetSchema(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.expected, resp)
		})
	}
}

func TestGetShard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		topo      []*vtctldatapb.Shard
		topoError error
		req       *vtctldatapb.GetShardRequest
		expected  *vtctldatapb.GetShardResponse
		shouldErr bool
	}{
		{
			name: "success",
			topo: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			topoError: nil,
			req: &vtctldatapb.GetShardRequest{
				Keyspace:  "testkeyspace",
				ShardName: "-",
			},
			expected: &vtctldatapb.GetShardResponse{
				Shard: &vtctldatapb.Shard{
					Keyspace: "testkeyspace",
					Name:     "-",
					Shard: &topodatapb.Shard{
						KeyRange:        &topodatapb.KeyRange{},
						IsMasterServing: true,
					},
				},
			},
			shouldErr: false,
		},
		{
			name:      "shard not found",
			topo:      nil,
			topoError: nil,
			req: &vtctldatapb.GetShardRequest{
				Keyspace:  "testkeyspace",
				ShardName: "-",
			},
			shouldErr: true,
		},
		{
			name: "unavailable topo server",
			topo: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			topoError: assert.AnError,
			req:       &vtctldatapb.GetShardRequest{},
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cells := []string{"zone1", "zone2", "zone3"}

			ctx := context.Background()
			ts, topofactory := memorytopo.NewServerAndFactory(cells...)
			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})

			testutil.AddShards(ctx, t, ts, tt.topo...)

			if tt.topoError != nil {
				topofactory.SetError(tt.topoError)
			}

			resp, err := vtctld.GetShard(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			assert.Equal(t, tt.expected, resp)
		})
	}
}

func TestGetSrvKeyspaces(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		cells        []string
		srvKeyspaces []*testutil.SrvKeyspace
		topoErr      error
		req          *vtctldatapb.GetSrvKeyspacesRequest
		expected     *vtctldatapb.GetSrvKeyspacesResponse
		shouldErr    bool
	}{
		{
			name:  "success",
			cells: []string{"zone1", "zone2"},
			srvKeyspaces: []*testutil.SrvKeyspace{
				{
					Cell:     "zone1",
					Keyspace: "testkeyspace",
					SrvKeyspace: &topodatapb.SrvKeyspace{
						ShardingColumnName: "zone1-sharding-col",
					},
				},
				{
					Cell:     "zone2",
					Keyspace: "testkeyspace",
					SrvKeyspace: &topodatapb.SrvKeyspace{
						ShardingColumnName: "zone2-sharding-col",
					},
				},
			},
			req: &vtctldatapb.GetSrvKeyspacesRequest{
				Keyspace: "testkeyspace",
			},
			expected: &vtctldatapb.GetSrvKeyspacesResponse{
				SrvKeyspaces: map[string]*topodatapb.SrvKeyspace{
					"zone1": {
						ShardingColumnName: "zone1-sharding-col",
					},
					"zone2": {
						ShardingColumnName: "zone2-sharding-col",
					},
				},
			},
			shouldErr: false,
		},
		{
			name:  "filtering by cell",
			cells: []string{"zone1", "zone2"},
			srvKeyspaces: []*testutil.SrvKeyspace{
				{
					Cell:     "zone1",
					Keyspace: "testkeyspace",
					SrvKeyspace: &topodatapb.SrvKeyspace{
						ShardingColumnName: "zone1-sharding-col",
					},
				},
				{
					Cell:     "zone2",
					Keyspace: "testkeyspace",
					SrvKeyspace: &topodatapb.SrvKeyspace{
						ShardingColumnName: "zone2-sharding-col",
					},
				},
			},
			req: &vtctldatapb.GetSrvKeyspacesRequest{
				Keyspace: "testkeyspace",
				Cells:    []string{"zone1"},
			},
			expected: &vtctldatapb.GetSrvKeyspacesResponse{
				SrvKeyspaces: map[string]*topodatapb.SrvKeyspace{
					"zone1": {
						ShardingColumnName: "zone1-sharding-col",
					},
				},
			},
			shouldErr: false,
		},
		{
			name:  "no srvkeyspace for single cell",
			cells: []string{"zone1", "zone2"},
			srvKeyspaces: []*testutil.SrvKeyspace{
				{
					Cell:     "zone1",
					Keyspace: "testkeyspace",
					SrvKeyspace: &topodatapb.SrvKeyspace{
						ShardingColumnName: "zone1-sharding-col",
					},
				},
			},
			req: &vtctldatapb.GetSrvKeyspacesRequest{
				Keyspace: "testkeyspace",
			},
			expected: &vtctldatapb.GetSrvKeyspacesResponse{
				SrvKeyspaces: map[string]*topodatapb.SrvKeyspace{
					"zone1": {
						ShardingColumnName: "zone1-sharding-col",
					},
					"zone2": nil,
				},
			},
			shouldErr: false,
		},
		{
			name:  "error getting cell names",
			cells: []string{"zone1"},
			srvKeyspaces: []*testutil.SrvKeyspace{
				{
					Cell:     "zone1",
					Keyspace: "testkeyspace",
					SrvKeyspace: &topodatapb.SrvKeyspace{
						ShardingColumnName: "zone1-sharding-col",
					},
				},
			},
			topoErr: assert.AnError,
			req: &vtctldatapb.GetSrvKeyspacesRequest{
				Keyspace: "testkeyspace",
			},
			shouldErr: true,
		},
		{
			name:  "error getting srvkeyspace",
			cells: []string{"zone1"},
			srvKeyspaces: []*testutil.SrvKeyspace{
				{
					Cell:     "zone1",
					Keyspace: "testkeyspace",
					SrvKeyspace: &topodatapb.SrvKeyspace{
						ShardingColumnName: "zone1-sharding-col",
					},
				},
			},
			topoErr: assert.AnError,
			req: &vtctldatapb.GetSrvKeyspacesRequest{
				Keyspace: "testkeyspace",
				Cells:    []string{"zone1"},
			},
			shouldErr: true,
		},
	}

	ctx := context.Background()

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if tt.req == nil {
				t.SkipNow()
			}

			ts, topofactory := memorytopo.NewServerAndFactory(tt.cells...)

			testutil.AddSrvKeyspaces(t, ts, tt.srvKeyspaces...)
			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})

			if tt.topoErr != nil {
				topofactory.SetError(tt.topoErr)
			}

			resp, err := vtctld.GetSrvKeyspaces(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)

				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.expected, resp)
		})
	}
}

func TestGetSrvVSchema(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ts, topofactory := memorytopo.NewServerAndFactory("zone1", "zone2")
	vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
		return NewVtctldServer(ts)
	})

	zone1SrvVSchema := &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"testkeyspace": {
				Sharded:                true,
				RequireExplicitRouting: false,
			},
		},
		RoutingRules: &vschemapb.RoutingRules{
			Rules: []*vschemapb.RoutingRule{},
		},
	}
	zone2SrvVSchema := &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"testkeyspace": {
				Sharded:                true,
				RequireExplicitRouting: false,
			},
			"unsharded": {
				Sharded:                false,
				RequireExplicitRouting: false,
			},
		},
		RoutingRules: &vschemapb.RoutingRules{
			Rules: []*vschemapb.RoutingRule{},
		},
	}

	err := ts.UpdateSrvVSchema(ctx, "zone1", zone1SrvVSchema)
	require.NoError(t, err, "cannot add zone1 srv vschema")
	err = ts.UpdateSrvVSchema(ctx, "zone2", zone2SrvVSchema)
	require.NoError(t, err, "cannot add zone2 srv vschema")

	expected := &vschemapb.SrvVSchema{ // have to copy our structs because of proto marshal artifacts
		Keyspaces: map[string]*vschemapb.Keyspace{
			"testkeyspace": {
				Sharded:                true,
				RequireExplicitRouting: false,
			},
		},
		RoutingRules: &vschemapb.RoutingRules{
			Rules: []*vschemapb.RoutingRule{},
		},
	}
	resp, err := vtctld.GetSrvVSchema(ctx, &vtctldatapb.GetSrvVSchemaRequest{Cell: "zone1"})
	assert.NoError(t, err)
	assert.Equal(t, expected.Keyspaces, resp.SrvVSchema.Keyspaces, "GetSrvVSchema(zone1) mismatch")
	assert.ElementsMatch(t, expected.RoutingRules.Rules, resp.SrvVSchema.RoutingRules.Rules, "GetSrvVSchema(zone1) rules mismatch")

	expected = &vschemapb.SrvVSchema{ // have to copy our structs because of proto marshal artifacts
		Keyspaces: map[string]*vschemapb.Keyspace{
			"testkeyspace": {
				Sharded:                true,
				RequireExplicitRouting: false,
			},
			"unsharded": {
				Sharded:                false,
				RequireExplicitRouting: false,
			},
		},
		RoutingRules: &vschemapb.RoutingRules{
			Rules: []*vschemapb.RoutingRule{},
		},
	}
	resp, err = vtctld.GetSrvVSchema(ctx, &vtctldatapb.GetSrvVSchemaRequest{Cell: "zone2"})
	assert.NoError(t, err)
	assert.Equal(t, expected.Keyspaces, resp.SrvVSchema.Keyspaces, "GetSrvVSchema(zone2) mismatch %+v %+v", zone2SrvVSchema.Keyspaces["testkeyspace"], resp.SrvVSchema.Keyspaces["testkeyspace"])
	assert.ElementsMatch(t, expected.RoutingRules.Rules, resp.SrvVSchema.RoutingRules.Rules, "GetSrvVSchema(zone2) rules mismatch")

	resp, err = vtctld.GetSrvVSchema(ctx, &vtctldatapb.GetSrvVSchemaRequest{Cell: "dne"})
	assert.Error(t, err, "GetSrvVSchema(dne)")
	assert.Nil(t, resp, "GetSrvVSchema(dne)")

	topofactory.SetError(assert.AnError)
	_, err = vtctld.GetSrvVSchema(ctx, &vtctldatapb.GetSrvVSchemaRequest{Cell: "zone1"})
	assert.Error(t, err)
}

func TestGetTablets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		cells     []string
		tablets   []*topodatapb.Tablet
		req       *vtctldatapb.GetTabletsRequest
		expected  []*topodatapb.Tablet
		shouldErr bool
	}{
		{
			name:      "no tablets",
			cells:     []string{"cell1"},
			tablets:   []*topodatapb.Tablet{},
			req:       &vtctldatapb.GetTabletsRequest{},
			expected:  []*topodatapb.Tablet{},
			shouldErr: false,
		},
		{
			name:  "keyspace and shard filter",
			cells: []string{"cell1"},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  100,
					},
					Keyspace: "ks1",
					Shard:    "-80",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  101,
					},
					Keyspace: "ks1",
					Shard:    "80-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  102,
					},
					Keyspace: "ks2",
					Shard:    "-",
				},
			},
			req: &vtctldatapb.GetTabletsRequest{
				Keyspace: "ks1",
				Shard:    "80-",
			},
			expected: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  101,
					},
					Keyspace: "ks1",
					Shard:    "80-",
				},
			},
			shouldErr: false,
		},
		{
			name:  "keyspace filter",
			cells: []string{"cell1"},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  100,
					},
					Keyspace: "ks1",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  101,
					},
					Keyspace: "ks1",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  102,
					},
					Keyspace: "otherkeyspace",
				},
			},
			req: &vtctldatapb.GetTabletsRequest{
				Keyspace: "ks1",
			},
			expected: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  100,
					},
					Keyspace: "ks1",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  101,
					},
					Keyspace: "ks1",
				},
			},
			shouldErr: false,
		},
		{
			name:  "keyspace and shard filter - stale primary",
			cells: []string{"cell1"},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  100,
					},
					Keyspace: "ks1",
					Shard:    "-80",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  101,
					},
					Keyspace: "ks1",
					Shard:    "80-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  102,
					},
					Keyspace:            "ks2",
					Shard:               "-",
					Type:                topodatapb.TabletType_MASTER,
					MasterTermStartTime: logutil.TimeToProto(time.Date(2006, time.January, 2, 15, 4, 5, 0, time.UTC)),
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  103,
					},
					Keyspace:            "ks2",
					Shard:               "-",
					Hostname:            "stale.primary",
					Type:                topodatapb.TabletType_MASTER,
					MasterTermStartTime: logutil.TimeToProto(time.Date(2006, time.January, 2, 14, 4, 5, 0, time.UTC)),
				},
			},
			req: &vtctldatapb.GetTabletsRequest{
				Keyspace: "ks2",
				Shard:    "-",
			},
			expected: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  102,
					},
					Keyspace:            "ks2",
					Shard:               "-",
					Type:                topodatapb.TabletType_MASTER,
					MasterTermStartTime: logutil.TimeToProto(time.Date(2006, time.January, 2, 15, 4, 5, 0, time.UTC)),
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  103,
					},
					Keyspace:            "ks2",
					Shard:               "-",
					Hostname:            "stale.primary",
					Type:                topodatapb.TabletType_UNKNOWN,
					MasterTermStartTime: logutil.TimeToProto(time.Date(2006, time.January, 2, 14, 4, 5, 0, time.UTC)),
				},
			},
			shouldErr: false,
		},
		{
			name:  "stale primary",
			cells: []string{"cell1"},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  100,
					},
					Keyspace:            "ks1",
					Shard:               "-",
					Hostname:            "slightly less stale",
					Type:                topodatapb.TabletType_MASTER,
					MasterTermStartTime: logutil.TimeToProto(time.Date(2006, time.January, 2, 15, 4, 5, 0, time.UTC)),
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  101,
					},
					Hostname:            "stale primary",
					Keyspace:            "ks1",
					Shard:               "-",
					Type:                topodatapb.TabletType_MASTER,
					MasterTermStartTime: logutil.TimeToProto(time.Date(2006, time.January, 2, 14, 4, 5, 0, time.UTC)),
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  103,
					},
					Hostname:            "true primary",
					Keyspace:            "ks1",
					Shard:               "-",
					Type:                topodatapb.TabletType_MASTER,
					MasterTermStartTime: logutil.TimeToProto(time.Date(2006, time.January, 2, 16, 4, 5, 0, time.UTC)),
				},
			},
			req: &vtctldatapb.GetTabletsRequest{},
			expected: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  100,
					},
					Keyspace:            "ks1",
					Shard:               "-",
					Hostname:            "slightly less stale",
					Type:                topodatapb.TabletType_UNKNOWN,
					MasterTermStartTime: logutil.TimeToProto(time.Date(2006, time.January, 2, 15, 4, 5, 0, time.UTC)),
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  101,
					},
					Hostname:            "stale primary",
					Keyspace:            "ks1",
					Shard:               "-",
					Type:                topodatapb.TabletType_UNKNOWN,
					MasterTermStartTime: logutil.TimeToProto(time.Date(2006, time.January, 2, 14, 4, 5, 0, time.UTC)),
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  103,
					},
					Hostname:            "true primary",
					Keyspace:            "ks1",
					Shard:               "-",
					Type:                topodatapb.TabletType_MASTER,
					MasterTermStartTime: logutil.TimeToProto(time.Date(2006, time.January, 2, 16, 4, 5, 0, time.UTC)),
				},
			},
			shouldErr: false,
		},
		{
			name:    "keyspace and shard filter - error",
			cells:   []string{"cell1"},
			tablets: []*topodatapb.Tablet{},
			req: &vtctldatapb.GetTabletsRequest{
				Keyspace: "ks1",
				Shard:    "-",
			},
			expected:  []*topodatapb.Tablet{},
			shouldErr: true,
		},
		{
			name:  "cells filter",
			cells: []string{"cell1", "cell2", "cell3"},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  100,
					},
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell2",
						Uid:  200,
					},
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell3",
						Uid:  300,
					},
				},
			},
			req: &vtctldatapb.GetTabletsRequest{
				Cells: []string{"cell1", "cell3"},
			},
			expected: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  100,
					},
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell3",
						Uid:  300,
					},
				},
			},
			shouldErr: false,
		},
		{
			name:  "cells filter with single error is nonfatal",
			cells: []string{"cell1"},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  100,
					},
					Keyspace: "ks1",
					Shard:    "-",
				},
			},
			req: &vtctldatapb.GetTabletsRequest{
				Cells: []string{"cell1", "doesnotexist"},
			},
			expected: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  100,
					},
					Keyspace: "ks1",
					Shard:    "-",
				},
			},
			shouldErr: false,
		},
		{
			name:  "cells filter with single error is fatal in strict mode",
			cells: []string{"cell1"},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  100,
					},
					Keyspace: "ks1",
					Shard:    "-",
				},
			},
			req: &vtctldatapb.GetTabletsRequest{
				Cells:  []string{"cell1", "doesnotexist"},
				Strict: true,
			},
			shouldErr: true,
		},
		{
			name:  "in nonstrict mode if all cells fail the request fails",
			cells: []string{"cell1"},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  100,
					},
					Keyspace: "ks1",
					Shard:    "-",
				},
			},
			req: &vtctldatapb.GetTabletsRequest{
				Cells: []string{"doesnotexist", "alsodoesnotexist"},
			},
			shouldErr: true,
		},
		{
			name:  "tablet alias filtering",
			cells: []string{"zone1"},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},
			req: &vtctldatapb.GetTabletsRequest{
				TabletAliases: []*topodatapb.TabletAlias{
					{
						Cell: "zone1",
						Uid:  100,
					},
					{
						// This tablet doesn't exist, but doesn't cause a failure.
						Cell: "zone404",
						Uid:  404,
					},
				},
				// The below filters are ignored, because TabletAliases always
				// takes precedence.
				Keyspace: "another_keyspace",
				Shard:    "-80",
				Cells:    []string{"zone404"},
			},
			expected: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},
			shouldErr: false,
		},
		{
			name:    "tablet alias filter with none found",
			tablets: []*topodatapb.Tablet{},
			req: &vtctldatapb.GetTabletsRequest{
				TabletAliases: []*topodatapb.TabletAlias{
					{
						Cell: "zone1",
						Uid:  101,
					},
				},
			},
			expected:  []*topodatapb.Tablet{},
			shouldErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			ts := memorytopo.NewServer(tt.cells...)
			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})

			testutil.AddTablets(ctx, t, ts, nil, tt.tablets...)

			resp, err := vtctld.GetTablets(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.ElementsMatch(t, tt.expected, resp.Tablets)
		})
	}
}

func TestGetVSchema(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ts := memorytopo.NewServer("zone1")
	vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
		return NewVtctldServer(ts)
	})

	t.Run("found", func(t *testing.T) {
		t.Parallel()

		err := ts.SaveVSchema(ctx, "testkeyspace", &vschemapb.Keyspace{
			Sharded: true,
			Vindexes: map[string]*vschemapb.Vindex{
				"v1": {
					Type: "hash",
				},
			},
		})
		require.NoError(t, err)

		expected := &vtctldatapb.GetVSchemaResponse{
			VSchema: &vschemapb.Keyspace{
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"v1": {
						Type: "hash",
					},
				},
			},
		}

		resp, err := vtctld.GetVSchema(ctx, &vtctldatapb.GetVSchemaRequest{
			Keyspace: "testkeyspace",
		})
		assert.NoError(t, err)
		assert.Equal(t, expected, resp)
	})

	t.Run("not found", func(t *testing.T) {
		t.Parallel()

		_, err := vtctld.GetVSchema(ctx, &vtctldatapb.GetVSchemaRequest{
			Keyspace: "doesnotexist",
		})
		assert.Error(t, err)
	})
}

func TestPlannedReparentShard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		ts      *topo.Server
		tmc     tmclient.TabletManagerClient
		tablets []*topodatapb.Tablet

		req                 *vtctldatapb.PlannedReparentShardRequest
		expected            *vtctldatapb.PlannedReparentShardResponse
		expectEventsToOccur bool
		shouldErr           bool
	}{
		{
			name: "successful reparent",
			ts:   memorytopo.NewServer("zone1"),
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type: topodatapb.TabletType_MASTER,
					MasterTermStartTime: &vttime.Time{
						Seconds: 100,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  200,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
					Type:     topodatapb.TabletType_RDONLY,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},
			tmc: &testutil.TabletManagerClient{
				DemoteMasterResults: map[string]struct {
					Status *replicationdatapb.MasterStatus
					Error  error
				}{
					"zone1-0000000100": {
						Status: &replicationdatapb.MasterStatus{
							Position: "primary-demotion position",
						},
						Error: nil,
					},
				},
				MasterPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000100": {
						Position: "doesn't matter",
						Error:    nil,
					},
				},
				PopulateReparentJournalResults: map[string]error{
					"zone1-0000000200": nil,
				},
				PromoteReplicaResults: map[string]struct {
					Result string
					Error  error
				}{
					"zone1-0000000200": {
						Result: "promotion position",
						Error:  nil,
					},
				},
				SetMasterResults: map[string]error{
					"zone1-0000000200": nil, // waiting for master-position during promotion
					// reparent SetMaster calls
					"zone1-0000000100": nil,
					"zone1-0000000101": nil,
				},
				WaitForPositionResults: map[string]map[string]error{
					"zone1-0000000200": {
						"primary-demotion position": nil,
					},
				},
			},
			req: &vtctldatapb.PlannedReparentShardRequest{
				Keyspace: "testkeyspace",
				Shard:    "-",
				NewPrimary: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  200,
				},
				WaitReplicasTimeout: protoutil.DurationToProto(time.Millisecond * 10),
			},
			expected: &vtctldatapb.PlannedReparentShardResponse{
				Keyspace: "testkeyspace",
				Shard:    "-",
				PromotedPrimary: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  200,
				},
			},
			expectEventsToOccur: true,
			shouldErr:           false,
		},
		{
			// Note: this is testing the error-handling done in
			// (*VtctldServer).PlannedReparentShard, not the logic of an PRS.
			// That logic is tested in reparentutil, and not here. Therefore,
			// the simplest way to trigger a failure is to attempt an PRS on a
			// shard that does not exist.
			name:    "failed reparent",
			ts:      memorytopo.NewServer("zone1"),
			tablets: nil,
			req: &vtctldatapb.PlannedReparentShardRequest{
				Keyspace: "testkeyspace",
				Shard:    "-",
			},
			expectEventsToOccur: false,
			shouldErr:           true,
		},
		{
			name: "invalid WaitReplicasTimeout",
			req: &vtctldatapb.PlannedReparentShardRequest{
				WaitReplicasTimeout: &vttime.Duration{
					Seconds: -1,
					Nanos:   1,
				},
			},
			shouldErr: true,
		},
	}

	ctx := context.Background()

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			testutil.AddTablets(ctx, t, tt.ts, &testutil.AddTabletOptions{
				AlsoSetShardMaster:  true,
				ForceSetShardMaster: true,
				SkipShardCreation:   false,
			}, tt.tablets...)

			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, tt.ts, tt.tmc, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})
			resp, err := vtctld.PlannedReparentShard(ctx, tt.req)

			// We defer this because we want to check in both error and non-
			// error cases, but after the main set of assertions for those
			// cases.
			defer func() {
				if !tt.expectEventsToOccur {
					testutil.AssertNoLogutilEventsOccurred(t, resp, "expected no events to occur during ERS")

					return
				}

				testutil.AssertLogutilEventsOccurred(t, resp, "expected events to occur during ERS")
			}()

			if tt.shouldErr {
				assert.Error(t, err)

				return
			}

			assert.NoError(t, err)
			testutil.AssertPlannedReparentShardResponsesEqual(t, *tt.expected, *resp)
		})
	}
}

func TestRemoveKeyspaceCell(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                    string
		keyspace                *vtctldatapb.Keyspace
		shards                  []*vtctldatapb.Shard
		topoError               error
		topoIsLocked            bool
		srvKeyspaceDoesNotExist bool
		req                     *vtctldatapb.RemoveKeyspaceCellRequest
		expected                *vtctldatapb.RemoveKeyspaceCellResponse
		shouldErr               bool
	}{
		{
			name:     "success",
			keyspace: nil,
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			topoError:               nil,
			topoIsLocked:            false,
			srvKeyspaceDoesNotExist: false,
			req: &vtctldatapb.RemoveKeyspaceCellRequest{
				Keyspace: "testkeyspace",
				Cell:     "zone1",
			},
			expected:  &vtctldatapb.RemoveKeyspaceCellResponse{},
			shouldErr: false,
		},
		{
			name: "success/empty keyspace",
			keyspace: &vtctldatapb.Keyspace{
				Name:     "testkeyspace",
				Keyspace: &topodatapb.Keyspace{},
			},
			shards:                  nil,
			topoError:               nil,
			topoIsLocked:            false,
			srvKeyspaceDoesNotExist: false,
			req: &vtctldatapb.RemoveKeyspaceCellRequest{
				Keyspace: "testkeyspace",
				Cell:     "zone1",
			},
			expected:  &vtctldatapb.RemoveKeyspaceCellResponse{},
			shouldErr: false,
		},
		{
			name: "keyspace not found",
			keyspace: &vtctldatapb.Keyspace{
				Name:     "otherkeyspace",
				Keyspace: &topodatapb.Keyspace{},
			},
			shards:                  nil,
			topoError:               nil,
			topoIsLocked:            false,
			srvKeyspaceDoesNotExist: false,
			req: &vtctldatapb.RemoveKeyspaceCellRequest{
				Keyspace: "testkeyspace",
				Cell:     "zone1",
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name:     "topo is down",
			keyspace: nil,
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			topoError:               assert.AnError,
			topoIsLocked:            false,
			srvKeyspaceDoesNotExist: false,
			req: &vtctldatapb.RemoveKeyspaceCellRequest{
				Keyspace: "testkeyspace",
				Cell:     "zone1",
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name:     "topo is locked",
			keyspace: nil,
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			topoError:               nil,
			topoIsLocked:            true,
			srvKeyspaceDoesNotExist: false,
			req: &vtctldatapb.RemoveKeyspaceCellRequest{
				Keyspace: "testkeyspace",
				Cell:     "zone1",
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name:     "srvkeyspace already deleted",
			keyspace: nil,
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			topoError:               nil,
			topoIsLocked:            false,
			srvKeyspaceDoesNotExist: true,
			req: &vtctldatapb.RemoveKeyspaceCellRequest{
				Keyspace: "testkeyspace",
				Cell:     "zone1",
			},
			expected:  nil,
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cells := []string{"zone1", "zone2", "zone3"}

			ctx := context.Background()
			ts, topofactory := memorytopo.NewServerAndFactory(cells...)
			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})

			// Setup topo
			if tt.keyspace != nil {
				testutil.AddKeyspace(ctx, t, ts, tt.keyspace)
			}

			testutil.AddShards(ctx, t, ts, tt.shards...)

			// For certain tests, we don't actually create the SrvKeyspace
			// object.
			if !tt.srvKeyspaceDoesNotExist {
				updateSrvKeyspace := func(keyspace string) {
					for _, cell := range cells {
						err := ts.UpdateSrvKeyspace(ctx, cell, keyspace, &topodatapb.SrvKeyspace{})
						require.NoError(t, err, "could not create empty SrvKeyspace for keyspace %s in cell %s", tt.req.Keyspace, cell)
					}
				}

				updateSrvKeyspace(tt.req.Keyspace)
				if tt.keyspace != nil {
					updateSrvKeyspace(tt.keyspace.Name)
				}
			}

			// Set errors and locks
			if tt.topoError != nil {
				topofactory.SetError(tt.topoError)
			}

			if tt.topoIsLocked {
				lctx, unlock, err := ts.LockKeyspace(ctx, tt.req.Keyspace, "testing locked keyspace")
				require.NoError(t, err, "cannot lock keyspace %s", tt.req.Keyspace)
				defer unlock(&err)

				ctx = lctx
			}

			resp, err := vtctld.RemoveKeyspaceCell(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.expected, resp)
		})
	}
}

func TestRemoveShardCell(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		servingCells      []string
		shards            []*vtctldatapb.Shard
		replicationGraphs []*topo.ShardReplicationInfo
		topoError         error
		topoIsLocked      bool
		req               *vtctldatapb.RemoveShardCellRequest
		expected          *vtctldatapb.RemoveShardCellResponse
		shouldErr         bool
	}{
		{
			name: "success",
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			replicationGraphs: []*topo.ShardReplicationInfo{
				topo.NewShardReplicationInfo(&topodatapb.ShardReplication{
					Nodes: []*topodatapb.ShardReplication_Node{
						{
							TabletAlias: &topodatapb.TabletAlias{
								Cell: "zone1",
								Uid:  100,
							},
						},
					},
				}, "zone1", "testkeyspace", "-"),
				topo.NewShardReplicationInfo(&topodatapb.ShardReplication{
					Nodes: []*topodatapb.ShardReplication_Node{
						{
							TabletAlias: &topodatapb.TabletAlias{
								Cell: "zone2",
								Uid:  200,
							},
						},
					},
				}, "zone2", "testkeyspace", "-"),
				topo.NewShardReplicationInfo(&topodatapb.ShardReplication{
					Nodes: []*topodatapb.ShardReplication_Node{
						{
							TabletAlias: &topodatapb.TabletAlias{
								Cell: "zone3",
								Uid:  300,
							},
						},
					},
				}, "zone3", "testkeyspace", "-"),
			},
			req: &vtctldatapb.RemoveShardCellRequest{
				Keyspace:  "testkeyspace",
				ShardName: "-",
				Cell:      "zone2",
				Recursive: true,
			},
			expected:  &vtctldatapb.RemoveShardCellResponse{},
			shouldErr: false,
		},
		{
			name: "success/no tablets",
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			req: &vtctldatapb.RemoveShardCellRequest{
				Keyspace:  "testkeyspace",
				ShardName: "-",
				Cell:      "zone2",
			},
			expected:  &vtctldatapb.RemoveShardCellResponse{},
			shouldErr: false,
		},
		{
			name:              "nonexistent shard",
			shards:            nil,
			replicationGraphs: nil,
			req: &vtctldatapb.RemoveShardCellRequest{
				Keyspace:  "testkeyspace",
				ShardName: "-",
				Cell:      "zone2",
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "cell does not exist",
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			req: &vtctldatapb.RemoveShardCellRequest{
				Keyspace:  "testkeyspace",
				ShardName: "-",
				Cell:      "fourthzone",
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "cell not in serving list",
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			servingCells:      []string{"zone1"},
			replicationGraphs: nil,
			req: &vtctldatapb.RemoveShardCellRequest{
				Keyspace:  "testkeyspace",
				ShardName: "-",
				Cell:      "zone2",
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "tablets/non-recursive",
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			replicationGraphs: []*topo.ShardReplicationInfo{
				topo.NewShardReplicationInfo(&topodatapb.ShardReplication{
					Nodes: []*topodatapb.ShardReplication_Node{
						{
							TabletAlias: &topodatapb.TabletAlias{
								Cell: "zone1",
								Uid:  100,
							},
						},
					},
				}, "zone1", "testkeyspace", "-"),
				topo.NewShardReplicationInfo(&topodatapb.ShardReplication{
					Nodes: []*topodatapb.ShardReplication_Node{
						{
							TabletAlias: &topodatapb.TabletAlias{
								Cell: "zone2",
								Uid:  200,
							},
						},
					},
				}, "zone2", "testkeyspace", "-"),
				topo.NewShardReplicationInfo(&topodatapb.ShardReplication{
					Nodes: []*topodatapb.ShardReplication_Node{
						{
							TabletAlias: &topodatapb.TabletAlias{
								Cell: "zone3",
								Uid:  300,
							},
						},
					},
				}, "zone3", "testkeyspace", "-"),
			},
			req: &vtctldatapb.RemoveShardCellRequest{
				Keyspace:  "testkeyspace",
				ShardName: "-",
				Cell:      "zone2",
				Recursive: false, // non-recursive + replication graph = failure
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "topo server down",
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			replicationGraphs: nil,
			req: &vtctldatapb.RemoveShardCellRequest{
				Keyspace:  "testkeyspace",
				ShardName: "-",
				Cell:      "zone2",
			},
			topoError:    assert.AnError,
			topoIsLocked: false,
			expected:     nil,
			shouldErr:    true,
		},
		// Not sure how to set up this test case.
		// {
		// 	name: "topo server down for replication check/no force",
		// },
		// Not sure how to set up this test case.
		// {
		// 	name: "topo server down for replication check/force",
		// },
		{
			name: "cannot lock keyspace",
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
				},
			},
			replicationGraphs: nil,
			req: &vtctldatapb.RemoveShardCellRequest{
				Keyspace:  "testkeyspace",
				ShardName: "-",
				Cell:      "zone2",
			},
			topoError:    nil,
			topoIsLocked: true,
			expected:     nil,
			shouldErr:    true,
		},
		// Not sure how to set up this test case.
		// {
		// 	name: "cannot delete srvkeyspace partition",
		// },
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cells := []string{"zone1", "zone2", "zone3"}

			ctx := context.Background()
			ts, topofactory := memorytopo.NewServerAndFactory(cells...)
			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})

			// Setup shard topos and replication graphs.
			testutil.AddShards(ctx, t, ts, tt.shards...)
			testutil.SetupReplicationGraphs(ctx, t, ts, tt.replicationGraphs...)

			// Set up srvkeyspace partitions; a little gross.
			servingCells := tt.servingCells
			if servingCells == nil { // we expect an explicit empty list to have a shard with no serving cells
				servingCells = cells
			}

			for _, shard := range tt.shards {
				lctx, unlock, lerr := ts.LockKeyspace(ctx, shard.Keyspace, "initializing serving graph for test")
				require.NoError(t, lerr, "cannot lock keyspace %s to initialize serving graph", shard.Keyspace)

				for _, cell := range servingCells {

					err := ts.UpdateSrvKeyspace(lctx, cell, shard.Keyspace, &topodatapb.SrvKeyspace{
						Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
							{
								ServedType: topodatapb.TabletType_REPLICA,
								ShardReferences: []*topodatapb.ShardReference{
									{
										Name: shard.Name,
									},
								},
							},
						},
					})
					require.NoError(t, err, "cannot update srvkeyspace for %s/%s in cell %v", shard.Keyspace, shard.Name, cell)
				}

				unlock(&lerr)
			}

			// Set errors and locks.
			if tt.topoError != nil {
				topofactory.SetError(tt.topoError)
			}

			if tt.topoIsLocked {
				lctx, unlock, err := ts.LockKeyspace(ctx, tt.req.Keyspace, "testing locked keyspace")
				require.NoError(t, err, "cannot lock keyspace %s", tt.req.Keyspace)
				defer unlock(&err)

				// Need to use the lock ctx in the RPC call so we fail when
				// attempting to lock the keyspace rather than waiting forever
				// for the lock. Explicitly setting a deadline would be another
				// way to achieve this.
				ctx = lctx
			}

			// Make the RPC and assert things about it.
			resp, err := vtctld.RemoveShardCell(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.expected, resp)
		})
	}
}

func TestReparentTablet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		tmc       tmclient.TabletManagerClient
		tablets   []*topodatapb.Tablet
		shards    []*vtctldatapb.Shard
		topoErr   error
		req       *vtctldatapb.ReparentTabletRequest
		expected  *vtctldatapb.ReparentTabletResponse
		shouldErr bool
	}{
		{
			name: "success",
			tmc: &testutil.TabletManagerClient{
				SetMasterResults: map[string]error{
					"zone1-0000000100": nil,
				},
			},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone2",
						Uid:  200,
					},
					Type:     topodatapb.TabletType_MASTER,
					Keyspace: "testkeyspace",
					Shard:    "-",
					MasterTermStartTime: &vttime.Time{
						Seconds: 1000,
					},
				},
			},
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
					Shard: &topodatapb.Shard{
						MasterAlias: &topodatapb.TabletAlias{
							Cell: "zone2",
							Uid:  200,
						},
						MasterTermStartTime: &vttime.Time{
							Seconds: 1000,
						},
						IsMasterServing: true,
					},
				},
			},
			req: &vtctldatapb.ReparentTabletRequest{
				Tablet: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			expected: &vtctldatapb.ReparentTabletResponse{
				Keyspace: "testkeyspace",
				Shard:    "-",
				Primary: &topodatapb.TabletAlias{
					Cell: "zone2",
					Uid:  200,
				},
			},
			shouldErr: false,
		},
		{
			name: "tablet is nil",
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone2",
						Uid:  200,
					},
					Type:     topodatapb.TabletType_MASTER,
					Keyspace: "testkeyspace",
					Shard:    "-",
					MasterTermStartTime: &vttime.Time{
						Seconds: 1000,
					},
				},
			},
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
					Shard: &topodatapb.Shard{
						MasterAlias: &topodatapb.TabletAlias{
							Cell: "zone2",
							Uid:  200,
						},
						MasterTermStartTime: &vttime.Time{
							Seconds: 1000,
						},
						IsMasterServing: true,
					},
				},
			},
			req: &vtctldatapb.ReparentTabletRequest{
				Tablet: nil,
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "tablet not in topo",
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone2",
						Uid:  200,
					},
					Type:     topodatapb.TabletType_MASTER,
					Keyspace: "testkeyspace",
					Shard:    "-",
					MasterTermStartTime: &vttime.Time{
						Seconds: 1000,
					},
				},
			},
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
					Shard: &topodatapb.Shard{
						MasterAlias: &topodatapb.TabletAlias{
							Cell: "zone2",
							Uid:  200,
						},
						MasterTermStartTime: &vttime.Time{
							Seconds: 1000,
						},
						IsMasterServing: true,
					},
				},
			},
			req: &vtctldatapb.ReparentTabletRequest{
				Tablet: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "shard not in topo",
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone2",
						Uid:  200,
					},
					Type:     topodatapb.TabletType_MASTER,
					Keyspace: "testkeyspace",
					Shard:    "-",
					MasterTermStartTime: &vttime.Time{
						Seconds: 1000,
					},
				},
			},
			shards: nil,
			req: &vtctldatapb.ReparentTabletRequest{
				Tablet: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "shard has no primary",
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
					Shard: &topodatapb.Shard{
						IsMasterServing: false,
					},
				},
			},
			req: &vtctldatapb.ReparentTabletRequest{
				Tablet: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "shard primary not in topo",
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone2",
						Uid:  200,
					},
					Type:     topodatapb.TabletType_MASTER,
					Keyspace: "testkeyspace",
					Shard:    "-",
					MasterTermStartTime: &vttime.Time{
						Seconds: 1000,
					},
				},
			},
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
					Shard: &topodatapb.Shard{
						MasterAlias: &topodatapb.TabletAlias{
							Cell: "zone3",
							Uid:  300,
						},
						MasterTermStartTime: &vttime.Time{
							Seconds: 1010,
						},
						IsMasterServing: true,
					},
				},
			},
			req: &vtctldatapb.ReparentTabletRequest{
				Tablet: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "shard primary is not type MASTER",
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone2",
						Uid:  200,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
					Shard: &topodatapb.Shard{
						MasterAlias: &topodatapb.TabletAlias{
							Cell: "zone2",
							Uid:  200,
						},
						MasterTermStartTime: &vttime.Time{
							Seconds: 1010,
						},
						IsMasterServing: true,
					},
				},
			},
			req: &vtctldatapb.ReparentTabletRequest{
				Tablet: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "shard primary is not actually in shard",
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone2",
						Uid:  200,
					},
					Type:     topodatapb.TabletType_MASTER,
					Keyspace: "otherkeyspace",
					Shard:    "-",
					MasterTermStartTime: &vttime.Time{
						Seconds: 1000,
					},
				},
			},
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
					Shard: &topodatapb.Shard{
						MasterAlias: &topodatapb.TabletAlias{
							Cell: "zone2",
							Uid:  200,
						},
						MasterTermStartTime: &vttime.Time{
							Seconds: 1010,
						},
						IsMasterServing: true,
					},
				},
			},
			req: &vtctldatapb.ReparentTabletRequest{
				Tablet: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "tmc.SetMaster failure",
			tmc: &testutil.TabletManagerClient{
				SetMasterResults: map[string]error{
					"zone1-0000000100": assert.AnError,
				},
			},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone2",
						Uid:  200,
					},
					Type:     topodatapb.TabletType_MASTER,
					Keyspace: "testkeyspace",
					Shard:    "-",
					MasterTermStartTime: &vttime.Time{
						Seconds: 1000,
					},
				},
			},
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
					Shard: &topodatapb.Shard{
						MasterAlias: &topodatapb.TabletAlias{
							Cell: "zone2",
							Uid:  200,
						},
						MasterTermStartTime: &vttime.Time{
							Seconds: 1000,
						},
						IsMasterServing: true,
					},
				},
			},
			req: &vtctldatapb.ReparentTabletRequest{
				Tablet: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "topo is down",
			tmc: &testutil.TabletManagerClient{
				SetMasterResults: map[string]error{
					"zone1-0000000100": nil,
				},
			},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone2",
						Uid:  200,
					},
					Type:     topodatapb.TabletType_MASTER,
					Keyspace: "testkeyspace",
					Shard:    "-",
					MasterTermStartTime: &vttime.Time{
						Seconds: 1000,
					},
				},
			},
			shards: []*vtctldatapb.Shard{
				{
					Keyspace: "testkeyspace",
					Name:     "-",
					Shard: &topodatapb.Shard{
						MasterAlias: &topodatapb.TabletAlias{
							Cell: "zone2",
							Uid:  200,
						},
						MasterTermStartTime: &vttime.Time{
							Seconds: 1000,
						},
						IsMasterServing: true,
					},
				},
			},
			topoErr: assert.AnError,
			req: &vtctldatapb.ReparentTabletRequest{
				Tablet: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			expected:  nil,
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if tt.req == nil {
				t.Skip("focused on other test cases right now")
			}

			cells := []string{"zone1", "zone2", "zone3"}

			ctx := context.Background()
			ts, topofactory := memorytopo.NewServerAndFactory(cells...)
			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, tt.tmc, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})

			testutil.AddTablets(ctx, t, ts, &testutil.AddTabletOptions{
				SkipShardCreation: true,
			}, tt.tablets...)
			testutil.AddShards(ctx, t, ts, tt.shards...)

			if tt.topoErr != nil {
				topofactory.SetError(tt.topoErr)
			}

			resp, err := vtctld.ReparentTablet(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.expected, resp)
		})
	}
}

func TestShardReplicationPositions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		ts         *topo.Server
		tablets    []*topodatapb.Tablet
		tmc        tmclient.TabletManagerClient
		ctxTimeout time.Duration
		req        *vtctldatapb.ShardReplicationPositionsRequest
		expected   *vtctldatapb.ShardReplicationPositionsResponse
		shouldErr  bool
	}{
		{
			name: "success",
			ts:   memorytopo.NewServer("zone1"),
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
					Type:     topodatapb.TabletType_MASTER,
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
					Type:     topodatapb.TabletType_REPLICA,
				},
			},
			tmc: &testutil.TabletManagerClient{
				MasterPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000100": {
						Position: "primary_tablet_position",
					},
				},
				ReplicationStatusResults: map[string]struct {
					Position *replicationdatapb.Status
					Error    error
				}{
					"zone1-0000000101": {
						Position: &replicationdatapb.Status{
							Position: "replica_tablet_position",
						},
					},
				},
			},
			req: &vtctldatapb.ShardReplicationPositionsRequest{
				Keyspace: "testkeyspace",
				Shard:    "-",
			},
			expected: &vtctldatapb.ShardReplicationPositionsResponse{
				ReplicationStatuses: map[string]*replicationdatapb.Status{
					"zone1-0000000100": {
						Position: "primary_tablet_position",
					},
					"zone1-0000000101": {
						Position: "replica_tablet_position",
					},
				},
				TabletMap: map[string]*topodatapb.Tablet{
					"zone1-0000000100": {
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  100,
						},
						Keyspace: "testkeyspace",
						Shard:    "-",
						Type:     topodatapb.TabletType_MASTER,
					},
					"zone1-0000000101": {
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  101,
						},
						Keyspace: "testkeyspace",
						Shard:    "-",
						Type:     topodatapb.TabletType_REPLICA,
					},
				},
			},
			shouldErr: false,
		},
		{
			name: "timeouts are nonfatal",
			ts:   memorytopo.NewServer("zone1"),
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
					Type:     topodatapb.TabletType_MASTER,
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
					Type:     topodatapb.TabletType_REPLICA,
				},
			},
			tmc: &testutil.TabletManagerClient{
				MasterPositionDelays: map[string]time.Duration{
					"zone1-0000000100": time.Millisecond * 100,
				},
				MasterPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000100": {
						Position: "primary_tablet_position",
					},
				},
				ReplicationStatusDelays: map[string]time.Duration{
					"zone1-0000000101": time.Millisecond * 100,
				},
				ReplicationStatusResults: map[string]struct {
					Position *replicationdatapb.Status
					Error    error
				}{
					"zone1-0000000101": {
						Position: &replicationdatapb.Status{
							Position: "replica_tablet_position",
						},
					},
				},
			},
			ctxTimeout: time.Millisecond * 10,
			req: &vtctldatapb.ShardReplicationPositionsRequest{
				Keyspace: "testkeyspace",
				Shard:    "-",
			},
			expected: &vtctldatapb.ShardReplicationPositionsResponse{
				ReplicationStatuses: map[string]*replicationdatapb.Status{
					"zone1-0000000100": nil,
					"zone1-0000000101": nil,
				},
				TabletMap: map[string]*topodatapb.Tablet{
					"zone1-0000000100": {
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  100,
						},
						Keyspace: "testkeyspace",
						Shard:    "-",
						Type:     topodatapb.TabletType_MASTER,
					},
					"zone1-0000000101": {
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  101,
						},
						Keyspace: "testkeyspace",
						Shard:    "-",
						Type:     topodatapb.TabletType_REPLICA,
					},
				},
			},
			shouldErr: false,
		},
		{
			name: "other rpc errors are fatal",
			ts:   memorytopo.NewServer("zone1"),
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
					Type:     topodatapb.TabletType_MASTER,
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
					Keyspace: "testkeyspace",
					Shard:    "-",
					Type:     topodatapb.TabletType_REPLICA,
				},
			},
			tmc: &testutil.TabletManagerClient{
				MasterPositionResults: map[string]struct {
					Position string
					Error    error
				}{
					"zone1-0000000100": {
						Error: assert.AnError,
					},
				},
				ReplicationStatusResults: map[string]struct {
					Position *replicationdatapb.Status
					Error    error
				}{
					"zone1-0000000101": {
						Position: &replicationdatapb.Status{
							Position: "replica_tablet_position",
						},
					},
				},
			},
			req: &vtctldatapb.ShardReplicationPositionsRequest{
				Keyspace: "testkeyspace",
				Shard:    "-",
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "nonexistent shard",
			ts:   memorytopo.NewServer("zone1"),
			req: &vtctldatapb.ShardReplicationPositionsRequest{
				Keyspace: "testkeyspace",
				Shard:    "-",
			},
			expected:  nil,
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()

			testutil.AddTablets(ctx, t, tt.ts, &testutil.AddTabletOptions{
				AlsoSetShardMaster: true,
				SkipShardCreation:  false,
			}, tt.tablets...)

			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, tt.ts, tt.tmc, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})

			if tt.ctxTimeout > 0 {
				_ctx, cancel := context.WithTimeout(ctx, tt.ctxTimeout)
				defer cancel()

				ctx = _ctx
			}

			resp, err := vtctld.ShardReplicationPositions(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)

				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.expected, resp)
		})
	}
}

func TestTabletExternallyReparented(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		topo         []*topodatapb.Tablet
		topoErr      error
		tmcHasNoTopo bool
		req          *vtctldatapb.TabletExternallyReparentedRequest
		expected     *vtctldatapb.TabletExternallyReparentedResponse
		shouldErr    bool
		expectedTopo []*topodatapb.Tablet
	}{
		{
			name: "success",
			topo: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_MASTER,
					Keyspace: "testkeyspace",
					Shard:    "-",
					MasterTermStartTime: &vttime.Time{
						Seconds: 1000,
					},
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone2",
						Uid:  200,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone3",
						Uid:  300,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},
			topoErr: nil,
			req: &vtctldatapb.TabletExternallyReparentedRequest{
				Tablet: &topodatapb.TabletAlias{
					Cell: "zone2",
					Uid:  200,
				},
			},
			expected: &vtctldatapb.TabletExternallyReparentedResponse{
				Keyspace: "testkeyspace",
				Shard:    "-",
				NewPrimary: &topodatapb.TabletAlias{
					Cell: "zone2",
					Uid:  200,
				},
				OldPrimary: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			shouldErr: false,
			// NOTE: this seems weird, right? Why is the old primary still a
			// MASTER, and why is the new primary's term start 0,0? Well, our
			// test client implementation is a little incomplete. See
			// ./testutil/test_tmclient.go for reference.
			expectedTopo: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_MASTER,
					Keyspace: "testkeyspace",
					Shard:    "-",
					MasterTermStartTime: &vttime.Time{
						Seconds: 1000,
					},
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone2",
						Uid:  200,
					},
					Type:                topodatapb.TabletType_UNKNOWN,
					Keyspace:            "testkeyspace",
					Shard:               "-",
					MasterTermStartTime: &vttime.Time{},
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone3",
						Uid:  300,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},
		},
		{
			name:    "tablet is nil",
			topo:    nil,
			topoErr: nil,
			req: &vtctldatapb.TabletExternallyReparentedRequest{
				Tablet: nil,
			},
			expected:     nil,
			shouldErr:    true,
			expectedTopo: nil,
		},
		{
			name: "topo is down",
			topo: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_MASTER,
					Keyspace: "testkeyspace",
					Shard:    "-",
					MasterTermStartTime: &vttime.Time{
						Seconds: 1000,
					},
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone2",
						Uid:  200,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone3",
						Uid:  300,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},
			topoErr: assert.AnError,
			req: &vtctldatapb.TabletExternallyReparentedRequest{
				Tablet: &topodatapb.TabletAlias{
					Cell: "zone2",
					Uid:  200,
				},
			},
			expected:  nil,
			shouldErr: true,
			expectedTopo: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_MASTER,
					Keyspace: "testkeyspace",
					Shard:    "-",
					MasterTermStartTime: &vttime.Time{
						Seconds: 1000,
					},
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone2",
						Uid:  200,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone3",
						Uid:  300,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},
		},
		{
			name: "tablet is already primary",
			topo: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_MASTER,
					Keyspace: "testkeyspace",
					Shard:    "-",
					MasterTermStartTime: &vttime.Time{
						Seconds: 1000,
					},
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone2",
						Uid:  200,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone3",
						Uid:  300,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},
			topoErr: nil,
			req: &vtctldatapb.TabletExternallyReparentedRequest{
				Tablet: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			expected: &vtctldatapb.TabletExternallyReparentedResponse{
				Keyspace: "testkeyspace",
				Shard:    "-",
				NewPrimary: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
				OldPrimary: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			shouldErr: false,
			expectedTopo: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_MASTER,
					Keyspace: "testkeyspace",
					Shard:    "-",
					MasterTermStartTime: &vttime.Time{
						Seconds: 1000,
					},
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone2",
						Uid:  200,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone3",
						Uid:  300,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},
		},
		{
			name: "cannot change tablet type",
			topo: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_MASTER,
					Keyspace: "testkeyspace",
					Shard:    "-",
					MasterTermStartTime: &vttime.Time{
						Seconds: 1000,
					},
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone2",
						Uid:  200,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone3",
						Uid:  300,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},
			topoErr:      nil,
			tmcHasNoTopo: true,
			req: &vtctldatapb.TabletExternallyReparentedRequest{
				Tablet: &topodatapb.TabletAlias{
					Cell: "zone2",
					Uid:  200,
				},
			},
			expected:  nil,
			shouldErr: true,
			expectedTopo: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Type:     topodatapb.TabletType_MASTER,
					Keyspace: "testkeyspace",
					Shard:    "-",
					MasterTermStartTime: &vttime.Time{
						Seconds: 1000,
					},
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone2",
						Uid:  200,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone3",
						Uid:  300,
					},
					Type:     topodatapb.TabletType_REPLICA,
					Keyspace: "testkeyspace",
					Shard:    "-",
				},
			},
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cells := []string{"zone1", "zone2", "zone3"}

			ctx := context.Background()
			ts, topofactory := memorytopo.NewServerAndFactory(cells...)
			tmc := testutil.TabletManagerClient{
				TopoServer: ts,
			}
			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, ts, &tmc, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return NewVtctldServer(ts)
			})

			if tt.tmcHasNoTopo {
				// For certain test cases, we want specifically just the
				// ChangeType call to fail, which is why we rely on a separate
				// bool rather than using tt.topoErr.
				tmc.TopoServer = nil
			}

			testutil.AddTablets(ctx, t, ts, &testutil.AddTabletOptions{
				AlsoSetShardMaster: true,
			}, tt.topo...)

			if tt.topoErr != nil {
				topofactory.SetError(tt.topoErr)
			}

			if tt.expectedTopo != nil {
				// assert on expectedTopo state when we've fininished the rest
				// of the test.
				defer func() {
					topofactory.SetError(nil)

					ctx, cancel := context.WithTimeout(ctx, time.Millisecond*10)
					defer cancel()

					resp, err := vtctld.GetTablets(ctx, &vtctldatapb.GetTabletsRequest{})
					require.NoError(t, err, "cannot get all tablets in the topo")
					assert.ElementsMatch(t, tt.expectedTopo, resp.Tablets)
				}()
			}

			resp, err := vtctld.TabletExternallyReparented(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.expected, resp)
		})
	}
}
