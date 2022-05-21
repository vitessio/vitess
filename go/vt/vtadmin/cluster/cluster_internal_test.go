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

package cluster

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"k8s.io/apimachinery/pkg/util/sets"

	"vitess.io/vitess/go/pools"
	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vitessdriver"
	"vitess.io/vitess/go/vt/vtadmin/cache"
	"vitess.io/vitess/go/vt/vtadmin/cluster/internal/caches/schemacache"
	"vitess.io/vitess/go/vt/vtadmin/cluster/resolver"
	vtadminvtctldclient "vitess.io/vitess/go/vt/vtadmin/vtctldclient"
	"vitess.io/vitess/go/vt/vtadmin/vtctldclient/fakevtctldclient"
	"vitess.io/vitess/go/vt/vtadmin/vtsql"
	"vitess.io/vitess/go/vt/vtadmin/vtsql/fakevtsql"
	"vitess.io/vitess/go/vt/vtctl/vtctldclient"

	logutilpb "vitess.io/vitess/go/vt/proto/logutil"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

type vtctldProxy struct {
	vtctldclient.VtctldClient
	dialErr error
}

func (fake *vtctldProxy) Dial(ctx context.Context) error { return fake.dialErr }

func TestDeleteTablets(t *testing.T) {
	t.Parallel()

	testClusterProto := &vtadminpb.Cluster{
		Id:   "test",
		Name: "test",
	}

	tests := []struct {
		name      string
		cluster   *Cluster
		timeout   time.Duration
		setup     func(t testing.TB, c *Cluster)
		req       *vtctldatapb.DeleteTabletsRequest
		expected  *vtctldatapb.DeleteTabletsResponse
		shouldErr bool
	}{
		{
			name: "ok",
			cluster: &Cluster{
				Vtctld: &fakevtctldclient.VtctldClient{
					DeleteTabletsResults: map[string]error{
						"zone1-0000000100,zone1-0000000101": nil,
					},
				},
				topoRWPool: pools.NewRPCPool(1, time.Millisecond*100, nil),
			},
			req: &vtctldatapb.DeleteTabletsRequest{
				TabletAliases: []*topodatapb.TabletAlias{
					{
						Cell: "zone1",
						Uid:  100,
					},
					{
						Cell: "zone1",
						Uid:  101,
					},
				},
			},
			expected: &vtctldatapb.DeleteTabletsResponse{},
		},
		{
			name: "error",
			cluster: &Cluster{
				Vtctld:     &fakevtctldclient.VtctldClient{},
				topoRWPool: pools.NewRPCPool(1, time.Millisecond*100, nil),
			},
			req: &vtctldatapb.DeleteTabletsRequest{
				TabletAliases: []*topodatapb.TabletAlias{
					{
						Cell: "zone1",
						Uid:  100,
					},
					{
						Cell: "zone1",
						Uid:  101,
					},
				},
			},
			shouldErr: true,
		},
		{
			name: "RPC pool full",
			cluster: &Cluster{
				Vtctld:     &fakevtctldclient.VtctldClient{},
				topoRWPool: pools.NewRPCPool(1, time.Millisecond*10, nil),
			},
			timeout: time.Millisecond * 50,
			setup: func(t testing.TB, c *Cluster) {
				err := c.topoRWPool.Acquire(context.Background())
				require.NoError(t, err, "failed to lock RPC pool")
				t.Cleanup(c.topoRWPool.Release)
			},
			req: &vtctldatapb.DeleteTabletsRequest{
				TabletAliases: []*topodatapb.TabletAlias{
					{
						Cell: "zone1",
						Uid:  100,
					},
					{
						Cell: "zone1",
						Uid:  101,
					},
				},
			},
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.cluster.ID = testClusterProto.Id
			tt.cluster.Name = testClusterProto.Name

			if tt.setup != nil {
				tt.setup(t, tt.cluster)
			}

			var (
				ctx    context.Context
				cancel context.CancelFunc
			)

			switch tt.timeout {
			case 0:
				ctx, cancel = context.WithCancel(context.Background())
			default:
				ctx, cancel = context.WithTimeout(context.Background(), tt.timeout)
			}
			defer cancel()

			resp, err := tt.cluster.DeleteTablets(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err, "expected error, got %+v", resp)
				return
			}

			require.NoError(t, err)
			utils.MustMatch(t, tt.expected, resp)
		})
	}
}

func TestEmergencyReparentShard(t *testing.T) {
	t.Parallel()

	testClusterProto := &vtadminpb.Cluster{
		Id:   "test",
		Name: "test",
	}

	tests := []struct {
		name      string
		cluster   *Cluster
		setup     func(t testing.TB, c *Cluster)
		timeout   time.Duration
		req       *vtctldatapb.EmergencyReparentShardRequest
		expected  *vtadminpb.EmergencyReparentShardResponse
		shouldErr bool
	}{
		{
			name: "ok",
			cluster: &Cluster{
				ID:   "test",
				Name: "test",
				Vtctld: &fakevtctldclient.VtctldClient{
					EmergencyReparentShardResults: map[string]struct {
						Response *vtctldatapb.EmergencyReparentShardResponse
						Error    error
					}{
						"ks1/-": {
							Response: &vtctldatapb.EmergencyReparentShardResponse{
								Keyspace: "ks1",
								Shard:    "-",
								PromotedPrimary: &topodatapb.TabletAlias{
									Cell: "zone1",
									Uid:  100,
								},
								Events: []*logutilpb.Event{{}, {}, {}},
							},
						},
						"ks2/-80": {
							Error: fmt.Errorf("some error: %w", assert.AnError),
						},
					},
				},
				emergencyReparentPool: pools.NewRPCPool(1, time.Second, nil),
			},
			req: &vtctldatapb.EmergencyReparentShardRequest{
				Keyspace: "ks1",
				Shard:    "-",
				NewPrimary: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			expected: &vtadminpb.EmergencyReparentShardResponse{
				Cluster:  testClusterProto,
				Keyspace: "ks1",
				Shard:    "-",
				PromotedPrimary: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
				Events: []*logutilpb.Event{{}, {}, {}},
			},
		},
		{
			name: "error",
			cluster: &Cluster{
				ID:   "test",
				Name: "test",
				Vtctld: &fakevtctldclient.VtctldClient{
					EmergencyReparentShardResults: map[string]struct {
						Response *vtctldatapb.EmergencyReparentShardResponse
						Error    error
					}{
						"ks1/-": {
							Response: &vtctldatapb.EmergencyReparentShardResponse{
								Keyspace: "ks1",
								Shard:    "-",
								PromotedPrimary: &topodatapb.TabletAlias{
									Cell: "zone1",
									Uid:  100,
								},
								Events: []*logutilpb.Event{{}, {}, {}},
							},
						},
						"ks2/-80": {
							Error: fmt.Errorf("some error: %w", assert.AnError),
						},
					},
				},
				emergencyReparentPool: pools.NewRPCPool(1, time.Second, nil),
			},
			req: &vtctldatapb.EmergencyReparentShardRequest{
				Keyspace: "ks2",
				Shard:    "-80",
				NewPrimary: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  200,
				},
			},
			shouldErr: true,
		},
		{
			name: "pool full",
			cluster: &Cluster{
				ID:   "test",
				Name: "test",
				Vtctld: &fakevtctldclient.VtctldClient{
					EmergencyReparentShardResults: map[string]struct {
						Response *vtctldatapb.EmergencyReparentShardResponse
						Error    error
					}{
						"ks1/-": {
							Response: &vtctldatapb.EmergencyReparentShardResponse{
								Keyspace: "ks1",
								Shard:    "-",
								PromotedPrimary: &topodatapb.TabletAlias{
									Cell: "zone1",
									Uid:  100,
								},
								Events: []*logutilpb.Event{{}, {}, {}},
							},
						},
						"ks2/-80": {
							Error: fmt.Errorf("some error: %w", assert.AnError),
						},
					},
				},
				emergencyReparentPool: pools.NewRPCPool(1, time.Millisecond*25, nil),
			},
			setup: func(t testing.TB, c *Cluster) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
				defer cancel()

				err := c.emergencyReparentPool.Acquire(ctx)
				require.NoError(t, err, "could not block ers pool in setup")
				t.Cleanup(c.emergencyReparentPool.Release)
			},
			timeout: time.Millisecond * 50,
			req: &vtctldatapb.EmergencyReparentShardRequest{
				Keyspace: "ks1",
				Shard:    "-",
				NewPrimary: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if tt.setup != nil {
				func(t *testing.T) {
					t.Helper()
					tt.setup(t, tt.cluster)
				}(t)
			}

			var (
				ctx    context.Context
				cancel context.CancelFunc
			)
			switch tt.timeout {
			case 0:
				ctx, cancel = context.WithCancel(context.Background())
			default:
				ctx, cancel = context.WithTimeout(context.Background(), tt.timeout)
			}
			defer cancel()

			resp, err := tt.cluster.EmergencyReparentShard(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			utils.MustMatch(t, tt.expected, resp)
		})
	}
}

func Test_getShardSets(t *testing.T) {
	t.Parallel()

	c := &Cluster{
		Vtctld: &vtctldProxy{
			VtctldClient: &fakevtctldclient.VtctldClient{
				GetKeyspaceResults: map[string]struct {
					Response *vtctldatapb.GetKeyspaceResponse
					Error    error
				}{
					"ks1": {
						Response: &vtctldatapb.GetKeyspaceResponse{
							Keyspace: &vtctldatapb.Keyspace{
								Name:     "ks1",
								Keyspace: &topodatapb.Keyspace{},
							},
						},
					},
					"ks2": {
						Response: &vtctldatapb.GetKeyspaceResponse{
							Keyspace: &vtctldatapb.Keyspace{
								Name:     "ks2",
								Keyspace: &topodatapb.Keyspace{},
							},
						},
					},
					"ks3": {
						Error: topo.NewError(topo.NoNode, "ks3"), /* we need to fail in a particular way */
					},
				},
				GetKeyspacesResults: struct {
					Keyspaces []*vtctldatapb.Keyspace
					Error     error
				}{
					Keyspaces: []*vtctldatapb.Keyspace{
						{
							Name:     "ks1",
							Keyspace: &topodatapb.Keyspace{},
						},
						{
							Name:     "ks2",
							Keyspace: &topodatapb.Keyspace{},
						},
					},
				},
				FindAllShardsInKeyspaceResults: map[string]struct {
					Response *vtctldatapb.FindAllShardsInKeyspaceResponse
					Error    error
				}{
					"ks1": {
						Response: &vtctldatapb.FindAllShardsInKeyspaceResponse{
							Shards: map[string]*vtctldatapb.Shard{
								"-80": {
									Keyspace: "ks1",
									Name:     "-80",
									Shard:    &topodatapb.Shard{},
								},
								"80-": {
									Keyspace: "ks1",
									Name:     "80-",
									Shard:    &topodatapb.Shard{},
								},
							},
						},
					},
					"ks2": {
						Response: &vtctldatapb.FindAllShardsInKeyspaceResponse{
							Shards: map[string]*vtctldatapb.Shard{
								"-": {
									Keyspace: "ks2",
									Name:     "-",
									Shard:    &topodatapb.Shard{},
								},
							},
						},
					},
				},
			},
		},
		topoReadPool: pools.NewRPCPool(5, 0, nil),
	}

	tests := []struct {
		name           string
		keyspaces      []string
		keyspaceShards []string
		result         map[string]sets.String
		shouldErr      bool
	}{
		{
			name:           "all keyspaces and shards",
			keyspaces:      nil,
			keyspaceShards: nil,
			result: map[string]sets.String{
				"ks1": sets.NewString("-80", "80-"),
				"ks2": sets.NewString("-"),
			},
		},
		{
			name:           "keyspaceShards filter",
			keyspaces:      nil,
			keyspaceShards: []string{"ks1/-80", "ks2/-"},
			result: map[string]sets.String{
				"ks1": sets.NewString("-80"),
				"ks2": sets.NewString("-"),
			},
		},
		{
			name:           "keyspace and shards filters",
			keyspaces:      []string{"ks1"},
			keyspaceShards: []string{"ks1/80-"},
			result: map[string]sets.String{
				"ks1": sets.NewString("80-"),
			},
		},
		{
			name:           "skipped non-existing shards and keyspaces",
			keyspaces:      nil,
			keyspaceShards: []string{"ks1/-" /* does not exist */, "ks1/-80", "ks1/80-", "ks3/-" /* does not exist */},
			result: map[string]sets.String{
				"ks1": sets.NewString("-80", "80-"),
			},
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, err := c.getShardSets(context.Background(), tt.keyspaces, tt.keyspaceShards)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.result, result)
		})
	}
}

func TestPlannedReparentShard(t *testing.T) {
	t.Parallel()

	testClusterProto := &vtadminpb.Cluster{
		Id:   "test",
		Name: "test",
	}

	tests := []struct {
		name      string
		cluster   *Cluster
		setup     func(t testing.TB, c *Cluster)
		timeout   time.Duration
		req       *vtctldatapb.PlannedReparentShardRequest
		expected  *vtadminpb.PlannedReparentShardResponse
		shouldErr bool
	}{
		{
			name: "ok",
			cluster: &Cluster{
				ID:   "test",
				Name: "test",
				Vtctld: &fakevtctldclient.VtctldClient{
					PlannedReparentShardResults: map[string]struct {
						Response *vtctldatapb.PlannedReparentShardResponse
						Error    error
					}{
						"ks1/-": {
							Response: &vtctldatapb.PlannedReparentShardResponse{
								Keyspace: "ks1",
								Shard:    "-",
								PromotedPrimary: &topodatapb.TabletAlias{
									Cell: "zone1",
									Uid:  100,
								},
								Events: []*logutilpb.Event{{}, {}, {}},
							},
						},
						"ks2/-80": {
							Error: fmt.Errorf("some error: %w", assert.AnError),
						},
					},
				},
				reparentPool: pools.NewRPCPool(1, time.Second, nil),
			},
			req: &vtctldatapb.PlannedReparentShardRequest{
				Keyspace: "ks1",
				Shard:    "-",
				NewPrimary: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			expected: &vtadminpb.PlannedReparentShardResponse{
				Cluster:  testClusterProto,
				Keyspace: "ks1",
				Shard:    "-",
				PromotedPrimary: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
				Events: []*logutilpb.Event{{}, {}, {}},
			},
		},
		{
			name: "error",
			cluster: &Cluster{
				ID:   "test",
				Name: "test",
				Vtctld: &fakevtctldclient.VtctldClient{
					PlannedReparentShardResults: map[string]struct {
						Response *vtctldatapb.PlannedReparentShardResponse
						Error    error
					}{
						"ks1/-": {
							Response: &vtctldatapb.PlannedReparentShardResponse{
								Keyspace: "ks1",
								Shard:    "-",
								PromotedPrimary: &topodatapb.TabletAlias{
									Cell: "zone1",
									Uid:  100,
								},
								Events: []*logutilpb.Event{{}, {}, {}},
							},
						},
						"ks2/-80": {
							Error: fmt.Errorf("some error: %w", assert.AnError),
						},
					},
				},
				reparentPool: pools.NewRPCPool(1, time.Second, nil),
			},
			req: &vtctldatapb.PlannedReparentShardRequest{
				Keyspace: "ks2",
				Shard:    "-80",
				NewPrimary: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  200,
				},
			},
			shouldErr: true,
		},
		{
			name: "pool full",
			cluster: &Cluster{
				ID:   "test",
				Name: "test",
				Vtctld: &fakevtctldclient.VtctldClient{
					PlannedReparentShardResults: map[string]struct {
						Response *vtctldatapb.PlannedReparentShardResponse
						Error    error
					}{
						"ks1/-": {
							Response: &vtctldatapb.PlannedReparentShardResponse{
								Keyspace: "ks1",
								Shard:    "-",
								PromotedPrimary: &topodatapb.TabletAlias{
									Cell: "zone1",
									Uid:  100,
								},
								Events: []*logutilpb.Event{{}, {}, {}},
							},
						},
						"ks2/-80": {
							Error: fmt.Errorf("some error: %w", assert.AnError),
						},
					},
				},
				reparentPool: pools.NewRPCPool(1, time.Millisecond*25, nil),
			},
			setup: func(t testing.TB, c *Cluster) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
				defer cancel()

				err := c.reparentPool.Acquire(ctx)
				require.NoError(t, err, "could not block prs pool in setup")
				t.Cleanup(c.reparentPool.Release)
			},
			timeout: time.Millisecond * 50,
			req: &vtctldatapb.PlannedReparentShardRequest{
				Keyspace: "ks1",
				Shard:    "-",
				NewPrimary: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if tt.setup != nil {
				func(t *testing.T) {
					t.Helper()
					tt.setup(t, tt.cluster)
				}(t)
			}

			var (
				ctx    context.Context
				cancel context.CancelFunc
			)
			switch tt.timeout {
			case 0:
				ctx, cancel = context.WithCancel(context.Background())
			default:
				ctx, cancel = context.WithTimeout(context.Background(), tt.timeout)
			}
			defer cancel()

			resp, err := tt.cluster.PlannedReparentShard(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			utils.MustMatch(t, tt.expected, resp)
		})
	}
}

func TestRefreshState(t *testing.T) {
	t.Parallel()

	testClusterProto := &vtadminpb.Cluster{
		Id:   "test",
		Name: "test",
	}

	tests := []struct {
		name              string
		cluster           *Cluster
		timeout           time.Duration
		setup             func(t testing.TB, c *Cluster)
		tablet            *vtadminpb.Tablet
		assertion         func(t assert.TestingT, err error, msgAndArgs ...any) bool
		assertionMsgExtra []any
	}{
		{
			name: "ok",
			cluster: &Cluster{
				Vtctld: &fakevtctldclient.VtctldClient{
					RefreshStateResults: map[string]error{
						"zone1-0000000100": nil,
					},
				},
				topoReadPool: pools.NewRPCPool(1, time.Millisecond*100, nil),
			},
			tablet: &vtadminpb.Tablet{
				Tablet: &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
				},
			},
			assertion: assert.NoError,
		},
		{
			name: "error",
			cluster: &Cluster{
				Vtctld: &fakevtctldclient.VtctldClient{
					RefreshStateResults: map[string]error{
						"zone1-0000000100": fmt.Errorf("some error"),
					},
				},
				topoReadPool: pools.NewRPCPool(1, time.Millisecond*100, nil),
			},
			tablet: &vtadminpb.Tablet{
				Tablet: &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
				},
			},
			assertion: assert.Error,
		},
		{
			name: "RPC pool full",
			cluster: &Cluster{
				Vtctld:       &fakevtctldclient.VtctldClient{},
				topoReadPool: pools.NewRPCPool(1, time.Millisecond*10, nil),
			},
			timeout: time.Millisecond * 50,
			setup: func(t testing.TB, c *Cluster) {
				err := c.topoReadPool.Acquire(context.Background())
				require.NoError(t, err, "failed to lock RPC pool")
				t.Cleanup(c.topoReadPool.Release)
			},
			tablet: &vtadminpb.Tablet{
				Tablet: &topodatapb.Tablet{Alias: &topodatapb.TabletAlias{}},
			},
			assertion: assert.Error,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.cluster.ID = testClusterProto.Id
			tt.cluster.Name = testClusterProto.Name

			if tt.setup != nil {
				tt.setup(t, tt.cluster)
			}

			var (
				ctx    context.Context
				cancel context.CancelFunc
			)

			switch tt.timeout {
			case 0:
				ctx, cancel = context.WithCancel(context.Background())
			default:
				ctx, cancel = context.WithTimeout(context.Background(), tt.timeout)
			}
			defer cancel()

			err := tt.cluster.RefreshState(ctx, tt.tablet)
			tt.assertion(t, err, tt.assertionMsgExtra...)
		})
	}
}

func Test_reloadKeyspaceSchemas(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tests := []struct {
		name      string
		cluster   *Cluster
		req       *vtadminpb.ReloadSchemasRequest
		expected  []*vtadminpb.ReloadSchemasResponse_KeyspaceResult
		shouldErr bool
	}{
		{
			name: "ok",
			cluster: &Cluster{
				ID:   "test",
				Name: "test",
				Vtctld: &fakevtctldclient.VtctldClient{
					GetKeyspacesResults: struct {
						Keyspaces []*vtctldatapb.Keyspace
						Error     error
					}{
						Keyspaces: []*vtctldatapb.Keyspace{
							{
								Name: "ks1",
							},
							{
								Name: "ks2",
							},
						},
					},
					ReloadSchemaKeyspaceResults: map[string]struct {
						Response *vtctldatapb.ReloadSchemaKeyspaceResponse
						Error    error
					}{
						"ks1": {
							Response: &vtctldatapb.ReloadSchemaKeyspaceResponse{
								Events: []*logutilpb.Event{
									{}, {}, {},
								},
							},
						},
						"ks2": {
							Error: assert.AnError, // _would_ cause a failure but our request filters it out
						},
					},
				},
				topoReadPool: pools.NewRPCPool(5, 0, nil),
			},
			req: &vtadminpb.ReloadSchemasRequest{
				Keyspaces: []string{"ks1"},
			},
			expected: []*vtadminpb.ReloadSchemasResponse_KeyspaceResult{
				{
					Keyspace: &vtadminpb.Keyspace{
						Cluster: &vtadminpb.Cluster{
							Id:   "test",
							Name: "test",
						},
						Keyspace: &vtctldatapb.Keyspace{
							Name: "ks1",
						},
					},
					Events: []*logutilpb.Event{
						{}, {}, {},
					},
				},
			},
		},
		{
			name: "no keyspaces specified defaults to all",
			cluster: &Cluster{
				ID:   "test",
				Name: "test",
				Vtctld: &fakevtctldclient.VtctldClient{
					GetKeyspacesResults: struct {
						Keyspaces []*vtctldatapb.Keyspace
						Error     error
					}{
						Keyspaces: []*vtctldatapb.Keyspace{
							{
								Name: "ks1",
							},
							{
								Name: "ks2",
							},
						},
					},
					ReloadSchemaKeyspaceResults: map[string]struct {
						Response *vtctldatapb.ReloadSchemaKeyspaceResponse
						Error    error
					}{
						"ks1": {
							Response: &vtctldatapb.ReloadSchemaKeyspaceResponse{
								Events: []*logutilpb.Event{
									{},
								},
							},
						},
						"ks2": {
							Response: &vtctldatapb.ReloadSchemaKeyspaceResponse{
								Events: []*logutilpb.Event{
									{}, {},
								},
							},
						},
					},
				},
				topoReadPool: pools.NewRPCPool(5, 0, nil),
			},
			req: &vtadminpb.ReloadSchemasRequest{},
			expected: []*vtadminpb.ReloadSchemasResponse_KeyspaceResult{
				{
					Keyspace: &vtadminpb.Keyspace{
						Cluster: &vtadminpb.Cluster{
							Id:   "test",
							Name: "test",
						},
						Keyspace: &vtctldatapb.Keyspace{
							Name: "ks1",
						},
					},
					Events: []*logutilpb.Event{
						{},
					},
				},
				{
					Keyspace: &vtadminpb.Keyspace{
						Cluster: &vtadminpb.Cluster{
							Id:   "test",
							Name: "test",
						},
						Keyspace: &vtctldatapb.Keyspace{
							Name: "ks2",
						},
					},
					Events: []*logutilpb.Event{
						{}, {},
					},
				},
			},
		},
		{
			name: "skip keyspaces not in cluster",
			cluster: &Cluster{
				ID:   "test",
				Name: "test",
				Vtctld: &fakevtctldclient.VtctldClient{
					GetKeyspacesResults: struct {
						Keyspaces []*vtctldatapb.Keyspace
						Error     error
					}{
						Keyspaces: []*vtctldatapb.Keyspace{
							{
								Name: "ks1",
							},
						},
					},
					ReloadSchemaKeyspaceResults: map[string]struct {
						Response *vtctldatapb.ReloadSchemaKeyspaceResponse
						Error    error
					}{
						"ks1": {
							Response: &vtctldatapb.ReloadSchemaKeyspaceResponse{
								Events: []*logutilpb.Event{
									{}, {}, {},
								},
							},
						},
					},
				},
				topoReadPool: pools.NewRPCPool(5, 0, nil),
			},
			req: &vtadminpb.ReloadSchemasRequest{
				Keyspaces: []string{"ks1", "anotherclusterks1"},
			},
			expected: []*vtadminpb.ReloadSchemasResponse_KeyspaceResult{
				{
					Keyspace: &vtadminpb.Keyspace{
						Cluster: &vtadminpb.Cluster{
							Id:   "test",
							Name: "test",
						},
						Keyspace: &vtctldatapb.Keyspace{
							Name: "ks1",
						},
					},
					Events: []*logutilpb.Event{
						{}, {}, {},
					},
				},
			},
		},
		{
			name: "GetKeyspaces error",
			cluster: &Cluster{
				ID:   "test",
				Name: "test",
				Vtctld: &fakevtctldclient.VtctldClient{
					GetKeyspacesResults: struct {
						Keyspaces []*vtctldatapb.Keyspace
						Error     error
					}{
						Error: assert.AnError,
					},
				},
				topoReadPool: pools.NewRPCPool(5, 0, nil),
			},
			req:       &vtadminpb.ReloadSchemasRequest{},
			shouldErr: true,
		},
		{
			name: "ReloadSchemaKeyspace error",
			cluster: &Cluster{
				ID:   "test",
				Name: "test",
				Vtctld: &fakevtctldclient.VtctldClient{
					GetKeyspacesResults: struct {
						Keyspaces []*vtctldatapb.Keyspace
						Error     error
					}{
						Keyspaces: []*vtctldatapb.Keyspace{
							{
								Name: "ks1",
							},
							{
								Name: "ks2",
							},
						},
					},
					ReloadSchemaKeyspaceResults: map[string]struct {
						Response *vtctldatapb.ReloadSchemaKeyspaceResponse
						Error    error
					}{
						"ks1": {
							Response: &vtctldatapb.ReloadSchemaKeyspaceResponse{
								Events: []*logutilpb.Event{
									{}, {}, {},
								},
							},
						},
						"ks2": {
							Error: assert.AnError,
						},
					},
				},
				topoReadPool: pools.NewRPCPool(5, 0, nil),
			},
			req:       &vtadminpb.ReloadSchemasRequest{},
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			results, err := tt.cluster.reloadKeyspaceSchemas(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)

			sort.Slice(tt.expected, func(i, j int) bool {
				return tt.expected[i].Keyspace.Keyspace.Name < tt.expected[j].Keyspace.Keyspace.Name
			})
			sort.Slice(results, func(i, j int) bool {
				return results[i].Keyspace.Keyspace.Name < results[j].Keyspace.Keyspace.Name
			})
			utils.MustMatch(t, tt.expected, results)
		})
	}
}

func Test_reloadShardSchemas(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tests := []struct {
		name      string
		cluster   *Cluster
		req       *vtadminpb.ReloadSchemasRequest
		expected  []*vtadminpb.ReloadSchemasResponse_ShardResult
		shouldErr bool
	}{
		{
			name: "ok",
			cluster: &Cluster{
				ID:   "test",
				Name: "test",
				Vtctld: &fakevtctldclient.VtctldClient{
					FindAllShardsInKeyspaceResults: map[string]struct {
						Response *vtctldatapb.FindAllShardsInKeyspaceResponse
						Error    error
					}{
						"ks1": {
							Response: &vtctldatapb.FindAllShardsInKeyspaceResponse{
								Shards: map[string]*vtctldatapb.Shard{
									"-": {
										Keyspace: "ks1",
										Name:     "-",
									},
								},
							},
						},
						"ks2": {
							Response: &vtctldatapb.FindAllShardsInKeyspaceResponse{
								Shards: map[string]*vtctldatapb.Shard{
									"-80": {
										Keyspace: "ks2",
										Name:     "-80",
									},
									"80-": {
										Keyspace: "ks2",
										Name:     "80-",
									},
								},
							},
						},
					},
					GetKeyspaceResults: map[string]struct {
						Response *vtctldatapb.GetKeyspaceResponse
						Error    error
					}{
						"ks1": {
							Response: &vtctldatapb.GetKeyspaceResponse{
								Keyspace: &vtctldatapb.Keyspace{
									Name: "ks1",
								},
							},
						},
						"ks2": {
							Response: &vtctldatapb.GetKeyspaceResponse{
								Keyspace: &vtctldatapb.Keyspace{
									Name: "ks2",
								},
							},
						},
					},
					GetKeyspacesResults: struct {
						Keyspaces []*vtctldatapb.Keyspace
						Error     error
					}{
						Keyspaces: []*vtctldatapb.Keyspace{
							{
								Name: "ks1",
							},
							{
								Name: "ks2",
							},
						},
					},
					ReloadSchemaShardResults: map[string]struct {
						Response *vtctldatapb.ReloadSchemaShardResponse
						Error    error
					}{
						"ks1/-": {
							Response: &vtctldatapb.ReloadSchemaShardResponse{
								Events: []*logutilpb.Event{
									{},
								},
							},
						},
						"ks2/-80": {
							Response: &vtctldatapb.ReloadSchemaShardResponse{
								Events: []*logutilpb.Event{
									{}, {},
								},
							},
						},
						"ks2/80-": { // skipped via request params
							Error: assert.AnError,
						},
					},
				},
				topoReadPool: pools.NewRPCPool(5, 0, nil),
			},
			req: &vtadminpb.ReloadSchemasRequest{
				KeyspaceShards: []string{"ks1/-", "ks2/-80"},
			},
			expected: []*vtadminpb.ReloadSchemasResponse_ShardResult{
				{
					Shard: &vtadminpb.Shard{
						Cluster: &vtadminpb.Cluster{
							Id:   "test",
							Name: "test",
						},
						Shard: &vtctldatapb.Shard{
							Keyspace: "ks1",
							Name:     "-",
						},
					},
					Events: []*logutilpb.Event{
						{},
					},
				},
				{
					Shard: &vtadminpb.Shard{
						Cluster: &vtadminpb.Cluster{
							Id:   "test",
							Name: "test",
						},
						Shard: &vtctldatapb.Shard{
							Keyspace: "ks2",
							Name:     "-80",
						},
					},
					Events: []*logutilpb.Event{
						{}, {},
					},
				},
			},
		},
		{
			name: "one shard fails",
			cluster: &Cluster{
				ID:   "test",
				Name: "test",
				Vtctld: &fakevtctldclient.VtctldClient{
					FindAllShardsInKeyspaceResults: map[string]struct {
						Response *vtctldatapb.FindAllShardsInKeyspaceResponse
						Error    error
					}{
						"ks1": {
							Response: &vtctldatapb.FindAllShardsInKeyspaceResponse{
								Shards: map[string]*vtctldatapb.Shard{
									"-": {
										Keyspace: "ks1",
										Name:     "-",
									},
								},
							},
						},
						"ks2": {
							Response: &vtctldatapb.FindAllShardsInKeyspaceResponse{
								Shards: map[string]*vtctldatapb.Shard{
									"-80": {
										Keyspace: "ks2",
										Name:     "-80",
									},
									"80-": {
										Keyspace: "ks2",
										Name:     "80-",
									},
								},
							},
						},
					},
					GetKeyspaceResults: map[string]struct {
						Response *vtctldatapb.GetKeyspaceResponse
						Error    error
					}{
						"ks1": {
							Response: &vtctldatapb.GetKeyspaceResponse{
								Keyspace: &vtctldatapb.Keyspace{
									Name: "ks1",
								},
							},
						},
						"ks2": {
							Response: &vtctldatapb.GetKeyspaceResponse{
								Keyspace: &vtctldatapb.Keyspace{
									Name: "ks2",
								},
							},
						},
					},
					GetKeyspacesResults: struct {
						Keyspaces []*vtctldatapb.Keyspace
						Error     error
					}{
						Keyspaces: []*vtctldatapb.Keyspace{
							{
								Name: "ks1",
							},
							{
								Name: "ks2",
							},
						},
					},
					ReloadSchemaShardResults: map[string]struct {
						Response *vtctldatapb.ReloadSchemaShardResponse
						Error    error
					}{
						"ks1/-": {
							Response: &vtctldatapb.ReloadSchemaShardResponse{
								Events: []*logutilpb.Event{
									{},
								},
							},
						},
						"ks2/-80": {
							Response: &vtctldatapb.ReloadSchemaShardResponse{
								Events: []*logutilpb.Event{
									{}, {},
								},
							},
						},
						"ks2/80-": {
							Error: assert.AnError,
						},
					},
				},
				topoReadPool: pools.NewRPCPool(5, 0, nil),
			},
			req: &vtadminpb.ReloadSchemasRequest{
				KeyspaceShards: []string{"ks1/-", "ks2/-80", "ks2/80-"},
			},
			shouldErr: true,
		},
		{
			name: "getShardSets failure",
			cluster: &Cluster{
				ID:   "test",
				Name: "test",
				Vtctld: &fakevtctldclient.VtctldClient{
					GetKeyspaceResults: map[string]struct {
						Response *vtctldatapb.GetKeyspaceResponse
						Error    error
					}{
						"ks1": {
							Error: assert.AnError,
						},
						"ks2": {
							Response: &vtctldatapb.GetKeyspaceResponse{
								Keyspace: &vtctldatapb.Keyspace{
									Name: "ks2",
								},
							},
						},
					},
					GetKeyspacesResults: struct {
						Keyspaces []*vtctldatapb.Keyspace
						Error     error
					}{
						Keyspaces: []*vtctldatapb.Keyspace{
							{
								Name: "ks1",
							},
							{
								Name: "ks2",
							},
						},
					},
					ReloadSchemaShardResults: map[string]struct {
						Response *vtctldatapb.ReloadSchemaShardResponse
						Error    error
					}{
						"ks1/-": {
							Response: &vtctldatapb.ReloadSchemaShardResponse{
								Events: []*logutilpb.Event{
									{},
								},
							},
						},
						"ks2/-80": {
							Response: &vtctldatapb.ReloadSchemaShardResponse{
								Events: []*logutilpb.Event{
									{}, {},
								},
							},
						},
						"ks2/80-": { // skipped via request params
							Error: assert.AnError,
						},
					},
				},
				topoReadPool: pools.NewRPCPool(5, 0, nil),
			},
			req: &vtadminpb.ReloadSchemasRequest{
				KeyspaceShards: []string{"ks1/-"},
			},
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			results, err := tt.cluster.reloadShardSchemas(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)

			keyFn := func(shard *vtadminpb.Shard) string {
				return fmt.Sprintf("%s/%s", shard.Shard.Keyspace, shard.Shard.Name)
			}
			sort.Slice(tt.expected, func(i, j int) bool {
				return keyFn(tt.expected[i].Shard) < keyFn(tt.expected[j].Shard)
			})
			sort.Slice(results, func(i, j int) bool {
				return keyFn(results[i].Shard) < keyFn(results[j].Shard)
			})
			utils.MustMatch(t, tt.expected, results)
		})
	}
}

func Test_reloadTabletSchemas(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tests := []struct {
		name      string
		cluster   *Cluster
		tablets   []*vtadminpb.Tablet
		dbErr     bool
		req       *vtadminpb.ReloadSchemasRequest
		expected  []*vtadminpb.ReloadSchemasResponse_TabletResult
		shouldErr bool
	}{
		{
			name: "ok",
			cluster: &Cluster{
				ID:   "test",
				Name: "test",
				Vtctld: &fakevtctldclient.VtctldClient{
					ReloadSchemaResults: map[string]struct {
						Response *vtctldatapb.ReloadSchemaResponse
						Error    error
					}{
						"zone1-0000000100": {
							Response: &vtctldatapb.ReloadSchemaResponse{},
						},
						"zone1-0000000101": {
							Response: &vtctldatapb.ReloadSchemaResponse{},
						},
						"zone2-0000000200": {
							Response: &vtctldatapb.ReloadSchemaResponse{},
						},
						"zone5-0000000500": {
							Error: assert.AnError,
						},
					},
				},
			},
			tablets: []*vtadminpb.Tablet{
				{
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  100,
						},
					},
				},
				{
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  101,
						},
					},
				},
				{
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone2",
							Uid:  200,
						},
					},
				},
				{
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone5",
							Uid:  500,
						},
					},
				},
			},
			req: &vtadminpb.ReloadSchemasRequest{
				Tablets: []*topodatapb.TabletAlias{
					{Cell: "zone1", Uid: 100},
					{Cell: "zone1", Uid: 101},
					{Cell: "zone2", Uid: 200},
					{Cell: "zone5", Uid: 500},
				},
			},
			expected: []*vtadminpb.ReloadSchemasResponse_TabletResult{
				{
					Tablet: &vtadminpb.Tablet{
						Cluster: &vtadminpb.Cluster{
							Id:   "test",
							Name: "test",
						},
						Tablet: &topodatapb.Tablet{
							Alias: &topodatapb.TabletAlias{
								Cell: "zone1",
								Uid:  100,
							},
						},
					},
					Result: "ok",
				},
				{
					Tablet: &vtadminpb.Tablet{
						Cluster: &vtadminpb.Cluster{
							Id:   "test",
							Name: "test",
						},
						Tablet: &topodatapb.Tablet{
							Alias: &topodatapb.TabletAlias{
								Cell: "zone1",
								Uid:  101,
							},
						},
					},
					Result: "ok",
				},
				{
					Tablet: &vtadminpb.Tablet{
						Cluster: &vtadminpb.Cluster{
							Id:   "test",
							Name: "test",
						},
						Tablet: &topodatapb.Tablet{
							Alias: &topodatapb.TabletAlias{
								Cell: "zone2",
								Uid:  200,
							},
						},
					},
					Result: "ok",
				},
				{
					Tablet: &vtadminpb.Tablet{
						Cluster: &vtadminpb.Cluster{
							Id:   "test",
							Name: "test",
						},
						Tablet: &topodatapb.Tablet{
							Alias: &topodatapb.TabletAlias{
								Cell: "zone5",
								Uid:  500,
							},
						},
					},
					Result: assert.AnError.Error(),
				},
			},
		},
		{
			name:    "FindTablets error",
			cluster: &Cluster{},
			dbErr:   true,
			req: &vtadminpb.ReloadSchemasRequest{
				Tablets: []*topodatapb.TabletAlias{
					{Cell: "zone1", Uid: 100},
				},
			},
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := vtsql.WithDialFunc(func(c vitessdriver.Configuration) (*sql.DB, error) {
				return sql.OpenDB(&fakevtsql.Connector{
					Tablets:   tt.tablets,
					ShouldErr: tt.dbErr,
				}), nil
			})(&vtsql.Config{
				Cluster:         tt.cluster.ToProto(),
				ResolverOptions: &resolver.Options{},
			})
			db, err := vtsql.New(ctx, cfg)
			require.NoError(t, err)
			defer db.Close()

			tt.cluster.DB = db

			results, err := tt.cluster.reloadTabletSchemas(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)

			sort.Slice(tt.expected, func(i, j int) bool {
				return topoproto.TabletAliasString(tt.expected[i].Tablet.Tablet.Alias) < topoproto.TabletAliasString(tt.expected[j].Tablet.Tablet.Alias)
			})
			sort.Slice(results, func(i, j int) bool {
				return topoproto.TabletAliasString(results[i].Tablet.Tablet.Alias) < topoproto.TabletAliasString(results[j].Tablet.Tablet.Alias)
			})
			utils.MustMatch(t, tt.expected, results)
		})
	}
}

func TestReparentTablet(t *testing.T) {
	t.Parallel()

	testClusterProto := &vtadminpb.Cluster{
		Id:   "test",
		Name: "test",
	}

	tests := []struct {
		name      string
		cluster   *Cluster
		timeout   time.Duration
		setup     func(t testing.TB, c *Cluster)
		tablet    *vtadminpb.Tablet
		expected  *vtadminpb.ReparentTabletResponse
		shouldErr bool
	}{
		{
			name: "ok",
			cluster: &Cluster{
				Vtctld: &fakevtctldclient.VtctldClient{
					ReparentTabletResults: map[string]struct {
						Response *vtctldatapb.ReparentTabletResponse
						Error    error
					}{
						"zone1-0000000100": {
							Response: &vtctldatapb.ReparentTabletResponse{
								Keyspace: "testks",
								Shard:    "-",
								Primary: &topodatapb.TabletAlias{
									Cell: "zone1",
									Uid:  500,
								},
							},
						},
					},
				},
				topoRWPool: pools.NewRPCPool(1, time.Millisecond*100, nil),
			},
			tablet: &vtadminpb.Tablet{
				Tablet: &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
				},
			},
			expected: &vtadminpb.ReparentTabletResponse{
				Keyspace: "testks",
				Shard:    "-",
				Primary: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  500,
				},
				Cluster: testClusterProto,
			},
		},
		{
			name: "error",
			cluster: &Cluster{
				Vtctld:     &fakevtctldclient.VtctldClient{},
				topoRWPool: pools.NewRPCPool(1, time.Millisecond*100, nil),
			},
			tablet: &vtadminpb.Tablet{
				Tablet: &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
				},
			},
			shouldErr: true,
		},
		{
			name: "RPC pool full",
			cluster: &Cluster{
				Vtctld:     &fakevtctldclient.VtctldClient{},
				topoRWPool: pools.NewRPCPool(1, time.Millisecond*10, nil),
			},
			timeout: time.Millisecond * 50,
			setup: func(t testing.TB, c *Cluster) {
				err := c.topoRWPool.Acquire(context.Background())
				require.NoError(t, err, "failed to lock RPC pool")
				t.Cleanup(c.topoRWPool.Release)
			},
			tablet: &vtadminpb.Tablet{
				Tablet: &topodatapb.Tablet{Alias: &topodatapb.TabletAlias{}},
			},
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.cluster.ID = testClusterProto.Id
			tt.cluster.Name = testClusterProto.Name

			if tt.setup != nil {
				tt.setup(t, tt.cluster)
			}

			var (
				ctx    context.Context
				cancel context.CancelFunc
			)

			switch tt.timeout {
			case 0:
				ctx, cancel = context.WithCancel(context.Background())
			default:
				ctx, cancel = context.WithTimeout(context.Background(), tt.timeout)
			}
			defer cancel()

			resp, err := tt.cluster.ReparentTablet(ctx, tt.tablet)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			utils.MustMatch(t, tt.expected, resp)
		})
	}
}

func TestTabletExternallyReparented(t *testing.T) {
	t.Parallel()

	testClusterProto := &vtadminpb.Cluster{
		Id:   "test",
		Name: "test",
	}

	tests := []struct {
		name      string
		cluster   *Cluster
		setup     func(t testing.TB, c *Cluster)
		timeout   time.Duration
		tablet    *vtadminpb.Tablet
		expected  *vtadminpb.TabletExternallyReparentedResponse
		shouldErr bool
	}{
		{
			name: "ok",
			cluster: &Cluster{
				ID:   "test",
				Name: "test",
				Vtctld: &fakevtctldclient.VtctldClient{
					TabletExternallyReparentedResults: map[string]struct {
						Response *vtctldatapb.TabletExternallyReparentedResponse
						Error    error
					}{
						"zone1-0000000100": {
							Response: &vtctldatapb.TabletExternallyReparentedResponse{
								Keyspace: "ks1",
								Shard:    "-",
								NewPrimary: &topodatapb.TabletAlias{
									Cell: "zone1",
									Uid:  100,
								},
								OldPrimary: &topodatapb.TabletAlias{
									Cell: "zone1",
									Uid:  200,
								},
							},
						},
						"zone1-0000000200": {
							Error: fmt.Errorf("some error: %w", assert.AnError),
						},
					},
				},
				topoRWPool: pools.NewRPCPool(1, time.Second, nil),
			},
			tablet: &vtadminpb.Tablet{
				Tablet: &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
				},
			},
			expected: &vtadminpb.TabletExternallyReparentedResponse{
				Cluster:  testClusterProto,
				Keyspace: "ks1",
				Shard:    "-",
				NewPrimary: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
				OldPrimary: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  200,
				},
			},
		},
		{
			name: "error",
			cluster: &Cluster{
				ID:   "test",
				Name: "test",
				Vtctld: &fakevtctldclient.VtctldClient{
					TabletExternallyReparentedResults: map[string]struct {
						Response *vtctldatapb.TabletExternallyReparentedResponse
						Error    error
					}{
						"zone1-0000000100": {
							Response: &vtctldatapb.TabletExternallyReparentedResponse{
								Keyspace: "ks1",
								Shard:    "-",
								NewPrimary: &topodatapb.TabletAlias{
									Cell: "zone1",
									Uid:  100,
								},
								OldPrimary: &topodatapb.TabletAlias{
									Cell: "zone1",
									Uid:  200,
								},
							},
						},
						"zone1-0000000200": {
							Error: fmt.Errorf("some error: %w", assert.AnError),
						},
					},
				},
				topoRWPool: pools.NewRPCPool(1, time.Second, nil),
			},
			tablet: &vtadminpb.Tablet{
				Tablet: &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  200,
					},
				},
			},
			shouldErr: true,
		},
		{
			name: "pool full",
			cluster: &Cluster{
				ID:   "test",
				Name: "test",
				Vtctld: &fakevtctldclient.VtctldClient{
					TabletExternallyReparentedResults: map[string]struct {
						Response *vtctldatapb.TabletExternallyReparentedResponse
						Error    error
					}{
						"zone1-0000000100": {
							Response: &vtctldatapb.TabletExternallyReparentedResponse{
								Keyspace: "ks1",
								Shard:    "-",
								NewPrimary: &topodatapb.TabletAlias{
									Cell: "zone1",
									Uid:  100,
								},
								OldPrimary: &topodatapb.TabletAlias{
									Cell: "zone1",
									Uid:  200,
								},
							},
						},
						"zone1-0000000200": {
							Error: fmt.Errorf("some error: %w", assert.AnError),
						},
					},
				},
				topoRWPool: pools.NewRPCPool(1, time.Millisecond*25, nil),
			},
			setup: func(t testing.TB, c *Cluster) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
				defer cancel()

				err := c.topoRWPool.Acquire(ctx)
				require.NoError(t, err, "could not block topoRW pool in setup")
				t.Cleanup(c.topoRWPool.Release)
			},
			timeout: time.Millisecond * 50,
			tablet: &vtadminpb.Tablet{
				Tablet: &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
				},
			},
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if tt.setup != nil {
				func(t *testing.T) {
					t.Helper()
					tt.setup(t, tt.cluster)
				}(t)
			}

			var (
				ctx    context.Context
				cancel context.CancelFunc
			)
			switch tt.timeout {
			case 0:
				ctx, cancel = context.WithCancel(context.Background())
			default:
				ctx, cancel = context.WithTimeout(context.Background(), tt.timeout)
			}
			defer cancel()

			resp, err := tt.cluster.TabletExternallyReparented(ctx, tt.tablet)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			utils.MustMatch(t, tt.expected, resp)
		})
	}
}

func TestSchemaCacheExcludeKeyspaces(t *testing.T) {
	t.Parallel()

	vtctld := &fakevtctldclient.VtctldClient{
		FindAllShardsInKeyspaceResults: map[string]struct {
			Response *vtctldatapb.FindAllShardsInKeyspaceResponse
			Error    error
		}{
			"ks1": {
				Response: &vtctldatapb.FindAllShardsInKeyspaceResponse{
					Shards: map[string]*vtctldatapb.Shard{
						"-": {
							Name: "-",
							Shard: &topodatapb.Shard{
								IsPrimaryServing: true,
								PrimaryAlias: &topodatapb.TabletAlias{
									Cell: "ks1",
									Uid:  100,
								},
							},
						},
					},
				},
			},
			"ks2": {
				Response: &vtctldatapb.FindAllShardsInKeyspaceResponse{
					Shards: map[string]*vtctldatapb.Shard{
						"-": {
							Name: "-",
							Shard: &topodatapb.Shard{
								IsPrimaryServing: true,
								PrimaryAlias: &topodatapb.TabletAlias{
									Cell: "ks2",
									Uid:  100,
								},
							},
						},
					},
				},
			},
			"nocache": {
				Response: &vtctldatapb.FindAllShardsInKeyspaceResponse{
					Shards: map[string]*vtctldatapb.Shard{
						"-": {
							Name: "-",
							Shard: &topodatapb.Shard{
								IsPrimaryServing: true,
								PrimaryAlias: &topodatapb.TabletAlias{
									Cell: "nocache",
									Uid:  100,
								},
							},
						},
					},
				},
			},
		},
		GetKeyspacesResults: struct {
			Keyspaces []*vtctldatapb.Keyspace
			Error     error
		}{
			Keyspaces: []*vtctldatapb.Keyspace{
				{
					Name: "ks1",
				},
				{
					Name: "ks2",
				},
				{
					Name: "nocache",
				},
			},
		},
		GetSchemaResults: map[string]struct {
			Response *vtctldatapb.GetSchemaResponse
			Error    error
		}{
			"ks1-0000000100": {
				Response: &vtctldatapb.GetSchemaResponse{
					Schema: &tabletmanagerdatapb.SchemaDefinition{
						TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
							{},
						},
					},
				},
			},
			"ks2-0000000100": {
				Response: &vtctldatapb.GetSchemaResponse{
					Schema: &tabletmanagerdatapb.SchemaDefinition{
						TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
							{},
						},
					},
				},
			},
			"nocache-0000000100": {
				Response: &vtctldatapb.GetSchemaResponse{
					Schema: &tabletmanagerdatapb.SchemaDefinition{
						TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
							{},
						},
					},
				},
			},
		},
	}

	cfg := Config{
		ID:            "test",
		Name:          "test",
		DiscoveryImpl: "dynamic",
		DiscoveryFlagsByImpl: map[string]map[string]string{
			"dynamic": {
				"discovery": `{}`,
			},
		},
		vtctldConfigOpts: []vtadminvtctldclient.ConfigOption{
			vtadminvtctldclient.WithDialFunc(func(addr string, ff grpcclient.FailFast, opts ...grpc.DialOption) (vtctldclient.VtctldClient, error) {
				return vtctld, nil
			}),
		},
		vtsqlConfigOpts: []vtsql.ConfigOption{
			vtsql.WithDialFunc(func(c vitessdriver.Configuration) (*sql.DB, error) {
				return sql.OpenDB(&fakevtsql.Connector{
					Tablets: []*vtadminpb.Tablet{
						{
							Tablet: &topodatapb.Tablet{
								Keyspace: "ks1",
								Shard:    "-",
								Type:     topodatapb.TabletType_PRIMARY,
								Alias: &topodatapb.TabletAlias{
									Cell: "ks1",
									Uid:  100,
								},
							},
							State: vtadminpb.Tablet_SERVING,
						},
						{
							Tablet: &topodatapb.Tablet{
								Keyspace: "ks2",
								Shard:    "-",
								Type:     topodatapb.TabletType_PRIMARY,
								Alias: &topodatapb.TabletAlias{
									Cell: "ks2",
									Uid:  100,
								},
							},
							State: vtadminpb.Tablet_SERVING,
						},
						{
							Tablet: &topodatapb.Tablet{
								Keyspace: "nocache",
								Shard:    "-",
								Type:     topodatapb.TabletType_PRIMARY,
								Alias: &topodatapb.TabletAlias{
									Cell: "nocache",
									Uid:  100,
								},
							},
							State: vtadminpb.Tablet_SERVING,
						},
					},
				}), nil
			}),
		},
		SchemaCacheConfig: &cache.Config{
			DefaultExpiration:       cache.NoExpiration,
			CleanupInterval:         cache.NoExpiration,
			BackfillEnqueueWaitTime: time.Hour,
		},
		SchemaCacheExcludeKeyspaces: []string{"nocache"},
	}

	ctx := context.Background()
	opts := GetSchemaOptions{
		BaseRequest: &vtctldatapb.GetSchemaRequest{
			IncludeViews: true,
		},
		TableSizeOptions: &vtadminpb.GetSchemaTableSizeOptions{
			AggregateSizes: true,
		},
	}

	t.Run("GetSchema", func(t *testing.T) {
		t.Parallel()

		c, err := New(ctx, cfg)
		require.NoError(t, err, "failed to create test cluster from config: %+v", cfg)

		t.Cleanup(func() {
			c.DB.Close()
			c.Vtctld.Close()
			c.schemaCache.Close()
		})

		_, err = c.GetSchema(ctx, "ks1", opts)
		require.NoError(t, err, "failed to GetSchema for ks1")

		time.Sleep(time.Millisecond * 100) // wait for background fill to finish
		_, ok, err := schemacache.LoadOne(c.schemaCache, schemacache.Key{
			ClusterID: "test",
			Keyspace:  "ks1",
		}, schemacache.LoadOptions{
			BaseRequest: opts.BaseRequest,
		})
		require.NoError(t, err, "schemacache.LoadOne(ks1) failed")

		assert.True(t, ok, "calling GetSchema(ks1) should fill the cache, but did not")

		_, err = c.GetSchema(ctx, "nocache", opts)
		require.NoError(t, err, "failed to GetSchema for nocache")

		time.Sleep(time.Millisecond * 100) // wait for background fill (which is not happening) to "finish"
		_, ok, err = schemacache.LoadOne(c.schemaCache, schemacache.Key{
			ClusterID: "test",
			Keyspace:  "nocache",
		}, schemacache.LoadOptions{
			BaseRequest: opts.BaseRequest,
		})
		require.NoError(t, err, "schemacache.LoadOne(nocache) failed")

		assert.False(t, ok, "calling GetSchema(nocache) should not fill the cache, but did")
	})

	t.Run("GetSchemas", func(t *testing.T) {
		t.Parallel()

		c, err := New(ctx, cfg)
		require.NoError(t, err, "failed to create test cluster from config: %+v", cfg)

		t.Cleanup(func() {
			c.DB.Close()
			c.Vtctld.Close()
			c.schemaCache.Close()
		})

		schemas, err := c.GetSchemas(ctx, opts)
		require.NoError(t, err, "failed to GetSchemas for cluster")
		assert.Len(t, schemas, 3, "all 3 keyspaces should have schemas returned")

		time.Sleep(time.Millisecond * 100) // wait for background fill to finish
		schemas, ok, err := schemacache.LoadAll(c.schemaCache, schemacache.Key{
			ClusterID: "test",
		}, schemacache.LoadOptions{
			BaseRequest: opts.BaseRequest,
		})
		require.NoError(t, err, "schemacache.LoadAll() failed")

		assert.True(t, ok, "calling GetSchemas() should fill the cache, but did not")
		assert.Len(t, schemas, 2, "GetSchemas() should cache 2 keyspaces (ks1, ks2) not 3 (+nocache)")

		for _, s := range schemas {
			if s.Keyspace == "nocache" {
				assert.Failf(t, "nocache should not be in cache", "GetSchemas() should not cache schema for `nocache` but did")
			}
		}

		// Then, even though it's not cached, GetSchemas still includes it in
		// the final response.
		schemas, err = c.GetSchemas(ctx, opts)
		require.NoError(t, err, "failed to GetSchemas for cluster")
		assert.Len(t, schemas, 3, "all 3 keyspaces should have schemas returned")
	})
}
