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

package cluster_test

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/util/sets"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtadmin/cluster"
	vtadminerrors "vitess.io/vitess/go/vt/vtadmin/errors"
	"vitess.io/vitess/go/vt/vtadmin/testutil"
	"vitess.io/vitess/go/vt/vtadmin/vtctldclient/fakevtctldclient"
	"vitess.io/vitess/go/vt/vtctl/vtctldclient"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

func TestCreateKeyspace(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tests := []struct {
		name      string
		cfg       testutil.TestClusterConfig
		req       *vtctldatapb.CreateKeyspaceRequest
		expected  *vtadminpb.Keyspace
		shouldErr bool
	}{
		{
			name: "success",
			cfg: testutil.TestClusterConfig{
				Cluster: &vtadminpb.Cluster{
					Id:   "c1",
					Name: "cluster1",
				},
				VtctldClient: &fakevtctldclient.VtctldClient{},
			},
			req: &vtctldatapb.CreateKeyspaceRequest{
				Name: "testkeyspace",
			},
			expected: &vtadminpb.Keyspace{
				Cluster: &vtadminpb.Cluster{
					Id:   "c1",
					Name: "cluster1",
				},
				Keyspace: &vtctldatapb.Keyspace{
					Name:     "testkeyspace",
					Keyspace: &topodatapb.Keyspace{},
				},
				Shards: map[string]*vtctldatapb.Shard{},
			},
		},
		{
			name: "snapshot",
			cfg: testutil.TestClusterConfig{
				Cluster: &vtadminpb.Cluster{
					Id:   "c1",
					Name: "cluster1",
				},
				VtctldClient: &fakevtctldclient.VtctldClient{},
			},
			req: &vtctldatapb.CreateKeyspaceRequest{
				Name:         "testkeyspace_snapshot",
				Type:         topodatapb.KeyspaceType_SNAPSHOT,
				BaseKeyspace: "testkeyspace",
				SnapshotTime: protoutil.TimeToProto(time.Date(2006, time.January, 2, 3, 4, 5, 0, time.UTC)),
			},
			expected: &vtadminpb.Keyspace{
				Cluster: &vtadminpb.Cluster{
					Id:   "c1",
					Name: "cluster1",
				},
				Keyspace: &vtctldatapb.Keyspace{
					Name: "testkeyspace_snapshot",
					Keyspace: &topodatapb.Keyspace{
						KeyspaceType: topodatapb.KeyspaceType_SNAPSHOT,
						BaseKeyspace: "testkeyspace",
						SnapshotTime: protoutil.TimeToProto(time.Date(2006, time.January, 2, 3, 4, 5, 0, time.UTC)),
					},
				},
				Shards: map[string]*vtctldatapb.Shard{},
			},
		},
		{
			name: "nil request",
			cfg: testutil.TestClusterConfig{
				Cluster: &vtadminpb.Cluster{
					Id:   "c1",
					Name: "cluster1",
				},
				VtctldClient: &fakevtctldclient.VtctldClient{},
			},
			req:       nil,
			shouldErr: true,
		},
		{
			name: "missing name",
			cfg: testutil.TestClusterConfig{
				Cluster: &vtadminpb.Cluster{
					Id:   "c1",
					Name: "cluster1",
				},
				VtctldClient: &fakevtctldclient.VtctldClient{},
			},
			req:       &vtctldatapb.CreateKeyspaceRequest{},
			shouldErr: true,
		},
		{
			name: "failure",
			cfg: testutil.TestClusterConfig{
				Cluster: &vtadminpb.Cluster{
					Id:   "c1",
					Name: "cluster1",
				},
				VtctldClient: &fakevtctldclient.VtctldClient{
					CreateKeyspaceShouldErr: true,
				},
			},
			req: &vtctldatapb.CreateKeyspaceRequest{
				Name: "testkeyspace",
			},
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cluster := testutil.BuildCluster(t, tt.cfg)

			resp, err := cluster.CreateKeyspace(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.expected, resp)
		})
	}
}

func TestCreateShard(t *testing.T) {
	t.Parallel()

	type test struct {
		name      string
		tc        *testutil.IntegrationTestCluster
		req       *vtctldatapb.CreateShardRequest
		shouldErr bool
		assertion func(t *testing.T, tt *test)
	}
	ctx := context.Background()
	tests := []*test{
		{
			name: "ok",
			tc: testutil.BuildIntegrationTestCluster(t, &vtadminpb.Cluster{
				Id:   "local",
				Name: "local",
			}, "zone1"),
			req: &vtctldatapb.CreateShardRequest{
				Keyspace:      "ks1",
				ShardName:     "-",
				IncludeParent: true,
			},
			assertion: func(t *testing.T, tt *test) {
				shard, err := tt.tc.Topo.GetShard(ctx, "ks1", "-")
				require.NoError(t, err, "topo.GetShard(ks1/-) failed")

				utils.MustMatch(t, &topodatapb.Shard{
					KeyRange:         &topodatapb.KeyRange{},
					IsPrimaryServing: true,
				}, shard.Shard)
			},
		},
		{
			name: "nil request",
			tc: testutil.BuildIntegrationTestCluster(t, &vtadminpb.Cluster{
				Id:   "local",
				Name: "local",
			}, "zone1"),
			req:       nil,
			shouldErr: true,
		},
		{
			name: "no keyspace in request",
			tc: testutil.BuildIntegrationTestCluster(t, &vtadminpb.Cluster{
				Id:   "local",
				Name: "local",
			}, "zone1"),
			req: &vtctldatapb.CreateShardRequest{
				Keyspace:  "",
				ShardName: "-",
			},
			shouldErr: true,
		},
		{
			name: "no shard name in request",
			tc: testutil.BuildIntegrationTestCluster(t, &vtadminpb.Cluster{
				Id:   "local",
				Name: "local",
			}, "zone1"),
			req: &vtctldatapb.CreateShardRequest{
				Keyspace:  "ks1",
				ShardName: "",
			},
			shouldErr: true,
		},
		{
			name: "vtctld.CreateShard fails",
			tc: testutil.BuildIntegrationTestCluster(t, &vtadminpb.Cluster{
				Id:   "local",
				Name: "local",
			}, "zone1"),
			req: &vtctldatapb.CreateShardRequest{
				Keyspace:  "ks1", // because IncludeParent=false and ks1 does not exist, we fail
				ShardName: "-",
			},
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.tc.Cluster.CreateShard(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			if tt.assertion != nil {
				func() {
					t.Helper()
					tt.assertion(t, tt)
				}()
			}
		})
	}
}

func TestDeleteKeyspace(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tests := []struct {
		name      string
		cfg       testutil.TestClusterConfig
		req       *vtctldatapb.DeleteKeyspaceRequest
		expected  *vtctldatapb.DeleteKeyspaceResponse
		shouldErr bool
	}{
		{
			name: "success",
			cfg: testutil.TestClusterConfig{
				Cluster: &vtadminpb.Cluster{
					Id:   "c1",
					Name: "cluster1",
				},
				VtctldClient: &fakevtctldclient.VtctldClient{},
			},
			req: &vtctldatapb.DeleteKeyspaceRequest{
				Keyspace: "ks1",
			},
			expected: &vtctldatapb.DeleteKeyspaceResponse{},
		},
		{
			name: "nil request",
			cfg: testutil.TestClusterConfig{
				Cluster: &vtadminpb.Cluster{
					Id:   "c1",
					Name: "cluster1",
				},
				VtctldClient: &fakevtctldclient.VtctldClient{},
			},
			req:       nil,
			shouldErr: true,
		},
		{
			name: "missing name",
			cfg: testutil.TestClusterConfig{
				Cluster: &vtadminpb.Cluster{
					Id:   "c1",
					Name: "cluster1",
				},
				VtctldClient: &fakevtctldclient.VtctldClient{},
			},
			req:       &vtctldatapb.DeleteKeyspaceRequest{},
			shouldErr: true,
		},
		{
			name: "failure",
			cfg: testutil.TestClusterConfig{
				Cluster: &vtadminpb.Cluster{
					Id:   "c1",
					Name: "cluster1",
				},
				VtctldClient: &fakevtctldclient.VtctldClient{
					DeleteKeyspaceShouldErr: true,
				},
			},
			req: &vtctldatapb.DeleteKeyspaceRequest{
				Keyspace: "ks1",
			},
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cluster := testutil.BuildCluster(t, tt.cfg)

			resp, err := cluster.DeleteKeyspace(ctx, tt.req)
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

	type test struct {
		name      string
		tc        *testutil.IntegrationTestCluster
		setup     func(t *testing.T, tt *test)
		req       *vtctldatapb.DeleteShardsRequest
		shouldErr bool
		assertion func(t *testing.T, tt *test)
	}
	ctx := context.Background()
	tests := []*test{
		{
			name: "ok",
			tc: testutil.BuildIntegrationTestCluster(t, &vtadminpb.Cluster{
				Id:   "local",
				Name: "local",
			}, "zone1"),
			setup: func(t *testing.T, tt *test) {
				ctx := context.Background()
				shards := []string{"-80", "80-"}
				for _, shard := range shards {
					_, err := tt.tc.Cluster.CreateShard(ctx, &vtctldatapb.CreateShardRequest{
						Keyspace:      "ks1",
						ShardName:     shard,
						IncludeParent: true,
						Force:         true,
					})
					require.NoError(t, err)
				}
			},
			req: &vtctldatapb.DeleteShardsRequest{
				Shards: []*vtctldatapb.Shard{
					{
						Keyspace: "ks1",
						Name:     "80-",
					},
				},
			},
			assertion: func(t *testing.T, tt *test) {
				shard, err := tt.tc.Topo.GetShard(ctx, "ks1", "-80")
				require.NoError(t, err, "topo.GetShard(ks1/-80) failed")

				utils.MustMatch(t, &topodatapb.Shard{
					KeyRange: &topodatapb.KeyRange{
						End: []byte{0x80},
					},
					IsPrimaryServing: true,
				}, shard.Shard)

				shard2, err2 := tt.tc.Topo.GetShard(ctx, "ks1", "80-")
				assert.True(t, topo.IsErrType(err2, topo.NoNode), "expected ks1/80- to be deleted, found %+v", shard2)
			},
		},
		{
			name: "nil request",
			tc: testutil.BuildIntegrationTestCluster(t, &vtadminpb.Cluster{
				Id:   "local",
				Name: "local",
			}, "zone1"),
			req:       nil,
			shouldErr: true,
		},
		{
			name: "vtctld.DeleteShards fails",
			tc: testutil.BuildIntegrationTestCluster(t, &vtadminpb.Cluster{
				Id:   "local",
				Name: "local",
			}, "zone1"),
			setup: func(t *testing.T, tt *test) {
				_, err := tt.tc.Cluster.Vtctld.CreateShard(ctx, &vtctldatapb.CreateShardRequest{
					Keyspace:      "ks1",
					ShardName:     "-",
					IncludeParent: true,
				})
				require.NoError(t, err, "CreateShard(ks1/-) failed")

				srvks := &topodatapb.SrvKeyspace{
					Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
						{
							ServedType: topodatapb.TabletType_PRIMARY,
							ShardReferences: []*topodatapb.ShardReference{
								{
									Name: "-",
								},
							},
						},
					},
				}
				err = tt.tc.Topo.UpdateSrvKeyspace(ctx, "zone1", "ks1", srvks)
				require.NoError(t, err, "UpdateSrvKeyspace(zone1, ks1, %+v) failed", srvks)
			},
			req: &vtctldatapb.DeleteShardsRequest{
				Shards: []*vtctldatapb.Shard{
					{
						Keyspace: "ks1",
						Name:     "-",
					},
				},
			},
			shouldErr: true,
			assertion: func(t *testing.T, tt *test) {
				shard, err := tt.tc.Topo.GetShard(ctx, "ks1", "-")
				require.NoError(t, err, "GetShard(ks1/-) failed")
				utils.MustMatch(t, &topodatapb.Shard{
					IsPrimaryServing: true,
					KeyRange:         &topodatapb.KeyRange{},
				}, shard.Shard)
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			if tt.setup != nil {
				func() {
					t.Helper()
					tt.setup(t, tt)
				}()
			}

			_, err := tt.tc.Cluster.DeleteShards(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			if tt.assertion != nil {
				func() {
					t.Helper()
					tt.assertion(t, tt)
				}()
			}
		})
	}
}

func TestFindTablet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		tablets       []*vtadminpb.Tablet
		filter        func(*vtadminpb.Tablet) bool
		expected      *vtadminpb.Tablet
		expectedError error
	}{
		{
			name: "returns the first matching tablet",
			tablets: []*vtadminpb.Tablet{
				{
					State: vtadminpb.Tablet_NOT_SERVING,
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "c0_cell1",
							Uid:  100,
						},
						Keyspace: "commerce",
					},
				},
				{
					State: vtadminpb.Tablet_SERVING,
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "c0_cell1",
							Uid:  101,
						},
						Keyspace: "commerce",
					},
				},
				{
					State: vtadminpb.Tablet_SERVING,
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "c0_cell1",
							Uid:  102,
						},
						Keyspace: "commerce",
					},
				},
			},

			filter: func(t *vtadminpb.Tablet) bool {
				return t.State == vtadminpb.Tablet_SERVING
			},
			expected: &vtadminpb.Tablet{
				Cluster: &vtadminpb.Cluster{
					Id:   "c0",
					Name: "cluster0",
				},
				State: vtadminpb.Tablet_SERVING,
				Tablet: &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: "c0_cell1",
						Uid:  101,
					},
					Keyspace: "commerce",
				},
			},
		},
		{
			name: "returns an error if no match found",
			tablets: []*vtadminpb.Tablet{
				{
					State: vtadminpb.Tablet_NOT_SERVING,
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "c0_cell1",
							Uid:  100,
						},
						Keyspace: "commerce",
					},
				},
				{
					State: vtadminpb.Tablet_NOT_SERVING,
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "c0_cell1",
							Uid:  101,
						},
						Keyspace: "commerce",
					},
				},
			},
			filter: func(t *vtadminpb.Tablet) bool {
				return t.State == vtadminpb.Tablet_SERVING
			},
			expectedError: vtadminerrors.ErrNoTablet,
		},
	}

	ctx := context.Background()

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cluster := testutil.BuildCluster(t, testutil.TestClusterConfig{
				Cluster: &vtadminpb.Cluster{
					Id:   "c0",
					Name: "cluster0",
				},
				Tablets: tt.tablets,
			})
			tablet, err := cluster.FindTablet(ctx, tt.filter)

			if tt.expectedError != nil {
				assert.True(t, errors.Is(err, tt.expectedError), "expected error type %w does not match actual error type %w", err, tt.expectedError)
			} else {
				assert.NoError(t, err)
				utils.MustMatch(t, tt.expected, tablet)
			}
		})
	}
}

func TestFindTablets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		tablets  []*vtadminpb.Tablet
		filter   func(*vtadminpb.Tablet) bool
		n        int
		expected []*vtadminpb.Tablet
	}{
		{
			name: "returns n filtered tablets",
			tablets: []*vtadminpb.Tablet{
				{
					State: vtadminpb.Tablet_SERVING,
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "c0_cell1",
							Uid:  100,
						},
						Keyspace: "commerce",
					},
				},
				{
					State: vtadminpb.Tablet_NOT_SERVING,
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "c0_cell1",
							Uid:  101,
						},
						Keyspace: "commerce",
					},
				},
				{
					State: vtadminpb.Tablet_SERVING,
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "c0_cell1",
							Uid:  102,
						},
						Keyspace: "commerce",
					},
				},
				{
					State: vtadminpb.Tablet_SERVING,
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "c0_cell1",
							Uid:  103,
						},
						Keyspace: "commerce",
					},
				},
			},
			filter: func(t *vtadminpb.Tablet) bool {
				return t.State == vtadminpb.Tablet_SERVING
			},
			n: 2,
			expected: []*vtadminpb.Tablet{
				{
					Cluster: &vtadminpb.Cluster{
						Id:   "c0",
						Name: "cluster0",
					},
					State: vtadminpb.Tablet_SERVING,
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "c0_cell1",
							Uid:  100,
						},
						Keyspace: "commerce",
					},
				},
				{
					Cluster: &vtadminpb.Cluster{
						Id:   "c0",
						Name: "cluster0",
					},
					State: vtadminpb.Tablet_SERVING,
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "c0_cell1",
							Uid:  102,
						},
						Keyspace: "commerce",
					},
				},
			},
		},
		{
			name: "returns all filtered tablets when n == -1",
			tablets: []*vtadminpb.Tablet{
				{
					State: vtadminpb.Tablet_SERVING,
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "c0_cell1",
							Uid:  100,
						},
						Keyspace: "commerce",
					},
				},
				{
					State: vtadminpb.Tablet_NOT_SERVING,
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "c0_cell1",
							Uid:  101,
						},
						Keyspace: "commerce",
					},
				},
				{
					State: vtadminpb.Tablet_SERVING,
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "c0_cell1",
							Uid:  102,
						},
						Keyspace: "commerce",
					},
				},
				{
					State: vtadminpb.Tablet_SERVING,
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "c0_cell1",
							Uid:  103,
						},
						Keyspace: "commerce",
					},
				},
			},
			filter: func(t *vtadminpb.Tablet) bool {
				return t.State == vtadminpb.Tablet_SERVING
			},
			n: -1,
			expected: []*vtadminpb.Tablet{
				{
					Cluster: &vtadminpb.Cluster{
						Id:   "c0",
						Name: "cluster0",
					},
					State: vtadminpb.Tablet_SERVING,
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "c0_cell1",
							Uid:  100,
						},
						Keyspace: "commerce",
					},
				},
				{
					Cluster: &vtadminpb.Cluster{
						Id:   "c0",
						Name: "cluster0",
					},
					State: vtadminpb.Tablet_SERVING,
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "c0_cell1",
							Uid:  102,
						},
						Keyspace: "commerce",
					},
				},
				{
					Cluster: &vtadminpb.Cluster{
						Id:   "c0",
						Name: "cluster0",
					},
					State: vtadminpb.Tablet_SERVING,
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "c0_cell1",
							Uid:  103,
						},
						Keyspace: "commerce",
					},
				},
			},
		},
	}

	ctx := context.Background()

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cluster := testutil.BuildCluster(t, testutil.TestClusterConfig{
				Cluster: &vtadminpb.Cluster{
					Id:   "c0",
					Name: "cluster0",
				},
				Tablets: tt.tablets,
			})
			tablets, err := cluster.FindTablets(ctx, tt.filter, tt.n)

			assert.NoError(t, err)
			testutil.AssertTabletSlicesEqual(t, tt.expected, tablets)
		})
	}
}

func TestFindWorkflows(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		cfg       testutil.TestClusterConfig
		keyspaces []string
		opts      cluster.FindWorkflowsOptions
		expected  *vtadminpb.ClusterWorkflows
		shouldErr bool
	}{
		{
			name: "success",
			cfg: testutil.TestClusterConfig{
				Cluster: &vtadminpb.Cluster{
					Id:   "c1",
					Name: "cluster1",
				},
				VtctldClient: &fakevtctldclient.VtctldClient{
					GetWorkflowsResults: map[string]struct {
						Response *vtctldatapb.GetWorkflowsResponse
						Error    error
					}{
						"ks1": {
							Response: &vtctldatapb.GetWorkflowsResponse{
								Workflows: []*vtctldatapb.Workflow{
									{
										Name: "workflow1",
									},
								},
							},
						},
					},
				},
			},
			keyspaces: []string{"ks1"},
			expected: &vtadminpb.ClusterWorkflows{
				Workflows: []*vtadminpb.Workflow{
					{
						Cluster: &vtadminpb.Cluster{
							Id:   "c1",
							Name: "cluster1",
						},
						Keyspace: "ks1",
						Workflow: &vtctldatapb.Workflow{
							Name: "workflow1",
						},
					},
				},
			},
			shouldErr: false,
		},
		{
			name: "error getting keyspaces is fatal",
			cfg: testutil.TestClusterConfig{
				Cluster: &vtadminpb.Cluster{
					Id:   "c1",
					Name: "cluster1",
				},
				VtctldClient: &fakevtctldclient.VtctldClient{
					GetKeyspacesResults: &struct {
						Keyspaces []*vtctldatapb.Keyspace
						Error     error
					}{
						Keyspaces: nil,
						Error:     assert.AnError,
					},
				},
			},
			keyspaces: nil,
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "no keyspaces found",
			cfg: testutil.TestClusterConfig{
				Cluster: &vtadminpb.Cluster{
					Id:   "c1",
					Name: "cluster1",
				},
				VtctldClient: &fakevtctldclient.VtctldClient{
					GetKeyspacesResults: &struct {
						Keyspaces []*vtctldatapb.Keyspace
						Error     error
					}{
						Keyspaces: []*vtctldatapb.Keyspace{},
						Error:     nil,
					},
				},
			},
			keyspaces: nil,
			expected: &vtadminpb.ClusterWorkflows{
				Workflows: []*vtadminpb.Workflow{},
			},
			shouldErr: false,
		},
		{
			name: "when specifying keyspaces and IgnoreKeyspaces, IgnoreKeyspaces is discarded",
			cfg: testutil.TestClusterConfig{
				Cluster: &vtadminpb.Cluster{
					Id:   "c1",
					Name: "cluster1",
				},
				VtctldClient: &fakevtctldclient.VtctldClient{
					GetKeyspacesResults: &struct {
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
						Error: nil,
					},
					GetWorkflowsResults: map[string]struct {
						Response *vtctldatapb.GetWorkflowsResponse
						Error    error
					}{
						"ks1": {
							Response: &vtctldatapb.GetWorkflowsResponse{
								Workflows: []*vtctldatapb.Workflow{
									{
										Name: "workflow1",
									},
									{
										Name: "workflow2",
									},
								},
							},
						},
						"ks2": {
							Response: &vtctldatapb.GetWorkflowsResponse{
								Workflows: []*vtctldatapb.Workflow{
									{
										Name: "workflow_a",
									},
								},
							},
						},
					},
				},
			},
			keyspaces: []string{"ks2"},
			opts: cluster.FindWorkflowsOptions{
				IgnoreKeyspaces: sets.NewString("ks2"),
			},
			expected: &vtadminpb.ClusterWorkflows{
				Workflows: []*vtadminpb.Workflow{
					{
						Cluster: &vtadminpb.Cluster{
							Id:   "c1",
							Name: "cluster1",
						},
						Keyspace: "ks2",
						Workflow: &vtctldatapb.Workflow{
							Name: "workflow_a",
						},
					},
				},
			},
			shouldErr: false,
		},
		{
			name: "ignore keyspaces",
			cfg: testutil.TestClusterConfig{
				Cluster: &vtadminpb.Cluster{
					Id:   "c1",
					Name: "cluster1",
				},
				VtctldClient: &fakevtctldclient.VtctldClient{
					GetKeyspacesResults: &struct {
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
						Error: nil,
					},
					GetWorkflowsResults: map[string]struct {
						Response *vtctldatapb.GetWorkflowsResponse
						Error    error
					}{
						"ks1": {
							Response: &vtctldatapb.GetWorkflowsResponse{
								Workflows: []*vtctldatapb.Workflow{
									{
										Name: "workflow1",
									},
									{
										Name: "workflow2",
									},
								},
							},
						},
						"ks2": {
							Response: &vtctldatapb.GetWorkflowsResponse{
								Workflows: []*vtctldatapb.Workflow{
									{
										Name: "workflow_a",
									},
								},
							},
						},
					},
				},
			},
			keyspaces: nil,
			opts: cluster.FindWorkflowsOptions{
				IgnoreKeyspaces: sets.NewString("ks2"),
			},
			expected: &vtadminpb.ClusterWorkflows{
				Workflows: []*vtadminpb.Workflow{
					{
						Cluster: &vtadminpb.Cluster{
							Id:   "c1",
							Name: "cluster1",
						},
						Keyspace: "ks1",
						Workflow: &vtctldatapb.Workflow{
							Name: "workflow1",
						},
					},
					{
						Cluster: &vtadminpb.Cluster{
							Id:   "c1",
							Name: "cluster1",
						},
						Keyspace: "ks1",
						Workflow: &vtctldatapb.Workflow{
							Name: "workflow2",
						},
					},
				},
			},
			shouldErr: false,
		},
		{
			name: "error getting workflows is fatal if all keyspaces fail",
			cfg: testutil.TestClusterConfig{
				Cluster: &vtadminpb.Cluster{
					Id:   "c1",
					Name: "cluster1",
				},
				VtctldClient: &fakevtctldclient.VtctldClient{
					GetWorkflowsResults: map[string]struct {
						Response *vtctldatapb.GetWorkflowsResponse
						Error    error
					}{
						"ks1": {
							Error: assert.AnError,
						},
					},
				},
			},
			keyspaces: []string{"ks1"},
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "error getting workflows is non-fatal if some keyspaces fail",
			cfg: testutil.TestClusterConfig{
				Cluster: &vtadminpb.Cluster{
					Id:   "c1",
					Name: "cluster1",
				},
				VtctldClient: &fakevtctldclient.VtctldClient{
					GetWorkflowsResults: map[string]struct {
						Response *vtctldatapb.GetWorkflowsResponse
						Error    error
					}{
						"ks1": {
							Error: assert.AnError,
						},
						"ks2": {
							Response: &vtctldatapb.GetWorkflowsResponse{
								Workflows: []*vtctldatapb.Workflow{
									{
										Name: "workflow1",
									},
								},
							},
						},
					},
				},
			},
			keyspaces: []string{"ks1", "ks2"},
			expected: &vtadminpb.ClusterWorkflows{
				Workflows: []*vtadminpb.Workflow{
					{
						Cluster: &vtadminpb.Cluster{
							Id:   "c1",
							Name: "cluster1",
						},
						Keyspace: "ks2",
						Workflow: &vtctldatapb.Workflow{
							Name: "workflow1",
						},
					},
				},
				Warnings: []string{"something about ks1"},
			},
			shouldErr: false,
		},
		{
			name: "filtered workflows",
			cfg: testutil.TestClusterConfig{
				Cluster: &vtadminpb.Cluster{
					Id:   "c1",
					Name: "cluster1",
				},
				VtctldClient: &fakevtctldclient.VtctldClient{
					GetWorkflowsResults: map[string]struct {
						Response *vtctldatapb.GetWorkflowsResponse
						Error    error
					}{
						"ks1": {
							Response: &vtctldatapb.GetWorkflowsResponse{
								Workflows: []*vtctldatapb.Workflow{
									{
										Name: "include_me",
									},
									{
										Name: "dont_include_me",
									},
								},
							},
						},
					},
				},
			},
			keyspaces: []string{"ks1"},
			opts: cluster.FindWorkflowsOptions{
				Filter: func(workflow *vtadminpb.Workflow) bool {
					return strings.HasPrefix(workflow.Workflow.Name, "include_me")
				},
			},
			expected: &vtadminpb.ClusterWorkflows{
				Workflows: []*vtadminpb.Workflow{
					{
						Cluster: &vtadminpb.Cluster{
							Id:   "c1",
							Name: "cluster1",
						},
						Keyspace: "ks1",
						Workflow: &vtctldatapb.Workflow{
							Name: "include_me",
						},
					},
				},
			},
			shouldErr: false,
		},
	}

	ctx := context.Background()

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := testutil.BuildCluster(t, tt.cfg)
			workflows, err := c.FindWorkflows(ctx, tt.keyspaces, tt.opts)
			if tt.shouldErr {
				assert.Error(t, err)

				return
			}

			assert.NoError(t, err)
			testutil.AssertClusterWorkflowsEqual(t, tt.expected, workflows)
		})
	}
}

func TestGetCellInfos(t *testing.T) {
	t.Parallel()

	cpb := &vtadminpb.Cluster{
		Id:   "test",
		Name: "test",
	}

	tests := []struct {
		name      string
		vtctld    *fakevtctldclient.VtctldClient
		req       *vtadminpb.GetCellInfosRequest
		expected  []*vtadminpb.ClusterCellInfo
		shouldErr bool
	}{
		{
			name: "all cells - ok",
			vtctld: &fakevtctldclient.VtctldClient{
				GetCellInfoNamesResults: &struct {
					Response *vtctldatapb.GetCellInfoNamesResponse
					Error    error
				}{
					Response: &vtctldatapb.GetCellInfoNamesResponse{
						Names: []string{"c1", "c2"},
					},
				},
				GetCellInfoResults: map[string]struct {
					Response *vtctldatapb.GetCellInfoResponse
					Error    error
				}{
					"c1": {
						Response: &vtctldatapb.GetCellInfoResponse{
							CellInfo: &topodatapb.CellInfo{
								ServerAddress: "http://cell1",
								Root:          "/vitess/cell1",
							},
						},
					},
					"c2": {
						Response: &vtctldatapb.GetCellInfoResponse{
							CellInfo: &topodatapb.CellInfo{
								ServerAddress: "http://cell2",
								Root:          "/vitess/cell2",
							},
						},
					},
				},
			},
			req: &vtadminpb.GetCellInfosRequest{},
			expected: []*vtadminpb.ClusterCellInfo{
				{
					Cluster: cpb,
					Name:    "c1",
					CellInfo: &topodatapb.CellInfo{
						ServerAddress: "http://cell1",
						Root:          "/vitess/cell1",
					},
				},
				{
					Cluster: cpb,
					Name:    "c2",
					CellInfo: &topodatapb.CellInfo{
						ServerAddress: "http://cell2",
						Root:          "/vitess/cell2",
					},
				},
			},
		},
		{
			name: "all cells - names only",
			vtctld: &fakevtctldclient.VtctldClient{
				GetCellInfoNamesResults: &struct {
					Response *vtctldatapb.GetCellInfoNamesResponse
					Error    error
				}{
					Response: &vtctldatapb.GetCellInfoNamesResponse{
						Names: []string{"c1", "c2"},
					},
				},
				GetCellInfoResults: map[string]struct {
					Response *vtctldatapb.GetCellInfoResponse
					Error    error
				}{
					"c1": {
						Response: &vtctldatapb.GetCellInfoResponse{
							CellInfo: &topodatapb.CellInfo{
								ServerAddress: "http://cell1",
								Root:          "/vitess/cell1",
							},
						},
					},
					"c2": {
						Response: &vtctldatapb.GetCellInfoResponse{
							CellInfo: &topodatapb.CellInfo{
								ServerAddress: "http://cell2",
								Root:          "/vitess/cell2",
							},
						},
					},
				},
			},
			req: &vtadminpb.GetCellInfosRequest{
				NamesOnly: true,
			},
			expected: []*vtadminpb.ClusterCellInfo{
				{
					Cluster: cpb,
					Name:    "c1",
				},
				{
					Cluster: cpb,
					Name:    "c2",
				},
			},
		},
		{
			name: "all cells - fail",
			vtctld: &fakevtctldclient.VtctldClient{
				GetCellInfoNamesResults: &struct {
					Response *vtctldatapb.GetCellInfoNamesResponse
					Error    error
				}{
					Error: fmt.Errorf("getcellinfonames failed"),
				},
			},
			req:       &vtadminpb.GetCellInfosRequest{},
			shouldErr: true,
		},
		{
			name: "cells - ok",
			vtctld: &fakevtctldclient.VtctldClient{
				GetCellInfoNamesResults: &struct {
					Response *vtctldatapb.GetCellInfoNamesResponse
					Error    error
				}{
					Response: &vtctldatapb.GetCellInfoNamesResponse{
						Names: []string{"c1", "c2"},
					},
				},
				GetCellInfoResults: map[string]struct {
					Response *vtctldatapb.GetCellInfoResponse
					Error    error
				}{
					"c1": {
						Response: &vtctldatapb.GetCellInfoResponse{
							CellInfo: &topodatapb.CellInfo{
								ServerAddress: "http://cell1",
								Root:          "/vitess/cell1",
							},
						},
					},
					"c2": {
						Response: &vtctldatapb.GetCellInfoResponse{
							CellInfo: &topodatapb.CellInfo{
								ServerAddress: "http://cell2",
								Root:          "/vitess/cell2",
							},
						},
					},
				},
			},
			req: &vtadminpb.GetCellInfosRequest{
				Cells: []string{"c1"},
			},
			expected: []*vtadminpb.ClusterCellInfo{
				{
					Cluster: cpb,
					Name:    "c1",
					CellInfo: &topodatapb.CellInfo{
						ServerAddress: "http://cell1",
						Root:          "/vitess/cell1",
					},
				},
			},
		},
		{
			name: "getcellinfo - fail",
			vtctld: &fakevtctldclient.VtctldClient{
				GetCellInfoNamesResults: &struct {
					Response *vtctldatapb.GetCellInfoNamesResponse
					Error    error
				}{
					Response: &vtctldatapb.GetCellInfoNamesResponse{
						Names: []string{"c1", "c2"},
					},
				},
				GetCellInfoResults: map[string]struct {
					Response *vtctldatapb.GetCellInfoResponse
					Error    error
				}{
					"c1": {
						Error: fmt.Errorf("GetCellInfo(c1) fail"),
					},
					"c2": {
						Response: &vtctldatapb.GetCellInfoResponse{
							CellInfo: &topodatapb.CellInfo{
								ServerAddress: "http://cell2",
								Root:          "/vitess/cell2",
							},
						},
					},
				},
			},
			req: &vtadminpb.GetCellInfosRequest{
				Cells: []string{"c1", "c2"},
			},
			shouldErr: true,
		},
		{
			name: "cells with names only still gives full info",
			vtctld: &fakevtctldclient.VtctldClient{
				GetCellInfoNamesResults: &struct {
					Response *vtctldatapb.GetCellInfoNamesResponse
					Error    error
				}{
					Response: &vtctldatapb.GetCellInfoNamesResponse{
						Names: []string{"c1", "c2"},
					},
				},
				GetCellInfoResults: map[string]struct {
					Response *vtctldatapb.GetCellInfoResponse
					Error    error
				}{
					"c1": {
						Response: &vtctldatapb.GetCellInfoResponse{
							CellInfo: &topodatapb.CellInfo{
								ServerAddress: "http://cell1",
								Root:          "/vitess/cell1",
							},
						},
					},
					"c2": {
						Response: &vtctldatapb.GetCellInfoResponse{
							CellInfo: &topodatapb.CellInfo{
								ServerAddress: "http://cell2",
								Root:          "/vitess/cell2",
							},
						},
					},
				},
			},
			req: &vtadminpb.GetCellInfosRequest{
				Cells:     []string{"c1"},
				NamesOnly: true,
			},
			expected: []*vtadminpb.ClusterCellInfo{
				{
					Cluster: cpb,
					Name:    "c1",
					CellInfo: &topodatapb.CellInfo{
						ServerAddress: "http://cell1",
						Root:          "/vitess/cell1",
					},
				},
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := testutil.BuildCluster(t, testutil.TestClusterConfig{
				Cluster:      cpb,
				VtctldClient: tt.vtctld,
			})
			cellInfos, err := c.GetCellInfos(context.Background(), tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)

			sort.Slice(tt.expected, func(i, j int) bool {
				return tt.expected[i].Name < tt.expected[j].Name
			})
			sort.Slice(cellInfos, func(i, j int) bool {
				return cellInfos[i].Name < cellInfos[j].Name
			})

			utils.MustMatch(t, tt.expected, cellInfos)
		})
	}
}

func TestGetCellsAliases(t *testing.T) {
	t.Parallel()

	cpb := &vtadminpb.Cluster{
		Id:   "test",
		Name: "test",
	}

	tests := []struct {
		name      string
		vtctld    *fakevtctldclient.VtctldClient
		expected  *vtadminpb.ClusterCellsAliases
		shouldErr bool
	}{
		{
			name: "ok",
			vtctld: &fakevtctldclient.VtctldClient{
				GetCellsAliasesResults: &struct {
					Response *vtctldatapb.GetCellsAliasesResponse
					Error    error
				}{
					Response: &vtctldatapb.GetCellsAliasesResponse{
						Aliases: map[string]*topodatapb.CellsAlias{
							"a1": {
								Cells: []string{"c1", "c2"},
							},
						},
					},
				},
			},
			expected: &vtadminpb.ClusterCellsAliases{
				Cluster: cpb,
				Aliases: map[string]*topodatapb.CellsAlias{
					"a1": {
						Cells: []string{"c1", "c2"},
					},
				},
			},
		},
		{
			name: "error",
			vtctld: &fakevtctldclient.VtctldClient{
				GetCellsAliasesResults: &struct {
					Response *vtctldatapb.GetCellsAliasesResponse
					Error    error
				}{
					Error: fmt.Errorf("this should fail"),
				},
			},
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := testutil.BuildCluster(t, testutil.TestClusterConfig{
				Cluster:      cpb,
				VtctldClient: tt.vtctld,
			})
			cellsAliases, err := c.GetCellsAliases(context.Background())
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			utils.MustMatch(t, tt.expected, cellsAliases)
		})
	}
}

func TestGetSchema(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		vtctld    vtctldclient.VtctldClient
		req       *vtctldatapb.GetSchemaRequest
		tablet    *vtadminpb.Tablet
		expected  *vtadminpb.Schema
		shouldErr bool
	}{
		{
			name: "success",
			vtctld: &fakevtctldclient.VtctldClient{
				GetSchemaResults: map[string]struct {
					Response *vtctldatapb.GetSchemaResponse
					Error    error
				}{
					"zone1-0000000100": {
						Response: &vtctldatapb.GetSchemaResponse{
							Schema: &tabletmanagerdatapb.SchemaDefinition{
								TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
									{
										Name: "some_table",
									},
								},
							},
						},
					},
				},
			},
			req: &vtctldatapb.GetSchemaRequest{},
			tablet: &vtadminpb.Tablet{
				State: vtadminpb.Tablet_SERVING,
				Tablet: &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "testkeyspace",
				},
			},
			expected: &vtadminpb.Schema{
				Cluster: &vtadminpb.Cluster{
					Name: "cluster0",
					Id:   "c0",
				},
				Keyspace: "testkeyspace",
				TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
					{
						Name: "some_table",
					},
				},
				TableSizes: map[string]*vtadminpb.Schema_TableSize{},
			},
			shouldErr: false,
		},
		{
			name: "error getting schema",
			vtctld: &fakevtctldclient.VtctldClient{
				GetSchemaResults: map[string]struct {
					Response *vtctldatapb.GetSchemaResponse
					Error    error
				}{
					"zone1-0000000100": {
						Error: assert.AnError,
					},
				},
			},
			req: &vtctldatapb.GetSchemaRequest{},
			tablet: &vtadminpb.Tablet{
				State: vtadminpb.Tablet_SERVING,
				Tablet: &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "testkeyspace",
				},
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "underlying schema is nil",
			vtctld: &fakevtctldclient.VtctldClient{
				GetSchemaResults: map[string]struct {
					Response *vtctldatapb.GetSchemaResponse
					Error    error
				}{
					"zone1-0000000100": {
						Response: &vtctldatapb.GetSchemaResponse{
							Schema: nil,
						},
						Error: nil,
					},
				},
			},
			req: &vtctldatapb.GetSchemaRequest{},
			tablet: &vtadminpb.Tablet{
				State: vtadminpb.Tablet_SERVING,
				Tablet: &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "testkeyspace",
				},
			},
			expected: &vtadminpb.Schema{
				Cluster: &vtadminpb.Cluster{
					Id:   "c2",
					Name: "cluster2",
				},
				Keyspace:         "testkeyspace",
				TableDefinitions: nil,
				TableSizes:       map[string]*vtadminpb.Schema_TableSize{},
			},
			shouldErr: false,
		},
	}

	ctx := context.Background()

	for i, tt := range tests {
		i := i
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := testutil.BuildCluster(t, testutil.TestClusterConfig{
				Cluster: &vtadminpb.Cluster{
					Id:   fmt.Sprintf("c%d", i),
					Name: fmt.Sprintf("cluster%d", i),
				},
				VtctldClient: tt.vtctld,
				Tablets:      []*vtadminpb.Tablet{tt.tablet},
				DBConfig:     testutil.Dbcfg{},
			})

			schema, err := c.GetSchema(ctx, "testkeyspace", cluster.GetSchemaOptions{
				BaseRequest: tt.req,
			})
			if tt.shouldErr {
				assert.Error(t, err)

				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.expected, schema)
		})
	}

	t.Run("does not modify passed-in request", func(t *testing.T) {
		t.Parallel()

		vtctld := &fakevtctldclient.VtctldClient{
			GetSchemaResults: map[string]struct {
				Response *vtctldatapb.GetSchemaResponse
				Error    error
			}{
				"zone1-0000000100": {
					Response: &vtctldatapb.GetSchemaResponse{},
				},
			},
		}

		req := &vtctldatapb.GetSchemaRequest{
			TabletAlias: &topodatapb.TabletAlias{
				Cell: "otherzone",
				Uid:  500,
			},
		}
		tablet := &vtadminpb.Tablet{
			Tablet: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
		}

		c := testutil.BuildCluster(t, testutil.TestClusterConfig{
			Cluster: &vtadminpb.Cluster{
				Id:   "c0",
				Name: "cluster0",
			},
			VtctldClient: vtctld,
		})

		_, _ = c.GetSchema(ctx, "testkeyspace", cluster.GetSchemaOptions{
			BaseRequest: req,
		})

		assert.NotEqual(t, req.TabletAlias, tablet.Tablet.Alias, "expected GetSchema to not modify original request object")
	})

	t.Run("size aggregation", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name      string
			cfg       testutil.TestClusterConfig
			keyspace  string
			opts      cluster.GetSchemaOptions
			expected  *vtadminpb.Schema
			shouldErr bool
		}{
			{
				name: "success",
				cfg: testutil.TestClusterConfig{
					Cluster: &vtadminpb.Cluster{
						Id:   "c0",
						Name: "cluster0",
					},
					Tablets: []*vtadminpb.Tablet{
						{
							Tablet: &topodatapb.Tablet{
								Alias: &topodatapb.TabletAlias{
									Cell: "zone1",
									Uid:  100,
								},
								Keyspace: "testkeyspace",
								Shard:    "-80",
							},
							State: vtadminpb.Tablet_SERVING,
						},
						{
							Tablet: &topodatapb.Tablet{
								Alias: &topodatapb.TabletAlias{
									Cell: "zone1",
									Uid:  200,
								},
								Keyspace: "testkeyspace",
								Shard:    "80-",
							},
							State: vtadminpb.Tablet_SERVING,
						},
					},
					VtctldClient: &fakevtctldclient.VtctldClient{
						FindAllShardsInKeyspaceResults: map[string]struct {
							Response *vtctldatapb.FindAllShardsInKeyspaceResponse
							Error    error
						}{
							"testkeyspace": {
								Response: &vtctldatapb.FindAllShardsInKeyspaceResponse{
									Shards: map[string]*vtctldatapb.Shard{
										"-80": {
											Name: "-80",
											Shard: &topodatapb.Shard{
												IsPrimaryServing: true,
												PrimaryAlias: &topodatapb.TabletAlias{
													Cell: "zone1",
													Uid:  100,
												},
											},
										},
										"80-": {
											Name: "80-",
											Shard: &topodatapb.Shard{
												IsPrimaryServing: true,
												PrimaryAlias: &topodatapb.TabletAlias{
													Cell: "zone1",
													Uid:  200,
												},
											},
										},
										"-": {
											Name: "-",
											Shard: &topodatapb.Shard{
												IsPrimaryServing: false,
											},
										},
									},
								},
							},
						},
						GetSchemaResults: map[string]struct {
							Response *vtctldatapb.GetSchemaResponse
							Error    error
						}{
							"zone1-0000000100": {
								Response: &vtctldatapb.GetSchemaResponse{
									Schema: &tabletmanagerdatapb.SchemaDefinition{
										DatabaseSchema: "CREATE DATABASE vt_testkeyspcae",
										TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
											{
												Name:       "foo",
												Schema:     "CREATE TABLE foo (\n\tid INT(11) NOT NULL\n) ENGINE=InnoDB",
												DataLength: 100,
												RowCount:   5,
											},
										},
									},
								},
							},
							"zone1-0000000200": {
								Response: &vtctldatapb.GetSchemaResponse{
									Schema: &tabletmanagerdatapb.SchemaDefinition{
										DatabaseSchema: "CREATE DATABASE vt_testkeyspcae",
										TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
											{
												Name:       "foo",
												Schema:     "CREATE TABLE foo (\n\tid INT(11) NOT NULL\n) ENGINE=InnoDB",
												DataLength: 200,
												RowCount:   420,
											},
										},
									},
								},
							},
						},
					},
				},
				keyspace: "testkeyspace",
				opts: cluster.GetSchemaOptions{
					TableSizeOptions: &vtadminpb.GetSchemaTableSizeOptions{
						AggregateSizes: true,
					},
				},
				expected: &vtadminpb.Schema{
					Cluster: &vtadminpb.Cluster{
						Id:   "c0",
						Name: "cluster0",
					},
					Keyspace: "testkeyspace",
					TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
						{
							Name:       "foo",
							Schema:     "CREATE TABLE foo (\n\tid INT(11) NOT NULL\n) ENGINE=InnoDB",
							DataLength: 0,
							RowCount:   0,
						},
					},
					TableSizes: map[string]*vtadminpb.Schema_TableSize{
						"foo": {
							DataLength: 100 + 200,
							RowCount:   5 + 420,
							ByShard: map[string]*vtadminpb.Schema_ShardTableSize{
								"-80": {
									DataLength: 100,
									RowCount:   5,
								},
								"80-": {
									DataLength: 200,
									RowCount:   420,
								},
							},
						},
					},
				},
				shouldErr: false,
			},
			{
				name: "include nonserving shards",
				cfg: testutil.TestClusterConfig{
					Cluster: &vtadminpb.Cluster{
						Id:   "c0",
						Name: "cluster0",
					},
					Tablets: []*vtadminpb.Tablet{
						{
							Tablet: &topodatapb.Tablet{
								Alias: &topodatapb.TabletAlias{
									Cell: "zone1",
									Uid:  100,
								},
								Keyspace: "testkeyspace",
								Shard:    "-80",
							},
							State: vtadminpb.Tablet_SERVING,
						},
						{
							Tablet: &topodatapb.Tablet{
								Alias: &topodatapb.TabletAlias{
									Cell: "zone1",
									Uid:  200,
								},
								Keyspace: "testkeyspace",
								Shard:    "80-",
							},
							State: vtadminpb.Tablet_SERVING,
						},
						{
							Tablet: &topodatapb.Tablet{
								Alias: &topodatapb.TabletAlias{
									Cell: "zone1",
									Uid:  300,
								},
								Keyspace: "testkeyspace",
								Shard:    "-",
							},
							State: vtadminpb.Tablet_SERVING,
						},
					},
					VtctldClient: &fakevtctldclient.VtctldClient{
						FindAllShardsInKeyspaceResults: map[string]struct {
							Response *vtctldatapb.FindAllShardsInKeyspaceResponse
							Error    error
						}{
							"testkeyspace": {
								Response: &vtctldatapb.FindAllShardsInKeyspaceResponse{
									Shards: map[string]*vtctldatapb.Shard{
										"-80": {
											Name: "-80",
											Shard: &topodatapb.Shard{
												IsPrimaryServing: true,
											},
										},
										"80-": {
											Name: "80-",
											Shard: &topodatapb.Shard{
												IsPrimaryServing: true,
											},
										},
										"-": {
											Name: "-",
											Shard: &topodatapb.Shard{
												IsPrimaryServing: false,
											},
										},
									},
								},
							},
						},
						GetSchemaResults: map[string]struct {
							Response *vtctldatapb.GetSchemaResponse
							Error    error
						}{
							"zone1-0000000100": {
								Response: &vtctldatapb.GetSchemaResponse{
									Schema: &tabletmanagerdatapb.SchemaDefinition{
										DatabaseSchema: "CREATE DATABASE vt_testkeyspcae",
										TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
											{
												Name:       "foo",
												Schema:     "CREATE TABLE foo (\n\tid INT(11) NOT NULL\n) ENGINE=InnoDB",
												DataLength: 100,
												RowCount:   5,
											},
										},
									},
								},
							},
							"zone1-0000000200": {
								Response: &vtctldatapb.GetSchemaResponse{
									Schema: &tabletmanagerdatapb.SchemaDefinition{
										DatabaseSchema: "CREATE DATABASE vt_testkeyspcae",
										TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
											{
												Name:       "foo",
												Schema:     "CREATE TABLE foo (\n\tid INT(11) NOT NULL\n) ENGINE=InnoDB",
												DataLength: 200,
												RowCount:   420,
											},
										},
									},
								},
							},
							"zone1-0000000300": {
								Response: &vtctldatapb.GetSchemaResponse{
									Schema: &tabletmanagerdatapb.SchemaDefinition{
										DatabaseSchema: "CREATE DATABASE vt_testkeyspcae",
										TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
											{
												Name:       "foo",
												Schema:     "CREATE TABLE foo (\n\tid INT(11) NOT NULL\n) ENGINE=InnoDB",
												DataLength: 1000,
												RowCount:   200,
											},
										},
									},
								},
							},
						},
					},
				},
				keyspace: "testkeyspace",
				opts: cluster.GetSchemaOptions{
					TableSizeOptions: &vtadminpb.GetSchemaTableSizeOptions{
						AggregateSizes:          true,
						IncludeNonServingShards: true,
					},
				},
				expected: &vtadminpb.Schema{
					Cluster: &vtadminpb.Cluster{
						Id:   "c0",
						Name: "cluster0",
					},
					Keyspace: "testkeyspace",
					TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
						{
							Name:       "foo",
							Schema:     "CREATE TABLE foo (\n\tid INT(11) NOT NULL\n) ENGINE=InnoDB",
							DataLength: 0,
							RowCount:   0,
						},
					},
					TableSizes: map[string]*vtadminpb.Schema_TableSize{
						"foo": {
							DataLength: 100 + 200 + 1000,
							RowCount:   5 + 420 + 200,
							ByShard: map[string]*vtadminpb.Schema_ShardTableSize{
								"-80": {
									DataLength: 100,
									RowCount:   5,
								},
								"80-": {
									DataLength: 200,
									RowCount:   420,
								},
								"-": {
									DataLength: 1000,
									RowCount:   200,
								},
							},
						},
					},
				},
				shouldErr: false,
			},
			{
				name: "no serving tablets found for shard",
				cfg: testutil.TestClusterConfig{
					Cluster: &vtadminpb.Cluster{
						Id:   "c0",
						Name: "cluster0",
					},
					Tablets: []*vtadminpb.Tablet{
						{
							Tablet: &topodatapb.Tablet{
								Alias: &topodatapb.TabletAlias{
									Cell: "zone1",
									Uid:  100,
								},
								Keyspace: "testkeyspace",
								Shard:    "-80",
							},
							State: vtadminpb.Tablet_NOT_SERVING,
						},
						{
							Tablet: &topodatapb.Tablet{
								Alias: &topodatapb.TabletAlias{
									Cell: "zone1",
									Uid:  200,
								},
								Keyspace: "testkeyspace",
								Shard:    "80-",
							},
							State: vtadminpb.Tablet_SERVING,
						},
					},
					VtctldClient: &fakevtctldclient.VtctldClient{
						FindAllShardsInKeyspaceResults: map[string]struct {
							Response *vtctldatapb.FindAllShardsInKeyspaceResponse
							Error    error
						}{
							"testkeyspace": {
								Response: &vtctldatapb.FindAllShardsInKeyspaceResponse{
									Shards: map[string]*vtctldatapb.Shard{
										"-80": {
											Name: "-80",
											Shard: &topodatapb.Shard{
												IsPrimaryServing: true,
												PrimaryAlias: &topodatapb.TabletAlias{
													Cell: "zone1",
													Uid:  100,
												},
											},
										},
										"80-": {
											Name: "80-",
											Shard: &topodatapb.Shard{
												IsPrimaryServing: true,
												PrimaryAlias: &topodatapb.TabletAlias{
													Cell: "zone1",
													Uid:  200,
												},
											},
										},
										"-": {
											Name: "-",
											Shard: &topodatapb.Shard{
												IsPrimaryServing: false,
											},
										},
									},
								},
							},
						},
						GetSchemaResults: map[string]struct {
							Response *vtctldatapb.GetSchemaResponse
							Error    error
						}{
							"zone1-0000000100": {
								Response: &vtctldatapb.GetSchemaResponse{
									Schema: &tabletmanagerdatapb.SchemaDefinition{
										DatabaseSchema: "CREATE DATABASE vt_testkeyspcae",
										TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
											{
												Name:       "foo",
												Schema:     "CREATE TABLE foo (\n\tid INT(11) NOT NULL\n) ENGINE=InnoDB",
												DataLength: 100,
												RowCount:   5,
											},
										},
									},
								},
							},
							"zone1-0000000200": {
								Response: &vtctldatapb.GetSchemaResponse{
									Schema: &tabletmanagerdatapb.SchemaDefinition{
										DatabaseSchema: "CREATE DATABASE vt_testkeyspcae",
										TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
											{
												Name:       "foo",
												Schema:     "CREATE TABLE foo (\n\tid INT(11) NOT NULL\n) ENGINE=InnoDB",
												DataLength: 200,
												RowCount:   420,
											},
										},
									},
								},
							},
						},
					},
				},
				keyspace: "testkeyspace",
				opts: cluster.GetSchemaOptions{
					TableSizeOptions: &vtadminpb.GetSchemaTableSizeOptions{
						AggregateSizes: true,
					},
				},
				expected:  nil,
				shouldErr: true,
			},
			{
				name: "ignore TableNamesOnly",
				cfg: testutil.TestClusterConfig{
					Cluster: &vtadminpb.Cluster{
						Id:   "c0",
						Name: "cluster0",
					},
					Tablets: []*vtadminpb.Tablet{
						{
							Tablet: &topodatapb.Tablet{
								Alias: &topodatapb.TabletAlias{
									Cell: "zone1",
									Uid:  100,
								},
								Keyspace: "testkeyspace",
								Shard:    "-80",
							},
							State: vtadminpb.Tablet_SERVING,
						},
						{
							Tablet: &topodatapb.Tablet{
								Alias: &topodatapb.TabletAlias{
									Cell: "zone1",
									Uid:  200,
								},
								Keyspace: "testkeyspace",
								Shard:    "80-",
							},
							State: vtadminpb.Tablet_SERVING,
						},
					},
					VtctldClient: &fakevtctldclient.VtctldClient{
						FindAllShardsInKeyspaceResults: map[string]struct {
							Response *vtctldatapb.FindAllShardsInKeyspaceResponse
							Error    error
						}{
							"testkeyspace": {
								Response: &vtctldatapb.FindAllShardsInKeyspaceResponse{
									Shards: map[string]*vtctldatapb.Shard{
										"-80": {
											Name: "-80",
											Shard: &topodatapb.Shard{
												IsPrimaryServing: true,
												PrimaryAlias: &topodatapb.TabletAlias{
													Cell: "zone1",
													Uid:  100,
												},
											},
										},
										"80-": {
											Name: "80-",
											Shard: &topodatapb.Shard{
												IsPrimaryServing: true,
												PrimaryAlias: &topodatapb.TabletAlias{
													Cell: "zone1",
													Uid:  200,
												},
											},
										},
										"-": {
											Name: "-",
											Shard: &topodatapb.Shard{
												IsPrimaryServing: false,
											},
										},
									},
								},
							},
						},
						GetSchemaResults: map[string]struct {
							Response *vtctldatapb.GetSchemaResponse
							Error    error
						}{
							"zone1-0000000100": {
								Response: &vtctldatapb.GetSchemaResponse{
									Schema: &tabletmanagerdatapb.SchemaDefinition{
										DatabaseSchema: "CREATE DATABASE vt_testkeyspcae",
										TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
											{
												Name:       "foo",
												Schema:     "CREATE TABLE foo (\n\tid INT(11) NOT NULL\n) ENGINE=InnoDB",
												DataLength: 100,
												RowCount:   5,
											},
										},
									},
								},
							},
							"zone1-0000000200": {
								Response: &vtctldatapb.GetSchemaResponse{
									Schema: &tabletmanagerdatapb.SchemaDefinition{
										DatabaseSchema: "CREATE DATABASE vt_testkeyspcae",
										TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
											{
												Name:       "foo",
												Schema:     "CREATE TABLE foo (\n\tid INT(11) NOT NULL\n) ENGINE=InnoDB",
												DataLength: 200,
												RowCount:   420,
											},
										},
									},
								},
							},
						},
					},
				},
				keyspace: "testkeyspace",
				opts: cluster.GetSchemaOptions{
					BaseRequest: &vtctldatapb.GetSchemaRequest{
						TableNamesOnly: true, // Just checking things to blow up if this gets set.
					},
					TableSizeOptions: &vtadminpb.GetSchemaTableSizeOptions{
						AggregateSizes: true,
					},
				},
				expected: &vtadminpb.Schema{
					Cluster: &vtadminpb.Cluster{
						Id:   "c0",
						Name: "cluster0",
					},
					Keyspace: "testkeyspace",
					TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
						{
							Name:       "foo",
							Schema:     "CREATE TABLE foo (\n\tid INT(11) NOT NULL\n) ENGINE=InnoDB",
							DataLength: 0,
							RowCount:   0,
						},
					},
					TableSizes: map[string]*vtadminpb.Schema_TableSize{
						"foo": {
							DataLength: 100 + 200,
							RowCount:   5 + 420,
							ByShard: map[string]*vtadminpb.Schema_ShardTableSize{
								"-80": {
									DataLength: 100,
									RowCount:   5,
								},
								"80-": {
									DataLength: 200,
									RowCount:   420,
								},
							},
						},
					},
				},
				shouldErr: false,
			},
			{
				name: "single GetSchema error fails the request",
				cfg: testutil.TestClusterConfig{
					Cluster: &vtadminpb.Cluster{
						Id:   "c0",
						Name: "cluster0",
					},
					Tablets: []*vtadminpb.Tablet{
						{
							Tablet: &topodatapb.Tablet{
								Alias: &topodatapb.TabletAlias{
									Cell: "zone1",
									Uid:  100,
								},
								Keyspace: "testkeyspace",
								Shard:    "-80",
							},
							State: vtadminpb.Tablet_SERVING,
						},
						{
							Tablet: &topodatapb.Tablet{
								Alias: &topodatapb.TabletAlias{
									Cell: "zone1",
									Uid:  200,
								},
								Keyspace: "testkeyspace",
								Shard:    "80-",
							},
							State: vtadminpb.Tablet_SERVING,
						},
					},
					VtctldClient: &fakevtctldclient.VtctldClient{
						FindAllShardsInKeyspaceResults: map[string]struct {
							Response *vtctldatapb.FindAllShardsInKeyspaceResponse
							Error    error
						}{
							"testkeyspace": {
								Response: &vtctldatapb.FindAllShardsInKeyspaceResponse{
									Shards: map[string]*vtctldatapb.Shard{
										"-80": {
											Name: "-80",
											Shard: &topodatapb.Shard{
												IsPrimaryServing: true,
												PrimaryAlias: &topodatapb.TabletAlias{
													Cell: "zone1",
													Uid:  100,
												},
											},
										},
										"80-": {
											Name: "80-",
											Shard: &topodatapb.Shard{
												IsPrimaryServing: true,
												PrimaryAlias: &topodatapb.TabletAlias{
													Cell: "zone1",
													Uid:  200,
												},
											},
										},
										"-": {
											Name: "-",
											Shard: &topodatapb.Shard{
												IsPrimaryServing: false,
											},
										},
									},
								},
							},
						},
						GetSchemaResults: map[string]struct {
							Response *vtctldatapb.GetSchemaResponse
							Error    error
						}{
							"zone1-0000000100": {
								Response: &vtctldatapb.GetSchemaResponse{
									Schema: &tabletmanagerdatapb.SchemaDefinition{
										DatabaseSchema: "CREATE DATABASE vt_testkeyspcae",
										TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
											{
												Name:       "foo",
												Schema:     "CREATE TABLE foo (\n\tid INT(11) NOT NULL\n) ENGINE=InnoDB",
												DataLength: 100,
												RowCount:   5,
											},
										},
									},
								},
							},
							"zone1-0000000200": {
								Error: assert.AnError,
							},
						},
					},
				},
				keyspace: "testkeyspace",
				opts: cluster.GetSchemaOptions{
					TableSizeOptions: &vtadminpb.GetSchemaTableSizeOptions{
						AggregateSizes: true,
					},
				},
				expected:  nil,
				shouldErr: true,
			},
			{
				name: "FindAllShardsInKeyspace error",
				cfg: testutil.TestClusterConfig{
					Cluster: &vtadminpb.Cluster{
						Id:   "c0",
						Name: "cluster0",
					},
					Tablets: []*vtadminpb.Tablet{
						{
							Tablet: &topodatapb.Tablet{
								Alias: &topodatapb.TabletAlias{
									Cell: "zone1",
									Uid:  100,
								},
								Keyspace: "testkeyspace",
								Shard:    "-80",
							},
							State: vtadminpb.Tablet_SERVING,
						},
						{
							Tablet: &topodatapb.Tablet{
								Alias: &topodatapb.TabletAlias{
									Cell: "zone1",
									Uid:  200,
								},
								Keyspace: "testkeyspace",
								Shard:    "80-",
							},
							State: vtadminpb.Tablet_SERVING,
						},
					},
					VtctldClient: &fakevtctldclient.VtctldClient{
						FindAllShardsInKeyspaceResults: map[string]struct {
							Response *vtctldatapb.FindAllShardsInKeyspaceResponse
							Error    error
						}{
							"testkeyspace": {
								Error: assert.AnError,
							},
						},
						GetSchemaResults: map[string]struct {
							Response *vtctldatapb.GetSchemaResponse
							Error    error
						}{
							"zone1-0000000100": {
								Response: &vtctldatapb.GetSchemaResponse{
									Schema: &tabletmanagerdatapb.SchemaDefinition{
										DatabaseSchema: "CREATE DATABASE vt_testkeyspcae",
										TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
											{
												Name:       "foo",
												Schema:     "CREATE TABLE foo (\n\tid INT(11) NOT NULL\n) ENGINE=InnoDB",
												DataLength: 100,
												RowCount:   5,
											},
										},
									},
								},
							},
							"zone1-0000000200": {
								Response: &vtctldatapb.GetSchemaResponse{
									Schema: &tabletmanagerdatapb.SchemaDefinition{
										DatabaseSchema: "CREATE DATABASE vt_testkeyspcae",
										TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
											{
												Name:       "foo",
												Schema:     "CREATE TABLE foo (\n\tid INT(11) NOT NULL\n) ENGINE=InnoDB",
												DataLength: 200,
												RowCount:   420,
											},
										},
									},
								},
							},
						},
					},
				},
				keyspace: "testkeyspace",
				opts: cluster.GetSchemaOptions{
					TableSizeOptions: &vtadminpb.GetSchemaTableSizeOptions{
						AggregateSizes: true,
					},
				},
				expected:  nil,
				shouldErr: true,
			},
			{
				name: "tablet filtering checks keyspace field",
				cfg: testutil.TestClusterConfig{
					Cluster: &vtadminpb.Cluster{
						Id:   "c0",
						Name: "cluster0",
					},
					Tablets: []*vtadminpb.Tablet{
						{
							Tablet: &topodatapb.Tablet{
								Alias: &topodatapb.TabletAlias{
									Cell: "zone1",
									Uid:  100,
								},
								Keyspace: "testkeyspace",
								Shard:    "-80",
							},
							State: vtadminpb.Tablet_SERVING,
						},
						{
							Tablet: &topodatapb.Tablet{
								Alias: &topodatapb.TabletAlias{
									Cell: "zone1",
									Uid:  300,
								},
								// Note this is for another keyspace, so we fail to find a tablet for testkeyspace/-80.
								Keyspace: "otherkeyspace",
								Shard:    "80-",
							},
							State: vtadminpb.Tablet_SERVING,
						},
					},
					VtctldClient: &fakevtctldclient.VtctldClient{
						FindAllShardsInKeyspaceResults: map[string]struct {
							Response *vtctldatapb.FindAllShardsInKeyspaceResponse
							Error    error
						}{
							"testkeyspace": {
								Response: &vtctldatapb.FindAllShardsInKeyspaceResponse{
									Shards: map[string]*vtctldatapb.Shard{
										"-80": {
											Name: "-80",
											Shard: &topodatapb.Shard{
												IsPrimaryServing: true,
												PrimaryAlias: &topodatapb.TabletAlias{
													Cell: "zone1",
													Uid:  100,
												},
											},
										},
										"80-": {
											Name: "80-",
											Shard: &topodatapb.Shard{
												IsPrimaryServing: true,
												PrimaryAlias: &topodatapb.TabletAlias{
													Cell: "zone1",
													Uid:  200,
												},
											},
										},
									},
								},
							},
						},
						GetSchemaResults: map[string]struct {
							Response *vtctldatapb.GetSchemaResponse
							Error    error
						}{
							"zone1-0000000100": {
								Response: &vtctldatapb.GetSchemaResponse{
									Schema: &tabletmanagerdatapb.SchemaDefinition{
										DatabaseSchema: "CREATE DATABASE vt_testkeyspcae",
										TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
											{
												Name:       "foo",
												Schema:     "CREATE TABLE foo (\n\tid INT(11) NOT NULL\n) ENGINE=InnoDB",
												DataLength: 100,
												RowCount:   5,
											},
										},
									},
								},
							},
							"zone1-0000000200": {
								Response: &vtctldatapb.GetSchemaResponse{
									Schema: &tabletmanagerdatapb.SchemaDefinition{
										DatabaseSchema: "CREATE DATABASE vt_testkeyspcae",
										TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
											{
												Name:       "foo",
												Schema:     "CREATE TABLE foo (\n\tid INT(11) NOT NULL\n) ENGINE=InnoDB",
												DataLength: 200,
												RowCount:   420,
											},
										},
									},
								},
							},
							"zone1-0000000300": {
								Response: &vtctldatapb.GetSchemaResponse{
									Schema: &tabletmanagerdatapb.SchemaDefinition{
										DatabaseSchema: "CREATE DATABASE vt_otherkeyspacekeyspcae",
										TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
											{
												Name:       "bar",
												Schema:     "CREATE TABLE bar (\n\tid INT(11) NOT NULL\n) ENGINE=InnoDB",
												DataLength: 101,
												RowCount:   202,
											},
										},
									},
								},
							},
						},
					},
				},
				keyspace: "testkeyspace",
				opts: cluster.GetSchemaOptions{
					TableSizeOptions: &vtadminpb.GetSchemaTableSizeOptions{
						AggregateSizes: true,
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

				if tt.keyspace == "" {
					t.SkipNow()
				}

				c := testutil.BuildCluster(t, tt.cfg)
				schema, err := c.GetSchema(ctx, tt.keyspace, tt.opts)
				if tt.shouldErr {
					assert.Error(t, err)

					return
				}

				// Clone so our mutation below doesn't trip the race detector.
				schema = proto.Clone(schema).(*vtadminpb.Schema)

				if schema.TableDefinitions != nil {
					// For simplicity, we're going to assert only on the state
					// of the aggregated sizes (in schema.TableSizes), since the
					// TableDefinitions size values depends on tablet iteration
					// order, and that's not something we're interested in
					// coupling the implementation to.
					for _, td := range schema.TableDefinitions {
						td.DataLength = 0
						td.RowCount = 0
					}
				}

				assert.NoError(t, err)
				testutil.AssertSchemaSlicesEqual(t, []*vtadminpb.Schema{tt.expected}, []*vtadminpb.Schema{schema})
			})
		}
	})
}

func TestGetShardReplicationPositions(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tests := []struct {
		name      string
		cfg       testutil.TestClusterConfig
		req       *vtadminpb.GetShardReplicationPositionsRequest
		expected  []*vtadminpb.ClusterShardReplicationPosition
		shouldErr bool
	}{
		{
			cfg: testutil.TestClusterConfig{
				Cluster: &vtadminpb.Cluster{
					Id:   "c1",
					Name: "cluster1",
				},
				VtctldClient: &fakevtctldclient.VtctldClient{
					GetKeyspaceResults: map[string]struct {
						Response *vtctldatapb.GetKeyspaceResponse
						Error    error
					}{
						"ks": {
							Response: &vtctldatapb.GetKeyspaceResponse{
								Keyspace: &vtctldatapb.Keyspace{
									Keyspace: &topodatapb.Keyspace{},
								},
							},
						},
					},
					FindAllShardsInKeyspaceResults: map[string]struct {
						Response *vtctldatapb.FindAllShardsInKeyspaceResponse
						Error    error
					}{
						"ks": {
							Response: &vtctldatapb.FindAllShardsInKeyspaceResponse{
								Shards: map[string]*vtctldatapb.Shard{
									"-": {
										Keyspace: "ks",
										Name:     "-",
										Shard:    &topodatapb.Shard{},
									},
								},
							},
						},
					},
					ShardReplicationPositionsResults: map[string]struct {
						Response *vtctldatapb.ShardReplicationPositionsResponse
						Error    error
					}{
						"ks/-": {
							Response: &vtctldatapb.ShardReplicationPositionsResponse{
								ReplicationStatuses: map[string]*replicationdatapb.Status{
									"zone1-001": {
										IoState:  int32(mysql.ReplicationStateStopped),
										SqlState: int32(mysql.ReplicationStateStopped),
										Position: "MySQL56/08d0dbbb-be29-11eb-9fea-0aafb9701138:1-109848265",
									},
									"zone1-002": { // Note: in reality other fields will be set on replicating hosts as well, but this is sufficient to illustrate in the testing.
										IoState:  int32(mysql.ReplicationStateRunning),
										SqlState: int32(mysql.ReplicationStateRunning),
										Position: "MySQL56/08d0dbbb-be29-11eb-9fea-0aafb9701138:1-109848265",
									},
									"zone1-003": {
										IoState:  int32(mysql.ReplicationStateRunning),
										SqlState: int32(mysql.ReplicationStateRunning),
										Position: "MySQL56/08d0dbbb-be29-11eb-9fea-0aafb9701138:1-109848265",
									},
								},
								TabletMap: map[string]*topodatapb.Tablet{
									"zone1-001": {
										Keyspace: "ks",
										Shard:    "-",
										Type:     topodatapb.TabletType_PRIMARY,
										Alias: &topodatapb.TabletAlias{
											Cell: "zone1",
											Uid:  1,
										},
									},
									"zone1-002": {
										Keyspace: "ks",
										Shard:    "-",
										Type:     topodatapb.TabletType_REPLICA,
										Alias: &topodatapb.TabletAlias{
											Cell: "zone1",
											Uid:  2,
										},
									},
									"zone1-003": {
										Keyspace: "ks",
										Shard:    "-",
										Type:     topodatapb.TabletType_RDONLY,
										Alias: &topodatapb.TabletAlias{
											Cell: "zone1",
											Uid:  3,
										},
									},
								},
							},
						},
					},
				},
			},
			req: &vtadminpb.GetShardReplicationPositionsRequest{
				KeyspaceShards: []string{"ks/-"},
			},
			expected: []*vtadminpb.ClusterShardReplicationPosition{
				{
					Cluster: &vtadminpb.Cluster{
						Id:   "c1",
						Name: "cluster1",
					},
					Keyspace: "ks",
					Shard:    "-",
					PositionInfo: &vtctldatapb.ShardReplicationPositionsResponse{
						ReplicationStatuses: map[string]*replicationdatapb.Status{
							"zone1-001": {
								IoState:  int32(mysql.ReplicationStateStopped),
								SqlState: int32(mysql.ReplicationStateStopped),
								Position: "MySQL56/08d0dbbb-be29-11eb-9fea-0aafb9701138:1-109848265",
							},
							"zone1-002": {
								IoState:  int32(mysql.ReplicationStateRunning),
								SqlState: int32(mysql.ReplicationStateRunning),
								Position: "MySQL56/08d0dbbb-be29-11eb-9fea-0aafb9701138:1-109848265",
							},
							"zone1-003": {
								IoState:  int32(mysql.ReplicationStateRunning),
								SqlState: int32(mysql.ReplicationStateRunning),
								Position: "MySQL56/08d0dbbb-be29-11eb-9fea-0aafb9701138:1-109848265",
							},
						},
						TabletMap: map[string]*topodatapb.Tablet{
							"zone1-001": {
								Keyspace: "ks",
								Shard:    "-",
								Type:     topodatapb.TabletType_PRIMARY,
								Alias: &topodatapb.TabletAlias{
									Cell: "zone1",
									Uid:  1,
								},
							},
							"zone1-002": {
								Keyspace: "ks",
								Shard:    "-",
								Type:     topodatapb.TabletType_REPLICA,
								Alias: &topodatapb.TabletAlias{
									Cell: "zone1",
									Uid:  2,
								},
							},
							"zone1-003": {
								Keyspace: "ks",
								Shard:    "-",
								Type:     topodatapb.TabletType_RDONLY,
								Alias: &topodatapb.TabletAlias{
									Cell: "zone1",
									Uid:  3,
								},
							},
						},
					},
				},
			},
		},
		{
			name: "error",
			cfg: testutil.TestClusterConfig{
				Cluster: &vtadminpb.Cluster{
					Id:   "c1",
					Name: "cluster1",
				},
				VtctldClient: &fakevtctldclient.VtctldClient{
					GetKeyspaceResults: map[string]struct {
						Response *vtctldatapb.GetKeyspaceResponse
						Error    error
					}{
						"ks": {
							Response: &vtctldatapb.GetKeyspaceResponse{
								Keyspace: &vtctldatapb.Keyspace{
									Keyspace: &topodatapb.Keyspace{},
								},
							},
						},
					},
					FindAllShardsInKeyspaceResults: map[string]struct {
						Response *vtctldatapb.FindAllShardsInKeyspaceResponse
						Error    error
					}{
						"ks": {
							Response: &vtctldatapb.FindAllShardsInKeyspaceResponse{
								Shards: map[string]*vtctldatapb.Shard{
									"-": {
										Keyspace: "ks",
										Name:     "-",
										Shard:    &topodatapb.Shard{},
									},
								},
							},
						},
					},
					ShardReplicationPositionsResults: map[string]struct {
						Response *vtctldatapb.ShardReplicationPositionsResponse
						Error    error
					}{
						"ks/-": {
							Error: assert.AnError,
						},
					},
				},
			},
			req: &vtadminpb.GetShardReplicationPositionsRequest{
				KeyspaceShards: []string{"ks/-"},
			},
			expected:  nil,
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := testutil.BuildCluster(t, tt.cfg)

			resp, err := c.GetShardReplicationPositions(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expected, resp)
		})
	}
}

func TestGetVSchema(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		cfg       testutil.TestClusterConfig
		keyspace  string
		expected  *vtadminpb.VSchema
		shouldErr bool
	}{
		{
			name: "success",
			cfg: testutil.TestClusterConfig{
				Cluster: &vtadminpb.Cluster{
					Id:   "c0",
					Name: "cluster0",
				},
				VtctldClient: &fakevtctldclient.VtctldClient{
					GetVSchemaResults: map[string]struct {
						Response *vtctldatapb.GetVSchemaResponse
						Error    error
					}{
						"testkeyspace": {
							Response: &vtctldatapb.GetVSchemaResponse{
								VSchema: &vschemapb.Keyspace{Sharded: true},
							},
						},
					},
				},
			},
			keyspace: "testkeyspace",
			expected: &vtadminpb.VSchema{
				Cluster: &vtadminpb.Cluster{
					Id:   "c0",
					Name: "cluster0",
				},
				Name:    "testkeyspace",
				VSchema: &vschemapb.Keyspace{Sharded: true},
			},
			shouldErr: false,
		},
		{
			name: "error",
			cfg: testutil.TestClusterConfig{
				Cluster: &vtadminpb.Cluster{
					Id:   "c0",
					Name: "cluster0",
				},
				VtctldClient: &fakevtctldclient.VtctldClient{
					GetVSchemaResults: map[string]struct {
						Response *vtctldatapb.GetVSchemaResponse
						Error    error
					}{
						"testkeyspace": {
							Response: &vtctldatapb.GetVSchemaResponse{
								VSchema: &vschemapb.Keyspace{Sharded: true},
							},
						},
					},
				},
			},
			keyspace:  "notfound",
			expected:  nil,
			shouldErr: true,
		},
	}

	ctx := context.Background()

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cluster := testutil.BuildCluster(t, tt.cfg)

			vschema, err := cluster.GetVSchema(ctx, tt.keyspace)
			if tt.shouldErr {
				assert.Error(t, err)

				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.expected, vschema)
		})
	}
}

func TestGetWorkflow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		cfg       testutil.TestClusterConfig
		keyspace  string
		workflow  string
		opts      cluster.GetWorkflowOptions
		expected  *vtadminpb.Workflow
		shouldErr bool
	}{
		{
			name: "found",
			cfg: testutil.TestClusterConfig{
				Cluster: &vtadminpb.Cluster{
					Id:   "c1",
					Name: "cluster1",
				},
				VtctldClient: &fakevtctldclient.VtctldClient{
					GetWorkflowsResults: map[string]struct {
						Response *vtctldatapb.GetWorkflowsResponse
						Error    error
					}{
						"ks1": {
							Response: &vtctldatapb.GetWorkflowsResponse{
								Workflows: []*vtctldatapb.Workflow{
									{
										Name: "workflow1",
									},
									{
										Name: "workflow2",
									},
								},
							},
						},
					},
				},
			},
			keyspace: "ks1",
			workflow: "workflow2",
			expected: &vtadminpb.Workflow{
				Cluster: &vtadminpb.Cluster{
					Id:   "c1",
					Name: "cluster1",
				},
				Keyspace: "ks1",
				Workflow: &vtctldatapb.Workflow{
					Name: "workflow2",
				},
			},
			shouldErr: false,
		},
		{
			name: "error getting workflows",
			cfg: testutil.TestClusterConfig{
				Cluster: &vtadminpb.Cluster{
					Id:   "c1",
					Name: "cluster1",
				},
				VtctldClient: &fakevtctldclient.VtctldClient{
					GetWorkflowsResults: map[string]struct {
						Response *vtctldatapb.GetWorkflowsResponse
						Error    error
					}{
						"ks1": {
							Error: assert.AnError,
						},
					},
				},
			},
			keyspace:  "ks1",
			workflow:  "workflow2",
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "no workflows found",
			cfg: testutil.TestClusterConfig{
				Cluster: &vtadminpb.Cluster{
					Id:   "c1",
					Name: "cluster1",
				},
				VtctldClient: &fakevtctldclient.VtctldClient{
					GetWorkflowsResults: map[string]struct {
						Response *vtctldatapb.GetWorkflowsResponse
						Error    error
					}{
						"ks1": {
							Response: &vtctldatapb.GetWorkflowsResponse{
								Workflows: []*vtctldatapb.Workflow{},
							},
						},
					},
				},
			},
			keyspace:  "ks1",
			workflow:  "workflow2",
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "multiple workflows found",
			cfg: testutil.TestClusterConfig{
				Cluster: &vtadminpb.Cluster{
					Id:   "c1",
					Name: "cluster1",
				},
				VtctldClient: &fakevtctldclient.VtctldClient{
					GetWorkflowsResults: map[string]struct {
						Response *vtctldatapb.GetWorkflowsResponse
						Error    error
					}{
						"ks1": {
							Response: &vtctldatapb.GetWorkflowsResponse{
								Workflows: []*vtctldatapb.Workflow{
									{
										Name: "duplicate",
									},
									{
										Name: "duplicate",
									},
								},
							},
						},
					},
				},
			},
			keyspace:  "ks1",
			workflow:  "duplicate",
			expected:  nil,
			shouldErr: true,
		},
	}

	ctx := context.Background()

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := testutil.BuildCluster(t, tt.cfg)
			workflow, err := c.GetWorkflow(ctx, tt.keyspace, tt.workflow, tt.opts)
			if tt.shouldErr {
				assert.Error(t, err)

				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.expected, workflow)
		})
	}
}

func TestGetWorkflows(t *testing.T) {
	t.Parallel()

	// Note: GetWorkflows is almost entirely a passthrough to FindWorkflows, so
	// these test cases mostly just verify we're calling that function more or
	// less correctly.

	tests := []struct {
		name      string
		cfg       testutil.TestClusterConfig
		keyspaces []string
		opts      cluster.GetWorkflowsOptions
		expected  *vtadminpb.ClusterWorkflows
		shouldErr bool
	}{
		{
			name: "success",
			cfg: testutil.TestClusterConfig{
				Cluster: &vtadminpb.Cluster{
					Id:   "c1",
					Name: "cluster1",
				},
				VtctldClient: &fakevtctldclient.VtctldClient{
					GetWorkflowsResults: map[string]struct {
						Response *vtctldatapb.GetWorkflowsResponse
						Error    error
					}{
						"ks1": {
							Response: &vtctldatapb.GetWorkflowsResponse{
								Workflows: []*vtctldatapb.Workflow{
									{
										Name: "ks1-workflow1",
									},
								},
							},
						},
						"ks2": {
							Response: &vtctldatapb.GetWorkflowsResponse{
								Workflows: []*vtctldatapb.Workflow{
									{
										Name: "ks2-workflow1",
									},
								},
							},
						},
					},
				},
			},
			keyspaces: []string{"ks1", "ks2"},
			expected: &vtadminpb.ClusterWorkflows{
				Workflows: []*vtadminpb.Workflow{
					{
						Cluster: &vtadminpb.Cluster{
							Id:   "c1",
							Name: "cluster1",
						},
						Keyspace: "ks1",
						Workflow: &vtctldatapb.Workflow{
							Name: "ks1-workflow1",
						},
					},
					{
						Cluster: &vtadminpb.Cluster{
							Id:   "c1",
							Name: "cluster1",
						},
						Keyspace: "ks2",
						Workflow: &vtctldatapb.Workflow{
							Name: "ks2-workflow1",
						},
					},
				},
			},
			shouldErr: false,
		},
		{
			name: "partial error",
			cfg: testutil.TestClusterConfig{
				Cluster: &vtadminpb.Cluster{
					Id:   "c1",
					Name: "cluster1",
				},
				VtctldClient: &fakevtctldclient.VtctldClient{
					GetWorkflowsResults: map[string]struct {
						Response *vtctldatapb.GetWorkflowsResponse
						Error    error
					}{
						"ks1": {
							Response: &vtctldatapb.GetWorkflowsResponse{
								Workflows: []*vtctldatapb.Workflow{
									{
										Name: "ks1-workflow1",
									},
								},
							},
						},
						"ks2": {
							Error: assert.AnError,
						},
					},
				},
			},
			keyspaces: []string{"ks1", "ks2"},
			expected: &vtadminpb.ClusterWorkflows{
				Workflows: []*vtadminpb.Workflow{
					{
						Cluster: &vtadminpb.Cluster{
							Id:   "c1",
							Name: "cluster1",
						},
						Keyspace: "ks1",
						Workflow: &vtctldatapb.Workflow{
							Name: "ks1-workflow1",
						},
					},
				},
				Warnings: []string{"something about ks2"},
			},
			shouldErr: false,
		},
		{
			name: "error",
			cfg: testutil.TestClusterConfig{
				Cluster: &vtadminpb.Cluster{
					Id:   "c1",
					Name: "cluster1",
				},
				VtctldClient: &fakevtctldclient.VtctldClient{
					GetWorkflowsResults: map[string]struct {
						Response *vtctldatapb.GetWorkflowsResponse
						Error    error
					}{
						"ks1": {
							Error: assert.AnError,
						},
					},
				},
			},
			keyspaces: []string{"ks1"},
			expected:  nil,
			shouldErr: true,
		},
	}

	ctx := context.Background()

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := testutil.BuildCluster(t, tt.cfg)
			workflows, err := c.GetWorkflows(ctx, tt.keyspaces, tt.opts)
			if tt.shouldErr {
				assert.Error(t, err)

				return
			}

			assert.NoError(t, err)
			testutil.AssertClusterWorkflowsEqual(t, tt.expected, workflows)
		})
	}
}

func TestSetWritable(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tests := []struct {
		name              string
		cfg               testutil.TestClusterConfig
		req               *vtctldatapb.SetWritableRequest
		assertion         func(t assert.TestingT, err error, msgAndArgs ...any) bool
		assertionMsgExtra []any
	}{
		{
			name: "ok",
			cfg: testutil.TestClusterConfig{
				Cluster: &vtadminpb.Cluster{
					Id:   "test",
					Name: "test",
				},
				VtctldClient: &fakevtctldclient.VtctldClient{
					SetWritableResults: map[string]error{
						"zone1-0000000100": nil,
					},
				},
			},
			req: &vtctldatapb.SetWritableRequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
				Writable: true,
			},
			assertion: assert.NoError,
		},
		{
			name: "error",
			cfg: testutil.TestClusterConfig{
				Cluster: &vtadminpb.Cluster{
					Id:   "test",
					Name: "test",
				},
				VtctldClient: &fakevtctldclient.VtctldClient{
					SetWritableResults: map[string]error{
						"zone1-0000000100": fmt.Errorf("some error"),
					},
				},
			},
			req: &vtctldatapb.SetWritableRequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
				Writable: true,
			},
			assertion: assert.Error,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := testutil.BuildCluster(t, tt.cfg)
			err := c.SetWritable(ctx, tt.req)
			tt.assertion(t, err, tt.assertionMsgExtra...)
		})
	}
}

func TestToggleTabletReplication(t *testing.T) {
	t.Parallel()

	type ReplicationState bool
	const (
		Start ReplicationState = true
		Stop  ReplicationState = false
	)

	testClusterProto := &vtadminpb.Cluster{
		Id:   "test",
		Name: "test",
	}

	ctx := context.Background()
	tests := []struct {
		name              string
		cfg               testutil.TestClusterConfig
		tablet            *vtadminpb.Tablet
		state             ReplicationState
		assertion         func(t assert.TestingT, err error, msgAndArgs ...any) bool
		assertionMsgExtra []any
	}{
		{
			name: "start/ok",
			cfg: testutil.TestClusterConfig{
				Cluster: testClusterProto,
				VtctldClient: &fakevtctldclient.VtctldClient{
					StartReplicationResults: map[string]error{
						"zone1-0000000100": nil,
						"zone1-0000000101": fmt.Errorf("some error"),
					},
					StopReplicationResults: map[string]error{
						"zone1-0000000100": fmt.Errorf("some error"),
						"zone1-0000000101": nil,
					},
				},
			},
			tablet: &vtadminpb.Tablet{
				Cluster: testClusterProto,
				Tablet: &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
				},
			},
			state:     Start,
			assertion: assert.NoError,
		},
		{
			name: "start/error",
			cfg: testutil.TestClusterConfig{
				Cluster: testClusterProto,
				VtctldClient: &fakevtctldclient.VtctldClient{
					StartReplicationResults: map[string]error{
						"zone1-0000000100": fmt.Errorf("some error"),
						"zone1-0000000101": nil,
					},
					StopReplicationResults: map[string]error{
						"zone1-0000000100": fmt.Errorf("some error"),
						"zone1-0000000101": nil,
					},
				},
			},
			tablet: &vtadminpb.Tablet{
				Cluster: testClusterProto,
				Tablet: &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
				},
			},
			state:     Start,
			assertion: assert.Error,
		},
		{
			name: "stop/ok",
			cfg: testutil.TestClusterConfig{
				Cluster: testClusterProto,
				VtctldClient: &fakevtctldclient.VtctldClient{
					StartReplicationResults: map[string]error{
						"zone1-0000000100": fmt.Errorf("some error"),
						"zone1-0000000101": nil,
					},
					StopReplicationResults: map[string]error{
						"zone1-0000000100": nil,
						"zone1-0000000101": fmt.Errorf("some error"),
					},
				},
			},
			tablet: &vtadminpb.Tablet{
				Cluster: testClusterProto,
				Tablet: &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
				},
			},
			state:     Stop,
			assertion: assert.NoError,
		},
		{
			name: "stop/error",
			cfg: testutil.TestClusterConfig{
				Cluster: testClusterProto,
				VtctldClient: &fakevtctldclient.VtctldClient{
					StartReplicationResults: map[string]error{
						"zone1-0000000100": fmt.Errorf("some error"),
						"zone1-0000000101": nil,
					},
					StopReplicationResults: map[string]error{
						"zone1-0000000100": fmt.Errorf("some error"),
						"zone1-0000000101": nil,
					},
				},
			},
			tablet: &vtadminpb.Tablet{
				Cluster: testClusterProto,
				Tablet: &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
				},
			},
			state:     Stop,
			assertion: assert.Error,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := testutil.BuildCluster(t, tt.cfg)
			err := c.ToggleTabletReplication(ctx, tt.tablet, bool(tt.state))
			tt.assertion(t, err, tt.assertionMsgExtra...)
		})
	}
}
