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

package vtadmin

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/vt/vtenv"

	_flag "vitess.io/vitess/go/internal/flag"
	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtadmin/cluster"
	"vitess.io/vitess/go/vt/vtadmin/cluster/discovery/fakediscovery"
	vtadminerrors "vitess.io/vitess/go/vt/vtadmin/errors"
	vtadmintestutil "vitess.io/vitess/go/vt/vtadmin/testutil"
	"vitess.io/vitess/go/vt/vtadmin/vtctldclient/fakevtctldclient"
	"vitess.io/vitess/go/vt/vtctl/grpcvtctldserver"
	"vitess.io/vitess/go/vt/vtctl/grpcvtctldserver/testutil"
	"vitess.io/vitess/go/vt/vtctl/vtctldclient"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	"vitess.io/vitess/go/vt/vttablet/tmclienttest"

	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtctlservicepb "vitess.io/vitess/go/vt/proto/vtctlservice"
	"vitess.io/vitess/go/vt/proto/vttime"
)

func TestMain(m *testing.M) {
	_flag.ParseFlagsForTest()
	os.Exit(m.Run())
}

func TestFindSchema(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		clusters  []vtadmintestutil.TestClusterConfig
		req       *vtadminpb.FindSchemaRequest
		expected  *vtadminpb.Schema
		shouldErr bool
	}{
		{
			name: "exact match",
			clusters: []vtadmintestutil.TestClusterConfig{
				{
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
									Name: "testkeyspace",
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
										TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
											{
												Name: "testtable",
											},
										},
									},
								},
							},
						},
						FindAllShardsInKeyspaceResults: map[string]struct {
							Response *vtctldatapb.FindAllShardsInKeyspaceResponse
							Error    error
						}{
							"testkeyspace": {
								Response: &vtctldatapb.FindAllShardsInKeyspaceResponse{
									Shards: map[string]*vtctldatapb.Shard{
										"-": {
											Shard: &topodatapb.Shard{
												IsPrimaryServing: true,
											},
										},
									},
								},
							},
						},
					},
					Tablets: []*vtadminpb.Tablet{
						{
							Tablet: &topodatapb.Tablet{
								Alias: &topodatapb.TabletAlias{
									Cell: "zone1",
									Uid:  100,
								},
								Keyspace: "testkeyspace",
								Shard:    "-",
							},
							State: vtadminpb.Tablet_SERVING,
						},
					},
				},
			},
			req: &vtadminpb.FindSchemaRequest{
				Table: "testtable",
			},
			expected: &vtadminpb.Schema{
				Cluster: &vtadminpb.Cluster{
					Id:   "c1",
					Name: "cluster1",
				},
				Keyspace: "testkeyspace",
				TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
					{
						Name: "testtable",
					},
				},
				TableSizes: map[string]*vtadminpb.Schema_TableSize{},
			},
			shouldErr: false,
		},
		{
			name: "error getting tablets",
			clusters: []vtadmintestutil.TestClusterConfig{
				{
					Cluster: &vtadminpb.Cluster{
						Id:   "c1",
						Name: "cluster1",
					},

					DBConfig: vtadmintestutil.Dbcfg{
						ShouldErr: true,
					},
					VtctldClient: &fakevtctldclient.VtctldClient{
						GetKeyspacesResults: &struct {
							Keyspaces []*vtctldatapb.Keyspace
							Error     error
						}{
							Keyspaces: []*vtctldatapb.Keyspace{
								{Name: "testkeyspace"},
							},
						},
					},
				},
			},
			req: &vtadminpb.FindSchemaRequest{
				Table: "testtable",
			},
			shouldErr: true,
		},
		{
			name: "error getting keyspaces",
			clusters: []vtadmintestutil.TestClusterConfig{
				{
					Cluster: &vtadminpb.Cluster{
						Id:   "c1",
						Name: "cluster1",
					},
					VtctldClient: &fakevtctldclient.VtctldClient{
						GetKeyspacesResults: &struct {
							Keyspaces []*vtctldatapb.Keyspace
							Error     error
						}{
							Error: fmt.Errorf("GetKeyspaces: %w", assert.AnError),
						},
					},
					Tablets: []*vtadminpb.Tablet{
						{
							Tablet: &topodatapb.Tablet{
								Alias: &topodatapb.TabletAlias{
									Cell: "zone1",
									Uid:  100,
								},
								Keyspace: "testkeyspace",
							},
							State: vtadminpb.Tablet_SERVING,
						},
					},
				},
			},
			req: &vtadminpb.FindSchemaRequest{
				Table: "testtable",
			},
			shouldErr: true,
		},
		{
			name: "error getting schemas",
			clusters: []vtadmintestutil.TestClusterConfig{
				{
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
									Name: "testkeyspace",
								},
							},
						},
						GetSchemaResults: map[string]struct {
							Response *vtctldatapb.GetSchemaResponse
							Error    error
						}{
							"zone1-0000000100": {
								Error: fmt.Errorf("GetSchema: %w", assert.AnError),
							},
						},
					},
					Tablets: []*vtadminpb.Tablet{
						{
							Tablet: &topodatapb.Tablet{
								Alias: &topodatapb.TabletAlias{
									Cell: "zone1",
									Uid:  100,
								},
								Keyspace: "testkeyspace",
							},
							State: vtadminpb.Tablet_SERVING,
						},
					},
				},
			},
			req: &vtadminpb.FindSchemaRequest{
				Table: "testtable",
			},
			shouldErr: true,
		},
		{
			name: "no schema found",
			clusters: []vtadmintestutil.TestClusterConfig{
				{
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
									Name: "testkeyspace",
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
										TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
											{
												Name: "othertable",
											},
										},
									},
								},
							},
						},
					},
					Tablets: []*vtadminpb.Tablet{
						{
							Tablet: &topodatapb.Tablet{
								Alias: &topodatapb.TabletAlias{
									Cell: "zone1",
									Uid:  100,
								},
								Keyspace: "testkeyspace",
							},
							State: vtadminpb.Tablet_SERVING,
						},
					},
				},
			},
			req: &vtadminpb.FindSchemaRequest{
				Table: "testtable",
			},
			shouldErr: true,
		},
		{
			name: "ambiguous schema errors",
			clusters: []vtadmintestutil.TestClusterConfig{
				{
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
									Name: "testkeyspace",
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
										TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
											{
												Name: "testtable",
											},
										},
									},
								},
							},
						},
					},
					Tablets: []*vtadminpb.Tablet{
						{
							Tablet: &topodatapb.Tablet{
								Alias: &topodatapb.TabletAlias{
									Cell: "zone1",
									Uid:  100,
								},
								Keyspace: "testkeyspace",
							},
							State: vtadminpb.Tablet_SERVING,
						},
					},
				},
				{
					Cluster: &vtadminpb.Cluster{
						Id:   "c2",
						Name: "cluster2",
					},
					VtctldClient: &fakevtctldclient.VtctldClient{
						GetKeyspacesResults: &struct {
							Keyspaces []*vtctldatapb.Keyspace
							Error     error
						}{
							Keyspaces: []*vtctldatapb.Keyspace{
								{
									Name: "testkeyspace",
								},
							},
						},
						GetSchemaResults: map[string]struct {
							Response *vtctldatapb.GetSchemaResponse
							Error    error
						}{
							"zone2-0000000200": {
								Response: &vtctldatapb.GetSchemaResponse{
									Schema: &tabletmanagerdatapb.SchemaDefinition{
										TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
											{
												Name: "testtable",
											},
										},
									},
								},
							},
						},
					},
					Tablets: []*vtadminpb.Tablet{
						{
							Tablet: &topodatapb.Tablet{
								Alias: &topodatapb.TabletAlias{
									Cell: "zone2",
									Uid:  200,
								},
								Keyspace: "testkeyspace",
							},
							State: vtadminpb.Tablet_SERVING,
						},
					},
				},
			},
			req: &vtadminpb.FindSchemaRequest{
				Table: "testtable",
			},
			shouldErr: true,
		},
		{
			name: "ambiguous schema with request scoped to single cluster passes",
			clusters: []vtadmintestutil.TestClusterConfig{
				{
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
									Name: "testkeyspace1",
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
										TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
											{
												Name: "testtable",
											},
										},
									},
								},
							},
						},
					},
					Tablets: []*vtadminpb.Tablet{
						{
							Tablet: &topodatapb.Tablet{
								Alias: &topodatapb.TabletAlias{
									Cell: "zone1",
									Uid:  100,
								},
								Keyspace: "testkeyspace1",
							},
							State: vtadminpb.Tablet_SERVING,
						},
					},
				},
				{
					Cluster: &vtadminpb.Cluster{
						Id:   "c2",
						Name: "cluster2",
					},
					VtctldClient: &fakevtctldclient.VtctldClient{
						GetKeyspacesResults: &struct {
							Keyspaces []*vtctldatapb.Keyspace
							Error     error
						}{
							Keyspaces: []*vtctldatapb.Keyspace{
								{
									Name: "testkeyspace2",
								},
							},
						},
						GetSchemaResults: map[string]struct {
							Response *vtctldatapb.GetSchemaResponse
							Error    error
						}{
							"zone2-0000000200": {
								Response: &vtctldatapb.GetSchemaResponse{
									Schema: &tabletmanagerdatapb.SchemaDefinition{
										TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
											{
												Name: "testtable",
											},
										},
									},
								},
							},
						},
					},
					Tablets: []*vtadminpb.Tablet{
						{
							Tablet: &topodatapb.Tablet{
								Alias: &topodatapb.TabletAlias{
									Cell: "zone2",
									Uid:  200,
								},
								Keyspace: "testkeyspace2",
							},
							State: vtadminpb.Tablet_SERVING,
						},
					},
				},
			},
			req: &vtadminpb.FindSchemaRequest{
				Table:      "testtable",
				ClusterIds: []string{"c1"},
			},
			expected: &vtadminpb.Schema{
				Cluster: &vtadminpb.Cluster{
					Id:   "c1",
					Name: "cluster1",
				},
				Keyspace: "testkeyspace1",
				TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
					{
						Name: "testtable",
					},
				},
				TableSizes: map[string]*vtadminpb.Schema_TableSize{},
			},
			shouldErr: false,
		},
	}

	ctx := context.Background()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			clusters := make([]*cluster.Cluster, len(tt.clusters))
			for i, cfg := range tt.clusters {
				clusters[i] = vtadmintestutil.BuildCluster(t, cfg)
			}

			api := NewAPI(vtenv.NewTestEnv(), clusters, Options{})
			defer api.Close()

			resp, err := api.FindSchema(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)

				return
			}

			assert.NoError(t, err)
			assert.Truef(t, proto.Equal(tt.expected, resp), "expected %v, got %v", tt.expected, resp)
		})
	}

	t.Run("size aggregation", func(t *testing.T) {
		t.Parallel()

		c1pb := &vtadminpb.Cluster{
			Id:   "c1",
			Name: "cluster1",
		}
		c2pb := &vtadminpb.Cluster{
			Id:   "c2",
			Name: "cluster2",
		}

		c1 := vtadmintestutil.BuildCluster(t, vtadmintestutil.TestClusterConfig{
			Cluster: c1pb,
			VtctldClient: &fakevtctldclient.VtctldClient{
				FindAllShardsInKeyspaceResults: map[string]struct {
					Response *vtctldatapb.FindAllShardsInKeyspaceResponse
					Error    error
				}{
					"testkeyspace": {
						Response: &vtctldatapb.FindAllShardsInKeyspaceResponse{
							Shards: map[string]*vtctldatapb.Shard{
								"-80": {
									Keyspace: "testkeyspace",
									Name:     "-80",
									Shard: &topodatapb.Shard{
										IsPrimaryServing: true,
										PrimaryAlias: &topodatapb.TabletAlias{
											Cell: "c1zone1",
											Uid:  100,
										},
									},
								},
								"80-": {
									Keyspace: "testkeyspace",
									Name:     "80-",
									Shard: &topodatapb.Shard{
										IsPrimaryServing: true,
										PrimaryAlias: &topodatapb.TabletAlias{
											Cell: "c1zone1",
											Uid:  200,
										},
									},
								},
							},
						},
					},
					"ks1": {
						Response: &vtctldatapb.FindAllShardsInKeyspaceResponse{
							Shards: map[string]*vtctldatapb.Shard{
								"-": {
									Keyspace: "ks1",
									Name:     "-",
									Shard: &topodatapb.Shard{
										IsPrimaryServing: true,
										PrimaryAlias: &topodatapb.TabletAlias{
											Cell: "c1zone1",
											Uid:  300,
										},
									},
								},
							},
						},
					},
				},
				GetKeyspacesResults: &struct {
					Keyspaces []*vtctldatapb.Keyspace
					Error     error
				}{
					Keyspaces: []*vtctldatapb.Keyspace{
						{Name: "testkeyspace"},
						{Name: "ks1"},
					},
				},
				GetSchemaResults: map[string]struct {
					Response *vtctldatapb.GetSchemaResponse
					Error    error
				}{
					"c1zone1-0000000100": {
						Response: &vtctldatapb.GetSchemaResponse{
							Schema: &tabletmanagerdatapb.SchemaDefinition{
								TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
									{
										Name:       "testtable",
										RowCount:   10,
										DataLength: 100,
									},
								},
							},
						},
					},
					"c1zone1-0000000200": {
						Response: &vtctldatapb.GetSchemaResponse{
							Schema: &tabletmanagerdatapb.SchemaDefinition{
								TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
									{
										Name:       "testtable",
										RowCount:   20,
										DataLength: 200,
									},
								},
							},
						},
					},
				},
			},
			Tablets: []*vtadminpb.Tablet{
				{
					Cluster: c1pb,
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "c1zone1",
							Uid:  100,
						},
						Keyspace: "testkeyspace",
						Shard:    "-80",
					},
					State: vtadminpb.Tablet_SERVING,
				},
				{
					Cluster: c1pb,
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "c1zone1",
							Uid:  200,
						},
						Keyspace: "testkeyspace",
						Shard:    "80-",
					},
					State: vtadminpb.Tablet_SERVING,
				},
			},
		},
		)
		c2 := vtadmintestutil.BuildCluster(t, vtadmintestutil.TestClusterConfig{
			Cluster: c2pb,
			VtctldClient: &fakevtctldclient.VtctldClient{
				FindAllShardsInKeyspaceResults: map[string]struct {
					Response *vtctldatapb.FindAllShardsInKeyspaceResponse
					Error    error
				}{
					"ks2": {
						Response: &vtctldatapb.FindAllShardsInKeyspaceResponse{
							Shards: map[string]*vtctldatapb.Shard{
								"-": {
									Keyspace: "ks2",
									Name:     "-",
									Shard: &topodatapb.Shard{
										IsPrimaryServing: true,
										PrimaryAlias: &topodatapb.TabletAlias{
											Cell: "c2z1",
											Uid:  100,
										},
									},
								},
							},
						},
					},
				},
				GetKeyspacesResults: &struct {
					Keyspaces []*vtctldatapb.Keyspace
					Error     error
				}{
					Keyspaces: []*vtctldatapb.Keyspace{
						{
							Name: "ks2",
						},
					},
				},
				GetSchemaResults: map[string]struct {
					Response *vtctldatapb.GetSchemaResponse
					Error    error
				}{
					"c2z1-0000000100": {
						Response: &vtctldatapb.GetSchemaResponse{},
					},
				},
			},
			Tablets: []*vtadminpb.Tablet{
				{
					Cluster: c2pb,
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "c2z1",
							Uid:  100,
						},
						Keyspace: "ks2",
						Shard:    "-",
					},
					State: vtadminpb.Tablet_SERVING,
				},
			},
		},
		)

		api := NewAPI(vtenv.NewTestEnv(), []*cluster.Cluster{c1, c2}, Options{})
		defer api.Close()

		schema, err := api.FindSchema(ctx, &vtadminpb.FindSchemaRequest{
			Table: "testtable",
			TableSizeOptions: &vtadminpb.GetSchemaTableSizeOptions{
				AggregateSizes: true,
			},
		})

		expected := &vtadminpb.Schema{
			Cluster:  c1pb,
			Keyspace: "testkeyspace",
			TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
				{
					Name: "testtable",
				},
			},
			TableSizes: map[string]*vtadminpb.Schema_TableSize{
				"testtable": {
					RowCount:   10 + 20,
					DataLength: 100 + 200,
					ByShard: map[string]*vtadminpb.Schema_ShardTableSize{
						"-80": {
							RowCount:   10,
							DataLength: 100,
						},
						"80-": {
							RowCount:   20,
							DataLength: 200,
						},
					},
				},
			},
		}

		if schema != nil {
			// Clone so our mutation below doesn't trip the race detector.
			schema = schema.CloneVT()
			for _, td := range schema.TableDefinitions {
				// Zero these out because they're non-deterministic and also not
				// relevant to the final result.
				td.RowCount = 0
				td.DataLength = 0
			}
		}

		assert.NoError(t, err)
		assert.Truef(t, proto.Equal(expected, schema), "expected %v, got %v", expected, schema)
	})
}

func TestGetClusters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		clusters []*cluster.Cluster
		expected []*vtadminpb.Cluster
	}{
		{
			name: "multiple clusters",
			clusters: []*cluster.Cluster{
				{
					ID:        "c1",
					Name:      "cluster1",
					Discovery: fakediscovery.New(),
				},
				{
					ID:        "c2",
					Name:      "cluster2",
					Discovery: fakediscovery.New(),
				},
			},
			expected: []*vtadminpb.Cluster{
				{
					Id:   "c1",
					Name: "cluster1",
				},
				{
					Id:   "c2",
					Name: "cluster2",
				},
			},
		},
		{
			name:     "no clusters",
			clusters: []*cluster.Cluster{},
			expected: []*vtadminpb.Cluster{},
		},
	}

	ctx := context.Background()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			api := NewAPI(vtenv.NewTestEnv(), tt.clusters, Options{})

			resp, err := api.GetClusters(ctx, &vtadminpb.GetClustersRequest{})
			assert.NoError(t, err)
			assert.ElementsMatch(t, tt.expected, resp.Clusters)
		})
	}
}

func TestGetGates(t *testing.T) {
	t.Parallel()

	fakedisco1 := fakediscovery.New()
	cluster1 := &cluster.Cluster{
		ID:        "c1",
		Name:      "cluster1",
		Discovery: fakedisco1,
	}
	cluster1Gates := []*vtadminpb.VTGate{
		{
			Hostname: "cluster1-gate1",
		},
		{
			Hostname: "cluster1-gate2",
		},
		{
			Hostname: "cluster1-gate3",
		},
	}
	fakedisco1.AddTaggedGates(nil, cluster1Gates...)

	expectedCluster1Gates := []*vtadminpb.VTGate{
		{
			Cluster: &vtadminpb.Cluster{
				Id:   cluster1.ID,
				Name: cluster1.Name,
			},
			Hostname: "cluster1-gate1",
		},
		{
			Cluster: &vtadminpb.Cluster{
				Id:   cluster1.ID,
				Name: cluster1.Name,
			},
			Hostname: "cluster1-gate2",
		},
		{
			Cluster: &vtadminpb.Cluster{
				Id:   cluster1.ID,
				Name: cluster1.Name,
			},
			Hostname: "cluster1-gate3",
		},
	}

	fakedisco2 := fakediscovery.New()
	cluster2 := &cluster.Cluster{
		ID:        "c2",
		Name:      "cluster2",
		Discovery: fakedisco2,
	}
	cluster2Gates := []*vtadminpb.VTGate{
		{
			Hostname: "cluster2-gate1",
		},
	}
	fakedisco2.AddTaggedGates(nil, cluster2Gates...)

	expectedCluster2Gates := []*vtadminpb.VTGate{
		{
			Cluster: &vtadminpb.Cluster{
				Id:   cluster2.ID,
				Name: cluster2.Name,
			},
			Hostname: "cluster2-gate1",
		},
	}

	api := NewAPI(vtenv.NewTestEnv(), []*cluster.Cluster{cluster1, cluster2}, Options{})
	ctx := context.Background()

	resp, err := api.GetGates(ctx, &vtadminpb.GetGatesRequest{})
	assert.NoError(t, err)
	assert.ElementsMatch(t, append(expectedCluster1Gates, expectedCluster2Gates...), resp.Gates)

	resp, err = api.GetGates(ctx, &vtadminpb.GetGatesRequest{ClusterIds: []string{cluster1.ID}})
	assert.NoError(t, err)
	assert.ElementsMatch(t, expectedCluster1Gates, resp.Gates)

	fakedisco1.SetGatesError(true)

	resp, err = api.GetGates(ctx, &vtadminpb.GetGatesRequest{})
	assert.Error(t, err)
	assert.Nil(t, resp)
}

func TestGetKeyspace(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		clusterShards [][]*vtctldatapb.Shard
		req           *vtadminpb.GetKeyspaceRequest
		expected      *vtadminpb.Keyspace
		shouldErr     bool
	}{
		{
			clusterShards: [][]*vtctldatapb.Shard{
				// cluster-0
				{
					{
						Keyspace: "ks",
						Name:     "-80",
						Shard:    &topodatapb.Shard{},
					},
					{
						Keyspace: "ks",
						Name:     "80-",
						Shard:    &topodatapb.Shard{},
					},
				},
				// cluster-1
				{
					{
						Keyspace: "ks",
						Name:     "-",
						Shard:    &topodatapb.Shard{},
					},
				},
			},
			req: &vtadminpb.GetKeyspaceRequest{
				ClusterId: "cluster-1",
				Keyspace:  "ks",
			},
			expected: &vtadminpb.Keyspace{
				Cluster: &vtadminpb.Cluster{
					Id:   "cluster-1",
					Name: "cluster-1",
				},
				Keyspace: &vtctldatapb.Keyspace{
					Name:     "ks",
					Keyspace: &topodatapb.Keyspace{},
				},
				Shards: map[string]*vtctldatapb.Shard{
					"-": {
						Keyspace: "ks",
						Name:     "-",
						Shard:    &topodatapb.Shard{},
					},
				},
			},
		},
		{
			name: "nonexistent cluster",
			clusterShards: [][]*vtctldatapb.Shard{
				// cluster-0
				{
					{
						Keyspace: "ks",
						Name:     "-80",
						Shard:    &topodatapb.Shard{},
					},
					{
						Keyspace: "ks",
						Name:     "80-",
						Shard:    &topodatapb.Shard{},
					},
				},
				// cluster-1
				{
					{
						Keyspace: "ks",
						Name:     "-",
						Shard:    &topodatapb.Shard{},
					},
				},
			},
			req: &vtadminpb.GetKeyspaceRequest{
				ClusterId: "cluster-2",
			},
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			topos := make([]*topo.Server, len(tt.clusterShards))
			vtctlds := make([]vtctlservicepb.VtctldServer, len(tt.clusterShards))

			for i, shards := range tt.clusterShards {
				ts := memorytopo.NewServer(ctx, "cell1")
				testutil.AddShards(ctx, t, ts, shards...)
				topos[i] = ts
				vtctlds[i] = testutil.NewVtctldServerWithTabletManagerClient(t, ts, nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
					return grpcvtctldserver.NewVtctldServer(vtenv.NewTestEnv(), ts)
				})
			}

			testutil.WithTestServers(t, func(t *testing.T, clients ...vtctldclient.VtctldClient) {
				clusters := make([]*cluster.Cluster, len(clients))
				for i, client := range clients {
					clusters[i] = vtadmintestutil.BuildCluster(t, vtadmintestutil.TestClusterConfig{
						Cluster: &vtadminpb.Cluster{
							Id:   fmt.Sprintf("cluster-%d", i),
							Name: fmt.Sprintf("cluster-%d", i),
						},
						VtctldClient: client,
					})
				}

				api := NewAPI(vtenv.NewTestEnv(), clusters, Options{})
				ks, err := api.GetKeyspace(ctx, tt.req)
				if tt.shouldErr {
					assert.Error(t, err)
					return
				}

				assert.NoError(t, err)
				assert.Truef(t, proto.Equal(tt.expected, ks), "expected %v, got %v", tt.expected, ks)
			}, vtctlds...)
		})
	}
}

func TestGetKeyspaces(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		clusterKeyspaces [][]*vtctldatapb.Keyspace
		clusterShards    [][]*vtctldatapb.Shard
		req              *vtadminpb.GetKeyspacesRequest
		expected         *vtadminpb.GetKeyspacesResponse
	}{
		{
			name: "multiple clusters, multiple shards",
			clusterKeyspaces: [][]*vtctldatapb.Keyspace{
				// cluster0
				{
					{
						Name:     "c0-ks0",
						Keyspace: &topodatapb.Keyspace{},
					},
				},
				// cluster1
				{
					{
						Name:     "c1-ks0",
						Keyspace: &topodatapb.Keyspace{},
					},
				},
			},
			clusterShards: [][]*vtctldatapb.Shard{
				// cluster0
				{
					{
						Keyspace: "c0-ks0",
						Name:     "-80",
					},
					{
						Keyspace: "c0-ks0",
						Name:     "80-",
					},
				},
				// cluster1
				{
					{
						Keyspace: "c1-ks0",
						Name:     "-",
					},
				},
			},
			req: &vtadminpb.GetKeyspacesRequest{},
			expected: &vtadminpb.GetKeyspacesResponse{
				Keyspaces: []*vtadminpb.Keyspace{
					{
						Cluster: &vtadminpb.Cluster{
							Id:   "c0",
							Name: "cluster0",
						},
						Keyspace: &vtctldatapb.Keyspace{
							Name:     "c0-ks0",
							Keyspace: &topodatapb.Keyspace{},
						},
						Shards: map[string]*vtctldatapb.Shard{
							"-80": {
								Keyspace: "c0-ks0",
								Name:     "-80",
								Shard: &topodatapb.Shard{
									IsPrimaryServing: true,
								},
							},
							"80-": {
								Keyspace: "c0-ks0",
								Name:     "80-",
								Shard: &topodatapb.Shard{
									IsPrimaryServing: true,
								},
							},
						},
					},
					{
						Cluster: &vtadminpb.Cluster{
							Id:   "c1",
							Name: "cluster1",
						},
						Keyspace: &vtctldatapb.Keyspace{
							Name:     "c1-ks0",
							Keyspace: &topodatapb.Keyspace{},
						},
						Shards: map[string]*vtctldatapb.Shard{
							"-": {
								Keyspace: "c1-ks0",
								Name:     "-",
								Shard: &topodatapb.Shard{
									IsPrimaryServing: true,
								},
							},
						},
					},
				},
			},
		},
		{
			name: "with snapshot",
			clusterKeyspaces: [][]*vtctldatapb.Keyspace{
				// cluster0
				{
					{
						Name:     "testkeyspace",
						Keyspace: &topodatapb.Keyspace{},
					},
					{
						Name: "snapshot",
						Keyspace: &topodatapb.Keyspace{
							KeyspaceType: topodatapb.KeyspaceType_SNAPSHOT,
							BaseKeyspace: "testkeyspace",
							SnapshotTime: &vttime.Time{Seconds: 10, Nanoseconds: 1},
						},
					},
				},
			},
			req: &vtadminpb.GetKeyspacesRequest{},
			expected: &vtadminpb.GetKeyspacesResponse{
				Keyspaces: []*vtadminpb.Keyspace{
					{
						Cluster: &vtadminpb.Cluster{
							Id:   "c0",
							Name: "cluster0",
						},
						Keyspace: &vtctldatapb.Keyspace{
							Name:     "testkeyspace",
							Keyspace: &topodatapb.Keyspace{},
						},
					},
					{
						Cluster: &vtadminpb.Cluster{
							Id:   "c0",
							Name: "cluster0",
						},
						Keyspace: &vtctldatapb.Keyspace{
							Name: "snapshot",
							Keyspace: &topodatapb.Keyspace{
								KeyspaceType: topodatapb.KeyspaceType_SNAPSHOT,
								BaseKeyspace: "testkeyspace",
								SnapshotTime: &vttime.Time{Seconds: 10, Nanoseconds: 1},
							},
						},
					},
				},
			},
		},
		{
			name: "filtered by cluster ID",
			clusterKeyspaces: [][]*vtctldatapb.Keyspace{
				// cluster0
				{
					{
						Name:     "c0-ks0",
						Keyspace: &topodatapb.Keyspace{},
					},
				},
				// cluster1
				{
					{
						Name:     "c1-ks0",
						Keyspace: &topodatapb.Keyspace{},
					},
				},
			},
			req: &vtadminpb.GetKeyspacesRequest{
				ClusterIds: []string{"c1"},
			},
			expected: &vtadminpb.GetKeyspacesResponse{
				Keyspaces: []*vtadminpb.Keyspace{
					{
						Cluster: &vtadminpb.Cluster{
							Id:   "c1",
							Name: "cluster1",
						},
						Keyspace: &vtctldatapb.Keyspace{
							Name:     "c1-ks0",
							Keyspace: &topodatapb.Keyspace{},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			// Note that these test cases were written prior to the existence of
			// WithTestServers, so they are all written with the assumption that
			// there are exactly 2 clusters.
			topos := []*topo.Server{
				memorytopo.NewServer(ctx, "c0_cell1"),
				memorytopo.NewServer(ctx, "c1_cell1"),
			}

			for cdx, cks := range tt.clusterKeyspaces {
				for _, ks := range cks {
					testutil.AddKeyspace(ctx, t, topos[cdx], ks)
				}
			}

			for cdx, css := range tt.clusterShards {
				testutil.AddShards(ctx, t, topos[cdx], css...)
			}

			servers := []vtctlservicepb.VtctldServer{
				testutil.NewVtctldServerWithTabletManagerClient(t, topos[0], nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
					return grpcvtctldserver.NewVtctldServer(vtenv.NewTestEnv(), ts)
				}),
				testutil.NewVtctldServerWithTabletManagerClient(t, topos[1], nil, func(ts *topo.Server) vtctlservicepb.VtctldServer {
					return grpcvtctldserver.NewVtctldServer(vtenv.NewTestEnv(), ts)
				}),
			}

			testutil.WithTestServers(t, func(t *testing.T, clients ...vtctldclient.VtctldClient) {
				clusters := []*cluster.Cluster{
					vtadmintestutil.BuildCluster(t, vtadmintestutil.TestClusterConfig{
						Cluster: &vtadminpb.Cluster{
							Id:   "c0",
							Name: "cluster0",
						},
						VtctldClient: clients[0],
					}),
					vtadmintestutil.BuildCluster(t, vtadmintestutil.TestClusterConfig{
						Cluster: &vtadminpb.Cluster{
							Id:   "c1",
							Name: "cluster1",
						},
						VtctldClient: clients[1],
					}),
				}

				api := NewAPI(vtenv.NewTestEnv(), clusters, Options{})
				resp, err := api.GetKeyspaces(ctx, tt.req)
				require.NoError(t, err)

				vtadmintestutil.AssertKeyspaceSlicesEqual(t, tt.expected.Keyspaces, resp.Keyspaces)
			}, servers...)
		})
	}
}

func TestGetSchema(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tests := []struct {
		name      string
		clusterID int
		ts        *topo.Server
		tmc       tmclient.TabletManagerClient
		tablets   []*vtadminpb.Tablet
		req       *vtadminpb.GetSchemaRequest
		expected  *vtadminpb.Schema
		shouldErr bool
	}{
		{
			name:      "success",
			clusterID: 1,
			ts:        memorytopo.NewServer(ctx, "zone1"),
			tmc: &testutil.TabletManagerClient{
				GetSchemaResults: map[string]struct {
					Schema *tabletmanagerdatapb.SchemaDefinition
					Error  error
				}{
					"zone1-0000000100": {
						Schema: &tabletmanagerdatapb.SchemaDefinition{
							TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
								{
									Name: "testtable",
								},
							},
						},
					},
				},
			},
			tablets: []*vtadminpb.Tablet{
				{
					Cluster: &vtadminpb.Cluster{
						Id:   "c1",
						Name: "cluster1",
					},
					State: vtadminpb.Tablet_SERVING,
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  100,
						},
						Keyspace: "testkeyspace",
					},
				},
			},
			req: &vtadminpb.GetSchemaRequest{
				ClusterId: "c1",
				Keyspace:  "testkeyspace",
				Table:     "testtable",
			},
			expected: &vtadminpb.Schema{
				Cluster: &vtadminpb.Cluster{
					Id:   "c1",
					Name: "cluster1",
				},
				Keyspace: "testkeyspace",
				TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
					{
						Name: "testtable",
					},
				},
			},
			shouldErr: false,
		},
		{
			name:      "cluster not found",
			clusterID: 1, // results in clusterId == "c1"
			ts:        memorytopo.NewServer(ctx, "zone1"),
			tablets:   nil,
			req: &vtadminpb.GetSchemaRequest{
				ClusterId: "c2",
				Keyspace:  "testkeyspace",
				Table:     "testtable",
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name:      "tablet not found for keyspace",
			clusterID: 1,
			ts:        memorytopo.NewServer(ctx, "zone1"),
			tablets: []*vtadminpb.Tablet{
				{
					Cluster: &vtadminpb.Cluster{
						Id:   "c1",
						Name: "cluster1",
					},
					State: vtadminpb.Tablet_SERVING,
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  100,
						},
						Keyspace: "otherkeyspace",
					},
				},
			},
			req: &vtadminpb.GetSchemaRequest{
				ClusterId: "c1",
				Keyspace:  "testkeyspace",
				Table:     "testtable",
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name:      "no serving tablet found for keyspace",
			clusterID: 1,
			ts:        memorytopo.NewServer(ctx, "zone1"),
			tablets: []*vtadminpb.Tablet{
				{
					Cluster: &vtadminpb.Cluster{
						Id:   "c1",
						Name: "cluster1",
					},
					State: vtadminpb.Tablet_NOT_SERVING,
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  100,
						},
						Keyspace: "testkeyspace",
					},
				},
			},
			req: &vtadminpb.GetSchemaRequest{
				ClusterId: "c1",
				Keyspace:  "testkeyspace",
				Table:     "testtable",
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name:      "error in GetSchema call",
			clusterID: 1,
			ts:        memorytopo.NewServer(ctx, "zone1"),
			tmc: &testutil.TabletManagerClient{
				GetSchemaResults: map[string]struct {
					Schema *tabletmanagerdatapb.SchemaDefinition
					Error  error
				}{
					"zone1-0000000100": {
						Schema: &tabletmanagerdatapb.SchemaDefinition{
							TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
								{
									Name: "testtable",
								},
								{
									Name: "table2",
								},
								{
									Name: "table3",
								},
							},
						},
						Error: assert.AnError,
					},
				},
			},
			tablets: []*vtadminpb.Tablet{
				{
					Cluster: &vtadminpb.Cluster{
						Id:   "c1",
						Name: "cluster1",
					},
					State: vtadminpb.Tablet_SERVING,
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  100,
						},
						Keyspace: "testkeyspace",
					},
				},
			},
			req: &vtadminpb.GetSchemaRequest{
				ClusterId: "c1",
				Keyspace:  "testkeyspace",
				Table:     "testtable",
			},
			expected:  nil,
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			vtctld := testutil.NewVtctldServerWithTabletManagerClient(t, tt.ts, tt.tmc, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return grpcvtctldserver.NewVtctldServer(vtenv.NewTestEnv(), ts)
			})

			testutil.AddTablets(ctx, t, tt.ts, nil, vtadmintestutil.TopodataTabletsFromVTAdminTablets(tt.tablets)...)

			testutil.WithTestServer(t, vtctld, func(t *testing.T, client vtctldclient.VtctldClient) {
				c := vtadmintestutil.BuildCluster(t, vtadmintestutil.TestClusterConfig{
					Cluster: &vtadminpb.Cluster{
						Id:   fmt.Sprintf("c%d", tt.clusterID),
						Name: fmt.Sprintf("cluster%d", tt.clusterID),
					},
					VtctldClient: client,
					Tablets:      tt.tablets,
				})
				api := NewAPI(vtenv.NewTestEnv(), []*cluster.Cluster{c}, Options{})
				defer api.Close()

				resp, err := api.GetSchema(ctx, tt.req)
				if tt.shouldErr {
					assert.Error(t, err)

					return
				}

				if resp != nil {
					// Clone so our mutation below doesn't trip the race detector.
					resp = resp.CloneVT()
				}

				assert.NoError(t, err)
				assert.Truef(t, proto.Equal(tt.expected, resp), "expected %v, got %v", tt.expected, resp)
			})
		})
	}

	t.Run("size aggregation", func(t *testing.T) {
		c1pb := &vtadminpb.Cluster{
			Id:   "c1",
			Name: "cluster1",
		}
		c1 := vtadmintestutil.BuildCluster(t, vtadmintestutil.TestClusterConfig{
			Cluster: c1pb,
			VtctldClient: &fakevtctldclient.VtctldClient{
				FindAllShardsInKeyspaceResults: map[string]struct {
					Response *vtctldatapb.FindAllShardsInKeyspaceResponse
					Error    error
				}{
					"testkeyspace": {
						Response: &vtctldatapb.FindAllShardsInKeyspaceResponse{
							Shards: map[string]*vtctldatapb.Shard{
								"-80": {
									Keyspace: "testkeyspace",
									Name:     "-80",
									Shard: &topodatapb.Shard{
										IsPrimaryServing: true,
										PrimaryAlias: &topodatapb.TabletAlias{
											Cell: "c1zone1",
											Uid:  100,
										},
									},
								},
								"80-": {
									Keyspace: "testkeyspace",
									Name:     "80-",
									Shard: &topodatapb.Shard{
										IsPrimaryServing: true,
										PrimaryAlias: &topodatapb.TabletAlias{
											Cell: "c1zone1",
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
					"c1zone1-0000000100": {
						Response: &vtctldatapb.GetSchemaResponse{
							Schema: &tabletmanagerdatapb.SchemaDefinition{
								TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
									{
										Name:       "testtable",
										RowCount:   10,
										DataLength: 100,
									},
								},
							},
						},
					},
					"c1zone1-0000000200": {
						Response: &vtctldatapb.GetSchemaResponse{
							Schema: &tabletmanagerdatapb.SchemaDefinition{
								TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
									{
										Name:       "testtable",
										RowCount:   20,
										DataLength: 200,
									},
								},
							},
						},
					},
				},
			},
			Tablets: []*vtadminpb.Tablet{
				{
					Cluster: c1pb,
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "c1zone1",
							Uid:  100,
						},
						Keyspace: "testkeyspace",
						Shard:    "-80",
					},
					State: vtadminpb.Tablet_SERVING,
				},
				{
					Cluster: c1pb,
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "c1zone1",
							Uid:  200,
						},
						Keyspace: "testkeyspace",
						Shard:    "80-",
					},
					State: vtadminpb.Tablet_SERVING,
				},
			},
		},
		)
		c2 := vtadmintestutil.BuildCluster(t, vtadmintestutil.TestClusterConfig{
			Cluster: &vtadminpb.Cluster{
				Id:   "c2",
				Name: "cluster2",
			},
		},
		)

		api := NewAPI(vtenv.NewTestEnv(), []*cluster.Cluster{c1, c2}, Options{})
		defer api.Close()

		schema, err := api.GetSchema(ctx, &vtadminpb.GetSchemaRequest{
			ClusterId: c1.ID,
			Keyspace:  "testkeyspace",
			Table:     "testtable",
			TableSizeOptions: &vtadminpb.GetSchemaTableSizeOptions{
				AggregateSizes: true,
			},
		})

		expected := &vtadminpb.Schema{
			Cluster:  c1pb,
			Keyspace: "testkeyspace",
			TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
				{
					Name: "testtable",
				},
			},
			TableSizes: map[string]*vtadminpb.Schema_TableSize{
				"testtable": {
					RowCount:   10 + 20,
					DataLength: 100 + 200,
					ByShard: map[string]*vtadminpb.Schema_ShardTableSize{
						"-80": {
							RowCount:   10,
							DataLength: 100,
						},
						"80-": {
							RowCount:   20,
							DataLength: 200,
						},
					},
				},
			},
		}

		if schema != nil {
			// Clone so our mutation below doesn't trip the race detector.
			schema = schema.CloneVT()
			for _, td := range schema.TableDefinitions {
				// Zero these out because they're non-deterministic and also not
				// relevant to the final result.
				td.RowCount = 0
				td.DataLength = 0
			}
		}

		assert.NoError(t, err)
		assert.Truef(t, proto.Equal(expected, schema), "expected %v, got %v", expected, schema)
	})
}

func TestGetSchemas(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		clusterTablets [][]*vtadminpb.Tablet
		// Indexed by tablet alias
		tabletSchemas map[string]*tabletmanagerdatapb.SchemaDefinition
		req           *vtadminpb.GetSchemasRequest
		expected      *vtadminpb.GetSchemasResponse
	}{
		{
			name: "one schema in one cluster",
			clusterTablets: [][]*vtadminpb.Tablet{
				// cluster0
				{
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
				},
				// cluster1
				{
					{
						State: vtadminpb.Tablet_SERVING,
						Tablet: &topodatapb.Tablet{
							Alias: &topodatapb.TabletAlias{
								Cell: "c1_cell1",
								Uid:  100,
							},
							Keyspace: "commerce",
						},
					},
				},
			},
			tabletSchemas: map[string]*tabletmanagerdatapb.SchemaDefinition{
				"c0_cell1-0000000100": {
					DatabaseSchema: "CREATE DATABASE vt_testkeyspace",
					TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
						{
							Name:       "t1",
							Schema:     `CREATE TABLE t1 (id int(11) not null,PRIMARY KEY (id));`,
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
			req: &vtadminpb.GetSchemasRequest{},
			expected: &vtadminpb.GetSchemasResponse{
				Schemas: []*vtadminpb.Schema{
					{
						Cluster: &vtadminpb.Cluster{
							Id:   "c0",
							Name: "cluster0",
						},
						Keyspace: "commerce",
						TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
							{
								Name:       "t1",
								Schema:     `CREATE TABLE t1 (id int(11) not null,PRIMARY KEY (id));`,
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
						TableSizes: map[string]*vtadminpb.Schema_TableSize{},
					},
				},
			},
		},
		{
			name: "one schema in each cluster",
			clusterTablets: [][]*vtadminpb.Tablet{
				// cluster0
				{
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
				},
				// cluster1
				{
					{
						State: vtadminpb.Tablet_SERVING,
						Tablet: &topodatapb.Tablet{
							Alias: &topodatapb.TabletAlias{
								Cell: "c1_cell1",
								Uid:  100,
							},
							Keyspace: "commerce",
						},
					},
				},
			},
			tabletSchemas: map[string]*tabletmanagerdatapb.SchemaDefinition{
				"c0_cell1-0000000100": {
					DatabaseSchema: "CREATE DATABASE vt_testkeyspace",
					TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
						{
							Name:       "t1",
							Schema:     `CREATE TABLE t1 (id int(11) not null,PRIMARY KEY (id));`,
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
				"c1_cell1-0000000100": {
					DatabaseSchema: "CREATE DATABASE vt_testkeyspace",
					TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
						{
							Name:       "t2",
							Schema:     `CREATE TABLE t2 (id int(11) not null,PRIMARY KEY (id));`,
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
			req: &vtadminpb.GetSchemasRequest{},
			expected: &vtadminpb.GetSchemasResponse{
				Schemas: []*vtadminpb.Schema{
					{
						Cluster: &vtadminpb.Cluster{
							Id:   "c0",
							Name: "cluster0",
						},
						Keyspace: "commerce",
						TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
							{
								Name:       "t1",
								Schema:     `CREATE TABLE t1 (id int(11) not null,PRIMARY KEY (id));`,
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
						TableSizes: map[string]*vtadminpb.Schema_TableSize{},
					},
					{
						Cluster: &vtadminpb.Cluster{
							Id:   "c1",
							Name: "cluster1",
						},
						Keyspace: "commerce",
						TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
							{
								Name:       "t2",
								Schema:     `CREATE TABLE t2 (id int(11) not null,PRIMARY KEY (id));`,
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
						TableSizes: map[string]*vtadminpb.Schema_TableSize{},
					},
				},
			},
		},
		{
			name: "filtered by cluster ID",
			clusterTablets: [][]*vtadminpb.Tablet{
				// cluster0
				{
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
				},
				// cluster1
				{
					{
						State: vtadminpb.Tablet_SERVING,
						Tablet: &topodatapb.Tablet{
							Alias: &topodatapb.TabletAlias{
								Cell: "c1_cell1",
								Uid:  100,
							},
							Keyspace: "commerce",
						},
					},
				},
			},
			tabletSchemas: map[string]*tabletmanagerdatapb.SchemaDefinition{
				"c0_cell1-0000000100": {
					DatabaseSchema: "CREATE DATABASE vt_testkeyspace",
					TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
						{
							Name:       "t1",
							Schema:     `CREATE TABLE t1 (id int(11) not null,PRIMARY KEY (id));`,
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
				"c1_cell1-0000000100": {
					DatabaseSchema: "CREATE DATABASE vt_testkeyspace",
					TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
						{
							Name:       "t2",
							Schema:     `CREATE TABLE t2 (id int(11) not null,PRIMARY KEY (id));`,
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
			req: &vtadminpb.GetSchemasRequest{
				ClusterIds: []string{"c1"},
			},
			expected: &vtadminpb.GetSchemasResponse{
				Schemas: []*vtadminpb.Schema{
					{
						Cluster: &vtadminpb.Cluster{
							Id:   "c1",
							Name: "cluster1",
						},
						Keyspace: "commerce",
						TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
							{
								Name:       "t2",
								Schema:     `CREATE TABLE t2 (id int(11) not null,PRIMARY KEY (id));`,
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
						TableSizes: map[string]*vtadminpb.Schema_TableSize{},
					},
				},
			},
		},
		{
			name: "filtered by cluster ID that doesn't exist",
			clusterTablets: [][]*vtadminpb.Tablet{
				// cluster0
				{
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
				},
			},
			tabletSchemas: map[string]*tabletmanagerdatapb.SchemaDefinition{
				"c0_cell1-0000000100": {
					DatabaseSchema: "CREATE DATABASE vt_testkeyspace",
					TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
						{
							Name:       "t1",
							Schema:     `CREATE TABLE t1 (id int(11) not null,PRIMARY KEY (id));`,
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
			req: &vtadminpb.GetSchemasRequest{
				ClusterIds: []string{"nope"},
			},
			expected: &vtadminpb.GetSchemasResponse{
				Schemas: nil,
			},
		},
		{
			name: "no schemas for any cluster",
			clusterTablets: [][]*vtadminpb.Tablet{
				// cluster0
				{
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
				},
			},
			tabletSchemas: map[string]*tabletmanagerdatapb.SchemaDefinition{},
			req:           &vtadminpb.GetSchemasRequest{},
			expected: &vtadminpb.GetSchemasResponse{
				Schemas: nil,
			},
		},
		{
			name: "no serving tablets",
			clusterTablets: [][]*vtadminpb.Tablet{
				// cluster0
				{
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
				},
			},
			tabletSchemas: map[string]*tabletmanagerdatapb.SchemaDefinition{
				"c0_cell1-0000000100": {
					DatabaseSchema: "CREATE DATABASE vt_testkeyspace",
					TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
						{
							Name:       "t1",
							Schema:     `CREATE TABLE t1 (id int(11) not null,PRIMARY KEY (id));`,
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
			req: &vtadminpb.GetSchemasRequest{},
			expected: &vtadminpb.GetSchemasResponse{
				Schemas: nil,
			},
		},
	}

	for _, tt := range tests {
		// Note that these test cases were written prior to the existence of
		// WithTestServers, so they are all written with the assumption that
		// there are exactly 2 clusters.
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			topos := []*topo.Server{
				memorytopo.NewServer(ctx, "c0_cell1"),
				memorytopo.NewServer(ctx, "c1_cell1"),
			}

			tmc := testutil.TabletManagerClient{
				GetSchemaResults: map[string]struct {
					Schema *tabletmanagerdatapb.SchemaDefinition
					Error  error
				}{},
			}

			vtctlds := []vtctlservicepb.VtctldServer{
				testutil.NewVtctldServerWithTabletManagerClient(t, topos[0], &tmc, func(ts *topo.Server) vtctlservicepb.VtctldServer {
					return grpcvtctldserver.NewVtctldServer(vtenv.NewTestEnv(), ts)
				}),
				testutil.NewVtctldServerWithTabletManagerClient(t, topos[1], &tmc, func(ts *topo.Server) vtctlservicepb.VtctldServer {
					return grpcvtctldserver.NewVtctldServer(vtenv.NewTestEnv(), ts)
				}),
			}

			testutil.WithTestServers(t, func(t *testing.T, clients ...vtctldclient.VtctldClient) {
				clusters := make([]*cluster.Cluster, len(topos))
				for cdx, toposerver := range topos {
					// Handle when a test doesn't define any tablets for a given cluster.
					var cts []*vtadminpb.Tablet
					if cdx < len(tt.clusterTablets) {
						cts = tt.clusterTablets[cdx]
					}

					for _, tablet := range cts {
						// AddTablet also adds the keyspace + shard for us.
						testutil.AddTablet(ctx, t, toposerver, tablet.Tablet, nil)

						// Adds each SchemaDefinition to the fake TabletManagerClient, or nil
						// if there are no schemas for that tablet. (All tablet aliases must
						// exist in the map. Otherwise, TabletManagerClient will return an error when
						// looking up the schema with tablet alias that doesn't exist.)
						alias := topoproto.TabletAliasString(tablet.Tablet.Alias)
						tmc.GetSchemaResults[alias] = struct {
							Schema *tabletmanagerdatapb.SchemaDefinition
							Error  error
						}{
							Schema: tt.tabletSchemas[alias],
							Error:  nil,
						}
					}

					clusters[cdx] = vtadmintestutil.BuildCluster(t, vtadmintestutil.TestClusterConfig{
						Cluster: &vtadminpb.Cluster{
							Id:   fmt.Sprintf("c%d", cdx),
							Name: fmt.Sprintf("cluster%d", cdx),
						},
						VtctldClient: clients[cdx],
						Tablets:      cts,
					})
				}

				api := NewAPI(vtenv.NewTestEnv(), clusters, Options{})
				defer api.Close()

				resp, err := api.GetSchemas(ctx, tt.req)
				require.NoError(t, err)

				vtadmintestutil.AssertSchemaSlicesEqual(t, tt.expected.Schemas, resp.Schemas)
			}, vtctlds...)
		})
	}

	t.Run("size aggregation", func(t *testing.T) {
		t.Parallel()

		c1pb := &vtadminpb.Cluster{
			Id:   "c1",
			Name: "cluster1",
		}
		c2pb := &vtadminpb.Cluster{
			Id:   "c2",
			Name: "cluster2",
		}

		c1 := vtadmintestutil.BuildCluster(t, vtadmintestutil.TestClusterConfig{
			Cluster: c1pb,
			VtctldClient: &fakevtctldclient.VtctldClient{
				FindAllShardsInKeyspaceResults: map[string]struct {
					Response *vtctldatapb.FindAllShardsInKeyspaceResponse
					Error    error
				}{
					"testkeyspace": {
						Response: &vtctldatapb.FindAllShardsInKeyspaceResponse{
							Shards: map[string]*vtctldatapb.Shard{
								"-80": {
									Keyspace: "testkeyspace",
									Name:     "-80",
									Shard: &topodatapb.Shard{
										IsPrimaryServing: true,
										PrimaryAlias: &topodatapb.TabletAlias{
											Cell: "c1zone1",
											Uid:  100,
										},
									},
								},
								"80-": {
									Keyspace: "testkeyspace",
									Name:     "80-",
									Shard: &topodatapb.Shard{
										IsPrimaryServing: true,
										PrimaryAlias: &topodatapb.TabletAlias{
											Cell: "c1zone1",
											Uid:  200,
										},
									},
								},
							},
						},
					},
					"ks1": {
						Response: &vtctldatapb.FindAllShardsInKeyspaceResponse{
							Shards: map[string]*vtctldatapb.Shard{
								"-": {
									Keyspace: "ks1",
									Name:     "-",
									Shard: &topodatapb.Shard{
										IsPrimaryServing: true,
										PrimaryAlias: &topodatapb.TabletAlias{
											Cell: "c1zone2",
											Uid:  100,
										},
									},
								},
							},
						},
					},
				},
				GetKeyspacesResults: &struct {
					Keyspaces []*vtctldatapb.Keyspace
					Error     error
				}{
					Keyspaces: []*vtctldatapb.Keyspace{
						{Name: "testkeyspace"},
						{Name: "ks1"},
					},
				},
				GetSchemaResults: map[string]struct {
					Response *vtctldatapb.GetSchemaResponse
					Error    error
				}{
					"c1zone1-0000000100": {
						Response: &vtctldatapb.GetSchemaResponse{
							Schema: &tabletmanagerdatapb.SchemaDefinition{
								TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
									{
										Name:       "testtable",
										RowCount:   10,
										DataLength: 100,
									},
								},
							},
						},
					},
					"c1zone1-0000000200": {
						Response: &vtctldatapb.GetSchemaResponse{
							Schema: &tabletmanagerdatapb.SchemaDefinition{
								TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
									{
										Name:       "testtable",
										RowCount:   20,
										DataLength: 200,
									},
								},
							},
						},
					},
				},
			},
			Tablets: []*vtadminpb.Tablet{
				{
					Cluster: c1pb,
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "c1zone1",
							Uid:  100,
						},
						Keyspace: "testkeyspace",
						Shard:    "-80",
					},
					State: vtadminpb.Tablet_SERVING,
				},
				{
					Cluster: c1pb,
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "c1zone1",
							Uid:  200,
						},
						Keyspace: "testkeyspace",
						Shard:    "80-",
					},
					State: vtadminpb.Tablet_SERVING,
				},
			},
		},
		)
		c2 := vtadmintestutil.BuildCluster(t, vtadmintestutil.TestClusterConfig{
			Cluster: c2pb,
			VtctldClient: &fakevtctldclient.VtctldClient{
				FindAllShardsInKeyspaceResults: map[string]struct {
					Response *vtctldatapb.FindAllShardsInKeyspaceResponse
					Error    error
				}{
					"ks2": {
						Response: &vtctldatapb.FindAllShardsInKeyspaceResponse{
							Shards: map[string]*vtctldatapb.Shard{
								"-": {
									Keyspace: "ks2",
									Name:     "-",
									Shard: &topodatapb.Shard{
										IsPrimaryServing: true,
										PrimaryAlias: &topodatapb.TabletAlias{
											Cell: "c2z1",
											Uid:  100,
										},
									},
								},
							},
						},
					},
				},
				GetKeyspacesResults: &struct {
					Keyspaces []*vtctldatapb.Keyspace
					Error     error
				}{
					Keyspaces: []*vtctldatapb.Keyspace{
						{
							Name: "ks2",
						},
					},
				},
				GetSchemaResults: map[string]struct {
					Response *vtctldatapb.GetSchemaResponse
					Error    error
				}{
					"c2z1-0000000100": {
						Response: &vtctldatapb.GetSchemaResponse{
							Schema: &tabletmanagerdatapb.SchemaDefinition{
								TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
									{
										Name:       "t2",
										DataLength: 5,
										RowCount:   7,
									},
									{
										Name:       "_t2_ghc",
										DataLength: 5,
										RowCount:   7,
									},
								},
							},
						},
					},
				},
			},
			Tablets: []*vtadminpb.Tablet{
				{
					Cluster: c2pb,
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: "c2z1",
							Uid:  100,
						},
						Keyspace: "ks2",
						Shard:    "-",
					},
					State: vtadminpb.Tablet_SERVING,
				},
			},
		},
		)

		api := NewAPI(vtenv.NewTestEnv(), []*cluster.Cluster{c1, c2}, Options{})
		defer api.Close()

		resp, err := api.GetSchemas(context.Background(), &vtadminpb.GetSchemasRequest{
			TableSizeOptions: &vtadminpb.GetSchemaTableSizeOptions{
				AggregateSizes: true,
			},
		})

		expected := &vtadminpb.GetSchemasResponse{
			Schemas: []*vtadminpb.Schema{
				{
					Cluster:  c1pb,
					Keyspace: "testkeyspace",
					TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
						{
							Name: "testtable",
						},
					},
					TableSizes: map[string]*vtadminpb.Schema_TableSize{
						"testtable": {
							RowCount:   10 + 20,
							DataLength: 100 + 200,
							ByShard: map[string]*vtadminpb.Schema_ShardTableSize{
								"-80": {
									RowCount:   10,
									DataLength: 100,
								},
								"80-": {
									RowCount:   20,
									DataLength: 200,
								},
							},
						},
					},
				},
				{
					Cluster:  c2pb,
					Keyspace: "ks2",
					TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
						{Name: "t2"},
						{Name: "_t2_ghc"},
					},
					TableSizes: map[string]*vtadminpb.Schema_TableSize{
						"t2": {
							DataLength: 5,
							RowCount:   7,
							ByShard: map[string]*vtadminpb.Schema_ShardTableSize{
								"-": {
									DataLength: 5,
									RowCount:   7,
								},
							},
						},
						"_t2_ghc": {
							DataLength: 5,
							RowCount:   7,
							ByShard: map[string]*vtadminpb.Schema_ShardTableSize{
								"-": {
									DataLength: 5,
									RowCount:   7,
								},
							},
						},
					},
				},
			},
		}

		if resp != nil {
			// Clone schemas so our mutations below don't trip the race detector.
			schemas := make([]*vtadminpb.Schema, len(resp.Schemas))
			for i, schema := range resp.Schemas {
				schema := schema.CloneVT()
				for _, td := range schema.TableDefinitions {
					// Zero these out because they're non-deterministic and also not
					// relevant to the final result.
					td.RowCount = 0
					td.DataLength = 0
				}

				schemas[i] = schema
			}

			resp.Schemas = schemas
		}

		assert.NoError(t, err)
		assert.Truef(t, proto.Equal(expected, resp), "expected: %v, got: %v", expected, resp)
	})
}

func TestGetSrvKeyspace(t *testing.T) {
	t.Parallel()

	clusterID := "c0"
	clusterName := "cluster0"

	tests := []struct {
		name             string
		cells            []string
		keyspace         string
		cellSrvKeyspaces map[string]*topodatapb.SrvKeyspace
		req              *vtadminpb.GetSrvKeyspaceRequest
		expected         *vtctldatapb.GetSrvKeyspacesResponse
		shouldErr        bool
	}{
		{
			name:     "success",
			cells:    []string{"zone0"},
			keyspace: "testkeyspace",
			cellSrvKeyspaces: map[string]*topodatapb.SrvKeyspace{
				"zone0": {
					Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
						{
							ServedType: topodatapb.TabletType_REPLICA,
							ShardTabletControls: []*topodatapb.ShardTabletControl{
								{
									Name:                 "-",
									QueryServiceDisabled: false,
								},
							},
						},
					},
				},
			},
			req: &vtadminpb.GetSrvKeyspaceRequest{
				ClusterId: clusterID,
				Keyspace:  "testkeyspace",
				Cells:     []string{"zone0"},
			},
			expected: &vtctldatapb.GetSrvKeyspacesResponse{
				SrvKeyspaces: map[string]*topodatapb.SrvKeyspace{
					"zone0": {
						Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
							{
								ServedType: topodatapb.TabletType_REPLICA,
								ShardTabletControls: []*topodatapb.ShardTabletControl{
									{
										Name:                 "-",
										QueryServiceDisabled: false,
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "cluster doesn't exist",
			req: &vtadminpb.GetSrvKeyspaceRequest{
				Cells:     []string{"doesnt-matter"},
				ClusterId: "doesnt-exist",
				Keyspace:  "doesnt-matter",
			},
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			tmc := testutil.TabletManagerClient{}

			toposerver := memorytopo.NewServer(ctx, tt.cells...)

			vtctldserver := testutil.NewVtctldServerWithTabletManagerClient(t, toposerver, &tmc, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return grpcvtctldserver.NewVtctldServer(vtenv.NewTestEnv(), ts)
			})

			testutil.WithTestServer(t, vtctldserver, func(t *testing.T, vtctldClient vtctldclient.VtctldClient) {
				for cell, sks := range tt.cellSrvKeyspaces {
					err := toposerver.UpdateSrvKeyspace(ctx, cell, tt.keyspace, sks)
					require.NoError(t, err)
				}

				clusters := []*cluster.Cluster{
					vtadmintestutil.BuildCluster(t, vtadmintestutil.TestClusterConfig{
						Cluster: &vtadminpb.Cluster{
							Id:   clusterID,
							Name: clusterName,
						},
						VtctldClient: vtctldClient,
					}),
				}

				api := NewAPI(vtenv.NewTestEnv(), clusters, Options{})
				resp, err := api.GetSrvKeyspace(ctx, tt.req)

				if tt.shouldErr {
					assert.Error(t, err)
					return
				}

				require.NoError(t, err)
				assert.Truef(t, proto.Equal(tt.expected, resp), "expected %v, got %v", tt.expected, resp)
			})
		})
	}
}

func TestGetSrvKeyspaces(t *testing.T) {
	t.Parallel()

	clusterID := "c0"
	clusterName := "cluster0"

	tests := []struct {
		name             string
		cells            []string
		keyspaces        []*vtctldatapb.Keyspace
		cellSrvKeyspaces map[string]map[string]*topodatapb.SrvKeyspace
		req              *vtadminpb.GetSrvKeyspacesRequest
		expected         *vtadminpb.GetSrvKeyspacesResponse
		shouldErr        bool
	}{
		{
			name:  "success",
			cells: []string{"zone0"},
			keyspaces: []*vtctldatapb.Keyspace{
				{
					Name:     "keyspace0",
					Keyspace: &topodatapb.Keyspace{},
				},
				{
					Name:     "keyspace1",
					Keyspace: &topodatapb.Keyspace{},
				},
			},
			cellSrvKeyspaces: map[string]map[string]*topodatapb.SrvKeyspace{
				"keyspace0": {
					"zone0": {
						Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
							{
								ServedType: topodatapb.TabletType_REPLICA,
								ShardTabletControls: []*topodatapb.ShardTabletControl{
									{
										Name:                 "-",
										QueryServiceDisabled: false,
									},
								},
							},
						},
					},
				},
				"keyspace1": {
					"zone0": {
						Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
							{
								ServedType: topodatapb.TabletType_REPLICA,
								ShardTabletControls: []*topodatapb.ShardTabletControl{
									{
										Name:                 "-",
										QueryServiceDisabled: false,
									},
								},
							},
						},
					},
				},
			},
			req: &vtadminpb.GetSrvKeyspacesRequest{
				ClusterIds: []string{clusterID},
				Cells:      []string{"zone0"},
			},
			expected: &vtadminpb.GetSrvKeyspacesResponse{
				SrvKeyspaces: map[string]*vtctldatapb.GetSrvKeyspacesResponse{
					"keyspace0": {
						SrvKeyspaces: map[string]*topodatapb.SrvKeyspace{
							"zone0": {
								Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
									{
										ServedType: topodatapb.TabletType_REPLICA,
										ShardTabletControls: []*topodatapb.ShardTabletControl{
											{
												Name:                 "-",
												QueryServiceDisabled: false,
											},
										},
									},
								},
							},
						},
					},
					"keyspace1": {
						SrvKeyspaces: map[string]*topodatapb.SrvKeyspace{
							"zone0": {
								Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
									{
										ServedType: topodatapb.TabletType_REPLICA,
										ShardTabletControls: []*topodatapb.ShardTabletControl{
											{
												Name:                 "-",
												QueryServiceDisabled: false,
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "cluster doesn't exist",
			req: &vtadminpb.GetSrvKeyspacesRequest{
				Cells:      []string{"doesnt-matter"},
				ClusterIds: []string{"doesnt-exist"},
			},
			expected: &vtadminpb.GetSrvKeyspacesResponse{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			tmc := testutil.TabletManagerClient{}

			toposerver := memorytopo.NewServer(ctx, tt.cells...)

			for _, ks := range tt.keyspaces {
				testutil.AddKeyspace(ctx, t, toposerver, ks)
			}

			vtctldserver := testutil.NewVtctldServerWithTabletManagerClient(t, toposerver, &tmc, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return grpcvtctldserver.NewVtctldServer(vtenv.NewTestEnv(), ts)
			})

			testutil.WithTestServer(t, vtctldserver, func(t *testing.T, vtctldClient vtctldclient.VtctldClient) {
				for keyspace, sks := range tt.cellSrvKeyspaces {
					for cell, sk := range sks {
						err := toposerver.UpdateSrvKeyspace(ctx, cell, keyspace, sk)
						require.NoError(t, err)
					}
				}

				clusters := []*cluster.Cluster{
					vtadmintestutil.BuildCluster(t, vtadmintestutil.TestClusterConfig{
						Cluster: &vtadminpb.Cluster{
							Id:   clusterID,
							Name: clusterName,
						},
						VtctldClient: vtctldClient,
					}),
				}

				api := NewAPI(vtenv.NewTestEnv(), clusters, Options{})
				resp, err := api.GetSrvKeyspaces(ctx, tt.req)

				if tt.shouldErr {
					assert.Error(t, err)
					return
				}

				require.NoError(t, err)
				assert.Truef(t, proto.Equal(tt.expected, resp), "expected %v, got %v", tt.expected, resp)
			})
		})
	}
}

func TestGetSrvVSchema(t *testing.T) {
	t.Parallel()

	clusterID := "c0"
	clusterName := "cluster0"

	tests := []struct {
		name  string
		cells []string

		// tt.cellSrvVSchemas maps cell name to the SrvVSchema for that cell.
		// Not all cells in tt.cells necessarily map to a SrvVSchema.
		cellSrvVSchemas map[string]*vschemapb.SrvVSchema
		srvVSchema      *vschemapb.SrvVSchema
		req             *vtadminpb.GetSrvVSchemaRequest
		expected        *vtadminpb.SrvVSchema
		shouldErr       bool
	}{
		{
			name:  "success",
			cells: []string{"zone0"},
			cellSrvVSchemas: map[string]*vschemapb.SrvVSchema{
				"zone0": {
					Keyspaces: map[string]*vschemapb.Keyspace{
						"commerce": {
							Tables: map[string]*vschemapb.Table{
								"customer": {},
							},
						},
						"customer": {
							Tables: map[string]*vschemapb.Table{
								"customer": {},
							},
						},
					},
					RoutingRules: &vschemapb.RoutingRules{
						Rules: []*vschemapb.RoutingRule{
							{
								FromTable: "customer",
								ToTables:  []string{"commerce.customer"},
							},
							{
								FromTable: "customer@rdonly",
								ToTables:  []string{"customer.customer"},
							},
							{
								FromTable: "customer.customer",
								ToTables:  []string{"commerce.customer"},
							},
						},
					},
				},
			},
			req: &vtadminpb.GetSrvVSchemaRequest{
				Cell:      "zone0",
				ClusterId: clusterID,
			},
			expected: &vtadminpb.SrvVSchema{
				Cell: "zone0",
				Cluster: &vtadminpb.Cluster{
					Id:   clusterID,
					Name: clusterName,
				},
				SrvVSchema: &vschemapb.SrvVSchema{
					Keyspaces: map[string]*vschemapb.Keyspace{
						"commerce": {
							Tables: map[string]*vschemapb.Table{
								"customer": {},
							},
						},
						"customer": {
							Tables: map[string]*vschemapb.Table{
								"customer": {},
							},
						},
					},
					RoutingRules: &vschemapb.RoutingRules{
						Rules: []*vschemapb.RoutingRule{
							{
								FromTable: "customer",
								ToTables:  []string{"commerce.customer"},
							},
							{
								FromTable: "customer@rdonly",
								ToTables:  []string{"customer.customer"},
							},
							{
								FromTable: "customer.customer",
								ToTables:  []string{"commerce.customer"},
							},
						},
					},
				},
			},
		},
		{
			name:       "cluster doesn't exist",
			srvVSchema: nil,
			req: &vtadminpb.GetSrvVSchemaRequest{
				Cell:      "doesnt-matter",
				ClusterId: "doesnt-exist",
			},
			shouldErr: true,
		},
		{

			name:       "cell doesn't exist",
			srvVSchema: nil,
			req: &vtadminpb.GetSrvVSchemaRequest{
				Cell:      "doesnt-exist",
				ClusterId: clusterID,
			},
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			tmc := testutil.TabletManagerClient{}

			toposerver := memorytopo.NewServer(ctx, tt.cells...)

			vtctldserver := testutil.NewVtctldServerWithTabletManagerClient(t, toposerver, &tmc, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return grpcvtctldserver.NewVtctldServer(vtenv.NewTestEnv(), ts)
			})

			testutil.WithTestServer(t, vtctldserver, func(t *testing.T, vtctldClient vtctldclient.VtctldClient) {
				for cell, svs := range tt.cellSrvVSchemas {
					err := toposerver.UpdateSrvVSchema(ctx, cell, svs)
					require.NoError(t, err)
				}

				clusters := []*cluster.Cluster{
					vtadmintestutil.BuildCluster(t, vtadmintestutil.TestClusterConfig{
						Cluster: &vtadminpb.Cluster{
							Id:   clusterID,
							Name: clusterName,
						},
						VtctldClient: vtctldClient,
					}),
				}

				api := NewAPI(vtenv.NewTestEnv(), clusters, Options{})
				resp, err := api.GetSrvVSchema(ctx, tt.req)

				if tt.shouldErr {
					assert.Error(t, err)
					return
				}

				require.NoError(t, err)
				assert.Truef(t, proto.Equal(tt.expected, resp), "expected %v, got %v", tt.expected, resp)
			})
		})
	}
}

func TestGetSrvVSchemas(t *testing.T) {
	t.Parallel()

	clusterID := "c0"
	clusterName := "cluster0"

	tests := []struct {
		name  string
		cells []string
		// tt.cellSrvVSchemas maps cell name to the SrvVSchema for that cell.
		// Not all cells in tt.cells necessarily map to a SrvVSchema.
		cellSrvVSchemas map[string]*vschemapb.SrvVSchema
		req             *vtadminpb.GetSrvVSchemasRequest
		expected        *vtadminpb.GetSrvVSchemasResponse
		shouldErr       bool
	}{
		{
			name:  "returns all cells",
			cells: []string{"zone0", "zone1"},
			cellSrvVSchemas: map[string]*vschemapb.SrvVSchema{
				"zone0": {
					Keyspaces: map[string]*vschemapb.Keyspace{
						"commerce": {
							Tables: map[string]*vschemapb.Table{
								"customer": {},
							},
						},
						"customer": {
							Tables: map[string]*vschemapb.Table{
								"customer": {},
							},
						},
					},
					RoutingRules: &vschemapb.RoutingRules{
						Rules: []*vschemapb.RoutingRule{
							{
								FromTable: "customer",
								ToTables:  []string{"commerce.customer"},
							},
							{
								FromTable: "customer@rdonly",
								ToTables:  []string{"customer.customer"},
							},
							{
								FromTable: "customer.customer",
								ToTables:  []string{"commerce.customer"},
							},
						},
					},
				},
				"zone1": {
					Keyspaces:    map[string]*vschemapb.Keyspace{},
					RoutingRules: &vschemapb.RoutingRules{},
				},
			},
			req: &vtadminpb.GetSrvVSchemasRequest{},
			expected: &vtadminpb.GetSrvVSchemasResponse{
				SrvVSchemas: []*vtadminpb.SrvVSchema{
					{
						Cell: "zone0",
						Cluster: &vtadminpb.Cluster{
							Id:   clusterID,
							Name: clusterName,
						},
						SrvVSchema: &vschemapb.SrvVSchema{
							Keyspaces: map[string]*vschemapb.Keyspace{
								"commerce": {
									Tables: map[string]*vschemapb.Table{
										"customer": {},
									},
								},
								"customer": {
									Tables: map[string]*vschemapb.Table{
										"customer": {},
									},
								},
							},
							RoutingRules: &vschemapb.RoutingRules{
								Rules: []*vschemapb.RoutingRule{
									{
										FromTable: "customer",
										ToTables:  []string{"commerce.customer"},
									},
									{
										FromTable: "customer@rdonly",
										ToTables:  []string{"customer.customer"},
									},
									{
										FromTable: "customer.customer",
										ToTables:  []string{"commerce.customer"},
									},
								},
							},
						},
					},
					{
						Cell: "zone1",
						Cluster: &vtadminpb.Cluster{
							Id:   clusterID,
							Name: clusterName,
						},
						SrvVSchema: &vschemapb.SrvVSchema{
							RoutingRules: &vschemapb.RoutingRules{},
						},
					},
				},
			},
		},
		{
			name:  "filtering by cell",
			cells: []string{"zone0", "zone1"},
			cellSrvVSchemas: map[string]*vschemapb.SrvVSchema{
				"zone0": {
					Keyspaces: map[string]*vschemapb.Keyspace{
						"commerce": {
							Tables: map[string]*vschemapb.Table{
								"customer": {},
							},
						},
						"customer": {
							Tables: map[string]*vschemapb.Table{
								"customer": {},
							},
						},
					},
					RoutingRules: &vschemapb.RoutingRules{
						Rules: []*vschemapb.RoutingRule{
							{
								FromTable: "customer",
								ToTables:  []string{"commerce.customer"},
							},
							{
								FromTable: "customer@rdonly",
								ToTables:  []string{"customer.customer"},
							},
							{
								FromTable: "customer.customer",
								ToTables:  []string{"commerce.customer"},
							},
						},
					},
				},
				"zone1": {
					Keyspaces:    map[string]*vschemapb.Keyspace{},
					RoutingRules: &vschemapb.RoutingRules{},
				},
			},
			req: &vtadminpb.GetSrvVSchemasRequest{
				Cells: []string{"zone1"},
			},
			expected: &vtadminpb.GetSrvVSchemasResponse{
				SrvVSchemas: []*vtadminpb.SrvVSchema{
					{
						Cell: "zone1",
						Cluster: &vtadminpb.Cluster{
							Id:   clusterID,
							Name: clusterName,
						},
						SrvVSchema: &vschemapb.SrvVSchema{
							RoutingRules: &vschemapb.RoutingRules{},
						},
					},
				},
			},
		},
		{
			name:  "filtering by nonexistent cell",
			cells: []string{"zone0"},
			cellSrvVSchemas: map[string]*vschemapb.SrvVSchema{
				"zone0": {
					Keyspaces:    map[string]*vschemapb.Keyspace{},
					RoutingRules: &vschemapb.RoutingRules{},
				},
			},
			req: &vtadminpb.GetSrvVSchemasRequest{
				Cells: []string{"doesnt-exist"},
			},
			expected: &vtadminpb.GetSrvVSchemasResponse{},
		},
		{
			name:  "filtering with nonexistent cell",
			cells: []string{"zone0"},
			cellSrvVSchemas: map[string]*vschemapb.SrvVSchema{
				"zone0": {
					Keyspaces:    map[string]*vschemapb.Keyspace{},
					RoutingRules: &vschemapb.RoutingRules{},
				},
			},
			req: &vtadminpb.GetSrvVSchemasRequest{
				Cells: []string{"doesnt-exist", "zone0"},
			},
			expected: &vtadminpb.GetSrvVSchemasResponse{
				SrvVSchemas: []*vtadminpb.SrvVSchema{
					{
						Cell: "zone0",
						Cluster: &vtadminpb.Cluster{
							Id:   clusterID,
							Name: clusterName,
						},
						SrvVSchema: &vschemapb.SrvVSchema{
							RoutingRules: &vschemapb.RoutingRules{},
						},
					},
				},
			},
		},
		{
			name:  "existing cell without SrvVSchema",
			cells: []string{"zone0", "zone1"},
			cellSrvVSchemas: map[string]*vschemapb.SrvVSchema{
				"zone0": {
					Keyspaces:    map[string]*vschemapb.Keyspace{},
					RoutingRules: &vschemapb.RoutingRules{},
				},
			},
			req: &vtadminpb.GetSrvVSchemasRequest{
				Cells: []string{"zone1"},
			},
			expected: &vtadminpb.GetSrvVSchemasResponse{
				SrvVSchemas: []*vtadminpb.SrvVSchema{
					{
						Cell: "zone1",
						Cluster: &vtadminpb.Cluster{
							Id:   clusterID,
							Name: clusterName,
						},
						SrvVSchema: &vschemapb.SrvVSchema{},
					},
				},
			},
		},
		{
			name:  "filtering by nonexistent cluster",
			cells: []string{"zone0"},
			cellSrvVSchemas: map[string]*vschemapb.SrvVSchema{
				"zone0": {
					Keyspaces:    map[string]*vschemapb.Keyspace{},
					RoutingRules: &vschemapb.RoutingRules{},
				},
			},
			req: &vtadminpb.GetSrvVSchemasRequest{
				ClusterIds: []string{"doesnt-exist"},
			},
			expected: &vtadminpb.GetSrvVSchemasResponse{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			tmc := testutil.TabletManagerClient{}

			toposerver := memorytopo.NewServer(ctx, tt.cells...)

			vtctldserver := testutil.NewVtctldServerWithTabletManagerClient(t, toposerver, &tmc, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return grpcvtctldserver.NewVtctldServer(vtenv.NewTestEnv(), ts)
			})

			testutil.WithTestServer(t, vtctldserver, func(t *testing.T, vtctldClient vtctldclient.VtctldClient) {
				for cell, svs := range tt.cellSrvVSchemas {
					err := toposerver.UpdateSrvVSchema(ctx, cell, svs)
					require.NoError(t, err)
				}

				clusters := []*cluster.Cluster{
					vtadmintestutil.BuildCluster(t, vtadmintestutil.TestClusterConfig{
						Cluster: &vtadminpb.Cluster{
							Id:   clusterID,
							Name: clusterName,
						},
						VtctldClient: vtctldClient,
					}),
				}

				api := NewAPI(vtenv.NewTestEnv(), clusters, Options{})
				resp, err := api.GetSrvVSchemas(ctx, tt.req)

				if tt.shouldErr {
					assert.Error(t, err)
					return
				}

				require.NoError(t, err)
				vtadmintestutil.AssertSrvVSchemaSlicesEqual(t, tt.expected.SrvVSchemas, resp.SrvVSchemas)
			})
		})
	}
}

func TestGetTablet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		clusterTablets [][]*vtadminpb.Tablet
		dbconfigs      map[string]vtadmintestutil.Dbcfg
		req            *vtadminpb.GetTabletRequest
		expected       *vtadminpb.Tablet
		shouldErr      bool
	}{
		{
			name: "single cluster",
			clusterTablets: [][]*vtadminpb.Tablet{
				{
					/* cluster 0 */
					{
						State: vtadminpb.Tablet_SERVING,
						Tablet: &topodatapb.Tablet{
							Alias: &topodatapb.TabletAlias{
								Uid:  100,
								Cell: "zone1",
							},
							Hostname: "ks1-00-00-zone1-a",
							Keyspace: "ks1",
							Shard:    "-",
							Type:     topodatapb.TabletType_PRIMARY,
						},
					},
				},
			},
			dbconfigs: map[string]vtadmintestutil.Dbcfg{},
			req: &vtadminpb.GetTabletRequest{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			expected: &vtadminpb.Tablet{
				Cluster: &vtadminpb.Cluster{
					Id:   "c0",
					Name: "cluster0",
				},
				State: vtadminpb.Tablet_SERVING,
				Tablet: &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Uid:  100,
						Cell: "zone1",
					},
					Hostname: "ks1-00-00-zone1-a",
					Keyspace: "ks1",
					Shard:    "-",
					Type:     topodatapb.TabletType_PRIMARY,
				},
			},
			shouldErr: false,
		},
		{
			name: "one cluster errors",
			clusterTablets: [][]*vtadminpb.Tablet{
				/* cluster 0 */
				{
					{
						State: vtadminpb.Tablet_SERVING,
						Tablet: &topodatapb.Tablet{
							Alias: &topodatapb.TabletAlias{
								Uid:  100,
								Cell: "zone1",
							},
							Hostname: "ks1-00-00-zone1-a",
							Keyspace: "ks1",
							Shard:    "-",
							Type:     topodatapb.TabletType_PRIMARY,
						},
					},
				},
				/* cluster 1 */
				{
					{
						State: vtadminpb.Tablet_SERVING,
						Tablet: &topodatapb.Tablet{
							Alias: &topodatapb.TabletAlias{
								Uid:  200,
								Cell: "zone1",
							},
							Hostname: "ks2-00-00-zone1-a",
							Keyspace: "ks2",
							Shard:    "-",
							Type:     topodatapb.TabletType_PRIMARY,
						},
					},
				},
			},
			dbconfigs: map[string]vtadmintestutil.Dbcfg{
				"c1": {ShouldErr: true},
			},
			req: &vtadminpb.GetTabletRequest{
				Alias: &topodatapb.TabletAlias{
					Cell: "doesntmatter",
					Uid:  100,
				},
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "multi cluster, selecting one with tablet",
			clusterTablets: [][]*vtadminpb.Tablet{
				/* cluster 0 */
				{
					{
						State: vtadminpb.Tablet_SERVING,
						Tablet: &topodatapb.Tablet{
							Alias: &topodatapb.TabletAlias{
								Uid:  100,
								Cell: "zone1",
							},
							Hostname: "ks1-00-00-zone1-a",
							Keyspace: "ks1",
							Shard:    "-",
							Type:     topodatapb.TabletType_PRIMARY,
						},
					},
				},
				/* cluster 1 */
				{
					{
						State: vtadminpb.Tablet_SERVING,
						Tablet: &topodatapb.Tablet{
							Alias: &topodatapb.TabletAlias{
								Uid:  200,
								Cell: "zone1",
							},
							Hostname: "ks2-00-00-zone1-a",
							Keyspace: "ks2",
							Shard:    "-",
							Type:     topodatapb.TabletType_PRIMARY,
						},
					},
				},
			},
			dbconfigs: map[string]vtadmintestutil.Dbcfg{},
			req: &vtadminpb.GetTabletRequest{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
				ClusterIds: []string{"c0"},
			},
			expected: &vtadminpb.Tablet{
				Cluster: &vtadminpb.Cluster{
					Id:   "c0",
					Name: "cluster0",
				},
				State: vtadminpb.Tablet_SERVING,
				Tablet: &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Uid:  100,
						Cell: "zone1",
					},
					Hostname: "ks1-00-00-zone1-a",
					Keyspace: "ks1",
					Shard:    "-",
					Type:     topodatapb.TabletType_PRIMARY,
				},
			},
			shouldErr: false,
		},
		{
			name: "multi cluster, multiple results",
			clusterTablets: [][]*vtadminpb.Tablet{
				/* cluster 0 */
				{
					{
						State: vtadminpb.Tablet_SERVING,
						Tablet: &topodatapb.Tablet{
							Alias: &topodatapb.TabletAlias{
								Uid:  100,
								Cell: "zone1",
							},
							Hostname: "ks1-00-00-zone1-a",
							Keyspace: "ks1",
							Shard:    "-",
							Type:     topodatapb.TabletType_PRIMARY,
						},
					},
				},
				/* cluster 1 */
				{
					{
						State: vtadminpb.Tablet_SERVING,
						Tablet: &topodatapb.Tablet{
							Alias: &topodatapb.TabletAlias{
								Uid:  100,
								Cell: "zone1",
							},
							Hostname: "ks1-00-00-zone1-a",
							Keyspace: "ks1",
							Shard:    "-",
							Type:     topodatapb.TabletType_PRIMARY,
						},
					},
				},
			},
			dbconfigs: map[string]vtadmintestutil.Dbcfg{},
			req: &vtadminpb.GetTabletRequest{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "no results",
			clusterTablets: [][]*vtadminpb.Tablet{
				/* cluster 0 */
				{},
			},
			dbconfigs: map[string]vtadmintestutil.Dbcfg{},
			req: &vtadminpb.GetTabletRequest{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
			expected:  nil,
			shouldErr: true,
		},
	}

	ctx := context.Background()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			clusters := make([]*cluster.Cluster, len(tt.clusterTablets))

			for i, tablets := range tt.clusterTablets {
				cid := fmt.Sprintf("c%d", i)
				dbconfigs := tt.dbconfigs[cid]

				clusters[i] = vtadmintestutil.BuildCluster(t, vtadmintestutil.TestClusterConfig{
					Cluster: &vtadminpb.Cluster{
						Id:   cid,
						Name: fmt.Sprintf("cluster%d", i),
					},
					Tablets:  tablets,
					DBConfig: dbconfigs,
				})
			}

			api := NewAPI(vtenv.NewTestEnv(), clusters, Options{})
			resp, err := api.GetTablet(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			utils.MustMatch(t, tt.expected, resp)
		})
	}
}

func TestGetTablets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		clusterTablets [][]*vtadminpb.Tablet
		dbconfigs      map[string]vtadmintestutil.Dbcfg
		req            *vtadminpb.GetTabletsRequest
		expected       []*vtadminpb.Tablet
		shouldErr      bool
	}{
		{
			name: "single cluster",
			clusterTablets: [][]*vtadminpb.Tablet{
				{
					/* cluster 0 */
					{
						State: vtadminpb.Tablet_SERVING,
						Tablet: &topodatapb.Tablet{
							Alias: &topodatapb.TabletAlias{
								Uid:  100,
								Cell: "zone1",
							},
							Hostname: "ks1-00-00-zone1-a",
							Keyspace: "ks1",
							Shard:    "-",
							Type:     topodatapb.TabletType_PRIMARY,
						},
					},
				},
			},
			dbconfigs: map[string]vtadmintestutil.Dbcfg{},
			req:       &vtadminpb.GetTabletsRequest{},
			expected: []*vtadminpb.Tablet{
				{
					Cluster: &vtadminpb.Cluster{
						Id:   "c0",
						Name: "cluster0",
					},
					State: vtadminpb.Tablet_SERVING,
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Uid:  100,
							Cell: "zone1",
						},
						Hostname: "ks1-00-00-zone1-a",
						Keyspace: "ks1",
						Shard:    "-",
						Type:     topodatapb.TabletType_PRIMARY,
					},
				},
			},
			shouldErr: false,
		},
		{
			name: "one cluster errors",
			clusterTablets: [][]*vtadminpb.Tablet{
				/* cluster 0 */
				{
					{
						State: vtadminpb.Tablet_SERVING,
						Tablet: &topodatapb.Tablet{
							Alias: &topodatapb.TabletAlias{
								Uid:  100,
								Cell: "zone1",
							},
							Hostname: "ks1-00-00-zone1-a",
							Keyspace: "ks1",
							Shard:    "-",
							Type:     topodatapb.TabletType_PRIMARY,
						},
					},
				},
				/* cluster 1 */
				{
					{
						State: vtadminpb.Tablet_SERVING,
						Tablet: &topodatapb.Tablet{
							Alias: &topodatapb.TabletAlias{
								Uid:  200,
								Cell: "zone1",
							},
							Hostname: "ks2-00-00-zone1-a",
							Keyspace: "ks2",
							Shard:    "-",
							Type:     topodatapb.TabletType_PRIMARY,
						},
					},
				},
			},
			dbconfigs: map[string]vtadmintestutil.Dbcfg{
				"c1": {ShouldErr: true},
			},
			req:       &vtadminpb.GetTabletsRequest{},
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "multi cluster, selecting one",
			clusterTablets: [][]*vtadminpb.Tablet{
				/* cluster 0 */
				{
					{
						State: vtadminpb.Tablet_SERVING,
						Tablet: &topodatapb.Tablet{
							Alias: &topodatapb.TabletAlias{
								Uid:  100,
								Cell: "zone1",
							},
							Hostname: "ks1-00-00-zone1-a",
							Keyspace: "ks1",
							Shard:    "-",
							Type:     topodatapb.TabletType_PRIMARY,
						},
					},
				},
				/* cluster 1 */
				{
					{
						State: vtadminpb.Tablet_SERVING,
						Tablet: &topodatapb.Tablet{
							Alias: &topodatapb.TabletAlias{
								Uid:  200,
								Cell: "zone1",
							},
							Hostname: "ks2-00-00-zone1-a",
							Keyspace: "ks2",
							Shard:    "-",
							Type:     topodatapb.TabletType_PRIMARY,
						},
					},
				},
			},
			dbconfigs: map[string]vtadmintestutil.Dbcfg{},
			req:       &vtadminpb.GetTabletsRequest{ClusterIds: []string{"c0"}},
			expected: []*vtadminpb.Tablet{
				{
					Cluster: &vtadminpb.Cluster{
						Id:   "c0",
						Name: "cluster0",
					},
					State: vtadminpb.Tablet_SERVING,
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Uid:  100,
							Cell: "zone1",
						},
						Hostname: "ks1-00-00-zone1-a",
						Keyspace: "ks1",
						Shard:    "-",
						Type:     topodatapb.TabletType_PRIMARY,
					},
				},
			},
			shouldErr: false,
		},
	}

	ctx := context.Background()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			clusters := make([]*cluster.Cluster, len(tt.clusterTablets))

			for i, tablets := range tt.clusterTablets {
				cid := fmt.Sprintf("c%d", i)
				dbconfigs := tt.dbconfigs[cid]

				clusters[i] = vtadmintestutil.BuildCluster(t, vtadmintestutil.TestClusterConfig{
					Cluster: &vtadminpb.Cluster{
						Id:   cid,
						Name: fmt.Sprintf("cluster%d", i),
					},
					Tablets:  tablets,
					DBConfig: dbconfigs,
				})
			}

			api := NewAPI(vtenv.NewTestEnv(), clusters, Options{})
			resp, err := api.GetTablets(ctx, tt.req)
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

	tests := []struct {
		name       string
		clusterCfg vtadmintestutil.TestClusterConfig
		req        *vtadminpb.GetVSchemaRequest
		expected   *vtadminpb.VSchema
		shouldErr  bool
	}{
		{
			name: "success",
			clusterCfg: vtadmintestutil.TestClusterConfig{
				Cluster: &vtadminpb.Cluster{
					Id:   "c1",
					Name: "cluster1",
				},
				VtctldClient: &fakevtctldclient.VtctldClient{
					GetVSchemaResults: map[string]struct {
						Response *vtctldatapb.GetVSchemaResponse
						Error    error
					}{
						"testkeyspace": {
							Response: &vtctldatapb.GetVSchemaResponse{
								VSchema: &vschemapb.Keyspace{
									Sharded: true,
									Vindexes: map[string]*vschemapb.Vindex{
										"hash": {
											Type: "md5hash",
										},
									},
								},
							},
						},
					},
				},
			},
			req: &vtadminpb.GetVSchemaRequest{
				ClusterId: "c1",
				Keyspace:  "testkeyspace",
			},
			expected: &vtadminpb.VSchema{
				Cluster: &vtadminpb.Cluster{
					Id:   "c1",
					Name: "cluster1",
				},
				Name: "testkeyspace",
				VSchema: &vschemapb.Keyspace{
					Sharded: true,
					Vindexes: map[string]*vschemapb.Vindex{
						"hash": {
							Type: "md5hash",
						},
					},
				},
			},
			shouldErr: false,
		},
		{
			name: "no vschema for keyspace",
			clusterCfg: vtadmintestutil.TestClusterConfig{
				Cluster: &vtadminpb.Cluster{
					Id:   "c1",
					Name: "cluster1",
				},
				VtctldClient: &fakevtctldclient.VtctldClient{
					GetVSchemaResults: map[string]struct {
						Response *vtctldatapb.GetVSchemaResponse
						Error    error
					}{
						"testkeyspace": {
							Response: &vtctldatapb.GetVSchemaResponse{
								VSchema: &vschemapb.Keyspace{
									Sharded: true,
									Vindexes: map[string]*vschemapb.Vindex{
										"hash": {
											Type: "md5hash",
										},
									},
								},
							},
						},
					},
				},
			},
			req: &vtadminpb.GetVSchemaRequest{
				ClusterId: "c1",
				Keyspace:  "otherkeyspace",
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "cluster not found",
			clusterCfg: vtadmintestutil.TestClusterConfig{
				Cluster: &vtadminpb.Cluster{
					Id:   "c1",
					Name: "cluster1",
				},
			},
			req: &vtadminpb.GetVSchemaRequest{
				ClusterId: "c2",
				Keyspace:  "testkeyspace",
			},
			expected:  nil,
			shouldErr: true,
		},
	}

	ctx := context.Background()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			clusters := []*cluster.Cluster{vtadmintestutil.BuildCluster(t, tt.clusterCfg)}
			api := NewAPI(vtenv.NewTestEnv(), clusters, Options{})

			resp, err := api.GetVSchema(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)

				return
			}

			assert.NoError(t, err)
			assert.Truef(t, proto.Equal(tt.expected, resp), "expected %v, got %v", tt.expected, resp)
		})
	}
}

func TestGetVSchemas(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		clusterCfgs []vtadmintestutil.TestClusterConfig
		req         *vtadminpb.GetVSchemasRequest
		expected    *vtadminpb.GetVSchemasResponse
		shouldErr   bool
	}{
		{
			name: "success",
			clusterCfgs: []vtadmintestutil.TestClusterConfig{
				{
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
									Name: "testkeyspace",
								},
							},
						},
						GetVSchemaResults: map[string]struct {
							Response *vtctldatapb.GetVSchemaResponse
							Error    error
						}{
							"testkeyspace": {
								Response: &vtctldatapb.GetVSchemaResponse{
									VSchema: &vschemapb.Keyspace{},
								},
							},
						},
					},
				},
				{
					Cluster: &vtadminpb.Cluster{
						Id:   "c2",
						Name: "cluster2",
					},
					VtctldClient: &fakevtctldclient.VtctldClient{
						GetKeyspacesResults: &struct {
							Keyspaces []*vtctldatapb.Keyspace
							Error     error
						}{
							Keyspaces: []*vtctldatapb.Keyspace{
								{
									Name: "k2",
								},
							},
						},
						GetVSchemaResults: map[string]struct {
							Response *vtctldatapb.GetVSchemaResponse
							Error    error
						}{
							"k2": {
								Response: &vtctldatapb.GetVSchemaResponse{
									VSchema: &vschemapb.Keyspace{},
								},
							},
						},
					},
				},
			},
			req: &vtadminpb.GetVSchemasRequest{},
			expected: &vtadminpb.GetVSchemasResponse{
				VSchemas: []*vtadminpb.VSchema{
					{
						Cluster: &vtadminpb.Cluster{
							Id:   "c1",
							Name: "cluster1",
						},
						Name:    "testkeyspace",
						VSchema: &vschemapb.Keyspace{},
					},
					{
						Cluster: &vtadminpb.Cluster{
							Id:   "c2",
							Name: "cluster2",
						},
						Name:    "k2",
						VSchema: &vschemapb.Keyspace{},
					},
				},
			},
			shouldErr: false,
		},
		{
			name: "requesting specific clusters",
			clusterCfgs: []vtadmintestutil.TestClusterConfig{
				{
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
									Name: "testkeyspace",
								},
							},
						},
						GetVSchemaResults: map[string]struct {
							Response *vtctldatapb.GetVSchemaResponse
							Error    error
						}{
							"testkeyspace": {
								Response: &vtctldatapb.GetVSchemaResponse{
									VSchema: &vschemapb.Keyspace{},
								},
							},
						},
					},
				},
				{
					Cluster: &vtadminpb.Cluster{
						Id:   "c2",
						Name: "cluster2",
					},
					VtctldClient: &fakevtctldclient.VtctldClient{
						GetKeyspacesResults: &struct {
							Keyspaces []*vtctldatapb.Keyspace
							Error     error
						}{
							Keyspaces: []*vtctldatapb.Keyspace{
								{
									Name: "k2",
								},
							},
						},
						GetVSchemaResults: map[string]struct {
							Response *vtctldatapb.GetVSchemaResponse
							Error    error
						}{
							"k2": {
								Response: &vtctldatapb.GetVSchemaResponse{
									VSchema: &vschemapb.Keyspace{},
								},
							},
						},
					},
				},
			},
			req: &vtadminpb.GetVSchemasRequest{
				ClusterIds: []string{"c2"},
			},
			expected: &vtadminpb.GetVSchemasResponse{
				VSchemas: []*vtadminpb.VSchema{
					{
						Cluster: &vtadminpb.Cluster{
							Id:   "c2",
							Name: "cluster2",
						},
						Name:    "k2",
						VSchema: &vschemapb.Keyspace{},
					},
				},
			},
			shouldErr: false,
		},
		{
			name: "GetKeyspaces failure",
			clusterCfgs: []vtadmintestutil.TestClusterConfig{
				{
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
									Name: "testkeyspace",
								},
							},
						},
						GetVSchemaResults: map[string]struct {
							Response *vtctldatapb.GetVSchemaResponse
							Error    error
						}{
							"testkeyspace": {
								Response: &vtctldatapb.GetVSchemaResponse{
									VSchema: &vschemapb.Keyspace{},
								},
							},
						},
					},
				},
				{
					Cluster: &vtadminpb.Cluster{
						Id:   "c2",
						Name: "cluster2",
					},
					VtctldClient: &fakevtctldclient.VtctldClient{
						GetKeyspacesResults: &struct {
							Keyspaces []*vtctldatapb.Keyspace
							Error     error
						}{
							Error: assert.AnError,
						},
					},
				},
			},
			req:       &vtadminpb.GetVSchemasRequest{},
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "GetVSchema failure",
			clusterCfgs: []vtadmintestutil.TestClusterConfig{
				{
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
									Name: "testkeyspace",
								},
							},
						},
						GetVSchemaResults: map[string]struct {
							Response *vtctldatapb.GetVSchemaResponse
							Error    error
						}{
							"testkeyspace": {
								Error: assert.AnError,
							},
						},
					},
				},
				{
					Cluster: &vtadminpb.Cluster{
						Id:   "c2",
						Name: "cluster2",
					},
					VtctldClient: &fakevtctldclient.VtctldClient{
						GetKeyspacesResults: &struct {
							Keyspaces []*vtctldatapb.Keyspace
							Error     error
						}{
							Keyspaces: []*vtctldatapb.Keyspace{
								{
									Name: "k2",
								},
							},
						},
						GetVSchemaResults: map[string]struct {
							Response *vtctldatapb.GetVSchemaResponse
							Error    error
						}{
							"k2": {
								Response: &vtctldatapb.GetVSchemaResponse{
									VSchema: &vschemapb.Keyspace{},
								},
							},
						},
					},
				},
			},
			req:       &vtadminpb.GetVSchemasRequest{},
			expected:  nil,
			shouldErr: true,
		},
		{
			name:        "no clusters specified",
			clusterCfgs: []vtadmintestutil.TestClusterConfig{},
			req:         &vtadminpb.GetVSchemasRequest{},
			expected: &vtadminpb.GetVSchemasResponse{
				VSchemas: []*vtadminpb.VSchema{},
			},
			shouldErr: false,
		},
		{
			name:        "requested invalid cluster",
			clusterCfgs: []vtadmintestutil.TestClusterConfig{},
			req: &vtadminpb.GetVSchemasRequest{
				ClusterIds: []string{"c1"},
			},
			expected:  nil,
			shouldErr: true,
		},
	}

	ctx := context.Background()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if tt.req == nil {
				t.SkipNow()
			}

			clusters := vtadmintestutil.BuildClusters(t, tt.clusterCfgs...)
			api := NewAPI(vtenv.NewTestEnv(), clusters, Options{})

			resp, err := api.GetVSchemas(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)

				return
			}

			assert.NoError(t, err)
			assert.ElementsMatch(t, tt.expected.VSchemas, resp.VSchemas)
		})
	}
}

func TestGetVtctlds(t *testing.T) {
	t.Parallel()

	fakedisco1 := fakediscovery.New()
	cluster1 := &cluster.Cluster{
		ID:        "c1",
		Name:      "cluster1",
		Discovery: fakedisco1,
	}
	cluster1Vtctlds := []*vtadminpb.Vtctld{
		{
			Hostname: "cluster1-vtctld1",
		},
		{
			Hostname: "cluster1-vtctld2",
		},
		{
			Hostname: "cluster1-vtctld3",
		},
	}
	fakedisco1.AddTaggedVtctlds(nil, cluster1Vtctlds...)

	expectedCluster1Vtctlds := []*vtadminpb.Vtctld{
		{
			Cluster: &vtadminpb.Cluster{
				Id:   cluster1.ID,
				Name: cluster1.Name,
			},
			Hostname: "cluster1-vtctld1",
		},
		{
			Cluster: &vtadminpb.Cluster{
				Id:   cluster1.ID,
				Name: cluster1.Name,
			},
			Hostname: "cluster1-vtctld2",
		},
		{
			Cluster: &vtadminpb.Cluster{
				Id:   cluster1.ID,
				Name: cluster1.Name,
			},
			Hostname: "cluster1-vtctld3",
		},
	}

	fakedisco2 := fakediscovery.New()
	cluster2 := &cluster.Cluster{
		ID:        "c2",
		Name:      "cluster2",
		Discovery: fakedisco2,
	}
	cluster2Vtctlds := []*vtadminpb.Vtctld{
		{
			Hostname: "cluster2-vtctld1",
		},
	}
	fakedisco2.AddTaggedVtctlds(nil, cluster2Vtctlds...)

	expectedCluster2Vtctlds := []*vtadminpb.Vtctld{
		{
			Cluster: &vtadminpb.Cluster{
				Id:   cluster2.ID,
				Name: cluster2.Name,
			},
			Hostname: "cluster2-vtctld1",
		},
	}

	api := NewAPI(vtenv.NewTestEnv(), []*cluster.Cluster{cluster1, cluster2}, Options{})
	ctx := context.Background()

	resp, err := api.GetVtctlds(ctx, &vtadminpb.GetVtctldsRequest{})
	assert.NoError(t, err)
	assert.ElementsMatch(t, append(expectedCluster1Vtctlds, expectedCluster2Vtctlds...), resp.Vtctlds)

	resp, err = api.GetVtctlds(ctx, &vtadminpb.GetVtctldsRequest{ClusterIds: []string{cluster1.ID}})
	assert.NoError(t, err)
	assert.ElementsMatch(t, expectedCluster1Vtctlds, resp.Vtctlds)

	fakedisco1.SetVtctldsError(true)

	resp, err = api.GetVtctlds(ctx, &vtadminpb.GetVtctldsRequest{})
	assert.Error(t, err)
	assert.Nil(t, resp)
}

func TestGetWorkflow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		cfgs      []vtadmintestutil.TestClusterConfig
		req       *vtadminpb.GetWorkflowRequest
		expected  *vtadminpb.Workflow
		shouldErr bool
	}{
		{
			name: "success",
			cfgs: []vtadmintestutil.TestClusterConfig{
				{
					Cluster: &vtadminpb.Cluster{
						Id:   "c1",
						Name: "cluster1",
					},
					VtctldClient: &fakevtctldclient.VtctldClient{
						GetWorkflowsResults: map[string]struct {
							Response *vtctldatapb.GetWorkflowsResponse
							Error    error
						}{
							"testkeyspace": {
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
			},
			req: &vtadminpb.GetWorkflowRequest{
				ClusterId: "c1",
				Keyspace:  "testkeyspace",
				Name:      "workflow1",
			},
			expected: &vtadminpb.Workflow{
				Cluster: &vtadminpb.Cluster{
					Id:   "c1",
					Name: "cluster1",
				},
				Keyspace: "testkeyspace",
				Workflow: &vtctldatapb.Workflow{
					Name: "workflow1",
				},
			},
			shouldErr: false,
		},
		{
			name: "no such workflow",
			cfgs: []vtadmintestutil.TestClusterConfig{
				{
					Cluster: &vtadminpb.Cluster{
						Id:   "c1",
						Name: "cluster1",
					},
					VtctldClient: &fakevtctldclient.VtctldClient{
						GetWorkflowsResults: map[string]struct {
							Response *vtctldatapb.GetWorkflowsResponse
							Error    error
						}{
							"testkeyspace": {
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
			},
			req: &vtadminpb.GetWorkflowRequest{
				ClusterId: "c1",
				Keyspace:  "testkeyspace",
				Name:      "workflow3",
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "no such cluster",
			cfgs: []vtadmintestutil.TestClusterConfig{},
			req: &vtadminpb.GetWorkflowRequest{
				ClusterId: "c1",
				Keyspace:  "testkeyspace",
				Name:      "workflow1",
			},
			expected:  nil,
			shouldErr: true,
		},
	}

	ctx := context.Background()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			api := NewAPI(vtenv.NewTestEnv(), vtadmintestutil.BuildClusters(t, tt.cfgs...), Options{})

			resp, err := api.GetWorkflow(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)

				return
			}

			assert.NoError(t, err)
			assert.Truef(t, proto.Equal(tt.expected, resp), "expected %v, got %v", tt.expected, resp)
		})
	}
}

func TestGetWorkflows(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		cfgs      []vtadmintestutil.TestClusterConfig
		req       *vtadminpb.GetWorkflowsRequest
		expected  *vtadminpb.GetWorkflowsResponse
		shouldErr bool
	}{
		{
			name: "success",
			cfgs: []vtadmintestutil.TestClusterConfig{
				{
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
									Name: "testkeyspace",
								},
							},
						},
						GetWorkflowsResults: map[string]struct {
							Response *vtctldatapb.GetWorkflowsResponse
							Error    error
						}{
							"testkeyspace": {
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
				{
					Cluster: &vtadminpb.Cluster{
						Id:   "c2",
						Name: "cluster2",
					},
					VtctldClient: &fakevtctldclient.VtctldClient{
						GetKeyspacesResults: &struct {
							Keyspaces []*vtctldatapb.Keyspace
							Error     error
						}{
							Keyspaces: []*vtctldatapb.Keyspace{
								{
									Name: "otherkeyspace",
								},
							},
						},
						GetWorkflowsResults: map[string]struct {
							Response *vtctldatapb.GetWorkflowsResponse
							Error    error
						}{
							"otherkeyspace": {
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
			},
			req: &vtadminpb.GetWorkflowsRequest{},
			expected: &vtadminpb.GetWorkflowsResponse{
				WorkflowsByCluster: map[string]*vtadminpb.ClusterWorkflows{
					"c1": {
						Workflows: []*vtadminpb.Workflow{
							{
								Cluster: &vtadminpb.Cluster{
									Id:   "c1",
									Name: "cluster1",
								},
								Keyspace: "testkeyspace",
								Workflow: &vtctldatapb.Workflow{
									Name: "workflow1",
								},
							},
							{
								Cluster: &vtadminpb.Cluster{
									Id:   "c1",
									Name: "cluster1",
								},
								Keyspace: "testkeyspace",
								Workflow: &vtctldatapb.Workflow{
									Name: "workflow2",
								},
							},
						},
					},
					"c2": {
						Workflows: []*vtadminpb.Workflow{
							{
								Cluster: &vtadminpb.Cluster{
									Id:   "c2",
									Name: "cluster2",
								},
								Keyspace: "otherkeyspace",
								Workflow: &vtctldatapb.Workflow{
									Name: "workflow1",
								},
							},
						},
					},
				},
			},
			shouldErr: false,
		},
		{
			name: "one cluster has partial error then request succeeds",
			cfgs: []vtadmintestutil.TestClusterConfig{
				{
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
									Name: "testkeyspace",
								},
							},
						},
						GetWorkflowsResults: map[string]struct {
							Response *vtctldatapb.GetWorkflowsResponse
							Error    error
						}{
							"testkeyspace": {
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
				{
					Cluster: &vtadminpb.Cluster{
						Id:   "c2",
						Name: "cluster2",
					},
					VtctldClient: &fakevtctldclient.VtctldClient{
						GetKeyspacesResults: &struct {
							Keyspaces []*vtctldatapb.Keyspace
							Error     error
						}{
							Keyspaces: []*vtctldatapb.Keyspace{
								{
									Name: "otherkeyspace",
								},
								{
									Name: "badkeyspace",
								},
							},
						},
						GetWorkflowsResults: map[string]struct {
							Response *vtctldatapb.GetWorkflowsResponse
							Error    error
						}{
							"otherkeyspace": {
								Response: &vtctldatapb.GetWorkflowsResponse{
									Workflows: []*vtctldatapb.Workflow{
										{
											Name: "workflow1",
										},
									},
								},
							},
							"badkeyspace": {
								Error: assert.AnError,
							},
						},
					},
				},
			},
			req: &vtadminpb.GetWorkflowsRequest{},
			expected: &vtadminpb.GetWorkflowsResponse{
				WorkflowsByCluster: map[string]*vtadminpb.ClusterWorkflows{
					"c1": {
						Workflows: []*vtadminpb.Workflow{
							{
								Cluster: &vtadminpb.Cluster{
									Id:   "c1",
									Name: "cluster1",
								},
								Keyspace: "testkeyspace",
								Workflow: &vtctldatapb.Workflow{
									Name: "workflow1",
								},
							},
							{
								Cluster: &vtadminpb.Cluster{
									Id:   "c1",
									Name: "cluster1",
								},
								Keyspace: "testkeyspace",
								Workflow: &vtctldatapb.Workflow{
									Name: "workflow2",
								},
							},
						},
						Warnings: []string{},
					},
					"c2": {
						Workflows: []*vtadminpb.Workflow{
							{
								Cluster: &vtadminpb.Cluster{
									Id:   "c2",
									Name: "cluster2",
								},
								Keyspace: "otherkeyspace",
								Workflow: &vtctldatapb.Workflow{
									Name: "workflow1",
								},
							},
						},
						Warnings: []string{"some warning about badkeyspace"},
					},
				},
			},
			shouldErr: false,
		},
		{
			name: "IgnoreKeyspaces applies across clusters",
			cfgs: []vtadmintestutil.TestClusterConfig{
				{
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
									Name: "testkeyspace",
								},
							},
						},
						GetWorkflowsResults: map[string]struct {
							Response *vtctldatapb.GetWorkflowsResponse
							Error    error
						}{
							"testkeyspace": {
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
				{
					Cluster: &vtadminpb.Cluster{
						Id:   "c2",
						Name: "cluster2",
					},
					VtctldClient: &fakevtctldclient.VtctldClient{
						GetKeyspacesResults: &struct {
							Keyspaces []*vtctldatapb.Keyspace
							Error     error
						}{
							Keyspaces: []*vtctldatapb.Keyspace{
								{
									Name: "testkeyspace",
								},
								{
									Name: "otherkeyspace",
								},
							},
						},
						GetWorkflowsResults: map[string]struct {
							Response *vtctldatapb.GetWorkflowsResponse
							Error    error
						}{
							"testkeyspace": {
								Response: &vtctldatapb.GetWorkflowsResponse{
									Workflows: []*vtctldatapb.Workflow{
										{
											Name: "workflow1",
										},
									},
								},
							},
							"otherkeyspace": {
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
			},
			req: &vtadminpb.GetWorkflowsRequest{
				IgnoreKeyspaces: []string{"testkeyspace"},
			},
			expected: &vtadminpb.GetWorkflowsResponse{
				WorkflowsByCluster: map[string]*vtadminpb.ClusterWorkflows{
					"c1": {},
					"c2": {
						Workflows: []*vtadminpb.Workflow{
							{
								Cluster: &vtadminpb.Cluster{
									Id:   "c2",
									Name: "cluster2",
								},
								Keyspace: "otherkeyspace",
								Workflow: &vtctldatapb.Workflow{
									Name: "workflow1",
								},
							},
						},
					},
				},
			},
			shouldErr: false,
		},
		{
			name: "one cluster has fatal error, request fails",
			cfgs: []vtadmintestutil.TestClusterConfig{
				{
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
									Name: "testkeyspace",
								},
							},
						},
						GetWorkflowsResults: map[string]struct {
							Response *vtctldatapb.GetWorkflowsResponse
							Error    error
						}{
							"testkeyspace": {
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
				{
					Cluster: &vtadminpb.Cluster{
						Id:   "c2",
						Name: "cluster2",
					},
					VtctldClient: &fakevtctldclient.VtctldClient{
						GetKeyspacesResults: &struct {
							Keyspaces []*vtctldatapb.Keyspace
							Error     error
						}{
							Error: assert.AnError, // GetKeyspaces is a fatal error
						},
					},
				},
			},
			req:       &vtadminpb.GetWorkflowsRequest{},
			expected:  nil,
			shouldErr: true,
		},
	}

	ctx := context.Background()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			api := NewAPI(vtenv.NewTestEnv(), vtadmintestutil.BuildClusters(t, tt.cfgs...), Options{})

			resp, err := api.GetWorkflows(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)

				return
			}

			assert.NoError(t, err)
			require.NotNil(t, resp)

			vtadmintestutil.AssertGetWorkflowsResponsesEqual(t, tt.expected, resp)
		})
	}
}

func TestVTExplain(t *testing.T) {
	tests := []struct {
		name          string
		keyspaces     []*vtctldatapb.Keyspace
		shards        []*vtctldatapb.Shard
		srvVSchema    *vschemapb.SrvVSchema
		tabletSchemas map[string]*tabletmanagerdatapb.SchemaDefinition
		tablets       []*vtadminpb.Tablet
		req           *vtadminpb.VTExplainRequest
		expectedError error
	}{
		{
			name: "runs VTExplain given a valid request in a valid topology",
			keyspaces: []*vtctldatapb.Keyspace{
				{
					Name:     "commerce",
					Keyspace: &topodatapb.Keyspace{},
				},
			},
			shards: []*vtctldatapb.Shard{
				{
					Name:     "-",
					Keyspace: "commerce",
				},
			},
			srvVSchema: &vschemapb.SrvVSchema{
				Keyspaces: map[string]*vschemapb.Keyspace{
					"commerce": {
						Sharded: false,
						Tables: map[string]*vschemapb.Table{
							"customers": {},
						},
					},
				},
				RoutingRules: &vschemapb.RoutingRules{
					Rules: []*vschemapb.RoutingRule{},
				},
			},
			tabletSchemas: map[string]*tabletmanagerdatapb.SchemaDefinition{
				"c0_cell1-0000000100": {
					DatabaseSchema: "CREATE DATABASE commerce",
					TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
						{
							Name:       "t1",
							Schema:     `CREATE TABLE customers (id int(11) not null,PRIMARY KEY (id));`,
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
			tablets: []*vtadminpb.Tablet{
				{
					Cluster: &vtadminpb.Cluster{
						Id:   "c0",
						Name: "cluster0",
					},
					State: vtadminpb.Tablet_SERVING,
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Uid:  100,
							Cell: "c0_cell1",
						},
						Hostname: "tablet-cell1-a",
						Keyspace: "commerce",
						Shard:    "-",
						Type:     topodatapb.TabletType_REPLICA,
					},
				},
			},
			req: &vtadminpb.VTExplainRequest{
				Cluster:  "c0",
				Keyspace: "commerce",
				Sql:      "select * from customers",
			},
		},
		{
			name: "returns an error if no appropriate tablet found in keyspace",
			keyspaces: []*vtctldatapb.Keyspace{
				{
					Name:     "commerce",
					Keyspace: &topodatapb.Keyspace{},
				},
			},
			shards: []*vtctldatapb.Shard{
				{
					Name:     "-",
					Keyspace: "commerce",
				},
			},
			srvVSchema: &vschemapb.SrvVSchema{
				Keyspaces: map[string]*vschemapb.Keyspace{
					"commerce": {
						Sharded: false,
						Tables: map[string]*vschemapb.Table{
							"customers": {},
						},
					},
				},
				RoutingRules: &vschemapb.RoutingRules{
					Rules: []*vschemapb.RoutingRule{},
				},
			},
			tabletSchemas: map[string]*tabletmanagerdatapb.SchemaDefinition{
				"c0_cell1-0000000102": {
					DatabaseSchema: "CREATE DATABASE commerce",
					TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
						{
							Name:       "t1",
							Schema:     `CREATE TABLE customers (id int(11) not null,PRIMARY KEY (id));`,
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
			tablets: []*vtadminpb.Tablet{
				{
					Cluster: &vtadminpb.Cluster{
						Id:   "c0",
						Name: "cluster0",
					},
					State: vtadminpb.Tablet_SERVING,
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Uid:  100,
							Cell: "c0_cell1",
						},
						Hostname: "tablet-cell1-a",
						Keyspace: "commerce",
						Shard:    "-",
						Type:     topodatapb.TabletType_PRIMARY,
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
							Uid:  101,
							Cell: "c0_cell1",
						},
						Hostname: "tablet-cell1-b",
						Keyspace: "commerce",
						Shard:    "-",
						Type:     topodatapb.TabletType_DRAINED,
					},
				},
				{
					Cluster: &vtadminpb.Cluster{
						Id:   "c0",
						Name: "cluster0",
					},
					State: vtadminpb.Tablet_NOT_SERVING,
					Tablet: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Uid:  102,
							Cell: "c0_cell1",
						},
						Hostname: "tablet-cell1-c",
						Keyspace: "commerce",
						Shard:    "-",
						Type:     topodatapb.TabletType_REPLICA,
					},
				},
			},
			req: &vtadminpb.VTExplainRequest{
				Cluster:  "c0",
				Keyspace: "commerce",
				Sql:      "select * from customers",
			},
			expectedError: vtadminerrors.ErrNoTablet,
		},
		{
			name: "returns an error if cluster unspecified in request",
			req: &vtadminpb.VTExplainRequest{
				Keyspace: "commerce",
				Sql:      "select * from customers",
			},
			expectedError: vtadminerrors.ErrInvalidRequest,
		},
		{
			name: "returns an error if keyspace unspecified in request",
			req: &vtadminpb.VTExplainRequest{
				Cluster: "c0",
				Sql:     "select * from customers",
			},
			expectedError: vtadminerrors.ErrInvalidRequest,
		},
		{
			name: "returns an error if SQL unspecified in request",
			req: &vtadminpb.VTExplainRequest{
				Cluster:  "c0",
				Keyspace: "commerce",
			},
			expectedError: vtadminerrors.ErrInvalidRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			toposerver := memorytopo.NewServer(ctx, "c0_cell1")

			tmc := testutil.TabletManagerClient{
				GetSchemaResults: map[string]struct {
					Schema *tabletmanagerdatapb.SchemaDefinition
					Error  error
				}{},
			}

			vtctldserver := testutil.NewVtctldServerWithTabletManagerClient(t, toposerver, &tmc, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return grpcvtctldserver.NewVtctldServer(vtenv.NewTestEnv(), ts)
			})

			testutil.WithTestServer(t, vtctldserver, func(t *testing.T, vtctldClient vtctldclient.VtctldClient) {
				if tt.srvVSchema != nil {
					err := toposerver.UpdateSrvVSchema(ctx, "c0_cell1", tt.srvVSchema)
					require.NoError(t, err)
				}
				testutil.AddKeyspaces(ctx, t, toposerver, tt.keyspaces...)
				testutil.AddShards(ctx, t, toposerver, tt.shards...)

				for _, tablet := range tt.tablets {
					testutil.AddTablet(ctx, t, toposerver, tablet.Tablet, nil)

					// Adds each SchemaDefinition to the fake TabletManagerClient, or nil
					// if there are no schemas for that tablet. (All tablet aliases must
					// exist in the map. Otherwise, TabletManagerClient will return an error when
					// looking up the schema with tablet alias that doesn't exist.)
					alias := topoproto.TabletAliasString(tablet.Tablet.Alias)
					tmc.GetSchemaResults[alias] = struct {
						Schema *tabletmanagerdatapb.SchemaDefinition
						Error  error
					}{
						Schema: tt.tabletSchemas[alias],
						Error:  nil,
					}
				}

				clusters := []*cluster.Cluster{
					vtadmintestutil.BuildCluster(t, vtadmintestutil.TestClusterConfig{
						Cluster: &vtadminpb.Cluster{
							Id:   "c0",
							Name: "cluster0",
						},
						VtctldClient: vtctldClient,
						Tablets:      tt.tablets,
					}),
				}

				api := NewAPI(vtenv.NewTestEnv(), clusters, Options{})
				resp, err := api.VTExplain(ctx, tt.req)

				if tt.expectedError != nil {
					assert.True(t, errors.Is(err, tt.expectedError), "expected error type %w does not match actual error type %w", err, tt.expectedError)
				} else {
					require.NoError(t, err)

					// We don't particularly care to test the contents of the VTExplain response,
					// just that it exists.
					assert.NotEmpty(t, resp.Response)
				}
			})
		})
	}
}

type ServeHTTPVtctldResponse struct {
	Result ServeHTTPVtctldResult `json:"result"`
	Ok     bool                  `json:"ok"`
}

type ServeHTTPVtctldResult struct {
	Vtctlds []*vtadminpb.Vtctld `json:"vtctlds"`
}

type ServeHTTPResponse struct {
	Result ServeHTTPResult `json:"result"`
	Ok     bool            `json:"ok"`
}

type ServeHTTPResult struct {
	Clusters []*vtadminpb.Cluster `json:"clusters"`
}

func TestServeHTTP(t *testing.T) {
	t.Parallel()

	testCluster, _ := cluster.Config{
		ID:            "dynamiccluster1",
		Name:          "dynamiccluster1",
		DiscoveryImpl: "dynamic",
		DiscoveryFlagsByImpl: cluster.FlagsByImpl{
			"dynamic": {
				"discovery": "{\"vtctlds\": [{\"host\":{\"fqdn\": \"localhost:15000\", \"hostname\": \"localhost:15999\"}}], \"vtgates\": [{\"host\": {\"hostname\": \"localhost:15991\"}}]}",
			},
		},
	}.Cluster(context.Background())
	defer testCluster.Close()

	tests := []struct {
		name                  string
		cookie                string
		enableDynamicClusters bool
		testClusterVtctld     string
		clusters              []*cluster.Cluster
		expected              []*vtadminpb.Cluster
		expectedVtctlds       []*vtadminpb.Vtctld
		repeat                bool
	}{
		{
			name:                  "multiple clusters without dynamic clusters",
			enableDynamicClusters: false,
			clusters: []*cluster.Cluster{
				{
					ID:        "c1",
					Name:      "cluster1",
					Discovery: fakediscovery.New(),
				},
				{
					ID:        "c2",
					Name:      "cluster2",
					Discovery: fakediscovery.New(),
				},
			},
			expected: []*vtadminpb.Cluster{
				{
					Id:   "c1",
					Name: "cluster1",
				},
				{
					Id:   "c2",
					Name: "cluster2",
				},
			},
		},
		{
			name:                  "no clusters without dynamic clusters",
			enableDynamicClusters: false,
			clusters:              []*cluster.Cluster{},
			expected:              []*vtadminpb.Cluster{},
		},
		{
			name:                  "multiple clusters with dynamic clusters",
			enableDynamicClusters: true,
			cookie:                `{"id": "dynamiccluster1", "name": "dynamiccluster1", "discovery": "dynamic", "discovery-dynamic-discovery": "{\"vtctlds\": [{\"host\":{\"fqdn\": \"localhost:15000\", \"hostname\": \"localhost:15999\"}}], \"vtgates\": [{\"host\": {\"hostname\": \"localhost:15991\"}}]}"}`,
			clusters: []*cluster.Cluster{
				{
					ID:        "c1",
					Name:      "cluster1",
					Discovery: fakediscovery.New(),
				},
				{
					ID:        "c2",
					Name:      "cluster2",
					Discovery: fakediscovery.New(),
				},
			},
			expected: []*vtadminpb.Cluster{
				{
					Id:   "dynamiccluster1",
					Name: "dynamiccluster1",
				},
			},
		},
		{
			name:                  "dynamic clusters - cluster is updated when values change",
			enableDynamicClusters: true,
			cookie:                `{"id": "dynamiccluster1", "name": "dynamiccluster1", "discovery": "dynamic", "discovery-dynamic-discovery": "{\"vtctlds\": [{\"host\":{\"fqdn\": \"localhost:15001\", \"hostname\": \"localhost:15998\"}}], \"vtgates\": [{\"host\": {\"hostname\": \"localhost:15991\"}}]}"}`,
			clusters: []*cluster.Cluster{
				testCluster,
			},
			expected: []*vtadminpb.Cluster{
				{
					Id:   "dynamiccluster1",
					Name: "dynamiccluster1",
				},
			},
			testClusterVtctld: "dynamiccluster1",
			expectedVtctlds: []*vtadminpb.Vtctld{
				{
					Hostname: "localhost:15998",
					Cluster:  &vtadminpb.Cluster{Id: "dynamiccluster1", Name: "dynamiccluster1"},
					FQDN:     "localhost:15001",
				},
			},
		},
		{
			name:                  "multiple clusters with dynamic clusters - no duplicates",
			enableDynamicClusters: true,
			cookie:                `{"id": "dynamiccluster1", "name": "dynamiccluster1", "discovery": "dynamic", "discovery-dynamic-discovery": "{\"vtctlds\": [{\"host\":{\"fqdn\": \"localhost:15000\", \"hostname\": \"localhost:15999\"}}], \"vtgates\": [{\"host\": {\"hostname\": \"localhost:15991\"}}]}"}`,
			clusters: []*cluster.Cluster{
				{
					ID:        "c1",
					Name:      "cluster1",
					Discovery: fakediscovery.New(),
				},
				{
					ID:        "c2",
					Name:      "cluster2",
					Discovery: fakediscovery.New(),
				},
			},
			expected: []*vtadminpb.Cluster{
				{
					Id:   "dynamiccluster1",
					Name: "dynamiccluster1",
				},
			},
			repeat: true,
		},
		{
			name:                  "multiple clusters with invalid json cookie and dynamic clusters",
			enableDynamicClusters: true,
			cookie:                `{"id "dynamiccluster1", "name": "dynamiccluster1", "discovery": "dynamic", "discovery-dynamic-discovery": "{\"vtctlds\": [{\"host\":{\"fqdn\": \"localhost:15000\", \"hostname\": \"localhost:15999\"}}], \"vtgates\": [{\"host\": {\"hostname\": \"localhost:15991\"}}]}"}`,
			clusters: []*cluster.Cluster{
				{
					ID:        "c1",
					Name:      "cluster1",
					Discovery: fakediscovery.New(),
				},
				{
					ID:        "c2",
					Name:      "cluster2",
					Discovery: fakediscovery.New(),
				},
			},
			expected: []*vtadminpb.Cluster{
				{
					Id:   "c1",
					Name: "cluster1",
				},
				{
					Id:   "c2",
					Name: "cluster2",
				},
			},
		},
		{
			name:                  "no clusters with dynamic clusters",
			enableDynamicClusters: true,
			clusters:              []*cluster.Cluster{},
			expected:              []*vtadminpb.Cluster{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			api := NewAPI(vtenv.NewTestEnv(), tt.clusters, Options{EnableDynamicClusters: tt.enableDynamicClusters})

			// Copy the Cookie over to a new Request
			req := httptest.NewRequest(http.MethodGet, "/api/clusters", nil)
			req.AddCookie(&http.Cookie{Name: "cluster", Value: url.QueryEscape(base64.StdEncoding.EncodeToString([]byte(tt.cookie)))})

			w := httptest.NewRecorder()

			api.ServeHTTP(w, req)

			if tt.repeat {
				api.ServeHTTP(w, req)
			}

			res := w.Result()
			defer res.Body.Close()

			dec := json.NewDecoder(res.Body)
			dec.DisallowUnknownFields()
			var clustersResponse ServeHTTPResponse
			err := dec.Decode(&clustersResponse)

			assert.NoError(t, err)
			assert.ElementsMatch(t, tt.expected, clustersResponse.Result.Clusters)

			if tt.testClusterVtctld != "" {
				req := httptest.NewRequest(http.MethodGet, "/api/vtctlds?cluster="+tt.testClusterVtctld, nil)
				req.AddCookie(&http.Cookie{Name: "cluster", Value: url.QueryEscape(base64.StdEncoding.EncodeToString([]byte(tt.cookie)))})

				w := httptest.NewRecorder()

				api.ServeHTTP(w, req)

				if tt.repeat {
					api.ServeHTTP(w, req)
				}

				res := w.Result()
				defer res.Body.Close()

				dec := json.NewDecoder(res.Body)
				dec.DisallowUnknownFields()
				var vtctldsResponse ServeHTTPVtctldResponse
				err := dec.Decode(&vtctldsResponse)

				assert.NoError(t, err)
				assert.ElementsMatch(t, tt.expectedVtctlds, vtctldsResponse.Result.Vtctlds)
			}
		})
	}
}

func init() {
	// For tests that don't actually care about mocking the tmclient (i.e. they
	// call grpcvtctldserver.NewVtctldServer to initialize the unit under test),
	// this needs to be set.
	//
	// Tests that do care about the tmclient should use
	// testutil.NewVtctldServerWithTabletManagerClient to initialize their
	// VtctldServer.
	tmclienttest.SetProtocol("go.vt.vtadmin", "vtadmin.test")
	tmclient.RegisterTabletManagerClientFactory("vtadmin.test", func() tmclient.TabletManagerClient {
		return nil
	})
}

//go:generate -command authztestgen go run ./testutil/authztestgen
//go:generate authztestgen -c ./testutil/authztestgen/config.json -o ./api_authz_test.go
