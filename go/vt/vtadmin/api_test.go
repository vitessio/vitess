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
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vitessdriver"
	"vitess.io/vitess/go/vt/vtadmin/cluster"
	"vitess.io/vitess/go/vt/vtadmin/cluster/discovery/fakediscovery"
	"vitess.io/vitess/go/vt/vtadmin/grpcserver"
	"vitess.io/vitess/go/vt/vtadmin/http"
	vtadmintestutil "vitess.io/vitess/go/vt/vtadmin/testutil"
	vtadminvtctldclient "vitess.io/vitess/go/vt/vtadmin/vtctldclient"
	"vitess.io/vitess/go/vt/vtadmin/vtsql"
	"vitess.io/vitess/go/vt/vtadmin/vtsql/fakevtsql"
	"vitess.io/vitess/go/vt/vtctl/grpcvtctldserver"
	"vitess.io/vitess/go/vt/vtctl/grpcvtctldserver/testutil"
	"vitess.io/vitess/go/vt/vtctl/vtctldclient"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtctlservicepb "vitess.io/vitess/go/vt/proto/vtctlservice"
	"vitess.io/vitess/go/vt/proto/vttime"
)

func TestGetClusters(t *testing.T) {
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

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			api := NewAPI(tt.clusters, grpcserver.Options{}, http.Options{})
			ctx := context.Background()

			resp, err := api.GetClusters(ctx, &vtadminpb.GetClustersRequest{})
			assert.NoError(t, err)
			assert.ElementsMatch(t, tt.expected, resp.Clusters)
		})
	}
}

func TestGetGates(t *testing.T) {
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

	api := NewAPI([]*cluster.Cluster{cluster1, cluster2}, grpcserver.Options{}, http.Options{})
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

func TestGetKeyspaces(t *testing.T) {
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
				//cluster0
				{
					{
						Name:     "c0-ks0",
						Keyspace: &topodatapb.Keyspace{},
					},
				},
				//cluster1
				{
					{
						Name:     "c1-ks0",
						Keyspace: &topodatapb.Keyspace{},
					},
				},
			},
			clusterShards: [][]*vtctldatapb.Shard{
				//cluster0
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
				//cluster1
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
									IsMasterServing: true,
								},
							},
							"80-": {
								Keyspace: "c0-ks0",
								Name:     "80-",
								Shard: &topodatapb.Shard{
									IsMasterServing: true,
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
									IsMasterServing: true,
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
				//cluster0
				{
					{
						Name:     "c0-ks0",
						Keyspace: &topodatapb.Keyspace{},
					},
				},
				//cluster1
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
		topos := []*topo.Server{
			memorytopo.NewServer("c0_cell1"),
			memorytopo.NewServer("c1_cell1"),
		}

		for cdx, cks := range tt.clusterKeyspaces {
			for _, ks := range cks {
				testutil.AddKeyspace(context.Background(), t, topos[cdx], ks)
			}
		}

		for cdx, css := range tt.clusterShards {
			testutil.AddShards(context.Background(), t, topos[cdx], css...)
		}

		// Setting up WithTestServer in a generic, recursive way is... unpleasant,
		// so all tests are set-up and run in the context of these two clusters.
		testutil.WithTestServer(t, grpcvtctldserver.NewVtctldServer(topos[0]), func(t *testing.T, cluster0Client vtctldclient.VtctldClient) {
			testutil.WithTestServer(t, grpcvtctldserver.NewVtctldServer(topos[1]), func(t *testing.T, cluster1Client vtctldclient.VtctldClient) {
				clusterClients := []vtctldclient.VtctldClient{cluster0Client, cluster1Client}

				clusters := []*cluster.Cluster{
					buildCluster(0, clusterClients[0], nil, nil),
					buildCluster(1, clusterClients[1], nil, nil),
				}

				api := NewAPI(clusters, grpcserver.Options{}, http.Options{})
				resp, err := api.GetKeyspaces(context.Background(), tt.req)
				require.NoError(t, err)

				vtadmintestutil.AssertKeyspaceSlicesEqual(t, tt.expected.Keyspaces, resp.Keyspaces)
			})
		})
	}
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
				Schemas: []*vtadminpb.Schema{},
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
				Schemas: []*vtadminpb.Schema{},
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
				Schemas: []*vtadminpb.Schema{},
			},
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			topos := []*topo.Server{
				memorytopo.NewServer("c0_cell1"),
				memorytopo.NewServer("c1_cell1"),
			}

			tmc := testutil.TabletManagerClient{
				GetSchemaResults: map[string]struct {
					Schema *tabletmanagerdatapb.SchemaDefinition
					Error  error
				}{},
			}

			vtctlds := []vtctlservicepb.VtctldServer{
				testutil.NewVtctldServerWithTabletManagerClient(t, topos[0], &tmc, func(ts *topo.Server) vtctlservicepb.VtctldServer {
					return grpcvtctldserver.NewVtctldServer(ts)
				}),
				testutil.NewVtctldServerWithTabletManagerClient(t, topos[1], &tmc, func(ts *topo.Server) vtctlservicepb.VtctldServer {
					return grpcvtctldserver.NewVtctldServer(ts)
				}),
			}

			// Setting up WithTestServer in a generic, recursive way is... unpleasant,
			// so all tests are set-up and run in the context of these two clusters.
			testutil.WithTestServer(t, vtctlds[0], func(t *testing.T, cluster0Client vtctldclient.VtctldClient) {
				testutil.WithTestServer(t, vtctlds[1], func(t *testing.T, cluster1Client vtctldclient.VtctldClient) {
					// Put 'em in a slice so we can look them up by index
					clusterClients := []vtctldclient.VtctldClient{cluster0Client, cluster1Client}

					// Build the clusters
					clusters := make([]*cluster.Cluster, len(topos))
					for cdx, toposerver := range topos {
						// Handle when a test doesn't define any tablets for a given cluster.
						var cts []*vtadminpb.Tablet
						if cdx < len(tt.clusterTablets) {
							cts = tt.clusterTablets[cdx]
						}

						for _, tablet := range cts {
							// AddTablet also adds the keyspace + shard for us.
							testutil.AddTablet(context.Background(), t, toposerver, tablet.Tablet, nil)

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

						clusters[cdx] = buildCluster(cdx, clusterClients[cdx], cts, nil)
					}

					api := NewAPI(clusters, grpcserver.Options{}, http.Options{})

					resp, err := api.GetSchemas(context.Background(), tt.req)
					require.NoError(t, err)

					vtadmintestutil.AssertSchemaSlicesEqual(t, tt.expected.Schemas, resp.Schemas, tt.name)
				})
			})
		})
	}
}

func TestGetTablets(t *testing.T) {
	tests := []struct {
		name           string
		clusterTablets [][]*vtadminpb.Tablet
		dbconfigs      map[string]*dbcfg
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
							Type:     topodatapb.TabletType_MASTER,
						},
					},
				},
			},
			dbconfigs: map[string]*dbcfg{},
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
						Type:     topodatapb.TabletType_MASTER,
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
							Type:     topodatapb.TabletType_MASTER,
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
							Type:     topodatapb.TabletType_MASTER,
						},
					},
				},
			},
			dbconfigs: map[string]*dbcfg{
				"c1": {shouldErr: true},
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
							Type:     topodatapb.TabletType_MASTER,
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
							Type:     topodatapb.TabletType_MASTER,
						},
					},
				},
			},
			dbconfigs: map[string]*dbcfg{},
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
						Type:     topodatapb.TabletType_MASTER,
					},
				},
			},
			shouldErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clusters := make([]*cluster.Cluster, len(tt.clusterTablets))

			for i, tablets := range tt.clusterTablets {
				cluster := buildCluster(i, nil, tablets, tt.dbconfigs)
				clusters[i] = cluster
			}

			api := NewAPI(clusters, grpcserver.Options{}, http.Options{})
			resp, err := api.GetTablets(context.Background(), tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.ElementsMatch(t, tt.expected, resp.Tablets)
		})
	}
}

// This test only validates the error handling on dialing database connections.
// Other cases are covered by one or both of TestGetTablets and TestGetTablet.
func Test_getTablets(t *testing.T) {
	api := &API{}
	disco := fakediscovery.New()
	disco.AddTaggedGates(nil, &vtadminpb.VTGate{Hostname: "gate"})

	db := vtsql.New(&vtsql.Config{
		Cluster: &vtadminpb.Cluster{
			Id:   "c1",
			Name: "one",
		},
		Discovery: disco,
	})
	db.DialFunc = func(cfg vitessdriver.Configuration) (*sql.DB, error) {
		return nil, assert.AnError
	}

	_, err := api.getTablets(context.Background(), &cluster.Cluster{
		DB: db,
	})
	assert.Error(t, err)
}

func TestGetTablet(t *testing.T) {
	tests := []struct {
		name           string
		clusterTablets [][]*vtadminpb.Tablet
		dbconfigs      map[string]*dbcfg
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
							Type:     topodatapb.TabletType_MASTER,
						},
					},
				},
			},
			dbconfigs: map[string]*dbcfg{},
			req: &vtadminpb.GetTabletRequest{
				Hostname: "ks1-00-00-zone1-a",
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
					Type:     topodatapb.TabletType_MASTER,
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
							Type:     topodatapb.TabletType_MASTER,
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
							Type:     topodatapb.TabletType_MASTER,
						},
					},
				},
			},
			dbconfigs: map[string]*dbcfg{
				"c1": {shouldErr: true},
			},
			req: &vtadminpb.GetTabletRequest{
				Hostname: "doesn't matter",
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
							Type:     topodatapb.TabletType_MASTER,
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
							Type:     topodatapb.TabletType_MASTER,
						},
					},
				},
			},
			dbconfigs: map[string]*dbcfg{},
			req: &vtadminpb.GetTabletRequest{
				Hostname:   "ks1-00-00-zone1-a",
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
					Type:     topodatapb.TabletType_MASTER,
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
							Type:     topodatapb.TabletType_MASTER,
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
							Hostname: "ks1-00-00-zone1-a",
							Keyspace: "ks1",
							Shard:    "-",
							Type:     topodatapb.TabletType_MASTER,
						},
					},
				},
			},
			dbconfigs: map[string]*dbcfg{},
			req: &vtadminpb.GetTabletRequest{
				Hostname: "ks1-00-00-zone1-a",
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
			dbconfigs: map[string]*dbcfg{},
			req: &vtadminpb.GetTabletRequest{
				Hostname: "ks1-00-00-zone1-a",
			},
			expected:  nil,
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clusters := make([]*cluster.Cluster, len(tt.clusterTablets))

			for i, tablets := range tt.clusterTablets {
				cluster := buildCluster(i, nil, tablets, tt.dbconfigs)
				clusters[i] = cluster
			}

			api := NewAPI(clusters, grpcserver.Options{}, http.Options{})
			resp, err := api.GetTablet(context.Background(), tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.expected, resp)
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
						Type:     topodatapb.TabletType_MASTER,
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
			expectedError: ErrNoTablet,
		},
		{
			name: "returns an error if cluster unspecified in request",
			req: &vtadminpb.VTExplainRequest{
				Keyspace: "commerce",
				Sql:      "select * from customers",
			},
			expectedError: ErrInvalidRequest,
		},
		{
			name: "returns an error if keyspace unspecified in request",
			req: &vtadminpb.VTExplainRequest{
				Cluster: "c0",
				Sql:     "select * from customers",
			},
			expectedError: ErrInvalidRequest,
		},
		{
			name: "returns an error if SQL unspecified in request",
			req: &vtadminpb.VTExplainRequest{
				Cluster:  "c0",
				Keyspace: "commerce",
			},
			expectedError: ErrInvalidRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			toposerver := memorytopo.NewServer("c0_cell1")

			tmc := testutil.TabletManagerClient{
				GetSchemaResults: map[string]struct {
					Schema *tabletmanagerdatapb.SchemaDefinition
					Error  error
				}{},
			}

			vtctldserver := testutil.NewVtctldServerWithTabletManagerClient(t, toposerver, &tmc, func(ts *topo.Server) vtctlservicepb.VtctldServer {
				return grpcvtctldserver.NewVtctldServer(ts)
			})

			testutil.WithTestServer(t, vtctldserver, func(t *testing.T, vtctldClient vtctldclient.VtctldClient) {
				if tt.srvVSchema != nil {
					err := toposerver.UpdateSrvVSchema(context.Background(), "c0_cell1", tt.srvVSchema)
					require.NoError(t, err)
				}
				testutil.AddKeyspaces(context.Background(), t, toposerver, tt.keyspaces...)
				testutil.AddShards(context.Background(), t, toposerver, tt.shards...)

				for _, tablet := range tt.tablets {
					testutil.AddTablet(context.Background(), t, toposerver, tablet.Tablet, nil)

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

				c := buildCluster(0, vtctldClient, tt.tablets, nil)
				clusters := []*cluster.Cluster{c}

				api := NewAPI(clusters, grpcserver.Options{}, http.Options{})
				resp, err := api.VTExplain(context.Background(), tt.req)

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

type dbcfg struct {
	shouldErr bool
}

// shared helper for building a cluster that contains the given tablets and
// talking to the given vtctld server. dbconfigs contains an optional config
// for controlling the behavior of the cluster's DB at the package sql level.
func buildCluster(i int, vtctldClient vtctldclient.VtctldClient, tablets []*vtadminpb.Tablet, dbconfigs map[string]*dbcfg) *cluster.Cluster {
	disco := fakediscovery.New()
	disco.AddTaggedGates(nil, &vtadminpb.VTGate{Hostname: fmt.Sprintf("cluster%d-gate", i)})
	disco.AddTaggedVtctlds(nil, &vtadminpb.Vtctld{Hostname: "doesn't matter"})

	cluster := &cluster.Cluster{
		ID:        fmt.Sprintf("c%d", i),
		Name:      fmt.Sprintf("cluster%d", i),
		Discovery: disco,
	}

	dbconfig, ok := dbconfigs[cluster.ID]
	if !ok {
		dbconfig = &dbcfg{shouldErr: false}
	}

	db := vtsql.New(&vtsql.Config{
		Cluster:   cluster.ToProto(),
		Discovery: disco,
	})
	db.DialFunc = func(cfg vitessdriver.Configuration) (*sql.DB, error) {
		return sql.OpenDB(&fakevtsql.Connector{Tablets: tablets, ShouldErr: dbconfig.shouldErr}), nil
	}

	vtctld := vtadminvtctldclient.New(&vtadminvtctldclient.Config{
		Cluster:   cluster.ToProto(),
		Discovery: disco,
	})
	vtctld.DialFunc = func(addr string, ff grpcclient.FailFast, opts ...grpc.DialOption) (vtctldclient.VtctldClient, error) {
		return vtctldClient, nil
	}

	cluster.DB = db
	cluster.Vtctld = vtctld

	return cluster
}

func init() {
	// For tests that don't actually care about mocking the tmclient (i.e. they
	// call grpcvtctldserver.NewVtctldServer to initialize the unit under test),
	// this needs to be set.
	//
	// Tests that do care about the tmclient should use
	// testutil.NewVtctldServerWithTabletManagerClient to initialize their
	// VtctldServer.
	*tmclient.TabletManagerProtocol = "vtadmin.test"
	tmclient.RegisterTabletManagerClientFactory("vtadmin.test", func() tmclient.TabletManagerClient {
		return nil
	})
}
