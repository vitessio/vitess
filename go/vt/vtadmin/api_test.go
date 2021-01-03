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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"vitess.io/vitess/go/vt/vitessdriver"
	"vitess.io/vitess/go/vt/vtadmin/cluster"
	"vitess.io/vitess/go/vt/vtadmin/cluster/discovery/fakediscovery"
	"vitess.io/vitess/go/vt/vtadmin/grpcserver"
	"vitess.io/vitess/go/vt/vtadmin/http"
	"vitess.io/vitess/go/vt/vtadmin/vtsql"
	"vitess.io/vitess/go/vt/vtadmin/vtsql/fakevtsql"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

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

	api := NewAPI([]*cluster.Cluster{cluster1, cluster2}, grpcserver.Options{}, http.Options{})
	ctx := context.Background()

	resp, err := api.GetGates(ctx, &vtadminpb.GetGatesRequest{})
	assert.NoError(t, err)
	assert.ElementsMatch(t, append(cluster1Gates, cluster2Gates...), resp.Gates)

	resp, err = api.GetGates(ctx, &vtadminpb.GetGatesRequest{ClusterIds: []string{cluster1.ID}})
	assert.NoError(t, err)
	assert.ElementsMatch(t, cluster1Gates, resp.Gates)

	fakedisco1.SetGatesError(true)

	resp, err = api.GetGates(ctx, &vtadminpb.GetGatesRequest{})
	assert.Error(t, err)
	assert.Nil(t, resp)
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
				cluster := buildCluster(i, tablets, tt.dbconfigs)
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
				cluster := buildCluster(i, tablets, tt.dbconfigs)
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

type dbcfg struct {
	shouldErr bool
}

// shared helper for building a cluster that contains the given tablets.
// dbconfigs contains an optional config for controlling the behavior of the
// cluster's DB at the package sql level.
func buildCluster(i int, tablets []*vtadminpb.Tablet, dbconfigs map[string]*dbcfg) *cluster.Cluster {
	disco := fakediscovery.New()
	disco.AddTaggedGates(nil, &vtadminpb.VTGate{Hostname: fmt.Sprintf("cluster%d-gate", i)})

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

	cluster.DB = db

	return cluster
}
