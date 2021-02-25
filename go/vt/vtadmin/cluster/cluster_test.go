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
	"database/sql"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/vitessdriver"
	"vitess.io/vitess/go/vt/vtadmin/cluster"
	"vitess.io/vitess/go/vt/vtadmin/cluster/discovery/fakediscovery"
	vtadminerrors "vitess.io/vitess/go/vt/vtadmin/errors"
	"vitess.io/vitess/go/vt/vtadmin/testutil"
	"vitess.io/vitess/go/vt/vtadmin/vtsql"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vtadmin"
	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

// This test only validates the error handling on dialing database connections.
// Other cases are covered by one or both of TestFindTablets and TestFindTablet.
func TestGetTablets(t *testing.T) {
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

	c := &cluster.Cluster{DB: db}
	_, err := c.GetTablets(context.Background())
	assert.Error(t, err)
}

func TestFindTablets(t *testing.T) {
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
					Cluster: &vtadmin.Cluster{
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
					Cluster: &vtadmin.Cluster{
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
					Cluster: &vtadmin.Cluster{
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
					Cluster: &vtadmin.Cluster{
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
					Cluster: &vtadmin.Cluster{
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

	for _, tt := range tests {
		cluster := testutil.BuildCluster(0, nil, tt.tablets, nil)
		tablets, err := cluster.FindTablets(context.Background(), tt.filter, tt.n)

		assert.NoError(t, err)
		testutil.AssertTabletSlicesEqual(t, tt.expected, tablets)
	}
}

func TestFindTablet(t *testing.T) {
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
				Cluster: &vtadmin.Cluster{
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

	for _, tt := range tests {
		cluster := testutil.BuildCluster(0, nil, tt.tablets, nil)
		tablet, err := cluster.FindTablet(context.Background(), tt.filter)

		if tt.expectedError != nil {
			assert.True(t, errors.Is(err, tt.expectedError), "expected error type %w does not match actual error type %w", err, tt.expectedError)
		} else {
			assert.NoError(t, err)
			testutil.AssertTabletsEqual(t, tt.expected, tablet)
		}
	}
}
