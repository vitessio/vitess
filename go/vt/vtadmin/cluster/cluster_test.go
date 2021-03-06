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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vitessdriver"
	"vitess.io/vitess/go/vt/vtadmin/cluster"
	"vitess.io/vitess/go/vt/vtadmin/cluster/discovery/fakediscovery"
	vtadminerrors "vitess.io/vitess/go/vt/vtadmin/errors"
	"vitess.io/vitess/go/vt/vtadmin/testutil"
	"vitess.io/vitess/go/vt/vtadmin/vtsql"
	"vitess.io/vitess/go/vt/vtctl/vtctldclient"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
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
			vtctld: &testutil.VtctldClient{
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
			},
			shouldErr: false,
		},
		{
			name: "error getting schema",
			vtctld: &testutil.VtctldClient{
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
			vtctld: &testutil.VtctldClient{
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
				Tablet: &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
					Keyspace: "testkeyspace",
				},
			},
			expected:  nil,
			shouldErr: false,
		},
	}

	for i, tt := range tests {
		i := i
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cluster := testutil.BuildCluster(testutil.TestClusterConfig{
				Cluster: &vtadminpb.Cluster{
					Id:   fmt.Sprintf("c%d", i),
					Name: fmt.Sprintf("cluster%d", i),
				},
				VtctldClient: tt.vtctld,
				Tablets:      nil,
				DBConfig:     testutil.Dbcfg{},
			})

			ctx := context.Background()
			err := cluster.Vtctld.Dial(ctx)
			require.NoError(t, err, "could not dial test vtctld")

			schema, err := cluster.GetSchema(ctx, tt.req, tt.tablet)
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

		vtctld := &testutil.VtctldClient{
			GetSchemaResults: map[string]struct {
				Response *vtctldatapb.GetSchemaResponse
				Error    error
			}{
				"zone1-0000000100": {
					Response: &vtctldatapb.GetSchemaResponse{},
				},
			},
		}

		ctx := context.Background()
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

		cluster := testutil.BuildCluster(testutil.TestClusterConfig{
			Cluster: &vtadminpb.Cluster{
				Id:   "c0",
				Name: "cluster0",
			},
			VtctldClient: vtctld,
		})

		err := cluster.Vtctld.Dial(ctx)
		require.NoError(t, err, "could not dial test vtctld")

		cluster.GetSchema(ctx, req, tablet)

		assert.NotEqual(t, req.TabletAlias, tablet.Tablet.Alias, "expected GetSchema to not modify original request object")
	})
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
				VtctldClient: &testutil.VtctldClient{
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
				VtctldClient: &testutil.VtctldClient{
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

			cluster := testutil.BuildCluster(tt.cfg)
			err := cluster.Vtctld.Dial(ctx)
			require.NoError(t, err, "could not dial test vtctld")

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
			cluster := testutil.BuildCluster(testutil.TestClusterConfig{
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
			cluster := testutil.BuildCluster(testutil.TestClusterConfig{
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
				testutil.AssertTabletsEqual(t, tt.expected, tablet)
			}
		})
	}
}
