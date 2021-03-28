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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/sets"

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

			c := testutil.BuildCluster(testutil.TestClusterConfig{
				Cluster: &vtadminpb.Cluster{
					Id:   fmt.Sprintf("c%d", i),
					Name: fmt.Sprintf("cluster%d", i),
				},
				VtctldClient: tt.vtctld,
				Tablets:      nil,
				DBConfig:     testutil.Dbcfg{},
			})

			err := c.Vtctld.Dial(ctx)
			require.NoError(t, err, "could not dial test vtctld")

			schema, err := c.GetSchema(ctx, "testkeyspace", cluster.GetSchemaOptions{
				Tablets:     []*vtadminpb.Tablet{tt.tablet},
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

		c := testutil.BuildCluster(testutil.TestClusterConfig{
			Cluster: &vtadminpb.Cluster{
				Id:   "c0",
				Name: "cluster0",
			},
			VtctldClient: vtctld,
		})

		err := c.Vtctld.Dial(ctx)
		require.NoError(t, err, "could not dial test vtctld")

		c.GetSchema(ctx, "testkeyspace", cluster.GetSchemaOptions{
			BaseRequest: req,
			Tablets:     []*vtadminpb.Tablet{tablet},
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
					VtctldClient: &testutil.VtctldClient{
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
												IsMasterServing: true,
											},
										},
										"80-": {
											Name: "80-",
											Shard: &topodatapb.Shard{
												IsMasterServing: true,
											},
										},
										"-": {
											Name: "-",
											Shard: &topodatapb.Shard{
												IsMasterServing: false,
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
					VtctldClient: &testutil.VtctldClient{
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
												IsMasterServing: true,
											},
										},
										"80-": {
											Name: "80-",
											Shard: &topodatapb.Shard{
												IsMasterServing: true,
											},
										},
										"-": {
											Name: "-",
											Shard: &topodatapb.Shard{
												IsMasterServing: false,
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
					VtctldClient: &testutil.VtctldClient{
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
												IsMasterServing: true,
											},
										},
										"80-": {
											Name: "80-",
											Shard: &topodatapb.Shard{
												IsMasterServing: true,
											},
										},
										"-": {
											Name: "-",
											Shard: &topodatapb.Shard{
												IsMasterServing: false,
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
					VtctldClient: &testutil.VtctldClient{
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
												IsMasterServing: true,
											},
										},
										"80-": {
											Name: "80-",
											Shard: &topodatapb.Shard{
												IsMasterServing: true,
											},
										},
										"-": {
											Name: "-",
											Shard: &topodatapb.Shard{
												IsMasterServing: false,
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
					VtctldClient: &testutil.VtctldClient{
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
		}

		for _, tt := range tests {
			tt := tt

			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				if tt.keyspace == "" {
					t.SkipNow()
				}

				c := testutil.BuildCluster(tt.cfg)
				schema, err := c.GetSchema(ctx, tt.keyspace, tt.opts)
				if tt.shouldErr {
					assert.Error(t, err)

					return
				}

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
				VtctldClient: &testutil.VtctldClient{
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
				VtctldClient: &testutil.VtctldClient{
					GetKeyspacesResults: struct {
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
				VtctldClient: &testutil.VtctldClient{
					GetKeyspacesResults: struct {
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
				VtctldClient: &testutil.VtctldClient{
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
				VtctldClient: &testutil.VtctldClient{
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
				VtctldClient: &testutil.VtctldClient{
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
				VtctldClient: &testutil.VtctldClient{
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
				VtctldClient: &testutil.VtctldClient{
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

			c := testutil.BuildCluster(tt.cfg)
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

// This test only validates the error handling on dialing database connections.
// Other cases are covered by one or both of TestFindTablets and TestFindTablet.
func TestGetTablets(t *testing.T) {
	t.Parallel()

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
				VtctldClient: &testutil.VtctldClient{
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
				VtctldClient: &testutil.VtctldClient{
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
				VtctldClient: &testutil.VtctldClient{
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
				VtctldClient: &testutil.VtctldClient{
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

			c := testutil.BuildCluster(tt.cfg)
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
				VtctldClient: &testutil.VtctldClient{
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
				VtctldClient: &testutil.VtctldClient{
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
				VtctldClient: &testutil.VtctldClient{
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

			c := testutil.BuildCluster(tt.cfg)
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
