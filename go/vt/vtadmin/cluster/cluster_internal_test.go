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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/sets"

	"vitess.io/vitess/go/pools"
	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vitessdriver"
	"vitess.io/vitess/go/vt/vtadmin/cluster/resolver"
	"vitess.io/vitess/go/vt/vtadmin/vtctldclient/fakevtctldclient"
	"vitess.io/vitess/go/vt/vtadmin/vtsql"
	"vitess.io/vitess/go/vt/vtadmin/vtsql/fakevtsql"
	"vitess.io/vitess/go/vt/vtctl/vtctldclient"

	logutilpb "vitess.io/vitess/go/vt/proto/logutil"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

type vtctldProxy struct {
	vtctldclient.VtctldClient
	dialErr error
}

func (fake *vtctldProxy) Dial(ctx context.Context) error { return fake.dialErr }

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
