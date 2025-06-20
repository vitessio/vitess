/*
Copyright 2023 The Vitess Authors.

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

package topo_test

import (
	"cmp"
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"
	"google.golang.org/protobuf/proto"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
)

var (
	cells   = []string{"zone1", "zone2"}
	kss     = []string{"ks1", "ks2"}
	shards  = []string{"-80", "80-"}
	tablets []*topodatapb.Tablet
)

func init() {
	uid := 1
	for _, cell := range cells {
		for _, ks := range kss {
			for _, shard := range shards {
				tablet := getTablet(ks, shard, cell, int32(uid))
				tablets = append(tablets, tablet)
				uid++
			}
		}
	}
}

// Test various cases of calls to GetTabletsByCell.
// GetTabletsByCell first tries to get all the tablets using List.
// If the response is too large, we will get an error, and fall back to one tablet at a time.
func TestServerGetTabletsByCell(t *testing.T) {
	const cell = "zone1"
	const keyspace = "keyspace"
	const shard = "shard"

	tests := []struct {
		name               string
		createShardTablets int
		expectedTablets    []*topodatapb.Tablet
		opt                *topo.GetTabletsByCellOptions
		listError          error
		keyspaceShards     []*topo.KeyspaceShard
	}{
		{
			name: "negative concurrency",
			keyspaceShards: []*topo.KeyspaceShard{
				{Keyspace: keyspace, Shard: shard},
			},
			createShardTablets: 1,
			expectedTablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: cell,
						Uid:  uint32(1),
					},
					Hostname: "host1",
					PortMap: map[string]int32{
						"vt": int32(1),
					},
					Keyspace: keyspace,
					Shard:    shard,
				},
			},
			// Ensure this doesn't panic.
		},
		{
			name: "single",
			keyspaceShards: []*topo.KeyspaceShard{
				{Keyspace: keyspace, Shard: shard},
			},
			createShardTablets: 1,
			expectedTablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: cell,
						Uid:  uint32(1),
					},
					Hostname: "host1",
					PortMap: map[string]int32{
						"vt": int32(1),
					},
					Keyspace: keyspace,
					Shard:    shard,
				},
			},
			// Make sure the defaults apply as expected.
			opt: nil,
		},
		{
			name: "multiple",
			keyspaceShards: []*topo.KeyspaceShard{
				{Keyspace: keyspace, Shard: shard},
			},
			// Should work with more than 1 tablet
			createShardTablets: 4,
			expectedTablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: cell,
						Uid:  uint32(1),
					},
					Hostname: "host1",
					PortMap: map[string]int32{
						"vt": int32(1),
					},
					Keyspace: keyspace,
					Shard:    shard,
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: cell,
						Uid:  uint32(2),
					},
					Hostname: "host1",
					PortMap: map[string]int32{
						"vt": int32(2),
					},
					Keyspace: keyspace,
					Shard:    shard,
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: cell,
						Uid:  uint32(3),
					},
					Hostname: "host1",
					PortMap: map[string]int32{
						"vt": int32(3),
					},
					Keyspace: keyspace,
					Shard:    shard,
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: cell,
						Uid:  uint32(4),
					},
					Hostname: "host1",
					PortMap: map[string]int32{
						"vt": int32(4),
					},
					Keyspace: keyspace,
					Shard:    shard,
				},
			},
		},
		{
			name: "multiple with list error",
			keyspaceShards: []*topo.KeyspaceShard{
				{Keyspace: keyspace, Shard: shard},
			},
			// Should work with more than 1 tablet when List returns an error
			createShardTablets: 4,
			expectedTablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: cell,
						Uid:  uint32(1),
					},
					Hostname: "host1",
					PortMap: map[string]int32{
						"vt": int32(1),
					},
					Keyspace: keyspace,
					Shard:    shard,
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: cell,
						Uid:  uint32(2),
					},
					Hostname: "host1",
					PortMap: map[string]int32{
						"vt": int32(2),
					},
					Keyspace: keyspace,
					Shard:    shard,
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: cell,
						Uid:  uint32(3),
					},
					Hostname: "host1",
					PortMap: map[string]int32{
						"vt": int32(3),
					},
					Keyspace: keyspace,
					Shard:    shard,
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: cell,
						Uid:  uint32(4),
					},
					Hostname: "host1",
					PortMap: map[string]int32{
						"vt": int32(4),
					},
					Keyspace: keyspace,
					Shard:    shard,
				},
			},
			listError: topo.NewError(topo.ResourceExhausted, ""),
		},
		{
			name: "filtered by keyspace and shard",
			keyspaceShards: []*topo.KeyspaceShard{
				{Keyspace: keyspace, Shard: shard},
				{Keyspace: "filtered", Shard: "-"},
			},
			// Should create 2 tablets in 2 different shards (4 total)
			// but only a single shard is returned
			createShardTablets: 2,
			expectedTablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: cell,
						Uid:  uint32(1),
					},
					Hostname: "host1",
					PortMap: map[string]int32{
						"vt": int32(1),
					},
					Keyspace: keyspace,
					Shard:    shard,
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: cell,
						Uid:  uint32(2),
					},
					Hostname: "host1",
					PortMap: map[string]int32{
						"vt": int32(2),
					},
					Keyspace: keyspace,
					Shard:    shard,
				},
			},
			opt: &topo.GetTabletsByCellOptions{
				KeyspaceShard: &topo.KeyspaceShard{
					Keyspace: keyspace,
					Shard:    shard,
				},
			},
		},
		{
			name: "filtered by keyspace and no shard",
			keyspaceShards: []*topo.KeyspaceShard{
				{Keyspace: keyspace, Shard: shard},
				{Keyspace: keyspace, Shard: shard + "2"},
			},
			// Should create 2 tablets in 2 different shards (4 total)
			// in the same keyspace and both shards are returned due to
			// empty shard
			createShardTablets: 2,
			expectedTablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: cell,
						Uid:  uint32(1),
					},
					Hostname: "host1",
					PortMap: map[string]int32{
						"vt": int32(1),
					},
					Keyspace: keyspace,
					Shard:    shard,
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: cell,
						Uid:  uint32(2),
					},
					Hostname: "host1",
					PortMap: map[string]int32{
						"vt": int32(2),
					},
					Keyspace: keyspace,
					Shard:    shard,
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: cell,
						Uid:  uint32(3),
					},
					Hostname: "host1",
					PortMap: map[string]int32{
						"vt": int32(3),
					},
					Keyspace: keyspace,
					Shard:    shard + "2",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: cell,
						Uid:  uint32(4),
					},
					Hostname: "host1",
					PortMap: map[string]int32{
						"vt": int32(4),
					},
					Keyspace: keyspace,
					Shard:    shard + "2",
				},
			},
			opt: &topo.GetTabletsByCellOptions{
				KeyspaceShard: &topo.KeyspaceShard{
					Keyspace: keyspace,
					Shard:    "",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			ts, factory := memorytopo.NewServerAndFactory(ctx, cell)
			defer ts.Close()
			if tt.listError != nil {
				factory.AddOperationError(memorytopo.List, ".*", tt.listError)
			}

			// Create an ephemeral keyspace and generate shard records within
			// the keyspace to fetch later.
			createdKeyspaces := make(map[string]bool, len(tt.keyspaceShards))
			for _, kss := range tt.keyspaceShards {
				if !createdKeyspaces[kss.Keyspace] {
					require.NoError(t, ts.CreateKeyspace(ctx, kss.Keyspace, &topodatapb.Keyspace{}))
					createdKeyspaces[kss.Keyspace] = true
				}
				require.NoError(t, ts.CreateShard(ctx, kss.Keyspace, kss.Shard, nil))
			}

			var uid uint32 = 1
			for _, kss := range tt.keyspaceShards {
				for i := 0; i < tt.createShardTablets; i++ {
					tablet := &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{
							Cell: cell,
							Uid:  uid,
						},
						Hostname: "host1",
						PortMap: map[string]int32{
							"vt": int32(uid),
						},
						Keyspace: kss.Keyspace,
						Shard:    kss.Shard,
					}
					require.NoError(t, ts.CreateTablet(ctx, tablet))
					uid++
				}
			}

			// Verify that we return a complete list of tablets and that each
			// tablet matches what we expect.
			out, err := ts.GetTabletsByCell(ctx, cell, tt.opt)
			require.NoError(t, err)
			require.Len(t, out, len(tt.expectedTablets))

			// We also check that the results for getting tablets individually
			// matches the output we get from listing them.
			out2, err := ts.GetTabletsIndividuallyByCell(ctx, cell, tt.opt)
			require.NoError(t, err)
			require.ElementsMatch(t, out, out2)

			slices.SortFunc(out, func(i, j *topo.TabletInfo) int {
				return cmp.Compare(i.Alias.Uid, j.Alias.Uid)
			})
			slices.SortFunc(tt.expectedTablets, func(i, j *topodatapb.Tablet) int {
				return cmp.Compare(i.Alias.Uid, j.Alias.Uid)
			})

			for i, tablet := range out {
				checkTabletsEqual(t, tt.expectedTablets[i], tablet.Tablet)
			}
		})
	}
}

func TestServerGetTabletsByCellPartialResults(t *testing.T) {
	const cell = "zone1"
	const keyspace = "keyspace"
	const shard = "shard"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ts, factory := memorytopo.NewServerAndFactory(ctx, cell)
	defer ts.Close()

	// Create an ephemeral keyspace and generate shard records within
	// the keyspace to fetch later.
	require.NoError(t, ts.CreateKeyspace(ctx, keyspace, &topodatapb.Keyspace{}))
	require.NoError(t, ts.CreateShard(ctx, keyspace, shard, nil))

	tablets := make([]*topo.TabletInfo, 3)

	for i := 0; i < len(tablets); i++ {
		tablet := &topodatapb.Tablet{
			Alias: &topodatapb.TabletAlias{
				Cell: cell,
				Uid:  uint32(i),
			},
			Hostname: "host1",
			PortMap: map[string]int32{
				"vt": int32(i),
			},
			Keyspace: keyspace,
			Shard:    shard,
		}
		tInfo := &topo.TabletInfo{Tablet: tablet}
		tablets[i] = tInfo
		require.NoError(t, ts.CreateTablet(ctx, tablet))
	}

	// Force fallback to getting tablets individually.
	factory.AddOperationError(memorytopo.List, ".*", topo.NewError(topo.NoImplementation, "List not supported"))

	// Cause the Get for the second tablet to fail.
	factory.AddOperationError(memorytopo.Get, "tablets/zone1-0000000001/Tablet", errors.New("fake error"))

	// Verify that we return a partial list of tablets and that each
	// tablet matches what we expect.
	out, err := ts.GetTabletsByCell(ctx, cell, nil)
	assert.Error(t, err)
	assert.True(t, topo.IsErrType(err, topo.PartialResult), "Not a partial result: %v", err)
	assert.Len(t, out, 2)
	assert.True(t, proto.Equal(tablets[0].Tablet, out[0].Tablet), "Got: %v, want %v", tablets[0].Tablet, out[0].Tablet)
	assert.True(t, proto.Equal(tablets[2].Tablet, out[1].Tablet), "Got: %v, want %v", tablets[2].Tablet, out[1].Tablet)
}

func getTablet(keyspace string, shard string, cell string, uid int32) *topodatapb.Tablet {
	return &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: cell,
			Uid:  uint32(uid),
		},
		Hostname: "host1",
		PortMap: map[string]int32{
			"vt": uid,
		},
		Keyspace: keyspace,
		Shard:    shard,
	}
}

func checkTabletsEqual(t *testing.T, expected, tablet *topodatapb.Tablet) {
	t.Helper()
	require.Equal(t, expected.Alias.String(), tablet.Alias.String())
	require.Equal(t, expected.Keyspace, tablet.Keyspace)
	require.Equal(t, expected.Shard, tablet.Shard)
}

func checkTabletMapEqual(t *testing.T, expected, tabletMap map[string]*topo.TabletInfo) {
	t.Helper()
	require.Len(t, tabletMap, len(expected))
	for key, tablet := range tabletMap {
		expectedTablet, ok := expected[key]
		require.True(t, ok, "unexpected tablet %v", key)
		checkTabletsEqual(t, expectedTablet.Tablet, tablet.Tablet)
	}
}

func checkTabletListEqual(t *testing.T, expected, tabletMap []*topo.TabletInfo) {
	t.Helper()
	require.Len(t, tabletMap, len(expected))
	for _, tablet := range tabletMap {
		found := false
		for _, expectedTablet := range expected {
			if topoproto.TabletAliasString(tablet.Alias) == topoproto.TabletAliasString(expectedTablet.Alias) {
				checkTabletsEqual(t, expectedTablet.Tablet, tablet.Tablet)
				found = true
				break
			}
		}
		require.True(t, found, "unexpected tablet %v", tablet)
	}
}

func setupFunc(t *testing.T, ctx context.Context, ts *topo.Server) {
	for _, ks := range kss {
		for _, shard := range shards {
			_, err := ts.GetOrCreateShard(ctx, ks, shard, nil)
			require.NoError(t, err)
		}
	}
	for _, tablet := range tablets {
		require.NoError(t, ts.CreateTablet(ctx, tablet))
	}
}

// TestServerGetTabletMapAndList tests the GetTabletMap and GetTabletList methods.
func TestServerGetTabletMapAndList(t *testing.T) {
	tests := []struct {
		name          string
		tabletAliases []*topodatapb.TabletAlias
		opt           *topo.GetTabletsByCellOptions
		want          map[string]*topo.TabletInfo
	}{
		{
			name: "single tablet without filtering - found",
			tabletAliases: []*topodatapb.TabletAlias{
				{
					Cell: cells[0],
					Uid:  2,
				},
			},
			opt: nil,
			want: map[string]*topo.TabletInfo{
				"zone1-0000000002": {
					Tablet: tablets[1],
				},
			},
		},
		{
			name: "single tablet without filtering - not found",
			tabletAliases: []*topodatapb.TabletAlias{
				{
					Cell: cells[0],
					Uid:  2050,
				},
			},
			opt:  nil,
			want: map[string]*topo.TabletInfo{},
		},
		{
			name: "multiple tablets without filtering",
			tabletAliases: []*topodatapb.TabletAlias{
				{
					Cell: cells[0],
					Uid:  2,
				},
				{
					Cell: cells[0],
					Uid:  3,
				},
				{
					Cell: cells[0],
					Uid:  4,
				},
				{
					Cell: cells[1],
					Uid:  5,
				},
				{
					Cell: cells[1],
					Uid:  205,
				},
			},
			opt: nil,
			want: map[string]*topo.TabletInfo{
				"zone1-0000000002": {
					Tablet: tablets[1],
				},
				"zone1-0000000003": {
					Tablet: tablets[2],
				},
				"zone1-0000000004": {
					Tablet: tablets[3],
				},
				"zone2-0000000005": {
					Tablet: tablets[4],
				},
			},
		},
		{
			name: "multiple tablets with filtering",
			tabletAliases: []*topodatapb.TabletAlias{
				{
					Cell: cells[0],
					Uid:  2,
				},
				{
					Cell: cells[0],
					Uid:  3,
				},
				{
					Cell: cells[0],
					Uid:  4,
				},
				{
					Cell: cells[1],
					Uid:  5,
				},
				{
					Cell: cells[1],
					Uid:  6,
				},
				{
					Cell: cells[1],
					Uid:  205,
				},
			},
			opt: &topo.GetTabletsByCellOptions{
				KeyspaceShard: &topo.KeyspaceShard{
					Keyspace: kss[0],
					Shard:    shards[1],
				},
			},
			want: map[string]*topo.TabletInfo{
				"zone1-0000000002": {
					Tablet: tablets[1],
				},
				"zone2-0000000006": {
					Tablet: tablets[5],
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			ts := memorytopo.NewServer(ctx, cells...)
			defer ts.Close()
			// This creates a tablet in each cell, keyspace, and shard, totalling 8 tablets.
			setupFunc(t, ctx, ts)

			tabletMap, err := ts.GetTabletMap(ctx, tt.tabletAliases, tt.opt)
			require.NoError(t, err)
			checkTabletMapEqual(t, tt.want, tabletMap)

			tabletList, err := ts.GetTabletList(ctx, tt.tabletAliases, tt.opt)
			require.NoError(t, err)
			checkTabletListEqual(t, maps.Values(tt.want), tabletList)
		})
	}
}

// TestGetTabletsIndividuallyByCell tests the GetTabletsIndividuallyByCell function.
func TestGetTabletsIndividuallyByCell(t *testing.T) {
	tests := []struct {
		name     string
		keyspace string
		shard    string
		cell     string
		want     []*topo.TabletInfo
	}{
		{
			name:     "cell with filtering",
			keyspace: kss[0],
			shard:    shards[1],
			cell:     cells[0],
			want: []*topo.TabletInfo{
				{
					Tablet: tablets[1],
				},
			},
		},
		{
			name: "cell without filtering",
			cell: cells[0],
			want: []*topo.TabletInfo{
				{
					Tablet: tablets[0],
				},
				{
					Tablet: tablets[1],
				},
				{
					Tablet: tablets[2],
				},
				{
					Tablet: tablets[3],
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			ts := memorytopo.NewServer(ctx, cells...)
			defer ts.Close()
			// This creates a tablet in each cell, keyspace, and shard, totalling 8 tablets.
			setupFunc(t, ctx, ts)

			tabletList, err := ts.GetTabletsIndividuallyByCell(ctx, tt.cell, &topo.GetTabletsByCellOptions{KeyspaceShard: &topo.KeyspaceShard{Keyspace: tt.keyspace, Shard: tt.shard}})
			require.NoError(t, err)
			checkTabletListEqual(t, tt.want, tabletList)

			if tt.keyspace != "" && tt.shard != "" {
				// We can also check that this result matches what we get from GetTabletsByShardCell.
				tl, err := ts.GetTabletsByShardCell(ctx, tt.keyspace, tt.shard, []string{tt.cell})
				require.NoError(t, err)
				checkTabletListEqual(t, tabletList, tl)
			}
		})
	}
}
