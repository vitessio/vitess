/*
Copyright 2025 The Vitess Authors.

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
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// TestGetTabletsAndMapByShardCell tests GetTabletMapForShardByCell and GetTabletsByShardCell calls.
func TestGetTabletsAndMapByShardCell(t *testing.T) {
	tests := []struct {
		name     string
		keyspace string
		shard    string
		cells    []string
		want     map[string]*topo.TabletInfo
	}{
		{
			name:     "no cells provided",
			keyspace: kss[0],
			shard:    shards[1],
			want: map[string]*topo.TabletInfo{
				"zone1-0000000002": {
					Tablet: tablets[1],
				},
				"zone2-0000000006": {
					Tablet: tablets[5],
				},
			},
		},
		{
			name:     "multiple cells",
			keyspace: kss[0],
			shard:    shards[1],
			cells:    cells,
			want: map[string]*topo.TabletInfo{
				"zone1-0000000002": {
					Tablet: tablets[1],
				},
				"zone2-0000000006": {
					Tablet: tablets[5],
				},
			},
		},
		{
			name:     "only one cell",
			keyspace: kss[0],
			shard:    shards[1],
			cells:    []string{cells[0]},
			want: map[string]*topo.TabletInfo{
				"zone1-0000000002": {
					Tablet: tablets[1],
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			ts := memorytopo.NewServer(ctx, cells...)
			defer ts.Close()
			// This creates a tablet in each cell, keyspace, and shard, totalling 8 tablets.
			setupFunc(t, ctx, ts)

			tabletMap, err := ts.GetTabletMapForShardByCell(ctx, tt.keyspace, tt.shard, tt.cells)
			require.NoError(t, err)
			checkTabletMapEqual(t, tt.want, tabletMap)

			tabletList, err := ts.GetTabletsByShardCell(ctx, tt.keyspace, tt.shard, tt.cells)
			require.NoError(t, err)
			checkTabletListEqual(t, maps.Values(tt.want), tabletList)
		})
	}
}

// TestGetTabletMapForShardWithCellTimeout checks that a cell whose topology server does not
// answer costs at most the cell timeout, and that the tablets of the other cells are returned.
func TestGetTabletMapForShardWithCellTimeout(t *testing.T) {
	ctx := t.Context()
	seeded, factory := memorytopo.NewServerAndFactory(ctx, "zone1", "zone2")
	t.Cleanup(seeded.Close)
	require.NoError(t, seeded.CreateKeyspace(ctx, "ks", &topodatapb.Keyspace{}))
	require.NoError(t, seeded.CreateShard(ctx, "ks", "0"))
	for _, alias := range []*topodatapb.TabletAlias{{Cell: "zone1", Uid: 1}, {Cell: "zone2", Uid: 2}} {
		require.NoError(t, seeded.CreateTablet(ctx, &topodatapb.Tablet{Alias: alias, Keyspace: "ks", Shard: "0", Hostname: "host"}))
	}
	// zone2's topology server is cut off.
	require.NoError(t, seeded.UpdateCellInfoFields(ctx, "zone2", func(ci *topodatapb.CellInfo) error {
		ci.ServerAddress = memorytopo.UnreachableServerAddr
		return nil
	}))
	ts, err := topo.NewWithFactory(factory, "", "")
	require.NoError(t, err)
	t.Cleanup(ts.Close)

	readCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	start := time.Now()
	tablets, err := ts.GetTabletMapForShardWithCellTimeout(readCtx, "ks", "0", 100*time.Millisecond)
	assert.Less(t, time.Since(start), 10*time.Second, "the unreachable cell must cost at most the cell timeout")
	require.True(t, topo.IsErrType(err, topo.PartialResult), "got %v", err)
	assert.Equal(t, []string{"zone1-0000000001"}, maps.Keys(tablets))

	// The cells that did not answer are named, so that a tablet of a cut-off cell can be told
	// from a tablet that does not exist.
	tablets, failed, err := ts.GetTabletMapAndFailedCellsForShard(readCtx, "ks", "0", 100*time.Millisecond)
	require.True(t, topo.IsErrType(err, topo.PartialResult), "got %v", err)
	assert.Equal(t, []string{"zone1-0000000001"}, maps.Keys(tablets))
	assert.Equal(t, []string{"zone2"}, failed)

	// A shard that does not exist is an error, not a partial result.
	_, err = ts.GetTabletMapForShardWithCellTimeout(readCtx, "ks", "-80", 100*time.Millisecond)
	require.True(t, topo.IsErrType(err, topo.NoNode), "got %v", err)
}
