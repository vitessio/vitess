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

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
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
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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
