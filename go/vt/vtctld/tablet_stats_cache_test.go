/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vtctld

import (
	"context"
	"reflect"
	"testing"

	"vitess.io/vitess/go/vt/discovery"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestStatsUpdate(t *testing.T) {
	tabletStatsCache := newTabletStatsCache()

	// Creating some tablets with their latest health information.
	tablet1Stats1 := tabletStats("ks1", "cell1", "-80", topodatapb.TabletType_REPLICA, 200)
	tablet1Stats2 := tabletStats("ks1", "cell1", "-80", topodatapb.TabletType_REPLICA, 200)
	tablet2Stats1 := tabletStats("ks1", "cell1", "-80", topodatapb.TabletType_REPLICA, 100)

	ctx := context.Background()

	// Insert tablet1.
	tabletStatsCache.StatsUpdate(ctx, tablet1Stats1)
	results1 := tabletStatsCache.statuses["ks1"]["-80"]["cell1"][topodatapb.TabletType_REPLICA]
	if got, want := results1[0], tablet1Stats1; !got.DeepEqual(want) {
		t.Errorf("got: %v, want: %v", got, want)
	}

	// Update tablet1.
	tabletStatsCache.StatsUpdate(ctx, tablet1Stats2)
	results2 := tabletStatsCache.statuses["ks1"]["-80"]["cell1"][topodatapb.TabletType_REPLICA]
	if got, want := results2[0], tablet1Stats2; !got.DeepEqual(want) {
		t.Errorf("got: %v, want: %v", got, want)
	}

	// Insert tablet. List of tablets will be resorted.
	tabletStatsCache.StatsUpdate(ctx, tablet2Stats1)
	results3 := tabletStatsCache.statuses["ks1"]["-80"]["cell1"][topodatapb.TabletType_REPLICA]
	if got, want := results3[0], tablet2Stats1; !got.DeepEqual(want) {
		t.Errorf("got: %v, want: %v", got, want)
	}

	results4 := tabletStatsCache.statuses["ks1"]["-80"]["cell1"][topodatapb.TabletType_REPLICA]
	if got, want := results4[1], tablet1Stats2; !got.DeepEqual(want) {
		t.Errorf("got: %v, want: %v", got, want)
	}

	// Delete tablet2.
	tablet2Stats1.Up = false
	tabletStatsCache.StatsUpdate(ctx, tablet2Stats1)
	results5 := tabletStatsCache.statuses["ks1"]["-80"]["cell1"][topodatapb.TabletType_REPLICA]
	for _, stat := range results5 {
		if stat.DeepEqual(tablet2Stats1) {
			t.Errorf("not deleleted from statusesByAliases")
		}
	}

	_, ok := tabletStatsCache.statusesByAlias[tablet2Stats1.Tablet.Alias.String()]
	if ok {
		t.Errorf("not deleted from statusesByAliases")
	}

	// Delete tablet1. List of known cells should be empty now.
	tablet1Stats2.Up = false
	tabletStatsCache.StatsUpdate(ctx, tablet1Stats2)
	if ok {
		t.Errorf("not deleted from cells")
	}
}

func TestHeatmapData(t *testing.T) {
	// Creating and Sending updates to 12 tablets.
	ts1 := tabletStats("ks1", "cell1", "-80", topodatapb.TabletType_MASTER, 100)
	ts2 := tabletStats("ks1", "cell1", "-80", topodatapb.TabletType_REPLICA, 200)
	ts3 := tabletStats("ks1", "cell1", "-80", topodatapb.TabletType_REPLICA, 300)
	ts4 := tabletStats("ks1", "cell1", "80-", topodatapb.TabletType_REPLICA, 400)

	ts5 := tabletStats("ks1", "cell2", "-80", topodatapb.TabletType_REPLICA, 500)
	ts6 := tabletStats("ks1", "cell2", "-80", topodatapb.TabletType_RDONLY, 600)
	ts7 := tabletStats("ks1", "cell2", "80-", topodatapb.TabletType_MASTER, 700)
	ts8 := tabletStats("ks1", "cell2", "80-", topodatapb.TabletType_RDONLY, 800)
	ts9 := tabletStats("ks1", "cell2", "80-", topodatapb.TabletType_RDONLY, 900)

	ts10 := tabletStats("ks2", "cell1", "-80", topodatapb.TabletType_MASTER, 1000)
	ts11 := tabletStats("ks2", "cell1", "-80", topodatapb.TabletType_RDONLY, 1100)
	ts12 := tabletStats("ks2", "cell1", "-80", topodatapb.TabletType_RDONLY, 1200)
	ts13 := tabletStats("ks2", "cell1", "80-", topodatapb.TabletType_RDONLY, 1300)

	ts14 := tabletStats("ks2", "cell2", "-80", topodatapb.TabletType_REPLICA, 1400)
	ts15 := tabletStats("ks2", "cell2", "-80", topodatapb.TabletType_RDONLY, 1500)
	ts16 := tabletStats("ks2", "cell2", "80-", topodatapb.TabletType_MASTER, 1600)
	ts17 := tabletStats("ks2", "cell2", "80-", topodatapb.TabletType_REPLICA, 1700)
	ts18 := tabletStats("ks2", "cell2", "80-", topodatapb.TabletType_REPLICA, 1800)

	tabletStatsCache := newTabletStatsCache()
	ctx := context.Background()
	tabletStatsCache.StatsUpdate(ctx, ts1)
	tabletStatsCache.StatsUpdate(ctx, ts2)
	tabletStatsCache.StatsUpdate(ctx, ts3)
	tabletStatsCache.StatsUpdate(ctx, ts4)
	tabletStatsCache.StatsUpdate(ctx, ts5)
	tabletStatsCache.StatsUpdate(ctx, ts6)
	tabletStatsCache.StatsUpdate(ctx, ts7)
	tabletStatsCache.StatsUpdate(ctx, ts8)
	tabletStatsCache.StatsUpdate(ctx, ts9)
	tabletStatsCache.StatsUpdate(ctx, ts10)
	tabletStatsCache.StatsUpdate(ctx, ts11)
	tabletStatsCache.StatsUpdate(ctx, ts12)
	tabletStatsCache.StatsUpdate(ctx, ts13)
	tabletStatsCache.StatsUpdate(ctx, ts14)
	tabletStatsCache.StatsUpdate(ctx, ts15)
	tabletStatsCache.StatsUpdate(ctx, ts16)
	tabletStatsCache.StatsUpdate(ctx, ts17)
	tabletStatsCache.StatsUpdate(ctx, ts18)

	// Checking that the heatmap data is returned correctly for the following view: (keyspace="ks1", cell=all, type="all").
	got, err := tabletStatsCache.heatmapData("ks1", "all", "all", "lag")
	if err != nil {
		t.Errorf("could not get heatmap data: %v", err)
	}
	want := []heatmap{
		{
			KeyspaceLabel: label{Name: "ks1", Rowspan: 7},
			Data: [][]float64{
				{float64(-1), float64(ts9.Stats.SecondsBehindMaster)},
				{float64(ts6.Stats.SecondsBehindMaster), float64(ts8.Stats.SecondsBehindMaster)},
				{float64(ts5.Stats.SecondsBehindMaster), float64(-1)},
				{float64(-1), float64(ts7.Stats.SecondsBehindMaster)},
				{float64(ts3.Stats.SecondsBehindMaster), float64(-1)},
				{float64(ts2.Stats.SecondsBehindMaster), float64(ts4.Stats.SecondsBehindMaster)},
				{float64(ts1.Stats.SecondsBehindMaster), float64(-1)},
			},
			Aliases: [][]*topodatapb.TabletAlias{
				{nil, ts9.Tablet.Alias},
				{ts6.Tablet.Alias, ts8.Tablet.Alias},
				{ts5.Tablet.Alias, nil},
				{nil, ts7.Tablet.Alias},
				{ts3.Tablet.Alias, nil},
				{ts2.Tablet.Alias, ts4.Tablet.Alias},
				{ts1.Tablet.Alias, nil},
			},
			CellAndTypeLabels: []yLabel{
				{
					CellLabel: label{Name: "cell1", Rowspan: 3},
					TypeLabels: []label{
						{Name: topodatapb.TabletType_MASTER.String(), Rowspan: 1},
						{Name: topodatapb.TabletType_REPLICA.String(), Rowspan: 2},
					},
				},
				{
					CellLabel: label{Name: "cell2", Rowspan: 4},
					TypeLabels: []label{
						{Name: topodatapb.TabletType_MASTER.String(), Rowspan: 1},
						{Name: topodatapb.TabletType_REPLICA.String(), Rowspan: 1},
						{Name: topodatapb.TabletType_RDONLY.String(), Rowspan: 2},
					},
				},
			},
			ShardLabels: []string{"-80", "80-"},
			YGridLines:  []float64{1.5, 2.5, 3.5, 5.5, 6.5},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got: %v, want: %v", got, want)
	}

	// Checking that the heatmap data is returned correctly for the following view: (keyspace="ks1", cell=all, type="REPLICA").
	got2, err := tabletStatsCache.heatmapData("ks1", "all", "REPLICA", "lag")
	if err != nil {
		t.Errorf("could not get heatmap data: %v", err)
	}
	want2 := []heatmap{
		{
			KeyspaceLabel: label{Name: "ks1", Rowspan: 3},
			Data: [][]float64{
				{float64(ts5.Stats.SecondsBehindMaster), float64(-1)},
				{float64(ts3.Stats.SecondsBehindMaster), float64(-1)},
				{float64(ts2.Stats.SecondsBehindMaster), float64(ts4.Stats.SecondsBehindMaster)},
			},
			Aliases: [][]*topodatapb.TabletAlias{
				{ts5.Tablet.Alias, nil},
				{ts3.Tablet.Alias, nil},
				{ts2.Tablet.Alias, ts4.Tablet.Alias},
			},
			CellAndTypeLabels: []yLabel{
				{
					CellLabel: label{Name: "cell1", Rowspan: 2},
					TypeLabels: []label{
						{Name: topodatapb.TabletType_REPLICA.String(), Rowspan: 2},
					},
				},
				{
					CellLabel: label{Name: "cell2", Rowspan: 1},
					TypeLabels: []label{
						{Name: topodatapb.TabletType_REPLICA.String(), Rowspan: 1},
					},
				},
			},
			ShardLabels: []string{"-80", "80-"},
			YGridLines:  []float64{0.5, 2.5},
		},
	}
	if !reflect.DeepEqual(got2, want2) {
		t.Errorf("got: %v, want: %v", got2, want2)
	}

	// Checking that the heatmap data is returned correctly for the following view: (keyspace="ks2", cell="cell1", type="all").
	got3, err := tabletStatsCache.heatmapData("ks2", "cell1", "all", "lag")
	if err != nil {
		t.Errorf("could not get heatmap data: %v", err)
	}
	want3 := []heatmap{
		{
			KeyspaceLabel: label{Name: "ks2", Rowspan: 3},
			Data: [][]float64{
				{float64(ts12.Stats.SecondsBehindMaster), float64(-1)},
				{float64(ts11.Stats.SecondsBehindMaster), float64(ts13.Stats.SecondsBehindMaster)},
				{float64(ts10.Stats.SecondsBehindMaster), float64(-1)},
			},
			Aliases: [][]*topodatapb.TabletAlias{
				{ts12.Tablet.Alias, nil},
				{ts11.Tablet.Alias, ts13.Tablet.Alias},
				{ts10.Tablet.Alias, nil},
			},
			CellAndTypeLabels: []yLabel{
				{
					CellLabel: label{Name: "cell1", Rowspan: 3},
					TypeLabels: []label{
						{Name: topodatapb.TabletType_MASTER.String(), Rowspan: 1},
						{Name: topodatapb.TabletType_RDONLY.String(), Rowspan: 2},
					},
				},
			},
			ShardLabels: []string{"-80", "80-"},
			YGridLines:  []float64{1.5, 2.5},
		},
	}
	if !reflect.DeepEqual(got3, want3) {
		t.Errorf("got: %v, want: %v", got3, want3)
	}

	// Checking that the heatmap data is returned correctly for the following view: (keyspace="all", cell="all", type="all").
	got4, err := tabletStatsCache.heatmapData("all", "all", "all", "lag")
	if err != nil {
		t.Errorf("could not get heatmap data: %v", err)
	}
	want4 := []heatmap{
		{
			KeyspaceLabel: label{Name: "ks1", Rowspan: 2},
			Data: [][]float64{
				{float64(500), float64(700)},
				{float64(200), float64(400)},
			},
			Aliases: nil,
			CellAndTypeLabels: []yLabel{
				{
					CellLabel: label{Name: "cell1", Rowspan: 1},
				},
				{
					CellLabel: label{Name: "cell2", Rowspan: 1},
				},
			},
			ShardLabels: []string{"-80", "80-"},
			YGridLines:  []float64{0.5, 1.5},
		},
		{
			KeyspaceLabel: label{Name: "ks2", Rowspan: 2},
			Data: [][]float64{
				{float64(1400), float64(1600)},
				{float64(1000), float64(1300)},
			},
			Aliases: nil,
			CellAndTypeLabels: []yLabel{
				{
					CellLabel: label{Name: "cell1", Rowspan: 1},
				},
				{
					CellLabel: label{Name: "cell2", Rowspan: 1},
				},
			},
			ShardLabels: []string{"-80", "80-"},
			YGridLines:  []float64{0.5, 1.5},
		},
	}
	if !reflect.DeepEqual(got4, want4) {
		t.Errorf("got: %v, want: %v", got4, want4)
	}

	// Checking that the heatmap data is returned correctly for the following view: (keyspace="ks1", cell="cell2", type="MASTER").
	got5, err := tabletStatsCache.heatmapData("ks1", "cell2", "MASTER", "lag")
	if err != nil {
		t.Errorf("could not get heatmap data: %v", err)
	}
	want5 := []heatmap{
		{
			KeyspaceLabel: label{Name: "ks1", Rowspan: 1},
			Data: [][]float64{
				{float64(-1), float64(ts7.Stats.SecondsBehindMaster)},
			},
			Aliases: [][]*topodatapb.TabletAlias{
				{nil, ts7.Tablet.Alias},
			},
			CellAndTypeLabels: []yLabel{
				{
					CellLabel: label{Name: "cell2", Rowspan: 1},
					TypeLabels: []label{
						{Name: topodatapb.TabletType_MASTER.String(), Rowspan: 1},
					},
				},
			},
			ShardLabels: []string{"-80", "80-"},
			YGridLines:  []float64{0.5},
		},
	}
	if !reflect.DeepEqual(got5, want5) {
		t.Errorf("got: %v, want: %v", got5, want5)
	}
}

func TestTabletStats(t *testing.T) {
	// Creating tabletStats.
	ts1 := tabletStats("ks1", "cell1", "-80", topodatapb.TabletType_MASTER, 200)
	ts2 := tabletStats("ks1", "cell1", "-80", topodatapb.TabletType_REPLICA, 100)
	ts3 := tabletStats("ks1", "cell1", "-80", topodatapb.TabletType_REPLICA, 300)
	ctx := context.Background()
	tabletStatsCache := newTabletStatsCache()
	tabletStatsCache.StatsUpdate(ctx, ts1)
	tabletStatsCache.StatsUpdate(ctx, ts2)

	// Test 1: tablet1 and tablet2 are updated with the stats received by the HealthCheck module.
	got1, err := tabletStatsCache.tabletStats(ts1.Tablet.Alias)
	want1 := ts1
	if err != nil || !got1.DeepEqual(want1) {
		t.Errorf("got: %v, want: %v", got1, want1)
	}

	got2, err := tabletStatsCache.tabletStats(ts2.Tablet.Alias)
	want2 := ts2
	if err != nil || !got2.DeepEqual(want2) {
		t.Errorf("got: %v, want: %v", got2, want2)
	}

	// Test 2: tablet3 isn't found in the map since no update was received for it.
	_, gotErr := tabletStatsCache.tabletStats(ts3.Tablet.Alias)
	wantErr := "could not find tablet: cell:\"cell1\" uid:300 "
	if gotErr.Error() != wantErr {
		t.Errorf("got: %v, want: %v", gotErr.Error(), wantErr)
	}
}

func TestTopologyInfo(t *testing.T) {
	ts1 := tabletStats("ks1", "cell1", "0", topodatapb.TabletType_MASTER, 100)
	ts2 := tabletStats("ks1", "cell1", "0", topodatapb.TabletType_REPLICA, 200)
	ts3 := tabletStats("ks1", "cell2", "0", topodatapb.TabletType_REPLICA, 300)
	ts4 := tabletStats("ks1", "cell2", "0", topodatapb.TabletType_RDONLY, 400)
	ts5 := tabletStats("ks1", "cell3", "0", topodatapb.TabletType_RDONLY, 500)
	ts6 := tabletStats("ks1", "cell3", "0", topodatapb.TabletType_RDONLY, 600)
	ts7 := tabletStats("ks2", "cell1", "0", topodatapb.TabletType_MASTER, 700)

	ctx := context.Background()
	tabletStatsCache := newTabletStatsCache()
	tabletStatsCache.StatsUpdate(ctx, ts1)
	tabletStatsCache.StatsUpdate(ctx, ts2)
	tabletStatsCache.StatsUpdate(ctx, ts3)
	tabletStatsCache.StatsUpdate(ctx, ts4)
	tabletStatsCache.StatsUpdate(ctx, ts5)
	tabletStatsCache.StatsUpdate(ctx, ts6)
	tabletStatsCache.StatsUpdate(ctx, ts7)

	var testcases = []struct {
		keyspace string
		cell     string
		want     *topologyInfo
	}{
		{"all", "all", &topologyInfo{
			Keyspaces:   []string{"ks1", "ks2"},
			Cells:       []string{"cell1", "cell2", "cell3"},
			TabletTypes: []string{topodatapb.TabletType_MASTER.String(), topodatapb.TabletType_REPLICA.String(), topodatapb.TabletType_RDONLY.String()},
		},
		},
		{"ks1", "all", &topologyInfo{
			Keyspaces:   []string{"ks1", "ks2"},
			Cells:       []string{"cell1", "cell2", "cell3"},
			TabletTypes: []string{topodatapb.TabletType_MASTER.String(), topodatapb.TabletType_REPLICA.String(), topodatapb.TabletType_RDONLY.String()},
		},
		},
		{"ks2", "all", &topologyInfo{
			Keyspaces:   []string{"ks1", "ks2"},
			Cells:       []string{"cell1"},
			TabletTypes: []string{topodatapb.TabletType_MASTER.String()},
		},
		},
		{"ks1", "cell2", &topologyInfo{
			Keyspaces:   []string{"ks1", "ks2"},
			Cells:       []string{"cell1", "cell2", "cell3"},
			TabletTypes: []string{topodatapb.TabletType_REPLICA.String(), topodatapb.TabletType_RDONLY.String()},
		},
		},
		{"all", "cell2", &topologyInfo{
			Keyspaces:   []string{"ks1", "ks2"},
			Cells:       []string{"cell1", "cell2", "cell3"},
			TabletTypes: []string{topodatapb.TabletType_REPLICA.String(), topodatapb.TabletType_RDONLY.String()},
		},
		},
	}

	for _, tc := range testcases {
		got := tabletStatsCache.topologyInfo(tc.keyspace, tc.cell)
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("got: %v, want: %v", got, tc.want)
		}
	}
}

// tabletStats will create a discovery.TabletStats object.
func tabletStats(keyspace, cell, shard string, tabletType topodatapb.TabletType, uid uint32) *discovery.TabletStats {
	target := &querypb.Target{
		Keyspace:   keyspace,
		Shard:      shard,
		TabletType: tabletType,
	}
	tablet := &topodatapb.Tablet{
		Alias:    &topodatapb.TabletAlias{Cell: cell, Uid: uid},
		Keyspace: keyspace,
		Shard:    shard,
		Type:     tabletType,
		PortMap:  map[string]int32{"vt": int32(uid)},
	}
	realtimeStats := &querypb.RealtimeStats{
		HealthError: "",
		// uid is used for SecondsBehindMaster to give it a unique value.
		SecondsBehindMaster: uid,
	}
	stats := &discovery.TabletStats{
		Tablet:    tablet,
		Target:    target,
		Up:        true,
		Serving:   true,
		Stats:     realtimeStats,
		LastError: nil,
	}
	return stats
}
