package vtctld

import (
	"reflect"
	"testing"

	"github.com/youtube/vitess/go/vt/discovery"
	"github.com/youtube/vitess/go/vt/proto/topodata"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

func TestStatsUpdate(t *testing.T) {
	tabletStatsCache := newTabletStatsCache()

	// Creating some tablets with their latest health information.
	tablet1Stats1 := tabletStats("cell1", "ks1", "-80", topodatapb.TabletType_REPLICA, 200)
	tablet1Stats2 := tabletStats("cell1", "ks1", "-80", topodatapb.TabletType_REPLICA, 200)
	tablet2Stats1 := tabletStats("cell1", "ks1", "-80", topodatapb.TabletType_REPLICA, 100)

	// Insert tablet1.
	tabletStatsCache.StatsUpdate(tablet1Stats1)
	results1 := tabletStatsCache.statuses["ks1"]["-80"]["cell1"][topodatapb.TabletType_REPLICA]
	if got, want := results1[0], tablet1Stats1; !reflect.DeepEqual(got, want) {
		t.Errorf("got: %v, want: %v", got, want)
	}

	// Update tablet1.
	tabletStatsCache.StatsUpdate(tablet1Stats2)
	results2 := tabletStatsCache.statuses["ks1"]["-80"]["cell1"][topodatapb.TabletType_REPLICA]
	if got, want := results2[0], tablet1Stats2; !reflect.DeepEqual(got, want) {
		t.Errorf("got: %v, want: %v", got, want)
	}

	// Insert tablet. List of tablets will be resorted.
	tabletStatsCache.StatsUpdate(tablet2Stats1)
	results3 := tabletStatsCache.statuses["ks1"]["-80"]["cell1"][topodatapb.TabletType_REPLICA]
	if got, want := results3[0], tablet2Stats1; !reflect.DeepEqual(got, want) {
		t.Errorf("got: %v, want: %v", got, want)
	}

	results4 := tabletStatsCache.statuses["ks1"]["-80"]["cell1"][topodatapb.TabletType_REPLICA]
	if got, want := results4[1], tablet1Stats2; !reflect.DeepEqual(got, want) {
		t.Errorf("got: %v, want: %v", got, want)
	}

	// Check tablet count in cell1.
	if got, want := tabletStatsCache.tabletCountsByCell["cell1"], 2; got != want {
		t.Errorf("got: %v, want: %v", got, want)
	}

	// Delete tablet2.
	tablet2Stats1.Up = false
	tabletStatsCache.StatsUpdate(tablet2Stats1)
	results5 := tabletStatsCache.statuses["ks1"]["-80"]["cell1"][topodatapb.TabletType_REPLICA]
	for _, stat := range results5 {
		if reflect.DeepEqual(stat, tablet2Stats1) {
			t.Errorf("not deleleted from statusesByAliases")
		}
	}

	_, ok := tabletStatsCache.statusesByAlias[tablet2Stats1.Tablet.Alias.String()]
	if ok {
		t.Errorf("not deleted from statusesByAliases")
	}

	// Check tablet count in cell1.
	if got, want := tabletStatsCache.tabletCountsByCell["cell1"], 1; got != want {
		t.Errorf("got: %v, want: %v", got, want)
	}

	// Delete tablet1. List of known cells should be empty now.
	tablet1Stats2.Up = false
	tabletStatsCache.StatsUpdate(tablet1Stats2)
	_, ok = tabletStatsCache.tabletCountsByCell["cell1"]
	if ok {
		t.Errorf("not deleted from cells")
	}
}

func TestHeatmapData(t *testing.T) {
	// Creating and Sending updates to 12 tablets.
	ts1 := tabletStats("cell1", "ks1", "-80", topodatapb.TabletType_MASTER, 100)
	ts2 := tabletStats("cell1", "ks1", "-80", topodatapb.TabletType_REPLICA, 200)
	ts3 := tabletStats("cell1", "ks1", "-80", topodatapb.TabletType_REPLICA, 300)
	ts4 := tabletStats("cell1", "ks1", "-80", topodatapb.TabletType_RDONLY, 400)
	ts5 := tabletStats("cell2", "ks1", "-80", topodatapb.TabletType_MASTER, 500)
	ts6 := tabletStats("cell2", "ks1", "-80", topodatapb.TabletType_REPLICA, 600)
	ts7 := tabletStats("cell1", "ks1", "80-", topodatapb.TabletType_MASTER, 700)
	ts8 := tabletStats("cell1", "ks1", "80-", topodatapb.TabletType_REPLICA, 800)
	ts9 := tabletStats("cell1", "ks1", "80-", topodatapb.TabletType_RDONLY, 900)
	ts10 := tabletStats("cell1", "ks1", "80-", topodatapb.TabletType_RDONLY, 1000)
	ts11 := tabletStats("cell2", "ks1", "80-", topodatapb.TabletType_MASTER, 1100)
	ts12 := tabletStats("cell2", "ks1", "80-", topodatapb.TabletType_RDONLY, 1200)

	tabletStatsCache := newTabletStatsCache()

	tabletStatsCache.StatsUpdate(ts1)
	tabletStatsCache.StatsUpdate(ts2)
	tabletStatsCache.StatsUpdate(ts3)
	tabletStatsCache.StatsUpdate(ts4)
	tabletStatsCache.StatsUpdate(ts5)
	tabletStatsCache.StatsUpdate(ts6)
	tabletStatsCache.StatsUpdate(ts7)
	tabletStatsCache.StatsUpdate(ts8)
	tabletStatsCache.StatsUpdate(ts9)
	tabletStatsCache.StatsUpdate(ts10)
	tabletStatsCache.StatsUpdate(ts11)
	tabletStatsCache.StatsUpdate(ts12)

	// Checking that the heatmap data is returned correctly for the following view: (keyspace="ks1", cell=all, type=all).
	heatmap, err := tabletStatsCache.heatmapData("ks1", "all", "all", "lag")
	gotData, gotTabletAliases, gotLabels := heatmap.Data, heatmap.Aliases, heatmap.Labels
	wantData := [][]float64{
		{float64(-1), float64(ts12.Stats.SecondsBehindMaster)},
		{float64(ts6.Stats.SecondsBehindMaster), float64(-1)},
		{float64(ts5.Stats.SecondsBehindMaster), float64(ts11.Stats.SecondsBehindMaster)},
		{float64(-1), float64(ts10.Stats.SecondsBehindMaster)},
		{float64(ts4.Stats.SecondsBehindMaster), float64(ts9.Stats.SecondsBehindMaster)},
		{float64(ts3.Stats.SecondsBehindMaster), float64(-1)},
		{float64(ts2.Stats.SecondsBehindMaster), float64(ts8.Stats.SecondsBehindMaster)},
		{float64(ts1.Stats.SecondsBehindMaster), float64(ts7.Stats.SecondsBehindMaster)},
	}
	wantTabletAliases := [][]*topodata.TabletAlias{
		{nil, ts12.Tablet.Alias},
		{ts6.Tablet.Alias, nil},
		{ts5.Tablet.Alias, ts11.Tablet.Alias},
		{nil, ts10.Tablet.Alias},
		{ts4.Tablet.Alias, ts9.Tablet.Alias},
		{ts3.Tablet.Alias, nil},
		{ts2.Tablet.Alias, ts8.Tablet.Alias},
		{ts1.Tablet.Alias, ts7.Tablet.Alias},
	}
	wantLabels := []yLabel{
		{
			Label: label{Name: "cell1", Rowspan: 5},
			NestedLabels: []label{
				{Name: topodatapb.TabletType_MASTER.String(), Rowspan: 1},
				{Name: topodatapb.TabletType_REPLICA.String(), Rowspan: 2},
				{Name: topodatapb.TabletType_RDONLY.String(), Rowspan: 2},
			},
		},
		{
			Label: label{Name: "cell2", Rowspan: 3},
			NestedLabels: []label{
				{Name: topodatapb.TabletType_MASTER.String(), Rowspan: 1},
				{Name: topodatapb.TabletType_REPLICA.String(), Rowspan: 1},
				{Name: topodatapb.TabletType_RDONLY.String(), Rowspan: 1},
			},
		},
	}

	if err != nil {
		t.Errorf("couldn't get heatmap data: %v", err)
	}

	if !reflect.DeepEqual(gotData, wantData) {
		t.Errorf("got: %v, want: %v", gotData, wantData)
	}

	if !reflect.DeepEqual(gotTabletAliases, wantTabletAliases) {
		t.Errorf("got: %v, want: %v", gotTabletAliases, wantTabletAliases)
	}

	if !reflect.DeepEqual(gotLabels, wantLabels) {
		t.Errorf("got: %v, want: %v", gotLabels, wantLabels)
	}

	// Checking that the heatmap data is returned correctly for the following view: (keyspace="ks1", cell="cell1", type="all").
	heatmap2, err := tabletStatsCache.heatmapData("ks1", "cell1", "all", "lag")
	gotData2, gotTabletAliases2, gotLabels2 := heatmap2.Data, heatmap2.Aliases, heatmap2.Labels
	wantData2 := [][]float64{
		{float64(-1), float64(ts10.Stats.SecondsBehindMaster)},
		{float64(ts4.Stats.SecondsBehindMaster), float64(ts9.Stats.SecondsBehindMaster)},
		{float64(ts3.Stats.SecondsBehindMaster), float64(-1)},
		{float64(ts2.Stats.SecondsBehindMaster), float64(ts8.Stats.SecondsBehindMaster)},
		{float64(ts1.Stats.SecondsBehindMaster), float64(ts7.Stats.SecondsBehindMaster)},
	}
	wantTabletAliases2 := [][]*topodata.TabletAlias{
		{nil, ts10.Tablet.Alias},
		{ts4.Tablet.Alias, ts9.Tablet.Alias},
		{ts3.Tablet.Alias, nil},
		{ts2.Tablet.Alias, ts8.Tablet.Alias},
		{ts1.Tablet.Alias, ts7.Tablet.Alias},
	}
	wantLabels2 := []yLabel{
		{
			Label: label{Name: topodatapb.TabletType_MASTER.String(), Rowspan: 1},
		},
		{
			Label: label{Name: topodatapb.TabletType_REPLICA.String(), Rowspan: 2},
		},
		{
			Label: label{Name: topodatapb.TabletType_RDONLY.String(), Rowspan: 2},
		},
	}

	if err != nil {
		t.Errorf("couldn't get heatmap data: %v", err)
	}

	if !reflect.DeepEqual(gotData2, wantData2) {
		t.Errorf("got: %v, want: %v", gotData2, wantData2)
	}

	if !reflect.DeepEqual(gotTabletAliases2, wantTabletAliases2) {
		t.Errorf("got: %v, want: %v", gotTabletAliases2, wantTabletAliases2)
	}

	if !reflect.DeepEqual(gotLabels2, wantLabels2) {
		t.Errorf("got: %v, want: %v", gotLabels2, wantLabels2)
	}

	// Checking that the heatmap data is returned correctly for the following view: (keyspace="ks1", cell="cell1", type="REPLICA").
	heatmap3, err := tabletStatsCache.heatmapData("ks1", "cell1", topodata.TabletType_REPLICA.String(), "lag")
	gotData3, gotTabletAliases3, gotLabels3 := heatmap3.Data, heatmap3.Aliases, heatmap3.Labels
	wantData3 := [][]float64{
		{float64(ts3.Stats.SecondsBehindMaster), float64(-1)},
		{float64(ts2.Stats.SecondsBehindMaster), float64(ts8.Stats.SecondsBehindMaster)},
	}
	wantTabletAliases3 := [][]*topodata.TabletAlias{
		{ts3.Tablet.Alias, nil},
		{ts2.Tablet.Alias, ts8.Tablet.Alias},
	}
	var wantLabels3 []yLabel

	if err != nil {
		t.Errorf("couldn't get heatmap data: %v", err)
	}

	if !reflect.DeepEqual(gotData3, wantData3) {
		t.Errorf("got: %v, want: %v", gotData3, wantData3)
	}

	if !reflect.DeepEqual(gotTabletAliases3, wantTabletAliases3) {
		t.Errorf("got: %v, want: %v", gotTabletAliases3, wantTabletAliases3)
	}

	if !reflect.DeepEqual(gotLabels3, wantLabels3) {
		t.Errorf("got: %v, want: %v", gotLabels3, wantLabels3)
	}

	// Checking that the heatmap data is returned correctly for the following view: (keyspace="all", cell="cell1", type="all").
	heatmap4, err := tabletStatsCache.heatmapData("all", "cell1", "all", "lag")
	gotData4, gotTabletAliases4, gotLabels4 := heatmap4.Data, heatmap4.Aliases, heatmap4.Labels
	wantData4 := [][]float64{
		{float64(-1), float64(ts10.Stats.SecondsBehindMaster)},
		{float64(ts4.Stats.SecondsBehindMaster), float64(ts9.Stats.SecondsBehindMaster)},
		{float64(ts3.Stats.SecondsBehindMaster), float64(-1)},
		{float64(ts2.Stats.SecondsBehindMaster), float64(ts8.Stats.SecondsBehindMaster)},
		{float64(ts1.Stats.SecondsBehindMaster), float64(ts7.Stats.SecondsBehindMaster)},
	}
	wantTabletAliases4 := [][]*topodata.TabletAlias{
		{nil, ts10.Tablet.Alias},
		{ts4.Tablet.Alias, ts9.Tablet.Alias},
		{ts3.Tablet.Alias, nil},
		{ts2.Tablet.Alias, ts8.Tablet.Alias},
		{ts1.Tablet.Alias, ts7.Tablet.Alias},
	}
	wantLabels4 := []yLabel{
		{
			Label: label{Name: "ks1", Rowspan: 5},
			NestedLabels: []label{
				{Name: topodatapb.TabletType_MASTER.String(), Rowspan: 1},
				{Name: topodatapb.TabletType_REPLICA.String(), Rowspan: 2},
				{Name: topodatapb.TabletType_RDONLY.String(), Rowspan: 2},
			},
		},
	}

	if err != nil {
		t.Errorf("couldn't get heatmap data: %v", err)
	}

	if !reflect.DeepEqual(gotData4, wantData4) {
		t.Errorf("got: %v, want: %v", gotData4, wantData4)
	}

	if !reflect.DeepEqual(gotTabletAliases4, wantTabletAliases4) {
		t.Errorf("got: %v, want: %v", gotTabletAliases4, wantTabletAliases4)
	}

	if !reflect.DeepEqual(gotLabels4, wantLabels4) {
		t.Errorf("got: %v, want: %v", gotLabels4, wantLabels4)
	}
}

func TestAggregatedHeatmapData(t *testing.T) {
	ts1 := tabletStats("cell1", "ks1", "-80", topodatapb.TabletType_MASTER, 200)
	ts2 := tabletStats("cell1", "ks1", "-80", topodatapb.TabletType_REPLICA, 100)
	ts3 := tabletStats("cell2", "ks1", "80-", topodatapb.TabletType_REPLICA, 400)
	ts4 := tabletStats("cell2", "ks1", "80-", topodatapb.TabletType_MASTER, 600)
	ts5 := tabletStats("cell1", "ks2", "-80", topodatapb.TabletType_REPLICA, 700)
	ts6 := tabletStats("cell1", "ks2", "-80", topodatapb.TabletType_REPLICA, 300)
	ts7 := tabletStats("cell2", "ks2", "80-", topodatapb.TabletType_REPLICA, 800)
	ts8 := tabletStats("cell2", "ks2", "80-", topodatapb.TabletType_REPLICA, 500)

	tabletStatsCache := newTabletStatsCache()
	tabletStatsCache.StatsUpdate(ts1)
	tabletStatsCache.StatsUpdate(ts2)
	tabletStatsCache.StatsUpdate(ts3)
	tabletStatsCache.StatsUpdate(ts4)
	tabletStatsCache.StatsUpdate(ts5)
	tabletStatsCache.StatsUpdate(ts6)
	tabletStatsCache.StatsUpdate(ts7)
	tabletStatsCache.StatsUpdate(ts8)

	// aggregatedHeatmapData should return the average of the replication lag of the tablets in all cells for a keyspace.
	got, err := tabletStatsCache.aggregatedHeatmapData("lag")
	if err != nil {
		t.Errorf("could not get aggregated heatmap data: %v", err)
	}
	want := heatmap{
		Data: [][]float64{
			{-1, 650},
			{500, -1},
			{-1, 500},
			{150, -1},
		},
		Aliases: nil,
		Labels: []yLabel{
			{
				Label: label{Name: "ks1", Rowspan: 2},
			},
			{
				Label: label{Name: "ks2", Rowspan: 2},
			},
		},
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("got: %v, want: %v", got, want)
	}

	// aggregatedHeatmapData should return the average of the healthy metric of the tablets in all cells for a keyspace.
	ts2.Stats.HealthError = "Unhealthy"
	ts3.Stats.SecondsBehindMaster = 75
	ts4.Stats.SecondsBehindMaster = 10
	ts5.Serving = false
	ts6.Serving = false
	tabletStatsCache.StatsUpdate(ts2)
	tabletStatsCache.StatsUpdate(ts4)
	tabletStatsCache.StatsUpdate(ts6)
	tabletStatsCache.StatsUpdate(ts8)
	got2, err := tabletStatsCache.aggregatedHeatmapData("healthy")
	if err != nil {
		t.Errorf("could not get aggregated heatmap data: %v", err)
	}
	want2 := heatmap{
		Data: [][]float64{
			{0, 3},
			{3, 0},
			{0, 1.5},
			{3, 0},
		},
		Aliases: nil,
		Labels: []yLabel{
			{
				Label: label{Name: "ks1", Rowspan: 2},
			},
			{
				Label: label{Name: "ks2", Rowspan: 2},
			},
		},
	}

	if !reflect.DeepEqual(got2, want2) {
		t.Errorf("got: %v, want: %v", got2, want2)
	}
}

func TestTabletStats(t *testing.T) {
	// Creating tabletStats.
	ts1 := tabletStats("cell1", "ks1", "-80", topodatapb.TabletType_MASTER, 200)
	ts2 := tabletStats("cell1", "ks1", "-80", topodatapb.TabletType_REPLICA, 100)
	ts3 := tabletStats("cell1", "ks1", "-80", topodatapb.TabletType_REPLICA, 300)

	tabletStatsCache := newTabletStatsCache()
	tabletStatsCache.StatsUpdate(ts1)
	tabletStatsCache.StatsUpdate(ts2)

	// Test 1: tablet1 and tablet2 are updated with the stats received by the HealthCheck module.
	got1, err := tabletStatsCache.tabletStats(ts1.Tablet.Alias)
	want1 := *ts1
	if err != nil || !reflect.DeepEqual(got1, want1) {
		t.Errorf("got: %v, want: %v", got1, want1)
	}

	got2, err := tabletStatsCache.tabletStats(ts2.Tablet.Alias)
	want2 := *ts2
	if err != nil || !reflect.DeepEqual(got2, want2) {
		t.Errorf("got: %v, want: %v", got2, want2)
	}

	// Test 2: tablet3 isn't found in the map since no update was received for it.
	stats, got3 := tabletStatsCache.tabletStats(ts3.Tablet.Alias)
	want3 := "could not find tablet: cell:\"cell1\" uid:300 "
	emptyStat := discovery.TabletStats{}
	if !reflect.DeepEqual(stats, emptyStat) || got3.Error() != want3 {
		t.Errorf("got: %v, want: %v", got3.Error(), want3)
	}
}

// tabletStats will create a discovery.TabletStats object.
func tabletStats(cell, keyspace, shard string, tabletType topodatapb.TabletType, uid uint32) *discovery.TabletStats {
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
