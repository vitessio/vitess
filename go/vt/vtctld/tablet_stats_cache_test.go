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
		{float64(ts1.Stats.SecondsBehindMaster), float64(ts7.Stats.SecondsBehindMaster)},
		{float64(ts2.Stats.SecondsBehindMaster), float64(ts8.Stats.SecondsBehindMaster)},
		{float64(ts3.Stats.SecondsBehindMaster), float64(-1)},
		{float64(ts4.Stats.SecondsBehindMaster), float64(ts9.Stats.SecondsBehindMaster)},
		{float64(-1), float64(ts10.Stats.SecondsBehindMaster)},
		{float64(ts5.Stats.SecondsBehindMaster), float64(ts11.Stats.SecondsBehindMaster)},
		{float64(ts6.Stats.SecondsBehindMaster), float64(-1)},
		{float64(-1), float64(ts12.Stats.SecondsBehindMaster)},
	}
	wantTabletAliases := [][]*topodata.TabletAlias{
		{ts1.Tablet.Alias, ts7.Tablet.Alias},
		{ts2.Tablet.Alias, ts8.Tablet.Alias},
		{ts3.Tablet.Alias, nil},
		{ts4.Tablet.Alias, ts9.Tablet.Alias},
		{nil, ts10.Tablet.Alias},
		{ts5.Tablet.Alias, ts11.Tablet.Alias},
		{ts6.Tablet.Alias, nil},
		{nil, ts12.Tablet.Alias},
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
		t.Errorf("got: %v, want: %v", gotData, wantData)
	}

	if !reflect.DeepEqual(gotLabels, wantLabels) {
		t.Errorf("got: %v, want: %v", gotLabels, wantLabels)
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
