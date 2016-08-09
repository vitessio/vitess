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
	tabletStatsCache := tabletStatsCache{
		statuses:        make(map[string]map[string]map[string]map[topodatapb.TabletType][]*discovery.TabletStats),
		statusesByAlias: make(map[string]*discovery.TabletStats),
		cells:           make(map[string]bool),
	}

	// Creating some tablets with their latest health information
	tablet1Stats1 := tabletStats("cell1", "ks1", "-80", topodatapb.TabletType_REPLICA, 200)
	tablet1Stats2 := tabletStats("cell1", "ks1", "-80", topodatapb.TabletType_REPLICA, 200)
	tablet2Stats1 := tabletStats("cell1", "ks1", "-80", topodatapb.TabletType_REPLICA, 100)

	// Test 1: tablet1's stats should be updated with the one received by the HealthCheck object
	tabletStatsCache.StatsUpdate(tablet1Stats1)
	results1 := tabletStatsCache.statuses["ks1"]["-80"]["cell1"][topodatapb.TabletType_REPLICA]
	got1 := results1[0]
	want1 := tablet1Stats1
	if !reflect.DeepEqual(got1, want1) {
		t.Errorf("got: %v, want: %v", got1, want1)
	}

	// Test 2: tablet1's stats should be updated with the new one received by the HealthCheck object.
	tabletStatsCache.StatsUpdate(tablet1Stats2)
	results2 := tabletStatsCache.statuses["ks1"]["-80"]["cell1"][topodatapb.TabletType_REPLICA]
	got2 := results2[0]
	want2 := tablet1Stats2
	if !reflect.DeepEqual(got1, want1) {
		t.Errorf("got: %v, want: %v", got2, want2)
	}

	// Test 3: tablet2's stats should be updated with the one received by the HealthCheck object,
	// leaving tablet1's stats unchanged.
	tabletStatsCache.StatsUpdate(tablet2Stats1)
	results3 := tabletStatsCache.statuses["ks1"]["-80"]["cell1"][topodatapb.TabletType_REPLICA]
	got3 := results3[0]
	want3 := tablet2Stats1
	if !reflect.DeepEqual(got1, want1) {
		t.Errorf("got: %v, want: %v", got3, want3)
	}

	results4 := tabletStatsCache.statuses["ks1"]["-80"]["cell1"][topodatapb.TabletType_REPLICA]
	got4 := results4[1]
	want4 := tablet1Stats2
	if got4 != want4 {
		t.Errorf("got: %v, want: %v", got4, want4)
	}

	// Test 4: tablet2 should be removed from all lists upon receiving an update that
	// serving status has changed.
	tablet2Stats1.Up = false
	tabletStatsCache.StatsUpdate(tablet2Stats1)
	results5 := tabletStatsCache.statuses["ks1"]["-80"]["cell1"][topodatapb.TabletType_REPLICA]
	for _, stat := range results5 {
		if reflect.DeepEqual(stat, tablet2Stats1) {
			t.Errorf("not deleleted from statuses")
		}
	}

	_, ok := tabletStatsCache.statusesByAlias[tablet2Stats1.Tablet.Alias.String()]
	if ok {
		t.Errorf("Tablet not deleted from statusesByAliases")
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

	tabletStatsCache := tabletStatsCache{
		statuses:        make(map[string]map[string]map[string]map[topodatapb.TabletType][]*discovery.TabletStats),
		statusesByAlias: make(map[string]*discovery.TabletStats),
		cells:           make(map[string]bool),
	}
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
	gotData, gotTabletAliases, gotLabels := tabletStatsCache.heatmapData("ks1", "all", "all", "lag")
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
			Label: "cell1",
			NestedLabels: []string{"5", topodatapb.TabletType_MASTER.String(), "1", topodatapb.TabletType_REPLICA.String(),
				"2", topodatapb.TabletType_RDONLY.String(), "2"},
		},
		{
			Label:        "cell2",
			NestedLabels: []string{"3", topodatapb.TabletType_MASTER.String(), "1", topodatapb.TabletType_REPLICA.String(), "1", topodatapb.TabletType_RDONLY.String(), "1"},
		},
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

// tabletStats will constscovery.TabletStats object.
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
