package vtctld

import (
	"reflect"
	"testing"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/discovery"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/vt/zktopo/zktestserver"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

func TestStatsUpdate(t *testing.T) {

	ctx := context.Background()
	cells := []string{"cell1", "cell2"}
	ts := zktestserver.New(t, cells)

	// Populate topo.
	ts.CreateKeyspace(ctx, "ks1", &topodatapb.Keyspace{ShardingColumnName: "shardcol"})
	ts.Impl.CreateShard(ctx, "ks1", "-80", &topodatapb.Shard{
		Cells:    cells,
		KeyRange: &topodatapb.KeyRange{Start: nil, End: []byte{0x80}},
	})
	ts.Impl.CreateShard(ctx, "ks1", "80-", &topodatapb.Shard{
		Cells:    cells,
		KeyRange: &topodatapb.KeyRange{Start: []byte{0x80}, End: nil},
	})

	realtimeStats := newRealtimeStatsForTesting()

	tablet1Stats1 := createTabletStats("cell1", "ks1", "-80", topodatapb.TabletType_REPLICA.String(), 100)
	tablet1Stats2 := createTabletStats("cell1", "ks1", "-80", topodatapb.TabletType_REPLICA.String(), 100)
	tablet2Stats1 := createTabletStats("cell1", "ks1", "-80", topodatapb.TabletType_REPLICA.String(), 200)

	// Test 1: tablet1's stats should be updated with the one received by the HealthCheck object.
	realtimeStats.tabletStats.StatsUpdate(tablet1Stats1)
	results1 := realtimeStats.tabletStats.statuses["ks1"]["-80"]["cell1"][topodatapb.TabletType_REPLICA.String()]

	got1 := (results1[0]).String()
	want1 := tablet1Stats1.String()
	if got1 != want1 {
		t.Errorf("got: %v, want: %v", got1, want1)
	}

	// Test 2: tablet1's stats should be updated with the new one received by the HealthCheck object.
	realtimeStats.tabletStats.StatsUpdate(tablet1Stats2)
	results2 := realtimeStats.tabletStats.statuses["ks1"]["-80"]["cell1"][topodatapb.TabletType_REPLICA.String()]
	got2 := results2[0].String()
	want2 := tablet1Stats2.String()
	if got2 != want2 {
		t.Errorf("got: %v, want: %v", got2, want2)
	}

	// Test 3: tablet2's stats should be updated with the one received by the HealthCheck object,
	// leaving tablet1's stats unchanged.
	realtimeStats.tabletStats.StatsUpdate(tablet2Stats1)
	results3 := realtimeStats.tabletStats.statuses["ks1"]["-80"]["cell1"][topodatapb.TabletType_REPLICA.String()]
	got3 := results3[1].String()
	want3 := tablet2Stats1.String()
	if got3 != want3 {
		t.Errorf("got: %v, want: %v", got3, want3)
	}

	realtimeStats.tabletStats.StatsUpdate(tablet2Stats1)
	results4 := realtimeStats.tabletStats.statuses["ks1"]["-80"]["cell1"][topodatapb.TabletType_REPLICA.String()]
	got4 := results4[0].String()
	want4 := tablet1Stats2.String()
	if got4 != want4 {
		t.Errorf("got: %v, want: %v", got4, want4)
	}
}

func TestHeatmapData(t *testing.T) {

	// Creating and Sending updates to 12 tablets.
	tabletStats1 := createTabletStats("cell1", "ks1", "-80", topodatapb.TabletType_MASTER.String(), 100)
	tabletStats2 := createTabletStats("cell1", "ks1", "-80", topodatapb.TabletType_REPLICA.String(), 200)
	tabletStats3 := createTabletStats("cell1", "ks1", "-80", topodatapb.TabletType_REPLICA.String(), 300)
	tabletStats4 := createTabletStats("cell1", "ks1", "-80", topodatapb.TabletType_RDONLY.String(), 400)
	tabletStats5 := createTabletStats("cell2", "ks1", "-80", topodatapb.TabletType_MASTER.String(), 500)
	tabletStats6 := createTabletStats("cell2", "ks1", "-80", topodatapb.TabletType_REPLICA.String(), 600)
	tabletStats7 := createTabletStats("cell1", "ks1", "80-", topodatapb.TabletType_MASTER.String(), 700)
	tabletStats8 := createTabletStats("cell1", "ks1", "80-", topodatapb.TabletType_REPLICA.String(), 800)
	tabletStats9 := createTabletStats("cell1", "ks1", "80-", topodatapb.TabletType_RDONLY.String(), 900)
	tabletStats10 := createTabletStats("cell1", "ks1", "80-", topodatapb.TabletType_RDONLY.String(), 1000)
	tabletStats11 := createTabletStats("cell2", "ks1", "80-", topodatapb.TabletType_MASTER.String(), 1100)
	tabletStats12 := createTabletStats("cell2", "ks1", "80-", topodatapb.TabletType_RDONLY.String(), 1200)

	realtimeStats := newRealtimeStatsForTesting()
	realtimeStats.tabletStats.StatsUpdate(tabletStats1)
	realtimeStats.tabletStats.StatsUpdate(tabletStats2)
	realtimeStats.tabletStats.StatsUpdate(tabletStats3)
	realtimeStats.tabletStats.StatsUpdate(tabletStats4)
	realtimeStats.tabletStats.StatsUpdate(tabletStats5)
	realtimeStats.tabletStats.StatsUpdate(tabletStats6)
	realtimeStats.tabletStats.StatsUpdate(tabletStats7)
	realtimeStats.tabletStats.StatsUpdate(tabletStats8)
	realtimeStats.tabletStats.StatsUpdate(tabletStats9)
	realtimeStats.tabletStats.StatsUpdate(tabletStats10)
	realtimeStats.tabletStats.StatsUpdate(tabletStats11)
	realtimeStats.tabletStats.StatsUpdate(tabletStats12)

	// Checking that the heatmap data is returned correctly for the following view: (keyspace, all, all).
	gotData, gotLabels := realtimeStats.tabletStats.heatmapData("lag", "ks1", "all", "all")
	wantData := [][]float64{
		{float64(tabletStats1.Stats.SecondsBehindMaster), float64(tabletStats7.Stats.SecondsBehindMaster)},
		{float64(tabletStats2.Stats.SecondsBehindMaster), float64(tabletStats8.Stats.SecondsBehindMaster)},
		{float64(tabletStats3.Stats.SecondsBehindMaster), float64(-1)},
		{float64(tabletStats4.Stats.SecondsBehindMaster), float64(tabletStats9.Stats.SecondsBehindMaster)},
		{float64(-1), float64(tabletStats10.Stats.SecondsBehindMaster)},
		{float64(tabletStats5.Stats.SecondsBehindMaster), float64(tabletStats11.Stats.SecondsBehindMaster)},
		{float64(tabletStats6.Stats.SecondsBehindMaster), float64(-1)},
		{float64(-1), float64(tabletStats12.Stats.SecondsBehindMaster)},
	}
	wantLabels := make([]yLabel, 0)
	label := yLabel{
		label: "cell1",
		nestedLabels: []string{topodatapb.TabletType_MASTER.String(), topodatapb.TabletType_REPLICA.String(),
			"", topodatapb.TabletType_BATCH.String(), ""},
	}
	wantLabels = append(wantLabels, label)
	label = yLabel{
		label:        "cell2",
		nestedLabels: []string{topodatapb.TabletType_MASTER.String(), topodatapb.TabletType_REPLICA.String(), topodatapb.TabletType_RDONLY.String()},
	}
	wantLabels = append(wantLabels, label)

	if !reflect.DeepEqual(gotData, wantData) {
		t.Errorf("got: %v, want: %v", gotData, wantData)
	}

	if !reflect.DeepEqual(gotLabels, wantLabels) {
		t.Errorf("got: %v, want: %+v", gotLabels, wantLabels)
	}
}

// createTabletStats will construct a discovery.TabletStats object.
func createTabletStats(cell, keyspace, shard, tabletType string, uid uint32) *discovery.TabletStats {
	typeOfTablet, _ := topoproto.ParseTabletType(tabletType)
	target := &querypb.Target{
		Keyspace:   keyspace,
		Shard:      shard,
		TabletType: typeOfTablet,
	}
	tablet := &topodatapb.Tablet{
		Alias:    &topodatapb.TabletAlias{Cell: cell, Uid: uid},
		Keyspace: keyspace,
		Shard:    shard,
		Type:     typeOfTablet,
		PortMap:  map[string]int32{"vt": int32(uid)},
	}
	realtimeStats := &querypb.RealtimeStats{
		HealthError:         "",
		SecondsBehindMaster: uid,
		BinlogPlayersCount:  0,
	}
	stats := &discovery.TabletStats{
		Tablet:  tablet,
		Target:  target,
		Up:      true,
		Serving: true,
		TabletExternallyReparentedTimestamp: 5,
		Stats:     realtimeStats,
		LastError: nil,
	}
	return stats
}
