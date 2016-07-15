package vtctld

import (
	"strconv"
	"testing"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/discovery"
	"github.com/youtube/vitess/go/vt/zktopo/zktestserver"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

func TestRealtimeStats(t *testing.T) {
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

	tablet1 := &topodatapb.Tablet{
		Alias:    &topodatapb.TabletAlias{Cell: "cell1", Uid: 100},
		Keyspace: "ks1",
		Shard:    "-80",
		Type:     topodatapb.TabletType_REPLICA,
		KeyRange: &topodatapb.KeyRange{Start: nil, End: []byte{0x80}},
		PortMap:  map[string]int32{"vt": 100},
	}
	ts.CreateTablet(ctx, tablet1)

	tablet2 := &topodatapb.Tablet{
		Alias:    &topodatapb.TabletAlias{Cell: "cell2", Uid: 200},
		Keyspace: "ks1",
		Shard:    "-80",
		Type:     topodatapb.TabletType_REPLICA,
		KeyRange: &topodatapb.KeyRange{Start: nil, End: []byte{0x80}},
		PortMap:  map[string]int32{"vt": 200},
	}
	ts.CreateTablet(ctx, tablet2)

	realtimeStats, err := newRealtimeStats(ts)
	if err != nil {
		t.Errorf("newRealtimeStats error: %v", err)
	}

	tar := &querypb.Target{
		Keyspace:   "ks1",
		Shard:      "-80",
		TabletType: topodatapb.TabletType_REPLICA,
	}

	stats1 := &querypb.RealtimeStats{
		HealthError:         "",
		SecondsBehindMaster: 2,
		BinlogPlayersCount:  0,
		CpuUsage:            12.1,
		Qps:                 5.6,
	}

	// Test 1: update sent to tablet1 and tablet1 should recieve it.
	want1 := &discovery.TabletStats{
		Tablet:  tablet1,
		Target:  tar,
		Up:      true,
		Serving: true,
		TabletExternallyReparentedTimestamp: 5,
		Stats:     stats1,
		LastError: nil,
	}

	realtimeStats.mimicStatsUpdateForTesting(want1)
	result := realtimeStats.tabletStatuses("cell1", "ks1", "-80", "REPLICA")
	checkResult(tablet1.Alias.Uid, result, want1, t)

	// Test 2: send another update to tablet1 and tablet1 should recieve it.
	stats2 := &querypb.RealtimeStats{
		HealthError:         "Unhealthy tablet",
		SecondsBehindMaster: 15,
		BinlogPlayersCount:  0,
		CpuUsage:            56.5,
		Qps:                 7.9,
	}

	want2 := &discovery.TabletStats{
		Tablet:  tablet1,
		Target:  tar,
		Up:      true,
		Serving: true,
		TabletExternallyReparentedTimestamp: 5,
		Stats:     stats2,
		LastError: nil,
	}

	realtimeStats.mimicStatsUpdateForTesting(want2)
	result = realtimeStats.tabletStatuses("cell1", "ks1", "-80", "REPLICA")
	checkResult(tablet1.Alias.Uid, result, want2, t)

	// Test 3: send an update to tablet2 and tablet1 should remain unchanged.
	stats3 := &querypb.RealtimeStats{
		HealthError:         "Unhealthy tablet",
		SecondsBehindMaster: 15,
		BinlogPlayersCount:  0,
		CpuUsage:            56.5,
		Qps:                 7.9,
	}

	want3 := &discovery.TabletStats{
		Tablet:  tablet2,
		Target:  tar,
		Up:      true,
		Serving: true,
		TabletExternallyReparentedTimestamp: 5,
		Stats:     stats3,
		LastError: nil,
	}

	realtimeStats.mimicStatsUpdateForTesting(want3)
	result = realtimeStats.tabletStatuses("cell1", "ks1", "-80", "REPLICA")
	checkResult(tablet1.Alias.Uid, result, want2, t)

	if err := realtimeStats.Stop(); err != nil {
		t.Errorf("realtimeStats.Stop() failed: %v", err)
	}
}

// checkResult checks to see that the TabletStats received are as expected
func checkResult(chosenUID uint32, resultMap map[string]*discovery.TabletStats, original *discovery.TabletStats, t *testing.T) {
	var result *discovery.TabletStats
	for tabID, mes := range resultMap {
		tabID2, err := strconv.ParseUint(tabID, 10, 32)
		if err == nil && uint32(tabID2) == chosenUID {
			result = mes
			break
		}
	}

	if got, want := result.String(), original.String(); got != want {
		t.Errorf("got: %#v, want: %#v", got, want)
	}

}
