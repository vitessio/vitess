package vtctld

import (
	"strconv"
	"testing"

	"golang.org/x/net/context"

	"github.com/golang/protobuf/proto"
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

	setUpFlagsForTests()
	rts := initHealthCheck(ts)
	//**************************************************//

	tar := &querypb.Target{
		Keyspace:   "ks1",
		Shard:      "-80",
		TabletType: topodatapb.TabletType_REPLICA,
	}

	expectedSt1 := &querypb.RealtimeStats{
		HealthError:         "",
		SecondsBehindMaster: 2,
		BinlogPlayersCount:  0,
		CpuUsage:            12.1,
		Qps:                 5.6,
	}

	//Test 1: update sent to tablet1 and tablet1 recieved it
	tabStats := &discovery.TabletStats{
		Tablet:  tablet1,
		Target:  tar,
		Up:      true,
		Serving: true,
		TabletExternallyReparentedTimestamp: 5,
		Stats:     expectedSt1,
		LastError: nil,
	}

	rts.mimicStatsUpdate(tabStats)
	result := rts.getUpdate("cell1", "ks1", "-80", "REPLICA")
	checkResult(tablet1.Alias.Uid, result, expectedSt1, t)

	//Test 2: send another update to tablet1
	expectedSt2 := &querypb.RealtimeStats{
		HealthError:         "Unhealthy tablet",
		SecondsBehindMaster: 15,
		BinlogPlayersCount:  0,
		CpuUsage:            56.5,
		Qps:                 7.9,
	}

	tabStats2 := &discovery.TabletStats{
		Tablet:  tablet1,
		Target:  tar,
		Up:      true,
		Serving: true,
		TabletExternallyReparentedTimestamp: 5,
		Stats:     expectedSt2,
		LastError: nil,
	}

	rts.mimicStatsUpdate(tabStats2)
	result = rts.getUpdate("cell1", "ks1", "-80", "REPLICA")
	checkResult(tablet1.Alias.Uid, result, expectedSt2, t)

	//Test 3: send an update to tablet2
	expectedSt3 := &querypb.RealtimeStats{
		HealthError:         "Unhealthy tablet",
		SecondsBehindMaster: 15,
		BinlogPlayersCount:  0,
		CpuUsage:            56.5,
		Qps:                 7.9,
	}

	tabStats3 := &discovery.TabletStats{
		Tablet:  tablet2,
		Target:  tar,
		Up:      true,
		Serving: true,
		TabletExternallyReparentedTimestamp: 5,
		Stats:     expectedSt3,
		LastError: nil,
	}

	rts.mimicStatsUpdate(tabStats3)
	result = rts.getUpdate("cell1", "ks1", "-80", "REPLICA")
	checkResult(tablet1.Alias.Uid, result, expectedSt2, t)
}

func checkResult(chosenUID uint32, result map[string]*querypb.RealtimeStats, want *querypb.RealtimeStats, t *testing.T) {
	var got *querypb.RealtimeStats
	for tabID, mes := range result {
		tabID2, err := strconv.ParseUint(tabID, 10, 32)
		if err == nil && uint32(tabID2) == chosenUID {
			got = mes
			break
		}
	}

	if !proto.Equal(got, want) {
		t.Errorf("RealtimeStats = %#v, want %#v", got, want)
	}
}
