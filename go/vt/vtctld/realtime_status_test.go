package vtctld

import (
	"strconv"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/golang/protobuf/proto"
	"github.com/youtube/vitess/go/vt/discovery"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/tabletmanager/tmclient"
	"github.com/youtube/vitess/go/vt/tabletserver/grpcqueryservice"
	"github.com/youtube/vitess/go/vt/tabletserver/queryservice/fakes"
	"github.com/youtube/vitess/go/vt/vttest/fakesqldb"
	"github.com/youtube/vitess/go/vt/wrangler"
	"github.com/youtube/vitess/go/vt/wrangler/testlib"
	"github.com/youtube/vitess/go/vt/zktopo/zktestserver"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// TestRealtimeStats tests the functionality of the realtimeStats object without using the healthcheck object.
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

	realtimeStats := newRealtimeStatsForTesting()

	target := &querypb.Target{
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

	// Test 1: update sent to tablet1 and tablet1 should receive it.
	want1 := &discovery.TabletStats{
		Tablet:  tablet1,
		Target:  target,
		Up:      true,
		Serving: true,
		TabletExternallyReparentedTimestamp: 5,
		Stats:     stats1,
		LastError: nil,
	}

	realtimeStats.tabletStats.StatsUpdate(want1)
	result := realtimeStats.tabletStatuses("cell1", "ks1", "-80", "REPLICA")
	checkResult(t, tablet1.Alias.Uid, result, want1)

	// Test 2: send another update to tablet1 and tablet1 should receive it.
	stats2 := &querypb.RealtimeStats{
		HealthError:         "Unhealthy tablet",
		SecondsBehindMaster: 15,
		BinlogPlayersCount:  0,
		CpuUsage:            56.5,
		Qps:                 7.9,
	}
	want2 := &discovery.TabletStats{
		Tablet:  tablet1,
		Target:  target,
		Up:      true,
		Serving: true,
		TabletExternallyReparentedTimestamp: 5,
		Stats:     stats2,
		LastError: nil,
	}

	realtimeStats.tabletStats.StatsUpdate(want2)
	result = realtimeStats.tabletStatuses("cell1", "ks1", "-80", "REPLICA")
	checkResult(t, tablet1.Alias.Uid, result, want2)

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
		Target:  target,
		Up:      true,
		Serving: true,
		TabletExternallyReparentedTimestamp: 5,
		Stats:     stats3,
		LastError: nil,
	}

	realtimeStats.tabletStats.StatsUpdate(want3)
	result = realtimeStats.tabletStatuses("cell1", "ks1", "-80", "REPLICA")
	checkResult(t, tablet1.Alias.Uid, result, want2)
}

// TestRealtimeStatsWithQueryService uses fakeTablets and the fakeQueryService to copy the environment
// needed for the healthcheck object.
func TestRealtimeStatsWithQueryService(t *testing.T) {
	// Set up testing keyspace with 2 tablets within 2 cells.
	const keyspace string = "ks"
	const shard string = "-80"
	ctx := context.Background()
	db := fakesqldb.Register()
	ts := zktestserver.New(t, []string{"cell1", "cell2"})
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())

	if err := ts.CreateKeyspace(context.Background(), keyspace, &topodatapb.Keyspace{
		ShardingColumnName: "keyspace_id",
		ShardingColumnType: topodatapb.KeyspaceIdType_UINT64,
	}); err != nil {
		t.Fatalf("CreateKeyspace failed: %v", err)
	}

	t1 := testlib.NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_REPLICA, db,
		testlib.TabletKeyspaceShard(t, keyspace, shard))
	t2 := testlib.NewFakeTablet(t, wr, "cell2", 1, topodatapb.TabletType_REPLICA, db,
		testlib.TabletKeyspaceShard(t, keyspace, shard))
	for _, ft := range []*(testlib.FakeTablet){t1, t2} {
		ft.StartActionLoop(t, wr)
		defer ft.StopActionLoop(t)
	}

	target := &querypb.Target{
		Keyspace:   keyspace,
		Shard:      shard,
		TabletType: topodatapb.TabletType_REPLICA,
	}
	fqs1 := fakes.NewStreamHealthQueryService(*target)
	fqs2 := fakes.NewStreamHealthQueryService(*target)
	grpcqueryservice.RegisterForTest(t1.RPCServer, fqs1)
	grpcqueryservice.RegisterForTest(t2.RPCServer, fqs2)

	fqs1.AddDefaultHealthResponse()

	realtimeStats, err := newRealtimeStats(ts)
	if err != nil {
		t.Errorf("newRealtimeStats error: %v", err)
	}

	if err1 := discovery.WaitForTablets(ctx, realtimeStats.healthCheck, "cell1", keyspace, shard, []topodatapb.TabletType{topodatapb.TabletType_REPLICA}); err != nil {
		t.Errorf("waitForTablets failed: %v", err1)
	}

	// Test 1: Tablet1 receives an update.
	result := realtimeStats.tabletStatuses("cell1", keyspace, shard, "replica")
	if result == nil {
		t.Errorf("stats not recieved")
	}

	temp := &querypb.RealtimeStats{
		SecondsBehindMaster: 1,
	}
	checkEquality(t, result["0"].Stats, temp)

	// Test 2: Tablet 1 receives a new update.
	fqs1.AddHealthResponseWithQPS(2.0)

	time.Sleep(time.Second * 5)

	result = realtimeStats.tabletStatuses("cell1", keyspace, shard, "replica")
	if result == nil {
		t.Errorf("stats not recieved")
	}

	temp = &querypb.RealtimeStats{
		SecondsBehindMaster: 1,
		Qps:                 2.0,
	}
	checkEquality(t, result["0"].Stats, temp)

	// Test 3: Tablet 2 receives a new update and leaves tablet 1's unchanged.
	fqs2.AddHealthResponseWithQPS(3.0)

	time.Sleep(time.Second * 5)

	result = realtimeStats.tabletStatuses("cell1", keyspace, shard, "replica")
	if result == nil {
		t.Errorf("stats not recieved")
	}
	checkEquality(t, result["0"].Stats, temp)

	temp = &querypb.RealtimeStats{
		SecondsBehindMaster: 1,
		Qps:                 3.0,
	}
	result = realtimeStats.tabletStatuses("cell2", keyspace, shard, "replica")
	checkEquality(t, result["1"].Stats, temp)
}

// checkEquality compares two RealtimeStats proto messages.
func checkEquality(t *testing.T, got *querypb.RealtimeStats, want *querypb.RealtimeStats) {
	if !proto.Equal(got, want) {
		t.Errorf("got: %v, want: %v", got, want)
	}
}

// checkResult checks to see that the TabletStats received are as expected.
func checkResult(t *testing.T, wantedUID uint32, resultMap map[string]*discovery.TabletStats, original *discovery.TabletStats) {
	result, ok := resultMap[strconv.FormatUint(uint64(wantedUID), 10)]
	if !ok {
		t.Errorf("No such tablet in tabletStatsCache")
	}
	if got, want := result.String(), original.String(); got != want {
		t.Errorf("got: %#v, want: %#v", got, want)
	}
}

// newRealtimeStatsForTesting creates a new realtimeStats object without creating a healthcheck object.
func newRealtimeStatsForTesting() *realtimeStats {
	tabletStatsCache := tabletStatsCache{
		statuses: make(map[string]map[string]*discovery.TabletStats),
	}
	return &realtimeStats{
		tabletStats: tabletStatsCache,
	}
}
