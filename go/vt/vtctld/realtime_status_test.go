package vtctld

import (
	"fmt"
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

// TestRealtimeStats tests the functionality of the realtimeStats object without using the HealthCheck object.
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

	// Test 1: tablet1's stats should be updated with the one received by the HealthCheck object.
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

	// Test 2: tablet1's stats should be updated with the new one received by the HealthCheck object.
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

	// Test 3: tablet2's stats should be updated with the one received by the HealthCheck object,
	// leaving tablet1's stats unchanged.
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

// TestRealtimeStatsWithQueryService uses fakeTablets and the fakeQueryService to
// copy the environment needed for the HealthCheck object.
func TestRealtimeStatsWithQueryService(t *testing.T) {
	// Set up testing keyspace with 2 tablets within 2 cells.
	keyspace := "ks"
	shard := "-80"
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

	target := querypb.Target{
		Keyspace:   keyspace,
		Shard:      shard,
		TabletType: topodatapb.TabletType_REPLICA,
	}
	fqs1 := fakes.NewStreamHealthQueryService(target)
	fqs2 := fakes.NewStreamHealthQueryService(target)
	grpcqueryservice.Register(t1.RPCServer, fqs1)
	grpcqueryservice.Register(t2.RPCServer, fqs2)

	fqs1.AddDefaultHealthResponse()

	realtimeStats, err := newRealtimeStats(ts)
	if err != nil {
		t.Fatalf("newRealtimeStats error: %v", err)
	}

	if err := discovery.WaitForTablets(ctx, realtimeStats.healthCheck, "cell1", keyspace, shard, []topodatapb.TabletType{topodatapb.TabletType_REPLICA}); err != nil {
		t.Fatalf("waitForTablets failed: %v", err)
	}

	// Test 1: tablet1's stats should be updated with the one received by the HealthCheck object.
	result := realtimeStats.tabletStatuses("cell1", keyspace, shard, "replica")
	got, want := result["0"].Stats, &querypb.RealtimeStats{
		SecondsBehindMaster: 1,
	}
	if !proto.Equal(got, want) {
		t.Errorf("got: %v, want: %v", got, want)
	}

	// Test 2: tablet1's stats should be updated with the new one received by the HealthCheck object.
	fqs1.AddHealthResponseWithQPS(2.0)
	want2 := &querypb.RealtimeStats{
		SecondsBehindMaster: 1,
		Qps:                 2.0,
	}
	if err := waitForTest(realtimeStats, want2, "0", "cell1", keyspace, shard, "replica"); err != nil {
		t.Errorf("%v", err)
	}

	// Test 3: tablet2's stats should be updated with the one received by the HealthCheck object,
	// leaving tablet1's stats unchanged.
	fqs2.AddHealthResponseWithQPS(3.0)
	want3 := &querypb.RealtimeStats{
		SecondsBehindMaster: 1,
		Qps:                 3.0,
	}
	if err := waitForTest(realtimeStats, want3, "1", "cell2", keyspace, shard, "replica"); err != nil {
		t.Errorf("%v", err)
	}

	if err := waitForTest(realtimeStats, want2, "0", "cell1", keyspace, shard, "replica"); err != nil {
		t.Errorf("%v", err)
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

// waitForTest ensures that the HealthCheck object received an update and passed
// that information to the correct tablet.
func waitForTest(realtimeStats *realtimeStats, want *querypb.RealtimeStats, tabletIndex string, cell, keyspace, shard, tabletType string) error {
	deadline := time.Now().Add(time.Second * 5)
	for {
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout error when getting tabletStatuses")
		}
		result, ok := (realtimeStats.tabletStatuses(cell, keyspace, shard, tabletType))[tabletIndex]
		if !ok {
			continue
		}
		got := result.Stats
		if proto.Equal(got, want) {
			break
		}
	}
	return nil
}

// newRealtimeStatsForTesting creates a new realtimeStats object without creating a HealthCheck object.
func newRealtimeStatsForTesting() *realtimeStats {
	tabletStatsCache := &tabletStatsCache{
		statuses: make(map[string]map[string]*discovery.TabletStats),
	}
	return &realtimeStats{
		tabletStats: tabletStatsCache,
	}
}
