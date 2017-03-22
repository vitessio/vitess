package discovery

import (
	"testing"

	"github.com/youtube/vitess/go/vt/topo"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// TestTabletStatsCache tests the functionality of the TabletStatsCache class.
func TestTabletStatsCache(t *testing.T) {
	// We want to unit test TabletStatsCache without a full-blown
	// HealthCheck object, so we can't call NewTabletStatsCache.
	// So we just construct this object here.
	tsc := &TabletStatsCache{
		cell:    "cell",
		entries: make(map[string]map[string]map[topodatapb.TabletType]*tabletStatsCacheEntry),
	}

	// empty
	a := tsc.GetTabletStats("k", "s", topodatapb.TabletType_MASTER)
	if len(a) != 0 {
		t.Errorf("wrong result, expected empty list: %v", a)
	}

	// add a tablet
	tablet1 := topo.NewTablet(10, "cell", "host1")
	ts1 := &TabletStats{
		Key:     "t1",
		Tablet:  tablet1,
		Target:  &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Up:      true,
		Serving: true,
		Stats:   &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},
	}
	tsc.StatsUpdate(ts1)

	// check it's there
	a = tsc.GetTabletStats("k", "s", topodatapb.TabletType_REPLICA)
	if len(a) != 1 || !ts1.DeepEqual(&a[0]) {
		t.Errorf("unexpected result: %v", a)
	}
	a = tsc.GetHealthyTabletStats("k", "s", topodatapb.TabletType_REPLICA)
	if len(a) != 1 || !ts1.DeepEqual(&a[0]) {
		t.Errorf("unexpected result: %v", a)
	}

	// update stats with a change that won't change health array
	stillHealthyTs1 := &TabletStats{
		Key:     "t1",
		Tablet:  tablet1,
		Target:  &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Up:      true,
		Serving: true,
		Stats:   &querypb.RealtimeStats{SecondsBehindMaster: 2, CpuUsage: 0.2},
	}
	tsc.StatsUpdate(stillHealthyTs1)

	// check the previous ts1 is still there, as the new one is ignored.
	a = tsc.GetTabletStats("k", "s", topodatapb.TabletType_REPLICA)
	if len(a) != 1 || !ts1.DeepEqual(&a[0]) {
		t.Errorf("unexpected result: %v", a)
	}
	a = tsc.GetHealthyTabletStats("k", "s", topodatapb.TabletType_REPLICA)
	if len(a) != 1 || !ts1.DeepEqual(&a[0]) {
		t.Errorf("unexpected result: %v", a)
	}

	// update stats with a change that will change arrays
	notHealthyTs1 := &TabletStats{
		Key:     "t1",
		Tablet:  tablet1,
		Target:  &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Up:      true,
		Serving: true,
		Stats:   &querypb.RealtimeStats{SecondsBehindMaster: 35, CpuUsage: 0.2},
	}
	tsc.StatsUpdate(notHealthyTs1)

	// check it's there
	a = tsc.GetTabletStats("k", "s", topodatapb.TabletType_REPLICA)
	if len(a) != 1 || !notHealthyTs1.DeepEqual(&a[0]) {
		t.Errorf("unexpected result: %v", a)
	}
	a = tsc.GetHealthyTabletStats("k", "s", topodatapb.TabletType_REPLICA)
	if len(a) != 1 || !notHealthyTs1.DeepEqual(&a[0]) {
		t.Errorf("unexpected result: %v", a)
	}

	// add a second tablet
	tablet2 := topo.NewTablet(11, "cell", "host2")
	ts2 := &TabletStats{
		Key:     "t2",
		Tablet:  tablet2,
		Target:  &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Up:      true,
		Serving: true,
		Stats:   &querypb.RealtimeStats{SecondsBehindMaster: 10, CpuUsage: 0.2},
	}
	tsc.StatsUpdate(ts2)

	// check it's there
	a = tsc.GetTabletStats("k", "s", topodatapb.TabletType_REPLICA)
	if len(a) != 2 {
		t.Errorf("unexpected result: %v", a)
	} else {
		if a[0].Tablet.Alias.Uid == 11 {
			a[0], a[1] = a[1], a[0]
		}
		if !ts1.DeepEqual(&a[0]) || !ts2.DeepEqual(&a[1]) {
			t.Errorf("unexpected result: %v", a)
		}
	}
	a = tsc.GetHealthyTabletStats("k", "s", topodatapb.TabletType_REPLICA)
	if len(a) != 2 {
		t.Errorf("unexpected result: %v", a)
	} else {
		if a[0].Tablet.Alias.Uid == 11 {
			a[0], a[1] = a[1], a[0]
		}
		if !ts1.DeepEqual(&a[0]) || !ts2.DeepEqual(&a[1]) {
			t.Errorf("unexpected result: %v", a)
		}
	}

	// one tablet goes unhealthy
	ts2.Serving = false
	tsc.StatsUpdate(ts2)

	// check we only have one left in healthy version
	a = tsc.GetTabletStats("k", "s", topodatapb.TabletType_REPLICA)
	if len(a) != 2 {
		t.Errorf("unexpected result: %v", a)
	} else {
		if a[0].Tablet.Alias.Uid == 11 {
			a[0], a[1] = a[1], a[0]
		}
		if !ts1.DeepEqual(&a[0]) || !ts2.DeepEqual(&a[1]) {
			t.Errorf("unexpected result: %v", a)
		}
	}
	a = tsc.GetHealthyTabletStats("k", "s", topodatapb.TabletType_REPLICA)
	if len(a) != 1 || !ts1.DeepEqual(&a[0]) {
		t.Errorf("unexpected result: %v", a)
	}

	// second tablet turns into a master, we receive down + up
	ts2.Serving = true
	ts2.Up = false
	tsc.StatsUpdate(ts2)
	ts2.Up = true
	ts2.Target.TabletType = topodatapb.TabletType_MASTER
	ts2.TabletExternallyReparentedTimestamp = 10
	tsc.StatsUpdate(ts2)

	// check we only have one replica left
	a = tsc.GetTabletStats("k", "s", topodatapb.TabletType_REPLICA)
	if len(a) != 1 || !ts1.DeepEqual(&a[0]) {
		t.Errorf("unexpected result: %v", a)
	}

	// check we have a master now
	a = tsc.GetTabletStats("k", "s", topodatapb.TabletType_MASTER)
	if len(a) != 1 || !ts2.DeepEqual(&a[0]) {
		t.Errorf("unexpected result: %v", a)
	}

	// reparent: old replica goes into master
	ts1.Up = false
	tsc.StatsUpdate(ts1)
	ts1.Up = true
	ts1.Target.TabletType = topodatapb.TabletType_MASTER
	ts1.TabletExternallyReparentedTimestamp = 20
	tsc.StatsUpdate(ts1)

	// check we lost all replicas, and master is new one
	a = tsc.GetTabletStats("k", "s", topodatapb.TabletType_REPLICA)
	if len(a) != 0 {
		t.Errorf("unexpected result: %v", a)
	}
	a = tsc.GetHealthyTabletStats("k", "s", topodatapb.TabletType_MASTER)
	if len(a) != 1 || !ts1.DeepEqual(&a[0]) {
		t.Errorf("unexpected result: %v", a)
	}

	// old master sending an old ping should be ignored
	tsc.StatsUpdate(ts2)
	a = tsc.GetHealthyTabletStats("k", "s", topodatapb.TabletType_MASTER)
	if len(a) != 1 || !ts1.DeepEqual(&a[0]) {
		t.Errorf("unexpected result: %v", a)
	}
}
