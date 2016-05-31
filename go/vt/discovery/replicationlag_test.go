package discovery

import (
	"reflect"
	"testing"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	"github.com/youtube/vitess/go/vt/topo"
)

func TestFilterByReplicationLag(t *testing.T) {
	// 0 tablet
	got := FilterByReplicationLag([]*TabletStats{})
	if len(got) != 0 {
		t.Errorf("FilterByReplicationLag([]) = %+v, want []", got)
	}
	// 1 serving tablet
	ts1 := &TabletStats{
		Tablet:  topo.NewTablet(1, "cell", "host1"),
		Serving: true,
		Stats:   &querypb.RealtimeStats{},
	}
	ts2 := &TabletStats{
		Tablet:  topo.NewTablet(2, "cell", "host2"),
		Serving: false,
		Stats:   &querypb.RealtimeStats{},
	}
	got = FilterByReplicationLag([]*TabletStats{ts1, ts2})
	if len(got) != 1 {
		t.Errorf("len(FilterByReplicationLag([{Tablet: {Uid: 1}, Serving: true}, {Tablet: {Uid: 2}, Serving: false}])) = %v, want 1", len(got))
	}
	if len(got) > 0 && !reflect.DeepEqual(got[0], ts1) {
		t.Errorf("FilterByReplicationLag([{Tablet: {Uid: 1}, Serving: true}, {Tablet: {Uid: 2}, Serving: false}]) = %+v, want %+v", got[0], ts1)
	}
	// lags of (1s, 1s, 1s, 30s)
	ts1 = &TabletStats{
		Tablet:  topo.NewTablet(1, "cell", "host1"),
		Serving: true,
		Stats:   &querypb.RealtimeStats{SecondsBehindMaster: 1},
	}
	ts2 = &TabletStats{
		Tablet:  topo.NewTablet(2, "cell", "host2"),
		Serving: true,
		Stats:   &querypb.RealtimeStats{SecondsBehindMaster: 1},
	}
	ts3 := &TabletStats{
		Tablet:  topo.NewTablet(3, "cell", "host3"),
		Serving: true,
		Stats:   &querypb.RealtimeStats{SecondsBehindMaster: 1},
	}
	ts4 := &TabletStats{
		Tablet:  topo.NewTablet(4, "cell", "host4"),
		Serving: true,
		Stats:   &querypb.RealtimeStats{SecondsBehindMaster: 30},
	}
	got = FilterByReplicationLag([]*TabletStats{ts1, ts2, ts3, ts4})
	if len(got) != 4 || !reflect.DeepEqual(got[0], ts1) || !reflect.DeepEqual(got[1], ts2) || !reflect.DeepEqual(got[2], ts3) || !reflect.DeepEqual(got[3], ts4) {
		t.Errorf("FilterByReplicationLag([1s, 1s, 1s, 30s]) = %+v, want all", got)
	}
	// lags of (5s, 10s, 15s, 120s)
	ts1 = &TabletStats{
		Tablet:  topo.NewTablet(1, "cell", "host1"),
		Serving: true,
		Stats:   &querypb.RealtimeStats{SecondsBehindMaster: 5},
	}
	ts2 = &TabletStats{
		Tablet:  topo.NewTablet(2, "cell", "host2"),
		Serving: true,
		Stats:   &querypb.RealtimeStats{SecondsBehindMaster: 10},
	}
	ts3 = &TabletStats{
		Tablet:  topo.NewTablet(3, "cell", "host3"),
		Serving: true,
		Stats:   &querypb.RealtimeStats{SecondsBehindMaster: 15},
	}
	ts4 = &TabletStats{
		Tablet:  topo.NewTablet(4, "cell", "host4"),
		Serving: true,
		Stats:   &querypb.RealtimeStats{SecondsBehindMaster: 120},
	}
	got = FilterByReplicationLag([]*TabletStats{ts1, ts2, ts3, ts4})
	if len(got) != 3 || !reflect.DeepEqual(got[0], ts1) || !reflect.DeepEqual(got[1], ts2) || !reflect.DeepEqual(got[2], ts3) {
		t.Errorf("FilterByReplicationLag([5s, 10s, 15s, 120s]) = %+v, want [5s, 10s, 15s]", got)
	}
	// lags of (30m, 35m, 40m, 45m)
	ts1 = &TabletStats{
		Tablet:  topo.NewTablet(1, "cell", "host1"),
		Serving: true,
		Stats:   &querypb.RealtimeStats{SecondsBehindMaster: 30 * 60},
	}
	ts2 = &TabletStats{
		Tablet:  topo.NewTablet(2, "cell", "host2"),
		Serving: true,
		Stats:   &querypb.RealtimeStats{SecondsBehindMaster: 35 * 60},
	}
	ts3 = &TabletStats{
		Tablet:  topo.NewTablet(3, "cell", "host3"),
		Serving: true,
		Stats:   &querypb.RealtimeStats{SecondsBehindMaster: 40 * 60},
	}
	ts4 = &TabletStats{
		Tablet:  topo.NewTablet(4, "cell", "host4"),
		Serving: true,
		Stats:   &querypb.RealtimeStats{SecondsBehindMaster: 45 * 60},
	}
	got = FilterByReplicationLag([]*TabletStats{ts1, ts2, ts3, ts4})
	if len(got) != 4 || !reflect.DeepEqual(got[0], ts1) || !reflect.DeepEqual(got[1], ts2) || !reflect.DeepEqual(got[2], ts3) || !reflect.DeepEqual(got[3], ts4) {
		t.Errorf("FilterByReplicationLag([30m, 35m, 40m, 45m]) = %+v, want all", got)
	}
	// lags of (1s, 1s, 1m, 40m, 40m) - not run filter the second time as first run removed two items.
	ts1 = &TabletStats{
		Tablet:  topo.NewTablet(1, "cell", "host1"),
		Serving: true,
		Stats:   &querypb.RealtimeStats{SecondsBehindMaster: 1},
	}
	ts2 = &TabletStats{
		Tablet:  topo.NewTablet(2, "cell", "host2"),
		Serving: true,
		Stats:   &querypb.RealtimeStats{SecondsBehindMaster: 1},
	}
	ts3 = &TabletStats{
		Tablet:  topo.NewTablet(3, "cell", "host3"),
		Serving: true,
		Stats:   &querypb.RealtimeStats{SecondsBehindMaster: 1 * 60},
	}
	ts4 = &TabletStats{
		Tablet:  topo.NewTablet(4, "cell", "host4"),
		Serving: true,
		Stats:   &querypb.RealtimeStats{SecondsBehindMaster: 40 * 60},
	}
	ts5 := &TabletStats{
		Tablet:  topo.NewTablet(5, "cell", "host5"),
		Serving: true,
		Stats:   &querypb.RealtimeStats{SecondsBehindMaster: 40 * 60},
	}
	got = FilterByReplicationLag([]*TabletStats{ts1, ts2, ts3, ts4, ts5})
	if len(got) != 3 || !reflect.DeepEqual(got[0], ts1) || !reflect.DeepEqual(got[1], ts2) || !reflect.DeepEqual(got[2], ts3) {
		t.Errorf("FilterByReplicationLag([1s, 1s, 1m, 40m, 40m]) = %+v, want [1s, 1s, 1m]", got)
	}
	// lags of (1s, 1s, 10m, 40m) - run filter twice to remove two items
	ts1 = &TabletStats{
		Tablet:  topo.NewTablet(1, "cell", "host1"),
		Serving: true,
		Stats:   &querypb.RealtimeStats{SecondsBehindMaster: 1},
	}
	ts2 = &TabletStats{
		Tablet:  topo.NewTablet(2, "cell", "host2"),
		Serving: true,
		Stats:   &querypb.RealtimeStats{SecondsBehindMaster: 1},
	}
	ts3 = &TabletStats{
		Tablet:  topo.NewTablet(3, "cell", "host3"),
		Serving: true,
		Stats:   &querypb.RealtimeStats{SecondsBehindMaster: 10 * 60},
	}
	ts4 = &TabletStats{
		Tablet:  topo.NewTablet(4, "cell", "host4"),
		Serving: true,
		Stats:   &querypb.RealtimeStats{SecondsBehindMaster: 40 * 60},
	}
	got = FilterByReplicationLag([]*TabletStats{ts1, ts2, ts3, ts4})
	if len(got) != 2 || !reflect.DeepEqual(got[0], ts1) || !reflect.DeepEqual(got[1], ts2) {
		t.Errorf("FilterByReplicationLag([1s, 1s, 10m, 40m]) = %+v, want [1s, 1s]", got)
	}
	// lags of (1m, 100m) - return at least 2 items to avoid overloading if the 2nd one is not delayed too much
	ts1 = &TabletStats{
		Tablet:  topo.NewTablet(1, "cell", "host1"),
		Serving: true,
		Stats:   &querypb.RealtimeStats{SecondsBehindMaster: 1 * 60},
	}
	ts2 = &TabletStats{
		Tablet:  topo.NewTablet(2, "cell", "host2"),
		Serving: true,
		Stats:   &querypb.RealtimeStats{SecondsBehindMaster: 100 * 60},
	}
	got = FilterByReplicationLag([]*TabletStats{ts1, ts2})
	if len(got) != 2 || !reflect.DeepEqual(got[0], ts1) || !reflect.DeepEqual(got[1], ts2) {
		t.Errorf("FilterByReplicationLag([1m, 100m]) = %+v, want all", got)
	}
	// lags of (1m, 3h) - return 1 if the 2nd one is delayed too much
	ts1 = &TabletStats{
		Tablet:  topo.NewTablet(1, "cell", "host1"),
		Serving: true,
		Stats:   &querypb.RealtimeStats{SecondsBehindMaster: 1 * 60},
	}
	ts2 = &TabletStats{
		Tablet:  topo.NewTablet(2, "cell", "host2"),
		Serving: true,
		Stats:   &querypb.RealtimeStats{SecondsBehindMaster: 3 * 60 * 60},
	}
	got = FilterByReplicationLag([]*TabletStats{ts1, ts2})
	if len(got) != 1 || !reflect.DeepEqual(got[0], ts1) {
		t.Errorf("FilterByReplicationLag([1m, 3h]) = %+v, want [1m]", got)
	}
}
