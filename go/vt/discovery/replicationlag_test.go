package discovery

import (
	"reflect"
	"testing"

	qpb "github.com/youtube/vitess/go/vt/proto/query"
)

func TestFilterByReplicationLag(t *testing.T) {
	// 0 endpoint
	got := FilterByReplicationLag([]*EndPointStats{})
	if len(got) != 0 {
		t.Errorf("FilterByReplicationLag([]) = %+v, want []", got)
	}
	// 1 serving endpoint
	eps1 := &EndPointStats{
		Serving: true,
		Stats:   &qpb.RealtimeStats{},
	}
	eps2 := &EndPointStats{
		Serving: false,
		Stats:   &qpb.RealtimeStats{},
	}
	got = FilterByReplicationLag([]*EndPointStats{eps1, eps2})
	if len(got) != 1 {
		t.Errorf("FilterByReplicationLag([EPS{Serving: true}, EPS{Serving: false}]) = %+v, want one", got)
	}
	// lags of (1s, 1s, 1s, 30s)
	eps1 = &EndPointStats{
		Serving: true,
		Stats:   &qpb.RealtimeStats{SecondsBehindMaster: 1},
	}
	eps2 = &EndPointStats{
		Serving: true,
		Stats:   &qpb.RealtimeStats{SecondsBehindMaster: 1},
	}
	eps3 := &EndPointStats{
		Serving: true,
		Stats:   &qpb.RealtimeStats{SecondsBehindMaster: 1},
	}
	eps4 := &EndPointStats{
		Serving: true,
		Stats:   &qpb.RealtimeStats{SecondsBehindMaster: 30},
	}
	got = FilterByReplicationLag([]*EndPointStats{eps1, eps2, eps3, eps4})
	if len(got) != 4 || !reflect.DeepEqual(got[0], eps1) || !reflect.DeepEqual(got[1], eps2) || !reflect.DeepEqual(got[2], eps3) || !reflect.DeepEqual(got[3], eps4) {
		t.Errorf("FilterByReplicationLag([1s, 1s, 1s, 30s]) = %+v, want all", got)
	}
	// lags of (5s, 10s, 15s, 120s)
	eps1 = &EndPointStats{
		Serving: true,
		Stats:   &qpb.RealtimeStats{SecondsBehindMaster: 5},
	}
	eps2 = &EndPointStats{
		Serving: true,
		Stats:   &qpb.RealtimeStats{SecondsBehindMaster: 10},
	}
	eps3 = &EndPointStats{
		Serving: true,
		Stats:   &qpb.RealtimeStats{SecondsBehindMaster: 15},
	}
	eps4 = &EndPointStats{
		Serving: true,
		Stats:   &qpb.RealtimeStats{SecondsBehindMaster: 120},
	}
	got = FilterByReplicationLag([]*EndPointStats{eps1, eps2, eps3, eps4})
	if len(got) != 3 || !reflect.DeepEqual(got[0], eps1) || !reflect.DeepEqual(got[1], eps2) || !reflect.DeepEqual(got[2], eps3) {
		t.Errorf("FilterByReplicationLag([5s, 10s, 15s, 120s]) = %+v, want [5s, 10s, 15s]", got)
	}
	// lags of (30m, 35m, 40m, 45m)
	eps1 = &EndPointStats{
		Serving: true,
		Stats:   &qpb.RealtimeStats{SecondsBehindMaster: 1800},
	}
	eps2 = &EndPointStats{
		Serving: true,
		Stats:   &qpb.RealtimeStats{SecondsBehindMaster: 2100},
	}
	eps3 = &EndPointStats{
		Serving: true,
		Stats:   &qpb.RealtimeStats{SecondsBehindMaster: 2400},
	}
	eps4 = &EndPointStats{
		Serving: true,
		Stats:   &qpb.RealtimeStats{SecondsBehindMaster: 2700},
	}
	got = FilterByReplicationLag([]*EndPointStats{eps1, eps2, eps3, eps4})
	if len(got) != 4 || !reflect.DeepEqual(got[0], eps1) || !reflect.DeepEqual(got[1], eps2) || !reflect.DeepEqual(got[2], eps3) || !reflect.DeepEqual(got[3], eps4) {
		t.Errorf("FilterByReplicationLag([30m, 35m, 40m, 45m]) = %+v, want all", got)
	}
}
