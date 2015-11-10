package discovery

import (
	"reflect"
	"testing"

	qpb "github.com/youtube/vitess/go/vt/proto/query"
	tpb "github.com/youtube/vitess/go/vt/proto/topodata"
)

func TestFilterByReplicationLag(t *testing.T) {
	// 0 endpoint
	got := FilterByReplicationLag([]*EndPointStats{})
	if len(got) != 0 {
		t.Errorf("FilterByReplicationLag([]) = %+v, want []", got)
	}
	// 1 serving endpoint
	eps1 := &EndPointStats{
		EndPoint: &tpb.EndPoint{Uid: 1},
		Serving:  true,
		Stats:    &qpb.RealtimeStats{},
	}
	eps2 := &EndPointStats{
		EndPoint: &tpb.EndPoint{Uid: 2},
		Serving:  false,
		Stats:    &qpb.RealtimeStats{},
	}
	got = FilterByReplicationLag([]*EndPointStats{eps1, eps2})
	if len(got) != 1 {
		t.Errorf("len(FilterByReplicationLag([{EndPoint: {Uid: 1}, Serving: true}, {EndPoint: {Uid: 2}, Serving: false}])) = %v, want 1", len(got))
	}
	if len(got) > 0 && !reflect.DeepEqual(got[0], eps1) {
		t.Errorf("FilterByReplicationLag([{EndPoint: {Uid: 1}, Serving: true}, {EndPoint: {Uid: 2}, Serving: false}]) = %+v, want %+v", got[0], eps1)
	}
	// lags of (1s, 1s, 1s, 30s)
	eps1 = &EndPointStats{
		EndPoint: &tpb.EndPoint{Uid: 1},
		Serving:  true,
		Stats:    &qpb.RealtimeStats{SecondsBehindMaster: 1},
	}
	eps2 = &EndPointStats{
		EndPoint: &tpb.EndPoint{Uid: 2},
		Serving:  true,
		Stats:    &qpb.RealtimeStats{SecondsBehindMaster: 1},
	}
	eps3 := &EndPointStats{
		EndPoint: &tpb.EndPoint{Uid: 3},
		Serving:  true,
		Stats:    &qpb.RealtimeStats{SecondsBehindMaster: 1},
	}
	eps4 := &EndPointStats{
		EndPoint: &tpb.EndPoint{Uid: 4},
		Serving:  true,
		Stats:    &qpb.RealtimeStats{SecondsBehindMaster: 30},
	}
	got = FilterByReplicationLag([]*EndPointStats{eps1, eps2, eps3, eps4})
	if len(got) != 4 || !reflect.DeepEqual(got[0], eps1) || !reflect.DeepEqual(got[1], eps2) || !reflect.DeepEqual(got[2], eps3) || !reflect.DeepEqual(got[3], eps4) {
		t.Errorf("FilterByReplicationLag([1s, 1s, 1s, 30s]) = %+v, want all", got)
	}
	// lags of (5s, 10s, 15s, 120s)
	eps1 = &EndPointStats{
		EndPoint: &tpb.EndPoint{Uid: 1},
		Serving:  true,
		Stats:    &qpb.RealtimeStats{SecondsBehindMaster: 5},
	}
	eps2 = &EndPointStats{
		EndPoint: &tpb.EndPoint{Uid: 2},
		Serving:  true,
		Stats:    &qpb.RealtimeStats{SecondsBehindMaster: 10},
	}
	eps3 = &EndPointStats{
		EndPoint: &tpb.EndPoint{Uid: 3},
		Serving:  true,
		Stats:    &qpb.RealtimeStats{SecondsBehindMaster: 15},
	}
	eps4 = &EndPointStats{
		EndPoint: &tpb.EndPoint{Uid: 4},
		Serving:  true,
		Stats:    &qpb.RealtimeStats{SecondsBehindMaster: 120},
	}
	got = FilterByReplicationLag([]*EndPointStats{eps1, eps2, eps3, eps4})
	if len(got) != 3 || !reflect.DeepEqual(got[0], eps1) || !reflect.DeepEqual(got[1], eps2) || !reflect.DeepEqual(got[2], eps3) {
		t.Errorf("FilterByReplicationLag([5s, 10s, 15s, 120s]) = %+v, want [5s, 10s, 15s]", got)
	}
	// lags of (30m, 35m, 40m, 45m)
	eps1 = &EndPointStats{
		EndPoint: &tpb.EndPoint{Uid: 1},
		Serving:  true,
		Stats:    &qpb.RealtimeStats{SecondsBehindMaster: 30 * 60},
	}
	eps2 = &EndPointStats{
		EndPoint: &tpb.EndPoint{Uid: 2},
		Serving:  true,
		Stats:    &qpb.RealtimeStats{SecondsBehindMaster: 35 * 60},
	}
	eps3 = &EndPointStats{
		EndPoint: &tpb.EndPoint{Uid: 3},
		Serving:  true,
		Stats:    &qpb.RealtimeStats{SecondsBehindMaster: 40 * 60},
	}
	eps4 = &EndPointStats{
		EndPoint: &tpb.EndPoint{Uid: 4},
		Serving:  true,
		Stats:    &qpb.RealtimeStats{SecondsBehindMaster: 45 * 60},
	}
	got = FilterByReplicationLag([]*EndPointStats{eps1, eps2, eps3, eps4})
	if len(got) != 4 || !reflect.DeepEqual(got[0], eps1) || !reflect.DeepEqual(got[1], eps2) || !reflect.DeepEqual(got[2], eps3) || !reflect.DeepEqual(got[3], eps4) {
		t.Errorf("FilterByReplicationLag([30m, 35m, 40m, 45m]) = %+v, want all", got)
	}
}
