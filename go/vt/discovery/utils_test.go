package discovery

import (
	"reflect"
	"testing"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

func TestRemoveUnhealthyEndpoints(t *testing.T) {
	var testcases = []struct {
		desc  string
		input []*EndPointStats
		want  []*EndPointStats
	}{
		{
			desc:  "tablets missing Stats",
			input: []*EndPointStats{replica(1), replica(2)},
			want:  []*EndPointStats{},
		},
		{
			desc:  "all tablets healthy",
			input: []*EndPointStats{healthy(replica(1)), healthy(replica(2))},
			want:  []*EndPointStats{healthy(replica(1)), healthy(replica(2))},
		},
		{
			desc:  "one unhealthy tablet (error)",
			input: []*EndPointStats{healthy(replica(1)), unhealthyError(replica(2))},
			want:  []*EndPointStats{healthy(replica(1))},
		},
		{
			desc:  "one unhealthy tablet (lag)",
			input: []*EndPointStats{healthy(replica(1)), unhealthyLag(replica(2))},
			want:  []*EndPointStats{healthy(replica(1))},
		},
		{
			desc:  "no filtering by tablet type",
			input: []*EndPointStats{healthy(master(1)), healthy(replica(2)), healthy(rdonly(3))},
			want:  []*EndPointStats{healthy(master(1)), healthy(replica(2)), healthy(rdonly(3))},
		},
		{
			desc:  "non-serving tablets won't be removed",
			input: []*EndPointStats{notServing(healthy(replica(1)))},
			want:  []*EndPointStats{notServing(healthy(replica(1)))},
		},
	}

	for _, tc := range testcases {
		if got := RemoveUnhealthyEndpoints(tc.input); !reflect.DeepEqual(got, tc.want) {
			t.Errorf("test case '%v' failed: RemoveUnhealthyEndpoints(%v) = %#v, want: %#v", tc.desc, tc.input, got, tc.want)
		}
	}
}

func TestGetCurrentMaster(t *testing.T) {
	var testcases = []struct {
		desc  string
		input []*EndPointStats
		want  []*EndPointStats
	}{
		{
			desc:  "zero masters remains zero",
			input: []*EndPointStats{replica(1), rdonly(2)},
			want:  []*EndPointStats{},
		},
		{
			desc:  "single master",
			input: []*EndPointStats{master(1)},
			want:  []*EndPointStats{master(1)},
		},
		{
			desc:  "multiple masters with different reparent times",
			input: []*EndPointStats{reparentAt(10, master(1)), reparentAt(11, master(2))},
			want:  []*EndPointStats{reparentAt(11, master(2))},
		},
	}

	for _, tc := range testcases {
		if got := GetCurrentMaster(tc.input); !reflect.DeepEqual(got, tc.want) {
			t.Errorf("test case '%v' failed: GetCurrentMaster(%v) = %v, want: %v", tc.desc, tc.input, got, tc.want)
		}
	}
}

func master(uid uint32) *EndPointStats {
	return minimalEndPointStats(uid, topodatapb.TabletType_MASTER)
}

func replica(uid uint32) *EndPointStats {
	return minimalEndPointStats(uid, topodatapb.TabletType_REPLICA)
}

func rdonly(uid uint32) *EndPointStats {
	return minimalEndPointStats(uid, topodatapb.TabletType_RDONLY)
}

func minimalEndPointStats(uid uint32, tabletType topodatapb.TabletType) *EndPointStats {
	return &EndPointStats{
		EndPoint: &topodatapb.EndPoint{Uid: uid},
		Target: &querypb.Target{
			Keyspace:   "test_keyspace",
			Shard:      "-80",
			TabletType: tabletType,
		},
		Serving: true,
	}
}

func reparentAt(timestamp int64, eps *EndPointStats) *EndPointStats {
	eps.TabletExternallyReparentedTimestamp = timestamp
	return eps
}

func healthy(eps *EndPointStats) *EndPointStats {
	eps.Stats = &querypb.RealtimeStats{
		SecondsBehindMaster: uint32(1),
	}
	return eps
}

func unhealthyLag(eps *EndPointStats) *EndPointStats {
	eps.Stats = &querypb.RealtimeStats{
		SecondsBehindMaster: uint32(3600),
	}
	return eps
}

func unhealthyError(eps *EndPointStats) *EndPointStats {
	eps.Stats = &querypb.RealtimeStats{
		HealthError: "unhealthy",
	}
	return eps
}

func notServing(eps *EndPointStats) *EndPointStats {
	eps.Serving = false
	return eps
}
