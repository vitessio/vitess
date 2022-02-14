package discovery

import (
	"errors"
	"testing"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestRemoveUnhealthyTablets(t *testing.T) {
	var testcases = []struct {
		desc  string
		input []LegacyTabletStats
		want  []LegacyTabletStats
	}{{
		desc:  "tablets missing Stats",
		input: []LegacyTabletStats{replica(1), replica(2)},
		want:  []LegacyTabletStats{},
	}, {
		desc:  "all tablets healthy",
		input: []LegacyTabletStats{healthy(replica(1)), healthy(replica(2))},
		want:  []LegacyTabletStats{healthy(replica(1)), healthy(replica(2))},
	}, {
		desc:  "one unhealthy tablet (error)",
		input: []LegacyTabletStats{healthy(replica(1)), unhealthyError(replica(2))},
		want:  []LegacyTabletStats{healthy(replica(1))},
	}, {
		desc:  "one error tablet",
		input: []LegacyTabletStats{healthy(replica(1)), unhealthyLastError(replica(2))},
		want:  []LegacyTabletStats{healthy(replica(1))},
	}, {
		desc:  "one unhealthy tablet (lag)",
		input: []LegacyTabletStats{healthy(replica(1)), unhealthyLag(replica(2))},
		want:  []LegacyTabletStats{healthy(replica(1))},
	}, {
		desc:  "no filtering by tablet type",
		input: []LegacyTabletStats{healthy(primary(1)), healthy(replica(2)), healthy(rdonly(3))},
		want:  []LegacyTabletStats{healthy(primary(1)), healthy(replica(2)), healthy(rdonly(3))},
	}, {
		desc:  "non-serving tablets won't be removed",
		input: []LegacyTabletStats{notServing(healthy(replica(1)))},
		want:  []LegacyTabletStats{notServing(healthy(replica(1)))},
	}}

	for _, tc := range testcases {
		got := RemoveUnhealthyTablets(tc.input)
		if len(got) != len(tc.want) {
			t.Errorf("test case '%v' failed: RemoveUnhealthyTablets(%v) = %#v, want: %#v", tc.desc, tc.input, got, tc.want)
		} else {
			for i := range tc.want {
				if !got[i].DeepEqual(&tc.want[i]) {
					t.Errorf("test case '%v' failed: RemoveUnhealthyTablets(%v) = %#v, want: %#v", tc.desc, tc.input, got, tc.want)
				}
			}
		}
	}
}

func primary(uid uint32) LegacyTabletStats {
	return minimalTabletStats(uid, topodatapb.TabletType_PRIMARY)
}

func replica(uid uint32) LegacyTabletStats {
	return minimalTabletStats(uid, topodatapb.TabletType_REPLICA)
}

func rdonly(uid uint32) LegacyTabletStats {
	return minimalTabletStats(uid, topodatapb.TabletType_RDONLY)
}

func minimalTabletStats(uid uint32, tabletType topodatapb.TabletType) LegacyTabletStats {
	return LegacyTabletStats{
		Tablet: &topodatapb.Tablet{
			Alias: &topodatapb.TabletAlias{
				Uid: uid},
		},
		Target: &querypb.Target{
			Keyspace:   "test_keyspace",
			Shard:      "-80",
			TabletType: tabletType,
		},
		Serving: true,
	}
}

func healthy(ts LegacyTabletStats) LegacyTabletStats {
	ts.Stats = &querypb.RealtimeStats{
		ReplicationLagSeconds: uint32(1),
	}
	return ts
}

func unhealthyLag(ts LegacyTabletStats) LegacyTabletStats {
	ts.Stats = &querypb.RealtimeStats{
		ReplicationLagSeconds: uint32(3600),
	}
	return ts
}

func unhealthyError(ts LegacyTabletStats) LegacyTabletStats {
	ts.Stats = &querypb.RealtimeStats{
		HealthError: "unhealthy",
	}
	return ts
}

func unhealthyLastError(ts LegacyTabletStats) LegacyTabletStats {
	ts.LastError = errors.New("err")
	return ts
}

func notServing(ts LegacyTabletStats) LegacyTabletStats {
	ts.Serving = false
	return ts
}
