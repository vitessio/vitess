/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package discovery

import (
	"testing"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestRemoveUnhealthyTablets(t *testing.T) {
	var testcases = []struct {
		desc  string
		input []TabletStats
		want  []TabletStats
	}{
		{
			desc:  "tablets missing Stats",
			input: []TabletStats{replica(1), replica(2)},
			want:  []TabletStats{},
		},
		{
			desc:  "all tablets healthy",
			input: []TabletStats{healthy(replica(1)), healthy(replica(2))},
			want:  []TabletStats{healthy(replica(1)), healthy(replica(2))},
		},
		{
			desc:  "one unhealthy tablet (error)",
			input: []TabletStats{healthy(replica(1)), unhealthyError(replica(2))},
			want:  []TabletStats{healthy(replica(1))},
		},
		{
			desc:  "one unhealthy tablet (lag)",
			input: []TabletStats{healthy(replica(1)), unhealthyLag(replica(2))},
			want:  []TabletStats{healthy(replica(1))},
		},
		{
			desc:  "no filtering by tablet type",
			input: []TabletStats{healthy(master(1)), healthy(replica(2)), healthy(rdonly(3))},
			want:  []TabletStats{healthy(master(1)), healthy(replica(2)), healthy(rdonly(3))},
		},
		{
			desc:  "non-serving tablets won't be removed",
			input: []TabletStats{notServing(healthy(replica(1)))},
			want:  []TabletStats{notServing(healthy(replica(1)))},
		},
	}

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

func master(uid uint32) TabletStats {
	return minimalTabletStats(uid, topodatapb.TabletType_MASTER)
}

func replica(uid uint32) TabletStats {
	return minimalTabletStats(uid, topodatapb.TabletType_REPLICA)
}

func rdonly(uid uint32) TabletStats {
	return minimalTabletStats(uid, topodatapb.TabletType_RDONLY)
}

func minimalTabletStats(uid uint32, tabletType topodatapb.TabletType) TabletStats {
	return TabletStats{
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

func healthy(ts TabletStats) TabletStats {
	ts.Stats = &querypb.RealtimeStats{
		SecondsBehindMaster: uint32(1),
	}
	return ts
}

func unhealthyLag(ts TabletStats) TabletStats {
	ts.Stats = &querypb.RealtimeStats{
		SecondsBehindMaster: uint32(3600),
	}
	return ts
}

func unhealthyError(ts TabletStats) TabletStats {
	ts.Stats = &querypb.RealtimeStats{
		HealthError: "unhealthy",
	}
	return ts
}

func notServing(ts TabletStats) TabletStats {
	ts.Serving = false
	return ts
}
