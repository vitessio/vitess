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
	"fmt"
	"testing"
	"time"

	"vitess.io/vitess/go/test/utils"

	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/topo"
)

func init() {
	lowReplicationLag = 30 * time.Second
	highReplicationLagMinServing = 2 * time.Hour
	minNumTablets = 2
	legacyReplicationLagAlgorithm = true
}

// testSetLegacyReplicationLagAlgorithm is a test helper function, if this is used by a production code path, something is wrong.
func testSetLegacyReplicationLagAlgorithm(newLegacy bool) {
	legacyReplicationLagAlgorithm = newLegacy
}

// testSetMinNumTablets is a test helper function, if this is used by a production code path, something is wrong.
func testSetMinNumTablets(newMin int) {
	minNumTablets = newMin
}

func TestFilterByReplicationLagUnhealthy(t *testing.T) {
	// 1 healthy serving tablet, 1 not healthy
	ts1 := &TabletHealth{
		Tablet:  topo.NewTablet(1, "cell", "host1"),
		Serving: true,
		Stats:   &querypb.RealtimeStats{},
	}
	ts2 := &TabletHealth{
		Tablet:  topo.NewTablet(2, "cell", "host2"),
		Serving: false,
		Stats:   &querypb.RealtimeStats{},
	}
	got := FilterStatsByReplicationLag([]*TabletHealth{ts1, ts2})
	want := []*TabletHealth{ts1}
	mustMatch(t, want, got, "FilterStatsByReplicationLag")
}

func TestFilterByReplicationLag(t *testing.T) {
	// Use simplified logic
	testSetLegacyReplicationLagAlgorithm(false)

	cases := []struct {
		description string
		input       []uint32
		output      []uint32
	}{
		{
			"0 tablet",
			[]uint32{},
			[]uint32{},
		},
		{
			"lags of (1s) - return all items with low lag.",
			[]uint32{1},
			[]uint32{1},
		},
		{
			"lags of (1s, 1s, 1s, 30s) - return all items with low lag.",
			[]uint32{1, 1, 1, 30},
			[]uint32{1, 1, 1, 30},
		},
		{
			"lags of (1s, 1s, 1s, 40m, 40m, 40m) - return all items with low lag.",
			[]uint32{1, 1, 1, 40 * 60, 40 * 60, 40 * 60},
			[]uint32{1, 1, 1},
		},
		{
			"lags of (1s, 40m, 40m, 40m) - return at least 2 items if they don't have very high lag.",
			[]uint32{1, 40 * 60, 40 * 60, 40 * 60},
			[]uint32{1, 40 * 60},
		},
		{
			"lags of (30m, 35m, 40m, 45m) - return at least 2 items if they don't have very high lag.",
			[]uint32{30 * 60, 35 * 60, 40 * 60, 45 * 60},
			[]uint32{30 * 60, 35 * 60},
		},
		{
			"lags of (2h, 3h, 4h, 5h) - return <2 items if the others have very high lag.",
			[]uint32{2 * 60 * 60, 3 * 60 * 60, 4 * 60 * 60, 5 * 60 * 60},
			[]uint32{2 * 60 * 60},
		},
		{
			"lags of (3h, 30h) - return nothing if all have very high lag.",
			[]uint32{3 * 60 * 60, 30 * 60 * 60},
			[]uint32{},
		},
	}

	for _, tc := range cases {
		lts := make([]*TabletHealth, len(tc.input))
		for i, lag := range tc.input {
			lts[i] = &TabletHealth{
				Tablet:  topo.NewTablet(uint32(i+1), "cell", fmt.Sprintf("host-%vs-behind", lag)),
				Serving: true,
				Stats:   &querypb.RealtimeStats{ReplicationLagSeconds: lag},
			}
		}
		got := FilterStatsByReplicationLag(lts)
		if len(got) != len(tc.output) {
			t.Errorf("FilterStatsByReplicationLag(%v) failed: got output:\n%v\nExpected: %v", tc.description, got, tc.output)
			continue
		}
		for i, elag := range tc.output {
			if got[i].Stats.ReplicationLagSeconds != elag {
				t.Errorf("FilterStatsByReplicationLag(%v) failed: got output:\n%v\nExpected value index %v to be %v", tc.description, got, i, elag)
			}
		}
	}

	// Reset to the default
	testSetLegacyReplicationLagAlgorithm(true)
}

func TestFilterByReplicationLagThreeTabletMin(t *testing.T) {
	// Use at least 3 tablets if possible
	testSetMinNumTablets(3)
	// lags of (1s, 1s, 10m, 11m) - returns at least32 items where the slightly delayed ones that are returned are the 10m and 11m ones.
	ts1 := &TabletHealth{
		Tablet:  topo.NewTablet(1, "cell", "host1"),
		Serving: true,
		Stats:   &querypb.RealtimeStats{ReplicationLagSeconds: 1},
	}
	ts2 := &TabletHealth{
		Tablet:  topo.NewTablet(2, "cell", "host2"),
		Serving: true,
		Stats:   &querypb.RealtimeStats{ReplicationLagSeconds: 1},
	}
	ts3 := &TabletHealth{
		Tablet:  topo.NewTablet(3, "cell", "host3"),
		Serving: true,
		Stats:   &querypb.RealtimeStats{ReplicationLagSeconds: 10 * 60},
	}
	ts4 := &TabletHealth{
		Tablet:  topo.NewTablet(4, "cell", "host4"),
		Serving: true,
		Stats:   &querypb.RealtimeStats{ReplicationLagSeconds: 11 * 60},
	}
	got := FilterStatsByReplicationLag([]*TabletHealth{ts1, ts2, ts3, ts4})
	want := []*TabletHealth{ts1, ts2, ts3}
	mustMatch(t, want, got, "FilterStatsByReplicationLag")

	// lags of (11m, 10m, 1s, 1s) - reordered tablets returns the same 3 items where the slightly delayed one that is returned is the 10m and 11m ones.
	ts1 = &TabletHealth{
		Tablet:  topo.NewTablet(1, "cell", "host1"),
		Serving: true,
		Stats:   &querypb.RealtimeStats{ReplicationLagSeconds: 11 * 60},
	}
	ts2 = &TabletHealth{
		Tablet:  topo.NewTablet(2, "cell", "host2"),
		Serving: true,
		Stats:   &querypb.RealtimeStats{ReplicationLagSeconds: 10 * 60},
	}
	ts3 = &TabletHealth{
		Tablet:  topo.NewTablet(3, "cell", "host3"),
		Serving: true,
		Stats:   &querypb.RealtimeStats{ReplicationLagSeconds: 1},
	}
	ts4 = &TabletHealth{
		Tablet:  topo.NewTablet(4, "cell", "host4"),
		Serving: true,
		Stats:   &querypb.RealtimeStats{ReplicationLagSeconds: 1},
	}
	got = FilterStatsByReplicationLag([]*TabletHealth{ts1, ts2, ts3, ts4})
	want = []*TabletHealth{ts3, ts4, ts2}
	mustMatch(t, want, got, "FilterStatsByReplicationLag")
	// Reset to the default
	testSetMinNumTablets(2)
}

func TestFilterStatsByReplicationLagOneTabletMin(t *testing.T) {
	// Use at least 1 tablets if possible
	testSetMinNumTablets(1)
	// lags of (1s, 100m) - return only healthy tablet if that is all that is available.
	ts1 := &TabletHealth{
		Tablet:  topo.NewTablet(1, "cell", "host1"),
		Serving: true,
		Stats:   &querypb.RealtimeStats{ReplicationLagSeconds: 1},
	}
	ts2 := &TabletHealth{
		Tablet:  topo.NewTablet(2, "cell", "host2"),
		Serving: true,
		Stats:   &querypb.RealtimeStats{ReplicationLagSeconds: 100 * 60},
	}
	got := FilterStatsByReplicationLag([]*TabletHealth{ts1, ts2})
	want := []*TabletHealth{ts1}
	mustMatch(t, want, got, "FilterStatsByReplicationLag")

	// lags of (1m, 100m) - return only healthy tablet if that is all that is healthy enough.
	ts1 = &TabletHealth{
		Tablet:  topo.NewTablet(1, "cell", "host1"),
		Serving: true,
		Stats:   &querypb.RealtimeStats{ReplicationLagSeconds: 1 * 60},
	}
	ts2 = &TabletHealth{
		Tablet:  topo.NewTablet(2, "cell", "host2"),
		Serving: true,
		Stats:   &querypb.RealtimeStats{ReplicationLagSeconds: 100 * 60},
	}
	got = FilterStatsByReplicationLag([]*TabletHealth{ts1, ts2})
	want = []*TabletHealth{ts1}
	utils.MustMatch(t, want, got, "FilterStatsByReplicationLag")
	// Reset to the default
	testSetMinNumTablets(2)
}
