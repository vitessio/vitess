/*
Copyright 2023 The Vitess Authors.

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

package balancer

import (
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/discovery"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
)

var nextTestTabletUID int

func createTestTablet(cell string) *discovery.TabletHealth {
	nextTestTabletUID++
	tablet := topo.NewTablet(uint32(nextTestTabletUID), cell, strconv.Itoa(nextTestTabletUID))
	tablet.PortMap["vt"] = 1
	tablet.PortMap["grpc"] = 2
	tablet.Keyspace = "k"
	tablet.Shard = "s"

	return &discovery.TabletHealth{
		Tablet:               tablet,
		Target:               &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Serving:              false,
		Stats:                nil,
		PrimaryTermStartTime: 0,
	}
}

// allow 2% fuzz
const FUZZ = 2

func fuzzyEquals(a, b int) bool {
	diff := a - b
	if diff < 0 {
		diff = -diff
	}
	return diff < a*FUZZ/100
}

func TestAllocateFlows(t *testing.T) {
	cases := []struct {
		test        string
		tablets     []*discovery.TabletHealth
		vtgateCells []string
	}{
		{
			"balanced one tablet per cell",
			[]*discovery.TabletHealth{
				createTestTablet("a"),
				createTestTablet("b"),
				createTestTablet("c"),
				createTestTablet("d"),
			},
			[]string{"a", "b", "c", "d"},
		},
		{
			"balanced multiple tablets per cell",
			[]*discovery.TabletHealth{
				createTestTablet("a"),
				createTestTablet("b"),
				createTestTablet("c"),
				createTestTablet("d"),
				createTestTablet("a"),
				createTestTablet("b"),
				createTestTablet("c"),
				createTestTablet("d"),
			},
			[]string{"a", "b", "c", "d"},
		},
		{
			"vtgate in cell with no tablets",
			[]*discovery.TabletHealth{
				createTestTablet("a"),
				createTestTablet("b"),
				createTestTablet("c"),
				createTestTablet("d"),
			},
			[]string{"a", "b", "c", "d", "e"},
		},
		{
			"vtgates in multiple cells with no tablets",
			[]*discovery.TabletHealth{
				createTestTablet("a"),
				createTestTablet("b"),
				createTestTablet("c"),
				createTestTablet("d"),
			},
			[]string{"a", "b", "c", "d", "e", "f", "g"},
		},
		{
			"imbalanced multiple tablets in one cell",
			[]*discovery.TabletHealth{
				createTestTablet("a"),
				createTestTablet("a"),
				createTestTablet("b"),
				createTestTablet("c"),
			},
			[]string{"a", "b", "c"},
		},
		{
			"imbalanced multiple tablets in multiple cells",
			[]*discovery.TabletHealth{
				createTestTablet("a"),
				createTestTablet("a"),
				createTestTablet("a"),
				createTestTablet("a"),
				createTestTablet("a"),
				createTestTablet("a"),
				createTestTablet("b"),
				createTestTablet("b"),
				createTestTablet("c"),
				createTestTablet("d"),
				createTestTablet("d"),
				createTestTablet("d"),
				createTestTablet("d"),
			},
			[]string{"a", "b", "c", "d"},
		},
		{
			"heavy imbalance",
			[]*discovery.TabletHealth{
				createTestTablet("a"),
				createTestTablet("a"),
				createTestTablet("a"),
				createTestTablet("a"),
				createTestTablet("a"),
				createTestTablet("a"),
				createTestTablet("b"),
				createTestTablet("c"),
				createTestTablet("c"),
			},
			[]string{"a", "b", "c", "d"},
		},
	}

	target := &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA}

	for _, c := range cases {
		t.Logf("\n\nTest Case: %s\n\n", c.test)

		tablets := c.tablets
		vtGateCells := c.vtgateCells

		tabletsByCell := make(map[string][]*discovery.TabletHealth)
		for _, tablet := range tablets {
			cell := tablet.Tablet.Alias.Cell
			tabletsByCell[cell] = append(tabletsByCell[cell], tablet)
		}

		allocationPerTablet := make(map[uint32]int)
		expectedPerTablet := ALLOCATION / len(tablets)

		expectedPerCell := make(map[string]int)
		for cell := range tabletsByCell {
			expectedPerCell[cell] = ALLOCATION / len(tablets) * len(tabletsByCell[cell])
		}

		// Run the balancer over each vtgate cell
		for _, localCell := range vtGateCells {
			b := NewTabletBalancer("balanced", localCell, strings.Join(vtGateCells, ",")).(*tabletBalancer)
			a := b.allocateFlows(tablets)
			b.allocations[discovery.KeyFromTarget(target)] = a

			t.Logf("Target Flows %v, Balancer: %s XXX %d %v \n", expectedPerCell, b.print(), len(b.allocations), b.allocations)

			// Accumulate all the output per tablet cell
			outflowPerCell := make(map[string]int)
			for _, outflow := range a.Outflows {
				for tabletCell, flow := range outflow {
					if flow < 0 {
						t.Errorf("balancer %v negative outflow", b.print())
					}
					outflowPerCell[tabletCell] += flow
				}
			}

			// Check in / out flow to each tablet cell
			for cell := range tabletsByCell {
				expectedForCell := expectedPerCell[cell]

				if !fuzzyEquals(a.Inflows[cell], expectedForCell) || !fuzzyEquals(outflowPerCell[cell], expectedForCell) {
					t.Errorf("Balancer {%s} ExpectedPerCell {%v} did not allocate correct flow to cell %s: expected %d, inflow %d outflow %d",
						b.print(), expectedPerCell, cell, expectedForCell, a.Inflows[cell], outflowPerCell[cell])
				}
			}

			// Accumulate the allocations for all runs to compare what the system does as a whole
			// when routing from all vtgate cells
			for uid, flow := range a.Allocation {
				allocationPerTablet[uid] += flow
			}
		}

		// Check that the allocations all add up
		for _, tablet := range tablets {
			uid := tablet.Tablet.Alias.Uid

			allocation := allocationPerTablet[uid]
			if !fuzzyEquals(allocation, expectedPerTablet) {
				t.Errorf("did not allocate full allocation to tablet %d: expected %d got %d",
					uid, expectedPerTablet, allocation)
			}
		}
	}
}

func TestBalancedShuffle(t *testing.T) {
	cases := []struct {
		test        string
		tablets     []*discovery.TabletHealth
		vtgateCells []string
	}{
		{
			"simple balanced",
			[]*discovery.TabletHealth{
				createTestTablet("a"),
				createTestTablet("b"),
				createTestTablet("c"),
				createTestTablet("d"),
			},

			[]string{"a", "b", "c", "d"},
		},
		{
			"simple unbalanced",
			[]*discovery.TabletHealth{
				createTestTablet("a"),
				createTestTablet("a"),
				createTestTablet("a"),
				createTestTablet("b"),
				createTestTablet("c"),
				createTestTablet("d"),
			},

			[]string{"a", "b", "c", "d"},
		},
		{
			"mixed unbalanced",
			[]*discovery.TabletHealth{
				createTestTablet("a"),
				createTestTablet("a"),
				createTestTablet("a"),
				createTestTablet("a"),
				createTestTablet("a"),
				createTestTablet("b"),
				createTestTablet("c"),
				createTestTablet("c"),
			},

			[]string{"a", "b", "c", "d"},
		},
	}

	target := &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA}
	for _, c := range cases {
		t.Logf("\n\nTest Case: %s\n\n", c.test)

		tablets := c.tablets
		vtGateCells := c.vtgateCells

		// test unbalanced distribution

		routed := make(map[uint32]int)

		expectedPerCell := make(map[string]int)
		for _, tablet := range tablets {
			cell := tablet.Tablet.Alias.Cell
			expectedPerCell[cell] += ALLOCATION / len(tablets)
		}

		// Run the algorithm a bunch of times to get a random enough sample
		N := 1000000
		for _, localCell := range vtGateCells {
			b := NewTabletBalancer("balanced", localCell, strings.Join(vtGateCells, ",")).(*tabletBalancer)

			for i := 0; i < N/len(vtGateCells); i++ {
				b.ShuffleTablets(target, tablets)
				if i == 0 {
					t.Logf("Target Flows %v, Balancer: %s\n", expectedPerCell, b.print())
					t.Logf(b.print())
				}

				routed[tablets[0].Tablet.Alias.Uid]++
			}
		}

		expected := N / len(tablets)
		delta := make(map[uint32]int)
		for _, tablet := range tablets {
			got := routed[tablet.Tablet.Alias.Uid]
			delta[tablet.Tablet.Alias.Uid] = got - expected
			if !fuzzyEquals(got, expected) {
				t.Errorf("routing to tablet %d got %d expected %d", tablet.Tablet.Alias.Uid, got, expected)
			}
		}
		t.Logf("Expected %d per tablet, Routed %v, Delta %v, Max delta %d", N/len(tablets), routed, delta, expected*FUZZ/100)
	}
}

func TestTopologyChanged(t *testing.T) {
	allTablets := []*discovery.TabletHealth{
		createTestTablet("a"),
		createTestTablet("a"),
		createTestTablet("b"),
		createTestTablet("b"),
	}

	target := &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA}

	b := NewTabletBalancer("balanced", "b", "a,b").(*tabletBalancer)

	N := 1

	// initially create a slice of tablets with just the two in cell a
	tablets := allTablets
	tablets = tablets[0:2]

	for i := 0; i < N; i++ {
		b.ShuffleTablets(target, tablets)
		allocation, totalAllocation := b.getAllocation(target, tablets)

		if totalAllocation != ALLOCATION/2 {
			t.Errorf("totalAllocation mismatch %s", b.print())
		}

		if allocation[allTablets[0].Tablet.Alias.Uid] != ALLOCATION/4 {
			t.Errorf("allocation mismatch %s, cell %s", b.print(), allTablets[0].Tablet.Alias.Cell)
		}

		if tablets[0].Tablet.Alias.Cell != "a" {
			t.Errorf("shuffle promoted wrong tablet from cell %s", tablets[0].Tablet.Alias.Cell)
		}
	}

	// Run again with the full topology, but without triggering a topology change
	// event to cause a reallocation
	tablets2 := allTablets
	for i := 0; i < N; i++ {
		b.ShuffleTablets(target, tablets2)

		allocation, totalAllocation := b.getAllocation(target, tablets2)

		if totalAllocation != ALLOCATION/2 {
			t.Errorf("totalAllocation mismatch %s", b.print())
		}

		if allocation[allTablets[0].Tablet.Alias.Uid] != ALLOCATION/4 {
			t.Errorf("allocation mismatch %s, cell %s", b.print(), allTablets[0].Tablet.Alias.Cell)
		}

		if tablets2[0].Tablet.Alias.Cell != "a" {
			t.Errorf("shuffle promoted wrong tablet from cell %s", tablets2[0].Tablet.Alias.Cell)
		}
	}

	// Trigger toplogy changed event, now traffic should go to b
	b.TopologyChanged()
	for i := 0; i < N; i++ {
		b.ShuffleTablets(target, tablets2)

		allocation, totalAllocation := b.getAllocation(target, tablets2)

		if totalAllocation != ALLOCATION/2 {
			t.Errorf("totalAllocation mismatch %s", b.print())
		}

		if allocation[allTablets[0].Tablet.Alias.Uid] != ALLOCATION/4 {
			t.Errorf("allocation mismatch %s, cell %s", b.print(), allTablets[0].Tablet.Alias.Cell)
		}

		if tablets2[0].Tablet.Alias.Cell != "b" {
			t.Errorf("shuffle promoted wrong tablet from cell %s", tablets2[0].Tablet.Alias.Cell)
		}
	}
}

func TestAffinityShuffle(t *testing.T) {
	balancer := NewTabletBalancer("affinity", "cell1", "")

	target := &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA}

	ts1 := &discovery.TabletHealth{
		Tablet:  topo.NewTablet(1, "cell1", "host1"),
		Target:  target,
		Serving: true,
		Stats:   &querypb.RealtimeStats{ReplicationLagSeconds: 1, CpuUsage: 0.2},
	}

	ts2 := &discovery.TabletHealth{
		Tablet:  topo.NewTablet(2, "cell1", "host2"),
		Target:  target,
		Serving: true,
		Stats:   &querypb.RealtimeStats{ReplicationLagSeconds: 1, CpuUsage: 0.2},
	}

	ts3 := &discovery.TabletHealth{
		Tablet:  topo.NewTablet(3, "cell2", "host3"),
		Target:  target,
		Serving: true,
		Stats:   &querypb.RealtimeStats{ReplicationLagSeconds: 1, CpuUsage: 0.2},
	}

	ts4 := &discovery.TabletHealth{
		Tablet:  topo.NewTablet(4, "cell2", "host4"),
		Target:  target,
		Serving: true,
		Stats:   &querypb.RealtimeStats{ReplicationLagSeconds: 1, CpuUsage: 0.2},
	}

	sameCellTablets := []*discovery.TabletHealth{ts1, ts2}
	diffCellTablets := []*discovery.TabletHealth{ts3, ts4}
	mixedTablets := []*discovery.TabletHealth{ts1, ts2, ts3, ts4}

	// repeat shuffling 10 times and every time the same cell tablets should be in the front
	for i := 0; i < 10; i++ {
		balancer.ShuffleTablets(target, sameCellTablets)
		assert.Len(t, sameCellTablets, 2, "Wrong number of TabletHealth")
		assert.Equal(t, sameCellTablets[0].Tablet.Alias.Cell, "cell1", "Wrong tablet cell")
		assert.Equal(t, sameCellTablets[1].Tablet.Alias.Cell, "cell1", "Wrong tablet cell")

		balancer.ShuffleTablets(target, diffCellTablets)
		assert.Len(t, diffCellTablets, 2, "should shuffle in only diff cell tablets")
		assert.Contains(t, diffCellTablets, ts3, "diffCellTablets should contain %v", ts3)
		assert.Contains(t, diffCellTablets, ts4, "diffCellTablets should contain %v", ts4)

		balancer.ShuffleTablets(target, mixedTablets)
		assert.Len(t, mixedTablets, 4, "should have 4 tablets, got %+v", mixedTablets)

		assert.Contains(t, mixedTablets[0:2], ts1, "should have same cell tablets in the front, got %+v", mixedTablets)
		assert.Contains(t, mixedTablets[0:2], ts2, "should have same cell tablets in the front, got %+v", mixedTablets)

		assert.Contains(t, mixedTablets[2:4], ts3, "should have diff cell tablets in the rear, got %+v", mixedTablets)
		assert.Contains(t, mixedTablets[2:4], ts4, "should have diff cell tablets in the rear, got %+v", mixedTablets)
	}
}
