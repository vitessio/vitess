/*
Copyright 2024 The Vitess Authors.

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
			b := NewTabletBalancer(localCell, vtGateCells).(*tabletBalancer)
			a := b.allocateFlows(tablets)
			b.allocations[discovery.KeyFromTarget(target)] = a

			t.Logf("Target Flows %v, Balancer: %s, Allocations: %v \n", expectedPerCell, b.print(), b.allocations)

			// Accumulate all the output per tablet cell
			outflowPerCell := make(map[string]int)
			for _, outflow := range a.Outflows {
				for tabletCell, flow := range outflow {
					assert.GreaterOrEqual(t, flow, 0, b.print())
					outflowPerCell[tabletCell] += flow
				}
			}

			// Check in / out flow to each tablet cell
			for cell := range tabletsByCell {
				expectedForCell := expectedPerCell[cell]

				assert.InEpsilonf(t, expectedForCell, a.Inflows[cell], 0.01,
					"did not allocate correct inflow to cell %s. Balancer {%s} ExpectedPerCell {%v}",
					cell, b.print(), expectedPerCell)
				assert.InEpsilonf(t, expectedForCell, outflowPerCell[cell], 0.01,
					"did not allocate correct outflow to cell %s. Balancer {%s} ExpectedPerCell {%v}",
					cell, b.print(), expectedPerCell)
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
			assert.InEpsilonf(t, expectedPerTablet, allocation, 0.01,
				"did not allocate full allocation to tablet %d", uid)
		}
	}
}

func TestBalancedPick(t *testing.T) {
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
		{
			"one target same cell",
			[]*discovery.TabletHealth{
				createTestTablet("a"),
			},

			[]string{"a"},
		},
		{
			"one target other cell",
			[]*discovery.TabletHealth{
				createTestTablet("a"),
			},

			[]string{"b", "c", "d"},
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
			b := NewTabletBalancer(localCell, vtGateCells).(*tabletBalancer)

			for i := 0; i < N/len(vtGateCells); i++ {
				th := b.Pick(target, tablets)
				if i == 0 {
					t.Logf("Target Flows %v, Balancer: %s\n", expectedPerCell, b.print())
				}

				routed[th.Tablet.Alias.Uid]++
			}
		}

		expected := N / len(tablets)
		delta := make(map[uint32]int)
		for _, tablet := range tablets {
			got := routed[tablet.Tablet.Alias.Uid]
			delta[tablet.Tablet.Alias.Uid] = got - expected
			assert.InEpsilonf(t, expected, got, 0.01,
				"routing to tablet %d", tablet.Tablet.Alias.Uid)
		}
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

	b := NewTabletBalancer("b", []string{"a", "b"}).(*tabletBalancer)

	N := 1

	// initially create a slice of tablets with just the two in cell a
	tablets := allTablets
	tablets = tablets[0:2]

	for i := 0; i < N; i++ {
		th := b.Pick(target, tablets)
		allocation, totalAllocation := b.getAllocation(target, tablets)

		assert.Equalf(t, ALLOCATION/2, totalAllocation, "totalAllocation mismatch %s", b.print())
		assert.Equalf(t, ALLOCATION/4, allocation[th.Tablet.Alias.Uid], "allocation mismatch %s, cell %s", b.print(), allTablets[0].Tablet.Alias.Cell)
		assert.Equalf(t, "a", th.Tablet.Alias.Cell, "shuffle promoted wrong tablet from cell %s", allTablets[0].Tablet.Alias.Cell)
	}

	// Run again with the full topology. Now traffic should go to cell b
	for i := 0; i < N; i++ {
		th := b.Pick(target, allTablets)

		allocation, totalAllocation := b.getAllocation(target, allTablets)

		assert.Equalf(t, ALLOCATION/2, totalAllocation, "totalAllocation mismatch %s", b.print())
		assert.Equalf(t, ALLOCATION/4, allocation[th.Tablet.Alias.Uid], "allocation mismatch %s, cell %s", b.print(), allTablets[0].Tablet.Alias.Cell)
		assert.Equalf(t, "b", th.Tablet.Alias.Cell, "shuffle promoted wrong tablet from cell %s", allTablets[0].Tablet.Alias.Cell)
	}

	// Run again with a node in the topology replaced.
	newTablet := createTestTablet("b")
	allTablets[2] = newTablet
	for i := 0; i < N; i++ {
		th := b.Pick(target, allTablets)

		allocation, totalAllocation := b.getAllocation(target, allTablets)

		assert.Equalf(t, ALLOCATION/2, totalAllocation, "totalAllocation mismatch %s", b.print())
		assert.Equalf(t, ALLOCATION/4, allocation[th.Tablet.Alias.Uid], "allocation mismatch %s, cell %s", b.print(), allTablets[0].Tablet.Alias.Cell)
		assert.Equalf(t, "b", th.Tablet.Alias.Cell, "shuffle promoted wrong tablet from cell %s", allTablets[0].Tablet.Alias.Cell)
	}

}
