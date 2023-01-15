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
	"fmt"
	"math/rand"
	"strings"
	"sync"

	"vitess.io/vitess/go/vt/discovery"
)

/*

The tabletBalancer probabalistically orders the list of available tablets into
a ranked order of preference in order to satisfy two high-level goals:

1. Balance the load across the available replicas
2. Prefer a replica in the same cell as the vtgate if possible

In some topologies this is trivial to accomplish by simply preferring tablets in the
local cell, assuming there are a proportional number of local tablets in each cell to
satisfy the inbound traffic to the vtgates in that cell.

However, for topologies with a relatively small number of tablets in each cell, a simple
affinity algorithm does not effectively balance the load.

As a simple example:

  Given three cells with vtgates, four replicas spread into those cells, where each vtgate
  receives an equal query share. If each routes only to its local cell, the tablets will be
  unbalanced since two of them receive 1/3 of the queries, but the two replicas in the same
  cell will only receive 1/6 of the queries.

  Cell A: 1/3 --> vtgate --> 1/3 => vttablet

  Cell B: 1/3 --> vtgate --> 1/3 => vttablet

  Cell C: 1/3 --> vtgate --> 1/6 => vttablet
                         \-> 1/6 => vttablet

Other topologies that can cause similar pathologies include cases where there may be cells
containing replicas but no local vtgates, and/or cells that have only vtgates but no replicas.

For these topologies, the tabletBalancer can be configured in a mode that proportionally assigns
the output flow to each tablet, regardless of whether or not the topology is balanced. The local
cell is still preferred where possible, but only as long as the global query balance is maintained.

To accomplish this goal, the balancer is optionally configured into balanced mode and is given:

* The list of cells that receive inbound traffic to vtgates
* The local cell where the vtgate exists
* The set of tablets and their cells (learned from discovery)

The model assumes equal probability of a query coming from each cell that has a vtgate, i.e.
traffic is effectively load balanced between the cells with vtgates.

Given that information, the balancer builds a simple model to determine how much query load
would go to each tablet if vtgate only routed to its local cell. Then if any tablets are
unbalanced, it shifts the desired allocation away from the local cell preference in order to
even out the query load.

Based on this global model, the vtgate then probabalistically picks a destination for each
query to be sent by ordering the available tablets accordingly.

Assuming each vtgate is configured with and discovers the same information about the topology,
then each should come the the same conclusion about the global flows, and cooperatively should
converge on the desired balanced query load.

*/

type TabletBalancer interface {
	// Randomly shuffle the tablets into an order for routing queries
	ShuffleTablets(tablets []*discovery.TabletHealth)

	// Callback when the topology changes to invalidate any cached state
	TopologyChanged()
}

func NewTabletBalancer(mode, localCell, vtGateCells string) TabletBalancer {
	if mode == "balanced" {
		vtgateCells := strings.Split(vtGateCells, ",")
		return &tabletBalancer{isBalanced: true, localCell: localCell, vtGateCells: vtgateCells}
	}
	return &tabletBalancer{isBalanced: false, localCell: localCell}
}

type tabletBalancer struct {
	//
	// Configuration
	//
	isBalanced bool

	// The local cell for the vtgate
	localCell string

	// The set of cells that have vtgates
	vtGateCells []string

	// mu protects the data structures below
	mu sync.Mutex

	//
	// Data structures built during the algorithm, exposed in the object for unit testing purposes
	//

	// Target flow per cell based on the number of tablets discovered in the cell
	//
	// XXX CHANGE TO expected inflow per cell
	target map[string]int

	// Input flows allocated for each cell
	inflows map[string]int

	// Output flows from each vtgate cell to each target cell
	outflows map[string]map[string]int

	// Allocation routed to each tablet from the local cell used for ranking
	allocation      map[uint32]int
	totalAllocation int
}

func (b *tabletBalancer) print() string {
	return fmt.Sprintf("LocalCell: %s, VtGateCells: %s, outflows: %v, inflows: %v, allocation:%v, total:%v",
		b.localCell, b.vtGateCells, b.outflows, b.inflows, b.allocation, b.totalAllocation)
}

// ShuffleTablets is the main entry point to the balancer.
//
// It shuffles the tablets into a preference list for routing a given query.
// However, since all tablets should be healthy, the query will almost always go
// to the first tablet in the list, so the balancer ranking algoritm randomly
// shuffles the list to break ties, then chooses a weighted random selection
// based on the balance alorithm to promote to the first in the set.
func (b *tabletBalancer) ShuffleTablets(tablets []*discovery.TabletHealth) {

	numTablets := len(tablets)

	if b.isBalanced {
		allocationMap, totalAllocation := b.getAllocation(tablets)

		rand.Shuffle(numTablets, func(i, j int) { tablets[i], tablets[j] = tablets[j], tablets[i] })

		// Do another O(n) seek through the list to effect the weighted sample by picking
		// a random point in the allocation space and seeking forward in the list of (randomized)
		// tablets to that point, promoting the tablet to the head of the list.
		r := rand.Intn(totalAllocation)
		for i := 0; i < numTablets; i++ {
			flow := allocationMap[tablets[i].Tablet.Alias.Uid]
			if r < flow {
				tablets[0], tablets[i] = tablets[i], tablets[0]
				break
			}
			r -= flow
		}
	} else {
		// Randomly shuffle, then boost same-cell to the front
		rand.Shuffle(numTablets, func(i, j int) { tablets[i], tablets[j] = tablets[j], tablets[i] })
		i := 0
		for j := 0; j < numTablets; j++ {
			if tablets[j].Tablet.Alias.Cell == b.localCell {
				tablets[i], tablets[j] = tablets[j], tablets[i]
				i++
			}
		}
	}
}

// TopologyChanged is a callback to indicate the topology changed and any cached
// allocations should be cleared
func (b *tabletBalancer) TopologyChanged() {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Signal that the topology should be rebuilt
	b.totalAllocation = 0
}

// To stick with integer arithmetic, use 1,000,000 as the full load
const ALLOCATION = 1000000

func (b *tabletBalancer) allocateFlows(allTablets []*discovery.TabletHealth) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Initialization: Set up some data structures and derived values
	b.target = make(map[string]int)
	b.inflows = make(map[string]int)
	b.outflows = make(map[string]map[string]int)
	b.allocation = make(map[uint32]int)
	flowPerVtgateCell := ALLOCATION / len(b.vtGateCells)
	flowPerTablet := ALLOCATION / len(allTablets)
	cellExistsWithNoTablets := false

	for _, th := range allTablets {
		b.target[th.Tablet.Alias.Cell] += flowPerTablet
	}

	//
	// First pass: Allocate vtgate flow to the local cell where the vtgate exists
	// and along the way figure out if there are any vtgates with no local tablets.
	//
	for _, cell := range b.vtGateCells {
		outflow := map[string]int{}
		target := b.target[cell]

		if target > 0 {
			b.inflows[cell] += flowPerVtgateCell
			outflow[cell] = flowPerVtgateCell
		} else {
			cellExistsWithNoTablets = true
		}

		b.outflows[cell] = outflow
	}

	//
	// Figure out if there is a shortfall
	//
	underAllocated := make(map[string]int)
	unbalancedFlow := 0
	for cell, allocation := range b.target {
		if b.inflows[cell] < allocation {
			underAllocated[cell] = allocation - b.inflows[cell]
			unbalancedFlow += underAllocated[cell]
		}
	}

	//
	// Second pass: if there are any vtgates with no local tablets, allocate the underallocated amount
	// proportionally to all cells that may need it
	//
	if cellExistsWithNoTablets {
		for _, vtgateCell := range b.vtGateCells {
			target := b.target[vtgateCell]
			if target != 0 {
				continue
			}

			for underAllocatedCell, underAllocatedFlow := range underAllocated {
				allocation := flowPerVtgateCell * underAllocatedFlow / unbalancedFlow
				b.inflows[underAllocatedCell] += allocation
				b.outflows[vtgateCell][underAllocatedCell] += allocation
			}
		}

		// Recompute underallocated after these flows were assigned
		unbalancedFlow = 0
		underAllocated = make(map[string]int)
		for cell, allocation := range b.target {
			if b.inflows[cell] < allocation {
				underAllocated[cell] = allocation - b.inflows[cell]
				unbalancedFlow += underAllocated[cell]
			}
		}
	}

	//
	// Third pass: Shift remaining imbalance if any cell is over/under allocated after
	// assigning local cell traffic and distributing load from cells without tablets.
	//
	if /* fudge for integer arithmetic */ unbalancedFlow > 10 {

		// cells which are overallocated
		overAllocated := make(map[string]int)
		for cell, allocation := range b.target {
			if b.inflows[cell] > allocation {
				overAllocated[cell] = b.inflows[cell] - allocation
			}
		}

		// fmt.Printf("outflows %v over %v under %v\n", b.outflows, overAllocated, underAllocated)

		//
		// For each overallocated cell, proportionally shift flow from targets that are overallocated
		// to targets that are underallocated.
		//
		// Note this is an O(N^3) loop, but only over the cells which need adjustment.
		//
		for _, vtgateCell := range b.vtGateCells {
			for underAllocatedCell, underAllocatedFlow := range underAllocated {
				for overAllocatedCell, overAllocatedFlow := range overAllocated {

					currentFlow := b.outflows[vtgateCell][overAllocatedCell]
					if currentFlow == 0 {
						continue
					}

					// Shift a proportional fraction of the amount that the cell is currently allocated weighted
					// by the fraction that this vtgate cell is already sending to the overallocated cell, and the
					// fraction that the new target is underallocated
					//
					// Note that the operator order matters -- multiplications need to occur before divisions
					// to avoid truncating the integer values.
					shiftFlow := overAllocatedFlow * currentFlow * underAllocatedFlow / b.inflows[overAllocatedCell] / unbalancedFlow

					//fmt.Printf("shift %d %s %s -> %s (over %d current %d in %d under %d unbalanced %d) \n", shiftFlow, vtgateCell, overAllocatedCell, underAllocatedCell,
					//	overAllocatedFlow, currentFlow, b.inflows[overAllocatedCell], underAllocatedFlow, unbalancedFlow)

					b.outflows[vtgateCell][overAllocatedCell] -= shiftFlow
					b.inflows[overAllocatedCell] -= shiftFlow

					b.inflows[underAllocatedCell] += shiftFlow
					b.outflows[vtgateCell][underAllocatedCell] += shiftFlow
				}
			}
		}
	}

	//
	// Finally, once the cell flows are all adjusted, figure out the local allocation to each
	// tablet in the target cells
	//
	outflow := b.outflows[b.localCell]
	for _, tablet := range allTablets {
		cell := tablet.Tablet.Alias.Cell
		flow := outflow[cell]
		if flow > 0 {
			b.allocation[tablet.Tablet.Alias.Uid] = flow * flowPerTablet / b.target[cell]
			b.totalAllocation += flow * flowPerTablet / b.target[cell]
		}
	}
}

// getAllocation builds the allocation map if needed and returns a copy
func (b *tabletBalancer) getAllocation(tablets []*discovery.TabletHealth) (map[uint32]int, int) {

	if b.totalAllocation == 0 {
		b.allocateFlows(tablets)
	}

	return b.allocation, b.totalAllocation
}
