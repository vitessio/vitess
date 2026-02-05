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
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"sync"

	"vitess.io/vitess/go/vt/discovery"
	querypb "vitess.io/vitess/go/vt/proto/query"
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

For these topologies, the tabletBalancer proportionally assigns the output flow to each tablet,
preferring the local cell where possible, but only as long as the global query balance is
maintained.

To accomplish this goal, the balancer is given:

* The list of cells that receive inbound traffic to vtgates
* The local cell where the vtgate exists
* The set of tablets and their cells (learned from discovery)

The model assumes there is an equal probablility of a query coming from each vtgate cell, i.e.
traffic is effectively load balanced between the cells with vtgates.

Given that information, the balancer builds a simple model to determine how much query load
would go to each tablet if vtgate only routed to its local cell. Then if any tablets are
unbalanced, it shifts the desired allocation away from the local cell preference in order to
even out the query load.

Based on this global model, the vtgate then probabalistically picks a destination for each
query to be sent and uses these weights to order the available tablets accordingly.

Assuming each vtgate is configured with and discovers the same information about the topology,
and the input flow is balanced across the vtgate cells (as mentioned above), then each vtgate
should come the the same conclusion about the global flows, and cooperatively should
converge on the desired balanced query load.

*/

type TabletBalancer interface {
	// Pick is the main entry point to the balancer. Returns the best tablet out of the list
	// for a given query to maintain the desired balanced allocation over multiple executions.
	Pick(target *querypb.Target, tablets []*discovery.TabletHealth) *discovery.TabletHealth

	// DebugHandler provides a summary of tablet balancer state
	DebugHandler(w http.ResponseWriter, r *http.Request)
}

func NewTabletBalancer(localCell string, vtGateCells []string) TabletBalancer {
	return &tabletBalancer{
		localCell:   localCell,
		vtGateCells: vtGateCells,
		allocations: map[discovery.KeyspaceShardTabletType]*targetAllocation{},
	}
}

type tabletBalancer struct {
	//
	// Configuration
	//

	// The local cell for the vtgate
	localCell string

	// The set of cells that have vtgates
	vtGateCells []string

	// mu protects the allocation map
	mu sync.Mutex

	//
	// Allocations for balanced mode, calculated once per target and invalidated
	// whenever the topology changes.
	//
	allocations map[discovery.KeyspaceShardTabletType]*targetAllocation
}

type targetAllocation struct {
	// Target flow per cell based on the number of tablets discovered in the cell
	Target map[string]int // json:target

	// Input flows allocated for each cell
	Inflows map[string]int

	// Output flows from each vtgate cell to each target cell
	Outflows map[string]map[string]int

	// Allocation routed to each tablet from the local cell used for ranking
	Allocation map[uint32]int

	// Tablets that local cell does not route to
	Unallocated map[uint32]struct{}

	// Total allocation which is basically 1,000,000 / len(vtgatecells)
	TotalAllocation int
}

func (b *tabletBalancer) print() string {
	allocations, _ := json.Marshal(&b.allocations)
	return fmt.Sprintf("LocalCell: %s, VtGateCells: %s, allocations: %s",
		b.localCell, b.vtGateCells, string(allocations))
}

func (b *tabletBalancer) DebugHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintf(w, "Local Cell: %v\r\n", b.localCell)
	fmt.Fprintf(w, "Vtgate Cells: %v\r\n", b.vtGateCells)

	b.mu.Lock()
	defer b.mu.Unlock()
	allocations, _ := json.MarshalIndent(b.allocations, "", "  ")
	fmt.Fprintf(w, "Allocations: %v\r\n", string(allocations))
}

// Pick is the main entry point to the balancer.
//
// Given the total allocation for the set of tablets, choose the best target
// by a weighted random sample so that over time the system will achieve the
// desired balanced allocation.
func (b *tabletBalancer) Pick(target *querypb.Target, tablets []*discovery.TabletHealth) *discovery.TabletHealth {

	numTablets := len(tablets)
	if numTablets == 0 {
		return nil
	}

	allocationMap, totalAllocation := b.getAllocation(target, tablets)

	r := rand.IntN(totalAllocation)
	for i := 0; i < numTablets; i++ {
		flow := allocationMap[tablets[i].Tablet.Alias.Uid]
		if r < flow {
			return tablets[i]
		}
		r -= flow
	}

	return tablets[0]
}

// To stick with integer arithmetic, use 1,000,000 as the full load
const ALLOCATION = 1000000

func (b *tabletBalancer) allocateFlows(allTablets []*discovery.TabletHealth) *targetAllocation {
	// Initialization: Set up some data structures and derived values
	a := targetAllocation{
		Target:      map[string]int{},
		Inflows:     map[string]int{},
		Outflows:    map[string]map[string]int{},
		Allocation:  map[uint32]int{},
		Unallocated: map[uint32]struct{}{},
	}
	flowPerVtgateCell := ALLOCATION / len(b.vtGateCells)
	flowPerTablet := ALLOCATION / len(allTablets)
	cellExistsWithNoTablets := false

	for _, th := range allTablets {
		a.Target[th.Tablet.Alias.Cell] += flowPerTablet
	}

	//
	// First pass: Allocate vtgate flow to the local cell where the vtgate exists
	// and along the way figure out if there are any vtgates with no local tablets.
	//
	for _, cell := range b.vtGateCells {
		outflow := map[string]int{}
		target := a.Target[cell]

		if target > 0 {
			a.Inflows[cell] += flowPerVtgateCell
			outflow[cell] = flowPerVtgateCell
		} else {
			cellExistsWithNoTablets = true
		}

		a.Outflows[cell] = outflow
	}

	//
	// Figure out if there is a shortfall
	//
	underAllocated := make(map[string]int)
	unbalancedFlow := 0
	for cell, allocation := range a.Target {
		if a.Inflows[cell] < allocation {
			underAllocated[cell] = allocation - a.Inflows[cell]
			unbalancedFlow += underAllocated[cell]
		}
	}

	//
	// Second pass: if there are any vtgates with no local tablets, allocate the underallocated amount
	// proportionally to all cells that may need it
	//
	if cellExistsWithNoTablets {
		for _, vtgateCell := range b.vtGateCells {
			target := a.Target[vtgateCell]
			if target != 0 {
				continue
			}

			for underAllocatedCell, underAllocatedFlow := range underAllocated {
				allocation := flowPerVtgateCell * underAllocatedFlow / unbalancedFlow
				a.Inflows[underAllocatedCell] += allocation
				a.Outflows[vtgateCell][underAllocatedCell] += allocation
			}
		}

		// Recompute underallocated after these flows were assigned
		unbalancedFlow = 0
		underAllocated = make(map[string]int)
		for cell, allocation := range a.Target {
			if a.Inflows[cell] < allocation {
				underAllocated[cell] = allocation - a.Inflows[cell]
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
		for cell, allocation := range a.Target {
			if a.Inflows[cell] > allocation {
				overAllocated[cell] = a.Inflows[cell] - allocation
			}
		}

		// fmt.Printf("outflows %v over %v under %v\n", a.Outflows, overAllocated, underAllocated)

		//
		// For each overallocated cell, proportionally shift flow from targets that are overallocated
		// to targets that are underallocated.
		//
		// Note this is an O(N^3) loop, but only over the cells which need adjustment.
		//
		for _, vtgateCell := range b.vtGateCells {
			for underAllocatedCell, underAllocatedFlow := range underAllocated {
				for overAllocatedCell, overAllocatedFlow := range overAllocated {

					currentFlow := a.Outflows[vtgateCell][overAllocatedCell]
					if currentFlow == 0 {
						continue
					}

					// Shift a proportional fraction of the amount that the cell is currently allocated weighted
					// by the fraction that this vtgate cell is already sending to the overallocated cell, and the
					// fraction that the new target is underallocated
					//
					// Note that the operator order matters -- multiplications need to occur before divisions
					// to avoid truncating the integer values.
					shiftFlow := overAllocatedFlow * currentFlow * underAllocatedFlow / a.Inflows[overAllocatedCell] / unbalancedFlow

					//fmt.Printf("shift %d %s %s -> %s (over %d current %d in %d under %d unbalanced %d) \n", shiftFlow, vtgateCell, overAllocatedCell, underAllocatedCell,
					//	overAllocatedFlow, currentFlow, a.Inflows[overAllocatedCell], underAllocatedFlow, unbalancedFlow)

					a.Outflows[vtgateCell][overAllocatedCell] -= shiftFlow
					a.Inflows[overAllocatedCell] -= shiftFlow

					a.Inflows[underAllocatedCell] += shiftFlow
					a.Outflows[vtgateCell][underAllocatedCell] += shiftFlow
				}
			}
		}
	}

	//
	// Finally, once the cell flows are all adjusted, figure out the local allocation to each
	// tablet in the target cells
	//
	outflow := a.Outflows[b.localCell]
	for _, tablet := range allTablets {
		cell := tablet.Tablet.Alias.Cell
		flow := outflow[cell]
		if flow > 0 {
			a.Allocation[tablet.Tablet.Alias.Uid] = flow * flowPerTablet / a.Target[cell]
			a.TotalAllocation += flow * flowPerTablet / a.Target[cell]
		} else {
			a.Unallocated[tablet.Tablet.Alias.Uid] = struct{}{}
		}
	}

	return &a
}

// getAllocation builds the allocation map if needed and returns a copy of the map
func (b *tabletBalancer) getAllocation(target *querypb.Target, tablets []*discovery.TabletHealth) (map[uint32]int, int) {
	b.mu.Lock()
	defer b.mu.Unlock()

	allocation, exists := b.allocations[discovery.KeyFromTarget(target)]
	if exists && (len(allocation.Allocation)+len(allocation.Unallocated)) == len(tablets) {
		mismatch := false
		for _, tablet := range tablets {
			if _, ok := allocation.Allocation[tablet.Tablet.Alias.Uid]; !ok {
				if _, ok := allocation.Unallocated[tablet.Tablet.Alias.Uid]; !ok {
					mismatch = true
					break
				}
			}
		}
		if !mismatch {
			// No change in tablets for this target. Return computed allocation
			return allocation.Allocation, allocation.TotalAllocation
		}
	}

	allocation = b.allocateFlows(tablets)
	b.allocations[discovery.KeyFromTarget(target)] = allocation

	return allocation.Allocation, allocation.TotalAllocation
}
