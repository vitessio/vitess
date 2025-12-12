/*
Copyright 2025 The Vitess Authors.

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
	"math/rand/v2"
	"net/http"

	"vitess.io/vitess/go/vt/discovery"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

/*

The randomBalancer provides a simple, stateless load balancing strategy that
uniformly distributes queries across all available tablets without considering
cell affinity.

Unlike the flow-based balancer which attempts to maintain cell affinity while
balancing load, the random balancer ignores cell boundaries entirely and picks
tablets with uniform probability (1/N for N available tablets).

This is useful in scenarios where:
- Traffic distribution across cells is unpredictable or uneven
- Cell affinity optimization is not beneficial for the workload
- Simpler, more predictable load distribution is desired

The balancer can optionally filter tablets to only those in specified cells
via the vtGateCells parameter, but within that filtered set, selection is
purely random without any cell preference.

*/

func newRandomBalancer(localCell string, vtGateCells []string) TabletBalancer {
	// Build map for O(1) cell membership lookups
	cellsMap := make(map[string]struct{}, len(vtGateCells))
	for _, cell := range vtGateCells {
		cellsMap[cell] = struct{}{}
	}

	return &randomBalancer{
		localCell:      localCell,
		vtGateCells:    vtGateCells,
		vtGateCellsMap: cellsMap,
	}
}

type randomBalancer struct {
	// The local cell for the vtgate (used for debugging/logging only)
	localCell string

	// Optional list of cells to filter tablets to. If empty, all tablets are considered.
	vtGateCells []string

	// Map of vtGateCells for O(1) lookup performance. Initialized from vtGateCells.
	vtGateCellsMap map[string]struct{}
}

// Pick returns a random tablet from the list with uniform probability (1/N).
// If vtGateCells is configured, only tablets in those cells are considered.
func (b *randomBalancer) Pick(target *querypb.Target, tablets []*discovery.TabletHealth, _ PickOpts) *discovery.TabletHealth {
	if len(tablets) == 0 {
		return nil
	}

	if len(tablets) == 1 {
		// Single tablet: check if in target cells (if filtering enabled)
		if len(b.vtGateCells) > 0 {
			if _, ok := b.vtGateCellsMap[tablets[0].Tablet.Alias.Cell]; ok {
				return tablets[0]
			}
			return nil
		}
		return tablets[0]
	}

	// Multiple tablets with cell filtering: use fast-path random sampling
	if len(b.vtGateCells) > 0 {
		// Fast path: try 3 random samples
		for range 3 {
			candidate := tablets[rand.IntN(len(tablets))]
			if _, ok := b.vtGateCellsMap[candidate.Tablet.Alias.Cell]; ok {
				return candidate
			}
		}

		// fallback to filtering
		filtered := make([]*discovery.TabletHealth, 0, len(tablets))
		for _, tablet := range tablets {
			if _, ok := b.vtGateCellsMap[tablet.Tablet.Alias.Cell]; ok {
				filtered = append(filtered, tablet)
			}
		}
		if len(filtered) == 0 {
			return nil
		}
		if len(filtered) == 1 {
			return filtered[0]
		}

		tablets = filtered
	}

	// Uniform random selection
	return tablets[rand.IntN(len(tablets))]
}

func (b *randomBalancer) DebugHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintf(w, "Balancer Mode: random\r\n")
	fmt.Fprintf(w, "Local Cell: %v\r\n", b.localCell)
	if len(b.vtGateCells) > 0 {
		fmt.Fprintf(w, "Filtered to Cells: %v\r\n", b.vtGateCells)
	} else {
		fmt.Fprintf(w, "Cells: all (no filter)\r\n")
	}
	fmt.Fprintf(w, "Strategy: Uniform random selection (1/N probability per tablet)\r\n")
}
