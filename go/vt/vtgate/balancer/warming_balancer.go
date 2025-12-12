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
	"fmt"
	"math/rand/v2"
	"net/http"
	"time"

	"vitess.io/vitess/go/vt/discovery"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

// newWarmingBalancer creates a new warming balancer.
// The warming balancer routes a small percentage of traffic to newly started replicas
// while directing the majority of traffic to established replicas with warm buffer pools.
func newWarmingBalancer(localCell string, vtGateCells []string, warmingPeriod time.Duration, warmingTrafficPercent int) *warmingBalancer {
	return &warmingBalancer{
		localCell:             localCell,
		vtGateCells:           vtGateCells,
		warmingPeriod:         warmingPeriod,
		warmingTrafficPercent: warmingTrafficPercent,
	}
}

type warmingBalancer struct {
	localCell             string
	vtGateCells           []string
	warmingPeriod         time.Duration
	warmingTrafficPercent int
}

// Pick implements the warming logic for tablet selection.
//
// The algorithm:
//  1. Prefers local cell tablets (falls back to all tablets if local cell has none)
//  2. Separates tablets into "old" (started before warmingPeriod) and "new"
//     (started within warmingPeriod)
//  3. If ALL tablets are new, picks randomly (nothing to warm against)
//  4. If there are no new tablets, picks randomly from old tablets
//  5. Otherwise, routes warmingTrafficPercent to new tablets, rest to old
//
// Tablets with TabletStartTime == 0 are treated as old for backwards compatibility
// with older vttablets that don't report start time.
func (b *warmingBalancer) Pick(_ *querypb.Target, tablets []*discovery.TabletHealth) *discovery.TabletHealth {
	if len(tablets) == 0 {
		return nil
	}

	// Prefer local cell tablets (matching default cell mode behavior).
	// Fall back to all tablets only if local cell has none.
	if filtered := filterByCell(tablets, b.localCell); len(filtered) > 0 {
		tablets = filtered
	}

	warmingCutoff := time.Now().Add(-b.warmingPeriod)
	var oldTablets, newTablets []*discovery.TabletHealth

	for _, th := range tablets {
		// Tablets with zero start time (old vttablets) are treated as old
		// for backwards compatibility during rolling upgrades.
		if th.TabletStartTime == 0 || time.Unix(th.TabletStartTime, 0).Before(warmingCutoff) {
			oldTablets = append(oldTablets, th)
		} else {
			newTablets = append(newTablets, th)
		}
	}

	switch {
	case len(oldTablets) == 0:
		// All tablets are new - pick randomly (nothing to warm against)
		return newTablets[rand.IntN(len(newTablets))]
	case len(newTablets) == 0:
		// No new tablets - pick from old tablets randomly
		return oldTablets[rand.IntN(len(oldTablets))]
	default:
		// Mixed: route warmingTrafficPercent to new, rest to old
		if rand.IntN(100) < b.warmingTrafficPercent {
			return newTablets[rand.IntN(len(newTablets))]
		}
		return oldTablets[rand.IntN(len(oldTablets))]
	}
}

func filterByCell(tablets []*discovery.TabletHealth, cell string) []*discovery.TabletHealth {
	var filtered []*discovery.TabletHealth
	for _, th := range tablets {
		if th.Tablet.Alias.Cell == cell {
			filtered = append(filtered, th)
		}
	}
	return filtered
}

// DebugHandler provides debug information about the warming balancer configuration.
func (b *warmingBalancer) DebugHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintf(w, "Balancer Mode: warming\r\n")
	fmt.Fprintf(w, "Local Cell: %v\r\n", b.localCell)
	fmt.Fprintf(w, "VTGate Cells: %v\r\n", b.vtGateCells)
	fmt.Fprintf(w, "Warming Period: %v\r\n", b.warmingPeriod)
	fmt.Fprintf(w, "Warming Traffic Percent: %d%%\r\n", b.warmingTrafficPercent)
}
