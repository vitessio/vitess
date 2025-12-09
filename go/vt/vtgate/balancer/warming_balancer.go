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
	"sync"
	"time"

	"vitess.io/vitess/go/vt/discovery"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// WarmingConfig holds configuration for the warming balancer.
type WarmingConfig struct {
	// WarmingPeriod is how long a tablet is considered "new" after startup.
	// During this period, new tablets receive reduced traffic to allow their
	// buffer pools to warm up.
	WarmingPeriod time.Duration

	// WarmingTrafficPercent is the percentage of traffic (0-100) to send to
	// new tablets during the warming period. The remaining traffic goes to
	// old tablets.
	WarmingTrafficPercent int
}

// DefaultWarmingConfig returns a default warming configuration with sensible defaults.
func DefaultWarmingConfig() WarmingConfig {
	return WarmingConfig{
		WarmingPeriod:         30 * time.Minute,
		WarmingTrafficPercent: 10,
	}
}

// NewWarmingBalancer creates a new warming balancer with the specified configuration.
// The warming balancer routes a small percentage of traffic to newly started replicas
// while directing the majority of traffic to established replicas with warm buffer pools.
func NewWarmingBalancer(localCell string, vtGateCells []string, config WarmingConfig) TabletBalancer {
	return &warmingBalancer{
		localCell:             localCell,
		vtGateCells:           vtGateCells,
		warmingPeriod:         config.WarmingPeriod,
		warmingTrafficPercent: config.WarmingTrafficPercent,
	}
}

type warmingBalancer struct {
	localCell             string
	vtGateCells           []string
	warmingPeriod         time.Duration
	warmingTrafficPercent int

	// mu protects debug state only
	mu            sync.Mutex
	lastPickStats *warmingPickStats
}

type warmingPickStats struct {
	Timestamp  time.Time
	OldTablets int
	NewTablets int
	AllNewMode bool
	PickedNew  bool
}

// Pick implements the warming logic for tablet selection.
//
// The algorithm:
//  1. Only applies warming logic to REPLICA type tablets (PRIMARY and RDONLY
//     use random selection)
//  2. Separates tablets into "old" (started before warmingPeriod) and "new"
//     (started within warmingPeriod)
//  3. If ALL tablets are new, picks randomly (nothing to warm against)
//  4. If there are no new tablets, picks randomly from old tablets
//  5. Otherwise, routes warmingTrafficPercent to new tablets, rest to old
//
// Tablets with TabletStartTime == 0 are treated as old for backwards compatibility
// with older vttablets that don't report start time.
func (b *warmingBalancer) Pick(target *querypb.Target, tablets []*discovery.TabletHealth) *discovery.TabletHealth {
	if len(tablets) == 0 {
		return nil
	}

	// Only apply warming logic to REPLICA tablets.
	// PRIMARY and RDONLY use simple random distribution.
	if target.TabletType != topodatapb.TabletType_REPLICA {
		return tablets[rand.IntN(len(tablets))]
	}

	now := time.Now()
	warmingCutoff := now.Add(-b.warmingPeriod)

	var oldTablets, newTablets []*discovery.TabletHealth

	for _, th := range tablets {
		// Tablets with zero start time (old vttablets) are treated as old
		// for backwards compatibility and safety during rolling upgrades.
		if th.TabletStartTime == 0 {
			oldTablets = append(oldTablets, th)
			continue
		}

		tabletStartTime := time.Unix(th.TabletStartTime, 0)
		if tabletStartTime.Before(warmingCutoff) {
			oldTablets = append(oldTablets, th)
		} else {
			newTablets = append(newTablets, th)
		}
	}

	// Update debug stats
	stats := &warmingPickStats{
		Timestamp:  now,
		OldTablets: len(oldTablets),
		NewTablets: len(newTablets),
	}

	var picked *discovery.TabletHealth

	switch {
	case len(oldTablets) == 0:
		// Case 1: All tablets are new - no warming needed, pick randomly.
		// This handles the scenario where an entire zone is recycled.
		stats.AllNewMode = true
		picked = newTablets[rand.IntN(len(newTablets))]
		stats.PickedNew = true

	case len(newTablets) == 0:
		// Case 2: No new tablets - pick from old tablets randomly.
		picked = oldTablets[rand.IntN(len(oldTablets))]
		stats.PickedNew = false

	default:
		// Case 3: Mixed old and new tablets - route based on warming percentage.
		// Roll a number 0-99; if < warmingTrafficPercent, send to new tablet.
		if rand.IntN(100) < b.warmingTrafficPercent {
			picked = newTablets[rand.IntN(len(newTablets))]
			stats.PickedNew = true
		} else {
			picked = oldTablets[rand.IntN(len(oldTablets))]
			stats.PickedNew = false
		}
	}

	b.mu.Lock()
	b.lastPickStats = stats
	b.mu.Unlock()

	return picked
}

// DebugHandler provides debug information about the warming balancer state.
func (b *warmingBalancer) DebugHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintf(w, "Balancer Mode: warming\r\n")
	fmt.Fprintf(w, "Local Cell: %v\r\n", b.localCell)
	fmt.Fprintf(w, "VTGate Cells: %v\r\n", b.vtGateCells)
	fmt.Fprintf(w, "Warming Period: %v\r\n", b.warmingPeriod)
	fmt.Fprintf(w, "Warming Traffic Percent: %d%%\r\n", b.warmingTrafficPercent)

	b.mu.Lock()
	stats := b.lastPickStats
	b.mu.Unlock()

	if stats != nil {
		fmt.Fprintf(w, "\r\nLast Pick Stats:\r\n")
		fmt.Fprintf(w, "  Timestamp: %v\r\n", stats.Timestamp)
		fmt.Fprintf(w, "  Old Tablets: %d\r\n", stats.OldTablets)
		fmt.Fprintf(w, "  New Tablets: %d\r\n", stats.NewTablets)
		fmt.Fprintf(w, "  All-New Mode: %v\r\n", stats.AllNewMode)
		fmt.Fprintf(w, "  Picked New: %v\r\n", stats.PickedNew)
	}
}
