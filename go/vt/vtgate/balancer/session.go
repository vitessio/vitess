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
	"context"
	"fmt"
	"net/http"
	"sync"

	"vitess.io/vitess/go/vt/discovery"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

// SessionBalancer implements the [TabletBalancer] interface. For a given
// session, it will return the same tablet for its duration. The tablet is initially
// selected randomly, with preference to tablets in the local cell.
type SessionBalancer struct {
	// localCell is the cell the gateway is currently running in.
	localCell string

	// hc is the tablet health check.
	hc discovery.HealthCheck

	mu sync.RWMutex

	// localRings are the hash rings created for each target. It contains only tablets
	// local to [localCell].
	localRings map[discovery.KeyspaceShardTabletType]*hashRing

	// externalRings are the hash rings created for each target. It contains only tablets
	// external to [localCell].
	externalRings map[discovery.KeyspaceShardTabletType]*hashRing
}

// NewSessionBalancer creates a new session balancer.
func NewSessionBalancer(ctx context.Context, localCell string, hc discovery.HealthCheck) TabletBalancer {
	b := &SessionBalancer{
		localCell:     localCell,
		hc:            hc,
		localRings:    make(map[discovery.KeyspaceShardTabletType]*hashRing),
		externalRings: make(map[discovery.KeyspaceShardTabletType]*hashRing),
	}

	// Set up health check subscription
	hcChan := b.hc.Subscribe("SessionBalancer")
	go b.watchHealthCheck(ctx, hcChan)

	return b
}

// Pick is the main entry point to the balancer.
//
// For a given session, it will return the same tablet for its duration. The tablet is
// initially selected randomly, with preference to tablets in the local cell.
func (b *SessionBalancer) Pick(target *querypb.Target, _ []*discovery.TabletHealth, opts *PickOpts) *discovery.TabletHealth {
	return nil
}

// DebugHandler provides a summary of the session balancer state.
func (b *SessionBalancer) DebugHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintf(w, "Session Balancer\n")
	fmt.Fprintf(w, "================\n")
	fmt.Fprintf(w, "Local Cell: %s\n\n", b.localCell)

	b.mu.RLock()
	defer b.mu.RUnlock()

	fmt.Fprintf(w, "Local Rings (%d):\n", len(b.localRings))
	for key := range b.localRings {
		fmt.Fprintf(w, "  - %s\n", key)
	}

	fmt.Fprintf(w, "\nExternal Rings (%d):\n", len(b.externalRings))
	for key := range b.externalRings {
		fmt.Fprintf(w, "  - %s\n", key)
	}
}

// watchHealthCheck watches the health check channel for tablet health changes, and updates hash rings accordingly.
func (b *SessionBalancer) watchHealthCheck(ctx context.Context, hcChan chan *discovery.TabletHealth) {
	for {
		select {
		case <-ctx.Done():
			b.hc.Unsubscribe(hcChan)
			return
		case tablet := <-hcChan:
			if tablet == nil {
				return
			}

			b.onTabletHealthChange(tablet)
		}
	}
}

// onTabletHealthChange is the handler for tablet health events.
func (b *SessionBalancer) onTabletHealthChange(tablet *discovery.TabletHealth) {
	b.mu.Lock()
	defer b.mu.Unlock()

	var ring *hashRing
	if tablet.Target.Cell == b.localCell {
		ring = getRing(b.localRings, tablet)
	} else {
		ring = getRing(b.externalRings, tablet)
	}

	if tablet.Serving {
		ring.add(tablet)
		ring.sort()
	} else {
		ring.remove(tablet)
	}
}

// getRing gets or creates a new ring for the given tablet.
func getRing(rings map[discovery.KeyspaceShardTabletType]*hashRing, tablet *discovery.TabletHealth) *hashRing {
	key := discovery.KeyFromTarget(tablet.Target)

	ring, exists := rings[key]
	if !exists {
		ring = newHashRing()
		rings[key] = ring
	}

	return ring
}
