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
	"maps"
	"net/http"
	"slices"
	"strings"
	"sync"

	"vitess.io/vitess/go/vt/discovery"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
)

// tabletTypesToWatch are the tablet types that will be included in the hash rings.
var tabletTypesToWatch = []topodata.TabletType{topodata.TabletType_PRIMARY, topodata.TabletType_REPLICA, topodata.TabletType_BATCH}

// SessionBalancer implements the TabletBalancer interface. For a given session,
// it will return the same tablet for its duration, with preference to tablets in
// the local cell.
type SessionBalancer struct {
	// localCell is the cell the gateway is currently running in.
	localCell string

	// hc is the tablet health check.
	hc discovery.HealthCheck

	mu sync.RWMutex

	// localRings are the hash rings created for each target. It contains only tablets
	// local to localCell.
	localRings map[discovery.KeyspaceShardTabletType]*hashRing

	// externalRings are the hash rings created for each target. It contains only tablets
	// external to localCell.
	externalRings map[discovery.KeyspaceShardTabletType]*hashRing

	// tabletMap is a map of all the tablets by alias currently in any of the hash rings.
	tabletMap map[string]*discovery.TabletHealth
}

// NewSessionBalancer creates a new session balancer.
func NewSessionBalancer(ctx context.Context, localCell string, topoServer srvtopo.Server, hc discovery.HealthCheck) (TabletBalancer, error) {
	b := &SessionBalancer{
		localCell:     localCell,
		hc:            hc,
		localRings:    make(map[discovery.KeyspaceShardTabletType]*hashRing),
		externalRings: make(map[discovery.KeyspaceShardTabletType]*hashRing),
		tabletMap:     make(map[string]*discovery.TabletHealth),
	}

	// Set up health check subscription
	hcChan := b.hc.Subscribe("SessionBalancer")

	// Build initial hash rings

	// Find all the targets we're watching
	targets, _, err := srvtopo.FindAllTargetsAndKeyspaces(ctx, topoServer, b.localCell, discovery.KeyspacesToWatch, tabletTypesToWatch)
	if err != nil {
		return nil, fmt.Errorf("session balancer: failed to find all targets and keyspaces: %w", err)
	}

	// Add each tablet to the hash ring
	for _, target := range targets {
		tablets := b.hc.GetHealthyTabletStats(target)
		for _, tablet := range tablets {
			b.onTabletHealthChange(tablet)
		}
	}

	// Start watcher to keep track of tablet health
	go b.watchHealthCheck(ctx, topoServer, hcChan)

	return b, nil
}

// Pick is the main entry point to the balancer.
//
// For a given session, it will return the same tablet for its duration, with preference to tablets
// in the local cell.
func (b *SessionBalancer) Pick(target *querypb.Target, _ []*discovery.TabletHealth, opts *PickOpts) *discovery.TabletHealth {
	if opts == nil {
		return nil
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	// Try to find a tablet in the local cell first
	tablet := getFromRing(b.localRings, target, opts.InvalidTablets, opts.SessionUUID)
	if tablet != nil {
		return tablet
	}

	// If we didn't find a tablet in the local cell, try external cells
	tablet = getFromRing(b.externalRings, target, opts.InvalidTablets, opts.SessionUUID)
	return tablet
}

// DebugHandler provides a summary of the session balancer state.
func (b *SessionBalancer) DebugHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintf(w, "Session balancer\n")
	fmt.Fprintf(w, "================\n")
	fmt.Fprintf(w, "Local cell: %s\n\n", b.localCell)

	b.mu.RLock()
	defer b.mu.RUnlock()

	fmt.Fprint(w, b.print())
}

// watchHealthCheck watches the health check channel for tablet health changes, and updates hash rings accordingly.
func (b *SessionBalancer) watchHealthCheck(ctx context.Context, topoServer srvtopo.Server, hcChan chan *discovery.TabletHealth) {
	// Start watching health check channel for future tablet health changes
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

// onTabletHealthChange is the handler for tablet health events. If a tablet goes into serving,
// it is added to the appropriate (local or external) hash ring for its target. If it goes out
// of serving, it is removed from the hash ring.
func (b *SessionBalancer) onTabletHealthChange(tablet *discovery.TabletHealth) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Remove this tablet from other ring in case the target has changed. This can happen in
	// a reparent for example, where the same tablet of target REPLICA now has a target of
	// PRIMARY, and vice versa.
	oldTablet, exists := b.tabletMap[topoproto.TabletAliasString(tablet.Tablet.Alias)]
	if exists {
		oldTarget := discovery.KeyFromTarget(oldTablet.Target)
		newTarget := discovery.KeyFromTarget(tablet.Target)

		if oldTarget != newTarget {
			b.removeFromRing(oldTablet)
		}
	}

	if tablet.Serving {
		b.addToRing(tablet)
	} else {
		b.removeFromRing(tablet)
	}
}

// addToRing adds a tablet to the appropriate (local or external) ring.
func (b *SessionBalancer) addToRing(tablet *discovery.TabletHealth) {
	ring := b.getRing(tablet)

	ring.add(tablet)
	b.tabletMap[topoproto.TabletAliasString(tablet.Tablet.Alias)] = tablet
}

// removeFromRing removes a tablet from the appropriate (local or external) ring.
func (b *SessionBalancer) removeFromRing(tablet *discovery.TabletHealth) {
	ring := b.getRing(tablet)

	ring.remove(tablet)
	delete(b.tabletMap, topoproto.TabletAliasString(tablet.Tablet.Alias))
}

// getRing gets the appropriate (local or external) ring for the tablet.
func (b *SessionBalancer) getRing(tablet *discovery.TabletHealth) *hashRing {
	if tablet.Target.Cell == b.localCell {
		return getOrCreateRing(b.localRings, tablet)
	}

	return getOrCreateRing(b.externalRings, tablet)
}

// getOrCreateRing gets or creates a new ring for the given tablet.
func getOrCreateRing(rings map[discovery.KeyspaceShardTabletType]*hashRing, tablet *discovery.TabletHealth) *hashRing {
	key := discovery.KeyFromTarget(tablet.Target)

	ring, exists := rings[key]
	if !exists {
		ring = newHashRing()
		rings[key] = ring
	}

	return ring
}

// print returns a string representation of the session balancer state for debugging.
func (b *SessionBalancer) print() string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	sb := strings.Builder{}

	sb.WriteString("Local rings:\n")
	if len(b.localRings) == 0 {
		sb.WriteString("\tNo local rings\n")
	}

	for target, ring := range b.localRings {
		sb.WriteString(fmt.Sprintf("\t - Target: %s\n", target))
		sb.WriteString(fmt.Sprintf("\t\tNode count: %d\n", len(ring.nodes)))
		sb.WriteString(fmt.Sprintf("\t\tTablets: %+v\n", slices.Collect(maps.Keys(ring.tablets))))
	}

	sb.WriteString("External rings:\n")
	if len(b.externalRings) == 0 {
		sb.WriteString("\tNo external rings\n")
	}

	for target, ring := range b.externalRings {
		sb.WriteString(fmt.Sprintf("\t - Target: %s\n", target))
		sb.WriteString(fmt.Sprintf("\t\tNode count: %d\n", len(ring.nodes)))
		sb.WriteString(fmt.Sprintf("\t\tTablets: %+v\n", slices.Collect(maps.Keys(ring.tablets))))
	}

	return sb.String()
}

// getFromRing gets a tablet from the respective ring for the given target and session hash.
func getFromRing(rings map[discovery.KeyspaceShardTabletType]*hashRing, target *querypb.Target, invalidTablets map[string]bool, sessionUUID string) *discovery.TabletHealth {
	key := discovery.KeyFromTarget(target)

	ring, exists := rings[key]
	if !exists {
		return nil
	}

	return ring.get(sessionUUID, invalidTablets)
}
