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

	"github.com/cespare/xxhash/v2"

	"vitess.io/vitess/go/vt/discovery"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
)

// tabletTypesToWatch are the tablet types that will be included in the hash rings.
var tabletTypesToWatch = map[topodata.TabletType]struct{}{topodata.TabletType_REPLICA: {}, topodata.TabletType_RDONLY: {}}

// SessionBalancer implements the TabletBalancer interface. For a given session,
// it will return the same tablet for its duration, with preference to tablets in
// the local cell.
type SessionBalancer struct {
	// localCell is the cell the gateway is currently running in.
	localCell string

	// hc is the tablet health check.
	hc discovery.HealthCheck

	mu sync.RWMutex

	// localTablets is a map of tablets in the local cell for each target.
	localTablets map[discovery.KeyspaceShardTabletType]TabletSet

	// externalTablets is a map of tablets external to this cell for each target.
	externalTablets map[discovery.KeyspaceShardTabletType]TabletSet

	// tablets keeps track of the latest state of each tablet, keyed by alias. This
	// is used to remove tablets from old targets when their target changes (a
	// PlannedReparentShard for example).
	tablets TabletSet
}

// TabletSet represents a set of tablets, keyed by alias.
type TabletSet map[string]*discovery.TabletHealth

// NewSessionBalancer creates a new session balancer.
func NewSessionBalancer(ctx context.Context, localCell string, topoServer srvtopo.Server, hc discovery.HealthCheck) (TabletBalancer, error) {
	b := &SessionBalancer{
		localCell:       localCell,
		hc:              hc,
		localTablets:    make(map[discovery.KeyspaceShardTabletType]TabletSet),
		externalTablets: make(map[discovery.KeyspaceShardTabletType]TabletSet),
		tablets:         make(TabletSet),
	}

	// Set up health check subscription
	hcChan := b.hc.Subscribe("SessionBalancer")

	// Build initial hash rings

	// Find all the targets we're watching
	targets, _, err := srvtopo.FindAllTargetsAndKeyspaces(ctx, topoServer, b.localCell, discovery.KeyspacesToWatch, slices.Collect(maps.Keys(tabletTypesToWatch)))
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
	go b.watchHealthCheck(ctx, hcChan)

	return b, nil
}

// watchHealthCheck watches the health check channel for tablet health changes and updates the set of tablets accordingly.
func (b *SessionBalancer) watchHealthCheck(ctx context.Context, hcChan chan *discovery.TabletHealth) {
	for {
		select {
		case <-ctx.Done():
			b.hc.Unsubscribe(hcChan)
			return
		case tablet := <-hcChan:
			if tablet == nil {
				continue
			}

			// Remove tablet from old target if it has changed
			b.removeOldTablet(tablet)

			// Ignore tablets we aren't supposed to watch
			if _, ok := tabletTypesToWatch[tablet.Target.TabletType]; !ok {
				continue
			}

			b.onTabletHealthChange(tablet)
		}
	}
}

// removeOldTablet removes the entry for a tablet in its old target if its target has changed. For example, if a
// reparent happens and a replica is now a primary, we need to remove it from the list of tablets for the replica
// target.
func (b *SessionBalancer) removeOldTablet(newTablet *discovery.TabletHealth) {
	b.mu.Lock()
	defer b.mu.Unlock()

	alias := tabletAlias(newTablet)
	prevTablet, exists := b.tablets[alias]
	if !exists {
		return
	}

	prevTargetKey := discovery.KeyFromTarget(prevTablet.Target)
	currentTargetKey := discovery.KeyFromTarget(newTablet.Target)

	// If this tablet's target changed, remove it from its old target.
	if prevTargetKey == currentTargetKey {
		return
	}

	b.removeTablet(b.localTablets, prevTargetKey, prevTablet)
	b.removeTablet(b.externalTablets, prevTargetKey, prevTablet)
}

// onTabletHealthChange is the handler for tablet health events. If a tablet goes into serving,
// it is added to the appropriate (local or external) hash ring for its target. If it goes out
// of serving, it is removed from the hash ring.
func (b *SessionBalancer) onTabletHealthChange(newTablet *discovery.TabletHealth) {
	b.mu.Lock()
	defer b.mu.Unlock()

	var tablets map[discovery.KeyspaceShardTabletType]TabletSet
	if newTablet.Tablet.Alias.Cell == b.localCell {
		tablets = b.localTablets
	} else {
		tablets = b.externalTablets
	}

	alias := tabletAlias(newTablet)
	targetKey := discovery.KeyFromTarget(newTablet.Target)

	if newTablet.Serving {
		b.addTablet(tablets, targetKey, newTablet)
		b.tablets[alias] = newTablet
	} else {
		b.removeTablet(tablets, targetKey, newTablet)
		delete(b.tablets, alias)
	}
}

// addTablet adds a tablet to the target in the given list of tablets.
func (b *SessionBalancer) addTablet(tablets map[discovery.KeyspaceShardTabletType]TabletSet, targetKey discovery.KeyspaceShardTabletType, tablet *discovery.TabletHealth) {
	target, ok := tablets[targetKey]
	if !ok {
		// Create the set if one has not been created for this target yet
		tablets[targetKey] = make(TabletSet)
		target = tablets[targetKey]
	}

	alias := tabletAlias(tablet)
	target[alias] = tablet
}

// removeTablet removes the tablet from the target in the given list of tablets.
func (b *SessionBalancer) removeTablet(tablets map[discovery.KeyspaceShardTabletType]TabletSet, targetKey discovery.KeyspaceShardTabletType, tablet *discovery.TabletHealth) {
	alias := tabletAlias(tablet)
	delete(tablets[targetKey], alias)
}

// Pick is the main entry point to the balancer.
//
// For a given session, it will return the same tablet for its duration, with preference to tablets
// in the local cell.
func (b *SessionBalancer) Pick(target *querypb.Target, _ []*discovery.TabletHealth, opts PickOpts) *discovery.TabletHealth {
	if opts.SessionUUID == "" {
		return nil
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	targetKey := discovery.KeyFromTarget(target)

	// Try to find a tablet in the local cell first
	tablet := pick(b.localTablets[targetKey], opts)
	if tablet != nil {
		return tablet
	}

	// If we didn't find a tablet in the local cell, try external cells
	tablet = pick(b.externalTablets[targetKey], opts)
	return tablet
}

// pick picks the highest weight valid tablet from the set of tablets.
func pick(tablets TabletSet, opts PickOpts) *discovery.TabletHealth {
	var maxWeight uint64
	var maxTablet *discovery.TabletHealth

	for alias, tablet := range tablets {
		invalid := opts.InvalidTablets[alias]
		if invalid {
			continue
		}

		weight := weight(alias, opts.SessionUUID)
		if tablet == nil || weight > maxWeight {
			maxWeight = weight
			maxTablet = tablet
		}
	}

	return maxTablet
}

// weight computes the weight of a tablet by hashing its alias and the session UUID together.
func weight(alias string, sessionUUID string) uint64 {
	return xxhash.Sum64String(alias + "#" + sessionUUID)
}

// tabletAlias returns the tablet's alias as a string.
func tabletAlias(tablet *discovery.TabletHealth) string {
	return topoproto.TabletAliasString(tablet.Tablet.Alias)
}

// DebugHandler provides a summary of the session balancer state.
func (b *SessionBalancer) DebugHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprint(w, b.print())
}

// print returns a string representation of the session balancer state for debugging.
func (b *SessionBalancer) print() string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	sb := strings.Builder{}

	sb.WriteString("Session balancer\n")
	sb.WriteString("================\n")
	sb.WriteString(fmt.Sprintf("Local cell: %s\n\n", b.localCell))

	sb.WriteString("Local tablets:\n")

	for target, tablets := range b.localTablets {
		if len(tablets) == 0 {
			continue
		}

		sb.WriteString(fmt.Sprintf("\t - Target: %s\n", target))
		sb.WriteString(fmt.Sprintf("\t\tTablets: %+v\n", slices.Collect(maps.Keys(tablets))))
	}

	sb.WriteString("External tablets:\n")

	for target, tablets := range b.externalTablets {
		if len(tablets) == 0 {
			continue
		}

		sb.WriteString(fmt.Sprintf("\t - Target: %s\n", target))
		sb.WriteString(fmt.Sprintf("\t\tTablets: %+v\n", slices.Collect(maps.Keys(tablets))))
	}

	return sb.String()
}
