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
	"net/http"

	"github.com/cespare/xxhash/v2"

	"vitess.io/vitess/go/vt/discovery"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/topo/topoproto"
)

// SessionBalancer implements the TabletBalancer interface. For a given session,
// it will return the same tablet for its duration, with preference to tablets in
// the local cell.
type SessionBalancer struct {
	// localCell is the cell the gateway is currently running in.
	localCell string
}

// NewSessionBalancer creates a new session balancer.
func NewSessionBalancer(localCell string) TabletBalancer {
	return &SessionBalancer{localCell: localCell}
}

// Pick is the main entry point to the balancer.
//
// For a given session, it will return the same tablet for its duration, with preference to tablets
// in the local cell.
func (b *SessionBalancer) Pick(target *querypb.Target, tablets []*discovery.TabletHealth, opts PickOpts) *discovery.TabletHealth {
	if opts.SessionUUID == "" {
		return nil
	}

	// Find the highest weight local and external tablets
	var maxLocalWeight, maxExternalWeight uint64
	var maxLocalTablet, maxExternalTablet *discovery.TabletHealth

	for _, tablet := range tablets {
		alias := tabletAlias(tablet)

		// Ignore invalid tablets
		if _, invalid := opts.InvalidTablets[alias]; invalid {
			continue
		}

		weight := weight(alias, opts.SessionUUID)

		if b.isLocal(tablet) && ((maxLocalTablet == nil) || (weight > maxLocalWeight)) {
			maxLocalWeight = weight
			maxLocalTablet = tablet
		}

		// We can consider all tablets here since we'd only use this if there were no
		// valid local tablets (meaning we'd have only considered external tablets anyway).
		if (maxExternalTablet == nil) || (weight > maxExternalWeight) {
			maxExternalWeight = weight
			maxExternalTablet = tablet
		}
	}

	// If we found a valid local tablet, use that
	if maxLocalTablet != nil {
		return maxLocalTablet
	}

	// Otherwise, use the max external tablet (if it exists)
	return maxExternalTablet
}

// weight computes the weight of a tablet by hashing its alias and the session UUID together.
func weight(alias string, sessionUUID string) uint64 {
	return xxhash.Sum64String(alias + "#" + sessionUUID)
}

// tabletAlias returns the tablet's alias as a string.
func tabletAlias(tablet *discovery.TabletHealth) string {
	return topoproto.TabletAliasString(tablet.Tablet.Alias)
}

// isLocal returns true if the tablet is in the local cell.
func (b *SessionBalancer) isLocal(tablet *discovery.TabletHealth) bool {
	return tablet.Tablet.Alias.Cell == b.localCell
}

// DebugHandler provides a summary of the session balancer state.
func (b *SessionBalancer) DebugHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintf(w, "Local cell: %s", b.localCell)
}
