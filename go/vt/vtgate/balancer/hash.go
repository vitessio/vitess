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
	"net/http"

	"vitess.io/vitess/go/vt/discovery"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

// ConsistentHashBalancer implements the [TabletBalancer] interface. For a given
// session, it will return the same tablet for its duration. The tablet is initially
// selected randomly, with preference to tablets in the local cell.
type ConsistentHashBalancer struct {
	// localCell is the cell the gateway is currently running in.
	localCell string

	// hc is the tablet health check.
	hc discovery.HealthCheck

	// // hcChan is the channel to receive tablet health events on.
	// hcChan chan *discovery.TabletHealth
}

// NewConsistentHashBalancer creates a new consistent hash balancer.
func NewConsistentHashBalancer(ctx context.Context, localCell string, hc discovery.HealthCheck) TabletBalancer {
	b := &ConsistentHashBalancer{
		localCell: localCell,
		hc:        hc,
	}

	// Set up health check subscription
	hcChan := b.hc.Subscribe("ConsistentHashBalancer")
	go b.watchHealthCheck(ctx, hcChan)

	return b
}

// Pick is the main entry point to the balancer.
//
// For a given session, it will return the same tablet for its duration. The tablet is
// initially selected randomly, with preference to tablets in the local cell.
func (b *ConsistentHashBalancer) Pick(target *querypb.Target, _ []*discovery.TabletHealth, opts *PickOpts) *discovery.TabletHealth {
	return nil
}

// DebugHandler provides a summary of the consistent hash balancer state.
func (b *ConsistentHashBalancer) DebugHandler(w http.ResponseWriter, r *http.Request) {}

// watchHealthCheck watches the health check channel for tablet health changes, and updates hash rings accordingly.
func (b *ConsistentHashBalancer) watchHealthCheck(ctx context.Context, hcChan chan *discovery.TabletHealth) {
	for {
		select {
		case <-ctx.Done():
			b.hc.Unsubscribe(hcChan)
			close(hcChan)
			return
		case th := <-hcChan:
			if th == nil {
				return
			}
		}
	}
}
