/*
Copyright 2019 The Vitess Authors.

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

package discovery

import (
	"fmt"
	"math/rand"
	"time"

	"golang.org/x/net/context"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"
)

// These are vars because they need to be overridden for testing.
var (
	healthCheckTopologyRefresh = 30 * time.Second
	healthcheckRetryDelay      = 5 * time.Second
	healthCheckTimeout         = 1 * time.Minute
)

// TabletPicker gives a simplified API for picking tablets.
type TabletPicker struct {
	ts          *topo.Server
	cell        string
	keyspace    string
	shard       string
	tabletTypes []topodatapb.TabletType

	healthCheck HealthCheck
	watcher     *TopologyWatcher
	statsCache  *TabletStatsCache
}

// NewTabletPicker returns a TabletPicker.
func NewTabletPicker(ctx context.Context, ts *topo.Server, cell, keyspace, shard, tabletTypesStr string) (*TabletPicker, error) {
	tabletTypes, err := topoproto.ParseTabletTypes(tabletTypesStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse list of tablet types: %v", tabletTypesStr)
	}

	// These have to be initialized in the following sequence (watcher must be last).
	healthCheck := NewHealthCheck(healthcheckRetryDelay, healthCheckTimeout)
	statsCache := NewTabletStatsCache(healthCheck, ts, cell)
	watcher := NewShardReplicationWatcher(ctx, ts, healthCheck, cell, keyspace, shard, healthCheckTopologyRefresh, DefaultTopoReadConcurrency)

	return &TabletPicker{
		ts:          ts,
		cell:        cell,
		keyspace:    keyspace,
		shard:       shard,
		tabletTypes: tabletTypes,
		healthCheck: healthCheck,
		watcher:     watcher,
		statsCache:  statsCache,
	}, nil
}

// PickForStreaming picks all healthy tablets including the non-serving ones.
func (tp *TabletPicker) PickForStreaming(ctx context.Context) (*topodatapb.Tablet, error) {
	// wait for any of required the tablets (useful for the first run at least, fast for next runs)
	if err := tp.statsCache.WaitByFilter(ctx, tp.keyspace, tp.shard, tp.tabletTypes, RemoveUnhealthyTablets); err != nil {
		return nil, vterrors.Wrapf(err, "error waiting for tablets for %v %v %v", tp.cell, tp.keyspace, tp.shard)
	}

	// Refilter the tablets list based on the same criteria.
	for _, tabletType := range tp.tabletTypes {
		addrs := RemoveUnhealthyTablets(tp.statsCache.GetTabletStats(tp.keyspace, tp.shard, tabletType))
		if len(addrs) > 0 {
			return addrs[rand.Intn(len(addrs))].Tablet, nil
		}
	}
	// Unreachable.
	return nil, fmt.Errorf("can't find any healthy source tablet for %v %v %v", tp.keyspace, tp.shard, tp.tabletTypes)
}

// Close shuts down TabletPicker.
func (tp *TabletPicker) Close() {
	tp.watcher.Stop()
	tp.healthCheck.Close()
}

func init() {
	// TODO(sougou): consolidate this call to be once per process.
	rand.Seed(time.Now().UnixNano())
}
