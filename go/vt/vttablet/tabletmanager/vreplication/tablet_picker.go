/*
Copyright 2018 The Vitess Authors.

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

package vreplication

import (
	"flag"
	"fmt"
	"math/rand"
	"time"

	"vitess.io/vitess/go/vt/vterrors"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var (
	healthCheckTopologyRefresh = flag.Duration("vreplication_healthcheck_topology_refresh", 30*time.Second, "refresh interval for re-reading the topology")
	healthcheckRetryDelay      = flag.Duration("vreplication_healthcheck_retry_delay", 5*time.Second, "delay before retrying a failed healthcheck")
	healthCheckTimeout         = flag.Duration("vreplication_healthcheck_timeout", time.Minute, "the health check timeout period")
)

type tabletPicker struct {
	ts          *topo.Server
	cell        string
	keyspace    string
	shard       string
	tabletTypes []topodatapb.TabletType

	healthCheck discovery.HealthCheck
	watcher     *discovery.TopologyWatcher
	statsCache  *discovery.TabletStatsCache
}

func newTabletPicker(ts *topo.Server, cell, keyspace, shard, tabletTypesStr string) (*tabletPicker, error) {
	tabletTypes, err := topoproto.ParseTabletTypes(tabletTypesStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse list of tablet types: %v", tabletTypesStr)
	}

	// These have to be initialized in the following sequence (watcher must be last).
	healthCheck := discovery.NewHealthCheck(*healthcheckRetryDelay, *healthCheckTimeout)
	statsCache := discovery.NewTabletStatsCache(healthCheck, ts, cell)
	watcher := discovery.NewShardReplicationWatcher(ts, healthCheck, cell, keyspace, shard, *healthCheckTopologyRefresh, discovery.DefaultTopoReadConcurrency)

	return &tabletPicker{
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

func (tp *tabletPicker) Pick(ctx context.Context) (*topodatapb.Tablet, error) {
	// wait for any of required the tablets (useful for the first run at least, fast for next runs)
	if err := tp.statsCache.WaitForAnyTablet(ctx, tp.cell, tp.keyspace, tp.shard, tp.tabletTypes); err != nil {
		return nil, vterrors.Wrapf(err, "error waiting for tablets for %v %v %v", tp.cell, tp.keyspace, tp.shard)
	}

	// Find the server list from the health check.
	// Note: We cannot use statsCache.GetHealthyTabletStats() here because it does
	// not return non-serving tablets. We must include non-serving tablets because
	// some tablets may not be serving if their traffic was already migrated to the
	// destination shards.
	for _, tabletType := range tp.tabletTypes {
		addrs := discovery.RemoveUnhealthyTablets(tp.statsCache.GetTabletStats(tp.keyspace, tp.shard, tabletType))
		if len(addrs) > 0 {
			return addrs[rand.Intn(len(addrs))].Tablet, nil
		}
	}
	return nil, fmt.Errorf("can't find any healthy source tablet for %v %v %v", tp.keyspace, tp.shard, tp.tabletTypes)
}

func (tp *tabletPicker) Close() {
	tp.watcher.Stop()
	tp.healthCheck.Close()
}

func init() {
	// TODO(sougou): consolidate this call to be once per process.
	rand.Seed(time.Now().UnixNano())
}
