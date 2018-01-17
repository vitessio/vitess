/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package discovery

import (
	"sync"
	"time"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/srvtopo"
	"github.com/youtube/vitess/go/vt/topo"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

var (
	// How much to sleep between each check.
	waitAvailableTabletInterval = 100 * time.Millisecond
)

// WaitForTablets waits for at least one tablet in the given cell /
// keyspace / shard / tablet type before returning. The tablets do not
// have to be healthy.  It will return ctx.Err() if the context is canceled.
func (tc *TabletStatsCache) WaitForTablets(ctx context.Context, cell, keyspace, shard string, tabletType topodatapb.TabletType) error {
	targets := []*querypb.Target{
		{
			Keyspace:   keyspace,
			Shard:      shard,
			TabletType: tabletType,
		},
	}
	return tc.waitForTablets(ctx, targets, false)
}

// WaitForAnyTablet waits for a single tablet of any of the types.
// It doesn't have to be serving.
func (tc *TabletStatsCache) WaitForAnyTablet(ctx context.Context, cell, keyspace, shard string, tabletTypes []topodatapb.TabletType) error {
	return tc.waitForAnyTablet(ctx, keyspace, shard, tabletTypes)
}

// WaitForAllServingTablets waits for at least one healthy serving tablet in
// the given cell for all keyspaces / shards before returning.
// It will return ctx.Err() if the context is canceled.
// It will return an error if it can't read the necessary topology records.
func (tc *TabletStatsCache) WaitForAllServingTablets(ctx context.Context, ts srvtopo.Server, cell string, types []topodatapb.TabletType) error {
	targets, err := FindAllTargets(ctx, ts, cell, types)
	if err != nil {
		return err
	}

	return tc.waitForTablets(ctx, targets, true)
}

// FindAllTargets goes through all serving shards in the topology
// for the provided tablet types. It returns one Target object per
// keyspace / shard / matching TabletType.
func FindAllTargets(ctx context.Context, ts srvtopo.Server, cell string, tabletTypes []topodatapb.TabletType) ([]*querypb.Target, error) {
	ksNames, err := ts.GetSrvKeyspaceNames(ctx, cell)
	if err != nil {
		return nil, err
	}

	var targets []*querypb.Target
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errRecorder concurrency.AllErrorRecorder
	for _, ksName := range ksNames {
		wg.Add(1)
		go func(keyspace string) {
			defer wg.Done()

			// Get SrvKeyspace for cell/keyspace.
			ks, err := ts.GetSrvKeyspace(ctx, cell, keyspace)
			if err != nil {
				if err == topo.ErrNoNode {
					// Possibly a race condition, or leftover
					// crud in the topology service. Just log it.
					log.Warningf("GetSrvKeyspace(%v, %v) returned ErrNoNode, skipping that SrvKeyspace", cell, keyspace)
				} else {
					// More serious error, abort.
					errRecorder.RecordError(err)
				}
				return
			}

			// Get all shard names that are used for serving.
			for _, ksPartition := range ks.Partitions {
				// Check we're waiting for tablets of that type.
				waitForIt := false
				for _, tt := range tabletTypes {
					if tt == ksPartition.ServedType {
						waitForIt = true
					}
				}
				if !waitForIt {
					continue
				}

				// Add all the shards. Note we can't have
				// duplicates, as there is only one entry per
				// TabletType in the Partitions list.
				mu.Lock()
				for _, shard := range ksPartition.ShardReferences {
					targets = append(targets, &querypb.Target{
						Cell:       cell,
						Keyspace:   keyspace,
						Shard:      shard.Name,
						TabletType: ksPartition.ServedType,
					})
				}
				mu.Unlock()
			}
		}(ksName)
	}
	wg.Wait()
	if errRecorder.HasErrors() {
		return nil, errRecorder.Error()
	}

	return targets, nil
}

// waitForTablets is the internal method that polls for tablets.
func (tc *TabletStatsCache) waitForTablets(ctx context.Context, targets []*querypb.Target, requireServing bool) error {
	for {
		// We nil targets as we find them.
		allPresent := true
		for i, target := range targets {
			if target == nil {
				continue
			}

			var stats []TabletStats
			if requireServing {
				stats = tc.GetHealthyTabletStats(target.Keyspace, target.Shard, target.TabletType)
			} else {
				stats = tc.GetTabletStats(target.Keyspace, target.Shard, target.TabletType)
			}
			if len(stats) == 0 {
				allPresent = false
			} else {
				targets[i] = nil
			}
		}

		if allPresent {
			// we found everything we needed
			return nil
		}

		// Unblock after the sleep or when the context has expired.
		timer := time.NewTimer(waitAvailableTabletInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// waitForAnyTablet is the internal method that polls for any tablet of required type
func (tc *TabletStatsCache) waitForAnyTablet(ctx context.Context, keyspace, shard string, types []topodatapb.TabletType) error {
	for {
		for _, tt := range types {
			stats := tc.GetTabletStats(keyspace, shard, tt)
			if len(stats) > 0 {
				return nil
			}
		}

		// Unblock after the sleep or when the context has expired.
		timer := time.NewTimer(waitAvailableTabletInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
