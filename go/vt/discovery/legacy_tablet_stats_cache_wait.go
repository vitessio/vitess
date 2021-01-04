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
	"time"

	"context"

	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var (
	// How much to sleep between each check.
	waitAvailableTabletInterval = 100 * time.Millisecond
)

// WaitForTablets waits for at least one tablet in the given
// keyspace / shard / tablet type before returning. The tablets do not
// have to be healthy.  It will return ctx.Err() if the context is canceled.
func (tc *LegacyTabletStatsCache) WaitForTablets(ctx context.Context, keyspace, shard string, tabletType topodatapb.TabletType) error {
	targets := []*querypb.Target{
		{
			Keyspace:   keyspace,
			Shard:      shard,
			TabletType: tabletType,
		},
	}
	return tc.waitForTablets(ctx, targets, false)
}

// WaitForAllServingTablets waits for at least one healthy serving tablet in
// each given target before returning.
// It will return ctx.Err() if the context is canceled.
// It will return an error if it can't read the necessary topology records.
func (tc *LegacyTabletStatsCache) WaitForAllServingTablets(ctx context.Context, targets []*querypb.Target) error {
	return tc.waitForTablets(ctx, targets, true)
}

// waitForTablets is the internal method that polls for tablets.
func (tc *LegacyTabletStatsCache) waitForTablets(ctx context.Context, targets []*querypb.Target, requireServing bool) error {
	for {
		// We nil targets as we find them.
		allPresent := true
		for i, target := range targets {
			if target == nil {
				continue
			}

			var stats []LegacyTabletStats
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
			for _, target := range targets {
				if target != nil {
					log.Infof("couldn't find tablets for target: %v", target)
				}
			}
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// WaitByFilter waits for at least one tablet based on the filter function.
func (tc *LegacyTabletStatsCache) WaitByFilter(ctx context.Context, keyspace, shard string, tabletTypes []topodatapb.TabletType, filter func([]LegacyTabletStats) []LegacyTabletStats) error {
	for {
		for _, tt := range tabletTypes {
			stats := tc.GetTabletStats(keyspace, shard, tt)
			stats = filter(stats)
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
