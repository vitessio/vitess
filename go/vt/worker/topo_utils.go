/*
Copyright 2017 Google Inc.

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

package worker

import (
	"flag"
	"fmt"
	"math/rand"
	"time"

	log "github.com/golang/glog"

	"github.com/youtube/vitess/go/vt/discovery"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/vt/wrangler"
	"golang.org/x/net/context"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

var (
	// waitForHealthyTabletsTimeout intends to wait for the
	// healthcheck to automatically return rdonly instances which
	// have been taken out by previous *Clone or *Diff runs.
	// Therefore, the default for this variable must be higher
	// than vttablet's -health_check_interval.
	waitForHealthyTabletsTimeout = flag.Duration("wait_for_healthy_rdonly_tablets_timeout", 60*time.Second, "maximum time to wait at the start if less than --min_healthy_rdonly_tablets are available")
)

// FindHealthyRdonlyTablet returns a random healthy RDONLY tablet.
// Since we don't want to use them all, we require at least
// minHealthyRdonlyTablets servers to be healthy.
// May block up to -wait_for_healthy_rdonly_tablets_timeout.
func FindHealthyRdonlyTablet(ctx context.Context, wr *wrangler.Wrangler, tsc *discovery.TabletStatsCache, cell, keyspace, shard string, minHealthyRdonlyTablets int) (*topodatapb.TabletAlias, error) {
	if tsc == nil {
		// No healthcheck instance provided. Create one.
		healthCheck := discovery.NewHealthCheck(*remoteActionsTimeout, *healthcheckRetryDelay, *healthCheckTimeout)
		tsc = discovery.NewTabletStatsCache(healthCheck, cell)
		watcher := discovery.NewShardReplicationWatcher(wr.TopoServer(), healthCheck, cell, keyspace, shard, *healthCheckTopologyRefresh, discovery.DefaultTopoReadConcurrency)
		defer watcher.Stop()
		defer healthCheck.Close()
	}

	healthyTablets, err := waitForHealthyRdonlyTablets(ctx, wr, tsc, cell, keyspace, shard, minHealthyRdonlyTablets, *waitForHealthyTabletsTimeout)
	if err != nil {
		return nil, err
	}

	// random server in the list is what we want
	index := rand.Intn(len(healthyTablets))
	return healthyTablets[index].Tablet.Alias, nil
}

func waitForHealthyRdonlyTablets(ctx context.Context, wr *wrangler.Wrangler, tsc *discovery.TabletStatsCache, cell, keyspace, shard string, minHealthyRdonlyTablets int, timeout time.Duration) ([]discovery.TabletStats, error) {
	busywaitCtx, busywaitCancel := context.WithTimeout(ctx, timeout)
	defer busywaitCancel()

	start := time.Now()
	deadlineForLog, _ := busywaitCtx.Deadline()
	log.V(2).Infof("Waiting for enough healthy RDONLY tablets to become available in (%v,%v/%v). required: %v Waiting up to %.1f seconds.",
		cell, keyspace, shard, minHealthyRdonlyTablets, deadlineForLog.Sub(time.Now()).Seconds())

	// Wait for at least one RDONLY tablet initially before checking the list.
	if err := tsc.WaitForTablets(busywaitCtx, cell, keyspace, shard, []topodatapb.TabletType{topodatapb.TabletType_RDONLY}); err != nil {
		return nil, fmt.Errorf("error waiting for RDONLY tablets for (%v,%v/%v): %v", cell, keyspace, shard, err)
	}

	var healthyTablets []discovery.TabletStats
	for {
		select {
		case <-busywaitCtx.Done():
			return nil, fmt.Errorf("not enough healthy RDONLY tablets to choose from in (%v,%v/%v), have %v healthy ones, need at least %v Context error: %v",
				cell, keyspace, shard, len(healthyTablets), minHealthyRdonlyTablets, busywaitCtx.Err())
		default:
		}

		healthyTablets = discovery.RemoveUnhealthyTablets(tsc.GetTabletStats(keyspace, shard, topodatapb.TabletType_RDONLY))
		if len(healthyTablets) >= minHealthyRdonlyTablets {
			break
		}

		deadlineForLog, _ := busywaitCtx.Deadline()
		wr.Logger().Infof("Waiting for enough healthy RDONLY tablets to become available (%v,%v/%v). available: %v required: %v Waiting up to %.1f more seconds.",
			cell, keyspace, shard, len(healthyTablets), minHealthyRdonlyTablets, deadlineForLog.Sub(time.Now()).Seconds())
		// Block for 1 second because 2 seconds is the -health_check_interval flag value in integration tests.
		timer := time.NewTimer(1 * time.Second)
		select {
		case <-busywaitCtx.Done():
			timer.Stop()
		case <-timer.C:
		}
	}
	log.V(2).Infof("At least %v healthy RDONLY tablets are available in (%v,%v/%v) (required: %v). Took %.1f seconds to find this out.",
		len(healthyTablets), cell, keyspace, shard, minHealthyRdonlyTablets, time.Now().Sub(start).Seconds())
	return healthyTablets, nil
}

// FindWorkerTablet will:
// - find a rdonly instance in the keyspace / shard
// - mark it as worker
// - tag it with our worker process
func FindWorkerTablet(ctx context.Context, wr *wrangler.Wrangler, cleaner *wrangler.Cleaner, tsc *discovery.TabletStatsCache, cell, keyspace, shard string, minHealthyRdonlyTablets int) (*topodatapb.TabletAlias, error) {
	tabletAlias, err := FindHealthyRdonlyTablet(ctx, wr, tsc, cell, keyspace, shard, minHealthyRdonlyTablets)
	if err != nil {
		return nil, err
	}

	// We add the tag before calling ChangeSlaveType, so the destination
	// vttablet reloads the worker URL when it reloads the tablet.
	ourURL := servenv.ListeningURL.String()
	wr.Logger().Infof("Adding tag[worker]=%v to tablet %v", ourURL, topoproto.TabletAliasString(tabletAlias))
	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	_, err = wr.TopoServer().UpdateTabletFields(shortCtx, tabletAlias, func(tablet *topodatapb.Tablet) error {
		if tablet.Tags == nil {
			tablet.Tags = make(map[string]string)
		}
		tablet.Tags["worker"] = ourURL
		tablet.Tags["drain_reason"] = "Used by vtworker"
		return nil
	})
	cancel()
	if err != nil {
		return nil, err
	}
	// Using "defer" here because we remove the tag *before* calling
	// ChangeSlaveType back, so we need to record this tag change after the change
	// slave type change in the cleaner.
	defer wrangler.RecordTabletTagAction(cleaner, tabletAlias, "worker", "")
	defer wrangler.RecordTabletTagAction(cleaner, tabletAlias, "drain_reason", "")

	wr.Logger().Infof("Changing tablet %v to '%v'", topoproto.TabletAliasString(tabletAlias), topodatapb.TabletType_DRAINED)
	shortCtx, cancel = context.WithTimeout(ctx, *remoteActionsTimeout)
	err = wr.ChangeSlaveType(shortCtx, tabletAlias, topodatapb.TabletType_DRAINED)
	cancel()
	if err != nil {
		return nil, err
	}

	// Record a clean-up action to take the tablet back to rdonly.
	wrangler.RecordChangeSlaveTypeAction(cleaner, tabletAlias, topodatapb.TabletType_DRAINED, topodatapb.TabletType_RDONLY)
	return tabletAlias, nil
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
