// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

import (
	"flag"
	"fmt"
	"math/rand"
	"time"

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
	waitForHealthyTabletsTimeout = flag.Duration("wait_for_healthy_rdonly_tablets_timeout", 60*time.Second, "maximum time to wait if less than --min_healthy_rdonly_tablets are available")
)

// FindHealthyRdonlyTablet returns a random healthy RDONLY tablet.
// Since we don't want to use them all, we require at least
// minHealthyRdonlyTablets servers to be healthy.
// May block up to -wait_for_healthy_rdonly_tablets_timeout.
func FindHealthyRdonlyTablet(ctx context.Context, wr *wrangler.Wrangler, cell, keyspace, shard string, minHealthyRdonlyTablets int) (*topodatapb.TabletAlias, error) {
	busywaitCtx, busywaitCancel := context.WithTimeout(ctx, *waitForHealthyTabletsTimeout)
	defer busywaitCancel()

	// Use a healthcheck instance to get the health of all RDONLY tablets.
	healthCheck := discovery.NewHealthCheck(*remoteActionsTimeout, *healthcheckRetryDelay, *healthCheckTimeout)
	watcher := discovery.NewShardReplicationWatcher(wr.TopoServer(), healthCheck, cell, keyspace, shard, *healthCheckTopologyRefresh, discovery.DefaultTopoReadConcurrency)
	defer watcher.Stop()
	defer healthCheck.Close()

	start := time.Now()
	deadlineForLog, _ := busywaitCtx.Deadline()
	wr.Logger().Infof("Waiting for enough healthy RDONLY tablets to become available. required: %v Waiting up to %.1f seconds.",
		minHealthyRdonlyTablets, deadlineForLog.Sub(time.Now()).Seconds())

	// Wait for at least one RDONLY tablet initially before checking the list.
	if err := discovery.WaitForTablets(busywaitCtx, healthCheck, cell, keyspace, shard, []topodatapb.TabletType{topodatapb.TabletType_RDONLY}); err != nil {
		return nil, fmt.Errorf("error waiting for RDONLY tablets for (%v,%v/%v): %v", cell, keyspace, shard, err)
	}

	var healthyTablets []*discovery.TabletStats
	for {
		select {
		case <-busywaitCtx.Done():
			return nil, fmt.Errorf("not enough healthy RDONLY tablets to choose from in (%v,%v/%v), have %v healthy ones, need at least %v Context error: %v",
				cell, keyspace, shard, len(healthyTablets), minHealthyRdonlyTablets, busywaitCtx.Err())
		default:
		}

		healthyTablets = discovery.RemoveUnhealthyTablets(
			healthCheck.GetTabletStatsFromTarget(keyspace, shard, topodatapb.TabletType_RDONLY))
		if len(healthyTablets) >= minHealthyRdonlyTablets {
			break
		}

		deadlineForLog, _ := busywaitCtx.Deadline()
		wr.Logger().Infof("Waiting for enough healthy RDONLY tablets to become available. available: %v required: %v Waiting up to %.1f more seconds.",
			len(healthyTablets), minHealthyRdonlyTablets, deadlineForLog.Sub(time.Now()).Seconds())
		// Block for 1 second because 2 seconds is the -health_check_interval flag value in integration tests.
		timer := time.NewTimer(1 * time.Second)
		select {
		case <-busywaitCtx.Done():
			timer.Stop()
		case <-timer.C:
		}
	}
	wr.Logger().Infof("At least %v healthy RDONLY tablets are available (required: %v). Took %.1f seconds to find this out.",
		len(healthyTablets), minHealthyRdonlyTablets, time.Now().Sub(start).Seconds())

	// random server in the list is what we want
	index := rand.Intn(len(healthyTablets))
	return healthyTablets[index].Tablet.Alias, nil
}

// FindWorkerTablet will:
// - find a rdonly instance in the keyspace / shard
// - mark it as worker
// - tag it with our worker process
func FindWorkerTablet(ctx context.Context, wr *wrangler.Wrangler, cleaner *wrangler.Cleaner, cell, keyspace, shard string, minHealthyRdonlyTablets int) (*topodatapb.TabletAlias, error) {
	tabletAlias, err := FindHealthyRdonlyTablet(ctx, wr, cell, keyspace, shard, minHealthyRdonlyTablets)
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

	wr.Logger().Infof("Changing tablet %v to '%v'", topoproto.TabletAliasString(tabletAlias), topodatapb.TabletType_WORKER)
	shortCtx, cancel = context.WithTimeout(ctx, *remoteActionsTimeout)
	err = wr.ChangeSlaveType(shortCtx, tabletAlias, topodatapb.TabletType_WORKER)
	cancel()
	if err != nil {
		return nil, err
	}

	// Record a clean-up action to take the tablet back to rdonly.
	wrangler.RecordChangeSlaveTypeAction(cleaner, tabletAlias, topodatapb.TabletType_WORKER, topodatapb.TabletType_RDONLY)
	return tabletAlias, nil
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
