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
	minHealthyEndPoints = flag.Int("min_healthy_rdonly_endpoints", 2, "minimum number of healthy rdonly endpoints before taking out one")

	// WaitForHealthyEndPointsTimeout intent is to wait for the
	// healthcheck to automatically return rdonly instances which
	// have been taken out by previous *Clone or *Diff runs.
	// Therefore, the default for this variable must be higher
	// than -health_check_interval.
	// (it is public for tests to override it)
	WaitForHealthyEndPointsTimeout = flag.Duration("wait_for_healthy_rdonly_endpoints_timeout", 60*time.Second, "maximum time to wait if less than --min_healthy_rdonly_endpoints are available")

	healthCheckTopologyRefresh = flag.Duration("worker_healthcheck_topology_refresh", 30*time.Second, "refresh interval for re-reading the topology")
	healthcheckRetryDelay      = flag.Duration("worker_healthcheck_retry_delay", 5*time.Second, "delay before retrying a failed healthcheck")
	healthCheckTimeout         = flag.Duration("worker_healthcheck_timeout", time.Minute, "the health check timeout period")
)

// FindHealthyRdonlyEndPoint returns a random healthy endpoint.
// Since we don't want to use them all, we require at least
// minHealthyEndPoints servers to be healthy.
// May block up to -wait_for_healthy_rdonly_endpoints_timeout.
func FindHealthyRdonlyEndPoint(ctx context.Context, wr *wrangler.Wrangler, cell, keyspace, shard string) (*topodatapb.TabletAlias, error) {
	busywaitCtx, busywaitCancel := context.WithTimeout(ctx, *WaitForHealthyEndPointsTimeout)
	defer busywaitCancel()

	// create a discovery healthcheck, wait for it to have one rdonly
	// endpoints at this point
	healthCheck := discovery.NewHealthCheck(*remoteActionsTimeout, *healthcheckRetryDelay, *healthCheckTimeout, "" /* statsSuffix */)
	watcher := discovery.NewShardReplicationWatcher(wr.TopoServer(), healthCheck, cell, keyspace, shard, *healthCheckTopologyRefresh, 5 /*topoReadConcurrency*/)
	defer watcher.Stop()
	defer healthCheck.Close()

	deadlineForLog, _ := busywaitCtx.Deadline()
	wr.Logger().Infof("Waiting for enough healthy rdonly endpoints to become available. required: %v Waiting up to %.1f seconds.",
		*minHealthyEndPoints, deadlineForLog.Sub(time.Now()).Seconds())
	if err := discovery.WaitForEndPoints(busywaitCtx, healthCheck, cell, keyspace, shard, []topodatapb.TabletType{topodatapb.TabletType_RDONLY}); err != nil {
		return nil, fmt.Errorf("error waiting for rdonly endpoints for (%v,%v/%v): %v", cell, keyspace, shard, err)
	}

	var healthyEndpoints []*topodatapb.EndPoint
	for {
		select {
		case <-busywaitCtx.Done():
			return nil, fmt.Errorf("Not enough healthy rdonly endpoints to choose from in (%v,%v/%v), have %v healthy ones, need at least %v Context Error: %v",
				cell, keyspace, shard, len(healthyEndpoints), *minHealthyEndPoints, busywaitCtx.Err())
		default:
		}

		addrs := healthCheck.GetEndPointStatsFromTarget(keyspace, shard, topodatapb.TabletType_RDONLY)
		healthyEndpoints = make([]*topodatapb.EndPoint, 0, len(addrs))
		for _, addr := range addrs {
			// Note we do not check the 'Serving' flag here.
			// This is mainly to avoid the case where we run a
			// Diff between a source and destination, and the source
			// is not serving (disabled by TabletControl).
			// When we switch the tablet to 'worker', it will
			// go back to serving state.
			if addr.Stats == nil || addr.Stats.HealthError != "" || addr.Stats.SecondsBehindMaster > 30 {
				continue
			}
			healthyEndpoints = append(healthyEndpoints, addr.EndPoint)
		}

		if len(healthyEndpoints) >= *minHealthyEndPoints {
			break
		}

		deadlineForLog, _ := busywaitCtx.Deadline()
		wr.Logger().Infof("Waiting for enough healthy rdonly endpoints to become available. available: %v required: %v Waiting up to %.1f more seconds.",
			len(healthyEndpoints), *minHealthyEndPoints, deadlineForLog.Sub(time.Now()).Seconds())
		// Block for 1 second because 2 seconds is the -health_check_interval flag value in integration tests.
		timer := time.NewTimer(1 * time.Second)
		select {
		case <-busywaitCtx.Done():
			timer.Stop()
		case <-timer.C:
		}
	}

	// random server in the list is what we want
	index := rand.Intn(len(healthyEndpoints))
	return &topodatapb.TabletAlias{
		Cell: cell,
		Uid:  healthyEndpoints[index].Uid,
	}, nil
}

// FindWorkerTablet will:
// - find a rdonly instance in the keyspace / shard
// - mark it as worker
// - tag it with our worker process
func FindWorkerTablet(ctx context.Context, wr *wrangler.Wrangler, cleaner *wrangler.Cleaner, cell, keyspace, shard string) (*topodatapb.TabletAlias, error) {
	tabletAlias, err := FindHealthyRdonlyEndPoint(ctx, wr, cell, keyspace, shard)
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
	// We will alter this one later on and let the tablet go back to
	// 'spare' if we have stopped replication for too long on it.
	wrangler.RecordChangeSlaveTypeAction(cleaner, tabletAlias, topodatapb.TabletType_RDONLY)
	return tabletAlias, nil
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
