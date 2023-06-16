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

package vtgate

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/buffer"
	"vitess.io/vitess/go/vt/vttablet/queryservice"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

var (
	_ discovery.HealthCheck = (*discovery.HealthCheckImpl)(nil)
	// CellsToWatch is the list of cells the healthcheck operates over. If it is empty, only the local cell is watched
	CellsToWatch string

	bufferImplementation = "keyspace_events"
	initialTabletTimeout = 30 * time.Second
	// retryCount is the number of times a query will be retried on error
	retryCount = 2
)

func init() {
	servenv.OnParseFor("vtgate", func(fs *pflag.FlagSet) {
		fs.StringVar(&CellsToWatch, "cells_to_watch", "", "comma-separated list of cells for watching tablets")
		fs.StringVar(&bufferImplementation, "buffer_implementation", "keyspace_events", "Allowed values: healthcheck (legacy implementation), keyspace_events (default)")
		fs.DurationVar(&initialTabletTimeout, "gateway_initial_tablet_timeout", 30*time.Second, "At startup, the tabletGateway will wait up to this duration to get at least one tablet per keyspace/shard/tablet type")
		fs.IntVar(&retryCount, "retry-count", 2, "retry count")
	})
}

// TabletGateway implements the Gateway interface.
// This implementation uses the new healthcheck module.
type TabletGateway struct {
	queryservice.QueryService
	hc                   discovery.HealthCheck
	kev                  *discovery.KeyspaceEventWatcher
	srvTopoServer        srvtopo.Server
	localCell            string
	retryCount           int
	defaultConnCollation uint32

	// mu protects the fields of this group.
	mu sync.Mutex
	// statusAggregators is a map indexed by the key
	// keyspace/shard/tablet_type.
	statusAggregators map[string]*TabletStatusAggregator

	// buffer, if enabled, buffers requests during a detected PRIMARY failover.
	buffer *buffer.Buffer
}

func createHealthCheck(ctx context.Context, retryDelay, timeout time.Duration, ts *topo.Server, cell, cellsToWatch string) discovery.HealthCheck {
	return discovery.NewHealthCheck(ctx, retryDelay, timeout, ts, cell, cellsToWatch)
}

// NewTabletGateway creates and returns a new TabletGateway
func NewTabletGateway(ctx context.Context, hc discovery.HealthCheck, serv srvtopo.Server, localCell string) *TabletGateway {
	// hack to accomodate various users of gateway + tests
	if hc == nil {
		var topoServer *topo.Server
		if serv != nil {
			var err error
			topoServer, err = serv.GetTopoServer()
			if err != nil {
				log.Exitf("Unable to create new TabletGateway: %v", err)
			}
		}
		hc = createHealthCheck(ctx, healthCheckRetryDelay, healthCheckTimeout, topoServer, localCell, CellsToWatch)
	}
	gw := &TabletGateway{
		hc:                hc,
		srvTopoServer:     serv,
		localCell:         localCell,
		retryCount:        retryCount,
		statusAggregators: make(map[string]*TabletStatusAggregator),
	}
	gw.setupBuffering(ctx)
	gw.QueryService = queryservice.Wrap(nil, gw.withRetry)
	return gw
}

func (gw *TabletGateway) setupBuffering(ctx context.Context) {
	cfg := buffer.NewConfigFromFlags()
	gw.buffer = buffer.New(cfg)

	switch bufferImplementation {
	case "healthcheck":
		// subscribe to healthcheck updates so that buffer can be notified if needed
		// we run this in a separate goroutine so that normal processing doesn't need to block
		hcChan := gw.hc.Subscribe()
		bufferCtx, bufferCancel := context.WithCancel(ctx)

		go func(ctx context.Context, c chan *discovery.TabletHealth, buffer *buffer.Buffer) {
			defer bufferCancel()

			for {
				select {
				case <-ctx.Done():
					return
				case result := <-hcChan:
					if result == nil {
						return
					}
					if result.Target.TabletType == topodatapb.TabletType_PRIMARY {
						buffer.ProcessPrimaryHealth(result)
					}
				}
			}
		}(bufferCtx, hcChan, gw.buffer)

	case "keyspace_events":
		gw.kev = discovery.NewKeyspaceEventWatcher(ctx, gw.srvTopoServer, gw.hc, gw.localCell)
		ksChan := gw.kev.Subscribe()
		bufferCtx, bufferCancel := context.WithCancel(ctx)

		go func(ctx context.Context, c chan *discovery.KeyspaceEvent, buffer *buffer.Buffer) {
			defer bufferCancel()

			for {
				select {
				case <-ctx.Done():
					return
				case result := <-ksChan:
					if result == nil {
						return
					}
					buffer.HandleKeyspaceEvent(result)
				}
			}
		}(bufferCtx, ksChan, gw.buffer)

	default:
		log.Exitf("unknown buffering implementation for TabletGateway: %q", bufferImplementation)
	}
}

// QueryServiceByAlias satisfies the Gateway interface
func (gw *TabletGateway) QueryServiceByAlias(alias *topodatapb.TabletAlias, target *querypb.Target) (queryservice.QueryService, error) {
	qs, err := gw.hc.TabletConnection(alias, target)
	return queryservice.Wrap(qs, gw.withShardError), NewShardError(err, target)
}

// RegisterStats registers the stats to export the lag since the last refresh
// and the checksum of the topology
func (gw *TabletGateway) RegisterStats() {
	gw.hc.RegisterStats()
}

// WaitForTablets is part of the Gateway interface.
func (gw *TabletGateway) WaitForTablets(tabletTypesToWait []topodatapb.TabletType) (err error) {
	log.Infof("Gateway waiting for serving tablets of types %v ...", tabletTypesToWait)
	ctx, cancel := context.WithTimeout(context.Background(), initialTabletTimeout)
	defer cancel()

	defer func() {
		switch err {
		case nil:
			// Log so we know everything is fine.
			log.Infof("Waiting for tablets completed")
		case context.DeadlineExceeded:
			// In this scenario, we were able to reach the
			// topology service, but some tablets may not be
			// ready. We just warn and keep going.
			log.Warningf("Timeout waiting for all keyspaces / shards to have healthy tablets of types %v, may be in degraded mode", tabletTypesToWait)
			err = nil
		}
	}()

	// Skip waiting for tablets if we are not told to do so.
	if len(tabletTypesToWait) == 0 {
		return nil
	}

	// Finds the targets to look for.
	targets, err := srvtopo.FindAllTargets(ctx, gw.srvTopoServer, gw.localCell, tabletTypesToWait)
	if err != nil {
		return err
	}
	return gw.hc.WaitForAllServingTablets(ctx, targets)
}

// Close shuts down underlying connections.
// This function hides the inner implementation.
func (gw *TabletGateway) Close(_ context.Context) error {
	gw.buffer.Shutdown()
	return gw.hc.Close()
}

// CacheStatus returns a list of TabletCacheStatus per
// keyspace/shard/tablet_type.
func (gw *TabletGateway) CacheStatus() TabletCacheStatusList {
	gw.mu.Lock()
	res := make(TabletCacheStatusList, 0, len(gw.statusAggregators))
	for _, aggr := range gw.statusAggregators {
		res = append(res, aggr.GetCacheStatus())
	}
	gw.mu.Unlock()
	sort.Sort(res)
	return res
}

// withRetry gets available connections and executes the action. If there are retryable errors,
// it retries retryCount times before failing. It does not retry if the connection is in
// the middle of a transaction. While returning the error check if it maybe a result of
// a resharding event, and set the re-resolve bit and let the upper layers
// re-resolve and retry.
//
// withRetry also adds shard information to errors returned from the inner QueryService, so
// withShardError should not be combined with withRetry.
func (gw *TabletGateway) withRetry(ctx context.Context, target *querypb.Target, _ queryservice.QueryService,
	_ string, inTransaction bool, inner func(ctx context.Context, target *querypb.Target, conn queryservice.QueryService) (bool, error)) error {
	// for transactions, we connect to a specific tablet instead of letting gateway choose one
	if inTransaction && target.TabletType != topodatapb.TabletType_PRIMARY {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "tabletGateway's query service can only be used for non-transactional queries on replicas")
	}
	var tabletLastUsed *topodatapb.Tablet
	var err error
	invalidTablets := make(map[string]bool)

	if len(discovery.AllowedTabletTypes) > 0 {
		var match bool
		for _, allowed := range discovery.AllowedTabletTypes {
			if allowed == target.TabletType {
				match = true
				break
			}
		}
		if !match {
			return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "requested tablet type %v is not part of the allowed tablet types for this vtgate: %+v", target.TabletType.String(), discovery.AllowedTabletTypes)
		}
	}

	bufferedOnce := false
	for i := 0; i < gw.retryCount+1; i++ {
		// Check if we should buffer PRIMARY queries which failed due to an ongoing
		// failover.
		// Note: We only buffer once and only "!inTransaction" queries i.e.
		// a) no transaction is necessary (e.g. critical reads) or
		// b) no transaction was created yet.
		if !bufferedOnce && !inTransaction && target.TabletType == topodatapb.TabletType_PRIMARY {
			// The next call blocks if we should buffer during a failover.
			retryDone, bufferErr := gw.buffer.WaitForFailoverEnd(ctx, target.Keyspace, target.Shard, err)

			// Request may have been buffered.
			if retryDone != nil {
				// We're going to retry this request as part of a buffer drain.
				// Notify the buffer after we retried.
				defer retryDone()
				bufferedOnce = true
			}

			if bufferErr != nil {
				err = vterrors.Wrapf(bufferErr,
					"failed to automatically buffer and retry failed request during failover. original err (type=%T): %v",
					err, err)
				break
			}
		}

		tablets := gw.hc.GetHealthyTabletStats(target)
		if len(tablets) == 0 {
			// if we have a keyspace event watcher, check if the reason why our primary is not available is that it's currently being resharded
			// or if a reparent operation is in progress.
			if kev := gw.kev; kev != nil {
				if kev.TargetIsBeingResharded(target) {
					err = vterrors.Errorf(vtrpcpb.Code_CLUSTER_EVENT, "current keyspace is being resharded")
					continue
				}
				primary, notServing := kev.PrimaryIsNotServing(target)
				if notServing {
					err = vterrors.Errorf(vtrpcpb.Code_CLUSTER_EVENT, "primary is not serving, there may be a reparent operation in progress")
					continue
				}
				// if primary is serving, but we initially found no tablet, we're in an inconsistent state
				// we then retry the entire loop
				if primary != nil {
					err = vterrors.Errorf(vtrpcpb.Code_UNAVAILABLE, "inconsistent state detected, primary is serving but initially found no available tablet")
					continue
				}
			}

			// fail fast if there is no tablet
			err = vterrors.Errorf(vtrpcpb.Code_UNAVAILABLE, "no healthy tablet available for '%s'", target.String())
			break
		}
		gw.shuffleTablets(gw.localCell, tablets)

		var th *discovery.TabletHealth
		// skip tablets we tried before
		for _, t := range tablets {
			if _, ok := invalidTablets[topoproto.TabletAliasString(t.Tablet.Alias)]; !ok {
				th = t
				break
			}
		}
		if th == nil {
			// do not override error from last attempt.
			if err == nil {
				err = vterrors.VT14002()
			}
			break
		}

		tabletLastUsed = th.Tablet
		// execute
		if th.Conn == nil {
			err = vterrors.VT14003(tabletLastUsed)
			invalidTablets[topoproto.TabletAliasString(tabletLastUsed.Alias)] = true
			continue
		}

		gw.updateDefaultConnCollation(tabletLastUsed)

		startTime := time.Now()
		var canRetry bool
		canRetry, err = inner(ctx, target, th.Conn)
		gw.updateStats(target, startTime, err)
		if canRetry {
			invalidTablets[topoproto.TabletAliasString(tabletLastUsed.Alias)] = true
			continue
		}
		break
	}
	return NewShardError(err, target)
}

// withShardError adds shard information to errors returned from the inner QueryService.
func (gw *TabletGateway) withShardError(ctx context.Context, target *querypb.Target, conn queryservice.QueryService,
	_ string, _ bool, inner func(ctx context.Context, target *querypb.Target, conn queryservice.QueryService) (bool, error)) error {
	_, err := inner(ctx, target, conn)
	return NewShardError(err, target)
}

func (gw *TabletGateway) updateStats(target *querypb.Target, startTime time.Time, err error) {
	elapsed := time.Since(startTime)
	aggr := gw.getStatsAggregator(target)
	aggr.UpdateQueryInfo("", target.TabletType, elapsed, err != nil)
}

func (gw *TabletGateway) getStatsAggregator(target *querypb.Target) *TabletStatusAggregator {
	key := fmt.Sprintf("%v/%v/%v", target.Keyspace, target.Shard, target.TabletType.String())

	// get existing aggregator
	gw.mu.Lock()
	defer gw.mu.Unlock()
	aggr, ok := gw.statusAggregators[key]
	if ok {
		return aggr
	}
	// create a new one if it doesn't exist yet
	aggr = NewTabletStatusAggregator(target.Keyspace, target.Shard, target.TabletType, key)
	gw.statusAggregators[key] = aggr
	return aggr
}

func (gw *TabletGateway) shuffleTablets(cell string, tablets []*discovery.TabletHealth) {
	sameCell, diffCell, sameCellMax := 0, 0, -1
	length := len(tablets)

	// move all same cell tablets to the front, this is O(n)
	for {
		sameCellMax = diffCell - 1
		sameCell = gw.nextTablet(cell, tablets, sameCell, length, true)
		diffCell = gw.nextTablet(cell, tablets, diffCell, length, false)
		// either no more diffs or no more same cells should stop the iteration
		if sameCell < 0 || diffCell < 0 {
			break
		}

		if sameCell < diffCell {
			// fast forward the `sameCell` lookup to `diffCell + 1`, `diffCell` unchanged
			sameCell = diffCell + 1
		} else {
			// sameCell > diffCell, swap needed
			tablets[sameCell], tablets[diffCell] = tablets[diffCell], tablets[sameCell]
			sameCell++
			diffCell++
		}
	}

	// shuffle in same cell tablets
	for i := sameCellMax; i > 0; i-- {
		swap := rand.Intn(i + 1)
		tablets[i], tablets[swap] = tablets[swap], tablets[i]
	}

	// shuffle in diff cell tablets
	for i, diffCellMin := length-1, sameCellMax+1; i > diffCellMin; i-- {
		swap := rand.Intn(i-sameCellMax) + diffCellMin
		tablets[i], tablets[swap] = tablets[swap], tablets[i]
	}
}

func (gw *TabletGateway) nextTablet(cell string, tablets []*discovery.TabletHealth, offset, length int, sameCell bool) int {
	for ; offset < length; offset++ {
		if (tablets[offset].Tablet.Alias.Cell == cell) == sameCell {
			return offset
		}
	}
	return -1
}

// TabletsCacheStatus returns a displayable version of the health check cache.
func (gw *TabletGateway) TabletsCacheStatus() discovery.TabletsCacheStatusList {
	return gw.hc.CacheStatus()
}

func (gw *TabletGateway) updateDefaultConnCollation(tablet *topodatapb.Tablet) {
	if atomic.CompareAndSwapUint32(&gw.defaultConnCollation, 0, tablet.DefaultConnCollation) {
		return
	}
	if atomic.LoadUint32(&gw.defaultConnCollation) != tablet.DefaultConnCollation {
		log.Warning("this Vitess cluster has tablets with different default connection collations")
	}
}

// DefaultConnCollation returns the default connection collation of this TabletGateway
func (gw *TabletGateway) DefaultConnCollation() collations.ID {
	return collations.ID(atomic.LoadUint32(&gw.defaultConnCollation))
}

// NewShardError returns a new error with the shard info amended.
func NewShardError(in error, target *querypb.Target) error {
	if in == nil {
		return nil
	}
	if target != nil {
		return vterrors.Wrapf(in, "target: %s.%s.%s", target.Keyspace, target.Shard, topoproto.TabletTypeLString(target.TabletType))
	}
	return in
}
