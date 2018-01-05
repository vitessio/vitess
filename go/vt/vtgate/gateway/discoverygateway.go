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

package gateway

import (
	"flag"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/flagutil"
	"github.com/youtube/vitess/go/vt/discovery"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vterrors"
	"github.com/youtube/vitess/go/vt/vtgate/buffer"
	"github.com/youtube/vitess/go/vt/vtgate/masterbuffer"
	"github.com/youtube/vitess/go/vt/vttablet/queryservice"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
)

var (
	cellsToWatch        = flag.String("cells_to_watch", "", "comma-separated list of cells for watching tablets")
	tabletFilters       flagutil.StringListValue
	refreshInterval     = flag.Duration("tablet_refresh_interval", 1*time.Minute, "tablet refresh interval")
	topoReadConcurrency = flag.Int("topo_read_concurrency", 32, "concurrent topo reads")
	allowedTabletTypes  []topodatapb.TabletType
)

const (
	gatewayImplementationDiscovery = "discoverygateway"
)

func init() {
	flag.Var(&tabletFilters, "tablet_filters", "Specifies a comma-separated list of 'keyspace|shard_name or keyrange' values to filter the tablets to watch")
	topoproto.TabletTypeListVar(&allowedTabletTypes, "allowed_tablet_types", "Specifies the tablet types this vtgate is allowed to route queries to")
	RegisterCreator(gatewayImplementationDiscovery, createDiscoveryGateway)
}

type discoveryGateway struct {
	queryservice.QueryService
	hc            discovery.HealthCheck
	tsc           *discovery.TabletStatsCache
	topoServer    *topo.Server
	srvTopoServer topo.SrvTopoServer
	localCell     string
	retryCount    int

	// tabletsWatchers contains a list of all the watchers we use.
	// We create one per cell.
	tabletsWatchers []*discovery.TopologyWatcher

	// mu protects the fields of this group.
	mu sync.RWMutex
	// statusAggregators is a map indexed by the key
	// keyspace/shard/tablet_type.
	statusAggregators map[string]*TabletStatusAggregator

	// buffer, if enabled, buffers requests during a detected MASTER failover.
	buffer *buffer.Buffer
}

func createDiscoveryGateway(hc discovery.HealthCheck, topoServer *topo.Server, serv topo.SrvTopoServer, cell string, retryCount int) Gateway {
	dg := &discoveryGateway{
		hc:                hc,
		tsc:               discovery.NewTabletStatsCacheDoNotSetListener(topoServer, cell),
		topoServer:        topoServer,
		srvTopoServer:     serv,
		localCell:         cell,
		retryCount:        retryCount,
		tabletsWatchers:   make([]*discovery.TopologyWatcher, 0, 1),
		statusAggregators: make(map[string]*TabletStatusAggregator),
		buffer:            buffer.New(),
	}

	// Set listener which will update TabletStatsCache and MasterBuffer.
	// We set sendDownEvents=true because it's required by TabletStatsCache.
	hc.SetListener(dg, true /* sendDownEvents */)

	log.Infof("loading tablets for cells: %v", *cellsToWatch)
	for _, c := range strings.Split(*cellsToWatch, ",") {
		if c == "" {
			continue
		}
		var tr discovery.TabletRecorder = dg.hc
		if len(tabletFilters) > 0 {
			fbs, err := discovery.NewFilterByShard(dg.hc, tabletFilters)
			if err != nil {
				log.Exitf("Cannot parse tablet_filters parameter: %v", err)
			}
			tr = fbs
		}

		ctw := discovery.NewCellTabletsWatcher(dg.topoServer, tr, c, *refreshInterval, *topoReadConcurrency)
		dg.tabletsWatchers = append(dg.tabletsWatchers, ctw)
	}
	dg.QueryService = queryservice.Wrap(dg, dg.withRetry)
	return dg
}

// StatsUpdate forwards HealthCheck updates to TabletStatsCache and MasterBuffer.
// It is part of the discovery.HealthCheckStatsListener interface.
func (dg *discoveryGateway) StatsUpdate(ts *discovery.TabletStats) {
	dg.tsc.StatsUpdate(ts)

	if ts.Target.TabletType == topodatapb.TabletType_MASTER {
		dg.buffer.StatsUpdate(ts)
	}
}

// WaitForTablets is part of the gateway.Gateway interface.
func (dg *discoveryGateway) WaitForTablets(ctx context.Context, tabletTypesToWait []topodatapb.TabletType) error {
	// Skip waiting for tablets if we are not told to do so.
	if len(tabletTypesToWait) == 0 {
		return nil
	}

	return dg.tsc.WaitForAllServingTablets(ctx, dg.srvTopoServer, dg.localCell, tabletTypesToWait)
}

// StreamHealth is currently not implemented.
// This function hides the inner implementation.
// TODO(alainjobart): Maybe we should?
func (dg *discoveryGateway) StreamHealth(ctx context.Context, callback func(*querypb.StreamHealthResponse) error) error {
	panic("not implemented")
}

// Close shuts down underlying connections.
// This function hides the inner implementation.
func (dg *discoveryGateway) Close(ctx context.Context) error {
	dg.buffer.Shutdown()
	for _, ctw := range dg.tabletsWatchers {
		ctw.Stop()
	}
	return nil
}

// CacheStatus returns a list of TabletCacheStatus per
// keyspace/shard/tablet_type.
func (dg *discoveryGateway) CacheStatus() TabletCacheStatusList {
	dg.mu.RLock()
	res := make(TabletCacheStatusList, 0, len(dg.statusAggregators))
	for _, aggr := range dg.statusAggregators {
		res = append(res, aggr.GetCacheStatus())
	}
	dg.mu.RUnlock()
	sort.Sort(res)
	return res
}

// withRetry gets available connections and executes the action. If there are retryable errors,
// it retries retryCount times before failing. It does not retry if the connection is in
// the middle of a transaction. While returning the error check if it maybe a result of
// a resharding event, and set the re-resolve bit and let the upper layers
// re-resolve and retry.
func (dg *discoveryGateway) withRetry(ctx context.Context, target *querypb.Target, conn queryservice.QueryService, name string, inTransaction bool, inner func(ctx context.Context, target *querypb.Target, conn queryservice.QueryService) (error, bool)) error {
	var tabletLastUsed *topodatapb.Tablet
	var err error
	invalidTablets := make(map[string]bool)

	if len(allowedTabletTypes) > 0 {
		var match bool
		for _, allowed := range allowedTabletTypes {
			if allowed == target.TabletType {
				match = true
				break
			}
		}
		if !match {
			return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "requested tablet type %v is not part of the allowed tablet types for this vtgate: %+v", target.TabletType.String(), allowedTabletTypes)
		}
	}

	bufferedOnce := false
	for i := 0; i < dg.retryCount+1; i++ {
		// Check if we should buffer MASTER queries which failed due to an ongoing
		// failover.
		// Note: We only buffer once and only "!inTransaction" queries i.e.
		// a) no transaction is necessary (e.g. critical reads) or
		// b) no transaction was created yet.
		if !bufferedOnce && !inTransaction && target.TabletType == topodatapb.TabletType_MASTER {
			// The next call blocks if we should buffer during a failover.
			retryDone, bufferErr := dg.buffer.WaitForFailoverEnd(ctx, target.Keyspace, target.Shard, err)
			if bufferErr != nil {
				// Buffering failed e.g. buffer is already full. Do not retry.
				err = vterrors.Errorf(
					vterrors.Code(bufferErr),
					"failed to automatically buffer and retry failed request during failover: %v original err (type=%T): %v",
					bufferErr, err, err)
				break
			}

			// Request may have been buffered.
			if retryDone != nil {
				// We're going to retry this request as part of a buffer drain.
				// Notify the buffer after we retried.
				defer retryDone()
				bufferedOnce = true
			}
		}

		tablets := dg.tsc.GetHealthyTabletStats(target.Keyspace, target.Shard, target.TabletType)
		if len(tablets) == 0 {
			// fail fast if there is no tablet
			err = vterrors.New(vtrpcpb.Code_UNAVAILABLE, "no valid tablet")
			break
		}
		shuffleTablets(dg.localCell, tablets)

		// skip tablets we tried before
		var ts *discovery.TabletStats
		for _, t := range tablets {
			if _, ok := invalidTablets[t.Key]; !ok {
				ts = &t
				break
			}
		}
		if ts == nil {
			if err == nil {
				// do not override error from last attempt.
				err = vterrors.New(vtrpcpb.Code_UNAVAILABLE, "no available connection")
			}
			break
		}

		// execute
		tabletLastUsed = ts.Tablet
		conn := dg.hc.GetConnection(ts.Key)
		if conn == nil {
			err = vterrors.Errorf(vtrpcpb.Code_UNAVAILABLE, "no connection for key %v tablet %+v", ts.Key, ts.Tablet)
			invalidTablets[ts.Key] = true
			continue
		}

		// Potentially buffer this request.
		if bufferErr := masterbuffer.FakeBuffer(target, inTransaction, i); bufferErr != nil {
			return bufferErr
		}

		startTime := time.Now()
		var canRetry bool
		err, canRetry = inner(ctx, ts.Target, conn)
		dg.updateStats(target, startTime, err)
		if canRetry {
			invalidTablets[ts.Key] = true
			continue
		}
		break
	}
	return NewShardError(err, target, tabletLastUsed, inTransaction)
}

func shuffleTablets(cell string, tablets []discovery.TabletStats) {
	sameCell, diffCell, sameCellMax := 0, 0, -1
	length := len(tablets)

	// move all same cell tablets to the front, this is O(n)
	for {
		sameCellMax = diffCell - 1
		sameCell = nextTablet(cell, tablets, sameCell, length, true)
		diffCell = nextTablet(cell, tablets, diffCell, length, false)
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

	//shuffle in same cell tablets
	for i := sameCellMax; i > 0; i-- {
		swap := rand.Intn(i + 1)
		tablets[i], tablets[swap] = tablets[swap], tablets[i]
	}

	//shuffle in diff cell tablets
	for i, diffCellMin := length-1, sameCellMax+1; i > diffCellMin; i-- {
		swap := rand.Intn(i-sameCellMax) + diffCellMin
		tablets[i], tablets[swap] = tablets[swap], tablets[i]
	}
}

func nextTablet(cell string, tablets []discovery.TabletStats, offset, length int, sameCell bool) int {
	for ; offset < length; offset++ {
		if (tablets[offset].Tablet.Alias.Cell == cell) == sameCell {
			return offset
		}
	}
	return -1
}

func (dg *discoveryGateway) updateStats(target *querypb.Target, startTime time.Time, err error) {
	elapsed := time.Now().Sub(startTime)
	aggr := dg.getStatsAggregator(target)
	aggr.UpdateQueryInfo("", target.TabletType, elapsed, err != nil)
}

func (dg *discoveryGateway) getStatsAggregator(target *querypb.Target) *TabletStatusAggregator {
	key := fmt.Sprintf("%v/%v/%v", target.Keyspace, target.Shard, target.TabletType.String())

	// get existing aggregator
	dg.mu.RLock()
	aggr, ok := dg.statusAggregators[key]
	dg.mu.RUnlock()
	if ok {
		return aggr
	}
	// create a new one, but check again before the creation
	dg.mu.Lock()
	defer dg.mu.Unlock()
	aggr, ok = dg.statusAggregators[key]
	if ok {
		return aggr
	}
	aggr = NewTabletStatusAggregator(target.Keyspace, target.Shard, target.TabletType, key)
	dg.statusAggregators[key] = aggr
	return aggr
}
