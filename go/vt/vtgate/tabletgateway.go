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
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/buffer"
	"vitess.io/vitess/go/vt/vttablet/queryservice"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

const (
	tabletGatewayImplementation = "tabletgateway"
)

func init() {
	RegisterGatewayCreator(tabletGatewayImplementation, createTabletGateway)
}

// TabletGateway implements the Gateway interface.
// This implementation uses the new healthcheck module.
type TabletGateway struct {
	queryservice.QueryService
	hc            discovery.HealthCheck
	srvTopoServer srvtopo.Server
	localCell     string
	retryCount    int

	// mu protects the fields of this group.
	mu sync.Mutex
	// statusAggregators is a map indexed by the key
	// keyspace/shard/tablet_type.
	statusAggregators map[string]*TabletStatusAggregator

	// buffer, if enabled, buffers requests during a detected MASTER failover.
	buffer *buffer.Buffer
}

func createTabletGateway(ctx context.Context, unused discovery.LegacyHealthCheck, serv srvtopo.Server,
	cell string, unused2 int) Gateway {
	return NewTabletGateway(ctx, serv, cell)
}

// NewTabletGateway creates and returns a new TabletGateway
func NewTabletGateway(ctx context.Context, serv srvtopo.Server, localCell string) *TabletGateway {
	var topoServer *topo.Server
	if serv != nil {
		var err error
		topoServer, err = serv.GetTopoServer()
		if err != nil {
			log.Exitf("Unable to create new TabletGateway: %v", err)
		}
	}

	hc := discovery.NewHealthCheck(ctx, *HealthCheckRetryDelay, *HealthCheckTimeout, topoServer, localCell)

	gw := &TabletGateway{
		hc:                hc,
		srvTopoServer:     serv,
		localCell:         localCell,
		retryCount:        *RetryCount,
		statusAggregators: make(map[string]*TabletStatusAggregator),
		buffer:            buffer.New(),
	}

	// TODO(deepthi): start healthcheck here
	//hc.Open()

	gw.QueryService = queryservice.Wrap(nil, gw.withRetry)
	return gw
}

// RegisterStats registers the stats to export the lag since the last refresh
// and the checksum of the topology
func (gw *TabletGateway) RegisterStats() {
	gw.hc.RegisterStats()
}

// StatsUpdate forwards LegacyHealthCheck updates to TabletStatsCache and MasterBuffer.
// It is part of the discovery.HealthCheckStatsListener interface.
// TODO(deepthi): figure out how to update buffer
//func (gw *TabletGateway) StatsUpdate(ts *discovery.LegacyTabletStats) {
//	if ts.Target.TabletType == topodatapb.TabletType_MASTER {
//		gw.buffer.StatsUpdate(ts)
//	}
//}

// WaitForTablets is part of the Gateway interface.
func (gw *TabletGateway) WaitForTablets(ctx context.Context, tabletTypesToWait []topodatapb.TabletType) error {
	// Skip waiting for tablets if we are not told to do so.
	if len(tabletTypesToWait) == 0 {
		return nil
	}

	// Finds the targets to look for.
	_, err := srvtopo.FindAllTargets(ctx, gw.srvTopoServer, gw.localCell, tabletTypesToWait)
	if err != nil {
		return err
	}
	return nil
	// TODO(deepthi): this needs to be implemented
	//return gw.hc.WaitForAllServingTablets(ctx, targets)
}

// Close shuts down underlying connections.
// This function hides the inner implementation.
func (gw *TabletGateway) Close(ctx context.Context) error {
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
func (gw *TabletGateway) withRetry(ctx context.Context, target *querypb.Target, unused queryservice.QueryService,
	name string, inTransaction bool, inner func(ctx context.Context, target *querypb.Target, conn queryservice.QueryService) (bool, error)) error {
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
			return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
				"requested tablet type %v is not part of the allowed tablet types for this vtgate: %+v",
				target.TabletType.String(), discovery.AllowedTabletTypes)
		}
	}

	bufferedOnce := false
	for i := 0; i < gw.retryCount+1; i++ {
		// Check if we should buffer MASTER queries which failed due to an ongoing
		// failover.
		// Note: We only buffer once and only "!inTransaction" queries i.e.
		// a) no transaction is necessary (e.g. critical reads) or
		// b) no transaction was created yet.
		if !bufferedOnce && !inTransaction && target.TabletType == topodatapb.TabletType_MASTER {
			// The next call blocks if we should buffer during a failover.
			retryDone, bufferErr := gw.buffer.WaitForFailoverEnd(ctx, target.Keyspace, target.Shard, err)
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

		tablets := gw.hc.GetHealthyTabletStats(target.Keyspace, target.Shard, target.TabletType)
		if len(tablets) == 0 {
			// fail fast if there is no tablet
			err = vterrors.New(vtrpcpb.Code_UNAVAILABLE, "no valid tablet")
			break
		}
		gw.shuffleTablets(gw.localCell, tablets)

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
		conn := gw.hc.GetConnection(ts.Key)
		if conn == nil {
			err = vterrors.Errorf(vtrpcpb.Code_UNAVAILABLE, "no connection for key %v tablet %+v", ts.Key, ts.Tablet)
			invalidTablets[ts.Key] = true
			continue
		}

		startTime := time.Now()
		var canRetry bool
		canRetry, err = inner(ctx, ts.Target, conn)
		gw.updateStats(target, startTime, err)
		if canRetry {
			invalidTablets[ts.Key] = true
			continue
		}
		break
	}
	return NewShardError(err, target, tabletLastUsed)
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

func (gw *TabletGateway) shuffleTablets(cell string, tablets []discovery.TabletStats) {
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

func (gw *TabletGateway) nextTablet(cell string, tablets []discovery.TabletStats, offset, length int, sameCell bool) int {
	for ; offset < length; offset++ {
		if (tablets[offset].Tablet.Alias.Cell == cell) == sameCell {
			return offset
		}
	}
	return -1
}
