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
	"fmt"
	"sort"
	"sync"
	"time"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vttablet/queryservice"
	"vitess.io/vitess/go/vt/vttablet/tabletconn"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// L2VTGateConn keeps a single connection to a vtgate backend.  The
// underlying vtgate backend must have been started with the
// '-enable_forwarding' flag.
//
// It will keep a healthcheck connection going to the target, to get
// the list of available Targets. It remembers them, and exposes a
// srvtopo.TargetStats interface to query them.
type L2VTGateConn struct {
	queryservice.QueryService

	// addr is the destination address. Immutable.
	addr string

	// name is the name to display for stats. Immutable.
	name string

	// retryCount is the number of times to retry an action. Immutable.
	retryCount int

	// cancel is associated with the life cycle of this L2VTGateConn.
	// It is called when Close is called.
	cancel context.CancelFunc

	// mu protects the following fields.
	mu sync.RWMutex
	// stats has all the stats we received from the other side.
	stats map[l2VTGateConnKey]*l2VTGateConnValue
	// statusAggregators is a map indexed by the key
	// name:keyspace/shard/tablet type
	statusAggregators map[string]*TabletStatusAggregator
}

type l2VTGateConnKey struct {
	keyspace   string
	shard      string
	tabletType topodatapb.TabletType
}

type l2VTGateConnValue struct {
	tabletExternallyReparentedTimestamp int64

	// aggregates has the per-cell aggregates.
	aggregates map[string]*querypb.AggregateStats
}

// NewL2VTGateConn creates a new L2VTGateConn object. It also starts
// the background go routine to monitor its health.
func NewL2VTGateConn(name, addr string, retryCount int) (*L2VTGateConn, error) {
	conn, err := tabletconn.GetDialer()(&topodatapb.Tablet{
		Hostname: addr,
	}, grpcclient.FailFast(true))
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	c := &L2VTGateConn{
		addr:              addr,
		name:              name,
		cancel:            cancel,
		stats:             make(map[l2VTGateConnKey]*l2VTGateConnValue),
		statusAggregators: make(map[string]*TabletStatusAggregator),
	}
	c.QueryService = queryservice.Wrap(conn, c.withRetry)
	go c.checkConn(ctx)
	return c, nil
}

// Close is part of the queryservice.QueryService interface.
func (c *L2VTGateConn) Close(ctx context.Context) error {
	c.cancel()
	return nil
}

func (c *L2VTGateConn) servingConnStats(res map[string]int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, s := range c.stats {
		key := fmt.Sprintf("%s.%s.%s", k.keyspace, k.shard, topoproto.TabletTypeLString(k.tabletType))
		var htc int32
		for _, stats := range s.aggregates {
			htc += stats.HealthyTabletCount
		}
		res[key] += int64(htc)
	}
}

func (c *L2VTGateConn) checkConn(ctx context.Context) {
	for {
		err := c.StreamHealth(ctx, c.streamHealthCallback)
		log.Warningf("StreamHealth to %v failed, will retry after 30s: %v", c.addr, err)
		time.Sleep(30 * time.Second)
	}
}

func (c *L2VTGateConn) streamHealthCallback(shr *querypb.StreamHealthResponse) error {
	key := l2VTGateConnKey{
		keyspace:   shr.Target.Keyspace,
		shard:      shr.Target.Shard,
		tabletType: shr.Target.TabletType,
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.stats[key]
	if !ok {
		// No current value for this keyspace/shard/tablet type.
		// Check if we received a delete, drop it.
		if shr.AggregateStats == nil || (shr.AggregateStats.HealthyTabletCount == 0 && shr.AggregateStats.UnhealthyTabletCount == 0) {
			return nil
		}

		// It's a record for a keyspace/shard/tablet type we
		// don't know yet, just create our new record with one
		// entry in the map for the cell.
		c.stats[key] = &l2VTGateConnValue{
			tabletExternallyReparentedTimestamp: shr.TabletExternallyReparentedTimestamp,
			aggregates: map[string]*querypb.AggregateStats{
				shr.Target.Cell: shr.AggregateStats,
			},
		}
		return nil
	}

	// Save our new value.
	e.tabletExternallyReparentedTimestamp = shr.TabletExternallyReparentedTimestamp
	e.aggregates[shr.Target.Cell] = shr.AggregateStats
	return nil
}

// GetAggregateStats is the discovery part of srvtopo.TargetStats interface.
func (c *L2VTGateConn) GetAggregateStats(target *querypb.Target) (*querypb.AggregateStats, error) {
	key := l2VTGateConnKey{
		keyspace:   target.Keyspace,
		shard:      target.Shard,
		tabletType: target.TabletType,
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	e, ok := c.stats[key]
	if !ok {
		return nil, topo.NewError(topo.NoNode, target.String())
	}

	a, ok := e.aggregates[target.Cell]
	if !ok {
		return nil, topo.NewError(topo.NoNode, target.String())
	}
	return a, nil
}

// GetMasterCell is the discovery part of the srvtopo.TargetStats interface.
func (c *L2VTGateConn) GetMasterCell(keyspace, shard string) (cell string, err error) {
	key := l2VTGateConnKey{
		keyspace:   keyspace,
		shard:      shard,
		tabletType: topodatapb.TabletType_MASTER,
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	e, ok := c.stats[key]
	if !ok {
		return "", topo.NewError(topo.NoNode, keyspace+"/"+shard)
	}

	for cell := range e.aggregates {
		return cell, nil
	}
	return "", topo.NewError(topo.NoNode, keyspace+"/"+shard)
}

// CacheStatus returns a list of TabletCacheStatus per
// name:keyspace/shard/tablet type.
func (c *L2VTGateConn) CacheStatus() TabletCacheStatusList {
	c.mu.RLock()
	res := make(TabletCacheStatusList, 0, len(c.statusAggregators))
	for _, aggr := range c.statusAggregators {
		res = append(res, aggr.GetCacheStatus())
	}
	c.mu.RUnlock()
	sort.Sort(res)
	return res
}

func (c *L2VTGateConn) updateStats(target *querypb.Target, startTime time.Time, err error) {
	elapsed := time.Now().Sub(startTime)
	aggr := c.getStatsAggregator(target)
	aggr.UpdateQueryInfo("", target.TabletType, elapsed, err != nil)
}

func (c *L2VTGateConn) getStatsAggregator(target *querypb.Target) *TabletStatusAggregator {
	key := fmt.Sprintf("%v:%v/%v/%v", c.name, target.Keyspace, target.Shard, target.TabletType.String())

	// get existing aggregator
	c.mu.RLock()
	aggr, ok := c.statusAggregators[key]
	c.mu.RUnlock()
	if ok {
		return aggr
	}

	// create a new one, but check again before the creation
	c.mu.Lock()
	defer c.mu.Unlock()
	aggr, ok = c.statusAggregators[key]
	if ok {
		return aggr
	}
	aggr = NewTabletStatusAggregator(target.Keyspace, target.Shard, target.TabletType, key)
	c.statusAggregators[key] = aggr
	return aggr
}

// withRetry uses the connection to execute the action. If there are
// retryable errors, it retries retryCount times before failing. It
// does not retry if the connection is in the middle of a
// transaction. While returning the error check if it maybe a result
// of a resharding event, and set the re-resolve bit and let the upper
// layers re-resolve and retry.
func (c *L2VTGateConn) withRetry(ctx context.Context, target *querypb.Target, conn queryservice.QueryService, name string, inTransaction bool, inner func(context.Context, *querypb.Target, queryservice.QueryService) (error, bool)) error {
	var err error
	for i := 0; i < c.retryCount+1; i++ {
		startTime := time.Now()
		var canRetry bool
		err, canRetry = inner(ctx, target, conn)
		if target != nil {
			// target can be nil for StreamHealth calls.
			c.updateStats(target, startTime, err)
		}
		if canRetry {
			continue
		}
		break
	}
	return NewShardError(err, target, nil, inTransaction)
}
