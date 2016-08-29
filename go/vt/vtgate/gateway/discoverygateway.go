// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

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
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/discovery"
	"github.com/youtube/vitess/go/vt/tabletserver/querytypes"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vterrors"
	"github.com/youtube/vitess/go/vt/vtgate/masterbuffer"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

var (
	cellsToWatch        = flag.String("cells_to_watch", "", "comma-separated list of cells for watching tablets")
	tabletFilters       flagutil.StringListValue
	refreshInterval     = flag.Duration("tablet_refresh_interval", 1*time.Minute, "tablet refresh interval")
	topoReadConcurrency = flag.Int("topo_read_concurrency", 32, "concurrent topo reads")
)

const (
	gatewayImplementationDiscovery = "discoverygateway"
)

func init() {
	flag.Var(&tabletFilters, "tablet_filters", "Specifies a comma-separated list of 'keyspace|shard_name or keyrange' values to filter the tablets to watch")
	RegisterCreator(gatewayImplementationDiscovery, createDiscoveryGateway)
}

type discoveryGateway struct {
	hc            discovery.HealthCheck
	tsc           *discovery.TabletStatsCache
	topoServer    topo.Server
	srvTopoServer topo.SrvTopoServer
	localCell     string
	retryCount    int

	// tabletsWatchers contains a list of all the watchers we use.
	// We create one per cell.
	tabletsWatchers []*discovery.TopologyWatcher

	// mu protects all fields below.
	mu sync.RWMutex
	// statusAggregators is a map indexed by the key
	// keyspace/shard/tablet_type.
	statusAggregators map[string]*TabletStatusAggregator
}

func createDiscoveryGateway(hc discovery.HealthCheck, topoServer topo.Server, serv topo.SrvTopoServer, cell string, retryCount int) Gateway {
	dg := &discoveryGateway{
		hc:                hc,
		tsc:               discovery.NewTabletStatsCache(hc, cell),
		topoServer:        topoServer,
		srvTopoServer:     serv,
		localCell:         cell,
		retryCount:        retryCount,
		tabletsWatchers:   make([]*discovery.TopologyWatcher, 0, 1),
		statusAggregators: make(map[string]*TabletStatusAggregator),
	}
	log.Infof("loading tablets for cells: %v", *cellsToWatch)
	for _, c := range strings.Split(*cellsToWatch, ",") {
		if c == "" {
			continue
		}
		var tr discovery.TabletRecorder = dg.hc
		if len(tabletFilters) > 0 {
			fbs, err := discovery.NewFilterByShard(dg.hc, tabletFilters)
			if err != nil {
				log.Fatalf("Cannot parse tablet_filters parameter: %v", err)
			}
			tr = fbs
		}

		ctw := discovery.NewCellTabletsWatcher(dg.topoServer, tr, c, *refreshInterval, *topoReadConcurrency)
		dg.tabletsWatchers = append(dg.tabletsWatchers, ctw)
	}
	return dg
}

// WaitForTablets is part of the gateway.Gateway interface.
func (dg *discoveryGateway) WaitForTablets(ctx context.Context, tabletTypesToWait []topodatapb.TabletType) error {
	// Skip waiting for tablets if we are not told to do so.
	if len(tabletTypesToWait) == 0 {
		return nil
	}

	return dg.tsc.WaitForAllServingTablets(ctx, dg.srvTopoServer, dg.localCell, tabletTypesToWait)
}

// Execute executes the non-streaming query for the specified keyspace, shard, and tablet type.
func (dg *discoveryGateway) Execute(ctx context.Context, keyspace, shard string, tabletType topodatapb.TabletType, query string, bindVars map[string]interface{}, transactionID int64) (qr *sqltypes.Result, err error) {
	err = dg.withRetry(ctx, keyspace, shard, tabletType, func(conn tabletconn.TabletConn, target *querypb.Target) error {
		var innerErr error
		startTime := time.Now()
		qr, innerErr = conn.Execute(ctx, target, query, bindVars, transactionID)
		dg.updateStats(keyspace, shard, tabletType, startTime, innerErr)
		return innerErr
	}, transactionID, false)
	return qr, err
}

// ExecuteBatch executes a group of queries for the specified keyspace, shard, and tablet type.
func (dg *discoveryGateway) ExecuteBatch(ctx context.Context, keyspace, shard string, tabletType topodatapb.TabletType, queries []querytypes.BoundQuery, asTransaction bool, transactionID int64) (qrs []sqltypes.Result, err error) {
	err = dg.withRetry(ctx, keyspace, shard, tabletType, func(conn tabletconn.TabletConn, target *querypb.Target) error {
		var innerErr error
		startTime := time.Now()
		qrs, innerErr = conn.ExecuteBatch(ctx, target, queries, asTransaction, transactionID)
		dg.updateStats(keyspace, shard, tabletType, startTime, innerErr)
		return innerErr
	}, transactionID, false)
	return qrs, err
}

// StreamExecute executes a streaming query for the specified keyspace, shard, and tablet type.
func (dg *discoveryGateway) StreamExecute(ctx context.Context, keyspace, shard string, tabletType topodatapb.TabletType, query string, bindVars map[string]interface{}) (sqltypes.ResultStream, error) {
	var stream sqltypes.ResultStream
	err := dg.withRetry(ctx, keyspace, shard, tabletType, func(conn tabletconn.TabletConn, target *querypb.Target) error {
		var err error
		stream, err = conn.StreamExecute(ctx, target, query, bindVars)
		return err
	}, 0, true)
	if err != nil {
		return nil, err
	}
	return stream, nil
}

// Begin starts a transaction for the specified keyspace, shard, and tablet type.
// It returns the transaction ID.
func (dg *discoveryGateway) Begin(ctx context.Context, keyspace string, shard string, tabletType topodatapb.TabletType) (transactionID int64, err error) {
	err = dg.withRetry(ctx, keyspace, shard, tabletType, func(conn tabletconn.TabletConn, target *querypb.Target) error {
		var innerErr error
		startTime := time.Now()
		transactionID, innerErr = conn.Begin(ctx, target)
		dg.updateStats(keyspace, shard, tabletType, startTime, innerErr)
		return innerErr
	}, 0, false)
	return transactionID, err
}

// Commit commits the current transaction for the specified keyspace, shard, and tablet type.
func (dg *discoveryGateway) Commit(ctx context.Context, keyspace, shard string, tabletType topodatapb.TabletType, transactionID int64) error {
	return dg.withRetry(ctx, keyspace, shard, tabletType, func(conn tabletconn.TabletConn, target *querypb.Target) error {
		startTime := time.Now()
		innerErr := conn.Commit(ctx, target, transactionID)
		dg.updateStats(keyspace, shard, tabletType, startTime, innerErr)
		return innerErr
	}, transactionID, false)
}

// Rollback rolls back the current transaction for the specified keyspace, shard, and tablet type.
func (dg *discoveryGateway) Rollback(ctx context.Context, keyspace, shard string, tabletType topodatapb.TabletType, transactionID int64) error {
	return dg.withRetry(ctx, keyspace, shard, tabletType, func(conn tabletconn.TabletConn, target *querypb.Target) error {
		startTime := time.Now()
		innerErr := conn.Rollback(ctx, target, transactionID)
		dg.updateStats(keyspace, shard, tabletType, startTime, innerErr)
		return innerErr
	}, transactionID, false)
}

// BeginExecute executes a begin and the non-streaming query for the
// specified keyspace, shard, and tablet type.
func (dg *discoveryGateway) BeginExecute(ctx context.Context, keyspace, shard string, tabletType topodatapb.TabletType, query string, bindVars map[string]interface{}) (qr *sqltypes.Result, transactionID int64, err error) {
	err = dg.withRetry(ctx, keyspace, shard, tabletType, func(conn tabletconn.TabletConn, target *querypb.Target) error {
		var innerErr error
		startTime := time.Now()
		qr, transactionID, innerErr = conn.BeginExecute(ctx, target, query, bindVars)
		dg.updateStats(keyspace, shard, tabletType, startTime, innerErr)
		return innerErr
	}, 0, false)
	return qr, transactionID, err
}

// BeginExecuteBatch executes a begin and a group of queries for the
// specified keyspace, shard, and tablet type.
func (dg *discoveryGateway) BeginExecuteBatch(ctx context.Context, keyspace, shard string, tabletType topodatapb.TabletType, queries []querytypes.BoundQuery, asTransaction bool) (qrs []sqltypes.Result, transactionID int64, err error) {
	err = dg.withRetry(ctx, keyspace, shard, tabletType, func(conn tabletconn.TabletConn, target *querypb.Target) error {
		var innerErr error
		startTime := time.Now()
		qrs, transactionID, innerErr = conn.BeginExecuteBatch(ctx, target, queries, asTransaction)
		dg.updateStats(keyspace, shard, tabletType, startTime, innerErr)
		return innerErr
	}, 0, false)
	return qrs, transactionID, err
}

// SplitQuery splits a query into sub-queries for the specified keyspace, shard, and tablet type.
func (dg *discoveryGateway) SplitQuery(ctx context.Context, keyspace, shard string, tabletType topodatapb.TabletType, sql string, bindVariables map[string]interface{}, splitColumn string, splitCount int64) (queries []querytypes.QuerySplit, err error) {
	err = dg.withRetry(ctx, keyspace, shard, tabletType, func(conn tabletconn.TabletConn, target *querypb.Target) error {
		var innerErr error
		startTime := time.Now()
		queries, innerErr = conn.SplitQuery(ctx, target, querytypes.BoundQuery{
			Sql:           sql,
			BindVariables: bindVariables,
		}, splitColumn, splitCount)
		dg.updateStats(keyspace, shard, tabletType, startTime, innerErr)
		return innerErr
	}, 0, false)
	return
}

// SplitQuery splits a query into sub-queries for the specified keyspace, shard, and tablet type.
// TODO(erez): Rename to SplitQuery after migration to SplitQuery V2.
func (dg *discoveryGateway) SplitQueryV2(
	ctx context.Context,
	keyspace,
	shard string,
	tabletType topodatapb.TabletType,
	sql string,
	bindVariables map[string]interface{},
	splitColumns []string,
	splitCount int64,
	numRowsPerQueryPart int64,
	algorithm querypb.SplitQueryRequest_Algorithm) (queries []querytypes.QuerySplit, err error) {

	err = dg.withRetry(ctx, keyspace, shard, tabletType, func(conn tabletconn.TabletConn, target *querypb.Target) error {
		var innerErr error
		startTime := time.Now()
		queries, innerErr = conn.SplitQueryV2(ctx, target, querytypes.BoundQuery{
			Sql:           sql,
			BindVariables: bindVariables,
		}, splitColumns, splitCount, numRowsPerQueryPart, algorithm)
		dg.updateStats(keyspace, shard, tabletType, startTime, innerErr)
		return innerErr
	}, 0, false)
	return
}

// Close shuts down underlying connections.
func (dg *discoveryGateway) Close(ctx context.Context) error {
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
func (dg *discoveryGateway) withRetry(ctx context.Context, keyspace, shard string, tabletType topodatapb.TabletType, action func(conn tabletconn.TabletConn, target *querypb.Target) error, transactionID int64, isStreaming bool) error {
	var tabletLastUsed *topodatapb.Tablet
	var err error
	inTransaction := (transactionID != 0)
	invalidTablets := make(map[string]bool)

	for i := 0; i < dg.retryCount+1; i++ {
		tablets := dg.tsc.GetHealthyTabletStats(keyspace, shard, tabletType)
		if len(tablets) == 0 {
			// fail fast if there is no tablet
			err = vterrors.FromError(vtrpcpb.ErrorCode_INTERNAL_ERROR, fmt.Errorf("no valid tablet"))
			break
		}
		shuffleTablets(tablets)

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
				err = vterrors.FromError(vtrpcpb.ErrorCode_INTERNAL_ERROR, fmt.Errorf("no available connection"))
			}
			break
		}

		// execute
		tabletLastUsed = ts.Tablet
		conn := dg.hc.GetConnection(ts.Key)
		if conn == nil {
			err = vterrors.FromError(vtrpcpb.ErrorCode_INTERNAL_ERROR, fmt.Errorf("no connection for key %v tablet %+v", ts.Key, ts.Tablet))
			invalidTablets[ts.Key] = true
			continue
		}

		// Potentially buffer this request.
		if bufferErr := masterbuffer.FakeBuffer(keyspace, shard, tabletType, inTransaction, i); bufferErr != nil {
			return bufferErr
		}

		err = action(conn, ts.Target)
		if dg.canRetry(ctx, err, transactionID, isStreaming) {
			invalidTablets[ts.Key] = true
			continue
		}
		break
	}
	return NewShardError(err, keyspace, shard, tabletType, tabletLastUsed, inTransaction)
}

// canRetry determines whether a query can be retried or not.
// OperationalErrors like retry/fatal are retryable if query is not in a txn.
// All other errors are non-retryable.
func (dg *discoveryGateway) canRetry(ctx context.Context, err error, transactionID int64, isStreaming bool) bool {
	if err == nil {
		return false
	}
	// Do not retry if ctx.Done() is closed.
	select {
	case <-ctx.Done():
		return false
	default:
	}
	if serverError, ok := err.(*tabletconn.ServerError); ok {
		switch serverError.ServerCode {
		case vtrpcpb.ErrorCode_INTERNAL_ERROR:
			// Do not retry on fatal error for streaming query.
			// For streaming query, vttablet sends:
			// - QUERY_NOT_SERVED, if streaming is not started yet;
			// - INTERNAL_ERROR, if streaming is broken halfway.
			// For non-streaming query, handle as QUERY_NOT_SERVED.
			if isStreaming {
				return false
			}
			fallthrough
		case vtrpcpb.ErrorCode_QUERY_NOT_SERVED:
			// Retry on QUERY_NOT_SERVED and
			// INTERNAL_ERROR if not in a transaction.
			inTransaction := (transactionID != 0)
			return !inTransaction
		default:
			// Not retry for RESOURCE_EXHAUSTED and normal
			// server errors.
			return false
		}
	}
	// Do not retry on operational error.
	return false
}

func shuffleTablets(tablets []discovery.TabletStats) {
	index := 0
	length := len(tablets)
	for i := length - 1; i > 0; i-- {
		index = rand.Intn(i + 1)
		tablets[i], tablets[index] = tablets[index], tablets[i]
	}
}

func (dg *discoveryGateway) updateStats(keyspace, shard string, tabletType topodatapb.TabletType, startTime time.Time, err error) {
	elapsed := time.Now().Sub(startTime)
	aggr := dg.getStatsAggregator(keyspace, shard, tabletType)
	aggr.UpdateQueryInfo("", tabletType, elapsed, err != nil)
}

func (dg *discoveryGateway) getStatsAggregator(keyspace, shard string, tabletType topodatapb.TabletType) *TabletStatusAggregator {
	key := fmt.Sprintf("%v/%v/%v", keyspace, shard, tabletType.String())

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
	aggr = NewTabletStatusAggregator(keyspace, shard, tabletType, key)
	dg.statusAggregators[key] = aggr
	return aggr
}
