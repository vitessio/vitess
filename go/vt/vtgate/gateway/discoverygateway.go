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
	"github.com/youtube/vitess/go/vt/vtgate/buffer"
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

	// mu protects the fields of this group.
	mu sync.RWMutex
	// statusAggregators is a map indexed by the key
	// keyspace/shard/tablet_type.
	statusAggregators map[string]*TabletStatusAggregator

	// buffer, if enabled, buffers requests during a detected MASTER failover.
	buffer *buffer.Buffer
}

func createDiscoveryGateway(hc discovery.HealthCheck, topoServer topo.Server, serv topo.SrvTopoServer, cell string, retryCount int) Gateway {
	dg := &discoveryGateway{
		hc:                hc,
		tsc:               discovery.NewTabletStatsCacheDoNotSetListener(cell),
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
				log.Fatalf("Cannot parse tablet_filters parameter: %v", err)
			}
			tr = fbs
		}

		ctw := discovery.NewCellTabletsWatcher(dg.topoServer, tr, c, *refreshInterval, *topoReadConcurrency)
		dg.tabletsWatchers = append(dg.tabletsWatchers, ctw)
	}
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

// Execute executes the non-streaming query for the specified keyspace, shard, and tablet type.
func (dg *discoveryGateway) Execute(ctx context.Context, target *querypb.Target, query string, bindVars map[string]interface{}, transactionID int64, options *querypb.ExecuteOptions) (qr *sqltypes.Result, err error) {
	err = dg.withRetry(ctx, target, func(conn tabletconn.TabletConn, target *querypb.Target) error {
		var innerErr error
		startTime := time.Now()
		qr, innerErr = conn.Execute(ctx, target, query, bindVars, transactionID, options)
		dg.updateStats(target, startTime, innerErr)
		return innerErr
	}, transactionID != 0, false)
	return qr, err
}

// ExecuteBatch executes a group of queries for the specified keyspace, shard, and tablet type.
func (dg *discoveryGateway) ExecuteBatch(ctx context.Context, target *querypb.Target, queries []querytypes.BoundQuery, asTransaction bool, transactionID int64, options *querypb.ExecuteOptions) (qrs []sqltypes.Result, err error) {
	err = dg.withRetry(ctx, target, func(conn tabletconn.TabletConn, target *querypb.Target) error {
		var innerErr error
		startTime := time.Now()
		qrs, innerErr = conn.ExecuteBatch(ctx, target, queries, asTransaction, transactionID, options)
		dg.updateStats(target, startTime, innerErr)
		return innerErr
	}, transactionID != 0, false)
	return qrs, err
}

// StreamExecute executes a streaming query for the specified keyspace, shard, and tablet type.
func (dg *discoveryGateway) StreamExecute(ctx context.Context, target *querypb.Target, query string, bindVars map[string]interface{}, options *querypb.ExecuteOptions) (sqltypes.ResultStream, error) {
	var stream sqltypes.ResultStream
	err := dg.withRetry(ctx, target, func(conn tabletconn.TabletConn, target *querypb.Target) error {
		var err error
		stream, err = conn.StreamExecute(ctx, target, query, bindVars, options)
		return err
	}, false, true)
	if err != nil {
		return nil, err
	}
	return stream, nil
}

// Begin starts a transaction for the specified keyspace, shard, and tablet type.
// It returns the transaction ID.
func (dg *discoveryGateway) Begin(ctx context.Context, target *querypb.Target) (transactionID int64, err error) {
	err = dg.withRetry(ctx, target, func(conn tabletconn.TabletConn, target *querypb.Target) error {
		var innerErr error
		startTime := time.Now()
		transactionID, innerErr = conn.Begin(ctx, target)
		dg.updateStats(target, startTime, innerErr)
		return innerErr
	}, false, false)
	return transactionID, err
}

// Commit commits the current transaction for the specified keyspace, shard, and tablet type.
func (dg *discoveryGateway) Commit(ctx context.Context, target *querypb.Target, transactionID int64) error {
	return dg.withRetry(ctx, target, func(conn tabletconn.TabletConn, target *querypb.Target) error {
		startTime := time.Now()
		innerErr := conn.Commit(ctx, target, transactionID)
		dg.updateStats(target, startTime, innerErr)
		return innerErr
	}, true, false)
}

// Rollback rolls back the current transaction for the specified keyspace, shard, and tablet type.
func (dg *discoveryGateway) Rollback(ctx context.Context, target *querypb.Target, transactionID int64) error {
	return dg.withRetry(ctx, target, func(conn tabletconn.TabletConn, target *querypb.Target) error {
		startTime := time.Now()
		innerErr := conn.Rollback(ctx, target, transactionID)
		dg.updateStats(target, startTime, innerErr)
		return innerErr
	}, true, false)
}

// Prepare rolls back the current transaction for the specified keyspace, shard, and tablet type.
func (dg *discoveryGateway) Prepare(ctx context.Context, target *querypb.Target, transactionID int64, dtid string) error {
	return dg.withRetry(ctx, target, func(conn tabletconn.TabletConn, target *querypb.Target) error {
		startTime := time.Now()
		innerErr := conn.Prepare(ctx, target, transactionID, dtid)
		dg.updateStats(target, startTime, innerErr)
		return innerErr
	}, true, false)
}

// CommitPrepared rolls back the current transaction for the specified keyspace, shard, and tablet type.
func (dg *discoveryGateway) CommitPrepared(ctx context.Context, target *querypb.Target, dtid string) (err error) {
	return dg.withRetry(ctx, target, func(conn tabletconn.TabletConn, target *querypb.Target) error {
		startTime := time.Now()
		innerErr := conn.CommitPrepared(ctx, target, dtid)
		dg.updateStats(target, startTime, innerErr)
		return innerErr
	}, true, false)
}

// RollbackPrepared rolls back the current transaction for the specified keyspace, shard, and tablet type.
func (dg *discoveryGateway) RollbackPrepared(ctx context.Context, target *querypb.Target, dtid string, originalID int64) (err error) {
	return dg.withRetry(ctx, target, func(conn tabletconn.TabletConn, target *querypb.Target) error {
		startTime := time.Now()
		innerErr := conn.RollbackPrepared(ctx, target, dtid, originalID)
		dg.updateStats(target, startTime, innerErr)
		return innerErr
	}, true, false)
}

// CreateTransaction rolls back the current transaction for the specified keyspace, shard, and tablet type.
func (dg *discoveryGateway) CreateTransaction(ctx context.Context, target *querypb.Target, dtid string, participants []*querypb.Target) (err error) {
	return dg.withRetry(ctx, target, func(conn tabletconn.TabletConn, target *querypb.Target) error {
		startTime := time.Now()
		innerErr := conn.CreateTransaction(ctx, target, dtid, participants)
		dg.updateStats(target, startTime, innerErr)
		return innerErr
	}, true, false)
}

// StartCommit rolls back the current transaction for the specified keyspace, shard, and tablet type.
func (dg *discoveryGateway) StartCommit(ctx context.Context, target *querypb.Target, transactionID int64, dtid string) (err error) {
	return dg.withRetry(ctx, target, func(conn tabletconn.TabletConn, target *querypb.Target) error {
		startTime := time.Now()
		innerErr := conn.StartCommit(ctx, target, transactionID, dtid)
		dg.updateStats(target, startTime, innerErr)
		return innerErr
	}, true, false)
}

// SetRollback rolls back the current transaction for the specified keyspace, shard, and tablet type.
func (dg *discoveryGateway) SetRollback(ctx context.Context, target *querypb.Target, dtid string, transactionID int64) (err error) {
	return dg.withRetry(ctx, target, func(conn tabletconn.TabletConn, target *querypb.Target) error {
		startTime := time.Now()
		innerErr := conn.SetRollback(ctx, target, dtid, transactionID)
		dg.updateStats(target, startTime, innerErr)
		return innerErr
	}, true, false)
}

// ConcludeTransaction rolls back the current transaction for the specified keyspace, shard, and tablet type.
func (dg *discoveryGateway) ConcludeTransaction(ctx context.Context, target *querypb.Target, dtid string) (err error) {
	return dg.withRetry(ctx, target, func(conn tabletconn.TabletConn, target *querypb.Target) error {
		startTime := time.Now()
		innerErr := conn.ConcludeTransaction(ctx, target, dtid)
		dg.updateStats(target, startTime, innerErr)
		return innerErr
	}, true, false)
}

// ReadTransaction rolls back the current transaction for the specified keyspace, shard, and tablet type.
func (dg *discoveryGateway) ReadTransaction(ctx context.Context, target *querypb.Target, dtid string) (metadata *querypb.TransactionMetadata, err error) {
	err = dg.withRetry(ctx, target, func(conn tabletconn.TabletConn, target *querypb.Target) error {
		startTime := time.Now()
		var innerErr error
		metadata, innerErr = conn.ReadTransaction(ctx, target, dtid)
		dg.updateStats(target, startTime, innerErr)
		return innerErr
	}, false, false)
	return metadata, err
}

// BeginExecute executes a begin and the non-streaming query for the
// specified keyspace, shard, and tablet type.
func (dg *discoveryGateway) BeginExecute(ctx context.Context, target *querypb.Target, query string, bindVars map[string]interface{}, options *querypb.ExecuteOptions) (qr *sqltypes.Result, transactionID int64, err error) {
	err = dg.withRetry(ctx, target, func(conn tabletconn.TabletConn, target *querypb.Target) error {
		var innerErr error
		startTime := time.Now()
		qr, transactionID, innerErr = conn.BeginExecute(ctx, target, query, bindVars, options)
		dg.updateStats(target, startTime, innerErr)
		return innerErr
	}, false, false)
	return qr, transactionID, err
}

// BeginExecuteBatch executes a begin and a group of queries for the
// specified keyspace, shard, and tablet type.
func (dg *discoveryGateway) BeginExecuteBatch(ctx context.Context, target *querypb.Target, queries []querytypes.BoundQuery, asTransaction bool, options *querypb.ExecuteOptions) (qrs []sqltypes.Result, transactionID int64, err error) {
	err = dg.withRetry(ctx, target, func(conn tabletconn.TabletConn, target *querypb.Target) error {
		var innerErr error
		startTime := time.Now()
		qrs, transactionID, innerErr = conn.BeginExecuteBatch(ctx, target, queries, asTransaction, options)
		dg.updateStats(target, startTime, innerErr)
		return innerErr
	}, false, false)
	return qrs, transactionID, err
}

// SplitQuery splits a query into sub-queries for the specified keyspace, shard, and tablet type.
func (dg *discoveryGateway) SplitQuery(
	ctx context.Context,
	target *querypb.Target,
	query querytypes.BoundQuery,
	splitColumns []string,
	splitCount int64,
	numRowsPerQueryPart int64,
	algorithm querypb.SplitQueryRequest_Algorithm) (queries []querytypes.QuerySplit, err error) {

	err = dg.withRetry(ctx, target, func(conn tabletconn.TabletConn, target *querypb.Target) error {
		var innerErr error
		startTime := time.Now()
		queries, innerErr = conn.SplitQuery(ctx, target, query, splitColumns, splitCount, numRowsPerQueryPart, algorithm)
		dg.updateStats(target, startTime, innerErr)
		return innerErr
	}, false, false)
	return
}

// UpdateStream starts an update stream for the specified keyspace,
// shard, and tablet type.
func (dg *discoveryGateway) UpdateStream(ctx context.Context, target *querypb.Target, position string, timestamp int64) (tabletconn.StreamEventReader, error) {
	var stream tabletconn.StreamEventReader
	err := dg.withRetry(ctx, target, func(conn tabletconn.TabletConn, target *querypb.Target) error {
		var err error
		stream, err = conn.UpdateStream(ctx, target, position, timestamp)
		return err
	}, false, true)
	if err != nil {
		return nil, err
	}
	return stream, nil
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

// StreamHealth is currently not implemented.
// TODO(alainjobart): Maybe we should?
func (dg *discoveryGateway) StreamHealth(ctx context.Context) (tabletconn.StreamHealthReader, error) {
	panic("not implemented")
}

// withRetry gets available connections and executes the action. If there are retryable errors,
// it retries retryCount times before failing. It does not retry if the connection is in
// the middle of a transaction. While returning the error check if it maybe a result of
// a resharding event, and set the re-resolve bit and let the upper layers
// re-resolve and retry.
func (dg *discoveryGateway) withRetry(ctx context.Context, target *querypb.Target, action func(conn tabletconn.TabletConn, target *querypb.Target) error, inTransaction, isStreaming bool) error {
	var tabletLastUsed *topodatapb.Tablet
	var err error
	invalidTablets := make(map[string]bool)

	for i := 0; i < dg.retryCount+1; i++ {
		// Check if we should buffer MASTER queries which failed due to an ongoing
		// failover.
		// Note: We only buffer "!inTransaction" queries i.e.
		// a) no transaction is necessary (e.g. critical reads) or
		// b) no transaction was created yet.
		if !inTransaction && target.TabletType == topodatapb.TabletType_MASTER {
			// The next call blocks if we should buffer during a failover.
			retryDone, bufferErr := dg.buffer.WaitForFailoverEnd(ctx, target.Keyspace, target.Shard, err)
			if bufferErr != nil {
				// Buffering failed e.g. buffer is already full. Do not retry.
				err = vterrors.WithSuffix(
					vterrors.WithPrefix(
						"failed to automatically buffer and retry failed request during failover: ",
						bufferErr),
					fmt.Sprintf(" original err (type=%T): %v", err, err))
				break
			}

			// Request may have been buffered.
			if retryDone != nil {
				// We're going to retry this request as part of a buffer drain.
				// Notify the buffer after we retried.
				defer retryDone()
			}
		}

		tablets := dg.tsc.GetHealthyTabletStats(target.Keyspace, target.Shard, target.TabletType)
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
		if bufferErr := masterbuffer.FakeBuffer(target, inTransaction, i); bufferErr != nil {
			return bufferErr
		}

		err = action(conn, ts.Target)
		if dg.canRetry(ctx, err, inTransaction, isStreaming) {
			invalidTablets[ts.Key] = true
			continue
		}
		break
	}
	return NewShardError(err, target, tabletLastUsed, inTransaction)
}

// canRetry determines whether a query can be retried or not.
// OperationalErrors like retry/fatal are retryable if query is not in a txn.
// All other errors are non-retryable.
func (dg *discoveryGateway) canRetry(ctx context.Context, err error, inTransaction, isStreaming bool) bool {
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
