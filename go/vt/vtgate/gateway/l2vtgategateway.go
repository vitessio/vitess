// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gateway

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/discovery"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/tabletserver/querytypes"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/topoproto"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

const (
	gatewayImplementationL2VTGate = "l2vtgategateway"
)

func init() {
	RegisterCreator(gatewayImplementationL2VTGate, createL2VTGateGateway)
}

// l2VTGateConn is a connection to a backend l2vtgate pool
type l2VTGateConn struct {
	// set at construction time
	addr     string
	keyspace string
	shard    string
	keyRange *topodatapb.KeyRange // only set if shard is also a KeyRange

	conn tabletconn.TabletConn
}

type l2VTGateGateway struct {
	retryCount int

	// mu protects all fields below.
	mu sync.RWMutex

	// connMap is the main map to find the right l2 vtgate pool.
	// It is indexed by keyspace name.
	connMap map[string][]*l2VTGateConn

	// statusAggregators is a map indexed by the key
	// l2vtgate address + tablet type
	statusAggregators map[string]*TabletStatusAggregator
}

func createL2VTGateGateway(hc discovery.HealthCheck, topoServer topo.Server, serv topo.SrvTopoServer, cell string, retryCount int, tabletTypesToWait []topodatapb.TabletType) Gateway {
	lg := &l2VTGateGateway{
		retryCount:        retryCount,
		connMap:           make(map[string][]*l2VTGateConn),
		statusAggregators: make(map[string]*TabletStatusAggregator),
	}
	return lg
}

func (lg *l2VTGateGateway) addL2VTGateConn(addr, keyspace, shard string) error {
	lg.mu.Lock()
	defer lg.mu.Unlock()

	// extract keyrange if it's a range
	canonical, kr, err := topo.ValidateShardName(shard)
	if err != nil {
		return fmt.Errorf("error parsing shard name %v: %v", shard, err)
	}

	// check for duplicates
	for _, c := range lg.connMap[keyspace] {
		if c.shard == canonical {
			return fmt.Errorf("duplicate %v/%v entry", keyspace, shard)
		}
	}

	// FIXME(alainjobart):
	// - use a per-l2VTGateConn context that can be closed
	// - do the hostname / port right
	// - connection timeout should be a flag
	// - we should pass a blocking / non-blocking flag (regular vtgate to vttablet is blocking, this one should be non-blocking as it's talking to a VIP)
	ctx := context.Background()
	conn, err := tabletconn.GetDialer()(ctx, &topodatapb.Tablet{
		Hostname: addr,
	}, 30*time.Second)
	if err != nil {
		return err
	}

	lg.connMap[keyspace] = append(lg.connMap[keyspace], &l2VTGateConn{
		addr:     addr,
		keyspace: keyspace,
		shard:    canonical,
		keyRange: kr,
		conn:     conn,
	})
	return nil
}

// Execute executes the non-streaming query for the specified keyspace, shard, and tablet type.
func (lg *l2VTGateGateway) Execute(ctx context.Context, keyspace, shard string, tabletType topodatapb.TabletType, query string, bindVars map[string]interface{}, transactionID int64) (qr *sqltypes.Result, err error) {
	err = lg.withRetry(ctx, keyspace, shard, tabletType, func(conn *l2VTGateConn, target *querypb.Target) error {
		var innerErr error
		startTime := time.Now()
		qr, innerErr = conn.conn.Execute(ctx, target, query, bindVars, transactionID)
		lg.updateStats(conn, tabletType, startTime, innerErr)
		return innerErr
	}, transactionID, false)
	return qr, err
}

// ExecuteBatch executes a group of queries for the specified keyspace, shard, and tablet type.
func (lg *l2VTGateGateway) ExecuteBatch(ctx context.Context, keyspace, shard string, tabletType topodatapb.TabletType, queries []querytypes.BoundQuery, asTransaction bool, transactionID int64) (qrs []sqltypes.Result, err error) {
	err = lg.withRetry(ctx, keyspace, shard, tabletType, func(conn *l2VTGateConn, target *querypb.Target) error {
		var innerErr error
		startTime := time.Now()
		qrs, innerErr = conn.conn.ExecuteBatch(ctx, target, queries, asTransaction, transactionID)
		lg.updateStats(conn, tabletType, startTime, innerErr)
		return innerErr
	}, transactionID, false)
	return qrs, err
}

// StreamExecute executes a streaming query for the specified keyspace, shard, and tablet type.
func (lg *l2VTGateGateway) StreamExecute(ctx context.Context, keyspace, shard string, tabletType topodatapb.TabletType, query string, bindVars map[string]interface{}) (sqltypes.ResultStream, error) {
	var usedConn *l2VTGateConn
	var stream sqltypes.ResultStream
	err := lg.withRetry(ctx, keyspace, shard, tabletType, func(conn *l2VTGateConn, target *querypb.Target) error {
		var err error
		stream, err = conn.conn.StreamExecute(ctx, target, query, bindVars)
		usedConn = conn
		return err
	}, 0, true)
	if err != nil {
		return nil, err
	}
	return stream, nil
}

// Begin starts a transaction for the specified keyspace, shard, and tablet type.
// It returns the transaction ID.
func (lg *l2VTGateGateway) Begin(ctx context.Context, keyspace string, shard string, tabletType topodatapb.TabletType) (transactionID int64, err error) {
	err = lg.withRetry(ctx, keyspace, shard, tabletType, func(conn *l2VTGateConn, target *querypb.Target) error {
		var innerErr error
		startTime := time.Now()
		transactionID, innerErr = conn.conn.Begin(ctx, target)
		lg.updateStats(conn, tabletType, startTime, innerErr)
		return innerErr
	}, 0, false)
	return transactionID, err
}

// Commit commits the current transaction for the specified keyspace, shard, and tablet type.
func (lg *l2VTGateGateway) Commit(ctx context.Context, keyspace, shard string, tabletType topodatapb.TabletType, transactionID int64) error {
	return lg.withRetry(ctx, keyspace, shard, tabletType, func(conn *l2VTGateConn, target *querypb.Target) error {
		startTime := time.Now()
		innerErr := conn.conn.Commit(ctx, target, transactionID)
		lg.updateStats(conn, tabletType, startTime, innerErr)
		return innerErr
	}, transactionID, false)
}

// Rollback rolls back the current transaction for the specified keyspace, shard, and tablet type.
func (lg *l2VTGateGateway) Rollback(ctx context.Context, keyspace, shard string, tabletType topodatapb.TabletType, transactionID int64) error {
	return lg.withRetry(ctx, keyspace, shard, tabletType, func(conn *l2VTGateConn, target *querypb.Target) error {
		startTime := time.Now()
		innerErr := conn.conn.Rollback(ctx, target, transactionID)
		lg.updateStats(conn, tabletType, startTime, innerErr)
		return innerErr
	}, transactionID, false)
}

// BeginExecute executes a begin and the non-streaming query for the
// specified keyspace, shard, and tablet type.
func (lg *l2VTGateGateway) BeginExecute(ctx context.Context, keyspace, shard string, tabletType topodatapb.TabletType, query string, bindVars map[string]interface{}) (qr *sqltypes.Result, transactionID int64, err error) {
	err = lg.withRetry(ctx, keyspace, shard, tabletType, func(conn *l2VTGateConn, target *querypb.Target) error {
		var innerErr error
		startTime := time.Now()
		qr, transactionID, innerErr = conn.conn.BeginExecute(ctx, target, query, bindVars)
		lg.updateStats(conn, tabletType, startTime, innerErr)
		return innerErr
	}, 0, false)
	return qr, transactionID, err
}

// BeginExecuteBatch executes a begin and a group of queries for the
// specified keyspace, shard, and tablet type.
func (lg *l2VTGateGateway) BeginExecuteBatch(ctx context.Context, keyspace, shard string, tabletType topodatapb.TabletType, queries []querytypes.BoundQuery, asTransaction bool) (qrs []sqltypes.Result, transactionID int64, err error) {
	err = lg.withRetry(ctx, keyspace, shard, tabletType, func(conn *l2VTGateConn, target *querypb.Target) error {
		var innerErr error
		startTime := time.Now()
		qrs, transactionID, innerErr = conn.conn.BeginExecuteBatch(ctx, target, queries, asTransaction)
		lg.updateStats(conn, tabletType, startTime, innerErr)
		return innerErr
	}, 0, false)
	return qrs, transactionID, err
}

// SplitQuery splits a query into sub-queries for the specified keyspace, shard, and tablet type.
func (lg *l2VTGateGateway) SplitQuery(ctx context.Context, keyspace, shard string, tabletType topodatapb.TabletType, sql string, bindVariables map[string]interface{}, splitColumn string, splitCount int64) (queries []querytypes.QuerySplit, err error) {
	err = lg.withRetry(ctx, keyspace, shard, tabletType, func(conn *l2VTGateConn, target *querypb.Target) error {
		var innerErr error
		startTime := time.Now()
		queries, innerErr = conn.conn.SplitQuery(ctx, target, querytypes.BoundQuery{
			Sql:           sql,
			BindVariables: bindVariables,
		}, splitColumn, splitCount)
		lg.updateStats(conn, tabletType, startTime, innerErr)
		return innerErr
	}, 0, false)
	return
}

// SplitQuery splits a query into sub-queries for the specified keyspace, shard, and tablet type.
// TODO(erez): Rename to SplitQuery after migration to SplitQuery V2.
func (lg *l2VTGateGateway) SplitQueryV2(
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

	err = lg.withRetry(ctx, keyspace, shard, tabletType, func(conn *l2VTGateConn, target *querypb.Target) error {
		var innerErr error
		startTime := time.Now()
		queries, innerErr = conn.conn.SplitQueryV2(ctx, target, querytypes.BoundQuery{
			Sql:           sql,
			BindVariables: bindVariables,
		}, splitColumns, splitCount, numRowsPerQueryPart, algorithm)
		lg.updateStats(conn, tabletType, startTime, innerErr)
		return innerErr
	}, 0, false)
	return
}

// Close shuts down underlying connections.
func (lg *l2VTGateGateway) Close(ctx context.Context) error {
	lg.mu.Lock()
	defer lg.mu.Unlock()

	// FIXME(alainjobart) not sure about the ongoing queries
	for _, a := range lg.connMap {
		for _, c := range a {
			c.conn.Close()
		}
	}
	return nil
}

// CacheStatus returns a list of TabletCacheStatus per
// keyspace/shard/tablet_type.
func (lg *l2VTGateGateway) CacheStatus() TabletCacheStatusList {
	lg.mu.RLock()
	res := make(TabletCacheStatusList, 0, len(lg.statusAggregators))
	for _, aggr := range lg.statusAggregators {
		res = append(res, aggr.GetCacheStatus())
	}
	lg.mu.RUnlock()
	sort.Sort(res)
	return res
}

// getConn returns the right l2VTGateConn for a given keyspace / shard.
func (lg *l2VTGateGateway) getConn(keyspace, shard string) (*l2VTGateConn, error) {
	lg.mu.RLock()
	defer lg.mu.RUnlock()

	canonical, kr, err := topo.ValidateShardName(shard)
	if err != nil {
		return nil, fmt.Errorf("invalid shard name: %v", shard)
	}

	for _, c := range lg.connMap[keyspace] {
		if canonical == c.shard {
			// Exact match (probably a non-sharded keyspace).
			return c, nil
		}
		if key.KeyRangesIntersect(kr, c.keyRange) {
			// There is overlap, we can just send to the destination.
			// FIXME(alainjobart) if canonical is not entirely covered by l2vtgate,
			// this is probably an error. We probably want key.KeyRangeIncludes(), NYI.
			return c, nil
		}
	}

	return nil, fmt.Errorf("no configured destination for %v/%v", keyspace, shard)
}

// withRetry gets available connections and executes the action. If there are retryable errors,
// it retries retryCount times before failing. It does not retry if the connection is in
// the middle of a transaction. While returning the error check if it maybe a result of
// a resharding event, and set the re-resolve bit and let the upper layers
// re-resolve and retry.
func (lg *l2VTGateGateway) withRetry(ctx context.Context, keyspace, shard string, tabletType topodatapb.TabletType, action func(conn *l2VTGateConn, target *querypb.Target) error, transactionID int64, isStreaming bool) error {
	inTransaction := (transactionID != 0)

	conn, err := lg.getConn(keyspace, shard)
	if err != nil {
		return fmt.Errorf("no configured destination for %v/%v: %v", keyspace, shard, err)
	}
	target := &querypb.Target{
		Keyspace:   keyspace,
		Shard:      shard,
		TabletType: tabletType,
	}

	for i := 0; i < lg.retryCount+1; i++ {
		err = action(conn, target)
		if lg.canRetry(ctx, err, transactionID, isStreaming) {
			continue
		}
		break
	}
	return NewShardError(err, keyspace, shard, tabletType, nil, inTransaction)
}

// canRetry determines whether a query can be retried or not.
// OperationalErrors like retry/fatal are retryable if query is not in a txn.
// All other errors are non-retryable.
func (lg *l2VTGateGateway) canRetry(ctx context.Context, err error, transactionID int64, isStreaming bool) bool {
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

func (lg *l2VTGateGateway) updateStats(conn *l2VTGateConn, tabletType topodatapb.TabletType, startTime time.Time, err error) {
	elapsed := time.Now().Sub(startTime)
	aggr := lg.getStatsAggregator(conn, tabletType)
	aggr.UpdateQueryInfo("", tabletType, elapsed, err != nil)
}

func (lg *l2VTGateGateway) getStatsAggregator(conn *l2VTGateConn, tabletType topodatapb.TabletType) *TabletStatusAggregator {
	key := fmt.Sprintf("%v:%v", conn.addr, topoproto.TabletTypeLString(tabletType))

	// get existing aggregator
	lg.mu.RLock()
	aggr, ok := lg.statusAggregators[key]
	lg.mu.RUnlock()
	if ok {
		return aggr
	}
	// create a new one, but check again before the creation
	lg.mu.Lock()
	defer lg.mu.Unlock()
	aggr, ok = lg.statusAggregators[key]
	if ok {
		return aggr
	}
	aggr = NewTabletStatusAggregator(conn.keyspace, conn.shard, tabletType, key)
	lg.statusAggregators[key] = aggr
	return aggr
}
