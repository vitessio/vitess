// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"flag"
	"fmt"
	"math/rand"
	"strings"
	"time"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/vt/discovery"
	"github.com/youtube/vitess/go/vt/tabletserver/querytypes"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vterrors"
	"github.com/youtube/vitess/go/vt/vtgate/txbuffer"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

var (
	cellsToWatch        = flag.String("cells_to_watch", "", "comma-separated list of cells for watching endpoints")
	refreshInterval     = flag.Duration("endpoint_refresh_interval", 1*time.Minute, "endpoint refresh interval")
	topoReadConcurrency = flag.Int("topo_read_concurrency", 32, "concurrent topo reads")
)

const (
	gatewayImplementationDiscovery = "discoverygateway"
)

func init() {
	RegisterGatewayCreator(gatewayImplementationDiscovery, createDiscoveryGateway)
}

func createDiscoveryGateway(hc discovery.HealthCheck, topoServer topo.Server, serv topo.SrvTopoServer, cell string, _ time.Duration, retryCount int, _, _, _ time.Duration, _ *stats.MultiTimings, tabletTypesToWait []topodatapb.TabletType) Gateway {
	dg := &discoveryGateway{
		hc:                hc,
		topoServer:        topoServer,
		srvTopoServer:     serv,
		localCell:         cell,
		retryCount:        retryCount,
		tabletTypesToWait: tabletTypesToWait,
		tabletsWatchers:   make([]*discovery.TopologyWatcher, 0, 1),
	}
	dg.hc.SetListener(dg)
	for _, c := range strings.Split(*cellsToWatch, ",") {
		if c == "" {
			continue
		}
		ctw := discovery.NewCellTabletsWatcher(dg.topoServer, dg.hc, c, *refreshInterval, *topoReadConcurrency)
		dg.tabletsWatchers = append(dg.tabletsWatchers, ctw)
	}
	err := dg.waitForEndPoints()
	if err != nil {
		log.Errorf("createDiscoveryGateway: %v", err)
	}
	return dg
}

type discoveryGateway struct {
	hc                discovery.HealthCheck
	topoServer        topo.Server
	srvTopoServer     topo.SrvTopoServer
	localCell         string
	retryCount        int
	tabletTypesToWait []topodatapb.TabletType

	tabletsWatchers []*discovery.TopologyWatcher
}

func (dg *discoveryGateway) waitForEndPoints() error {
	// Skip waiting for endpoints if we are not told to do so.
	if len(dg.tabletTypesToWait) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err := discovery.WaitForAllEndPoints(ctx, dg.hc, dg.srvTopoServer, dg.localCell, dg.tabletTypesToWait)
	if err == discovery.ErrWaitForEndPointsTimeout {
		// ignore this error, we will still start up, and may not serve
		// all endpoints.
		err = nil
	}
	return err
}

// InitializeConnections creates connections to VTTablets.
func (dg *discoveryGateway) InitializeConnections(ctx context.Context) error {
	return nil
}

// Execute executes the non-streaming query for the specified keyspace, shard, and tablet type.
func (dg *discoveryGateway) Execute(ctx context.Context, keyspace, shard string, tabletType topodatapb.TabletType, query string, bindVars map[string]interface{}, transactionID int64) (qr *sqltypes.Result, err error) {
	err = dg.withRetry(ctx, keyspace, shard, tabletType, func(conn tabletconn.TabletConn) error {
		var innerErr error
		qr, innerErr = conn.Execute(ctx, query, bindVars, transactionID)
		return innerErr
	}, transactionID, false)
	return qr, err
}

// ExecuteBatch executes a group of queries for the specified keyspace, shard, and tablet type.
func (dg *discoveryGateway) ExecuteBatch(ctx context.Context, keyspace, shard string, tabletType topodatapb.TabletType, queries []querytypes.BoundQuery, asTransaction bool, transactionID int64) (qrs []sqltypes.Result, err error) {
	err = dg.withRetry(ctx, keyspace, shard, tabletType, func(conn tabletconn.TabletConn) error {
		var innerErr error
		qrs, innerErr = conn.ExecuteBatch(ctx, queries, asTransaction, transactionID)
		return innerErr
	}, transactionID, false)
	return qrs, err
}

// StreamExecute executes a streaming query for the specified keyspace, shard, and tablet type.
func (dg *discoveryGateway) StreamExecute(ctx context.Context, keyspace, shard string, tabletType topodatapb.TabletType, query string, bindVars map[string]interface{}, transactionID int64) (<-chan *sqltypes.Result, tabletconn.ErrFunc) {
	var usedConn tabletconn.TabletConn
	var erFunc tabletconn.ErrFunc
	var results <-chan *sqltypes.Result
	err := dg.withRetry(ctx, keyspace, shard, tabletType, func(conn tabletconn.TabletConn) error {
		var err error
		results, erFunc, err = conn.StreamExecute(ctx, query, bindVars, transactionID)
		usedConn = conn
		return err
	}, transactionID, true)
	if err != nil {
		return results, func() error { return err }
	}
	inTransaction := (transactionID != 0)
	return results, func() error {
		return WrapError(erFunc(), keyspace, shard, tabletType, usedConn.EndPoint(), inTransaction)
	}
}

// Begin starts a transaction for the specified keyspace, shard, and tablet type.
// It returns the transaction ID.
func (dg *discoveryGateway) Begin(ctx context.Context, keyspace string, shard string, tabletType topodatapb.TabletType) (transactionID int64, err error) {
	attemptNumber := 0
	err = dg.withRetry(ctx, keyspace, shard, tabletType, func(conn tabletconn.TabletConn) error {
		var innerErr error
		// Potentially buffer this transaction.
		txbuffer.FakeBuffer(keyspace, shard, attemptNumber)
		transactionID, innerErr = conn.Begin(ctx)
		attemptNumber++
		return innerErr
	}, 0, false)
	return transactionID, err
}

// Commit commits the current transaction for the specified keyspace, shard, and tablet type.
func (dg *discoveryGateway) Commit(ctx context.Context, keyspace, shard string, tabletType topodatapb.TabletType, transactionID int64) error {
	return dg.withRetry(ctx, keyspace, shard, tabletType, func(conn tabletconn.TabletConn) error {
		return conn.Commit(ctx, transactionID)
	}, transactionID, false)
}

// Rollback rolls back the current transaction for the specified keyspace, shard, and tablet type.
func (dg *discoveryGateway) Rollback(ctx context.Context, keyspace, shard string, tabletType topodatapb.TabletType, transactionID int64) error {
	return dg.withRetry(ctx, keyspace, shard, tabletType, func(conn tabletconn.TabletConn) error {
		return conn.Rollback(ctx, transactionID)
	}, transactionID, false)
}

// SplitQuery splits a query into sub-queries for the specified keyspace, shard, and tablet type.
func (dg *discoveryGateway) SplitQuery(ctx context.Context, keyspace, shard string, tabletType topodatapb.TabletType, sql string, bindVariables map[string]interface{}, splitColumn string, splitCount int64) (queries []querytypes.QuerySplit, err error) {
	err = dg.withRetry(ctx, keyspace, shard, tabletType, func(conn tabletconn.TabletConn) error {
		var innerErr error
		queries, innerErr = conn.SplitQuery(ctx, querytypes.BoundQuery{
			Sql:           sql,
			BindVariables: bindVariables,
		}, splitColumn, splitCount)
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

// CacheStatus returns a list of GatewayEndPointCacheStatus per endpoint.
func (dg *discoveryGateway) CacheStatus() GatewayEndPointCacheStatusList {
	return nil
}

// StatsUpdate receives updates about target and realtime stats changes.
func (dg *discoveryGateway) StatsUpdate(*discovery.EndPointStats) {
}

// withRetry gets available connections and executes the action. If there are retryable errors,
// it retries retryCount times before failing. It does not retry if the connection is in
// the middle of a transaction. While returning the error check if it maybe a result of
// a resharding event, and set the re-resolve bit and let the upper layers
// re-resolve and retry.
func (dg *discoveryGateway) withRetry(ctx context.Context, keyspace, shard string, tabletType topodatapb.TabletType, action func(conn tabletconn.TabletConn) error, transactionID int64, isStreaming bool) error {
	var endPointLastUsed *topodatapb.EndPoint
	var err error
	inTransaction := (transactionID != 0)
	invalidEndPoints := make(map[string]bool)

	for i := 0; i < dg.retryCount+1; i++ {
		var endPoint *topodatapb.EndPoint
		endPoints := dg.getEndPoints(keyspace, shard, tabletType)
		if len(endPoints) == 0 {
			// fail fast if there is no endpoint
			err = vterrors.FromError(vtrpcpb.ErrorCode_INTERNAL_ERROR, fmt.Errorf("no valid endpoint"))
			break
		}
		shuffleEndPoints(endPoints)

		// skip endpoints we tried before
		for _, ep := range endPoints {
			if _, ok := invalidEndPoints[discovery.EndPointToMapKey(ep)]; !ok {
				endPoint = ep
				break
			}
		}
		if endPoint == nil {
			if err == nil {
				// do not override error from last attempt.
				err = vterrors.FromError(vtrpcpb.ErrorCode_INTERNAL_ERROR, fmt.Errorf("no available connection"))
			}
			break
		}

		// execute
		endPointLastUsed = endPoint
		conn := dg.hc.GetConnection(endPoint)
		if conn == nil {
			err = vterrors.FromError(vtrpcpb.ErrorCode_INTERNAL_ERROR, fmt.Errorf("no connection for %+v", endPoint))
			invalidEndPoints[discovery.EndPointToMapKey(endPoint)] = true
			continue
		}
		err = action(conn)
		if dg.canRetry(ctx, err, transactionID, isStreaming) {
			invalidEndPoints[discovery.EndPointToMapKey(endPoint)] = true
			continue
		}
		break
	}
	return WrapError(err, keyspace, shard, tabletType, endPointLastUsed, inTransaction)
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
		switch serverError.Code {
		case tabletconn.ERR_FATAL:
			// Do not retry on fatal error for streaming query.
			// For streaming query, vttablet sends:
			// - RETRY, if streaming is not started yet;
			// - FATAL, if streaming is broken halfway.
			// For non-streaming query, handle as ERR_RETRY.
			if isStreaming {
				return false
			}
			fallthrough
		case tabletconn.ERR_RETRY:
			// Retry on RETRY and FATAL if not in a transaction.
			inTransaction := (transactionID != 0)
			return !inTransaction
		default:
			// Not retry for TX_POOL_FULL and normal server errors.
			return false
		}
	}
	// Do not retry on operational error.
	return false
}

func shuffleEndPoints(endPoints []*topodatapb.EndPoint) {
	index := 0
	length := len(endPoints)
	for i := length - 1; i > 0; i-- {
		index = rand.Intn(i + 1)
		endPoints[i], endPoints[index] = endPoints[index], endPoints[i]
	}
}

// getEndPoints gets all available endpoints from HealthCheck,
// and selects the usable ones based several rules:
// master - return one from any cells with latest reparent timestamp;
// replica - return all from local cell.
// TODO(liang): select replica by replication lag.
func (dg *discoveryGateway) getEndPoints(keyspace, shard string, tabletType topodatapb.TabletType) []*topodatapb.EndPoint {
	epsList := dg.hc.GetEndPointStatsFromTarget(keyspace, shard, tabletType)
	// for master, use any cells and return the one with max reparent timestamp.
	if tabletType == topodatapb.TabletType_MASTER {
		var maxTimestamp int64
		var ep *topodatapb.EndPoint
		for _, eps := range epsList {
			if eps.LastError != nil || !eps.Serving {
				continue
			}
			if eps.TabletExternallyReparentedTimestamp >= maxTimestamp {
				maxTimestamp = eps.TabletExternallyReparentedTimestamp
				ep = eps.EndPoint
			}
		}
		if ep == nil {
			return nil
		}
		return []*topodatapb.EndPoint{ep}
	}
	// for non-master, use only endpoints from local cell and filter by replication lag.
	list := make([]*discovery.EndPointStats, 0, len(epsList))
	for _, eps := range epsList {
		if eps.LastError != nil || !eps.Serving {
			continue
		}
		if dg.localCell != eps.Cell {
			continue
		}
		list = append(list, eps)
	}
	list = discovery.FilterByReplicationLag(list)
	epList := make([]*topodatapb.EndPoint, 0, len(list))
	for _, eps := range list {
		epList = append(epList, eps.EndPoint)
	}
	return epList
}

// WrapError returns ShardConnError which preserves the original error code if possible,
// adds the connection context
// and adds a bit to determine whether the keyspace/shard needs to be
// re-resolved for a potential sharding event.
func WrapError(in error, keyspace, shard string, tabletType topodatapb.TabletType, endPoint *topodatapb.EndPoint, inTransaction bool) (wrapped error) {
	if in == nil {
		return nil
	}
	shardIdentifier := fmt.Sprintf("%s.%s.%s, %+v", keyspace, shard, strings.ToLower(tabletType.String()), endPoint)
	code := tabletconn.ERR_NORMAL
	serverError, ok := in.(*tabletconn.ServerError)
	if ok {
		code = serverError.Code
	}

	shardConnErr := &ShardConnError{
		Code:            code,
		ShardIdentifier: shardIdentifier,
		InTransaction:   inTransaction,
		Err:             in,
		EndPointCode:    vterrors.RecoverVtErrorCode(in),
	}
	return shardConnErr
}
