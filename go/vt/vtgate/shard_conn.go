// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"fmt"
	"sync"
	"time"

	log "github.com/golang/glog"
	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/timer"
	"github.com/youtube/vitess/go/vt/concurrency"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"
)

// ShardConn represents a load balanced connection to a group
// of vttablets that belong to the same shard. ShardConn can
// be concurrently used across goroutines. Such requests are
// interleaved on the same underlying connection.
type ShardConn struct {
	keyspace           string
	shard              string
	tabletType         topo.TabletType
	retryDelay         time.Duration
	retryCount         int
	connTimeoutTotal   time.Duration
	connTimeoutPerConn time.Duration
	connLife           time.Duration
	balancer           *Balancer
	consolidator       *sync2.Consolidator
	ticker             *timer.RandTicker

	connectTimings *stats.MultiTimings

	// conn needs a mutex because it can change during the lifetime of ShardConn.
	mu   sync.Mutex
	conn tabletconn.TabletConn
}

// NewShardConn creates a new ShardConn. It creates a Balancer using
// serv, cell, keyspace, tabletType and retryDelay. retryCount is the max
// number of retries before a ShardConn returns an error on an operation.
func NewShardConn(ctx context.Context, serv SrvTopoServer, cell, keyspace, shard string, tabletType topo.TabletType, retryDelay time.Duration, retryCount int, connTimeoutTotal, connTimeoutPerConn, connLife time.Duration, tabletConnectTimings *stats.MultiTimings) *ShardConn {
	getAddresses := func() (*topo.EndPoints, error) {
		endpoints, err := serv.GetEndPoints(ctx, cell, keyspace, shard, tabletType)
		if err != nil {
			return nil, fmt.Errorf("endpoints fetch error: %v", err)
		}
		return endpoints, nil
	}
	blc := NewBalancer(getAddresses, retryDelay)
	var ticker *timer.RandTicker
	if tabletType != topo.TYPE_MASTER {
		ticker = timer.NewRandTicker(connLife, connLife/2)
	}
	sdc := &ShardConn{
		keyspace:           keyspace,
		shard:              shard,
		tabletType:         tabletType,
		retryDelay:         retryDelay,
		retryCount:         retryCount,
		connTimeoutTotal:   connTimeoutTotal,
		connTimeoutPerConn: connTimeoutPerConn,
		connLife:           connLife,
		balancer:           blc,
		ticker:             ticker,
		consolidator:       sync2.NewConsolidator(),
		connectTimings:     tabletConnectTimings,
	}
	if ticker != nil {
		go func() {
			for range ticker.C {
				sdc.closeCurrent()
			}
		}()
	}
	return sdc
}

// ShardConnError is the shard conn specific error.
type ShardConnError struct {
	Code            int
	ShardIdentifier string
	InTransaction   bool
	// Preserve the original error, so that we don't need to parse the error string.
	Err error
}

func (e *ShardConnError) Error() string {
	if e.ShardIdentifier == "" {
		return fmt.Sprintf("%v", e.Err)
	}
	return fmt.Sprintf("shard, host: %s, %v", e.ShardIdentifier, e.Err)
}

// Dial creates tablet connection and connects to the vttablet.
// It is not necessary to call this function before serving queries,
// but it would reduce connection overhead when serving the first query.
func (sdc *ShardConn) Dial(ctx context.Context) error {
	return sdc.withRetry(ctx, func(conn tabletconn.TabletConn) error {
		return nil
	}, 0, false)
}

// Execute executes a non-streaming query on vttablet. If there are connection errors,
// it retries retryCount times before failing. It does not retry if the connection is in
// the middle of a transaction.
func (sdc *ShardConn) Execute(ctx context.Context, query string, bindVars map[string]interface{}, transactionID int64) (qr *mproto.QueryResult, err error) {
	err = sdc.withRetry(ctx, func(conn tabletconn.TabletConn) error {
		var innerErr error
		qr, innerErr = conn.Execute(ctx, query, bindVars, transactionID)
		return innerErr
	}, transactionID, false)
	return qr, err
}

// ExecuteBatch executes a group of queries. The retry rules are the same as Execute.
func (sdc *ShardConn) ExecuteBatch(ctx context.Context, queries []tproto.BoundQuery, transactionID int64) (qrs *tproto.QueryResultList, err error) {
	err = sdc.withRetry(ctx, func(conn tabletconn.TabletConn) error {
		var innerErr error
		qrs, innerErr = conn.ExecuteBatch(ctx, queries, transactionID)
		return innerErr
	}, transactionID, false)
	return qrs, err
}

// StreamExecute executes a streaming query on vttablet. The retry rules are the same as Execute.
func (sdc *ShardConn) StreamExecute(ctx context.Context, query string, bindVars map[string]interface{}, transactionID int64) (<-chan *mproto.QueryResult, tabletconn.ErrFunc) {
	var usedConn tabletconn.TabletConn
	var erFunc tabletconn.ErrFunc
	var results <-chan *mproto.QueryResult
	err := sdc.withRetry(ctx, func(conn tabletconn.TabletConn) error {
		var err error
		results, erFunc, err = conn.StreamExecute(ctx, query, bindVars, transactionID)
		usedConn = conn
		return err
	}, transactionID, true)
	if err != nil {
		return results, func() error { return err }
	}
	inTransaction := (transactionID != 0)
	return results, func() error { return sdc.WrapError(erFunc(), usedConn.EndPoint(), inTransaction) }
}

// Begin begins a transaction. The retry rules are the same as Execute.
func (sdc *ShardConn) Begin(ctx context.Context) (transactionID int64, err error) {
	err = sdc.withRetry(ctx, func(conn tabletconn.TabletConn) error {
		var innerErr error
		transactionID, innerErr = conn.Begin(ctx)
		return innerErr
	}, 0, false)
	return transactionID, err
}

// Commit commits the current transaction. The retry rules are the same as Execute.
func (sdc *ShardConn) Commit(ctx context.Context, transactionID int64) (err error) {
	return sdc.withRetry(ctx, func(conn tabletconn.TabletConn) error {
		return conn.Commit(ctx, transactionID)
	}, transactionID, false)
}

// Rollback rolls back the current transaction. The retry rules are the same as Execute.
func (sdc *ShardConn) Rollback(ctx context.Context, transactionID int64) (err error) {
	return sdc.withRetry(ctx, func(conn tabletconn.TabletConn) error {
		return conn.Rollback(ctx, transactionID)
	}, transactionID, false)
}

// SplitQuery splits a query into sub queries. The retry rules are the same as Execute.
func (sdc *ShardConn) SplitQuery(ctx context.Context, query tproto.BoundQuery, splitCount int) (queries []tproto.QuerySplit, err error) {
	err = sdc.withRetry(ctx, func(conn tabletconn.TabletConn) error {
		var innerErr error
		queries, innerErr = conn.SplitQuery(ctx, query, splitCount)
		return innerErr
	}, 0, false)
	return
}

// Close closes the underlying TabletConn.
func (sdc *ShardConn) Close() {
	if sdc.ticker != nil {
		sdc.ticker.Stop()
	}
	sdc.closeCurrent()
}

func (sdc *ShardConn) closeCurrent() {
	sdc.mu.Lock()
	defer sdc.mu.Unlock()
	if sdc.conn == nil {
		return
	}
	go sdc.conn.Close()
	sdc.conn = nil
}

// withRetry sets up the connection and executes the action. If there are connection errors,
// it retries retryCount times before failing. It does not retry if the connection is in
// the middle of a transaction. While returning the error check if it maybe a result of
// a resharding event, and set the re-resolve bit and let the upper layers
// re-resolve and retry.
func (sdc *ShardConn) withRetry(ctx context.Context, action func(conn tabletconn.TabletConn) error, transactionID int64, isStreaming bool) error {
	var conn tabletconn.TabletConn
	var endPoint topo.EndPoint
	var err error
	var isTimeout bool
	inTransaction := (transactionID != 0)
	// execute the action at least once even without retrying
	for i := 0; i < sdc.retryCount+1; i++ {
		conn, endPoint, isTimeout, err = sdc.getConn(ctx)
		if err != nil {
			if isTimeout || i == sdc.retryCount {
				break
			}
			time.Sleep(sdc.retryDelay)
			continue
		}
		err = action(conn)
		if sdc.canRetry(ctx, err, transactionID, conn, isStreaming) {
			continue
		}
		break
	}
	return sdc.WrapError(err, endPoint, inTransaction)
}

type connectResult struct {
	Conn      tabletconn.TabletConn
	EndPoint  topo.EndPoint
	IsTimeout bool
}

// getConn reuses an existing connection if possible.
// If no connection is available,
// it creates a new connection if no connection is being created.
// Otherwise it waits for the connection to be created.
func (sdc *ShardConn) getConn(ctx context.Context) (conn tabletconn.TabletConn, endPoint topo.EndPoint, isTimeout bool, err error) {
	sdc.mu.Lock()
	if sdc.conn != nil {
		conn = sdc.conn
		endPoint = conn.EndPoint()
		sdc.mu.Unlock()
		return conn, endPoint, false, nil
	}

	key := fmt.Sprintf("%s.%s.%s", sdc.keyspace, sdc.shard, sdc.tabletType)
	q, ok := sdc.consolidator.Create(key)
	sdc.mu.Unlock()
	if ok {
		defer q.Broadcast()
		conn, endPoint, isTimeout, err := sdc.getNewConn(ctx)
		log.Infof("Connecting to end point: %v", endPoint)
		q.Result = &connectResult{Conn: conn, EndPoint: endPoint, IsTimeout: isTimeout}
		q.Err = err
	} else {
		q.Wait()
	}

	connResult := q.Result.(*connectResult)
	return connResult.Conn, connResult.EndPoint, connResult.IsTimeout, q.Err
}

// getNewConn creates a new tablet connection with a separate per conn timeout.
// It limits the overall timeout to connTimeoutTotal by checking elapsed time after each blocking call.
func (sdc *ShardConn) getNewConn(ctx context.Context) (conn tabletconn.TabletConn, endPoint topo.EndPoint, isTimeout bool, err error) {
	startTime := time.Now()

	endPoints, err := sdc.balancer.Get()
	if err != nil {
		// Error when getting endpoint
		return nil, topo.EndPoint{}, false, err
	}
	if len(endPoints) == 0 {
		// No valid endpoint
		return nil, topo.EndPoint{}, false, fmt.Errorf("no valid endpoint")
	}
	if time.Now().Sub(startTime) >= sdc.connTimeoutTotal {
		return nil, topo.EndPoint{}, true, fmt.Errorf("timeout when getting endpoints")
	}

	// Iterate through all endpoints to create a connection
	perConnTimeout := sdc.getConnTimeoutPerConn(len(endPoints))
	allErrors := new(concurrency.AllErrorRecorder)
	for _, endPoint := range endPoints {
		perConnStartTime := time.Now()
		conn, err = tabletconn.GetDialer()(ctx, endPoint, sdc.keyspace, sdc.shard, perConnTimeout)
		if err == nil {
			sdc.connectTimings.Record([]string{sdc.keyspace, sdc.shard, string(sdc.tabletType)}, perConnStartTime)
			sdc.mu.Lock()
			defer sdc.mu.Unlock()
			sdc.conn = conn
			return conn, endPoint, false, nil
		}
		// Markdown the endpoint if it failed to connect
		sdc.balancer.MarkDown(endPoint.Uid, err.Error())
		allErrors.RecordError(fmt.Errorf("%v %+v", err, endPoint))
		if time.Now().Sub(startTime) >= sdc.connTimeoutTotal {
			err = fmt.Errorf("timeout when connecting to %+v", endPoint)
			allErrors.RecordError(err)
			return nil, topo.EndPoint{}, true, allErrors.Error()
		}
	}
	return nil, topo.EndPoint{}, false, allErrors.Error()
}

// getConnTimeoutPerConn determines the appropriate timeout per connection.
func (sdc *ShardConn) getConnTimeoutPerConn(endPointCount int) time.Duration {
	if endPointCount <= 1 {
		return sdc.connTimeoutTotal
	}
	if sdc.connTimeoutPerConn > sdc.connTimeoutTotal {
		return sdc.connTimeoutTotal
	}
	return sdc.connTimeoutPerConn
}

// canRetry determines whether a query can be retried or not.
// OperationalErrors like retry/fatal cause a reconnect and retry if query is not in a txn.
// TxPoolFull causes a retry and all other errors are non-retry.
func (sdc *ShardConn) canRetry(ctx context.Context, err error, transactionID int64, conn tabletconn.TabletConn, isStreaming bool) bool {
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
			sdc.markDown(conn, err.Error())
			return !inTransaction
		default:
			// Not retry for TX_POOL_FULL and normal server errors.
			return false
		}
	}
	// Do not retry on operational error.
	// TODO(liang): handle the case when VTGate is idle
	// while vttablet is gracefully shutdown.
	// We want to retry in that case.
	sdc.markDown(conn, err.Error())
	return false
}

// markDown closes conn and temporarily marks the associated
// end point as unusable.
func (sdc *ShardConn) markDown(conn tabletconn.TabletConn, reason string) {
	sdc.mu.Lock()
	defer sdc.mu.Unlock()
	if conn != sdc.conn {
		return
	}
	sdc.balancer.MarkDown(conn.EndPoint().Uid, reason)

	go sdc.conn.Close()
	sdc.conn = nil
}

// WrapError returns ShardConnError which preserves the original error code if possible,
// adds the connection context
// and adds a bit to determine whether the keyspace/shard needs to be
// re-resolved for a potential sharding event.
func (sdc *ShardConn) WrapError(in error, endPoint topo.EndPoint, inTransaction bool) (wrapped error) {
	if in == nil {
		return nil
	}
	shardIdentifier := fmt.Sprintf("%s.%s.%s, %+v", sdc.keyspace, sdc.shard, sdc.tabletType, endPoint)
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
	}
	return shardConnErr
}
