// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"fmt"
	"sync"
	"time"

	"code.google.com/p/go.net/context"
	mproto "github.com/youtube/vitess/go/mysql/proto"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
	"github.com/youtube/vitess/go/vt/topo"
)

// ShardConn represents a load balanced connection to a group
// of vttablets that belong to the same shard. ShardConn can
// be concurrently used across goroutines. Such requests are
// interleaved on the same underlying connection.
type ShardConn struct {
	keyspace   string
	shard      string
	tabletType topo.TabletType
	retryDelay time.Duration
	retryCount int
	timeout    time.Duration
	balancer   *Balancer

	// conn needs a mutex because it can change during the lifetime of ShardConn.
	mu   sync.Mutex
	conn tabletconn.TabletConn
}

// NewShardConn creates a new ShardConn. It creates a Balancer using
// serv, cell, keyspace, tabletType and retryDelay. retryCount is the max
// number of retries before a ShardConn returns an error on an operation.
func NewShardConn(ctx context.Context, serv SrvTopoServer, cell, keyspace, shard string, tabletType topo.TabletType, retryDelay time.Duration, retryCount int, timeout time.Duration) *ShardConn {
	getAddresses := func() (*topo.EndPoints, error) {
		endpoints, err := serv.GetEndPoints(ctx, cell, keyspace, shard, tabletType)
		if err != nil {
			return nil, fmt.Errorf("endpoints fetch error: %v", err)
		}
		return endpoints, nil
	}
	blc := NewBalancer(getAddresses, retryDelay)
	return &ShardConn{
		keyspace:   keyspace,
		shard:      shard,
		tabletType: tabletType,
		retryDelay: retryDelay,
		retryCount: retryCount,
		timeout:    timeout,
		balancer:   blc,
	}
}

type ShardConnError struct {
	Code            int
	ShardIdentifier string
	InTransaction   bool
	Err             string
}

func (e *ShardConnError) Error() string {
	if e.ShardIdentifier == "" {
		return e.Err
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

func (sdc *ShardConn) SplitQuery(ctx context.Context, query tproto.BoundQuery, splitCount int) (queries []tproto.QuerySplit, err error) {
	err = sdc.withRetry(ctx, func(conn tabletconn.TabletConn) error {
		var innerErr error
		queries, innerErr = conn.SplitQuery(ctx, query, splitCount)
		return innerErr
	}, 0, false)
	return
}

// Close closes the underlying TabletConn. ShardConn can be
// reused after this because it opens connections on demand.
func (sdc *ShardConn) Close() {
	sdc.mu.Lock()
	defer sdc.mu.Unlock()
	if sdc.conn == nil {
		return
	}
	sdc.conn.Close()
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
	var retry bool
	inTransaction := (transactionID != 0)
	// execute the action at least once even without retrying
	for i := 0; i < sdc.retryCount+1; i++ {
		conn, endPoint, err, retry = sdc.getConn(ctx)
		if err != nil {
			if retry {
				continue
			}
			return sdc.WrapError(err, endPoint, inTransaction)
		}
		// no timeout for streaming query
		if isStreaming {
			err = action(conn)
		} else {
			tmr := time.NewTimer(sdc.timeout)
			done := make(chan int)
			var errAction error
			go func() {
				errAction = action(conn)
				close(done)
			}()
			select {
			case <-tmr.C:
				err = tabletconn.OperationalError("vttablet: call timeout")
			case <-done:
				err = errAction
			}
			tmr.Stop()
		}
		if sdc.canRetry(err, transactionID, conn) {
			continue
		}
		return sdc.WrapError(err, endPoint, inTransaction)
	}
	return sdc.WrapError(err, endPoint, inTransaction)
}

// getConn reuses an existing connection if possible. Otherwise
// it returns a connection which it will save for future reuse.
// If it returns an error, retry will tell you if getConn can be retried.
// If the context has a deadline and exceeded, it returns error and no-retry immediately.
func (sdc *ShardConn) getConn(ctx context.Context) (conn tabletconn.TabletConn, endPoint topo.EndPoint, err error, retry bool) {
	sdc.mu.Lock()
	defer sdc.mu.Unlock()

	// fail-fast if deadline exceeded
	deadline, ok := ctx.Deadline()
	if ok {
		if time.Now().After(deadline) {
			return nil, topo.EndPoint{}, tabletconn.OperationalError("vttablet: deadline exceeded"), false
		}
	}

	if sdc.conn != nil {
		return sdc.conn, sdc.conn.EndPoint(), nil, false
	}

	endPoint, err = sdc.balancer.Get()
	if err != nil {
		return nil, topo.EndPoint{}, err, false
	}
	conn, err = tabletconn.GetDialer()(ctx, endPoint, sdc.keyspace, sdc.shard, sdc.timeout)
	if err != nil {
		sdc.balancer.MarkDown(endPoint.Uid, err.Error())
		return nil, endPoint, err, true
	}
	sdc.conn = conn
	return sdc.conn, endPoint, nil, false
}

// canRetry determines whether a query can be retried or not.
// OperationalErrors like retry/fatal cause a reconnect and retry if query is not in a txn.
// TxPoolFull causes a retry and all other errors are non-retry.
func (sdc *ShardConn) canRetry(err error, transactionID int64, conn tabletconn.TabletConn) bool {
	if err == nil {
		return false
	}
	if serverError, ok := err.(*tabletconn.ServerError); ok {
		switch serverError.Code {
		case tabletconn.ERR_TX_POOL_FULL:
			// Retry without reconnecting.
			time.Sleep(sdc.retryDelay)
			return true
		case tabletconn.ERR_RETRY, tabletconn.ERR_FATAL:
			// No-op: treat these errors as operational by breaking out of this switch
		default:
			// Should not retry for normal server errors.
			return false
		}
	}
	// Non-server errors or fatal/retry errors. Retry if we're not in a transaction.
	inTransaction := (transactionID != 0)
	sdc.markDown(conn, err.Error())
	return !inTransaction
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

	// Launch as goroutine so we don't block
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
		Err:             in.Error(),
	}
	return shardConnErr
}
