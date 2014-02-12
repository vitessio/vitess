// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"fmt"
	"sync"
	"time"

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
	balancer   *Balancer

	// conn needs a mutex because it can change during the lifetime of ShardConn.
	mu   sync.Mutex
	conn tabletconn.TabletConn
}

// NewShardConn creates a new ShardConn. It creates a Balancer using
// serv, cell, keyspace, tabletType and retryDelay. retryCount is the max
// number of retries before a ShardConn returns an error on an operation.
func NewShardConn(serv SrvTopoServer, cell, keyspace, shard string, tabletType topo.TabletType, retryDelay time.Duration, retryCount int) *ShardConn {
	getAddresses := func() (*topo.EndPoints, error) {
		endpoints, err := serv.GetEndPoints(cell, keyspace, shard, tabletType)
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
		balancer:   blc,
	}
}

type ShardConnError struct {
	Code            int
	ShardIdentifier string
	topoReResolve   bool
	Err             string
}

func (e *ShardConnError) Error() string {
	return fmt.Sprintf("%v, shard, host: %s", e.Err, e.ShardIdentifier)
}

// Execute executes a non-streaming query on vttablet. If there are connection errors,
// it retries retryCount times before failing. It does not retry if the connection is in
// the middle of a transaction.
func (sdc *ShardConn) Execute(context interface{}, query string, bindVars map[string]interface{}, transactionId int64) (qr *mproto.QueryResult, err error) {
	err = sdc.withRetry(context, func(conn tabletconn.TabletConn) error {
		var innerErr error
		qr, innerErr = conn.Execute(context, query, bindVars, transactionId)
		return innerErr
	}, transactionId)
	return qr, err
}

// ExecuteBatch executes a group of queries. The retry rules are the same as Execute.
func (sdc *ShardConn) ExecuteBatch(context interface{}, queries []tproto.BoundQuery, transactionId int64) (qrs *tproto.QueryResultList, err error) {
	err = sdc.withRetry(context, func(conn tabletconn.TabletConn) error {
		var innerErr error
		qrs, innerErr = conn.ExecuteBatch(context, queries, transactionId)
		return innerErr
	}, transactionId)
	return qrs, err
}

// StreamExecute executes a streaming query on vttablet. The retry rules are the same as Execute.
func (sdc *ShardConn) StreamExecute(context interface{}, query string, bindVars map[string]interface{}, transactionId int64) (results <-chan *mproto.QueryResult, errFunc tabletconn.ErrFunc) {
	var usedConn tabletconn.TabletConn
	var erFunc tabletconn.ErrFunc
	err := sdc.withRetry(context, func(conn tabletconn.TabletConn) error {
		results, erFunc = conn.StreamExecute(context, query, bindVars, transactionId)
		usedConn = conn
		return erFunc()
	}, transactionId)
	if err != nil {
		return results, func() error { return err }
	}
	inTransaction := (transactionId != 0)
	return results, func() error { return sdc.WrapError(erFunc(), usedConn, inTransaction) }
}

// Begin begins a transaction. The retry rules are the same as Execute.
func (sdc *ShardConn) Begin(context interface{}) (transactionId int64, err error) {
	err = sdc.withRetry(context, func(conn tabletconn.TabletConn) error {
		var innerErr error
		transactionId, innerErr = conn.Begin(context)
		return innerErr
	}, 0)
	return transactionId, err
}

// Commit commits the current transaction. The retry rules are the same as Execute.
func (sdc *ShardConn) Commit(context interface{}, transactionId int64) (err error) {
	return sdc.withRetry(context, func(conn tabletconn.TabletConn) error {
		return conn.Commit(context, transactionId)
	}, transactionId)
}

// Rollback rolls back the current transaction. The retry rules are the same as Execute.
func (sdc *ShardConn) Rollback(context interface{}, transactionId int64) (err error) {
	return sdc.withRetry(context, func(conn tabletconn.TabletConn) error {
		return conn.Rollback(context, transactionId)
	}, transactionId)
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
func (sdc *ShardConn) withRetry(context interface{}, action func(conn tabletconn.TabletConn) error, transactionId int64) error {
	var conn tabletconn.TabletConn
	var err error
	var retry bool
	inTransaction := (transactionId != 0)
	for i := 0; i < sdc.retryCount; i++ {
		conn, err, retry = sdc.getConn(context)
		if err != nil {
			if retry {
				continue
			}
			return sdc.WrapError(err, conn, inTransaction)
		}
		if err = action(conn); sdc.canRetry(err, transactionId, conn) {
			continue
		}
		return sdc.WrapError(err, conn, inTransaction)
	}
	return sdc.WrapError(err, conn, inTransaction)
}

// getConn reuses an existing connection if possible. Otherwise
// it returns a connection which it will save for future reuse.
// If it returns an error,  retry will tell you if getConn can be retried.
func (sdc *ShardConn) getConn(context interface{}) (conn tabletconn.TabletConn, err error, retry bool) {
	sdc.mu.Lock()
	defer sdc.mu.Unlock()
	if sdc.conn != nil {
		return sdc.conn, nil, false
	}

	endPoint, err := sdc.balancer.Get()
	if err != nil {
		return nil, err, false
	}
	conn, err = tabletconn.GetDialer()(context, endPoint, sdc.keyspace, sdc.shard)
	if err != nil {
		sdc.balancer.MarkDown(endPoint.Uid)
		return nil, err, true
	}
	sdc.conn = conn
	return sdc.conn, nil, false
}

// canRetry determines whether a query can be retried or not.
// OperationalErrors like retry/fatal cause a reconnect and retry if query is not in a txn.
// TxPoolFull causes a retry and all other errors are non-retry.
func (sdc *ShardConn) canRetry(err error, transactionId int64, conn tabletconn.TabletConn) bool {
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
	inTransaction := (transactionId != 0)
	sdc.markDown(conn)
	return !inTransaction
}

func shouldResolveTopo(err error, inTransaction bool) bool {
	if err == nil {
		return false
	}
	if serverError, ok := err.(*tabletconn.ServerError); ok {
		switch serverError.Code {
		case tabletconn.ERR_RETRY, tabletconn.ERR_FATAL:
			return !inTransaction
		}
	}
	return false
}

// markDown closes conn and temporarily marks the associated
// end point as unusable.
func (sdc *ShardConn) markDown(conn tabletconn.TabletConn) {
	sdc.mu.Lock()
	defer sdc.mu.Unlock()
	if conn != sdc.conn {
		return
	}
	sdc.balancer.MarkDown(conn.EndPoint().Uid)

	// Launch as goroutine so we don't block
	go sdc.conn.Close()
	sdc.conn = nil
}

// WrapError returns ShardConnError which preserves the original error code if possible,
// adds the connection context
// and adds a bit to determine whether the keyspace/shard needs to be
// re-resolved for a potential sharding event.
func (sdc *ShardConn) WrapError(in error, conn tabletconn.TabletConn, inTransaction bool) (wrapped error) {
	if in == nil {
		return nil
	}
	shardIdentifier := fmt.Sprintf("%s.%s.%s", sdc.keyspace, sdc.shard, sdc.tabletType)
	if conn != nil {
		shardIdentifier += fmt.Sprintf(", %s", conn.EndPoint().Host)
	}

	code := tabletconn.ERR_NORMAL
	serverError, ok := in.(*tabletconn.ServerError)
	if ok {
		code = serverError.Code
	}

	topoReResolve := shouldResolveTopo(in, inTransaction)

	shardConnErr := &ShardConnError{Code: code,
		ShardIdentifier: shardIdentifier,
		topoReResolve:   topoReResolve,
		Err:             in.Error(),
	}
	return shardConnErr
}
