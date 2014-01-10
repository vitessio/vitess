// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"fmt"
	"sync"
	"time"

	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
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
	conn TabletConn
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

// Execute executes a non-streaming query on vttablet. If there are connection errors,
// it retries retryCount times before failing. It does not retry if the connection is in
// the middle of a transaction.
func (sdc *ShardConn) Execute(query string, bindVars map[string]interface{}, transactionId int64) (qr *proto.QueryResult, err error) {
	err = sdc.withRetry(func(conn TabletConn) error {
		var innerErr error
		qr, innerErr = conn.Execute(query, bindVars, transactionId)
		return innerErr
	}, transactionId)
	return qr, err
}

// ExecuteBatch executes a group of queries. The retry rules are the same as Execute.
func (sdc *ShardConn) ExecuteBatch(queries []tproto.BoundQuery, transactionId int64) (qrs *proto.QueryResultList, err error) {
	err = sdc.withRetry(func(conn TabletConn) error {
		var innerErr error
		qrs, innerErr = conn.ExecuteBatch(queries, transactionId)
		return innerErr
	}, transactionId)
	return qrs, err
}

// StreamExecute executes a streaming query on vttablet. The retry rules are the same as Execute.
func (sdc *ShardConn) StreamExecute(query string, bindVars map[string]interface{}, transactionId int64) (results <-chan *proto.QueryResult, errFunc ErrFunc) {
	var usedConn TabletConn
	// We can ignore the error return because errFunc will have it
	_ = sdc.withRetry(func(conn TabletConn) error {
		results, errFunc = conn.StreamExecute(query, bindVars, transactionId)
		usedConn = conn
		return errFunc()
	}, transactionId)
	return results, func() error { return sdc.WrapError(errFunc(), usedConn) }
}

// Begin begins a transaction. The retry rules are the same as Execute.
func (sdc *ShardConn) Begin() (transactionId int64, err error) {
	err = sdc.withRetry(func(conn TabletConn) error {
		var innerErr error
		transactionId, innerErr = conn.Begin()
		return innerErr
	}, 0)
	return transactionId, err
}

// Commit commits the current transaction. The retry rules are the same as Execute.
func (sdc *ShardConn) Commit(transactionId int64) (err error) {
	return sdc.withRetry(func(conn TabletConn) error {
		return conn.Commit(transactionId)
	}, transactionId)
}

// Rollback rolls back the current transaction. The retry rules are the same as Execute.
func (sdc *ShardConn) Rollback(transactionId int64) (err error) {
	return sdc.withRetry(func(conn TabletConn) error {
		return conn.Rollback(transactionId)
	}, transactionId)
}

// withRetry sets up the connection and exexutes the action. If there are connection errors,
// it retries retryCount times before failing. It does not retry if the connection is in
// the middle of a transaction.
func (sdc *ShardConn) withRetry(action func(conn TabletConn) error, transactionId int64) error {
	var conn TabletConn
	var err error
	for i := 0; i < sdc.retryCount; i++ {
		if conn, err = sdc.getConn(); err != nil {
			continue
		}
		if err = action(conn); sdc.canRetry(err, transactionId, conn) {
			continue
		}
		return sdc.WrapError(err, conn)
	}
	return sdc.WrapError(err, conn)
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

// getConn reuses an existing connection if possible. Otherwise
// it returns a connection which it will save for future reuse.
func (sdc *ShardConn) getConn() (TabletConn, error) {
	sdc.mu.Lock()
	defer sdc.mu.Unlock()
	if sdc.conn != nil {
		return sdc.conn, nil
	}

	endPoint, err := sdc.balancer.Get()
	if err != nil {
		return nil, err
	}
	conn, err := GetDialer()(endPoint, sdc.keyspace, sdc.shard)
	if err != nil {
		sdc.balancer.MarkDown(endPoint.Uid)
		return nil, err
	}
	sdc.conn = conn
	return sdc.conn, nil
}

// canRetry determines whether a query can be retried or not.
// OperationalErrors like retry/fatal cause a reconnect and retry if query is not in a txn.
// TxPoolFull causes a retry and all other errors are non-retry.
func (sdc *ShardConn) canRetry(err error, transactionId int64, conn TabletConn) bool {
	if err == nil {
		return false
	}
	if serverError, ok := err.(*ServerError); ok {
		switch serverError.Code {
		case ERR_TX_POOL_FULL:
			// Retry without reconnecting.
			time.Sleep(sdc.retryDelay)
			return true
		case ERR_RETRY, ERR_FATAL:
			// No-op: treat these errors as operational by breaking out of this switch
		default:
			// Should not retry for normal server errors.
			return false
		}
	}
	// Non-server errors or fatal/retry errors. Retry if we're not in a transaction.
	sdc.markDown(conn)
	inTransaction := (transactionId != 0)
	return !inTransaction
}

// markDown closes conn and temporarily marks the associated
// end point as unusable.
func (sdc *ShardConn) markDown(conn TabletConn) {
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

// WrapError adds the connection context to an error.
func (sdc *ShardConn) WrapError(in error, conn TabletConn) (wrapped error) {
	if in == nil {
		return nil
	}
	var endPoint topo.EndPoint
	if conn != nil {
		endPoint = conn.EndPoint()
	}
	return fmt.Errorf(
		"%v, shard: (%s.%s.%s), host: %s",
		in, sdc.keyspace, sdc.shard, sdc.tabletType, endPoint.Host)
}
