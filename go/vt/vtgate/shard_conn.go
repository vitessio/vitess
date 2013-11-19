// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"fmt"
	"time"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/topo"
)

// ShardConn represents a load balanced connection to a group
// of vttablets that belong to the same shard. ShardConn should
// not be concurrently used across goroutines.
type ShardConn struct {
	keyspace   string
	shard      string
	tabletType topo.TabletType
	retryDelay time.Duration
	retryCount int
	balancer   *Balancer
	endPoint   topo.EndPoint
	conn       TabletConn
}

// NewShardConn creates a new ShardConn. It creates or reuses a Balancer from
// the supplied BalancerMap. retryDelay is as specified by Balancer. retryCount
// is the max number of retries before a ShardConn returns an error on an operation.
func NewShardConn(blm *BalancerMap, keyspace, shard string, tabletType topo.TabletType, retryDelay time.Duration, retryCount int) *ShardConn {
	return &ShardConn{
		keyspace:   keyspace,
		shard:      shard,
		tabletType: tabletType,
		retryDelay: retryDelay,
		retryCount: retryCount,
		balancer:   blm.Balancer(keyspace, shard, tabletType, retryDelay),
	}
}

// canRetry determines whether a query can be retried or not.
// OperationalErrors like retry/fatal cause a reconnect and retry if query is not in a txn.
// TxPoolFull causes a retry and all other errors are non-retry.
func (sdc *ShardConn) canRetry(err error) bool {
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
	inTransaction := (sdc.TransactionId() != 0)
	sdc.balancer.MarkDown(sdc.endPoint.Uid)
	sdc.Close()
	return !inTransaction
}

// Execute executes a non-streaming query on vttablet. If there are connection errors,
// it retries retryCount times before failing. It does not retry if the connection is in
// the middle of a transaction.
func (sdc *ShardConn) Execute(query string, bindVars map[string]interface{}) (qr *mproto.QueryResult, err error) {
	for i := 0; i < sdc.retryCount; i++ {
		if sdc.conn == nil {
			var endPoint topo.EndPoint
			endPoint, err = sdc.balancer.Get()
			if err != nil {
				return nil, sdc.WrapError(err)
			}
			var conn TabletConn
			conn, err = GetDialer()(endPoint, sdc.keyspace, sdc.shard)
			if err != nil {
				sdc.balancer.MarkDown(endPoint.Uid)
				continue
			}
			sdc.endPoint = endPoint
			sdc.conn = conn
		}
		qr, err = sdc.conn.Execute(query, bindVars)
		if sdc.canRetry(err) {
			continue
		}
		return qr, sdc.WrapError(err)
	}
	return qr, sdc.WrapError(err)
}

// ExecuteBatch executes a group of queries. The retry rules are the same as Execute.
func (sdc *ShardConn) ExecuteBatch(queries []tproto.BoundQuery) (qrs *tproto.QueryResultList, err error) {
	for i := 0; i < sdc.retryCount; i++ {
		if sdc.conn == nil {
			var endPoint topo.EndPoint
			endPoint, err = sdc.balancer.Get()
			if err != nil {
				return nil, sdc.WrapError(err)
			}
			var conn TabletConn
			conn, err = GetDialer()(endPoint, sdc.keyspace, sdc.shard)
			if err != nil {
				sdc.balancer.MarkDown(endPoint.Uid)
				continue
			}
			sdc.endPoint = endPoint
			sdc.conn = conn
		}
		qrs, err = sdc.conn.ExecuteBatch(queries)
		if sdc.canRetry(err) {
			continue
		}
		return qrs, sdc.WrapError(err)
	}
	return qrs, sdc.WrapError(err)
}

// StreamExecute executes a streaming query on vttablet. The retry rules are the same as Execute.
// Calling other functions while streaming is not recommended.
func (sdc *ShardConn) StreamExecute(query string, bindVars map[string]interface{}) (results <-chan *mproto.QueryResult, errFunc ErrFunc) {
	var err error
	for i := 0; i < sdc.retryCount; i++ {
		if sdc.conn == nil {
			var endPoint topo.EndPoint
			endPoint, err = sdc.balancer.Get()
			if err != nil {
				goto return_error
			}
			var conn TabletConn
			conn, err = GetDialer()(endPoint, sdc.keyspace, sdc.shard)
			if err != nil {
				sdc.balancer.MarkDown(endPoint.Uid)
				continue
			}
			sdc.endPoint = endPoint
			sdc.conn = conn
		}
		results, errFunc = sdc.conn.StreamExecute(query, bindVars)
		err = errFunc()
		if sdc.canRetry(err) {
			continue
		}
		return results, func() error { return sdc.WrapError(err) }
	}

return_error:
	r := make(chan *mproto.QueryResult)
	close(r)
	return r, func() error { return sdc.WrapError(err) }
}

// Begin begins a transaction. The retry rules are the same as Execute.
func (sdc *ShardConn) Begin() (err error) {
	if sdc.TransactionId() != 0 {
		return sdc.WrapError(fmt.Errorf("cannot begin: already in transaction"))
	}
	for i := 0; i < sdc.retryCount; i++ {
		if sdc.conn == nil {
			var endPoint topo.EndPoint
			endPoint, err = sdc.balancer.Get()
			if err != nil {
				return sdc.WrapError(err)
			}
			var conn TabletConn
			conn, err = GetDialer()(endPoint, sdc.keyspace, sdc.shard)
			if err != nil {
				sdc.balancer.MarkDown(endPoint.Uid)
				continue
			}
			sdc.endPoint = endPoint
			sdc.conn = conn
		}
		err = sdc.conn.Begin()
		if sdc.canRetry(err) {
			continue
		}
		return sdc.WrapError(err)
	}
	return sdc.WrapError(err)
}

// Commit commits the current transaction. There are no retries on this operation.
func (sdc *ShardConn) Commit() (err error) {
	if sdc.TransactionId() == 0 {
		return sdc.WrapError(fmt.Errorf("cannot commit: not in transaction"))
	}
	return sdc.WrapError(sdc.conn.Commit())
}

// Rollback rolls back the current transaction. There are no retries on this operation.
func (sdc *ShardConn) Rollback() (err error) {
	if sdc.TransactionId() == 0 {
		return sdc.WrapError(fmt.Errorf("cannot rollback: not in transaction"))
	}
	return sdc.WrapError(sdc.conn.Rollback())
}

func (sdc *ShardConn) TransactionId() int64 {
	if sdc.conn == nil {
		return 0
	}
	return sdc.conn.TransactionId()
}

// Close closes the underlying vttablet connection.
func (sdc *ShardConn) Close() error {
	if sdc.conn == nil {
		return nil
	}
	if sdc.TransactionId() != 0 {
		sdc.conn.Rollback()
	}
	err := sdc.conn.Close()
	sdc.endPoint = topo.EndPoint{}
	sdc.conn = nil
	return sdc.WrapError(err)
}

// WrapError adds the connection context to an error.
func (sdc *ShardConn) WrapError(in error) (wrapped error) {
	if in == nil {
		return nil
	}
	return fmt.Errorf(
		"%v, shard: (%s.%s.%s), host: %s",
		in, sdc.keyspace, sdc.shard, sdc.tabletType, sdc.endPoint.Host)
}
