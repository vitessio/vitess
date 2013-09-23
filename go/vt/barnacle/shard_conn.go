// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package barnacle

import (
	"fmt"
	"strings"
	"time"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/rpcplus"
	"github.com/youtube/vitess/go/vt/topo"
)

// ShardConn represents a load balanced connection to a group
// of vttablets that belong to the same shard. ShardConn should
// not be concurrently used across goroutines.
type ShardConn struct {
	tabletProtocol string
	keyspace       string
	shard          string
	tabletType     topo.TabletType
	retryDelay     time.Duration
	retryCount     int
	balancer       *Balancer
	address        string
	conn           TabletConn
}

// NewShardConn creates a new ShardConn. It creates or reuses a Balancer from
// the supplied BalancerMap. retryDelay is as specified by Balancer. retryCount
// is the max number of retries before a ShardConn returns an error on an operation.
func NewShardConn(blm *BalancerMap, tabletProtocol, keyspace, shard string, tabletType topo.TabletType, retryDelay time.Duration, retryCount int) *ShardConn {
	return &ShardConn{
		tabletProtocol: tabletProtocol,
		keyspace:       keyspace,
		shard:          shard,
		tabletType:     tabletType,
		retryDelay:     retryDelay,
		retryCount:     retryCount,
		balancer:       blm.Balancer(keyspace, shard, tabletType, retryDelay),
	}
}

func (sdc *ShardConn) canRetry(err error) bool {
	if err == nil {
		return false
	}
	if _, ok := err.(rpcplus.ServerError); ok {
		errString := err.Error()
		if strings.HasPrefix(errString, "tx_pool_full") {
			// Retry without reconnecting.
			time.Sleep(sdc.retryDelay)
			return true
		}
		if !strings.HasPrefix(errString, "retry") && !strings.HasPrefix(errString, "fatal") {
			// Should not retry for normal server errors.
			return false
		}
	}
	// Non-server errors or fatal/retry errors. Retry if we're not in a transaction.
	inTransaction := (sdc.TransactionId() != 0)
	sdc.balancer.MarkDown(sdc.address)
	sdc.Close()
	return !inTransaction
}

// Execute executes a non-streaming query on vttablet. If there are connection errors,
// it retries retryCount times before failing. It does not retry if the connection is in
// the middle of a transaction.
func (sdc *ShardConn) Execute(query string, bindVars map[string]interface{}) (qr *mproto.QueryResult, err error) {
	for i := 0; i < sdc.retryCount; i++ {
		if sdc.conn == nil {
			var addr string
			addr, err = sdc.balancer.Get()
			if err != nil {
				return nil, sdc.wrapError(err)
			}
			var conn TabletConn
			conn, err = GetDialer(sdc.tabletProtocol)(addr, sdc.keyspace, sdc.shard, "", "", false)
			if err != nil {
				sdc.balancer.MarkDown(addr)
				continue
			}
			sdc.address = addr
			sdc.conn = conn
		}
		qr, err = sdc.conn.Execute(query, bindVars)
		if sdc.canRetry(err) {
			continue
		}
		return qr, sdc.wrapError(err)
	}
	return qr, sdc.wrapError(err)
}

// StreamExecute executes a streaming query on vttablet. The retry rules are the same.
// Calling other functions while streaming is not recommended.
func (sdc *ShardConn) StreamExecute(query string, bindVars map[string]interface{}) (results <-chan *mproto.QueryResult, errFunc ErrFunc) {
	var err error
	for i := 0; i < sdc.retryCount; i++ {
		if sdc.conn == nil {
			var addr string
			addr, err = sdc.balancer.Get()
			if err != nil {
				goto return_error
			}
			var conn TabletConn
			conn, err = GetDialer(sdc.tabletProtocol)(addr, sdc.keyspace, sdc.shard, "", "", false)
			if err != nil {
				sdc.balancer.MarkDown(addr)
				continue
			}
			sdc.address = addr
			sdc.conn = conn
		}
		results, errFunc = sdc.conn.StreamExecute(query, bindVars)
		err = errFunc()
		if sdc.canRetry(err) {
			continue
		}
		return results, func() error { return sdc.wrapError(err) }
	}

return_error:
	r := make(chan *mproto.QueryResult)
	close(r)
	return r, func() error { return sdc.wrapError(err) }
}

// Begin begins a transaction. The retry rules are the same.
func (sdc *ShardConn) Begin() (err error) {
	if sdc.TransactionId() != 0 {
		return sdc.wrapError(fmt.Errorf("cannot begin: already in transaction"))
	}
	for i := 0; i < sdc.retryCount; i++ {
		if sdc.conn == nil {
			var addr string
			addr, err = sdc.balancer.Get()
			if err != nil {
				return sdc.wrapError(err)
			}
			var conn TabletConn
			conn, err = GetDialer(sdc.tabletProtocol)(addr, sdc.keyspace, sdc.shard, "", "", false)
			if err != nil {
				sdc.balancer.MarkDown(addr)
				continue
			}
			sdc.address = addr
			sdc.conn = conn
		}
		err = sdc.conn.Begin()
		if sdc.canRetry(err) {
			continue
		}
		return sdc.wrapError(err)
	}
	return sdc.wrapError(err)
}

// Commit commits the current transaction. There are no retries on this operation.
func (sdc *ShardConn) Commit() (err error) {
	if sdc.TransactionId() == 0 {
		return sdc.wrapError(fmt.Errorf("cannot commit: not in transaction"))
	}
	return sdc.wrapError(sdc.conn.Commit())
}

// Rollback rolls back the current transaction. There are no retries on this operation.
func (sdc *ShardConn) Rollback() (err error) {
	if sdc.TransactionId() == 0 {
		return sdc.wrapError(fmt.Errorf("cannot rollback: not in transaction"))
	}
	return sdc.wrapError(sdc.conn.Rollback())
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
	err := sdc.conn.Close()
	sdc.address = ""
	sdc.conn = nil
	return sdc.wrapError(err)
}

func (sdc *ShardConn) wrapError(in error) (wrapped error) {
	if in == nil {
		return nil
	}
	return fmt.Errorf(
		"%v, shard: (%s.%s.%s), address: %s",
		in, sdc.keyspace, sdc.shard, sdc.tabletType, sdc.address)
}
