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

func (sdc *ShardConn) connect() error {
	var lastError error
	for i := 0; i < sdc.retryCount; i++ {
		addr, err := sdc.balancer.Get()
		if err != nil {
			return err
		}
		conn, err := GetDialer(sdc.tabletProtocol)(addr, sdc.keyspace, sdc.shard, "", "", false)
		if err != nil {
			lastError = err
			sdc.balancer.MarkDown(addr)
			continue
		}
		sdc.address = addr
		sdc.conn = conn
		return nil
	}
	return fmt.Errorf("could not obtain connection to %s.%s.%s, last error: %v", sdc.keyspace, sdc.shard, sdc.tabletType, lastError)
}

// Close closes the underlying vttablet connection.
func (sdc *ShardConn) Close() error {
	if sdc.conn == nil {
		return nil
	}
	err := sdc.conn.Close()
	sdc.address = ""
	sdc.conn = nil
	return err
}

func (sdc *ShardConn) mustReturn(err error) bool {
	if err == nil {
		return true
	}
	if _, ok := err.(rpcplus.ServerError); ok {
		return true
	}
	inTransaction := sdc.InTransaction()
	sdc.balancer.MarkDown(sdc.address)
	sdc.Close()
	return inTransaction
}

// Execute executes a non-streaming query on vttablet. If there are connection errors,
// it retries retryCount times before failing. It does not retry if the connection is in
// the middle of a transaction.
func (sdc *ShardConn) Execute(query string, bindVars map[string]interface{}) (qr *mproto.QueryResult, err error) {
	for i := 0; i < 2; i++ {
		if sdc.conn == nil {
			if err = sdc.connect(); err != nil {
				return nil, err
			}
		}
		qr, err = sdc.conn.Execute(query, bindVars)
		if sdc.mustReturn(err) {
			return qr, err
		}
	}
	return qr, err
}

// StreamExecute executes a streaming query on vttablet. The retry rules are the same.
// Calling other functions while streaming is not recommended.
func (sdc *ShardConn) StreamExecute(query string, bindVars map[string]interface{}) (results <-chan *mproto.QueryResult, errFunc ErrFunc) {
	for i := 0; i < 2; i++ {
		if sdc.conn == nil {
			if err := sdc.connect(); err != nil {
				r := make(chan *mproto.QueryResult)
				close(r)
				return r, func() error { return err }
			}
		}
		results, errFunc = sdc.conn.StreamExecute(query, bindVars)
		err := errFunc()
		if sdc.mustReturn(err) {
			return results, errFunc
		}
	}
	return results, errFunc
}

// Begin begins a transaction. The retry rules are the same.
func (sdc *ShardConn) Begin() (err error) {
	if sdc.InTransaction() {
		return fmt.Errorf("cannot begin: already in transaction")
	}
	for i := 0; i < 2; i++ {
		if sdc.conn == nil {
			if err = sdc.connect(); err != nil {
				return err
			}
		}
		for i := 0; i < sdc.retryCount; i++ {
			err = sdc.conn.Begin()
			if !strings.HasPrefix(err.Error(), "tx_pool_full") {
				break
			}
			time.Sleep(sdc.retryDelay)
		}
		if sdc.mustReturn(err) {
			return err
		}
	}
	return err
}

// Commit commits the current transaction. There are no retries on this operation.
func (sdc *ShardConn) Commit() (err error) {
	if !sdc.InTransaction() {
		return fmt.Errorf("cannot commit: not in transaction")
	}
	return sdc.conn.Commit()
}

// Rollback rolls back the current transaction. There are no retries on this operation.
func (sdc *ShardConn) Rollback() (err error) {
	if !sdc.InTransaction() {
		return fmt.Errorf("cannot rollback: not in transaction")
	}
	return sdc.conn.Rollback()
}

func (sdc *ShardConn) InTransaction() bool {
	return sdc.conn != nil && sdc.conn.InTransaction()
}
