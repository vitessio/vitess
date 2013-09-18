// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package barnacle

import (
	"fmt"
	"sync"
	"time"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/vt/topo"
)

var idGen sync2.AtomicInt64

type VTConn struct {
	Id               int64
	balancerMap      *BalancerMap
	tabletType       topo.TabletType
	retryDelay       time.Duration
	retryCount       int
	shardConns       map[string]*ShardConn
	transactionId    int64
	transactionConns []*ShardConn
}

func NewVTConn(blm *BalancerMap, tabletType topo.TabletType, retryDelay time.Duration, retryCount int) *VTConn {
	return &VTConn{
		Id:          idGen.Add(1),
		balancerMap: blm,
		tabletType:  tabletType,
		retryDelay:  retryDelay,
		retryCount:  retryCount,
		shardConns:  make(map[string]*ShardConn),
	}
}

// Close closes the underlying ShardConn connections.
func (vtc *VTConn) Close() error {
	if vtc.shardConns == nil {
		return nil
	}
	for _, v := range vtc.shardConns {
		v.Close()
	}
	vtc.shardConns = nil
	vtc.balancerMap = nil
	return nil
}

func (vtc *VTConn) getConnection(keyspace, shard string) (*ShardConn, error) {
	key := fmt.Sprintf("%s.%s.%s", keyspace, vtc.tabletType, shard)
	sdc, ok := vtc.shardConns[key]
	if !ok {
		sdc = NewShardConn(vtc.balancerMap, keyspace, shard, vtc.tabletType, vtc.retryDelay, vtc.retryCount)
		vtc.shardConns[key] = sdc
	}
	if vtc.transactionId != 0 {
		if sdc.InTransaction() {
			return sdc, nil
		}
		if err := sdc.Begin(); err != nil {
			return nil, err
		}
		vtc.transactionConns = append(vtc.transactionConns, sdc)
	}
	return sdc, nil
}

func (vtc *VTConn) execOnShard(query string, bindVars map[string]interface{}, keyspace string, shard string) (qr *mproto.QueryResult, err error) {
	sdc, err := vtc.getConnection(keyspace, shard)
	if err != nil {
		return nil, err
	}
	qr, err = sdc.ExecDirect(query, bindVars)
	if err != nil {
		return nil, err
	}
	return qr, nil
}

func (vtc *VTConn) appendResult(qr, innerqr *mproto.QueryResult) {
	if qr.Fields == nil {
		qr.Fields = innerqr.Fields
	}
	qr.RowsAffected += innerqr.RowsAffected
	if innerqr.InsertId != 0 {
		qr.InsertId = innerqr.InsertId
	}
	qr.Rows = append(qr.Rows, innerqr.Rows...)
}

// ExecDirect executes a non-streaming query on the specified shards.
func (vtc *VTConn) ExecDirect(query string, bindVars map[string]interface{}, keyspace string, shards []string) (qr *mproto.QueryResult, err error) {
	switch len(shards) {
	case 0:
		return nil, nil
	case 1:
		return vtc.execOnShard(query, bindVars, keyspace, shards[0])
	}
	qr = new(mproto.QueryResult)
	resultChan := make(chan *mproto.QueryResult, len(shards))
	errorChan := make(chan error, len(shards))
	var wg sync.WaitGroup
	wg.Add(len(shards))
	for _, shard := range shards {
		go func() {
			defer wg.Done()
			innerqr, err := vtc.execOnShard(query, bindVars, keyspace, shard)
			if err != nil {
				errorChan <- err
				return
			}
			resultChan <- innerqr
		}()
	}
	go func() {
		wg.Wait()
		close(resultChan)
		close(errorChan)
	}()
	for innerqr := range resultChan {
		vtc.appendResult(qr, innerqr)
	}
	for err = range errorChan {
		return nil, err
	}
	return qr, nil
}

// ExecStream executes a streaming query on vttablet. The retry rules are the same.
func (vtc *VTConn) ExecStream(query string, bindVars map[string]interface{}) (sr *StreamResult, err error) {
	return nil, fmt.Errorf("Unimplemented")
}

// Begin begins a transaction. The retry rules are the same.
func (vtc *VTConn) Begin() (err error) {
	if vtc.transactionId != 0 {
		return fmt.Errorf("cannot begin: already in a transaction")
	}
	vtc.transactionId = idGen.Add(1)
	return nil
}

// Commit commits the current transaction. There are no retries on this operation.
func (vtc *VTConn) Commit() (err error) {
	if vtc.transactionId == 0 {
		return fmt.Errorf("cannot commit: not in transaction")
	}
	committing := true
	for _, tConn := range vtc.transactionConns {
		if !committing {
			tConn.Rollback()
			continue
		}
		if err = tConn.Commit(); err != nil {
			committing = false
		}
	}
	vtc.transactionConns = nil
	vtc.transactionId = 0
	return err
}

// Rollback rolls back the current transaction. There are no retries on this operation.
func (vtc *VTConn) Rollback() (err error) {
	for _, tConn := range vtc.transactionConns {
		tConn.Rollback()
	}
	vtc.transactionConns = nil
	vtc.transactionId = 0
	return nil
}
