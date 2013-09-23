// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package barnacle

import (
	"fmt"
	"strings"
	"sync"
	"time"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/topo"
)

var idGen sync2.AtomicInt64

type VTConn struct {
	mu               sync.Mutex
	Id               int64
	balancerMap      *BalancerMap
	tabletProtocol   string
	tabletType       topo.TabletType
	retryDelay       time.Duration
	retryCount       int
	shardConns       map[string]*ShardConn
	transactionId    int64
	connsMu          sync.Mutex
	transactionConns []*ShardConn
}

func NewVTConn(blm *BalancerMap, tabletProtocol string, tabletType topo.TabletType, retryDelay time.Duration, retryCount int) *VTConn {
	return &VTConn{
		Id:             idGen.Add(1),
		balancerMap:    blm,
		tabletProtocol: tabletProtocol,
		tabletType:     tabletType,
		retryDelay:     retryDelay,
		retryCount:     retryCount,
		shardConns:     make(map[string]*ShardConn),
	}
}

// Execute executes a non-streaming query on the specified shards.
func (vtc *VTConn) Execute(query string, bindVars map[string]interface{}, keyspace string, shards []string) (qr *mproto.QueryResult, err error) {
	vtc.mu.Lock()
	defer vtc.mu.Unlock()

	switch len(shards) {
	case 0:
		return nil, nil
	case 1:
		return vtc.execOnShard(query, bindVars, keyspace, shards[0])
	}
	qr = new(mproto.QueryResult)
	results := make(chan *mproto.QueryResult, len(shards))
	allErrors := new(concurrency.AllErrorRecorder)
	var wg sync.WaitGroup
	for shard := range unique(shards) {
		wg.Add(1)
		go func(shard string) {
			defer wg.Done()
			innerqr, err := vtc.execOnShard(query, bindVars, keyspace, shard)
			if err != nil {
				allErrors.RecordError(err)
				return
			}
			results <- innerqr
		}(shard)
	}
	go func() {
		wg.Wait()
		close(results)
	}()
	for innerqr := range results {
		appendResult(qr, innerqr)
	}
	if allErrors.HasErrors() {
		if vtc.transactionId != 0 {
			errstr := allErrors.Error().Error()
			// We cannot recover from these errors
			if strings.Contains(errstr, "tx_pool_full") || strings.Contains(errstr, "not_in_tx") {
				vtc.rollback()
			}
		}
		return nil, allErrors.Error()
	}
	return qr, nil
}

// StreamExecute executes a streaming query on vttablet. The retry rules are the same.
func (vtc *VTConn) StreamExecute(query string, bindVars map[string]interface{}, keyspace string, shards []string, sendReply func(reply interface{}) error) error {
	vtc.mu.Lock()
	defer vtc.mu.Unlock()

	if vtc.transactionId != 0 {
		return fmt.Errorf("cannot stream in a transaction")
	}
	results := make(chan *mproto.QueryResult, len(shards))
	allErrors := new(concurrency.AllErrorRecorder)
	var wg sync.WaitGroup
	for shard := range unique(shards) {
		wg.Add(1)
		go func(shard string) {
			defer wg.Done()
			sr, errFunc := vtc.getConnection(keyspace, shard).StreamExecute(query, bindVars)
			for qr := range sr {
				results <- qr
			}
			err := errFunc()
			if err != nil {
				allErrors.RecordError(err)
			}
		}(shard)
	}
	go func() {
		wg.Wait()
		close(results)
	}()
	var replyErr error
	for innerqr := range results {
		// We still need to finish pumping
		if replyErr != nil {
			continue
		}
		replyErr = sendReply(innerqr)
	}
	if replyErr != nil {
		allErrors.RecordError(replyErr)
	}
	return allErrors.Error()
}

// Begin begins a transaction. The retry rules are the same.
func (vtc *VTConn) Begin() (txid int64, err error) {
	vtc.mu.Lock()
	defer vtc.mu.Unlock()

	if vtc.transactionId != 0 {
		return 0, fmt.Errorf("cannot begin: already in a transaction")
	}
	vtc.transactionId = idGen.Add(1)
	return vtc.transactionId, nil
}

// Commit commits the current transaction. There are no retries on this operation.
func (vtc *VTConn) Commit() (err error) {
	vtc.mu.Lock()
	defer vtc.mu.Unlock()

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
	vtc.mu.Lock()
	defer vtc.mu.Unlock()
	vtc.rollback()
	return nil
}

func (vtc *VTConn) rollback() {
	for _, tConn := range vtc.transactionConns {
		tConn.Rollback()
	}
	vtc.transactionConns = nil
	vtc.transactionId = 0
}

func (vtc *VTConn) TransactionId() int64 {
	vtc.mu.Lock()
	defer vtc.mu.Unlock()
	return vtc.transactionId
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

func (vtc *VTConn) getConnection(keyspace, shard string) *ShardConn {
	vtc.connsMu.Lock()
	defer vtc.connsMu.Unlock()

	key := fmt.Sprintf("%s.%s.%s", keyspace, vtc.tabletType, shard)
	sdc, ok := vtc.shardConns[key]
	if !ok {
		sdc = NewShardConn(vtc.balancerMap, vtc.tabletProtocol, keyspace, shard, vtc.tabletType, vtc.retryDelay, vtc.retryCount)
		vtc.shardConns[key] = sdc
	}
	return sdc
}

func (vtc *VTConn) prepConnection(keyspace, shard string) (*ShardConn, error) {
	sdc := vtc.getConnection(keyspace, shard)
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
	sdc, err := vtc.prepConnection(keyspace, shard)
	if err != nil {
		return nil, err
	}
	qr, err = sdc.Execute(query, bindVars)
	if err != nil {
		return nil, err
	}
	return qr, nil
}

func appendResult(qr, innerqr *mproto.QueryResult) {
	if qr.Fields == nil {
		qr.Fields = innerqr.Fields
	}
	qr.RowsAffected += innerqr.RowsAffected
	if innerqr.InsertId != 0 {
		qr.InsertId = innerqr.InsertId
	}
	qr.Rows = append(qr.Rows, innerqr.Rows...)
}

func unique(in []string) map[string]struct{} {
	out := make(map[string]struct{}, len(in))
	for _, v := range in {
		out[v] = struct{}{}
	}
	return out
}
