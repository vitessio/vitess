// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"fmt"
	"strings"
	"sync"
	"time"

	log "github.com/golang/glog"
	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/vt/concurrency"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/topo"
)

var idGen sync2.AtomicInt64

// ScatterConn is used for executing queries across
// multiple ShardConn connections.
type ScatterConn struct {
	mu             sync.Mutex
	Id             int64
	balancerMap    *BalancerMap
	tabletProtocol string
	tabletType     topo.TabletType
	retryDelay     time.Duration
	retryCount     int
	shardConns     map[string]*ShardConn

	// Transaction tracking vars
	transactionId  int64
	connsMu        sync.Mutex
	transactionIds map[*ShardConn]int64
	commitOrder    []*ShardConn
}

// NewScatterConn creates a new ScatterConn. All input parameters are passed through
// for creating the appropriate ShardConn.
func NewScatterConn(blm *BalancerMap, tabletProtocol string, tabletType topo.TabletType, retryDelay time.Duration, retryCount int) *ScatterConn {
	return &ScatterConn{
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
func (stc *ScatterConn) Execute(query string, bindVars map[string]interface{}, keyspace string, shards []string) (*mproto.QueryResult, error) {
	stc.mu.Lock()
	defer stc.mu.Unlock()

	qr := new(mproto.QueryResult)
	allErrors := new(concurrency.AllErrorRecorder)
	switch len(shards) {
	case 0:
		return qr, nil
	case 1:
		// Fast-path for single shard execution
		var err error
		qr, err = stc.execOnShard(query, bindVars, keyspace, shards[0])
		allErrors.RecordError(err)
	default:
		results := make(chan *mproto.QueryResult, len(shards))
		var wg sync.WaitGroup
		for shard := range unique(shards) {
			wg.Add(1)
			go func(shard string) {
				defer wg.Done()
				innerqr, err := stc.execOnShard(query, bindVars, keyspace, shard)
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
	}
	if allErrors.HasErrors() {
		if stc.transactionId != 0 {
			errstr := allErrors.Error().Error()
			// We cannot recover from these errors
			if strings.Contains(errstr, "tx_pool_full") || strings.Contains(errstr, "not_in_tx") {
				stc.rollback()
			}
		}
		return nil, allErrors.Error()
	}
	return qr, nil
}

// Execute executes a non-streaming query on the specified shards.
func (stc *ScatterConn) ExecuteBatch(queries []tproto.BoundQuery, keyspace string, shards []string) (qrs *tproto.QueryResultList, err error) {
	stc.mu.Lock()
	defer stc.mu.Unlock()

	qrs = &tproto.QueryResultList{List: make([]mproto.QueryResult, len(queries))}
	allErrors := new(concurrency.AllErrorRecorder)
	if len(shards) == 0 {
		return qrs, nil
	}
	results := make(chan *tproto.QueryResultList, len(shards))
	var wg sync.WaitGroup
	for shard := range unique(shards) {
		wg.Add(1)
		go func(shard string) {
			defer wg.Done()
			sdc, err := stc.getConnection(keyspace, shard)
			if err != nil {
				allErrors.RecordError(err)
				return
			}
			innerqrs, err := sdc.ExecuteBatch(queries)
			if err != nil {
				allErrors.RecordError(err)
				return
			}
			results <- innerqrs
		}(shard)
	}
	go func() {
		wg.Wait()
		close(results)
	}()
	for innerqr := range results {
		for i := range qrs.List {
			appendResult(&qrs.List[i], &innerqr.List[i])
		}
	}
	if allErrors.HasErrors() {
		if stc.transactionId != 0 {
			errstr := allErrors.Error().Error()
			// We cannot recover from these errors
			if strings.Contains(errstr, "tx_pool_full") || strings.Contains(errstr, "not_in_tx") {
				stc.rollback()
			}
		}
		return nil, allErrors.Error()
	}
	return qrs, nil
}

// StreamExecute executes a streaming query on vttablet. The retry rules are the same.
func (stc *ScatterConn) StreamExecute(query string, bindVars map[string]interface{}, keyspace string, shards []string, sendReply func(reply interface{}) error) error {
	stc.mu.Lock()
	defer stc.mu.Unlock()

	if stc.transactionId != 0 {
		return fmt.Errorf("cannot stream in a transaction")
	}
	results := make(chan *mproto.QueryResult, len(shards))
	allErrors := new(concurrency.AllErrorRecorder)
	var wg sync.WaitGroup
	for shard := range unique(shards) {
		wg.Add(1)
		go func(shard string) {
			defer wg.Done()
			sdc, _ := stc.getConnection(keyspace, shard)
			sr, errFunc := sdc.StreamExecute(query, bindVars)
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
func (stc *ScatterConn) Begin() error {
	stc.mu.Lock()
	defer stc.mu.Unlock()

	if stc.transactionId != 0 {
		return fmt.Errorf("cannot begin: already in a transaction")
	}
	stc.transactionId = idGen.Add(1)
	stc.transactionIds = make(map[*ShardConn]int64)
	return nil
}

// Commit commits the current transaction. There are no retries on this operation.
func (stc *ScatterConn) Commit() (err error) {
	stc.mu.Lock()
	defer stc.mu.Unlock()

	if stc.transactionId == 0 {
		return fmt.Errorf("cannot commit: not in transaction")
	}
	committing := true
	for _, tConn := range stc.commitOrder {
		if !committing {
			tConn.Rollback()
			continue
		}
		if err = tConn.Commit(); err != nil {
			committing = false
		}
	}
	stc.transactionIds = nil
	stc.commitOrder = nil
	stc.transactionId = 0
	return err
}

// Rollback rolls back the current transaction. There are no retries on this operation.
func (stc *ScatterConn) Rollback() (err error) {
	stc.mu.Lock()
	defer stc.mu.Unlock()
	stc.rollback()
	return nil
}

func (stc *ScatterConn) rollback() {
	for _, tConn := range stc.commitOrder {
		tConn.Rollback()
	}
	stc.transactionIds = nil
	stc.commitOrder = nil
	stc.transactionId = 0
}

func (stc *ScatterConn) TransactionId() int64 {
	stc.mu.Lock()
	defer stc.mu.Unlock()
	return stc.transactionId
}

// Close closes the underlying ShardConn connections.
func (stc *ScatterConn) Close() error {
	stc.mu.Lock()
	defer stc.mu.Unlock()
	if stc.shardConns == nil {
		return nil
	}
	for _, v := range stc.shardConns {
		v.Close()
	}
	stc.shardConns = nil
	stc.balancerMap = nil
	return nil
}

// getConnection can fail only if we're in a transaction. Otherwise, it should
// always succeed.
func (stc *ScatterConn) getConnection(keyspace, shard string) (*ShardConn, error) {
	stc.connsMu.Lock()
	defer stc.connsMu.Unlock()

	key := fmt.Sprintf("%s.%s.%s", keyspace, stc.tabletType, shard)
	sdc, ok := stc.shardConns[key]
	if !ok {
		sdc = NewShardConn(stc.balancerMap, stc.tabletProtocol, keyspace, shard, stc.tabletType, stc.retryDelay, stc.retryCount)
		stc.shardConns[key] = sdc
	}
	if stc.transactionId != 0 {
		if txid := sdc.TransactionId(); txid != 0 {
			if txid != stc.transactionIds[sdc] {
				// This error will cause the transaction to abort.
				return nil, sdc.WrapError(fmt.Errorf("not_in_tx: connection is in a different transaction"))
			}
			return sdc, nil
		}
		if err := sdc.Begin(); err != nil {
			return nil, err
		}
		stc.transactionIds[sdc] = sdc.TransactionId()
		stc.commitOrder = append(stc.commitOrder, sdc)
		return sdc, nil
	}
	// This check is a failsafe. Should never happen.
	if sdc.TransactionId() != 0 {
		log.Warningf("Unexpected: connection %#v is in transaction", sdc)
		sdc.Rollback()
	}
	return sdc, nil
}

func (stc *ScatterConn) execOnShard(query string, bindVars map[string]interface{}, keyspace string, shard string) (qr *mproto.QueryResult, err error) {
	sdc, err := stc.getConnection(keyspace, shard)
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
