// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"fmt"
	"strings"
	"sync"
	"time"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/vt/concurrency"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
)

var idGen sync2.AtomicInt64

// ScatterConn is used for executing queries across
// multiple ShardConn connections.
type ScatterConn struct {
	toposerv   SrvTopoServer
	cell       string
	retryDelay time.Duration
	retryCount int
	timeout    time.Duration

	mu         sync.Mutex
	shardConns map[string]*ShardConn
}

// shardActionFunc defines the contract for a shard action. Every such function
// executes the necessary action on conn, sends the results to sResults, and
// return an error if any.
// multiGo is capable of executing multiple shardActionFunc actions in parallel
// and consolidating the results and errors for the caller.
type shardActionFunc func(conn *ShardConn, transactionId int64, sResults chan<- interface{}) error

// NewScatterConn creates a new ScatterConn. All input parameters are passed through
// for creating the appropriate ShardConn.
func NewScatterConn(serv SrvTopoServer, cell string, retryDelay time.Duration, retryCount int, timeout time.Duration) *ScatterConn {
	return &ScatterConn{
		toposerv:   serv,
		cell:       cell,
		retryDelay: retryDelay,
		retryCount: retryCount,
		timeout:    timeout,
		shardConns: make(map[string]*ShardConn),
	}
}

// Execute executes a non-streaming query on the specified shards.
func (stc *ScatterConn) Execute(
	context interface{},
	query string,
	bindVars map[string]interface{},
	keyspace string,
	shards []string,
	tabletType topo.TabletType,
	session *SafeSession,
) (*mproto.QueryResult, error) {
	results, allErrors := stc.multiGo(
		context,
		keyspace,
		shards,
		tabletType,
		session,
		func(sdc *ShardConn, transactionId int64, sResults chan<- interface{}) error {
			innerqr, err := sdc.Execute(context, query, bindVars, transactionId)
			if err != nil {
				return err
			}
			sResults <- innerqr
			return nil
		})

	qr := new(mproto.QueryResult)
	for innerqr := range results {
		innerqr := innerqr.(*mproto.QueryResult)
		appendResult(qr, innerqr)
	}
	if allErrors.HasErrors() {
		return nil, allErrors.AggrError(stc.aggregateErrors)
	}
	return qr, nil
}

func (stc *ScatterConn) ExecuteEntityIds(
	context interface{},
	shards []string,
	sqls map[string]string,
	bindVars map[string]map[string]interface{},
	keyspace string,
	tabletType topo.TabletType,
	session *SafeSession,
) (*mproto.QueryResult, error) {
	var lock sync.Mutex
	results, allErrors := stc.multiGo(
		context,
		keyspace,
		shards,
		tabletType,
		session,
		func(sdc *ShardConn, transactionId int64, sResults chan<- interface{}) error {
			shard := sdc.shard
			lock.Lock()
			sql := sqls[shard]
			bindVar := bindVars[shard]
			lock.Unlock()
			innerqr, err := sdc.Execute(context, sql, bindVar, transactionId)
			if err != nil {
				return err
			}
			sResults <- innerqr
			return nil
		})
	qr := new(mproto.QueryResult)
	for innerqr := range results {
		innerqr := innerqr.(*mproto.QueryResult)
		appendResult(qr, innerqr)
	}
	if allErrors.HasErrors() {
		return nil, allErrors.AggrError(stc.aggregateErrors)
	}
	return qr, nil
}

// ExecuteBatch executes a batch of non-streaming queries on the specified shards.
func (stc *ScatterConn) ExecuteBatch(
	context interface{},
	queries []tproto.BoundQuery,
	keyspace string,
	shards []string,
	tabletType topo.TabletType,
	session *SafeSession,
) (qrs *tproto.QueryResultList, err error) {
	results, allErrors := stc.multiGo(
		context,
		keyspace,
		shards,
		tabletType,
		session,
		func(sdc *ShardConn, transactionId int64, sResults chan<- interface{}) error {
			innerqrs, err := sdc.ExecuteBatch(context, queries, transactionId)
			if err != nil {
				return err
			}
			sResults <- innerqrs
			return nil
		})

	qrs = &tproto.QueryResultList{}
	qrs.List = make([]mproto.QueryResult, len(queries))
	for innerqr := range results {
		innerqr := innerqr.(*tproto.QueryResultList)
		for i := range qrs.List {
			appendResult(&qrs.List[i], &innerqr.List[i])
		}
	}
	if allErrors.HasErrors() {
		return nil, allErrors.AggrError(stc.aggregateErrors)
	}
	return qrs, nil
}

// StreamExecute executes a streaming query on vttablet. The retry rules are the same.
func (stc *ScatterConn) StreamExecute(
	context interface{},
	query string,
	bindVars map[string]interface{},
	keyspace string,
	shards []string,
	tabletType topo.TabletType,
	session *SafeSession,
	sendReply func(reply *mproto.QueryResult) error,
) error {
	results, allErrors := stc.multiGo(
		context,
		keyspace,
		shards,
		tabletType,
		session,
		func(sdc *ShardConn, transactionId int64, sResults chan<- interface{}) error {
			sr, errFunc := sdc.StreamExecute(context, query, bindVars, transactionId)
			for qr := range sr {
				sResults <- qr
			}
			return errFunc()
		})
	var replyErr error
	for innerqr := range results {
		// We still need to finish pumping
		if replyErr != nil {
			continue
		}
		replyErr = sendReply(innerqr.(*mproto.QueryResult))
	}
	if replyErr != nil {
		allErrors.RecordError(replyErr)
	}
	return allErrors.AggrError(stc.aggregateErrors)
}

// Commit commits the current transaction. There are no retries on this operation.
func (stc *ScatterConn) Commit(context interface{}, session *SafeSession) (err error) {
	if !session.InTransaction() {
		return fmt.Errorf("cannot commit: not in transaction")
	}
	committing := true
	for _, shardSession := range session.ShardSessions {
		sdc := stc.getConnection(shardSession.Keyspace, shardSession.Shard, shardSession.TabletType)
		if !committing {
			go sdc.Rollback(context, shardSession.TransactionId)
			continue
		}
		if err = sdc.Commit(context, shardSession.TransactionId); err != nil {
			committing = false
		}
	}
	session.Reset()
	return err
}

// Rollback rolls back the current transaction. There are no retries on this operation.
func (stc *ScatterConn) Rollback(context interface{}, session *SafeSession) (err error) {
	for _, shardSession := range session.ShardSessions {
		sdc := stc.getConnection(shardSession.Keyspace, shardSession.Shard, shardSession.TabletType)
		go sdc.Rollback(context, shardSession.TransactionId)
	}
	session.Reset()
	return nil
}

// Close closes the underlying ShardConn connections.
func (stc *ScatterConn) Close() error {
	stc.mu.Lock()
	defer stc.mu.Unlock()
	for _, v := range stc.shardConns {
		go v.Close()
	}
	stc.shardConns = make(map[string]*ShardConn)
	return nil
}

func (stc *ScatterConn) aggregateErrors(errors []error) error {
	if len(errors) == 0 {
		return nil
	}
	allRetryableError := true
	for _, e := range errors {
		connError, ok := e.(*ShardConnError)
		if !ok || (connError.Code != tabletconn.ERR_RETRY && connError.Code != tabletconn.ERR_FATAL) || connError.InTransaction {
			allRetryableError = false
			break
		}
	}
	var code int
	if allRetryableError {
		code = tabletconn.ERR_RETRY
	} else {
		code = tabletconn.ERR_NORMAL
	}
	errs := make([]string, 0, len(errors))
	for _, e := range errors {
		errs = append(errs, e.Error())
	}
	return &ShardConnError{
		Code: code,
		Err:  fmt.Sprintf("%v", strings.Join(errs, "\n")),
	}
}

// multiGo performs the requested 'action' on the specified shards in parallel.
// For each shard, it obtains a ShardConn connection. If the requested
// session is in a transaction, it opens a new transactions on the connection,
// and updates the Session with the transaction id. If the session already
// contains a transaction id for the shard, it reuses it.
// If there are any unrecoverable errors during a transaction, multiGo
// rolls back the transaction for all shards.
// The action function must match the shardActionFunc signature.
func (stc *ScatterConn) multiGo(
	context interface{},
	keyspace string,
	shards []string,
	tabletType topo.TabletType,
	session *SafeSession,
	action shardActionFunc,
) (rResults <-chan interface{}, allErrors *concurrency.AllErrorRecorder) {
	allErrors = new(concurrency.AllErrorRecorder)
	results := make(chan interface{}, len(shards))
	var wg sync.WaitGroup
	// We need the shards to be unique.
	for shard := range unique(shards) {
		wg.Add(1)
		go func(shard string) {
			defer wg.Done()
			stc.execShardAction(context, keyspace, shard, tabletType, session, action, allErrors, results)
		}(shard)
	}
	go func() {
		wg.Wait()
		// If we want to rollback, we have to do it before closing results
		// so that the session is updated to be not InTransaction.
		if allErrors.HasErrors() {
			if session.InTransaction() {
				errstr := allErrors.Error().Error()
				// We cannot recover from these errors
				if strings.Contains(errstr, "tx_pool_full") || strings.Contains(errstr, "not_in_tx") {
					stc.Rollback(context, session)
				}
			}
		}
		close(results)
	}()
	return results, allErrors
}

// execShardAction executes the action on a particular shard.
// If the action fails, it determines whether the keyspace/shard
// have moved, re-resolves the topology and tries again, if it is
// not executing a transaction.
func (stc *ScatterConn) execShardAction(
	context interface{},
	keyspace string,
	shard string,
	tabletType topo.TabletType,
	session *SafeSession,
	action shardActionFunc,
	allErrors *concurrency.AllErrorRecorder,
	results chan interface{},
) {
	for {
		sdc := stc.getConnection(keyspace, shard, tabletType)
		transactionId, err := stc.updateSession(context, sdc, keyspace, shard, tabletType, session)
		if err != nil {
			allErrors.RecordError(err)
			return
		}
		err = action(sdc, transactionId, results)
		if err != nil {
			allErrors.RecordError(err)
			return
		}
		break
	}
}

func (stc *ScatterConn) getConnection(keyspace, shard string, tabletType topo.TabletType) *ShardConn {
	stc.mu.Lock()
	defer stc.mu.Unlock()

	key := fmt.Sprintf("%s.%s.%s", keyspace, shard, tabletType)
	sdc, ok := stc.shardConns[key]
	if !ok {
		sdc = NewShardConn(stc.toposerv, stc.cell, keyspace, shard, tabletType, stc.retryDelay, stc.retryCount, stc.timeout)
		stc.shardConns[key] = sdc
	}
	return sdc
}

func (stc *ScatterConn) updateSession(
	context interface{},
	sdc *ShardConn,
	keyspace, shard string,
	tabletType topo.TabletType,
	session *SafeSession,
) (transactionId int64, err error) {
	if !session.InTransaction() {
		return 0, nil
	}
	// No need to protect ourselves from the race condition between
	// Find and Append. The higher level functions ensure that no
	// duplicate (keyspace, shard, tabletType) tuples can execute
	// this at the same time.
	transactionId = session.Find(keyspace, shard, tabletType)
	if transactionId != 0 {
		return transactionId, nil
	}
	transactionId, err = sdc.Begin(context)
	if err != nil {
		return 0, err
	}
	session.Append(&proto.ShardSession{
		Keyspace:      keyspace,
		TabletType:    tabletType,
		Shard:         shard,
		TransactionId: transactionId,
	})
	return transactionId, nil
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
