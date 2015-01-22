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
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/vt/concurrency"
	kproto "github.com/youtube/vitess/go/vt/key"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
	"golang.org/x/net/context"
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
	timings    *stats.MultiTimings

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
func NewScatterConn(serv SrvTopoServer, statsName, cell string, retryDelay time.Duration, retryCount int, timeout time.Duration) *ScatterConn {
	return &ScatterConn{
		toposerv:   serv,
		cell:       cell,
		retryDelay: retryDelay,
		retryCount: retryCount,
		timeout:    timeout,
		timings:    stats.NewMultiTimings(statsName, []string{"Operation", "Keyspace", "ShardName", "DbType"}),
		shardConns: make(map[string]*ShardConn),
	}
}

// InitializeConnections pre-initializes all ShardConn which create underlying connections.
// It also populates topology cache by accessing it.
// It is not necessary to call this function before serving queries,
// but it would reduce connection overhead when serving.
func (stc *ScatterConn) InitializeConnections(ctx context.Context) error {
	ksNames, err := stc.toposerv.GetSrvKeyspaceNames(ctx, stc.cell)
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	var errRecorder concurrency.AllErrorRecorder
	for _, ksName := range ksNames {
		wg.Add(1)
		go func(keyspace string) {
			defer wg.Done()
			// get SrvKeyspace for cell/keyspace
			ks, err := stc.toposerv.GetSrvKeyspace(ctx, stc.cell, keyspace)
			if err != nil {
				errRecorder.RecordError(err)
				return
			}
			// work on all shards of all serving tablet types
			for _, tabletType := range ks.TabletTypes {
				ksPartition, ok := ks.Partitions[tabletType]
				if !ok {
					errRecorder.RecordError(fmt.Errorf("%v.%v is not in SrvKeyspace.Partitions", keyspace, string(tabletType)))
					continue
				}
				for _, shard := range ksPartition.Shards {
					wg.Add(1)
					go func(shardName string, tabletType topo.TabletType) {
						defer wg.Done()
						shardConn := stc.getConnection(ctx, keyspace, shardName, tabletType)
						err = shardConn.Dial(ctx)
						if err != nil {
							errRecorder.RecordError(err)
							return
						}
					}(shard.ShardName(), tabletType)
				}
			}
		}(ksName)
	}
	wg.Wait()
	if errRecorder.HasErrors() {
		return errRecorder.Error()
	}
	return nil
}

// Execute executes a non-streaming query on the specified shards.
func (stc *ScatterConn) Execute(
	context context.Context,
	query string,
	bindVars map[string]interface{},
	keyspace string,
	shards []string,
	tabletType topo.TabletType,
	session *SafeSession,
) (*mproto.QueryResult, error) {
	results, allErrors := stc.multiGo(
		context,
		"Execute",
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

// ExecuteMulti is like Execute,
// but each shard gets its own bindVars. If len(shards) is not equal to
// len(bindVars), the function panics.
func (stc *ScatterConn) ExecuteMulti(
	context context.Context,
	query string,
	keyspace string,
	shardVars map[string]map[string]interface{},
	tabletType topo.TabletType,
	session *SafeSession,
) (*mproto.QueryResult, error) {
	results, allErrors := stc.multiGo(
		context,
		"Execute",
		keyspace,
		getShards(shardVars),
		tabletType,
		session,
		func(sdc *ShardConn, transactionId int64, sResults chan<- interface{}) error {
			innerqr, err := sdc.Execute(context, query, shardVars[sdc.shard], transactionId)
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

// ExecuteEntityIds executes queries that are shard specific.
func (stc *ScatterConn) ExecuteEntityIds(
	context context.Context,
	shards []string,
	sqls map[string]string,
	bindVars map[string]map[string]interface{},
	keyspace string,
	tabletType topo.TabletType,
	session *SafeSession,
) (*mproto.QueryResult, error) {
	results, allErrors := stc.multiGo(
		context,
		"ExecuteEntityIds",
		keyspace,
		shards,
		tabletType,
		session,
		func(sdc *ShardConn, transactionId int64, sResults chan<- interface{}) error {
			shard := sdc.shard
			sql := sqls[shard]
			bindVar := bindVars[shard]
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
	context context.Context,
	queries []tproto.BoundQuery,
	keyspace string,
	shards []string,
	tabletType topo.TabletType,
	session *SafeSession,
) (qrs *tproto.QueryResultList, err error) {
	results, allErrors := stc.multiGo(
		context,
		"ExecuteBatch",
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
	context context.Context,
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
		"StreamExecute",
		keyspace,
		shards,
		tabletType,
		session,
		func(sdc *ShardConn, transactionId int64, sResults chan<- interface{}) error {
			sr, errFunc := sdc.StreamExecute(context, query, bindVars, transactionId)
			if sr != nil {
				for qr := range sr {
					sResults <- qr
				}
			}
			return errFunc()
		})
	var replyErr error
	fieldSent := false
	for innerqr := range results {
		// We still need to finish pumping
		if replyErr != nil {
			continue
		}
		mqr := innerqr.(*mproto.QueryResult)
		// only send field info once for scattered streaming
		if len(mqr.Fields) > 0 && len(mqr.Rows) == 0 {
			if fieldSent {
				continue
			}
			fieldSent = true
		}
		replyErr = sendReply(mqr)
	}
	if replyErr != nil {
		allErrors.RecordError(replyErr)
	}
	return allErrors.AggrError(stc.aggregateErrors)
}

// StreamExecuteMulti is like StreamExecute,
// but each shard gets its own bindVars. If len(shards) is not equal to
// len(bindVars), the function panics.
func (stc *ScatterConn) StreamExecuteMulti(
	context context.Context,
	query string,
	keyspace string,
	shardVars map[string]map[string]interface{},
	tabletType topo.TabletType,
	session *SafeSession,
	sendReply func(reply *mproto.QueryResult) error,
) error {
	results, allErrors := stc.multiGo(
		context,
		"StreamExecute",
		keyspace,
		getShards(shardVars),
		tabletType,
		session,
		func(sdc *ShardConn, transactionId int64, sResults chan<- interface{}) error {
			sr, errFunc := sdc.StreamExecute(context, query, shardVars[sdc.shard], transactionId)
			if sr != nil {
				for qr := range sr {
					sResults <- qr
				}
			}
			return errFunc()
		})
	var replyErr error
	fieldSent := false
	for innerqr := range results {
		// We still need to finish pumping
		if replyErr != nil {
			continue
		}
		mqr := innerqr.(*mproto.QueryResult)
		// only send field info once for scattered streaming
		if len(mqr.Fields) > 0 && len(mqr.Rows) == 0 {
			if fieldSent {
				continue
			}
			fieldSent = true
		}
		replyErr = sendReply(mqr)
	}
	if replyErr != nil {
		allErrors.RecordError(replyErr)
	}
	return allErrors.AggrError(stc.aggregateErrors)
}

// Commit commits the current transaction. There are no retries on this operation.
func (stc *ScatterConn) Commit(context context.Context, session *SafeSession) (err error) {
	if session == nil {
		return fmt.Errorf("cannot commit: empty session")
	}
	if !session.InTransaction() {
		return fmt.Errorf("cannot commit: not in transaction")
	}
	if len(session.ShardSessions) == 0 {
		return fmt.Errorf("cannot commit: invalid session, always update session from RPC response")
	}
	committing := true
	for _, shardSession := range session.ShardSessions {
		sdc := stc.getConnection(context, shardSession.Keyspace, shardSession.Shard, shardSession.TabletType)
		if !committing {
			sdc.Rollback(context, shardSession.TransactionId)
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
func (stc *ScatterConn) Rollback(context context.Context, session *SafeSession) (err error) {
	if session == nil {
		return fmt.Errorf("cannot rollback: empty session")
	}
	if !session.InTransaction() {
		return fmt.Errorf("cannot rollback: not in transaction")
	}
	if len(session.ShardSessions) == 0 {
		return fmt.Errorf("cannot rollback: invalid session, always update session from RPC response")
	}
	for _, shardSession := range session.ShardSessions {
		sdc := stc.getConnection(context, shardSession.Keyspace, shardSession.Shard, shardSession.TabletType)
		sdc.Rollback(context, shardSession.TransactionId)
	}
	session.Reset()
	return nil
}

// SplitQuery scatters a SplitQuery request to all shards. For a set of
// splits received from a shard, it construct a KeyRange queries by
// appending that shard's keyrange to the splits. Aggregates all splits across
// all shards in no specific order and returns.
func (stc *ScatterConn) SplitQuery(ctx context.Context, query tproto.BoundQuery, splitCount int, keyRangeByShard map[string]kproto.KeyRange, keyspace string) ([]proto.SplitQueryPart, error) {
	actionFunc := func(sdc *ShardConn, transactionID int64, results chan<- interface{}) error {
		// Get all splits from this shard
		queries, err := sdc.SplitQuery(ctx, query, splitCount)
		if err != nil {
			return err
		}
		// Append the keyrange for this shard to all the splits received
		keyranges := []kproto.KeyRange{keyRangeByShard[sdc.shard]}
		splits := []proto.SplitQueryPart{}
		for _, query := range queries {
			krq := &proto.KeyRangeQuery{
				Sql:           query.Query.Sql,
				BindVariables: query.Query.BindVariables,
				Keyspace:      keyspace,
				KeyRanges:     keyranges,
				TabletType:    topo.TYPE_RDONLY,
			}
			split := proto.SplitQueryPart{
				Query: krq,
				Size:  query.RowCount,
			}
			splits = append(splits, split)
		}
		// Push all the splits from this shard to results channel
		results <- splits
		return nil
	}

	shards := []string{}
	for shard := range keyRangeByShard {
		shards = append(shards, shard)
	}
	allSplits, allErrors := stc.multiGo(ctx, "SplitQuery", keyspace, shards, topo.TYPE_RDONLY, NewSafeSession(&proto.Session{}), actionFunc)
	splits := []proto.SplitQueryPart{}
	for s := range allSplits {
		splits = append(splits, s.([]proto.SplitQueryPart)...)
	}
	if allErrors.HasErrors() {
		err := allErrors.AggrError(stc.aggregateErrors)
		return nil, err
	}
	return splits, nil
}

// Close closes the underlying ShardConn connections.
func (stc *ScatterConn) Close() error {
	stc.mu.Lock()
	defer stc.mu.Unlock()
	for _, v := range stc.shardConns {
		v.Close()
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
	context context.Context,
	name string,
	keyspace string,
	shards []string,
	tabletType topo.TabletType,
	session *SafeSession,
	action shardActionFunc,
) (rResults <-chan interface{}, allErrors *concurrency.AllErrorRecorder) {
	allErrors = new(concurrency.AllErrorRecorder)
	results := make(chan interface{}, len(shards))
	var wg sync.WaitGroup
	for shard := range unique(shards) {
		wg.Add(1)
		go func(shard string) {
			defer wg.Done()
			startTime := time.Now()
			defer stc.timings.Record([]string{name, keyspace, shard, string(tabletType)}, startTime)

			sdc := stc.getConnection(context, keyspace, shard, tabletType)
			transactionID, err := stc.updateSession(context, sdc, keyspace, shard, tabletType, session)
			if err != nil {
				allErrors.RecordError(err)
				return
			}
			err = action(sdc, transactionID, results)
			if err != nil {
				allErrors.RecordError(err)
				return
			}
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

func (stc *ScatterConn) getConnection(context context.Context, keyspace, shard string, tabletType topo.TabletType) *ShardConn {
	stc.mu.Lock()
	defer stc.mu.Unlock()

	key := fmt.Sprintf("%s.%s.%s", keyspace, shard, tabletType)
	sdc, ok := stc.shardConns[key]
	if !ok {
		sdc = NewShardConn(context, stc.toposerv, stc.cell, keyspace, shard, tabletType, stc.retryDelay, stc.retryCount, stc.timeout)
		stc.shardConns[key] = sdc
	}
	return sdc
}

func (stc *ScatterConn) updateSession(
	context context.Context,
	sdc *ShardConn,
	keyspace, shard string,
	tabletType topo.TabletType,
	session *SafeSession,
) (transactionID int64, err error) {
	if !session.InTransaction() {
		return 0, nil
	}
	// No need to protect ourselves from the race condition between
	// Find and Append. The higher level functions ensure that no
	// duplicate (keyspace, shard, tabletType) tuples can execute
	// this at the same time.
	transactionID = session.Find(keyspace, shard, tabletType)
	if transactionID != 0 {
		return transactionID, nil
	}
	transactionID, err = sdc.Begin(context)
	if err != nil {
		return 0, err
	}
	session.Append(&proto.ShardSession{
		Keyspace:      keyspace,
		TabletType:    tabletType,
		Shard:         shard,
		TransactionId: transactionID,
	})
	return transactionID, nil
}

func getShards(shardVars map[string]map[string]interface{}) []string {
	shards := make([]string, 0, len(shardVars))
	for k := range shardVars {
		shards = append(shards, k)
	}
	return shards
}

func appendResult(qr, innerqr *mproto.QueryResult) {
	if innerqr.RowsAffected == 0 && len(innerqr.Fields) == 0 {
		return
	}
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
