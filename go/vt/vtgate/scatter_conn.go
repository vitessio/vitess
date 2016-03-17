// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/discovery"
	"github.com/youtube/vitess/go/vt/tabletserver/querytypes"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vterrors"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

// ScatterConn is used for executing queries across
// multiple shard level connections.
type ScatterConn struct {
	timings              *stats.MultiTimings
	tabletCallErrorCount *stats.MultiCounters
	gateway              Gateway
	testGateway          Gateway // test health checking module
}

// shardActionFunc defines the contract for a shard action. Every such function
// executes the necessary action on a shard, sends the results to sResults,
// and return an error if any.
// multiGo is capable of executing multiple shardActionFunc actions in parallel
// and consolidating the results and errors for the caller.
type shardActionFunc func(shard string, transactionID int64, sResults chan<- interface{}) error

// NewScatterConn creates a new ScatterConn. All input parameters are passed through
// for creating the appropriate connections.
func NewScatterConn(hc discovery.HealthCheck, topoServer topo.Server, serv topo.SrvTopoServer, statsName, cell string, retryDelay time.Duration, retryCount int, connTimeoutTotal, connTimeoutPerConn, connLife time.Duration, tabletTypesToWait []topodatapb.TabletType, testGateway string) *ScatterConn {
	tabletCallErrorCountStatsName := ""
	tabletConnectStatsName := ""
	if statsName != "" {
		tabletCallErrorCountStatsName = statsName + "ErrorCount"
		tabletConnectStatsName = statsName + "TabletConnect"
	}
	connTimings := stats.NewMultiTimings(tabletConnectStatsName, []string{"Keyspace", "ShardName", "DbType"})
	gateway := GetGatewayCreator()(hc, topoServer, serv, cell, retryDelay, retryCount, connTimeoutTotal, connTimeoutPerConn, connLife, connTimings, tabletTypesToWait)

	sc := &ScatterConn{
		timings:              stats.NewMultiTimings(statsName, []string{"Operation", "Keyspace", "ShardName", "DbType"}),
		tabletCallErrorCount: stats.NewMultiCounters(tabletCallErrorCountStatsName, []string{"Operation", "Keyspace", "ShardName", "DbType"}),
		gateway:              gateway,
	}

	// this is to test health checking module when using existing gateway
	if testGateway != "" {
		if gc := GetGatewayCreatorByName(testGateway); gc != nil {
			sc.testGateway = gc(hc, topoServer, serv, cell, retryDelay, retryCount, connTimeoutTotal, connTimeoutPerConn, connLife, connTimings, nil)
		}
	}

	return sc
}

// InitializeConnections pre-initializes connections for all shards.
// It also populates topology cache by accessing it.
// It is not necessary to call this function before serving queries,
// but it would reduce connection overhead when serving.
func (stc *ScatterConn) InitializeConnections(ctx context.Context) error {
	// temporarily start healthchecking regardless of gateway used
	if stc.testGateway != nil {
		stc.testGateway.InitializeConnections(ctx)
	}
	return stc.gateway.InitializeConnections(ctx)
}

// Execute executes a non-streaming query on the specified shards.
func (stc *ScatterConn) Execute(
	ctx context.Context,
	query string,
	bindVars map[string]interface{},
	keyspace string,
	shards []string,
	tabletType topodatapb.TabletType,
	session *SafeSession,
	notInTransaction bool,
) (*sqltypes.Result, error) {
	results, allErrors := stc.multiGo(
		ctx,
		"Execute",
		keyspace,
		shards,
		tabletType,
		session,
		notInTransaction,
		func(shard string, transactionID int64, sResults chan<- interface{}) error {
			innerqr, err := stc.gateway.Execute(ctx, keyspace, shard, tabletType, query, bindVars, transactionID)
			if err != nil {
				return err
			}
			sResults <- innerqr
			return nil
		})

	qr := new(sqltypes.Result)
	for innerqr := range results {
		innerqr := innerqr.(*sqltypes.Result)
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
	ctx context.Context,
	query string,
	keyspace string,
	shardVars map[string]map[string]interface{},
	tabletType topodatapb.TabletType,
	session *SafeSession,
	notInTransaction bool,
) (*sqltypes.Result, error) {
	results, allErrors := stc.multiGo(
		ctx,
		"Execute",
		keyspace,
		getShards(shardVars),
		tabletType,
		session,
		notInTransaction,
		func(shard string, transactionID int64, sResults chan<- interface{}) error {
			innerqr, err := stc.gateway.Execute(ctx, keyspace, shard, tabletType, query, shardVars[shard], transactionID)
			if err != nil {
				return err
			}
			sResults <- innerqr
			return nil
		})

	qr := new(sqltypes.Result)
	for innerqr := range results {
		innerqr := innerqr.(*sqltypes.Result)
		appendResult(qr, innerqr)
	}
	if allErrors.HasErrors() {
		return nil, allErrors.AggrError(stc.aggregateErrors)
	}
	return qr, nil
}

// ExecuteEntityIds executes queries that are shard specific.
func (stc *ScatterConn) ExecuteEntityIds(
	ctx context.Context,
	shards []string,
	sqls map[string]string,
	bindVars map[string]map[string]interface{},
	keyspace string,
	tabletType topodatapb.TabletType,
	session *SafeSession,
	notInTransaction bool,
) (*sqltypes.Result, error) {
	results, allErrors := stc.multiGo(
		ctx,
		"ExecuteEntityIds",
		keyspace,
		shards,
		tabletType,
		session,
		notInTransaction,
		func(shard string, transactionID int64, sResults chan<- interface{}) error {
			sql := sqls[shard]
			bindVar := bindVars[shard]
			innerqr, err := stc.gateway.Execute(ctx, keyspace, shard, tabletType, sql, bindVar, transactionID)
			if err != nil {
				return err
			}
			sResults <- innerqr
			return nil
		})

	qr := new(sqltypes.Result)
	for innerqr := range results {
		innerqr := innerqr.(*sqltypes.Result)
		appendResult(qr, innerqr)
	}
	if allErrors.HasErrors() {
		return nil, allErrors.AggrError(stc.aggregateErrors)
	}
	return qr, nil
}

// scatterBatchRequest needs to be built to perform a scatter batch query.
// A VTGate batch request will get translated into a differnt set of batches
// for each keyspace:shard, and those results will map to different positions in the
// results list. The length specifies the total length of the final results
// list. In each request variable, the resultIndexes specifies the position
// for each result from the shard.
type scatterBatchRequest struct {
	Length   int
	Requests map[string]*shardBatchRequest
}

type shardBatchRequest struct {
	Queries         []querytypes.BoundQuery
	Keyspace, Shard string
	ResultIndexes   []int
}

// ExecuteBatch executes a batch of non-streaming queries on the specified shards.
func (stc *ScatterConn) ExecuteBatch(
	ctx context.Context,
	batchRequest *scatterBatchRequest,
	tabletType topodatapb.TabletType,
	asTransaction bool,
	session *SafeSession) (qrs []sqltypes.Result, err error) {
	allErrors := new(concurrency.AllErrorRecorder)

	results := make([]sqltypes.Result, batchRequest.Length)
	var resMutex sync.Mutex

	var wg sync.WaitGroup
	for _, req := range batchRequest.Requests {
		wg.Add(1)
		go func(req *shardBatchRequest) {
			statsKey := []string{"ExecuteBatch", req.Keyspace, req.Shard, strings.ToLower(tabletType.String())}
			defer wg.Done()
			startTime := time.Now()
			defer stc.timings.Record(statsKey, startTime)

			transactionID, err := stc.updateSession(ctx, req.Keyspace, req.Shard, tabletType, session, false)
			if err != nil {
				allErrors.RecordError(err)
				stc.tabletCallErrorCount.Add(statsKey, 1)
				return
			}

			innerqrs, err := stc.gateway.ExecuteBatch(ctx, req.Keyspace, req.Shard, tabletType, req.Queries, asTransaction, transactionID)
			if err != nil {
				allErrors.RecordError(err)
				// Don't increment the error counter for duplicate keys, as those errors
				// are caused by client queries and are not VTGate's fault.
				// TODO(aaijazi): get rid of this string parsing, and handle all cases of invalid input
				if !strings.Contains(err.Error(), errDupKey) && !strings.Contains(err.Error(), errOutOfRange) {
					stc.tabletCallErrorCount.Add(statsKey, 1)
				}
				return
			}
			// Encapsulate in a function for safe mutex operation.
			func() {
				resMutex.Lock()
				defer resMutex.Unlock()
				for i, result := range innerqrs {
					appendResult(&results[req.ResultIndexes[i]], &result)
				}
			}()
		}(req)
	}
	wg.Wait()
	// If we want to rollback, we have to do it before closing results
	// so that the session is updated to be not InTransaction.
	if allErrors.HasErrors() {
		if session.InTransaction() {
			errstr := allErrors.Error().Error()
			// We cannot recover from these errors
			// TODO(aaijazi): get rid of this string parsing
			if strings.Contains(errstr, "tx_pool_full") || strings.Contains(errstr, "not_in_tx") {
				stc.Rollback(ctx, session)
			}
		}
		return nil, allErrors.AggrError(stc.aggregateErrors)
	}
	return results, nil
}

// StreamExecute executes a streaming query on vttablet. The retry rules are the same.
// The implementation of this function is similar to multiGo. A change there is likely
// to require a change in this function also.
func (stc *ScatterConn) StreamExecute(
	ctx context.Context,
	query string,
	bindVars map[string]interface{},
	keyspace string,
	shards []string,
	tabletType topodatapb.TabletType,
	sendReply func(reply *sqltypes.Result) error,
) error {
	results, allErrors := stc.multiGo(
		ctx,
		"StreamExecute",
		keyspace,
		shards,
		tabletType,
		NewSafeSession(nil),
		false,
		func(shard string, transactionID int64, sResults chan<- interface{}) error {
			sr, errFunc := stc.gateway.StreamExecute(ctx, keyspace, shard, tabletType, query, bindVars, transactionID)
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
		mqr := innerqr.(*sqltypes.Result)
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
	ctx context.Context,
	query string,
	keyspace string,
	shardVars map[string]map[string]interface{},
	tabletType topodatapb.TabletType,
	sendReply func(reply *sqltypes.Result) error,
) error {
	results, allErrors := stc.multiGo(
		ctx,
		"StreamExecute",
		keyspace,
		getShards(shardVars),
		tabletType,
		NewSafeSession(nil),
		false,
		func(shard string, transactionID int64, sResults chan<- interface{}) error {
			sr, errFunc := stc.gateway.StreamExecute(ctx, keyspace, shard, tabletType, query, shardVars[shard], transactionID)
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
		mqr := innerqr.(*sqltypes.Result)
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
func (stc *ScatterConn) Commit(ctx context.Context, session *SafeSession) (err error) {
	if session == nil {
		return vterrors.FromError(
			vtrpcpb.ErrorCode_BAD_INPUT,
			fmt.Errorf("cannot commit: empty session"),
		)
	}
	if !session.InTransaction() {
		return vterrors.FromError(
			vtrpcpb.ErrorCode_NOT_IN_TX,
			fmt.Errorf("cannot commit: not in transaction"),
		)
	}
	committing := true
	for _, shardSession := range session.ShardSessions {
		if !committing {
			stc.gateway.Rollback(ctx, shardSession.Target.Keyspace, shardSession.Target.Shard, shardSession.Target.TabletType, shardSession.TransactionId)
			continue
		}
		if err = stc.gateway.Commit(ctx, shardSession.Target.Keyspace, shardSession.Target.Shard, shardSession.Target.TabletType, shardSession.TransactionId); err != nil {
			committing = false
		}
	}
	session.Reset()
	return err
}

// Rollback rolls back the current transaction. There are no retries on this operation.
func (stc *ScatterConn) Rollback(ctx context.Context, session *SafeSession) (err error) {
	if session == nil {
		return nil
	}
	for _, shardSession := range session.ShardSessions {
		stc.gateway.Rollback(ctx, shardSession.Target.Keyspace, shardSession.Target.Shard, shardSession.Target.TabletType, shardSession.TransactionId)
	}
	session.Reset()
	return nil
}

// SplitQueryKeyRange scatters a SplitQuery request to all shards. For a set of
// splits received from a shard, it construct a KeyRange queries by
// appending that shard's keyrange to the splits. Aggregates all splits across
// all shards in no specific order and returns.
func (stc *ScatterConn) SplitQueryKeyRange(ctx context.Context, sql string, bindVariables map[string]interface{}, splitColumn string, splitCount int64, keyRangeByShard map[string]*topodatapb.KeyRange, keyspace string) ([]*vtgatepb.SplitQueryResponse_Part, error) {
	tabletType := topodatapb.TabletType_RDONLY
	actionFunc := func(shard string, transactionID int64, results chan<- interface{}) error {
		// Get all splits from this shard
		queries, err := stc.gateway.SplitQuery(ctx, keyspace, shard, tabletType, sql, bindVariables, splitColumn, splitCount)
		if err != nil {
			return err
		}
		// Append the keyrange for this shard to all the splits received,
		// if keyrange is nil for the shard (e.g. for single-sharded keyspaces during resharding),
		// append empty keyrange to represent the entire keyspace.
		keyranges := []*topodatapb.KeyRange{{Start: []byte{}, End: []byte{}}}
		if keyRangeByShard[shard] != nil {
			keyranges = []*topodatapb.KeyRange{keyRangeByShard[shard]}
		}
		splits := make([]*vtgatepb.SplitQueryResponse_Part, len(queries))
		for i, query := range queries {
			q, err := querytypes.BindVariablesToProto3(query.BindVariables)
			if err != nil {
				return err
			}
			splits[i] = &vtgatepb.SplitQueryResponse_Part{
				Query: &querypb.BoundQuery{
					Sql:           query.Sql,
					BindVariables: q,
				},
				KeyRangePart: &vtgatepb.SplitQueryResponse_KeyRangePart{
					Keyspace:  keyspace,
					KeyRanges: keyranges,
				},
				Size: query.RowCount,
			}
		}
		// Push all the splits from this shard to results channel
		results <- splits
		return nil
	}

	shards := []string{}
	for shard := range keyRangeByShard {
		shards = append(shards, shard)
	}
	allSplits, allErrors := stc.multiGo(ctx, "SplitQuery", keyspace, shards, tabletType, NewSafeSession(&vtgatepb.Session{}), false, actionFunc)
	splits := []*vtgatepb.SplitQueryResponse_Part{}
	for s := range allSplits {
		splits = append(splits, s.([]*vtgatepb.SplitQueryResponse_Part)...)
	}
	if allErrors.HasErrors() {
		err := allErrors.AggrError(stc.aggregateErrors)
		return nil, err
	}
	// We shuffle the query-parts here. External frameworks like MapReduce may
	// "deal" these jobs to workers in the order they are in the list. Without
	// shuffling workers can be very unevenly distributed among
	// the shards they query. E.g. all workers will first query the first shard,
	// then most of them to the second shard, etc, which results with uneven
	// load balancing among shards.
	shuffleQueryParts(splits)
	return splits, nil
}

// SplitQueryCustomSharding scatters a SplitQuery request to all
// shards. For a set of splits received from a shard, it construct a
// KeyRange queries by appending that shard's name to the
// splits. Aggregates all splits across all shards in no specific
// order and returns.
func (stc *ScatterConn) SplitQueryCustomSharding(ctx context.Context, sql string, bindVariables map[string]interface{}, splitColumn string, splitCount int64, shards []string, keyspace string) ([]*vtgatepb.SplitQueryResponse_Part, error) {
	tabletType := topodatapb.TabletType_RDONLY
	actionFunc := func(shard string, transactionID int64, results chan<- interface{}) error {
		// Get all splits from this shard
		queries, err := stc.gateway.SplitQuery(ctx, keyspace, shard, tabletType, sql, bindVariables, splitColumn, splitCount)
		if err != nil {
			return err
		}
		// Use the shards list for all the splits received
		shards := []string{shard}
		splits := make([]*vtgatepb.SplitQueryResponse_Part, len(queries))
		for i, query := range queries {
			q, err := querytypes.BindVariablesToProto3(query.BindVariables)
			if err != nil {
				return err
			}
			splits[i] = &vtgatepb.SplitQueryResponse_Part{
				Query: &querypb.BoundQuery{
					Sql:           query.Sql,
					BindVariables: q,
				},
				ShardPart: &vtgatepb.SplitQueryResponse_ShardPart{
					Keyspace: keyspace,
					Shards:   shards,
				},
				Size: query.RowCount,
			}
		}
		// Push all the splits from this shard to results channel
		results <- splits
		return nil
	}

	allSplits, allErrors := stc.multiGo(ctx, "SplitQuery", keyspace, shards, tabletType, NewSafeSession(&vtgatepb.Session{}), false, actionFunc)
	splits := []*vtgatepb.SplitQueryResponse_Part{}
	for s := range allSplits {
		splits = append(splits, s.([]*vtgatepb.SplitQueryResponse_Part)...)
	}
	if allErrors.HasErrors() {
		err := allErrors.AggrError(stc.aggregateErrors)
		return nil, err
	}
	// See the comment for the analogues line in SplitQueryKeyRange for
	// the motivation for shuffling.
	shuffleQueryParts(splits)
	return splits, nil
}

// randomGenerator is the randomGenerator used for the randomness
// of 'shuffleQueryParts'. It's initialized in 'init()' below.
type shuffleQueryPartsRandomGeneratorInterface interface {
	Intn(n int) int
}

var shuffleQueryPartsRandomGenerator shuffleQueryPartsRandomGeneratorInterface

func init() {
	shuffleQueryPartsRandomGenerator =
		rand.New(rand.NewSource(time.Now().UnixNano()))
}

// injectShuffleQueryParsRandomGenerator injects the given object
// as the random generator used by shuffleQueryParts. This function
// should only be used in tests and should not be called concurrently.
// It returns the previous shuffleQueryPartsRandomGenerator used.
func injectShuffleQueryPartsRandomGenerator(
	randGen shuffleQueryPartsRandomGeneratorInterface) shuffleQueryPartsRandomGeneratorInterface {
	oldRandGen := shuffleQueryPartsRandomGenerator
	shuffleQueryPartsRandomGenerator = randGen
	return oldRandGen
}

// shuffleQueryParts performs an in-place shuffle of the the given array.
// The result is a psuedo-random permutation of the array chosen uniformally
// from the space of all permutations.
func shuffleQueryParts(splits []*vtgatepb.SplitQueryResponse_Part) {
	for i := len(splits) - 1; i >= 1; i-- {
		randIndex := shuffleQueryPartsRandomGenerator.Intn(i + 1)
		// swap splits[i], splits[randIndex]
		splits[randIndex], splits[i] = splits[i], splits[randIndex]
	}
}

// Close closes the underlying Gateway.
func (stc *ScatterConn) Close() error {
	return stc.gateway.Close(context.Background())
}

// GetGatewayCacheStatus returns a displayable version of the Gateway cache.
func (stc *ScatterConn) GetGatewayCacheStatus() GatewayEndPointCacheStatusList {
	return stc.gateway.CacheStatus()
}

// ScatterConnError is the ScatterConn specific error.
type ScatterConnError struct {
	Code int
	// Preserve the original errors, so that we don't need to parse the error string.
	Errs []error
	// serverCode is the error code to use for all the server errors in aggregate
	serverCode vtrpcpb.ErrorCode
}

func (e *ScatterConnError) Error() string {
	return fmt.Sprintf("%v", vterrors.ConcatenateErrors(e.Errs))
}

// VtErrorCode returns the underlying Vitess error code
func (e *ScatterConnError) VtErrorCode() vtrpcpb.ErrorCode { return e.serverCode }

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

	return &ScatterConnError{
		Code:       code,
		Errs:       errors,
		serverCode: aggregateVtGateErrorCodes(errors),
	}
}

// multiGo performs the requested 'action' on the specified shards in parallel.
// For each shard, if the requested
// session is in a transaction, it opens a new transactions on the connection,
// and updates the Session with the transaction id. If the session already
// contains a transaction id for the shard, it reuses it.
// If there are any unrecoverable errors during a transaction, multiGo
// rolls back the transaction for all shards.
// The action function must match the shardActionFunc signature.
// This function has similarities with StreamExecute. A change there will likely
// require a change here also.
func (stc *ScatterConn) multiGo(
	ctx context.Context,
	name string,
	keyspace string,
	shards []string,
	tabletType topodatapb.TabletType,
	session *SafeSession,
	notInTransaction bool,
	action shardActionFunc,
) (rResults <-chan interface{}, allErrors *concurrency.AllErrorRecorder) {
	allErrors = new(concurrency.AllErrorRecorder)
	results := make(chan interface{}, len(shards))
	var wg sync.WaitGroup
	for shard := range unique(shards) {
		wg.Add(1)
		go func(shard string) {
			statsKey := []string{name, keyspace, shard, strings.ToLower(tabletType.String())}
			defer wg.Done()
			startTime := time.Now()
			defer stc.timings.Record(statsKey, startTime)

			transactionID, err := stc.updateSession(ctx, keyspace, shard, tabletType, session, notInTransaction)
			if err != nil {
				allErrors.RecordError(err)
				stc.tabletCallErrorCount.Add(statsKey, 1)
				return
			}
			err = action(shard, transactionID, results)
			if err != nil {
				allErrors.RecordError(err)
				// Don't increment the error counter for duplicate keys, as those errors
				// are caused by client queries and are not VTGate's fault.
				// TODO(aaijazi): get rid of this string parsing, and handle all cases of invalid input
				if !strings.Contains(err.Error(), errDupKey) && !strings.Contains(err.Error(), errOutOfRange) {
					stc.tabletCallErrorCount.Add(statsKey, 1)
				}
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
				// TODO(aaijazi): get rid of this string parsing. Might want a function that searches
				// through a deeply nested error chain a particular error.
				if strings.Contains(errstr, "tx_pool_full") || strings.Contains(errstr, "not_in_tx") {
					stc.Rollback(ctx, session)
				}
			}
		}
		close(results)
	}()
	return results, allErrors
}

func (stc *ScatterConn) updateSession(
	ctx context.Context,
	keyspace, shard string,
	tabletType topodatapb.TabletType,
	session *SafeSession,
	notInTransaction bool,
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
	// We are in a transaction at higher level,
	// but client requires not to start a transaction for this query.
	// If a transaction was started on this conn, we will use it (as above).
	if notInTransaction {
		return 0, nil
	}
	transactionID, err = stc.gateway.Begin(ctx, keyspace, shard, tabletType)
	if err != nil {
		return 0, err
	}
	session.Append(&vtgatepb.Session_ShardSession{
		Target: &querypb.Target{
			Keyspace:   keyspace,
			Shard:      shard,
			TabletType: tabletType,
		},
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

func appendResult(qr, innerqr *sqltypes.Result) {
	if innerqr.RowsAffected == 0 && len(innerqr.Fields) == 0 {
		return
	}
	if qr.Fields == nil {
		qr.Fields = innerqr.Fields
	}
	qr.RowsAffected += innerqr.RowsAffected
	if innerqr.InsertID != 0 {
		qr.InsertID = innerqr.InsertID
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
