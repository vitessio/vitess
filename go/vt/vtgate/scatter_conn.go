/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vtgate

import (
	"flag"
	"io"
	"math/rand"
	"sync"
	"time"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/gateway"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

var (
	messageStreamGracePeriod = flag.Duration("message_stream_grace_period", 30*time.Second, "the amount of time to give for a vttablet to resume if it ends a message stream, usually because of a reparent.")
)

// ScatterConn is used for executing queries across
// multiple shard level connections.
type ScatterConn struct {
	timings              *stats.MultiTimings
	tabletCallErrorCount *stats.CountersWithMultiLabels
	txConn               *TxConn
	gateway              gateway.Gateway
	healthCheck          discovery.HealthCheck
}

// shardActionFunc defines the contract for a shard action
// outside of a transaction. Every such function executes the
// necessary action on a shard, sends the results to sResults, and
// return an error if any.  multiGo is capable of executing
// multiple shardActionFunc actions in parallel and
// consolidating the results and errors for the caller.
type shardActionFunc func(rs *srvtopo.ResolvedShard, i int) error

// shardActionTransactionFunc defines the contract for a shard action
// that may be in a transaction. Every such function executes the
// necessary action on a shard (with an optional Begin call), aggregates
// the results, and return an error if any.
// multiGoTransaction is capable of executing multiple
// shardActionTransactionFunc actions in parallel and consolidating
// the results and errors for the caller.
type shardActionTransactionFunc func(rs *srvtopo.ResolvedShard, i int, shouldBegin bool, transactionID int64) (int64, error)

// NewScatterConn creates a new ScatterConn.
func NewScatterConn(statsName string, txConn *TxConn, gw gateway.Gateway, hc discovery.HealthCheck) *ScatterConn {
	tabletCallErrorCountStatsName := ""
	if statsName != "" {
		tabletCallErrorCountStatsName = statsName + "ErrorCount"
	}
	return &ScatterConn{
		timings: stats.NewMultiTimings(
			statsName,
			"Scatter connection timings",
			[]string{"Operation", "Keyspace", "ShardName", "DbType"}),
		tabletCallErrorCount: stats.NewCountersWithMultiLabels(
			tabletCallErrorCountStatsName,
			"Error count from tablet calls in scatter conns",
			[]string{"Operation", "Keyspace", "ShardName", "DbType"}),
		txConn:      txConn,
		gateway:     gw,
		healthCheck: hc,
	}
}

func (stc *ScatterConn) startAction(name string, target *querypb.Target) (time.Time, []string) {
	statsKey := []string{name, target.Keyspace, target.Shard, topoproto.TabletTypeLString(target.TabletType)}
	startTime := time.Now()
	return startTime, statsKey
}

func (stc *ScatterConn) endAction(startTime time.Time, allErrors *concurrency.AllErrorRecorder, statsKey []string, err *error, session *SafeSession) {
	if *err != nil {
		allErrors.RecordError(*err)
		// Don't increment the error counter for duplicate
		// keys or bad queries, as those errors are caused by
		// client queries and are not VTGate's fault.
		ec := vterrors.Code(*err)
		if ec != vtrpcpb.Code_ALREADY_EXISTS && ec != vtrpcpb.Code_INVALID_ARGUMENT {
			stc.tabletCallErrorCount.Add(statsKey, 1)
		}
		if ec == vtrpcpb.Code_RESOURCE_EXHAUSTED || ec == vtrpcpb.Code_ABORTED {
			session.SetRollback()
		}
	}
	stc.timings.Record(statsKey, startTime)
}

// Execute executes a non-streaming query on the specified shards.
func (stc *ScatterConn) Execute(
	ctx context.Context,
	query string,
	bindVars map[string]*querypb.BindVariable,
	rss []*srvtopo.ResolvedShard,
	tabletType topodatapb.TabletType,
	session *SafeSession,
	notInTransaction bool,
	options *querypb.ExecuteOptions,
) (*sqltypes.Result, error) {

	// mu protects qr
	var mu sync.Mutex
	qr := new(sqltypes.Result)

	allErrors := stc.multiGoTransaction(
		ctx,
		"Execute",
		rss,
		tabletType,
		session,
		notInTransaction,
		func(rs *srvtopo.ResolvedShard, i int, shouldBegin bool, transactionID int64) (int64, error) {
			var innerqr *sqltypes.Result
			if shouldBegin {
				var err error
				innerqr, transactionID, err = rs.QueryService.BeginExecute(ctx, rs.Target, query, bindVars, options)
				if err != nil {
					return transactionID, err
				}
			} else {
				var err error
				innerqr, err = rs.QueryService.Execute(ctx, rs.Target, query, bindVars, transactionID, options)
				if err != nil {
					return transactionID, err
				}
			}

			mu.Lock()
			defer mu.Unlock()
			// Don't append more rows if row count is exceeded.
			if len(qr.Rows) <= *maxMemoryRows {
				qr.AppendResult(innerqr)
			}
			return transactionID, nil
		},
	)

	if len(qr.Rows) > *maxMemoryRows {
		return nil, vterrors.Errorf(vtrpcpb.Code_RESOURCE_EXHAUSTED, "in-memory row count exceeded allowed limit of %d", *maxMemoryRows)
	}

	return qr, allErrors.AggrError(vterrors.Aggregate)
}

// ExecuteMultiShard is like Execute,
// but each shard gets its own Sql Queries and BindVariables.
//
// It always returns a non-nil query result and an array of
// shard errors which may be nil so that callers can optionally
// process a partially-successful operation.
func (stc *ScatterConn) ExecuteMultiShard(
	ctx context.Context,
	rss []*srvtopo.ResolvedShard,
	queries []*querypb.BoundQuery,
	tabletType topodatapb.TabletType,
	session *SafeSession,
	notInTransaction bool,
	autocommit bool,
) (qr *sqltypes.Result, errs []error) {

	// mu protects qr
	var mu sync.Mutex
	qr = new(sqltypes.Result)

	allErrors := stc.multiGoTransaction(
		ctx,
		"Execute",
		rss,
		tabletType,
		session,
		notInTransaction,
		func(rs *srvtopo.ResolvedShard, i int, shouldBegin bool, transactionID int64) (int64, error) {
			var (
				innerqr *sqltypes.Result
				err     error
				opts    *querypb.ExecuteOptions
			)
			if session != nil && session.Session != nil {
				opts = session.Session.Options
			}

			switch {
			case autocommit:
				innerqr, err = stc.executeAutocommit(ctx, rs, queries[i].Sql, queries[i].BindVariables, opts)
			case shouldBegin:
				innerqr, transactionID, err = rs.QueryService.BeginExecute(ctx, rs.Target, queries[i].Sql, queries[i].BindVariables, opts)
			default:
				innerqr, err = rs.QueryService.Execute(ctx, rs.Target, queries[i].Sql, queries[i].BindVariables, transactionID, opts)
			}
			if err != nil {
				return transactionID, err
			}

			mu.Lock()
			defer mu.Unlock()
			// Don't append more rows if row count is exceeded.
			if len(qr.Rows) <= *maxMemoryRows {
				qr.AppendResult(innerqr)
			}
			return transactionID, nil
		},
	)

	if len(qr.Rows) > *maxMemoryRows {
		return nil, []error{vterrors.Errorf(vtrpcpb.Code_RESOURCE_EXHAUSTED, "in-memory row count exceeded allowed limit of %d", *maxMemoryRows)}
	}

	return qr, allErrors.GetErrors()
}

func (stc *ScatterConn) executeAutocommit(ctx context.Context, rs *srvtopo.ResolvedShard, sql string, bindVariables map[string]*querypb.BindVariable, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	queries := []*querypb.BoundQuery{{
		Sql:           sql,
		BindVariables: bindVariables,
	}}
	// ExecuteBatch is a stop-gap because it's the only function that can currently do
	// single round-trip commit.
	qrs, err := rs.QueryService.ExecuteBatch(ctx, rs.Target, queries, true /* asTransaction */, 0, options)
	if err != nil {
		return nil, err
	}
	return &qrs[0], nil
}

// ExecuteEntityIds executes queries that are shard specific.
func (stc *ScatterConn) ExecuteEntityIds(
	ctx context.Context,
	rss []*srvtopo.ResolvedShard,
	sqls []string,
	bindVars []map[string]*querypb.BindVariable,
	tabletType topodatapb.TabletType,
	session *SafeSession,
	notInTransaction bool,
	options *querypb.ExecuteOptions,
) (*sqltypes.Result, error) {

	// mu protects qr
	var mu sync.Mutex
	qr := new(sqltypes.Result)

	allErrors := stc.multiGoTransaction(
		ctx,
		"ExecuteEntityIds",
		rss,
		tabletType,
		session,
		notInTransaction,
		func(rs *srvtopo.ResolvedShard, i int, shouldBegin bool, transactionID int64) (int64, error) {
			var innerqr *sqltypes.Result
			var err error

			if shouldBegin {
				innerqr, transactionID, err = rs.QueryService.BeginExecute(ctx, rs.Target, sqls[i], bindVars[i], options)
			} else {
				innerqr, err = rs.QueryService.Execute(ctx, rs.Target, sqls[i], bindVars[i], transactionID, options)
			}
			if err != nil {
				return transactionID, err
			}

			mu.Lock()
			defer mu.Unlock()
			qr.AppendResult(innerqr)
			return transactionID, nil
		})

	return qr, allErrors.AggrError(vterrors.Aggregate)
}

// scatterBatchRequest needs to be built to perform a scatter batch query.
// A VTGate batch request will get translated into a different set of batches
// for each keyspace:shard, and those results will map to different positions in the
// results list. The length specifies the total length of the final results
// list. In each request variable, the resultIndexes specifies the position
// for each result from the shard.
type scatterBatchRequest struct {
	// length is the total number of queries we have.
	length int
	// requests maps the 'keyspace:shard' key to the structure below.
	requests map[string]*shardBatchRequest
}

type shardBatchRequest struct {
	// rs is the ResolvedShard to send the queries to.
	rs *srvtopo.ResolvedShard
	// queries are the queries to send to that ResolvedShard.
	queries []*querypb.BoundQuery
	// resultIndexes describes the index of the query and its results
	// into the full original query array.
	resultIndexes []int
}

func boundShardQueriesToScatterBatchRequest(ctx context.Context, resolver *srvtopo.Resolver, boundQueries []*vtgatepb.BoundShardQuery, tabletType topodatapb.TabletType) (*scatterBatchRequest, error) {
	requests := &scatterBatchRequest{
		length:   len(boundQueries),
		requests: make(map[string]*shardBatchRequest),
	}
	for i, boundQuery := range boundQueries {
		rss, err := resolver.ResolveDestination(ctx, boundQuery.Keyspace, tabletType, key.DestinationShards(boundQuery.Shards))
		if err != nil {
			return nil, err
		}

		for _, rs := range rss {
			key := rs.Target.Keyspace + ":" + rs.Target.Shard
			request := requests.requests[key]
			if request == nil {
				request = &shardBatchRequest{
					rs: rs,
				}
				requests.requests[key] = request
			}
			request.queries = append(request.queries, boundQuery.Query)
			request.resultIndexes = append(request.resultIndexes, i)
		}
	}
	return requests, nil
}

func boundKeyspaceIDQueriesToScatterBatchRequest(ctx context.Context, resolver *srvtopo.Resolver, boundQueries []*vtgatepb.BoundKeyspaceIdQuery, tabletType topodatapb.TabletType) (*scatterBatchRequest, error) {
	requests := &scatterBatchRequest{
		length:   len(boundQueries),
		requests: make(map[string]*shardBatchRequest),
	}
	for i, boundQuery := range boundQueries {
		rss, err := resolver.ResolveDestination(ctx, boundQuery.Keyspace, tabletType, key.DestinationKeyspaceIDs(boundQuery.KeyspaceIds))
		if err != nil {
			return nil, err
		}

		for _, rs := range rss {
			key := rs.Target.Keyspace + ":" + rs.Target.Shard
			request := requests.requests[key]
			if request == nil {
				request = &shardBatchRequest{
					rs: rs,
				}
				requests.requests[key] = request
			}
			request.queries = append(request.queries, boundQuery.Query)
			request.resultIndexes = append(request.resultIndexes, i)
		}
	}
	return requests, nil
}

// ExecuteBatch executes a batch of non-streaming queries on the specified shards.
func (stc *ScatterConn) ExecuteBatch(
	ctx context.Context,
	batchRequest *scatterBatchRequest,
	tabletType topodatapb.TabletType,
	asTransaction bool,
	session *SafeSession,
	options *querypb.ExecuteOptions) (qrs []sqltypes.Result, err error) {

	allErrors := new(concurrency.AllErrorRecorder)

	results := make([]sqltypes.Result, batchRequest.length)
	var resMutex sync.Mutex

	var wg sync.WaitGroup
	for _, req := range batchRequest.requests {
		wg.Add(1)
		go func(req *shardBatchRequest) {
			defer wg.Done()
			var err error
			startTime, statsKey := stc.startAction("ExecuteBatch", req.rs.Target)
			defer stc.endAction(startTime, allErrors, statsKey, &err, session)

			shouldBegin, transactionID := transactionInfo(req.rs.Target, session, false)
			var innerqrs []sqltypes.Result
			if shouldBegin {
				innerqrs, transactionID, err = req.rs.QueryService.BeginExecuteBatch(ctx, req.rs.Target, req.queries, asTransaction, options)
				if transactionID != 0 {
					if appendErr := session.Append(&vtgatepb.Session_ShardSession{
						Target:        req.rs.Target,
						TransactionId: transactionID,
					}, stc.txConn.mode); appendErr != nil {
						err = appendErr
					}
				}
				if err != nil {
					return
				}
			} else {
				innerqrs, err = req.rs.QueryService.ExecuteBatch(ctx, req.rs.Target, req.queries, asTransaction, transactionID, options)
				if err != nil {
					return
				}
			}

			resMutex.Lock()
			defer resMutex.Unlock()
			for i, result := range innerqrs {
				results[req.resultIndexes[i]].AppendResult(&result)
			}
		}(req)
	}
	wg.Wait()

	if session.MustRollback() {
		stc.txConn.Rollback(ctx, session)
	}
	if allErrors.HasErrors() {
		return nil, allErrors.AggrError(vterrors.Aggregate)
	}
	return results, nil
}

func (stc *ScatterConn) processOneStreamingResult(mu *sync.Mutex, fieldSent *bool, qr *sqltypes.Result, callback func(*sqltypes.Result) error) error {
	mu.Lock()
	defer mu.Unlock()
	if *fieldSent {
		if len(qr.Rows) == 0 {
			// It's another field info result. Don't send.
			return nil
		}
	} else {
		if len(qr.Fields) == 0 {
			// Unreachable: this can happen only if vttablet misbehaves.
			return vterrors.New(vtrpcpb.Code_INTERNAL, "received rows before fields for shard")
		}
		*fieldSent = true
	}

	return callback(qr)
}

// StreamExecute executes a streaming query on vttablet. The retry rules are the same.
// Note we guarantee the callback will not be called concurrently
// by mutiple go routines, through processOneStreamingResult.
func (stc *ScatterConn) StreamExecute(
	ctx context.Context,
	query string,
	bindVars map[string]*querypb.BindVariable,
	rss []*srvtopo.ResolvedShard,
	tabletType topodatapb.TabletType,
	options *querypb.ExecuteOptions,
	callback func(reply *sqltypes.Result) error,
) error {

	// mu protects fieldSent, replyErr and callback
	var mu sync.Mutex
	fieldSent := false

	allErrors := stc.multiGo(ctx, "StreamExecute", rss, tabletType, func(rs *srvtopo.ResolvedShard, i int) error {
		return rs.QueryService.StreamExecute(ctx, rs.Target, query, bindVars, 0, options, func(qr *sqltypes.Result) error {
			return stc.processOneStreamingResult(&mu, &fieldSent, qr, callback)
		})
	})
	return allErrors.AggrError(vterrors.Aggregate)
}

// StreamExecuteMulti is like StreamExecute,
// but each shard gets its own bindVars. If len(shards) is not equal to
// len(bindVars), the function panics.
// Note we guarantee the callback will not be called concurrently
// by mutiple go routines, through processOneStreamingResult.
func (stc *ScatterConn) StreamExecuteMulti(
	ctx context.Context,
	query string,
	rss []*srvtopo.ResolvedShard,
	bindVars []map[string]*querypb.BindVariable,
	tabletType topodatapb.TabletType,
	options *querypb.ExecuteOptions,
	callback func(reply *sqltypes.Result) error,
) error {
	// mu protects fieldSent, callback and replyErr
	var mu sync.Mutex
	fieldSent := false

	allErrors := stc.multiGo(ctx, "StreamExecute", rss, tabletType, func(rs *srvtopo.ResolvedShard, i int) error {
		return rs.QueryService.StreamExecute(ctx, rs.Target, query, bindVars[i], 0, options, func(qr *sqltypes.Result) error {
			return stc.processOneStreamingResult(&mu, &fieldSent, qr, callback)
		})
	})
	return allErrors.AggrError(vterrors.Aggregate)
}

// timeTracker is a convenience wrapper used by MessageStream
// to track how long a stream has been unavailable.
type timeTracker struct {
	mu         sync.Mutex
	timestamps map[*querypb.Target]time.Time
}

func newTimeTracker() *timeTracker {
	return &timeTracker{
		timestamps: make(map[*querypb.Target]time.Time),
	}
}

// Reset resets the timestamp set by Record.
func (tt *timeTracker) Reset(target *querypb.Target) {
	tt.mu.Lock()
	defer tt.mu.Unlock()
	delete(tt.timestamps, target)
}

// Record records the time to Now if there was no previous timestamp,
// and it keeps returning that value until the next Reset.
func (tt *timeTracker) Record(target *querypb.Target) time.Time {
	tt.mu.Lock()
	defer tt.mu.Unlock()
	last, ok := tt.timestamps[target]
	if !ok {
		last = time.Now()
		tt.timestamps[target] = last
	}
	return last
}

// MessageStream streams messages from the specified shards.
// Note we guarantee the callback will not be called concurrently
// by mutiple go routines, through processOneStreamingResult.
func (stc *ScatterConn) MessageStream(ctx context.Context, rss []*srvtopo.ResolvedShard, name string, callback func(*sqltypes.Result) error) error {
	// The cancelable context is used for handling errors
	// from individual streams.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// mu is used to merge multiple callback calls into one.
	var mu sync.Mutex
	fieldSent := false
	lastErrors := newTimeTracker()
	allErrors := stc.multiGo(ctx, "MessageStream", rss, topodatapb.TabletType_MASTER, func(rs *srvtopo.ResolvedShard, i int) error {
		// This loop handles the case where a reparent happens, which can cause
		// an individual stream to end. If we don't succeed on the retries for
		// messageStreamGracePeriod, we abort and return an error.
		for {
			err := rs.QueryService.MessageStream(ctx, rs.Target, name, func(qr *sqltypes.Result) error {
				lastErrors.Reset(rs.Target)
				return stc.processOneStreamingResult(&mu, &fieldSent, qr, callback)
			})
			// nil and EOF are equivalent. UNAVAILABLE can be returned by vttablet if it's demoted
			// from master to replica. For any of these conditions, we have to retry.
			if err != nil && err != io.EOF && vterrors.Code(err) != vtrpcpb.Code_UNAVAILABLE {
				cancel()
				return err
			}

			// There was no error. We have to see if we need to retry.
			// If context was canceled, likely due to client disconnect,
			// return normally without retrying.
			select {
			case <-ctx.Done():
				return nil
			default:
			}
			firstErrorTimeStamp := lastErrors.Record(rs.Target)
			if time.Since(firstErrorTimeStamp) >= *messageStreamGracePeriod {
				// Cancel all streams and return an error.
				cancel()
				return vterrors.Errorf(vtrpcpb.Code_DEADLINE_EXCEEDED, "message stream from %v has repeatedly failed for longer than %v", rs.Target, *messageStreamGracePeriod)
			}

			// It's not been too long since our last good send. Wait and retry.
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(*messageStreamGracePeriod / 5):
			}
		}
	})
	return allErrors.AggrError(vterrors.Aggregate)
}

// MessageAck acks messages across multiple shards.
func (stc *ScatterConn) MessageAck(ctx context.Context, rss []*srvtopo.ResolvedShard, values [][]*querypb.Value, name string) (int64, error) {
	var mu sync.Mutex
	var totalCount int64
	allErrors := stc.multiGo(ctx, "MessageAck", rss, topodatapb.TabletType_MASTER, func(rs *srvtopo.ResolvedShard, i int) error {
		count, err := rs.QueryService.MessageAck(ctx, rs.Target, name, values[i])
		if err != nil {
			return err
		}
		mu.Lock()
		totalCount += count
		mu.Unlock()
		return nil
	})
	return totalCount, allErrors.AggrError(vterrors.Aggregate)
}

// UpdateStream just sends the query to the ResolvedShard,
// and sends the results back.
func (stc *ScatterConn) UpdateStream(ctx context.Context, rs *srvtopo.ResolvedShard, timestamp int64, position string, callback func(*querypb.StreamEvent) error) error {
	return rs.QueryService.UpdateStream(ctx, rs.Target, position, timestamp, callback)
}

// SplitQuery scatters a SplitQuery request to the shards whose names are given in 'shards'.
// For every set of *querypb.QuerySplit's received from a shard, it applies the given
// 'querySplitToPartFunc' function to convert each *querypb.QuerySplit into a
// 'SplitQueryResponse_Part' message. Finally, it aggregates the obtained
// SplitQueryResponse_Parts across all shards and returns the resulting slice.
func (stc *ScatterConn) SplitQuery(
	ctx context.Context,
	sql string,
	bindVariables map[string]*querypb.BindVariable,
	splitColumns []string,
	perShardSplitCount int64,
	numRowsPerQueryPart int64,
	algorithm querypb.SplitQueryRequest_Algorithm,
	rss []*srvtopo.ResolvedShard,
	querySplitToQueryPartFunc func(
		querySplit *querypb.QuerySplit, rs *srvtopo.ResolvedShard) (*vtgatepb.SplitQueryResponse_Part, error)) ([]*vtgatepb.SplitQueryResponse_Part, error) {

	tabletType := topodatapb.TabletType_RDONLY
	// allParts will collect the query-parts from all the shards. It's protected
	// by allPartsMutex.
	var allParts []*vtgatepb.SplitQueryResponse_Part
	var allPartsMutex sync.Mutex

	allErrors := stc.multiGo(
		ctx,
		"SplitQuery",
		rss,
		tabletType,
		func(rs *srvtopo.ResolvedShard, i int) error {
			// Get all splits from this shard
			query := &querypb.BoundQuery{
				Sql:           sql,
				BindVariables: bindVariables,
			}
			querySplits, err := rs.QueryService.SplitQuery(
				ctx,
				rs.Target,
				query,
				splitColumns,
				perShardSplitCount,
				numRowsPerQueryPart,
				algorithm)
			if err != nil {
				return err
			}
			parts := make([]*vtgatepb.SplitQueryResponse_Part, len(querySplits))
			for i, querySplit := range querySplits {
				parts[i], err = querySplitToQueryPartFunc(querySplit, rs)
				if err != nil {
					return err
				}
			}
			// Aggregate the parts from this shard into allParts.
			allPartsMutex.Lock()
			defer allPartsMutex.Unlock()
			allParts = append(allParts, parts...)
			return nil
		},
	)

	if allErrors.HasErrors() {
		err := allErrors.AggrError(vterrors.Aggregate)
		return nil, err
	}
	// We shuffle the query-parts here. External frameworks like MapReduce may
	// "deal" these jobs to workers in the order they are in the list. Without
	// shuffling workers can be very unevenly distributed among
	// the shards they query. E.g. all workers will first query the first shard,
	// then most of them to the second shard, etc, which results with uneven
	// load balancing among shards.
	shuffleQueryParts(allParts)
	return allParts, nil
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
// lint:ignore U1000 available for tests to use
func injectShuffleQueryPartsRandomGenerator(
	randGen shuffleQueryPartsRandomGeneratorInterface) shuffleQueryPartsRandomGeneratorInterface {
	oldRandGen := shuffleQueryPartsRandomGenerator
	shuffleQueryPartsRandomGenerator = randGen
	return oldRandGen
}

// shuffleQueryParts performs an in-place shuffle of the given array.
// The result is a pseudo-random permutation of the array chosen uniformally
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
func (stc *ScatterConn) GetGatewayCacheStatus() gateway.TabletCacheStatusList {
	return stc.gateway.CacheStatus()
}

// multiGo performs the requested 'action' on the specified
// shards in parallel. This does not handle any transaction state.
// The action function must match the shardActionFunc2 signature.
func (stc *ScatterConn) multiGo(
	ctx context.Context,
	name string,
	rss []*srvtopo.ResolvedShard,
	tabletType topodatapb.TabletType,
	action shardActionFunc,
) (allErrors *concurrency.AllErrorRecorder) {
	allErrors = new(concurrency.AllErrorRecorder)
	if len(rss) == 0 {
		return allErrors
	}

	oneShard := func(rs *srvtopo.ResolvedShard, i int) {
		var err error
		startTime, statsKey := stc.startAction(name, rs.Target)
		// Send a dummy session.
		// TODO(sougou): plumb a real session through this call.
		defer stc.endAction(startTime, allErrors, statsKey, &err, NewSafeSession(nil))
		err = action(rs, i)
	}

	if len(rss) == 1 {
		// only one shard, do it synchronously.
		oneShard(rss[0], 0)
		return allErrors
	}

	var wg sync.WaitGroup
	for i, rs := range rss {
		wg.Add(1)
		go func(rs *srvtopo.ResolvedShard, i int) {
			defer wg.Done()
			oneShard(rs, i)
		}(rs, i)
	}
	wg.Wait()
	return allErrors
}

// multiGoTransaction performs the requested 'action' on the specified
// ResolvedShards in parallel. For each shard, if the requested
// session is in a transaction, it opens a new transactions on the connection,
// and updates the Session with the transaction id. If the session already
// contains a transaction id for the shard, it reuses it.
// The action function must match the shardActionTransactionFunc signature.
//
// It returns an error recorder in which each shard error is recorded positionally,
// i.e. if rss[2] had an error, then the error recorder will store that error
// in the second position.
func (stc *ScatterConn) multiGoTransaction(
	ctx context.Context,
	name string,
	rss []*srvtopo.ResolvedShard,
	tabletType topodatapb.TabletType,
	session *SafeSession,
	notInTransaction bool,
	action shardActionTransactionFunc,
) (allErrors *concurrency.AllErrorRecorder) {

	numShards := len(rss)
	allErrors = new(concurrency.AllErrorRecorder)

	if numShards == 0 {
		return allErrors
	}
	oneShard := func(rs *srvtopo.ResolvedShard, i int) {
		var err error
		startTime, statsKey := stc.startAction(name, rs.Target)
		defer stc.endAction(startTime, allErrors, statsKey, &err, session)

		shouldBegin, transactionID := transactionInfo(rs.Target, session, notInTransaction)
		transactionID, err = action(rs, i, shouldBegin, transactionID)
		if shouldBegin && transactionID != 0 {
			if appendErr := session.Append(&vtgatepb.Session_ShardSession{
				Target:        rs.Target,
				TransactionId: transactionID,
			}, stc.txConn.mode); appendErr != nil {
				err = appendErr
			}
		}
	}

	var wg sync.WaitGroup
	if numShards == 1 {
		// only one shard, do it synchronously.
		for i, rs := range rss {
			oneShard(rs, i)
			goto end
		}
	}

	for i, rs := range rss {
		wg.Add(1)
		go func(rs *srvtopo.ResolvedShard, i int) {
			defer wg.Done()
			oneShard(rs, i)
		}(rs, i)
	}
	wg.Wait()

end:
	if session.MustRollback() {
		stc.txConn.Rollback(ctx, session)
	}
	return allErrors
}

// transactionInfo looks at the current session, and returns:
// - shouldBegin: if we should call 'Begin' to get a transactionID
// - transactionID: the transactionID to use, or 0 if not in a transaction.
func transactionInfo(
	target *querypb.Target,
	session *SafeSession,
	notInTransaction bool,
) (shouldBegin bool, transactionID int64) {
	if !session.InTransaction() {
		return false, 0
	}
	// No need to protect ourselves from the race condition between
	// Find and Append. The higher level functions ensure that no
	// duplicate (target) tuples can execute
	// this at the same time.
	transactionID = session.Find(target.Keyspace, target.Shard, target.TabletType)
	if transactionID != 0 {
		return false, transactionID
	}
	// We are in a transaction at higher level,
	// but client requires not to start a transaction for this query.
	// If a transaction was started on this conn, we will use it (as above).
	if notInTransaction {
		return false, 0
	}

	return true, 0
}
