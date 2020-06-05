/*
Copyright 2019 The Vitess Authors.

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
	"sync"
	"time"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"

	"vitess.io/vitess/go/vt/vttablet/queryservice"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/discovery"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"
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
	gateway              Gateway
	legacyHealthCheck    discovery.LegacyHealthCheck
}

// shardActionFunc defines the contract for a shard action
// outside of a transaction. Every such function executes the
// necessary action on a shard, sends the results to sResults, and
// return an error if any.  multiGo is capable of executing
// multiple shardActionFunc actions in parallel and
// consolidating the results and errors for the caller.
type shardActionFunc func(rs *srvtopo.ResolvedShard, i int) error

// NewLegacyScatterConn creates a new ScatterConn.
func NewLegacyScatterConn(statsName string, txConn *TxConn, gw Gateway, hc discovery.LegacyHealthCheck) *ScatterConn {
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
		txConn:            txConn,
		gateway:           gw,
		legacyHealthCheck: hc,
	}
}

// NewScatterConn creates a new ScatterConn.
func NewScatterConn(statsName string, txConn *TxConn, gw *TabletGateway) *ScatterConn {
	// this only works with TabletGateway
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
		txConn:  txConn,
		gateway: gw,
		// gateway has a reference to healthCheck so we don't need this any more
		legacyHealthCheck: nil,
	}
}

func (stc *ScatterConn) startAction(name string, target *querypb.Target) (time.Time, []string) {
	statsKey := []string{name, target.Keyspace, target.Shard, topoproto.TabletTypeLString(target.TabletType)}
	startTime := time.Now()
	return startTime, statsKey
}

func (stc *ScatterConn) handleError(err error, statsKey []string, session *SafeSession) {
	if err != nil {
		// Don't increment the error counter for duplicate
		// keys or bad queries, as those errors are caused by
		// client queries and are not VTGate's fault.
		ec := vterrors.Code(err)
		if ec != vtrpcpb.Code_ALREADY_EXISTS && ec != vtrpcpb.Code_INVALID_ARGUMENT {
			stc.tabletCallErrorCount.Add(statsKey, 1)
		}
		if ec == vtrpcpb.Code_RESOURCE_EXHAUSTED || ec == vtrpcpb.Code_ABORTED {
			session.SetRollback()
		}
	}
}

// Execute executes a non-streaming query on the specified shards.
func (stc *ScatterConn) Execute(
	ctx context.Context,
	query string,
	bindVars map[string]*querypb.BindVariable,
	rss []*srvtopo.ResolvedShard,
	session *SafeSession,
	notInTransaction bool,
	options *querypb.ExecuteOptions,
	autocommit bool,
) (*sqltypes.Result, error) {
	qf := func(int) (string, map[string]*querypb.BindVariable) {
		return query, bindVars
	}
	result, errors := stc.multiGoTxOrReserve(ctx, "Execute", rss, session, options, notInTransaction, autocommit, false, qf)
	if len(errors) > 0 {
		return nil, vterrors.Aggregate(errors)
	}
	return result, nil
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
	session *SafeSession,
	notInTransaction bool,
	autocommit bool,
) (*sqltypes.Result, []error) {

	qf := func(idx int) (string, map[string]*querypb.BindVariable) {
		bq := queries[idx]
		return bq.Sql, bq.BindVariables
	}
	var opts *querypb.ExecuteOptions
	if session != nil && session.Session != nil {
		opts = session.Session.Options
	}

	return stc.multiGoTxOrReserve(ctx, "ExecuteMultiShard", rss, session, opts, notInTransaction, autocommit, false, qf)
}

func (stc *ScatterConn) executeOne(
	ctx context.Context,
	rs *srvtopo.ResolvedShard,
	info tabletConnInfo,
	query string,
	bv map[string]*querypb.BindVariable,
	autocommit bool,
	session *SafeSession,
	opts *querypb.ExecuteOptions,
) (int64, int64, *topodatapb.TabletAlias, *sqltypes.Result, error) {
	switch {
	case autocommit:
		innerqr, err := stc.executeAutocommit(ctx, rs, query, bv, opts)
		if err != nil {
			return 0, 0, nil, nil, err
		}
		return 0, 0, nil, innerqr, nil
	case info.shouldBegin && info.shouldReserve:
		innerqr, transactionID, reservedID, alias, err := rs.Gateway.ReserveBeginExecute(ctx, rs.Target, query, bv, opts, session.SetSystemVarQueries())
		if err != nil {
			return 0, 0, nil, nil, err
		}
		return transactionID, reservedID, alias, innerqr, nil
	case info.shouldBegin:
		innerqr, transactionID, alias, err := rs.Gateway.BeginExecute(ctx, rs.Target, query, bv, info.reserveID, opts)
		if err != nil {
			return 0, 0, nil, nil, err
		}
		return transactionID, info.reserveID, alias, innerqr, nil
	case info.shouldReserve:
		innerqr, reservedID, alias, err := rs.Gateway.ReserveExecute(ctx, rs.Target, query, bv, info.transactionID, opts, session.SetSystemVarQueries())
		if err != nil {
			return 0, 0, nil, nil, err
		}
		return info.transactionID, reservedID, alias, innerqr, nil
	default:
		var qs queryservice.QueryService
		_, usingLegacy := rs.Gateway.(*DiscoveryGateway)
		if info.transactionID != 0 && usingLegacy && rs.Target.TabletType != topodatapb.TabletType_MASTER {
			return 0, 0, nil, nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "replica transactions not supported using the legacy healthcheck")
		}

		if usingLegacy || info.transactionID == 0 {
			qs = rs.Gateway
		} else {
			var err error
			qs, err = rs.Gateway.QueryServiceByAlias(info.alias)
			if err != nil {
				return 0, 0, nil, nil, err
			}
		}
		innerqr, err := qs.Execute(ctx, rs.Target, query, bv, info.transactionID, info.reserveID, opts)
		return info.transactionID, info.reserveID, info.alias, innerqr, err
	}
}

func (stc *ScatterConn) executeAutocommit(ctx context.Context, rs *srvtopo.ResolvedShard, sql string, bindVariables map[string]*querypb.BindVariable, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	queries := []*querypb.BoundQuery{{
		Sql:           sql,
		BindVariables: bindVariables,
	}}
	// ExecuteBatch is a stop-gap because it's the only function that can currently do
	// single round-trip commit.
	qrs, err := rs.Gateway.ExecuteBatch(ctx, rs.Target, queries, true /* asTransaction */, 0, options)
	if err != nil {
		return nil, err
	}
	return &qrs[0], nil
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
// by multiple go routines, through processOneStreamingResult.
func (stc *ScatterConn) StreamExecute(
	ctx context.Context,
	query string,
	bindVars map[string]*querypb.BindVariable,
	rss []*srvtopo.ResolvedShard,
	options *querypb.ExecuteOptions,
	callback func(reply *sqltypes.Result) error,
) error {

	// mu protects fieldSent, replyErr and callback
	var mu sync.Mutex
	fieldSent := false

	allErrors := stc.multiGo("StreamExecute", rss, func(rs *srvtopo.ResolvedShard, i int) error {
		return rs.Gateway.StreamExecute(ctx, rs.Target, query, bindVars, 0, options, func(qr *sqltypes.Result) error {
			return stc.processOneStreamingResult(&mu, &fieldSent, qr, callback)
		})
	})
	return allErrors.AggrError(vterrors.Aggregate)
}

// StreamExecuteMulti is like StreamExecute,
// but each shard gets its own bindVars. If len(shards) is not equal to
// len(bindVars), the function panics.
// Note we guarantee the callback will not be called concurrently
// by multiple go routines, through processOneStreamingResult.
func (stc *ScatterConn) StreamExecuteMulti(
	ctx context.Context,
	query string,
	rss []*srvtopo.ResolvedShard,
	bindVars []map[string]*querypb.BindVariable,
	options *querypb.ExecuteOptions,
	callback func(reply *sqltypes.Result) error,
) error {
	// mu protects fieldSent, callback and replyErr
	var mu sync.Mutex
	fieldSent := false

	allErrors := stc.multiGo("StreamExecute", rss, func(rs *srvtopo.ResolvedShard, i int) error {
		return rs.Gateway.StreamExecute(ctx, rs.Target, query, bindVars[i], 0, options, func(qr *sqltypes.Result) error {
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
// by multiple go routines, through processOneStreamingResult.
func (stc *ScatterConn) MessageStream(ctx context.Context, rss []*srvtopo.ResolvedShard, name string, callback func(*sqltypes.Result) error) error {
	// The cancelable context is used for handling errors
	// from individual streams.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// mu is used to merge multiple callback calls into one.
	var mu sync.Mutex
	fieldSent := false
	lastErrors := newTimeTracker()
	allErrors := stc.multiGo("MessageStream", rss, func(rs *srvtopo.ResolvedShard, i int) error {
		// This loop handles the case where a reparent happens, which can cause
		// an individual stream to end. If we don't succeed on the retries for
		// messageStreamGracePeriod, we abort and return an error.
		for {
			err := rs.Gateway.MessageStream(ctx, rs.Target, name, func(qr *sqltypes.Result) error {
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

// Close closes the underlying Gateway.
func (stc *ScatterConn) Close() error {
	return stc.gateway.Close(context.Background())
}

// GetGatewayCacheStatus returns a displayable version of the Gateway cache.
func (stc *ScatterConn) GetGatewayCacheStatus() TabletCacheStatusList {
	return stc.gateway.CacheStatus()
}

// GetLegacyHealthCheckCacheStatus returns a displayable version of the HealthCheck cache.
func (stc *ScatterConn) GetLegacyHealthCheckCacheStatus() discovery.LegacyTabletsCacheStatusList {
	if stc.legacyHealthCheck != nil {
		return stc.legacyHealthCheck.CacheStatus()
	}
	return nil
}

// GetHealthCheckCacheStatus returns a displayable version of the HealthCheck cache.
func (stc *ScatterConn) GetHealthCheckCacheStatus() discovery.TabletsCacheStatusList {
	if UsingLegacyGateway() {
		panic("this should never be called")
	}

	return stc.gateway.TabletsCacheStatus()
}

// multiGo performs the requested 'action' on the specified
// shards in parallel. This does not handle any transaction state.
// The action function must match the shardActionFunc2 signature.
func (stc *ScatterConn) multiGo(
	name string,
	rss []*srvtopo.ResolvedShard,
	action shardActionFunc,
) (allErrors *concurrency.AllErrorRecorder) {
	allErrors = new(concurrency.AllErrorRecorder)
	if len(rss) == 0 {
		return allErrors
	}

	oneShard := func(rs *srvtopo.ResolvedShard, i int) {
		startTime, statsKey := stc.startAction(name, rs.Target)
		// Send a dummy session.
		defer stc.timings.Record(statsKey, startTime)
		err := action(rs, i)
		if err != nil {
			// TODO(sougou): plumb a real session through this call.
			stc.handleError(err, statsKey, NewSafeSession(nil))
			allErrors.RecordError(err)
		}
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

// multiGoTxOrReserve performs the requested 'action' on the specified
// ResolvedShards in parallel. For each shard, if the requested
// session is in a transaction, it opens a new transactions on the connection,
// and updates the Session with the transaction id. If the session already
// contains a transaction id for the shard, it reuses it.
// The action function must match the shardActionTransactionFunc signature.
//
// It returns an error slice in which each shard error is recorded positionally,
// i.e. if rss[2] had an error, then the error recorder will store that error
// in the second position.
func (stc *ScatterConn) multiGoTxOrReserve(
	ctx context.Context,
	name string,
	rss []*srvtopo.ResolvedShard,
	session *SafeSession,
	options *querypb.ExecuteOptions,
	notInTransaction bool,
	autocommit bool,
	needReserved bool,
	query func(i int) (string, map[string]*querypb.BindVariable),
) (*sqltypes.Result, []error) {

	numShards := len(rss)
	errors := make([]error, numShards)

	if numShards == 0 {
		return &sqltypes.Result{}, nil
	}

	// mu protects qr
	var mu sync.Mutex
	result := new(sqltypes.Result)

	oneShard := func(rs *srvtopo.ResolvedShard, shardIdx int) {
		startTime, statsKey := stc.startAction(name, rs.Target)
		defer stc.timings.Record(statsKey, startTime)

		connInfo := txAndReservedInfo(rs.Target, session, notInTransaction, needReserved)
		query, bindVars := query(shardIdx)
		transactionID, reservedID, alias, innerResult, err := stc.executeOne(ctx, rs, connInfo, query, bindVars, autocommit, session, options)
		if err != nil {
			stc.handleError(err, statsKey, session)
			errors[shardIdx] = err
		}
		if connInfo.shouldBegin || connInfo.shouldReserve && transactionID != 0 {
			appendErr := session.Append(&vtgatepb.Session_ShardSession{
				Target:        rs.Target,
				TransactionId: transactionID,
				ReservedId:    reservedID,
				TabletAlias:   alias,
			}, stc.txConn.mode)
			stc.handleError(appendErr, statsKey, session)
		}

		mu.Lock()
		defer mu.Unlock()
		// Don't append more rows if row count is exceeded.
		if len(result.Rows) <= *maxMemoryRows {
			result.AppendResult(innerResult)
		}
	}

	if numShards == 1 {
		// only one shard, do it synchronously.
		for i, rs := range rss {
			oneShard(rs, i)
		}
	} else {
		var wg sync.WaitGroup
		for i, rs := range rss {
			wg.Add(1)
			go func(rs *srvtopo.ResolvedShard, i int) {
				defer wg.Done()
				oneShard(rs, i)
			}(rs, i)
		}
		wg.Wait()
	}

	if session.MustRollback() {
		stc.txConn.Rollback(ctx, session)
	}
	return result, errors
}

type tabletConnInfo struct {
	shouldBegin   bool
	shouldReserve bool
	transactionID int64
	reserveID     int64
	alias         *topodatapb.TabletAlias
}

// txAndReservedInfo looks at the current session, and returns:
// - shouldBegin: if we should call 'Begin' to get a transactionID
// - transactionID: the transactionID to use, or 0 if not in a transaction.
func txAndReservedInfo(
	target *querypb.Target,
	session *SafeSession,
	notInTransaction bool,
	needReserved bool,
) tabletConnInfo {
	// No need to protect ourselves from the race condition between
	// Find and Append. The higher level functions ensure that no
	// duplicate (target) tuples can execute
	// this at the same time.
	transactionID, reservedID, alias := session.Find(target.Keyspace, target.Shard, target.TabletType)

	shouldReserve := needReserved && reservedID == 0
	var shouldBegin bool

	// We are in a transaction at higher level,
	// but client requires not to start a transaction for this query.
	// If a transaction was started on this conn, we will use it (as above).
	if transactionID != 0 || notInTransaction {
		shouldBegin = false
	} else {
		shouldBegin = !session.InTransaction()
	}
	return tabletConnInfo{
		shouldBegin:   shouldBegin,
		shouldReserve: shouldReserve,
		transactionID: transactionID,
		reserveID:     reservedID,
		alias:         alias}
}
