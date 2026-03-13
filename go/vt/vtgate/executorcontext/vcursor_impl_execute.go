/*
Copyright 2026 The Vitess Authors.

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

package executorcontext

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/google/uuid"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/key"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/buffer"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/logstats"
)

// MaxBufferingRetries is to represent max retries on buffering.
const MaxBufferingRetries = 3

func (vc *VCursorImpl) CloneForMirroring(ctx context.Context) engine.VCursor {
	callerId := callerid.EffectiveCallerIDFromContext(ctx)
	immediateCallerId := callerid.ImmediateCallerIDFromContext(ctx)

	clonedCtx := callerid.NewContext(ctx, callerId, immediateCallerId)

	v := &VCursorImpl{
		config:         vc.config,
		SafeSession:    NewAutocommitSession(vc.SafeSession.Session),
		keyspace:       vc.keyspace,
		tabletType:     vc.tabletType,
		destination:    vc.destination,
		marginComments: vc.marginComments,
		executor:       vc.executor,
		resolver:       vc.resolver,
		topoServer:     vc.topoServer,
		logStats:       &logstats.LogStats{Ctx: clonedCtx},
		metrics:        vc.metrics,

		ignoreMaxMemoryRows: vc.ignoreMaxMemoryRows,
		vschema:             vc.vschema,
		vm:                  vc.vm,
		semTable:            vc.semTable,
		warnings:            vc.warnings,
		observer:            vc.observer,
	}

	v.marginComments.Trailing += "/* mirror query */"

	return v
}

func (vc *VCursorImpl) CloneForReplicaWarming(ctx context.Context) engine.VCursor {
	callerId := callerid.EffectiveCallerIDFromContext(ctx)
	immediateCallerId := callerid.ImmediateCallerIDFromContext(ctx)

	timedCtx, _ := context.WithTimeout(context.Background(), vc.config.WarmingReadsTimeout) //nolint
	clonedCtx := callerid.NewContext(timedCtx, callerId, immediateCallerId)

	v := &VCursorImpl{
		config:         vc.config,
		SafeSession:    NewAutocommitSession(vc.SafeSession.Session),
		keyspace:       vc.keyspace,
		tabletType:     topodatapb.TabletType_REPLICA,
		destination:    vc.destination,
		marginComments: vc.marginComments,
		executor:       vc.executor,
		resolver:       vc.resolver,
		topoServer:     vc.topoServer,
		logStats:       &logstats.LogStats{Ctx: clonedCtx},
		metrics:        vc.metrics,

		ignoreMaxMemoryRows: vc.ignoreMaxMemoryRows,
		vschema:             vc.vschema,
		vm:                  vc.vm,
		semTable:            vc.semTable,
		warnings:            vc.warnings,
		observer:            vc.observer,
	}

	v.marginComments.Trailing += "/* warming read */"

	return v
}

func (vc *VCursorImpl) cloneWithAutocommitSession() *VCursorImpl {
	safeSession := vc.SafeSession.NewAutocommitSession()
	return &VCursorImpl{
		config:         vc.config,
		SafeSession:    safeSession,
		keyspace:       vc.keyspace,
		tabletType:     vc.tabletType,
		destination:    vc.destination,
		marginComments: vc.marginComments,
		executor:       vc.executor,
		logStats:       vc.logStats,
		metrics:        vc.metrics,

		resolver:   vc.resolver,
		vschema:    vc.vschema,
		vm:         vc.vm,
		topoServer: vc.topoServer,
		observer:   vc.observer,
	}
}

func (vc *VCursorImpl) ExecutePrimitive(ctx context.Context, primitive engine.Primitive, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	for range MaxBufferingRetries {
		res, err := primitive.TryExecute(ctx, vc, bindVars, wantfields)
		if err != nil && vterrors.RootCause(err) == buffer.ShardMissingError {
			continue
		}
		vc.logOpTraffic(primitive, res)
		if res != nil && res.InsertIDUpdated() {
			vc.SafeSession.LastInsertId = res.InsertID
		}
		return res, err
	}
	return nil, vterrors.New(vtrpcpb.Code_UNAVAILABLE, "upstream shards are not available")
}

func (vc *VCursorImpl) logOpTraffic(primitive engine.Primitive, res *sqltypes.Result) {
	if vc.interOpStats == nil {
		return
	}

	vc.mu.Lock()
	defer vc.mu.Unlock()

	rows := vc.interOpStats[primitive]
	if res == nil {
		rows = append(rows, 0)
	} else {
		rows = append(rows, len(res.Rows))
	}
	vc.interOpStats[primitive] = rows
}

func (vc *VCursorImpl) logShardsQueried(primitive engine.Primitive, shardsNb int) {
	if vc.shardsStats == nil {
		return
	}
	vc.mu.Lock()
	defer vc.mu.Unlock()
	vc.shardsStats[primitive] += engine.ShardsQueried(shardsNb)
}

func (vc *VCursorImpl) ExecutePrimitiveStandalone(ctx context.Context, primitive engine.Primitive, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	// clone the VCursorImpl with a new session.
	newVC := vc.cloneWithAutocommitSession()
	for range MaxBufferingRetries {
		res, err := primitive.TryExecute(ctx, newVC, bindVars, wantfields)
		if err != nil && vterrors.RootCause(err) == buffer.ShardMissingError {
			continue
		}
		vc.logOpTraffic(primitive, res)
		return res, err
	}
	return nil, vterrors.New(vtrpcpb.Code_UNAVAILABLE, "upstream shards are not available")
}

func (vc *VCursorImpl) wrapCallback(callback func(*sqltypes.Result) error, primitive engine.Primitive) func(*sqltypes.Result) error {
	if vc.interOpStats == nil {
		return func(r *sqltypes.Result) error {
			if r.InsertIDUpdated() {
				vc.SafeSession.LastInsertId = r.InsertID
			}
			return callback(r)
		}
	}

	return func(r *sqltypes.Result) error {
		if r.InsertIDUpdated() {
			vc.SafeSession.LastInsertId = r.InsertID
		}
		vc.logOpTraffic(primitive, r)
		return callback(r)
	}
}

func (vc *VCursorImpl) StreamExecutePrimitive(ctx context.Context, primitive engine.Primitive, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	callback = vc.wrapCallback(callback, primitive)

	for range MaxBufferingRetries {
		err := primitive.TryStreamExecute(ctx, vc, bindVars, wantfields, callback)
		if err != nil && vterrors.RootCause(err) == buffer.ShardMissingError {
			continue
		}
		return err
	}
	return vterrors.New(vtrpcpb.Code_UNAVAILABLE, "upstream shards are not available")
}

func (vc *VCursorImpl) StreamExecutePrimitiveStandalone(ctx context.Context, primitive engine.Primitive, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(result *sqltypes.Result) error) error {
	callback = vc.wrapCallback(callback, primitive)

	// clone the VCursorImpl with a new session.
	newVC := vc.cloneWithAutocommitSession()
	for range MaxBufferingRetries {
		err := primitive.TryStreamExecute(ctx, newVC, bindVars, wantfields, callback)
		if err != nil && vterrors.RootCause(err) == buffer.ShardMissingError {
			continue
		}
		return err
	}
	return vterrors.New(vtrpcpb.Code_UNAVAILABLE, "upstream shards are not available")
}

// Execute is part of the engine.VCursor interface.
func (vc *VCursorImpl) Execute(ctx context.Context, method string, query string, bindVars map[string]*querypb.BindVariable, rollbackOnError bool, co vtgatepb.CommitOrder) (*sqltypes.Result, error) {
	session := vc.SafeSession
	if co == vtgatepb.CommitOrder_AUTOCOMMIT {
		// For autocommit, we have to create an independent session.
		session = vc.SafeSession.NewAutocommitSession()
		rollbackOnError = false
	} else {
		session.SetCommitOrder(co)
		defer session.SetCommitOrder(vtgatepb.CommitOrder_NORMAL)
	}

	err := vc.markSavepoint(ctx, rollbackOnError, map[string]*querypb.BindVariable{})
	if err != nil {
		return nil, err
	}

	qr, err := vc.executor.Execute(ctx, nil, method, session, vc.marginComments.Leading+query+vc.marginComments.Trailing, bindVars, false)
	// If there is no error, it indicates at least one successful execution,
	// meaning a rollback should be triggered if a failure occurs later.
	vc.setRollbackOnPartialExecIfRequired(err == nil, rollbackOnError)

	return qr, err
}

// markSavepoint opens an internal savepoint before executing the original query.
// This happens only when rollback is allowed and no other savepoint was executed
// and the query is executed in an explicit transaction (i.e. started by the client).
func (vc *VCursorImpl) markSavepoint(ctx context.Context, needsRollbackOnParialExec bool, bindVars map[string]*querypb.BindVariable) error {
	if !needsRollbackOnParialExec || !vc.SafeSession.CanAddSavepoint() {
		return nil
	}
	uID := "_vt" + strings.ReplaceAll(uuid.NewString(), "-", "_")
	spQuery := fmt.Sprintf("%ssavepoint %s%s", vc.marginComments.Leading, uID, vc.marginComments.Trailing)
	vc.SafeSession.SetExecReadQuery(true)
	_, err := vc.executor.Execute(ctx, nil, "MarkSavepoint", vc.SafeSession, spQuery, bindVars, false)
	if err != nil {
		return err
	}
	vc.SafeSession.SetSavepoint(uID)
	vc.SafeSession.SetExecReadQuery(false)
	return nil
}

// ExecuteMultiShard is part of the engine.VCursor interface.
func (vc *VCursorImpl) ExecuteMultiShard(ctx context.Context, primitive engine.Primitive, rss []*srvtopo.ResolvedShard, queries []*querypb.BoundQuery, rollbackOnError, canAutocommit, fetchLastInsertID bool) (*sqltypes.Result, []error) {
	noOfShards := len(rss)
	atomic.AddUint64(&vc.logStats.ShardQueries, uint64(noOfShards))
	err := vc.markSavepoint(ctx, rollbackOnError && (noOfShards > 1), map[string]*querypb.BindVariable{})
	if err != nil {
		return nil, []error{err}
	}

	qr, errs := vc.executor.ExecuteMultiShard(ctx, primitive, rss, commentedShardQueries(queries, vc.marginComments), vc.SafeSession, canAutocommit, vc.ignoreMaxMemoryRows, vc.observer, fetchLastInsertID)
	vc.setRollbackOnPartialExecIfRequired(len(errs) != len(rss), rollbackOnError)
	vc.logShardsQueried(primitive, len(rss))
	if qr != nil && qr.InsertIDUpdated() {
		vc.SafeSession.LastInsertId = qr.InsertID
	}
	return qr, errs
}

// StreamExecuteMulti is the streaming version of ExecuteMultiShard.
func (vc *VCursorImpl) StreamExecuteMulti(ctx context.Context, primitive engine.Primitive, query string, rss []*srvtopo.ResolvedShard, bindVars []map[string]*querypb.BindVariable, rollbackOnError, autocommit, fetchLastInsertID bool, callback func(reply *sqltypes.Result) error) []error {
	callback = vc.wrapCallback(callback, primitive)

	noOfShards := len(rss)
	atomic.AddUint64(&vc.logStats.ShardQueries, uint64(noOfShards))
	err := vc.markSavepoint(ctx, rollbackOnError && (noOfShards > 1), map[string]*querypb.BindVariable{})
	if err != nil {
		return []error{err}
	}

	errs := vc.executor.StreamExecuteMulti(ctx, primitive, vc.marginComments.Leading+query+vc.marginComments.Trailing, rss, bindVars, vc.SafeSession, autocommit, callback, vc.observer, fetchLastInsertID)
	vc.setRollbackOnPartialExecIfRequired(len(errs) != len(rss), rollbackOnError)

	return errs
}

// ExecuteLock is for executing advisory lock statements.
func (vc *VCursorImpl) ExecuteLock(ctx context.Context, rs *srvtopo.ResolvedShard, query *querypb.BoundQuery, lockFuncType sqlparser.LockingFuncType) (*sqltypes.Result, error) {
	query.Sql = vc.marginComments.Leading + query.Sql + vc.marginComments.Trailing
	return vc.executor.ExecuteLock(ctx, rs, query, vc.SafeSession, lockFuncType)
}

// ExecuteStandalone is part of the engine.VCursor interface.
func (vc *VCursorImpl) ExecuteStandalone(ctx context.Context, primitive engine.Primitive, query string, bindVars map[string]*querypb.BindVariable, rs *srvtopo.ResolvedShard, fetchLastInsertID bool) (*sqltypes.Result, error) {
	rss := []*srvtopo.ResolvedShard{rs}
	bqs := []*querypb.BoundQuery{
		{
			Sql:           vc.marginComments.Leading + query + vc.marginComments.Trailing,
			BindVariables: bindVars,
		},
	}
	// The autocommit flag is always set to false because we currently don't
	// execute DMLs through ExecuteStandalone.
	qr, errs := vc.executor.ExecuteMultiShard(ctx, primitive, rss, bqs, NewAutocommitSession(vc.SafeSession.Session), false /* autocommit */, vc.ignoreMaxMemoryRows, vc.observer, fetchLastInsertID)
	vc.logShardsQueried(primitive, len(rss))
	if qr.InsertIDUpdated() {
		vc.SafeSession.LastInsertId = qr.InsertID
	}
	return qr, vterrors.Aggregate(errs)
}

// ExecuteKeyspaceID is part of the engine.VCursor interface.
func (vc *VCursorImpl) ExecuteKeyspaceID(ctx context.Context, keyspace string, ksid []byte, query string, bindVars map[string]*querypb.BindVariable, rollbackOnError, autocommit bool) (*sqltypes.Result, error) {
	atomic.AddUint64(&vc.logStats.ShardQueries, 1)
	rss, _, err := vc.ResolveDestinations(ctx, keyspace, nil, []key.ShardDestination{key.DestinationKeyspaceID(ksid)})
	if err != nil {
		return nil, err
	}
	queries := []*querypb.BoundQuery{{
		Sql:           query,
		BindVariables: bindVars,
	}}

	// This applies only when VTGate works in SINGLE transaction_mode.
	// This function is only called from consistent_lookup vindex when the lookup row getting inserting finds a duplicate.
	// In such scenario, original row needs to be locked to check if it already exists or no other transaction is working on it or does not write to it.
	// This creates a transaction but that transaction is for locking purpose only and should not cause multi-db transaction error.
	// This fields helps in to ignore multi-db transaction error when it states `execReadQuery`.
	if !rollbackOnError {
		vc.SafeSession.SetExecReadQuery(true)
		defer func() {
			vc.SafeSession.SetExecReadQuery(false)
		}()
	}
	qr, errs := vc.ExecuteMultiShard(ctx, nil, rss, queries, rollbackOnError, autocommit, false)
	return qr, vterrors.Aggregate(errs)
}

func (vc *VCursorImpl) InTransactionAndIsDML() bool {
	if !vc.SafeSession.InTransaction() {
		return false
	}
	switch vc.logStats.StmtType {
	case "INSERT", "REPLACE", "UPDATE", "DELETE":
		return true
	}
	return false
}

func (vc *VCursorImpl) LookupRowLockShardSession() vtgatepb.CommitOrder {
	switch vc.logStats.StmtType {
	case "DELETE", "UPDATE":
		return vtgatepb.CommitOrder_POST
	}
	return vtgatepb.CommitOrder_PRE
}

// AutocommitApproval is part of the engine.VCursor interface.
func (vc *VCursorImpl) AutocommitApproval() bool {
	return vc.SafeSession.AutocommitApproval()
}

// setRollbackOnPartialExecIfRequired sets the value on SafeSession.rollbackOnPartialExec
// when the query gets successfully executed on at least one shard,
// there does not exist any old savepoint for which rollback is already set
// and rollback on error is allowed.
func (vc *VCursorImpl) setRollbackOnPartialExecIfRequired(atleastOneSuccess bool, rollbackOnError bool) {
	if atleastOneSuccess && rollbackOnError && !vc.SafeSession.IsRollbackSet() {
		vc.SafeSession.SetRollbackCommand()
	}
}

// fixupPartiallyMovedShards checks if any of the shards in the route has a ShardRoutingRule (true when a keyspace
// is in the middle of being moved to another keyspace using MoveTables moving a subset of shards at a time
func (vc *VCursorImpl) fixupPartiallyMovedShards(rss []*srvtopo.ResolvedShard) ([]*srvtopo.ResolvedShard, error) {
	if vc.vschema.ShardRoutingRules == nil {
		return rss, nil
	}
	for ind, rs := range rss {
		targetKeyspace, err := vc.FindRoutedShard(rs.Target.Keyspace, rs.Target.Shard)
		if err != nil {
			return nil, err
		}
		if targetKeyspace == rs.Target.Keyspace {
			continue
		}
		rss[ind] = rs.WithKeyspace(targetKeyspace)
	}
	return rss, nil
}

func (vc *VCursorImpl) ResolveDestinations(ctx context.Context, keyspace string, ids []*querypb.Value, destinations []key.ShardDestination) ([]*srvtopo.ResolvedShard, [][]*querypb.Value, error) {
	rss, values, err := vc.resolver.ResolveDestinations(ctx, keyspace, vc.tabletType, ids, destinations)
	if err != nil {
		return nil, nil, err
	}
	if vc.config.EnableShardRouting {
		rss, err = vc.fixupPartiallyMovedShards(rss)
		if err != nil {
			return nil, nil, err
		}
	}
	return rss, values, err
}

func (vc *VCursorImpl) ResolveDestinationsMultiCol(ctx context.Context, keyspace string, ids [][]sqltypes.Value, destinations []key.ShardDestination) ([]*srvtopo.ResolvedShard, [][][]sqltypes.Value, error) {
	rss, values, err := vc.resolver.ResolveDestinationsMultiCol(ctx, keyspace, vc.tabletType, ids, destinations)
	if err != nil {
		return nil, nil, err
	}
	if vc.config.EnableShardRouting {
		rss, err = vc.fixupPartiallyMovedShards(rss)
		if err != nil {
			return nil, nil, err
		}
	}
	return rss, values, err
}

func commentedShardQueries(shardQueries []*querypb.BoundQuery, marginComments sqlparser.MarginComments) []*querypb.BoundQuery {
	if marginComments.Leading == "" && marginComments.Trailing == "" {
		return shardQueries
	}
	newQueries := make([]*querypb.BoundQuery, len(shardQueries))
	for i, v := range shardQueries {
		newQueries[i] = &querypb.BoundQuery{
			Sql:           marginComments.Leading + v.Sql + marginComments.Trailing,
			BindVariables: v.BindVariables,
		}
	}
	return newQueries
}

func (vc *VCursorImpl) StartPrimitiveTrace() func() engine.Stats {
	vc.interOpStats = make(map[engine.Primitive]engine.RowsReceived)
	vc.shardsStats = make(map[engine.Primitive]engine.ShardsQueried)
	return func() engine.Stats {
		return engine.Stats{
			InterOpStats: vc.interOpStats,
			ShardsStats:  vc.shardsStats,
		}
	}
}

func (vc *VCursorImpl) GetContextWithTimeOut(ctx context.Context) (context.Context, context.CancelFunc) {
	if vc.queryTimeout == 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, vc.queryTimeout)
}
