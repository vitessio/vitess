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
	"context"
	"fmt"
	"net/http"
	"time"

	"vitess.io/vitess/go/cache"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/vtgate/planbuilder"
	"vitess.io/vitess/go/vt/vtgate/vschemaacl"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

/*
This file is a temporary construct, to allow us to have both the legacy `execute()` method and the new version
that uses plans for most things side by side. It should be removed
*/

var _ executeMethod = (*planExecute)(nil)

type planExecute struct {
	e *Executor
}

// NewTestExecutor is only meant to be used from tests. It allows test code to inject wich `execute()` method to use
func NewTestExecutor(ctx context.Context, strat func(executor *Executor) executeMethod, serv srvtopo.Server, cell string, resolver *Resolver, normalize bool, streamSize int, queryPlanCacheSize int64) *Executor {
	e := &Executor{
		serv:        serv,
		cell:        cell,
		resolver:    resolver,
		scatterConn: resolver.scatterConn,
		txConn:      resolver.scatterConn.txConn,
		plans:       cache.NewLRUCache(queryPlanCacheSize),
		normalize:   normalize,
		streamSize:  streamSize,
	}

	e.exec = strat(e)

	vschemaacl.Init()
	e.vm = &VSchemaManager{e: e}
	e.vm.watchSrvVSchema(ctx, cell)

	executorOnce.Do(func() {
		stats.NewGaugeFunc("QueryPlanCacheLength", "Query plan cache length", e.plans.Length)
		stats.NewGaugeFunc("QueryPlanCacheSize", "Query plan cache size", e.plans.Size)
		stats.NewGaugeFunc("QueryPlanCacheCapacity", "Query plan cache capacity", e.plans.Capacity)
		stats.NewCounterFunc("QueryPlanCacheEvictions", "Query plan cache evictions", e.plans.Evictions)
		stats.Publish("QueryPlanCacheOldest", stats.StringFunc(func() string {
			return fmt.Sprintf("%v", e.plans.Oldest())
		}))
		http.Handle(pathQueryPlans, e)
		http.Handle(pathScatterStats, e)
		http.Handle(pathVSchema, e)
	})
	return e
}

func (e *planExecute) execute(ctx context.Context, safeSession *SafeSession, sql string, bindVars map[string]*querypb.BindVariable, logStats *LogStats) (*sqltypes.Result, error) {
	// 1: Prepare before planning and execution

	// Start an implicit transaction if necessary.
	err := e.startTxIfNecessary(ctx, safeSession)
	if err != nil {
		return nil, err
	}

	if bindVars == nil {
		bindVars = make(map[string]*querypb.BindVariable)
	}

	query, comments := sqlparser.SplitMarginComments(sql)
	vcursor, err := newVCursorImpl(ctx, safeSession, comments, e.e, logStats, e.e.vm, e.e.resolver.resolver)
	if err != nil {
		return nil, err
	}

	// 2: Create a plan for the query
	plan, err := e.e.getPlan(
		vcursor,
		query,
		comments,
		bindVars,
		skipQueryPlanCache(safeSession),
		logStats,
	)
	if err == planbuilder.ErrPlanNotSupported {
		return nil, err
	}
	execStart := e.logPlanningFinished(logStats, sql)

	if err != nil {
		safeSession.ClearWarnings()
		return nil, err
	}

	if plan.Type != sqlparser.StmtShow {
		safeSession.ClearWarnings()
	}

	// We need to explicitly handle errors, and begin/commit/rollback, since these control transactions. Everything else
	// will fall through and be handled through planning
	switch plan.Type {
	case sqlparser.StmtBegin:
		return e.e.handleBegin(ctx, safeSession, vcursor.tabletType, logStats)
	case sqlparser.StmtCommit:
		return e.e.handleCommit(ctx, safeSession, logStats)
	case sqlparser.StmtRollback:
		return e.e.handleRollback(ctx, safeSession, logStats)
	}

	// 3: Prepare for execution
	err = e.e.addNeededBindVars(plan.BindVarNeeds, bindVars, safeSession)
	if err != nil {
		logStats.Error = err
		return nil, err
	}

	if plan.Instructions.NeedsTransaction() {
		return e.insideTransaction(ctx, safeSession, logStats,
			e.executePlan(ctx, plan, vcursor, bindVars, execStart))
	}

	return e.executePlan(ctx, plan, vcursor, bindVars, execStart)(logStats, safeSession)
}

func (e *planExecute) startTxIfNecessary(ctx context.Context, safeSession *SafeSession) error {
	if !safeSession.Autocommit && !safeSession.InTransaction() {
		if err := e.e.txConn.Begin(ctx, safeSession); err != nil {
			return err
		}
	}
	return nil
}

func (e *planExecute) insideTransaction(ctx context.Context, safeSession *SafeSession, logStats *LogStats, f currFunc) (*sqltypes.Result, error) {
	mustCommit := false
	if safeSession.Autocommit && !safeSession.InTransaction() {
		mustCommit = true
		if err := e.e.txConn.Begin(ctx, safeSession); err != nil {
			return nil, err
		}
	}

	// The SetAutocommitable flag should be same as mustCommit.
	// If we started a transaction because of autocommit, then mustCommit
	// will be true, which means that we can autocommit. If we were already
	// in a transaction, it means that the app started it, or we are being
	// called recursively. If so, we cannot autocommit because whatever we
	// do is likely not final.
	// The control flow is such that autocommitable can only be turned on
	// at the beginning, but never after.
	safeSession.SetAutocommittable(mustCommit)

	// Execute!
	result, err := f(logStats, safeSession)
	if err != nil {
		return nil, err
	}

	if mustCommit {
		// The defer acts as a failsafe. If commit was successful,
		// the rollback will be a no-op.
		defer e.e.txConn.Rollback(ctx, safeSession)

		commitStart := time.Now()
		if err := e.e.txConn.Commit(ctx, safeSession); err != nil {
			return nil, err
		}
		logStats.CommitTime = time.Since(commitStart)
	}
	return result, nil
}

type currFunc func(*LogStats, *SafeSession) (*sqltypes.Result, error)

func (e *planExecute) executePlan(ctx context.Context, plan *engine.Plan, vcursor *vcursorImpl, bindVars map[string]*querypb.BindVariable, execStart time.Time) currFunc {
	return func(logStats *LogStats, safeSession *SafeSession) (*sqltypes.Result, error) {
		// 4: Execute!
		qr, err := plan.Instructions.Execute(vcursor, bindVars, true)
		if err == nil && qr != nil && qr.InsertID > 0 {
			safeSession.LastInsertId = qr.InsertID
		}

		// 5: Log and add statistics
		errCount := e.logExecutionEnd(logStats, execStart, plan, err, qr)
		plan.AddStats(1, time.Since(logStats.StartTime), uint64(logStats.ShardQueries), logStats.RowsAffected, errCount)

		// Check if there was partial DML execution. If so, rollback the transaction.
		if err != nil && safeSession.InTransaction() && vcursor.rollbackOnPartialExec {
			_ = e.e.txConn.Rollback(ctx, safeSession)
			err = vterrors.Errorf(vtrpcpb.Code_ABORTED, "transaction rolled back due to partial DML execution: %v", err)
		}
		return qr, err
	}
}

func (e *planExecute) logExecutionEnd(logStats *LogStats, execStart time.Time, plan *engine.Plan, err error, qr *sqltypes.Result) uint64 {
	logStats.ExecuteTime = time.Since(execStart)

	e.e.updateQueryCounts("Execute", plan.Instructions.RouteType(), plan.Instructions.GetKeyspaceName(), plan.Instructions.GetTableName(), int64(logStats.ShardQueries))

	var errCount uint64
	if err != nil {
		logStats.Error = err
		errCount = 1
	} else {
		logStats.RowsAffected = qr.RowsAffected
	}
	return errCount
}

func (e *planExecute) logPlanningFinished(logStats *LogStats, sql string) time.Time {
	execStart := time.Now()
	logStats.StmtType = sqlparser.Preview(sql).String()
	logStats.PlanTime = execStart.Sub(logStats.StartTime)
	return execStart
}
