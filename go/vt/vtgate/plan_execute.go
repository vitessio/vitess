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
	"strings"
	"time"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/logstats"
	"vitess.io/vitess/go/vt/vtgate/vtgateservice"
)

type planExec func(ctx context.Context, plan *engine.Plan, vc *vcursorImpl, bindVars map[string]*querypb.BindVariable, startTime time.Time) error
type txResult func(sqlparser.StatementType, *sqltypes.Result) error

func waitForNewerVSchema(ctx context.Context, e *Executor, lastVSchemaCreated time.Time) bool {
	timeout := 30 * time.Second
	pollingInterval := 10 * time.Millisecond
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	ticker := time.NewTicker(pollingInterval)
	defer ticker.Stop()
	defer cancel()
	for {
		select {
		case <-waitCtx.Done():
			return false
		case <-ticker.C:
			if e.VSchema().GetCreated().After(lastVSchemaCreated) {
				return true
			}
		}
	}
}

func (e *Executor) newExecute(
	ctx context.Context,
	mysqlCtx vtgateservice.MySQLConnection,
	safeSession *SafeSession,
	sql string,
	bindVars map[string]*querypb.BindVariable,
	logStats *logstats.LogStats,
	execPlan planExec, // used when there is a plan to execute
	recResult txResult, // used when it's something simple like begin/commit/rollback/savepoint
) error {
	// 1: Prepare before planning and execution

	// Start an implicit transaction if necessary.
	err := e.startTxIfNecessary(ctx, safeSession)
	if err != nil {
		return err
	}

	if bindVars == nil {
		bindVars = make(map[string]*querypb.BindVariable)
	}

	query, comments := sqlparser.SplitMarginComments(sql)

	// 2: Parse and Validate query
	stmt, reservedVars, err := parseAndValidateQuery(query, e.env.Parser())
	if err != nil {
		return err
	}

	var lastVSchemaCreated time.Time
	vs := e.VSchema()
	lastVSchemaCreated = vs.GetCreated()
	for try := 0; try < MaxBufferingRetries; try++ {
		if try > 0 && !vs.GetCreated().After(lastVSchemaCreated) {
			// There is a race due to which the executor's vschema may not have been updated yet.
			// Without a wait we fail non-deterministically since the previous vschema will not have the updated routing rules
			if waitForNewerVSchema(ctx, e, lastVSchemaCreated) {
				vs = e.VSchema()
			}
		}

		vcursor, err := newVCursorImpl(safeSession, comments, e, logStats, e.vm, vs, e.resolver.resolver, e.serv, e.warnShardedOnly, e.pv)
		if err != nil {
			return err
		}

		// 3: Create a plan for the query
		// If we are retrying, it is likely that the routing rules have changed and hence we need to
		// replan the query since the target keyspace of the resolved shards may have changed as a
		// result of MoveTables. So we cannot reuse the plan from the first try.
		// When buffering ends, many queries might be getting planned at the same time. Ideally we
		// should be able to reuse plans once the first drained query has been planned. For now, we
		// punt on this and choose not to prematurely optimize since it is not clear how much caching
		// will help and if it will result in hard-to-track edge cases.

		var plan *engine.Plan
		plan, err = e.getPlan(ctx, vcursor, query, stmt, comments, bindVars, reservedVars, e.normalize, logStats)
		execStart := e.logPlanningFinished(logStats, plan)

		if err != nil {
			safeSession.ClearWarnings()
			return err
		}

		if plan.Type != sqlparser.StmtShow {
			safeSession.ClearWarnings()
		}

		// add any warnings that the planner wants to add
		for _, warning := range plan.Warnings {
			safeSession.RecordWarning(warning)
		}

		result, err := e.handleTransactions(ctx, mysqlCtx, safeSession, plan, logStats, vcursor, stmt)
		if err != nil {
			return err
		}
		if result != nil {
			return recResult(plan.Type, result)
		}

		// 4: Prepare for execution
		err = e.addNeededBindVars(vcursor, plan.BindVarNeeds, bindVars, safeSession)
		if err != nil {
			logStats.Error = err
			return err
		}

		// 5: Execute the plan and retry if needed
		if plan.Instructions.NeedsTransaction() {
			err = e.insideTransaction(ctx, safeSession, logStats,
				func() error {
					return execPlan(ctx, plan, vcursor, bindVars, execStart)
				})
		} else {
			err = execPlan(ctx, plan, vcursor, bindVars, execStart)
		}

		if err == nil || safeSession.InTransaction() {
			return err
		}

		rootCause := vterrors.RootCause(err)
		if rootCause != nil && strings.Contains(rootCause.Error(), "enforce denied tables") {
			log.V(2).Infof("Retry: %d, will retry query %s due to %v", try, query, err)
			lastVSchemaCreated = vs.GetCreated()
			continue
		}

		return err
	}
	return vterrors.New(vtrpcpb.Code_INTERNAL, fmt.Sprintf("query %s failed after retries: %v ", query, err))
}

// handleTransactions deals with transactional queries: begin, commit, rollback and savepoint management
func (e *Executor) handleTransactions(
	ctx context.Context,
	mysqlCtx vtgateservice.MySQLConnection,
	safeSession *SafeSession,
	plan *engine.Plan,
	logStats *logstats.LogStats,
	vcursor *vcursorImpl,
	stmt sqlparser.Statement,
) (*sqltypes.Result, error) {
	// We need to explicitly handle errors, and begin/commit/rollback, since these control transactions. Everything else
	// will fall through and be handled through planning
	switch plan.Type {
	case sqlparser.StmtBegin:
		qr, err := e.handleBegin(ctx, safeSession, logStats, stmt)
		return qr, err
	case sqlparser.StmtCommit:
		qr, err := e.handleCommit(ctx, safeSession, logStats)
		return qr, err
	case sqlparser.StmtRollback:
		qr, err := e.handleRollback(ctx, safeSession, logStats)
		return qr, err
	case sqlparser.StmtSavepoint:
		qr, err := e.handleSavepoint(ctx, safeSession, plan.Original, "Savepoint", logStats, func(_ string) (*sqltypes.Result, error) {
			// Safely to ignore as there is no transaction.
			return &sqltypes.Result{}, nil
		}, vcursor.ignoreMaxMemoryRows)
		return qr, err
	case sqlparser.StmtSRollback:
		qr, err := e.handleSavepoint(ctx, safeSession, plan.Original, "Rollback Savepoint", logStats, func(query string) (*sqltypes.Result, error) {
			// Error as there is no transaction, so there is no savepoint that exists.
			return nil, vterrors.NewErrorf(vtrpcpb.Code_NOT_FOUND, vterrors.SPDoesNotExist, "SAVEPOINT does not exist: %s", query)
		}, vcursor.ignoreMaxMemoryRows)
		return qr, err
	case sqlparser.StmtRelease:
		qr, err := e.handleSavepoint(ctx, safeSession, plan.Original, "Release Savepoint", logStats, func(query string) (*sqltypes.Result, error) {
			// Error as there is no transaction, so there is no savepoint that exists.
			return nil, vterrors.NewErrorf(vtrpcpb.Code_NOT_FOUND, vterrors.SPDoesNotExist, "SAVEPOINT does not exist: %s", query)
		}, vcursor.ignoreMaxMemoryRows)
		return qr, err
	case sqlparser.StmtKill:
		return e.handleKill(ctx, mysqlCtx, stmt, logStats)
	}
	return nil, nil
}

func (e *Executor) startTxIfNecessary(ctx context.Context, safeSession *SafeSession) error {
	if !safeSession.Autocommit && !safeSession.InTransaction() {
		if err := e.txConn.Begin(ctx, safeSession, nil); err != nil {
			return err
		}
	}
	return nil
}

func (e *Executor) insideTransaction(ctx context.Context, safeSession *SafeSession, logStats *logstats.LogStats, execPlan func() error) error {
	mustCommit := false
	if safeSession.Autocommit && !safeSession.InTransaction() {
		mustCommit = true
		if err := e.txConn.Begin(ctx, safeSession, nil); err != nil {
			return err
		}
		// The defer acts as a failsafe. If commit was successful,
		// the rollback will be a no-op.
		defer e.txConn.Rollback(ctx, safeSession) // nolint:errcheck
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

	// If we want to instantly commit the query, then there is no need to add savepoints.
	// Any partial failure of the query will be taken care by rollback.
	safeSession.SetSavepointState(!mustCommit)

	// Execute!
	err := execPlan()
	if err != nil {
		return err
	}

	if mustCommit {
		commitStart := time.Now()
		if err := e.txConn.Commit(ctx, safeSession); err != nil {
			return err
		}
		logStats.CommitTime = time.Since(commitStart)
	}
	return nil
}

func (e *Executor) executePlan(
	ctx context.Context,
	safeSession *SafeSession,
	plan *engine.Plan,
	vcursor *vcursorImpl,
	bindVars map[string]*querypb.BindVariable,
	logStats *logstats.LogStats,
	execStart time.Time,
) (*sqltypes.Result, error) {

	// 4: Execute!
	qr, err := vcursor.ExecutePrimitive(ctx, plan.Instructions, bindVars, true)

	// 5: Log and add statistics
	e.setLogStats(logStats, plan, vcursor, execStart, err, qr)

	// Check if there was partial DML execution. If so, rollback the effect of the partially executed query.
	if err != nil {
		return nil, e.rollbackExecIfNeeded(ctx, safeSession, bindVars, logStats, err)
	}
	return qr, nil
}

// rollbackExecIfNeeded rollbacks the partial execution if earlier it was detected that it needs partial query execution to be rolled back.
func (e *Executor) rollbackExecIfNeeded(ctx context.Context, safeSession *SafeSession, bindVars map[string]*querypb.BindVariable, logStats *logstats.LogStats, err error) error {
	if safeSession.InTransaction() && safeSession.IsRollbackSet() {
		rErr := e.rollbackPartialExec(ctx, safeSession, bindVars, logStats)
		return vterrors.Wrap(err, rErr.Error())
	}
	return err
}

// rollbackPartialExec rollbacks to the savepoint or rollbacks transaction based on the value set on SafeSession.rollbackOnPartialExec.
// Once, it is used the variable is reset.
// If it fails to rollback to the previous savepoint then, the transaction is forced to be rolled back.
func (e *Executor) rollbackPartialExec(ctx context.Context, safeSession *SafeSession, bindVars map[string]*querypb.BindVariable, logStats *logstats.LogStats) error {
	var err error
	var errMsg strings.Builder

	// If the context got cancelled we still have to revert the partial DML execution.
	// We cannot use the parent context here anymore.
	if ctx.Err() != nil {
		errMsg.WriteString("context canceled: ")
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
	}

	// needs to rollback only once.
	rQuery := safeSession.rollbackOnPartialExec
	if rQuery != txRollback {
		safeSession.SavepointRollback()
		_, _, err = e.execute(ctx, nil, safeSession, rQuery, bindVars, logStats)
		// If no error, the revert is successful with the savepoint. Notify the reason as error to the client.
		if err == nil {
			errMsg.WriteString("reverted partial DML execution failure")
			return vterrors.New(vtrpcpb.Code_ABORTED, errMsg.String())
		}
		// not able to rollback changes of the failed query, so have to abort the complete transaction.
	}

	// abort the transaction.
	_ = e.txConn.Rollback(ctx, safeSession)
	errMsg.WriteString("transaction rolled back to reverse changes of partial DML execution")
	if err != nil {
		return vterrors.Wrap(err, errMsg.String())
	}
	return vterrors.New(vtrpcpb.Code_ABORTED, errMsg.String())
}

func (e *Executor) setLogStats(logStats *logstats.LogStats, plan *engine.Plan, vcursor *vcursorImpl, execStart time.Time, err error, qr *sqltypes.Result) {
	logStats.StmtType = plan.Type.String()
	logStats.ActiveKeyspace = vcursor.keyspace
	logStats.TablesUsed = plan.TablesUsed
	logStats.TabletType = vcursor.TabletType().String()
	errCount := e.logExecutionEnd(logStats, execStart, plan, err, qr)
	plan.AddStats(1, time.Since(logStats.StartTime), logStats.ShardQueries, logStats.RowsAffected, logStats.RowsReturned, errCount)
}

func (e *Executor) logExecutionEnd(logStats *logstats.LogStats, execStart time.Time, plan *engine.Plan, err error, qr *sqltypes.Result) uint64 {
	logStats.ExecuteTime = time.Since(execStart)

	e.updateQueryCounts(plan.Instructions.RouteType(), plan.Instructions.GetKeyspaceName(), plan.Instructions.GetTableName(), int64(logStats.ShardQueries))

	var errCount uint64
	if err != nil {
		logStats.Error = err
		errCount = 1
	} else {
		logStats.RowsAffected = qr.RowsAffected
		logStats.RowsReturned = uint64(len(qr.Rows))
	}
	return errCount
}

func (e *Executor) logPlanningFinished(logStats *logstats.LogStats, plan *engine.Plan) time.Time {
	execStart := time.Now()
	if plan != nil {
		logStats.StmtType = plan.Type.String()
	}
	logStats.PlanTime = execStart.Sub(logStats.StartTime)
	return execStart
}
