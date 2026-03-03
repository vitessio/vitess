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
	"time"

	"golang.org/x/exp/maps"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/srvtopo"
	topoprotopb "vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func (vc *VCursorImpl) Session() engine.SessionActions {
	return vc
}

// TargetString returns the current TargetString of the session.
func (vc *VCursorImpl) TargetString() string {
	return vc.SafeSession.TargetString
}

// ShardDestination implements the ContextVSchema interface
func (vc *VCursorImpl) ShardDestination() key.ShardDestination {
	return vc.destination
}

// TabletType implements the ContextVSchema interface
func (vc *VCursorImpl) TabletType() topodatapb.TabletType {
	return vc.tabletType
}

// TargetDestination implements the ContextVSchema interface
func (vc *VCursorImpl) TargetDestination(qualifier string) (key.ShardDestination, *vindexes.Keyspace, topodatapb.TabletType, error) {
	keyspaceName := vc.getActualKeyspace()
	if vc.destination == nil && qualifier != "" {
		keyspaceName = qualifier
	}
	if keyspaceName == "" {
		return nil, nil, 0, ErrNoKeyspace
	}
	keyspace := vc.vschema.Keyspaces[keyspaceName]
	if keyspace == nil {
		return nil, nil, 0, vterrors.VT05003(keyspaceName)
	}
	return vc.destination, keyspace.Keyspace, vc.tabletType, nil
}

func (vc *VCursorImpl) SetTarget(target string) error {
	keyspace, tabletType, destination, tabletAlias, err := topoprotopb.ParseDestination(target, vc.config.DefaultTabletType)
	if err != nil {
		return err
	}

	// Tablet targeting must be set before starting a transaction, not during.
	if tabletAlias != nil && vc.SafeSession.InTransaction() {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
			"cannot set tablet target while in a transaction")
	}

	if _, ok := vc.vschema.Keyspaces[keyspace]; !ignoreKeyspace(keyspace) && !ok {
		return vterrors.VT05003(keyspace)
	}

	if vc.SafeSession.InTransaction() && tabletType != topodatapb.TabletType_PRIMARY {
		return vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.LockOrActiveTransaction, "can't execute the given command because you have an active transaction")
	}
	vc.SafeSession.SetTargetString(target)
	vc.keyspace = keyspace
	vc.destination = destination
	vc.tabletType = tabletType
	return nil
}

func ignoreKeyspace(keyspace string) bool {
	return keyspace == "" || sqlparser.SystemSchema(keyspace)
}

func (vc *VCursorImpl) SetUDV(key string, value any) error {
	bindValue, err := sqltypes.BuildBindVariable(value)
	if err != nil {
		return err
	}
	vc.SafeSession.SetUserDefinedVariable(key, bindValue)
	return nil
}

func (vc *VCursorImpl) GetUDV(name string) *querypb.BindVariable {
	return vc.SafeSession.GetUDV(name)
}

func (vc *VCursorImpl) SetSysVar(name string, expr string) {
	vc.SafeSession.SetSystemVariable(name, expr)
}

// HasSystemVariables returns whether the session has set system variables or not
func (vc *VCursorImpl) HasSystemVariables() bool {
	return vc.SafeSession.HasSystemVariables()
}

// GetSystemVariables takes a visitor function that will save each system variables of the session
func (vc *VCursorImpl) GetSystemVariables(f func(k string, v string)) {
	vc.SafeSession.GetSystemVariables(f)
}

// GetSystemVariablesCopy returns a copy of the system variables of the session. Changes to the original map will not affect the session.
func (vc *VCursorImpl) GetSystemVariablesCopy() map[string]string {
	vc.SafeSession.mu.Lock()
	defer vc.SafeSession.mu.Unlock()
	return maps.Clone(vc.SafeSession.SystemVariables)
}

func (vc *VCursorImpl) CheckForReservedConnection(setVarComment string, stmt sqlparser.Statement) {
	if setVarComment == "" {
		return
	}
	switch stmt.(type) {
	// If the statement supports optimizer hints or a transaction statement or a SET statement
	// no reserved connection is needed
	case *sqlparser.Begin, *sqlparser.Commit, *sqlparser.Rollback, *sqlparser.Savepoint,
		*sqlparser.SRollback, *sqlparser.Release, *sqlparser.Set, *sqlparser.Show,
		sqlparser.SupportOptimizerHint:
	default:
		vc.NeedsReservedConn()
	}
}

// NeedsReservedConn implements the SessionActions interface
func (vc *VCursorImpl) NeedsReservedConn() {
	vc.SafeSession.SetReservedConn(true)
}

func (vc *VCursorImpl) InReservedConn() bool {
	return vc.SafeSession.InReservedConn()
}

func (vc *VCursorImpl) ShardSession() []*srvtopo.ResolvedShard {
	ss := vc.SafeSession.GetShardSessions()
	if len(ss) == 0 {
		return nil
	}
	rss := make([]*srvtopo.ResolvedShard, len(ss))
	for i, shardSession := range ss {
		rss[i] = &srvtopo.ResolvedShard{
			Target:  shardSession.Target,
			Gateway: vc.resolver.GetGateway(),
		}
	}
	return rss
}

// SetAutocommit implements the SessionActions interface
func (vc *VCursorImpl) SetAutocommit(ctx context.Context, autocommit bool) error {
	if autocommit && vc.SafeSession.InTransaction() {
		if err := vc.executor.Commit(ctx, vc.SafeSession); err != nil {
			return err
		}
	}
	vc.SafeSession.Autocommit = autocommit
	return nil
}

// SetQueryTimeout implements the SessionActions interface
func (vc *VCursorImpl) SetQueryTimeout(maxExecutionTime int64) {
	vc.SafeSession.QueryTimeout = maxExecutionTime
}

// SetTransactionTimeout implements the SessionActions interface
func (vc *VCursorImpl) SetTransactionTimeout(transactionTimeout int64) {
	vc.SafeSession.GetOrCreateOptions().TransactionTimeout = &transactionTimeout
}

// SetClientFoundRows implements the SessionActions interface
func (vc *VCursorImpl) SetClientFoundRows(_ context.Context, clientFoundRows bool) error {
	vc.SafeSession.GetOrCreateOptions().ClientFoundRows = clientFoundRows
	return nil
}

// SetSkipQueryPlanCache implements the SessionActions interface
func (vc *VCursorImpl) SetSkipQueryPlanCache(_ context.Context, skipQueryPlanCache bool) error {
	vc.SafeSession.GetOrCreateOptions().SkipQueryPlanCache = skipQueryPlanCache
	return nil
}

// SetSQLSelectLimit implements the SessionActions interface
func (vc *VCursorImpl) SetSQLSelectLimit(limit int64) error {
	vc.SafeSession.GetOrCreateOptions().SqlSelectLimit = limit
	return nil
}

// SetTransactionMode implements the SessionActions interface
func (vc *VCursorImpl) SetTransactionMode(mode vtgatepb.TransactionMode) {
	vc.SafeSession.TransactionMode = mode
}

// SetWorkload implements the SessionActions interface
func (vc *VCursorImpl) SetWorkload(workload querypb.ExecuteOptions_Workload) {
	vc.SafeSession.GetOrCreateOptions().Workload = workload
}

// SetPlannerVersion implements the SessionActions interface
func (vc *VCursorImpl) SetPlannerVersion(v plancontext.PlannerVersion) {
	vc.SafeSession.GetOrCreateOptions().PlannerVersion = v
}

func (vc *VCursorImpl) SetPriority(priority string) {
	if priority != "" {
		vc.SafeSession.GetOrCreateOptions().Priority = priority
	} else if vc.SafeSession.Options != nil && vc.SafeSession.Options.Priority != "" {
		vc.SafeSession.Options.Priority = ""
	}
}

func (vc *VCursorImpl) SetExecQueryTimeout(timeout *int) {
	// Determine the effective timeout: use passed timeout if non-nil, otherwise use session's query timeout if available
	var execTimeout *int
	if timeout != nil {
		execTimeout = timeout
	} else if sessionTimeout := vc.getQueryTimeout(); sessionTimeout > 0 {
		execTimeout = &sessionTimeout
	}

	// If no effective timeout and no session options, return early
	if execTimeout == nil {
		if vc.SafeSession.GetOptions() == nil {
			return
		}
		vc.SafeSession.GetOrCreateOptions().Timeout = nil
		return
	}

	vc.queryTimeout = time.Duration(*execTimeout) * time.Millisecond
	// Set the authoritative timeout using the determined execTimeout
	vc.SafeSession.GetOrCreateOptions().Timeout = &querypb.ExecuteOptions_AuthoritativeTimeout{
		AuthoritativeTimeout: int64(*execTimeout),
	}
}

// getQueryTimeout returns timeout based on the priority
// session setting > global default specified by a flag.
func (vc *VCursorImpl) getQueryTimeout() int {
	sessionQueryTimeout := int(vc.SafeSession.GetQueryTimeout())
	if sessionQueryTimeout != 0 {
		return sessionQueryTimeout
	}
	return vc.config.QueryTimeout
}

// SetConsolidator implements the SessionActions interface
func (vc *VCursorImpl) SetConsolidator(consolidator querypb.ExecuteOptions_Consolidator) {
	// Avoid creating session Options when they do not yet exist and the
	// consolidator is unspecified.
	if consolidator == querypb.ExecuteOptions_CONSOLIDATOR_UNSPECIFIED && vc.SafeSession.GetOptions() == nil {
		return
	}
	vc.SafeSession.GetOrCreateOptions().Consolidator = consolidator
}

func (vc *VCursorImpl) SetWorkloadName(workloadName string) {
	if workloadName != "" {
		vc.SafeSession.GetOrCreateOptions().WorkloadName = workloadName
	}
}

// SetFoundRows implements the SessionActions interface
func (vc *VCursorImpl) SetFoundRows(foundRows uint64) {
	vc.SafeSession.SetFoundRows(foundRows)
}

// SetInDMLExecution implements the SessionActions interface
func (vc *VCursorImpl) SetInDMLExecution(inDMLExec bool) {
	vc.SafeSession.SetInDMLExecution(inDMLExec)
}

// SetDDLStrategy implements the SessionActions interface
func (vc *VCursorImpl) SetDDLStrategy(strategy string) {
	vc.SafeSession.SetDDLStrategy(strategy)
}

// GetDDLStrategy implements the SessionActions interface
func (vc *VCursorImpl) GetDDLStrategy() string {
	return vc.SafeSession.GetDDLStrategy()
}

// SetMigrationContext implements the SessionActions interface
func (vc *VCursorImpl) SetMigrationContext(migrationContext string) {
	vc.SafeSession.SetMigrationContext(migrationContext)
}

// GetMigrationContext implements the SessionActions interface
func (vc *VCursorImpl) GetMigrationContext() string {
	return vc.SafeSession.GetMigrationContext()
}

// GetSessionUUID implements the SessionActions interface
func (vc *VCursorImpl) GetSessionUUID() string {
	return vc.SafeSession.GetSessionUUID()
}

// SetSessionEnableSystemSettings implements the SessionActions interface
func (vc *VCursorImpl) SetSessionEnableSystemSettings(_ context.Context, allow bool) error {
	vc.SafeSession.SetSessionEnableSystemSettings(allow)
	return nil
}

// GetSessionEnableSystemSettings implements the SessionActions interface
func (vc *VCursorImpl) GetSessionEnableSystemSettings() bool {
	return vc.SafeSession.GetSessionEnableSystemSettings()
}

// SetReadAfterWriteGTID implements the SessionActions interface
func (vc *VCursorImpl) SetReadAfterWriteGTID(vtgtid string) {
	vc.SafeSession.SetReadAfterWriteGTID(vtgtid)
}

// SetReadAfterWriteTimeout implements the SessionActions interface
func (vc *VCursorImpl) SetReadAfterWriteTimeout(timeout float64) {
	vc.SafeSession.SetReadAfterWriteTimeout(timeout)
}

// SetSessionTrackGTIDs implements the SessionActions interface
func (vc *VCursorImpl) SetSessionTrackGTIDs(enable bool) {
	vc.SafeSession.SetSessionTrackGtids(enable)
}

// HasCreatedTempTable implements the SessionActions interface
func (vc *VCursorImpl) HasCreatedTempTable() {
	vc.SafeSession.GetOrCreateOptions().HasCreatedTempTables = true
}

// GetWarnings implements the SessionActions interface
func (vc *VCursorImpl) GetWarnings() []*querypb.QueryWarning {
	return vc.SafeSession.GetWarnings()
}

// AnyAdvisoryLockTaken implements the SessionActions interface
func (vc *VCursorImpl) AnyAdvisoryLockTaken() bool {
	return vc.SafeSession.HasAdvisoryLock()
}

// AddAdvisoryLock implements the SessionActions interface
func (vc *VCursorImpl) AddAdvisoryLock(name string) {
	vc.SafeSession.AddAdvisoryLock(name)
}

// RemoveAdvisoryLock implements the SessionActions interface
func (vc *VCursorImpl) RemoveAdvisoryLock(name string) {
	vc.SafeSession.RemoveAdvisoryLock(name)
}

func (vc *VCursorImpl) SetCommitOrder(co vtgatepb.CommitOrder) {
	vc.SafeSession.SetCommitOrder(co)
}

func (vc *VCursorImpl) InTransaction() bool {
	return vc.SafeSession.InTransaction()
}

func (vc *VCursorImpl) Commit(ctx context.Context) error {
	return vc.executor.Commit(ctx, vc.SafeSession)
}

// RecordWarning stores the given warning in the current session
func (vc *VCursorImpl) RecordWarning(warning *querypb.QueryWarning) {
	vc.SafeSession.RecordWarning(warning)
}

// VExplainLogging enables logging for VEXPLAIN
func (vc *VCursorImpl) VExplainLogging() {
	vc.SafeSession.EnableLogging(vc.Environment().Parser())
}

// GetVExplainLogs retrieves VEXPLAIN logs
func (vc *VCursorImpl) GetVExplainLogs() []engine.ExecuteEntry {
	return vc.SafeSession.GetLogs()
}

// GetBindVars implements the VSchema interface
func (vc *VCursorImpl) GetBindVars() map[string]*querypb.BindVariable {
	return vc.bindVars
}

func (vc *VCursorImpl) SetBindVars(m map[string]*querypb.BindVariable) {
	vc.bindVars = m
}

// SetForeignKeyCheckState updates the foreign key checks state of the vcursor.
func (vc *VCursorImpl) SetForeignKeyCheckState(fkChecksState *bool) {
	vc.fkChecksState = fkChecksState
}

// GetForeignKeyChecksState gets the stored foreign key checks state in the vcursor.
func (vc *VCursorImpl) GetForeignKeyChecksState() *bool {
	return vc.fkChecksState
}

func (vc *VCursorImpl) CachePlan() bool {
	return vc.SafeSession.CachePlan()
}

func (vc *VCursorImpl) SetLastInsertID(id uint64) {
	vc.SafeSession.mu.Lock()
	defer vc.SafeSession.mu.Unlock()
	vc.SafeSession.LastInsertId = id
}

func (vc *VCursorImpl) PlanPrepareStatement(ctx context.Context, query string) (*engine.Plan, error) {
	return vc.executor.PlanPrepareStmt(ctx, vc.SafeSession, query)
}

func (vc *VCursorImpl) ClearPrepareData(name string) {
	delete(vc.SafeSession.PrepareStatement, name)
}

func (vc *VCursorImpl) StorePrepareData(stmtName string, prepareData *vtgatepb.PrepareData) {
	vc.SafeSession.StorePrepareData(stmtName, prepareData)
}

func (vc *VCursorImpl) GetPrepareData(stmtName string) *vtgatepb.PrepareData {
	return vc.SafeSession.GetPrepareData(stmtName)
}
