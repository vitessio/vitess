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
	"sort"
	"strings"
	"sync/atomic"

	"vitess.io/vitess/go/vt/vtgate/logstats"

	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"

	"github.com/google/uuid"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/key"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/topo"
	topoprotopb "vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/buffer"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vtgate/vschemaacl"
)

var _ engine.VCursor = (*vcursorImpl)(nil)
var _ plancontext.VSchema = (*vcursorImpl)(nil)
var _ iExecute = (*Executor)(nil)
var _ vindexes.VCursor = (*vcursorImpl)(nil)

// vcursor_impl needs these facilities to be able to be able to execute queries for vindexes
type iExecute interface {
	Execute(ctx context.Context, method string, session *SafeSession, s string, vars map[string]*querypb.BindVariable) (*sqltypes.Result, error)
	ExecuteMultiShard(ctx context.Context, rss []*srvtopo.ResolvedShard, queries []*querypb.BoundQuery, session *SafeSession, autocommit bool, ignoreMaxMemoryRows bool) (qr *sqltypes.Result, errs []error)
	StreamExecuteMulti(ctx context.Context, query string, rss []*srvtopo.ResolvedShard, vars []map[string]*querypb.BindVariable, session *SafeSession, autocommit bool, callback func(reply *sqltypes.Result) error) []error
	ExecuteLock(ctx context.Context, rs *srvtopo.ResolvedShard, query *querypb.BoundQuery, session *SafeSession, lockFuncType sqlparser.LockingFuncType) (*sqltypes.Result, error)
	Commit(ctx context.Context, safeSession *SafeSession) error
	ExecuteMessageStream(ctx context.Context, rss []*srvtopo.ResolvedShard, name string, callback func(*sqltypes.Result) error) error
	ExecuteVStream(ctx context.Context, rss []*srvtopo.ResolvedShard, filter *binlogdatapb.Filter, gtid string, callback func(evs []*binlogdatapb.VEvent) error) error
	ReleaseLock(ctx context.Context, session *SafeSession) error

	showVitessReplicationStatus(ctx context.Context, filter *sqlparser.ShowFilter) (*sqltypes.Result, error)
	showShards(ctx context.Context, filter *sqlparser.ShowFilter, destTabletType topodatapb.TabletType) (*sqltypes.Result, error)
	showTablets(filter *sqlparser.ShowFilter) (*sqltypes.Result, error)
	showVitessMetadata(ctx context.Context, filter *sqlparser.ShowFilter) (*sqltypes.Result, error)
	setVitessMetadata(ctx context.Context, name, value string) error

	// TODO: remove when resolver is gone
	ParseDestinationTarget(targetString string) (string, topodatapb.TabletType, key.Destination, error)
	VSchema() *vindexes.VSchema
}

// VSchemaOperator is an interface to Vschema Operations
type VSchemaOperator interface {
	GetCurrentSrvVschema() *vschemapb.SrvVSchema
	UpdateVSchema(ctx context.Context, ksName string, vschema *vschemapb.SrvVSchema) error
}

// vcursorImpl implements the VCursor functionality used by dependent
// packages to call back into VTGate.
type vcursorImpl struct {
	safeSession    *SafeSession
	keyspace       string
	tabletType     topodatapb.TabletType
	destination    key.Destination
	marginComments sqlparser.MarginComments
	executor       iExecute
	resolver       *srvtopo.Resolver
	topoServer     *topo.Server
	logStats       *logstats.LogStats
	collation      collations.ID

	ignoreMaxMemoryRows bool
	vschema             *vindexes.VSchema
	vm                  VSchemaOperator
	semTable            *semantics.SemTable
	warnShardedOnly     bool // when using sharded only features, a warning will be warnings field

	warnings []*querypb.QueryWarning // any warnings that are accumulated during the planning phase are stored here
	pv       plancontext.PlannerVersion
}

// newVcursorImpl creates a vcursorImpl. Before creating this object, you have to separate out any marginComments that came with
// the query and supply it here. Trailing comments are typically sent by the application for various reasons,
// including as identifying markers. So, they have to be added back to all queries that are executed
// on behalf of the original query.
func newVCursorImpl(
	safeSession *SafeSession,
	marginComments sqlparser.MarginComments,
	executor *Executor,
	logStats *logstats.LogStats,
	vm VSchemaOperator,
	vschema *vindexes.VSchema,
	resolver *srvtopo.Resolver,
	serv srvtopo.Server,
	warnShardedOnly bool,
	pv plancontext.PlannerVersion,
) (*vcursorImpl, error) {
	keyspace, tabletType, destination, err := parseDestinationTarget(safeSession.TargetString, vschema)
	if err != nil {
		return nil, err
	}

	var ts *topo.Server
	// We don't have access to the underlying TopoServer if this vtgate is
	// filtering keyspaces because we don't have an accurate view of the topo.
	if serv != nil && !discovery.FilteringKeyspaces() {
		ts, err = serv.GetTopoServer()
		if err != nil {
			return nil, err
		}
	}

	// we only support collations for the new TabletGateway implementation
	var connCollation collations.ID
	if executor != nil {
		if gw, isTabletGw := executor.resolver.resolver.GetGateway().(*TabletGateway); isTabletGw {
			connCollation = gw.DefaultConnCollation()
		}
	}
	if connCollation == collations.Unknown {
		connCollation = collations.Default()
	}

	return &vcursorImpl{
		safeSession:     safeSession,
		keyspace:        keyspace,
		tabletType:      tabletType,
		destination:     destination,
		marginComments:  marginComments,
		executor:        executor,
		logStats:        logStats,
		collation:       connCollation,
		resolver:        resolver,
		vschema:         vschema,
		vm:              vm,
		topoServer:      ts,
		warnShardedOnly: warnShardedOnly,
		pv:              pv,
	}, nil
}

// HasSystemVariables returns whether the session has set system variables or not
func (vc *vcursorImpl) HasSystemVariables() bool {
	return vc.safeSession.HasSystemVariables()
}

// GetSystemVariables takes a visitor function that will save each system variables of the session
func (vc *vcursorImpl) GetSystemVariables(f func(k string, v string)) {
	vc.safeSession.GetSystemVariables(f)
}

// ConnCollation returns the collation of this session
func (vc *vcursorImpl) ConnCollation() collations.ID {
	return vc.collation
}

// MaxMemoryRows returns the maxMemoryRows flag value.
func (vc *vcursorImpl) MaxMemoryRows() int {
	return maxMemoryRows
}

// ExceedsMaxMemoryRows returns a boolean indicating whether the maxMemoryRows value has been exceeded.
// Returns false if the max memory rows override directive is set to true.
func (vc *vcursorImpl) ExceedsMaxMemoryRows(numRows int) bool {
	return !vc.ignoreMaxMemoryRows && numRows > maxMemoryRows
}

// SetIgnoreMaxMemoryRows sets the ignoreMaxMemoryRows value.
func (vc *vcursorImpl) SetIgnoreMaxMemoryRows(ignoreMaxMemoryRows bool) {
	vc.ignoreMaxMemoryRows = ignoreMaxMemoryRows
}

// RecordWarning stores the given warning in the current session
func (vc *vcursorImpl) RecordWarning(warning *querypb.QueryWarning) {
	vc.safeSession.RecordWarning(warning)
}

// IsShardRoutingEnabled implements the VCursor interface.
func (vc *vcursorImpl) IsShardRoutingEnabled() bool {
	return enableShardRouting
}

// FindTable finds the specified table. If the keyspace what specified in the input, it gets used as qualifier.
// Otherwise, the keyspace from the request is used, if one was provided.
func (vc *vcursorImpl) FindTable(name sqlparser.TableName) (*vindexes.Table, string, topodatapb.TabletType, key.Destination, error) {
	destKeyspace, destTabletType, dest, err := vc.executor.ParseDestinationTarget(name.Qualifier.String())
	if err != nil {
		return nil, "", destTabletType, nil, err
	}
	if destKeyspace == "" {
		destKeyspace = vc.keyspace
	}
	table, err := vc.vschema.FindTable(destKeyspace, name.Name.String())
	if err != nil {
		return nil, "", destTabletType, nil, err
	}
	return table, destKeyspace, destTabletType, dest, err
}

func (vc *vcursorImpl) FindView(name sqlparser.TableName) sqlparser.SelectStatement {
	ks, _, _, err := vc.executor.ParseDestinationTarget(name.Qualifier.String())
	if err != nil {
		return nil
	}
	if ks == "" {
		ks = vc.keyspace
	}
	return vc.vschema.FindView(ks, name.Name.String())
}

func (vc *vcursorImpl) FindRoutedTable(name sqlparser.TableName) (*vindexes.Table, error) {
	destKeyspace, destTabletType, _, err := vc.executor.ParseDestinationTarget(name.Qualifier.String())
	if err != nil {
		return nil, err
	}
	if destKeyspace == "" {
		destKeyspace = vc.keyspace
	}

	table, err := vc.vschema.FindRoutedTable(destKeyspace, name.Name.String(), destTabletType)
	if err != nil {
		return nil, err
	}

	return table, nil
}

// FindTableOrVindex finds the specified table or vindex.
func (vc *vcursorImpl) FindTableOrVindex(name sqlparser.TableName) (*vindexes.Table, vindexes.Vindex, string, topodatapb.TabletType, key.Destination, error) {
	destKeyspace, destTabletType, dest, err := vc.executor.ParseDestinationTarget(name.Qualifier.String())
	if err != nil {
		return nil, nil, "", destTabletType, nil, err
	}
	if destKeyspace == "" {
		destKeyspace = vc.getActualKeyspace()
	}
	table, vindex, err := vc.vschema.FindTableOrVindex(destKeyspace, name.Name.String(), vc.tabletType)
	if err != nil {
		return nil, nil, "", destTabletType, nil, err
	}
	return table, vindex, destKeyspace, destTabletType, dest, nil
}

func (vc *vcursorImpl) getActualKeyspace() string {
	if !sqlparser.SystemSchema(vc.keyspace) {
		return vc.keyspace
	}
	ks, err := vc.AnyKeyspace()
	if err != nil {
		return ""
	}
	return ks.Name
}

// DefaultKeyspace returns the default keyspace of the current request
// if there is one. If the keyspace specified in the target cannot be
// identified, it returns an error.
func (vc *vcursorImpl) DefaultKeyspace() (*vindexes.Keyspace, error) {
	if ignoreKeyspace(vc.keyspace) {
		return nil, errNoKeyspace
	}
	ks, ok := vc.vschema.Keyspaces[vc.keyspace]
	if !ok {
		return nil, vterrors.NewErrorf(vtrpcpb.Code_NOT_FOUND, vterrors.BadDb, "Unknown database '%s' in vschema", vc.keyspace)
	}
	return ks.Keyspace, nil
}

var errNoDbAvailable = vterrors.NewErrorf(vtrpcpb.Code_FAILED_PRECONDITION, vterrors.NoDB, "no database available")

func (vc *vcursorImpl) AnyKeyspace() (*vindexes.Keyspace, error) {
	keyspace, err := vc.DefaultKeyspace()
	if err == nil {
		return keyspace, nil
	}
	if err != errNoKeyspace {
		return nil, err
	}

	if len(vc.vschema.Keyspaces) == 0 {
		return nil, errNoDbAvailable
	}

	var keyspaces = make([]*vindexes.Keyspace, 0, len(vc.vschema.Keyspaces))
	for _, ks := range vc.vschema.Keyspaces {
		keyspaces = append(keyspaces, ks.Keyspace)
	}
	sort.Slice(keyspaces, func(i, j int) bool {
		return keyspaces[i].Name < keyspaces[j].Name
	})

	// Look for any sharded keyspace if present, otherwise take the first keyspace,
	// sorted alphabetically
	for _, ks := range keyspaces {
		if ks.Sharded {
			return ks, nil
		}
	}
	return keyspaces[0], nil
}

func (vc *vcursorImpl) FirstSortedKeyspace() (*vindexes.Keyspace, error) {
	if len(vc.vschema.Keyspaces) == 0 {
		return nil, errNoDbAvailable
	}
	kss := vc.vschema.Keyspaces
	keys := make([]string, 0, len(kss))
	for ks := range kss {
		keys = append(keys, ks)
	}
	sort.Strings(keys)

	return kss[keys[0]].Keyspace, nil
}

// SysVarSetEnabled implements the ContextVSchema interface
func (vc *vcursorImpl) SysVarSetEnabled() bool {
	return vc.GetSessionEnableSystemSettings()
}

// KeyspaceExists provides whether the keyspace exists or not.
func (vc *vcursorImpl) KeyspaceExists(ks string) bool {
	return vc.vschema.Keyspaces[ks] != nil
}

// AllKeyspace implements the ContextVSchema interface
func (vc *vcursorImpl) AllKeyspace() ([]*vindexes.Keyspace, error) {
	if len(vc.vschema.Keyspaces) == 0 {
		return nil, errNoDbAvailable
	}
	var kss []*vindexes.Keyspace
	for _, ks := range vc.vschema.Keyspaces {
		kss = append(kss, ks.Keyspace)
	}
	return kss, nil
}

// FindKeyspace implements the VSchema interface
func (vc *vcursorImpl) FindKeyspace(keyspace string) (*vindexes.Keyspace, error) {
	if len(vc.vschema.Keyspaces) == 0 {
		return nil, errNoDbAvailable
	}
	for _, ks := range vc.vschema.Keyspaces {
		if ks.Keyspace.Name == keyspace {
			return ks.Keyspace, nil
		}
	}
	return nil, nil
}

// Planner implements the ContextVSchema interface
func (vc *vcursorImpl) Planner() plancontext.PlannerVersion {
	if vc.safeSession.Options != nil &&
		vc.safeSession.Options.PlannerVersion != querypb.ExecuteOptions_DEFAULT_PLANNER {
		return vc.safeSession.Options.PlannerVersion
	}
	return vc.pv
}

// GetSemTable implements the ContextVSchema interface
func (vc *vcursorImpl) GetSemTable() *semantics.SemTable {
	return vc.semTable
}

// TargetString returns the current TargetString of the session.
func (vc *vcursorImpl) TargetString() string {
	return vc.safeSession.TargetString
}

// MaxBufferingRetries is to represent max retries on buffering.
const MaxBufferingRetries = 3

func (vc *vcursorImpl) ExecutePrimitive(ctx context.Context, primitive engine.Primitive, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	for try := 0; try < MaxBufferingRetries; try++ {
		res, err := primitive.TryExecute(ctx, vc, bindVars, wantfields)
		if err != nil && vterrors.RootCause(err) == buffer.ShardMissingError {
			continue
		}
		return res, err
	}
	return nil, vterrors.New(vtrpcpb.Code_UNAVAILABLE, "upstream shards are not available")
}

func (vc *vcursorImpl) StreamExecutePrimitive(ctx context.Context, primitive engine.Primitive, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	for try := 0; try < MaxBufferingRetries; try++ {
		err := primitive.TryStreamExecute(ctx, vc, bindVars, wantfields, callback)
		if err != nil && vterrors.RootCause(err) == buffer.ShardMissingError {
			continue
		}
		return err
	}
	return vterrors.New(vtrpcpb.Code_UNAVAILABLE, "upstream shards are not available")
}

func (vc *vcursorImpl) StreamExecutePrimitiveStandalone(ctx context.Context, primitive engine.Primitive, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(result *sqltypes.Result) error) error {
	// clone the vcursorImpl with a new session.
	newVC := vc.cloneWithAutocommitSession()
	for try := 0; try < MaxBufferingRetries; try++ {
		err := primitive.TryStreamExecute(ctx, newVC, bindVars, wantfields, callback)
		if err != nil && vterrors.RootCause(err) == buffer.ShardMissingError {
			continue
		}
		return err
	}
	return vterrors.New(vtrpcpb.Code_UNAVAILABLE, "upstream shards are not available")
}

// Execute is part of the engine.VCursor interface.
func (vc *vcursorImpl) Execute(ctx context.Context, method string, query string, bindVars map[string]*querypb.BindVariable, rollbackOnError bool, co vtgatepb.CommitOrder) (*sqltypes.Result, error) {
	session := vc.safeSession
	if co == vtgatepb.CommitOrder_AUTOCOMMIT {
		// For autocommit, we have to create an independent session.
		session = NewAutocommitSession(vc.safeSession.Session)
		session.logging = vc.safeSession.logging
		rollbackOnError = false
	} else {
		session.SetCommitOrder(co)
		defer session.SetCommitOrder(vtgatepb.CommitOrder_NORMAL)
	}

	err := vc.markSavepoint(ctx, rollbackOnError, map[string]*querypb.BindVariable{})
	if err != nil {
		return nil, err
	}

	qr, err := vc.executor.Execute(ctx, method, session, vc.marginComments.Leading+query+vc.marginComments.Trailing, bindVars)
	vc.setRollbackOnPartialExecIfRequired(err != nil, rollbackOnError)

	return qr, err
}

// markSavepoint opens an internal savepoint before executing the original query.
// This happens only when rollback is allowed and no other savepoint was executed
// and the query is executed in an explicit transaction (i.e. started by the client).
func (vc *vcursorImpl) markSavepoint(ctx context.Context, needsRollbackOnParialExec bool, bindVars map[string]*querypb.BindVariable) error {
	if !needsRollbackOnParialExec || !vc.safeSession.CanAddSavepoint() {
		return nil
	}
	uID := fmt.Sprintf("_vt%s", strings.ReplaceAll(uuid.NewString(), "-", "_"))
	spQuery := fmt.Sprintf("%ssavepoint %s%s", vc.marginComments.Leading, uID, vc.marginComments.Trailing)
	_, err := vc.executor.Execute(ctx, "MarkSavepoint", vc.safeSession, spQuery, bindVars)
	if err != nil {
		return err
	}
	vc.safeSession.SetSavepoint(uID)
	return nil
}

const txRollback = "Rollback Transaction"

// ExecuteMultiShard is part of the engine.VCursor interface.
func (vc *vcursorImpl) ExecuteMultiShard(ctx context.Context, rss []*srvtopo.ResolvedShard, queries []*querypb.BoundQuery, rollbackOnError, canAutocommit bool) (*sqltypes.Result, []error) {
	noOfShards := len(rss)
	atomic.AddUint64(&vc.logStats.ShardQueries, uint64(noOfShards))
	err := vc.markSavepoint(ctx, rollbackOnError && (noOfShards > 1), map[string]*querypb.BindVariable{})
	if err != nil {
		return nil, []error{err}
	}

	qr, errs := vc.executor.ExecuteMultiShard(ctx, rss, commentedShardQueries(queries, vc.marginComments), vc.safeSession, canAutocommit, vc.ignoreMaxMemoryRows)
	vc.setRollbackOnPartialExecIfRequired(len(errs) != len(rss), rollbackOnError)

	return qr, errs
}

// StreamExecuteMulti is the streaming version of ExecuteMultiShard.
func (vc *vcursorImpl) StreamExecuteMulti(ctx context.Context, query string, rss []*srvtopo.ResolvedShard, bindVars []map[string]*querypb.BindVariable, rollbackOnError bool, autocommit bool, callback func(reply *sqltypes.Result) error) []error {
	noOfShards := len(rss)
	atomic.AddUint64(&vc.logStats.ShardQueries, uint64(noOfShards))
	err := vc.markSavepoint(ctx, rollbackOnError && (noOfShards > 1), map[string]*querypb.BindVariable{})
	if err != nil {
		return []error{err}
	}

	errs := vc.executor.StreamExecuteMulti(ctx, vc.marginComments.Leading+query+vc.marginComments.Trailing, rss, bindVars, vc.safeSession, autocommit, callback)
	vc.setRollbackOnPartialExecIfRequired(len(errs) != len(rss), rollbackOnError)

	return errs
}

// ExecuteLock is for executing advisory lock statements.
func (vc *vcursorImpl) ExecuteLock(ctx context.Context, rs *srvtopo.ResolvedShard, query *querypb.BoundQuery, lockFuncType sqlparser.LockingFuncType) (*sqltypes.Result, error) {
	query.Sql = vc.marginComments.Leading + query.Sql + vc.marginComments.Trailing
	return vc.executor.ExecuteLock(ctx, rs, query, vc.safeSession, lockFuncType)
}

// ExecuteStandalone is part of the engine.VCursor interface.
func (vc *vcursorImpl) ExecuteStandalone(ctx context.Context, query string, bindVars map[string]*querypb.BindVariable, rs *srvtopo.ResolvedShard) (*sqltypes.Result, error) {
	rss := []*srvtopo.ResolvedShard{rs}
	bqs := []*querypb.BoundQuery{
		{
			Sql:           vc.marginComments.Leading + query + vc.marginComments.Trailing,
			BindVariables: bindVars,
		},
	}
	// The autocommit flag is always set to false because we currently don't
	// execute DMLs through ExecuteStandalone.
	qr, errs := vc.executor.ExecuteMultiShard(ctx, rss, bqs, NewAutocommitSession(vc.safeSession.Session), false /* autocommit */, vc.ignoreMaxMemoryRows)
	return qr, vterrors.Aggregate(errs)
}

// ExecuteKeyspaceID is part of the engine.VCursor interface.
func (vc *vcursorImpl) ExecuteKeyspaceID(ctx context.Context, keyspace string, ksid []byte, query string, bindVars map[string]*querypb.BindVariable, rollbackOnError, autocommit bool) (*sqltypes.Result, error) {
	atomic.AddUint64(&vc.logStats.ShardQueries, 1)
	rss, _, err := vc.ResolveDestinations(ctx, keyspace, nil, []key.Destination{key.DestinationKeyspaceID(ksid)})
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
	// This fields helps in to ignore multi-db transaction error when it states `queryFromVindex`.
	if !rollbackOnError {
		vc.safeSession.queryFromVindex = true
		defer func() {
			vc.safeSession.queryFromVindex = false
		}()
	}
	qr, errs := vc.ExecuteMultiShard(ctx, rss, queries, rollbackOnError, autocommit)
	return qr, vterrors.Aggregate(errs)
}

func (vc *vcursorImpl) InTransactionAndIsDML() bool {
	if !vc.safeSession.InTransaction() {
		return false
	}
	switch vc.logStats.StmtType {
	case "INSERT", "REPLACE", "UPDATE", "DELETE":
		return true
	}
	return false
}

func (vc *vcursorImpl) LookupRowLockShardSession() vtgatepb.CommitOrder {
	switch vc.logStats.StmtType {
	case "DELETE", "UPDATE":
		return vtgatepb.CommitOrder_POST
	}
	return vtgatepb.CommitOrder_PRE
}

// AutocommitApproval is part of the engine.VCursor interface.
func (vc *vcursorImpl) AutocommitApproval() bool {
	return vc.safeSession.AutocommitApproval()
}

// setRollbackOnPartialExecIfRequired sets the value on SafeSession.rollbackOnPartialExec
// when the query gets successfully executed on at least one shard,
// there does not exist any old savepoint for which rollback is already set
// and rollback on error is allowed.
func (vc *vcursorImpl) setRollbackOnPartialExecIfRequired(atleastOneSuccess bool, rollbackOnError bool) {
	if atleastOneSuccess && rollbackOnError && !vc.safeSession.IsRollbackSet() {
		vc.safeSession.SetRollbackCommand()
	}
}

// fixupPartiallyMovedShards checks if any of the shards in the route has a ShardRoutingRule (true when a keyspace
// is in the middle of being moved to another keyspace using MoveTables moving a subset of shards at a time
func (vc *vcursorImpl) fixupPartiallyMovedShards(rss []*srvtopo.ResolvedShard) ([]*srvtopo.ResolvedShard, error) {
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

func (vc *vcursorImpl) ResolveDestinations(ctx context.Context, keyspace string, ids []*querypb.Value, destinations []key.Destination) ([]*srvtopo.ResolvedShard, [][]*querypb.Value, error) {
	rss, values, err := vc.resolver.ResolveDestinations(ctx, keyspace, vc.tabletType, ids, destinations)
	if err != nil {
		return nil, nil, err
	}
	if enableShardRouting {
		rss, err = vc.fixupPartiallyMovedShards(rss)
		if err != nil {
			return nil, nil, err
		}
	}
	return rss, values, err
}

func (vc *vcursorImpl) ResolveDestinationsMultiCol(ctx context.Context, keyspace string, ids [][]sqltypes.Value, destinations []key.Destination) ([]*srvtopo.ResolvedShard, [][][]sqltypes.Value, error) {
	rss, values, err := vc.resolver.ResolveDestinationsMultiCol(ctx, keyspace, vc.tabletType, ids, destinations)
	if err != nil {
		return nil, nil, err
	}
	if enableShardRouting {
		rss, err = vc.fixupPartiallyMovedShards(rss)
		if err != nil {
			return nil, nil, err
		}
	}
	return rss, values, err
}

func (vc *vcursorImpl) Session() engine.SessionActions {
	return vc
}

func (vc *vcursorImpl) SetTarget(target string) error {
	keyspace, tabletType, _, err := topoprotopb.ParseDestination(target, defaultTabletType)
	if err != nil {
		return err
	}
	if _, ok := vc.vschema.Keyspaces[keyspace]; !ignoreKeyspace(keyspace) && !ok {
		return vterrors.NewErrorf(vtrpcpb.Code_NOT_FOUND, vterrors.BadDb, "unknown database '%s'", keyspace)
	}

	if vc.safeSession.InTransaction() && tabletType != topodatapb.TabletType_PRIMARY {
		return vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.LockOrActiveTransaction, "can't execute the given command because you have an active transaction")
	}
	vc.safeSession.SetTargetString(target)
	return nil
}

func ignoreKeyspace(keyspace string) bool {
	return keyspace == "" || sqlparser.SystemSchema(keyspace)
}

func (vc *vcursorImpl) SetUDV(key string, value any) error {
	bindValue, err := sqltypes.BuildBindVariable(value)
	if err != nil {
		return err
	}
	vc.safeSession.SetUserDefinedVariable(key, bindValue)
	return nil
}

func (vc *vcursorImpl) SetSysVar(name string, expr string) {
	vc.safeSession.SetSystemVariable(name, expr)
}

// NeedsReservedConn implements the SessionActions interface
func (vc *vcursorImpl) NeedsReservedConn() {
	vc.safeSession.SetReservedConn(true)
}

func (vc *vcursorImpl) InReservedConn() bool {
	return vc.safeSession.InReservedConn()
}

func (vc *vcursorImpl) ShardSession() []*srvtopo.ResolvedShard {
	ss := vc.safeSession.GetShardSessions()
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

// Destination implements the ContextVSchema interface
func (vc *vcursorImpl) Destination() key.Destination {
	return vc.destination
}

// TabletType implements the ContextVSchema interface
func (vc *vcursorImpl) TabletType() topodatapb.TabletType {
	return vc.tabletType
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

// TargetDestination implements the ContextVSchema interface
func (vc *vcursorImpl) TargetDestination(qualifier string) (key.Destination, *vindexes.Keyspace, topodatapb.TabletType, error) {
	keyspaceName := vc.keyspace
	if vc.destination == nil && qualifier != "" {
		keyspaceName = qualifier
	}
	if keyspaceName == "" {
		return nil, nil, 0, errNoKeyspace
	}
	keyspace := vc.vschema.Keyspaces[keyspaceName]
	if keyspace == nil {
		return nil, nil, 0, vterrors.NewErrorf(vtrpcpb.Code_NOT_FOUND, vterrors.BadDb, "Unknown database '%s' in vschema", keyspaceName)
	}
	return vc.destination, keyspace.Keyspace, vc.tabletType, nil
}

// SetAutocommit implements the SessionActions interface
func (vc *vcursorImpl) SetAutocommit(ctx context.Context, autocommit bool) error {
	if autocommit && vc.safeSession.InTransaction() {
		if err := vc.executor.Commit(ctx, vc.safeSession); err != nil {
			return err
		}
	}
	vc.safeSession.Autocommit = autocommit
	return nil
}

// SetQueryTimeout implements the SessionActions interface
func (vc *vcursorImpl) SetQueryTimeout(maxExecutionTime int64) {
	vc.safeSession.QueryTimeout = maxExecutionTime
}

// GetQueryTimeout implements the SessionActions interface
// The priority of adding query timeouts -
// 1. Query timeout comment directive.
// 2. If the comment directive is unspecified, then we use the session setting.
// 3. If the comment directive and session settings is unspecified, then we use the global default specified by a flag.
func (vc *vcursorImpl) GetQueryTimeout(queryTimeoutFromComments int) int {
	if queryTimeoutFromComments != 0 {
		return queryTimeoutFromComments
	}
	sessionQueryTimeout := int(vc.safeSession.GetQueryTimeout())
	if sessionQueryTimeout != 0 {
		return sessionQueryTimeout
	}
	return queryTimeout
}

// SetClientFoundRows implements the SessionActions interface
func (vc *vcursorImpl) SetClientFoundRows(_ context.Context, clientFoundRows bool) error {
	vc.safeSession.GetOrCreateOptions().ClientFoundRows = clientFoundRows
	return nil
}

// SetSkipQueryPlanCache implements the SessionActions interface
func (vc *vcursorImpl) SetSkipQueryPlanCache(_ context.Context, skipQueryPlanCache bool) error {
	vc.safeSession.GetOrCreateOptions().SkipQueryPlanCache = skipQueryPlanCache
	return nil
}

// SetSQLSelectLimit implements the SessionActions interface
func (vc *vcursorImpl) SetSQLSelectLimit(limit int64) error {
	vc.safeSession.GetOrCreateOptions().SqlSelectLimit = limit
	return nil
}

// SetTransactionMode implements the SessionActions interface
func (vc *vcursorImpl) SetTransactionMode(mode vtgatepb.TransactionMode) {
	vc.safeSession.TransactionMode = mode
}

// SetWorkload implements the SessionActions interface
func (vc *vcursorImpl) SetWorkload(workload querypb.ExecuteOptions_Workload) {
	vc.safeSession.GetOrCreateOptions().Workload = workload
}

// SetPlannerVersion implements the SessionActions interface
func (vc *vcursorImpl) SetPlannerVersion(v plancontext.PlannerVersion) {
	vc.safeSession.GetOrCreateOptions().PlannerVersion = v
}

// SetFoundRows implements the SessionActions interface
func (vc *vcursorImpl) SetFoundRows(foundRows uint64) {
	vc.safeSession.FoundRows = foundRows
	vc.safeSession.foundRowsHandled = true
}

// SetDDLStrategy implements the SessionActions interface
func (vc *vcursorImpl) SetDDLStrategy(strategy string) {
	vc.safeSession.SetDDLStrategy(strategy)
}

// GetDDLStrategy implements the SessionActions interface
func (vc *vcursorImpl) GetDDLStrategy() string {
	return vc.safeSession.GetDDLStrategy()
}

// GetSessionUUID implements the SessionActions interface
func (vc *vcursorImpl) GetSessionUUID() string {
	return vc.safeSession.GetSessionUUID()
}

// SetSessionEnableSystemSettings implements the SessionActions interface
func (vc *vcursorImpl) SetSessionEnableSystemSettings(_ context.Context, allow bool) error {
	vc.safeSession.SetSessionEnableSystemSettings(allow)
	return nil
}

// GetSessionEnableSystemSettings implements the SessionActions interface
func (vc *vcursorImpl) GetSessionEnableSystemSettings() bool {
	return vc.safeSession.GetSessionEnableSystemSettings()
}

// SetReadAfterWriteGTID implements the SessionActions interface
func (vc *vcursorImpl) SetReadAfterWriteGTID(vtgtid string) {
	vc.safeSession.SetReadAfterWriteGTID(vtgtid)
}

// SetReadAfterWriteTimeout implements the SessionActions interface
func (vc *vcursorImpl) SetReadAfterWriteTimeout(timeout float64) {
	vc.safeSession.SetReadAfterWriteTimeout(timeout)
}

// SetSessionTrackGTIDs implements the SessionActions interface
func (vc *vcursorImpl) SetSessionTrackGTIDs(enable bool) {
	vc.safeSession.SetSessionTrackGtids(enable)
}

// HasCreatedTempTable implements the SessionActions interface
func (vc *vcursorImpl) HasCreatedTempTable() {
	vc.safeSession.GetOrCreateOptions().HasCreatedTempTables = true
}

// GetWarnings implements the SessionActions interface
func (vc *vcursorImpl) GetWarnings() []*querypb.QueryWarning {
	return vc.safeSession.GetWarnings()
}

// AnyAdvisoryLockTaken implements the SessionActions interface
func (vc *vcursorImpl) AnyAdvisoryLockTaken() bool {
	return vc.safeSession.HasAdvisoryLock()
}

// AddAdvisoryLock implements the SessionActions interface
func (vc *vcursorImpl) AddAdvisoryLock(name string) {
	vc.safeSession.AddAdvisoryLock(name)
}

// RemoveAdvisoryLock implements the SessionActions interface
func (vc *vcursorImpl) RemoveAdvisoryLock(name string) {
	vc.safeSession.RemoveAdvisoryLock(name)
}

func (vc *vcursorImpl) SetCommitOrder(co vtgatepb.CommitOrder) {
	vc.safeSession.SetCommitOrder(co)
}

func (vc *vcursorImpl) InTransaction() bool {
	return vc.safeSession.InTransaction()
}

// GetDBDDLPluginName implements the VCursor interface
func (vc *vcursorImpl) GetDBDDLPluginName() string {
	return dbDDLPlugin
}

// KeyspaceAvailable implements the VCursor interface
func (vc *vcursorImpl) KeyspaceAvailable(ks string) bool {
	_, exists := vc.executor.VSchema().Keyspaces[ks]
	return exists
}

// ErrorIfShardedF implements the VCursor interface
func (vc *vcursorImpl) ErrorIfShardedF(ks *vindexes.Keyspace, warn, errFormat string, params ...any) error {
	if ks.Sharded {
		return vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, errFormat, params...)
	}
	vc.WarnUnshardedOnly("'%s' not supported in sharded mode", warn)

	return nil
}

// WarnUnshardedOnly implements the VCursor interface
func (vc *vcursorImpl) WarnUnshardedOnly(format string, params ...any) {
	if vc.warnShardedOnly {
		vc.warnings = append(vc.warnings, &querypb.QueryWarning{
			Code:    mysql.ERNotSupportedYet,
			Message: fmt.Sprintf(format, params...),
		})
	}
}

// PlannerWarning implements the VCursor interface
func (vc *vcursorImpl) PlannerWarning(message string) {
	if message == "" {
		return
	}
	vc.warnings = append(vc.warnings, &querypb.QueryWarning{
		Code:    mysql.ERNotSupportedYet,
		Message: message,
	})
}

// ForeignKeyMode implements the VCursor interface
func (vc *vcursorImpl) ForeignKeyMode() string {
	return strings.ToLower(foreignKeyMode)
}

// ParseDestinationTarget parses destination target string and sets default keyspace if possible.
func parseDestinationTarget(targetString string, vschema *vindexes.VSchema) (string, topodatapb.TabletType, key.Destination, error) {
	destKeyspace, destTabletType, dest, err := topoprotopb.ParseDestination(targetString, defaultTabletType)
	// Set default keyspace
	if destKeyspace == "" && len(vschema.Keyspaces) == 1 {
		for k := range vschema.Keyspaces {
			destKeyspace = k
		}
	}
	return destKeyspace, destTabletType, dest, err
}

func (vc *vcursorImpl) planPrefixKey(ctx context.Context) string {
	if vc.destination != nil {
		switch vc.destination.(type) {
		case key.DestinationKeyspaceID, key.DestinationKeyspaceIDs:
			resolved, _, err := vc.ResolveDestinations(ctx, vc.keyspace, nil, []key.Destination{vc.destination})
			if err == nil && len(resolved) > 0 {
				shards := make([]string, len(resolved))
				for i := 0; i < len(shards); i++ {
					shards[i] = resolved[i].Target.GetShard()
				}
				sort.Strings(shards)
				return fmt.Sprintf("%s%sKsIDsResolved(%s)", vc.keyspace, vindexes.TabletTypeSuffix[vc.tabletType], strings.Join(shards, ","))
			}
		default:
			// use destination string (out of the switch)
		}
		return fmt.Sprintf("%s%s%s", vc.keyspace, vindexes.TabletTypeSuffix[vc.tabletType], vc.destination.String())
	}
	return fmt.Sprintf("%s%s", vc.keyspace, vindexes.TabletTypeSuffix[vc.tabletType])
}

func (vc *vcursorImpl) GetKeyspace() string {
	return vc.keyspace
}

func (vc *vcursorImpl) ExecuteVSchema(ctx context.Context, keyspace string, vschemaDDL *sqlparser.AlterVschema) error {
	srvVschema := vc.vm.GetCurrentSrvVschema()
	if srvVschema == nil {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "vschema not loaded")
	}

	user := callerid.ImmediateCallerIDFromContext(ctx)
	allowed := vschemaacl.Authorized(user)
	if !allowed {
		return vterrors.NewErrorf(vtrpcpb.Code_PERMISSION_DENIED, vterrors.AccessDeniedError, "User '%s' is not authorized to perform vschema operations", user.GetUsername())

	}

	// Resolve the keyspace either from the table qualifier or the target keyspace
	var ksName string
	if !vschemaDDL.Table.IsEmpty() {
		ksName = vschemaDDL.Table.Qualifier.String()
	}
	if ksName == "" {
		ksName = keyspace
	}
	if ksName == "" {
		return errNoKeyspace
	}

	ks := srvVschema.Keyspaces[ksName]
	ks, err := topotools.ApplyVSchemaDDL(ksName, ks, vschemaDDL)

	if err != nil {
		return err
	}

	srvVschema.Keyspaces[ksName] = ks

	return vc.vm.UpdateVSchema(ctx, ksName, srvVschema)

}

func (vc *vcursorImpl) MessageStream(ctx context.Context, rss []*srvtopo.ResolvedShard, tableName string, callback func(*sqltypes.Result) error) error {
	atomic.AddUint64(&vc.logStats.ShardQueries, uint64(len(rss)))
	return vc.executor.ExecuteMessageStream(ctx, rss, tableName, callback)
}

func (vc *vcursorImpl) VStream(ctx context.Context, rss []*srvtopo.ResolvedShard, filter *binlogdatapb.Filter, gtid string, callback func(evs []*binlogdatapb.VEvent) error) error {
	return vc.executor.ExecuteVStream(ctx, rss, filter, gtid, callback)
}

func (vc *vcursorImpl) ShowExec(ctx context.Context, command sqlparser.ShowCommandType, filter *sqlparser.ShowFilter) (*sqltypes.Result, error) {
	switch command {
	case sqlparser.VitessReplicationStatus:
		return vc.executor.showVitessReplicationStatus(ctx, filter)
	case sqlparser.VitessShards:
		return vc.executor.showShards(ctx, filter, vc.tabletType)
	case sqlparser.VitessTablets:
		return vc.executor.showTablets(filter)
	case sqlparser.VitessVariables:
		return vc.executor.showVitessMetadata(ctx, filter)
	default:
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "bug: unexpected show command: %v", command)
	}
}

func (vc *vcursorImpl) GetVSchema() *vindexes.VSchema {
	return vc.vschema
}

func (vc *vcursorImpl) GetSrvVschema() *vschemapb.SrvVSchema {
	return vc.vm.GetCurrentSrvVschema()
}

func (vc *vcursorImpl) SetExec(ctx context.Context, name string, value string) error {
	return vc.executor.setVitessMetadata(ctx, name, value)
}

func (vc *vcursorImpl) CanUseSetVar() bool {
	return sqlparser.IsMySQL80AndAbove() && setVarEnabled
}

func (vc *vcursorImpl) ReleaseLock(ctx context.Context) error {
	return vc.executor.ReleaseLock(ctx, vc.safeSession)
}

func (vc *vcursorImpl) cloneWithAutocommitSession() *vcursorImpl {
	safeSession := NewAutocommitSession(vc.safeSession.Session)
	return &vcursorImpl{
		safeSession:     safeSession,
		keyspace:        vc.keyspace,
		tabletType:      vc.tabletType,
		destination:     vc.destination,
		marginComments:  vc.marginComments,
		executor:        vc.executor,
		logStats:        vc.logStats,
		collation:       vc.collation,
		resolver:        vc.resolver,
		vschema:         vc.vschema,
		vm:              vc.vm,
		topoServer:      vc.topoServer,
		warnShardedOnly: vc.warnShardedOnly,
		pv:              vc.pv,
	}
}

func (vc *vcursorImpl) VtExplainLogging() {
	vc.safeSession.EnableLogging()
}

func (vc *vcursorImpl) GetVTExplainLogs() []engine.ExecuteEntry {
	return vc.safeSession.logging.GetLogs()
}
func (vc *vcursorImpl) FindRoutedShard(keyspace, shard string) (keyspaceName string, err error) {
	return vc.vschema.FindRoutedShard(keyspace, shard)
}
