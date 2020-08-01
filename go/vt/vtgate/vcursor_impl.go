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
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"vitess.io/vitess/go/mysql"

	"vitess.io/vitess/go/vt/callerid"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vtgate/vschemaacl"

	"vitess.io/vitess/go/vt/vtgate/planbuilder"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	topoprotopb "vitess.io/vitess/go/vt/topo/topoproto"
)

var _ engine.VCursor = (*vcursorImpl)(nil)
var _ planbuilder.ContextVSchema = (*vcursorImpl)(nil)
var _ iExecute = (*Executor)(nil)

// vcursor_impl needs these facilities to be able to be able to execute queries for vindexes
type iExecute interface {
	Execute(ctx context.Context, method string, session *SafeSession, s string, vars map[string]*querypb.BindVariable) (*sqltypes.Result, error)
	ExecuteMultiShard(ctx context.Context, rss []*srvtopo.ResolvedShard, queries []*querypb.BoundQuery, session *SafeSession, autocommit bool, ignoreMaxMemoryRows bool) (qr *sqltypes.Result, errs []error)
	StreamExecuteMulti(ctx context.Context, s string, rss []*srvtopo.ResolvedShard, vars []map[string]*querypb.BindVariable, options *querypb.ExecuteOptions, callback func(reply *sqltypes.Result) error) error

	// TODO: remove when resolver is gone
	ParseDestinationTarget(targetString string) (string, topodatapb.TabletType, key.Destination, error)
}

//VSchemaOperator is an interface to Vschema Operations
type VSchemaOperator interface {
	GetCurrentSrvVschema() *vschemapb.SrvVSchema
	GetCurrentVschema() (*vindexes.VSchema, error)
	UpdateVSchema(ctx context.Context, ksName string, vschema *vschemapb.SrvVSchema) error
}

// vcursorImpl implements the VCursor functionality used by dependent
// packages to call back into VTGate.
type vcursorImpl struct {
	ctx            context.Context
	safeSession    *SafeSession
	keyspace       string
	tabletType     topodatapb.TabletType
	destination    key.Destination
	marginComments sqlparser.MarginComments
	executor       iExecute
	resolver       *srvtopo.Resolver
	logStats       *LogStats
	// rollbackOnPartialExec is set to true if any DML was successfully
	// executed. If there was a subsequent failure, the transaction
	// must be forced to rollback.
	rollbackOnPartialExec bool
	ignoreMaxMemoryRows   bool
	vschema               *vindexes.VSchema
	vm                    VSchemaOperator
}

func (vc *vcursorImpl) ExecuteVSchema(keyspace string, vschemaDDL *sqlparser.DDL) error {
	srvVschema := vc.vm.GetCurrentSrvVschema()
	if srvVschema == nil {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "vschema not loaded")
	}

	allowed := vschemaacl.Authorized(callerid.ImmediateCallerIDFromContext(vc.ctx))
	if !allowed {
		return vterrors.Errorf(vtrpcpb.Code_PERMISSION_DENIED, "not authorized to perform vschema operations")

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

	return vc.vm.UpdateVSchema(vc.ctx, ksName, srvVschema)

}

// newVcursorImpl creates a vcursorImpl. Before creating this object, you have to separate out any marginComments that came with
// the query and supply it here. Trailing comments are typically sent by the application for various reasons,
// including as identifying markers. So, they have to be added back to all queries that are executed
// on behalf of the original query.
func newVCursorImpl(ctx context.Context, safeSession *SafeSession, marginComments sqlparser.MarginComments, executor *Executor, logStats *LogStats, vm VSchemaOperator, vschema *vindexes.VSchema, resolver *srvtopo.Resolver) (*vcursorImpl, error) {
	keyspace, tabletType, destination, err := parseDestinationTarget(safeSession.TargetString, vschema)
	if err != nil {
		return nil, err
	}

	// With DiscoveryGateway transactions are only allowed on master.
	if UsingLegacyGateway() && safeSession.InTransaction() && tabletType != topodatapb.TabletType_MASTER {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "newVCursorImpl: transactions are supported only for master tablet types, current type: %v", tabletType)
	}

	return &vcursorImpl{
		ctx:            ctx,
		safeSession:    safeSession,
		keyspace:       keyspace,
		tabletType:     tabletType,
		destination:    destination,
		marginComments: marginComments,
		executor:       executor,
		logStats:       logStats,
		resolver:       resolver,
		vschema:        vschema,
		vm:             vm,
	}, nil
}

// Context returns the current Context.
func (vc *vcursorImpl) Context() context.Context {
	return vc.ctx
}

// MaxMemoryRows returns the maxMemoryRows flag value.
func (vc *vcursorImpl) MaxMemoryRows() int {
	return *maxMemoryRows
}

// ExceedsMaxMemoryRows returns a boolean indicating whether the maxMemoryRows value has been exceeded.
// Returns false if the max memory rows override directive is set to true.
func (vc *vcursorImpl) ExceedsMaxMemoryRows(numRows int) bool {
	return !vc.ignoreMaxMemoryRows && numRows > *maxMemoryRows
}

// SetIgnoreMaxMemoryRows sets the ignoreMaxMemoryRows value.
func (vc *vcursorImpl) SetIgnoreMaxMemoryRows(ignoreMaxMemoryRows bool) {
	vc.ignoreMaxMemoryRows = ignoreMaxMemoryRows
}

// SetContextTimeout updates context and sets a timeout.
func (vc *vcursorImpl) SetContextTimeout(timeout time.Duration) context.CancelFunc {
	ctx, cancel := context.WithTimeout(vc.ctx, timeout)
	vc.ctx = ctx
	return cancel
}

// ErrorGroupCancellableContext updates context that can be cancelled.
func (vc *vcursorImpl) ErrorGroupCancellableContext() *errgroup.Group {
	g, ctx := errgroup.WithContext(vc.ctx)
	vc.ctx = ctx
	return g
}

// RecordWarning stores the given warning in the current session
func (vc *vcursorImpl) RecordWarning(warning *querypb.QueryWarning) {
	vc.safeSession.RecordWarning(warning)
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

// FindTablesOrVindex finds the specified table or vindex.
func (vc *vcursorImpl) FindTablesOrVindex(name sqlparser.TableName) ([]*vindexes.Table, vindexes.Vindex, string, topodatapb.TabletType, key.Destination, error) {
	destKeyspace, destTabletType, dest, err := vc.executor.ParseDestinationTarget(name.Qualifier.String())
	if err != nil {
		return nil, nil, "", destTabletType, nil, err
	}
	if destKeyspace == "" {
		destKeyspace = vc.keyspace
	}
	tables, vindex, err := vc.vschema.FindTablesOrVindex(destKeyspace, name.Name.String(), vc.tabletType)
	if err != nil {
		return nil, nil, "", destTabletType, nil, err
	}
	return tables, vindex, destKeyspace, destTabletType, dest, nil
}

// DefaultKeyspace returns the default keyspace of the current request
// if there is one. If the keyspace specified in the target cannot be
// identified, it returns an error.
func (vc *vcursorImpl) DefaultKeyspace() (*vindexes.Keyspace, error) {
	if vc.keyspace == "" {
		return nil, errNoKeyspace
	}
	ks, ok := vc.vschema.Keyspaces[vc.keyspace]
	if !ok {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "keyspace %s not found in vschema", vc.keyspace)
	}
	return ks.Keyspace, nil
}

func (vc *vcursorImpl) AnyKeyspace() (*vindexes.Keyspace, error) {
	keyspace, err := vc.DefaultKeyspace()
	if err == nil {
		return keyspace, nil
	}
	if err != errNoKeyspace {
		return nil, err
	}

	if len(vc.vschema.Keyspaces) == 0 {
		return nil, vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "no keyspaces available")
	}

	// Looks for any sharded keyspace if present, otherwise take any keyspace.
	for _, ks := range vc.vschema.Keyspaces {
		keyspace = ks.Keyspace
		if keyspace.Sharded {
			return keyspace, nil
		}
	}
	return keyspace, nil
}

func (vc *vcursorImpl) FirstSortedKeyspace() (*vindexes.Keyspace, error) {
	if len(vc.vschema.Keyspaces) == 0 {
		return nil, vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "no keyspaces available")
	}
	kss := vc.vschema.Keyspaces
	keys := make([]string, 0, len(kss))
	for ks := range kss {
		keys = append(keys, ks)
	}
	sort.Strings(keys)

	return kss[keys[0]].Keyspace, nil
}

// TargetString returns the current TargetString of the session.
func (vc *vcursorImpl) TargetString() string {
	return vc.safeSession.TargetString
}

// Execute is part of the engine.VCursor interface.
func (vc *vcursorImpl) Execute(method string, query string, bindVars map[string]*querypb.BindVariable, rollbackOnError bool, co vtgatepb.CommitOrder) (*sqltypes.Result, error) {
	session := vc.safeSession
	if co == vtgatepb.CommitOrder_AUTOCOMMIT {
		// For autocommit, we have to create an independent session.
		session = NewAutocommitSession(vc.safeSession.Session)
	} else {
		session.SetCommitOrder(co)
		defer session.SetCommitOrder(vtgatepb.CommitOrder_NORMAL)
	}

	qr, err := vc.executor.Execute(vc.ctx, method, session, vc.marginComments.Leading+query+vc.marginComments.Trailing, bindVars)
	if err == nil && rollbackOnError {
		vc.rollbackOnPartialExec = true
	}
	return qr, err
}

// ExecuteMultiShard is part of the engine.VCursor interface.
func (vc *vcursorImpl) ExecuteMultiShard(rss []*srvtopo.ResolvedShard, queries []*querypb.BoundQuery, rollbackOnError, autocommit bool) (*sqltypes.Result, []error) {
	atomic.AddUint32(&vc.logStats.ShardQueries, uint32(len(queries)))
	qr, errs := vc.executor.ExecuteMultiShard(vc.ctx, rss, commentedShardQueries(queries, vc.marginComments), vc.safeSession, autocommit, vc.ignoreMaxMemoryRows)

	if errs == nil && rollbackOnError {
		vc.rollbackOnPartialExec = true
	}
	return qr, errs
}

// AutocommitApproval is part of the engine.VCursor interface.
func (vc *vcursorImpl) AutocommitApproval() bool {
	return vc.safeSession.AutocommitApproval()
}

// ExecuteStandalone is part of the engine.VCursor interface.
func (vc *vcursorImpl) ExecuteStandalone(query string, bindVars map[string]*querypb.BindVariable, rs *srvtopo.ResolvedShard) (*sqltypes.Result, error) {
	rss := []*srvtopo.ResolvedShard{rs}
	bqs := []*querypb.BoundQuery{
		{
			Sql:           vc.marginComments.Leading + query + vc.marginComments.Trailing,
			BindVariables: bindVars,
		},
	}
	// The autocommit flag is always set to false because we currently don't
	// execute DMLs through ExecuteStandalone.
	qr, errs := vc.executor.ExecuteMultiShard(vc.ctx, rss, bqs, NewAutocommitSession(vc.safeSession.Session), false /* autocommit */, vc.ignoreMaxMemoryRows)
	return qr, vterrors.Aggregate(errs)
}

// StreamExeculteMulti is the streaming version of ExecuteMultiShard.
func (vc *vcursorImpl) StreamExecuteMulti(query string, rss []*srvtopo.ResolvedShard, bindVars []map[string]*querypb.BindVariable, callback func(reply *sqltypes.Result) error) error {
	atomic.AddUint32(&vc.logStats.ShardQueries, uint32(len(rss)))
	return vc.executor.StreamExecuteMulti(vc.ctx, vc.marginComments.Leading+query+vc.marginComments.Trailing, rss, bindVars, vc.safeSession.Options, callback)
}

// ExecuteKeyspaceID is part of the engine.VCursor interface.
func (vc *vcursorImpl) ExecuteKeyspaceID(keyspace string, ksid []byte, query string, bindVars map[string]*querypb.BindVariable, rollbackOnError, autocommit bool) (*sqltypes.Result, error) {
	atomic.AddUint32(&vc.logStats.ShardQueries, 1)
	rss, _, err := vc.ResolveDestinations(keyspace, nil, []key.Destination{key.DestinationKeyspaceID(ksid)})
	if err != nil {
		return nil, err
	}
	queries := []*querypb.BoundQuery{{
		Sql:           query,
		BindVariables: bindVars,
	}}
	qr, errs := vc.ExecuteMultiShard(rss, queries, rollbackOnError, autocommit)

	if len(errs) == 0 {
		if rollbackOnError {
			vc.rollbackOnPartialExec = true
		}
		return qr, nil
	}
	return nil, errs[0]
}

func (vc *vcursorImpl) ResolveDestinations(keyspace string, ids []*querypb.Value, destinations []key.Destination) ([]*srvtopo.ResolvedShard, [][]*querypb.Value, error) {
	return vc.resolver.ResolveDestinations(vc.ctx, keyspace, vc.tabletType, ids, destinations)
}

func (vc *vcursorImpl) Session() engine.SessionActions {
	return vc
}

func (vc *vcursorImpl) SetTarget(target string) error {
	keyspace, tabletType, _, err := topoprotopb.ParseDestination(target, defaultTabletType)
	if err != nil {
		return err
	}
	if _, ok := vc.vschema.Keyspaces[keyspace]; keyspace != "" && !ok {
		return mysql.NewSQLError(mysql.ERBadDb, "42000", "Unknown database '%s'", keyspace)
	}

	if vc.safeSession.InTransaction() && tabletType != topodatapb.TabletType_MASTER {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "cannot change to a non-master type in the middle of a transaction: %v", tabletType)
	}
	vc.safeSession.SetTargetString(target)
	return nil
}

func (vc *vcursorImpl) SetUDV(key string, value interface{}) error {
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

//NeedsReservedConn implements the SessionActions interface
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
		return nil, nil, 0, vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "keyspace not specified")
	}
	keyspace := vc.vschema.Keyspaces[keyspaceName]
	if keyspace == nil {
		return nil, nil, 0, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "no keyspace with name [%s] found", keyspaceName)
	}
	return vc.destination, keyspace.Keyspace, vc.tabletType, nil
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

func (vc *vcursorImpl) planPrefixKey() string {
	if vc.destination != nil {
		switch vc.destination.(type) {
		case key.DestinationKeyspaceID, key.DestinationKeyspaceIDs:
			resolved, _, err := vc.ResolveDestinations(vc.keyspace, nil, []key.Destination{vc.destination})
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
