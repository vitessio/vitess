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
	"sync/atomic"
	"time"

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
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

var _ engine.VCursor = (*vcursorImpl)(nil)

// vcursorImpl implements the VCursor functionality used by dependent
// packages to call back into VTGate.
type vcursorImpl struct {
	ctx            context.Context
	safeSession    *SafeSession
	keyspace       string
	tabletType     topodatapb.TabletType
	marginComments sqlparser.MarginComments
	executor       *Executor
	logStats       *LogStats
	// hasPartialDML is set to true if any DML was successfully
	// executed. If there was a subsequent failure, the transaction
	// must be forced to rollback.
	hasPartialDML bool
}

// newVcursorImpl creates a vcursorImpl. Before creating this object, you have to separate out any marginComments that came with
// the query and supply it here. Trailing comments are typically sent by the application for various reasons,
// including as identifying markers. So, they have to be added back to all queries that are executed
// on behalf of the original query.
func newVCursorImpl(ctx context.Context, safeSession *SafeSession, keyspace string, tabletType topodatapb.TabletType, marginComments sqlparser.MarginComments, executor *Executor, logStats *LogStats) *vcursorImpl {
	return &vcursorImpl{
		ctx:            ctx,
		safeSession:    safeSession,
		keyspace:       keyspace,
		tabletType:     tabletType,
		marginComments: marginComments,
		executor:       executor,
		logStats:       logStats,
	}
}

// Context returns the current Context.
func (vc *vcursorImpl) Context() context.Context {
	return vc.ctx
}

// SetContextTimeout updates context and sets a timeout.
func (vc *vcursorImpl) SetContextTimeout(timeout time.Duration) context.CancelFunc {
	ctx, cancel := context.WithTimeout(vc.ctx, timeout)
	vc.ctx = ctx
	return cancel
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
	table, err := vc.executor.VSchema().FindTable(destKeyspace, name.Name.String())
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
	tables, vindex, err := vc.executor.VSchema().FindTablesOrVindex(destKeyspace, name.Name.String(), vc.tabletType)
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
	ks, ok := vc.executor.VSchema().Keyspaces[vc.keyspace]
	if !ok {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "keyspace %s not found in vschema", vc.keyspace)
	}
	return ks.Keyspace, nil
}

// TargetString returns the current TargetString of the session.
func (vc *vcursorImpl) TargetString() string {
	return vc.safeSession.TargetString
}

// Execute performs a V3 level execution of the query.
func (vc *vcursorImpl) Execute(method string, query string, BindVars map[string]*querypb.BindVariable, isDML bool) (*sqltypes.Result, error) {
	qr, err := vc.executor.Execute(vc.ctx, method, vc.safeSession, vc.marginComments.Leading+query+vc.marginComments.Trailing, BindVars)
	if err == nil {
		vc.hasPartialDML = true
	}
	return qr, err
}

// ExecuteAutocommit performs a V3 level execution of the query in a separate autocommit session.
func (vc *vcursorImpl) ExecuteAutocommit(method string, query string, BindVars map[string]*querypb.BindVariable, isDML bool) (*sqltypes.Result, error) {
	qr, err := vc.executor.Execute(vc.ctx, method, NewAutocommitSession(vc.safeSession.Session), vc.marginComments.Leading+query+vc.marginComments.Trailing, BindVars)
	if err == nil {
		vc.hasPartialDML = true
	}
	return qr, err
}

// ExecuteMultiShard is part of the engine.VCursor interface.
func (vc *vcursorImpl) ExecuteMultiShard(rss []*srvtopo.ResolvedShard, queries []*querypb.BoundQuery, isDML, autocommit bool) (*sqltypes.Result, []error) {
	atomic.AddUint32(&vc.logStats.ShardQueries, uint32(len(queries)))
	qr, errs := vc.executor.scatterConn.ExecuteMultiShard(vc.ctx, rss, commentedShardQueries(queries, vc.marginComments), vc.tabletType, vc.safeSession, false, autocommit)

	if errs == nil {
		vc.hasPartialDML = true
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
	qr, errs := vc.executor.scatterConn.ExecuteMultiShard(vc.ctx, rss, bqs, vc.tabletType, NewAutocommitSession(vc.safeSession.Session), false, false /* autocommit */)
	return qr, vterrors.Aggregate(errs)
}

// StreamExeculteMulti is the streaming version of ExecuteMultiShard.
func (vc *vcursorImpl) StreamExecuteMulti(query string, rss []*srvtopo.ResolvedShard, bindVars []map[string]*querypb.BindVariable, callback func(reply *sqltypes.Result) error) error {
	atomic.AddUint32(&vc.logStats.ShardQueries, uint32(len(rss)))
	return vc.executor.scatterConn.StreamExecuteMulti(vc.ctx, vc.marginComments.Leading+query+vc.marginComments.Trailing, rss, bindVars, vc.tabletType, vc.safeSession.Options, callback)
}

func (vc *vcursorImpl) ResolveDestinations(keyspace string, ids []*querypb.Value, destinations []key.Destination) ([]*srvtopo.ResolvedShard, [][]*querypb.Value, error) {
	return vc.executor.resolver.resolver.ResolveDestinations(vc.ctx, keyspace, vc.tabletType, ids, destinations)
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
