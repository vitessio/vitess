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

	"golang.org/x/net/context"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodata "vitess.io/vitess/go/vt/proto/topodata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// vcursorImpl implements the VCursor functionality used by dependent
// packages to call back into VTGate.
type vcursorImpl struct {
	ctx              context.Context
	safeSession      *SafeSession
	keyspace         string
	tabletType       topodata.TabletType
	trailingComments string
	executor         *Executor
	logStats         *LogStats
	// hasPartialDML is set to true if any DML was successfully
	// executed. If there was a subsequent failure, the transaction
	// must be forced to rollback.
	hasPartialDML bool
}

// newVcursorImpl creates a vcursorImpl. Before creating this object, you have to separate out any trailingComments that came with
// the query and supply it here. Trailing comments are typically sent by the application for various reasons,
// including as identifying markers. So, they have to be added back to all queries that are executed
// on behalf of the original query.
func newVCursorImpl(ctx context.Context, safeSession *SafeSession, keyspace string, tabletType topodata.TabletType, trailingComments string, executor *Executor, logStats *LogStats) *vcursorImpl {
	return &vcursorImpl{
		ctx:              ctx,
		safeSession:      safeSession,
		keyspace:         keyspace,
		tabletType:       tabletType,
		trailingComments: trailingComments,
		executor:         executor,
		logStats:         logStats,
	}
}

// Context returns the current Context.
func (vc *vcursorImpl) Context() context.Context {
	return vc.ctx
}

// FindTable finds the specified table. If the keyspace what specified in the input, it gets used as qualifier.
// Otherwise, the keyspace from the request is used, if one was provided.
func (vc *vcursorImpl) FindTable(name sqlparser.TableName) (*vindexes.Table, key.Destination, string, topodatapb.TabletType, error) {
	dest, destKeyspace, destTabletType, err := key.ParseDestination(name.Qualifier.String(), vc.tabletType)
	if err != nil {
		return nil, nil, "", destTabletType, err
	}
	if destKeyspace == "" {
		destKeyspace = vc.keyspace
	}
	table, err := vc.executor.VSchema().FindTable(destKeyspace, name.Name.String())
	if err != nil {
		return nil, nil, "", destTabletType, err
	}
	return table, dest, destKeyspace, destTabletType, err
}

// FindTableOrVindex finds the specified table or vindex.
func (vc *vcursorImpl) FindTableOrVindex(name sqlparser.TableName) (*vindexes.Table, vindexes.Vindex, key.Destination, string, topodatapb.TabletType, error) {
	dest, destKeyspace, destTabletType, err := key.ParseDestination(name.Qualifier.String(), vc.tabletType)
	if err != nil {
		return nil, nil, nil, "", destTabletType, err
	}
	if destKeyspace == "" {
		destKeyspace = vc.keyspace
	}
	table, vindex, err := vc.executor.VSchema().FindTableOrVindex(destKeyspace, name.Name.String())
	if err != nil {
		return nil, nil, nil, "", destTabletType, err
	}
	return table, vindex, dest, destKeyspace, destTabletType, nil
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

// Execute performs a V3 level execution of the query.
func (vc *vcursorImpl) Execute(method string, query string, BindVars map[string]*querypb.BindVariable, isDML bool) (*sqltypes.Result, error) {
	qr, err := vc.executor.Execute(vc.ctx, method, vc.safeSession, query+vc.trailingComments, BindVars)
	if err == nil {
		vc.hasPartialDML = true
	}
	return qr, err
}

// ExecuteAutocommit performs a V3 level execution of the query in a separate autocommit session.
func (vc *vcursorImpl) ExecuteAutocommit(method string, query string, BindVars map[string]*querypb.BindVariable, isDML bool) (*sqltypes.Result, error) {
	qr, err := vc.executor.Execute(vc.ctx, method, NewAutocommitSession(vc.safeSession.Session), query+vc.trailingComments, BindVars)
	if err == nil {
		vc.hasPartialDML = true
	}
	return qr, err
}

// ExecuteMultiShard is part of the engine.VCursor interface.
func (vc *vcursorImpl) ExecuteMultiShard(rss []*srvtopo.ResolvedShard, queries []*querypb.BoundQuery, isDML, canAutocommit bool) (*sqltypes.Result, error) {
	atomic.AddUint32(&vc.logStats.ShardQueries, uint32(len(queries)))
	qr, err := vc.executor.scatterConn.ExecuteMultiShard(vc.ctx, rss, commentedShardQueries(queries, vc.trailingComments), vc.tabletType, vc.safeSession, false, canAutocommit)
	if err == nil {
		vc.hasPartialDML = true
	}
	return qr, err
}

// ExecuteStandalone is part of the engine.VCursor interface.
func (vc *vcursorImpl) ExecuteStandalone(query string, bindVars map[string]*querypb.BindVariable, rs *srvtopo.ResolvedShard) (*sqltypes.Result, error) {
	rss := []*srvtopo.ResolvedShard{rs}
	bqs := []*querypb.BoundQuery{
		{
			Sql:           query + vc.trailingComments,
			BindVariables: bindVars,
		},
	}
	// The canAutocommit flag is not significant because we currently don't execute DMLs through ExecuteStandalone.
	// But we set it to true for future-proofing this function.
	return vc.executor.scatterConn.ExecuteMultiShard(vc.ctx, rss, bqs, vc.tabletType, NewAutocommitSession(vc.safeSession.Session), false, true /* canAutocommit */)
}

// StreamExeculteMulti is the streaming version of ExecuteMultiShard.
func (vc *vcursorImpl) StreamExecuteMulti(query string, rss []*srvtopo.ResolvedShard, bindVars []map[string]*querypb.BindVariable, callback func(reply *sqltypes.Result) error) error {
	atomic.AddUint32(&vc.logStats.ShardQueries, uint32(len(rss)))
	return vc.executor.scatterConn.StreamExecuteMulti(vc.ctx, query+vc.trailingComments, rss, bindVars, vc.tabletType, vc.safeSession.Options, callback)
}

func (vc *vcursorImpl) ResolveDestinations(keyspace string, ids []*querypb.Value, destinations []key.Destination) ([]*srvtopo.ResolvedShard, [][]*querypb.Value, error) {
	return vc.executor.resolver.resolver.ResolveDestinations(vc.ctx, keyspace, vc.tabletType, ids, destinations)
}

func commentedShardQueries(shardQueries []*querypb.BoundQuery, trailingComments string) []*querypb.BoundQuery {
	if trailingComments == "" {
		return shardQueries
	}
	newQueries := make([]*querypb.BoundQuery, len(shardQueries))
	for i, v := range shardQueries {
		newQueries[i] = &querypb.BoundQuery{
			Sql:           v.Sql + trailingComments,
			BindVariables: v.BindVariables,
		}
	}
	return newQueries
}
