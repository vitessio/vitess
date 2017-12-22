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

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vterrors"
	"github.com/youtube/vitess/go/vt/vtgate/vindexes"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

// vcursorImpl implements the VCursor functionality used by dependent
// packages to call back into VTGate.
type vcursorImpl struct {
	ctx              context.Context
	session          *vtgatepb.Session
	target           querypb.Target
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
func newVCursorImpl(ctx context.Context, session *vtgatepb.Session, target querypb.Target, trailingComments string, executor *Executor, logStats *LogStats) *vcursorImpl {
	return &vcursorImpl{
		ctx:              ctx,
		session:          session,
		target:           target,
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
func (vc *vcursorImpl) FindTable(name sqlparser.TableName) (*vindexes.Table, error) {
	ks := name.Qualifier.String()
	if ks == "" {
		ks = vc.target.Keyspace
	}
	return vc.executor.VSchema().FindTable(ks, name.Name.String())
}

// FindTableOrVindex finds the specified table or vindex.
func (vc *vcursorImpl) FindTableOrVindex(name sqlparser.TableName) (*vindexes.Table, vindexes.Vindex, error) {
	ks := name.Qualifier.String()
	if ks == "" {
		ks = vc.target.Keyspace
	}
	return vc.executor.VSchema().FindTableOrVindex(ks, name.Name.String())
}

// DefaultKeyspace returns the default keyspace of the current request
// if there is one. If the keyspace specified in the target cannot be
// identified, it returns an error.
func (vc *vcursorImpl) DefaultKeyspace() (*vindexes.Keyspace, error) {
	if vc.target.Keyspace == "" {
		return nil, errNoKeyspace
	}
	ks, ok := vc.executor.VSchema().Keyspaces[vc.target.Keyspace]
	if !ok {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "keyspace %s not found in vschema", vc.target.Keyspace)
	}
	return ks.Keyspace, nil
}

// Execute performs a V3 level execution of the query. It does not take any routing directives.
func (vc *vcursorImpl) Execute(method string, query string, BindVars map[string]*querypb.BindVariable, isDML bool) (*sqltypes.Result, error) {
	qr, err := vc.executor.Execute(vc.ctx, method, vc.session, query+vc.trailingComments, BindVars)
	if err == nil {
		vc.hasPartialDML = true
	}
	return qr, err
}

// ExecuteMultiShard executes different queries on different shards and returns the combined result.
func (vc *vcursorImpl) ExecuteMultiShard(keyspace string, shardQueries map[string]*querypb.BoundQuery, isDML bool) (*sqltypes.Result, error) {
	atomic.AddUint32(&vc.logStats.ShardQueries, uint32(len(shardQueries)))
	qr, err := vc.executor.scatterConn.ExecuteMultiShard(vc.ctx, keyspace, commentedShardQueries(shardQueries, vc.trailingComments), vc.target.TabletType, NewSafeSession(vc.session), false, vc.session.Options)
	if err == nil {
		vc.hasPartialDML = true
	}
	return qr, err
}

// ExecuteStandalone executes the specified query on keyspace:shard, but outside of the current transaction, as an independent statement.
func (vc *vcursorImpl) ExecuteStandalone(query string, BindVars map[string]*querypb.BindVariable, keyspace, shard string) (*sqltypes.Result, error) {
	bq := map[string]*querypb.BoundQuery{
		shard: {
			Sql:           query + vc.trailingComments,
			BindVariables: BindVars,
		},
	}
	qr, err := vc.executor.scatterConn.ExecuteMultiShard(vc.ctx, keyspace, bq, vc.target.TabletType, NewSafeSession(nil), false, vc.session.Options)
	if err == nil {
		vc.hasPartialDML = true
	}
	return qr, err
}

// StreamExeculteMulti is the streaming version of ExecuteMultiShard.
func (vc *vcursorImpl) StreamExecuteMulti(query string, keyspace string, shardVars map[string]map[string]*querypb.BindVariable, callback func(reply *sqltypes.Result) error) error {
	atomic.AddUint32(&vc.logStats.ShardQueries, uint32(len(shardVars)))
	return vc.executor.scatterConn.StreamExecuteMulti(vc.ctx, query+vc.trailingComments, keyspace, shardVars, vc.target.TabletType, vc.session.Options, callback)
}

// GetKeyspaceShards returns the list of shards for a keyspace, and the mapped keyspace if an alias was used.
func (vc *vcursorImpl) GetKeyspaceShards(keyspace *vindexes.Keyspace) (string, []*topodatapb.ShardReference, error) {
	ks, _, allShards, err := getKeyspaceShards(vc.ctx, vc.executor.serv, vc.executor.cell, keyspace.Name, vc.target.TabletType)
	if err != nil {
		return "", nil, err
	}
	if !keyspace.Sharded && len(allShards) != 1 {
		return "", nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "unsharded keyspace %s has multiple shards: possible cause: sharded keyspace is marked as unsharded in vschema", ks)
	}
	if len(allShards) == 0 {
		return "", nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "keyspace %s has no shards", ks)
	}
	return ks, allShards, err
}

func (vc *vcursorImpl) GetShardForKeyspaceID(allShards []*topodatapb.ShardReference, keyspaceID []byte) (string, error) {
	return getShardForKeyspaceID(allShards, keyspaceID)
}

func commentedShardQueries(shardQueries map[string]*querypb.BoundQuery, trailingComments string) map[string]*querypb.BoundQuery {
	if trailingComments == "" {
		return shardQueries
	}
	newQueries := make(map[string]*querypb.BoundQuery, len(shardQueries))
	for k, v := range shardQueries {
		newQueries[k] = &querypb.BoundQuery{
			Sql:           v.Sql + trailingComments,
			BindVariables: v.BindVariables,
		}
	}
	return newQueries
}
