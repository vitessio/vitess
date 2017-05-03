// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vterrors"
	"github.com/youtube/vitess/go/vt/vtgate/vindexes"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/querytypes"

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
	// hasPartialDML is set to true if any DML was successfully
	// executed. If there was a subsequent failure, the transaction
	// must be forced to rollback.
	hasPartialDML bool
}

// newVcursorImpl creates a vcursorImpl. Before creating this object, you have to separate out any trailingComments that came with
// the query and supply it here. Trailing comments are typically sent by the application for various reasons,
// including as identifying markers. So, they have to be added back to all queries that are executed
// on behalf of the original query.
func newVCursorImpl(ctx context.Context, session *vtgatepb.Session, target querypb.Target, trailingComments string, executor *Executor) *vcursorImpl {
	return &vcursorImpl{
		ctx:              ctx,
		session:          session,
		target:           target,
		trailingComments: trailingComments,
		executor:         executor,
	}
}

// Find finds the specified table. If the keyspace what specified in the input, it gets used as qualifier.
// Otherwise, the keyspace from the request is used, if one was provided.
func (vc *vcursorImpl) Find(keyspace, tablename sqlparser.TableIdent) (table *vindexes.Table, err error) {
	ks := keyspace.String()
	if ks == "" {
		ks = vc.target.Keyspace
	}
	return vc.executor.vschema.Find(ks, tablename.String())
}

// Execute performs a V3 level execution of the query. It does not take any routing directives.
func (vc *vcursorImpl) Execute(query string, BindVars map[string]interface{}, isDML bool) (*sqltypes.Result, error) {
	qr, err := vc.executor.Execute(vc.ctx, vc.session, query+vc.trailingComments, BindVars)
	if err == nil {
		vc.hasPartialDML = true
	}
	return qr, err
}

// ExecuteMultiShard executes different queries on different shards and returns the combined result.
func (vc *vcursorImpl) ExecuteMultiShard(keyspace string, shardQueries map[string]querytypes.BoundQuery, isDML bool) (*sqltypes.Result, error) {
	qr, err := vc.executor.scatterConn.ExecuteMultiShard(vc.ctx, keyspace, commentedShardQueries(shardQueries, vc.trailingComments), vc.target.TabletType, NewSafeSession(vc.session), false, vc.session.Options)
	if err == nil {
		vc.hasPartialDML = true
	}
	return qr, err
}

// ExecuteStandalone executes the specified query on keyspace:shard, but outside of the current transaction, as an independent statement.
func (vc *vcursorImpl) ExecuteStandalone(query string, BindVars map[string]interface{}, keyspace, shard string) (*sqltypes.Result, error) {
	bq := map[string]querytypes.BoundQuery{
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
func (vc *vcursorImpl) StreamExecuteMulti(query string, keyspace string, shardVars map[string]map[string]interface{}, callback func(reply *sqltypes.Result) error) error {
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

func commentedShardQueries(shardQueries map[string]querytypes.BoundQuery, trailingComments string) map[string]querytypes.BoundQuery {
	if trailingComments == "" {
		return shardQueries
	}
	newQueries := make(map[string]querytypes.BoundQuery, len(shardQueries))
	for k, v := range shardQueries {
		newQueries[k] = querytypes.BoundQuery{
			Sql:           v.Sql + trailingComments,
			BindVariables: v.BindVariables,
		}
	}
	return newQueries
}
