// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/vterrors"
	"github.com/youtube/vitess/go/vt/vtgate/vindexes"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/querytypes"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

// vcursorImpl implements the VCursor functionality used by dependent
// packages to call back into VTGate.
type vcursorImpl struct {
	ctx              context.Context
	tabletType       topodatapb.TabletType
	session          *vtgatepb.Session
	trailingComments string
	executor         *Executor
}

// newVcursorImpl creates a vcursorImpl. Before creating this object, you have to separate out any trailingComments that came with
// the query and supply it here. Trailing comments are typically sent by the application for various reasons,
// including as identifying markers. So, they have to be added back to all queries that are executed
// on behalf of the original query.
func newVCursorImpl(ctx context.Context, tabletType topodatapb.TabletType, session *vtgatepb.Session, trailingComments string, executor *Executor) *vcursorImpl {
	return &vcursorImpl{
		ctx:              ctx,
		tabletType:       tabletType,
		session:          session,
		trailingComments: trailingComments,
		executor:         executor,
	}
}

// Execute performs a V3 level execution of the query. It does not take any routing directives.
func (vc *vcursorImpl) Execute(query string, BindVars map[string]interface{}) (*sqltypes.Result, error) {
	return vc.executor.Execute(vc.ctx, vc.session, query+vc.trailingComments, BindVars)
}

// ExecuteMultiShard executes different queries on different shards and returns the combined result.
func (vc *vcursorImpl) ExecuteMultiShard(keyspace string, shardQueries map[string]querytypes.BoundQuery) (*sqltypes.Result, error) {
	return vc.executor.scatterConn.ExecuteMultiShard(vc.ctx, keyspace, commentedShardQueries(shardQueries, vc.trailingComments), vc.tabletType, NewSafeSession(vc.session), false, vc.session.Options)
}

// ExecuteStandalone executes the specified query on keyspace:shard, but outside of the current transaction, as an independent statement.
func (vc *vcursorImpl) ExecuteStandalone(query string, BindVars map[string]interface{}, keyspace, shard string) (*sqltypes.Result, error) {
	bq := map[string]querytypes.BoundQuery{
		shard: {
			Sql:           query + vc.trailingComments,
			BindVariables: BindVars,
		},
	}
	return vc.executor.scatterConn.ExecuteMultiShard(vc.ctx, keyspace, bq, vc.tabletType, NewSafeSession(nil), false, vc.session.Options)
}

// StreamExeculteMulti is the streaming version of ExecuteMultiShard.
func (vc *vcursorImpl) StreamExecuteMulti(query string, keyspace string, shardVars map[string]map[string]interface{}, callback func(reply *sqltypes.Result) error) error {
	return vc.executor.scatterConn.StreamExecuteMulti(vc.ctx, query+vc.trailingComments, keyspace, shardVars, vc.tabletType, vc.session.Options, callback)
}

// GetKeyspaceShards returns the list of shards for a keyspace, and the mapped keyspace if an alias was used.
func (vc *vcursorImpl) GetKeyspaceShards(keyspace *vindexes.Keyspace) (string, []*topodatapb.ShardReference, error) {
	ks, _, allShards, err := getKeyspaceShards(vc.ctx, vc.executor.serv, vc.executor.cell, keyspace.Name, vc.tabletType)
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
