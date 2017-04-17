// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/querytypes"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
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

func newVCursorImpl(ctx context.Context, tabletType topodatapb.TabletType, session *vtgatepb.Session, trailingComments string, executor *Executor) *vcursorImpl {
	return &vcursorImpl{
		ctx:              ctx,
		tabletType:       tabletType,
		session:          session,
		trailingComments: trailingComments,
		executor:         executor,
	}
}

func (vc *vcursorImpl) Execute(query string, bindvars map[string]interface{}) (*sqltypes.Result, error) {
	return vc.executor.Execute(vc.ctx, query+vc.trailingComments, bindvars, vc.session)
}

func (vc *vcursorImpl) ExecuteMultiShard(keyspace string, shardQueries map[string]querytypes.BoundQuery) (*sqltypes.Result, error) {
	return vc.executor.scatterConn.ExecuteMultiShard(vc.ctx, keyspace, commentedShardQueries(shardQueries, vc.trailingComments), vc.tabletType, NewSafeSession(vc.session), false, vc.session.Options)
}

func (vc *vcursorImpl) StreamExecuteMulti(query string, keyspace string, shardVars map[string]map[string]interface{}, callback func(reply *sqltypes.Result) error) error {
	return vc.executor.scatterConn.StreamExecuteMulti(vc.ctx, query+vc.trailingComments, keyspace, shardVars, vc.tabletType, vc.session.Options, callback)
}

func (vc *vcursorImpl) GetAnyShard(keyspace string) (ks, shard string, err error) {
	return getAnyShard(vc.ctx, vc.executor.serv, vc.executor.cell, keyspace, vc.tabletType)
}

func (vc *vcursorImpl) ScatterConnExecute(query string, bindVars map[string]interface{}, keyspace string, shards []string) (*sqltypes.Result, error) {
	return vc.executor.scatterConn.Execute(vc.ctx, query+vc.trailingComments, bindVars, keyspace, shards, vc.tabletType, NewSafeSession(vc.session), false, vc.session.Options)
}

func (vc *vcursorImpl) GetKeyspaceShards(keyspace string) (string, *topodatapb.SrvKeyspace, []*topodatapb.ShardReference, error) {
	return getKeyspaceShards(vc.ctx, vc.executor.serv, vc.executor.cell, keyspace, vc.tabletType)
}

func (vc *vcursorImpl) GetShardForKeyspaceID(allShards []*topodatapb.ShardReference, keyspaceID []byte) (string, error) {
	return getShardForKeyspaceID(allShards, keyspaceID)
}

func (vc *vcursorImpl) ExecuteShard(keyspace string, shardQueries map[string]querytypes.BoundQuery) (*sqltypes.Result, error) {
	return vc.executor.scatterConn.ExecuteMultiShard(vc.ctx, keyspace, commentedShardQueries(shardQueries, vc.trailingComments), vc.tabletType, NewSafeSession(nil), false, vc.session.Options)
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
