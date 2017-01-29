// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
	"github.com/youtube/vitess/go/vt/tabletserver/querytypes"
)

type queryExecutor struct {
	ctx        context.Context
	tabletType topodatapb.TabletType
	session    *vtgatepb.Session
	options    *querypb.ExecuteOptions
	router     *Router
}

func newQueryExecutor(ctx context.Context, tabletType topodatapb.TabletType, session *vtgatepb.Session, options *querypb.ExecuteOptions, router *Router) *queryExecutor {
	return &queryExecutor{
		ctx:        ctx,
		tabletType: tabletType,
		session:    session,
		options:    options,
		router:     router,
	}
}

// Execute method call from vindex call to vtgate.
func (vc *queryExecutor) Execute(query string, bindvars map[string]interface{}) (*sqltypes.Result, error) {
	// We have to use an empty keyspace here, becasue vindexes that call back can reference
	// any table.
	return vc.router.Execute(vc.ctx, query, bindvars, "", vc.tabletType, vc.session, false, vc.options)
}

// ExecuteMultiShard method call from engine call to vtgate.
func (vc *queryExecutor) ExecuteMultiShard(keyspace string, shardQueries map[string]querytypes.BoundQuery, notInTransaction bool) (*sqltypes.Result, error) {
	return vc.router.scatterConn.ExecuteMultiShard(vc.ctx, keyspace, shardQueries, vc.tabletType, NewSafeSession(vc.session), notInTransaction, vc.options)
}

// StreamExecuteMulti method call from engine call to vtgate.
func (vc *queryExecutor) StreamExecuteMulti(query string, keyspace string, shardVars map[string]map[string]interface{}, callback func(reply *sqltypes.Result) error) error {
	return vc.router.scatterConn.StreamExecuteMulti(vc.ctx, query, keyspace, shardVars, vc.tabletType, vc.options, callback)
}

// GetAnyShard method call from engine call to vtgate.
func (vc *queryExecutor) GetAnyShard(keyspace string) (ks, shard string, err error) {
	return getAnyShard(vc.ctx, vc.router.serv, vc.router.cell, keyspace, vc.tabletType)
}

// ScatterConnExecute method call from engine call to vtgate.
func (vc *queryExecutor) ScatterConnExecute(query string, bindVars map[string]interface{}, keyspace string, shards []string, notInTransaction bool) (*sqltypes.Result, error) {
	return vc.router.scatterConn.Execute(vc.ctx, query, bindVars, keyspace, shards, vc.tabletType, NewSafeSession(vc.session), notInTransaction, vc.options)
}

// GetKeyspaceShards method call from engine call to vtgate.
func (vc *queryExecutor) GetKeyspaceShards(keyspace string) (string, *topodatapb.SrvKeyspace, []*topodatapb.ShardReference, error) {
	return getKeyspaceShards(vc.ctx, vc.router.serv, vc.router.cell, keyspace, vc.tabletType)
}

// GetShardForKeyspaceID method call from engine call to vtgate.
func (vc *queryExecutor) GetShardForKeyspaceID(allShards []*topodatapb.ShardReference, keyspaceID []byte) (string, error) {
	return getShardForKeyspaceID(allShards, keyspaceID)
}

func (vc *queryExecutor) ExecuteShard(keyspace string, shardQueries map[string]querytypes.BoundQuery) (*sqltypes.Result, error) {
	return vc.router.scatterConn.ExecuteMultiShard(vc.ctx, keyspace, shardQueries, vc.tabletType, NewSafeSession(nil), false, vc.options)
}
