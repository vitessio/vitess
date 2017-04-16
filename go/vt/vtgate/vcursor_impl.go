// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"sort"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vterrors"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/querytypes"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

// vcursorImpl implements the VCursor functionality used by dependent
// packages to call back into VTGate.
type vcursorImpl struct {
	ctx        context.Context
	tabletType topodatapb.TabletType
	session    *vtgatepb.Session
	executor   *Executor
}

func newVCursorImpl(ctx context.Context, tabletType topodatapb.TabletType, session *vtgatepb.Session, executor *Executor) *vcursorImpl {
	return &vcursorImpl{
		ctx:        ctx,
		tabletType: tabletType,
		session:    session,
		executor:   executor,
	}
}

func (vc *vcursorImpl) Execute(query string, bindvars map[string]interface{}) (*sqltypes.Result, error) {
	return vc.executor.Execute(vc.ctx, query, bindvars, vc.session)
}

func (vc *vcursorImpl) ExecuteMultiShard(keyspace string, shardQueries map[string]querytypes.BoundQuery) (*sqltypes.Result, error) {
	return vc.executor.scatterConn.ExecuteMultiShard(vc.ctx, keyspace, shardQueries, vc.tabletType, NewSafeSession(vc.session), false, vc.session.Options)
}

func (vc *vcursorImpl) StreamExecuteMulti(query string, keyspace string, shardVars map[string]map[string]interface{}, callback func(reply *sqltypes.Result) error) error {
	return vc.executor.scatterConn.StreamExecuteMulti(vc.ctx, query, keyspace, shardVars, vc.tabletType, vc.session.Options, callback)
}

func (vc *vcursorImpl) GetAnyShard(keyspace string) (ks, shard string, err error) {
	return getAnyShard(vc.ctx, vc.executor.serv, vc.executor.cell, keyspace, vc.tabletType)
}

func (vc *vcursorImpl) ScatterConnExecute(query string, bindVars map[string]interface{}, keyspace string, shards []string) (*sqltypes.Result, error) {
	return vc.executor.scatterConn.Execute(vc.ctx, query, bindVars, keyspace, shards, vc.tabletType, NewSafeSession(vc.session), false, vc.session.Options)
}

func (vc *vcursorImpl) GetKeyspaceShards(keyspace string) (string, *topodatapb.SrvKeyspace, []*topodatapb.ShardReference, error) {
	return getKeyspaceShards(vc.ctx, vc.executor.serv, vc.executor.cell, keyspace, vc.tabletType)
}

func (vc *vcursorImpl) GetShardForKeyspaceID(allShards []*topodatapb.ShardReference, keyspaceID []byte) (string, error) {
	return getShardForKeyspaceID(allShards, keyspaceID)
}

func (vc *vcursorImpl) ExecuteShard(keyspace string, shardQueries map[string]querytypes.BoundQuery) (*sqltypes.Result, error) {
	return vc.executor.scatterConn.ExecuteMultiShard(vc.ctx, keyspace, shardQueries, vc.tabletType, NewSafeSession(nil), false, vc.session.Options)
}

func (vc *vcursorImpl) ExecuteShow(query string, bindvars map[string]interface{}, keyspace string) (*sqltypes.Result, error) {
	if query == sqlparser.ShowDatabasesStr || query == sqlparser.ShowKeyspacesStr {
		keyspaces, err := getAllKeyspaces(vc.ctx, vc.executor.serv, vc.executor.cell)
		if err != nil {
			return nil, err
		}

		rows := make([][]sqltypes.Value, len(keyspaces))
		for i, v := range keyspaces {
			rows[i] = []sqltypes.Value{sqltypes.MakeString([]byte(v))}
		}

		result := &sqltypes.Result{
			Fields: []*querypb.Field{{
				Name: "Databases",
				Type: sqltypes.VarChar,
			}},
			RowsAffected: uint64(len(keyspaces)),
			InsertID:     0,
			Rows:         rows,
		}

		return result, nil
	}

	if query == sqlparser.ShowShardsStr {
		keyspaces, err := getAllKeyspaces(vc.ctx, vc.executor.serv, vc.executor.cell)
		if err != nil {
			return nil, err
		}

		var shards []string

		for _, keyspace := range keyspaces {
			_, _, ksShards, err := getKeyspaceShards(vc.ctx, vc.executor.serv, vc.executor.cell, keyspace, vc.tabletType)
			if err != nil {
				return nil, err
			}

			for _, shard := range ksShards {
				shards = append(shards, keyspace+"/"+shard.Name)
			}
		}

		rows := make([][]sqltypes.Value, len(shards))
		for i, v := range shards {
			rows[i] = []sqltypes.Value{sqltypes.MakeString([]byte(v))}
		}

		result := &sqltypes.Result{
			Fields: []*querypb.Field{{
				Name: "Shards",
				Type: sqltypes.VarChar,
			}},
			RowsAffected: uint64(len(shards)),
			InsertID:     0,
			Rows:         rows,
		}

		return result, nil
	}

	if query == sqlparser.ShowVSchemaTablesStr {
		if keyspace == "" {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "No keyspace selected")
		}
		vschema := vc.executor.VSchema()
		if vschema == nil {
			return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "vschema not initialized")
		}
		ks, ok := vschema.Keyspaces[keyspace]
		if !ok {
			return nil, vterrors.Errorf(vtrpcpb.Code_NOT_FOUND, "keyspace %s not found in vschema", keyspace)
		}

		var tables []string
		for name := range ks.Tables {
			tables = append(tables, name)
		}
		sort.Strings(tables)

		rows := make([][]sqltypes.Value, len(tables))
		for i, v := range tables {
			rows[i] = []sqltypes.Value{sqltypes.MakeString([]byte(v))}
		}

		result := &sqltypes.Result{
			Fields: []*querypb.Field{{
				Name: "Tables",
				Type: sqltypes.VarChar,
			}},
			RowsAffected: uint64(len(rows)),
			InsertID:     0,
			Rows:         rows,
		}

		return result, nil
	}

	return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "unimplemented metadata query: "+query)
}
