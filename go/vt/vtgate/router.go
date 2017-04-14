// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"errors"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
	"github.com/youtube/vitess/go/vt/vtgate/queryinfo"
	"github.com/youtube/vitess/go/vt/vtgate/vindexes"
)

// Router is the layer to route queries to the correct shards
// based on the values in the query.
type Router struct {
	serv        topo.SrvTopoServer
	cell        string
	planner     *Planner
	scatterConn *ScatterConn
}

// NewRouter creates a new Router.
func NewRouter(ctx context.Context, serv topo.SrvTopoServer, cell, statsName string, scatterConn *ScatterConn, normalize bool) *Router {
	return &Router{
		serv:        serv,
		cell:        cell,
		planner:     NewPlanner(ctx, serv, cell, 5000, normalize),
		scatterConn: scatterConn,
	}
}

// Execute routes a non-streaming query.
func (rtr *Router) Execute(ctx context.Context, sql string, bindVars map[string]interface{}, keyspace string, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	if bindVars == nil {
		bindVars = make(map[string]interface{})
	}
	vcursor := newQueryExecutor(ctx, tabletType, session, options, rtr)
	queryConstruct := queryinfo.NewQueryConstruct(sql, keyspace, bindVars, notInTransaction)
	plan, err := rtr.planner.GetPlan(sql, keyspace, bindVars)
	if err != nil {
		return nil, err
	}
	return plan.Instructions.Execute(vcursor, queryConstruct, make(map[string]interface{}), true)
}

// StreamExecute executes a streaming query.
func (rtr *Router) StreamExecute(ctx context.Context, sql string, bindVars map[string]interface{}, keyspace string, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) error {
	if bindVars == nil {
		bindVars = make(map[string]interface{})
	}
	vcursor := newQueryExecutor(ctx, tabletType, nil, options, rtr)
	queryConstruct := queryinfo.NewQueryConstruct(sql, keyspace, bindVars, false)
	plan, err := rtr.planner.GetPlan(sql, keyspace, bindVars)
	if err != nil {
		return err
	}
	return plan.Instructions.StreamExecute(vcursor, queryConstruct, make(map[string]interface{}), true, callback)
}

// MessageAck acks messages.
func (rtr *Router) MessageAck(ctx context.Context, keyspace, name string, ids []*querypb.Value) (int64, error) {
	vschema := rtr.planner.VSchema()
	if vschema == nil {
		return 0, errors.New("vschema not initialized")
	}
	table, err := vschema.Find(keyspace, name)
	if err != nil {
		return 0, err
	}
	vcursor := newQueryExecutor(ctx, topodatapb.TabletType_MASTER, nil, nil, rtr)
	newKeyspace, _, allShards, err := getKeyspaceShards(ctx, rtr.serv, rtr.cell, table.Keyspace.Name, topodatapb.TabletType_MASTER)
	shardIDs := make(map[string][]*querypb.Value)
	if table.Keyspace.Sharded {
		// We always use the (unique) primary vindex. The ID must be the
		// primary vindex for message tables.
		mapper := table.ColumnVindexes[0].Vindex.(vindexes.Unique)
		// convert []*querypb.Value to []interface{} for calling Map.
		asInterface := make([]interface{}, 0, len(ids))
		for _, id := range ids {
			asInterface = append(asInterface, &querypb.BindVariable{
				Type:  id.Type,
				Value: id.Value,
			})
		}
		ksids, err := mapper.Map(vcursor, asInterface)
		if err != nil {
			return 0, err
		}
		for i, ksid := range ksids {
			if len(ksid) == 0 {
				continue
			}
			shard, err := getShardForKeyspaceID(allShards, ksid)
			if err != nil {
				return 0, err
			}
			shardIDs[shard] = append(shardIDs[shard], ids[i])
		}
	} else {
		shardIDs[allShards[0].Name] = ids
	}
	return rtr.scatterConn.MessageAck(ctx, newKeyspace, shardIDs, name)
}

// IsKeyspaceRangeBasedSharded returns true if the keyspace in the vschema is
// marked as sharded.
func (rtr *Router) IsKeyspaceRangeBasedSharded(keyspace string) bool {
	vschema := rtr.planner.VSchema()
	ks, ok := vschema.Keyspaces[keyspace]
	if !ok {
		return false
	}
	if ks.Keyspace == nil {
		return false
	}
	return ks.Keyspace.Sharded
}
