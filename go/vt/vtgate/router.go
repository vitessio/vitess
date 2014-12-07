// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

// This is a V3 file. Do not intermix with V2.

import (
	"fmt"

	"code.google.com/p/go.net/context"
	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtgate/planbuilder"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
)

// Router is the layer to route queries to the correct shards
// based on the values in the query.
type Router struct {
	serv        SrvTopoServer
	cell        string
	planner     *Planner
	scatterConn *ScatterConn
}

// NewRouter creates a new Router.
func NewRouter(serv SrvTopoServer, cell string, schema *planbuilder.Schema, statsName string, scatterConn *ScatterConn) *Router {
	return &Router{
		serv:        serv,
		cell:        cell,
		planner:     NewPlanner(schema, 5000),
		scatterConn: scatterConn,
	}
}

// Execute routes a non-streaming query.
func (rtr *Router) Execute(context context.Context, query *proto.Query) (*mproto.QueryResult, error) {
	plan := rtr.planner.GetPlan(string(query.Sql))
	switch plan.ID {
	case planbuilder.SelectUnsharded, planbuilder.UpdateUnsharded,
		planbuilder.DeleteUnsharded, planbuilder.InsertUnsharded:
		return rtr.execUnsharded(context, query, plan)
	case planbuilder.SelectEqual:
		return rtr.execSelectEqual(context, query, plan)
	case planbuilder.InsertSharded:
		return rtr.execInsertSharded(context, query, plan)
	default:
		return nil, fmt.Errorf("plan %+v unimplemented", plan)
	}
}

func (rtr *Router) execUnsharded(context context.Context, query *proto.Query, plan *planbuilder.Plan) (*mproto.QueryResult, error) {
	ks, allShards, err := getKeyspaceShards(rtr.serv, rtr.cell, plan.Table.Keyspace.Name, query.TabletType)
	if err != nil {
		return nil, err
	}
	if len(allShards) != 1 {
		return nil, fmt.Errorf("unsharded keyspace %s has multiple shards: %+v", ks, allShards)
	}
	shards := []string{allShards[0].ShardName()}
	return rtr.scatterConn.Execute(
		context,
		query.Sql,
		query.BindVariables,
		ks,
		shards,
		query.TabletType,
		NewSafeSession(query.Session))
}

func (rtr *Router) execSelectEqual(context context.Context, query *proto.Query, plan *planbuilder.Plan) (*mproto.QueryResult, error) {
	keys, err := rtr.resolveKeys([]interface{}{plan.Values}, query.BindVariables)
	if err != nil {
		return nil, err
	}
	ks, shardsToIDs, err := rtr.resolveShards(query.TabletType, keys, plan)
	shards := make([]string, 0, len(shardsToIDs))
	for k := range shardsToIDs {
		shards = append(shards, k)
	}
	return rtr.scatterConn.Execute(
		context,
		query.Sql,
		query.BindVariables,
		ks,
		shards,
		query.TabletType,
		NewSafeSession(query.Session))
}

func (rtr *Router) execInsertSharded(context context.Context, query *proto.Query, plan *planbuilder.Plan) (*mproto.QueryResult, error) {
	input := plan.Values.([]interface{})[:1]
	keys, err := rtr.resolveKeys(input, query.BindVariables)
	if err != nil {
		return nil, err
	}
	ks, shard, ksid, err := rtr.resolveSingle(query.TabletType, keys[0], plan)
	if query.BindVariables == nil {
		query.BindVariables = make(map[string]interface{})
	}
	query.BindVariables["keyspace_id"] = string(ksid)
	query.Sql = query.Sql + fmt.Sprintf(" /* _routing keyspace_id:%v */", ksid)
	return rtr.scatterConn.Execute(
		context,
		query.Sql,
		query.BindVariables,
		ks,
		[]string{shard},
		query.TabletType,
		NewSafeSession(query.Session))
}

func (rtr *Router) resolveKeys(vals []interface{}, bindVars map[string]interface{}) (keys []interface{}, err error) {
	keys = make([]interface{}, 0, len(vals))
	for _, val := range vals {
		switch val := val.(type) {
		case string:
			v, ok := bindVars[val[1:]]
			if !ok {
				return nil, fmt.Errorf("could not find bind var %s", val)
			}
			keys = append(keys, v)
		case sqltypes.Value:
			keys = append(keys, val)
		default:
			panic("unexpected")
		}
	}
	return keys, nil
}

func (rtr *Router) resolveShards(tabletType topo.TabletType, shardKeys []interface{}, plan *planbuilder.Plan) (newKeyspace string, shardsToIDs map[string][]interface{}, err error) {
	newKeyspace, allShards, err := getKeyspaceShards(rtr.serv, rtr.cell, plan.Table.Keyspace.Name, tabletType)
	if err != nil {
		return "", nil, err
	}
	shardsToIDs = make(map[string][]interface{})
	add := func(shard string, id interface{}) {
		ids, ok := shardsToIDs[shard]
		if !ok {
			ids = make([]interface{}, 0, 8)
			shardsToIDs[shard] = ids
		}
		// Check for duplicates
		for _, current := range ids {
			if id == current {
				return
			}
		}
		ids = append(ids, id)
	}
	switch mapper := plan.ColVindex.Vindex.(type) {
	case planbuilder.Unique:
		ksids, err := mapper.Map(nil, shardKeys)
		if err != nil {
			return "", nil, err
		}
		for i, ksid := range ksids {
			shard, err := getShardForKeyspaceId(allShards, ksid)
			if err != nil {
				return "", nil, err
			}
			add(shard, shardKeys[i])
		}
	case planbuilder.NonUnique:
		ksidss, err := mapper.Map(nil, shardKeys)
		if err != nil {
			return "", nil, err
		}
		for i, ksids := range ksidss {
			for _, ksid := range ksids {
				shard, err := getShardForKeyspaceId(allShards, ksid)
				if err != nil {
					return "", nil, err
				}
				add(shard, shardKeys[i])
			}
		}
	default:
		panic("unexpected")
	}
	return newKeyspace, shardsToIDs, nil
}

func (rtr *Router) resolveSingle(tabletType topo.TabletType, shardKey interface{}, plan *planbuilder.Plan) (newKeyspace, shard string, keyspace_id key.KeyspaceId, err error) {
	newKeyspace, allShards, err := getKeyspaceShards(rtr.serv, rtr.cell, plan.Table.Keyspace.Name, tabletType)
	if err != nil {
		return "", "", "", err
	}
	// TODO(sougou): clean up this hack,
	var vindex planbuilder.Vindex
	if plan.ID == planbuilder.InsertSharded {
		vindex = plan.Table.ColVindexes[0].Vindex
	} else {
		vindex = plan.ColVindex.Vindex
	}
	switch mapper := vindex.(type) {
	case planbuilder.Unique:
		ksids, err := mapper.Map(nil, []interface{}{shardKey})
		if err != nil {
			return "", "", "", err
		}
		keyspace_id = ksids[0]
		shard, err = getShardForKeyspaceId(allShards, keyspace_id)
		if err != nil {
			return "", "", "", err
		}
	default:
		panic("unexpected")
	}
	return newKeyspace, shard, keyspace_id, nil
}
