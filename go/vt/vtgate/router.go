// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

// This is a V3 file. Do not intermix with V2.

import (
	"fmt"

	"code.google.com/p/go.net/context"
	mproto "github.com/youtube/vitess/go/mysql/proto"
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
func (rtr *Router) Execute(ctx context.Context, query *proto.Query) (*mproto.QueryResult, error) {
	if query.BindVariables == nil {
		query.BindVariables = make(map[string]interface{})
	}
	vcursor := newRequestContext(ctx, query, rtr)
	plan := rtr.planner.GetPlan(string(query.Sql))
	switch plan.ID {
	case planbuilder.SelectUnsharded, planbuilder.UpdateUnsharded,
		planbuilder.DeleteUnsharded, planbuilder.InsertUnsharded:
		return rtr.execUnsharded(vcursor, plan)
	case planbuilder.SelectEqual:
		return rtr.execSelectEqual(vcursor, plan)
	case planbuilder.InsertSharded:
		return rtr.execInsertSharded(vcursor, plan)
	default:
		return nil, fmt.Errorf("plan %+v unimplemented", plan)
	}
}

func (rtr *Router) execUnsharded(vcursor *requestContext, plan *planbuilder.Plan) (*mproto.QueryResult, error) {
	ks, allShards, err := getKeyspaceShards(rtr.serv, rtr.cell, plan.Table.Keyspace.Name, vcursor.query.TabletType)
	if err != nil {
		return nil, err
	}
	if len(allShards) != 1 {
		return nil, fmt.Errorf("unsharded keyspace %s has multiple shards: %+v", ks, allShards)
	}
	shards := []string{allShards[0].ShardName()}
	return rtr.scatterConn.Execute(
		vcursor.ctx,
		vcursor.query.Sql,
		vcursor.query.BindVariables,
		ks,
		shards,
		vcursor.query.TabletType,
		NewSafeSession(vcursor.query.Session))
}

func (rtr *Router) execSelectEqual(vcursor *requestContext, plan *planbuilder.Plan) (*mproto.QueryResult, error) {
	keys, err := rtr.resolveKeys([]interface{}{plan.Values}, vcursor.query.BindVariables)
	if err != nil {
		return nil, err
	}
	ks, routing, err := rtr.resolveShards(vcursor, keys, plan)
	return rtr.scatterConn.Execute(
		vcursor.ctx,
		vcursor.query.Sql,
		vcursor.query.BindVariables,
		ks,
		routing.Shards(),
		vcursor.query.TabletType,
		NewSafeSession(vcursor.query.Session))
}

func (rtr *Router) execInsertSharded(vcursor *requestContext, plan *planbuilder.Plan) (*mproto.QueryResult, error) {
	input := plan.Values.([]interface{})[:1]
	keys, err := rtr.resolveKeys(input, vcursor.query.BindVariables)
	if err != nil {
		return nil, err
	}
	ksid, err := rtr.handlePrimary(vcursor, keys[0], plan.Table.ColVindexes[0], vcursor.query.BindVariables)
	if err != nil {
		return nil, err
	}
	ks, shard, err := rtr.getRouting(plan.Table.Keyspace.Name, vcursor.query.TabletType, ksid)
	if err != nil {
		return nil, err
	}
	if vcursor.query.BindVariables == nil {
		vcursor.query.BindVariables = make(map[string]interface{})
	}
	vcursor.query.BindVariables["keyspace_id"] = string(ksid)
	vcursor.query.Sql = vcursor.query.Sql + fmt.Sprintf(" /* _routing keyspace_id:%v */", ksid)
	return rtr.scatterConn.Execute(
		vcursor.ctx,
		vcursor.query.Sql,
		vcursor.query.BindVariables,
		ks,
		[]string{shard},
		vcursor.query.TabletType,
		NewSafeSession(vcursor.query.Session))
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
		default:
			keys = append(keys, val)
		}
	}
	return keys, nil
}

func (rtr *Router) resolveShards(vcursor *requestContext, vindexKeys []interface{}, plan *planbuilder.Plan) (newKeyspace string, routing routingMap, err error) {
	newKeyspace, allShards, err := getKeyspaceShards(rtr.serv, rtr.cell, plan.Table.Keyspace.Name, vcursor.query.TabletType)
	if err != nil {
		return "", nil, err
	}
	routing = make(routingMap)
	switch mapper := plan.ColVindex.Vindex.(type) {
	case planbuilder.Unique:
		ksids, err := mapper.Map(vcursor, vindexKeys)
		if err != nil {
			return "", nil, err
		}
		for i, ksid := range ksids {
			shard, err := getShardForKeyspaceId(allShards, ksid)
			if err != nil {
				return "", nil, err
			}
			routing.Add(shard, vindexKeys[i])
		}
	case planbuilder.NonUnique:
		ksidss, err := mapper.Map(vcursor, vindexKeys)
		if err != nil {
			return "", nil, err
		}
		for i, ksids := range ksidss {
			for _, ksid := range ksids {
				shard, err := getShardForKeyspaceId(allShards, ksid)
				if err != nil {
					return "", nil, err
				}
				routing.Add(shard, vindexKeys[i])
			}
		}
	default:
		panic("unexpected")
	}
	return newKeyspace, routing, nil
}

func (rtr *Router) handlePrimary(vcursor *requestContext, vindexKey interface{}, colVindex *planbuilder.ColVindex, bv map[string]interface{}) (ksid key.KeyspaceId, err error) {
	if colVindex.Owned {
		if vindexKey == nil {
			generator, ok := colVindex.Vindex.(planbuilder.FunctionalGenerator)
			if !ok {
				return "", fmt.Errorf("value must be supplied for column %s", colVindex.Col)
			}
			vindexKey, err = generator.Generate(vcursor)
			if err != nil {
				return "", err
			}
		} else {
			err = colVindex.Vindex.(planbuilder.Functional).Create(vcursor, vindexKey)
			if err != nil {
				return "", err
			}
		}
	}
	if vindexKey == nil {
		return "", fmt.Errorf("value must be supplied for column %s", colVindex.Col)
	}
	mapper := colVindex.Vindex.(planbuilder.Unique)
	ksids, err := mapper.Map(vcursor, []interface{}{vindexKey})
	if err != nil {
		return "", err
	}
	ksid = ksids[0]
	bv["_"+colVindex.Col] = vindexKey
	return ksid, nil
}

func (rtr *Router) getRouting(keyspace string, tabletType topo.TabletType, ksid key.KeyspaceId) (newKeyspace, shard string, err error) {
	newKeyspace, allShards, err := getKeyspaceShards(rtr.serv, rtr.cell, keyspace, tabletType)
	if err != nil {
		return "", "", err
	}
	shard, err = getShardForKeyspaceId(allShards, ksid)
	if err != nil {
		return "", "", err
	}
	return newKeyspace, shard, nil
}
