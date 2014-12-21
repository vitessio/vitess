// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

// This is a V3 file. Do not intermix with V2.

import (
	"fmt"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtgate/planbuilder"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
	"golang.org/x/net/context"
)

const (
	ksidName   = "keyspace_id"
	dmlPostfix = " /* _routing keyspace_id:%v */"
)

// Router is the layer to route queries to the correct shards
// based on the values in the query.
type Router struct {
	serv        SrvTopoServer
	cell        string
	planner     *Planner
	scatterConn *ScatterConn
}

type scatterParams struct {
	query, ks string
	shardVars map[string]map[string]interface{}
}

func newScatterParams(query, ks string, bv map[string]interface{}, shards []string) *scatterParams {
	shardVars := make(map[string]map[string]interface{}, len(shards))
	for _, shard := range shards {
		shardVars[shard] = bv
	}
	return &scatterParams{
		query:     query,
		ks:        ks,
		shardVars: shardVars,
	}
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
	case planbuilder.UpdateEqual:
		return rtr.execUpdateEqual(vcursor, plan)
	case planbuilder.DeleteEqual:
		return rtr.execDeleteEqual(vcursor, plan)
	case planbuilder.InsertSharded:
		return rtr.execInsertSharded(vcursor, plan)
	}

	var err error
	var params *scatterParams
	switch plan.ID {
	case planbuilder.SelectUnsharded, planbuilder.UpdateUnsharded,
		planbuilder.DeleteUnsharded, planbuilder.InsertUnsharded:
		params, err = rtr.paramsUnsharded(vcursor, plan)
	case planbuilder.SelectEqual:
		params, err = rtr.paramsSelectEqual(vcursor, plan)
	case planbuilder.SelectIN:
		params, err = rtr.paramsSelectIN(vcursor, plan)
	case planbuilder.SelectKeyrange:
		params, err = rtr.paramsSelectKeyrange(vcursor, plan)
	case planbuilder.SelectScatter:
		params, err = rtr.paramsSelectScatter(vcursor, plan)
	default:
		panic("unexpected")
	}
	if err != nil {
		return nil, err
	}
	return rtr.scatterConn.ExecuteMulti(
		ctx,
		params.query,
		params.ks,
		params.shardVars,
		query.TabletType,
		NewSafeSession(vcursor.query.Session),
	)
}

// StreamExecute executes a streaming query.
func (rtr *Router) StreamExecute(ctx context.Context, query *proto.Query, sendReply func(*mproto.QueryResult) error) error {
	if query.BindVariables == nil {
		query.BindVariables = make(map[string]interface{})
	}
	vcursor := newRequestContext(ctx, query, rtr)
	plan := rtr.planner.GetPlan(string(query.Sql))

	var err error
	var params *scatterParams
	switch plan.ID {
	case planbuilder.SelectUnsharded:
		params, err = rtr.paramsUnsharded(vcursor, plan)
	case planbuilder.SelectEqual:
		params, err = rtr.paramsSelectEqual(vcursor, plan)
	case planbuilder.SelectIN:
		params, err = rtr.paramsSelectIN(vcursor, plan)
	case planbuilder.SelectKeyrange:
		params, err = rtr.paramsSelectKeyrange(vcursor, plan)
	case planbuilder.SelectScatter:
		params, err = rtr.paramsSelectScatter(vcursor, plan)
	default:
		return fmt.Errorf("query %q cannot be used for streaming", query.Sql)
	}
	if err != nil {
		return err
	}
	return rtr.scatterConn.StreamExecuteMulti(
		ctx,
		params.query,
		params.ks,
		params.shardVars,
		query.TabletType,
		NewSafeSession(vcursor.query.Session),
		sendReply,
	)
}

func (rtr *Router) paramsUnsharded(vcursor *requestContext, plan *planbuilder.Plan) (*scatterParams, error) {
	ks, allShards, err := getKeyspaceShards(vcursor.ctx, rtr.serv, rtr.cell, plan.Table.Keyspace.Name, vcursor.query.TabletType)
	if err != nil {
		return nil, fmt.Errorf("paramsUnsharded: %v", err)
	}
	if len(allShards) != 1 {
		return nil, fmt.Errorf("unsharded keyspace %s has multiple shards", ks)
	}
	return newScatterParams(vcursor.query.Sql, ks, vcursor.query.BindVariables, []string{allShards[0].ShardName()}), nil
}

func (rtr *Router) paramsSelectEqual(vcursor *requestContext, plan *planbuilder.Plan) (*scatterParams, error) {
	keys, err := rtr.resolveKeys([]interface{}{plan.Values}, vcursor.query.BindVariables)
	if err != nil {
		return nil, fmt.Errorf("paramsSelectEqual: %v", err)
	}
	ks, routing, err := rtr.resolveShards(vcursor, keys, plan)
	return newScatterParams(plan.Rewritten, ks, vcursor.query.BindVariables, routing.Shards()), nil
}

func (rtr *Router) paramsSelectIN(vcursor *requestContext, plan *planbuilder.Plan) (*scatterParams, error) {
	keys, err := rtr.resolveKeys(plan.Values.([]interface{}), vcursor.query.BindVariables)
	if err != nil {
		return nil, fmt.Errorf("paramsSelectIN: %v", err)
	}
	ks, routing, err := rtr.resolveShards(vcursor, keys, plan)
	return &scatterParams{
		query:     plan.Rewritten,
		ks:        ks,
		shardVars: routing.ShardVars(vcursor.query.BindVariables),
	}, nil
}

func (rtr *Router) paramsSelectKeyrange(vcursor *requestContext, plan *planbuilder.Plan) (*scatterParams, error) {
	keys, err := rtr.resolveKeys(plan.Values.([]interface{}), vcursor.query.BindVariables)
	if err != nil {
		return nil, fmt.Errorf("paramsSelectKeyrange: %v", err)
	}
	kr, err := getKeyRange(keys)
	if err != nil {
		return nil, fmt.Errorf("paramsSelectKeyrange: %v", err)
	}
	ks, shards, err := mapExactShards(vcursor.ctx, rtr.serv, rtr.cell, plan.Table.Keyspace.Name, vcursor.query.TabletType, kr)
	if err != nil {
		return nil, fmt.Errorf("paramsSelectKeyrange: %v", err)
	}
	if len(shards) != 1 {
		return nil, fmt.Errorf("keyrange must match exactly one shard: %+v", keys)
	}
	return newScatterParams(plan.Rewritten, ks, vcursor.query.BindVariables, shards), nil
}

func getKeyRange(keys []interface{}) (key.KeyRange, error) {
	var ksids []key.KeyspaceId
	for _, k := range keys {
		switch k := k.(type) {
		case string:
			ksids = append(ksids, key.KeyspaceId(k))
		default:
			return key.KeyRange{}, fmt.Errorf("expecting strings for keyrange: %+v", keys)
		}
	}
	return key.KeyRange{
		Start: ksids[0],
		End:   ksids[1],
	}, nil
}

func (rtr *Router) paramsSelectScatter(vcursor *requestContext, plan *planbuilder.Plan) (*scatterParams, error) {
	ks, allShards, err := getKeyspaceShards(vcursor.ctx, rtr.serv, rtr.cell, plan.Table.Keyspace.Name, vcursor.query.TabletType)
	if err != nil {
		return nil, fmt.Errorf("paramsSelectScatter: %v", err)
	}
	var shards []string
	for _, shard := range allShards {
		shards = append(shards, shard.ShardName())
	}
	return newScatterParams(plan.Rewritten, ks, vcursor.query.BindVariables, shards), nil
}

func (rtr *Router) execUpdateEqual(vcursor *requestContext, plan *planbuilder.Plan) (*mproto.QueryResult, error) {
	keys, err := rtr.resolveKeys([]interface{}{plan.Values}, vcursor.query.BindVariables)
	if err != nil {
		return nil, fmt.Errorf("execUpdateEqual: %v", err)
	}
	ks, shard, ksid, err := rtr.resolveSingleShard(vcursor, keys[0], plan)
	if err != nil {
		return nil, fmt.Errorf("execUpdateEqual: %v", err)
	}
	if ksid == key.MinKey {
		return &mproto.QueryResult{}, nil
	}
	vcursor.query.BindVariables[ksidName] = string(ksid)
	rewritten := plan.Rewritten + fmt.Sprintf(dmlPostfix, ksid)
	return rtr.scatterConn.Execute(
		vcursor.ctx,
		rewritten,
		vcursor.query.BindVariables,
		ks,
		[]string{shard},
		vcursor.query.TabletType,
		NewSafeSession(vcursor.query.Session))
}

func (rtr *Router) execDeleteEqual(vcursor *requestContext, plan *planbuilder.Plan) (*mproto.QueryResult, error) {
	keys, err := rtr.resolveKeys([]interface{}{plan.Values}, vcursor.query.BindVariables)
	if err != nil {
		return nil, fmt.Errorf("execDeleteEqual: %v", err)
	}
	ks, shard, ksid, err := rtr.resolveSingleShard(vcursor, keys[0], plan)
	if err != nil {
		return nil, fmt.Errorf("execDeleteEqual: %v", err)
	}
	if ksid == key.MinKey {
		return &mproto.QueryResult{}, nil
	}
	if plan.Subquery != "" {
		err = rtr.deleteVindexEntries(vcursor, plan, ks, shard, ksid)
		if err != nil {
			return nil, fmt.Errorf("execDeleteEqual: %v", err)
		}
	}
	vcursor.query.BindVariables[ksidName] = string(ksid)
	rewritten := plan.Rewritten + fmt.Sprintf(dmlPostfix, ksid)
	return rtr.scatterConn.Execute(
		vcursor.ctx,
		rewritten,
		vcursor.query.BindVariables,
		ks,
		[]string{shard},
		vcursor.query.TabletType,
		NewSafeSession(vcursor.query.Session))
}

func (rtr *Router) execInsertSharded(vcursor *requestContext, plan *planbuilder.Plan) (*mproto.QueryResult, error) {
	input := plan.Values.([]interface{})
	keys, err := rtr.resolveKeys(input, vcursor.query.BindVariables)
	if err != nil {
		return nil, fmt.Errorf("execInsertSharded: %v", err)
	}
	ksid, generated, err := rtr.handlePrimary(vcursor, keys[0], plan.Table.ColVindexes[0], vcursor.query.BindVariables)
	if err != nil {
		return nil, fmt.Errorf("execInsertSharded: %v", err)
	}
	ks, shard, err := rtr.getRouting(vcursor.ctx, plan.Table.Keyspace.Name, vcursor.query.TabletType, ksid)
	if err != nil {
		return nil, fmt.Errorf("execInsertSharded: %v", err)
	}
	for i := 1; i < len(keys); i++ {
		newgen, err := rtr.handleNonPrimary(vcursor, keys[i], plan.Table.ColVindexes[i], vcursor.query.BindVariables, ksid)
		if err != nil {
			return nil, err
		}
		if newgen != 0 {
			if generated != 0 {
				return nil, fmt.Errorf("insert generated more than one value")
			}
			generated = newgen
		}
	}
	vcursor.query.BindVariables[ksidName] = string(ksid)
	rewritten := plan.Rewritten + fmt.Sprintf(dmlPostfix, ksid)
	result, err := rtr.scatterConn.Execute(
		vcursor.ctx,
		rewritten,
		vcursor.query.BindVariables,
		ks,
		[]string{shard},
		vcursor.query.TabletType,
		NewSafeSession(vcursor.query.Session))
	if err != nil {
		return nil, fmt.Errorf("execInsertSharded: %v", err)
	}
	if generated != 0 {
		if result.InsertId != 0 {
			return nil, fmt.Errorf("vindex and db generated a value each for insert")
		}
		result.InsertId = uint64(generated)
	}
	return result, nil
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
		case []byte:
			keys = append(keys, string(val))
		default:
			keys = append(keys, val)
		}
	}
	return keys, nil
}

func (rtr *Router) resolveShards(vcursor *requestContext, vindexKeys []interface{}, plan *planbuilder.Plan) (newKeyspace string, routing routingMap, err error) {
	newKeyspace, allShards, err := getKeyspaceShards(vcursor.ctx, rtr.serv, rtr.cell, plan.Table.Keyspace.Name, vcursor.query.TabletType)
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
			if ksid == key.MinKey {
				continue
			}
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
				if ksid == key.MinKey {
					continue
				}
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

func (rtr *Router) resolveSingleShard(vcursor *requestContext, vindexKey interface{}, plan *planbuilder.Plan) (newKeyspace, shard string, ksid key.KeyspaceId, err error) {
	newKeyspace, allShards, err := getKeyspaceShards(vcursor.ctx, rtr.serv, rtr.cell, plan.Table.Keyspace.Name, vcursor.query.TabletType)
	if err != nil {
		return "", "", "", err
	}
	mapper, ok := plan.ColVindex.Vindex.(planbuilder.Unique)
	if !ok {
		panic("unexpected")
	}
	ksids, err := mapper.Map(vcursor, []interface{}{vindexKey})
	if err != nil {
		return "", "", "", err
	}
	if len(ksids) != 1 {
		panic("unexpected")
	}
	ksid = ksids[0]
	if ksid == key.MinKey {
		return "", "", ksid, nil
	}
	shard, err = getShardForKeyspaceId(allShards, ksid)
	if err != nil {
		return "", "", "", err
	}
	return newKeyspace, shard, ksid, nil
}

func (rtr *Router) deleteVindexEntries(vcursor *requestContext, plan *planbuilder.Plan, ks, shard string, ksid key.KeyspaceId) error {
	result, err := rtr.scatterConn.Execute(
		vcursor.ctx,
		plan.Subquery,
		vcursor.query.BindVariables,
		ks,
		[]string{shard},
		vcursor.query.TabletType,
		NewSafeSession(vcursor.query.Session))
	if err != nil {
		return err
	}
	if len(result.Rows) == 0 {
		return nil
	}
	if len(result.Rows[0]) != len(plan.Table.Owned) {
		panic("unexpected")
	}
	for i, colVindex := range plan.Table.Owned {
		keys := make(map[interface{}]bool)
		for _, row := range result.Rows {
			k, err := mproto.Convert(result.Fields[i].Type, row[i])
			if err != nil {
				return err
			}
			switch k := k.(type) {
			case []byte:
				keys[string(k)] = true
			default:
				keys[k] = true
			}
		}
		var ids []interface{}
		for k := range keys {
			ids = append(ids, k)
		}
		switch vindex := colVindex.Vindex.(type) {
		case planbuilder.Functional:
			if err = vindex.Delete(vcursor, ids, ksid); err != nil {
				return err
			}
		case planbuilder.Lookup:
			if err = vindex.Delete(vcursor, ids, ksid); err != nil {
				return err
			}
		default:
			panic("unexpceted")
		}
	}
	return nil
}

func (rtr *Router) handlePrimary(vcursor *requestContext, vindexKey interface{}, colVindex *planbuilder.ColVindex, bv map[string]interface{}) (ksid key.KeyspaceId, generated int64, err error) {
	if colVindex.Owned {
		if vindexKey == nil {
			generator, ok := colVindex.Vindex.(planbuilder.FunctionalGenerator)
			if !ok {
				return "", 0, fmt.Errorf("value must be supplied for column %s", colVindex.Col)
			}
			generated, err = generator.Generate(vcursor)
			vindexKey = generated
			if err != nil {
				return "", 0, err
			}
		} else {
			err = colVindex.Vindex.(planbuilder.Functional).Create(vcursor, vindexKey)
			if err != nil {
				return "", 0, err
			}
		}
	}
	if vindexKey == nil {
		return "", 0, fmt.Errorf("value must be supplied for column %s", colVindex.Col)
	}
	mapper := colVindex.Vindex.(planbuilder.Unique)
	ksids, err := mapper.Map(vcursor, []interface{}{vindexKey})
	if err != nil {
		return "", 0, err
	}
	ksid = ksids[0]
	if ksid == key.MinKey {
		return "", 0, fmt.Errorf("could not map %v to a keyspace id", vindexKey)
	}
	bv["_"+colVindex.Col] = vindexKey
	return ksid, generated, nil
}

func (rtr *Router) handleNonPrimary(vcursor *requestContext, vindexKey interface{}, colVindex *planbuilder.ColVindex, bv map[string]interface{}, ksid key.KeyspaceId) (generated int64, err error) {
	if colVindex.Owned {
		if vindexKey == nil {
			generator, ok := colVindex.Vindex.(planbuilder.LookupGenerator)
			if !ok {
				return 0, fmt.Errorf("value must be supplied for column %s", colVindex.Col)
			}
			generated, err = generator.Generate(vcursor, ksid)
			vindexKey = generated
			if err != nil {
				return 0, err
			}
		} else {
			err = colVindex.Vindex.(planbuilder.Lookup).Create(vcursor, vindexKey, ksid)
			if err != nil {
				return 0, err
			}
		}
	} else {
		if vindexKey == nil {
			reversible, ok := colVindex.Vindex.(planbuilder.Reversible)
			if !ok {
				return 0, fmt.Errorf("value must be supplied for column %s", colVindex.Col)
			}
			vindexKey, err = reversible.ReverseMap(vcursor, ksid)
			if err != nil {
				return 0, err
			}
			if vindexKey == nil {
				return 0, fmt.Errorf("could not compute value for column %v", colVindex.Col)
			}
		} else {
			ok, err := colVindex.Vindex.Verify(vcursor, vindexKey, ksid)
			if err != nil {
				return 0, err
			}
			if !ok {
				return 0, fmt.Errorf("value %v for column %s does not map to keyspace id %v", vindexKey, colVindex.Col, ksid)
			}
		}
	}
	bv["_"+colVindex.Col] = vindexKey
	return generated, nil
}

func (rtr *Router) getRouting(ctx context.Context, keyspace string, tabletType topo.TabletType, ksid key.KeyspaceId) (newKeyspace, shard string, err error) {
	newKeyspace, allShards, err := getKeyspaceShards(ctx, rtr.serv, rtr.cell, keyspace, tabletType)
	if err != nil {
		return "", "", err
	}
	shard, err = getShardForKeyspaceId(allShards, ksid)
	if err != nil {
		return "", "", err
	}
	return newKeyspace, shard, nil
}
