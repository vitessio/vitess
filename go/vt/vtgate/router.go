// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

// This is a V3 file. Do not intermix with V2.

import (
	"encoding/hex"
	"fmt"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/sqlannotation"
	"github.com/youtube/vitess/go/vt/vtgate/planbuilder"
	"golang.org/x/net/context"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
)

const (
	ksidName = "keyspace_id"
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
func (rtr *Router) Execute(ctx context.Context, sql string, bindVariables map[string]interface{}, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool) (*sqltypes.Result, error) {
	if bindVariables == nil {
		bindVariables = make(map[string]interface{})
	}
	vcursor := newRequestContext(ctx, sql, bindVariables, tabletType, session, notInTransaction, rtr)
	plan := rtr.planner.GetPlan(sql)

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
		return nil, fmt.Errorf("cannot route query: %s: %s", sql, plan.Reason)
	}
	if err != nil {
		return nil, err
	}
	return rtr.scatterConn.ExecuteMulti(
		ctx,
		params.query,
		params.ks,
		params.shardVars,
		tabletType,
		NewSafeSession(session),
		notInTransaction,
	)
}

// StreamExecute executes a streaming query.
func (rtr *Router) StreamExecute(ctx context.Context, sql string, bindVariables map[string]interface{}, tabletType topodatapb.TabletType, sendReply func(*sqltypes.Result) error) error {
	if bindVariables == nil {
		bindVariables = make(map[string]interface{})
	}
	vcursor := newRequestContext(ctx, sql, bindVariables, tabletType, nil, false, rtr)
	plan := rtr.planner.GetPlan(sql)

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
		return fmt.Errorf("query %q cannot be used for streaming", sql)
	}
	if err != nil {
		return err
	}
	return rtr.scatterConn.StreamExecuteMulti(
		ctx,
		params.query,
		params.ks,
		params.shardVars,
		tabletType,
		sendReply,
	)
}

func (rtr *Router) paramsUnsharded(vcursor *requestContext, plan *planbuilder.Plan) (*scatterParams, error) {
	ks, _, allShards, err := getKeyspaceShards(vcursor.ctx, rtr.serv, rtr.cell, plan.Table.Keyspace.Name, vcursor.tabletType)
	if err != nil {
		return nil, fmt.Errorf("paramsUnsharded: %v", err)
	}
	if len(allShards) != 1 {
		return nil, fmt.Errorf("unsharded keyspace %s has multiple shards", ks)
	}
	return newScatterParams(vcursor.sql, ks, vcursor.bindVariables, []string{allShards[0].Name}), nil
}

func (rtr *Router) paramsSelectEqual(vcursor *requestContext, plan *planbuilder.Plan) (*scatterParams, error) {
	keys, err := rtr.resolveKeys([]interface{}{plan.Values}, vcursor.bindVariables)
	if err != nil {
		return nil, fmt.Errorf("paramsSelectEqual: %v", err)
	}
	ks, routing, err := rtr.resolveShards(vcursor, keys, plan)
	if err != nil {
		return nil, fmt.Errorf("paramsSelectEqual: %v", err)
	}
	return newScatterParams(plan.Rewritten, ks, vcursor.bindVariables, routing.Shards()), nil
}

func (rtr *Router) paramsSelectIN(vcursor *requestContext, plan *planbuilder.Plan) (*scatterParams, error) {
	keys, err := rtr.resolveKeys(plan.Values.([]interface{}), vcursor.bindVariables)
	if err != nil {
		return nil, fmt.Errorf("paramsSelectIN: %v", err)
	}
	ks, routing, err := rtr.resolveShards(vcursor, keys, plan)
	if err != nil {
		return nil, fmt.Errorf("paramsSelectEqual: %v", err)
	}
	return &scatterParams{
		query:     plan.Rewritten,
		ks:        ks,
		shardVars: routing.ShardVars(vcursor.bindVariables),
	}, nil
}

func (rtr *Router) paramsSelectKeyrange(vcursor *requestContext, plan *planbuilder.Plan) (*scatterParams, error) {
	keys, err := rtr.resolveKeys(plan.Values.([]interface{}), vcursor.bindVariables)
	if err != nil {
		return nil, fmt.Errorf("paramsSelectKeyrange: %v", err)
	}
	kr, err := getKeyRange(keys)
	if err != nil {
		return nil, fmt.Errorf("paramsSelectKeyrange: %v", err)
	}
	ks, shards, err := mapExactShards(vcursor.ctx, rtr.serv, rtr.cell, plan.Table.Keyspace.Name, vcursor.tabletType, kr)
	if err != nil {
		return nil, fmt.Errorf("paramsSelectKeyrange: %v", err)
	}
	if len(shards) != 1 {
		return nil, fmt.Errorf("keyrange must match exactly one shard: %+v", keys)
	}
	return newScatterParams(plan.Rewritten, ks, vcursor.bindVariables, shards), nil
}

func getKeyRange(keys []interface{}) (*topodatapb.KeyRange, error) {
	var ksids [][]byte
	for _, k := range keys {
		switch k := k.(type) {
		case string:
			ksids = append(ksids, []byte(k))
		default:
			return nil, fmt.Errorf("expecting strings for keyrange: %+v", keys)
		}
	}
	return &topodatapb.KeyRange{
		Start: ksids[0],
		End:   ksids[1],
	}, nil
}

func (rtr *Router) paramsSelectScatter(vcursor *requestContext, plan *planbuilder.Plan) (*scatterParams, error) {
	ks, _, allShards, err := getKeyspaceShards(vcursor.ctx, rtr.serv, rtr.cell, plan.Table.Keyspace.Name, vcursor.tabletType)
	if err != nil {
		return nil, fmt.Errorf("paramsSelectScatter: %v", err)
	}
	var shards []string
	for _, shard := range allShards {
		shards = append(shards, shard.Name)
	}
	return newScatterParams(plan.Rewritten, ks, vcursor.bindVariables, shards), nil
}

func (rtr *Router) execUpdateEqual(vcursor *requestContext, plan *planbuilder.Plan) (*sqltypes.Result, error) {
	keys, err := rtr.resolveKeys([]interface{}{plan.Values}, vcursor.bindVariables)
	if err != nil {
		return nil, fmt.Errorf("execUpdateEqual: %v", err)
	}
	ks, shard, ksid, err := rtr.resolveSingleShard(vcursor, keys[0], plan)
	if err != nil {
		return nil, fmt.Errorf("execUpdateEqual: %v", err)
	}
	if len(ksid) == 0 {
		return &sqltypes.Result{}, nil
	}
	vcursor.bindVariables[ksidName] = string(ksid)
	rewritten := sqlannotation.AddKeyspaceID(plan.Rewritten, ksid)
	return rtr.scatterConn.Execute(
		vcursor.ctx,
		rewritten,
		vcursor.bindVariables,
		ks,
		[]string{shard},
		vcursor.tabletType,
		NewSafeSession(vcursor.session),
		vcursor.notInTransaction)
}

func (rtr *Router) execDeleteEqual(vcursor *requestContext, plan *planbuilder.Plan) (*sqltypes.Result, error) {
	keys, err := rtr.resolveKeys([]interface{}{plan.Values}, vcursor.bindVariables)
	if err != nil {
		return nil, fmt.Errorf("execDeleteEqual: %v", err)
	}
	ks, shard, ksid, err := rtr.resolveSingleShard(vcursor, keys[0], plan)
	if err != nil {
		return nil, fmt.Errorf("execDeleteEqual: %v", err)
	}
	if len(ksid) == 0 {
		return &sqltypes.Result{}, nil
	}
	if plan.Subquery != "" {
		err = rtr.deleteVindexEntries(vcursor, plan, ks, shard, ksid)
		if err != nil {
			return nil, fmt.Errorf("execDeleteEqual: %v", err)
		}
	}
	vcursor.bindVariables[ksidName] = string(ksid)
	rewritten := sqlannotation.AddKeyspaceID(plan.Rewritten, ksid)
	return rtr.scatterConn.Execute(
		vcursor.ctx,
		rewritten,
		vcursor.bindVariables,
		ks,
		[]string{shard},
		vcursor.tabletType,
		NewSafeSession(vcursor.session),
		vcursor.notInTransaction)
}

func (rtr *Router) execInsertSharded(vcursor *requestContext, plan *planbuilder.Plan) (*sqltypes.Result, error) {
	input := plan.Values.([]interface{})
	keys, err := rtr.resolveKeys(input, vcursor.bindVariables)
	if err != nil {
		return nil, fmt.Errorf("execInsertSharded: %v", err)
	}
	ksid, generated, err := rtr.handlePrimary(vcursor, keys[0], plan.Table.ColVindexes[0], vcursor.bindVariables)
	if err != nil {
		return nil, fmt.Errorf("execInsertSharded: %v", err)
	}
	ks, shard, err := rtr.getRouting(vcursor.ctx, plan.Table.Keyspace.Name, vcursor.tabletType, ksid)
	if err != nil {
		return nil, fmt.Errorf("execInsertSharded: %v", err)
	}
	for i := 1; i < len(keys); i++ {
		newgen, err := rtr.handleNonPrimary(vcursor, keys[i], plan.Table.ColVindexes[i], vcursor.bindVariables, ksid)
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
	vcursor.bindVariables[ksidName] = string(ksid)
	rewritten := sqlannotation.AddKeyspaceID(plan.Rewritten, ksid)
	result, err := rtr.scatterConn.Execute(
		vcursor.ctx,
		rewritten,
		vcursor.bindVariables,
		ks,
		[]string{shard},
		vcursor.tabletType,
		NewSafeSession(vcursor.session),
		vcursor.notInTransaction)
	if err != nil {
		return nil, fmt.Errorf("execInsertSharded: %v", err)
	}
	if generated != 0 {
		if result.InsertID != 0 {
			return nil, fmt.Errorf("vindex and db generated a value each for insert")
		}
		result.InsertID = uint64(generated)
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
	newKeyspace, _, allShards, err := getKeyspaceShards(vcursor.ctx, rtr.serv, rtr.cell, plan.Table.Keyspace.Name, vcursor.tabletType)
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
			if len(ksid) == 0 {
				continue
			}
			shard, err := getShardForKeyspaceID(allShards, ksid)
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
				shard, err := getShardForKeyspaceID(allShards, ksid)
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

func (rtr *Router) resolveSingleShard(vcursor *requestContext, vindexKey interface{}, plan *planbuilder.Plan) (newKeyspace, shard string, ksid []byte, err error) {
	newKeyspace, _, allShards, err := getKeyspaceShards(vcursor.ctx, rtr.serv, rtr.cell, plan.Table.Keyspace.Name, vcursor.tabletType)
	if err != nil {
		return "", "", nil, err
	}
	mapper := plan.ColVindex.Vindex.(planbuilder.Unique)
	ksids, err := mapper.Map(vcursor, []interface{}{vindexKey})
	if err != nil {
		return "", "", nil, err
	}
	ksid = ksids[0]
	if len(ksid) == 0 {
		return "", "", ksid, nil
	}
	shard, err = getShardForKeyspaceID(allShards, ksid)
	if err != nil {
		return "", "", nil, err
	}
	return newKeyspace, shard, ksid, nil
}

func (rtr *Router) deleteVindexEntries(vcursor *requestContext, plan *planbuilder.Plan, ks, shard string, ksid []byte) error {
	result, err := rtr.scatterConn.Execute(
		vcursor.ctx,
		plan.Subquery,
		vcursor.bindVariables,
		ks,
		[]string{shard},
		vcursor.tabletType,
		NewSafeSession(vcursor.session),
		vcursor.notInTransaction)
	if err != nil {
		return err
	}
	if len(result.Rows) == 0 {
		return nil
	}
	for i, colVindex := range plan.Table.Owned {
		keys := make(map[interface{}]bool)
		for _, row := range result.Rows {
			switch k := row[i].ToNative().(type) {
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

func (rtr *Router) handlePrimary(vcursor *requestContext, vindexKey interface{}, colVindex *planbuilder.ColVindex, bv map[string]interface{}) (ksid []byte, generated int64, err error) {
	if colVindex.Owned {
		if vindexKey == nil {
			generator, ok := colVindex.Vindex.(planbuilder.FunctionalGenerator)
			if !ok {
				return nil, 0, fmt.Errorf("value must be supplied for column %s", colVindex.Col)
			}
			generated, err = generator.Generate(vcursor)
			vindexKey = generated
			if err != nil {
				return nil, 0, err
			}
		} else {
			// TODO(sougou): I think we have to ignore dup key error if this was
			// an upsert. For now, I'm punting on this because this would be a very
			// uncommon use case. We should revisit this when work on v3 resumes.
			err = colVindex.Vindex.(planbuilder.Functional).Create(vcursor, vindexKey)
			if err != nil {
				return nil, 0, err
			}
		}
	}
	if vindexKey == nil {
		return nil, 0, fmt.Errorf("value must be supplied for column %s", colVindex.Col)
	}
	mapper := colVindex.Vindex.(planbuilder.Unique)
	ksids, err := mapper.Map(vcursor, []interface{}{vindexKey})
	if err != nil {
		return nil, 0, err
	}
	ksid = ksids[0]
	if len(ksid) == 0 {
		return nil, 0, fmt.Errorf("could not map %v to a keyspace id", vindexKey)
	}
	bv["_"+colVindex.Col] = vindexKey
	return ksid, generated, nil
}

func (rtr *Router) handleNonPrimary(vcursor *requestContext, vindexKey interface{}, colVindex *planbuilder.ColVindex, bv map[string]interface{}, ksid []byte) (generated int64, err error) {
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
				return 0, fmt.Errorf("value %v for column %s does not map to keyspace id %v", vindexKey, colVindex.Col, hex.EncodeToString(ksid))
			}
		}
	}
	bv["_"+colVindex.Col] = vindexKey
	return generated, nil
}

func (rtr *Router) getRouting(ctx context.Context, keyspace string, tabletType topodatapb.TabletType, ksid []byte) (newKeyspace, shard string, err error) {
	newKeyspace, _, allShards, err := getKeyspaceShards(ctx, rtr.serv, rtr.cell, keyspace, tabletType)
	if err != nil {
		return "", "", err
	}
	shard, err = getShardForKeyspaceID(allShards, ksid)
	if err != nil {
		return "", "", err
	}
	return newKeyspace, shard, nil
}
