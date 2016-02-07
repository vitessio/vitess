// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"errors"
	"fmt"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/vtgate/planbuilder"
)

// Execute2 routes a non-streaming query for the new plan.
func (rtr *Router) Execute2(vcursor *requestContext, plan interface{}) (*sqltypes.Result, error) {
	route, ok := plan.(*planbuilder.Route)
	if !ok {
		return nil, errors.New("no joins yet")
	}
	return rtr.execRoute(vcursor, route)
}

func (rtr *Router) execRoute(vcursor *requestContext, route *planbuilder.Route) (*sqltypes.Result, error) {
	var err error
	var params *scatterParams
	switch route.PlanID {
	case planbuilder.SelectUnsharded:
		params, err = rtr.paramsUnsharded2(vcursor, route)
	case planbuilder.SelectEqual, planbuilder.SelectEqualUnique:
		params, err = rtr.paramsSelectEqual2(vcursor, route)
	case planbuilder.SelectIN:
		params, err = rtr.paramsSelectIN2(vcursor, route)
	case planbuilder.SelectScatter:
		params, err = rtr.paramsSelectScatter2(vcursor, route)
	default:
		// TODO(sougou): improve error.
		return nil, fmt.Errorf("unsupported query plan: %v", route)
	}
	if err != nil {
		return nil, err
	}
	return rtr.scatterConn.ExecuteMulti(
		vcursor.ctx,
		params.query,
		params.ks,
		params.shardVars,
		vcursor.tabletType,
		NewSafeSession(vcursor.session),
		vcursor.notInTransaction,
	)
}

// StreamExecute2 executes a streaming query for the new plan.
func (rtr *Router) StreamExecute2(vcursor *requestContext, plan interface{}, sendReply func(*sqltypes.Result) error) error {
	route, ok := plan.(*planbuilder.Route)
	if !ok {
		return errors.New("no joins yet")
	}
	return rtr.streamExecRoute(vcursor, route, sendReply)
}

func (rtr *Router) streamExecRoute(vcursor *requestContext, route *planbuilder.Route, sendReply func(*sqltypes.Result) error) error {
	var err error
	var params *scatterParams
	switch route.PlanID {
	case planbuilder.SelectUnsharded:
		params, err = rtr.paramsUnsharded2(vcursor, route)
	case planbuilder.SelectEqual, planbuilder.SelectEqualUnique:
		params, err = rtr.paramsSelectEqual2(vcursor, route)
	case planbuilder.SelectIN:
		params, err = rtr.paramsSelectIN2(vcursor, route)
	case planbuilder.SelectScatter:
		params, err = rtr.paramsSelectScatter2(vcursor, route)
	default:
		// TODO(sougou): improve error.
		return fmt.Errorf("unsupported query plan: %v", route)
	}
	if err != nil {
		return err
	}
	return rtr.scatterConn.StreamExecuteMulti(
		vcursor.ctx,
		params.query,
		params.ks,
		params.shardVars,
		vcursor.tabletType,
		sendReply,
	)
}

func (rtr *Router) paramsUnsharded2(vcursor *requestContext, route *planbuilder.Route) (*scatterParams, error) {
	ks, _, allShards, err := getKeyspaceShards(vcursor.ctx, rtr.serv, rtr.cell, route.Keyspace.Name, vcursor.tabletType)
	if err != nil {
		return nil, fmt.Errorf("paramsUnsharded: %v", err)
	}
	if len(allShards) != 1 {
		return nil, fmt.Errorf("unsharded keyspace %s has multiple shards", ks)
	}
	return newScatterParams(vcursor.sql, ks, vcursor.bindVariables, []string{allShards[0].Name}), nil
}

func (rtr *Router) paramsSelectEqual2(vcursor *requestContext, route *planbuilder.Route) (*scatterParams, error) {
	keys, err := rtr.resolveKeys([]interface{}{route.Values}, vcursor.bindVariables)
	if err != nil {
		return nil, fmt.Errorf("paramsSelectEqual: %v", err)
	}
	ks, routing, err := rtr.resolveShards2(vcursor, keys, route)
	if err != nil {
		return nil, fmt.Errorf("paramsSelectEqual: %v", err)
	}
	return newScatterParams(route.Query, ks, vcursor.bindVariables, routing.Shards()), nil
}

func (rtr *Router) paramsSelectIN2(vcursor *requestContext, route *planbuilder.Route) (*scatterParams, error) {
	keys, err := rtr.resolveKeys(route.Values.([]interface{}), vcursor.bindVariables)
	if err != nil {
		return nil, fmt.Errorf("paramsSelectIN: %v", err)
	}
	ks, routing, err := rtr.resolveShards2(vcursor, keys, route)
	if err != nil {
		return nil, fmt.Errorf("paramsSelectEqual: %v", err)
	}
	return &scatterParams{
		query:     route.Query,
		ks:        ks,
		shardVars: routing.ShardVars(vcursor.bindVariables),
	}, nil
}

func (rtr *Router) paramsSelectScatter2(vcursor *requestContext, route *planbuilder.Route) (*scatterParams, error) {
	ks, _, allShards, err := getKeyspaceShards(vcursor.ctx, rtr.serv, rtr.cell, route.Keyspace.Name, vcursor.tabletType)
	if err != nil {
		return nil, fmt.Errorf("paramsSelectScatter: %v", err)
	}
	var shards []string
	for _, shard := range allShards {
		shards = append(shards, shard.Name)
	}
	return newScatterParams(route.Query, ks, vcursor.bindVariables, shards), nil
}

func (rtr *Router) resolveShards2(vcursor *requestContext, vindexKeys []interface{}, route *planbuilder.Route) (newKeyspace string, routing routingMap, err error) {
	newKeyspace, _, allShards, err := getKeyspaceShards(vcursor.ctx, rtr.serv, rtr.cell, route.Keyspace.Name, vcursor.tabletType)
	if err != nil {
		return "", nil, err
	}
	routing = make(routingMap)
	switch mapper := route.Vindex.(type) {
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
