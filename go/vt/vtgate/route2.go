// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"fmt"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/vtgate/planbuilder"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

func (rtr *Router) execPlan(vcursor *requestContext, plan interface{}) (*sqltypes.Result, error) {
	switch plan := plan.(type) {
	case *planbuilder.Join:
		return rtr.execJoin(vcursor, plan)
	case *planbuilder.Route:
		return rtr.execRoute(vcursor, plan)
	}
	panic("unreachable")
}

func (rtr *Router) execJoin(vcursor *requestContext, join *planbuilder.Join) (*sqltypes.Result, error) {
	lresult, err := rtr.execPlan(vcursor, join.Left)
	if err != nil {
		return nil, err
	}
	saved := copyBindVars(vcursor.JoinVars)
	defer func() { vcursor.JoinVars = saved }()
	result := &sqltypes.Result{}
	if len(lresult.Rows) == 0 {
		// We still need field info from the RHS.
		// TODO(sougou): Change this to use an impossible query.
		for k := range join.Vars {
			vcursor.JoinVars[k] = nil
		}
		rresult, err := rtr.execPlan(vcursor, join.Right)
		if err != nil {
			return nil, err
		}
		result.Fields = joinFields(lresult.Fields, rresult.Fields, join.Cols)
		return result, nil
	}
	for _, lrow := range lresult.Rows {
		for k, col := range join.Vars {
			vcursor.JoinVars[k] = lrow[col]
		}
		rresult, err := rtr.execPlan(vcursor, join.Right)
		if err != nil {
			return nil, err
		}
		result.Fields = joinFields(lresult.Fields, rresult.Fields, join.Cols)
		if join.IsLeft && len(rresult.Rows) == 0 {
			rresult.Rows = [][]sqltypes.Value{make([]sqltypes.Value, len(rresult.Fields))}
		}
		for _, rrow := range rresult.Rows {
			result.Rows = append(result.Rows, joinRows(lrow, rrow, join.Cols))
		}
		result.RowsAffected += uint64(len(rresult.Rows))
	}
	return result, nil
}

func copyBindVars(bindVars map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{})
	for k, v := range bindVars {
		out[k] = v
	}
	return out
}

func joinFields(lfields, rfields []*querypb.Field, cols []int) []*querypb.Field {
	if lfields == nil || rfields == nil {
		return nil
	}
	fields := make([]*querypb.Field, len(cols))
	for i, index := range cols {
		if index < 0 {
			fields[i] = lfields[-index-1]
			continue
		}
		fields[i] = rfields[index-1]
	}
	return fields
}

func joinRows(lrow, rrow []sqltypes.Value, cols []int) []sqltypes.Value {
	row := make([]sqltypes.Value, len(cols))
	for i, index := range cols {
		if index < 0 {
			row[i] = lrow[-index-1]
			continue
		}
		row[i] = rrow[index-1]
	}
	return row
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
	saved := copyBindVars(vcursor.bindVars)
	defer func() { vcursor.bindVars = saved }()
	for k := range route.UseVars {
		vcursor.bindVars[k] = vcursor.JoinVars[k]
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

func (rtr *Router) streamExecPlan(vcursor *requestContext, plan interface{}, sendReply func(*sqltypes.Result) error) error {
	switch plan := plan.(type) {
	case *planbuilder.Join:
		return rtr.streamExecPlan(vcursor, plan, sendReply)
	case *planbuilder.Route:
		return rtr.streamExecRoute(vcursor, plan, sendReply)
	}
	panic("unreachable")
}

func (rtr *Router) streamExecJoin(vcursor *requestContext, join *planbuilder.Join, sendReply func(*sqltypes.Result) error) error {
	fieldSent := false
	var rhsLen int
	err := rtr.streamExecPlan(vcursor, join.Left, func(lresult *sqltypes.Result) error {
		saved := copyBindVars(vcursor.JoinVars)
		defer func() { vcursor.JoinVars = saved }()
		for _, lrow := range lresult.Rows {
			for k, col := range join.Vars {
				vcursor.JoinVars[k] = lrow[col]
			}
			rowSent := false
			err := rtr.streamExecPlan(vcursor, join.Right, func(rresult *sqltypes.Result) error {
				result := &sqltypes.Result{}
				if !fieldSent {
					result.Fields = joinFields(lresult.Fields, rresult.Fields, join.Cols)
					fieldSent = true
					rhsLen = len(rresult.Fields)
				}
				if len(result.Rows) != 0 {
					rowSent = true
				}
				for _, rrow := range rresult.Rows {
					result.Rows = append(result.Rows, joinRows(lrow, rrow, join.Cols))
				}
				return sendReply(result)
			})
			if err != nil {
				return err
			}
			if join.IsLeft && !rowSent {
				result := &sqltypes.Result{}
				result.Rows = [][]sqltypes.Value{joinRows(
					lrow,
					make([]sqltypes.Value, rhsLen),
					join.Cols,
				)}
				err := sendReply(result)
				if err != nil {
					return err
				}
			}
		}
		if !fieldSent {
			for k := range join.Vars {
				vcursor.JoinVars[k] = nil
			}
			return rtr.streamExecPlan(vcursor, join.Right, func(rresult *sqltypes.Result) error {
				result := &sqltypes.Result{}
				result.Fields = joinFields(lresult.Fields, rresult.Fields, join.Cols)
				fieldSent = true
				rhsLen = len(rresult.Fields)
				return sendReply(result)
			})
		}
		return nil
	})
	return err
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
	saved := copyBindVars(vcursor.bindVars)
	defer func() { vcursor.bindVars = saved }()
	for k := range route.UseVars {
		vcursor.bindVars[k] = vcursor.JoinVars[k]
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
	return newScatterParams(vcursor.sql, ks, vcursor.bindVars, []string{allShards[0].Name}), nil
}

func (rtr *Router) paramsSelectEqual2(vcursor *requestContext, route *planbuilder.Route) (*scatterParams, error) {
	keys, err := rtr.resolveKeys([]interface{}{route.Values}, vcursor.bindVars)
	if err != nil {
		return nil, fmt.Errorf("paramsSelectEqual: %v", err)
	}
	ks, routing, err := rtr.resolveShards2(vcursor, keys, route)
	if err != nil {
		return nil, fmt.Errorf("paramsSelectEqual: %v", err)
	}
	return newScatterParams(route.Query, ks, vcursor.bindVars, routing.Shards()), nil
}

func (rtr *Router) paramsSelectIN2(vcursor *requestContext, route *planbuilder.Route) (*scatterParams, error) {
	keys, err := rtr.resolveKeys(route.Values.([]interface{}), vcursor.bindVars)
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
		shardVars: routing.ShardVars(vcursor.bindVars),
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
	return newScatterParams(route.Query, ks, vcursor.bindVars, shards), nil
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
