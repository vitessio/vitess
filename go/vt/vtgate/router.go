// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

// This is a V3 file. Do not intermix with V2.

import (
	"fmt"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/context"
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
	case planbuilder.SelectSingleShardKey:
		return rtr.execSelectSingleShardKey(context, query, plan)
	default:
		return nil, fmt.Errorf("plan unimplemented")
	}
}

func (rtr *Router) execSelectSingleShardKey(context context.Context, query *proto.Query, plan *planbuilder.Plan) (*mproto.QueryResult, error) {
	// TODO(sougou): simplify this.
	hind := NewHashIndex(rtr.planner.schema.Tables[plan.TableName].Keyspace.Name, rtr.serv, rtr.cell)
	keys, err := resolveKeys([]interface{}{plan.Values}, query.BindVariables)
	if err != nil {
		return nil, err
	}
	ks, shards, err := hind.Resolve(query.TabletType, keys)
	if len(shards) != 1 {
		panic("unexpected")
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

func resolveKeys(vals []interface{}, bindVars map[string]interface{}) (keys []interface{}, err error) {
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
