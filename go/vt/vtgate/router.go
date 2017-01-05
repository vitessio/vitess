// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
	"github.com/youtube/vitess/go/vt/vtgate/queryinfo"
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
func (rtr *Router) StreamExecute(ctx context.Context, sql string, bindVars map[string]interface{}, keyspace string, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions, sendReply func(*sqltypes.Result) error) error {
	if bindVars == nil {
		bindVars = make(map[string]interface{})
	}
	vcursor := newQueryExecutor(ctx, tabletType, nil, options, rtr)
	queryConstruct := queryinfo.NewQueryConstruct(sql, keyspace, bindVars, false)
	plan, err := rtr.planner.GetPlan(sql, keyspace, bindVars)
	if err != nil {
		return err
	}
	return plan.Instructions.StreamExecute(vcursor, queryConstruct, make(map[string]interface{}), true, sendReply)
}

// ExecuteBatch routes a non-streaming queries.
func (rtr *Router) ExecuteBatch(ctx context.Context, sqlList []string, bindVarsList []map[string]interface{}, keyspace string, tabletType topodatapb.TabletType, asTransaction bool, session *vtgatepb.Session, options *querypb.ExecuteOptions) ([]sqltypes.QueryResponse, error) {
	if bindVarsList == nil {
		bindVarsList = make([]map[string]interface{}, len(sqlList))
	}
	queryResponseList := make([]sqltypes.QueryResponse, 0, len(sqlList))
	for sqlNum, query := range sqlList {
		var queryResponse sqltypes.QueryResponse

		bindVars := bindVarsList[sqlNum]
		if bindVars == nil {
			bindVars = make(map[string]interface{})
		}
		//Using same QueryExecutor -> marking notInTransaction as false and not using asTransaction flag
		vcursor := newQueryExecutor(ctx, tabletType, session, options, rtr)
		queryConstruct := queryinfo.NewQueryConstruct(query, keyspace, bindVars, false)
		plan, err := rtr.planner.GetPlan(query, keyspace, bindVars)
		if err != nil {
			queryResponse.QueryError = err
		} else {
			result, err := plan.Instructions.Execute(vcursor, queryConstruct, make(map[string]interface{}), true)
			queryResponse.QueryResult = result
			queryResponse.QueryError = err
		}
		queryResponseList = append(queryResponseList, queryResponse)
	}
	return queryResponseList, nil
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
