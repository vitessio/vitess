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
	result, err := plan.Instructions.Execute(vcursor, queryConstruct, make(map[string]interface{}), true)
	//Mapping the generated session back to VtGateSession object
	vcursor.session.copySession(session)
	return result, err
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
func (rtr *Router) ExecuteBatch(ctx context.Context, sqlList []string, bindVarsList []map[string]interface{}, keyspace string, tabletType topodatapb.TabletType, asTransaction bool, session *vtgatepb.Session, options *querypb.ExecuteOptions, execParallel bool) ([]sqltypes.QueryResponse, error) {
	queryBatchConstruct, err := queryinfo.NewQueryBatchConstruct(sqlList, keyspace, bindVarsList, asTransaction)
	if err != nil {
		return nil, err
	}
	plan, err := rtr.planner.GetBatchPlan(queryBatchConstruct, execParallel)
	if err != nil {
		return nil, err
	}
	vcursor := newQueryExecutor(ctx, tabletType, session, options, rtr)
	results, err := plan.Instructions.ExecuteBatch(vcursor, queryBatchConstruct, make(map[string]interface{}), true)
	//Mapping the generated session back to VtGateSession object
	vcursor.session.copySession(session)
	return results, err
}

// executeVIndex routes a non-streaming vindex query.
func (rtr *Router) executeVIndex(vcursor *queryExecutor, sql string, bindVars map[string]interface{}) (*sqltypes.Result, error) {
	if bindVars == nil {
		bindVars = make(map[string]interface{})
	}
	// We have to use an empty keyspace here, because vIndexes that call back can reference
	// any table.
	queryConstruct := queryinfo.NewQueryConstruct(sql, "", bindVars, false)
	plan, err := rtr.planner.GetPlan(sql, "", bindVars)
	if err != nil {
		return nil, err
	}

	return plan.Instructions.Execute(vcursor, queryConstruct, make(map[string]interface{}), true)
}

// streamExecuteVIndex routes a streaming vindex query.
func (rtr *Router) streamExecuteVIndex(vcursor *queryExecutor, sql string, bindVars map[string]interface{}, sendReply func(*sqltypes.Result) error) error {
	panic("streamExecuteVIndex::This method should not be called")
}

// executeBatchVIndex routes a non-streaming vindex queries.
func (rtr *Router) executeBatchVIndex(vcursor *queryExecutor, sqlList []string, bindVarsList []map[string]interface{}, asTransaction bool, execParallel bool) ([]sqltypes.QueryResponse, error) {
	queryBatchConstruct, err := queryinfo.NewQueryBatchConstruct(sqlList, "", bindVarsList, asTransaction)
	if err != nil {
		return nil, err
	}
	plan, err := rtr.planner.GetBatchPlan(queryBatchConstruct, execParallel)
	if err != nil {
		return nil, err
	}
	return plan.Instructions.ExecuteBatch(vcursor, queryBatchConstruct, make(map[string]interface{}), true)
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
