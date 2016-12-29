// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"encoding/hex"
	"fmt"
	"strconv"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/sqlannotation"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtgate/engine"
	"github.com/youtube/vitess/go/vt/vtgate/vindexes"
	"golang.org/x/net/context"

	"strings"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
	"github.com/youtube/vitess/go/vt/tabletserver/querytypes"
)

// Router is the layer to route queries to the correct shards
// based on the values in the query.
type Router struct {
	serv        topo.SrvTopoServer
	cell        string
	planner     *Planner
	scatterConn *ScatterConn
}

type scatterParams struct {
	ks        string
	shardVars map[string]map[string]interface{}
}

func newScatterParams(ks string, bv map[string]interface{}, shards []string) *scatterParams {
	shardVars := make(map[string]map[string]interface{}, len(shards))
	for _, shard := range shards {
		shardVars[shard] = bv
	}
	return &scatterParams{
		ks:        ks,
		shardVars: shardVars,
	}
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
	vcursor := newQueryExecutor(ctx, sql, bindVars, keyspace, tabletType, session, notInTransaction, options, rtr)
	plan, err := rtr.planner.GetPlan(sql, keyspace, bindVars)
	if err != nil {
		return nil, err
	}
	return plan.Instructions.Execute(vcursor, make(map[string]interface{}), true)
}

// StreamExecute executes a streaming query.
func (rtr *Router) StreamExecute(ctx context.Context, sql string, bindVars map[string]interface{}, keyspace string, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions, sendReply func(*sqltypes.Result) error) error {
	if bindVars == nil {
		bindVars = make(map[string]interface{})
	}
	vcursor := newQueryExecutor(ctx, sql, bindVars, keyspace, tabletType, nil, false, options, rtr)
	plan, err := rtr.planner.GetPlan(sql, keyspace, bindVars)
	if err != nil {
		return err
	}
	return plan.Instructions.StreamExecute(vcursor, make(map[string]interface{}), true, sendReply)
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
		vcursor := newQueryExecutor(ctx, query, bindVars, keyspace, tabletType, session, false, options, rtr)
		plan, err := rtr.planner.GetPlan(query, keyspace, bindVars)
		if err != nil {
			queryResponse.QueryError = err
		} else {
			result, err := plan.Instructions.Execute(vcursor, make(map[string]interface{}), true)
			queryResponse.QueryResult = result
			queryResponse.QueryError = err
		}
		queryResponseList = append(queryResponseList, queryResponse)
	}
	return queryResponseList, nil
}

// ExecuteRoute executes the route query for all route opcodes.
func (rtr *Router) ExecuteRoute(vcursor *queryExecutor, route *engine.Route, joinvars map[string]interface{}) (*sqltypes.Result, error) {
	saved := copyBindVars(vcursor.bindVars)
	defer func() { vcursor.bindVars = saved }()
	for k, v := range joinvars {
		vcursor.bindVars[k] = v
	}

	switch route.Opcode {
	case engine.UpdateEqual:
		return rtr.execUpdateEqual(vcursor, route)
	case engine.DeleteEqual:
		return rtr.execDeleteEqual(vcursor, route)
	case engine.InsertSharded:
		return rtr.execInsertSharded(vcursor, route)
	case engine.InsertUnsharded:
		return rtr.execInsertUnsharded(vcursor, route)
	}

	var err error
	var params *scatterParams
	switch route.Opcode {
	case engine.SelectUnsharded, engine.UpdateUnsharded,
		engine.DeleteUnsharded:
		params, err = rtr.paramsUnsharded(vcursor, route)
	case engine.SelectEqual, engine.SelectEqualUnique:
		params, err = rtr.paramsSelectEqual(vcursor, route)
	case engine.SelectIN:
		params, err = rtr.paramsSelectIN(vcursor, route)
	case engine.SelectScatter:
		params, err = rtr.paramsSelectScatter(vcursor, route)
	default:
		// TODO(sougou): improve error.
		return nil, fmt.Errorf("unsupported query route: %v", route)
	}
	if err != nil {
		return nil, err
	}

	shardQueries := rtr.getShardQueries(vcursor, route.Query+vcursor.comments, params)
	return rtr.scatterConn.ExecuteMultiShard(
		vcursor.ctx,
		params.ks,
		shardQueries,
		vcursor.tabletType,
		NewSafeSession(vcursor.session),
		vcursor.notInTransaction,
		vcursor.options,
	)
}

func copyBindVars(bindVars map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{})
	for k, v := range bindVars {
		out[k] = v
	}
	return out
}

// GetRouteFields fetches the field info for the route.
func (rtr *Router) GetRouteFields(vcursor *queryExecutor, route *engine.Route, joinvars map[string]interface{}) (*sqltypes.Result, error) {
	saved := copyBindVars(vcursor.bindVars)
	defer func() { vcursor.bindVars = saved }()
	for k := range joinvars {
		vcursor.bindVars[k] = nil
	}
	ks, shard, err := getAnyShard(vcursor.ctx, rtr.serv, rtr.cell, route.Keyspace.Name, vcursor.tabletType)
	if err != nil {
		return nil, err
	}

	return rtr.scatterConn.Execute(
		vcursor.ctx,
		route.FieldQuery,
		vcursor.bindVars,
		ks,
		[]string{shard},
		vcursor.tabletType,
		NewSafeSession(vcursor.session),
		vcursor.notInTransaction,
		vcursor.options,
	)
}

// StreamExecuteRoute performs a streaming route. Only selects are allowed.
func (rtr *Router) StreamExecuteRoute(vcursor *queryExecutor, route *engine.Route, joinvars map[string]interface{}, sendReply func(*sqltypes.Result) error) error {
	saved := copyBindVars(vcursor.bindVars)
	defer func() { vcursor.bindVars = saved }()
	for k, v := range joinvars {
		vcursor.bindVars[k] = v
	}

	var err error
	var params *scatterParams
	switch route.Opcode {
	case engine.SelectUnsharded:
		params, err = rtr.paramsUnsharded(vcursor, route)
	case engine.SelectEqual, engine.SelectEqualUnique:
		params, err = rtr.paramsSelectEqual(vcursor, route)
	case engine.SelectIN:
		params, err = rtr.paramsSelectIN(vcursor, route)
	case engine.SelectScatter:
		params, err = rtr.paramsSelectScatter(vcursor, route)
	default:
		return fmt.Errorf("query %q cannot be used for streaming", route.Query)
	}
	if err != nil {
		return err
	}
	return rtr.scatterConn.StreamExecuteMulti(
		vcursor.ctx,
		route.Query+vcursor.comments,
		params.ks,
		params.shardVars,
		vcursor.tabletType,
		vcursor.options,
		sendReply,
	)
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

func (rtr *Router) paramsUnsharded(vcursor *queryExecutor, route *engine.Route) (*scatterParams, error) {
	ks, _, allShards, err := getKeyspaceShards(vcursor.ctx, rtr.serv, rtr.cell, route.Keyspace.Name, vcursor.tabletType)
	if err != nil {
		return nil, fmt.Errorf("paramsUnsharded: %v", err)
	}
	if len(allShards) != 1 {
		return nil, fmt.Errorf("unsharded keyspace %s has multiple shards", ks)
	}
	return newScatterParams(ks, vcursor.bindVars, []string{allShards[0].Name}), nil
}

func (rtr *Router) paramsSelectEqual(vcursor *queryExecutor, route *engine.Route) (*scatterParams, error) {
	keys, err := rtr.resolveKeys([]interface{}{route.Values}, vcursor.bindVars)
	if err != nil {
		return nil, fmt.Errorf("paramsSelectEqual: %v", err)
	}
	ks, routing, err := rtr.resolveShards(vcursor, keys, route)
	if err != nil {
		return nil, fmt.Errorf("paramsSelectEqual: %v", err)
	}
	return newScatterParams(ks, vcursor.bindVars, routing.Shards()), nil
}

func (rtr *Router) paramsSelectIN(vcursor *queryExecutor, route *engine.Route) (*scatterParams, error) {
	vals, err := rtr.resolveList(route.Values, vcursor.bindVars)
	if err != nil {
		return nil, fmt.Errorf("paramsSelectIN: %v", err)
	}
	keys, err := rtr.resolveKeys(vals, vcursor.bindVars)
	if err != nil {
		return nil, fmt.Errorf("paramsSelectIN: %v", err)
	}
	ks, routing, err := rtr.resolveShards(vcursor, keys, route)
	if err != nil {
		return nil, fmt.Errorf("paramsSelectEqual: %v", err)
	}
	return &scatterParams{
		ks:        ks,
		shardVars: routing.ShardVars(vcursor.bindVars),
	}, nil
}

func (rtr *Router) paramsSelectScatter(vcursor *queryExecutor, route *engine.Route) (*scatterParams, error) {
	ks, _, allShards, err := getKeyspaceShards(vcursor.ctx, rtr.serv, rtr.cell, route.Keyspace.Name, vcursor.tabletType)
	if err != nil {
		return nil, fmt.Errorf("paramsSelectScatter: %v", err)
	}
	var shards []string
	for _, shard := range allShards {
		shards = append(shards, shard.Name)
	}
	return newScatterParams(ks, vcursor.bindVars, shards), nil
}

func (rtr *Router) execUpdateEqual(vcursor *queryExecutor, route *engine.Route) (*sqltypes.Result, error) {
	keys, err := rtr.resolveKeys([]interface{}{route.Values}, vcursor.bindVars)
	if err != nil {
		return nil, fmt.Errorf("execUpdateEqual: %v", err)
	}
	ks, shard, ksid, err := rtr.resolveSingleShard(vcursor, keys[0], route)
	if err != nil {
		return nil, fmt.Errorf("execUpdateEqual: %v", err)
	}
	if len(ksid) == 0 {
		return &sqltypes.Result{}, nil
	}
	rewritten := sqlannotation.AddKeyspaceIDs(route.Query, [][]byte{ksid}, vcursor.comments)
	return rtr.scatterConn.Execute(
		vcursor.ctx,
		rewritten,
		vcursor.bindVars,
		ks,
		[]string{shard},
		vcursor.tabletType,
		NewSafeSession(vcursor.session),
		vcursor.notInTransaction,
		vcursor.options)
}

func (rtr *Router) execDeleteEqual(vcursor *queryExecutor, route *engine.Route) (*sqltypes.Result, error) {
	keys, err := rtr.resolveKeys([]interface{}{route.Values}, vcursor.bindVars)
	if err != nil {
		return nil, fmt.Errorf("execDeleteEqual: %v", err)
	}
	ks, shard, ksid, err := rtr.resolveSingleShard(vcursor, keys[0], route)
	if err != nil {
		return nil, fmt.Errorf("execDeleteEqual: %v", err)
	}
	if len(ksid) == 0 {
		return &sqltypes.Result{}, nil
	}
	if route.Subquery != "" {
		err = rtr.deleteVindexEntries(vcursor, route, ks, shard, ksid)
		if err != nil {
			return nil, fmt.Errorf("execDeleteEqual: %v", err)
		}
	}
	rewritten := sqlannotation.AddKeyspaceIDs(route.Query, [][]byte{ksid}, vcursor.comments)
	return rtr.scatterConn.Execute(
		vcursor.ctx,
		rewritten,
		vcursor.bindVars,
		ks,
		[]string{shard},
		vcursor.tabletType,
		NewSafeSession(vcursor.session),
		vcursor.notInTransaction,
		vcursor.options)
}

func (rtr *Router) execInsertUnsharded(vcursor *queryExecutor, route *engine.Route) (*sqltypes.Result, error) {
	insertid, err := rtr.handleGenerate(vcursor, route.Generate)
	if err != nil {
		return nil, fmt.Errorf("execInsertUnsharded: %v", err)
	}
	params, err := rtr.paramsUnsharded(vcursor, route)
	if err != nil {
		return nil, fmt.Errorf("execInsertUnsharded: %v", err)
	}

	shardQueries := rtr.getShardQueries(vcursor, route.Query+vcursor.comments, params)
	result, err := rtr.scatterConn.ExecuteMultiShard(
		vcursor.ctx,
		params.ks,
		shardQueries,
		vcursor.tabletType,
		NewSafeSession(vcursor.session),
		vcursor.notInTransaction,
		vcursor.options,
	)
	if err != nil {
		return nil, fmt.Errorf("execInsertUnsharded: %v", err)
	}
	if insertid != 0 {
		if result.InsertID != 0 {
			return nil, fmt.Errorf("sequence and db generated a value each for insert")
		}
		result.InsertID = uint64(insertid)
	}
	return result, nil
}

func (rtr *Router) execInsertSharded(vcursor *queryExecutor, route *engine.Route) (*sqltypes.Result, error) {
	insertid, err := rtr.handleGenerate(vcursor, route.Generate)
	if err != nil {
		return nil, fmt.Errorf("execInsertSharded: %v", err)
	}
	keyspace, shardQueries, err := rtr.getInsertShardedRoute(vcursor, route)
	if err != nil {
		return nil, fmt.Errorf("execInsertSharded: %v", err)
	}

	result, err := rtr.scatterConn.ExecuteMultiShard(
		vcursor.ctx,
		keyspace,
		shardQueries,
		vcursor.tabletType,
		NewSafeSession(vcursor.session),
		vcursor.notInTransaction,
		vcursor.options)

	if err != nil {
		return nil, fmt.Errorf("execInsertSharded: %v", err)
	}

	if insertid != 0 {
		if result.InsertID != 0 {
			return nil, fmt.Errorf("sequence and db generated a value each for insert")
		}
		result.InsertID = uint64(insertid)
	}

	return result, nil
}

func (rtr *Router) getInsertShardedRoute(vcursor *queryExecutor, route *engine.Route) (keyspace string, shardQueries map[string]querytypes.BoundQuery, err error) {
	keyspaceIDs := [][]byte{}
	routing := make(map[string][]string)
	shardKeyspaceIDMap := make(map[string][][]byte)
	keyspace, _, allShards, err := getKeyspaceShards(vcursor.ctx, rtr.serv, rtr.cell, route.Keyspace.Name, vcursor.tabletType)
	if err != nil {
		return "", nil, fmt.Errorf("getInsertShardedRoute: %v", err)
	}

	inputs := route.Values.([]interface{})
	for rowNum, input := range inputs {
		keys, err := rtr.resolveKeys(input.([]interface{}), vcursor.bindVars)
		if err != nil {
			return "", nil, fmt.Errorf("getInsertShardedRoute: %v", err)
		}
		for colNum := 0; colNum < len(keys); colNum++ {
			if colNum == 0 {
				ksid, err := rtr.handlePrimary(vcursor, keys[colNum], route.Table.ColumnVindexes[colNum], vcursor.bindVars, rowNum)
				if err != nil {
					return "", nil, fmt.Errorf("getInsertShardedRoute: %v", err)
				}
				keyspaceIDs = append(keyspaceIDs, ksid)
			} else {
				err := rtr.handleNonPrimary(vcursor, keys[colNum], route.Table.ColumnVindexes[colNum], vcursor.bindVars, keyspaceIDs[rowNum], rowNum)
				if err != nil {
					return "", nil, fmt.Errorf("getInsertShardedRoute: %v", err)
				}
			}
		}
		shard, err := getShardForKeyspaceID(allShards, keyspaceIDs[rowNum])
		routing[shard] = append(routing[shard], route.Mid[rowNum])
		if err != nil {
			return "", nil, fmt.Errorf("getInsertShardedRoute: %v", err)
		}
		shardKeyspaceIDMap[shard] = append(shardKeyspaceIDMap[shard], keyspaceIDs[rowNum])
	}

	shardQueries = make(map[string]querytypes.BoundQuery, len(routing))
	for shard := range routing {
		rewritten := route.Prefix + strings.Join(routing[shard], ",") + route.Suffix
		if err != nil {
			return "", nil, fmt.Errorf("getInsertShardedRoute: Error While Rewriting Query: %v", err)
		}
		rewrittenQuery := sqlannotation.AddKeyspaceIDs(rewritten, shardKeyspaceIDMap[shard], vcursor.comments)
		query := querytypes.BoundQuery{
			Sql:           rewrittenQuery,
			BindVariables: vcursor.bindVars,
		}
		shardQueries[shard] = query
	}

	return keyspace, shardQueries, nil
}

// resolveList returns a list of values, typically for an IN clause. If the input
// is a bind var name, it uses the list provided in the bind var. If the input is
// already a list, it returns just that.
func (rtr *Router) resolveList(val interface{}, bindVars map[string]interface{}) ([]interface{}, error) {
	switch v := val.(type) {
	case []interface{}:
		return v, nil
	case string:
		// It can only be a list bind var.
		list, ok := bindVars[v[2:]]
		if !ok {
			return nil, fmt.Errorf("could not find bind var %s", v)
		}

		// Lists can be an []interface{}, or a *querypb.BindVariable
		// with type TUPLE.
		switch l := list.(type) {
		case []interface{}:
			return l, nil
		case *querypb.BindVariable:
			if l.Type != querypb.Type_TUPLE {
				return nil, fmt.Errorf("expecting list for bind var %s: %v", v, list)
			}
			result := make([]interface{}, len(l.Values))
			for i, val := range l.Values {
				// We can use MakeTrusted as the lower
				// layers will verify the value if needed.
				result[i] = sqltypes.MakeTrusted(val.Type, val.Value)
			}
			return result, nil
		default:
			return nil, fmt.Errorf("expecting list for bind var %s: %v", v, list)
		}
	default:
		panic("unexpected")
	}
}

// resolveKeys takes a list as input that may have values or bind var names.
// It returns a new list with all the bind vars resolved.
func (rtr *Router) resolveKeys(vals []interface{}, bindVars map[string]interface{}) (keys []interface{}, err error) {
	keys = make([]interface{}, 0, len(vals))
	for _, val := range vals {
		if v, ok := val.(string); ok {
			val, ok = bindVars[v[1:]]
			if !ok {
				return nil, fmt.Errorf("could not find bind var %s", v)
			}
		}
		keys = append(keys, val)
	}
	return keys, nil
}

func (rtr *Router) resolveShards(vcursor *queryExecutor, vindexKeys []interface{}, route *engine.Route) (newKeyspace string, routing routingMap, err error) {
	newKeyspace, _, allShards, err := getKeyspaceShards(vcursor.ctx, rtr.serv, rtr.cell, route.Keyspace.Name, vcursor.tabletType)
	if err != nil {
		return "", nil, err
	}
	routing = make(routingMap)
	switch mapper := route.Vindex.(type) {
	case vindexes.Unique:
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
	case vindexes.NonUnique:
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

func (rtr *Router) resolveSingleShard(vcursor *queryExecutor, vindexKey interface{}, route *engine.Route) (newKeyspace, shard string, ksid []byte, err error) {
	newKeyspace, _, allShards, err := getKeyspaceShards(vcursor.ctx, rtr.serv, rtr.cell, route.Keyspace.Name, vcursor.tabletType)
	if err != nil {
		return "", "", nil, err
	}
	mapper := route.Vindex.(vindexes.Unique)
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

func (rtr *Router) deleteVindexEntries(vcursor *queryExecutor, route *engine.Route, ks, shard string, ksid []byte) error {
	result, err := rtr.scatterConn.Execute(
		vcursor.ctx,
		route.Subquery,
		vcursor.bindVars,
		ks,
		[]string{shard},
		vcursor.tabletType,
		NewSafeSession(vcursor.session),
		vcursor.notInTransaction,
		vcursor.options)
	if err != nil {
		return err
	}
	if len(result.Rows) == 0 {
		return nil
	}
	for i, colVindex := range route.Table.Owned {
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
		case vindexes.Lookup:
			if err = vindex.Delete(vcursor, ids, ksid); err != nil {
				return err
			}
		default:
			panic("unexpceted")
		}
	}
	return nil
}

func (rtr *Router) handleGenerate(vcursor *queryExecutor, gen *engine.Generate) (insertid int64, err error) {
	if gen == nil {
		return 0, nil
	}
	count := 0
	resolved := make([]interface{}, len(gen.Value.([]interface{})))
	for i, val := range gen.Value.([]interface{}) {
		if v, ok := val.(string); ok {
			val, ok = vcursor.bindVars[v[1:]]
			if !ok {
				return 0, fmt.Errorf("handleGenerate: could not find bind var %s", v)
			}
		}
		if val == nil {
			count++
		} else if v, ok := val.(*querypb.BindVariable); ok && v.Type == sqltypes.Null {
			count++
		} else {
			resolved[i] = val
		}
	}
	if count != 0 {
		// TODO(sougou): This is similar to paramsUnsharded.
		ks, _, allShards, err := getKeyspaceShards(vcursor.ctx, rtr.serv, rtr.cell, gen.Keyspace.Name, vcursor.tabletType)
		if err != nil {
			return 0, fmt.Errorf("handleGenerate: %v", err)
		}
		if len(allShards) != 1 {
			return 0, fmt.Errorf("unsharded keyspace %s has multiple shards", ks)
		}
		params := newScatterParams(ks, map[string]interface{}{"n": int64(count)}, []string{allShards[0].Name})
		// We nil out the transaction context for this particular call.
		// TODO(sougou): Use ExecuteShard instead.
		shardQueries := rtr.getShardQueries(vcursor, gen.Query, params)
		qr, err := rtr.scatterConn.ExecuteMultiShard(
			vcursor.ctx,
			params.ks,
			shardQueries,
			vcursor.tabletType,
			NewSafeSession(nil),
			false,
			vcursor.options,
		)
		if err != nil {
			return 0, err
		}
		// If no rows are returned, it's an internal error, and the code
		// must panic, which will caught and reported.
		insertid, err = qr.Rows[0][0].ParseInt64()
		if err != nil {
			return 0, err
		}
	}
	cur := insertid
	for i, v := range resolved {
		if v != nil {
			vcursor.bindVars[engine.SeqVarName+strconv.Itoa(i)] = v
		} else {
			vcursor.bindVars[engine.SeqVarName+strconv.Itoa(i)] = cur
			cur++
		}
	}
	return insertid, nil
}

func (rtr *Router) handlePrimary(vcursor *queryExecutor, vindexKey interface{}, colVindex *vindexes.ColumnVindex, bv map[string]interface{}, rowNum int) (ksid []byte, err error) {
	if vindexKey == nil {
		return nil, fmt.Errorf("value must be supplied for column %v", colVindex.Column)
	}
	mapper := colVindex.Vindex.(vindexes.Unique)
	ksids, err := mapper.Map(vcursor, []interface{}{vindexKey})
	if err != nil {
		return nil, err
	}
	ksid = ksids[0]
	if len(ksid) == 0 {
		return nil, fmt.Errorf("could not map %v to a keyspace id", vindexKey)
	}
	bv["_"+colVindex.Column.CompliantName()+strconv.Itoa(rowNum)] = vindexKey
	return ksid, nil
}

func (rtr *Router) handleNonPrimary(vcursor *queryExecutor, vindexKey interface{}, colVindex *vindexes.ColumnVindex, bv map[string]interface{}, ksid []byte, rowNum int) error {
	if colVindex.Owned {
		if vindexKey == nil {
			return fmt.Errorf("value must be supplied for column %v", colVindex.Column)
		}
		err := colVindex.Vindex.(vindexes.Lookup).Create(vcursor, vindexKey, ksid)
		if err != nil {
			return err
		}
	} else {
		if vindexKey == nil {
			reversible, ok := colVindex.Vindex.(vindexes.Reversible)
			if !ok {
				return fmt.Errorf("value must be supplied for column %v", colVindex.Column)
			}
			var err error
			vindexKey, err = reversible.ReverseMap(vcursor, ksid)
			if err != nil {
				return err
			}
			if vindexKey == nil {
				return fmt.Errorf("could not compute value for column %v", colVindex.Column)
			}
		} else {
			ok, err := colVindex.Vindex.Verify(vcursor, vindexKey, ksid)
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("value %v for column %v does not map to keyspace id %v", vindexKey, colVindex.Column, hex.EncodeToString(ksid))
			}
		}
	}
	bv["_"+colVindex.Column.CompliantName()+strconv.Itoa(rowNum)] = vindexKey
	return nil
}

func (rtr *Router) getShardQueries(vcursor *queryExecutor, query string, params *scatterParams) map[string]querytypes.BoundQuery {

	shardQueries := make(map[string]querytypes.BoundQuery, len(params.shardVars))
	for shard, shardVars := range params.shardVars {
		query := querytypes.BoundQuery{
			Sql:           query,
			BindVariables: shardVars,
		}
		shardQueries[shard] = query
	}
	return shardQueries
}
