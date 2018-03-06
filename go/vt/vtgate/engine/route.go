/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package engine

import (
	"encoding/json"
	"fmt"
	"sort"

	"vitess.io/vitess/go/jsonutil"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

var _ Primitive = (*Route)(nil)

// Route represents the instructions to route a read query to
// one or many vttablets.
type Route struct {
	// the fields are described in the RouteOpcode values comments.
	// Opcode is the execution opcode.
	Opcode RouteOpcode

	// Keyspace specifies the keyspace to send the query to.
	Keyspace *vindexes.Keyspace

	// Query specifies the query to be executed.
	Query string

	// FieldQuery specifies the query to be executed for a GetFieldInfo request.
	FieldQuery string

	// Vindex specifies the vindex to be used.
	Vindex vindexes.Vindex
	// Values specifies the vindex values to use for routing.
	Values []sqltypes.PlanValue

	// OrderBy specifies the key order for merge sorting. This will be
	// set only for scatter queries that need the results to be
	// merge-sorted.
	OrderBy []OrderbyParams

	// TruncateColumnCount specifies the number of columns to return
	// in the final result. Rest of the columns are truncated
	// from the result received. If 0, no truncation happens.
	TruncateColumnCount int
}

// OrderbyParams specifies the parameters for ordering.
// This is used for merge-sorting scatter queries.
type OrderbyParams struct {
	Col  int
	Desc bool
}

// MarshalJSON serializes the Route into a JSON representation.
// It's used for testing and diagnostics.
func (route *Route) MarshalJSON() ([]byte, error) {
	var vindexName string
	if route.Vindex != nil {
		vindexName = route.Vindex.String()
	}
	marshalRoute := struct {
		Opcode              RouteOpcode
		Keyspace            *vindexes.Keyspace   `json:",omitempty"`
		Query               string               `json:",omitempty"`
		FieldQuery          string               `json:",omitempty"`
		Vindex              string               `json:",omitempty"`
		Values              []sqltypes.PlanValue `json:",omitempty"`
		OrderBy             []OrderbyParams      `json:",omitempty"`
		TruncateColumnCount int                  `json:",omitempty"`
	}{
		Opcode:              route.Opcode,
		Keyspace:            route.Keyspace,
		Query:               route.Query,
		FieldQuery:          route.FieldQuery,
		Vindex:              vindexName,
		Values:              route.Values,
		OrderBy:             route.OrderBy,
		TruncateColumnCount: route.TruncateColumnCount,
	}
	return jsonutil.MarshalNoEscape(marshalRoute)
}

// RouteOpcode is a number representing the opcode
// for the Route primitve.
type RouteOpcode int

// This is the list of RouteOpcode values.
const (
	// SelectUnsharded is the opcode for routing a
	// select statement to an unsharded database.
	SelectUnsharded = RouteOpcode(iota)
	// SelectEqualUnique is for routing a query to
	// a single shard. Requires: A Unique Vindex, and
	// a single Value.
	SelectEqualUnique
	// SelectEqual is for routing a query using a
	// non-unique vindex. Requires: A Vindex, and
	// a single Value.
	SelectEqual
	// SelectIN is for routing a query that has an IN
	// clause using a Vindex. Requires: A Vindex,
	// and a Values list.
	SelectIN
	// SelectScatter is for routing a scatter query
	// to all shards of a keyspace.
	SelectScatter
	// SelectNext is for fetching from a sequence.
	SelectNext
	// SelectDBA is for executing a DBA statement.
	SelectDBA
)

var routeName = map[RouteOpcode]string{
	SelectUnsharded:   "SelectUnsharded",
	SelectEqualUnique: "SelectEqualUnique",
	SelectEqual:       "SelectEqual",
	SelectIN:          "SelectIN",
	SelectScatter:     "SelectScatter",
	SelectNext:        "SelectNext",
	SelectDBA:         "SelectDBA",
}

// MarshalJSON serializes the RouteOpcode as a JSON string.
// It's used for testing and diagnostics.
func (code RouteOpcode) MarshalJSON() ([]byte, error) {
	return json.Marshal(routeName[code])
}

// Execute performs a non-streaming exec.
func (route *Route) Execute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	qr, err := route.execute(vcursor, bindVars, wantfields)
	if err != nil {
		return nil, err
	}
	return qr.Truncate(route.TruncateColumnCount), nil
}

func (route *Route) execute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	switch route.Opcode {
	case SelectNext, SelectDBA:
		return execAnyShard(vcursor, route.Query, bindVars, route.Keyspace)
	}

	var ks string
	var shardVars map[string]map[string]*querypb.BindVariable
	var err error
	switch route.Opcode {
	case SelectUnsharded, SelectScatter:
		ks, shardVars, err = route.paramsAllShards(vcursor, bindVars)
	case SelectEqual, SelectEqualUnique:
		ks, shardVars, err = route.paramsSelectEqual(vcursor, bindVars)
	case SelectIN:
		ks, shardVars, err = route.paramsSelectIN(vcursor, bindVars)
	default:
		// Unreachable.
		return nil, fmt.Errorf("unsupported query route: %v", route)
	}
	if err != nil {
		return nil, err
	}

	// No route.
	if len(shardVars) == 0 {
		if wantfields {
			return route.GetFields(vcursor, bindVars)
		}
		return &sqltypes.Result{}, nil
	}

	shardQueries := getShardQueries(route.Query, shardVars)
	result, err := vcursor.ExecuteMultiShard(ks, shardQueries, false /* isDML */, false /* canAutocommit */)
	if err != nil {
		return nil, err
	}
	if len(route.OrderBy) == 0 {
		return result, nil
	}

	return route.sort(result)
}

// StreamExecute performs a streaming exec.
func (route *Route) StreamExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	var ks string
	var shardVars map[string]map[string]*querypb.BindVariable
	var err error
	switch route.Opcode {
	case SelectUnsharded, SelectScatter:
		ks, shardVars, err = route.paramsAllShards(vcursor, bindVars)
	case SelectEqual, SelectEqualUnique:
		ks, shardVars, err = route.paramsSelectEqual(vcursor, bindVars)
	case SelectIN:
		ks, shardVars, err = route.paramsSelectIN(vcursor, bindVars)
	default:
		return fmt.Errorf("query %q cannot be used for streaming", route.Query)
	}
	if err != nil {
		return err
	}

	// No route.
	if len(shardVars) == 0 {
		if wantfields {
			r, err := route.GetFields(vcursor, bindVars)
			if err != nil {
				return err
			}
			return callback(r)
		}
		return nil
	}

	if len(route.OrderBy) == 0 {
		return vcursor.StreamExecuteMulti(route.Query, ks, shardVars, func(qr *sqltypes.Result) error {
			return callback(qr.Truncate(route.TruncateColumnCount))
		})
	}

	return mergeSort(vcursor, route.Query, route.OrderBy, ks, shardVars, func(qr *sqltypes.Result) error {
		return callback(qr.Truncate(route.TruncateColumnCount))
	})
}

// GetFields fetches the field info.
func (route *Route) GetFields(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	ks, shard, err := anyShard(vcursor, route.Keyspace)
	if err != nil {
		return nil, err
	}

	qr, err := execShard(vcursor, route.FieldQuery, bindVars, ks, shard, false /* isDML */, false /* canAutocommit */)
	if err != nil {
		return nil, err
	}
	return qr.Truncate(route.TruncateColumnCount), nil
}

func (route *Route) paramsAllShards(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (ks string, shardVars map[string]map[string]*querypb.BindVariable, err error) {
	ks, allShards, err := vcursor.GetKeyspaceShards(route.Keyspace)
	if err != nil {
		return "", nil, vterrors.Wrap(err, "paramsAllShards")
	}
	shardVars = make(map[string]map[string]*querypb.BindVariable, len(allShards))
	for _, shard := range allShards {
		shardVars[shard.Name] = bindVars
	}
	return ks, shardVars, nil
}

func (route *Route) paramsSelectEqual(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (ks string, shardVars map[string]map[string]*querypb.BindVariable, err error) {
	key, err := route.Values[0].ResolveValue(bindVars)
	if err != nil {
		return "", nil, vterrors.Wrap(err, "paramsSelectEqual")
	}
	ks, routing, err := route.resolveShards(vcursor, bindVars, []sqltypes.Value{key})
	if err != nil {
		return "", nil, vterrors.Wrap(err, "paramsSelectEqual")
	}
	shardVars = make(map[string]map[string]*querypb.BindVariable)
	for _, shard := range routing.Shards() {
		shardVars[shard] = bindVars
	}
	return ks, shardVars, nil
}

func (route *Route) paramsSelectIN(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (ks string, shardVars map[string]map[string]*querypb.BindVariable, err error) {
	keys, err := route.Values[0].ResolveList(bindVars)
	if err != nil {
		return "", nil, vterrors.Wrap(err, "paramsSelectIN")
	}
	ks, routing, err := route.resolveShards(vcursor, bindVars, keys)
	if err != nil {
		return "", nil, vterrors.Wrap(err, "paramsSelectEqual")
	}
	return ks, routing.ShardVars(bindVars), nil
}

func (route *Route) resolveShards(vcursor VCursor, bindVars map[string]*querypb.BindVariable, vindexKeys []sqltypes.Value) (newKeyspace string, routing routingMap, err error) {
	newKeyspace, allShards, err := vcursor.GetKeyspaceShards(route.Keyspace)
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
		var shards []string
		for i, ksid := range ksids {
			switch {
			case ksid.Range != nil:
				// Even for a unique vindex, a KeyRange can be returned if a keypace
				// id cannot be identified. For example, this can happen during backfill.
				// In such cases, we scatter over the KeyRange.
				// Use the multi-keyspace id API to convert a keyrange to shards.
				shards, err = vcursor.GetShardsForKsids(allShards, vindexes.Ksids{Range: ksid.Range})
				if err != nil {
					return "", nil, err
				}
			case ksid.ID != nil:
				shard, err := vcursor.GetShardForKeyspaceID(allShards, ksid.ID)
				if err != nil {
					return "", nil, err
				}
				shards = []string{shard}
			}
			for _, shard := range shards {
				routing.Add(shard, sqltypes.ValueToProto(vindexKeys[i]))
			}
		}
	case vindexes.NonUnique:
		ksidss, err := mapper.Map(vcursor, vindexKeys)
		if err != nil {
			return "", nil, err
		}
		for i, ksids := range ksidss {
			shards, err := vcursor.GetShardsForKsids(allShards, ksids)
			if err != nil {
				return "", nil, err
			}
			for _, shard := range shards {
				routing.Add(shard, sqltypes.ValueToProto(vindexKeys[i]))
			}
		}
	default:
		panic("unexpected")
	}
	return newKeyspace, routing, nil
}

func (route *Route) sort(in *sqltypes.Result) (*sqltypes.Result, error) {
	var err error
	// Since Result is immutable, we make a copy.
	// The copy can be shallow because we won't be changing
	// the contents of any row.
	out := &sqltypes.Result{
		Fields:       in.Fields,
		Rows:         in.Rows,
		RowsAffected: in.RowsAffected,
		InsertID:     in.InsertID,
	}

	sort.Slice(out.Rows, func(i, j int) bool {
		// If there are any errors below, the function sets
		// the external err and returns true. Once err is set,
		// all subsequent calls return true. This will make
		// Slice think that all elements are in the correct
		// order and return more quickly.
		for _, order := range route.OrderBy {
			if err != nil {
				return true
			}
			var cmp int
			cmp, err = sqltypes.NullsafeCompare(out.Rows[i][order.Col], out.Rows[j][order.Col])
			if err != nil {
				return true
			}
			if cmp == 0 {
				continue
			}
			if order.Desc {
				cmp = -cmp
			}
			return cmp < 0
		}
		return true
	})

	return out, err
}

func resolveSingleShard(vcursor VCursor, vindex vindexes.Vindex, keyspace *vindexes.Keyspace, bindVars map[string]*querypb.BindVariable, vindexKey sqltypes.Value) (newKeyspace, shard string, ksid []byte, err error) {
	newKeyspace, allShards, err := vcursor.GetKeyspaceShards(keyspace)
	if err != nil {
		return "", "", nil, err
	}
	mapper := vindex.(vindexes.Unique)
	ksids, err := mapper.Map(vcursor, []sqltypes.Value{vindexKey})
	if err != nil {
		return "", "", nil, err
	}
	if err := ksids[0].ValidateUnique(); err != nil {
		return "", "", nil, err
	}
	ksid = ksids[0].ID
	if ksid == nil {
		return "", "", ksid, nil
	}
	shard, err = vcursor.GetShardForKeyspaceID(allShards, ksid)
	if err != nil {
		return "", "", nil, err
	}
	return newKeyspace, shard, ksid, nil
}

func execAnyShard(vcursor VCursor, query string, bindVars map[string]*querypb.BindVariable, keyspace *vindexes.Keyspace) (*sqltypes.Result, error) {
	ks, shard, err := anyShard(vcursor, keyspace)
	if err != nil {
		return nil, fmt.Errorf("execAnyShard: %v", err)
	}
	return vcursor.ExecuteStandalone(query, bindVars, ks, shard)
}

func execShard(vcursor VCursor, query string, bindVars map[string]*querypb.BindVariable, keyspace, shard string, isDML, canAutocommit bool) (*sqltypes.Result, error) {
	return vcursor.ExecuteMultiShard(keyspace, map[string]*querypb.BoundQuery{
		shard: {
			Sql:           query,
			BindVariables: bindVars,
		},
	}, isDML, canAutocommit)
}

func anyShard(vcursor VCursor, keyspace *vindexes.Keyspace) (string, string, error) {
	ks, allShards, err := vcursor.GetKeyspaceShards(keyspace)
	if err != nil {
		return "", "", err
	}
	if len(allShards) == 0 {
		// This code is unreachable. It's just a sanity check.
		return "", "", fmt.Errorf("No shards for keyspace: %s", ks)
	}
	return ks, allShards[0].Name, nil
}

func getShardQueries(query string, shardVars map[string]map[string]*querypb.BindVariable) map[string]*querypb.BoundQuery {
	shardQueries := make(map[string]*querypb.BoundQuery, len(shardVars))
	for shard, shardVars := range shardVars {
		shardQueries[shard] = &querypb.BoundQuery{
			Sql:           query,
			BindVariables: shardVars,
		}
	}
	return shardQueries
}
