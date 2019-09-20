/*
Copyright 2019 The Vitess Authors.

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
	"time"

	"vitess.io/vitess/go/jsonutil"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
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

	// TargetDestination specifies an explicit target destination to send the query to.
	// This bypases the core of the v3 engine.
	TargetDestination key.Destination

	// TargetTabletType specifies an explicit target destination tablet type
	// this is only used in conjunction with TargetDestination
	TargetTabletType topodatapb.TabletType

	// Query specifies the query to be executed.
	Query string

	// TableName specifies the table to send the query to.
	TableName string

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

	// QueryTimeout contains the optional timeout (in milliseconds) to apply to this query
	QueryTimeout int

	// ScatterErrorsAsWarnings is true if results should be returned even if some shards have an error
	ScatterErrorsAsWarnings bool
}

// NewSimpleRoute creates a Route with the bare minimum of parameters.
func NewSimpleRoute(opcode RouteOpcode, keyspace *vindexes.Keyspace) *Route {
	return &Route{
		Opcode:   opcode,
		Keyspace: keyspace,
	}
}

// NewRoute creates a Route.
func NewRoute(opcode RouteOpcode, keyspace *vindexes.Keyspace, query, fieldQuery string) *Route {
	return &Route{
		Opcode:     opcode,
		Keyspace:   keyspace,
		Query:      query,
		FieldQuery: fieldQuery,
	}
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
		Opcode                  RouteOpcode
		Keyspace                *vindexes.Keyspace   `json:",omitempty"`
		Query                   string               `json:",omitempty"`
		FieldQuery              string               `json:",omitempty"`
		Vindex                  string               `json:",omitempty"`
		Values                  []sqltypes.PlanValue `json:",omitempty"`
		OrderBy                 []OrderbyParams      `json:",omitempty"`
		TruncateColumnCount     int                  `json:",omitempty"`
		QueryTimeout            int                  `json:",omitempty"`
		ScatterErrorsAsWarnings bool                 `json:",omitempty"`
		Table                   string               `json:",omitempty"`
	}{
		Opcode:                  route.Opcode,
		Keyspace:                route.Keyspace,
		Query:                   route.Query,
		FieldQuery:              route.FieldQuery,
		Vindex:                  vindexName,
		Values:                  route.Values,
		OrderBy:                 route.OrderBy,
		TruncateColumnCount:     route.TruncateColumnCount,
		QueryTimeout:            route.QueryTimeout,
		ScatterErrorsAsWarnings: route.ScatterErrorsAsWarnings,
		Table:                   route.TableName,
	}
	return jsonutil.MarshalNoEscape(marshalRoute)
}

// RouteOpcode is a number representing the opcode
// for the Route primitve. Adding new opcodes here
// will require review of the join code and
// the finalizeOptions code in planbuilder.
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
	// SelectReference is for fetching from a reference table.
	SelectReference
)

var routeName = map[RouteOpcode]string{
	SelectUnsharded:   "SelectUnsharded",
	SelectEqualUnique: "SelectEqualUnique",
	SelectEqual:       "SelectEqual",
	SelectIN:          "SelectIN",
	SelectScatter:     "SelectScatter",
	SelectNext:        "SelectNext",
	SelectDBA:         "SelectDBA",
	SelectReference:   "SelectReference",
}

var (
	partialSuccessScatterQueries = stats.NewCounter("PartialSuccessScatterQueries", "Count of partially successful scatter queries")
)

// MarshalJSON serializes the RouteOpcode as a JSON string.
// It's used for testing and diagnostics.
func (code RouteOpcode) MarshalJSON() ([]byte, error) {
	return json.Marshal(routeName[code])
}

// RouteType returns a description of the query routing type used by the primitive
func (route *Route) RouteType() string {
	return routeName[route.Opcode]
}

// GetKeyspaceName specifies the Keyspace that this primitive routes to.
func (route *Route) GetKeyspaceName() string {
	return route.Keyspace.Name
}

// GetTableName specifies the table that this primitive routes to.
func (route *Route) GetTableName() string {
	return route.TableName
}

// SetTruncateColumnCount sets the truncate column count.
func (route *Route) SetTruncateColumnCount(count int) {
	route.TruncateColumnCount = count
}

// Execute performs a non-streaming exec.
func (route *Route) Execute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	if route.QueryTimeout != 0 {
		cancel := vcursor.SetContextTimeout(time.Duration(route.QueryTimeout) * time.Millisecond)
		defer cancel()
	}
	qr, err := route.execute(vcursor, bindVars, wantfields)
	if err != nil {
		return nil, err
	}
	return qr.Truncate(route.TruncateColumnCount), nil
}

func (route *Route) execute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	var rss []*srvtopo.ResolvedShard
	var bvs []map[string]*querypb.BindVariable
	var err error
	switch route.Opcode {
	case SelectUnsharded, SelectNext, SelectDBA, SelectReference:
		rss, bvs, err = route.paramsAnyShard(vcursor, bindVars)
	case SelectScatter:
		rss, bvs, err = route.paramsAllShards(vcursor, bindVars)
	case SelectEqual, SelectEqualUnique:
		rss, bvs, err = route.paramsSelectEqual(vcursor, bindVars)
	case SelectIN:
		rss, bvs, err = route.paramsSelectIn(vcursor, bindVars)
	default:
		// Unreachable.
		return nil, fmt.Errorf("unsupported query route: %v", route)
	}
	if err != nil {
		return nil, err
	}

	// No route.
	if len(rss) == 0 {
		if wantfields {
			return route.GetFields(vcursor, bindVars)
		}
		return &sqltypes.Result{}, nil
	}

	queries := getQueries(route.Query, bvs)
	result, errs := vcursor.ExecuteMultiShard(rss, queries, false /* isDML */, false /* autocommit */)

	if errs != nil {
		if route.ScatterErrorsAsWarnings {
			partialSuccessScatterQueries.Add(1)

			for _, err := range errs {
				if err != nil {
					serr := mysql.NewSQLErrorFromError(err).(*mysql.SQLError)
					vcursor.RecordWarning(&querypb.QueryWarning{Code: uint32(serr.Num), Message: err.Error()})
				}
			}
			// fall through
		} else {
			return nil, vterrors.Aggregate(errs)
		}
	}
	if len(route.OrderBy) == 0 {
		return result, nil
	}

	return route.sort(result)
}

// StreamExecute performs a streaming exec.
func (route *Route) StreamExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	var rss []*srvtopo.ResolvedShard
	var bvs []map[string]*querypb.BindVariable
	var err error
	if route.QueryTimeout != 0 {
		cancel := vcursor.SetContextTimeout(time.Duration(route.QueryTimeout) * time.Millisecond)
		defer cancel()
	}
	switch route.Opcode {
	case SelectUnsharded, SelectNext, SelectDBA, SelectReference:
		rss, bvs, err = route.paramsAnyShard(vcursor, bindVars)
	case SelectScatter:
		rss, bvs, err = route.paramsAllShards(vcursor, bindVars)
	case SelectEqual, SelectEqualUnique:
		rss, bvs, err = route.paramsSelectEqual(vcursor, bindVars)
	case SelectIN:
		rss, bvs, err = route.paramsSelectIn(vcursor, bindVars)
	default:
		return fmt.Errorf("query %q cannot be used for streaming", route.Query)
	}
	if err != nil {
		return err
	}

	// No route.
	if len(rss) == 0 {
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
		return vcursor.StreamExecuteMulti(route.Query, rss, bvs, func(qr *sqltypes.Result) error {
			return callback(qr.Truncate(route.TruncateColumnCount))
		})
	}

	return mergeSort(vcursor, route.Query, route.OrderBy, rss, bvs, func(qr *sqltypes.Result) error {
		return callback(qr.Truncate(route.TruncateColumnCount))
	})
}

// GetFields fetches the field info.
func (route *Route) GetFields(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	rss, _, err := vcursor.ResolveDestinations(route.Keyspace.Name, nil, []key.Destination{key.DestinationAnyShard{}})
	if err != nil {
		return nil, err
	}
	if len(rss) != 1 {
		// This code is unreachable. It's just a sanity check.
		return nil, fmt.Errorf("no shards for keyspace: %s", route.Keyspace.Name)
	}
	qr, err := execShard(vcursor, route.FieldQuery, bindVars, rss[0], false /* isDML */, false /* canAutocommit */)
	if err != nil {
		return nil, err
	}
	return qr.Truncate(route.TruncateColumnCount), nil
}

func (route *Route) paramsAllShards(vcursor VCursor, bindVars map[string]*querypb.BindVariable) ([]*srvtopo.ResolvedShard, []map[string]*querypb.BindVariable, error) {
	rss, _, err := vcursor.ResolveDestinations(route.Keyspace.Name, nil, []key.Destination{key.DestinationAllShards{}})
	if err != nil {
		return nil, nil, vterrors.Wrap(err, "paramsAllShards")
	}
	multiBindVars := make([]map[string]*querypb.BindVariable, len(rss))
	for i := range multiBindVars {
		multiBindVars[i] = bindVars
	}
	return rss, multiBindVars, nil
}

func (route *Route) paramsAnyShard(vcursor VCursor, bindVars map[string]*querypb.BindVariable) ([]*srvtopo.ResolvedShard, []map[string]*querypb.BindVariable, error) {
	rss, _, err := vcursor.ResolveDestinations(route.Keyspace.Name, nil, []key.Destination{key.DestinationAnyShard{}})
	if err != nil {
		return nil, nil, vterrors.Wrap(err, "paramsAnyShard")
	}
	multiBindVars := make([]map[string]*querypb.BindVariable, len(rss))
	for i := range multiBindVars {
		multiBindVars[i] = bindVars
	}
	return rss, multiBindVars, nil
}

func (route *Route) paramsSelectEqual(vcursor VCursor, bindVars map[string]*querypb.BindVariable) ([]*srvtopo.ResolvedShard, []map[string]*querypb.BindVariable, error) {
	key, err := route.Values[0].ResolveValue(bindVars)
	if err != nil {
		return nil, nil, vterrors.Wrap(err, "paramsSelectEqual")
	}
	rss, _, err := route.resolveShards(vcursor, []sqltypes.Value{key})
	if err != nil {
		return nil, nil, vterrors.Wrap(err, "paramsSelectEqual")
	}
	multiBindVars := make([]map[string]*querypb.BindVariable, len(rss))
	for i := range multiBindVars {
		multiBindVars[i] = bindVars
	}
	return rss, multiBindVars, nil
}

func (route *Route) paramsSelectIn(vcursor VCursor, bindVars map[string]*querypb.BindVariable) ([]*srvtopo.ResolvedShard, []map[string]*querypb.BindVariable, error) {
	keys, err := route.Values[0].ResolveList(bindVars)
	if err != nil {
		return nil, nil, vterrors.Wrap(err, "paramsSelectIn")
	}
	rss, values, err := route.resolveShards(vcursor, keys)
	if err != nil {
		return nil, nil, vterrors.Wrap(err, "paramsSelectIn")
	}
	return rss, shardVars(bindVars, values), nil
}

func (route *Route) resolveShards(vcursor VCursor, vindexKeys []sqltypes.Value) ([]*srvtopo.ResolvedShard, [][]*querypb.Value, error) {
	// Convert vindexKeys to []*querypb.Value
	ids := make([]*querypb.Value, len(vindexKeys))
	for i, vik := range vindexKeys {
		ids[i] = sqltypes.ValueToProto(vik)
	}

	// Map using the Vindex
	destinations, err := route.Vindex.Map(vcursor, vindexKeys)
	if err != nil {
		return nil, nil, err
	}

	// And use the Resolver to map to ResolvedShards.
	return vcursor.ResolveDestinations(route.Keyspace.Name, ids, destinations)
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

func resolveSingleShard(vcursor VCursor, vindex vindexes.Vindex, keyspace *vindexes.Keyspace, vindexKey sqltypes.Value) (*srvtopo.ResolvedShard, []byte, error) {
	destinations, err := vindex.Map(vcursor, []sqltypes.Value{vindexKey})
	if err != nil {
		return nil, nil, err
	}
	var ksid []byte
	switch d := destinations[0].(type) {
	case key.DestinationKeyspaceID:
		ksid = d
	case key.DestinationNone:
		return nil, nil, nil
	default:
		return nil, nil, fmt.Errorf("cannot map vindex to unique keyspace id: %v", destinations[0])
	}
	rss, _, err := vcursor.ResolveDestinations(keyspace.Name, nil, destinations)
	if err != nil {
		return nil, nil, err
	}
	if len(rss) != 1 {
		return nil, nil, fmt.Errorf("ResolveDestinations maps to %v shards", len(rss))
	}
	return rss[0], ksid, nil
}

func execShard(vcursor VCursor, query string, bindVars map[string]*querypb.BindVariable, rs *srvtopo.ResolvedShard, isDML, canAutocommit bool) (*sqltypes.Result, error) {
	autocommit := canAutocommit && vcursor.AutocommitApproval()
	result, errs := vcursor.ExecuteMultiShard([]*srvtopo.ResolvedShard{rs}, []*querypb.BoundQuery{
		{
			Sql:           query,
			BindVariables: bindVars,
		},
	}, isDML, autocommit)
	return result, vterrors.Aggregate(errs)
}

func getQueries(query string, bvs []map[string]*querypb.BindVariable) []*querypb.BoundQuery {
	queries := make([]*querypb.BoundQuery, len(bvs))
	for i, bv := range bvs {
		queries[i] = &querypb.BoundQuery{
			Sql:           query,
			BindVariables: bv,
		}
	}
	return queries
}

func shardVars(bv map[string]*querypb.BindVariable, mapVals [][]*querypb.Value) []map[string]*querypb.BindVariable {
	shardVars := make([]map[string]*querypb.BindVariable, len(mapVals))
	for i, vals := range mapVals {
		newbv := make(map[string]*querypb.BindVariable, len(bv)+1)
		for k, v := range bv {
			newbv[k] = v
		}
		newbv[ListVarName] = &querypb.BindVariable{
			Type:   querypb.Type_TUPLE,
			Values: vals,
		}
		shardVars[i] = newbv
	}
	return shardVars
}
