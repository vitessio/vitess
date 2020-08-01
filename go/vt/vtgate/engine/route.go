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
	"strconv"
	"time"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/topo/topoproto"

	"vitess.io/vitess/go/vt/vtgate/evalengine"

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
	Vindex vindexes.SingleColumn
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

	// Route does not take inputs
	noInputs

	// Route does not need transaction handling
	noTxNeeded
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

func (obp OrderbyParams) String() string {
	val := strconv.Itoa(obp.Col)
	if obp.Desc {
		val += " DESC"
	} else {
		val += " ASC"
	}
	return val
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
	// SelectNone is used for queries that always return empty values
	SelectNone
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
	SelectNone:        "SelectNone",
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
	case SelectNone:
		rss, bvs, err = nil, nil, nil
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
	result, errs := vcursor.ExecuteMultiShard(rss, queries, false /* rollbackOnError */, false /* autocommit */)

	if errs != nil {
		if route.ScatterErrorsAsWarnings {
			partialSuccessScatterQueries.Add(1)

			for _, err := range errs {
				if err != nil {
					serr := mysql.NewSQLErrorFromError(err).(*mysql.SQLError)
					vcursor.Session().RecordWarning(&querypb.QueryWarning{Code: uint32(serr.Num), Message: err.Error()})
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

	// There is an order by. We have to merge-sort.
	prims := make([]StreamExecutor, 0, len(rss))
	for i, rs := range rss {
		prims = append(prims, &shardRoute{
			query: route.Query,
			rs:    rs,
			bv:    bvs[i],
		})
	}
	ms := MergeSort{
		Primitives: prims,
		OrderBy:    route.OrderBy,
	}
	return ms.StreamExecute(vcursor, bindVars, wantfields, func(qr *sqltypes.Result) error {
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
	qr, err := execShard(vcursor, route.FieldQuery, bindVars, rss[0], false /* rollbackOnError */, false /* canAutocommit */)
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
	rss, _, err := resolveShards(vcursor, route.Vindex, route.Keyspace, []sqltypes.Value{key})
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
	rss, values, err := resolveShards(vcursor, route.Vindex, route.Keyspace, keys)
	if err != nil {
		return nil, nil, vterrors.Wrap(err, "paramsSelectIn")
	}
	return rss, shardVars(bindVars, values), nil
}

func resolveShards(vcursor VCursor, vindex vindexes.SingleColumn, keyspace *vindexes.Keyspace, vindexKeys []sqltypes.Value) ([]*srvtopo.ResolvedShard, [][]*querypb.Value, error) {
	// Convert vindexKeys to []*querypb.Value
	ids := make([]*querypb.Value, len(vindexKeys))
	for i, vik := range vindexKeys {
		ids[i] = sqltypes.ValueToProto(vik)
	}

	// Map using the Vindex
	destinations, err := vindex.Map(vcursor, vindexKeys)
	if err != nil {
		return nil, nil, err
	}

	// And use the Resolver to map to ResolvedShards.
	return vcursor.ResolveDestinations(keyspace.Name, ids, destinations)
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
			cmp, err = evalengine.NullsafeCompare(out.Rows[i][order.Col], out.Rows[j][order.Col])
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

func resolveSingleShard(vcursor VCursor, vindex vindexes.SingleColumn, keyspace *vindexes.Keyspace, vindexKey sqltypes.Value) (*srvtopo.ResolvedShard, []byte, error) {
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

func resolveMultiShard(vcursor VCursor, vindex vindexes.SingleColumn, keyspace *vindexes.Keyspace, vindexKey []sqltypes.Value) ([]*srvtopo.ResolvedShard, error) {
	destinations, err := vindex.Map(vcursor, vindexKey)
	if err != nil {
		return nil, err
	}
	rss, _, err := vcursor.ResolveDestinations(keyspace.Name, nil, destinations)
	if err != nil {
		return nil, err
	}
	return rss, nil
}

func resolveKeyspaceID(vcursor VCursor, vindex vindexes.SingleColumn, vindexKey sqltypes.Value) ([]byte, error) {
	destinations, err := vindex.Map(vcursor, []sqltypes.Value{vindexKey})
	if err != nil {
		return nil, err
	}
	switch ksid := destinations[0].(type) {
	case key.DestinationKeyspaceID:
		return ksid, nil
	case key.DestinationNone:
		return nil, nil
	default:
		return nil, fmt.Errorf("cannot map vindex to unique keyspace id: %v", destinations[0])
	}
}

func execShard(vcursor VCursor, query string, bindVars map[string]*querypb.BindVariable, rs *srvtopo.ResolvedShard, rollbackOnError, canAutocommit bool) (*sqltypes.Result, error) {
	autocommit := canAutocommit && vcursor.AutocommitApproval()
	result, errs := vcursor.ExecuteMultiShard([]*srvtopo.ResolvedShard{rs}, []*querypb.BoundQuery{
		{
			Sql:           query,
			BindVariables: bindVars,
		},
	}, rollbackOnError, autocommit)
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

func allowOnlyMaster(rss ...*srvtopo.ResolvedShard) error {
	for _, rs := range rss {
		if rs != nil && rs.Target.TabletType != topodatapb.TabletType_MASTER {
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "supported only for master tablet type, current type: %v", topoproto.TabletTypeLString(rs.Target.TabletType))
		}
	}
	return nil
}

func (route *Route) description() PrimitiveDescription {
	other := map[string]interface{}{
		"Query":      route.Query,
		"Table":      route.TableName,
		"FieldQuery": route.FieldQuery,
	}
	if route.Vindex != nil {
		other["Vindex"] = route.Vindex.String()
	}
	if len(route.Values) > 0 {
		other["Values"] = route.Values
	}

	return PrimitiveDescription{
		OperatorType:      "Route",
		Variant:           routeName[route.Opcode],
		Keyspace:          route.Keyspace,
		TargetDestination: route.TargetDestination,
		Other:             other,
	}
}
