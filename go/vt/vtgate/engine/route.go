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
	"fmt"
	"sort"

	"strconv"
	"strings"

	"github.com/youtube/vitess/go/jsonutil"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/sqlannotation"
	"github.com/youtube/vitess/go/vt/vterrors"
	"github.com/youtube/vitess/go/vt/vtgate/vindexes"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

var _ Primitive = (*Route)(nil)

// Route represents the instructions to route a query to
// one or many vttablets. The meaning and values for the
// the fields are described in the RouteOpcode values comments.
type Route struct {
	// Opcode is the execution opcode.
	Opcode RouteOpcode

	// Keyspace specifies the keyspace to send the query to.
	Keyspace *vindexes.Keyspace

	// Query specifies the query to be executed.
	Query string

	// FieldQuery specifies the query to be executed for a GetFieldInfo request.
	FieldQuery string

	// Vindex and Values specify how routing must be computed
	Vindex vindexes.Vindex
	Values []sqltypes.PlanValue

	// JoinVars contains the list of joinvar keys that will be used
	// to extract join variables.
	JoinVars map[string]struct{}

	// OrderBy specifies the key order for merge sorting. This will be
	// set only for scatter queries that need the results to be
	// merge-sorted.
	OrderBy []OrderbyParams

	// The following variables are only set for DMLs.

	// Table sepcifies the table for the route.
	Table *vindexes.Table

	// Subquery is only set for deletes. A select of the rows to be
	// deleted is performed to figure out which lookup vindex entries must
	// be deleted.
	Subquery string

	// Generate is only set for inserts where a sequence must be generated.
	Generate *Generate

	// Prefix, Mid and Suffix are set for multi-value inserts.
	Prefix string
	Mid    []string
	Suffix string
}

// OrderbyParams specifies the parameters for ordering.
// This is used for merge-sorting scatter queries.
type OrderbyParams struct {
	Col  int
	Desc bool
}

// NewRoute creates a new Route.
func NewRoute(opcode RouteOpcode, keyspace *vindexes.Keyspace) *Route {
	return &Route{
		Opcode:   opcode,
		Keyspace: keyspace,
		JoinVars: make(map[string]struct{}),
	}
}

// MarshalJSON serializes the Route into a JSON representation.
// It's used for testing and diagnostics.
func (route *Route) MarshalJSON() ([]byte, error) {
	var tname, vindexName string
	if route.Table != nil {
		tname = route.Table.Name.String()
	}
	if route.Vindex != nil {
		vindexName = route.Vindex.String()
	}
	marshalRoute := struct {
		Opcode     RouteOpcode          `json:",omitempty"`
		Keyspace   *vindexes.Keyspace   `json:",omitempty"`
		Query      string               `json:",omitempty"`
		FieldQuery string               `json:",omitempty"`
		Vindex     string               `json:",omitempty"`
		Values     []sqltypes.PlanValue `json:",omitempty"`
		JoinVars   map[string]struct{}  `json:",omitempty"`
		OrderBy    []OrderbyParams      `json:",omitempty"`
		Table      string               `json:",omitempty"`
		Subquery   string               `json:",omitempty"`
		Generate   *Generate            `json:",omitempty"`
		Prefix     string               `json:",omitempty"`
		Mid        []string             `json:",omitempty"`
		Suffix     string               `json:",omitempty"`
	}{
		Opcode:     route.Opcode,
		Keyspace:   route.Keyspace,
		Query:      route.Query,
		FieldQuery: route.FieldQuery,
		Vindex:     vindexName,
		Values:     route.Values,
		JoinVars:   route.JoinVars,
		OrderBy:    route.OrderBy,
		Table:      tname,
		Subquery:   route.Subquery,
		Generate:   route.Generate,
		Prefix:     route.Prefix,
		Mid:        route.Mid,
		Suffix:     route.Suffix,
	}
	return jsonutil.MarshalNoEscape(marshalRoute)
}

// Generate represents the instruction to generate
// a value from a sequence.
type Generate struct {
	Keyspace *vindexes.Keyspace
	Query    string
	// Values are the supplied values for the column, which
	// will be stored as a list within the PlanValue. New
	// values will be generated based on how many were not
	// supplied (NULL).
	Values sqltypes.PlanValue
}

// RouteOpcode is a number representing the opcode
// for the Route primitve.
type RouteOpcode int

// This is the list of RouteOpcode values. The opcode
// dictates which fields must be set in the Route.
// All routes require the Query and a Keyspace
// to be correctly set.
// For any Select opcode, the FieldQuery is set
// to a statement with an impossible where clause.
// This gets used to build the field info in situations
// where joins end up returning no rows.
// In the case of a join, joinVars will also be set.
// These are variables that will be supplied by the
// Join primitive when it invokes a Route.
// All DMLs must have the Table field set. The
// ColVindexes in the field will be used to perform
// various computations and sanity checks.
// The rest of the fields depend on the opcode.
const (
	NoCode = RouteOpcode(iota)
	// SelectUnsharded is the opcode for routing a
	// select statement to an unsharded database.
	SelectUnsharded
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
	// ExecDBA is for executing a DBA statement.
	ExecDBA
	// UpdateUnsharded is for routing an update statement
	// to an unsharded keyspace.
	UpdateUnsharded
	// UpdateEqual is for routing an update statement
	// to a single shard: Requires: A Vindex, and
	// a single Value.
	UpdateEqual
	// DeleteUnsharded is for routing a delete statement
	// to an unsharded keyspace.
	DeleteUnsharded
	// DeleteEqual is for routing a delete statement
	// to a single shard. Requires: A Vindex, a single
	// Value, and a Subquery, which will be used to
	// determine if lookup rows need to be deleted.
	DeleteEqual
	// InsertUnsharded is for routing an insert statement
	// to an unsharded keyspace.
	InsertUnsharded
	// InsertUnsharded is for routing an insert statement
	// to a single shard. Requires: A list of Values, one
	// for each ColVindex. If the table has an Autoinc column,
	// A Generate subplan must be created.
	InsertSharded
	// InsertShardedIgnore is for INSERT IGNORE and
	// INSERT...ON DUPLICATE KEY constructs.
	InsertShardedIgnore
	// NumCodes is the total number of opcodes for routes.
	NumCodes
)

// routeName must exactly match order of opcode constants.
var routeName = [NumCodes]string{
	"Error",
	"SelectUnsharded",
	"SelectEqualUnique",
	"SelectEqual",
	"SelectIN",
	"SelectScatter",
	"SelectNext",
	"ExecDBA",
	"UpdateUnsharded",
	"UpdateEqual",
	"DeleteUnsharded",
	"DeleteEqual",
	"InsertUnsharded",
	"InsertSharded",
	"InsertShardedIgnore",
}

func (code RouteOpcode) String() string {
	if code < 0 || code >= NumCodes {
		return ""
	}
	return routeName[code]
}

// MarshalJSON serializes the RouteOpcode as a JSON string.
// It's used for testing and diagnostics.
func (code RouteOpcode) MarshalJSON() ([]byte, error) {
	return ([]byte)(fmt.Sprintf("\"%s\"", code.String())), nil
}

type scatterParams struct {
	ks        string
	shardVars map[string]map[string]*querypb.BindVariable
}

func newScatterParams(ks string, bv map[string]*querypb.BindVariable, shards []string) *scatterParams {
	shardVars := make(map[string]map[string]*querypb.BindVariable, len(shards))
	for _, shard := range shards {
		shardVars[shard] = bv
	}
	return &scatterParams{
		ks:        ks,
		shardVars: shardVars,
	}
}

// Execute performs a non-streaming exec.
func (route *Route) Execute(vcursor VCursor, bindVars, joinVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	bindVars = combineVars(bindVars, joinVars)

	switch route.Opcode {
	case SelectNext, ExecDBA:
		return route.execAnyShard(vcursor, bindVars)
	case UpdateEqual:
		return route.execUpdateEqual(vcursor, bindVars)
	case DeleteEqual:
		return route.execDeleteEqual(vcursor, bindVars)
	case InsertSharded, InsertShardedIgnore:
		return route.execInsertSharded(vcursor, bindVars)
	case InsertUnsharded:
		return route.execInsertUnsharded(vcursor, bindVars)
	}

	var err error
	var params *scatterParams
	isDML := false
	switch route.Opcode {
	case SelectUnsharded, SelectScatter:
		params, err = route.paramsAllShards(vcursor, bindVars)
	case UpdateUnsharded, DeleteUnsharded:
		isDML = true
		params, err = route.paramsAllShards(vcursor, bindVars)
	case SelectEqual, SelectEqualUnique:
		params, err = route.paramsSelectEqual(vcursor, bindVars)
	case SelectIN:
		params, err = route.paramsSelectIN(vcursor, bindVars)
	default:
		// Unreachable.
		return nil, fmt.Errorf("unsupported query route: %v", route)
	}
	if err != nil {
		return nil, err
	}

	shardQueries := route.getShardQueries(route.Query, params)
	result, err := vcursor.ExecuteMultiShard(params.ks, shardQueries, isDML)
	if err != nil {
		return nil, err
	}
	if len(route.OrderBy) == 0 {
		return result, nil
	}

	return route.sort(result)
}

// StreamExecute performs a streaming exec.
func (route *Route) StreamExecute(vcursor VCursor, bindVars, joinVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	bindVars = combineVars(bindVars, joinVars)

	var err error
	var params *scatterParams
	switch route.Opcode {
	case SelectUnsharded, SelectScatter:
		params, err = route.paramsAllShards(vcursor, bindVars)
	case SelectEqual, SelectEqualUnique:
		params, err = route.paramsSelectEqual(vcursor, bindVars)
	case SelectIN:
		params, err = route.paramsSelectIN(vcursor, bindVars)
	default:
		return fmt.Errorf("query %q cannot be used for streaming", route.Query)
	}
	if err != nil {
		return err
	}
	if len(route.OrderBy) == 0 {
		return vcursor.StreamExecuteMulti(
			route.Query,
			params.ks,
			params.shardVars,
			callback,
		)
	}

	return mergeSort(vcursor, route.Query, route.OrderBy, params, callback)
}

// GetFields fetches the field info.
func (route *Route) GetFields(vcursor VCursor, bindVars, joinVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	bindVars = combineVars(bindVars, joinVars)

	ks, shard, err := route.anyShard(vcursor, route.Keyspace)
	if err != nil {
		return nil, err
	}

	return route.execShard(vcursor, route.FieldQuery, bindVars, ks, shard, false /* isDML */)
}

func combineVars(bv1, bv2 map[string]*querypb.BindVariable) map[string]*querypb.BindVariable {
	out := make(map[string]*querypb.BindVariable)
	for k, v := range bv1 {
		out[k] = v
	}
	for k, v := range bv2 {
		out[k] = v
	}
	return out
}

func (route *Route) paramsAllShards(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*scatterParams, error) {
	ks, allShards, err := vcursor.GetKeyspaceShards(route.Keyspace)
	if err != nil {
		return nil, vterrors.Wrap(err, "paramsAllShards")
	}
	var shards []string
	for _, shard := range allShards {
		shards = append(shards, shard.Name)
	}
	return newScatterParams(ks, bindVars, shards), nil
}

func (route *Route) paramsSelectEqual(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*scatterParams, error) {
	key, err := route.Values[0].ResolveValue(bindVars)
	if err != nil {
		return nil, vterrors.Wrap(err, "paramsSelectEqual")
	}
	ks, routing, err := route.resolveShards(vcursor, bindVars, []sqltypes.Value{key})
	if err != nil {
		return nil, vterrors.Wrap(err, "paramsSelectEqual")
	}
	return newScatterParams(ks, bindVars, routing.Shards()), nil
}

func (route *Route) paramsSelectIN(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*scatterParams, error) {
	keys, err := route.Values[0].ResolveList(bindVars)
	if err != nil {
		return nil, vterrors.Wrap(err, "paramsSelectIN")
	}
	ks, routing, err := route.resolveShards(vcursor, bindVars, keys)
	if err != nil {
		return nil, vterrors.Wrap(err, "paramsSelectEqual")
	}
	return &scatterParams{
		ks:        ks,
		shardVars: routing.ShardVars(bindVars),
	}, nil
}

func (route *Route) execUpdateEqual(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	key, err := route.Values[0].ResolveValue(bindVars)
	if err != nil {
		return nil, vterrors.Wrap(err, "execUpdateEqual")
	}
	ks, shard, ksid, err := route.resolveSingleShard(vcursor, bindVars, key)
	if err != nil {
		return nil, vterrors.Wrap(err, "execUpdateEqual")
	}
	if len(ksid) == 0 {
		return &sqltypes.Result{}, nil
	}
	rewritten := sqlannotation.AddKeyspaceIDs(route.Query, [][]byte{ksid}, "")
	return route.execShard(vcursor, rewritten, bindVars, ks, shard, true /* isDML */)
}

func (route *Route) execDeleteEqual(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	key, err := route.Values[0].ResolveValue(bindVars)
	if err != nil {
		return nil, vterrors.Wrap(err, "execDeleteEqual")
	}
	ks, shard, ksid, err := route.resolveSingleShard(vcursor, bindVars, key)
	if err != nil {
		return nil, vterrors.Wrap(err, "execDeleteEqual")
	}
	if len(ksid) == 0 {
		return &sqltypes.Result{}, nil
	}
	if route.Subquery != "" && len(route.Table.Owned) != 0 {
		err = route.deleteVindexEntries(vcursor, bindVars, ks, shard, ksid)
		if err != nil {
			return nil, vterrors.Wrap(err, "execDeleteEqual")
		}
	}
	rewritten := sqlannotation.AddKeyspaceIDs(route.Query, [][]byte{ksid}, "")
	return route.execShard(vcursor, rewritten, bindVars, ks, shard, true /* isDML */)
}

func (route *Route) execInsertUnsharded(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	insertID, err := route.processGenerate(vcursor, bindVars)
	if err != nil {
		return nil, vterrors.Wrap(err, "execInsertUnsharded")
	}
	params, err := route.paramsAllShards(vcursor, bindVars)
	if err != nil {
		return nil, vterrors.Wrap(err, "execInsertUnsharded")
	}

	shardQueries := route.getShardQueries(route.Query, params)
	result, err := vcursor.ExecuteMultiShard(params.ks, shardQueries, true /* isDML */)
	if err != nil {
		return nil, vterrors.Wrap(err, "execInsertUnsharded")
	}

	// If processGenerate generated new values, it supercedes
	// any ids that MySQL might have generated. If both generated
	// values, we don't return an error because this behavior
	// is required to support migration.
	if insertID != 0 {
		result.InsertID = uint64(insertID)
	}
	return result, nil
}

func (route *Route) execInsertSharded(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	insertID, err := route.processGenerate(vcursor, bindVars)
	if err != nil {
		return nil, vterrors.Wrap(err, "execInsertSharded")
	}
	keyspace, shardQueries, err := route.getInsertShardedRoute(vcursor, bindVars)
	if err != nil {
		return nil, vterrors.Wrap(err, "execInsertSharded")
	}

	result, err := vcursor.ExecuteMultiShard(keyspace, shardQueries, true /* isDML */)
	if err != nil {
		return nil, vterrors.Wrap(err, "execInsertSharded")
	}

	// If processGenerate generated new values, it supercedes
	// any ids that MySQL might have generated. If both generated
	// values, we don't return an error because this behavior
	// is required to support migration.
	if insertID != 0 {
		result.InsertID = uint64(insertID)
	}
	return result, nil
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
		for i, ksid := range ksids {
			if len(ksid) == 0 {
				continue
			}
			shard, err := vcursor.GetShardForKeyspaceID(allShards, ksid)
			if err != nil {
				return "", nil, err
			}
			routing.Add(shard, sqltypes.ValueToProto(vindexKeys[i]))
		}
	case vindexes.NonUnique:
		ksidss, err := mapper.Map(vcursor, vindexKeys)
		if err != nil {
			return "", nil, err
		}
		for i, ksids := range ksidss {
			for _, ksid := range ksids {
				shard, err := vcursor.GetShardForKeyspaceID(allShards, ksid)
				if err != nil {
					return "", nil, err
				}
				routing.Add(shard, sqltypes.ValueToProto(vindexKeys[i]))
			}
		}
	default:
		panic("unexpected")
	}
	return newKeyspace, routing, nil
}

func (route *Route) resolveSingleShard(vcursor VCursor, bindVars map[string]*querypb.BindVariable, vindexKey sqltypes.Value) (newKeyspace, shard string, ksid []byte, err error) {
	newKeyspace, allShards, err := vcursor.GetKeyspaceShards(route.Keyspace)
	if err != nil {
		return "", "", nil, err
	}
	mapper := route.Vindex.(vindexes.Unique)
	ksids, err := mapper.Map(vcursor, []sqltypes.Value{vindexKey})
	if err != nil {
		return "", "", nil, err
	}
	ksid = ksids[0]
	if len(ksid) == 0 {
		return "", "", ksid, nil
	}
	shard, err = vcursor.GetShardForKeyspaceID(allShards, ksid)
	if err != nil {
		return "", "", nil, err
	}
	return newKeyspace, shard, ksid, nil
}

func (route *Route) deleteVindexEntries(vcursor VCursor, bindVars map[string]*querypb.BindVariable, ks, shard string, ksid []byte) error {
	result, err := route.execShard(vcursor, route.Subquery, bindVars, ks, shard, false /* isDML */)
	if err != nil {
		return err
	}
	if len(result.Rows) == 0 {
		return nil
	}
	for i, colVindex := range route.Table.Owned {
		ids := make([]sqltypes.Value, 0, len(result.Rows))
		for _, row := range result.Rows {
			ids = append(ids, row[i])
		}
		if err = colVindex.Vindex.(vindexes.Lookup).Delete(vcursor, ids, ksid); err != nil {
			return err
		}
	}
	return nil
}

// processGenerate generates new values using a sequence if necessary.
// If no value was generated, it returns 0. Values are generated only
// for cases where none are supplied.
func (route *Route) processGenerate(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (insertID int64, err error) {
	if route.Generate == nil {
		return 0, nil
	}

	// Scan input values to compute the number of values to generate, and
	// keep track of where they should be filled.
	resolved, err := route.Generate.Values.ResolveList(bindVars)
	if err != nil {
		return 0, vterrors.Wrap(err, "processGenerate")
	}
	count := int64(0)
	for _, val := range resolved {
		if val.IsNull() {
			count++
		}
	}

	// If generation is needed, generate the requested number of values (as one call).
	if count != 0 {
		ks, shard, err := route.anyShard(vcursor, route.Generate.Keyspace)
		if err != nil {
			return 0, vterrors.Wrap(err, "processGenerate")
		}
		bindVars := map[string]*querypb.BindVariable{"n": sqltypes.Int64BindVariable(count)}
		qr, err := vcursor.ExecuteStandalone(route.Generate.Query, bindVars, ks, shard)
		if err != nil {
			return 0, err
		}
		// If no rows are returned, it's an internal error, and the code
		// must panic, which will be caught and reported.
		insertID, err = sqltypes.ToInt64(qr.Rows[0][0])
		if err != nil {
			return 0, err
		}
	}

	// Fill the holes where no value was supplied.
	cur := insertID
	for i, v := range resolved {
		if v.IsNull() {
			bindVars[SeqVarName+strconv.Itoa(i)] = sqltypes.Int64BindVariable(cur)
			cur++
		} else {
			bindVars[SeqVarName+strconv.Itoa(i)] = sqltypes.ValueBindVariable(v)
		}
	}
	return insertID, nil
}

// getInsertShardedRoute performs all the vindex related work
// and returns a map of shard to queries.
// Using the primary vindex, it computes the target keyspace ids.
// For owned vindexes, it creates entries.
// For unowned vindexes with no input values, it reverse maps.
// For unowned vindexes with values, it validates.
// If it's an IGNORE or ON DUPLICATE key insert, it drops unroutable rows.
func (route *Route) getInsertShardedRoute(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (keyspace string, shardQueries map[string]*querypb.BoundQuery, err error) {
	keyspace, allShards, err := vcursor.GetKeyspaceShards(route.Keyspace)
	if err != nil {
		return "", nil, vterrors.Wrap(err, "getInsertShardedRoute")
	}

	allKeys := make([][]sqltypes.Value, len(route.Values))
	for colNum, colValues := range route.Values {
		var err error
		allKeys[colNum], err = colValues.ResolveList(bindVars)
		if err != nil {
			return "", nil, vterrors.Wrap(err, "getInsertShardedRoute")
		}
	}

	// The output from the following 'process' functions is a list of
	// keyspace ids. For regular inserts, a failure to find a route
	// results in an error. For 'ignore' type inserts, the keyspace
	// id is returned as nil, which is used later to drop such rows.
	keyspaceIDs, err := route.processPrimary(vcursor, allKeys[0], route.Table.ColumnVindexes[0], bindVars)
	if err != nil {
		return "", nil, vterrors.Wrap(err, "getInsertShardedRoute")
	}
	for colNum := 1; colNum < len(allKeys); colNum++ {
		colVindex := route.Table.ColumnVindexes[colNum]
		var err error
		if colVindex.Owned {
			switch route.Opcode {
			case InsertSharded:
				err = route.processOwned(vcursor, allKeys[colNum], colVindex, bindVars, keyspaceIDs)
			case InsertShardedIgnore:
				// For InsertShardedIgnore, the work is substantially different.
				// So,, we use a separate function.
				err = route.processOwnedIgnore(vcursor, allKeys[colNum], colVindex, bindVars, keyspaceIDs)
			default:
				err = vterrors.Errorf(vtrpcpb.Code_INTERNAL, "BUG: unexpected opcode: %v", route.Opcode)
			}
		} else {
			err = route.processUnowned(vcursor, allKeys[colNum], colVindex, bindVars, keyspaceIDs)
		}
		if err != nil {
			return "", nil, vterrors.Wrap(err, "getInsertShardedRoute")
		}
	}

	shardKeyspaceIDMap := make(map[string][][]byte)
	routing := make(map[string][]string)
	for rowNum, ksid := range keyspaceIDs {
		if ksid == nil {
			continue
		}
		shard, err := vcursor.GetShardForKeyspaceID(allShards, ksid)
		if err != nil {
			return "", nil, vterrors.Wrap(err, "getInsertShardedRoute")
		}
		shardKeyspaceIDMap[shard] = append(shardKeyspaceIDMap[shard], ksid)
		routing[shard] = append(routing[shard], route.Mid[rowNum])
	}

	shardQueries = make(map[string]*querypb.BoundQuery, len(routing))
	for shard := range routing {
		rewritten := route.Prefix + strings.Join(routing[shard], ",") + route.Suffix
		rewritten = sqlannotation.AddKeyspaceIDs(rewritten, shardKeyspaceIDMap[shard], "")
		shardQueries[shard] = &querypb.BoundQuery{
			Sql:           rewritten,
			BindVariables: bindVars,
		}
	}

	return keyspace, shardQueries, nil
}

// processPrimary maps the primary vindex values to the kesypace ids.
func (route *Route) processPrimary(vcursor VCursor, vindexKeys []sqltypes.Value, colVindex *vindexes.ColumnVindex, bv map[string]*querypb.BindVariable) (keyspaceIDs [][]byte, err error) {
	for _, vindexKey := range vindexKeys {
		if vindexKey.IsNull() {
			return nil, fmt.Errorf("value must be supplied for column %v", colVindex.Column)
		}
	}
	mapper := colVindex.Vindex.(vindexes.Unique)
	keyspaceIDs, err = mapper.Map(vcursor, vindexKeys)
	if err != nil {
		return nil, err
	}

	for rowNum, vindexKey := range vindexKeys {
		if keyspaceIDs[rowNum] == nil {
			if route.Opcode != InsertShardedIgnore {
				return nil, fmt.Errorf("could not map %v to a keyspace id", vindexKey)
			}
			// InsertShardedIgnore: skip the row.
			continue
		}
		bv[insertVarName(colVindex, rowNum)] = sqltypes.ValueBindVariable(vindexKey)
	}
	return keyspaceIDs, nil
}

// processOwned creates vindex entries for the values of an owned column for InsertSharded.
func (route *Route) processOwned(vcursor VCursor, vindexKeys []sqltypes.Value, colVindex *vindexes.ColumnVindex, bv map[string]*querypb.BindVariable, ksids [][]byte) error {
	for rowNum, vindexKey := range vindexKeys {
		if vindexKey.IsNull() {
			return fmt.Errorf("value must be supplied for column %v", colVindex.Column)
		}
		bv[insertVarName(colVindex, rowNum)] = sqltypes.ValueBindVariable(vindexKey)
	}
	return colVindex.Vindex.(vindexes.Lookup).Create(vcursor, vindexKeys, ksids, false /* ignoreMode */)
}

// processOwnedIgnore creates vindex entries for the values of an owned column for InsertShardedIgnore.
func (route *Route) processOwnedIgnore(vcursor VCursor, vindexKeys []sqltypes.Value, colVindex *vindexes.ColumnVindex, bv map[string]*querypb.BindVariable, ksids [][]byte) error {
	var createIndexes []int
	var createKeys []sqltypes.Value
	var createKsids [][]byte

	for rowNum, vindexKey := range vindexKeys {
		if ksids[rowNum] == nil {
			continue
		}
		if vindexKey.IsNull() {
			return fmt.Errorf("value must be supplied for column %v", colVindex.Column)
		}
		createIndexes = append(createIndexes, rowNum)
		createKeys = append(createKeys, vindexKey)
		createKsids = append(createKsids, ksids[rowNum])
		bv[insertVarName(colVindex, rowNum)] = sqltypes.ValueBindVariable(vindexKey)
	}
	if createKeys == nil {
		return nil
	}

	err := colVindex.Vindex.(vindexes.Lookup).Create(vcursor, createKeys, createKsids, true /* ignoreMode */)
	if err != nil {
		return err
	}

	// After creation, verify that the keys map to the keyspace ids. If not, remove
	// those that don't map.
	verified, err := colVindex.Vindex.Verify(vcursor, createKeys, createKsids)
	if err != nil {
		return err
	}
	for i, v := range verified {
		if !v {
			ksids[createIndexes[i]] = nil
		}
	}
	return nil
}

// processUnowned either reverse maps or validates the values for an unowned column.
func (route *Route) processUnowned(vcursor VCursor, vindexKeys []sqltypes.Value, colVindex *vindexes.ColumnVindex, bv map[string]*querypb.BindVariable, ksids [][]byte) error {
	var reverseIndexes []int
	var reverseKsids [][]byte
	var verifyIndexes []int
	var verifyKeys []sqltypes.Value
	var verifyKsids [][]byte

	for rowNum, vindexKey := range vindexKeys {
		if ksids[rowNum] == nil {
			continue
		}
		if vindexKey.IsNull() {
			reverseIndexes = append(reverseIndexes, rowNum)
			reverseKsids = append(reverseKsids, ksids[rowNum])
		} else {
			verifyIndexes = append(verifyIndexes, rowNum)
			verifyKeys = append(verifyKeys, vindexKey)
			verifyKsids = append(verifyKsids, ksids[rowNum])
		}
	}

	// For cases where a value was not supplied, we reverse map it
	// from the keyspace id, if possible.
	if reverseKsids != nil {
		reversible, ok := colVindex.Vindex.(vindexes.Reversible)
		if !ok {
			return fmt.Errorf("value must be supplied for column %v", colVindex.Column)
		}
		reverseKeys, err := reversible.ReverseMap(vcursor, reverseKsids)
		if err != nil {
			return err
		}
		for i, reverseKey := range reverseKeys {
			rowNum := reverseIndexes[i]
			bv[insertVarName(colVindex, rowNum)] = sqltypes.ValueBindVariable(reverseKey)
		}
	}

	// If values were supplied, we validate against keyspace id.
	if verifyKsids != nil {
		verified, err := colVindex.Vindex.Verify(vcursor, verifyKeys, verifyKsids)
		if err != nil {
			return err
		}
		for i, v := range verified {
			rowNum := verifyIndexes[i]
			if !v {
				if route.Opcode != InsertShardedIgnore {
					return fmt.Errorf("values %v for column %v does not map to keyspaceids", vindexKeys, colVindex.Column)
				}
				// InsertShardedIgnore: skip the row.
				ksids[rowNum] = nil
				continue
			}
			bv[insertVarName(colVindex, rowNum)] = sqltypes.ValueBindVariable(vindexKeys[rowNum])
		}
	}
	return nil
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

func (route *Route) execAnyShard(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	ks, shard, err := route.anyShard(vcursor, route.Keyspace)
	if err != nil {
		return nil, fmt.Errorf("execAnyShard: %v", err)
	}
	return vcursor.ExecuteStandalone(route.Query, bindVars, ks, shard)
}

func (route *Route) execShard(vcursor VCursor, query string, bindVars map[string]*querypb.BindVariable, keyspace, shard string, isDML bool) (*sqltypes.Result, error) {
	return vcursor.ExecuteMultiShard(keyspace, map[string]*querypb.BoundQuery{
		shard: {
			Sql:           query,
			BindVariables: bindVars,
		},
	}, isDML)
}

func (route *Route) anyShard(vcursor VCursor, keyspace *vindexes.Keyspace) (string, string, error) {
	ks, allShards, err := vcursor.GetKeyspaceShards(keyspace)
	if err != nil {
		return "", "", err
	}
	return ks, allShards[0].Name, nil
}

func (route *Route) getShardQueries(query string, params *scatterParams) map[string]*querypb.BoundQuery {
	shardQueries := make(map[string]*querypb.BoundQuery, len(params.shardVars))
	for shard, shardVars := range params.shardVars {
		shardQueries[shard] = &querypb.BoundQuery{
			Sql:           query,
			BindVariables: shardVars,
		}
	}
	return shardQueries
}

func insertVarName(colVindex *vindexes.ColumnVindex, rowNum int) string {
	return "_" + colVindex.Column.CompliantName() + strconv.Itoa(rowNum)
}
