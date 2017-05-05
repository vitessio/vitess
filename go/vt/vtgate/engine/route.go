// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package engine

import (
	"encoding/json"
	"fmt"

	"strconv"
	"strings"

	"github.com/youtube/vitess/go/sqltypes"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
	"github.com/youtube/vitess/go/vt/sqlannotation"
	"github.com/youtube/vitess/go/vt/vtgate/vindexes"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/querytypes"
)

// Route represents the instructions to route a query to
// one or many vttablets. The meaning and values for the
// the fields are described in the RouteOpcode values comments.
type Route struct {
	Opcode     RouteOpcode
	Keyspace   *vindexes.Keyspace
	Query      string
	FieldQuery string
	Vindex     vindexes.Vindex
	Values     interface{}
	JoinVars   map[string]struct{}
	Table      *vindexes.Table
	Subquery   string
	Generate   *Generate
	Prefix     string
	Mid        []string
	Suffix     string
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
		Opcode     RouteOpcode         `json:",omitempty"`
		Keyspace   *vindexes.Keyspace  `json:",omitempty"`
		Query      string              `json:",omitempty"`
		FieldQuery string              `json:",omitempty"`
		Vindex     string              `json:",omitempty"`
		Values     interface{}         `json:",omitempty"`
		JoinVars   map[string]struct{} `json:",omitempty"`
		Table      string              `json:",omitempty"`
		Subquery   string              `json:",omitempty"`
		Generate   *Generate           `json:",omitempty"`
		Prefix     string              `json:",omitempty"`
		Mid        []string            `json:",omitempty"`
		Suffix     string              `json:",omitempty"`
	}{
		Opcode:     route.Opcode,
		Keyspace:   route.Keyspace,
		Query:      route.Query,
		FieldQuery: route.FieldQuery,
		Vindex:     vindexName,
		Values:     prettyValue(route.Values),
		JoinVars:   route.JoinVars,
		Table:      tname,
		Subquery:   route.Subquery,
		Generate:   route.Generate,
		Prefix:     route.Prefix,
		Mid:        route.Mid,
		Suffix:     route.Suffix,
	}
	return json.Marshal(marshalRoute)
}

// Generate represents the instruction to generate
// a value from a sequence.
// TODO(sougou): we should eventually merge this with SelectNext
// but it's not worth it right now.
type Generate struct {
	Keyspace *vindexes.Keyspace
	Query    string
	// Values are the supplied values. New values will be generated
	// for NULL values. Otherwise, the supplied value will
	// be used.
	Values interface{}
}

// MarshalJSON serializes Generate into a JSON representation.
// It's used for testing and diagnostics.
func (gen *Generate) MarshalJSON() ([]byte, error) {
	jsongen := struct {
		Keyspace *vindexes.Keyspace `json:",omitempty"`
		Query    string             `json:",omitempty"`
		Values   interface{}        `json:",omitempty"`
	}{
		Keyspace: gen.Keyspace,
		Query:    gen.Query,
		Values:   prettyValue(gen.Values),
	}
	return json.Marshal(jsongen)
}

// prettyValue converts the Values field of a Route
// to a form that will be human-readable when
// converted to JSON. This is for testing and diagnostics.
func prettyValue(value interface{}) interface{} {
	switch value := value.(type) {
	case []byte:
		return string(value)
	case []interface{}:
		newvals := make([]interface{}, len(value))
		for i, old := range value {
			newvals[i] = prettyValue(old)
		}
		return newvals
	}
	return value
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
	"UpdateUnsharded",
	"UpdateEqual",
	"DeleteUnsharded",
	"DeleteEqual",
	"InsertUnsharded",
	"InsertSharded",
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

// Execute performs a non-streaming exec.
func (route *Route) Execute(vcursor VCursor, bindVars, joinVars map[string]interface{}, wantfields bool) (*sqltypes.Result, error) {
	bindVars = combineVars(bindVars, joinVars)

	switch route.Opcode {
	case SelectNext:
		return route.execSelectNext(vcursor, bindVars)
	case UpdateEqual:
		return route.execUpdateEqual(vcursor, bindVars)
	case DeleteEqual:
		return route.execDeleteEqual(vcursor, bindVars)
	case InsertSharded:
		return route.execInsertSharded(vcursor, bindVars)
	case InsertUnsharded:
		return route.execInsertUnsharded(vcursor, bindVars)
	}

	var err error
	var params *scatterParams
	switch route.Opcode {
	case SelectUnsharded, UpdateUnsharded, DeleteUnsharded, SelectScatter:
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
	return vcursor.ExecuteMultiShard(params.ks, shardQueries)
}

// StreamExecute performs a streaming exec.
func (route *Route) StreamExecute(vcursor VCursor, bindVars, joinVars map[string]interface{}, wantfields bool, callback func(*sqltypes.Result) error) error {
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
	return vcursor.StreamExecuteMulti(
		route.Query,
		params.ks,
		params.shardVars,
		callback,
	)
}

// GetFields fetches the field info.
func (route *Route) GetFields(vcursor VCursor, bindVars, joinVars map[string]interface{}) (*sqltypes.Result, error) {
	bindVars = combineVars(bindVars, joinVars)

	ks, shard, err := route.anyShard(vcursor, route.Keyspace)
	if err != nil {
		return nil, err
	}

	return route.execShard(vcursor, route.FieldQuery, bindVars, ks, shard)
}

func combineVars(bv1, bv2 map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{})
	for k, v := range bv1 {
		out[k] = v
	}
	for k, v := range bv2 {
		out[k] = v
	}
	return out
}

func (route *Route) paramsAllShards(vcursor VCursor, bindVars map[string]interface{}) (*scatterParams, error) {
	ks, allShards, err := vcursor.GetKeyspaceShards(route.Keyspace)
	if err != nil {
		return nil, fmt.Errorf("paramsAllShards: %v", err)
	}
	var shards []string
	for _, shard := range allShards {
		shards = append(shards, shard.Name)
	}
	return newScatterParams(ks, bindVars, shards), nil
}

func (route *Route) paramsSelectEqual(vcursor VCursor, bindVars map[string]interface{}) (*scatterParams, error) {
	keys, err := route.resolveKeys([]interface{}{route.Values}, bindVars)
	if err != nil {
		return nil, fmt.Errorf("paramsSelectEqual: %v", err)
	}
	ks, routing, err := route.resolveShards(vcursor, bindVars, keys)
	if err != nil {
		return nil, fmt.Errorf("paramsSelectEqual: %v", err)
	}
	return newScatterParams(ks, bindVars, routing.Shards()), nil
}

func (route *Route) paramsSelectIN(vcursor VCursor, bindVars map[string]interface{}) (*scatterParams, error) {
	vals, err := route.resolveList(route.Values, bindVars)
	if err != nil {
		return nil, fmt.Errorf("paramsSelectIN: %v", err)
	}
	keys, err := route.resolveKeys(vals, bindVars)
	if err != nil {
		return nil, fmt.Errorf("paramsSelectIN: %v", err)
	}
	ks, routing, err := route.resolveShards(vcursor, bindVars, keys)
	if err != nil {
		return nil, fmt.Errorf("paramsSelectEqual: %v", err)
	}
	return &scatterParams{
		ks:        ks,
		shardVars: routing.ShardVars(bindVars),
	}, nil
}

func (route *Route) execSelectNext(vcursor VCursor, bindVars map[string]interface{}) (*sqltypes.Result, error) {
	ks, shard, err := route.anyShard(vcursor, route.Keyspace)
	if err != nil {
		return nil, fmt.Errorf("execSelectNext: %v", err)
	}
	return vcursor.ExecuteStandalone(route.Query, bindVars, ks, shard)
}

func (route *Route) execUpdateEqual(vcursor VCursor, bindVars map[string]interface{}) (*sqltypes.Result, error) {
	keys, err := route.resolveKeys([]interface{}{route.Values}, bindVars)
	if err != nil {
		return nil, fmt.Errorf("execUpdateEqual: %v", err)
	}
	ks, shard, ksid, err := route.resolveSingleShard(vcursor, bindVars, keys[0])
	if err != nil {
		return nil, fmt.Errorf("execUpdateEqual: %v", err)
	}
	if len(ksid) == 0 {
		return &sqltypes.Result{}, nil
	}
	rewritten := sqlannotation.AddKeyspaceIDs(route.Query, [][]byte{ksid}, "")
	return route.execShard(vcursor, rewritten, bindVars, ks, shard)
}

func (route *Route) execDeleteEqual(vcursor VCursor, bindVars map[string]interface{}) (*sqltypes.Result, error) {
	keys, err := route.resolveKeys([]interface{}{route.Values}, bindVars)
	if err != nil {
		return nil, fmt.Errorf("execDeleteEqual: %v", err)
	}
	ks, shard, ksid, err := route.resolveSingleShard(vcursor, bindVars, keys[0])
	if err != nil {
		return nil, fmt.Errorf("execDeleteEqual: %v", err)
	}
	if len(ksid) == 0 {
		return &sqltypes.Result{}, nil
	}
	if route.Subquery != "" && len(route.Table.Owned) != 0 {
		err = route.deleteVindexEntries(vcursor, bindVars, ks, shard, ksid)
		if err != nil {
			return nil, fmt.Errorf("execDeleteEqual: %v", err)
		}
	}
	rewritten := sqlannotation.AddKeyspaceIDs(route.Query, [][]byte{ksid}, "")
	return route.execShard(vcursor, rewritten, bindVars, ks, shard)
}

func (route *Route) execInsertUnsharded(vcursor VCursor, bindVars map[string]interface{}) (*sqltypes.Result, error) {
	insertID, err := route.handleGenerate(vcursor, bindVars)
	if err != nil {
		return nil, fmt.Errorf("execInsertUnsharded: %v", err)
	}
	params, err := route.paramsAllShards(vcursor, bindVars)
	if err != nil {
		return nil, fmt.Errorf("execInsertUnsharded: %v", err)
	}

	shardQueries := route.getShardQueries(route.Query, params)
	result, err := vcursor.ExecuteMultiShard(params.ks, shardQueries)
	if err != nil {
		return nil, fmt.Errorf("execInsertUnsharded: %v", err)
	}

	// If handleGenerate generated new values, it supercedes
	// any ids that MySQL might have generated. If both generated
	// values, we don't return an error because this behavior
	// is required to support migration.
	if insertID != 0 {
		result.InsertID = uint64(insertID)
	}
	return result, nil
}

func (route *Route) execInsertSharded(vcursor VCursor, bindVars map[string]interface{}) (*sqltypes.Result, error) {
	insertID, err := route.handleGenerate(vcursor, bindVars)
	if err != nil {
		return nil, fmt.Errorf("execInsertSharded: %v", err)
	}
	keyspace, shardQueries, err := route.getInsertShardedRoute(vcursor, bindVars)
	if err != nil {
		return nil, fmt.Errorf("execInsertSharded: %v", err)
	}

	result, err := vcursor.ExecuteMultiShard(keyspace, shardQueries)

	if err != nil {
		return nil, fmt.Errorf("execInsertSharded: %v", err)
	}

	// If handleGenerate generated new values, it supercedes
	// any ids that MySQL might have generated. If both generated
	// values, we don't return an error because this behavior
	// is required to support migration.
	if insertID != 0 {
		result.InsertID = uint64(insertID)
	}
	return result, nil
}

func (route *Route) getInsertShardedRoute(vcursor VCursor, bindVars map[string]interface{}) (keyspace string, shardQueries map[string]querytypes.BoundQuery, err error) {
	keyspaceIDs := [][]byte{}
	routing := make(map[string][]string)
	shardKeyspaceIDMap := make(map[string][][]byte)
	keyspace, allShards, err := vcursor.GetKeyspaceShards(route.Keyspace)
	if err != nil {
		return "", nil, fmt.Errorf("getInsertShardedRoute: %v", err)
	}

	inputs := route.Values.([]interface{})
	allKeys := make([][]interface{}, len(inputs[0].([]interface{})))
	for _, input := range inputs {
		keys, err := route.resolveKeys(input.([]interface{}), bindVars)
		if err != nil {
			return "", nil, fmt.Errorf("getInsertShardedRoute: %v", err)
		}
		for colNum := 0; colNum < len(keys); colNum++ {
			allKeys[colNum] = append(allKeys[colNum], keys[colNum])
		}
	}

	keyspaceIDs, err = route.handlePrimary(vcursor, allKeys[0], route.Table.ColumnVindexes[0], bindVars)
	if err != nil {
		return "", nil, fmt.Errorf("getInsertShardedRoute: %v", err)
	}

	for colNum := 1; colNum < len(allKeys); colNum++ {
		err := route.handleNonPrimary(vcursor, allKeys[colNum], route.Table.ColumnVindexes[colNum], bindVars, keyspaceIDs)
		if err != nil {
			return "", nil, fmt.Errorf("getInsertShardedRoute: %v", err)
		}
	}
	for rowNum := range keyspaceIDs {
		shard, err := vcursor.GetShardForKeyspaceID(allShards, keyspaceIDs[rowNum])
		routing[shard] = append(routing[shard], route.Mid[rowNum])
		if err != nil {
			return "", nil, fmt.Errorf("getInsertShardedRoute: %v", err)
		}
		shardKeyspaceIDMap[shard] = append(shardKeyspaceIDMap[shard], keyspaceIDs[rowNum])
	}

	shardQueries = make(map[string]querytypes.BoundQuery, len(routing))
	for shard := range routing {
		rewritten := route.Prefix + strings.Join(routing[shard], ",") + route.Suffix
		rewritten = sqlannotation.AddKeyspaceIDs(rewritten, shardKeyspaceIDMap[shard], "")
		query := querytypes.BoundQuery{
			Sql:           rewritten,
			BindVariables: bindVars,
		}
		shardQueries[shard] = query
	}

	return keyspace, shardQueries, nil
}

// resolveList returns a list of values, typically for an IN clause. If the input
// is a bind var name, it uses the list provided in the bind var. If the input is
// already a list, it returns just that.
func (route *Route) resolveList(val interface{}, bindVars map[string]interface{}) ([]interface{}, error) {
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
func (route *Route) resolveKeys(vals []interface{}, bindVars map[string]interface{}) (keys []interface{}, err error) {
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

func (route *Route) resolveShards(vcursor VCursor, bindVars map[string]interface{}, vindexKeys []interface{}) (newKeyspace string, routing routingMap, err error) {
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
			routing.Add(shard, vindexKeys[i])
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
				routing.Add(shard, vindexKeys[i])
			}
		}
	default:
		panic("unexpected")
	}
	return newKeyspace, routing, nil
}

func (route *Route) resolveSingleShard(vcursor VCursor, bindVars map[string]interface{}, vindexKey interface{}) (newKeyspace, shard string, ksid []byte, err error) {
	newKeyspace, allShards, err := vcursor.GetKeyspaceShards(route.Keyspace)
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
	shard, err = vcursor.GetShardForKeyspaceID(allShards, ksid)
	if err != nil {
		return "", "", nil, err
	}
	return newKeyspace, shard, ksid, nil
}

func (route *Route) deleteVindexEntries(vcursor VCursor, bindVars map[string]interface{}, ks, shard string, ksid []byte) error {
	result, err := route.execShard(vcursor, route.Subquery, bindVars, ks, shard)
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
			panic("unexpected")
		}
	}
	return nil
}

// handleGenerate generates new values using a sequence if necessary.
// If no value was generated, it returns 0. Values are generated only
// for cases where none are supplied.
func (route *Route) handleGenerate(vcursor VCursor, bindVars map[string]interface{}) (insertID int64, err error) {
	if route.Generate == nil {
		return 0, nil
	}

	// Scan input values to compute the number of values to generate, and
	// keep track of where they should be filled.
	count := 0
	resolved := make([]interface{}, len(route.Generate.Values.([]interface{})))
	for i, val := range route.Generate.Values.([]interface{}) {
		if v, ok := val.(string); ok {
			val, ok = bindVars[v[1:]]
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

	// If generation is needed, generate the requested number of values (as one call).
	if count != 0 {
		ks, shard, err := route.anyShard(vcursor, route.Generate.Keyspace)
		if err != nil {
			return 0, fmt.Errorf("handleGenerate: %v", err)
		}
		bindVars := map[string]interface{}{"n": int64(count)}
		qr, err := vcursor.ExecuteStandalone(route.Generate.Query, bindVars, ks, shard)
		if err != nil {
			return 0, err
		}
		// If no rows are returned, it's an internal error, and the code
		// must panic, which will caught and reported.
		insertID, err = qr.Rows[0][0].ParseInt64()
		if err != nil {
			return 0, err
		}
	}

	// Fill the holes where no value was supplied.
	cur := insertID
	for i, v := range resolved {
		if v != nil {
			bindVars[SeqVarName+strconv.Itoa(i)] = v
		} else {
			bindVars[SeqVarName+strconv.Itoa(i)] = cur
			cur++
		}
	}
	return insertID, nil
}

func (route *Route) handlePrimary(vcursor VCursor, vindexKeys []interface{}, colVindex *vindexes.ColumnVindex, bv map[string]interface{}) (keyspaceIDs [][]byte, err error) {
	for _, vindexkey := range vindexKeys {
		if vindexkey == nil {
			return nil, fmt.Errorf("value must be supplied for column %v", colVindex.Column)
		}
	}
	mapper := colVindex.Vindex.(vindexes.Unique)
	keyspaceIDs, err = mapper.Map(vcursor, vindexKeys)
	if err != nil {
		return nil, err
	}
	if len(keyspaceIDs) != len(vindexKeys) {
		return nil, fmt.Errorf("could not map %v to a keyspaceids", vindexKeys)
	}
	for rowNum, vindexKey := range vindexKeys {
		if len(keyspaceIDs[rowNum]) == 0 {
			return nil, fmt.Errorf("could not map %v to a keyspace id", vindexKey)
		}
		bv["_"+colVindex.Column.CompliantName()+strconv.Itoa(rowNum)] = vindexKey
	}
	return keyspaceIDs, nil
}

func (route *Route) handleNonPrimary(vcursor VCursor, vindexKeys []interface{}, colVindex *vindexes.ColumnVindex, bv map[string]interface{}, ksids [][]byte) error {
	if colVindex.Owned {
		for rowNum, vindexKey := range vindexKeys {
			if vindexKey == nil {
				return fmt.Errorf("value must be supplied for column %v", colVindex.Column)
			}
			bv["_"+colVindex.Column.CompliantName()+strconv.Itoa(rowNum)] = vindexKey
		}
		err := colVindex.Vindex.(vindexes.Lookup).Create(vcursor, vindexKeys, ksids)
		if err != nil {
			return err
		}
	} else {
		var reverseKsids [][]byte
		var verifyKsids [][]byte
		for rowNum, vindexKey := range vindexKeys {
			if vindexKey == nil {
				reverseKsids = append(reverseKsids, ksids[rowNum])
			} else {
				verifyKsids = append(verifyKsids, ksids[rowNum])
			}
			bv["_"+colVindex.Column.CompliantName()+strconv.Itoa(rowNum)] = vindexKey
		}
		var err error
		if reverseKsids != nil {
			reversible, ok := colVindex.Vindex.(vindexes.Reversible)
			if !ok {
				return fmt.Errorf("value must be supplied for column %v", colVindex.Column)
			}
			vindexKeys, err = reversible.ReverseMap(vcursor, reverseKsids)
			if err != nil {
				return err
			}
			for rowNum, vindexKey := range vindexKeys {
				bv["_"+colVindex.Column.CompliantName()+strconv.Itoa(rowNum)] = vindexKey
			}
		}

		if verifyKsids != nil {
			ok, err := colVindex.Vindex.Verify(vcursor, vindexKeys, verifyKsids)
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("values %v for column %v does not map to keyspaceids", vindexKeys, colVindex.Column)
			}
		}
	}
	return nil
}

func (route *Route) execShard(vcursor VCursor, query string, bindVars map[string]interface{}, keyspace, shard string) (*sqltypes.Result, error) {
	return vcursor.ExecuteMultiShard(keyspace, map[string]querytypes.BoundQuery{
		shard: {
			Sql:           query,
			BindVariables: bindVars,
		},
	})
}

func (route *Route) anyShard(vcursor VCursor, keyspace *vindexes.Keyspace) (string, string, error) {
	ks, allShards, err := vcursor.GetKeyspaceShards(keyspace)
	if err != nil {
		return "", "", err
	}
	return ks, allShards[0].Name, nil
}

func (route *Route) getShardQueries(query string, params *scatterParams) map[string]querytypes.BoundQuery {
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
