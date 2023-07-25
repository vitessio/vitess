/*
Copyright 2022 The Vitess Authors.

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
	"context"
	"encoding/json"
	"strconv"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// Opcode is a number representing the opcode
// for any engine primitve.
type Opcode int

// This is the list of Opcode values.
const (
	// Unsharded is for routing a statement
	// to an unsharded keyspace.
	Unsharded = Opcode(iota)
	// EqualUnique is for routing a query to a single shard.
	// Requires: A Unique Vindex, and a single Value.
	EqualUnique
	// Equal is for routing a query using a non-unique vindex.
	// Requires: A Vindex, and a single Value.
	Equal
	// IN is for routing a statement to a multi shard.
	// Requires: A Vindex, and a multi Values.
	IN
	// MultiEqual is used for routing queries with IN with tuple clause
	// Requires: A Vindex, and a multi Tuple Values.
	MultiEqual
	// SubShard is for when we are missing one or more columns from a composite vindex
	SubShard
	// Scatter is for routing a scattered statement.
	Scatter
	// Next is for fetching from a sequence.
	Next
	// DBA is used for routing DBA queries
	// e.g: Select * from information_schema.tables where schema_name = "a"
	DBA
	// Reference is for fetching from a reference table.
	Reference
	// None is used for queries which do not need routing
	None
	// ByDestination is to route explicitly to a given target destination.
	// Is used when the query explicitly sets a target destination:
	// in the clause e.g: UPDATE `keyspace[-]`.x1 SET foo=1
	ByDestination
)

var opName = map[Opcode]string{
	Unsharded:     "Unsharded",
	EqualUnique:   "EqualUnique",
	Equal:         "Equal",
	IN:            "IN",
	MultiEqual:    "MultiEqual",
	Scatter:       "Scatter",
	DBA:           "DBA",
	Next:          "Next",
	Reference:     "Reference",
	None:          "None",
	ByDestination: "ByDestination",
	SubShard:      "SubShard",
}

// MarshalJSON serializes the Opcode as a JSON string.
// It's used for testing and diagnostics.
func (code Opcode) MarshalJSON() ([]byte, error) {
	return json.Marshal(opName[code])
}

// String returns a string presentation of this opcode
func (code Opcode) String() string {
	return opName[code]
}

type RoutingParameters struct {
	// Opcode is the execution opcode.
	Opcode Opcode

	// Keyspace specifies the keyspace to send the query to.
	Keyspace *vindexes.Keyspace

	// The following two fields are used when routing information_schema queries
	SysTableTableSchema []evalengine.Expr
	SysTableTableName   map[string]evalengine.Expr

	// TargetDestination specifies an explicit target destination to send the query to.
	// This will bypass the routing logic.
	TargetDestination key.Destination // update `user[-]@replica`.user set ....

	// Vindex specifies the vindex to be used.
	Vindex vindexes.Vindex

	// Values specifies the vindex values to use for routing.
	Values []evalengine.Expr
}

func (code Opcode) IsSingleShard() bool {
	switch code {
	case Unsharded, DBA, Next, EqualUnique, Reference:
		return true
	}
	return false
}

func (rp *RoutingParameters) findRoute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) ([]*srvtopo.ResolvedShard, []map[string]*querypb.BindVariable, error) {
	switch rp.Opcode {
	case None:
		return nil, nil, nil
	case DBA:
		return rp.systemQuery(ctx, vcursor, bindVars)
	case Unsharded, Next:
		return rp.unsharded(ctx, vcursor, bindVars)
	case Reference:
		return rp.anyShard(ctx, vcursor, bindVars)
	case Scatter:
		return rp.byDestination(ctx, vcursor, bindVars, key.DestinationAllShards{})
	case ByDestination:
		return rp.byDestination(ctx, vcursor, bindVars, rp.TargetDestination)
	case Equal, EqualUnique, SubShard:
		switch rp.Vindex.(type) {
		case vindexes.MultiColumn:
			return rp.equalMultiCol(ctx, vcursor, bindVars)
		default:
			return rp.equal(ctx, vcursor, bindVars)
		}
	case IN:
		switch rp.Vindex.(type) {
		case vindexes.MultiColumn:
			return rp.inMultiCol(ctx, vcursor, bindVars)
		default:
			return rp.in(ctx, vcursor, bindVars)
		}
	case MultiEqual:
		switch rp.Vindex.(type) {
		case vindexes.MultiColumn:
			return rp.multiEqualMultiCol(ctx, vcursor, bindVars)
		default:
			return rp.multiEqual(ctx, vcursor, bindVars)
		}
	default:
		// Unreachable.
		return nil, nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unsupported opcode: %v", rp.Opcode)
	}
}

func (rp *RoutingParameters) systemQuery(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) ([]*srvtopo.ResolvedShard, []map[string]*querypb.BindVariable, error) {
	destinations, err := rp.routeInfoSchemaQuery(ctx, vcursor, bindVars)
	if err != nil {
		return nil, nil, err
	}

	return destinations, []map[string]*querypb.BindVariable{bindVars}, nil
}

func (rp *RoutingParameters) routeInfoSchemaQuery(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) ([]*srvtopo.ResolvedShard, error) {
	defaultRoute := func() ([]*srvtopo.ResolvedShard, error) {
		ks := rp.Keyspace.Name
		destinations, _, err := vcursor.ResolveDestinations(ctx, ks, nil, []key.Destination{key.DestinationAnyShard{}})
		return destinations, vterrors.Wrapf(err, "failed to find information about keyspace `%s`", ks)
	}

	if len(rp.SysTableTableName) == 0 && len(rp.SysTableTableSchema) == 0 {
		return defaultRoute()
	}

	env := evalengine.NewExpressionEnv(ctx, bindVars, vcursor)
	var specifiedKS string
	for _, tableSchema := range rp.SysTableTableSchema {
		result, err := env.Evaluate(tableSchema)
		if err != nil {
			return nil, err
		}
		ks := result.Value(vcursor.ConnCollation()).ToString()
		if specifiedKS == "" {
			specifiedKS = ks
		}
		if specifiedKS != ks {
			return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "specifying two different database in the query is not supported")
		}
	}
	if specifiedKS != "" {
		bindVars[sqltypes.BvSchemaName] = sqltypes.StringBindVariable(specifiedKS)
	}

	tableNames := map[string]string{}
	for tblBvName, sysTableName := range rp.SysTableTableName {
		val, err := env.Evaluate(sysTableName)
		if err != nil {
			return nil, err
		}
		tabName := val.Value(vcursor.ConnCollation()).ToString()
		tableNames[tblBvName] = tabName
		bindVars[tblBvName] = sqltypes.StringBindVariable(tabName)
	}

	// if the table_schema is system schema, route to default keyspace.
	if sqlparser.SystemSchema(specifiedKS) {
		return defaultRoute()
	}

	// the use has specified a table_name - let's check if it's a routed table
	if len(tableNames) > 0 {
		rss, err := rp.routedTable(ctx, vcursor, bindVars, specifiedKS, tableNames)
		if err != nil {
			// Only if keyspace is not found in vschema, we try with default keyspace.
			// As the in the table_schema predicates for a keyspace 'ks' it can contain 'vt_ks'.
			if vterrors.ErrState(err) == vterrors.BadDb {
				return defaultRoute()
			}
			return nil, err
		}
		if rss != nil {
			return rss, nil
		}
	}

	// it was not a routed table, and we dont have a schema name to look up. give up
	if specifiedKS == "" {
		return defaultRoute()
	}

	// we only have table_schema to work with
	destinations, _, err := vcursor.ResolveDestinations(ctx, specifiedKS, nil, []key.Destination{key.DestinationAnyShard{}})
	if err != nil {
		log.Errorf("failed to route information_schema query to keyspace [%s]", specifiedKS)
		bindVars[sqltypes.BvSchemaName] = sqltypes.StringBindVariable(specifiedKS)
		return defaultRoute()
	}
	setReplaceSchemaName(bindVars)
	return destinations, nil
}

func (rp *RoutingParameters) routedTable(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, tableSchema string, tableNames map[string]string) ([]*srvtopo.ResolvedShard, error) {
	var routedKs *vindexes.Keyspace
	for tblBvName, tableName := range tableNames {
		tbl := sqlparser.TableName{
			Name:      sqlparser.NewIdentifierCS(tableName),
			Qualifier: sqlparser.NewIdentifierCS(tableSchema),
		}
		routedTable, err := vcursor.FindRoutedTable(tbl)
		if err != nil {
			return nil, err
		}

		if routedTable != nil {
			// if we were able to find information about this table, let's use it

			// check if the query is send to single keyspace.
			if routedKs == nil {
				routedKs = routedTable.Keyspace
			}
			if routedKs.Name != routedTable.Keyspace.Name {
				return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "cannot send the query to multiple keyspace due to different table_name: %s, %s", routedKs.Name, routedTable.Keyspace.Name)
			}

			shards, _, err := vcursor.ResolveDestinations(ctx, routedTable.Keyspace.Name, nil, []key.Destination{key.DestinationAnyShard{}})
			bindVars[tblBvName] = sqltypes.StringBindVariable(routedTable.Name.String())
			if tableSchema != "" {
				setReplaceSchemaName(bindVars)
			}
			return shards, err
		}
		// no routed table info found. we'll return nil and check on the outside if we can find the table_schema
		bindVars[tblBvName] = sqltypes.StringBindVariable(tableName)
	}
	return nil, nil
}

func (rp *RoutingParameters) anyShard(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) ([]*srvtopo.ResolvedShard, []map[string]*querypb.BindVariable, error) {
	rss, _, err := vcursor.ResolveDestinations(ctx, rp.Keyspace.Name, nil, []key.Destination{key.DestinationAnyShard{}})
	if err != nil {
		return nil, nil, err
	}
	multiBindVars := make([]map[string]*querypb.BindVariable, len(rss))
	for i := range multiBindVars {
		multiBindVars[i] = bindVars
	}
	return rss, multiBindVars, nil
}

func (rp *RoutingParameters) unsharded(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) ([]*srvtopo.ResolvedShard, []map[string]*querypb.BindVariable, error) {
	rss, _, err := vcursor.ResolveDestinations(ctx, rp.Keyspace.Name, nil, []key.Destination{key.DestinationAllShards{}})
	if err != nil {
		return nil, nil, err
	}
	if len(rss) != 1 {
		return nil, nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "Keyspace '%s' does not have exactly one shard: %v", rp.Keyspace.Name, rss)
	}
	multiBindVars := make([]map[string]*querypb.BindVariable, len(rss))
	for i := range multiBindVars {
		multiBindVars[i] = bindVars
	}
	return rss, multiBindVars, nil
}

func (rp *RoutingParameters) byDestination(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, destination key.Destination) ([]*srvtopo.ResolvedShard, []map[string]*querypb.BindVariable, error) {
	rss, _, err := vcursor.ResolveDestinations(ctx, rp.Keyspace.Name, nil, []key.Destination{destination})
	if err != nil {
		return nil, nil, err
	}
	multiBindVars := make([]map[string]*querypb.BindVariable, len(rss))
	for i := range multiBindVars {
		multiBindVars[i] = bindVars
	}
	return rss, multiBindVars, err
}

func (rp *RoutingParameters) equal(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) ([]*srvtopo.ResolvedShard, []map[string]*querypb.BindVariable, error) {
	env := evalengine.NewExpressionEnv(ctx, bindVars, vcursor)
	value, err := env.Evaluate(rp.Values[0])
	if err != nil {
		return nil, nil, err
	}
	rss, _, err := resolveShards(ctx, vcursor, rp.Vindex.(vindexes.SingleColumn), rp.Keyspace, []sqltypes.Value{value.Value(vcursor.ConnCollation())})
	if err != nil {
		return nil, nil, err
	}
	multiBindVars := make([]map[string]*querypb.BindVariable, len(rss))
	for i := range multiBindVars {
		multiBindVars[i] = bindVars
	}
	return rss, multiBindVars, nil
}

func (rp *RoutingParameters) equalMultiCol(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) ([]*srvtopo.ResolvedShard, []map[string]*querypb.BindVariable, error) {
	env := evalengine.NewExpressionEnv(ctx, bindVars, vcursor)
	var rowValue []sqltypes.Value
	for _, rvalue := range rp.Values {
		v, err := env.Evaluate(rvalue)
		if err != nil {
			return nil, nil, err
		}
		rowValue = append(rowValue, v.Value(vcursor.ConnCollation()))
	}

	rss, _, err := resolveShardsMultiCol(ctx, vcursor, rp.Vindex.(vindexes.MultiColumn), rp.Keyspace, [][]sqltypes.Value{rowValue}, false /* shardIdsNeeded */)
	if err != nil {
		return nil, nil, err
	}
	multiBindVars := make([]map[string]*querypb.BindVariable, len(rss))
	for i := range multiBindVars {
		multiBindVars[i] = bindVars
	}
	return rss, multiBindVars, nil
}

func (rp *RoutingParameters) in(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) ([]*srvtopo.ResolvedShard, []map[string]*querypb.BindVariable, error) {
	env := evalengine.NewExpressionEnv(ctx, bindVars, vcursor)
	value, err := env.Evaluate(rp.Values[0])
	if err != nil {
		return nil, nil, err
	}
	rss, values, err := resolveShards(ctx, vcursor, rp.Vindex.(vindexes.SingleColumn), rp.Keyspace, value.TupleValues())
	if err != nil {
		return nil, nil, err
	}
	return rss, shardVars(bindVars, values), nil
}

func (rp *RoutingParameters) inMultiCol(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) ([]*srvtopo.ResolvedShard, []map[string]*querypb.BindVariable, error) {
	rowColValues, isSingleVal, err := generateRowColValues(ctx, vcursor, bindVars, rp.Values)
	if err != nil {
		return nil, nil, err
	}

	rss, mapVals, err := resolveShardsMultiCol(ctx, vcursor, rp.Vindex.(vindexes.MultiColumn), rp.Keyspace, rowColValues, true /* shardIdsNeeded */)
	if err != nil {
		return nil, nil, err
	}
	return rss, shardVarsMultiCol(bindVars, mapVals, isSingleVal), nil
}

func (rp *RoutingParameters) multiEqual(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) ([]*srvtopo.ResolvedShard, []map[string]*querypb.BindVariable, error) {
	env := evalengine.NewExpressionEnv(ctx, bindVars, vcursor)
	value, err := env.Evaluate(rp.Values[0])
	if err != nil {
		return nil, nil, err
	}
	rss, _, err := resolveShards(ctx, vcursor, rp.Vindex.(vindexes.SingleColumn), rp.Keyspace, value.TupleValues())
	if err != nil {
		return nil, nil, err
	}
	multiBindVars := make([]map[string]*querypb.BindVariable, len(rss))
	for i := range multiBindVars {
		multiBindVars[i] = bindVars
	}
	return rss, multiBindVars, nil
}

func (rp *RoutingParameters) multiEqualMultiCol(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) ([]*srvtopo.ResolvedShard, []map[string]*querypb.BindVariable, error) {
	var multiColValues [][]sqltypes.Value
	env := evalengine.NewExpressionEnv(ctx, bindVars, vcursor)
	for _, rvalue := range rp.Values {
		v, err := env.Evaluate(rvalue)
		if err != nil {
			return nil, nil, err
		}
		multiColValues = append(multiColValues, v.TupleValues())
	}

	// transpose from multi col value to vindex keys with one value from each multi column values.
	// [1,3]
	// [2,4]
	// [5,6]
	// change
	// [1,2,5]
	// [3,4,6]

	rowColValues := make([][]sqltypes.Value, len(multiColValues[0]))
	for _, colValues := range multiColValues {
		for row, colVal := range colValues {
			rowColValues[row] = append(rowColValues[row], colVal)
		}
	}

	rss, _, err := resolveShardsMultiCol(ctx, vcursor, rp.Vindex.(vindexes.MultiColumn), rp.Keyspace, rowColValues, false /* shardIdsNotNeeded */)
	if err != nil {
		return nil, nil, err
	}
	multiBindVars := make([]map[string]*querypb.BindVariable, len(rss))
	for i := range multiBindVars {
		multiBindVars[i] = bindVars
	}
	return rss, multiBindVars, nil
}

func setReplaceSchemaName(bindVars map[string]*querypb.BindVariable) {
	delete(bindVars, sqltypes.BvSchemaName)
	bindVars[sqltypes.BvReplaceSchemaName] = sqltypes.Int64BindVariable(1)
}

func resolveShards(ctx context.Context, vcursor VCursor, vindex vindexes.SingleColumn, keyspace *vindexes.Keyspace, vindexKeys []sqltypes.Value) ([]*srvtopo.ResolvedShard, [][]*querypb.Value, error) {
	// Convert vindexKeys to []*querypb.Value
	ids := make([]*querypb.Value, len(vindexKeys))
	for i, vik := range vindexKeys {
		ids[i] = sqltypes.ValueToProto(vik)
	}

	// Map using the Vindex
	destinations, err := vindex.Map(ctx, vcursor, vindexKeys)
	if err != nil {
		return nil, nil, err

	}

	// And use the Resolver to map to ResolvedShards.
	return vcursor.ResolveDestinations(ctx, keyspace.Name, ids, destinations)
}

func resolveShardsMultiCol(ctx context.Context, vcursor VCursor, vindex vindexes.MultiColumn, keyspace *vindexes.Keyspace, rowColValues [][]sqltypes.Value, shardIdsNeeded bool) ([]*srvtopo.ResolvedShard, [][][]*querypb.Value, error) {
	destinations, err := vindex.Map(ctx, vcursor, rowColValues)
	if err != nil {
		return nil, nil, err
	}

	// And use the Resolver to map to ResolvedShards.
	rss, shardsValues, err := vcursor.ResolveDestinationsMultiCol(ctx, keyspace.Name, rowColValues, destinations)
	if err != nil {
		return nil, nil, err
	}

	if shardIdsNeeded {
		return rss, buildMultiColumnVindexValues(shardsValues), nil
	}
	return rss, nil, nil
}

// buildMultiColumnVindexValues takes in the values resolved for each shard and transposes them
// and eliminates duplicates, returning the values to be used for each column for a multi column
// vindex in each shard.
func buildMultiColumnVindexValues(shardsValues [][][]sqltypes.Value) [][][]*querypb.Value {
	var shardsIds [][][]*querypb.Value
	for _, shardValues := range shardsValues {
		// shardValues -> [[0,1], [0,2], [0,3]]
		// shardIds -> [[0,0,0], [1,2,3]]
		// cols = 2
		cols := len(shardValues[0])
		shardIds := make([][]*querypb.Value, cols)
		colValSeen := make([]map[string]any, cols)
		for _, values := range shardValues {
			for colIdx, value := range values {
				if colValSeen[colIdx] == nil {
					colValSeen[colIdx] = map[string]any{}
				}
				if _, found := colValSeen[colIdx][value.String()]; found {
					continue
				}
				shardIds[colIdx] = append(shardIds[colIdx], sqltypes.ValueToProto(value))
				colValSeen[colIdx][value.String()] = nil
			}
		}
		shardsIds = append(shardsIds, shardIds)
	}
	return shardsIds
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

func shardVarsMultiCol(bv map[string]*querypb.BindVariable, mapVals [][][]*querypb.Value, isSingleVal map[int]any) []map[string]*querypb.BindVariable {
	shardVars := make([]map[string]*querypb.BindVariable, len(mapVals))
	for i, shardVals := range mapVals {
		newbv := make(map[string]*querypb.BindVariable, len(bv)+len(shardVals)-len(isSingleVal))
		for k, v := range bv {
			newbv[k] = v
		}
		for j, vals := range shardVals {
			if _, found := isSingleVal[j]; found {
				// this vindex column is non-tuple column hence listVal bind variable is not required to be set.
				continue
			}
			newbv[ListVarName+strconv.Itoa(j)] = &querypb.BindVariable{
				Type:   querypb.Type_TUPLE,
				Values: vals,
			}
		}
		shardVars[i] = newbv
	}
	return shardVars
}

func generateRowColValues(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, values []evalengine.Expr) ([][]sqltypes.Value, map[int]any, error) {
	// gather values from all the column in the vindex
	var multiColValues [][]sqltypes.Value
	var lv []sqltypes.Value
	isSingleVal := map[int]any{}
	env := evalengine.NewExpressionEnv(ctx, bindVars, vcursor)
	for colIdx, rvalue := range values {
		result, err := env.Evaluate(rvalue)
		if err != nil {
			return nil, nil, err
		}
		lv = result.TupleValues()
		if lv == nil {
			v, err := env.Evaluate(rvalue)
			if err != nil {
				return nil, nil, err
			}
			isSingleVal[colIdx] = nil
			lv = []sqltypes.Value{v.Value(vcursor.ConnCollation())}
		}
		multiColValues = append(multiColValues, lv)
	}

	/*
		need to convert them into vindex keys
		from: cola (1,2) colb (3,4,5)
		to: keys (1,3) (1,4) (1,5) (2,3) (2,4) (2,5)

		so that the vindex can map them into correct destination.
	*/

	var rowColValues [][]sqltypes.Value
	for _, firstCol := range multiColValues[0] {
		rowColValues = append(rowColValues, []sqltypes.Value{firstCol})
	}
	for idx := 1; idx < len(multiColValues); idx++ {
		rowColValues = buildRowColValues(rowColValues, multiColValues[idx])
	}
	return rowColValues, isSingleVal, nil
}

// buildRowColValues will take [1,2][1,3] as left input and [4,5] as right input
// convert it into [1,2,4][1,2,5][1,3,4][1,3,5]
// all combination of left and right side.
func buildRowColValues(left [][]sqltypes.Value, right []sqltypes.Value) [][]sqltypes.Value {
	var allCombinations [][]sqltypes.Value
	for _, firstPart := range left {
		for _, secondPart := range right {
			allCombinations = append(allCombinations, append(firstPart, secondPart))
		}
	}
	return allCombinations
}
