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
	"strings"
	"time"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"

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
	Vindex vindexes.Vindex

	// Values specifies the vindex values to use for routing.
	Values []evalengine.Expr

	// OrderBy specifies the key order for merge sorting. This will be
	// set only for scatter queries that need the results to be
	// merge-sorted.
	OrderBy []OrderByParams

	// TruncateColumnCount specifies the number of columns to return
	// in the final result. Rest of the columns are truncated
	// from the result received. If 0, no truncation happens.
	TruncateColumnCount int

	// QueryTimeout contains the optional timeout (in milliseconds) to apply to this query
	QueryTimeout int

	// ScatterErrorsAsWarnings is true if results should be returned even if some shards have an error
	ScatterErrorsAsWarnings bool

	// The following two fields are used when routing information_schema queries
	SysTableTableSchema []evalengine.Expr
	SysTableTableName   map[string]evalengine.Expr

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

// OrderByParams specifies the parameters for ordering.
// This is used for merge-sorting scatter queries.
type OrderByParams struct {
	Col int
	// WeightStringCol is the weight_string column that will be used for sorting.
	// It is set to -1 if such a column is not added to the query
	WeightStringCol   int
	Desc              bool
	StarColFixedIndex int
	// v3 specific boolean. Used to also add weight strings originating from GroupBys to the Group by clause
	FromGroupBy bool
	// Collation ID for comparison using collation
	CollationID collations.ID
}

// String returns a string. Used for plan descriptions
func (obp OrderByParams) String() string {
	val := strconv.Itoa(obp.Col)
	if obp.StarColFixedIndex > obp.Col {
		val = strconv.Itoa(obp.StarColFixedIndex)
	}
	if obp.WeightStringCol != -1 && obp.WeightStringCol != obp.Col {
		val = fmt.Sprintf("(%s|%d)", val, obp.WeightStringCol)
	}
	if obp.Desc {
		val += " DESC"
	} else {
		val += " ASC"
	}
	if obp.CollationID != collations.Unknown {
		collation := collations.Local().LookupByID(obp.CollationID)
		val += " COLLATE " + collation.Name()
	}
	return val
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
	// SelectMultiEqual is the opcode for routing a query
	// based on multiple vindex input values, similar to
	// SelectIN, but the query sent to each shard is the
	// same.
	SelectMultiEqual
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
	// NumRouteOpcodes is the number of opcodes
	NumRouteOpcodes
)

var routeName = map[RouteOpcode]string{
	SelectUnsharded:   "SelectUnsharded",
	SelectEqualUnique: "SelectEqualUnique",
	SelectEqual:       "SelectEqual",
	SelectIN:          "SelectIN",
	SelectMultiEqual:  "SelectMultiEqual",
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

// String returns a string presentation of this opcode
func (code RouteOpcode) String() string {
	return routeName[code]
}

// RouteType returns a description of the query routing type used by the primitive
func (route *Route) RouteType() string {
	return route.Opcode.String()
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

// TryExecute performs a non-streaming exec.
func (route *Route) TryExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	if route.QueryTimeout != 0 {
		cancel := vcursor.SetContextTimeout(time.Duration(route.QueryTimeout) * time.Millisecond)
		defer cancel()
	}
	qr, err := route.executeInternal(vcursor, bindVars, wantfields)
	if err != nil {
		return nil, err
	}
	return qr.Truncate(route.TruncateColumnCount), nil
}

func (route *Route) executeInternal(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	rss, bvs, err := route.findRoute(vcursor, bindVars)
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
		errs = filterOutNilErrors(errs)
		if !route.ScatterErrorsAsWarnings || len(errs) == len(rss) {
			return nil, vterrors.Aggregate(errs)
		}

		partialSuccessScatterQueries.Add(1)

		for _, err := range errs {
			serr := mysql.NewSQLErrorFromError(err).(*mysql.SQLError)
			vcursor.Session().RecordWarning(&querypb.QueryWarning{Code: uint32(serr.Num), Message: err.Error()})
		}
	}

	if len(route.OrderBy) == 0 {
		return result, nil
	}

	return route.sort(result)
}

func (route *Route) findRoute(vcursor VCursor, bindVars map[string]*querypb.BindVariable) ([]*srvtopo.ResolvedShard, []map[string]*querypb.BindVariable, error) {
	switch route.Opcode {
	case SelectDBA:
		return route.paramsSystemQuery(vcursor, bindVars)
	case SelectUnsharded, SelectNext, SelectReference:
		return route.paramsAnyShard(vcursor, bindVars)
	case SelectScatter:
		return route.paramsAllShards(vcursor, bindVars)
	case SelectEqual, SelectEqualUnique:
		switch route.Vindex.(type) {
		case vindexes.MultiColumn:
			return route.paramsSelectEqualMultiCol(vcursor, bindVars)
		default:
			return route.paramsSelectEqual(vcursor, bindVars)
		}
	case SelectIN:
		switch route.Vindex.(type) {
		case vindexes.MultiColumn:
			return route.paramsSelectInMultiCol(vcursor, bindVars)
		default:
			return route.paramsSelectIn(vcursor, bindVars)
		}
	case SelectMultiEqual:
		switch route.Vindex.(type) {
		case vindexes.MultiColumn:
			return route.paramsSelectMultiEqualMultiCol(vcursor, bindVars)
		default:
			return route.paramsSelectMultiEqual(vcursor, bindVars)
		}
	case SelectNone:
		return nil, nil, nil
	default:
		// Unreachable.
		return nil, nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unsupported query route: %v", route)
	}
}

func filterOutNilErrors(errs []error) []error {
	var errors []error
	for _, err := range errs {
		if err != nil {
			errors = append(errors, err)
		}
	}
	return errors
}

// TryStreamExecute performs a streaming exec.
func (route *Route) TryStreamExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	if route.QueryTimeout != 0 {
		cancel := vcursor.SetContextTimeout(time.Duration(route.QueryTimeout) * time.Millisecond)
		defer cancel()
	}
	rss, bvs, err := route.findRoute(vcursor, bindVars)
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
		errs := vcursor.StreamExecuteMulti(route.Query, rss, bvs, false /* rollbackOnError */, false /* autocommit */, func(qr *sqltypes.Result) error {
			return callback(qr.Truncate(route.TruncateColumnCount))
		})
		if len(errs) > 0 {
			if !route.ScatterErrorsAsWarnings || len(errs) == len(rss) {
				return vterrors.Aggregate(errs)
			}
			partialSuccessScatterQueries.Add(1)
			for _, err := range errs {
				sErr := mysql.NewSQLErrorFromError(err).(*mysql.SQLError)
				vcursor.Session().RecordWarning(&querypb.QueryWarning{Code: uint32(sErr.Num), Message: err.Error()})
			}
		}
		return nil
	}

	// There is an order by. We have to merge-sort.
	return route.mergeSort(vcursor, bindVars, wantfields, callback, rss, bvs)
}

func (route *Route) mergeSort(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error, rss []*srvtopo.ResolvedShard, bvs []map[string]*querypb.BindVariable) error {
	prims := make([]StreamExecutor, 0, len(rss))
	for i, rs := range rss {
		prims = append(prims, &shardRoute{
			query: route.Query,
			rs:    rs,
			bv:    bvs[i],
		})
	}
	ms := MergeSort{
		Primitives:              prims,
		OrderBy:                 route.OrderBy,
		ScatterErrorsAsWarnings: route.ScatterErrorsAsWarnings,
	}
	return vcursor.StreamExecutePrimitive(&ms, bindVars, wantfields, func(qr *sqltypes.Result) error {
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
		return nil, nil, err
	}
	multiBindVars := make([]map[string]*querypb.BindVariable, len(rss))
	for i := range multiBindVars {
		multiBindVars[i] = bindVars
	}
	return rss, multiBindVars, nil
}

func (route *Route) paramsSystemQuery(vcursor VCursor, bindVars map[string]*querypb.BindVariable) ([]*srvtopo.ResolvedShard, []map[string]*querypb.BindVariable, error) {
	destinations, err := route.routeInfoSchemaQuery(vcursor, bindVars)
	if err != nil {
		return nil, nil, err
	}

	return destinations, []map[string]*querypb.BindVariable{bindVars}, nil
}

func (route *Route) routeInfoSchemaQuery(vcursor VCursor, bindVars map[string]*querypb.BindVariable) ([]*srvtopo.ResolvedShard, error) {
	defaultRoute := func() ([]*srvtopo.ResolvedShard, error) {
		ks := route.Keyspace.Name
		destinations, _, err := vcursor.ResolveDestinations(ks, nil, []key.Destination{key.DestinationAnyShard{}})
		return destinations, vterrors.Wrapf(err, "failed to find information about keyspace `%s`", ks)
	}

	if len(route.SysTableTableName) == 0 && len(route.SysTableTableSchema) == 0 {
		return defaultRoute()
	}

	env := evalengine.EnvWithBindVars(bindVars)
	var specifiedKS string
	for _, tableSchema := range route.SysTableTableSchema {
		result, err := env.Evaluate(tableSchema)
		if err != nil {
			return nil, err
		}
		ks := result.Value().ToString()
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
	for tblBvName, sysTableName := range route.SysTableTableName {
		val, err := env.Evaluate(sysTableName)
		if err != nil {
			return nil, err
		}
		tabName := val.Value().ToString()
		tableNames[tblBvName] = tabName
		bindVars[tblBvName] = sqltypes.StringBindVariable(tabName)
	}

	// if the table_schema is system schema, route to default keyspace.
	if sqlparser.SystemSchema(specifiedKS) {
		return defaultRoute()
	}

	// the use has specified a table_name - let's check if it's a routed table
	if len(tableNames) > 0 {
		rss, err := route.paramsRoutedTable(vcursor, bindVars, specifiedKS, tableNames)
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
	destinations, _, err := vcursor.ResolveDestinations(specifiedKS, nil, []key.Destination{key.DestinationAnyShard{}})
	if err != nil {
		log.Errorf("failed to route information_schema query to keyspace [%s]", specifiedKS)
		bindVars[sqltypes.BvSchemaName] = sqltypes.StringBindVariable(specifiedKS)
		return defaultRoute()
	}
	setReplaceSchemaName(bindVars)
	return destinations, nil
}

func (route *Route) paramsRoutedTable(vcursor VCursor, bindVars map[string]*querypb.BindVariable, tableSchema string, tableNames map[string]string) ([]*srvtopo.ResolvedShard, error) {
	var routedKs *vindexes.Keyspace
	for tblBvName, tableName := range tableNames {
		tbl := sqlparser.TableName{
			Name:      sqlparser.NewTableIdent(tableName),
			Qualifier: sqlparser.NewTableIdent(tableSchema),
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

			shards, _, err := vcursor.ResolveDestinations(routedTable.Keyspace.Name, nil, []key.Destination{key.DestinationAnyShard{}})
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

func setReplaceSchemaName(bindVars map[string]*querypb.BindVariable) {
	delete(bindVars, sqltypes.BvSchemaName)
	bindVars[sqltypes.BvReplaceSchemaName] = sqltypes.Int64BindVariable(1)
}

func (route *Route) paramsAnyShard(vcursor VCursor, bindVars map[string]*querypb.BindVariable) ([]*srvtopo.ResolvedShard, []map[string]*querypb.BindVariable, error) {
	rss, _, err := vcursor.ResolveDestinations(route.Keyspace.Name, nil, []key.Destination{key.DestinationAnyShard{}})
	if err != nil {
		return nil, nil, err
	}
	multiBindVars := make([]map[string]*querypb.BindVariable, len(rss))
	for i := range multiBindVars {
		multiBindVars[i] = bindVars
	}
	return rss, multiBindVars, nil
}

func (route *Route) paramsSelectEqual(vcursor VCursor, bindVars map[string]*querypb.BindVariable) ([]*srvtopo.ResolvedShard, []map[string]*querypb.BindVariable, error) {
	env := evalengine.EnvWithBindVars(bindVars)
	value, err := env.Evaluate(route.Values[0])
	if err != nil {
		return nil, nil, err
	}
	rss, _, err := resolveShards(vcursor, route.Vindex.(vindexes.SingleColumn), route.Keyspace, []sqltypes.Value{value.Value()})
	if err != nil {
		return nil, nil, err
	}
	multiBindVars := make([]map[string]*querypb.BindVariable, len(rss))
	for i := range multiBindVars {
		multiBindVars[i] = bindVars
	}
	return rss, multiBindVars, nil
}

func (route *Route) paramsSelectEqualMultiCol(vcursor VCursor, bindVars map[string]*querypb.BindVariable) ([]*srvtopo.ResolvedShard, []map[string]*querypb.BindVariable, error) {
	env := evalengine.EnvWithBindVars(bindVars)
	var rowValue []sqltypes.Value
	for _, rvalue := range route.Values {
		v, err := env.Evaluate(rvalue)
		if err != nil {
			return nil, nil, err
		}
		rowValue = append(rowValue, v.Value())
	}

	rss, _, err := resolveShardsMultiCol(vcursor, route.Vindex.(vindexes.MultiColumn), route.Keyspace, [][]sqltypes.Value{rowValue}, false /* shardIdsNeeded */)
	if err != nil {
		return nil, nil, err
	}
	multiBindVars := make([]map[string]*querypb.BindVariable, len(rss))
	for i := range multiBindVars {
		multiBindVars[i] = bindVars
	}
	return rss, multiBindVars, nil
}

func resolveShardsMultiCol(vcursor VCursor, vindex vindexes.MultiColumn, keyspace *vindexes.Keyspace, rowColValues [][]sqltypes.Value, shardIdsNeeded bool) ([]*srvtopo.ResolvedShard, [][][]*querypb.Value, error) {
	destinations, err := vindex.Map(vcursor, rowColValues)
	if err != nil {
		return nil, nil, err
	}

	// And use the Resolver to map to ResolvedShards.
	rss, shardsValues, err := vcursor.ResolveDestinationsMultiCol(keyspace.Name, rowColValues, destinations)
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
		colValSeen := make([]map[string]interface{}, cols)
		for _, values := range shardValues {
			for colIdx, value := range values {
				if colValSeen[colIdx] == nil {
					colValSeen[colIdx] = map[string]interface{}{}
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

func shardVarsMultiCol(bv map[string]*querypb.BindVariable, mapVals [][][]*querypb.Value, isSingleVal map[int]interface{}) []map[string]*querypb.BindVariable {
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

func (route *Route) paramsSelectIn(vcursor VCursor, bindVars map[string]*querypb.BindVariable) ([]*srvtopo.ResolvedShard, []map[string]*querypb.BindVariable, error) {
	env := evalengine.EnvWithBindVars(bindVars)
	value, err := env.Evaluate(route.Values[0])
	if err != nil {
		return nil, nil, err
	}
	rss, values, err := resolveShards(vcursor, route.Vindex.(vindexes.SingleColumn), route.Keyspace, value.TupleValues())
	if err != nil {
		return nil, nil, err
	}
	return rss, shardVars(bindVars, values), nil
}

func (route *Route) paramsSelectInMultiCol(vcursor VCursor, bindVars map[string]*querypb.BindVariable) ([]*srvtopo.ResolvedShard, []map[string]*querypb.BindVariable, error) {
	// gather values from all the column in the vindex
	var multiColValues [][]sqltypes.Value
	var err error
	var lv []sqltypes.Value
	isSingleVal := map[int]interface{}{}
	env := evalengine.EnvWithBindVars(bindVars)
	for colIdx, rvalue := range route.Values {
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
			lv = []sqltypes.Value{v.Value()}
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

	rss, mapVals, err := resolveShardsMultiCol(vcursor, route.Vindex.(vindexes.MultiColumn), route.Keyspace, rowColValues, true /* shardIdsNeeded */)
	if err != nil {
		return nil, nil, err
	}
	return rss, shardVarsMultiCol(bindVars, mapVals, isSingleVal), nil
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

func (route *Route) paramsSelectMultiEqual(vcursor VCursor, bindVars map[string]*querypb.BindVariable) ([]*srvtopo.ResolvedShard, []map[string]*querypb.BindVariable, error) {
	env := evalengine.EnvWithBindVars(bindVars)
	value, err := env.Evaluate(route.Values[0])
	if err != nil {
		return nil, nil, err
	}
	rss, _, err := resolveShards(vcursor, route.Vindex.(vindexes.SingleColumn), route.Keyspace, value.TupleValues())
	if err != nil {
		return nil, nil, err
	}
	multiBindVars := make([]map[string]*querypb.BindVariable, len(rss))
	for i := range multiBindVars {
		multiBindVars[i] = bindVars
	}
	return rss, multiBindVars, nil
}

func (route *Route) paramsSelectMultiEqualMultiCol(vcursor VCursor, bindVars map[string]*querypb.BindVariable) ([]*srvtopo.ResolvedShard, []map[string]*querypb.BindVariable, error) {
	var multiColValues [][]sqltypes.Value
	env := evalengine.EnvWithBindVars(bindVars)
	for _, rvalue := range route.Values {
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

	rss, _, err := resolveShardsMultiCol(vcursor, route.Vindex.(vindexes.MultiColumn), route.Keyspace, rowColValues, false /* shardIdsNotNeeded */)
	if err != nil {
		return nil, nil, err
	}
	multiBindVars := make([]map[string]*querypb.BindVariable, len(rss))
	for i := range multiBindVars {
		multiBindVars[i] = bindVars
	}
	return rss, multiBindVars, nil
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

	comparers := extractSlices(route.OrderBy)

	sort.Slice(out.Rows, func(i, j int) bool {
		var cmp int
		if err != nil {
			return true
		}
		// If there are any errors below, the function sets
		// the external err and returns true. Once err is set,
		// all subsequent calls return true. This will make
		// Slice think that all elements are in the correct
		// order and return more quickly.
		for _, c := range comparers {
			cmp, err = c.compare(out.Rows[i], out.Rows[j])
			if err != nil {
				return true
			}
			if cmp == 0 {
				continue
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

func allowOnlyPrimary(rss ...*srvtopo.ResolvedShard) error {
	for _, rs := range rss {
		if rs != nil && rs.Target.TabletType != topodatapb.TabletType_PRIMARY {
			return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "supported only for primary tablet type, current type: %v", topoproto.TabletTypeLString(rs.Target.TabletType))
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
	if route.Values != nil {
		formattedValues := make([]string, 0, len(route.Values))
		for _, value := range route.Values {
			formattedValues = append(formattedValues, evalengine.FormatExpr(value))
		}
		other["Values"] = formattedValues
	}
	if len(route.SysTableTableSchema) != 0 {
		sysTabSchema := "["
		for idx, tableSchema := range route.SysTableTableSchema {
			if idx != 0 {
				sysTabSchema += ", "
			}
			sysTabSchema += evalengine.FormatExpr(tableSchema)
		}
		sysTabSchema += "]"
		other["SysTableTableSchema"] = sysTabSchema
	}
	if len(route.SysTableTableName) != 0 {
		var sysTableName []string
		for k, v := range route.SysTableTableName {
			sysTableName = append(sysTableName, k+":"+evalengine.FormatExpr(v))
		}
		sort.Strings(sysTableName)
		other["SysTableTableName"] = "[" + strings.Join(sysTableName, ", ") + "]"
	}
	orderBy := GenericJoin(route.OrderBy, orderByToString)
	if orderBy != "" {
		other["OrderBy"] = orderBy
	}
	if route.TruncateColumnCount > 0 {
		other["ResultColumns"] = route.TruncateColumnCount
	}
	if route.ScatterErrorsAsWarnings {
		other["ScatterErrorsAsWarnings"] = true
	}
	if route.QueryTimeout > 0 {
		other["QueryTimeout"] = route.QueryTimeout
	}
	return PrimitiveDescription{
		OperatorType:      "Route",
		Variant:           routeName[route.Opcode],
		Keyspace:          route.Keyspace,
		TargetDestination: route.TargetDestination,
		Other:             other,
	}
}

func orderByToString(in interface{}) string {
	return in.(OrderByParams).String()
}
