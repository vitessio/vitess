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
	"fmt"
	"sort"
	"strings"
	"time"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var _ Primitive = (*RouteGen4)(nil)

// RouteGen4 represents the instructions to route a read query to
// one or many vttablets.
type RouteGen4 struct {
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
	Value evalengine.Expr

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

// NewSimpleRouteGen4 creates a Route with the bare minimum of parameters.
func NewSimpleRouteGen4(opcode RouteOpcode, keyspace *vindexes.Keyspace) *RouteGen4 {
	return &RouteGen4{
		Opcode:   opcode,
		Keyspace: keyspace,
	}
}

// NewRouteGen4 creates a Route.
func NewRouteGen4(opcode RouteOpcode, keyspace *vindexes.Keyspace, query, fieldQuery string) *RouteGen4 {
	return &RouteGen4{
		Opcode:     opcode,
		Keyspace:   keyspace,
		Query:      query,
		FieldQuery: fieldQuery,
	}
}

// RouteType returns a description of the query routing type used by the primitive
func (route *RouteGen4) RouteType() string {
	return route.Opcode.String()
}

// GetKeyspaceName specifies the Keyspace that this primitive routes to.
func (route *RouteGen4) GetKeyspaceName() string {
	return route.Keyspace.Name
}

// GetTableName specifies the table that this primitive routes to.
func (route *RouteGen4) GetTableName() string {
	return route.TableName
}

// SetTruncateColumnCount sets the truncate column count.
func (route *RouteGen4) SetTruncateColumnCount(count int) {
	route.TruncateColumnCount = count
}

// TryExecute performs a non-streaming exec.
func (route *RouteGen4) TryExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
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

func (route *RouteGen4) executeInternal(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	var rss []*srvtopo.ResolvedShard
	var bvs []map[string]*querypb.BindVariable
	var err error
	switch route.Opcode {
	case SelectDBA:
		rss, bvs, err = route.paramsSystemQuery(vcursor, bindVars)
	case SelectUnsharded, SelectNext, SelectReference:
		rss, bvs, err = route.paramsAnyShard(vcursor, bindVars)
	case SelectScatter:
		rss, bvs, err = route.paramsAllShards(vcursor, bindVars)
	case SelectEqual, SelectEqualUnique:
		rss, bvs, err = route.paramsSelectEqual(vcursor, bindVars)
	case SelectIN:
		rss, bvs, err = route.paramsSelectIn(vcursor, bindVars)
	case SelectMultiEqual:
		rss, bvs, err = route.paramsSelectMultiEqual(vcursor, bindVars)
	case SelectNone:
		rss, bvs, err = nil, nil, nil
	default:
		// Unreachable.
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unsupported query route: %v", route)
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

// TryStreamExecute performs a streaming exec.
func (route *RouteGen4) TryStreamExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	var rss []*srvtopo.ResolvedShard
	var bvs []map[string]*querypb.BindVariable
	var err error
	if route.QueryTimeout != 0 {
		cancel := vcursor.SetContextTimeout(time.Duration(route.QueryTimeout) * time.Millisecond)
		defer cancel()
	}
	switch route.Opcode {
	case SelectDBA:
		rss, bvs, err = route.paramsSystemQuery(vcursor, bindVars)
	case SelectUnsharded, SelectNext, SelectReference:
		rss, bvs, err = route.paramsAnyShard(vcursor, bindVars)
	case SelectScatter:
		rss, bvs, err = route.paramsAllShards(vcursor, bindVars)
	case SelectEqual, SelectEqualUnique:
		rss, bvs, err = route.paramsSelectEqual(vcursor, bindVars)
	case SelectIN:
		rss, bvs, err = route.paramsSelectIn(vcursor, bindVars)
	case SelectMultiEqual:
		rss, bvs, err = route.paramsSelectMultiEqual(vcursor, bindVars)
	case SelectNone:
		rss, bvs, err = nil, nil, nil
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
		errs := vcursor.StreamExecuteMulti(route.Query, rss, bvs, func(qr *sqltypes.Result) error {
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

func (route *RouteGen4) mergeSort(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error, rss []*srvtopo.ResolvedShard, bvs []map[string]*querypb.BindVariable) error {
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
func (route *RouteGen4) GetFields(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
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

func (route *RouteGen4) paramsAllShards(vcursor VCursor, bindVars map[string]*querypb.BindVariable) ([]*srvtopo.ResolvedShard, []map[string]*querypb.BindVariable, error) {
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

func (route *RouteGen4) paramsSystemQuery(vcursor VCursor, bindVars map[string]*querypb.BindVariable) ([]*srvtopo.ResolvedShard, []map[string]*querypb.BindVariable, error) {
	destinations, err := route.routeInfoSchemaQuery(vcursor, bindVars)
	if err != nil {
		return nil, nil, err
	}

	return destinations, []map[string]*querypb.BindVariable{bindVars}, nil
}

func (route *RouteGen4) routeInfoSchemaQuery(vcursor VCursor, bindVars map[string]*querypb.BindVariable) ([]*srvtopo.ResolvedShard, error) {
	defaultRoute := func() ([]*srvtopo.ResolvedShard, error) {
		ks := route.Keyspace.Name
		destinations, _, err := vcursor.ResolveDestinations(ks, nil, []key.Destination{key.DestinationAnyShard{}})
		return destinations, vterrors.Wrapf(err, "failed to find information about keyspace `%s`", ks)
	}

	if len(route.SysTableTableName) == 0 && len(route.SysTableTableSchema) == 0 {
		return defaultRoute()
	}

	env := evalengine.ExpressionEnv{
		BindVars: bindVars,
		Row:      []sqltypes.Value{},
	}

	var specifiedKS string
	for _, tableSchema := range route.SysTableTableSchema {
		result, err := tableSchema.Evaluate(env)
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
		val, err := sysTableName.Evaluate(env)
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

func (route *RouteGen4) paramsRoutedTable(vcursor VCursor, bindVars map[string]*querypb.BindVariable, tableSchema string, tableNames map[string]string) ([]*srvtopo.ResolvedShard, error) {
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

func (route *RouteGen4) paramsAnyShard(vcursor VCursor, bindVars map[string]*querypb.BindVariable) ([]*srvtopo.ResolvedShard, []map[string]*querypb.BindVariable, error) {
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

func (route *RouteGen4) paramsSelectEqual(vcursor VCursor, bindVars map[string]*querypb.BindVariable) ([]*srvtopo.ResolvedShard, []map[string]*querypb.BindVariable, error) {
	result, err := route.Value.Evaluate(evalengine.ExpressionEnv{
		BindVars: bindVars,
		Row:      nil,
	})
	if err != nil {
		return nil, nil, err
	}
	key := evalengine.SQLValue(result)
	rss, _, err := resolveShards(vcursor, route.Vindex, route.Keyspace, []sqltypes.Value{key})
	if err != nil {
		return nil, nil, err
	}
	multiBindVars := make([]map[string]*querypb.BindVariable, len(rss))
	for i := range multiBindVars {
		multiBindVars[i] = bindVars
	}
	return rss, multiBindVars, nil
}

func (route *RouteGen4) paramsSelectIn(vcursor VCursor, bindVars map[string]*querypb.BindVariable) ([]*srvtopo.ResolvedShard, []map[string]*querypb.BindVariable, error) {
	panic("todo")
	// keys, err := route.Values[0].ResolveList(bindVars)
	// if err != nil {
	// 	return nil, nil, err
	// }
	// rss, values, err := resolveShards(vcursor, route.Vindex, route.Keyspace, keys)
	// if err != nil {
	// 	return nil, nil, err
	// }
	// return rss, shardVars(bindVars, values), nil
}

func (route *RouteGen4) paramsSelectMultiEqual(vcursor VCursor, bindVars map[string]*querypb.BindVariable) ([]*srvtopo.ResolvedShard, []map[string]*querypb.BindVariable, error) {
	panic("todo")
	//
	// keys, err := route.Values[0].ResolveList(bindVars)
	// if err != nil {
	// 	return nil, nil, err
	// }
	// rss, _, err := resolveShards(vcursor, route.Vindex, route.Keyspace, keys)
	// if err != nil {
	// 	return nil, nil, err
	// }
	// multiBindVars := make([]map[string]*querypb.BindVariable, len(rss))
	// for i := range multiBindVars {
	// 	multiBindVars[i] = bindVars
	// }
	// return rss, multiBindVars, nil
}

func (route *RouteGen4) sort(in *sqltypes.Result) (*sqltypes.Result, error) {
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

func (route *RouteGen4) description() PrimitiveDescription {
	other := map[string]interface{}{
		"Query":      route.Query,
		"Table":      route.TableName,
		"FieldQuery": route.FieldQuery,
	}
	if route.Vindex != nil {
		other["Vindex"] = route.Vindex.String()
	}
	if route.Value != nil  {
		other["Values"] = route.Value.String()
	}
	if len(route.SysTableTableSchema) != 0 {
		sysTabSchema := "["
		for idx, tableSchema := range route.SysTableTableSchema {
			if idx != 0 {
				sysTabSchema += ", "
			}
			sysTabSchema += tableSchema.String()
		}
		sysTabSchema += "]"
		other["SysTableTableSchema"] = sysTabSchema
	}
	if len(route.SysTableTableName) != 0 {
		var sysTableName []string
		for k, v := range route.SysTableTableName {
			sysTableName = append(sysTableName, k+":"+v.String())
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
