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
	"strconv"
	"strings"
	"time"

	"vitess.io/vitess/go/mysql/collations"
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
	// TargetTabletType specifies an explicit target destination tablet type
	// this is only used in conjunction with TargetDestination
	TargetTabletType topodatapb.TabletType

	// Query specifies the query to be executed.
	Query string

	// TableName specifies the table to send the query to.
	TableName string

	// FieldQuery specifies the query to be executed for a GetFieldInfo request.
	FieldQuery string

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

	// RoutingParameters parameters required for query routing.
	*RoutingParameters

	// NoRoutesSpecialHandling will make the route send a query to arbitrary shard if the routing logic can't find
	// the correct shard. This is important for queries where no matches does not mean empty result - examples would be:
	// select count(*) from tbl where lookupColumn = 'not there'
	// select exists(<subq>)
	NoRoutesSpecialHandling bool

	// Route does not take inputs
	noInputs

	// Route does not need transaction handling
	noTxNeeded
}

// NewSimpleRoute creates a Route with the bare minimum of parameters.
func NewSimpleRoute(opcode Opcode, keyspace *vindexes.Keyspace) *Route {
	return &Route{
		RoutingParameters: &RoutingParameters{
			Opcode:   opcode,
			Keyspace: keyspace,
		},
	}
}

// NewRoute creates a Route.
func NewRoute(opcode Opcode, keyspace *vindexes.Keyspace, query, fieldQuery string) *Route {
	return &Route{
		RoutingParameters: &RoutingParameters{
			Opcode:   opcode,
			Keyspace: keyspace,
		},
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

var (
	partialSuccessScatterQueries = stats.NewCounter("PartialSuccessScatterQueries", "Count of partially successful scatter queries")
)

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

const IgnoreReserveTxn = "IGNORE_EXISTING_CONNECTION"

func (route *Route) executeInternal(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	rss, bvs, err := route.findRoute(vcursor, bindVars)
	if err != nil {
		return nil, err
	}

	// Select Next - sequence query does not need to be executed in a dedicated connection (reserved or transaction)
	if route.Opcode == Next {
		restoreCtx := vcursor.SetContextWithValue(IgnoreReserveTxn, true)
		defer restoreCtx()
	}

	// No route.
	if len(rss) == 0 {
		if !route.NoRoutesSpecialHandling {
			if wantfields {
				return route.GetFields(vcursor, bindVars)
			}
			return &sqltypes.Result{}, nil
		}
		// Here we were earlier returning no rows back.
		// But this was incorrect for queries like select count(*) from user where name='x'
		// If the lookup_vindex for name, returns no shards, we still want a result from here
		// with a single row with 0 as the output.
		// However, at this level it is hard to distinguish between the cases that need a result
		// and the ones that don't. So, we are sending the query to any shard! This is safe because
		// the query contains a predicate that make it not match any rows on that shard. (If they did,
		// we should have gotten that shard back already from findRoute)
		rss, bvs, err = route.anyShard(vcursor, bindVars)
		if err != nil {
			return nil, err
		}
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
		if !route.NoRoutesSpecialHandling {
			if wantfields {
				r, err := route.GetFields(vcursor, bindVars)
				if err != nil {
					return err
				}
				return callback(r)
			}
			return nil
		}
		// Here we were earlier returning no rows back.
		// But this was incorrect for queries like select count(*) from user where name='x'
		// If the lookup_vindex for name, returns no shards, we still want a result from here
		// with a single row with 0 as the output.
		// However, at this level it is hard to distinguish between the cases that need a result
		// and the ones that don't. So, we are sending the query to any shard! This is safe because
		// the query contains a predicate that make it not match any rows on that shard. (If they did,
		// we should have gotten that shard back already from findRoute)
		rss, bvs, err = route.anyShard(vcursor, bindVars)
		if err != nil {
			return err
		}
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

func (route *Route) description() PrimitiveDescription {
	other := map[string]any{
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
		Variant:           route.Opcode.String(),
		Keyspace:          route.Keyspace,
		TargetDestination: route.TargetDestination,
		Other:             other,
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

func orderByToString(in any) string {
	return in.(OrderByParams).String()
}
