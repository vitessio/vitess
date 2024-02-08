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
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"time"

	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/evalengine"

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

var (
	replicaWarmingReadsMirrored = stats.NewCountersWithMultiLabels(
		"ReplicaWarmingReadsMirrored",
		"Number of reads mirrored to replicas to warm their bufferpools",
		[]string{"Keyspace"})
)

// Route represents the instructions to route a read query to
// one or many vttablets.
type Route struct {
	// Route does not take inputs
	noInputs

	// Route does not need transaction handling
	noTxNeeded

	// TargetTabletType specifies an explicit target destination tablet type
	// this is only used in conjunction with TargetDestination
	TargetTabletType topodatapb.TabletType

	// Query specifies the query to be executed.
	Query string

	// TableName specifies the tables to send the query to.
	TableName string

	// FieldQuery specifies the query to be executed for a GetFieldInfo request.
	FieldQuery string

	// OrderBy specifies the key order for merge sorting. This will be
	// set only for scatter queries that need the results to be
	// merge-sorted.
	OrderBy evalengine.Comparison

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
func (route *Route) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	ctx, cancelFunc := addQueryTimeout(ctx, vcursor, route.QueryTimeout)
	defer cancelFunc()
	qr, err := route.executeInternal(ctx, vcursor, bindVars, wantfields)
	if err != nil {
		return nil, err
	}
	return qr.Truncate(route.TruncateColumnCount), nil
}

// addQueryTimeout adds a query timeout to the context it receives and returns the modified context along with the cancel function.
func addQueryTimeout(ctx context.Context, vcursor VCursor, queryTimeout int) (context.Context, context.CancelFunc) {
	timeout := vcursor.Session().GetQueryTimeout(queryTimeout)
	if timeout != 0 {
		return context.WithTimeout(ctx, time.Duration(timeout)*time.Millisecond)
	}
	return ctx, func() {}
}

type cxtKey int

const (
	IgnoreReserveTxn cxtKey = iota
)

func (route *Route) executeInternal(
	ctx context.Context,
	vcursor VCursor,
	bindVars map[string]*querypb.BindVariable,
	wantfields bool,
) (*sqltypes.Result, error) {
	rss, bvs, err := route.findRoute(ctx, vcursor, bindVars)
	if err != nil {
		return nil, err
	}

	return route.executeShards(ctx, vcursor, bindVars, wantfields, rss, bvs)
}

func (route *Route) executeShards(
	ctx context.Context,
	vcursor VCursor,
	bindVars map[string]*querypb.BindVariable,
	wantfields bool,
	rss []*srvtopo.ResolvedShard,
	bvs []map[string]*querypb.BindVariable,
) (*sqltypes.Result, error) {
	// Select Next - sequence query does not need to be executed in a dedicated connection (reserved or transaction)
	if route.Opcode == Next {
		ctx = context.WithValue(ctx, IgnoreReserveTxn, true)
	}

	// No route.
	if len(rss) == 0 {
		if !route.NoRoutesSpecialHandling {
			if wantfields {
				return route.GetFields(ctx, vcursor, bindVars)
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
		var err error
		rss, bvs, err = route.anyShard(ctx, vcursor, bindVars)
		if err != nil {
			return nil, err
		}
	}

	queries := getQueries(route.Query, bvs)
	result, errs := vcursor.ExecuteMultiShard(ctx, route, rss, queries, false /* rollbackOnError */, false /* canAutocommit */)

	route.executeWarmingReplicaRead(ctx, vcursor, bindVars, queries)

	if errs != nil {
		errs = filterOutNilErrors(errs)
		if !route.ScatterErrorsAsWarnings || len(errs) == len(rss) {
			return nil, vterrors.Aggregate(errs)
		}

		partialSuccessScatterQueries.Add(1)

		for _, err := range errs {
			serr := sqlerror.NewSQLErrorFromError(err).(*sqlerror.SQLError)
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
func (route *Route) TryStreamExecute(
	ctx context.Context,
	vcursor VCursor,
	bindVars map[string]*querypb.BindVariable,
	wantfields bool,
	callback func(*sqltypes.Result) error,
) error {
	if route.QueryTimeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(route.QueryTimeout)*time.Millisecond)
		defer cancel()
	}
	rss, bvs, err := route.findRoute(ctx, vcursor, bindVars)
	if err != nil {
		return err
	}

	return route.streamExecuteShards(ctx, vcursor, bindVars, wantfields, callback, rss, bvs)
}

func (route *Route) streamExecuteShards(
	ctx context.Context,
	vcursor VCursor,
	bindVars map[string]*querypb.BindVariable,
	wantfields bool,
	callback func(*sqltypes.Result) error,
	rss []*srvtopo.ResolvedShard,
	bvs []map[string]*querypb.BindVariable,
) error {
	// No route.
	if len(rss) == 0 {
		if !route.NoRoutesSpecialHandling {
			if wantfields {
				r, err := route.GetFields(ctx, vcursor, bindVars)
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
		var err error
		rss, bvs, err = route.anyShard(ctx, vcursor, bindVars)
		if err != nil {
			return err
		}
	}

	if len(route.OrderBy) == 0 {
		errs := vcursor.StreamExecuteMulti(ctx, route, route.Query, rss, bvs, false /* rollbackOnError */, false /* autocommit */, func(qr *sqltypes.Result) error {
			return callback(qr.Truncate(route.TruncateColumnCount))
		})
		if len(errs) > 0 {
			if !route.ScatterErrorsAsWarnings || len(errs) == len(rss) {
				return vterrors.Aggregate(errs)
			}
			partialSuccessScatterQueries.Add(1)
			for _, err := range errs {
				sErr := sqlerror.NewSQLErrorFromError(err).(*sqlerror.SQLError)
				vcursor.Session().RecordWarning(&querypb.QueryWarning{Code: uint32(sErr.Num), Message: err.Error()})
			}
		}
		return nil
	}

	// There is an order by. We have to merge-sort.
	return route.mergeSort(ctx, vcursor, bindVars, wantfields, callback, rss, bvs)
}

func (route *Route) mergeSort(
	ctx context.Context,
	vcursor VCursor,
	bindVars map[string]*querypb.BindVariable,
	wantfields bool,
	callback func(*sqltypes.Result) error,
	rss []*srvtopo.ResolvedShard,
	bvs []map[string]*querypb.BindVariable,
) error {
	prims := make([]StreamExecutor, 0, len(rss))
	for i, rs := range rss {
		prims = append(prims, &shardRoute{
			query:     route.Query,
			rs:        rs,
			bv:        bvs[i],
			primitive: route,
		})
	}
	ms := MergeSort{
		Primitives:              prims,
		OrderBy:                 route.OrderBy,
		ScatterErrorsAsWarnings: route.ScatterErrorsAsWarnings,
	}
	return vcursor.StreamExecutePrimitive(ctx, &ms, bindVars, wantfields, func(qr *sqltypes.Result) error {
		return callback(qr.Truncate(route.TruncateColumnCount))
	})
}

// GetFields fetches the field info.
func (route *Route) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	var rs *srvtopo.ResolvedShard

	// Use an existing shard session
	sss := vcursor.Session().ShardSession()
	for _, ss := range sss {
		if ss.Target.Keyspace == route.Keyspace.Name {
			rs = ss
			break
		}
	}

	// If not find, then pick any shard.
	if rs == nil {
		rss, _, err := vcursor.ResolveDestinations(ctx, route.Keyspace.Name, nil, []key.Destination{key.DestinationAnyShard{}})
		if err != nil {
			return nil, err
		}
		if len(rss) != 1 {
			// This code is unreachable. It's just a sanity check.
			return nil, fmt.Errorf("no shards for keyspace: %s", route.Keyspace.Name)
		}
		rs = rss[0]
	}
	qr, err := execShard(ctx, route, vcursor, route.FieldQuery, bindVars, rs, false /* rollbackOnError */, false /* canAutocommit */)
	if err != nil {
		return nil, err
	}
	return qr.Truncate(route.TruncateColumnCount), nil
}

func (route *Route) sort(in *sqltypes.Result) (*sqltypes.Result, error) {
	// Since Result is immutable, we make a copy.
	// The copy can be shallow because we won't be changing
	// the contents of any row.
	out := in.ShallowCopy()

	if err := route.OrderBy.SortResult(out); err != nil {
		return nil, err
	}
	return out.Truncate(route.TruncateColumnCount), nil
}

func (route *Route) description() PrimitiveDescription {
	other := map[string]any{
		"Query":      route.Query,
		"Table":      route.GetTableName(),
		"FieldQuery": route.FieldQuery,
	}
	if route.Vindex != nil {
		other["Vindex"] = route.Vindex.String()
	}
	if route.Values != nil {
		formattedValues := make([]string, 0, len(route.Values))
		for _, value := range route.Values {
			formattedValues = append(formattedValues, sqlparser.String(value))
		}
		other["Values"] = formattedValues
	}
	if len(route.SysTableTableSchema) != 0 {
		sysTabSchema := "["
		for idx, tableSchema := range route.SysTableTableSchema {
			if idx != 0 {
				sysTabSchema += ", "
			}
			sysTabSchema += sqlparser.String(tableSchema)
		}
		sysTabSchema += "]"
		other["SysTableTableSchema"] = sysTabSchema
	}
	if len(route.SysTableTableName) != 0 {
		var sysTableName []string
		for k, v := range route.SysTableTableName {
			sysTableName = append(sysTableName, k+":"+sqlparser.String(v))
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

func (route *Route) executeAfterLookup(
	ctx context.Context,
	vcursor VCursor,
	bindVars map[string]*querypb.BindVariable,
	wantfields bool,
	ids []sqltypes.Value,
	dest []key.Destination,
) (*sqltypes.Result, error) {
	protoIds := make([]*querypb.Value, 0, len(ids))
	for _, id := range ids {
		protoIds = append(protoIds, sqltypes.ValueToProto(id))
	}
	rss, _, err := vcursor.ResolveDestinations(ctx, route.Keyspace.Name, protoIds, dest)
	if err != nil {
		return nil, err
	}
	bvs := make([]map[string]*querypb.BindVariable, len(rss))
	for i := range bvs {
		bvs[i] = bindVars
	}
	return route.executeShards(ctx, vcursor, bindVars, wantfields, rss, bvs)
}

func (route *Route) streamExecuteAfterLookup(
	ctx context.Context,
	vcursor VCursor,
	bindVars map[string]*querypb.BindVariable,
	wantfields bool,
	callback func(*sqltypes.Result) error,
	ids []sqltypes.Value,
	dest []key.Destination,
) error {
	protoIds := make([]*querypb.Value, 0, len(ids))
	for _, id := range ids {
		protoIds = append(protoIds, sqltypes.ValueToProto(id))
	}
	rss, _, err := vcursor.ResolveDestinations(ctx, route.Keyspace.Name, protoIds, dest)
	if err != nil {
		return err
	}
	bvs := make([]map[string]*querypb.BindVariable, len(rss))
	for i := range bvs {
		bvs[i] = bindVars
	}
	return route.streamExecuteShards(ctx, vcursor, bindVars, wantfields, callback, rss, bvs)
}

func execShard(
	ctx context.Context,
	primitive Primitive,
	vcursor VCursor,
	query string,
	bindVars map[string]*querypb.BindVariable,
	rs *srvtopo.ResolvedShard,
	rollbackOnError, canAutocommit bool,
) (*sqltypes.Result, error) {
	autocommit := canAutocommit && vcursor.AutocommitApproval()
	result, errs := vcursor.ExecuteMultiShard(ctx, primitive, []*srvtopo.ResolvedShard{rs}, []*querypb.BoundQuery{
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
	obp := in.(evalengine.OrderByParams)
	return obp.String()
}

func (route *Route) executeWarmingReplicaRead(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, queries []*querypb.BoundQuery) {
	switch route.Opcode {
	case Unsharded, Scatter, Equal, EqualUnique, IN, MultiEqual:
		// no-op
	default:
		return
	}

	if vcursor.GetWarmingReadsPercent() == 0 || rand.Intn(100) > vcursor.GetWarmingReadsPercent() {
		return
	}

	replicaVCursor := vcursor.CloneForReplicaWarming(ctx)
	warmingReadsChannel := vcursor.GetWarmingReadsChannel()

	select {
	// if there's no more room in the channel, drop the warming read
	case warmingReadsChannel <- true:
		go func(replicaVCursor VCursor) {
			defer func() {
				<-warmingReadsChannel
			}()
			rss, _, err := route.findRoute(ctx, replicaVCursor, bindVars)
			if err != nil {
				return
			}

			_, errs := replicaVCursor.ExecuteMultiShard(ctx, route, rss, queries, false /* rollbackOnError */, false /* autocommit */)
			if len(errs) > 0 {
				log.Warningf("Failed to execute warming replica read: %v", errs)
			} else {
				replicaWarmingReadsMirrored.Add([]string{route.Keyspace.Name}, 1)
			}
		}(replicaVCursor)
	default:
		log.Warning("Failed to execute warming replica read as pool is full")
	}
}
