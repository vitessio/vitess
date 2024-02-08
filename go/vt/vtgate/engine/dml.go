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
	"sort"
	"strings"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// DML contains the common elements between Update and Delete plans
type DML struct {
	txNeeded

	// Query specifies the query to be executed.
	Query string

	// KsidVindex is primary Vindex
	KsidVindex vindexes.Vindex

	// KsidLength is number of columns that represents KsidVindex
	KsidLength int

	// TableNames are the name of the tables involved in the query.
	TableNames []string

	// Vindexes are the column vindexes modified by this DML.
	Vindexes []*vindexes.ColumnVindex

	// OwnedVindexQuery is used for updating changes in lookup vindexes.
	OwnedVindexQuery string

	// Option to override the standard behavior and allow a multi-shard update
	// to use single round trip autocommit.
	MultiShardAutocommit bool

	// QueryTimeout contains the optional timeout (in milliseconds) to apply to this query
	QueryTimeout int

	PreventAutoCommit bool

	// RoutingParameters parameters required for query routing.
	*RoutingParameters
}

// NewDML returns and empty initialized DML struct.
func NewDML() *DML {
	return &DML{RoutingParameters: &RoutingParameters{}}
}

func (dml *DML) execUnsharded(ctx context.Context, primitive Primitive, vcursor VCursor, bindVars map[string]*querypb.BindVariable, rss []*srvtopo.ResolvedShard) (*sqltypes.Result, error) {
	return execShard(ctx, primitive, vcursor, dml.Query, bindVars, rss[0], true /* rollbackOnError */, !dml.PreventAutoCommit /* canAutocommit */)
}

func (dml *DML) execMultiDestination(ctx context.Context, primitive Primitive, vcursor VCursor, bindVars map[string]*querypb.BindVariable, rss []*srvtopo.ResolvedShard, dmlSpecialFunc func(context.Context, VCursor, map[string]*querypb.BindVariable, []*srvtopo.ResolvedShard) error) (*sqltypes.Result, error) {
	if len(rss) == 0 {
		return &sqltypes.Result{}, nil
	}
	err := dmlSpecialFunc(ctx, vcursor, bindVars, rss)
	if err != nil {
		return nil, err
	}
	queries := make([]*querypb.BoundQuery, len(rss))
	for i := range rss {
		queries[i] = &querypb.BoundQuery{
			Sql:           dml.Query,
			BindVariables: bindVars,
		}
	}
	return execMultiShard(ctx, primitive, vcursor, rss, queries, dml.MultiShardAutocommit)
}

// RouteType returns a description of the query routing type used by the primitive
func (dml *DML) RouteType() string {
	return dml.Opcode.String()
}

// GetKeyspaceName specifies the Keyspace that this primitive routes to.
func (dml *DML) GetKeyspaceName() string {
	return dml.Keyspace.Name
}

// GetTableName specifies the table that this primitive routes to.
func (dml *DML) GetTableName() string {
	sort.Strings(dml.TableNames)
	var tableNames []string
	var previousTbl string
	for _, name := range dml.TableNames {
		if name != previousTbl {
			tableNames = append(tableNames, name)
			previousTbl = name
		}
	}
	return strings.Join(tableNames, ", ")
}

func allowOnlyPrimary(rss ...*srvtopo.ResolvedShard) error {
	for _, rs := range rss {
		if rs != nil && rs.Target.TabletType != topodatapb.TabletType_PRIMARY {
			return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "supported only for primary tablet type, current type: %v", topoproto.TabletTypeLString(rs.Target.TabletType))
		}
	}
	return nil
}

func execMultiShard(ctx context.Context, primitive Primitive, vcursor VCursor, rss []*srvtopo.ResolvedShard, queries []*querypb.BoundQuery, multiShardAutoCommit bool) (*sqltypes.Result, error) {
	autocommit := (len(rss) == 1 || multiShardAutoCommit) && vcursor.AutocommitApproval()
	result, errs := vcursor.ExecuteMultiShard(ctx, primitive, rss, queries, true /* rollbackOnError */, autocommit)
	return result, vterrors.Aggregate(errs)
}

func resolveKeyspaceID(ctx context.Context, vcursor VCursor, vindex vindexes.Vindex, vindexKey []sqltypes.Value) ([]byte, error) {
	var destinations []key.Destination
	var err error
	switch vdx := vindex.(type) {
	case vindexes.MultiColumn:
		destinations, err = vdx.Map(ctx, vcursor, [][]sqltypes.Value{vindexKey})
	case vindexes.SingleColumn:
		destinations, err = vdx.Map(ctx, vcursor, vindexKey)
	}

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
