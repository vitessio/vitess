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
	// Query specifies the query to be executed.
	Query string

	// KsidVindex is primary Vindex
	KsidVindex vindexes.Vindex

	// KsidLength is number of columns that represents KsidVindex
	KsidLength int

	// Table specifies the table for the update.
	Table []*vindexes.Table

	// OwnedVindexQuery is used for updating changes in lookup vindexes.
	OwnedVindexQuery string

	// Option to override the standard behavior and allow a multi-shard update
	// to use single round trip autocommit.
	MultiShardAutocommit bool

	// QueryTimeout contains the optional timeout (in milliseconds) to apply to this query
	QueryTimeout int

	// RoutingParameters parameters required for query routing.
	*RoutingParameters

	txNeeded
}

// NewDML returns and empty initialized DML struct.
func NewDML() *DML {
	return &DML{RoutingParameters: &RoutingParameters{}}
}

func (dml *DML) execUnsharded(vcursor VCursor, bindVars map[string]*querypb.BindVariable, rss []*srvtopo.ResolvedShard) (*sqltypes.Result, error) {
	return execShard(vcursor, dml.Query, bindVars, rss[0], true, true /* canAutocommit */)
}

func (dml *DML) execMultiDestination(vcursor VCursor, bindVars map[string]*querypb.BindVariable, rss []*srvtopo.ResolvedShard, dmlSpecialFunc func(VCursor, map[string]*querypb.BindVariable, []*srvtopo.ResolvedShard) error) (*sqltypes.Result, error) {
	if len(rss) == 0 {
		return &sqltypes.Result{}, nil
	}
	err := dmlSpecialFunc(vcursor, bindVars, rss)
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
	return execMultiShard(vcursor, rss, queries, dml.MultiShardAutocommit)
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
	if dml.Table != nil {
		tableNameMap := map[string]any{}
		for _, table := range dml.Table {
			tableNameMap[table.Name.String()] = nil
		}

		var tableNames []string
		for name := range tableNameMap {
			tableNames = append(tableNames, name)
		}
		sort.Strings(tableNames)

		return strings.Join(tableNames, ", ")
	}
	return ""
}

// GetSingleTable returns single table used in dml.
func (dml *DML) GetSingleTable() (*vindexes.Table, error) {
	if len(dml.Table) > 1 {
		return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "unsupported dml on complex table expression")
	}
	return dml.Table[0], nil
}

func allowOnlyPrimary(rss ...*srvtopo.ResolvedShard) error {
	for _, rs := range rss {
		if rs != nil && rs.Target.TabletType != topodatapb.TabletType_PRIMARY {
			return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "supported only for primary tablet type, current type: %v", topoproto.TabletTypeLString(rs.Target.TabletType))
		}
	}
	return nil
}

func execMultiShard(vcursor VCursor, rss []*srvtopo.ResolvedShard, queries []*querypb.BoundQuery, multiShardAutoCommit bool) (*sqltypes.Result, error) {
	autocommit := (len(rss) == 1 || multiShardAutoCommit) && vcursor.AutocommitApproval()
	result, errs := vcursor.ExecuteMultiShard(rss, queries, true /* rollbackOnError */, autocommit)
	return result, vterrors.Aggregate(errs)
}

func resolveKeyspaceID(vcursor VCursor, vindex vindexes.Vindex, vindexKey []sqltypes.Value) ([]byte, error) {
	var destinations []key.Destination
	var err error
	switch vdx := vindex.(type) {
	case vindexes.MultiColumn:
		destinations, err = vdx.Map(vcursor, [][]sqltypes.Value{vindexKey})
	case vindexes.SingleColumn:
		destinations, err = vdx.Map(vcursor, vindexKey)
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
