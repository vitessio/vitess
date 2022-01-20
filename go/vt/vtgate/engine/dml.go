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

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
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
	Table *vindexes.Table

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

func resolveMultiValueShards(
	vcursor VCursor,
	keyspace *vindexes.Keyspace,
	query string,
	bindVars map[string]*querypb.BindVariable,
	values []evalengine.Expr,
	vindex vindexes.Vindex,
) ([]*srvtopo.ResolvedShard, []*querypb.BoundQuery, error) {
	var rss []*srvtopo.ResolvedShard
	var err error
	switch vindex.(type) {
	case vindexes.SingleColumn:
		rss, err = resolveSingleColVindex(vcursor, bindVars, keyspace, vindex, values)
	case vindexes.MultiColumn:
		rss, err = resolveMultiColVindex(vcursor, bindVars, keyspace, vindex, values)
	default:
		return nil, nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] unexpected vindex type: %T", vindex)
	}
	if err != nil {
		return nil, nil, err
	}
	queries := make([]*querypb.BoundQuery, len(rss))
	for i := range rss {
		queries[i] = &querypb.BoundQuery{
			Sql:           query,
			BindVariables: bindVars,
		}
	}
	return rss, queries, nil
}

func resolveSingleColVindex(vcursor VCursor, bindVars map[string]*querypb.BindVariable, keyspace *vindexes.Keyspace, vindex vindexes.Vindex, values []evalengine.Expr) ([]*srvtopo.ResolvedShard, error) {
	env := evalengine.EnvWithBindVars(bindVars)
	keys, err := env.Evaluate(values[0])
	if err != nil {
		return nil, err
	}
	rss, err := resolveMultiShard(vcursor, vindex.(vindexes.SingleColumn), keyspace, keys.TupleValues())
	if err != nil {
		return nil, err
	}
	return rss, nil
}

func resolveMultiColVindex(vcursor VCursor, bindVars map[string]*querypb.BindVariable, keyspace *vindexes.Keyspace, vindex vindexes.Vindex, values []evalengine.Expr) ([]*srvtopo.ResolvedShard, error) {
	rowColValues, _, err := generateRowColValues(bindVars, values)
	if err != nil {
		return nil, err
	}

	rss, _, err := resolveShardsMultiCol(vcursor, vindex.(vindexes.MultiColumn), keyspace, rowColValues, false /* shardIdsNeeded */)
	if err != nil {
		return nil, err
	}
	return rss, nil
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

func execMultiShard(vcursor VCursor, rss []*srvtopo.ResolvedShard, queries []*querypb.BoundQuery, multiShardAutoCommit bool) (*sqltypes.Result, error) {
	autocommit := (len(rss) == 1 || multiShardAutoCommit) && vcursor.AutocommitApproval()
	result, errs := vcursor.ExecuteMultiShard(rss, queries, true /* rollbackOnError */, autocommit)
	return result, vterrors.Aggregate(errs)
}

func allowOnlyPrimary(rss ...*srvtopo.ResolvedShard) error {
	for _, rs := range rss {
		if rs != nil && rs.Target.TabletType != topodatapb.TabletType_PRIMARY {
			return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "supported only for primary tablet type, current type: %v", topoproto.TabletTypeLString(rs.Target.TabletType))
		}
	}
	return nil
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
