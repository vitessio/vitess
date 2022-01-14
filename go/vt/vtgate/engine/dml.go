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
	// Opcode is the execution opcode.
	Opcode DMLOpcode

	// Keyspace specifies the keyspace to send the query to.
	Keyspace *vindexes.Keyspace

	// TargetDestination specifies the destination to send the query to.
	TargetDestination key.Destination

	// Query specifies the query to be executed.
	Query string

	// Vindex specifies the vindex to be used.
	Vindex vindexes.Vindex

	// Values specifies the vindex values to use for routing.
	// For now, only one value is specified.
	Values []evalengine.Expr

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

	txNeeded
}

// DMLOpcode is a number representing the opcode
// for the Update or Delete primitve.
type DMLOpcode int

// This is the list of UpdateOpcode values.
const (
	// Unsharded is for routing a dml statement
	// to an unsharded keyspace.
	Unsharded = DMLOpcode(iota)
	// Equal is for routing an dml statement to a single shard.
	// Requires: A Vindex, and a single Value.
	Equal
	// In is for routing an dml statement to a multi shard.
	// Requires: A Vindex, and a multi Values.
	In
	// Scatter is for routing a scattered dml statement.
	Scatter
	// ByDestination is to route explicitly to a given target destination.
	// Is used when the query explicitly sets a target destination:
	// in the clause e.g: UPDATE `keyspace[-]`.x1 SET foo=1
	ByDestination
)

var opcodeName = map[DMLOpcode]string{
	Unsharded:     "Unsharded",
	Equal:         "Equal",
	In:            "In",
	Scatter:       "Scatter",
	ByDestination: "ByDestination",
}

func (op DMLOpcode) String() string {
	return opcodeName[op]
}

func resolveMultiValueShards(
	vcursor VCursor,
	keyspace *vindexes.Keyspace,
	query string,
	bindVars map[string]*querypb.BindVariable,
	val evalengine.Expr,
	vindex vindexes.SingleColumn,
) ([]*srvtopo.ResolvedShard, []*querypb.BoundQuery, error) {
	env := evalengine.EnvWithBindVars(bindVars)
	keys, err := env.Evaluate(val)
	if err != nil {
		return nil, nil, err
	}
	rss, err := resolveMultiShard(vcursor, vindex, keyspace, keys.TupleValues())
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
