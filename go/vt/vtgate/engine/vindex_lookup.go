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

	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"

	"vitess.io/vitess/go/vt/key"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

var _ Primitive = (*VindexLookup)(nil)

type VindexLookup struct {
	Opcode Opcode

	// The vindex to use to do the Map
	Vindex vindexes.LookupPlanable

	// Keyspace specifies the keyspace to send the query to.
	Keyspace *vindexes.Keyspace

	Arguments []string

	// Values specifies the vindex values to use for routing.
	Values []evalengine.Expr

	// We fetch data in order to do the map from this primitive
	Lookup Primitive

	// This is the side that needs to be routed
	SendTo *Route
}

// RouteType implements the Primitive interface
func (vr *VindexLookup) RouteType() string {
	return "VindexLookup"
}

// GetKeyspaceName implements the Primitive interface
func (vr *VindexLookup) GetKeyspaceName() string {
	return vr.SendTo.GetKeyspaceName()
}

// GetTableName implements the Primitive interface
func (vr *VindexLookup) GetTableName() string {
	return vr.SendTo.GetTableName()
}

// GetFields implements the Primitive interface
func (vr *VindexLookup) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return vr.SendTo.GetFields(ctx, vcursor, bindVars)
}

// NeedsTransaction implements the Primitive interface
func (vr *VindexLookup) NeedsTransaction() bool {
	return vr.SendTo.NeedsTransaction()
}

// TryExecute implements the Primitive interface
func (vr *VindexLookup) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	ids, err := vr.generateIds(vcursor, bindVars)
	if err != nil {
		return nil, err
	}
	results, err := vr.lookup(ctx, vcursor, ids)
	if err != nil {
		return nil, err
	}

	dest, err := vr.mapVindexToDestination(ids, results, bindVars)
	if err != nil {
		return nil, err
	}

	return vr.SendTo.executeAfterLookup(ctx, vcursor, bindVars, wantfields, ids, dest)
}

func (vr *VindexLookup) mapVindexToDestination(ids []sqltypes.Value, results []*sqltypes.Result, bindVars map[string]*querypb.BindVariable) ([]key.Destination, error) {
	dest, err := vr.Vindex.MapResult(ids, results)
	if err != nil {
		return nil, err
	}

	if vr.Opcode == IN {
		valsBV := &querypb.BindVariable{Type: querypb.Type_TUPLE}
		valsBV.Values = make([]*querypb.Value, 0, len(ids))
		for _, value := range ids {
			valsBV.Values = append(valsBV.Values, sqltypes.ValueToProto(value))
		}
		bindVars[ListVarName] = valsBV
	}
	return dest, nil
}

// TryStreamExecute implements the Primitive interface
func (vr *VindexLookup) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	ids, err := vr.generateIds(vcursor, bindVars)
	if err != nil {
		return err
	}

	results, err := vr.lookup(ctx, vcursor, ids)
	if err != nil {
		return err
	}

	dest, err := vr.mapVindexToDestination(ids, results, bindVars)
	if err != nil {
		return err
	}

	return vr.SendTo.streamExecuteAfterLookup(ctx, vcursor, bindVars, wantfields, callback, ids, dest)
}

// Inputs implements the Primitive interface
func (vr *VindexLookup) Inputs() []Primitive {
	if vr.Lookup != nil {
		return []Primitive{vr.Lookup, vr.SendTo}
	}

	return []Primitive{vr.SendTo}
}

// description implements the Primitive interface
func (vr *VindexLookup) description() PrimitiveDescription {
	other := map[string]any{}
	if vr.Values != nil {
		formattedValues := make([]string, 0, len(vr.Values))
		for _, value := range vr.Values {
			formattedValues = append(formattedValues, evalengine.FormatExpr(value))
		}
		other["Values"] = formattedValues
	}
	other["Vindex"] = vr.Vindex.String()

	return PrimitiveDescription{
		OperatorType: "VindexLookup",
		Variant:      vr.Opcode.String(),
		Keyspace:     vr.Keyspace,
		Other:        other,
	}
}

func (vr *VindexLookup) lookup(ctx context.Context, vcursor VCursor, ids []sqltypes.Value) ([]*sqltypes.Result, error) {
	co := vr.Vindex.GetCommitOrder()
	if co != vtgatepb.CommitOrder_NORMAL {
		vcursor.Session().SetCommitOrder(co)
		defer vcursor.Session().SetCommitOrder(vtgatepb.CommitOrder_NORMAL)
	}
	if ids[0].IsIntegral() || vr.Vindex.AllowBatch() {
		return vr.executeBatch(ctx, vcursor, ids)
	}
	return vr.executeNonBatch(ctx, vcursor, ids)
}

func (vr *VindexLookup) executeNonBatch(ctx context.Context, vcursor VCursor, ids []sqltypes.Value) ([]*sqltypes.Result, error) {
	results := make([]*sqltypes.Result, 0, len(ids))
	// for non integral and binary type, fallback to send query per id
	for _, id := range ids {
		vars, err := sqltypes.BuildBindVariable([]any{id})
		if err != nil {
			return nil, err
		}
		bindVars := map[string]*querypb.BindVariable{
			vr.Arguments[0]: vars,
		}
		result, err := vcursor.ExecutePrimitive(ctx, vr.Lookup, bindVars, false)
		if err != nil {
			return nil, err
		}
		rows := make([][]sqltypes.Value, 0, len(result.Rows))
		for _, row := range result.Rows {
			rows = append(rows, []sqltypes.Value{row[1]})
		}
		results = append(results, &sqltypes.Result{
			Rows: rows,
		})
	}
	return results, nil
}

func (vr *VindexLookup) executeBatch(ctx context.Context, vcursor VCursor, ids []sqltypes.Value) ([]*sqltypes.Result, error) {
	results := make([]*sqltypes.Result, 0, len(ids))
	// for integral types, batch query all ids and then map them back to the input order
	vars, err := sqltypes.BuildBindVariable(ids)
	if err != nil {
		return nil, err
	}
	bindVars := map[string]*querypb.BindVariable{
		vr.Arguments[0]: vars,
	}
	result, err := vcursor.ExecutePrimitive(ctx, vr.Lookup, bindVars, false)
	if err != nil {
		return nil, vterrors.Wrapf(err, "failed while running the lookup query")
	}
	resultMap := make(map[string][][]sqltypes.Value)
	for _, row := range result.Rows {
		resultMap[row[0].ToString()] = append(resultMap[row[0].ToString()], []sqltypes.Value{row[1]})
	}

	for _, id := range ids {
		results = append(results, &sqltypes.Result{
			Rows: resultMap[id.ToString()],
		})
	}
	return results, nil
}

func (vr *VindexLookup) generateIds(vcursor VCursor, bindVars map[string]*querypb.BindVariable) ([]sqltypes.Value, error) {
	env := evalengine.EnvWithBindVars(bindVars, vcursor.ConnCollation())
	value, err := env.Evaluate(vr.Values[0])
	if err != nil {
		return nil, err
	}
	switch vr.Opcode {
	case Equal, EqualUnique:
		return []sqltypes.Value{value.Value()}, nil
	case IN:
		return value.TupleValues(), nil
	}
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "opcode %s not supported for VindexLookup", vr.Opcode.String())
}
