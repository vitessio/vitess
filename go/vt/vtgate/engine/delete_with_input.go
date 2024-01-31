/*
Copyright 2023 The Vitess Authors.

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

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

var _ Primitive = (*DeleteWithInput)(nil)

const DmVals = "dm_vals"

// DeleteWithInput represents the instructions to perform a delete operation based on the input result.
type DeleteWithInput struct {
	Delete Primitive
	Input  Primitive

	OutputCols []int

	txNeeded
}

func (del *DeleteWithInput) RouteType() string {
	return "DeleteWithInput"
}

func (del *DeleteWithInput) GetKeyspaceName() string {
	return del.Input.GetKeyspaceName()
}

func (del *DeleteWithInput) GetTableName() string {
	return del.Input.GetTableName()
}

func (del *DeleteWithInput) Inputs() ([]Primitive, []map[string]any) {
	return []Primitive{del.Input, del.Delete}, nil
}

// TryExecute performs a non-streaming exec.
func (del *DeleteWithInput) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, _ bool) (*sqltypes.Result, error) {
	inputRes, err := vcursor.ExecutePrimitive(ctx, del.Input, bindVars, false)
	if err != nil {
		return nil, err
	}

	var bv *querypb.BindVariable
	if len(del.OutputCols) == 1 {
		bv = getBVSingle(inputRes, del.OutputCols[0])
	} else {
		bv = getBVMulti(inputRes, del.OutputCols)
	}

	bindVars[DmVals] = bv
	return vcursor.ExecutePrimitive(ctx, del.Delete, bindVars, false)
}

func getBVSingle(res *sqltypes.Result, offset int) *querypb.BindVariable {
	bv := &querypb.BindVariable{Type: querypb.Type_TUPLE}
	for _, row := range res.Rows {
		bv.Values = append(bv.Values, sqltypes.ValueToProto(row[offset]))
	}
	return bv
}

func getBVMulti(res *sqltypes.Result, offsets []int) *querypb.BindVariable {
	bv := &querypb.BindVariable{Type: querypb.Type_TUPLE}
	outputVals := make([]sqltypes.Value, 0, len(offsets))
	for _, row := range res.Rows {
		for _, offset := range offsets {
			outputVals = append(outputVals, row[offset])
		}
		bv.Values = append(bv.Values, sqltypes.TupleToProto(outputVals))
		outputVals = outputVals[:0]
	}
	return bv
}

// TryStreamExecute performs a streaming exec.
func (del *DeleteWithInput) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	res, err := del.TryExecute(ctx, vcursor, bindVars, wantfields)
	if err != nil {
		return err
	}
	return callback(res)
}

// GetFields fetches the field info.
func (del *DeleteWithInput) GetFields(context.Context, VCursor, map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return nil, vterrors.VT13001("unreachable code for MULTI DELETE")
}

func (del *DeleteWithInput) description() PrimitiveDescription {
	other := map[string]any{
		"Offset": del.OutputCols,
	}
	return PrimitiveDescription{
		OperatorType:     "DeleteWithInput",
		TargetTabletType: topodatapb.TabletType_PRIMARY,
		Other:            other,
	}
}
