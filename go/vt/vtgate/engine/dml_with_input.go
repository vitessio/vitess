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
	"fmt"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

var _ Primitive = (*DMLWithInput)(nil)

const DmlVals = "dml_vals"

// DMLWithInput represents the instructions to perform a DML operation based on the input result.
type DMLWithInput struct {
	txNeeded

	Input Primitive

	DMLs       []Primitive
	OutputCols [][]int
	BVList     []map[string]int
}

func (dml *DMLWithInput) RouteType() string {
	return "DMLWithInput"
}

func (dml *DMLWithInput) GetKeyspaceName() string {
	return dml.Input.GetKeyspaceName()
}

func (dml *DMLWithInput) GetTableName() string {
	return dml.Input.GetTableName()
}

func (dml *DMLWithInput) Inputs() ([]Primitive, []map[string]any) {
	return append([]Primitive{dml.Input}, dml.DMLs...), nil
}

// TryExecute performs a non-streaming exec.
func (dml *DMLWithInput) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, _ bool) (*sqltypes.Result, error) {
	inputRes, err := vcursor.ExecutePrimitive(ctx, dml.Input, bindVars, false)
	if err != nil {
		return nil, err
	}
	if inputRes == nil || len(inputRes.Rows) == 0 {
		return &sqltypes.Result{}, nil
	}

	var res *sqltypes.Result
	for idx, prim := range dml.DMLs {
		var qr *sqltypes.Result
		if len(dml.BVList) == 0 || len(dml.BVList[idx]) == 0 {
			qr, err = executeLiteralUpdate(ctx, vcursor, bindVars, prim, inputRes, dml.OutputCols[idx])
		} else {
			qr, err = executeNonLiteralUpdate(ctx, vcursor, bindVars, prim, inputRes, dml.OutputCols[idx], dml.BVList[idx])
		}
		if err != nil {
			return nil, err
		}

		if res == nil {
			res = qr
		} else {
			res.RowsAffected += qr.RowsAffected
		}
	}
	return res, nil
}

// executeLiteralUpdate executes the primitive that can be executed with a single bind variable from the input result.
// The column updated have same value for all rows in the input result.
func executeLiteralUpdate(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, prim Primitive, inputRes *sqltypes.Result, outputCols []int) (*sqltypes.Result, error) {
	var bv *querypb.BindVariable
	if len(outputCols) == 1 {
		bv = getBVSingle(inputRes.Rows, outputCols[0])
	} else {
		bv = getBVMulti(inputRes.Rows, outputCols)
	}

	bindVars[DmlVals] = bv
	return vcursor.ExecutePrimitive(ctx, prim, bindVars, false)
}

func getBVSingle(rows []sqltypes.Row, offset int) *querypb.BindVariable {
	bv := &querypb.BindVariable{Type: querypb.Type_TUPLE}
	for _, row := range rows {
		bv.Values = append(bv.Values, sqltypes.ValueToProto(row[offset]))
	}
	return bv
}

func getBVMulti(rows []sqltypes.Row, offsets []int) *querypb.BindVariable {
	bv := &querypb.BindVariable{Type: querypb.Type_TUPLE}
	outputVals := make([]sqltypes.Value, 0, len(offsets))
	for _, row := range rows {
		for _, offset := range offsets {
			outputVals = append(outputVals, row[offset])
		}
		bv.Values = append(bv.Values, sqltypes.TupleToProto(outputVals))
		outputVals = outputVals[:0]
	}
	return bv
}

// executeNonLiteralUpdate executes the primitive that needs to be executed per row from the input result.
// The column updated might have different value for each row in the input result.
func executeNonLiteralUpdate(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, prim Primitive, inputRes *sqltypes.Result, outputCols []int, vars map[string]int) (qr *sqltypes.Result, err error) {
	var res *sqltypes.Result
	for _, row := range inputRes.Rows {
		var bv *querypb.BindVariable
		if len(outputCols) == 1 {
			bv = getBVSingle([]sqltypes.Row{row}, outputCols[0])
		} else {
			bv = getBVMulti([]sqltypes.Row{row}, outputCols)
		}
		bindVars[DmlVals] = bv
		for k, v := range vars {
			bindVars[k] = sqltypes.ValueBindVariable(row[v])
		}
		qr, err = vcursor.ExecutePrimitive(ctx, prim, bindVars, false)
		if err != nil {
			return nil, err
		}
		if res == nil {
			res = qr
		} else {
			res.RowsAffected += res.RowsAffected
		}
	}
	return res, nil
}

// TryStreamExecute performs a streaming exec.
func (dml *DMLWithInput) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	res, err := dml.TryExecute(ctx, vcursor, bindVars, wantfields)
	if err != nil {
		return err
	}
	return callback(res)
}

// GetFields fetches the field info.
func (dml *DMLWithInput) GetFields(context.Context, VCursor, map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return nil, vterrors.VT13001("unreachable code for DMLs")
}

func (dml *DMLWithInput) description() PrimitiveDescription {
	var offsets []string
	for idx, offset := range dml.OutputCols {
		offsets = append(offsets, fmt.Sprintf("%d:%v", idx, offset))
	}
	other := map[string]any{
		"Offset": offsets,
	}
	var bvList []string
	for idx, vars := range dml.BVList {
		if len(vars) == 0 {
			continue
		}
		bvList = append(bvList, fmt.Sprintf("%d:[%s]", idx, orderedStringIntMap(vars)))
	}
	if len(bvList) > 0 {
		other["BindVars"] = bvList
	}
	return PrimitiveDescription{
		OperatorType:     "DMLWithInput",
		TargetTabletType: topodatapb.TabletType_PRIMARY,
		Other:            other,
	}
}
