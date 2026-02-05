/*
Copyright 2025 The Vitess Authors.

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

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vterrors"
)

var _ Primitive = (*ValuesJoin)(nil)

// ValuesJoin is a primitive that joins two primitives by constructing a table from the rows of the LHS primitive.
// The table is passed in as a bind variable to the RHS primitive.
// It's called ValuesJoin because the LHS of the join is sent to the RHS as a VALUES clause.
type ValuesJoin struct {
	// Left and Right are the LHS and RHS primitives
	// of the Join. They can be any primitive.
	Left, Right Primitive

	Vars              []int
	RowConstructorArg string
	Cols              []int
	ColNames          []string
}

// TryExecute performs a non-streaming exec.
func (jv *ValuesJoin) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	lresult, err := vcursor.ExecutePrimitive(ctx, jv.Left, bindVars, wantfields)
	if err != nil {
		return nil, err
	}
	bv := &querypb.BindVariable{
		Type: querypb.Type_TUPLE,
	}
	if len(lresult.Rows) == 0 && wantfields {
		// If there are no rows, we still need to construct a single row
		// to send down to RHS for Values Table to execute correctly.
		// It will be used to execute the field query to provide the output fields.
		var vals []sqltypes.Value
		for _, field := range lresult.Fields {
			val, _ := sqltypes.NewValue(field.Type, nil)
			vals = append(vals, val)
		}
		bv.Values = append(bv.Values, sqltypes.TupleToProto(vals))

		bindVars[jv.RowConstructorArg] = bv
		return jv.Right.GetFields(ctx, vcursor, bindVars)
	}

	for i, row := range lresult.Rows {
		newRow := make(sqltypes.Row, 0, len(jv.Vars)+1)      // +1 since we always add the row ID
		newRow = append(newRow, sqltypes.NewInt64(int64(i))) // Adding the LHS row ID

		for _, loffset := range jv.Vars {
			newRow = append(newRow, row[loffset])
		}

		bv.Values = append(bv.Values, sqltypes.TupleToProto(newRow))
	}

	bindVars[jv.RowConstructorArg] = bv
	rresult, err := vcursor.ExecutePrimitive(ctx, jv.Right, bindVars, wantfields)
	if err != nil {
		return nil, err
	}

	result := &sqltypes.Result{}

	result.Fields = joinFields(lresult.Fields, rresult.Fields, jv.Cols)
	for i := range result.Fields {
		result.Fields[i].Name = jv.ColNames[i]
	}

	for _, rrow := range rresult.Rows {
		lhsRowID, err := rrow[len(rrow)-1].ToCastInt64()
		if err != nil {
			return nil, vterrors.VT13001("values joins cannot fetch lhs row ID: " + err.Error())
		}

		result.Rows = append(result.Rows, joinRows(lresult.Rows[lhsRowID], rrow, jv.Cols))
	}

	return result, nil
}

// TryStreamExecute performs a streaming exec.
func (jv *ValuesJoin) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	panic("implement me")
}

// GetFields fetches the field info.
func (jv *ValuesJoin) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return jv.Right.GetFields(ctx, vcursor, bindVars)
}

// Inputs returns the input primitives for this join
func (jv *ValuesJoin) Inputs() ([]Primitive, []map[string]any) {
	return []Primitive{jv.Left, jv.Right}, nil
}

// NeedsTransaction implements the Primitive interface
func (jv *ValuesJoin) NeedsTransaction() bool {
	return jv.Right.NeedsTransaction() || jv.Left.NeedsTransaction()
}

func (jv *ValuesJoin) description() PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType: "Join",
		Variant:      "Values",
		Other: map[string]any{
			"ValuesArg": jv.RowConstructorArg,
			"Vars":      jv.Vars,
		},
	}
}
