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
)

var _ Primitive = (*BlockJoin)(nil)

// BlockJoin is a primitive that joins two primitives by constructing a table from the rows of the LHS primitive.
// The table is passed in as a bind variable to the RHS primitive.
// It's called BlockJoin because the data from the LHS of the join is sent to the RHS as a block of values and not row by row.
type BlockJoin struct {
	// Left and Right are the LHS and RHS primitives
	// of the Join. They can be any primitive.
	Left, Right Primitive

	// The name for the bind var containing the tuple-of-tuples being sent to the RHS
	BindVarName string
}

// TryExecute performs a non-streaming exec.
func (jv *BlockJoin) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	lresult, err := vcursor.ExecutePrimitive(ctx, jv.Left, bindVars, wantfields)
	if err != nil {
		return nil, err
	}
	bv := &querypb.BindVariable{
		Type: querypb.Type_ROW_TUPLE,
	}
	if len(lresult.Rows) == 0 && wantfields {
		// If there are no rows, we still need to construct a single row
		// to send down to RHS for Values Table to execute correctly.
		// It will be used to execute the field query to provide the output fields.
		var vals []sqltypes.Value
		for _, field := range lresult.Fields {
			val, _ := sqltypes.NewValue(field.Type, valueForType(field))
			vals = append(vals, val)
		}
		bv.Values = append(bv.Values, sqltypes.TupleToProto(vals))

		bindVars[jv.BindVarName] = bv
		return jv.Right.GetFields(ctx, vcursor, bindVars)
	}

	for _, row := range lresult.Rows {
		bv.Values = append(bv.Values, sqltypes.TupleToProto(row))
	}

	bindVars[jv.BindVarName] = bv
	return vcursor.ExecutePrimitive(ctx, jv.Right, bindVars, wantfields)
}

// TryStreamExecute performs a streaming exec.
func (jv *BlockJoin) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	// TODO: Implement streaming. This is a placeholder implementation.
	rs, err := jv.TryExecute(ctx, vcursor, bindVars, wantfields)
	if err != nil {
		return err
	}
	return callback(rs)
}

// GetFields fetches the field info.
func (jv *BlockJoin) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return jv.Right.GetFields(ctx, vcursor, bindVars)
}

// Inputs returns the input primitives for this join
func (jv *BlockJoin) Inputs() ([]Primitive, []map[string]any) {
	return []Primitive{jv.Left, jv.Right}, nil
}

// NeedsTransaction implements the Primitive interface
func (jv *BlockJoin) NeedsTransaction() bool {
	return jv.Right.NeedsTransaction() || jv.Left.NeedsTransaction()
}

func (jv *BlockJoin) description() PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType: "Join",
		Variant:      "Block",
		Other: map[string]any{
			"BindVarName": jv.BindVarName,
		},
	}
}
