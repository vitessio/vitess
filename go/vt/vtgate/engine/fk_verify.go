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

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

// FkParent is a primitive that represents a parent table foreign key constraint to verify against.
type FkParent struct {
	Values []sqlparser.Exprs
	Cols   []CheckCol
	BvName string

	Exec Primitive
}

// FkVerify is a primitive that verifies that the foreign key constraints in parent tables are satisfied.
// It does this by executing a select distinct query on the parent table with the values that are being inserted/updated.
type FkVerify struct {
	Verify []*FkParent
	Exec   Primitive

	txNeeded
}

// RouteType implements the Primitive interface
func (f *FkVerify) RouteType() string {
	return "FKVerify"
}

// GetKeyspaceName implements the Primitive interface
func (f *FkVerify) GetKeyspaceName() string {
	return f.Exec.GetKeyspaceName()
}

// GetTableName implements the Primitive interface
func (f *FkVerify) GetTableName() string {
	return f.Exec.GetTableName()
}

// GetFields implements the Primitive interface
func (f *FkVerify) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] GetFields should not be called")
}

// TryExecute implements the Primitive interface
func (f *FkVerify) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	for _, fk := range f.Verify {
		pt := newProbeTable(fk.Cols)
		newBv := &querypb.BindVariable{
			Type: querypb.Type_TUPLE,
		}
		for _, exprs := range fk.Values {
			var row sqltypes.Row
			var values []*querypb.Value
			for _, expr := range exprs {
				val, err := getValue(expr, bindVars)
				if err != nil {
					return nil, vterrors.Wrapf(err, "unable to get value for the expression %v", expr)
				}
				row = append(row, val)
				values = append(values, sqltypes.ValueToProto(val))
			}
			if exists, err := pt.exists(row); err != nil {
				return nil, err
			} else if !exists {
				newBv.Values = append(newBv.Values, &querypb.Value{Type: querypb.Type_TUPLE, Values: values})
			}
		}
		distinctValues := len(newBv.Values)
		qr, err := vcursor.ExecutePrimitive(ctx, fk.Exec, map[string]*querypb.BindVariable{fk.BvName: newBv}, wantfields)
		if err != nil {
			return nil, err
		}
		if distinctValues != len(qr.Rows) {
			return nil, vterrors.NewErrorf(vtrpcpb.Code_FAILED_PRECONDITION, vterrors.NoReferencedRow2, "Cannot add or update a child row: a foreign key constraint fails")
		}
	}
	return vcursor.ExecutePrimitive(ctx, f.Exec, bindVars, wantfields)
}

// TryStreamExecute implements the Primitive interface
func (f *FkVerify) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	for _, fk := range f.Verify {
		pt := newProbeTable(fk.Cols)
		newBv := &querypb.BindVariable{
			Type: querypb.Type_TUPLE,
		}
		for _, exprs := range fk.Values {
			var row sqltypes.Row
			var values []*querypb.Value
			for _, expr := range exprs {
				val, err := getValue(expr, bindVars)
				if err != nil {
					return vterrors.Wrapf(err, "unable to get value for the expression %v", expr)
				}
				row = append(row, val)
				values = append(values, sqltypes.ValueToProto(val))
			}
			if exists, err := pt.exists(row); err != nil {
				return err
			} else if !exists {
				newBv.Values = append(newBv.Values, &querypb.Value{Type: querypb.Type_TUPLE, Values: values})
			}
		}
		distinctValues := len(newBv.Values)

		seenRows := 0
		err := vcursor.StreamExecutePrimitive(ctx, fk.Exec, map[string]*querypb.BindVariable{fk.BvName: newBv}, wantfields, func(qr *sqltypes.Result) error {
			seenRows += len(qr.Rows)
			return nil
		})
		if err != nil {
			return err
		}

		if distinctValues != seenRows {
			return vterrors.NewErrorf(vtrpcpb.Code_FAILED_PRECONDITION, vterrors.NoReferencedRow2, "Cannot add or update a child row: a foreign key constraint fails")
		}
	}
	return vcursor.StreamExecutePrimitive(ctx, f.Exec, bindVars, wantfields, callback)
}

// Inputs implements the Primitive interface
func (f *FkVerify) Inputs() []Primitive {
	return nil
}

func (f *FkVerify) description() PrimitiveDescription {
	var parentDesc []PrimitiveDescription
	for _, parent := range f.Verify {
		parentDesc = append(parentDesc, PrimitiveDescription{
			OperatorType: "FkVerifyParent",
			Inputs: []PrimitiveDescription{
				PrimitiveToPlanDescription(parent.Exec),
			},
			Other: map[string]any{
				"BvName": parent.BvName,
				"Cols":   parent.Cols,
			},
		})
	}
	return PrimitiveDescription{
		OperatorType: f.RouteType(),
		Other: map[string]any{
			"Parent": parentDesc,
			"Child":  PrimitiveToPlanDescription(f.Exec),
		},
	}
}

var _ Primitive = (*FkVerify)(nil)

func getValue(expr sqlparser.Expr, bindVars map[string]*querypb.BindVariable) (sqltypes.Value, error) {
	switch e := expr.(type) {
	case *sqlparser.Literal:
		return sqlparser.LiteralToValue(e)
	case sqlparser.BoolVal:
		b := int32(0)
		if e {
			b = 1
		}
		return sqltypes.NewInt32(b), nil
	case *sqlparser.NullVal:
		return sqltypes.NULL, nil
	case *sqlparser.Argument:
		bv, exists := bindVars[e.Name]
		if !exists {
			return sqltypes.Value{}, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] bind variable %s missing", e.Name)
		}
		return sqltypes.BindVariableToValue(bv)
	}
	return sqltypes.Value{}, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] unexpected expression type: %T", expr)
}
