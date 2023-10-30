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

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// FkChild contains the Child Primitive to be executed collecting the values from the Selection Primitive using the column indexes.
// BVName is used to pass the value as bind variable to the Child Primitive.
type FkChild struct {
	BVName string
	Cols   []int // indexes
	Exec   Primitive
}

// FkCascade is a primitive that implements foreign key cascading using Selection as values required to execute the FkChild Primitives.
// On success, it executes the Parent Primitive.
type FkCascade struct {
	// Selection is the Primitive that is used to find the rows that are going to be modified in the child tables.
	Selection Primitive
	// Children is a list of child foreign key Primitives that are executed using rows from the Selection Primitive.
	Children []*FkChild
	// Parent is the Primitive that is executed after the children are modified.
	Parent Primitive

	txNeeded
}

// RouteType implements the Primitive interface.
func (fkc *FkCascade) RouteType() string {
	return "FkCascade"
}

// GetKeyspaceName implements the Primitive interface.
func (fkc *FkCascade) GetKeyspaceName() string {
	return fkc.Parent.GetKeyspaceName()
}

// GetTableName implements the Primitive interface.
func (fkc *FkCascade) GetTableName() string {
	return fkc.Parent.GetTableName()
}

// GetFields implements the Primitive interface.
func (fkc *FkCascade) GetFields(_ context.Context, _ VCursor, _ map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] GetFields should not be called")
}

// TryExecute implements the Primitive interface.
func (fkc *FkCascade) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	// Execute the Selection primitive to find the rows that are going to modified.
	// This will be used to find the rows that need modification on the children.
	selectionRes, err := vcursor.ExecutePrimitive(ctx, fkc.Selection, bindVars, wantfields)
	if err != nil {
		return nil, err
	}

	// If no rows are to be modified, there is nothing to do.
	if len(selectionRes.Rows) == 0 {
		return &sqltypes.Result{}, nil
	}

	for _, child := range fkc.Children {
		// We create a bindVariable for each Child
		// that stores the tuple of columns involved in the fk constraint.
		bv := &querypb.BindVariable{
			Type: querypb.Type_TUPLE,
		}
		for _, row := range selectionRes.Rows {
			var tupleValues []sqltypes.Value
			for _, colIdx := range child.Cols {
				tupleValues = append(tupleValues, row[colIdx])
			}
			bv.Values = append(bv.Values, sqltypes.TupleToProto(tupleValues))
		}
		// Execute the child primitive, and bail out incase of failure.
		// Since this Primitive is always executed in a transaction, the changes should
		// be rolled back incase of an error.
		bindVars[child.BVName] = bv
		_, err = vcursor.ExecutePrimitive(ctx, child.Exec, bindVars, wantfields)
		if err != nil {
			return nil, err
		}
		delete(bindVars, child.BVName)
	}

	// All the children are modified successfully, we can now execute the Parent Primitive.
	return vcursor.ExecutePrimitive(ctx, fkc.Parent, bindVars, wantfields)
}

// TryStreamExecute implements the Primitive interface.
func (fkc *FkCascade) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	// We create a bindVariable for each Child
	// that stores the tuple of columns involved in the fk constraint.
	var bindVariables []*querypb.BindVariable
	for range fkc.Children {
		bindVariables = append(bindVariables, &querypb.BindVariable{
			Type: querypb.Type_TUPLE,
		})
	}

	// Execute the Selection primitive to find the rows that are going to modified.
	// This will be used to find the rows that need modification on the children.
	err := vcursor.StreamExecutePrimitive(ctx, fkc.Selection, bindVars, wantfields, func(result *sqltypes.Result) error {
		if len(result.Rows) == 0 {
			return nil
		}
		for idx, child := range fkc.Children {
			for _, row := range result.Rows {
				var tupleValues []sqltypes.Value
				for _, colIdx := range child.Cols {
					tupleValues = append(tupleValues, row[colIdx])
				}
				bindVariables[idx].Values = append(bindVariables[idx].Values, sqltypes.TupleToProto(tupleValues))
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Execute the child primitive, and bail out incase of failure.
	// Since this Primitive is always executed in a transaction, the changes should
	// be rolled back incase of an error.
	for idx, child := range fkc.Children {
		bindVars[child.BVName] = bindVariables[idx]
		err = vcursor.StreamExecutePrimitive(ctx, child.Exec, bindVars, wantfields, func(result *sqltypes.Result) error {
			return nil
		})
		if err != nil {
			return err
		}
		delete(bindVars, child.BVName)
	}

	// All the children are modified successfully, we can now execute the Parent Primitive.
	return vcursor.StreamExecutePrimitive(ctx, fkc.Parent, bindVars, wantfields, callback)
}

// Inputs implements the Primitive interface.
func (fkc *FkCascade) Inputs() ([]Primitive, []map[string]any) {
	var inputs []Primitive
	var inputsMap []map[string]any
	inputs = append(inputs, fkc.Selection)
	inputsMap = append(inputsMap, map[string]any{
		inputName: "Selection",
	})
	for idx, child := range fkc.Children {
		inputsMap = append(inputsMap, map[string]any{
			inputName: fmt.Sprintf("CascadeChild-%d", idx+1),
			"BvName":  child.BVName,
			"Cols":    child.Cols,
		})
		inputs = append(inputs, child.Exec)
	}
	inputs = append(inputs, fkc.Parent)
	inputsMap = append(inputsMap, map[string]any{
		inputName: "Parent",
	})
	return inputs, inputsMap
}

func (fkc *FkCascade) description() PrimitiveDescription {
	return PrimitiveDescription{OperatorType: fkc.RouteType()}
}

var _ Primitive = (*FkCascade)(nil)
