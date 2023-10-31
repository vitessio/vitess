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
	// BVName is the bind variable name for the tuple bind variable used in the primitive.
	BVName string
	// Cols are the indexes of the column that need to be selected from the SELECT query to create the tuple bind variable.
	Cols []int
	// UpdateExprBvNames is the list of bind variables for non-literal expressions in UPDATES.
	UpdateExprBvNames []string
	// UpdateExprCols is the list of indexes for non-literal expressions in UPDATES that need to be taken from the SELECT.
	UpdateExprCols []int
	// CompExprCols is the list of indexes for the comparison in the SELECTS to know if the column is actually being updated or not.
	CompExprCols []int
	Exec         Primitive
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
		// Having non-empty UpdateExprBvNames is an indication that we have an update query with non-literal expressions in it.
		// We need to run this query differently because we need to run an update for each row we get back from the SELECT.
		if len(child.UpdateExprBvNames) > 0 {
			err = fkc.executeNonLiteralExprFkChild(ctx, vcursor, bindVars, wantfields, selectionRes, child, false)
		} else {
			err = fkc.executeLiteralExprFkChild(ctx, vcursor, bindVars, wantfields, selectionRes, child, false)
		}
		if err != nil {
			return nil, err
		}
	}

	// All the children are modified successfully, we can now execute the Parent Primitive.
	return vcursor.ExecutePrimitive(ctx, fkc.Parent, bindVars, wantfields)
}

func (fkc *FkCascade) executeLiteralExprFkChild(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, selectionRes *sqltypes.Result, child *FkChild, isStreaming bool) error {
	// We create a bindVariable that stores the tuple of columns involved in the fk constraint.
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
	var err error
	if isStreaming {
		err = vcursor.StreamExecutePrimitive(ctx, child.Exec, bindVars, wantfields, func(result *sqltypes.Result) error { return nil })
	} else {
		_, err = vcursor.ExecutePrimitive(ctx, child.Exec, bindVars, wantfields)
	}
	if err != nil {
		return err
	}
	delete(bindVars, child.BVName)
	return nil
}

func (fkc *FkCascade) executeNonLiteralExprFkChild(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, selectionRes *sqltypes.Result, child *FkChild, isStreaming bool) error {
	// For each row in the SELECT we need to run the child primitive.
	for _, row := range selectionRes.Rows {
		// First we check if any of the columns is being updated at all.
		skipRow := true
		for _, colIdx := range child.CompExprCols {
			// The comparison expression in NULL means that the update expression itself was NULL.
			if row[colIdx].IsNull() {
				skipRow = false
				break
			}
			// Now we check if the column has updated or not.
			hasChanged, err := row[colIdx].ToBool()
			if err != nil {
				return err
			}
			if hasChanged {
				// If any column has changed, then we can't skip this row.
				// We need to execute the child primitive.
				skipRow = false
				break
			}
		}
		// If none of the columns have changed, then there is no update to cascade, we can move on.
		if skipRow {
			continue
		}
		// We create a bindVariable that stores the tuple of columns involved in the fk constraint.
		bv := &querypb.BindVariable{
			Type: querypb.Type_TUPLE,
		}
		// Create a tuple from the Row.
		var tupleValues []sqltypes.Value
		for _, colIdx := range child.Cols {
			tupleValues = append(tupleValues, row[colIdx])
		}
		bv.Values = append(bv.Values, sqltypes.TupleToProto(tupleValues))
		// Execute the child primitive, and bail out incase of failure.
		// Since this Primitive is always executed in a transaction, the changes should
		// be rolled back in case of an error.
		bindVars[child.BVName] = bv

		// Next, we need to copy the updated expressions value into the bind variables map.
		for idx, updateExprBvName := range child.UpdateExprBvNames {
			bindVars[updateExprBvName] = sqltypes.ValueBindVariable(row[child.UpdateExprCols[idx]])
		}
		var err error
		if isStreaming {
			err = vcursor.StreamExecutePrimitive(ctx, child.Exec, bindVars, wantfields, func(result *sqltypes.Result) error { return nil })
		} else {
			_, err = vcursor.ExecutePrimitive(ctx, child.Exec, bindVars, wantfields)
		}
		if err != nil {
			return err
		}
		// Remove the bind variables that have been used and are no longer required.
		delete(bindVars, child.BVName)
		for _, updateExprBvName := range child.UpdateExprBvNames {
			delete(bindVars, updateExprBvName)
		}
	}
	return nil
}

// TryStreamExecute implements the Primitive interface.
func (fkc *FkCascade) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	var selectionRes *sqltypes.Result
	// Execute the Selection primitive to find the rows that are going to modified.
	// This will be used to find the rows that need modification on the children.
	err := vcursor.StreamExecutePrimitive(ctx, fkc.Selection, bindVars, wantfields, func(result *sqltypes.Result) error {
		if len(result.Rows) == 0 {
			return nil
		}
		if selectionRes == nil {
			selectionRes = result
			return nil
		}
		selectionRes.Rows = append(selectionRes.Rows, result.Rows...)
		return nil
	})
	if err != nil {
		return err
	}

	// If no rows are to be modified, there is nothing to do.
	if selectionRes == nil || len(selectionRes.Rows) == 0 {
		return callback(&sqltypes.Result{})
	}

	// Execute the child primitive, and bail out incase of failure.
	// Since this Primitive is always executed in a transaction, the changes should
	// be rolled back incase of an error.
	for _, child := range fkc.Children {
		if len(child.UpdateExprBvNames) > 0 {
			err = fkc.executeNonLiteralExprFkChild(ctx, vcursor, bindVars, wantfields, selectionRes, child, true)
		} else {
			err = fkc.executeLiteralExprFkChild(ctx, vcursor, bindVars, wantfields, selectionRes, child, true)
		}
		if err != nil {
			return err
		}
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
		childInfoMap := map[string]any{
			inputName: fmt.Sprintf("CascadeChild-%d", idx+1),
			"BvName":  child.BVName,
			"Cols":    child.Cols,
		}
		if len(child.UpdateExprBvNames) > 0 {
			childInfoMap["UpdateExprBvNames"] = child.UpdateExprBvNames
			childInfoMap["UpdateExprCols"] = child.UpdateExprCols
			childInfoMap["CompExprCols"] = child.CompExprCols
		}
		inputsMap = append(inputsMap, childInfoMap)
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
