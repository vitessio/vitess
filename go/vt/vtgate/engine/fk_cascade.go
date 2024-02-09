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
	"maps"

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
	// NonLiteralInfo stores the information that is needed to run an update query with non-literal values.
	NonLiteralInfo []NonLiteralUpdateInfo
	Exec           Primitive
}

// NonLiteralUpdateInfo stores the information required to process non-literal update queries.
// It stores 4 information-
// 1. CompExprCol- The index of the comparison expression in the select query to know if the row value is actually being changed or not.
// 2. UpdateExprCol- The index of the updated expression in the select query.
// 3. UpdateExprBvName- The bind variable name to store the updated expression into.
type NonLiteralUpdateInfo struct {
	CompExprCol      int
	UpdateExprCol    int
	UpdateExprBvName string
}

// FkCascade is a primitive that implements foreign key cascading using Selection as values required to execute the FkChild Primitives.
// On success, it executes the Parent Primitive.
type FkCascade struct {
	txNeeded

	// Selection is the Primitive that is used to find the rows that are going to be modified in the child tables.
	Selection Primitive
	// Children is a list of child foreign key Primitives that are executed using rows from the Selection Primitive.
	Children []*FkChild
	// Parent is the Primitive that is executed after the children are modified.
	Parent Primitive
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
		if len(child.NonLiteralInfo) > 0 {
			err = fkc.executeNonLiteralExprFkChild(ctx, vcursor, bindVars, wantfields, selectionRes, child)
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

func (fkc *FkCascade) executeLiteralExprFkChild(ctx context.Context, vcursor VCursor, in map[string]*querypb.BindVariable, wantfields bool, selectionRes *sqltypes.Result, child *FkChild, isStreaming bool) error {
	bindVars := maps.Clone(in)
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
	return nil
}

func (fkc *FkCascade) executeNonLiteralExprFkChild(ctx context.Context, vcursor VCursor, in map[string]*querypb.BindVariable, wantfields bool, selectionRes *sqltypes.Result, child *FkChild) error {
	// For each row in the SELECT we need to run the child primitive.
	for _, row := range selectionRes.Rows {
		bindVars := maps.Clone(in)
		// First we check if any of the columns is being updated at all.
		skipRow := true
		for _, info := range child.NonLiteralInfo {
			// We use a null-safe comparison, so the value is guaranteed to be not null.
			// We check if the column has updated or not.
			isUnchanged, err := row[info.CompExprCol].ToBool()
			if err != nil {
				return err
			}
			if !isUnchanged {
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
		for _, info := range child.NonLiteralInfo {
			bindVars[info.UpdateExprBvName] = sqltypes.ValueBindVariable(row[info.UpdateExprCol])
		}
		_, err := vcursor.ExecutePrimitive(ctx, child.Exec, bindVars, wantfields)
		if err != nil {
			return err
		}
	}
	return nil
}

// TryStreamExecute implements the Primitive interface.
func (fkc *FkCascade) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	res, err := fkc.TryExecute(ctx, vcursor, bindVars, wantfields)
	if err != nil {
		return err
	}
	return callback(res)
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
		if len(child.NonLiteralInfo) > 0 {
			childInfoMap["NonLiteralUpdateInfo"] = child.NonLiteralInfo
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
