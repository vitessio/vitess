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
	"slices"
	"strings"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
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
	// ParentKeyUnique is true when the foreign key's parent columns contain a primary or unique key.
	// Only then are the child updates of a non-literal update ordered by key dependencies; see
	// nonLiteralUpdateOrder.
	ParentKeyUnique bool
	// ColTypes are the types of the foreign key's parent columns, in the order of Cols.
	// They are used to compare key values when ordering the child updates of a non-literal update.
	ColTypes []evalengine.Type
	Exec     Primitive
}

// NonLiteralUpdateInfo stores the information required to process non-literal update queries.
// It stores 4 information-
// 1. CompExprCol- The index of the comparison expression in the select query to know if the row value is actually being changed or not.
// 2. UpdateExprCol- The index of the updated expression in the select query.
// 3. UpdateExprBvName- The bind variable name to store the updated expression into.
// 4. FkColIdx- The position of the updated column among the foreign key's columns.
type NonLiteralUpdateInfo struct {
	CompExprCol      int
	UpdateExprCol    int
	UpdateExprBvName string
	FkColIdx         int
}

// FkCascade is a primitive that implements foreign key cascading using Selection as values required to execute the FkChild Primitives.
// On success, it executes the Parent Primitive.
type FkCascade struct {
	txNeeded
	noFields

	// Selection is the Primitive that is used to find the rows that are going to be modified in the child tables.
	Selection Primitive
	// Children is a list of child foreign key Primitives that are executed using rows from the Selection Primitive.
	Children []*FkChild
	// Parent is the Primitive that is executed after the children are modified.
	Parent Primitive
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
		tupleValues := make([]sqltypes.Value, 0, len(child.Cols))
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
	order, err := nonLiteralUpdateOrder(vcursor, selectionRes.Rows, child)
	if err != nil {
		return err
	}
	for _, rowIdx := range order {
		row := selectionRes.Rows[rowIdx]
		bindVars := maps.Clone(in)
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

// nonLiteralUpdateOrder returns the indexes of the selected rows whose foreign key value changes,
// in the order their child updates must run.
//
// A child update finds the child rows by the parent's old key value: WHERE (cols) IN ((:old)).
// If row a changes its key to the old key of row b and a's child update runs first, b's child
// update also matches the child rows that a's update just moved. MySQL updates the parent rows
// one at a time and checks the unique key after each one, so its UPDATE succeeds only if b gives
// up its key before a takes it. Running the child updates in that order moves every child row
// once, as MySQL's own cascade does. If the key changes form a cycle, no such order exists and
// MySQL fails the parent update with a duplicate key error, so we return that error before
// running any of this child's updates. vtgate rolls back the writes of the other children.
//
// This holds only when the parent key is unique. Without a unique key, MySQL's own cascade
// matches the children again in the order it updates the rows, so we keep the Selection order.
func nonLiteralUpdateOrder(vcursor VCursor, rows []sqltypes.Row, child *FkChild) ([]int, error) {
	var changed []int
	var oldKeys, newKeys [][]sqltypes.Value
	for rowIdx, row := range rows {
		isChanged := false
		for _, info := range child.NonLiteralInfo {
			// We use a null-safe comparison, so the value is guaranteed to be not null.
			isUnchanged, err := row[info.CompExprCol].ToBool()
			if err != nil {
				return nil, err
			}
			if !isUnchanged {
				isChanged = true
				break
			}
		}
		// If none of the columns have changed, then there is no update to cascade.
		if !isChanged {
			continue
		}
		oldKey := make([]sqltypes.Value, len(child.Cols))
		for i, colIdx := range child.Cols {
			oldKey[i] = row[colIdx]
		}
		newKey := slices.Clone(oldKey)
		for _, info := range child.NonLiteralInfo {
			newKey[info.FkColIdx] = row[info.UpdateExprCol]
		}
		changed = append(changed, rowIdx)
		oldKeys = append(oldKeys, oldKey)
		newKeys = append(newKeys, newKey)
	}
	if !child.ParentKeyUnique || len(changed) < 2 {
		return changed, nil
	}

	cmp := keyComparer(vcursor, child.ColTypes)

	// Sort the changed rows by their old key, so that we can look up the rows whose old key
	// equals another row's new key.
	byOldKey := make([]int, len(changed))
	for i := range byOldKey {
		byOldKey[i] = i
	}
	var cmpErr error
	slices.SortStableFunc(byOldKey, func(a, b int) int {
		c, err := cmp(oldKeys[a], oldKeys[b])
		if err != nil && cmpErr == nil {
			cmpErr = err
		}
		return c
	})
	if cmpErr != nil {
		return nil, cmpErr
	}

	// next[b] lists the rows that must run after row b, because they take b's old key.
	next := make([][]int, len(changed))
	waitingFor := make([]int, len(changed))
	for a, newKey := range newKeys {
		// A key that contains NULL matches no child rows.
		if slices.ContainsFunc(newKey, sqltypes.Value.IsNull) {
			continue
		}
		start, _ := slices.BinarySearchFunc(byOldKey, newKey, func(idx int, key []sqltypes.Value) int {
			c, err := cmp(oldKeys[idx], key)
			if err != nil && cmpErr == nil {
				cmpErr = err
			}
			return c
		})
		for _, b := range byOldKey[start:] {
			c, err := cmp(oldKeys[b], newKey)
			if err != nil {
				return nil, err
			}
			if c != 0 {
				break
			}
			if b != a {
				next[b] = append(next[b], a)
				waitingFor[a]++
			}
		}
	}
	if cmpErr != nil {
		return nil, cmpErr
	}

	// Kahn's algorithm, keeping the Selection order among rows that are ready.
	order := make([]int, 0, len(changed))
	var ready []int
	for i := range changed {
		if waitingFor[i] == 0 {
			ready = append(ready, i)
		}
	}
	for len(ready) > 0 {
		b := ready[0]
		ready = ready[1:]
		order = append(order, changed[b])
		for _, a := range next[b] {
			waitingFor[a]--
			if waitingFor[a] == 0 {
				ready = append(ready, a)
			}
		}
	}
	if len(order) < len(changed) {
		for i := range changed {
			if waitingFor[i] > 0 {
				return nil, vterrors.NewErrorf(vtrpcpb.Code_ALREADY_EXISTS, vterrors.DupEntry,
					"Duplicate entry '%s' for key: the update moves foreign key parent values in a cycle", formatKey(newKeys[i]))
			}
		}
	}
	return order, nil
}

// keyComparer returns a function that orders foreign key values column by column, comparing
// each column with its collation, as the child update's WHERE (cols) IN (...) clause does.
// NULL sorts first and equals NULL, so callers must not treat keys that contain NULL as equal.
func keyComparer(vcursor VCursor, colTypes []evalengine.Type) func(a, b []sqltypes.Value) (int, error) {
	collationEnv := vcursor.Environment().CollationEnv()
	connCollation := vcursor.ConnCollation()
	return func(a, b []sqltypes.Value) (int, error) {
		for i := range a {
			coll := connCollation
			var values *evalengine.EnumSetValues
			if i < len(colTypes) && colTypes[i].Valid() {
				if c := colTypes[i].Collation(); c != collations.Unknown {
					coll = c
				}
				values = colTypes[i].Values()
			}
			c, err := evalengine.NullsafeCompare(a[i], b[i], collationEnv, coll, values)
			if err != nil {
				return 0, err
			}
			if c != 0 {
				return c, nil
			}
		}
		return 0, nil
	}
}

func formatKey(key []sqltypes.Value) string {
	parts := make([]string, len(key))
	for i, v := range key {
		parts[i] = v.ToString()
	}
	return strings.Join(parts, "-")
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
	inputs := make([]Primitive, 0, len(fkc.Children)+2)
	inputsMap := make([]map[string]any, 0, len(fkc.Children)+2)
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
			childInfoMap["ParentKeyUnique"] = child.ParentKeyUnique
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
	return PrimitiveDescription{OperatorType: "FkCascade"}
}

var _ Primitive = (*FkCascade)(nil)
