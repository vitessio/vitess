/*
Copyright 2021 The Vitess Authors.

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
	"sync/atomic"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

var _ Primitive = (*SemiJoin)(nil)

// SemiJoin specifies the parameters for a SemiJoin primitive.
type SemiJoin struct {
	// Left and Right are the LHS and RHS primitives
	// of the SemiJoin. They can be any primitive.
	Left, Right Primitive

	// Vars defines the list of SemiJoinVars that need to
	// be built from the LHS result before invoking
	// the RHS subquery.
	Vars map[string]int
}

// TryExecute performs a non-streaming exec.
func (jn *SemiJoin) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	joinVars := make(map[string]*querypb.BindVariable)
	lresult, err := vcursor.ExecutePrimitive(ctx, jn.Left, bindVars, wantfields)
	if err != nil {
		return nil, err
	}
	result := &sqltypes.Result{Fields: lresult.Fields}
	for _, lrow := range lresult.Rows {
		for k, col := range jn.Vars {
			joinVars[k] = sqltypes.ValueBindVariable(lrow[col])
		}
		rresult, err := vcursor.ExecutePrimitive(ctx, jn.Right, combineVars(bindVars, joinVars), false)
		if err != nil {
			return nil, err
		}
		if len(rresult.Rows) > 0 {
			result.Rows = append(result.Rows, lrow)
		}
	}
	return result, nil
}

// TryStreamExecute performs a streaming exec.
func (jn *SemiJoin) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	err := vcursor.StreamExecutePrimitive(ctx, jn.Left, bindVars, wantfields, func(lresult *sqltypes.Result) error {
		joinVars := make(map[string]*querypb.BindVariable)
		result := &sqltypes.Result{Fields: lresult.Fields}
		for _, lrow := range lresult.Rows {
			for k, col := range jn.Vars {
				joinVars[k] = sqltypes.ValueBindVariable(lrow[col])
			}
			var rowAdded atomic.Bool
			err := vcursor.StreamExecutePrimitive(ctx, jn.Right, combineVars(bindVars, joinVars), false, func(rresult *sqltypes.Result) error {
				if len(rresult.Rows) > 0 {
					rowAdded.Store(true)
				}
				return nil
			})
			if err != nil {
				return err
			}
			if rowAdded.Load() {
				result.Rows = append(result.Rows, lrow)
			}
		}
		return callback(result)
	})
	return err
}

// GetFields fetches the field info.
func (jn *SemiJoin) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return jn.Left.GetFields(ctx, vcursor, bindVars)
}

// Inputs returns the input primitives for this SemiJoin
func (jn *SemiJoin) Inputs() ([]Primitive, []map[string]any) {
	return []Primitive{jn.Left, jn.Right}, []map[string]any{{
		inputName: "Outer",
	}, {
		inputName: "SubQuery",
	}}
}

// NeedsTransaction implements the Primitive interface
func (jn *SemiJoin) NeedsTransaction() bool {
	return jn.Right.NeedsTransaction() || jn.Left.NeedsTransaction()
}

func (jn *SemiJoin) description() PrimitiveDescription {
	other := map[string]any{}
	if len(jn.Vars) > 0 {
		other["JoinVars"] = orderedStringIntMap(jn.Vars)
	}
	return PrimitiveDescription{
		OperatorType: "SemiJoin",
		Other:        other,
	}
}
