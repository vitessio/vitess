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

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

var _ Primitive = (*SemiJoin)(nil)

// SemiJoin specifies the parameters for a SemiJoin primitive.
type SemiJoin struct {
	// Left and Right are the LHS and RHS primitives
	// of the SemiJoin. They can be any primitive.
	Left, Right Primitive `json:",omitempty"`

	// Vars defines the list of SemiJoinVars that need to
	// be built from the LHS result before invoking
	// the RHS subquery.
	Vars map[string]int `json:",omitempty"`
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
	joinVars := make(map[string]*querypb.BindVariable)
	err := vcursor.StreamExecutePrimitive(ctx, jn.Left, bindVars, wantfields, func(lresult *sqltypes.Result) error {
		result := &sqltypes.Result{Fields: lresult.Fields}
		for _, lrow := range lresult.Rows {
			for k, col := range jn.Vars {
				joinVars[k] = sqltypes.ValueBindVariable(lrow[col])
			}
			rowAdded := false
			err := vcursor.StreamExecutePrimitive(ctx, jn.Right, combineVars(bindVars, joinVars), false, func(rresult *sqltypes.Result) error {
				if len(rresult.Rows) > 0 && !rowAdded {
					result.Rows = append(result.Rows, lrow)
					rowAdded = true
				}
				return nil
			})
			if err != nil {
				return err
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

// RouteType returns a description of the query routing type used by the primitive
func (jn *SemiJoin) RouteType() string {
	return "SemiJoin"
}

// GetKeyspaceName specifies the Keyspace that this primitive routes to.
func (jn *SemiJoin) GetKeyspaceName() string {
	if jn.Left.GetKeyspaceName() == jn.Right.GetKeyspaceName() {
		return jn.Left.GetKeyspaceName()
	}
	return jn.Left.GetKeyspaceName() + "_" + jn.Right.GetKeyspaceName()
}

// GetTableName specifies the table that this primitive routes to.
func (jn *SemiJoin) GetTableName() string {
	return jn.Left.GetTableName() + "_" + jn.Right.GetTableName()
}

// NeedsTransaction implements the Primitive interface
func (jn *SemiJoin) NeedsTransaction() bool {
	return jn.Right.NeedsTransaction() || jn.Left.NeedsTransaction()
}

func (jn *SemiJoin) description() PrimitiveDescription {
	other := map[string]any{
		"TableName": jn.GetTableName(),
	}
	if len(jn.Vars) > 0 {
		other["JoinVars"] = orderedStringIntMap(jn.Vars)
	}
	return PrimitiveDescription{
		OperatorType: "SemiJoin",
		Other:        other,
	}
}
