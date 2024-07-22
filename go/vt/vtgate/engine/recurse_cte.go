/*
Copyright 2024 The Vitess Authors.

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

// RecurseCTE is used to represent recursive CTEs
// Init is used to represent the initial query.
// It's result are then used to start the recursion on the RecurseCTE side
// The values being sent to the RecurseCTE side are stored in the Vars map -
// the key is the bindvar name and the value is the index of the column in the recursive result
type RecurseCTE struct {
	Init, Recurse Primitive

	Vars map[string]int
}

var _ Primitive = (*RecurseCTE)(nil)

func (r *RecurseCTE) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	res, err := vcursor.ExecutePrimitive(ctx, r.Init, bindVars, wantfields)
	if err != nil {
		return nil, err
	}

	// recurseRows contains the rows used in the next recursion
	recurseRows := res.Rows
	joinVars := make(map[string]*querypb.BindVariable)
	for len(recurseRows) > 0 {
		// copy over the results from the previous recursion
		theseRows := recurseRows
		recurseRows = nil
		for _, row := range theseRows {
			for k, col := range r.Vars {
				joinVars[k] = sqltypes.ValueBindVariable(row[col])
			}
			rresult, err := vcursor.ExecutePrimitive(ctx, r.Recurse, combineVars(bindVars, joinVars), false)
			if err != nil {
				return nil, err
			}
			recurseRows = append(recurseRows, rresult.Rows...)
			res.Rows = append(res.Rows, rresult.Rows...)
		}
	}
	return res, nil
}

func (r *RecurseCTE) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	if vcursor.Session().InTransaction() {
		res, err := r.TryExecute(ctx, vcursor, bindVars, wantfields)
		if err != nil {
			return err
		}
		return callback(res)
	}
	return vcursor.StreamExecutePrimitive(ctx, r.Init, bindVars, wantfields, func(result *sqltypes.Result) error {
		err := callback(result)
		if err != nil {
			return err
		}
		return r.recurse(ctx, vcursor, bindVars, result, callback)
	})
}

func (r *RecurseCTE) recurse(ctx context.Context, vcursor VCursor, bindvars map[string]*querypb.BindVariable, result *sqltypes.Result, callback func(*sqltypes.Result) error) error {
	if len(result.Rows) == 0 {
		return nil
	}
	joinVars := make(map[string]*querypb.BindVariable)
	for _, row := range result.Rows {
		for k, col := range r.Vars {
			joinVars[k] = sqltypes.ValueBindVariable(row[col])
		}

		err := vcursor.StreamExecutePrimitive(ctx, r.Recurse, combineVars(bindvars, joinVars), false, func(result *sqltypes.Result) error {
			err := callback(result)
			if err != nil {
				return err
			}
			return r.recurse(ctx, vcursor, bindvars, result, callback)
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *RecurseCTE) RouteType() string {
	return "RecurseCTE"
}

func (r *RecurseCTE) GetKeyspaceName() string {
	if r.Init.GetKeyspaceName() == r.Recurse.GetKeyspaceName() {
		return r.Init.GetKeyspaceName()
	}
	return r.Init.GetKeyspaceName() + "_" + r.Recurse.GetKeyspaceName()
}

func (r *RecurseCTE) GetTableName() string {
	return r.Init.GetTableName()
}

func (r *RecurseCTE) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return r.Init.GetFields(ctx, vcursor, bindVars)
}

func (r *RecurseCTE) NeedsTransaction() bool {
	return false
}

func (r *RecurseCTE) Inputs() ([]Primitive, []map[string]any) {
	return []Primitive{r.Init, r.Recurse}, nil
}

func (r *RecurseCTE) description() PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType: "RecurseCTE",
	}
}
