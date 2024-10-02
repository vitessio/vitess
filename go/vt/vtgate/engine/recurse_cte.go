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
	"vitess.io/vitess/go/vt/vterrors"
)

// RecurseCTE is used to represent recursive CTEs
// Seed is used to represent the non-recursive part that initializes the result set.
// It's result are then used to start the recursion on the Term side
// The values being sent to the Term side are stored in the Vars map -
// the key is the bindvar name and the value is the index of the column in the recursive result
type RecurseCTE struct {
	Seed, Term Primitive

	Vars map[string]int
}

var _ Primitive = (*RecurseCTE)(nil)

func (r *RecurseCTE) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	res, err := vcursor.ExecutePrimitive(ctx, r.Seed, bindVars, wantfields)
	if err != nil {
		return nil, err
	}

	// recurseRows contains the rows used in the next recursion
	recurseRows := res.Rows
	joinVars := make(map[string]*querypb.BindVariable)
	loops := 0
	for len(recurseRows) > 0 {
		// copy over the results from the previous recursion
		theseRows := recurseRows
		recurseRows = nil
		for _, row := range theseRows {
			for k, col := range r.Vars {
				joinVars[k] = sqltypes.ValueBindVariable(row[col])
			}
			// check if the context is done - we might be in a long running recursion
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			rresult, err := vcursor.ExecutePrimitive(ctx, r.Term, combineVars(bindVars, joinVars), false)
			if err != nil {
				return nil, err
			}
			recurseRows = append(recurseRows, rresult.Rows...)
			res.Rows = append(res.Rows, rresult.Rows...)
			loops++
			if loops > 1000 { // TODO: This should be controlled with a system variable setting
				return nil, vterrors.VT09030("")
			}
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
	return vcursor.StreamExecutePrimitive(ctx, r.Seed, bindVars, wantfields, func(result *sqltypes.Result) error {
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

		err := vcursor.StreamExecutePrimitive(ctx, r.Term, combineVars(bindvars, joinVars), false, func(result *sqltypes.Result) error {
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
	if r.Seed.GetKeyspaceName() == r.Term.GetKeyspaceName() {
		return r.Seed.GetKeyspaceName()
	}
	return r.Seed.GetKeyspaceName() + "_" + r.Term.GetKeyspaceName()
}

func (r *RecurseCTE) GetTableName() string {
	return r.Seed.GetTableName()
}

func (r *RecurseCTE) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return r.Seed.GetFields(ctx, vcursor, bindVars)
}

func (r *RecurseCTE) NeedsTransaction() bool {
	return r.Seed.NeedsTransaction() || r.Term.NeedsTransaction()
}

func (r *RecurseCTE) Inputs() ([]Primitive, []map[string]any) {
	return []Primitive{r.Seed, r.Term}, nil
}

func (r *RecurseCTE) description() PrimitiveDescription {
	other := map[string]interface{}{
		"JoinVars": orderedStringIntMap(r.Vars),
	}

	return PrimitiveDescription{
		OperatorType: "RecurseCTE",
		Other:        other,
		Inputs:       nil,
	}
}
