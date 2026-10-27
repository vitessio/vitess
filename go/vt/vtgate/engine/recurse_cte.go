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
	"sync"

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

	// CheckCols is set for UNION DISTINCT, to discard rows already produced.
	CheckCols []CheckCol
}

var _ Primitive = (*RecurseCTE)(nil)

// maxRecurseDepth caps the number of recursion iterations before we abort.
// TODO: This should be controlled with the cte_max_recursion_depth system variable.
const maxRecurseDepth = 1000

func (r *RecurseCTE) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	res, err := vcursor.ExecutePrimitive(ctx, r.Seed, bindVars, wantfields)
	if err != nil {
		return nil, err
	}

	pt := r.newProbeTable(vcursor)
	res.Rows, err = pt.filter(res.Rows)
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
			loops++
			if loops > maxRecurseDepth {
				return nil, vterrors.VT09030("")
			}
			rresult, err := vcursor.ExecutePrimitive(ctx, r.Term, combineVars(bindVars, joinVars), false)
			if err != nil {
				return nil, err
			}
			newRows, err := pt.filter(rresult.Rows)
			if err != nil {
				return nil, err
			}
			recurseRows = append(recurseRows, newRows...)
			res.Rows = append(res.Rows, newRows...)
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

	// A scatter source delivers its result chunks concurrently, one goroutine
	// per shard, so both the shared frontier and the downstream callback must be
	// serialized.
	var mu sync.Mutex
	var recurseRows []sqltypes.Row
	pt := r.newProbeTable(vcursor)
	emit := func(result *sqltypes.Result) error {
		mu.Lock()
		defer mu.Unlock()
		if pt != nil {
			rows, err := pt.filter(result.Rows)
			if err != nil {
				return err
			}
			result = &sqltypes.Result{Fields: result.Fields, Rows: rows}
		}
		recurseRows = append(recurseRows, result.Rows...)
		return callback(result)
	}

	// Stream the seed, emitting its rows and collecting them to drive the first
	// recursion level.
	err := vcursor.StreamExecutePrimitive(ctx, r.Seed, bindVars, wantfields, emit)
	if err != nil {
		return err
	}

	// Expand each recursion level iteratively, mirroring the buffered path. Only
	// one Term stream is active at a time: recursing inside the Term callback
	// would nest a live stream per level and exhaust the connection pool.
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
				return err
			}
			loops++
			if loops > maxRecurseDepth {
				return vterrors.VT09030("")
			}
			err := vcursor.StreamExecutePrimitive(ctx, r.Term, combineVars(bindVars, joinVars), false, emit)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// newProbeTable returns nil for UNION ALL.
func (r *RecurseCTE) newProbeTable(vcursor VCursor) *probeTable {
	if len(r.CheckCols) == 0 {
		return nil
	}
	return newProbeTable(r.CheckCols, vcursor.Environment().CollationEnv())
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
	other := map[string]any{
		"JoinVars": orderedStringIntMap(r.Vars),
	}
	if len(r.CheckCols) > 0 {
		colls := make([]string, 0, len(r.CheckCols))
		for _, checkCol := range r.CheckCols {
			colls = append(colls, checkCol.String())
		}
		other["Collations"] = colls
	}

	return PrimitiveDescription{
		OperatorType: "RecurseCTE",
		Other:        other,
		Inputs:       nil,
	}
}
