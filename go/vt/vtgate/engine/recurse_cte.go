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
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
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

	// Distinct retains the term-execution cap until VTGate recursion deduplicates rows.
	Distinct bool
}

var _ Primitive = (*RecurseCTE)(nil)

// maxRecurseDepth caps expansion levels for UNION ALL. UNION DISTINCT retains
// the existing term-execution cap until recursive results are deduplicated.
// TODO: This should be controlled with the cte_max_recursion_depth system variable.
const maxRecurseDepth = 1000

func (r *RecurseCTE) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	res, err := vcursor.ExecutePrimitive(ctx, r.Seed, bindVars, wantfields)
	if err != nil {
		return nil, err
	}

	if vcursor.ExceedsMaxMemoryRows(len(res.Rows)) {
		return nil, vterrors.Errorf(vtrpcpb.Code_RESOURCE_EXHAUSTED, "in-memory row count exceeded allowed limit of %d", vcursor.MaxMemoryRows())
	}

	// recurseRows contains the rows used in the next recursion
	recurseRows := res.Rows
	joinVars := make(map[string]*querypb.BindVariable)
	iterations := 0
	for len(recurseRows) > 0 {
		if !r.Distinct {
			iterations++
			if iterations > maxRecurseDepth {
				return nil, vterrors.VT09030("")
			}
		}
		// copy over the results from the previous recursion
		theseRows := recurseRows
		recurseRows = nil
		for _, row := range theseRows {
			if r.Distinct {
				iterations++
				if iterations > maxRecurseDepth {
					return nil, vterrors.VT09030("")
				}
			}
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
			if vcursor.ExceedsMaxMemoryRows(len(res.Rows) + len(rresult.Rows)) {
				return nil, vterrors.Errorf(vtrpcpb.Code_RESOURCE_EXHAUSTED, "in-memory row count exceeded allowed limit of %d", vcursor.MaxMemoryRows())
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

	// A scatter source delivers its result chunks concurrently, one goroutine
	// per shard, so both the shared frontier and the downstream callback must be
	// serialized.
	var mu sync.Mutex

	// Keep the seed/next frontier and the current level within one row budget.
	// Latch callback errors so concurrent scatter chunks cannot resume delivery.
	var recurseRows []sqltypes.Row
	var currentLevelRows int
	var callbackErr error
	collect := func(result *sqltypes.Result) error {
		mu.Lock()
		defer mu.Unlock()
		if callbackErr != nil {
			return callbackErr
		}
		if callbackErr = ctx.Err(); callbackErr != nil {
			return callbackErr
		}
		if vcursor.ExceedsMaxMemoryRows(currentLevelRows + len(recurseRows) + len(result.Rows)) {
			callbackErr = vterrors.Errorf(vtrpcpb.Code_RESOURCE_EXHAUSTED, "in-memory row count exceeded allowed limit of %d", vcursor.MaxMemoryRows())
			return callbackErr
		}
		recurseRows = append(recurseRows, result.Rows...)
		callbackErr = callback(result)
		return callbackErr
	}
	err := vcursor.StreamExecutePrimitive(ctx, r.Seed, bindVars, wantfields, collect)
	if err != nil {
		return err
	}

	// Expand each recursion level iteratively, mirroring the buffered path. Only
	// one Term stream is active at a time: recursing inside the Term callback
	// would nest a live stream per level and exhaust the connection pool.
	joinVars := make(map[string]*querypb.BindVariable)
	iterations := 0
	for len(recurseRows) > 0 {
		if !r.Distinct {
			iterations++
			if iterations > maxRecurseDepth {
				return vterrors.VT09030("")
			}
		}
		// copy over the results from the previous recursion
		theseRows := recurseRows
		recurseRows = nil
		currentLevelRows = len(theseRows)
		for _, row := range theseRows {
			if r.Distinct {
				iterations++
				if iterations > maxRecurseDepth {
					return vterrors.VT09030("")
				}
			}
			for k, col := range r.Vars {
				joinVars[k] = sqltypes.ValueBindVariable(row[col])
			}
			// check if the context is done - we might be in a long running recursion
			if err := ctx.Err(); err != nil {
				return err
			}
			err := vcursor.StreamExecutePrimitive(ctx, r.Term, combineVars(bindVars, joinVars), false, collect)
			if err != nil {
				return err
			}
		}
	}
	return nil
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

	return PrimitiveDescription{
		OperatorType: "RecurseCTE",
		Other:        other,
		Inputs:       nil,
	}
}
