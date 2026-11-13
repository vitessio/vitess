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
	"strconv"
	"sync"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
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
	// TermCheckCols is the same for the term, where a weight string can be at another offset.
	CheckCols     []CheckCol
	TermCheckCols []CheckCol

	// Truncate drops the weight strings that were only needed for the deduplication.
	Truncate int

	// TermFromCTE maps the term's columns that only pass on a column of the previous row to it.
	// Each of them has a weight string column in TermCheckCols.
	TermFromCTE map[int]int
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

	seed, term := r.newProbeTables(vcursor)
	sqlmode := evalengine.ParseSQLMode(vcursor.SQLMode())
	res.Rows, err = seed.filter(res.Rows)
	if err != nil {
		return nil, err
	}

	// recurseRows contains the rows used in the next recursion
	recurseRows := res.Rows
	joinVars := make(map[string]*querypb.BindVariable)
	loops := 0
	seedLevel := true
	for len(recurseRows) > 0 {
		// copy over the results from the previous recursion
		theseRows := recurseRows
		recurseRows = nil
		parentsFromSeed := seedLevel
		seedLevel = false
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
			r.copyWeightStrings(rresult.Rows, row, parentsFromSeed)
			if err := r.store(rresult.Rows, sqlmode); err != nil {
				return nil, err
			}
			newRows, err := term.filter(rresult.Rows)
			if err != nil {
				return nil, err
			}
			recurseRows = append(recurseRows, newRows...)
			res.Rows = append(res.Rows, newRows...)
		}
	}
	return r.truncate(res), nil
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
	seed, term := r.newProbeTables(vcursor)
	sqlmode := evalengine.ParseSQLMode(vcursor.SQLMode())
	// the row that the Term is running for
	var parent sqltypes.Row
	var parentIsSeed bool
	emitFor := func(pt *probeTable, isTerm bool) func(result *sqltypes.Result) error {
		return func(result *sqltypes.Result) error {
			mu.Lock()
			defer mu.Unlock()
			if isTerm {
				r.copyWeightStrings(result.Rows, parent, parentIsSeed)
				if err := r.store(result.Rows, sqlmode); err != nil {
					return err
				}
			}
			if pt != nil {
				rows, err := pt.filter(result.Rows)
				if err != nil {
					return err
				}
				result = &sqltypes.Result{Fields: result.Fields, Rows: rows}
			}
			recurseRows = append(recurseRows, result.Rows...)
			return callback(r.truncate(result))
		}
	}

	// Stream the seed, emitting its rows and collecting them to drive the first
	// recursion level.
	err := vcursor.StreamExecutePrimitive(ctx, r.Seed, bindVars, wantfields, emitFor(seed, false))
	if err != nil {
		return err
	}

	// Expand each recursion level iteratively, mirroring the buffered path. Only
	// one Term stream is active at a time: recursing inside the Term callback
	// would nest a live stream per level and exhaust the connection pool.
	joinVars := make(map[string]*querypb.BindVariable)
	loops := 0
	seedLevel := true
	for len(recurseRows) > 0 {
		// copy over the results from the previous recursion
		theseRows := recurseRows
		recurseRows = nil
		parentsFromSeed := seedLevel
		seedLevel = false
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
			mu.Lock()
			parent, parentIsSeed = row, parentsFromSeed
			mu.Unlock()
			err := vcursor.StreamExecutePrimitive(ctx, r.Term, combineVars(bindVars, joinVars), false, emitFor(term, true))
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// newProbeTables returns the tables for the seed and the term, which share the rows seen. Both are nil for UNION ALL.
func (r *RecurseCTE) newProbeTables(vcursor VCursor) (seed, term *probeTable) {
	if len(r.CheckCols) == 0 {
		return nil, nil
	}
	collationEnv := vcursor.Environment().CollationEnv()
	seed = newProbeTable(r.CheckCols, collationEnv)
	term = newProbeTable(r.TermCheckCols, collationEnv)
	term.seenRows = seed.seenRows
	return seed, term
}

// copyWeightStrings takes the weight string of the term's pass-through columns from the parent row,
// as weight_string(:cte_col) would use the connection's collation instead of the column's.
// A parent column without a weight string passes on its value, to be compared by its bytes.
func (r *RecurseCTE) copyWeightStrings(rows []sqltypes.Row, parent sqltypes.Row, parentIsSeed bool) {
	parentCols := r.TermCheckCols
	if parentIsSeed {
		parentCols = r.CheckCols
	}
	for termCol, cteCol := range r.TermFromCTE {
		termWs := wsColFor(r.TermCheckCols, termCol)
		from := wsColFor(parentCols, cteCol)
		if from < 0 {
			from = cteCol
		}
		for _, row := range rows {
			row[termWs] = parent[from]
		}
	}
}

func wsColFor(checkCols []CheckCol, col int) int {
	for _, checkCol := range checkCols {
		if checkCol.Col == col && checkCol.WsCol != nil {
			return *checkCol.WsCol
		}
	}
	return -1
}

// store converts the term's values to the type of the CTE's columns, as MySQL stores them,
// so 1.5 becomes 2 under an integer column, and a value that MySQL rejects is an error
func (r *RecurseCTE) store(rows []sqltypes.Row, sqlmode evalengine.SQLMode) error {
	for _, checkCol := range r.TermCheckCols {
		// sqltypes.Unknown has all the flag bits set, so it also looks integral
		if typ := checkCol.Type.Type(); typ == sqltypes.Unknown || !sqltypes.IsIntegral(typ) {
			continue
		}
		for _, row := range rows {
			if v := row[checkCol.Col]; !v.IsNull() && !v.IsIntegral() {
				stored, err := evalengine.CastTo(v, checkCol.Type, sqlmode)
				if err != nil {
					return err
				}
				row[checkCol.Col] = stored
			}
		}
	}
	return nil
}

// truncate drops the columns that were only needed for the deduplication
func (r *RecurseCTE) truncate(res *sqltypes.Result) *sqltypes.Result {
	if r.Truncate == 0 {
		return res
	}
	return res.Truncate(r.Truncate)
}

func (r *RecurseCTE) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	res, err := r.Seed.GetFields(ctx, vcursor, bindVars)
	if err != nil {
		return nil, err
	}
	return r.truncate(res), nil
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
		other["Collations"] = checkColStrings(r.CheckCols)
	}
	if len(r.TermCheckCols) > 0 {
		other["TermCollations"] = checkColStrings(r.TermCheckCols)
	}
	if r.Truncate > 0 {
		other["ResultColumns"] = r.Truncate
	}
	if len(r.TermFromCTE) > 0 {
		fromCTE := make(map[string]int, len(r.TermFromCTE))
		for termCol, cteCol := range r.TermFromCTE {
			fromCTE[strconv.Itoa(termCol)] = cteCol
		}
		other["TermFromCTE"] = orderedStringIntMap(fromCTE)
	}

	return PrimitiveDescription{
		OperatorType: "RecurseCTE",
		Other:        other,
		Inputs:       nil,
	}
}
