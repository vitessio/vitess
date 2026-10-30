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
	"math"
	"strconv"
	"strings"
	"sync"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sysvars"
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

// defaultMaxRecurseDepth is the number of recursion iterations after which we
// abort when the session has not set cte_max_recursion_depth. It matches the
// MySQL default for that system variable.
const defaultMaxRecurseDepth = 1000

// maxRecurseDepth returns the recursion-depth limit for this session: the value
// of the cte_max_recursion_depth session system variable when it has been set,
// and the MySQL default otherwise.
func maxRecurseDepth(vcursor VCursor) uint64 {
	limit := uint64(defaultMaxRecurseDepth)
	session := vcursor.Session()
	if !session.HasSystemVariables() {
		return limit
	}
	session.GetSystemVariables(func(k string, v string) {
		if k != sysvars.CTEMaxRecursionDepth {
			return
		}
		if parsed, ok := parseRecurseDepth(v); ok {
			limit = parsed
		}
	})
	return limit
}

// maxRecurseDepthLimit is the largest value MySQL accepts for
// cte_max_recursion_depth. Larger values are clamped to it.
const maxRecurseDepthLimit = math.MaxUint32

// parseRecurseDepth parses the stored value of cte_max_recursion_depth. The
// session stores system variable values as they were assigned, in their
// SQL-encoded form, so a string literal is still wrapped in quotes here and an
// out-of-range number has not been clamped yet. Like MySQL, values above
// 4294967295 become 4294967295 and negative values become 0.
func parseRecurseDepth(v string) (uint64, bool) {
	v = strings.Trim(strings.TrimSpace(v), "'\"")
	if depth, err := strconv.ParseUint(v, 10, 64); err == nil {
		return min(depth, maxRecurseDepthLimit), true
	}
	if depth, err := strconv.ParseInt(v, 10, 64); err == nil && depth < 0 {
		return 0, true
	}
	return 0, false
}

// termReturnsNoRows reports whether the recursive term was planned as a route
// that never returns rows, like a recursive query block with WHERE false.
// MySQL does not execute such a block, so it never counts an iteration.
func termReturnsNoRows(term Primitive) bool {
	route, ok := term.(*Route)
	return ok && route.RoutingParameters != nil && route.Opcode == None
}

// exceedsMaxMemoryRows returns an error when the rows that the recursion keeps
// in memory are over the max-memory-rows limit. Counting levels instead of Term
// calls means a recursion whose frontier grows with every level is not stopped
// by the depth limit until it has kept far more rows than any single Term
// result, so the rows it keeps are checked on their own.
func exceedsMaxMemoryRows(vcursor VCursor, numRows int) error {
	if vcursor.ExceedsMaxMemoryRows(numRows) {
		return vterrors.NewErrorf(vtrpcpb.Code_RESOURCE_EXHAUSTED, vterrors.NetPacketTooLarge, "in-memory row count exceeded allowed limit of %d", vcursor.MaxMemoryRows())
	}
	return nil
}

func (r *RecurseCTE) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	res, err := vcursor.ExecutePrimitive(ctx, r.Seed, bindVars, wantfields)
	if err != nil {
		return nil, err
	}

	if termReturnsNoRows(r.Term) {
		return res, nil
	}

	// recurseRows contains the rows used in the next recursion
	recurseRows := res.Rows
	joinVars := make(map[string]*querypb.BindVariable)
	maxDepth := maxRecurseDepth(vcursor)
	var iterations uint64
	for len(recurseRows) > 0 {
		// Like MySQL, one iteration is one recursion level: the whole frontier
		// produced by the previous level, however many rows it holds.
		iterations++
		if iterations > maxDepth {
			return nil, vterrors.VT09030(iterations)
		}
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
			if err := exceedsMaxMemoryRows(vcursor, len(res.Rows)); err != nil {
				return nil, err
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

	// A scatter source delivers its result chunks concurrently, one goroutine
	// per shard, so both the shared frontier and the downstream callback must be
	// serialized.
	var mu sync.Mutex

	// Stream the seed, emitting its rows and collecting them to drive the first
	// recursion level.
	var recurseRows []sqltypes.Row
	err := vcursor.StreamExecutePrimitive(ctx, r.Seed, bindVars, wantfields, func(result *sqltypes.Result) error {
		mu.Lock()
		defer mu.Unlock()
		recurseRows = append(recurseRows, result.Rows...)
		if err := exceedsMaxMemoryRows(vcursor, len(recurseRows)); err != nil {
			return err
		}
		return callback(result)
	})
	if err != nil {
		return err
	}
	if termReturnsNoRows(r.Term) {
		return nil
	}

	// Expand each recursion level iteratively, mirroring the buffered path. Only
	// one Term stream is active at a time: recursing inside the Term callback
	// would nest a live stream per level and exhaust the connection pool.
	joinVars := make(map[string]*querypb.BindVariable)
	maxDepth := maxRecurseDepth(vcursor)
	var iterations uint64
	for len(recurseRows) > 0 {
		// Like MySQL, one iteration is one recursion level: the whole frontier
		// produced by the previous level, however many rows it holds. Streamed
		// rows cannot be unsent, so the guard fires before the level starts.
		iterations++
		if iterations > maxDepth {
			return vterrors.VT09030(iterations)
		}
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
			err := vcursor.StreamExecutePrimitive(ctx, r.Term, combineVars(bindVars, joinVars), false, func(result *sqltypes.Result) error {
				mu.Lock()
				defer mu.Unlock()
				recurseRows = append(recurseRows, result.Rows...)
				if err := exceedsMaxMemoryRows(vcursor, len(recurseRows)); err != nil {
					return err
				}
				return callback(result)
			})
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
