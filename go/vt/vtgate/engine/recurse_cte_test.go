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
	"errors"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

func TestRecurseDualQuery(t *testing.T) {
	// Test that the RecurseCTE primitive works as expected.
	// The test is testing something like this:
	// WITH RECURSIVE cte AS (SELECT 1 as col1 UNION SELECT col1+1 FROM cte WHERE col1 < 5) SELECT * FROM cte;
	leftPrim := &fakePrimitive{
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"col1",
					"int64",
				),
				"1",
			),
		},
	}
	rightFields := sqltypes.MakeTestFields(
		"col4",
		"int64",
	)

	rightPrim := &fakePrimitive{
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(
				rightFields,
				"2",
			),
			sqltypes.MakeTestResult(
				rightFields,
				"3",
			),
			sqltypes.MakeTestResult(
				rightFields,
				"4",
			), sqltypes.MakeTestResult(
				rightFields,
			),
		},
	}
	bv := map[string]*querypb.BindVariable{}

	cte := &RecurseCTE{
		Seed: leftPrim,
		Term: rightPrim,
		Vars: map[string]int{"col1": 0},
	}

	r, err := cte.TryExecute(t.Context(), &noopVCursor{}, bv, true)
	require.NoError(t, err)

	rightPrim.ExpectLog(t, []string{
		fmt.Sprintf(`Execute col1: %v false`, sqltypes.Int64BindVariable(1)),
		fmt.Sprintf(`Execute col1: %v false`, sqltypes.Int64BindVariable(2)),
		fmt.Sprintf(`Execute col1: %v false`, sqltypes.Int64BindVariable(3)),
		fmt.Sprintf(`Execute col1: %v false`, sqltypes.Int64BindVariable(4)),
	})

	wantRes := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col1",
			"int64",
		),
		"1",
		"2",
		"3",
		"4",
	)
	expectResult(t, r, wantRes)

	// testing the streaming mode.

	leftPrim.rewind()
	rightPrim.rewind()

	r, err = wrapStreamExecute(cte, &noopVCursor{}, bv, true)
	require.NoError(t, err)

	rightPrim.ExpectLog(t, []string{
		fmt.Sprintf(`StreamExecute col1: %v false`, sqltypes.Int64BindVariable(1)),
		fmt.Sprintf(`StreamExecute col1: %v false`, sqltypes.Int64BindVariable(2)),
		fmt.Sprintf(`StreamExecute col1: %v false`, sqltypes.Int64BindVariable(3)),
		fmt.Sprintf(`StreamExecute col1: %v false`, sqltypes.Int64BindVariable(4)),
	})
	expectResult(t, r, wantRes)

	// testing the streaming mode with transaction

	leftPrim.rewind()
	rightPrim.rewind()

	r, err = wrapStreamExecute(cte, &noopVCursor{inTx: true}, bv, true)
	require.NoError(t, err)

	rightPrim.ExpectLog(t, []string{
		fmt.Sprintf(`Execute col1: %v false`, sqltypes.Int64BindVariable(1)),
		fmt.Sprintf(`Execute col1: %v false`, sqltypes.Int64BindVariable(2)),
		fmt.Sprintf(`Execute col1: %v false`, sqltypes.Int64BindVariable(3)),
		fmt.Sprintf(`Execute col1: %v false`, sqltypes.Int64BindVariable(4)),
	})
	expectResult(t, r, wantRes)
}

// TestRecurseCTERecursionLimit verifies that the recursion-depth guard is
// enforced on both the buffered and streaming execution paths. The buffered
// path aborts after 1000 iterations with VT09030 ("Recursive query aborted
// after 1000 iterations."); the streaming path must enforce the same guard
// rather than recursing unbounded.
func TestRecurseCTERecursionLimit(t *testing.T) {
	fields := sqltypes.MakeTestFields("col1", "int64")

	// The Term keeps returning a single row well past the 1000-iteration guard,
	// so an unguarded recursion would run all the way to the end of the results.
	const iterations = 1100
	results := make([]*sqltypes.Result, 0, iterations)
	for i := range iterations {
		results = append(results, sqltypes.MakeTestResult(fields, strconv.Itoa(i+2)))
	}

	seed := &fakePrimitive{
		results: []*sqltypes.Result{sqltypes.MakeTestResult(fields, "1")},
	}
	term := &fakePrimitive{results: results, noLog: true}

	cte := &RecurseCTE{
		Seed: seed,
		Term: term,
		Vars: map[string]int{"col1": 0},
	}
	bv := map[string]*querypb.BindVariable{}
	cursor := &recurseMemoryVCursor{noopVCursor: &noopVCursor{}, limit: 2*maxRecurseDepth + 2}

	// Buffered path aborts at the guard.
	_, err := cte.TryExecute(t.Context(), cursor, bv, true)
	require.ErrorContains(t, err, "Recursive query aborted")

	// Streaming path must enforce the same guard. Streamed rows cannot be
	// unsent, so the guard must fire before the over-limit Term stream starts:
	// the client receives the seed row plus one row per allowed iteration, and
	// nothing from iteration 1001.
	seed.rewind()
	term.rewind()
	var streamed []sqltypes.Row
	err = cte.TryStreamExecute(t.Context(), cursor, bv, true, func(r *sqltypes.Result) error {
		streamed = append(streamed, r.Rows...)
		return nil
	})
	require.ErrorContains(t, err, "Recursive query aborted")
	require.Len(t, streamed, 1001)
}

// Cyclic UNION DISTINCT recursion must retain the existing cap on term
// executions, since VTGate does not yet deduplicate recursive results.
func TestRecurseCTEDistinctCycleCapsTermExecutions(t *testing.T) {
	fields := sqltypes.MakeTestFields("id", "int64")
	for _, seedRows := range []int{100, 1000} {
		t.Run(strconv.Itoa(seedRows), func(t *testing.T) {
			for _, mode := range []string{"execute", "stream", "transaction stream"} {
				t.Run(mode, func(t *testing.T) {
					seedResult := &sqltypes.Result{Fields: fields}
					for i := range seedRows {
						seedResult.Rows = append(seedResult.Rows, sqltypes.Row{sqltypes.NewInt64(int64(i))})
					}
					term := &cyclicRecurseTerm{fakePrimitive: &fakePrimitive{}}
					cte := &RecurseCTE{
						Seed:     &fakePrimitive{results: []*sqltypes.Result{seedResult}},
						Term:     term,
						Vars:     map[string]int{"id": 0},
						Distinct: true,
					}
					cursor := &recurseMemoryVCursor{noopVCursor: &noopVCursor{inTx: mode == "transaction stream"}, limit: 300000}
					var err error
					if mode == "execute" {
						_, err = cte.TryExecute(t.Context(), cursor, nil, true)
					} else {
						err = cte.TryStreamExecute(t.Context(), cursor, nil, true, func(*sqltypes.Result) error { return nil })
					}
					require.ErrorContains(t, err, "VT09030")
					require.Equal(t, maxRecurseDepth, term.calls)
				})
			}
		})
	}
}

type cyclicRecurseTerm struct {
	*fakePrimitive
	calls     int
	branching bool
	stopAfter int
}

func (p *cyclicRecurseTerm) TryExecute(_ context.Context, _ VCursor, bindVars map[string]*querypb.BindVariable, _ bool) (*sqltypes.Result, error) {
	p.calls++
	if p.stopAfter > 0 && p.calls >= p.stopAfter {
		return nil, errors.New("test term limit")
	}
	value, err := sqltypes.BindVariableToValue(bindVars["id"])
	rows := []sqltypes.Row{{value}}
	if p.branching {
		rows = append(rows, sqltypes.Row{value})
	}
	return &sqltypes.Result{Rows: rows}, err
}

func (p *cyclicRecurseTerm) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	result, err := p.TryExecute(ctx, vcursor, bindVars, wantfields)
	if err != nil {
		return err
	}
	return callback(result)
}

// TestRecurseCTEStreamConcurrentDelivery verifies that the streaming path is
// safe when a source delivers its result chunks concurrently, as a multi-shard
// scatter does (StreamExecuteMulti fans out one goroutine per shard, each
// invoking the callback without serialization). Run with -race to catch
// unsynchronized access to the shared recursion frontier.
func TestRecurseCTEStreamConcurrentDelivery(t *testing.T) {
	fields := sqltypes.MakeTestFields("col1", "int64")

	// The seed delivers many chunks concurrently, mimicking a scatter.
	const seedChunks = 64
	seedResults := make([]*sqltypes.Result, 0, seedChunks)
	for i := range seedChunks {
		seedResults = append(seedResults, sqltypes.MakeTestResult(fields, strconv.Itoa(i)))
	}
	seed := &fakePrimitive{results: seedResults, async: true, noLog: true}
	// The Term returns no rows, so recursion stops after the seed level.
	term := &fakePrimitive{results: []*sqltypes.Result{sqltypes.MakeTestResult(fields)}, noLog: true}

	cte := &RecurseCTE{
		Seed: seed,
		Term: term,
		Vars: map[string]int{"col1": 0},
	}
	bv := map[string]*querypb.BindVariable{}

	res, err := wrapStreamExecute(cte, &noopVCursor{}, bv, true)
	require.NoError(t, err)
	require.Len(t, res.Rows, seedChunks)
}

// Recursion depth depends on levels, even when the seed or a later frontier
// contains more rows than the depth limit.
func TestRecurseCTEDepthDoesNotCountRows(t *testing.T) {
	fields := sqltypes.MakeTestFields("id", "int64")
	rows := func(start, count int) []sqltypes.Row {
		result := make([]sqltypes.Row, count)
		for i := range count {
			result[i] = sqltypes.Row{sqltypes.NewInt64(int64(start + i))}
		}
		return result
	}
	for _, tc := range []struct {
		name               string
		seedRows, termRows int
	}{
		{"wide seed with empty term", maxRecurseDepth + 1, 0},
		{"wide recursive frontier", 1, maxRecurseDepth + 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, mode := range []string{"execute", "stream", "transaction stream"} {
				t.Run(mode, func(t *testing.T) {
					seedResult := &sqltypes.Result{Fields: fields, Rows: rows(1, tc.seedRows)}
					firstTerm := &sqltypes.Result{Fields: fields, Rows: rows(tc.seedRows+1, tc.termRows)}
					results := make([]*sqltypes.Result, tc.seedRows+tc.termRows)
					for i := range results {
						results[i] = &sqltypes.Result{Fields: fields}
					}
					results[0] = firstTerm
					term := &fakePrimitive{results: results, noLog: true}
					cte := &RecurseCTE{Seed: &fakePrimitive{results: []*sqltypes.Result{seedResult}}, Term: term, Vars: map[string]int{"id": 0}}
					cursor := &recurseMemoryVCursor{noopVCursor: &noopVCursor{inTx: mode == "transaction stream"}, limit: 2*maxRecurseDepth + 2}
					var result *sqltypes.Result
					var err error
					if mode == "execute" {
						result, err = cte.TryExecute(t.Context(), cursor, nil, true)
					} else {
						result, err = wrapStreamExecute(cte, cursor, nil, true)
					}
					require.NoError(t, err)
					want := seedResult.Copy()
					want.Rows = append(want.Rows, firstTerm.Rows...)
					expectResult(t, result, want)
					require.Equal(t, len(results), term.curResult, "every row must drive its recursive term")
				})
			}
		})
	}
}

// A small frontier does not bound cumulative work: every row can drive another
// Term query at every level. Stop cyclic streaming recursion at the row budget.
func TestRecurseCTEStreamCycleHonorsTermBudget(t *testing.T) {
	fields := sqltypes.MakeTestFields("id", "int64")
	const limit = 2500
	for _, seedRows := range []int{100, 1000} {
		t.Run(strconv.Itoa(seedRows), func(t *testing.T) {
			seedResult := &sqltypes.Result{Fields: fields}
			for i := range seedRows {
				seedResult.Rows = append(seedResult.Rows, sqltypes.Row{sqltypes.NewInt64(int64(i))})
			}
			term := &cyclicRecurseTerm{fakePrimitive: &fakePrimitive{}}
			cte := &RecurseCTE{Seed: &fakePrimitive{results: []*sqltypes.Result{seedResult}}, Term: term, Vars: map[string]int{"id": 0}}
			cursor := &recurseMemoryVCursor{noopVCursor: &noopVCursor{}, limit: limit}
			err := cte.TryStreamExecute(t.Context(), cursor, nil, true, func(*sqltypes.Result) error { return nil })
			require.Equal(t, limit, term.calls, "stop before starting an over-budget Term query")
			require.ErrorContains(t, err, "recursive term execution count exceeded allowed limit of 2500")
			require.Equal(t, vtrpcpb.Code_RESOURCE_EXHAUSTED, vterrors.Code(err))
			require.Equal(t, vterrors.NetPacketTooLarge, vterrors.ErrState(err))
		})
	}
}

// Finite streams may use the full Term budget, but a client row-memory
// override must not bypass the operator-controlled work cap.
func TestRecurseCTEStreamTermBudgetCannotBeOverridden(t *testing.T) {
	fields := sqltypes.MakeTestFields("id", "int64")
	const limit = 3
	for _, tc := range []struct {
		name   string
		calls  int
		ignore bool
	}{
		{name: "at budget", calls: limit},
		{name: "over budget", calls: limit + 1},
		{name: "override", calls: limit + 1, ignore: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			term := &fakePrimitive{noLog: true}
			for i := range tc.calls {
				result := sqltypes.MakeTestResult(fields)
				if i < tc.calls-1 {
					result = sqltypes.MakeTestResult(fields, strconv.Itoa(i+2))
				}
				term.results = append(term.results, result)
			}
			cte := &RecurseCTE{Seed: &fakePrimitive{results: []*sqltypes.Result{sqltypes.MakeTestResult(fields, "1")}}, Term: term, Vars: map[string]int{"id": 0}}
			cursor := &recurseMemoryVCursor{noopVCursor: &noopVCursor{}, limit: limit, ignore: tc.ignore}
			delivered := 0
			err := cte.TryStreamExecute(t.Context(), cursor, nil, true, func(result *sqltypes.Result) error {
				delivered += len(result.Rows)
				return nil
			})
			if tc.calls > limit {
				require.ErrorContains(t, err, "recursive term execution count exceeded allowed limit of 3")
				require.Equal(t, vtrpcpb.Code_RESOURCE_EXHAUSTED, vterrors.Code(err))
				require.Equal(t, vterrors.NetPacketTooLarge, vterrors.ErrState(err))
				require.Equal(t, limit, term.curResult)
				require.Equal(t, limit+1, delivered)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.calls, term.curResult)
			require.Equal(t, tc.calls, delivered)
		})
	}
}

// A branching UNION ALL must retain a work cap even when the client disables
// row-memory checks. Keep the primitive bounded if that guard is removed.
func TestRecurseCTETermBudgetWithRowOverride(t *testing.T) {
	const limit = 2500
	for _, mode := range []string{"execute", "stream", "transaction stream"} {
		t.Run(mode, func(t *testing.T) {
			seed := sqltypes.MakeTestResult(sqltypes.MakeTestFields("id", "int64"))
			for range 100 {
				seed.Rows = append(seed.Rows, sqltypes.Row{sqltypes.NewInt64(1)})
			}
			term := &cyclicRecurseTerm{fakePrimitive: &fakePrimitive{}, branching: true, stopAfter: limit + 1}
			cte := &RecurseCTE{Seed: &fakePrimitive{results: []*sqltypes.Result{seed}}, Term: term, Vars: map[string]int{"id": 0}}
			cursor := &recurseMemoryVCursor{noopVCursor: &noopVCursor{inTx: mode == "transaction stream"}, limit: limit, ignore: true}
			var err error
			if mode == "execute" {
				_, err = cte.TryExecute(t.Context(), cursor, nil, true)
			} else {
				err = cte.TryStreamExecute(t.Context(), cursor, nil, true, func(*sqltypes.Result) error { return nil })
			}
			require.Equal(t, limit, term.calls)
			require.ErrorContains(t, err, "recursive term execution count exceeded allowed limit of 2500")
			require.Equal(t, vtrpcpb.Code_RESOURCE_EXHAUSTED, vterrors.Code(err))
			require.Equal(t, vterrors.NetPacketTooLarge, vterrors.ErrState(err))
		})
	}
}

// Use per-cursor budgets so resource-limit tests do not change shared globals.
type recurseMemoryVCursor struct {
	*noopVCursor
	limit  int
	ignore bool
}

func (v *recurseMemoryVCursor) MaxMemoryRows() int { return v.limit }
func (v *recurseMemoryVCursor) ExceedsMaxMemoryRows(rows int) bool {
	return !v.ignore && rows > v.limit
}

// Row budgets apply at seed and term accumulation, including branching levels,
// while streaming can release completed levels and the override remains valid.
func TestRecurseCTEMemoryRows(t *testing.T) {
	fields := sqltypes.MakeTestFields("id", "int64")
	for _, tc := range []struct {
		name                       string
		seed                       int
		terms                      []int
		limit                      int
		ignore                     bool
		bufferedError, streamError bool
		workError                  bool
	}{
		{name: "seed over budget", seed: 4, terms: []int{0, 0, 0, 0}, limit: 3, bufferedError: true, streamError: true},
		{name: "seed at budget", seed: 3, terms: []int{0, 0, 0}, limit: 3},
		{name: "term over budget", seed: 1, terms: []int{3, 0, 0, 0}, limit: 3, bufferedError: true, streamError: true},
		{name: "term at budget", seed: 1, terms: []int{2, 0, 0}, limit: 3},
		{name: "completed levels within cumulative budget", seed: 1, terms: []int{1, 1, 0}, limit: 3},
		{name: "branching levels", seed: 1, terms: []int{2, 2, 2, 0, 0, 0, 0}, limit: 5, bufferedError: true, streamError: true},
		{name: "override seed budget", seed: 4, terms: []int{0, 0, 0, 0}, limit: 3, ignore: true, bufferedError: true, streamError: true, workError: true},
		{name: "override branching budget", seed: 1, terms: []int{2, 2, 2, 0, 0, 0, 0}, limit: 5, ignore: true, bufferedError: true, streamError: true, workError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, mode := range []string{"execute", "stream", "transaction stream"} {
				t.Run(mode, func(t *testing.T) {
					next := int64(1)
					makeResult := func(count int) *sqltypes.Result {
						result := &sqltypes.Result{Fields: fields}
						for range count {
							result.Rows = append(result.Rows, sqltypes.Row{sqltypes.NewInt64(next)})
							next++
						}
						return result
					}
					seed := &fakePrimitive{results: []*sqltypes.Result{makeResult(tc.seed)}}
					want := seed.results[0].Copy()
					term := &fakePrimitive{noLog: true}
					for _, count := range tc.terms {
						result := makeResult(count)
						term.results = append(term.results, result)
						want.Rows = append(want.Rows, result.Rows...)
					}
					cte := &RecurseCTE{Seed: seed, Term: term, Vars: map[string]int{"id": 0}}
					cursor := &recurseMemoryVCursor{noopVCursor: &noopVCursor{inTx: mode == "transaction stream"}, limit: tc.limit, ignore: tc.ignore}
					var result *sqltypes.Result
					var err error
					if mode == "execute" {
						result, err = cte.TryExecute(t.Context(), cursor, nil, true)
					} else {
						result, err = wrapStreamExecute(cte, cursor, nil, true)
					}
					wantError := tc.bufferedError
					if mode == "stream" {
						wantError = tc.streamError
					}
					if wantError {
						message := "in-memory row count exceeded allowed limit"
						if tc.workError {
							message = "recursive term execution count exceeded allowed limit"
						}
						require.ErrorContains(t, err, message)
						require.Equal(t, vtrpcpb.Code_RESOURCE_EXHAUSTED, vterrors.Code(err))
						require.Equal(t, vterrors.NetPacketTooLarge, vterrors.ErrState(err))
						if tc.name == "seed over budget" {
							require.Zero(t, term.curResult, "do not expand an oversized seed")
						}
						return
					}
					require.NoError(t, err)
					expectResult(t, result, want)
				})
			}
		})
	}
}

// Scatter callbacks must share one budget and stop delivering rows after any
// chunk exceeds it, for both the seed and recursive-term streams.
func TestRecurseCTEConcurrentMemoryRows(t *testing.T) {
	for _, source := range []string{"seed", "term"} {
		t.Run(source, func(t *testing.T) {
			fields := sqltypes.MakeTestFields("id", "int64")
			chunks := make([]*sqltypes.Result, 64)
			for i := range chunks {
				chunks[i] = sqltypes.MakeTestResult(fields, strconv.Itoa(i+1))
			}
			seed := &fakePrimitive{results: chunks, async: true, noLog: true}
			term := &fakePrimitive{results: []*sqltypes.Result{sqltypes.MakeTestResult(fields)}, noLog: true}
			if source == "term" {
				seed = &fakePrimitive{results: []*sqltypes.Result{sqltypes.MakeTestResult(fields, "1")}}
				term = &fakePrimitive{results: chunks, async: true, noLog: true}
			}
			cte := &RecurseCTE{Seed: seed, Term: term, Vars: map[string]int{"id": 0}}
			cursor := &recurseMemoryVCursor{noopVCursor: &noopVCursor{}, limit: 3}
			delivered := 0
			err := cte.TryStreamExecute(t.Context(), cursor, nil, true, func(result *sqltypes.Result) error {
				delivered += len(result.Rows)
				// Keep the test bounded if the memory guard is removed.
				if delivered > 10 {
					return vterrors.Errorf(vtrpcpb.Code_CANCELED, "test row limit")
				}
				return nil
			})
			require.ErrorContains(t, err, "in-memory row count exceeded allowed limit")
			require.Equal(t, vtrpcpb.Code_RESOURCE_EXHAUSTED, vterrors.Code(err))
			require.Equal(t, vterrors.NetPacketTooLarge, vterrors.ErrState(err))
			require.LessOrEqual(t, delivered, 3, "reject over-budget chunks before forwarding them")
		})
	}
}

// A terminal callback error or cancellation must stop concurrent delivery and
// must propagate unchanged from either source stream.
func TestRecurseCTEStreamStopsOnError(t *testing.T) {
	for _, source := range []string{"seed", "term"} {
		for _, cancelContext := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/cancel=%t", source, cancelContext), func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				t.Cleanup(cancel)
				fields := sqltypes.MakeTestFields("id", "int64")
				chunks := make([]*sqltypes.Result, 16)
				for i := range chunks {
					chunks[i] = sqltypes.MakeTestResult(fields, strconv.Itoa(i+1))
				}
				seed := &fakePrimitive{results: chunks, async: true, noLog: true}
				term := &fakePrimitive{results: []*sqltypes.Result{sqltypes.MakeTestResult(fields)}, noLog: true}
				if source == "term" {
					seed = &fakePrimitive{results: []*sqltypes.Result{sqltypes.MakeTestResult(fields, "1")}}
					term = &fakePrimitive{results: chunks, async: true, noLog: true}
				}
				cte := &RecurseCTE{Seed: seed, Term: term, Vars: map[string]int{"id": 0}}
				stop := errors.New("stop callback")
				calls := 0
				err := cte.TryStreamExecute(ctx, &noopVCursor{}, nil, true, func(result *sqltypes.Result) error {
					if len(result.Rows) == 0 {
						return nil
					}
					calls++
					if source == "term" && calls == 1 {
						return nil
					}
					if cancelContext {
						cancel()
						return nil
					}
					return stop
				})
				if cancelContext {
					require.ErrorIs(t, err, context.Canceled)
				} else {
					require.ErrorIs(t, err, stop)
				}
				expectedCalls := 1
				if source == "term" {
					expectedCalls = 2
				}
				require.Equal(t, expectedCalls, calls, "do not deliver rows after termination")
			})
		}
	}
}

// Scatter can aggregate errors from multiple shard callbacks and lose their
// MySQL state. Recursion must return its original latched callback error.
func TestRecurseCTEStreamPreservesCallbackErrorState(t *testing.T) {
	fields := sqltypes.MakeTestFields("id", "int64")
	for _, source := range []string{"seed", "term"} {
		t.Run(source, func(t *testing.T) {
			oversized := &aggregateRecurseCallbackErrors{fakePrimitive: &fakePrimitive{results: []*sqltypes.Result{sqltypes.MakeTestResult(fields, "1", "2", "3", "4")}}}
			var seed, term Primitive = oversized, &fakePrimitive{results: []*sqltypes.Result{sqltypes.MakeTestResult(fields)}}
			if source == "term" {
				seed = &fakePrimitive{results: []*sqltypes.Result{sqltypes.MakeTestResult(fields, "1")}}
				term = oversized
			}
			cte := &RecurseCTE{Seed: seed, Term: term, Vars: map[string]int{"id": 0}}
			cursor := &recurseMemoryVCursor{noopVCursor: &noopVCursor{}, limit: 3}
			err := cte.TryStreamExecute(t.Context(), cursor, nil, true, func(*sqltypes.Result) error { return nil })
			require.ErrorContains(t, err, "in-memory row count exceeded allowed limit")
			require.Equal(t, vtrpcpb.Code_RESOURCE_EXHAUSTED, vterrors.Code(err))
			require.Equal(t, vterrors.NetPacketTooLarge, vterrors.ErrState(err))
		})
	}
}

type aggregateRecurseCallbackErrors struct{ *fakePrimitive }

func (p *aggregateRecurseCallbackErrors) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	err := p.fakePrimitive.TryStreamExecute(ctx, vcursor, bindVars, wantfields, callback)
	if err == nil {
		return nil
	}
	return vterrors.Aggregate([]error{err, err})
}
