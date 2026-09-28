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
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
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

	// Buffered path aborts at the guard.
	_, err := cte.TryExecute(t.Context(), &noopVCursor{}, bv, true)
	require.ErrorContains(t, err, "Recursive query aborted")

	// Streaming path must enforce the same guard. Streamed rows cannot be
	// unsent, so the guard must fire before the over-limit Term stream starts:
	// the client receives the seed row plus one row per allowed iteration, and
	// nothing from iteration 1001.
	seed.rewind()
	term.rewind()
	var streamed []sqltypes.Row
	err = cte.TryStreamExecute(t.Context(), &noopVCursor{}, bv, true, func(r *sqltypes.Result) error {
		streamed = append(streamed, r.Rows...)
		return nil
	})
	require.ErrorContains(t, err, "Recursive query aborted")
	require.Len(t, streamed, 1001)
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
