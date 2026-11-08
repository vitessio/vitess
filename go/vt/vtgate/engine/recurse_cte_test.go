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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
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

// TestRecurseCTEDistinct checks that UNION DISTINCT discards rows already
// produced, so a cycle terminates instead of hitting the recursion limit.
func TestRecurseCTEDistinct(t *testing.T) {
	fields := sqltypes.MakeTestFields("name|id", "varchar|int64")

	// "A" duplicates "a" under the case-insensitive collation.
	seed := &fakePrimitive{
		results: []*sqltypes.Result{sqltypes.MakeTestResult(fields, "a|1", "A|1")},
		noLog:   true,
	}
	// (a,1) leads to (b,2) and back to itself; (b,2) leads only to seen rows.
	termResults := []*sqltypes.Result{
		sqltypes.MakeTestResult(fields, "b|2", "a|1"),
		sqltypes.MakeTestResult(fields, "B|2", "a|1"),
	}
	for range maxRecurseDepth {
		termResults = append(termResults, sqltypes.MakeTestResult(fields, "a|1"))
	}
	term := &fakePrimitive{results: termResults, noLog: true}

	checkCols := []CheckCol{
		{Col: 0, Type: evalengine.NewType(sqltypes.VarChar, collations.CollationUtf8mb4ID)},
		{Col: 1, Type: evalengine.NewType(sqltypes.Int64, collations.CollationBinaryID)},
	}
	cte := &RecurseCTE{
		Seed:          seed,
		Term:          term,
		Vars:          map[string]int{"cte_id": 1},
		CheckCols:     checkCols,
		TermCheckCols: checkCols,
	}
	bv := map[string]*querypb.BindVariable{}
	want := sqltypes.MakeTestResult(fields, "a|1", "b|2").Rows

	res, err := cte.TryExecute(t.Context(), &noopVCursor{}, bv, true)
	require.NoError(t, err)
	require.Equal(t, want, res.Rows)

	seed.rewind()
	term.rewind()
	res, err = wrapStreamExecute(cte, &noopVCursor{}, bv, true)
	require.NoError(t, err)
	require.Equal(t, want, res.Rows)
}

// TestRecurseCTEDistinctWeightStrings checks that a weight string at another offset on each side
// is used, and dropped from the result.
func TestRecurseCTEDistinctWeightStrings(t *testing.T) {
	// the weight string is column 1 of the seed and column 2 of the term
	seedFields := sqltypes.MakeTestFields("n|ws", "varchar|varbinary")
	termFields := sqltypes.MakeTestFields("n|id|ws", "varchar|int64|varbinary")
	seed := &fakePrimitive{
		results: []*sqltypes.Result{sqltypes.MakeTestResult(seedFields, "a|1", "A|1")},
		noLog:   true,
	}
	term := &fakePrimitive{
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(termFields, "A|7|1", "b|7|2"),
			{},
			{},
		},
		noLog: true,
	}

	// an unsupported collation, so the weight string is compared
	unsupported := evalengine.NewType(sqltypes.VarChar, collations.Unknown)
	cte := &RecurseCTE{
		Seed:          seed,
		Term:          term,
		Vars:          map[string]int{"n": 0},
		CheckCols:     []CheckCol{{Col: 0, WsCol: new(1), Type: unsupported}},
		TermCheckCols: []CheckCol{{Col: 0, WsCol: new(2), Type: unsupported}},
		Truncate:      1,
	}
	bv := map[string]*querypb.BindVariable{}
	want := sqltypes.MakeTestResult(sqltypes.MakeTestFields("n", "varchar"), "a", "b").Rows

	res, err := cte.TryExecute(t.Context(), &noopVCursor{}, bv, true)
	require.NoError(t, err)
	require.Equal(t, want, res.Rows)
	require.Len(t, res.Fields, 1)

	seed.rewind()
	term.rewind()
	res, err = wrapStreamExecute(cte, &noopVCursor{}, bv, true)
	require.NoError(t, err)
	require.Equal(t, want, res.Rows)
}

// TestRecurseCTEDistinctStoredValues checks that UNION DISTINCT compares values the way MySQL
// stores them in the CTE's columns, compares BIT and GEOMETRY by their bytes, and fails on values
// that MySQL rejects.
func TestRecurseCTEDistinctStoredValues(t *testing.T) {
	testcases := []struct {
		name   string
		typ    evalengine.Type
		fields []*querypb.Field
		seed   []string
		term   []string
		text   bool
		expect []string
		err    string
	}{{
		// the planner couldn't type the column
		name:   "unknown type",
		typ:    evalengine.NewUnknownType(),
		fields: sqltypes.MakeTestFields("n|ws", "varchar|varbinary"),
		seed:   []string{"alice|A", "bob|B", "alice|A"},
		// the term's text must not be converted to a number
		term:   []string{"carol|C"},
		text:   true,
		expect: []string{"alice", "bob", "carol"},
	}, {
		name:   "bit",
		typ:    evalengine.NewType(sqltypes.Bit, collations.CollationBinaryID),
		fields: sqltypes.MakeTestFields("n|ws", "bit|varbinary"),
		seed:   []string{"\x01|\x01", "\x02|\x02", "\x01|\x01"},
		expect: []string{"\x01", "\x02"},
	}, {
		name:   "geometry",
		typ:    evalengine.NewType(sqltypes.Geometry, collations.CollationBinaryID),
		fields: sqltypes.MakeTestFields("n|ws", "geometry|varbinary"),
		seed:   []string{"p1|p1", "p2|p2", "p1|p1"},
		expect: []string{"p1", "p2"},
	}, {
		// with recursive c as (select 1 as n union select 1.5e0 from c where n < 2) stores 1.5 as 2
		name:   "non-integer value under an integer column",
		typ:    evalengine.NewType(sqltypes.Int64, collations.CollationBinaryID),
		fields: sqltypes.MakeTestFields("n|ws", "int64|varbinary"),
		seed:   []string{"1|"},
		term:   []string{"1.5|"},
		expect: []string{"1", "2"},
	}, {
		// the same for text, unlike CAST('1.5' AS SIGNED)
		name:   "non-integer text under an integer column",
		typ:    evalengine.NewType(sqltypes.Int64, collations.CollationBinaryID),
		fields: sqltypes.MakeTestFields("n|ws", "int64|varbinary"),
		seed:   []string{"1|"},
		term:   []string{"1.5|"},
		text:   true,
		expect: []string{"1", "2"},
	}, {
		// MySQL rounds a double half to even, so 0.5 is stored as 0 and isn't a duplicate of 1
		name:   "double rounds half to even",
		typ:    evalengine.NewType(sqltypes.Int64, collations.CollationBinaryID),
		fields: sqltypes.MakeTestFields("n|ws", "int64|varbinary"),
		seed:   []string{"1|"},
		term:   []string{"0.5|"},
		expect: []string{"1", "0"},
	}, {
		// MySQL fails this with error 1366, it doesn't store 0
		name:   "text that isn't a number under an integer column",
		typ:    evalengine.NewType(sqltypes.Int64, collations.CollationBinaryID),
		fields: sqltypes.MakeTestFields("n|ws", "int64|varbinary"),
		seed:   []string{"1|"},
		term:   []string{"abc|"},
		text:   true,
		err:    "Incorrect integer value: 'abc'",
	}, {
		// MySQL fails this with error 1264
		name:   "value out of range under an integer column",
		typ:    evalengine.NewType(sqltypes.Int64, collations.CollationBinaryID),
		fields: sqltypes.MakeTestFields("n|ws", "int64|varbinary"),
		seed:   []string{"1|"},
		term:   []string{"1e30|"},
		err:    "BIGINT value is out of range in '1e30'",
	}, {
		name:   "negative value under an unsigned column",
		typ:    evalengine.NewType(sqltypes.Uint64, collations.CollationBinaryID),
		fields: sqltypes.MakeTestFields("n|ws", "uint64|varbinary"),
		seed:   []string{"1|"},
		term:   []string{"-2.5|"},
		err:    "BIGINT value is out of range in '-2.5'",
	}, {
		// decimals above 2^53 aren't the same value, as they would be as floats
		name:   "unknown type with large decimals",
		typ:    evalengine.NewUnknownType(),
		fields: sqltypes.MakeTestFields("n|ws", "decimal|varbinary"),
		seed:   []string{"9007199254740992.00|", "9007199254740993.00|", "9007199254740992.00|"},
		expect: []string{"9007199254740992.00", "9007199254740993.00"},
	}}
	// without a type, every value that isn't a number is compared by its weight string
	for _, typ := range []string{"enum", "set", "json", "bit", "geometry", "vector", "date"} {
		testcases = append(testcases, struct {
			name   string
			typ    evalengine.Type
			fields []*querypb.Field
			seed   []string
			term   []string
			text   bool
			expect []string
			err    string
		}{
			name:   "unknown type with " + typ + " values",
			typ:    evalengine.NewUnknownType(),
			fields: sqltypes.MakeTestFields("n|ws", typ+"|varbinary"),
			seed:   []string{"X|WX", "Y|WY", "X|WX"},
			expect: []string{"X", "Y"},
		})
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			seed := &fakePrimitive{results: []*sqltypes.Result{sqltypes.MakeTestResult(tc.fields, tc.seed...)}, noLog: true}
			termFields := sqltypes.MakeTestFields("n|ws", "float64|varbinary")
			if tc.text {
				termFields = sqltypes.MakeTestFields("n|ws", "varchar|varbinary")
			}
			term := &fakePrimitive{
				results: []*sqltypes.Result{sqltypes.MakeTestResult(termFields, tc.term...), {}, {}, {}},
				noLog:   true,
			}

			cte := &RecurseCTE{
				Seed:          seed,
				Term:          term,
				Vars:          map[string]int{"n": 0},
				CheckCols:     []CheckCol{{Col: 0, WsCol: new(1), Type: tc.typ}},
				TermCheckCols: []CheckCol{{Col: 0, WsCol: new(1), Type: tc.typ}},
				Truncate:      1,
			}
			bv := map[string]*querypb.BindVariable{}

			res, err := cte.TryExecute(t.Context(), &noopVCursor{}, bv, true)
			if tc.err != "" {
				require.ErrorContains(t, err, tc.err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expect, rowValues(res.Rows))
			}

			seed.rewind()
			term.rewind()
			res, err = wrapStreamExecute(cte, &noopVCursor{}, bv, true)
			if tc.err != "" {
				require.ErrorContains(t, err, tc.err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expect, rowValues(res.Rows))
		})
	}
}

// TestRecurseCTEGetFieldsTruncated checks that the fields don't include the weight strings
// that are dropped from the rows.
func TestRecurseCTEGetFieldsTruncated(t *testing.T) {
	cte := &RecurseCTE{
		Seed: &fakePrimitive{
			results: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("n|ws", "varchar|varbinary"))},
			noLog:   true,
		},
		Truncate: 1,
	}

	res, err := cte.GetFields(t.Context(), &noopVCursor{}, nil)
	require.NoError(t, err)
	require.Len(t, res.Fields, 1)
	require.Equal(t, "n", res.Fields[0].Name)
}

// TestRecurseCTEDistinctUnderConcatenate checks that the weight strings used for the
// deduplication don't reach a parent that expects the CTE's own columns.
func TestRecurseCTEDistinctUnderConcatenate(t *testing.T) {
	fields := sqltypes.MakeTestFields("n", "varchar")
	cte := &RecurseCTE{
		Seed: &fakePrimitive{
			results: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("n|ws", "varchar|varbinary"), "a|A", "a|A")},
			noLog:   true,
		},
		Term:          &fakePrimitive{results: []*sqltypes.Result{{}}, noLog: true},
		Vars:          map[string]int{"n": 0},
		CheckCols:     []CheckCol{{Col: 0, WsCol: new(1), Type: evalengine.NewUnknownType()}},
		TermCheckCols: []CheckCol{{Col: 0, WsCol: new(1), Type: evalengine.NewUnknownType()}},
		Truncate:      1,
	}
	other := &fakePrimitive{results: []*sqltypes.Result{sqltypes.MakeTestResult(fields, "b")}, noLog: true}

	res, err := NewConcatenate([]Primitive{cte, other}, nil).TryExecute(t.Context(), &noopVCursor{}, map[string]*querypb.BindVariable{}, true)
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b"}, rowValues(res.Rows))
}

// TestRecurseCTEDistinctTermFromCTE checks that a term column that only passes on a column of the
// previous row is compared by that row's weight string, not by weight_string(:cte_n), which differs
// for a latin1 column as it uses the connection's collation.
func TestRecurseCTEDistinctTermFromCTE(t *testing.T) {
	fields := sqltypes.MakeTestFields("n|ws", "varchar|varbinary")
	seed := &fakePrimitive{
		results: []*sqltypes.Result{sqltypes.MakeTestResult(fields, "ABC|\\x41\\x42\\x43")},
		noLog:   true,
	}
	term := &fakePrimitive{
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(fields, "ABC|\\x1C\\x47\\x1C\\x60\\x1C\\x7A"),
			{},
		},
		noLog: true,
	}

	// an unsupported collation, so the weight strings are compared
	unsupported := evalengine.NewType(sqltypes.VarChar, collations.Unknown)
	cte := &RecurseCTE{
		Seed:          seed,
		Term:          term,
		Vars:          map[string]int{"cte_n": 0},
		CheckCols:     []CheckCol{{Col: 0, WsCol: new(1), Type: unsupported}},
		TermCheckCols: []CheckCol{{Col: 0, WsCol: new(1), Type: unsupported}},
		Truncate:      1,
		TermFromCTE:   map[int]int{0: 0},
	}
	bv := map[string]*querypb.BindVariable{}

	res, err := cte.TryExecute(t.Context(), &noopVCursor{}, bv, true)
	require.NoError(t, err)
	require.Equal(t, []string{"ABC"}, rowValues(res.Rows))

	seed.rewind()
	term.rewind()
	res, err = wrapStreamExecute(cte, &noopVCursor{}, bv, true)
	require.NoError(t, err)
	require.Equal(t, []string{"ABC"}, rowValues(res.Rows))
}

// TestRecurseCTEDistinctTermFromCTELevels checks that the weight string of a pass-through column is
// read at the seed's offset for the first level and at the term's offset after that, over three levels.
func TestRecurseCTEDistinctTermFromCTELevels(t *testing.T) {
	seed := &fakePrimitive{
		results: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("n|id|ws", "varchar|int64|varbinary"), "ABC|1|S")},
		noLog:   true,
	}
	// the term returns the weight string in the connection's collation, at another offset
	termFields := sqltypes.MakeTestFields("n|id|other|ws", "varchar|int64|varbinary|varbinary")
	term := &fakePrimitive{
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(termFields, "ABC|2|o|C"),
			sqltypes.MakeTestResult(termFields, "ABC|3|o|C"),
			// a duplicate of the first level's row, which has the seed's weight string
			sqltypes.MakeTestResult(termFields, "ABC|2|o|C"),
		},
		noLog: true,
	}

	// an unsupported collation, so the weight strings are compared
	unsupported := evalengine.NewType(sqltypes.VarChar, collations.Unknown)
	intType := evalengine.NewType(sqltypes.Int64, collations.CollationBinaryID)
	cte := &RecurseCTE{
		Seed:          seed,
		Term:          term,
		Vars:          map[string]int{"cte_id": 1},
		CheckCols:     []CheckCol{{Col: 0, WsCol: new(2), Type: unsupported}, {Col: 1, Type: intType}},
		TermCheckCols: []CheckCol{{Col: 0, WsCol: new(3), Type: unsupported}, {Col: 1, Type: intType}},
		Truncate:      2,
		TermFromCTE:   map[int]int{0: 0},
	}
	bv := map[string]*querypb.BindVariable{}
	want := []string{"ABC|1", "ABC|2", "ABC|3"}

	res, err := cte.TryExecute(t.Context(), &noopVCursor{}, bv, true)
	require.NoError(t, err)
	require.Equal(t, want, rowStrings(res.Rows))

	seed.rewind()
	term.rewind()
	res, err = wrapStreamExecute(cte, &noopVCursor{}, bv, true)
	require.NoError(t, err)
	require.Equal(t, want, rowStrings(res.Rows))
}

// TestRecurseCTEDistinctTermFromCTEWithoutWeightString checks that a term column passing on a CTE
// column that has no weight string is compared by that column's value. The term's own
// weight_string(:cte_i) is NULL for a number, which would make every row a duplicate.
func TestRecurseCTEDistinctTermFromCTEWithoutWeightString(t *testing.T) {
	fields := sqltypes.MakeTestFields("n|i|ws", "varchar|int64|varbinary")
	seed := &fakePrimitive{
		results: []*sqltypes.Result{sqltypes.MakeTestResult(fields, "a|5|WA", "b|7|WB")},
		noLog:   true,
	}
	term := &fakePrimitive{
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(fields, "5|6|null"),
			sqltypes.MakeTestResult(fields, "7|6|null"),
			{},
			{},
		},
		noLog: true,
	}

	intType := evalengine.NewType(sqltypes.Int64, collations.CollationBinaryID)
	cte := &RecurseCTE{
		Seed:          seed,
		Term:          term,
		Vars:          map[string]int{"cte_i": 1},
		CheckCols:     []CheckCol{{Col: 0, WsCol: new(2), Type: evalengine.NewUnknownType()}, {Col: 1, Type: intType}},
		TermCheckCols: []CheckCol{{Col: 0, WsCol: new(2), Type: evalengine.NewUnknownType()}, {Col: 1, Type: intType}},
		Truncate:      2,
		TermFromCTE:   map[int]int{0: 1},
	}
	bv := map[string]*querypb.BindVariable{}
	want := []string{"a|5", "b|7", "5|6", "7|6"}

	res, err := cte.TryExecute(t.Context(), &noopVCursor{}, bv, true)
	require.NoError(t, err)
	require.Equal(t, want, rowStrings(res.Rows))

	seed.rewind()
	term.rewind()
	res, err = wrapStreamExecute(cte, &noopVCursor{}, bv, true)
	require.NoError(t, err)
	require.Equal(t, want, rowStrings(res.Rows))
}

// TestRecurseCTEDistinctTermByBytes checks that a term column whose weight string isn't in the
// collation of the CTE's column is compared by its bytes: it keeps values that differ, as MySQL
// does for a utf8mb4_bin column, and a repeated value still ends the recursion.
func TestRecurseCTEDistinctTermByBytes(t *testing.T) {
	seed := &fakePrimitive{
		results: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("n|ws", "varchar|varbinary"), "a|\\x00\\x00\\x61", "A|\\x00\\x00\\x41")},
		noLog:   true,
	}
	termFields := sqltypes.MakeTestFields("n", "varchar")
	term := &fakePrimitive{
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(termFields, "ax"),
			sqltypes.MakeTestResult(termFields, "Ax"),
			sqltypes.MakeTestResult(termFields, "ax"),
			sqltypes.MakeTestResult(termFields, "Ax"),
		},
		noLog: true,
	}

	cte := &RecurseCTE{
		Seed:      seed,
		Term:      term,
		Vars:      map[string]int{"cte_n": 0},
		CheckCols: []CheckCol{{Col: 0, WsCol: new(1), Type: evalengine.NewUnknownType()}},
		// the planner points the weight string at the column itself
		TermCheckCols: []CheckCol{{Col: 0, WsCol: new(0), Type: evalengine.NewUnknownType()}},
		Truncate:      1,
	}
	bv := map[string]*querypb.BindVariable{}
	want := []string{"a", "A", "ax", "Ax"}

	res, err := cte.TryExecute(t.Context(), &noopVCursor{}, bv, true)
	require.NoError(t, err)
	require.Equal(t, want, rowValues(res.Rows))

	seed.rewind()
	term.rewind()
	res, err = wrapStreamExecute(cte, &noopVCursor{}, bv, true)
	require.NoError(t, err)
	require.Equal(t, want, rowValues(res.Rows))
}

func rowStrings(rows []sqltypes.Row) []string {
	var values []string
	for _, row := range rows {
		var cols []string
		for _, col := range row {
			cols = append(cols, col.ToString())
		}
		values = append(values, strings.Join(cols, "|"))
	}
	return values
}

func rowValues(rows []sqltypes.Row) []string {
	var values []string
	for _, row := range rows {
		values = append(values, row[0].ToString())
	}
	return values
}
