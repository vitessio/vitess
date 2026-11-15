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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
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

// TestRecurseCTECharsets checks the rows of a recursion that reads a column
// of another character set than the seed. For
//
//	create table a (id int primary key, l varchar(10) character set latin1 collate latin1_swedish_ci);
//	create table d (id int primary key, u varchar(10) character set utf8mb4, l varchar(10) character set latin1 collate latin1_swedish_ci);
//	insert into a values (2, 'é');
//	insert into d values (1, 'é', 'x'), (2, 'ß', 'é');
//
// MySQL 8.0.46 returns, read with character_set_results = NULL,
//
//	with recursive r (id, l) as (select id, l from a where id = 2
//	  union all select d.id, d.u from r join d on d.l = r.l) select id, hex(l) from r
//	  2 E9, 2 DF, both latin1_swedish_ci
//	with recursive r as (select id, u as l from d where id = 1
//	  union all select d2.id, d2.l from r join d as d2 on d2.id = r.id + 1) select id, hex(l) from r
//	  1 C3A9, 2 C3A9, both utf8mb4_0900_ai_ci
//
// The rows of the result are in the character set of the column of the seed,
// and the value that goes into the next recursion is in utf8mb4, as every
// statement is.
func TestRecurseCTECharsets(t *testing.T) {
	latin1Swedish := collations.MySQL8().LookupByName("latin1_swedish_ci")
	utf8mb4 := collations.MySQL8().DefaultConnectionCharset()
	fields := func(collation collations.ID) []*querypb.Field {
		return []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32, Charset: uint32(collations.CollationBinaryID)},
			{Name: "l", Type: sqltypes.VarChar, Charset: uint32(collation)},
		}
	}
	result := func(collation collations.ID, id int32, text string) *sqltypes.Result {
		return &sqltypes.Result{
			Fields: fields(collation),
			Rows:   []sqltypes.Row{{sqltypes.NewInt32(id), sqltypes.MakeTrusted(sqltypes.VarChar, []byte(text))}},
		}
	}

	tests := []struct {
		name string
		// seed and term are the collation and the text of the row of the
		// seed and of the first recursion.
		seed, term         collations.ID
		seedText, termText string
		convert            bool
		// rows are the rows that are returned, and bindVars the text that
		// goes into each recursion.
		rows     []string
		bindVars []string
	}{{
		name: "latin1 seed, utf8mb4 recursion",
		seed: latin1Swedish, seedText: "\xe9",
		term: utf8mb4, termText: "ß",
		convert:  true,
		rows:     []string{"\xe9", "\xdf"},
		bindVars: []string{"é", "ß"},
	}, {
		name: "utf8mb4 seed, latin1 recursion",
		seed: utf8mb4, seedText: "é",
		term: latin1Swedish, termText: "\xe9",
		convert:  true,
		rows:     []string{"é", "é"},
		bindVars: []string{"é", "é"},
	}, {
		name: "the same character set",
		seed: latin1Swedish, seedText: "\xe9",
		term: latin1Swedish, termText: "\xdf",
		convert:  true,
		rows:     []string{"\xe9", "\xdf"},
		bindVars: []string{"é", "ß"},
	}, {
		// Without the flag every result is in the connection character
		// set, and nothing is converted.
		name: "without the flag",
		seed: utf8mb4, seedText: "é",
		term: utf8mb4, termText: "ß",
		rows:     []string{"é", "ß"},
		bindVars: []string{"é", "ß"},
	}}
	for _, tc := range tests {
		for _, stream := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s, stream %v", tc.name, stream), func(t *testing.T) {
				term := &fakePrimitive{results: []*sqltypes.Result{
					result(tc.term, 2, tc.termText),
					{Fields: fields(tc.term)},
				}}
				cte := &RecurseCTE{
					Seed: &fakePrimitive{results: []*sqltypes.Result{result(tc.seed, 1, tc.seedText)}},
					Term: term,
					Vars: map[string]int{"r_l": 1},
				}
				vcursor := &noopVCursor{convertResultCharset: tc.convert}

				var rows []string
				var sentFields [][]*querypb.Field
				collect := func(qr *sqltypes.Result) error {
					if len(qr.Fields) != 0 {
						sentFields = append(sentFields, qr.Fields)
					}
					for _, row := range qr.Rows {
						rows = append(rows, row[1].ToString())
					}
					return nil
				}
				if stream {
					require.NoError(t, cte.TryStreamExecute(t.Context(), vcursor, map[string]*querypb.BindVariable{}, true, collect))
				} else {
					qr, err := cte.TryExecute(t.Context(), vcursor, map[string]*querypb.BindVariable{}, true)
					require.NoError(t, err)
					require.NoError(t, collect(qr))
				}
				assert.Equal(t, tc.rows, rows)

				// The result has the fields of the seed, once.
				require.Len(t, sentFields, 1)
				assert.Equal(t, uint32(tc.seed), sentFields[0][1].Charset)

				call := "Execute"
				if stream {
					call = "StreamExecute"
				}
				var log []string
				for _, text := range tc.bindVars {
					log = append(log, fmt.Sprintf("%s r_l: %v %v", call, sqltypes.ValueBindVariable(sqltypes.NewVarChar(text)), tc.convert))
				}
				term.ExpectLog(t, log)
			})
		}
	}
}

// TestRecurseCTEIncorrectStringValue checks a recursion that reads a value
// with a character that the character set of the column of the seed does not
// have. For a latin1 column l of the seed and 'ab中cd' in the utf8mb4 column
// that the recursion reads, MySQL 8.0.46 fails with
//
//	ERROR 1366 (HY000): Incorrect string value: '\xE4\xB8\xADcd' for column 'l' at row 1
//
// with the default sql_mode, with STRICT_TRANS_TABLES, with STRICT_ALL_TABLES
// and with TRADITIONAL. With ” and with ONLY_FULL_GROUP_BY it returns
// 'ab?cd', 61 62 3F 63 64, and has the message as a warning. A session has
// its sql_mode as it stands in a SET statement, and none when it did not set
// it.
func TestRecurseCTEIncorrectStringValue(t *testing.T) {
	latin1Swedish := collations.MySQL8().LookupByName("latin1_swedish_ci")
	utf8mb4 := collations.MySQL8().DefaultConnectionCharset()
	const message = `Incorrect string value: '\xE4\xB8\xADcd' for column 'l' at row 1`

	tests := []struct {
		name    string
		sqlMode map[string]string
		strict  bool
	}{
		{"the session did not set it", nil, true},
		{"STRICT_TRANS_TABLES", map[string]string{"sql_mode": "'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES'"}, true},
		{"STRICT_ALL_TABLES", map[string]string{"sql_mode": "'STRICT_ALL_TABLES'"}, true},
		{"TRADITIONAL", map[string]string{"sql_mode": "'TRADITIONAL'"}, true},
		// 2097152 is STRICT_TRANS_TABLES, and 32 is ONLY_FULL_GROUP_BY.
		{"a number with STRICT_TRANS_TABLES", map[string]string{"sql_mode": "2097152"}, true},
		{"empty", map[string]string{"sql_mode": "''"}, false},
		{"ONLY_FULL_GROUP_BY", map[string]string{"sql_mode": "'ONLY_FULL_GROUP_BY'"}, false},
		{"a number without a strict mode", map[string]string{"sql_mode": "32"}, false},
	}
	for _, tc := range tests {
		for _, stream := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s, stream %v", tc.name, stream), func(t *testing.T) {
				field := func(collation collations.ID) []*querypb.Field {
					return []*querypb.Field{{Name: "l", Type: sqltypes.VarChar, Charset: uint32(collation)}}
				}
				cte := &RecurseCTE{
					Seed: &fakePrimitive{results: []*sqltypes.Result{{
						Fields: field(latin1Swedish),
						Rows:   []sqltypes.Row{{sqltypes.MakeTrusted(sqltypes.VarChar, []byte("abc"))}},
					}}},
					Term: &fakePrimitive{results: []*sqltypes.Result{
						{Fields: field(utf8mb4), Rows: []sqltypes.Row{{sqltypes.NewVarChar("ab中cd")}}},
						{Fields: field(utf8mb4)},
					}},
					Vars: map[string]int{"r_l": 0},
				}
				vcursor := &noopVCursor{convertResultCharset: true, systemVariables: tc.sqlMode}

				var rows []string
				collect := func(qr *sqltypes.Result) error {
					for _, row := range qr.Rows {
						rows = append(rows, row[0].ToString())
					}
					return nil
				}
				var err error
				if stream {
					err = cte.TryStreamExecute(t.Context(), vcursor, map[string]*querypb.BindVariable{}, true, collect)
				} else {
					var qr *sqltypes.Result
					qr, err = cte.TryExecute(t.Context(), vcursor, map[string]*querypb.BindVariable{}, true)
					if err == nil {
						require.NoError(t, collect(qr))
					}
				}

				if tc.strict {
					require.EqualError(t, err, message)
					assert.Equal(t, vtrpcpb.Code_INVALID_ARGUMENT, vterrors.Code(err))
					assert.Equal(t, vterrors.TruncatedWrongValueForField, vterrors.ErrState(err))
					assert.Empty(t, vcursor.recordedWarnings)
					return
				}
				require.NoError(t, err)
				assert.Equal(t, []string{"abc", "ab?cd"}, rows)
				assert.Equal(t, []*querypb.QueryWarning{{Code: 1366, Message: message}}, vcursor.recordedWarnings)
			})
		}
	}
}
