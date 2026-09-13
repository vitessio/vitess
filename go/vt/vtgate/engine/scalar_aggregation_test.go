/*
Copyright 2022 The Vitess Authors.

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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/sqlparser"
	. "vitess.io/vitess/go/vt/vtgate/engine/opcode"
)

func TestEmptyRows(outer *testing.T) {
	testCases := []struct {
		opcode      AggregateOpcode
		origOpcode  AggregateOpcode
		expectedVal string
		expectedTyp string
	}{{
		opcode:      AggregateCountDistinct,
		expectedVal: "0",
		expectedTyp: "int64",
	}, {
		opcode:      AggregateCount,
		expectedVal: "0",
		expectedTyp: "int64",
	}, {
		opcode:      AggregateSumDistinct,
		expectedVal: "null",
		expectedTyp: "decimal",
	}, {
		opcode:      AggregateSum,
		expectedVal: "null",
		expectedTyp: "decimal",
	}, {
		opcode:      AggregateSum,
		expectedVal: "0",
		expectedTyp: "int64",
		origOpcode:  AggregateCount,
	}, {
		opcode:      AggregateMax,
		expectedVal: "null",
		expectedTyp: "int64",
	}, {
		opcode:      AggregateMin,
		expectedVal: "null",
		expectedTyp: "int64",
	}}

	for _, test := range testCases {
		outer.Run(test.opcode.String(), func(t *testing.T) {
			fp := &fakePrimitive{
				results: []*sqltypes.Result{sqltypes.MakeTestResult(
					sqltypes.MakeTestFields(
						test.opcode.String(),
						"int64",
					),
					// Empty input table
				)},
			}

			oa := &ScalarAggregate{
				Aggregates: []*AggregateParams{{
					Opcode:     test.opcode,
					Col:        0,
					Alias:      test.opcode.String(),
					OrigOpcode: test.origOpcode,
				}},
				Input: fp,
			}

			result, err := oa.TryExecute(context.Background(), &noopVCursor{}, nil, false)
			require.NoError(t, err)

			wantResult := sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					test.opcode.String(),
					test.expectedTyp,
				),
				test.expectedVal,
			)
			utils.MustMatch(t, wantResult, result)
		})
	}
}

func TestScalarAggregateStreamExecute(t *testing.T) {
	fields := sqltypes.MakeTestFields(
		"col|weight_string(col)",
		"uint64|varbinary",
	)
	fp := &fakePrimitive{
		allResultsInOneCall: true,
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(fields,
				"1|null",
			), sqltypes.MakeTestResult(fields,
				"3|null",
			),
		},
	}

	oa := &ScalarAggregate{
		Aggregates: []*AggregateParams{{
			Opcode: AggregateSum,
			Col:    0,
		}},
		Input:               fp,
		TruncateColumnCount: 1,
	}

	var results []*sqltypes.Result
	err := oa.TryStreamExecute(t.Context(), &noopVCursor{}, nil, true, func(qr *sqltypes.Result) error {
		results = append(results, qr)
		return nil
	})
	require.NoError(t, err)
	// one for the fields, and one for the actual aggregation result
	require.Len(t, results, 2, "number of results")

	got := fmt.Sprintf("%v", results[1].Rows)
	assert.Equal(t, "[[DECIMAL(4)]]", got)
}

// TestScalarAggregateExecuteTruncate checks if truncate works
func TestScalarAggregateExecuteTruncate(t *testing.T) {
	fields := sqltypes.MakeTestFields(
		"col|weight_string(col)",
		"uint64|varbinary",
	)

	fp := &fakePrimitive{
		allResultsInOneCall: true,
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(fields,
				"1|null", "3|null",
			),
		},
	}

	oa := &ScalarAggregate{
		Aggregates: []*AggregateParams{{
			Opcode: AggregateSum,
			Col:    0,
		}},
		Input:               fp,
		TruncateColumnCount: 1,
	}

	qr, err := oa.TryExecute(t.Context(), &noopVCursor{}, nil, true)
	require.NoError(t, err)
	assert.Equal(t, "[[DECIMAL(4)]]", fmt.Sprintf("%v", qr.Rows))
}

// TestScalarGroupConcatWithAggrOnEngine tests group_concat with full aggregation on engine.
func TestScalarGroupConcatWithAggrOnEngine(t *testing.T) {
	fields := sqltypes.MakeTestFields(
		"c2",
		"varchar",
	)

	varbinaryFields := sqltypes.MakeTestFields(
		"c2",
		"varbinary",
	)

	textOutFields := sqltypes.MakeTestFields(
		"group_concat(c2)",
		"text",
	)

	tcases := []struct {
		name        string
		inputResult *sqltypes.Result
		expResult   *sqltypes.Result
	}{{
		name: "ending with null",
		inputResult: sqltypes.MakeTestResult(fields,
			"a", "a", "b", "null", "null"),
		expResult: sqltypes.MakeTestResult(textOutFields,
			`a,a,b`),
	}, {
		name:        "empty result",
		inputResult: sqltypes.MakeTestResult(fields),
		expResult: sqltypes.MakeTestResult(textOutFields,
			`null`),
	}, {
		name: "only null value for concat",
		inputResult: sqltypes.MakeTestResult(fields,
			"null", "null", "null"),
		expResult: sqltypes.MakeTestResult(textOutFields,
			`null`),
	}, {
		name: "empty string value for concat",
		inputResult: sqltypes.MakeTestResult(fields,
			"", "", ""),
		expResult: sqltypes.MakeTestResult(textOutFields,
			`,,`),
	}, {
		name: "varbinary column",
		inputResult: sqltypes.MakeTestResult(varbinaryFields,
			"foo", "null", "bar"),
		expResult: sqltypes.MakeTestResult(sqltypes.MakeTestFields(
			"group_concat(c2)",
			"blob",
		),
			`foo,bar`),
	}}

	for _, tcase := range tcases {
		t.Run(tcase.name, func(t *testing.T) {
			fp := &fakePrimitive{results: []*sqltypes.Result{tcase.inputResult}}
			oa := &ScalarAggregate{
				Aggregates: []*AggregateParams{{
					Opcode: AggregateGroupConcat,
					Col:    0,
					Alias:  "group_concat(c2)",
					Func:   &sqlparser.GroupConcatExpr{Separator: ","},
				}},
				Input: fp,
			}
			qr, err := oa.TryExecute(t.Context(), &noopVCursor{}, nil, false)
			require.NoError(t, err)
			utils.MustMatch(t, tcase.expResult, qr)

			fp.rewind()
			results := &sqltypes.Result{}
			err = oa.TryStreamExecute(t.Context(), &noopVCursor{}, nil, true, func(qr *sqltypes.Result) error {
				if qr.Fields != nil {
					results.Fields = qr.Fields
				}
				results.Rows = append(results.Rows, qr.Rows...)
				return nil
			})
			require.NoError(t, err)
			utils.MustMatch(t, tcase.expResult, results)
		})
	}
}

// TestScalarDistinctAggr tests distinct aggregation on engine.
func TestScalarDistinctAggrOnEngine(t *testing.T) {
	fields := sqltypes.MakeTestFields(
		"value|value",
		"int64|int64",
	)

	fp := &fakePrimitive{results: []*sqltypes.Result{sqltypes.MakeTestResult(
		fields,
		"100|100",
		"200|200",
		"200|200",
		"400|400",
		"400|400",
		"600|600",
	)}}

	oa := &ScalarAggregate{
		Aggregates: []*AggregateParams{
			NewAggregateParam(AggregateCountDistinct, 0, nil, "count(distinct value)", collations.MySQL8()),
			NewAggregateParam(AggregateSumDistinct, 1, nil, "sum(distinct value)", collations.MySQL8()),
		},
		Input: fp,
	}
	qr, err := oa.TryExecute(t.Context(), &noopVCursor{}, nil, false)
	require.NoError(t, err)
	require.Equal(t, `[[INT64(4) DECIMAL(1300)]]`, fmt.Sprintf("%v", qr.Rows))

	fp.rewind()
	results := &sqltypes.Result{}
	err = oa.TryStreamExecute(t.Context(), &noopVCursor{}, nil, true, func(qr *sqltypes.Result) error {
		if qr.Fields != nil {
			results.Fields = qr.Fields
		}
		results.Rows = append(results.Rows, qr.Rows...)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, `[[INT64(4) DECIMAL(1300)]]`, fmt.Sprintf("%v", results.Rows))
}

func TestScalarDistinctPushedDown(t *testing.T) {
	fields := sqltypes.MakeTestFields(
		"count(distinct value)|sum(distinct value)",
		"int64|decimal",
	)

	fp := &fakePrimitive{results: []*sqltypes.Result{sqltypes.MakeTestResult(
		fields,
		"2|200",
		"6|400",
		"3|700",
		"1|10",
		"7|30",
		"8|90",
	)}}

	countAggr := NewAggregateParam(AggregateSum, 0, nil, "count(distinct value)", collations.MySQL8())
	countAggr.OrigOpcode = AggregateCountDistinct
	sumAggr := NewAggregateParam(AggregateSum, 1, nil, "sum(distinct value)", collations.MySQL8())
	sumAggr.OrigOpcode = AggregateSumDistinct
	oa := &ScalarAggregate{
		Aggregates: []*AggregateParams{
			countAggr,
			sumAggr,
		},
		Input: fp,
	}
	qr, err := oa.TryExecute(t.Context(), &noopVCursor{}, nil, false)
	require.NoError(t, err)
	require.Equal(t, `[[INT64(27) DECIMAL(1430)]]`, fmt.Sprintf("%v", qr.Rows))

	fp.rewind()
	results := &sqltypes.Result{}
	err = oa.TryStreamExecute(t.Context(), &noopVCursor{}, nil, true, func(qr *sqltypes.Result) error {
		if qr.Fields != nil {
			results.Fields = qr.Fields
		}
		results.Rows = append(results.Rows, qr.Rows...)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, `[[INT64(27) DECIMAL(1430)]]`, fmt.Sprintf("%v", results.Rows))
}

// TestScalarGroupConcat tests group_concat with partial aggregation on engine.
func TestScalarGroupConcat(t *testing.T) {
	fields := sqltypes.MakeTestFields(
		"group_concat(c2)",
		"text",
	)

	varbinaryFields := sqltypes.MakeTestFields(
		"group_concat(c2)",
		"blob",
	)

	tcases := []struct {
		name        string
		inputResult *sqltypes.Result
		expResult   *sqltypes.Result
	}{{
		name: "ending with null",
		inputResult: sqltypes.MakeTestResult(fields,
			"a", "a", "b", "null", "null"),
		expResult: sqltypes.MakeTestResult(fields,
			`a,a,b`),
	}, {
		name:        "empty result",
		inputResult: sqltypes.MakeTestResult(fields),
		expResult: sqltypes.MakeTestResult(fields,
			`null`),
	}, {
		name: "only null value for concat",
		inputResult: sqltypes.MakeTestResult(fields,
			"null", "null", "null"),
		expResult: sqltypes.MakeTestResult(fields,
			`null`),
	}, {
		name: "empty string value for concat",
		inputResult: sqltypes.MakeTestResult(fields,
			"", "", ""),
		expResult: sqltypes.MakeTestResult(fields,
			`,,`),
	}, {
		name: "varbinary column",
		inputResult: sqltypes.MakeTestResult(varbinaryFields,
			"foo", "null", "bar"),
		expResult: sqltypes.MakeTestResult(varbinaryFields,
			`foo,bar`),
	}}

	for _, tcase := range tcases {
		t.Run(tcase.name, func(t *testing.T) {
			fp := &fakePrimitive{results: []*sqltypes.Result{tcase.inputResult}}
			oa := &ScalarAggregate{
				Aggregates: []*AggregateParams{{
					Opcode: AggregateGroupConcat,
					Col:    0,
					Func:   &sqlparser.GroupConcatExpr{Separator: ","},
				}},
				Input: fp,
			}
			qr, err := oa.TryExecute(t.Context(), &noopVCursor{}, nil, false)
			require.NoError(t, err)
			assert.Equal(t, tcase.expResult, qr)

			fp.rewind()
			results := &sqltypes.Result{}
			err = oa.TryStreamExecute(t.Context(), &noopVCursor{}, nil, true, func(qr *sqltypes.Result) error {
				if qr.Fields != nil {
					results.Fields = qr.Fields
				}
				results.Rows = append(results.Rows, qr.Rows...)
				return nil
			})
			require.NoError(t, err)
			assert.Equal(t, tcase.expResult, results)
		})
	}
}

// TestScalarJSONArrayAggMerge tests merging shard-level json_arrayagg partials
// without grouping. The aggregate params deliberately carry no Func: the merge
// aggregators do not depend on any AST state.
func TestScalarJSONArrayAggMerge(t *testing.T) {
	fields := sqltypes.MakeTestFields(
		"json_arrayagg(c2)",
		"json",
	)

	tcases := []struct {
		name        string
		inputResult *sqltypes.Result
		expResult   *sqltypes.Result
	}{{
		name: "two shard partials",
		inputResult: sqltypes.MakeTestResult(fields,
			`["a", "b"]`, `[1, null]`),
		expResult: sqltypes.MakeTestResult(fields,
			`["a", "b", 1, null]`),
	}, {
		name: "null partials are skipped",
		inputResult: sqltypes.MakeTestResult(fields,
			"null", `[{"a": 1}]`, "null"),
		expResult: sqltypes.MakeTestResult(fields,
			`[{"a": 1}]`),
	}, {
		name: "all shards empty",
		inputResult: sqltypes.MakeTestResult(fields,
			"null", "null"),
		expResult: sqltypes.MakeTestResult(fields,
			"null"),
	}, {
		name:        "no rows at all",
		inputResult: sqltypes.MakeTestResult(fields),
		expResult: sqltypes.MakeTestResult(fields,
			"null"),
	}, {
		name: "single partial is returned verbatim",
		inputResult: sqltypes.MakeTestResult(fields,
			`[1, 2, 3]`),
		expResult: sqltypes.MakeTestResult(fields,
			`[1, 2, 3]`),
	}, {
		name: "empty array partial adds no separator",
		inputResult: sqltypes.MakeTestResult(fields,
			`[]`, `[1]`),
		expResult: sqltypes.MakeTestResult(fields,
			`[1]`),
	}, {
		name: "only empty array partials",
		inputResult: sqltypes.MakeTestResult(fields,
			`[]`, `[]`),
		expResult: sqltypes.MakeTestResult(fields,
			`[]`),
	}, {
		name: "wide decimal element passes through the splice byte-identical",
		inputResult: sqltypes.MakeTestResult(fields,
			`[123456789012.12345678]`),
		expResult: sqltypes.MakeTestResult(fields,
			`[123456789012.12345678]`),
	}}

	for _, tcase := range tcases {
		t.Run(tcase.name, func(t *testing.T) {
			fp := &fakePrimitive{results: []*sqltypes.Result{tcase.inputResult}}
			oa := &ScalarAggregate{
				Aggregates: []*AggregateParams{
					NewAggregateParam(AggregateJSONArrayMerge, 0, nil, "", collations.MySQL8()),
				},
				Input: fp,
			}
			qr, err := oa.TryExecute(t.Context(), &noopVCursor{}, nil, false)
			require.NoError(t, err)
			assert.Equal(t, sqltypes.TypeJSON, qr.Fields[0].Type)
			assert.Equal(t, tcase.expResult, qr)

			fp.rewind()
			results := &sqltypes.Result{}
			err = oa.TryStreamExecute(t.Context(), &noopVCursor{}, nil, true, func(qr *sqltypes.Result) error {
				if qr.Fields != nil {
					results.Fields = qr.Fields
				}
				results.Rows = append(results.Rows, qr.Rows...)
				return nil
			})
			require.NoError(t, err)
			assert.Equal(t, tcase.expResult, results)
		})
	}
}

// TestScalarJSONArrayAggMergeRejectsNonArrayShape tests that a partial whose
// root is not a JSON array fails loudly instead of being spliced silently,
// on both the execute and the stream execute paths.
func TestScalarJSONArrayAggMergeRejectsNonArrayShape(t *testing.T) {
	fields := sqltypes.MakeTestFields(
		"json_arrayagg(c2)",
		"json",
	)

	for _, partial := range []string{`abc`, `{"a": 1}`, `1`} {
		t.Run(partial, func(t *testing.T) {
			fp := &fakePrimitive{results: []*sqltypes.Result{sqltypes.MakeTestResult(fields, partial)}}
			oa := &ScalarAggregate{
				Aggregates: []*AggregateParams{
					NewAggregateParam(AggregateJSONArrayMerge, 0, nil, "", collations.MySQL8()),
				},
				Input: fp,
			}
			_, err := oa.TryExecute(t.Context(), &noopVCursor{}, nil, false)
			require.ErrorContains(t, err, "unexpected json_arrayagg partial")

			fp.rewind()
			err = oa.TryStreamExecute(t.Context(), &noopVCursor{}, nil, true, func(*sqltypes.Result) error {
				return nil
			})
			require.ErrorContains(t, err, "unexpected json_arrayagg partial")
		})
	}
}

// TestScalarJSONObjectAggMerge tests merging shard-level json_objectagg partials
// without grouping: MySQL's "last duplicate key wins" rule in partial order, and
// members emitted in MySQL's key length-then-bytes normalized order.
func TestScalarJSONObjectAggMerge(t *testing.T) {
	fields := sqltypes.MakeTestFields(
		"json_objectagg(c1, c2)",
		"json",
	)

	tcases := []struct {
		name        string
		inputResult *sqltypes.Result
		expResult   *sqltypes.Result
	}{{
		name: "last duplicate key wins, keys emitted length-then-bytes",
		inputResult: sqltypes.MakeTestResult(fields,
			`{"a": 1, "b": 2}`, `{"b": 3, "ccc": 4}`),
		expResult: sqltypes.MakeTestResult(fields,
			`{"a": 1, "b": 3, "ccc": 4}`),
	}, {
		name: "shorter keys sort first, byte order within a length",
		inputResult: sqltypes.MakeTestResult(fields,
			`{"bb": 1}`, `{"a": 2}`, `{"z": 3}`),
		expResult: sqltypes.MakeTestResult(fields,
			`{"a": 2, "z": 3, "bb": 1}`),
	}, {
		name: "escaped key round-trips",
		inputResult: sqltypes.MakeTestResult(fields,
			`{"a\"b": 1}`),
		expResult: sqltypes.MakeTestResult(fields,
			`{"a\"b": 1}`),
	}, {
		name: "null member value is preserved",
		inputResult: sqltypes.MakeTestResult(fields,
			`{"k": null}`, `{"m": 1}`),
		expResult: sqltypes.MakeTestResult(fields,
			`{"k": null, "m": 1}`),
	}, {
		name: "null partials are skipped",
		inputResult: sqltypes.MakeTestResult(fields,
			"null", `{"a": 1}`),
		expResult: sqltypes.MakeTestResult(fields,
			`{"a": 1}`),
	}, {
		name: "all shards empty",
		inputResult: sqltypes.MakeTestResult(fields,
			"null", "null"),
		expResult: sqltypes.MakeTestResult(fields,
			"null"),
	}, {
		name:        "no rows at all",
		inputResult: sqltypes.MakeTestResult(fields),
		expResult: sqltypes.MakeTestResult(fields,
			"null"),
	}, {
		name: "single MySQL-normalized partial is byte-identical",
		inputResult: sqltypes.MakeTestResult(fields,
			`{"b": 1, "aa": 2}`),
		expResult: sqltypes.MakeTestResult(fields,
			`{"b": 1, "aa": 2}`),
	}, {
		// Nested multi-key objects inside member values are re-emitted in the
		// parser's lexicographic order instead of MySQL's length-then-bytes
		// order. This is the same (documented) divergence that the evalengine
		// JSON_OBJECT builtin and vstreamer already have.
		name: "nested objects re-emit in lexicographic order",
		inputResult: sqltypes.MakeTestResult(fields,
			`{"a": {"c": 2, "bb": 1}}`),
		expResult: sqltypes.MakeTestResult(fields,
			`{"a": {"bb": 1, "c": 2}}`),
	}, {
		name: "wide decimal member is preserved verbatim",
		inputResult: sqltypes.MakeTestResult(fields,
			`{"a": 123456789012.12345678}`),
		expResult: sqltypes.MakeTestResult(fields,
			`{"a": 123456789012.12345678}`),
	}, {
		name: "integer wider than uint64 is preserved verbatim",
		inputResult: sqltypes.MakeTestResult(fields,
			`{"a": 12345678901234567890123456789}`),
		expResult: sqltypes.MakeTestResult(fields,
			`{"a": 12345678901234567890123456789}`),
	}, {
		name: "trailing zeros in a decimal are preserved",
		inputResult: sqltypes.MakeTestResult(fields,
			`{"a": 1.230}`),
		expResult: sqltypes.MakeTestResult(fields,
			`{"a": 1.230}`),
	}, {
		name: "nested wide decimal is preserved",
		inputResult: sqltypes.MakeTestResult(fields,
			`{"a": {"b": 99999999999999999999999999.5}}`),
		expResult: sqltypes.MakeTestResult(fields,
			`{"a": {"b": 99999999999999999999999999.5}}`),
	}, {
		name: "merged partials preserve both decimal literals",
		inputResult: sqltypes.MakeTestResult(fields,
			`{"a": 1.10000000}`, `{"b": 2.20000000}`),
		expResult: sqltypes.MakeTestResult(fields,
			`{"a": 1.10000000, "b": 2.20000000}`),
	}}

	for _, tcase := range tcases {
		t.Run(tcase.name, func(t *testing.T) {
			fp := &fakePrimitive{results: []*sqltypes.Result{tcase.inputResult}}
			oa := &ScalarAggregate{
				Aggregates: []*AggregateParams{
					NewAggregateParam(AggregateJSONObjectMerge, 0, nil, "", collations.MySQL8()),
				},
				Input: fp,
			}
			qr, err := oa.TryExecute(t.Context(), &noopVCursor{}, nil, false)
			require.NoError(t, err)
			assert.Equal(t, sqltypes.TypeJSON, qr.Fields[0].Type)
			assert.Equal(t, tcase.expResult, qr)

			fp.rewind()
			results := &sqltypes.Result{}
			err = oa.TryStreamExecute(t.Context(), &noopVCursor{}, nil, true, func(qr *sqltypes.Result) error {
				if qr.Fields != nil {
					results.Fields = qr.Fields
				}
				results.Rows = append(results.Rows, qr.Rows...)
				return nil
			})
			require.NoError(t, err)
			assert.Equal(t, tcase.expResult, results)
		})
	}
}

// TestScalarJSONObjectAggMergeMalformedPartial tests that partials that are not
// complete JSON objects fail loudly instead of being merged silently, on both
// the execute and the stream execute paths.
func TestScalarJSONObjectAggMergeMalformedPartial(t *testing.T) {
	fields := sqltypes.MakeTestFields(
		"json_objectagg(c1, c2)",
		"json",
	)

	for _, partial := range []string{`[1]`, `not json`} {
		t.Run(partial, func(t *testing.T) {
			fp := &fakePrimitive{results: []*sqltypes.Result{sqltypes.MakeTestResult(fields, partial)}}
			oa := &ScalarAggregate{
				Aggregates: []*AggregateParams{
					NewAggregateParam(AggregateJSONObjectMerge, 0, nil, "", collations.MySQL8()),
				},
				Input: fp,
			}
			_, err := oa.TryExecute(t.Context(), &noopVCursor{}, nil, false)
			require.ErrorContains(t, err, "unexpected json_objectagg partial")

			fp.rewind()
			err = oa.TryStreamExecute(t.Context(), &noopVCursor{}, nil, true, func(*sqltypes.Result) error {
				return nil
			})
			require.ErrorContains(t, err, "unexpected json_objectagg partial")
		})
	}
}

// TestScalarJSONMergeAssociativity tests that merging the output of a previous
// merge together with further partials gives the same result as merging all the
// original partials at once — the property that makes stacked merges (route
// split under filters and subqueries) safe.
func TestScalarJSONMergeAssociativity(t *testing.T) {
	fields := sqltypes.MakeTestFields(
		"j",
		"json",
	)

	merge := func(t *testing.T, oc AggregateOpcode, partials ...string) string {
		fp := &fakePrimitive{results: []*sqltypes.Result{sqltypes.MakeTestResult(fields, partials...)}}
		oa := &ScalarAggregate{
			Aggregates: []*AggregateParams{
				NewAggregateParam(oc, 0, nil, "", collations.MySQL8()),
			},
			Input: fp,
		}
		qr, err := oa.TryExecute(t.Context(), &noopVCursor{}, nil, false)
		require.NoError(t, err)
		require.Len(t, qr.Rows, 1)
		return qr.Rows[0][0].ToString()
	}

	t.Run("array", func(t *testing.T) {
		mergeOfMerges := merge(t, AggregateJSONArrayMerge,
			merge(t, AggregateJSONArrayMerge, `[1, 2]`, `[3]`),
			`[4, null]`,
		)
		mergeOfAll := merge(t, AggregateJSONArrayMerge, `[1, 2]`, `[3]`, `[4, null]`)
		assert.Equal(t, mergeOfAll, mergeOfMerges)
	})

	t.Run("object", func(t *testing.T) {
		mergeOfMerges := merge(t, AggregateJSONObjectMerge,
			merge(t, AggregateJSONObjectMerge, `{"a": 1}`, `{"a": 2, "bb": 3}`),
			`{"c": 4}`,
		)
		mergeOfAll := merge(t, AggregateJSONObjectMerge, `{"a": 1}`, `{"a": 2, "bb": 3}`, `{"c": 4}`)
		assert.Equal(t, mergeOfAll, mergeOfMerges)
	})
}

// TestScalarJSONBuildOpcodesRejected pins the safety boundary of the dedicated
// merge opcode design: the user-facing build opcodes are only ever pushed down
// into shard queries and must never be evaluated at the vtgate level.
func TestScalarJSONBuildOpcodesRejected(t *testing.T) {
	fields := sqltypes.MakeTestFields("j", "json")

	for _, op := range []AggregateOpcode{AggregateJSONArrayAgg, AggregateJSONObjectAgg} {
		t.Run(op.String(), func(t *testing.T) {
			fp := &fakePrimitive{results: []*sqltypes.Result{sqltypes.MakeTestResult(fields)}}
			oa := &ScalarAggregate{
				Aggregates: []*AggregateParams{
					NewAggregateParam(op, 0, nil, "", collations.MySQL8()),
				},
				Input: fp,
			}
			_, err := oa.TryExecute(t.Context(), &noopVCursor{}, nil, false)
			require.ErrorContains(t, err, "must be pushed down to MySQL")
		})
	}
}
