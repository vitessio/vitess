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

// TestScalarJSONArrayAgg tests merging shard-level json_arrayagg partials
// without grouping.
func TestScalarJSONArrayAgg(t *testing.T) {
	fields := sqltypes.MakeTestFields(
		"json_arrayagg(c2)",
		"json",
	)

	varcharFields := sqltypes.MakeTestFields(
		"json_arrayagg(c2)",
		"varchar",
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
		name: "empty array partials do not add separators",
		inputResult: sqltypes.MakeTestResult(fields,
			`[]`, `[1, 2]`, `[]`),
		expResult: sqltypes.MakeTestResult(fields,
			`[1, 2]`),
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
		name: "output field type is rewritten to json",
		inputResult: sqltypes.MakeTestResult(varcharFields,
			`[1]`, `[2]`),
		expResult: sqltypes.MakeTestResult(fields,
			`[1, 2]`),
	}, {
		name: "wide decimal elements pass through the splice verbatim",
		inputResult: sqltypes.MakeTestResult(fields,
			`[123456789012.12345678]`),
		expResult: sqltypes.MakeTestResult(fields,
			`[123456789012.12345678]`),
	}}

	for _, tcase := range tcases {
		t.Run(tcase.name, func(t *testing.T) {
			fp := &fakePrimitive{results: []*sqltypes.Result{tcase.inputResult}}
			oa := &ScalarAggregate{
				Aggregates: []*AggregateParams{{
					Opcode: AggregateJSONArrayAgg,
					Col:    0,
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

// TestScalarJSONArrayAggMalformedPartial tests that json_arrayagg fails loudly
// when an input is not a shard-level JSON array partial.
func TestScalarJSONArrayAggMalformedPartial(t *testing.T) {
	fields := sqltypes.MakeTestFields(
		"json_arrayagg(c2)",
		"json",
	)

	// `[1], [2]` passes the bracket check but is not a single JSON array; only
	// the full parse rejects it.
	for _, partial := range []string{`abc`, `{"a": 1}`, `1`, `[1], [2]`} {
		t.Run(partial, func(t *testing.T) {
			fp := &fakePrimitive{results: []*sqltypes.Result{sqltypes.MakeTestResult(fields, partial)}}
			oa := &ScalarAggregate{
				Aggregates: []*AggregateParams{{
					Opcode: AggregateJSONArrayAgg,
					Col:    0,
				}},
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

// TestScalarJSONObjectAgg tests merging shard-level json_objectagg partials
// without grouping.
func TestScalarJSONObjectAgg(t *testing.T) {
	fields := sqltypes.MakeTestFields(
		"json_objectagg(c1, c2)",
		"json",
	)

	tcases := []struct {
		name        string
		inputResult *sqltypes.Result
		expResult   *sqltypes.Result
	}{{
		name: "last duplicate key wins and members sort by length then bytes",
		inputResult: sqltypes.MakeTestResult(fields,
			`{"a": 1, "b": 2}`, `{"b": 3, "ccc": 4}`),
		expResult: sqltypes.MakeTestResult(fields,
			`{"a": 1, "b": 3, "ccc": 4}`),
	}, {
		name: "shorter keys sort first",
		inputResult: sqltypes.MakeTestResult(fields,
			`{"bb": 1}`, `{"a": 2}`, `{"z": 3}`),
		expResult: sqltypes.MakeTestResult(fields,
			`{"a": 2, "z": 3, "bb": 1}`),
	}, {
		name: "escaped keys round-trip",
		inputResult: sqltypes.MakeTestResult(fields,
			`{"a\"b": 1}`, `{"c": 2}`),
		expResult: sqltypes.MakeTestResult(fields,
			`{"c": 2, "a\"b": 1}`),
	}, {
		name: "null member values are preserved",
		inputResult: sqltypes.MakeTestResult(fields,
			`{"k": null}`, `{"m": 1}`),
		expResult: sqltypes.MakeTestResult(fields,
			`{"k": null, "m": 1}`),
	}, {
		name: "null partials are skipped",
		inputResult: sqltypes.MakeTestResult(fields,
			"null", `{"a": 1}`, "null"),
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
		name: "single partial is returned in normalized order",
		inputResult: sqltypes.MakeTestResult(fields,
			`{"b": 1, "aa": 2}`),
		expResult: sqltypes.MakeTestResult(fields,
			`{"b": 1, "aa": 2}`),
	}, {
		name: "wide decimal member values are preserved verbatim",
		inputResult: sqltypes.MakeTestResult(fields,
			`{"a": 123456789012.12345678}`),
		expResult: sqltypes.MakeTestResult(fields,
			`{"a": 123456789012.12345678}`),
	}, {
		name: "numbers wider than uint64 are preserved verbatim",
		inputResult: sqltypes.MakeTestResult(fields,
			`{"a": 12345678901234567890123456789}`),
		expResult: sqltypes.MakeTestResult(fields,
			`{"a": 12345678901234567890123456789}`),
	}, {
		name: "trailing zeros in member values are preserved verbatim",
		inputResult: sqltypes.MakeTestResult(fields,
			`{"a": 1.230}`),
		expResult: sqltypes.MakeTestResult(fields,
			`{"a": 1.230}`),
	}, {
		name: "nested wide decimal member values are preserved verbatim",
		inputResult: sqltypes.MakeTestResult(fields,
			`{"a": {"b": 99999999999999999999999999.5}}`),
		expResult: sqltypes.MakeTestResult(fields,
			`{"a": {"b": 99999999999999999999999999.5}}`),
	}, {
		name: "merged partials preserve number literals",
		inputResult: sqltypes.MakeTestResult(fields,
			`{"a": 1.10000000}`, `{"b": 2.20000000}`),
		expResult: sqltypes.MakeTestResult(fields,
			`{"a": 1.10000000, "b": 2.20000000}`),
	}}

	for _, tcase := range tcases {
		t.Run(tcase.name, func(t *testing.T) {
			fp := &fakePrimitive{results: []*sqltypes.Result{tcase.inputResult}}
			oa := &ScalarAggregate{
				Aggregates: []*AggregateParams{{
					Opcode: AggregateJSONObjectAgg,
					Col:    0,
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

// TestScalarJSONObjectAggMalformedPartial tests that json_objectagg fails
// loudly when an input is not a shard-level JSON object partial.
func TestScalarJSONObjectAggMalformedPartial(t *testing.T) {
	fields := sqltypes.MakeTestFields(
		"json_objectagg(c1, c2)",
		"json",
	)

	for _, partial := range []string{`abc`, `[1]`, `1`} {
		t.Run(partial, func(t *testing.T) {
			fp := &fakePrimitive{results: []*sqltypes.Result{sqltypes.MakeTestResult(fields, partial)}}
			oa := &ScalarAggregate{
				Aggregates: []*AggregateParams{{
					Opcode: AggregateJSONObjectAgg,
					Col:    0,
				}},
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

// TestScalarJSONAggMergeAssociativity tests that merging an already-merged
// partial with further partials gives the same result as merging all the
// partials at once. This is what makes multi-level vtgate aggregators
// (e.g. a route split under a subquery split) correct.
func TestScalarJSONAggMergeAssociativity(t *testing.T) {
	runMerge := func(t *testing.T, oc AggregateOpcode, partials ...string) string {
		fields := sqltypes.MakeTestFields("agg", "json")
		fp := &fakePrimitive{results: []*sqltypes.Result{sqltypes.MakeTestResult(fields, partials...)}}
		oa := &ScalarAggregate{
			Aggregates: []*AggregateParams{{
				Opcode: oc,
				Col:    0,
			}},
			Input: fp,
		}
		qr, err := oa.TryExecute(t.Context(), &noopVCursor{}, nil, false)
		require.NoError(t, err)
		return qr.Rows[0][0].ToString()
	}

	t.Run("json_arrayagg", func(t *testing.T) {
		all := runMerge(t, AggregateJSONArrayAgg, `[1]`, `[2, 3]`, `[4]`)
		assert.Equal(t, `[1, 2, 3, 4]`, all)

		merged := runMerge(t, AggregateJSONArrayAgg, `[1]`, `[2, 3]`)
		assert.Equal(t, all, runMerge(t, AggregateJSONArrayAgg, merged, `[4]`))
	})

	t.Run("json_objectagg", func(t *testing.T) {
		all := runMerge(t, AggregateJSONObjectAgg, `{"a": 1}`, `{"bb": 2, "a": 9}`, `{"c": 3}`)
		assert.Equal(t, `{"a": 9, "c": 3, "bb": 2}`, all)

		merged := runMerge(t, AggregateJSONObjectAgg, `{"a": 1}`, `{"bb": 2, "a": 9}`)
		assert.Equal(t, all, runMerge(t, AggregateJSONObjectAgg, merged, `{"c": 3}`))
	})
}
