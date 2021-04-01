/*
Copyright 2019 The Vitess Authors.

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
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/utils"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestOrderedAggregateExecute(t *testing.T) {
	assert := assert.New(t)
	fields := sqltypes.MakeTestFields(
		"col|count(*)",
		"varbinary|decimal",
	)
	fp := &fakePrimitive{
		results: []*sqltypes.Result{sqltypes.MakeTestResult(
			fields,
			"a|1",
			"a|1",
			"b|2",
			"c|3",
			"c|4",
		)},
	}

	oa := &OrderedAggregate{
		Aggregates: []AggregateParams{{
			Opcode: AggregateCount,
			Col:    1,
		}},
		Keys:  []int{0},
		Input: fp,
	}

	result, err := oa.Execute(nil, nil, false)
	assert.NoError(err)

	wantResult := sqltypes.MakeTestResult(
		fields,
		"a|2",
		"b|2",
		"c|7",
	)
	assert.Equal(wantResult, result)
}

func TestOrderedAggregateExecuteTruncate(t *testing.T) {
	assert := assert.New(t)
	fp := &fakePrimitive{
		results: []*sqltypes.Result{sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"col|count(*)|weight_string(col)",
				"varchar|decimal|varbinary",
			),
			"a|1|A",
			"A|1|A",
			"b|2|B",
			"C|3|C",
			"c|4|C",
		)},
	}

	oa := &OrderedAggregate{
		Aggregates: []AggregateParams{{
			Opcode: AggregateCount,
			Col:    1,
		}},
		Keys:                []int{2},
		TruncateColumnCount: 2,
		Input:               fp,
	}

	result, err := oa.Execute(nil, nil, false)
	assert.NoError(err)

	wantResult := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col|count(*)",
			"varchar|decimal",
		),
		"a|2",
		"b|2",
		"C|7",
	)
	assert.Equal(wantResult, result)
}

func TestOrderedAggregateStreamExecute(t *testing.T) {
	assert := assert.New(t)
	fields := sqltypes.MakeTestFields(
		"col|count(*)",
		"varbinary|decimal",
	)
	fp := &fakePrimitive{
		results: []*sqltypes.Result{sqltypes.MakeTestResult(
			fields,
			"a|1",
			"a|1",
			"b|2",
			"c|3",
			"c|4",
		)},
	}

	oa := &OrderedAggregate{
		Aggregates: []AggregateParams{{
			Opcode: AggregateCount,
			Col:    1,
		}},
		Keys:  []int{0},
		Input: fp,
	}

	var results []*sqltypes.Result
	err := oa.StreamExecute(nil, nil, false, func(qr *sqltypes.Result) error {
		results = append(results, qr)
		return nil
	})
	assert.NoError(err)

	wantResults := sqltypes.MakeTestStreamingResults(
		fields,
		"a|2",
		"---",
		"b|2",
		"---",
		"c|7",
	)
	assert.Equal(wantResults, results)
}

func TestOrderedAggregateStreamExecuteTruncate(t *testing.T) {
	assert := assert.New(t)
	fp := &fakePrimitive{
		results: []*sqltypes.Result{sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"col|count(*)|weight_string(col)",
				"varchar|decimal|varbinary",
			),
			"a|1|A",
			"A|1|A",
			"b|2|B",
			"C|3|C",
			"c|4|C",
		)},
	}

	oa := &OrderedAggregate{
		Aggregates: []AggregateParams{{
			Opcode: AggregateCount,
			Col:    1,
		}},
		Keys:                []int{2},
		TruncateColumnCount: 2,
		Input:               fp,
	}

	var results []*sqltypes.Result
	err := oa.StreamExecute(nil, nil, false, func(qr *sqltypes.Result) error {
		results = append(results, qr)
		return nil
	})
	assert.NoError(err)

	wantResults := sqltypes.MakeTestStreamingResults(
		sqltypes.MakeTestFields(
			"col|count(*)",
			"varchar|decimal",
		),
		"a|2",
		"---",
		"b|2",
		"---",
		"C|7",
	)
	assert.Equal(wantResults, results)
}

func TestOrderedAggregateGetFields(t *testing.T) {
	assert := assert.New(t)
	input := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col|count(*)",
			"varbinary|decimal",
		),
	)
	fp := &fakePrimitive{results: []*sqltypes.Result{input}}

	oa := &OrderedAggregate{Input: fp}

	got, err := oa.GetFields(nil, nil)
	assert.NoError(err)
	assert.Equal(got, input)
}

func TestOrderedAggregateGetFieldsTruncate(t *testing.T) {
	assert := assert.New(t)
	result := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col|count(*)|weight_string(col)",
			"varchar|decimal|varbinary",
		),
	)
	fp := &fakePrimitive{results: []*sqltypes.Result{result}}

	oa := &OrderedAggregate{
		TruncateColumnCount: 2,
		Input:               fp,
	}

	got, err := oa.GetFields(nil, nil)
	assert.NoError(err)
	wantResult := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col|count(*)",
			"varchar|decimal",
		),
	)
	assert.Equal(wantResult, got)
}

func TestOrderedAggregateInputFail(t *testing.T) {
	fp := &fakePrimitive{sendErr: errors.New("input fail")}

	oa := &OrderedAggregate{Input: fp}

	want := "input fail"
	if _, err := oa.Execute(nil, nil, false); err == nil || err.Error() != want {
		t.Errorf("oa.Execute(): %v, want %s", err, want)
	}

	fp.rewind()
	if err := oa.StreamExecute(nil, nil, false, func(_ *sqltypes.Result) error { return nil }); err == nil || err.Error() != want {
		t.Errorf("oa.StreamExecute(): %v, want %s", err, want)
	}

	fp.rewind()
	if _, err := oa.GetFields(nil, nil); err == nil || err.Error() != want {
		t.Errorf("oa.GetFields(): %v, want %s", err, want)
	}
}

func TestOrderedAggregateExecuteCountDistinct(t *testing.T) {
	assert := assert.New(t)
	fp := &fakePrimitive{
		results: []*sqltypes.Result{sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"col1|col2|count(*)",
				"varbinary|decimal|int64",
			),
			// Two identical values
			"a|1|1",
			"a|1|2",
			// Single value
			"b|1|1",
			// Two different values
			"c|3|1",
			"c|4|1",
			// Single null
			"d|null|1",
			// Start with null
			"e|null|1",
			"e|1|1",
			// Null comes after first
			"f|1|1",
			"f|null|1",
			// Identical to non-identical transition
			"g|1|1",
			"g|1|1",
			"g|2|1",
			"g|3|1",
			// Non-identical to identical transition
			"h|1|1",
			"h|2|1",
			"h|2|1",
			"h|3|1",
			// Key transition, should still count 3
			"i|3|1",
			"i|4|1",
		)},
	}

	oa := &OrderedAggregate{
		HasDistinct: true,
		Aggregates: []AggregateParams{{
			Opcode: AggregateCountDistinct,
			Col:    1,
			Alias:  "count(distinct col2)",
		}, {
			// Also add a count(*)
			Opcode: AggregateCount,
			Col:    2,
		}},
		Keys:  []int{0},
		Input: fp,
	}

	result, err := oa.Execute(nil, nil, false)
	assert.NoError(err)

	wantResult := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col1|count(distinct col2)|count(*)",
			"varbinary|int64|int64",
		),
		"a|1|3",
		"b|1|1",
		"c|2|2",
		"d|0|1",
		"e|1|2",
		"f|1|2",
		"g|3|4",
		"h|3|4",
		"i|2|2",
	)
	assert.Equal(wantResult, result)
}

func TestOrderedAggregateStreamCountDistinct(t *testing.T) {
	assert := assert.New(t)
	fp := &fakePrimitive{
		results: []*sqltypes.Result{sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"col1|col2|count(*)",
				"varbinary|decimal|int64",
			),
			// Two identical values
			"a|1|1",
			"a|1|2",
			// Single value
			"b|1|1",
			// Two different values
			"c|3|1",
			"c|4|1",
			// Single null
			"d|null|1",
			// Start with null
			"e|null|1",
			"e|1|1",
			// Null comes after first
			"f|1|1",
			"f|null|1",
			// Identical to non-identical transition
			"g|1|1",
			"g|1|1",
			"g|2|1",
			"g|3|1",
			// Non-identical to identical transition
			"h|1|1",
			"h|2|1",
			"h|2|1",
			"h|3|1",
			// Key transition, should still count 3
			"i|3|1",
			"i|4|1",
		)},
	}

	oa := &OrderedAggregate{
		HasDistinct: true,
		Aggregates: []AggregateParams{{
			Opcode: AggregateCountDistinct,
			Col:    1,
			Alias:  "count(distinct col2)",
		}, {
			// Also add a count(*)
			Opcode: AggregateCount,
			Col:    2,
		}},
		Keys:  []int{0},
		Input: fp,
	}

	var results []*sqltypes.Result
	err := oa.StreamExecute(nil, nil, false, func(qr *sqltypes.Result) error {
		results = append(results, qr)
		return nil
	})
	assert.NoError(err)

	wantResults := sqltypes.MakeTestStreamingResults(
		sqltypes.MakeTestFields(
			"col1|count(distinct col2)|count(*)",
			"varbinary|int64|int64",
		),
		"a|1|3",
		"-----",
		"b|1|1",
		"-----",
		"c|2|2",
		"-----",
		"d|0|1",
		"-----",
		"e|1|2",
		"-----",
		"f|1|2",
		"-----",
		"g|3|4",
		"-----",
		"h|3|4",
		"-----",
		"i|2|2",
	)
	assert.Equal(wantResults, results)
}

func TestOrderedAggregateSumDistinctGood(t *testing.T) {
	assert := assert.New(t)
	fp := &fakePrimitive{
		results: []*sqltypes.Result{sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"col1|col2|sum(col3)",
				"varbinary|int64|decimal",
			),
			// Two identical values
			"a|1|1",
			"a|1|2",
			// Single value
			"b|1|1",
			// Two different values
			"c|3|1",
			"c|4|1",
			// Single null
			"d|null|1",
			// Start with null
			"e|null|1",
			"e|1|1",
			// Null comes after first
			"f|1|1",
			"f|null|1",
			// Identical to non-identical transition
			"g|1|1",
			"g|1|1",
			"g|2|1",
			"g|3|1",
			// Non-identical to identical transition
			"h|1|1",
			"h|2|1",
			"h|2|1",
			"h|3|1",
			// Key transition, should still count 3
			"i|3|1",
			"i|4|1",
		)},
	}

	oa := &OrderedAggregate{
		HasDistinct: true,
		Aggregates: []AggregateParams{{
			Opcode: AggregateSumDistinct,
			Col:    1,
			Alias:  "sum(distinct col2)",
		}, {
			// Also add a count(*)
			Opcode: AggregateSum,
			Col:    2,
		}},
		Keys:  []int{0},
		Input: fp,
	}

	result, err := oa.Execute(nil, nil, false)
	assert.NoError(err)

	wantResult := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col1|sum(distinct col2)|sum(col3)",
			"varbinary|decimal|decimal",
		),
		"a|1|3",
		"b|1|1",
		"c|7|2",
		"d|null|1",
		"e|1|2",
		"f|1|2",
		"g|6|4",
		"h|6|4",
		"i|7|2",
	)
	assert.Equal(wantResult, result)
}

func TestOrderedAggregateSumDistinctTolerateError(t *testing.T) {
	fp := &fakePrimitive{
		results: []*sqltypes.Result{sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"col1|col2",
				"varbinary|varbinary",
			),
			"a|aaa",
			"a|0",
			"a|1",
		)},
	}

	oa := &OrderedAggregate{
		HasDistinct: true,
		Aggregates: []AggregateParams{{
			Opcode: AggregateSumDistinct,
			Col:    1,
			Alias:  "sum(distinct col2)",
		}},
		Keys:  []int{0},
		Input: fp,
	}

	result, err := oa.Execute(nil, nil, false)
	assert.NoError(t, err)

	wantResult := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col1|sum(distinct col2)",
			"varbinary|decimal",
		),
		"a|1",
	)
	utils.MustMatch(t, wantResult, result, "")
}

func TestOrderedAggregateKeysFail(t *testing.T) {
	fields := sqltypes.MakeTestFields(
		"col|count(*)",
		"varchar|decimal",
	)
	fp := &fakePrimitive{
		results: []*sqltypes.Result{sqltypes.MakeTestResult(
			fields,
			"a|1",
			"a|1",
		)},
	}

	oa := &OrderedAggregate{
		Aggregates: []AggregateParams{{
			Opcode: AggregateCount,
			Col:    1,
		}},
		Keys:  []int{0},
		Input: fp,
	}

	want := "types are not comparable: VARCHAR vs VARCHAR"
	if _, err := oa.Execute(nil, nil, false); err == nil || err.Error() != want {
		t.Errorf("oa.Execute(): %v, want %s", err, want)
	}

	fp.rewind()
	if err := oa.StreamExecute(nil, nil, false, func(_ *sqltypes.Result) error { return nil }); err == nil || err.Error() != want {
		t.Errorf("oa.StreamExecute(): %v, want %s", err, want)
	}
}

func TestOrderedAggregateMergeFail(t *testing.T) {
	fields := sqltypes.MakeTestFields(
		"col|count(*)",
		"varbinary|decimal",
	)
	fp := &fakePrimitive{
		results: []*sqltypes.Result{sqltypes.MakeTestResult(
			fields,
			"a|1",
			"a|0",
		)},
	}

	oa := &OrderedAggregate{
		Aggregates: []AggregateParams{{
			Opcode: AggregateCount,
			Col:    1,
		}},
		Keys:  []int{0},
		Input: fp,
	}

	result := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "col",
				Type: querypb.Type_VARBINARY,
			},
			{
				Name: "count(*)",
				Type: querypb.Type_DECIMAL,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARBINARY, []byte("a")),
				sqltypes.MakeTrusted(querypb.Type_DECIMAL, []byte("1")),
			},
		},
	}

	res, err := oa.Execute(nil, nil, false)
	require.NoError(t, err)

	utils.MustMatch(t, result, res, "Found mismatched values")

	fp.rewind()
	err = oa.StreamExecute(nil, nil, false, func(_ *sqltypes.Result) error { return nil })
	require.NoError(t, err)
}

func TestMerge(t *testing.T) {
	assert := assert.New(t)
	oa := &OrderedAggregate{
		Aggregates: []AggregateParams{{
			Opcode: AggregateCount,
			Col:    1,
		}, {
			Opcode: AggregateSum,
			Col:    2,
		}, {
			Opcode: AggregateMin,
			Col:    3,
		}, {
			Opcode: AggregateMax,
			Col:    4,
		}},
	}
	fields := sqltypes.MakeTestFields(
		"a|b|c|d|e",
		"int64|int64|decimal|in32|varbinary",
	)
	r := sqltypes.MakeTestResult(fields,
		"1|2|3.2|3|ab",
		"1|3|2.8|2|bc",
	)

	merged, _, err := oa.merge(fields, r.Rows[0], r.Rows[1], sqltypes.NULL)
	assert.NoError(err)
	want := sqltypes.MakeTestResult(fields, "1|5|6|2|bc").Rows[0]
	assert.Equal(want, merged)

	// swap and retry
	merged, _, err = oa.merge(fields, r.Rows[1], r.Rows[0], sqltypes.NULL)
	assert.NoError(err)
	assert.Equal(want, merged)
}

func TestNoInputAndNoGroupingKeys(outer *testing.T) {
	testCases := []struct {
		name        string
		opcode      AggregateOpcode
		expectedVal string
		expectedTyp string
	}{{
		"count(distinct col1)",
		AggregateCountDistinct,
		"0",
		"int64",
	}, {
		"col1",
		AggregateCount,
		"0",
		"int64",
	}, {
		"sum(distinct col1)",
		AggregateSumDistinct,
		"null",
		"decimal",
	}, {
		"col1",
		AggregateSum,
		"null",
		"int64",
	}, {
		"col1",
		AggregateMax,
		"null",
		"int64",
	}, {
		"col1",
		AggregateMin,
		"null",
		"int64",
	}}

	for _, test := range testCases {
		outer.Run(test.name, func(t *testing.T) {
			assert := assert.New(t)
			fp := &fakePrimitive{
				results: []*sqltypes.Result{sqltypes.MakeTestResult(
					sqltypes.MakeTestFields(
						"col1",
						"int64",
					),
					// Empty input table
				)},
			}

			oa := &OrderedAggregate{
				HasDistinct: true,
				Aggregates: []AggregateParams{{
					Opcode: test.opcode,
					Col:    0,
					Alias:  test.name,
				}},
				Keys:  []int{},
				Input: fp,
			}

			result, err := oa.Execute(nil, nil, false)
			assert.NoError(err)

			wantResult := sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					test.name,
					test.expectedTyp,
				),
				test.expectedVal,
			)
			assert.Equal(wantResult, result)
		})
	}
}
