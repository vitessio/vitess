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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/utils"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
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
		Aggregates: []*AggregateParams{{
			Opcode: AggregateCount,
			Col:    1,
		}},
		GroupByKeys: []*GroupByParams{{KeyCol: 0}},
		Input:       fp,
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
		Aggregates: []*AggregateParams{{
			Opcode: AggregateCount,
			Col:    1,
		}},
		GroupByKeys:         []*GroupByParams{{KeyCol: 2}},
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
		Aggregates: []*AggregateParams{{
			Opcode: AggregateCount,
			Col:    1,
		}},
		GroupByKeys: []*GroupByParams{{KeyCol: 0}},
		Input:       fp,
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
		Aggregates: []*AggregateParams{{
			Opcode: AggregateCount,
			Col:    1,
		}},
		GroupByKeys:         []*GroupByParams{{KeyCol: 2}},
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
		PreProcess: true,
		Aggregates: []*AggregateParams{{
			Opcode: AggregateCountDistinct,
			Col:    1,
			Alias:  "count(distinct col2)",
		}, {
			// Also add a count(*)
			Opcode: AggregateCount,
			Col:    2,
		}},
		GroupByKeys: []*GroupByParams{{KeyCol: 0}},
		Input:       fp,
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
		PreProcess: true,
		Aggregates: []*AggregateParams{{
			Opcode: AggregateCountDistinct,
			Col:    1,
			Alias:  "count(distinct col2)",
		}, {
			// Also add a count(*)
			Opcode: AggregateCount,
			Col:    2,
		}},
		GroupByKeys: []*GroupByParams{{KeyCol: 0}},
		Input:       fp,
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
		PreProcess: true,
		Aggregates: []*AggregateParams{{
			Opcode: AggregateSumDistinct,
			Col:    1,
			Alias:  "sum(distinct col2)",
		}, {
			// Also add a count(*)
			Opcode: AggregateSum,
			Col:    2,
		}},
		GroupByKeys: []*GroupByParams{{KeyCol: 0}},
		Input:       fp,
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
		PreProcess: true,
		Aggregates: []*AggregateParams{{
			Opcode: AggregateSumDistinct,
			Col:    1,
			Alias:  "sum(distinct col2)",
		}},
		GroupByKeys: []*GroupByParams{{KeyCol: 0}},
		Input:       fp,
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
		Aggregates: []*AggregateParams{{
			Opcode: AggregateCount,
			Col:    1,
		}},
		GroupByKeys: []*GroupByParams{{KeyCol: 0}},
		Input:       fp,
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
		Aggregates: []*AggregateParams{{
			Opcode: AggregateCount,
			Col:    1,
		}},
		GroupByKeys: []*GroupByParams{{KeyCol: 0}},
		Input:       fp,
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
		Aggregates: []*AggregateParams{{
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

	merged, _, err := oa.merge(fields, r.Rows[0], r.Rows[1], nil)
	assert.NoError(err)
	want := sqltypes.MakeTestResult(fields, "1|5|6|2|bc").Rows[0]
	assert.Equal(want, merged)

	// swap and retry
	merged, _, err = oa.merge(fields, r.Rows[1], r.Rows[0], nil)
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
				PreProcess: true,
				Aggregates: []*AggregateParams{{
					Opcode: test.opcode,
					Col:    0,
					Alias:  test.name,
				}},
				GroupByKeys: []*GroupByParams{},
				Input:       fp,
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

func TestOrderedAggregateExecuteGtid(t *testing.T) {
	vgtid := binlogdatapb.VGtid{}
	vgtid.ShardGtids = append(vgtid.ShardGtids, &binlogdatapb.ShardGtid{
		Keyspace: "ks",
		Shard:    "-80",
		Gtid:     "a",
	})
	vgtid.ShardGtids = append(vgtid.ShardGtids, &binlogdatapb.ShardGtid{
		Keyspace: "ks",
		Shard:    "80-",
		Gtid:     "b",
	})
	fmt.Println(vgtid.String())

	fp := &fakePrimitive{
		results: []*sqltypes.Result{sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"keyspace|gtid|shard",
				"varchar|varchar|varchar",
			),
			"ks|a|-40",
			"ks|b|40-80",
			"ks|c|80-c0",
			"ks|d|c0-",
		)},
	}

	oa := &OrderedAggregate{
		PreProcess: true,
		Aggregates: []*AggregateParams{{
			Opcode: AggregateGtid,
			Col:    1,
			Alias:  "vgtid",
		}},
		TruncateColumnCount: 2,
		Input:               fp,
	}

	result, err := oa.Execute(nil, nil, false)
	require.NoError(t, err)

	wantResult := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"keyspace|vgtid",
			"varchar|varchar",
		),
		`ks|shard_gtids:{keyspace:"ks" shard:"-40" gtid:"a"} shard_gtids:{keyspace:"ks" shard:"40-80" gtid:"b"} shard_gtids:{keyspace:"ks" shard:"80-c0" gtid:"c"} shard_gtids:{keyspace:"ks" shard:"c0-" gtid:"d"}`,
	)
	assert.Equal(t, wantResult, result)
}

func TestCountDistinctOnVarchar(t *testing.T) {
	fields := sqltypes.MakeTestFields(
		"c1|c2|weight_string(c2)",
		"int64|varchar|varbinary",
	)
	fp := &fakePrimitive{
		results: []*sqltypes.Result{sqltypes.MakeTestResult(
			fields,
			"10|a|0x41",
			"10|a|0x41",
			"10|b|0x42",
			"20|b|0x42",
		)},
	}

	oa := &OrderedAggregate{
		PreProcess: true,
		Aggregates: []*AggregateParams{{
			Opcode:    AggregateCountDistinct,
			Col:       1,
			WCol:      2,
			WAssigned: true,
			Alias:     "count(distinct c2)",
		}},
		GroupByKeys:         []*GroupByParams{{KeyCol: 0}},
		Input:               fp,
		TruncateColumnCount: 2,
	}

	want := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"c1|count(distinct c2)",
			"int64|int64",
		),
		`10|2`,
		`20|1`,
	)

	qr, err := oa.Execute(nil, nil, false)
	require.NoError(t, err)
	assert.Equal(t, want, qr)

	fp.rewind()
	results := &sqltypes.Result{}
	err = oa.StreamExecute(nil, nil, false, func(qr *sqltypes.Result) error {
		if qr.Fields != nil {
			results.Fields = qr.Fields
		}
		results.Rows = append(results.Rows, qr.Rows...)
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, want, results)
}

func TestCountDistinctOnVarcharWithNulls(t *testing.T) {
	fields := sqltypes.MakeTestFields(
		"c1|c2|weight_string(c2)",
		"int64|varchar|varbinary",
	)
	fp := &fakePrimitive{
		results: []*sqltypes.Result{sqltypes.MakeTestResult(
			fields,
			"null|null|null",
			"null|a|0x41",
			"null|b|0x42",
			"10|null|null",
			"10|null|null",
			"10|a|0x41",
			"10|a|0x41",
			"10|b|0x42",
			"20|null|null",
			"20|b|0x42",
			"30|null|null",
			"30|null|null",
			"30|null|null",
			"30|null|null",
		)},
	}

	oa := &OrderedAggregate{
		PreProcess: true,
		Aggregates: []*AggregateParams{{
			Opcode:    AggregateCountDistinct,
			Col:       1,
			WCol:      2,
			WAssigned: true,
			Alias:     "count(distinct c2)",
		}},
		GroupByKeys:         []*GroupByParams{{KeyCol: 0}},
		Input:               fp,
		TruncateColumnCount: 2,
	}

	want := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"c1|count(distinct c2)",
			"int64|int64",
		),
		`null|2`,
		`10|2`,
		`20|1`,
		`30|0`,
	)

	qr, err := oa.Execute(nil, nil, false)
	require.NoError(t, err)
	assert.Equal(t, want, qr)

	fp.rewind()
	results := &sqltypes.Result{}
	err = oa.StreamExecute(nil, nil, false, func(qr *sqltypes.Result) error {
		if qr.Fields != nil {
			results.Fields = qr.Fields
		}
		results.Rows = append(results.Rows, qr.Rows...)
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, want, results)
}

func TestSumDistinctOnVarcharWithNulls(t *testing.T) {
	fields := sqltypes.MakeTestFields(
		"c1|c2|weight_string(c2)",
		"int64|varchar|varbinary",
	)
	fp := &fakePrimitive{
		results: []*sqltypes.Result{sqltypes.MakeTestResult(
			fields,
			"null|null|null",
			"null|a|0x41",
			"null|b|0x42",
			"10|null|null",
			"10|null|null",
			"10|a|0x41",
			"10|a|0x41",
			"10|b|0x42",
			"20|null|null",
			"20|b|0x42",
			"30|null|null",
			"30|null|null",
			"30|null|null",
			"30|null|null",
		)},
	}

	oa := &OrderedAggregate{
		PreProcess: true,
		Aggregates: []*AggregateParams{{
			Opcode:    AggregateSumDistinct,
			Col:       1,
			WCol:      2,
			WAssigned: true,
			Alias:     "sum(distinct c2)",
		}},
		GroupByKeys:         []*GroupByParams{{KeyCol: 0}},
		Input:               fp,
		TruncateColumnCount: 2,
	}

	want := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"c1|sum(distinct c2)",
			"int64|decimal",
		),
		`null|0`,
		`10|0`,
		`20|0`,
		`30|null`,
	)

	qr, err := oa.Execute(nil, nil, false)
	require.NoError(t, err)
	assert.Equal(t, want, qr)

	fp.rewind()
	results := &sqltypes.Result{}
	err = oa.StreamExecute(nil, nil, false, func(qr *sqltypes.Result) error {
		if qr.Fields != nil {
			results.Fields = qr.Fields
		}
		results.Rows = append(results.Rows, qr.Rows...)
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, want, results)
}

func TestMultiDistinct(t *testing.T) {
	fields := sqltypes.MakeTestFields(
		"c1|c2|c3",
		"int64|int64|int64",
	)
	fp := &fakePrimitive{
		results: []*sqltypes.Result{sqltypes.MakeTestResult(
			fields,
			"null|null|null",
			"null|1|2",
			"null|2|2",
			"10|null|null",
			"10|2|null",
			"10|2|1",
			"10|2|3",
			"10|3|3",
			"20|null|null",
			"20|null|null",
			"30|1|1",
			"30|1|2",
			"30|1|3",
			"40|1|1",
			"40|2|1",
			"40|3|1",
		)},
	}

	oa := &OrderedAggregate{
		PreProcess: true,
		Aggregates: []*AggregateParams{{
			Opcode: AggregateCountDistinct,
			Col:    1,
			Alias:  "count(distinct c2)",
		}, {
			Opcode: AggregateSumDistinct,
			Col:    2,
			Alias:  "sum(distinct c3)",
		}},
		GroupByKeys: []*GroupByParams{{KeyCol: 0}},
		Input:       fp,
	}

	want := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"c1|count(distinct c2)|sum(distinct c3)",
			"int64|int64|decimal",
		),
		`null|2|2`,
		`10|2|4`,
		`20|0|null`,
		`30|1|6`,
		`40|3|1`,
	)

	qr, err := oa.Execute(nil, nil, false)
	require.NoError(t, err)
	assert.Equal(t, want, qr)

	fp.rewind()
	results := &sqltypes.Result{}
	err = oa.StreamExecute(nil, nil, false, func(qr *sqltypes.Result) error {
		if qr.Fields != nil {
			results.Fields = qr.Fields
		}
		results.Rows = append(results.Rows, qr.Rows...)
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, want, results)
}
