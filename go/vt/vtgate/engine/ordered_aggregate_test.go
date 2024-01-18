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
	"context"
	"errors"
	"fmt"
	"testing"

	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/utils"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	. "vitess.io/vitess/go/vt/vtgate/engine/opcode"
)

func TestOrderedAggregateExecute(t *testing.T) {
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
		Aggregates:  []*AggregateParams{NewAggregateParam(AggregateSum, 1, "", collations.MySQL8())},
		GroupByKeys: []*GroupByParams{{KeyCol: 0}},
		Input:       fp,
	}

	result, err := oa.TryExecute(context.Background(), &noopVCursor{}, nil, false)
	assert.NoError(t, err)

	wantResult := sqltypes.MakeTestResult(
		fields,
		"a|2",
		"b|2",
		"c|7",
	)
	utils.MustMatch(t, wantResult, result)
}

func TestOrderedAggregateExecuteTruncate(t *testing.T) {
	fp := &fakePrimitive{
		results: []*sqltypes.Result{sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"col|count(*)|weight_string(col)",
				"varchar|int64|varbinary",
			),
			"a|1|A",
			"A|1|A",
			"b|2|B",
			"C|3|C",
			"c|4|C",
		)},
	}

	aggr := NewAggregateParam(AggregateSum, 1, "", collations.MySQL8())
	aggr.OrigOpcode = AggregateCountStar

	oa := &OrderedAggregate{
		Aggregates:          []*AggregateParams{aggr},
		GroupByKeys:         []*GroupByParams{{KeyCol: 2}},
		TruncateColumnCount: 2,
		Input:               fp,
	}

	result, err := oa.TryExecute(context.Background(), &noopVCursor{}, nil, false)
	assert.NoError(t, err)

	wantResult := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col|count(*)",
			"varchar|int64",
		),
		"a|2",
		"b|2",
		"C|7",
	)
	utils.MustMatch(t, wantResult, result)
}

func TestMinMaxFailsCorrectly(t *testing.T) {
	fp := &fakePrimitive{
		results: []*sqltypes.Result{sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"col|weight_string(col)",
				"varchar|varbinary",
			),
			"a|A",
			"A|A",
			"b|B",
			"C|C",
			"c|C",
		)},
	}

	aggr := NewAggregateParam(AggregateMax, 0, "", collations.MySQL8())
	aggr.WCol = 1
	oa := &ScalarAggregate{
		Aggregates:          []*AggregateParams{aggr},
		TruncateColumnCount: 1,
		Input:               fp,
	}

	_, err := oa.TryExecute(context.Background(), &noopVCursor{}, nil, false)
	assert.ErrorContains(t, err, "min/max on types that are not comparable is not supported")
}

func TestOrderedAggregateStreamExecute(t *testing.T) {
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
		Aggregates:  []*AggregateParams{NewAggregateParam(AggregateSum, 1, "", collations.MySQL8())},
		GroupByKeys: []*GroupByParams{{KeyCol: 0}},
		Input:       fp,
	}

	var results []*sqltypes.Result
	err := oa.TryStreamExecute(context.Background(), &noopVCursor{}, nil, true, func(qr *sqltypes.Result) error {
		results = append(results, qr)
		return nil
	})
	assert.NoError(t, err)

	wantResults := sqltypes.MakeTestStreamingResults(
		fields,
		"a|2",
		"---",
		"b|2",
		"---",
		"c|7",
	)
	utils.MustMatch(t, wantResults, results)
}

func TestOrderedAggregateStreamExecuteTruncate(t *testing.T) {
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
		Aggregates:          []*AggregateParams{NewAggregateParam(AggregateSum, 1, "", collations.MySQL8())},
		GroupByKeys:         []*GroupByParams{{KeyCol: 2}},
		TruncateColumnCount: 2,
		Input:               fp,
	}

	var results []*sqltypes.Result
	err := oa.TryStreamExecute(context.Background(), &noopVCursor{}, nil, true, func(qr *sqltypes.Result) error {
		results = append(results, qr)
		return nil
	})
	assert.NoError(t, err)

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
	utils.MustMatch(t, wantResults, results)
}

func TestOrderedAggregateGetFields(t *testing.T) {
	input := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col|count(*)",
			"varbinary|decimal",
		),
	)
	fp := &fakePrimitive{results: []*sqltypes.Result{input}}

	oa := &OrderedAggregate{Input: fp}

	got, err := oa.GetFields(context.Background(), nil, nil)
	assert.NoError(t, err)
	assert.Equal(t, got, input)
}

func TestOrderedAggregateInputFail(t *testing.T) {
	fp := &fakePrimitive{sendErr: errors.New("input fail")}

	oa := &OrderedAggregate{Input: fp}

	want := "input fail"
	if _, err := oa.TryExecute(context.Background(), &noopVCursor{}, nil, false); err == nil || err.Error() != want {
		t.Errorf("oa.Execute(): %v, want %s", err, want)
	}

	fp.rewind()
	if err := oa.TryStreamExecute(context.Background(), &noopVCursor{}, nil, false, func(_ *sqltypes.Result) error { return nil }); err == nil || err.Error() != want {
		t.Errorf("oa.StreamExecute(): %v, want %s", err, want)
	}

	fp.rewind()
	if _, err := oa.GetFields(context.Background(), nil, nil); err == nil || err.Error() != want {
		t.Errorf("oa.GetFields(): %v, want %s", err, want)
	}
}

func TestOrderedAggregateExecuteCountDistinct(t *testing.T) {
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

	aggr1 := NewAggregateParam(AggregateCountDistinct, 1, "count(distinct col2)", collations.MySQL8())
	aggr2 := NewAggregateParam(AggregateSum, 2, "", collations.MySQL8())
	aggr2.OrigOpcode = AggregateCountStar
	oa := &OrderedAggregate{
		Aggregates:  []*AggregateParams{aggr1, aggr2},
		GroupByKeys: []*GroupByParams{{KeyCol: 0}},
		Input:       fp,
	}

	result, err := oa.TryExecute(context.Background(), &noopVCursor{}, nil, false)
	assert.NoError(t, err)

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
	utils.MustMatch(t, wantResult, result)
}

func TestOrderedAggregateStreamCountDistinct(t *testing.T) {
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

	aggr2 := NewAggregateParam(AggregateSum, 2, "", collations.MySQL8())
	aggr2.OrigOpcode = AggregateCountDistinct

	oa := &OrderedAggregate{
		Aggregates: []*AggregateParams{
			NewAggregateParam(AggregateCountDistinct, 1, "count(distinct col2)", collations.MySQL8()),
			aggr2},
		GroupByKeys: []*GroupByParams{{KeyCol: 0}},
		Input:       fp,
	}

	var results []*sqltypes.Result
	err := oa.TryStreamExecute(context.Background(), &noopVCursor{}, nil, true, func(qr *sqltypes.Result) error {
		results = append(results, qr)
		return nil
	})
	assert.NoError(t, err)

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
	utils.MustMatch(t, wantResults, results)
}

func TestOrderedAggregateSumDistinctGood(t *testing.T) {
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
			"d|1|1",
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
		Aggregates: []*AggregateParams{
			NewAggregateParam(AggregateSumDistinct, 1, "sum(distinct col2)", collations.MySQL8()),
			NewAggregateParam(AggregateSum, 2, "", collations.MySQL8()),
		},
		GroupByKeys: []*GroupByParams{{KeyCol: 0}},
		Input:       fp,
	}

	result, err := oa.TryExecute(context.Background(), &noopVCursor{}, nil, false)
	assert.NoError(t, err)

	wantResult := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col1|sum(distinct col2)|sum(col3)",
			"varbinary|decimal|decimal",
		),
		"a|1|3",
		"b|1|1",
		"c|7|2",
		"d|1|2",
		"e|1|2",
		"f|1|2",
		"g|6|4",
		"h|6|4",
		"i|7|2",
	)
	want := fmt.Sprintf("%v", wantResult.Rows)
	got := fmt.Sprintf("%v", result.Rows)
	assert.Equal(t, want, got)
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
		Aggregates:  []*AggregateParams{NewAggregateParam(AggregateSumDistinct, 1, "sum(distinct col2)", collations.MySQL8())},
		GroupByKeys: []*GroupByParams{{KeyCol: 0}},
		Input:       fp,
	}

	result, err := oa.TryExecute(context.Background(), &noopVCursor{}, nil, false)
	assert.NoError(t, err)

	wantResult := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col1|sum(distinct col2)",
			"varbinary|float64",
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
		Aggregates:  []*AggregateParams{NewAggregateParam(AggregateSum, 1, "", collations.MySQL8())},
		GroupByKeys: []*GroupByParams{{KeyCol: 0}},
		Input:       fp,
	}

	want := "cannot compare strings, collation is unknown or unsupported (collation ID: 0)"
	if _, err := oa.TryExecute(context.Background(), &noopVCursor{}, nil, false); err == nil || err.Error() != want {
		t.Errorf("oa.Execute(): %v, want %s", err, want)
	}

	fp.rewind()
	if err := oa.TryStreamExecute(context.Background(), &noopVCursor{}, nil, false, func(_ *sqltypes.Result) error { return nil }); err == nil || err.Error() != want {
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
		Aggregates:  []*AggregateParams{NewAggregateParam(AggregateSum, 1, "", collations.MySQL8())},
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

	res, err := oa.TryExecute(context.Background(), &noopVCursor{}, nil, false)
	require.NoError(t, err)

	utils.MustMatch(t, result, res, "Found mismatched values")

	fp.rewind()
	err = oa.TryStreamExecute(context.Background(), &noopVCursor{}, nil, true, func(_ *sqltypes.Result) error { return nil })
	require.NoError(t, err)
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
		Aggregates:          []*AggregateParams{NewAggregateParam(AggregateGtid, 1, "vgtid", collations.MySQL8())},
		TruncateColumnCount: 2,
		Input:               fp,
	}

	result, err := oa.TryExecute(context.Background(), &noopVCursor{}, nil, false)
	require.NoError(t, err)

	wantResult := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"keyspace|vgtid",
			"varchar|varchar",
		),
		`ks|shard_gtids:{keyspace:"ks" shard:"-40" gtid:"a"} shard_gtids:{keyspace:"ks" shard:"40-80" gtid:"b"} shard_gtids:{keyspace:"ks" shard:"80-c0" gtid:"c"} shard_gtids:{keyspace:"ks" shard:"c0-" gtid:"d"}`,
	)
	utils.MustMatch(t, wantResult, result)
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

	aggr := NewAggregateParam(AggregateCountDistinct, 1, "count(distinct c2)", collations.MySQL8())
	aggr.WCol = 2
	oa := &OrderedAggregate{
		Aggregates:          []*AggregateParams{aggr},
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

	qr, err := oa.TryExecute(context.Background(), &noopVCursor{}, nil, false)
	require.NoError(t, err)
	utils.MustMatch(t, want, qr)

	fp.rewind()
	results := &sqltypes.Result{}
	err = oa.TryStreamExecute(context.Background(), &noopVCursor{}, nil, true, func(qr *sqltypes.Result) error {
		if qr.Fields != nil {
			results.Fields = qr.Fields
		}
		results.Rows = append(results.Rows, qr.Rows...)
		return nil
	})
	require.NoError(t, err)
	utils.MustMatch(t, want, results)
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

	aggr := NewAggregateParam(AggregateCountDistinct, 1, "count(distinct c2)", collations.MySQL8())
	aggr.WCol = 2
	oa := &OrderedAggregate{
		Aggregates:          []*AggregateParams{aggr},
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

	qr, err := oa.TryExecute(context.Background(), &noopVCursor{}, nil, false)
	require.NoError(t, err)
	utils.MustMatch(t, want, qr)

	fp.rewind()
	results := &sqltypes.Result{}
	err = oa.TryStreamExecute(context.Background(), &noopVCursor{}, nil, true, func(qr *sqltypes.Result) error {
		if qr.Fields != nil {
			results.Fields = qr.Fields
		}
		results.Rows = append(results.Rows, qr.Rows...)
		return nil
	})
	require.NoError(t, err)
	utils.MustMatch(t, want, results)
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

	aggr := NewAggregateParam(AggregateSumDistinct, 1, "sum(distinct c2)", collations.MySQL8())
	aggr.WCol = 2
	oa := &OrderedAggregate{
		Aggregates:          []*AggregateParams{aggr},
		GroupByKeys:         []*GroupByParams{{KeyCol: 0}},
		Input:               fp,
		TruncateColumnCount: 2,
	}

	want := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"c1|sum(distinct c2)",
			"int64|float64",
		),
		`null|0`,
		`10|0`,
		`20|0`,
		`30|null`,
	)

	qr, err := oa.TryExecute(context.Background(), &noopVCursor{}, nil, false)
	require.NoError(t, err)
	utils.MustMatch(t, want, qr)

	fp.rewind()
	results := &sqltypes.Result{}
	err = oa.TryStreamExecute(context.Background(), &noopVCursor{}, nil, true, func(qr *sqltypes.Result) error {
		if qr.Fields != nil {
			results.Fields = qr.Fields
		}
		results.Rows = append(results.Rows, qr.Rows...)
		return nil
	})
	require.NoError(t, err)
	utils.MustMatch(t, want, results)
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
		Aggregates: []*AggregateParams{
			NewAggregateParam(AggregateCountDistinct, 1, "count(distinct c2)", collations.MySQL8()),
			NewAggregateParam(AggregateSumDistinct, 2, "sum(distinct c3)", collations.MySQL8()),
		},
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

	qr, err := oa.TryExecute(context.Background(), &noopVCursor{}, nil, false)
	require.NoError(t, err)
	utils.MustMatch(t, want, qr)

	fp.rewind()
	results := &sqltypes.Result{}
	err = oa.TryStreamExecute(context.Background(), &noopVCursor{}, nil, true, func(qr *sqltypes.Result) error {
		if qr.Fields != nil {
			results.Fields = qr.Fields
		}
		results.Rows = append(results.Rows, qr.Rows...)
		return nil
	})
	require.NoError(t, err)
	utils.MustMatch(t, want, results)
}

func TestOrderedAggregateCollate(t *testing.T) {
	fields := sqltypes.MakeTestFields(
		"col|count(*)",
		"varchar|decimal",
	)
	fp := &fakePrimitive{
		results: []*sqltypes.Result{sqltypes.MakeTestResult(
			fields,
			"a|1",
			"A|1",
			"Ǎ|1",
			"b|2",
			"B|-1",
			"c|3",
			"c|4",
			"ß|11",
			"ss|2",
		)},
	}

	collationEnv := collations.MySQL8()
	collationID, _ := collationEnv.LookupID("utf8mb4_0900_ai_ci")
	oa := &OrderedAggregate{
		Aggregates:  []*AggregateParams{NewAggregateParam(AggregateSum, 1, "", collationEnv)},
		GroupByKeys: []*GroupByParams{{KeyCol: 0, Type: evalengine.NewType(sqltypes.Unknown, collationID)}},
		Input:       fp,
	}

	result, err := oa.TryExecute(context.Background(), &noopVCursor{}, nil, false)
	assert.NoError(t, err)

	wantResult := sqltypes.MakeTestResult(
		fields,
		"a|3",
		"b|1",
		"c|7",
		"ß|13",
	)
	utils.MustMatch(t, wantResult, result)
}

func TestOrderedAggregateCollateAS(t *testing.T) {
	fields := sqltypes.MakeTestFields(
		"col|count(*)",
		"varchar|decimal",
	)
	fp := &fakePrimitive{
		results: []*sqltypes.Result{sqltypes.MakeTestResult(
			fields,
			"a|1",
			"A|1",
			"Ǎ|1",
			"b|2",
			"c|3",
			"c|4",
			"Ç|4",
		)},
	}

	collationEnv := collations.MySQL8()
	collationID, _ := collationEnv.LookupID("utf8mb4_0900_as_ci")
	oa := &OrderedAggregate{
		Aggregates:  []*AggregateParams{NewAggregateParam(AggregateSum, 1, "", collationEnv)},
		GroupByKeys: []*GroupByParams{{KeyCol: 0, Type: evalengine.NewType(sqltypes.Unknown, collationID)}},
		Input:       fp,
	}

	result, err := oa.TryExecute(context.Background(), &noopVCursor{}, nil, false)
	assert.NoError(t, err)

	wantResult := sqltypes.MakeTestResult(
		fields,
		"a|2",
		"Ǎ|1",
		"b|2",
		"c|7",
		"Ç|4",
	)
	utils.MustMatch(t, wantResult, result)
}

func TestOrderedAggregateCollateKS(t *testing.T) {
	fields := sqltypes.MakeTestFields(
		"col|count(*)",
		"varchar|decimal",
	)
	fp := &fakePrimitive{
		results: []*sqltypes.Result{sqltypes.MakeTestResult(
			fields,
			"a|1",
			"A|1",
			"Ǎ|1",
			"b|2",
			"c|3",
			"c|4",
			"\xE3\x83\x8F\xE3\x81\xAF|2",
			"\xE3\x83\x8F\xE3\x83\x8F|1",
		)},
	}

	collationEnv := collations.MySQL8()
	collationID, _ := collationEnv.LookupID("utf8mb4_ja_0900_as_cs_ks")
	oa := &OrderedAggregate{
		Aggregates:  []*AggregateParams{NewAggregateParam(AggregateSum, 1, "", collationEnv)},
		GroupByKeys: []*GroupByParams{{KeyCol: 0, Type: evalengine.NewType(sqltypes.Unknown, collationID)}},
		Input:       fp,
	}

	result, err := oa.TryExecute(context.Background(), &noopVCursor{}, nil, false)
	assert.NoError(t, err)

	wantResult := sqltypes.MakeTestResult(
		fields,
		"a|1",
		"A|1",
		"Ǎ|1",
		"b|2",
		"c|7",
		"\xE3\x83\x8F\xE3\x81\xAF|2",
		"\xE3\x83\x8F\xE3\x83\x8F|1",
	)
	utils.MustMatch(t, wantResult, result)
}

// TestGroupConcatWithAggrOnEngine tests group_concat with full aggregation on engine.
func TestGroupConcatWithAggrOnEngine(t *testing.T) {
	fields := sqltypes.MakeTestFields(
		"c1|c2",
		"int64|varchar",
	)

	varbinaryFields := sqltypes.MakeTestFields(
		"c1|c2",
		"int64|varbinary",
	)

	textOutFields := sqltypes.MakeTestFields(
		"c1|group_concat(c2)",
		"int64|text",
	)

	var tcases = []struct {
		name        string
		inputResult *sqltypes.Result
		expResult   *sqltypes.Result
	}{{
		name: "multiple grouping keys",
		inputResult: sqltypes.MakeTestResult(fields,
			"10|a", "10|a", "10|b",
			"20|b",
			"30|null",
			"40|null", "40|c",
			"50|d", "50|null", "50|a", "50|", "50|"),
		expResult: sqltypes.MakeTestResult(textOutFields,
			`10|a,a,b`,
			`20|b`,
			`30|null`,
			`40|c`,
			`50|d,a,,`),
	}, {
		name:        "empty result",
		inputResult: sqltypes.MakeTestResult(fields),
		expResult:   sqltypes.MakeTestResult(textOutFields),
	}, {
		name: "null value for concat",
		inputResult: sqltypes.MakeTestResult(fields,
			"42|null", "42|null", "42|null"),
		expResult: sqltypes.MakeTestResult(textOutFields,
			`42|null`),
	}, {
		name: "concat on varbinary column",
		inputResult: sqltypes.MakeTestResult(varbinaryFields,
			"42|a", "42|b", "42|c"),
		expResult: sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"c1|group_concat(c2)",
				"int64|blob",
			),
			`42|a,b,c`),
	}}

	for _, tcase := range tcases {
		t.Run(tcase.name, func(t *testing.T) {
			fp := &fakePrimitive{results: []*sqltypes.Result{tcase.inputResult}}
			oa := &OrderedAggregate{
				Aggregates:  []*AggregateParams{NewAggregateParam(AggregateGroupConcat, 1, "group_concat(c2)", collations.MySQL8())},
				GroupByKeys: []*GroupByParams{{KeyCol: 0}},
				Input:       fp,
			}
			qr, err := oa.TryExecute(context.Background(), &noopVCursor{}, nil, false)
			require.NoError(t, err)
			if len(qr.Rows) == 0 {
				qr.Rows = nil // just to make the expectation.
				// empty slice or nil both are valid and will not cause any issue.
			}
			utils.MustMatch(t, tcase.expResult, qr)

			fp.rewind()
			results := &sqltypes.Result{}
			err = oa.TryStreamExecute(context.Background(), &noopVCursor{}, nil, true, func(qr *sqltypes.Result) error {
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

// TestGroupConcat tests group_concat with partial aggregation on engine.
func TestGroupConcat(t *testing.T) {
	fields := sqltypes.MakeTestFields(
		"c1|group_concat(c2)",
		"int64|text",
	)

	varbinaryFields := sqltypes.MakeTestFields(
		"c1|group_concat(c2)",
		"int64|blob",
	)

	var tcases = []struct {
		name        string
		inputResult *sqltypes.Result
		expResult   *sqltypes.Result
	}{{
		name: "multiple grouping keys",
		inputResult: sqltypes.MakeTestResult(fields,
			"10|a", "10|a", "10|b",
			"20|b",
			"30|null",
			"40|null", "40|c",
			"50|d", "50|null", "50|a", "50|", "50|"),
		expResult: sqltypes.MakeTestResult(fields,
			`10|a,a,b`,
			`20|b`,
			`30|null`,
			`40|c`,
			`50|d,a,,`),
	}, {
		name:        "empty result",
		inputResult: sqltypes.MakeTestResult(fields),
		expResult:   sqltypes.MakeTestResult(fields),
	}, {
		name: "null value for concat",
		inputResult: sqltypes.MakeTestResult(fields,
			"42|null", "42|null", "42|null"),
		expResult: sqltypes.MakeTestResult(fields,
			`42|null`),
	}, {
		name: "concat on varbinary column",
		inputResult: sqltypes.MakeTestResult(varbinaryFields,
			"42|a", "42|b", "42|c"),
		expResult: sqltypes.MakeTestResult(varbinaryFields,
			`42|a,b,c`),
	}}

	for _, tcase := range tcases {
		t.Run(tcase.name, func(t *testing.T) {
			fp := &fakePrimitive{results: []*sqltypes.Result{tcase.inputResult}}
			oa := &OrderedAggregate{
				Aggregates:  []*AggregateParams{NewAggregateParam(AggregateGroupConcat, 1, "", collations.MySQL8())},
				GroupByKeys: []*GroupByParams{{KeyCol: 0}},
				Input:       fp,
			}
			qr, err := oa.TryExecute(context.Background(), &noopVCursor{}, nil, false)
			require.NoError(t, err)
			if len(qr.Rows) == 0 {
				qr.Rows = nil // just to make the expectation.
				// empty slice or nil both are valid and will not cause any issue.
			}
			assert.Equal(t, tcase.expResult, qr)

			fp.rewind()
			results := &sqltypes.Result{}
			err = oa.TryStreamExecute(context.Background(), &noopVCursor{}, nil, true, func(qr *sqltypes.Result) error {
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
