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

package wrangler

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/topo"

	"vitess.io/vitess/go/vt/sqlparser"

	"context"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

func TestVDiffPlanSuccess(t *testing.T) {
	schm := &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{{
			Name:              "t1",
			Columns:           []string{"c1", "c2"},
			PrimaryKeyColumns: []string{"c1"},
			Fields:            sqltypes.MakeTestFields("c1|c2", "int64|int64"),
		}, {
			Name:              "nonpktext",
			Columns:           []string{"c1", "textcol"},
			PrimaryKeyColumns: []string{"c1"},
			Fields:            sqltypes.MakeTestFields("c1|textcol", "int64|varchar"),
		}, {
			Name:              "pktext",
			Columns:           []string{"textcol", "c2"},
			PrimaryKeyColumns: []string{"textcol"},
			Fields:            sqltypes.MakeTestFields("textcol|c2", "varchar|int64"),
		}, {
			Name:              "multipk",
			Columns:           []string{"c1", "c2"},
			PrimaryKeyColumns: []string{"c1", "c2"},
			Fields:            sqltypes.MakeTestFields("c1|c2", "int64|int64"),
		}, {
			Name:              "aggr",
			Columns:           []string{"c1", "c2", "c3", "c4"},
			PrimaryKeyColumns: []string{"c1"},
			Fields:            sqltypes.MakeTestFields("c1|c2|c3|c4", "int64|int64|int64|int64"),
		}},
	}

	testcases := []struct {
		input *binlogdatapb.Rule
		table string
		td    *tableDiffer
	}{{
		input: &binlogdatapb.Rule{
			Match: "t1",
		},
		table: "t1",
		td: &tableDiffer{
			targetTable:      "t1",
			sourceExpression: "select c1, c2 from t1 order by c1 asc",
			targetExpression: "select c1, c2 from t1 order by c1 asc",
			compareCols:      []int{-1, 1},
			comparePKs:       []int{0},
			sourcePrimitive:  newMergeSorter(nil, []int{0}),
			targetPrimitive:  newMergeSorter(nil, []int{0}),
		},
	}, {
		input: &binlogdatapb.Rule{
			Match:  "t1",
			Filter: "-80",
		},
		table: "t1",
		td: &tableDiffer{
			targetTable:      "t1",
			sourceExpression: "select c1, c2 from t1 order by c1 asc",
			targetExpression: "select c1, c2 from t1 order by c1 asc",
			compareCols:      []int{-1, 1},
			comparePKs:       []int{0},
			sourcePrimitive:  newMergeSorter(nil, []int{0}),
			targetPrimitive:  newMergeSorter(nil, []int{0}),
		},
	}, {
		input: &binlogdatapb.Rule{
			Match:  "t1",
			Filter: "select * from t1",
		},
		table: "t1",
		td: &tableDiffer{
			targetTable:      "t1",
			sourceExpression: "select c1, c2 from t1 order by c1 asc",
			targetExpression: "select c1, c2 from t1 order by c1 asc",
			compareCols:      []int{-1, 1},
			comparePKs:       []int{0},
			sourcePrimitive:  newMergeSorter(nil, []int{0}),
			targetPrimitive:  newMergeSorter(nil, []int{0}),
		},
	}, {
		input: &binlogdatapb.Rule{
			Match:  "t1",
			Filter: "select c2, c1 from t1",
		},
		table: "t1",
		td: &tableDiffer{
			targetTable:      "t1",
			sourceExpression: "select c2, c1 from t1 order by c1 asc",
			targetExpression: "select c2, c1 from t1 order by c1 asc",
			compareCols:      []int{0, -1},
			comparePKs:       []int{1},
			sourcePrimitive:  newMergeSorter(nil, []int{1}),
			targetPrimitive:  newMergeSorter(nil, []int{1}),
		},
	}, {
		input: &binlogdatapb.Rule{
			Match:  "t1",
			Filter: "select c0 as c1, c2 from t2",
		},
		table: "t1",
		td: &tableDiffer{
			targetTable:      "t1",
			sourceExpression: "select c0 as c1, c2 from t2 order by c1 asc",
			targetExpression: "select c1, c2 from t1 order by c1 asc",
			compareCols:      []int{-1, 1},
			comparePKs:       []int{0},
			sourcePrimitive:  newMergeSorter(nil, []int{0}),
			targetPrimitive:  newMergeSorter(nil, []int{0}),
		},
	}, {
		// non-pk text column.
		input: &binlogdatapb.Rule{
			Match:  "nonpktext",
			Filter: "select c1, textcol from nonpktext",
		},
		table: "nonpktext",
		td: &tableDiffer{
			targetTable:      "nonpktext",
			sourceExpression: "select c1, textcol, weight_string(textcol) from nonpktext order by c1 asc",
			targetExpression: "select c1, textcol, weight_string(textcol) from nonpktext order by c1 asc",
			compareCols:      []int{-1, 2},
			comparePKs:       []int{0},
			sourcePrimitive:  newMergeSorter(nil, []int{0}),
			targetPrimitive:  newMergeSorter(nil, []int{0}),
		},
	}, {
		// non-pk text column, different order.
		input: &binlogdatapb.Rule{
			Match:  "nonpktext",
			Filter: "select textcol, c1 from nonpktext",
		},
		table: "nonpktext",
		td: &tableDiffer{
			targetTable:      "nonpktext",
			sourceExpression: "select textcol, c1, weight_string(textcol) from nonpktext order by c1 asc",
			targetExpression: "select textcol, c1, weight_string(textcol) from nonpktext order by c1 asc",
			compareCols:      []int{2, -1},
			comparePKs:       []int{1},
			sourcePrimitive:  newMergeSorter(nil, []int{1}),
			targetPrimitive:  newMergeSorter(nil, []int{1}),
		},
	}, {
		// pk text column.
		input: &binlogdatapb.Rule{
			Match:  "pktext",
			Filter: "select textcol, c2 from pktext",
		},
		table: "pktext",
		td: &tableDiffer{
			targetTable:      "pktext",
			sourceExpression: "select textcol, c2, weight_string(textcol) from pktext order by textcol asc",
			targetExpression: "select textcol, c2, weight_string(textcol) from pktext order by textcol asc",
			compareCols:      []int{-1, 1},
			comparePKs:       []int{2},
			sourcePrimitive:  newMergeSorter(nil, []int{2}),
			targetPrimitive:  newMergeSorter(nil, []int{2}),
		},
	}, {
		// pk text column, different order.
		input: &binlogdatapb.Rule{
			Match:  "pktext",
			Filter: "select c2, textcol from pktext",
		},
		table: "pktext",
		td: &tableDiffer{
			targetTable:      "pktext",
			sourceExpression: "select c2, textcol, weight_string(textcol) from pktext order by textcol asc",
			targetExpression: "select c2, textcol, weight_string(textcol) from pktext order by textcol asc",
			compareCols:      []int{0, -1},
			comparePKs:       []int{2},
			sourcePrimitive:  newMergeSorter(nil, []int{2}),
			targetPrimitive:  newMergeSorter(nil, []int{2}),
		},
	}, {
		// text column as expression.
		input: &binlogdatapb.Rule{
			Match:  "pktext",
			Filter: "select c2, a+b as textcol from pktext",
		},
		table: "pktext",
		td: &tableDiffer{
			targetTable:      "pktext",
			sourceExpression: "select c2, a + b as textcol, weight_string(a + b) from pktext order by textcol asc",
			targetExpression: "select c2, textcol, weight_string(textcol) from pktext order by textcol asc",
			compareCols:      []int{0, -1},
			comparePKs:       []int{2},
			sourcePrimitive:  newMergeSorter(nil, []int{2}),
			targetPrimitive:  newMergeSorter(nil, []int{2}),
		},
	}, {
		input: &binlogdatapb.Rule{
			Match: "multipk",
		},
		table: "multipk",
		td: &tableDiffer{
			targetTable:      "multipk",
			sourceExpression: "select c1, c2 from multipk order by c1 asc, c2 asc",
			targetExpression: "select c1, c2 from multipk order by c1 asc, c2 asc",
			compareCols:      []int{-1, -1},
			comparePKs:       []int{0, 1},
			sourcePrimitive:  newMergeSorter(nil, []int{0, 1}),
			targetPrimitive:  newMergeSorter(nil, []int{0, 1}),
		},
	}, {
		// in_keyrange
		input: &binlogdatapb.Rule{
			Match:  "t1",
			Filter: "select * from t1 where in_keyrange('-80')",
		},
		table: "t1",
		td: &tableDiffer{
			targetTable:      "t1",
			sourceExpression: "select c1, c2 from t1 order by c1 asc",
			targetExpression: "select c1, c2 from t1 order by c1 asc",
			compareCols:      []int{-1, 1},
			comparePKs:       []int{0},
			sourcePrimitive:  newMergeSorter(nil, []int{0}),
			targetPrimitive:  newMergeSorter(nil, []int{0}),
		},
	}, {
		// in_keyrange on RHS of AND.
		// This is currently not a valid construct, but will be supported in the future.
		input: &binlogdatapb.Rule{
			Match:  "t1",
			Filter: "select * from t1 where c2 = 2 and in_keyrange('-80')",
		},
		table: "t1",
		td: &tableDiffer{
			targetTable:      "t1",
			sourceExpression: "select c1, c2 from t1 where c2 = 2 order by c1 asc",
			targetExpression: "select c1, c2 from t1 order by c1 asc",
			compareCols:      []int{-1, 1},
			comparePKs:       []int{0},
			sourcePrimitive:  newMergeSorter(nil, []int{0}),
			targetPrimitive:  newMergeSorter(nil, []int{0}),
		},
	}, {
		// in_keyrange on LHS of AND.
		// This is currently not a valid construct, but will be supported in the future.
		input: &binlogdatapb.Rule{
			Match:  "t1",
			Filter: "select * from t1 where in_keyrange('-80') and c2 = 2",
		},
		table: "t1",
		td: &tableDiffer{
			targetTable:      "t1",
			sourceExpression: "select c1, c2 from t1 where c2 = 2 order by c1 asc",
			targetExpression: "select c1, c2 from t1 order by c1 asc",
			compareCols:      []int{-1, 1},
			comparePKs:       []int{0},
			sourcePrimitive:  newMergeSorter(nil, []int{0}),
			targetPrimitive:  newMergeSorter(nil, []int{0}),
		},
	}, {
		// in_keyrange on cascaded AND expression
		// This is currently not a valid construct, but will be supported in the future.
		input: &binlogdatapb.Rule{
			Match:  "t1",
			Filter: "select * from t1 where c2 = 2 and c1 = 1 and in_keyrange('-80')",
		},
		table: "t1",
		td: &tableDiffer{
			targetTable:      "t1",
			sourceExpression: "select c1, c2 from t1 where c2 = 2 and c1 = 1 order by c1 asc",
			targetExpression: "select c1, c2 from t1 order by c1 asc",
			compareCols:      []int{-1, 1},
			comparePKs:       []int{0},
			sourcePrimitive:  newMergeSorter(nil, []int{0}),
			targetPrimitive:  newMergeSorter(nil, []int{0}),
		},
	}, {
		// in_keyrange parenthesized
		// This is currently not a valid construct, but will be supported in the future.
		input: &binlogdatapb.Rule{
			Match:  "t1",
			Filter: "select * from t1 where (c2 = 2 and in_keyrange('-80'))",
		},
		table: "t1",
		td: &tableDiffer{
			targetTable:      "t1",
			sourceExpression: "select c1, c2 from t1 where c2 = 2 order by c1 asc",
			targetExpression: "select c1, c2 from t1 order by c1 asc",
			compareCols:      []int{-1, 1},
			comparePKs:       []int{0},
			sourcePrimitive:  newMergeSorter(nil, []int{0}),
			targetPrimitive:  newMergeSorter(nil, []int{0}),
		},
	}, {
		// group by
		input: &binlogdatapb.Rule{
			Match:  "t1",
			Filter: "select * from t1 group by c1",
		},
		table: "t1",
		td: &tableDiffer{
			targetTable:      "t1",
			sourceExpression: "select c1, c2 from t1 group by c1 order by c1 asc",
			targetExpression: "select c1, c2 from t1 order by c1 asc",
			compareCols:      []int{-1, 1},
			comparePKs:       []int{0},
			sourcePrimitive:  newMergeSorter(nil, []int{0}),
			targetPrimitive:  newMergeSorter(nil, []int{0}),
		},
	}, {
		// aggregations
		input: &binlogdatapb.Rule{
			Match:  "aggr",
			Filter: "select c1, c2, count(*) as c3, sum(c4) as c4 from t1 group by c1",
		},
		table: "aggr",
		td: &tableDiffer{
			targetTable:      "aggr",
			sourceExpression: "select c1, c2, count(*) as c3, sum(c4) as c4 from t1 group by c1 order by c1 asc",
			targetExpression: "select c1, c2, c3, c4 from aggr order by c1 asc",
			compareCols:      []int{-1, 1, 2, 3},
			comparePKs:       []int{0},
			sourcePrimitive: &engine.OrderedAggregate{
				Aggregates: []engine.AggregateParams{{
					Opcode: engine.AggregateCount,
					Col:    2,
				}, {
					Opcode: engine.AggregateSum,
					Col:    3,
				}},
				Keys:  []int{0},
				Input: newMergeSorter(nil, []int{0}),
			},
			targetPrimitive: newMergeSorter(nil, []int{0}),
		},
	}}
	for _, tcase := range testcases {
		t.Run(tcase.input.Filter, func(t *testing.T) {
			filter := &binlogdatapb.Filter{Rules: []*binlogdatapb.Rule{tcase.input}}
			df := &vdiff{}
			err := df.buildVDiffPlan(context.Background(), filter, schm, nil)
			require.NoError(t, err, tcase.input)
			require.Equal(t, 1, len(df.differs), tcase.input)
			assert.Equal(t, tcase.td, df.differs[tcase.table], tcase.input)
		})
	}
}

func TestVDiffPlanFailure(t *testing.T) {
	schm := &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{{
			Name:              "t1",
			Columns:           []string{"c1", "c2"},
			PrimaryKeyColumns: []string{"c1"},
			Fields:            sqltypes.MakeTestFields("c1|c2", "int64|int64"),
		}},
	}

	testcases := []struct {
		input *binlogdatapb.Rule
		err   string
	}{{
		input: &binlogdatapb.Rule{
			Match:  "t1",
			Filter: "bad query",
		},
		err: "syntax error at position 4 near 'bad'",
	}, {
		input: &binlogdatapb.Rule{
			Match:  "t1",
			Filter: "update t1 set c1=2",
		},
		err: "unexpected: update t1 set c1 = 2",
	}, {
		input: &binlogdatapb.Rule{
			Match:  "t1",
			Filter: "select c1+1 from t1",
		},
		err: "expression needs an alias: c1 + 1",
	}, {
		input: &binlogdatapb.Rule{
			Match:  "t1",
			Filter: "select next 2 values from t1",
		},
		err: "unexpected: select next 2 values from t1",
	}, {
		input: &binlogdatapb.Rule{
			Match:  "t1",
			Filter: "select c3 from t1",
		},
		err: "column c3 not found in table t1",
	}}
	for _, tcase := range testcases {
		filter := &binlogdatapb.Filter{Rules: []*binlogdatapb.Rule{tcase.input}}
		df := &vdiff{}
		err := df.buildVDiffPlan(context.Background(), filter, schm, nil)
		assert.EqualError(t, err, tcase.err, tcase.input)
	}
}

func TestVDiffUnsharded(t *testing.T) {
	env := newTestVDiffEnv([]string{"0"}, []string{"0"}, "", nil)
	defer env.close()

	schm := &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{{
			Name:              "t1",
			Columns:           []string{"c1", "c2"},
			PrimaryKeyColumns: []string{"c1"},
			Fields:            sqltypes.MakeTestFields("c1|c2", "int64|int64"),
		}},
	}
	env.tmc.schema = schm

	fields := sqltypes.MakeTestFields(
		"c1|c2",
		"int64|int64",
	)

	testcases := []struct {
		id     string
		source []*sqltypes.Result
		target []*sqltypes.Result
		dr     *DiffReport
	}{{
		id: "1",
		source: sqltypes.MakeTestStreamingResults(fields,
			"1|3",
			"2|4",
			"---",
			"3|1",
		),
		target: sqltypes.MakeTestStreamingResults(fields,
			"1|3",
			"---",
			"2|4",
			"3|1",
		),
		dr: &DiffReport{
			ProcessedRows: 3,
			MatchingRows:  3,
		},
	}, {
		id: "2",
		source: sqltypes.MakeTestStreamingResults(fields,
			"1|3",
		),
		target: sqltypes.MakeTestStreamingResults(fields,
			"1|3",
			"---",
			"2|4",
			"3|1",
		),
		dr: &DiffReport{
			ProcessedRows:   3,
			MatchingRows:    1,
			ExtraRowsTarget: 2,
		},
	}, {
		id: "3",
		source: sqltypes.MakeTestStreamingResults(fields,
			"1|3",
			"---",
			"2|4",
			"3|1",
		),
		target: sqltypes.MakeTestStreamingResults(fields,
			"1|3",
		),
		dr: &DiffReport{
			ProcessedRows:   3,
			MatchingRows:    1,
			ExtraRowsSource: 2,
		},
	}, {
		id: "4",
		source: sqltypes.MakeTestStreamingResults(fields,
			"1|3",
			"---",
			"2|4",
			"3|1",
		),
		target: sqltypes.MakeTestStreamingResults(fields,
			"1|3",
			"---",
			"3|1",
		),
		dr: &DiffReport{
			ProcessedRows:   3,
			MatchingRows:    2,
			ExtraRowsSource: 1,
		},
	}, {
		id: "5",
		source: sqltypes.MakeTestStreamingResults(fields,
			"1|3",
			"---",
			"3|1",
		),
		target: sqltypes.MakeTestStreamingResults(fields,
			"1|3",
			"---",
			"2|4",
			"3|1",
		),
		dr: &DiffReport{
			ProcessedRows:   3,
			MatchingRows:    2,
			ExtraRowsTarget: 1,
		},
	}, {
		id: "6",
		source: sqltypes.MakeTestStreamingResults(fields,
			"1|3",
			"---",
			"2|3",
			"3|1",
		),
		target: sqltypes.MakeTestStreamingResults(fields,
			"1|3",
			"---",
			"2|4",
			"3|1",
		),
		dr: &DiffReport{
			ProcessedRows:  3,
			MatchingRows:   2,
			MismatchedRows: 1,
		},
	}}

	for _, tcase := range testcases {
		env.tablets[101].setResults("select c1, c2 from t1 order by c1 asc", vdiffSourceGtid, tcase.source)
		env.tablets[201].setResults("select c1, c2 from t1 order by c1 asc", vdiffTargetMasterPosition, tcase.target)

		dr, err := env.wr.VDiff(context.Background(), "target", env.workflow, env.cell, env.cell, "replica", 30*time.Second, "", 100, "")
		require.NoError(t, err)
		assert.Equal(t, tcase.dr, dr["t1"], tcase.id)
	}
}

func TestVDiffSharded(t *testing.T) {
	// Also test that highest position ""MariaDB/5-456-892" will be used
	// if lower positions are found.
	env := newTestVDiffEnv([]string{"-40", "40-"}, []string{"-80", "80-"}, "", map[string]string{
		"-40-80": "MariaDB/5-456-890",
		"40-80-": "MariaDB/5-456-891",
	})
	defer env.close()

	schm := &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{{
			Name:              "t1",
			Columns:           []string{"c1", "c2"},
			PrimaryKeyColumns: []string{"c1"},
			Fields:            sqltypes.MakeTestFields("c1|c2", "int64|int64"),
		}},
	}
	env.tmc.schema = schm

	query := "select c1, c2 from t1 order by c1 asc"
	fields := sqltypes.MakeTestFields(
		"c1|c2",
		"int64|int64",
	)

	env.tablets[101].setResults(
		query,
		vdiffSourceGtid,
		sqltypes.MakeTestStreamingResults(fields,
			"1|3",
			"2|4",
		),
	)
	env.tablets[111].setResults(
		query,
		vdiffSourceGtid,
		sqltypes.MakeTestStreamingResults(fields,
			"3|4",
		),
	)
	env.tablets[201].setResults(
		query,
		vdiffTargetMasterPosition,
		sqltypes.MakeTestStreamingResults(fields,
			"1|3",
		),
	)
	env.tablets[211].setResults(
		query,
		vdiffTargetMasterPosition,
		sqltypes.MakeTestStreamingResults(fields,
			"2|4",
			"3|4",
		),
	)

	dr, err := env.wr.VDiff(context.Background(), "target", env.workflow, env.cell, env.cell, "replica", 30*time.Second, "", 100, "")
	require.NoError(t, err)
	wantdr := &DiffReport{
		ProcessedRows: 3,
		MatchingRows:  3,
	}
	assert.Equal(t, wantdr, dr["t1"])
}

func TestVDiffAggregates(t *testing.T) {
	env := newTestVDiffEnv([]string{"-40", "40-"}, []string{"-80", "80-"}, "select c1, count(*) c2, sum(c3) c3 from t group by c1", nil)
	defer env.close()

	schm := &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{{
			Name:              "t1",
			Columns:           []string{"c1", "c2", "c3"},
			PrimaryKeyColumns: []string{"c1"},
			Fields:            sqltypes.MakeTestFields("c1|c2|c3", "int64|int64|int64"),
		}},
	}
	env.tmc.schema = schm

	sourceQuery := "select c1, count(*) as c2, sum(c3) as c3 from t group by c1 order by c1 asc"
	fields := sqltypes.MakeTestFields(
		"c1|c2|c3",
		"int64|int64|int64",
	)

	env.tablets[101].setResults(
		sourceQuery,
		vdiffSourceGtid,
		sqltypes.MakeTestStreamingResults(fields,
			"1|3|4",
			"2|4|5",
			"4|5|6",
		),
	)
	env.tablets[111].setResults(
		sourceQuery,
		vdiffSourceGtid,
		sqltypes.MakeTestStreamingResults(fields,
			"1|1|1",
			"3|2|2",
			"5|3|3",
		),
	)
	targetQuery := "select c1, c2, c3 from t1 order by c1 asc"
	env.tablets[201].setResults(
		targetQuery,
		vdiffTargetMasterPosition,
		sqltypes.MakeTestStreamingResults(fields,
			"1|4|5",
			"5|3|3",
		),
	)
	env.tablets[211].setResults(
		targetQuery,
		vdiffTargetMasterPosition,
		sqltypes.MakeTestStreamingResults(fields,
			"2|4|5",
			"3|2|2",
			"4|5|6",
		),
	)

	dr, err := env.wr.VDiff(context.Background(), "target", env.workflow, env.cell, env.cell, "replica", 30*time.Second, "", 100, "")
	require.NoError(t, err)
	wantdr := &DiffReport{
		ProcessedRows: 5,
		MatchingRows:  5,
	}
	assert.Equal(t, wantdr, dr["t1"])
}

func TestVDiffPKWeightString(t *testing.T) {
	// Also test that highest position ""MariaDB/5-456-892" will be used
	// if lower positions are found.
	env := newTestVDiffEnv([]string{"-40", "40-"}, []string{"-80", "80-"}, "", nil)
	defer env.close()

	schm := &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{{
			Name:              "t1",
			Columns:           []string{"c1", "c2"},
			PrimaryKeyColumns: []string{"c1"},
			Fields:            sqltypes.MakeTestFields("c1|c2", "varchar|int64"),
		}},
	}
	env.tmc.schema = schm

	query := "select c1, c2, weight_string(c1) from t1 order by c1 asc"
	fields := sqltypes.MakeTestFields(
		"c1|c2|weight_string(c1)",
		"varchar|int64|varbinary",
	)

	env.tablets[101].setResults(
		query,
		vdiffSourceGtid,
		sqltypes.MakeTestStreamingResults(fields,
			"a|3|A",
			"b|4|B",
		),
	)
	env.tablets[111].setResults(
		query,
		vdiffSourceGtid,
		sqltypes.MakeTestStreamingResults(fields,
			"C|5|C",
			"D|6|D",
		),
	)
	env.tablets[201].setResults(
		query,
		vdiffTargetMasterPosition,
		sqltypes.MakeTestStreamingResults(fields,
			"A|3|A",
		),
	)
	env.tablets[211].setResults(
		query,
		vdiffTargetMasterPosition,
		sqltypes.MakeTestStreamingResults(fields,
			"b|4|B",
			"c|5|C",
			"D|6|D",
		),
	)

	dr, err := env.wr.VDiff(context.Background(), "target", env.workflow, env.cell, env.cell, "replica", 30*time.Second, "", 100, "")
	require.NoError(t, err)
	wantdr := &DiffReport{
		ProcessedRows: 4,
		MatchingRows:  4,
	}
	assert.Equal(t, wantdr, dr["t1"])
}

func TestVDiffNoPKWeightString(t *testing.T) {
	// Also test that highest position ""MariaDB/5-456-892" will be used
	// if lower positions are found.
	env := newTestVDiffEnv([]string{"-40", "40-"}, []string{"-80", "80-"}, "", nil)
	defer env.close()

	schm := &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{{
			Name:              "t1",
			Columns:           []string{"c1", "c2"},
			PrimaryKeyColumns: []string{"c1"},
			Fields:            sqltypes.MakeTestFields("c1|c2", "int64|varchar"),
		}},
	}
	env.tmc.schema = schm

	query := "select c1, c2, weight_string(c2) from t1 order by c1 asc"
	fields := sqltypes.MakeTestFields(
		"c1|c2|weight_string(c2)",
		"int64|varchar|varbinary",
	)

	env.tablets[101].setResults(
		query,
		vdiffSourceGtid,
		sqltypes.MakeTestStreamingResults(fields,
			"3|a|A",
			"4|b|B",
		),
	)
	env.tablets[111].setResults(
		query,
		vdiffSourceGtid,
		sqltypes.MakeTestStreamingResults(fields,
			"5|C|C",
			"6|D|D",
		),
	)
	env.tablets[201].setResults(
		query,
		vdiffTargetMasterPosition,
		sqltypes.MakeTestStreamingResults(fields,
			"3|A|A",
		),
	)
	env.tablets[211].setResults(
		query,
		vdiffTargetMasterPosition,
		sqltypes.MakeTestStreamingResults(fields,
			"4|b|B",
			"5|c|C",
			"6|D|D",
		),
	)

	dr, err := env.wr.VDiff(context.Background(), "target", env.workflow, env.cell, env.cell, "replica", 30*time.Second, "", 100, "")
	require.NoError(t, err)
	wantdr := &DiffReport{
		ProcessedRows: 4,
		MatchingRows:  4,
	}
	assert.Equal(t, wantdr, dr["t1"])
}

func TestVDiffDefaults(t *testing.T) {
	env := newTestVDiffEnv([]string{"0"}, []string{"0"}, "", nil)
	defer env.close()

	schm := &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{{
			Name:              "t1",
			Columns:           []string{"c1", "c2"},
			PrimaryKeyColumns: []string{"c1"},
			Fields:            sqltypes.MakeTestFields("c1|c2", "int64|int64"),
		}},
	}
	env.tmc.schema = schm

	fields := sqltypes.MakeTestFields(
		"c1|c2",
		"int64|int64",
	)

	source := sqltypes.MakeTestStreamingResults(fields,
		"1|3",
		"2|4",
		"---",
		"3|1",
	)
	target := source
	env.tablets[101].setResults("select c1, c2 from t1 order by c1 asc", vdiffSourceGtid, source)
	env.tablets[201].setResults("select c1, c2 from t1 order by c1 asc", vdiffTargetMasterPosition, target)

	_, err := env.wr.VDiff(context.Background(), "target", env.workflow, "", "", "replica", 30*time.Second, "", 100, "")
	require.NoError(t, err)
	_, err = env.wr.VDiff(context.Background(), "target", env.workflow, "", env.cell, "replica", 30*time.Second, "", 100, "")
	require.NoError(t, err)

	var df map[string]*DiffReport
	df, err = env.wr.VDiff(context.Background(), "target", env.workflow, env.cell, "", "replica", 30*time.Second, "", 100, "")
	require.NoError(t, err)
	require.Equal(t, df["t1"].ProcessedRows, 3)
	df, err = env.wr.VDiff(context.Background(), "target", env.workflow, env.cell, "", "replica", 30*time.Second, "", 1, "")
	require.NoError(t, err)
	require.Equal(t, df["t1"].ProcessedRows, 1)
	df, err = env.wr.VDiff(context.Background(), "target", env.workflow, env.cell, "", "replica", 30*time.Second, "", 0, "")
	require.NoError(t, err)
	require.Equal(t, df["t1"].ProcessedRows, 0)

	_, err = env.wr.VDiff(context.Background(), "target", env.workflow, env.cell, "", "replica", 1*time.Nanosecond, "", 100, "")
	require.Error(t, err)
	err = topo.CheckKeyspaceLocked(context.Background(), "target")
	require.EqualErrorf(t, err, "keyspace target is not locked (no locksInfo)", "")
	err = topo.CheckKeyspaceLocked(context.Background(), "source")
	require.EqualErrorf(t, err, "keyspace source is not locked (no locksInfo)", "")
}

func TestVDiffReplicationWait(t *testing.T) {
	env := newTestVDiffEnv([]string{"0"}, []string{"0"}, "", nil)
	defer env.close()

	schm := &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{{
			Name:              "t1",
			Columns:           []string{"c1", "c2"},
			PrimaryKeyColumns: []string{"c1"},
			Fields:            sqltypes.MakeTestFields("c1|c2", "int64|int64"),
		}},
	}
	env.tmc.schema = schm

	fields := sqltypes.MakeTestFields(
		"c1|c2",
		"int64|int64",
	)

	source := sqltypes.MakeTestStreamingResults(fields,
		"1|3",
		"2|4",
		"---",
		"3|1",
	)
	target := source
	env.tablets[101].setResults("select c1, c2 from t1 order by c1 asc", vdiffSourceGtid, source)
	env.tablets[201].setResults("select c1, c2 from t1 order by c1 asc", vdiffTargetMasterPosition, target)

	_, err := env.wr.VDiff(context.Background(), "target", env.workflow, env.cell, env.cell, "replica", 0*time.Second, "", 100, "")
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "context deadline exceeded"))
}

func TestVDiffFindPKs(t *testing.T) {

	testcases := []struct {
		name         string
		table        *tabletmanagerdatapb.TableDefinition
		targetSelect *sqlparser.Select
		tdIn         *tableDiffer
		tdOut        *tableDiffer
		errorString  string
	}{
		{
			name: "",
			table: &tabletmanagerdatapb.TableDefinition{
				Name:              "t1",
				Columns:           []string{"c1", "c2"},
				PrimaryKeyColumns: []string{"c1"},
				Fields:            sqltypes.MakeTestFields("c1|c2", "int64|int64"),
			},
			targetSelect: &sqlparser.Select{
				SelectExprs: sqlparser.SelectExprs{
					&sqlparser.AliasedExpr{Expr: &sqlparser.ColName{Name: sqlparser.NewColIdent("c1")}},
					&sqlparser.AliasedExpr{Expr: &sqlparser.ColName{Name: sqlparser.NewColIdent("c2")}},
				},
			},
			tdIn: &tableDiffer{
				compareCols: []int{0, 1},
				comparePKs:  []int{},
			},
			tdOut: &tableDiffer{
				compareCols: []int{-1, 1},
				comparePKs:  []int{0},
			},
		}, {
			name: "",
			table: &tabletmanagerdatapb.TableDefinition{
				Name:              "t1",
				Columns:           []string{"c1", "c2", "c3", "c4"},
				PrimaryKeyColumns: []string{"c1", "c4"},
				Fields:            sqltypes.MakeTestFields("c1|c2|c3|c4", "int64|int64|varchar|int64"),
			},
			targetSelect: &sqlparser.Select{
				SelectExprs: sqlparser.SelectExprs{
					&sqlparser.AliasedExpr{Expr: &sqlparser.ColName{Name: sqlparser.NewColIdent("c1")}},
					&sqlparser.AliasedExpr{Expr: &sqlparser.ColName{Name: sqlparser.NewColIdent("c2")}},
					&sqlparser.AliasedExpr{Expr: &sqlparser.FuncExpr{Name: sqlparser.NewColIdent("c3")}},
					&sqlparser.AliasedExpr{Expr: &sqlparser.ColName{Name: sqlparser.NewColIdent("c4")}},
				},
			},
			tdIn: &tableDiffer{
				compareCols: []int{0, 1, 2, 3},
				comparePKs:  []int{},
			},
			tdOut: &tableDiffer{
				compareCols: []int{-1, 1, 2, -1},
				comparePKs:  []int{0, 3},
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := findPKs(tc.table, tc.targetSelect, tc.tdIn)
			require.NoError(t, err)
			require.EqualValues(t, tc.tdOut, tc.tdIn)
		})
	}

}

func TestLogSteps(t *testing.T) {
	testcases := []struct {
		n   int64
		log string
	}{
		{1, "1"}, {2000, "2k"}, {1000000, "1m"}, {330000, ""}, {330001, ""},
		{4000000, "4m"}, {40000000, "40m"}, {41000000, "41m"}, {4110000, ""},
		{5000000000, "5b"}, {5010000000, "5.010b"}, {5011000000, "5.011b"},
	}
	for _, tc := range testcases {
		t.Run(strconv.Itoa(int(tc.n)), func(t *testing.T) {
			require.Equal(t, tc.log, logSteps(tc.n))
		})
	}
}

func TestVDiffPlanInclude(t *testing.T) {
	schm := &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{{
			Name:              "t1",
			Columns:           []string{"c1", "c2"},
			PrimaryKeyColumns: []string{"c1"},
			Fields:            sqltypes.MakeTestFields("c1|c2", "int64|int64"),
		}, {
			Name:              "t2",
			Columns:           []string{"c1", "c2"},
			PrimaryKeyColumns: []string{"c1"},
			Fields:            sqltypes.MakeTestFields("c1|c2", "int64|int64"),
		}, {
			Name:              "t3",
			Columns:           []string{"c1", "c2"},
			PrimaryKeyColumns: []string{"c1"},
			Fields:            sqltypes.MakeTestFields("c1|c2", "int64|int64"),
		}, {
			Name:              "t4",
			Columns:           []string{"c1", "c2"},
			PrimaryKeyColumns: []string{"c1"},
			Fields:            sqltypes.MakeTestFields("c1|c2", "int64|int64"),
		}},
	}

	df := &vdiff{}
	rule := &binlogdatapb.Rule{
		Match: "/.*",
	}
	filter := &binlogdatapb.Filter{Rules: []*binlogdatapb.Rule{rule}}
	var err error
	err = df.buildVDiffPlan(context.Background(), filter, schm, []string{"t2"})
	require.NoError(t, err)
	require.Equal(t, 1, len(df.differs))
	err = df.buildVDiffPlan(context.Background(), filter, schm, []string{"t2", "t3"})
	require.NoError(t, err)
	require.Equal(t, 2, len(df.differs))
	err = df.buildVDiffPlan(context.Background(), filter, schm, []string{"t1", "t2", "t3"})
	require.NoError(t, err)
	require.Equal(t, 3, len(df.differs))
	err = df.buildVDiffPlan(context.Background(), filter, schm, []string{"t1", "t2", "t3", "t4"})
	require.NoError(t, err)
	require.Equal(t, 4, len(df.differs))
	err = df.buildVDiffPlan(context.Background(), filter, schm, []string{"t1", "t2", "t3", "t5"})
	require.Error(t, err)
}
