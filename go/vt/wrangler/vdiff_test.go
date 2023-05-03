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
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/engine/opcode"
)

func TestVDiffPlanSuccess(t *testing.T) {
	schm := &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{{
			Name:              "t1",
			Columns:           []string{"c1", "c2"},
			PrimaryKeyColumns: []string{"c1"},
			Fields:            sqltypes.MakeTestFields("c1|c2", "int64|int64"),
			Schema:            "create table t1(c1 bigint, c2 bigint, primary key(c1))",
		}, {
			Name:              "nonpktext",
			Columns:           []string{"c1", "textcol"},
			PrimaryKeyColumns: []string{"c1"},
			Fields:            sqltypes.MakeTestFields("c1|textcol", "int64|varchar"),
			Schema:            "create table nonpktext(c1 bigint, textcol text, primary key(c1))",
		}, {
			Name:              "pktext",
			Columns:           []string{"textcol", "c2"},
			PrimaryKeyColumns: []string{"textcol"},
			Fields:            sqltypes.MakeTestFields("textcol|c2", "varchar|int64"),
			Schema:            "create table pktext(textcol varchar(50), c2 bigint, primary key(textcol))",
		}, {
			Name:              "multipk",
			Columns:           []string{"c1", "c2"},
			PrimaryKeyColumns: []string{"c1", "c2"},
			Fields:            sqltypes.MakeTestFields("c1|c2", "int64|int64"),
			Schema:            "create table multipk(c1 bigint, c2 bigint, primary key(c1, c2))",
		}, {
			Name:              "aggr",
			Columns:           []string{"c1", "c2", "c3", "c4"},
			PrimaryKeyColumns: []string{"c1"},
			Fields:            sqltypes.MakeTestFields("c1|c2|c3|c4", "int64|int64|int64|int64"),
			Schema:            "create table aggr(c1 bigint, c2 bigint, c3 bigint, c4 bigint, primary key(c1))",
		}, {
			Name:              "datze",
			Columns:           []string{"id", "dt"},
			PrimaryKeyColumns: []string{"id"},
			Fields:            sqltypes.MakeTestFields("id|dt", "int64|datetime"),
			Schema:            "create table datze(id bigint, dt datetime, primary key(id))",
		}},
	}

	testcases := []struct {
		input          *binlogdatapb.Rule
		table          string
		td             *tableDiffer
		sourceTimeZone string
	}{{
		input: &binlogdatapb.Rule{
			Match: "t1",
		},
		table: "t1",
		td: &tableDiffer{
			targetTable:      "t1",
			sourceExpression: "select c1, c2 from t1 order by c1 asc",
			targetExpression: "select c1, c2 from t1 order by c1 asc",
			compareCols:      []compareColInfo{{0, collations.Collation(nil), true}, {1, collations.Collation(nil), false}},
			comparePKs:       []compareColInfo{{0, collations.Collation(nil), true}},
			pkCols:           []int{0},
			selectPks:        []int{0},
			sourcePrimitive:  newMergeSorter(nil, []compareColInfo{{0, collations.Collation(nil), true}}),
			targetPrimitive:  newMergeSorter(nil, []compareColInfo{{0, collations.Collation(nil), true}}),
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
			compareCols:      []compareColInfo{{0, collations.Collation(nil), true}, {1, collations.Collation(nil), false}},
			comparePKs:       []compareColInfo{{0, collations.Collation(nil), true}},
			pkCols:           []int{0},
			selectPks:        []int{0},
			sourcePrimitive:  newMergeSorter(nil, []compareColInfo{{0, collations.Collation(nil), true}}),
			targetPrimitive:  newMergeSorter(nil, []compareColInfo{{0, collations.Collation(nil), true}}),
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
			compareCols:      []compareColInfo{{0, collations.Collation(nil), true}, {1, collations.Collation(nil), false}},
			comparePKs:       []compareColInfo{{0, collations.Collation(nil), true}},
			pkCols:           []int{0},
			selectPks:        []int{0},
			sourcePrimitive:  newMergeSorter(nil, []compareColInfo{{0, collations.Collation(nil), true}}),
			targetPrimitive:  newMergeSorter(nil, []compareColInfo{{0, collations.Collation(nil), true}}),
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
			compareCols:      []compareColInfo{{0, collations.Collation(nil), false}, {1, collations.Collation(nil), true}},
			comparePKs:       []compareColInfo{{1, collations.Collation(nil), true}},
			pkCols:           []int{1},
			selectPks:        []int{1},
			sourcePrimitive:  newMergeSorter(nil, []compareColInfo{{1, collations.Collation(nil), true}}),
			targetPrimitive:  newMergeSorter(nil, []compareColInfo{{1, collations.Collation(nil), true}}),
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
			compareCols:      []compareColInfo{{0, collations.Collation(nil), true}, {1, collations.Collation(nil), false}},
			comparePKs:       []compareColInfo{{0, collations.Collation(nil), true}},
			pkCols:           []int{0},
			selectPks:        []int{0},
			sourcePrimitive:  newMergeSorter(nil, []compareColInfo{{0, collations.Collation(nil), true}}),
			targetPrimitive:  newMergeSorter(nil, []compareColInfo{{0, collations.Collation(nil), true}}),
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
			sourceExpression: "select c1, textcol from nonpktext order by c1 asc",
			targetExpression: "select c1, textcol from nonpktext order by c1 asc",
			compareCols:      []compareColInfo{{0, collations.Collation(nil), true}, {1, collations.Collation(nil), false}},
			comparePKs:       []compareColInfo{{0, collations.Collation(nil), true}},
			pkCols:           []int{0},
			selectPks:        []int{0},
			sourcePrimitive:  newMergeSorter(nil, []compareColInfo{{0, collations.Collation(nil), true}}),
			targetPrimitive:  newMergeSorter(nil, []compareColInfo{{0, collations.Collation(nil), true}}),
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
			sourceExpression: "select textcol, c1 from nonpktext order by c1 asc",
			targetExpression: "select textcol, c1 from nonpktext order by c1 asc",
			compareCols:      []compareColInfo{{0, collations.Collation(nil), false}, {1, collations.Collation(nil), true}},
			comparePKs:       []compareColInfo{{1, collations.Collation(nil), true}},
			pkCols:           []int{1},
			selectPks:        []int{1},
			sourcePrimitive:  newMergeSorter(nil, []compareColInfo{{1, collations.Collation(nil), true}}),
			targetPrimitive:  newMergeSorter(nil, []compareColInfo{{1, collations.Collation(nil), true}}),
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
			sourceExpression: "select textcol, c2 from pktext order by textcol asc",
			targetExpression: "select textcol, c2 from pktext order by textcol asc",
			compareCols:      []compareColInfo{{0, collations.Default().Get(), true}, {1, collations.Collation(nil), false}},
			comparePKs:       []compareColInfo{{0, collations.Default().Get(), true}},
			pkCols:           []int{0},
			selectPks:        []int{0},
			sourcePrimitive:  newMergeSorter(nil, []compareColInfo{{0, collations.Default().Get(), false}}),
			targetPrimitive:  newMergeSorter(nil, []compareColInfo{{0, collations.Default().Get(), false}}),
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
			sourceExpression: "select c2, textcol from pktext order by textcol asc",
			targetExpression: "select c2, textcol from pktext order by textcol asc",
			compareCols:      []compareColInfo{{0, collations.Collation(nil), false}, {1, collations.Default().Get(), true}},
			comparePKs:       []compareColInfo{{1, collations.Default().Get(), true}},
			pkCols:           []int{1},
			selectPks:        []int{1},
			sourcePrimitive:  newMergeSorter(nil, []compareColInfo{{1, collations.Default().Get(), false}}),
			targetPrimitive:  newMergeSorter(nil, []compareColInfo{{1, collations.Default().Get(), false}}),
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
			sourceExpression: "select c2, a + b as textcol from pktext order by textcol asc",
			targetExpression: "select c2, textcol from pktext order by textcol asc",
			compareCols:      []compareColInfo{{0, collations.Collation(nil), false}, {1, collations.Default().Get(), true}},
			comparePKs:       []compareColInfo{{1, collations.Default().Get(), true}},
			pkCols:           []int{1},
			selectPks:        []int{1},
			sourcePrimitive:  newMergeSorter(nil, []compareColInfo{{1, collations.Default().Get(), false}}),
			targetPrimitive:  newMergeSorter(nil, []compareColInfo{{1, collations.Default().Get(), false}}),
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
			compareCols:      []compareColInfo{{0, collations.Collation(nil), true}, {1, collations.Collation(nil), true}},
			comparePKs:       []compareColInfo{{0, collations.Collation(nil), true}, {1, collations.Collation(nil), true}},
			pkCols:           []int{0, 1},
			selectPks:        []int{0, 1},
			sourcePrimitive:  newMergeSorter(nil, []compareColInfo{{0, collations.Collation(nil), true}, {1, collations.Collation(nil), true}}),
			targetPrimitive:  newMergeSorter(nil, []compareColInfo{{0, collations.Collation(nil), true}, {1, collations.Collation(nil), true}}),
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
			compareCols:      []compareColInfo{{0, collations.Collation(nil), true}, {1, collations.Collation(nil), false}},
			comparePKs:       []compareColInfo{{0, collations.Collation(nil), true}},
			pkCols:           []int{0},
			selectPks:        []int{0},
			sourcePrimitive:  newMergeSorter(nil, []compareColInfo{{0, collations.Collation(nil), true}}),
			targetPrimitive:  newMergeSorter(nil, []compareColInfo{{0, collations.Collation(nil), true}}),
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
			compareCols:      []compareColInfo{{0, collations.Collation(nil), true}, {1, collations.Collation(nil), false}},
			comparePKs:       []compareColInfo{{0, collations.Collation(nil), true}},
			pkCols:           []int{0},
			selectPks:        []int{0},
			sourcePrimitive:  newMergeSorter(nil, []compareColInfo{{0, collations.Collation(nil), true}}),
			targetPrimitive:  newMergeSorter(nil, []compareColInfo{{0, collations.Collation(nil), true}}),
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
			compareCols:      []compareColInfo{{0, collations.Collation(nil), true}, {1, collations.Collation(nil), false}},
			comparePKs:       []compareColInfo{{0, collations.Collation(nil), true}},
			pkCols:           []int{0},
			selectPks:        []int{0},
			sourcePrimitive:  newMergeSorter(nil, []compareColInfo{{0, collations.Collation(nil), true}}),
			targetPrimitive:  newMergeSorter(nil, []compareColInfo{{0, collations.Collation(nil), true}}),
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
			compareCols:      []compareColInfo{{0, collations.Collation(nil), true}, {1, collations.Collation(nil), false}},
			comparePKs:       []compareColInfo{{0, collations.Collation(nil), true}},
			pkCols:           []int{0},
			selectPks:        []int{0},
			sourcePrimitive:  newMergeSorter(nil, []compareColInfo{{0, collations.Collation(nil), true}}),
			targetPrimitive:  newMergeSorter(nil, []compareColInfo{{0, collations.Collation(nil), true}}),
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
			compareCols:      []compareColInfo{{0, collations.Collation(nil), true}, {1, collations.Collation(nil), false}},
			comparePKs:       []compareColInfo{{0, collations.Collation(nil), true}},
			pkCols:           []int{0},
			selectPks:        []int{0},
			sourcePrimitive:  newMergeSorter(nil, []compareColInfo{{0, collations.Collation(nil), true}}),
			targetPrimitive:  newMergeSorter(nil, []compareColInfo{{0, collations.Collation(nil), true}}),
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
			compareCols:      []compareColInfo{{0, collations.Collation(nil), true}, {1, collations.Collation(nil), false}},
			comparePKs:       []compareColInfo{{0, collations.Collation(nil), true}},
			pkCols:           []int{0},
			selectPks:        []int{0},
			sourcePrimitive:  newMergeSorter(nil, []compareColInfo{{0, collations.Collation(nil), true}}),
			targetPrimitive:  newMergeSorter(nil, []compareColInfo{{0, collations.Collation(nil), true}}),
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
			compareCols:      []compareColInfo{{0, collations.Collation(nil), true}, {1, collations.Collation(nil), false}, {2, collations.Collation(nil), false}, {3, collations.Collation(nil), false}},
			comparePKs:       []compareColInfo{{0, collations.Collation(nil), true}},
			pkCols:           []int{0},
			selectPks:        []int{0},
			sourcePrimitive: &engine.OrderedAggregate{
				Aggregates: []*engine.AggregateParams{{
					Opcode: opcode.AggregateSum,
					Col:    2,
				}, {
					Opcode: opcode.AggregateSum,
					Col:    3,
				}},
				GroupByKeys: []*engine.GroupByParams{{KeyCol: 0, WeightStringCol: -1}},
				Input:       newMergeSorter(nil, []compareColInfo{{0, collations.Collation(nil), true}}),
			},
			targetPrimitive: newMergeSorter(nil, []compareColInfo{{0, collations.Collation(nil), true}}),
		},
	}, {
		input: &binlogdatapb.Rule{
			Match: "datze",
		},
		sourceTimeZone: "US/Pacific",
		table:          "datze",
		td: &tableDiffer{
			targetTable:      "datze",
			sourceExpression: "select id, dt from datze order by id asc",
			targetExpression: "select id, convert_tz(dt, 'UTC', 'US/Pacific') as dt from datze order by id asc",
			compareCols:      []compareColInfo{{0, collations.Collation(nil), true}, {1, collations.Collation(nil), false}},
			comparePKs:       []compareColInfo{{0, collations.Collation(nil), true}},
			pkCols:           []int{0},
			selectPks:        []int{0},
			sourcePrimitive:  newMergeSorter(nil, []compareColInfo{{0, collations.Collation(nil), true}}),
			targetPrimitive:  newMergeSorter(nil, []compareColInfo{{0, collations.Collation(nil), true}}),
		},
	}}

	for _, tcase := range testcases {
		t.Run(tcase.input.Filter, func(t *testing.T) {
			filter := &binlogdatapb.Filter{Rules: []*binlogdatapb.Rule{tcase.input}}
			df := &vdiff{sourceTimeZone: tcase.sourceTimeZone, targetTimeZone: "UTC"}
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
	env := newTestVDiffEnv(t, []string{"0"}, []string{"0"}, "", nil)
	defer env.close()

	schm := &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{{
			Name:              "t1",
			Columns:           []string{"c1", "c2"},
			PrimaryKeyColumns: []string{"c1"},
			Fields:            sqltypes.MakeTestFields("c1|c2", "int64|int64"),
			Schema:            "create table t1(c1 bigint, c2 bigint, primary key(c1))",
		}},
	}
	env.tmc.schema = schm

	fields := sqltypes.MakeTestFields(
		"c1|c2",
		"int64|int64",
	)

	testcases := []struct {
		id      string
		source  []*sqltypes.Result
		target  []*sqltypes.Result
		dr      *DiffReport
		onlyPks bool
		debug   bool
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
			TableName:     "t1",
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
			TableName:       "t1",
			ExtraRowsTargetDiffs: []*RowDiff{
				{
					Row: map[string]sqltypes.Value{
						"c1": sqltypes.NewInt64(2),
						"c2": sqltypes.NewInt64(4),
					},
					Query: "",
				},
			},
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
			TableName:       "t1",
			ExtraRowsSourceDiffs: []*RowDiff{
				{
					Row: map[string]sqltypes.Value{
						"c1": sqltypes.NewInt64(2),
						"c2": sqltypes.NewInt64(4),
					},
					Query: "",
				},
			},
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
			TableName:       "t1",
			ExtraRowsSourceDiffs: []*RowDiff{
				{
					Row: map[string]sqltypes.Value{
						"c1": sqltypes.NewInt64(2),
						"c2": sqltypes.NewInt64(4),
					},
					Query: "",
				},
			},
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
			TableName:       "t1",
			ExtraRowsTargetDiffs: []*RowDiff{
				{
					Row: map[string]sqltypes.Value{
						"c1": sqltypes.NewInt64(2),
						"c2": sqltypes.NewInt64(4),
					},
					Query: "",
				},
			},
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
			TableName:      "t1",
			MismatchedRowsSample: []*DiffMismatch{
				{
					Source: &RowDiff{Row: map[string]sqltypes.Value{
						"c1": sqltypes.NewInt64(2),
						"c2": sqltypes.NewInt64(3),
					},
						Query: "",
					},
					Target: &RowDiff{Row: map[string]sqltypes.Value{
						"c1": sqltypes.NewInt64(2),
						"c2": sqltypes.NewInt64(4),
					},
						Query: "",
					},
				},
			},
		},
	}, {
		id:      "7",
		onlyPks: true,
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
			TableName:      "t1",
			MismatchedRowsSample: []*DiffMismatch{
				{
					Source: &RowDiff{Row: map[string]sqltypes.Value{
						"c1": sqltypes.NewInt64(2),
					},
						Query: "",
					},
					Target: &RowDiff{Row: map[string]sqltypes.Value{
						"c1": sqltypes.NewInt64(2),
					},
						Query: "",
					},
				},
			},
		},
	}, {
		id:    "8",
		debug: true,
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
			TableName:      "t1",
			MismatchedRowsSample: []*DiffMismatch{
				{
					Source: &RowDiff{Row: map[string]sqltypes.Value{
						"c1": sqltypes.NewInt64(2),
						"c2": sqltypes.NewInt64(3),
					},
						Query: "select c1, c2 from t1 where c1=2;",
					},
					Target: &RowDiff{Row: map[string]sqltypes.Value{
						"c1": sqltypes.NewInt64(2),
						"c2": sqltypes.NewInt64(4),
					},
						Query: "select c1, c2 from t1 where c1=2;",
					},
				},
			},
		},
	}}

	for _, tcase := range testcases {
		t.Run(tcase.id, func(t *testing.T) {
			env.tablets[101].setResults("select c1, c2 from t1 order by c1 asc", vdiffSourceGtid, tcase.source)
			env.tablets[201].setResults("select c1, c2 from t1 order by c1 asc", vdiffTargetPrimaryPosition, tcase.target)

			dr, err := env.wr.VDiff(context.Background(), "target", env.workflow, env.cell, env.cell, "replica", 30*time.Second, "", 100, "", tcase.debug, tcase.onlyPks, 100)
			require.NoError(t, err)
			assert.Equal(t, tcase.dr, dr["t1"], tcase.id)
		})
	}
}

func TestVDiffSharded(t *testing.T) {
	// Also test that highest position ""MariaDB/5-456-892" will be used
	// if lower positions are found.
	env := newTestVDiffEnv(t, []string{"-40", "40-"}, []string{"-80", "80-"}, "", map[string]string{
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
			Schema:            "create table t1(c1 bigint, c2 bigint, primary key(c1))",
		},
			{
				Name:              "_t1_gho",
				Columns:           []string{"c1", "c2", "c3"},
				PrimaryKeyColumns: []string{"c2"},
				Fields:            sqltypes.MakeTestFields("c1|c2|c3", "int64|int64|int64"),
				Schema:            "create table _t1_gho(c1 bigint, c2 bigint, c3 bigint, primary key(c2))",
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
		vdiffTargetPrimaryPosition,
		sqltypes.MakeTestStreamingResults(fields,
			"1|3",
		),
	)
	env.tablets[211].setResults(
		query,
		vdiffTargetPrimaryPosition,
		sqltypes.MakeTestStreamingResults(fields,
			"2|4",
			"3|4",
		),
	)

	dr, err := env.wr.VDiff(context.Background(), "target", env.workflow, env.cell, env.cell, "replica", 30*time.Second, "", 100, "", false /*debug*/, false /*onlyPks*/, 100)
	require.NoError(t, err)
	wantdr := &DiffReport{
		ProcessedRows: 3,
		MatchingRows:  3,
		TableName:     "t1",
	}
	assert.Equal(t, wantdr, dr["t1"])
}

func TestVDiffAggregates(t *testing.T) {
	env := newTestVDiffEnv(t, []string{"-40", "40-"}, []string{"-80", "80-"}, "select c1, count(*) c2, sum(c3) c3 from t group by c1", nil)
	defer env.close()

	schm := &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{{
			Name:              "t1",
			Columns:           []string{"c1", "c2", "c3"},
			PrimaryKeyColumns: []string{"c1"},
			Fields:            sqltypes.MakeTestFields("c1|c2|c3", "int64|int64|int64"),
			Schema:            "create table t1(c1 bigint, c2 bigint, c3 bigint, primary key(c1))",
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
		vdiffTargetPrimaryPosition,
		sqltypes.MakeTestStreamingResults(fields,
			"1|4|5",
			"5|3|3",
		),
	)
	env.tablets[211].setResults(
		targetQuery,
		vdiffTargetPrimaryPosition,
		sqltypes.MakeTestStreamingResults(fields,
			"2|4|5",
			"3|2|2",
			"4|5|6",
		),
	)

	dr, err := env.wr.VDiff(context.Background(), "target", env.workflow, env.cell, env.cell, "replica", 30*time.Second, "", 100, "", false /*debug*/, false /*onlyPks*/, 100)
	require.NoError(t, err)
	wantdr := &DiffReport{
		ProcessedRows: 5,
		MatchingRows:  5,
		TableName:     "t1",
	}
	assert.Equal(t, wantdr, dr["t1"])
}

func TestVDiffDefaults(t *testing.T) {
	env := newTestVDiffEnv(t, []string{"0"}, []string{"0"}, "", nil)
	defer env.close()

	schm := &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{{
			Name:              "t1",
			Columns:           []string{"c1", "c2"},
			PrimaryKeyColumns: []string{"c1"},
			Fields:            sqltypes.MakeTestFields("c1|c2", "int64|int64"),
			Schema:            "create table t1(c1 bigint, c2 bigint, primary key(c1))",
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
	env.tablets[201].setResults("select c1, c2 from t1 order by c1 asc", vdiffTargetPrimaryPosition, target)

	_, err := env.wr.VDiff(context.Background(), "target", env.workflow, "", "", "replica", 30*time.Second, "", 100, "", false /*debug*/, false /*onlyPks*/, 100)
	require.NoError(t, err)
	_, err = env.wr.VDiff(context.Background(), "target", env.workflow, "", env.cell, "replica", 30*time.Second, "", 100, "", false /*debug*/, false /*onlyPks*/, 100)
	require.NoError(t, err)

	var df map[string]*DiffReport
	df, err = env.wr.VDiff(context.Background(), "target", env.workflow, env.cell, "", "replica", 30*time.Second, "", 100, "", false /*debug*/, false /*onlyPks*/, 100)
	require.NoError(t, err)
	require.Equal(t, df["t1"].ProcessedRows, 3)
	df, err = env.wr.VDiff(context.Background(), "target", env.workflow, env.cell, "", "replica", 30*time.Second, "", 1, "", false /*debug*/, false /*onlyPks*/, 100)
	require.NoError(t, err)
	require.Equal(t, df["t1"].ProcessedRows, 1)
	df, err = env.wr.VDiff(context.Background(), "target", env.workflow, env.cell, "", "replica", 30*time.Second, "", 0, "", false /*debug*/, false /*onlyPks*/, 100)
	require.NoError(t, err)
	require.Equal(t, df["t1"].ProcessedRows, 0)

	_, err = env.wr.VDiff(context.Background(), "target", env.workflow, env.cell, "", "replica", 1*time.Nanosecond, "", 100, "", false /*debug*/, false /*onlyPks*/, 100)
	require.Error(t, err)
	err = topo.CheckKeyspaceLocked(context.Background(), "target")
	require.EqualErrorf(t, err, "keyspace target is not locked (no locksInfo)", "")
	err = topo.CheckKeyspaceLocked(context.Background(), "source")
	require.EqualErrorf(t, err, "keyspace source is not locked (no locksInfo)", "")
}

func TestVDiffReplicationWait(t *testing.T) {
	env := newTestVDiffEnv(t, []string{"0"}, []string{"0"}, "", nil)
	defer env.close()

	schm := &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{{
			Name:              "t1",
			Columns:           []string{"c1", "c2"},
			PrimaryKeyColumns: []string{"c1"},
			Fields:            sqltypes.MakeTestFields("c1|c2", "int64|int64"),
			Schema:            "create table t1(c1 bigint, c2 bigint, primary key(c1))",
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
	env.tablets[201].setResults("select c1, c2 from t1 order by c1 asc", vdiffTargetPrimaryPosition, target)

	_, err := env.wr.VDiff(context.Background(), "target", env.workflow, env.cell, env.cell, "replica", 0*time.Second, "", 100, "", false /*debug*/, false /*onlyPks*/, 100)
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
				Schema:            "create table t1(c1 bigint, c2 bigint, primary key(c1))",
			},
			targetSelect: &sqlparser.Select{
				SelectExprs: sqlparser.SelectExprs{
					&sqlparser.AliasedExpr{Expr: &sqlparser.ColName{Name: sqlparser.NewIdentifierCI("c1")}},
					&sqlparser.AliasedExpr{Expr: &sqlparser.ColName{Name: sqlparser.NewIdentifierCI("c2")}},
				},
			},
			tdIn: &tableDiffer{
				compareCols: []compareColInfo{{0, collations.Collation(nil), false}, {1, collations.Collation(nil), false}},
				comparePKs:  []compareColInfo{},
				pkCols:      []int{},
			},
			tdOut: &tableDiffer{
				compareCols: []compareColInfo{{0, collations.Collation(nil), true}, {1, collations.Collation(nil), false}},
				comparePKs:  []compareColInfo{{0, collations.Collation(nil), true}},
				pkCols:      []int{0},
				selectPks:   []int{0},
			},
		}, {
			name: "",
			table: &tabletmanagerdatapb.TableDefinition{
				Name:              "t1",
				Columns:           []string{"c1", "c2", "c3", "c4"},
				PrimaryKeyColumns: []string{"c1", "c4"},
				Fields:            sqltypes.MakeTestFields("c1|c2|c3|c4", "int64|int64|varchar|int64"),
				Schema:            "create table t1(c1 bigint, c2 bigint, c3 varchar(50), c4 bigint, primary key(c1, c4))",
			},
			targetSelect: &sqlparser.Select{
				SelectExprs: sqlparser.SelectExprs{
					&sqlparser.AliasedExpr{Expr: &sqlparser.ColName{Name: sqlparser.NewIdentifierCI("c1")}},
					&sqlparser.AliasedExpr{Expr: &sqlparser.ColName{Name: sqlparser.NewIdentifierCI("c2")}},
					&sqlparser.AliasedExpr{Expr: &sqlparser.FuncExpr{Name: sqlparser.NewIdentifierCI("c3")}},
					&sqlparser.AliasedExpr{Expr: &sqlparser.ColName{Name: sqlparser.NewIdentifierCI("c4")}},
				},
			},
			tdIn: &tableDiffer{
				compareCols: []compareColInfo{{0, collations.Collation(nil), false}, {1, collations.Collation(nil), false}, {2, collations.Collation(nil), false}, {3, collations.Collation(nil), false}},
				comparePKs:  []compareColInfo{},
				pkCols:      []int{},
			},
			tdOut: &tableDiffer{
				compareCols: []compareColInfo{{0, collations.Collation(nil), true}, {1, collations.Collation(nil), false}, {2, collations.Collation(nil), false}, {3, collations.Collation(nil), true}},
				comparePKs:  []compareColInfo{{0, collations.Collation(nil), true}, {3, collations.Collation(nil), true}},
				pkCols:      []int{0, 3},
				selectPks:   []int{0, 3},
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

func TestVDiffPlanInclude(t *testing.T) {
	schm := &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{{
			Name:              "t1",
			Columns:           []string{"c1", "c2"},
			PrimaryKeyColumns: []string{"c1"},
			Fields:            sqltypes.MakeTestFields("c1|c2", "int64|int64"),
			Schema:            "create table t1(c1 bigint, c2 bigint, primary key(c1))",
		}, {
			Name:              "t2",
			Columns:           []string{"c1", "c2"},
			PrimaryKeyColumns: []string{"c1"},
			Fields:            sqltypes.MakeTestFields("c1|c2", "int64|int64"),
			Schema:            "create table t2(c1 bigint, c2 bigint, primary key(c1))",
		}, {
			Name:              "t3",
			Columns:           []string{"c1", "c2"},
			PrimaryKeyColumns: []string{"c1"},
			Fields:            sqltypes.MakeTestFields("c1|c2", "int64|int64"),
			Schema:            "create table t3(c1 bigint, c2 bigint, primary key(c1))",
		}, {
			Name:              "t4",
			Columns:           []string{"c1", "c2"},
			PrimaryKeyColumns: []string{"c1"},
			Fields:            sqltypes.MakeTestFields("c1|c2", "int64|int64"),
			Schema:            "create table t4(c1 bigint, c2 bigint, primary key(c1))",
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

func TestGetColumnCollations(t *testing.T) {
	collationEnv := collations.Local()
	tests := []struct {
		name    string
		table   *tabletmanagerdatapb.TableDefinition
		want    map[string]collations.Collation
		wantErr bool
	}{
		{
			name: "invalid schema",
			table: &tabletmanagerdatapb.TableDefinition{
				Schema: "wut is this",
			},
			wantErr: true,
		},
		{
			name: "no char pk",
			table: &tabletmanagerdatapb.TableDefinition{
				Schema: "create table t1 (c1 int, name varchar(10), primary key(c1))",
			},
			want: map[string]collations.Collation{
				"c1":   collations.Collation(nil),
				"name": collations.Default().Get(),
			},
		},
		{
			name: "char pk with global default collation",
			table: &tabletmanagerdatapb.TableDefinition{
				Schema: "create table t1 (c1 varchar(10), name varchar(10), primary key(c1))",
			},
			want: map[string]collations.Collation{
				"c1":   collations.Default().Get(),
				"name": collations.Default().Get(),
			},
		},
		{
			name: "compound char int pk with global default collation",
			table: &tabletmanagerdatapb.TableDefinition{
				Schema: "create table t1 (c1 int, name varchar(10), primary key(c1, name))",
			},
			want: map[string]collations.Collation{
				"c1":   collations.Collation(nil),
				"name": collations.Default().Get(),
			},
		},
		{
			name: "char pk with table default charset",
			table: &tabletmanagerdatapb.TableDefinition{
				Schema: "create table t1 (c1 varchar(10), name varchar(10), primary key(c1)) default character set ucs2",
			},
			want: map[string]collations.Collation{
				"c1":   collationEnv.DefaultCollationForCharset("ucs2"),
				"name": collationEnv.DefaultCollationForCharset("ucs2"),
			},
		},
		{
			name: "char pk with table default collation",
			table: &tabletmanagerdatapb.TableDefinition{
				Schema: "create table t1 (c1 varchar(10), name varchar(10), primary key(c1)) charset=utf32 collate=utf32_icelandic_ci",
			},
			want: map[string]collations.Collation{
				"c1":   collationEnv.LookupByName("utf32_icelandic_ci"),
				"name": collationEnv.LookupByName("utf32_icelandic_ci"),
			},
		},
		{
			name: "char pk with column charset override",
			table: &tabletmanagerdatapb.TableDefinition{
				Schema: "create table t1 (c1 varchar(10) charset sjis, name varchar(10), primary key(c1)) character set=utf8",
			},
			want: map[string]collations.Collation{
				"c1":   collationEnv.DefaultCollationForCharset("sjis"),
				"name": collationEnv.DefaultCollationForCharset("utf8mb3"),
			},
		},
		{
			name: "char pk with column collation override",
			table: &tabletmanagerdatapb.TableDefinition{
				Schema: "create table t1 (c1 varchar(10) collate hebrew_bin, name varchar(10), primary key(c1)) charset=hebrew",
			},
			want: map[string]collations.Collation{
				"c1":   collationEnv.LookupByName("hebrew_bin"),
				"name": collationEnv.DefaultCollationForCharset("hebrew"),
			},
		},
		{
			name: "compound char int pk with column collation override",
			table: &tabletmanagerdatapb.TableDefinition{
				Schema: "create table t1 (c1 varchar(10) collate utf16_turkish_ci, c2 int, name varchar(10), primary key(c1, c2)) charset=utf16 collate=utf16_icelandic_ci",
			},
			want: map[string]collations.Collation{
				"c1":   collationEnv.LookupByName("utf16_turkish_ci"),
				"c2":   collations.Collation(nil),
				"name": collationEnv.LookupByName("utf16_icelandic_ci"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getColumnCollations(tt.table)
			if (err != nil) != tt.wantErr {
				t.Errorf("getColumnCollations() error = %v, wantErr = %t", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getColumnCollations() = %+v, want %+v", got, tt.want)
			}
		})
	}
}
