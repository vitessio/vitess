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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
	"vitess.io/vitess/go/sqltypes"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
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
		},
	}, {
		// in_keyrange with other expressions.
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
		},
	}}
	for _, tcase := range testcases {
		filter := &binlogdatapb.Filter{Rules: []*binlogdatapb.Rule{tcase.input}}
		differs, err := buildVDiffPlan(context.Background(), filter, schm)
		require.NoError(t, err, tcase.input)
		require.Equal(t, 1, len(differs), tcase.input)
		assert.Equal(t, tcase.td, differs[tcase.table], tcase.input)
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
		_, err := buildVDiffPlan(context.Background(), filter, schm)
		assert.EqualError(t, err, tcase.err, tcase.input)
	}
}
