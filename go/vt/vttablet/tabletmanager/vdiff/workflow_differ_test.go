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

package vdiff

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/engine/opcode"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
)

func TestBuildPlanSuccess(t *testing.T) {
	vdenv := newTestVDiffEnv(t)
	defer vdenv.close()
	UUID := uuid.New()
	controllerQR := sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		vdiffTestCols,
		vdiffTestColTypes,
	),
		fmt.Sprintf("1|%s|%s|%s|%s|%s|%s|%s|", UUID, vdiffenv.workflow, tstenv.KeyspaceName, tstenv.ShardName, vdiffDBName, PendingState, optionsJS),
	)

	vdiffenv.dbClient.ExpectRequest("select * from _vt.vdiff where id = 1", noResults, nil)
	ct, err := newController(context.Background(), controllerQR.Named().Row(), vdiffenv.dbClientFactory, tstenv.TopoServ, vdiffenv.vde, vdiffenv.opts)
	require.NoError(t, err)

	testcases := []struct {
		input          *binlogdatapb.Rule
		table          string
		tablePlan      *tablePlan
		sourceTimeZone string
	}{{
		input: &binlogdatapb.Rule{
			Match: "t1",
		},
		table: "t1",
		tablePlan: &tablePlan{
			dbName:      vdiffDBName,
			table:       testSchema.TableDefinitions[tableDefMap["t1"]],
			sourceQuery: "select c1, c2 from t1 order by c1 asc",
			targetQuery: "select c1, c2 from t1 order by c1 asc",
			compareCols: []compareColInfo{{0, collations.Local().LookupByName(sqltypes.NULL.String()), true, "c1"}, {1, collations.Local().LookupByName(sqltypes.NULL.String()), false, "c2"}},
			comparePKs:  []compareColInfo{{0, collations.Local().LookupByName(sqltypes.NULL.String()), true, "c1"}},
			pkCols:      []int{0},
			selectPks:   []int{0},
			orderBy: sqlparser.OrderBy{&sqlparser.Order{
				Expr:      &sqlparser.ColName{Name: sqlparser.NewIdentifierCI("c1")},
				Direction: sqlparser.AscOrder,
			}},
		},
	}, {
		input: &binlogdatapb.Rule{
			Match:  "t1",
			Filter: "-80",
		},
		table: "t1",
		tablePlan: &tablePlan{
			dbName:      vdiffDBName,
			table:       testSchema.TableDefinitions[tableDefMap["t1"]],
			sourceQuery: "select c1, c2 from t1 where in_keyrange('-80') order by c1 asc",
			targetQuery: "select c1, c2 from t1 order by c1 asc",
			compareCols: []compareColInfo{{0, collations.Local().LookupByName(sqltypes.NULL.String()), true, "c1"}, {1, collations.Local().LookupByName(sqltypes.NULL.String()), false, "c2"}},
			comparePKs:  []compareColInfo{{0, collations.Local().LookupByName(sqltypes.NULL.String()), true, "c1"}},
			pkCols:      []int{0},
			selectPks:   []int{0},
			orderBy: sqlparser.OrderBy{&sqlparser.Order{
				Expr:      &sqlparser.ColName{Name: sqlparser.NewIdentifierCI("c1")},
				Direction: sqlparser.AscOrder,
			}},
		},
	}, {
		input: &binlogdatapb.Rule{
			Match:  "t1",
			Filter: "select * from t1",
		},
		table: "t1",
		tablePlan: &tablePlan{
			dbName:      vdiffDBName,
			table:       testSchema.TableDefinitions[tableDefMap["t1"]],
			sourceQuery: "select c1, c2 from t1 order by c1 asc",
			targetQuery: "select c1, c2 from t1 order by c1 asc",
			compareCols: []compareColInfo{{0, collations.Local().LookupByName(sqltypes.NULL.String()), true, "c1"}, {1, collations.Local().LookupByName(sqltypes.NULL.String()), false, "c2"}},
			comparePKs:  []compareColInfo{{0, collations.Local().LookupByName(sqltypes.NULL.String()), true, "c1"}},
			pkCols:      []int{0},
			selectPks:   []int{0},
			orderBy: sqlparser.OrderBy{&sqlparser.Order{
				Expr:      &sqlparser.ColName{Name: sqlparser.NewIdentifierCI("c1")},
				Direction: sqlparser.AscOrder,
			}},
		},
	}, {
		input: &binlogdatapb.Rule{
			Match:  "t1",
			Filter: "select c2, c1 from t1",
		},
		table: "t1",
		tablePlan: &tablePlan{
			dbName:      vdiffDBName,
			table:       testSchema.TableDefinitions[tableDefMap["t1"]],
			sourceQuery: "select c2, c1 from t1 order by c1 asc",
			targetQuery: "select c2, c1 from t1 order by c1 asc",
			compareCols: []compareColInfo{{0, collations.Local().LookupByName(sqltypes.NULL.String()), false, "c2"}, {1, collations.Local().LookupByName(sqltypes.NULL.String()), true, "c1"}},
			comparePKs:  []compareColInfo{{1, collations.Local().LookupByName(sqltypes.NULL.String()), true, "c1"}},
			pkCols:      []int{1},
			selectPks:   []int{1},
			orderBy: sqlparser.OrderBy{&sqlparser.Order{
				Expr:      &sqlparser.ColName{Name: sqlparser.NewIdentifierCI("c1")},
				Direction: sqlparser.AscOrder,
			}},
		},
	}, {
		input: &binlogdatapb.Rule{
			Match:  "t1",
			Filter: "select c0 as c1, c2 from t2",
		},
		table: "t1",
		tablePlan: &tablePlan{
			dbName:      vdiffDBName,
			table:       testSchema.TableDefinitions[tableDefMap["t1"]],
			sourceQuery: "select c0 as c1, c2 from t2 order by c1 asc",
			targetQuery: "select c1, c2 from t1 order by c1 asc",
			compareCols: []compareColInfo{{0, collations.Local().LookupByName(sqltypes.NULL.String()), true, "c1"}, {1, collations.Local().LookupByName(sqltypes.NULL.String()), false, "c2"}},
			comparePKs:  []compareColInfo{{0, collations.Local().LookupByName(sqltypes.NULL.String()), true, "c1"}},
			pkCols:      []int{0},
			selectPks:   []int{0},
			orderBy: sqlparser.OrderBy{&sqlparser.Order{
				Expr:      &sqlparser.ColName{Name: sqlparser.NewIdentifierCI("c1")},
				Direction: sqlparser.AscOrder,
			}},
		},
	}, {
		// non-pk text column.
		input: &binlogdatapb.Rule{
			Match:  "nonpktext",
			Filter: "select c1, textcol from nonpktext",
		},
		table: "nonpktext",
		tablePlan: &tablePlan{
			dbName:      vdiffDBName,
			table:       testSchema.TableDefinitions[tableDefMap["nonpktext"]],
			sourceQuery: "select c1, textcol from nonpktext order by c1 asc",
			targetQuery: "select c1, textcol from nonpktext order by c1 asc",
			compareCols: []compareColInfo{{0, collations.Local().LookupByName(sqltypes.NULL.String()), true, "c1"}, {1, collations.Local().LookupByName(sqltypes.NULL.String()), false, "textcol"}},
			comparePKs:  []compareColInfo{{0, collations.Local().LookupByName(sqltypes.NULL.String()), true, "c1"}},
			pkCols:      []int{0},
			selectPks:   []int{0},
			orderBy: sqlparser.OrderBy{&sqlparser.Order{
				Expr:      &sqlparser.ColName{Name: sqlparser.NewIdentifierCI("c1")},
				Direction: sqlparser.AscOrder,
			}},
		},
	}, {
		// non-pk text column, different order.
		input: &binlogdatapb.Rule{
			Match:  "nonpktext",
			Filter: "select textcol, c1 from nonpktext",
		},
		table: "nonpktext",
		tablePlan: &tablePlan{
			dbName:      vdiffDBName,
			table:       testSchema.TableDefinitions[tableDefMap["nonpktext"]],
			sourceQuery: "select textcol, c1 from nonpktext order by c1 asc",
			targetQuery: "select textcol, c1 from nonpktext order by c1 asc",
			compareCols: []compareColInfo{{0, collations.Local().LookupByName(sqltypes.NULL.String()), false, "textcol"}, {1, collations.Local().LookupByName(sqltypes.NULL.String()), true, "c1"}},
			comparePKs:  []compareColInfo{{1, collations.Local().LookupByName(sqltypes.NULL.String()), true, "c1"}},
			pkCols:      []int{1},
			selectPks:   []int{1},
			orderBy: sqlparser.OrderBy{&sqlparser.Order{
				Expr:      &sqlparser.ColName{Name: sqlparser.NewIdentifierCI("c1")},
				Direction: sqlparser.AscOrder,
			}},
		},
	}, {
		// pk text column.
		input: &binlogdatapb.Rule{
			Match:  "pktext",
			Filter: "select textcol, c2 from pktext",
		},
		table: "pktext",
		tablePlan: &tablePlan{
			dbName:      vdiffDBName,
			table:       testSchema.TableDefinitions[tableDefMap["pktext"]],
			sourceQuery: "select textcol, c2 from pktext order by textcol asc",
			targetQuery: "select textcol, c2 from pktext order by textcol asc",
			compareCols: []compareColInfo{{0, collations.Local().LookupByName(sqltypes.NULL.String()), true, "textcol"}, {1, collations.Local().LookupByName(sqltypes.NULL.String()), false, "c2"}},
			comparePKs:  []compareColInfo{{0, collations.Local().LookupByName(sqltypes.NULL.String()), true, "textcol"}},
			pkCols:      []int{0},
			selectPks:   []int{0},
			orderBy: sqlparser.OrderBy{&sqlparser.Order{
				Expr:      &sqlparser.ColName{Name: sqlparser.NewIdentifierCI("textcol")},
				Direction: sqlparser.AscOrder,
			}},
		},
	}, {
		// pk text column, different order.
		input: &binlogdatapb.Rule{
			Match:  "pktext",
			Filter: "select c2, textcol from pktext",
		},
		table: "pktext",
		tablePlan: &tablePlan{
			dbName:      vdiffDBName,
			table:       testSchema.TableDefinitions[tableDefMap["pktext"]],
			sourceQuery: "select c2, textcol from pktext order by textcol asc",
			targetQuery: "select c2, textcol from pktext order by textcol asc",
			compareCols: []compareColInfo{{0, collations.Local().LookupByName(sqltypes.NULL.String()), false, "c2"}, {1, collations.Local().LookupByName(sqltypes.NULL.String()), true, "textcol"}},
			comparePKs:  []compareColInfo{{1, collations.Local().LookupByName(sqltypes.NULL.String()), true, "textcol"}},
			pkCols:      []int{1},
			selectPks:   []int{1},
			orderBy: sqlparser.OrderBy{&sqlparser.Order{
				Expr:      &sqlparser.ColName{Name: sqlparser.NewIdentifierCI("textcol")},
				Direction: sqlparser.AscOrder,
			}},
		},
	}, {
		// text column as expression.
		input: &binlogdatapb.Rule{
			Match:  "pktext",
			Filter: "select c2, a+b as textcol from pktext",
		},
		table: "pktext",
		tablePlan: &tablePlan{
			dbName:      vdiffDBName,
			table:       testSchema.TableDefinitions[tableDefMap["pktext"]],
			sourceQuery: "select c2, a + b as textcol from pktext order by textcol asc",
			targetQuery: "select c2, textcol from pktext order by textcol asc",
			compareCols: []compareColInfo{{0, collations.Local().LookupByName(sqltypes.NULL.String()), false, "c2"}, {1, collations.Local().LookupByName(sqltypes.NULL.String()), true, "textcol"}},
			comparePKs:  []compareColInfo{{1, collations.Local().LookupByName(sqltypes.NULL.String()), true, "textcol"}},
			pkCols:      []int{1},
			selectPks:   []int{1},
			orderBy: sqlparser.OrderBy{&sqlparser.Order{
				Expr:      &sqlparser.ColName{Name: sqlparser.NewIdentifierCI("textcol")},
				Direction: sqlparser.AscOrder,
			}},
		},
	}, {
		// multiple pk columns.
		input: &binlogdatapb.Rule{
			Match: "multipk",
		},
		table: "multipk",
		tablePlan: &tablePlan{
			dbName:      vdiffDBName,
			table:       testSchema.TableDefinitions[tableDefMap["multipk"]],
			sourceQuery: "select c1, c2 from multipk order by c1 asc, c2 asc",
			targetQuery: "select c1, c2 from multipk order by c1 asc, c2 asc",
			compareCols: []compareColInfo{{0, collations.Local().LookupByName(sqltypes.NULL.String()), true, "c1"}, {1, collations.Local().LookupByName(sqltypes.NULL.String()), true, "c2"}},
			comparePKs:  []compareColInfo{{0, collations.Local().LookupByName(sqltypes.NULL.String()), true, "c1"}, {1, collations.Local().LookupByName(sqltypes.NULL.String()), true, "c2"}},
			pkCols:      []int{0, 1},
			selectPks:   []int{0, 1},
			orderBy: sqlparser.OrderBy{
				&sqlparser.Order{
					Expr:      &sqlparser.ColName{Name: sqlparser.NewIdentifierCI("c1")},
					Direction: sqlparser.AscOrder,
				},
				&sqlparser.Order{
					Expr:      &sqlparser.ColName{Name: sqlparser.NewIdentifierCI("c2")},
					Direction: sqlparser.AscOrder,
				},
			},
		},
	}, {
		// in_keyrange
		input: &binlogdatapb.Rule{
			Match:  "t1",
			Filter: "select * from t1 where in_keyrange('-80')",
		},
		table: "t1",
		tablePlan: &tablePlan{
			dbName:      vdiffDBName,
			table:       testSchema.TableDefinitions[tableDefMap["t1"]],
			sourceQuery: "select c1, c2 from t1 where in_keyrange('-80') order by c1 asc",
			targetQuery: "select c1, c2 from t1 order by c1 asc",
			compareCols: []compareColInfo{{0, collations.Local().LookupByName(sqltypes.NULL.String()), true, "c1"}, {1, collations.Local().LookupByName(sqltypes.NULL.String()), false, "c2"}},
			comparePKs:  []compareColInfo{{0, collations.Local().LookupByName(sqltypes.NULL.String()), true, "c1"}},
			pkCols:      []int{0},
			selectPks:   []int{0},
			orderBy: sqlparser.OrderBy{&sqlparser.Order{
				Expr:      &sqlparser.ColName{Name: sqlparser.NewIdentifierCI("c1")},
				Direction: sqlparser.AscOrder,
			}},
		},
	}, {
		// in_keyrange on RHS of AND.
		// This is currently not a valid construct, but will be supported in the future.
		input: &binlogdatapb.Rule{
			Match:  "t1",
			Filter: "select * from t1 where c2 = 2 and in_keyrange('-80')",
		},
		table: "t1",
		tablePlan: &tablePlan{
			dbName:      vdiffDBName,
			table:       testSchema.TableDefinitions[tableDefMap["t1"]],
			sourceQuery: "select c1, c2 from t1 where c2 = 2 and in_keyrange('-80') order by c1 asc",
			targetQuery: "select c1, c2 from t1 order by c1 asc",
			compareCols: []compareColInfo{{0, collations.Local().LookupByName(sqltypes.NULL.String()), true, "c1"}, {1, collations.Local().LookupByName(sqltypes.NULL.String()), false, "c2"}},
			comparePKs:  []compareColInfo{{0, collations.Local().LookupByName(sqltypes.NULL.String()), true, "c1"}},
			pkCols:      []int{0},
			selectPks:   []int{0},
			orderBy: sqlparser.OrderBy{&sqlparser.Order{
				Expr:      &sqlparser.ColName{Name: sqlparser.NewIdentifierCI("c1")},
				Direction: sqlparser.AscOrder,
			}},
		},
	}, {
		// in_keyrange on LHS of AND.
		// This is currently not a valid construct, but will be supported in the future.
		input: &binlogdatapb.Rule{
			Match:  "t1",
			Filter: "select * from t1 where in_keyrange('-80') and c2 = 2",
		},
		table: "t1",
		tablePlan: &tablePlan{
			dbName:      vdiffDBName,
			table:       testSchema.TableDefinitions[tableDefMap["t1"]],
			sourceQuery: "select c1, c2 from t1 where in_keyrange('-80') and c2 = 2 order by c1 asc",
			targetQuery: "select c1, c2 from t1 order by c1 asc",
			compareCols: []compareColInfo{{0, collations.Local().LookupByName(sqltypes.NULL.String()), true, "c1"}, {1, collations.Local().LookupByName(sqltypes.NULL.String()), false, "c2"}},
			comparePKs:  []compareColInfo{{0, collations.Local().LookupByName(sqltypes.NULL.String()), true, "c1"}},
			pkCols:      []int{0},
			selectPks:   []int{0},
			orderBy: sqlparser.OrderBy{&sqlparser.Order{
				Expr:      &sqlparser.ColName{Name: sqlparser.NewIdentifierCI("c1")},
				Direction: sqlparser.AscOrder,
			}},
		},
	}, {
		// in_keyrange on cascaded AND expression.
		// This is currently not a valid construct, but will be supported in the future.
		input: &binlogdatapb.Rule{
			Match:  "t1",
			Filter: "select * from t1 where c2 = 2 and c1 = 1 and in_keyrange('-80')",
		},
		table: "t1",
		tablePlan: &tablePlan{
			dbName:      vdiffDBName,
			table:       testSchema.TableDefinitions[tableDefMap["t1"]],
			sourceQuery: "select c1, c2 from t1 where c2 = 2 and c1 = 1 and in_keyrange('-80') order by c1 asc",
			targetQuery: "select c1, c2 from t1 order by c1 asc",
			compareCols: []compareColInfo{{0, collations.Local().LookupByName(sqltypes.NULL.String()), true, "c1"}, {1, collations.Local().LookupByName(sqltypes.NULL.String()), false, "c2"}},
			comparePKs:  []compareColInfo{{0, collations.Local().LookupByName(sqltypes.NULL.String()), true, "c1"}},
			pkCols:      []int{0},
			selectPks:   []int{0},
			orderBy: sqlparser.OrderBy{&sqlparser.Order{
				Expr:      &sqlparser.ColName{Name: sqlparser.NewIdentifierCI("c1")},
				Direction: sqlparser.AscOrder,
			}},
		},
	}, {
		// in_keyrange parenthesized.
		// This is currently not a valid construct, but will be supported in the future.
		input: &binlogdatapb.Rule{
			Match:  "t1",
			Filter: "select * from t1 where (c2 = 2 and in_keyrange('-80'))",
		},
		table: "t1",
		tablePlan: &tablePlan{
			dbName:      vdiffDBName,
			table:       testSchema.TableDefinitions[tableDefMap["t1"]],
			sourceQuery: "select c1, c2 from t1 where c2 = 2 and in_keyrange('-80') order by c1 asc",
			targetQuery: "select c1, c2 from t1 order by c1 asc",
			compareCols: []compareColInfo{{0, collations.Local().LookupByName(sqltypes.NULL.String()), true, "c1"}, {1, collations.Local().LookupByName(sqltypes.NULL.String()), false, "c2"}},
			comparePKs:  []compareColInfo{{0, collations.Local().LookupByName(sqltypes.NULL.String()), true, "c1"}},
			pkCols:      []int{0},
			selectPks:   []int{0},
			orderBy: sqlparser.OrderBy{&sqlparser.Order{
				Expr:      &sqlparser.ColName{Name: sqlparser.NewIdentifierCI("c1")},
				Direction: sqlparser.AscOrder,
			}},
		},
	}, {
		// group by
		input: &binlogdatapb.Rule{
			Match:  "t1",
			Filter: "select * from t1 group by c1",
		},
		table: "t1",
		tablePlan: &tablePlan{
			dbName:      vdiffDBName,
			table:       testSchema.TableDefinitions[tableDefMap["t1"]],
			sourceQuery: "select c1, c2 from t1 group by c1 order by c1 asc",
			targetQuery: "select c1, c2 from t1 order by c1 asc",
			compareCols: []compareColInfo{{0, collations.Local().LookupByName(sqltypes.NULL.String()), true, "c1"}, {1, collations.Local().LookupByName(sqltypes.NULL.String()), false, "c2"}},
			comparePKs:  []compareColInfo{{0, collations.Local().LookupByName(sqltypes.NULL.String()), true, "c1"}},
			pkCols:      []int{0},
			selectPks:   []int{0},
			orderBy: sqlparser.OrderBy{&sqlparser.Order{
				Expr:      &sqlparser.ColName{Name: sqlparser.NewIdentifierCI("c1")},
				Direction: sqlparser.AscOrder,
			}},
		},
	}, {
		// aggregations
		input: &binlogdatapb.Rule{
			Match:  "aggr",
			Filter: "select c1, c2, count(*) as c3, sum(c4) as c4 from t1 group by c1",
		},
		table: "aggr",
		tablePlan: &tablePlan{
			dbName:      vdiffDBName,
			table:       testSchema.TableDefinitions[tableDefMap["aggr"]],
			sourceQuery: "select c1, c2, count(*) as c3, sum(c4) as c4 from t1 group by c1 order by c1 asc",
			targetQuery: "select c1, c2, c3, c4 from aggr order by c1 asc",
			compareCols: []compareColInfo{{0, collations.Local().LookupByName(sqltypes.NULL.String()), true, "c1"}, {1, collations.Local().LookupByName(sqltypes.NULL.String()), false, "c2"}, {2, collations.Local().LookupByName(sqltypes.NULL.String()), false, "c3"}, {3, collations.Local().LookupByName(sqltypes.NULL.String()), false, "c4"}},
			comparePKs:  []compareColInfo{{0, collations.Local().LookupByName(sqltypes.NULL.String()), true, "c1"}},
			pkCols:      []int{0},
			selectPks:   []int{0},
			orderBy: sqlparser.OrderBy{&sqlparser.Order{
				Expr:      &sqlparser.ColName{Name: sqlparser.NewIdentifierCI("c1")},
				Direction: sqlparser.AscOrder,
			}},
			aggregates: []*engine.AggregateParams{
				engine.NewAggregateParam(opcode.AggregateSum, 2, ""),
				engine.NewAggregateParam(opcode.AggregateSum, 3, ""),
			},
		},
	}, {
		// date conversion on import.
		input: &binlogdatapb.Rule{
			Match: "datze",
		},
		sourceTimeZone: "US/Pacific",
		table:          "datze",
		tablePlan: &tablePlan{
			dbName:      vdiffDBName,
			table:       testSchema.TableDefinitions[tableDefMap["datze"]],
			sourceQuery: "select id, dt from datze order by id asc",
			targetQuery: "select id, convert_tz(dt, 'UTC', 'US/Pacific') as dt from datze order by id asc",
			compareCols: []compareColInfo{{0, collations.Local().LookupByName(sqltypes.NULL.String()), true, "id"}, {1, collations.Local().LookupByName(sqltypes.NULL.String()), false, "dt"}},
			comparePKs:  []compareColInfo{{0, collations.Local().LookupByName(sqltypes.NULL.String()), true, "id"}},
			pkCols:      []int{0},
			selectPks:   []int{0},
			orderBy: sqlparser.OrderBy{&sqlparser.Order{
				Expr:      &sqlparser.ColName{Name: sqlparser.NewIdentifierCI("id")},
				Direction: sqlparser.AscOrder,
			}},
		},
	}}

	for _, tcase := range testcases {
		t.Run(tcase.input.Filter, func(t *testing.T) {
			if tcase.sourceTimeZone != "" {
				ct.targetTimeZone = "UTC"
				ct.sourceTimeZone = tcase.sourceTimeZone
				defer func() {
					ct.targetTimeZone = ""
					ct.sourceTimeZone = ""
				}()
			}
			dbc := binlogplayer.NewMockDBClient(t)
			filter := &binlogdatapb.Filter{Rules: []*binlogdatapb.Rule{tcase.input}}
			vdiffenv.opts.CoreOptions.Tables = tcase.table
			wd, err := newWorkflowDiffer(ct, vdiffenv.opts)
			require.NoError(t, err)
			dbc.ExpectRequestRE("select vdt.lastpk as lastpk, vdt.mismatch as mismatch, vdt.report as report", noResults, nil)
			columnList := make([]string, len(tcase.tablePlan.comparePKs))
			collationList := make([]string, len(tcase.tablePlan.comparePKs))
			for i := range tcase.tablePlan.comparePKs {
				columnList[i] = tcase.tablePlan.comparePKs[i].colName
				if tcase.tablePlan.comparePKs[i].collation != nil {
					collationList[i] = tcase.tablePlan.comparePKs[i].collation.Name()
				} else {
					collationList[i] = sqltypes.NULL.String()
				}
			}
			columnBV, err := sqltypes.BuildBindVariable(columnList)
			require.NoError(t, err)
			query, err := sqlparser.ParseAndBind(sqlSelectColumnCollations,
				sqltypes.StringBindVariable(vdiffDBName),
				sqltypes.StringBindVariable(tcase.tablePlan.table.Name),
				columnBV,
			)
			require.NoError(t, err)
			dbc.ExpectRequest(query, sqltypes.MakeTestResult(sqltypes.MakeTestFields(
				"collation_name",
				"varchar",
			),
				collationList...,
			), nil)
			err = wd.buildPlan(dbc, filter, testSchema)
			require.NoError(t, err, tcase.input)
			require.Equal(t, 1, len(wd.tableDiffers), tcase.input)
			assert.Equal(t, tcase.tablePlan, wd.tableDiffers[tcase.table].tablePlan, tcase.input)

			// Confirm that the options are passed through.
			for _, td := range wd.tableDiffers {
				require.Equal(t, vdiffenv.opts, td.wd.opts)
			}
		})
	}
}

func TestBuildPlanInclude(t *testing.T) {
	vdenv := newTestVDiffEnv(t)
	defer vdenv.close()

	controllerQR := sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		vdiffTestCols,
		vdiffTestColTypes,
	),
		fmt.Sprintf("1|%s|%s|%s|%s|%s|%s|%s|", uuid.New(), vdiffenv.workflow, tstenv.KeyspaceName, tstenv.ShardName, vdiffDBName, PendingState, optionsJS),
	)
	vdiffenv.dbClient.ExpectRequest("select * from _vt.vdiff where id = 1", noResults, nil)
	ct, err := newController(context.Background(), controllerQR.Named().Row(), vdiffenv.dbClientFactory, tstenv.TopoServ, vdiffenv.vde, vdiffenv.opts)
	require.NoError(t, err)

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
	vdiffenv.tmc.schema = schm
	defer func() {
		vdiffenv.tmc.schema = testSchema
	}()
	rule := &binlogdatapb.Rule{
		Match: "/.*",
	}
	filter := &binlogdatapb.Filter{Rules: []*binlogdatapb.Rule{rule}}

	testcases := []struct {
		tables []string
	}{
		{tables: []string{"t2"}},
		{tables: []string{"t2", "t3"}},
		{tables: []string{"t1", "t2", "t3", "t4"}},
		{tables: []string{"t1", "t2", "t3", "t4"}},
	}

	for _, tcase := range testcases {
		dbc := binlogplayer.NewMockDBClient(t)
		vdiffenv.opts.CoreOptions.Tables = strings.Join(tcase.tables, ",")
		wd, err := newWorkflowDiffer(ct, vdiffenv.opts)
		require.NoError(t, err)
		for _, table := range tcase.tables {
			query := fmt.Sprintf(`select vdt.lastpk as lastpk, vdt.mismatch as mismatch, vdt.report as report
						from _vt.vdiff as vd inner join _vt.vdiff_table as vdt on (vd.id = vdt.vdiff_id)
						where vdt.vdiff_id = 1 and vdt.table_name = '%s'`, table)
			dbc.ExpectRequest(query, noResults, nil)
			dbc.ExpectRequestRE("select column_name as column_name, collation_name as collation_name from information_schema.columns .*", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
				"collation_name",
				"varchar",
			),
				"NULL",
			), nil)
		}
		err = wd.buildPlan(dbc, filter, schm)
		require.NoError(t, err)
		require.Equal(t, len(tcase.tables), len(wd.tableDiffers))
	}
}

func TestBuildPlanFailure(t *testing.T) {
	vdenv := newTestVDiffEnv(t)
	defer vdenv.close()
	UUID := uuid.New()

	controllerQR := sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		vdiffTestCols,
		vdiffTestColTypes,
	),
		fmt.Sprintf("1|%s|%s|%s|%s|%s|%s|%s|", UUID, vdiffenv.workflow, tstenv.KeyspaceName, tstenv.ShardName, vdiffDBName, PendingState, optionsJS),
	)
	vdiffenv.dbClient.ExpectRequest("select * from _vt.vdiff where id = 1", noResults, nil)
	ct, err := newController(context.Background(), controllerQR.Named().Row(), vdiffenv.dbClientFactory, tstenv.TopoServ, vdiffenv.vde, vdiffenv.opts)
	require.NoError(t, err)

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
		err: "column c3 not found in table t1 on tablet cell:\"cell1\" uid:100",
	}}
	for _, tcase := range testcases {
		dbc := binlogplayer.NewMockDBClient(t)
		filter := &binlogdatapb.Filter{Rules: []*binlogdatapb.Rule{tcase.input}}
		vdiffenv.opts.CoreOptions.Tables = tcase.input.Match
		wd, err := newWorkflowDiffer(ct, vdiffenv.opts)
		require.NoError(t, err)
		dbc.ExpectRequestRE("select vdt.lastpk as lastpk, vdt.mismatch as mismatch, vdt.report as report", noResults, nil)
		err = wd.buildPlan(dbc, filter, testSchema)
		assert.EqualError(t, err, tcase.err, tcase.input)
	}
}
