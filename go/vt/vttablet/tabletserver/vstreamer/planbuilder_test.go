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

package vstreamer

import (
	"fmt"
	"testing"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/vt/proto/topodata"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/utils"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/json2"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
)

var testLocalVSchema *localVSchema

func init() {
	input := `{
  "sharded": true,
  "vindexes": {
    "hash": {
      "type": "hash"
    },
    "region_vdx": {
      "type": "region_experimental",
			"params": {
				"region_bytes": "1"
			}
    }
  },
  "tables": {
    "t1": {
      "column_vindexes": [
        {
          "column": "id",
          "name": "hash"
        }
      ]
    },
    "regional": {
      "column_vindexes": [
        {
          "columns": [
						"region",
						"id"
					],
          "name": "region_vdx"
        }
      ]
    }
  }
}`
	var kspb vschemapb.Keyspace
	if err := json2.Unmarshal([]byte(input), &kspb); err != nil {
		panic(fmt.Errorf("Unmarshal failed: %v", err))
	}
	srvVSchema := &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"ks": &kspb,
		},
	}
	vschema := vindexes.BuildVSchema(srvVSchema)
	testLocalVSchema = &localVSchema{
		keyspace: "ks",
		vschema:  vschema,
	}
}

func TestMustSendDDL(t *testing.T) {
	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match: "/t1.*/",
		}, {
			Match: "t2",
		}},
	}
	testcases := []struct {
		sql    string
		db     string
		output bool
	}{{
		sql:    "create database db",
		output: false,
	}, {
		sql:    "create table foo(id int)",
		output: false,
	}, {
		sql:    "create table db.foo(id int)",
		output: false,
	}, {
		sql:    "create table mydb.foo(id int)",
		output: false,
	}, {
		sql:    "create table t1a(id int)",
		output: true,
	}, {
		sql:    "create table db.t1a(id int)",
		output: false,
	}, {
		sql:    "create table mydb.t1a(id int)",
		output: true,
	}, {
		sql:    "rename table t1a to foo, foo to bar",
		output: true,
	}, {
		sql:    "rename table foo to t1a, foo to bar",
		output: true,
	}, {
		sql:    "rename table foo to bar, t1a to bar",
		output: true,
	}, {
		sql:    "rename table foo to bar, bar to foo",
		output: false,
	}, {
		sql:    "drop table t1a, foo",
		output: true,
	}, {
		sql:    "drop table foo, t1a",
		output: true,
	}, {
		sql:    "drop table foo, bar",
		output: false,
	}, {
		sql:    "bad query",
		output: true,
	}, {
		sql:    "select * from t",
		output: true,
	}, {
		sql:    "drop table t2",
		output: true,
	}, {
		sql:    "create table t1a(id int)",
		db:     "db",
		output: false,
	}, {
		sql:    "create table t1a(id int)",
		db:     "mydb",
		output: true,
	}}
	for _, tcase := range testcases {
		q := mysql.Query{SQL: tcase.sql, Database: tcase.db}
		got := mustSendDDL(q, "mydb", filter)
		if got != tcase.output {
			t.Errorf("%v: %v, want %v", q, got, tcase.output)
		}
	}
}

func TestPlanBuilder(t *testing.T) {
	t1 := &Table{
		Name: "t1",
		Fields: []*querypb.Field{{
			Name: "id",
			Type: sqltypes.Int64,
		}, {
			Name: "val",
			Type: sqltypes.VarBinary,
		}},
	}
	// t1alt has no id column
	t1alt := &Table{
		Name: "t1",
		Fields: []*querypb.Field{{
			Name: "val",
			Type: sqltypes.VarBinary,
		}},
	}
	t2 := &Table{
		Name: "t2",
		Fields: []*querypb.Field{{
			Name: "id",
			Type: sqltypes.Int64,
		}, {
			Name: "val",
			Type: sqltypes.VarBinary,
		}},
	}
	regional := &Table{
		Name: "regional",
		Fields: []*querypb.Field{{
			Name: "region",
			Type: sqltypes.Int64,
		}, {
			Name: "id",
			Type: sqltypes.Int64,
		}, {
			Name: "val",
			Type: sqltypes.VarBinary,
		}},
	}

	testcases := []struct {
		inTable *Table
		inRule  *binlogdatapb.Rule
		outPlan *Plan
		outErr  string
	}{{
		inTable: t1,
		inRule:  &binlogdatapb.Rule{Match: "/.*/"},
		outPlan: &Plan{
			ColExprs: []ColExpr{{
				ColNum: 0,
				Field: &querypb.Field{
					Name: "id",
					Type: sqltypes.Int64,
				},
			}, {
				ColNum: 1,
				Field: &querypb.Field{
					Name: "val",
					Type: sqltypes.VarBinary,
				},
			}},
		},
	}, {
		inTable: t1,
		inRule:  &binlogdatapb.Rule{Match: "/.*/", Filter: "-80"},
		outPlan: &Plan{
			ColExprs: []ColExpr{{
				ColNum: 0,
				Field: &querypb.Field{
					Name: "id",
					Type: sqltypes.Int64,
				},
			}, {
				ColNum: 1,
				Field: &querypb.Field{
					Name: "val",
					Type: sqltypes.VarBinary,
				},
			}},
			Filters: []Filter{{
				Opcode:        VindexMatch,
				ColNum:        0,
				Value:         sqltypes.NULL,
				Vindex:        nil,
				VindexColumns: []int{0},
				KeyRange:      nil,
			}},
		},
	}, {
		inTable: t1,
		inRule:  &binlogdatapb.Rule{Match: "t1", Filter: "select * from t1"},
		outPlan: &Plan{
			ColExprs: []ColExpr{{
				ColNum: 0,
				Field: &querypb.Field{
					Name: "id",
					Type: sqltypes.Int64,
				},
			}, {
				ColNum: 1,
				Field: &querypb.Field{
					Name: "val",
					Type: sqltypes.VarBinary,
				},
			}},
		},
	}, {
		inTable: t1,
		inRule:  &binlogdatapb.Rule{Match: "t1", Filter: "select id, val from t1"},
		outPlan: &Plan{
			ColExprs: []ColExpr{{
				ColNum: 0,
				Field: &querypb.Field{
					Name: "id",
					Type: sqltypes.Int64,
				},
			}, {
				ColNum: 1,
				Field: &querypb.Field{
					Name: "val",
					Type: sqltypes.VarBinary,
				},
			}},
		},
	}, {
		inTable: t1,
		inRule:  &binlogdatapb.Rule{Match: "t1", Filter: "select val, id from t1"},
		outPlan: &Plan{
			ColExprs: []ColExpr{{
				ColNum: 1,
				Field: &querypb.Field{
					Name: "val",
					Type: sqltypes.VarBinary,
				},
			}, {
				ColNum: 0,
				Field: &querypb.Field{
					Name: "id",
					Type: sqltypes.Int64,
				},
			}},
		},
	}, {
		inTable: t1,
		inRule:  &binlogdatapb.Rule{Match: "t1", Filter: "select val, id from t1 where in_keyrange(id, 'hash', '-80')"},
		outPlan: &Plan{
			ColExprs: []ColExpr{{
				ColNum: 1,
				Field: &querypb.Field{
					Name: "val",
					Type: sqltypes.VarBinary,
				},
			}, {
				ColNum: 0,
				Field: &querypb.Field{
					Name: "id",
					Type: sqltypes.Int64,
				},
			}},
			Filters: []Filter{{
				Opcode:        VindexMatch,
				ColNum:        0,
				Value:         sqltypes.NULL,
				Vindex:        nil,
				VindexColumns: []int{0},
				KeyRange:      nil,
			}},
		},
	}, {
		inTable: t1,
		inRule:  &binlogdatapb.Rule{Match: "t1", Filter: "select val, id from t1 where in_keyrange('-80')"},
		outPlan: &Plan{
			ColExprs: []ColExpr{{
				ColNum: 1,
				Field: &querypb.Field{
					Name: "val",
					Type: sqltypes.VarBinary,
				},
			}, {
				ColNum: 0,
				Field: &querypb.Field{
					Name: "id",
					Type: sqltypes.Int64,
				},
			}},
			Filters: []Filter{{
				Opcode:        VindexMatch,
				ColNum:        0,
				Value:         sqltypes.NULL,
				Vindex:        nil,
				VindexColumns: []int{0},
				KeyRange:      nil,
			}},
		},
	}, {
		inTable: t1,
		inRule:  &binlogdatapb.Rule{Match: "t1", Filter: "select val, id from t1 where id = 1"},
		outPlan: &Plan{
			ColExprs: []ColExpr{{
				ColNum: 1,
				Field: &querypb.Field{
					Name: "val",
					Type: sqltypes.VarBinary,
				},
			}, {
				ColNum: 0,
				Field: &querypb.Field{
					Name: "id",
					Type: sqltypes.Int64,
				},
			}},
			Filters: []Filter{{
				Opcode:        Equal,
				ColNum:        0,
				Value:         sqltypes.NewInt64(1),
				Vindex:        nil,
				VindexColumns: nil,
				KeyRange:      nil,
			}},
		},
	}, {
		inTable: t2,
		inRule:  &binlogdatapb.Rule{Match: "/t1/"},
	}, {
		inTable: regional,
		inRule:  &binlogdatapb.Rule{Match: "regional", Filter: "select val, id from regional where in_keyrange('-80')"},
		outPlan: &Plan{
			ColExprs: []ColExpr{{
				ColNum: 2,
				Field: &querypb.Field{
					Name: "val",
					Type: sqltypes.VarBinary,
				},
			}, {
				ColNum: 1,
				Field: &querypb.Field{
					Name: "id",
					Type: sqltypes.Int64,
				},
			}},
			Filters: []Filter{{
				Opcode:        VindexMatch,
				ColNum:        0,
				Value:         sqltypes.NULL,
				Vindex:        nil,
				VindexColumns: []int{0, 1},
				KeyRange:      nil,
			}},
		},
	}, {
		inTable: regional,
		inRule:  &binlogdatapb.Rule{Match: "regional", Filter: "select id, keyspace_id() from regional"},
		outPlan: &Plan{
			ColExprs: []ColExpr{{
				ColNum: 1,
				Field: &querypb.Field{
					Name: "id",
					Type: sqltypes.Int64,
				},
			}, {
				Field: &querypb.Field{
					Name: "keyspace_id",
					Type: sqltypes.VarBinary,
				},
				Vindex:        testLocalVSchema.vschema.Keyspaces["ks"].Vindexes["region_vdx"],
				VindexColumns: []int{0, 1},
			}},
		},
	}, {
		inTable: t1,
		inRule:  &binlogdatapb.Rule{Match: "/*/"},
		outErr:  "error parsing regexp: missing argument to repetition operator: `*`",
	}, {
		inTable: t2,
		inRule:  &binlogdatapb.Rule{Match: "/.*/", Filter: "-80"},
		outErr:  `table t2 not found`,
	}, {
		inTable: t1alt,
		inRule:  &binlogdatapb.Rule{Match: "/.*/", Filter: "-80"},
		outErr:  `column id not found in table t1`,
	}, {
		inTable: t1,
		inRule:  &binlogdatapb.Rule{Match: "/.*/", Filter: "80"},
		outErr:  `malformed spec: doesn't define a range: "80"`,
	}, {
		inTable: t1,
		inRule:  &binlogdatapb.Rule{Match: "/.*/", Filter: "-80-"},
		outErr:  `error parsing keyrange: -80-`,
	}, {
		inTable: t1,
		inRule:  &binlogdatapb.Rule{Match: "t1", Filter: "bad query"},
		outErr:  `syntax error at position 4 near 'bad'`,
	}, {
		inTable: t1,
		inRule:  &binlogdatapb.Rule{Match: "t1", Filter: "delete from t1"},
		outErr:  `unsupported: delete from t1`,
	}, {
		inTable: t1,
		inRule:  &binlogdatapb.Rule{Match: "t1", Filter: "select * from t1, t2"},
		outErr:  `unsupported: select * from t1, t2`,
	}, {
		inTable: t1,
		inRule:  &binlogdatapb.Rule{Match: "t1", Filter: "select * from t1 join t2"},
		outErr:  `unsupported: select * from t1 join t2`,
	}, {
		inTable: t1,
		inRule:  &binlogdatapb.Rule{Match: "t1", Filter: "select * from a.t1"},
		outErr:  `unsupported: select * from a.t1`,
	}, {
		inTable: t1,
		inRule:  &binlogdatapb.Rule{Match: "t1", Filter: "select * from t2"},
		outErr:  `unsupported: select expression table t2 does not match the table entry name t1`,
	}, {
		inTable: t1,
		inRule:  &binlogdatapb.Rule{Match: "t1", Filter: "select *, id from t1"},
		outErr:  `unsupported: *, id`,
	}, {
		inTable: t1,
		inRule:  &binlogdatapb.Rule{Match: "t1", Filter: "select id, val from t1 where max(id)"},
		outErr:  `unsupported constraint: max(id)`,
	}, {
		inTable: t1,
		inRule:  &binlogdatapb.Rule{Match: "t1", Filter: "select id, val from t1 where in_keyrange(id)"},
		outErr:  `unsupported: id`,
	}, {
		inTable: t1,
		inRule:  &binlogdatapb.Rule{Match: "t1", Filter: "select id, val from t1 where in_keyrange(*, 'hash', '-80')"},
		outErr:  `[BUG] unexpected: *sqlparser.StarExpr *`,
	}, {
		inTable: t1,
		inRule:  &binlogdatapb.Rule{Match: "t1", Filter: "select id, val from t1 where in_keyrange(1, 'hash', '-80')"},
		outErr:  `[BUG] unexpected: *sqlparser.Literal 1`,
	}, {
		inTable: t1,
		inRule:  &binlogdatapb.Rule{Match: "t1", Filter: "select id, val from t1 where in_keyrange(id, 'lookup', '-80')"},
		outErr:  `vindex must be Unique to be used for VReplication: lookup`,
	}, {
		inTable: t1,
		inRule:  &binlogdatapb.Rule{Match: "t1", Filter: "select id, val from t1 where in_keyrange(id, 'hash', '80')"},
		outErr:  `malformed spec: doesn't define a range: "80"`,
	}, {
		inTable: t1,
		inRule:  &binlogdatapb.Rule{Match: "t1", Filter: "select id, val from t1 where in_keyrange(id, 'hash', '-80-')"},
		outErr:  `unexpected in_keyrange parameter: '-80-'`,
	}, {
		// analyzeExpr tests.
		inTable: t1,
		inRule:  &binlogdatapb.Rule{Match: "t1", Filter: "select id, * from t1"},
		outErr:  `unsupported: *`,
	}, {
		inTable: t1,
		inRule:  &binlogdatapb.Rule{Match: "t1", Filter: "select none from t1"},
		outErr:  "column `none` not found in table t1",
	}, {
		inTable: t1,
		inRule:  &binlogdatapb.Rule{Match: "t1", Filter: "select id, val, max(val) from t1"},
		outErr:  `unsupported function: max(val)`,
	}, {
		inTable: t1,
		inRule:  &binlogdatapb.Rule{Match: "t1", Filter: "select id+1, val from t1"},
		outErr:  `unsupported: id + 1`,
	}, {
		inTable: t1,
		inRule:  &binlogdatapb.Rule{Match: "t1", Filter: "select t1.id, val from t1"},
		outErr:  `unsupported qualifier for column: t1.id`,
	}, {
		// selString
		inTable: t1,
		inRule:  &binlogdatapb.Rule{Match: "t1", Filter: "select id, val from t1 where in_keyrange(id, *, '-80')"},
		outErr:  `unsupported: *`,
	}, {
		inTable: t1,
		inRule:  &binlogdatapb.Rule{Match: "t1", Filter: "select id, val from t1 where in_keyrange(id, 1+1, '-80')"},
		outErr:  `unsupported: 1 + 1`,
	}}
	for _, tcase := range testcases {
		t.Run(tcase.inRule.String(), func(t *testing.T) {
			plan, err := buildPlan(tcase.inTable, testLocalVSchema, &binlogdatapb.Filter{
				Rules: []*binlogdatapb.Rule{tcase.inRule},
			})

			if tcase.outErr != "" {
				assert.Nil(t, plan)
				assert.EqualError(t, err, tcase.outErr)
				return
			}

			require.NoError(t, err)
			if tcase.outPlan == nil {
				require.Nil(t, plan)
				return
			}

			require.NotNil(t, plan)
			plan.Table = nil
			for ind := range plan.Filters {
				plan.Filters[ind].KeyRange = nil
				if plan.Filters[ind].Opcode == VindexMatch {
					plan.Filters[ind].Value = sqltypes.NULL
				}
				plan.Filters[ind].Vindex = nil
				plan.Filters[ind].Vindex = nil
			}
			utils.MustMatch(t, tcase.outPlan, plan)
		})
	}
}

func TestPlanBuilderFilterComparison(t *testing.T) {
	t1 := &Table{
		Name: "t1",
		Fields: []*querypb.Field{{
			Name: "id",
			Type: sqltypes.Int64,
		}, {
			Name: "val",
			Type: sqltypes.VarBinary,
		}},
	}
	hashVindex, err := vindexes.CreateVindex("hash", "hash", nil)
	require.NoError(t, err)
	testcases := []struct {
		name       string
		inFilter   string
		outFilters []Filter
		outErr     string
	}{{
		name:       "equal",
		inFilter:   "select * from t1 where id = 1",
		outFilters: []Filter{{Opcode: Equal, ColNum: 0, Value: sqltypes.NewInt64(1)}},
	}, {
		name:       "not-equal",
		inFilter:   "select * from t1 where id <> 1",
		outFilters: []Filter{{Opcode: NotEqual, ColNum: 0, Value: sqltypes.NewInt64(1)}},
	}, {
		name:       "greater",
		inFilter:   "select * from t1 where val > 'abc'",
		outFilters: []Filter{{Opcode: GreaterThan, ColNum: 1, Value: sqltypes.NewVarChar("abc")}},
	}, {
		name:       "greater-than",
		inFilter:   "select * from t1 where id >= 1",
		outFilters: []Filter{{Opcode: GreaterThanEqual, ColNum: 0, Value: sqltypes.NewInt64(1)}},
	}, {
		name:     "less-than-with-and",
		inFilter: "select * from t1 where id < 2 and val <= 'xyz'",
		outFilters: []Filter{{Opcode: LessThan, ColNum: 0, Value: sqltypes.NewInt64(2)},
			{Opcode: LessThanEqual, ColNum: 1, Value: sqltypes.NewVarChar("xyz")},
		},
	}, {
		name:     "vindex-and-operators",
		inFilter: "select * from t1 where in_keyrange(id, 'hash', '-80') and id = 2 and val <> 'xyz'",
		outFilters: []Filter{
			{
				Opcode:        VindexMatch,
				ColNum:        0,
				Value:         sqltypes.NULL,
				Vindex:        hashVindex,
				VindexColumns: []int{0},
				KeyRange: &topodata.KeyRange{
					Start: nil,
					End:   []byte("\200"),
				},
			},
			{Opcode: Equal, ColNum: 0, Value: sqltypes.NewInt64(2)},
			{Opcode: NotEqual, ColNum: 1, Value: sqltypes.NewVarChar("xyz")},
		},
	}}

	for _, tcase := range testcases {
		t.Run(tcase.name, func(t *testing.T) {
			plan, err := buildPlan(t1, testLocalVSchema, &binlogdatapb.Filter{
				Rules: []*binlogdatapb.Rule{{Match: "t1", Filter: tcase.inFilter}},
			})

			if tcase.outErr != "" {
				assert.Nil(t, plan)
				assert.EqualError(t, err, tcase.outErr)
				return
			}
			require.NotNil(t, plan)
			require.ElementsMatchf(t, tcase.outFilters, plan.Filters, "want %+v, got: %+v", tcase.outFilters, plan.Filters)
		})
	}
}

func TestCompare(t *testing.T) {
	type testcase struct {
		opcode                   Opcode
		columnValue, filterValue sqltypes.Value
		want                     bool
	}
	int1 := sqltypes.NewInt32(1)
	int2 := sqltypes.NewInt32(2)
	testcases := []*testcase{
		{opcode: Equal, columnValue: int1, filterValue: int1, want: true},
		{opcode: Equal, columnValue: int1, filterValue: int2, want: false},
		{opcode: Equal, columnValue: int1, filterValue: sqltypes.NULL, want: false},
		{opcode: LessThan, columnValue: int2, filterValue: int1, want: false},
		{opcode: LessThan, columnValue: int1, filterValue: int2, want: true},
		{opcode: LessThan, columnValue: int1, filterValue: sqltypes.NULL, want: false},
		{opcode: GreaterThan, columnValue: int2, filterValue: int1, want: true},
		{opcode: GreaterThan, columnValue: int1, filterValue: int2, want: false},
		{opcode: GreaterThan, columnValue: int1, filterValue: sqltypes.NULL, want: false},
		{opcode: NotEqual, columnValue: int1, filterValue: int1, want: false},
		{opcode: NotEqual, columnValue: int1, filterValue: int2, want: true},
		{opcode: NotEqual, columnValue: sqltypes.NULL, filterValue: int1, want: false},
		{opcode: LessThanEqual, columnValue: int1, filterValue: sqltypes.NULL, want: false},
		{opcode: GreaterThanEqual, columnValue: int2, filterValue: int1, want: true},
		{opcode: LessThanEqual, columnValue: int2, filterValue: int1, want: false},
		{opcode: GreaterThanEqual, columnValue: int1, filterValue: int1, want: true},
		{opcode: LessThanEqual, columnValue: int1, filterValue: int2, want: true},
	}
	for _, tc := range testcases {
		t.Run("", func(t *testing.T) {
			got, err := compare(tc.opcode, tc.columnValue, tc.filterValue, collations.CollationUtf8mb4ID)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}
