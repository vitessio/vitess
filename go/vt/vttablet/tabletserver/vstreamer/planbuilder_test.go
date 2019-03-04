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
	"reflect"
	"testing"

	"vitess.io/vitess/go/json2"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
)

var testKSChema *vindexes.KeyspaceSchema

func init() {
	input := `{
  "sharded": true,
  "vindexes": {
    "hash": {
      "type": "hash"
    },
    "lookup": {
      "type": "lookup"
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
    }
  }
}`
	var kspb vschemapb.Keyspace
	if err := json2.Unmarshal([]byte(input), &kspb); err != nil {
		panic(fmt.Errorf("Unmarshal failed: %v", err))
	}
	kschema, err := vindexes.BuildKeyspaceSchema(&kspb, "ks")
	if err != nil {
		panic(err)
	}
	testKSChema = kschema
}

/*
func TestBuildSelect(t *testing.T) {
	t1 := &Table{
		TableMap: &mysql.TableMap{
			Name: "t1",
		},
		Columns: []schema.TableColumn{{
			Name: sqlparser.NewColIdent("v1"),
			Type: sqltypes.Int64,
		}, {
			Name: sqlparser.NewColIdent("v2"),
			Type: sqltypes.VarBinary,
		}, {
			Name: sqlparser.NewColIdent("v3"),
			Type: sqltypes.VarBinary,
		}},
		PKColumns: []int{0},
	}
	t2 := &Table{
		TableMap: &mysql.TableMap{
			Name: "t2",
		},
		Columns: []schema.TableColumn{{
			Name: sqlparser.NewColIdent("v1"),
			Type: sqltypes.Int64,
		}, {
			Name: sqlparser.NewColIdent("v2"),
			Type: sqltypes.VarBinary,
		}, {
			Name: sqlparser.NewColIdent("v3"),
			Type: sqltypes.VarBinary,
		}},
		PKColumns: []int{0, 1},
	}

	testcases := []struct {
		inTable   *Table
		inValues  []sqltypes.Value
		outSelect string
		outErr    string
	}{{
		inTable:   t1,
		inValues:  nil,
		outSelect: "select v1, v2, v3 from t1 order by v1",
	}, {
		inTable:   t2,
		inValues:  nil,
		outSelect: "select v1, v2, v3 from t2 order by v1, v2",
	}, {
		inTable:   t1,
		inValues:  []sqltypes.Value{sqltypes.NewInt64(1)},
		outSelect: "select v1, v2, v3 from t1 v1 > 1 order by v1",
	}, {
		inTable:   t2,
		inValues:  []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)},
		outSelect: "select v1, v2, v3 from t2 v1 >= 1 and v2 > 2 order by v1, v2",
	}, {
		inTable:  t2,
		inValues: []sqltypes.Value{sqltypes.NewInt64(1)},
		outErr:   "primary key values don't match length: [INT64(1)] vs [0 1]",
	}}
	for _, tcase := range testcases {
		got, err := tcase.inTable.buildSelect(tcase.inValues)
		gotErr := ""
		if err != nil {
			gotErr = err.Error()
		}
		if gotErr != tcase.outErr {
			t.Errorf("buildSelect(%v, %v) err: %v, want %v", tcase.inTable, tcase.inValues, err, tcase.outErr)
		}
		if got != tcase.outSelect {
			t.Errorf("buildSelect(%v, %v): %v, want %v", tcase.inTable, tcase.inValues, got, tcase.outSelect)
		}
	}
}
*/

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

func TestPlanbuilder(t *testing.T) {
	t1 := &Table{
		Name: "t1",
		Columns: []schema.TableColumn{{
			Name: sqlparser.NewColIdent("id"),
			Type: sqltypes.Int64,
		}, {
			Name: sqlparser.NewColIdent("val"),
			Type: sqltypes.VarBinary,
		}},
	}
	// t1alt has no id column
	t1alt := &Table{
		Name: "t1",
		Columns: []schema.TableColumn{{
			Name: sqlparser.NewColIdent("val"),
			Type: sqltypes.VarBinary,
		}},
	}
	t2 := &Table{
		Name: "t2",
		Columns: []schema.TableColumn{{
			Name: sqlparser.NewColIdent("id"),
			Type: sqltypes.Int64,
		}, {
			Name: sqlparser.NewColIdent("val"),
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
				Alias:  sqlparser.NewColIdent("id"),
				Type:   sqltypes.Int64,
			}, {
				ColNum: 1,
				Alias:  sqlparser.NewColIdent("val"),
				Type:   sqltypes.VarBinary,
			}},
		},
	}, {
		inTable: t1,
		inRule:  &binlogdatapb.Rule{Match: "/.*/", Filter: "-80"},
		outPlan: &Plan{
			ColExprs: []ColExpr{{
				ColNum: 0,
				Alias:  sqlparser.NewColIdent("id"),
				Type:   sqltypes.Int64,
			}, {
				ColNum: 1,
				Alias:  sqlparser.NewColIdent("val"),
				Type:   sqltypes.VarBinary,
			}},
			VindexColumn: 0,
		},
	}, {
		inTable: t1,
		inRule:  &binlogdatapb.Rule{Match: "t1", Filter: "select * from t1"},
		outPlan: &Plan{
			ColExprs: []ColExpr{{
				ColNum: 0,
				Alias:  sqlparser.NewColIdent("id"),
				Type:   sqltypes.Int64,
			}, {
				ColNum: 1,
				Alias:  sqlparser.NewColIdent("val"),
				Type:   sqltypes.VarBinary,
			}},
		},
	}, {
		inTable: t1,
		inRule:  &binlogdatapb.Rule{Match: "t1", Filter: "select id, val from t1"},
		outPlan: &Plan{
			ColExprs: []ColExpr{{
				ColNum: 0,
				Alias:  sqlparser.NewColIdent("id"),
				Type:   sqltypes.Int64,
			}, {
				ColNum: 1,
				Alias:  sqlparser.NewColIdent("val"),
				Type:   sqltypes.VarBinary,
			}},
		},
	}, {
		inTable: t1,
		inRule:  &binlogdatapb.Rule{Match: "t1", Filter: "select val, id from t1"},
		outPlan: &Plan{
			ColExprs: []ColExpr{{
				ColNum: 1,
				Alias:  sqlparser.NewColIdent("val"),
				Type:   sqltypes.VarBinary,
			}, {
				ColNum: 0,
				Alias:  sqlparser.NewColIdent("id"),
				Type:   sqltypes.Int64,
			}},
		},
	}, {
		inTable: t1,
		inRule:  &binlogdatapb.Rule{Match: "t1", Filter: "select val, id from t1 where in_keyrange(id, 'hash', '-80')"},
		outPlan: &Plan{
			ColExprs: []ColExpr{{
				ColNum: 1,
				Alias:  sqlparser.NewColIdent("val"),
				Type:   sqltypes.VarBinary,
			}, {
				ColNum: 0,
				Alias:  sqlparser.NewColIdent("id"),
				Type:   sqltypes.Int64,
			}},
			VindexColumn: 1,
		},
	}, {
		inTable: t1,
		inRule:  &binlogdatapb.Rule{Match: "t1", Filter: "select val, id from t1 where in_keyrange('-80')"},
		outPlan: &Plan{
			ColExprs: []ColExpr{{
				ColNum: 1,
				Alias:  sqlparser.NewColIdent("val"),
				Type:   sqltypes.VarBinary,
			}, {
				ColNum: 0,
				Alias:  sqlparser.NewColIdent("id"),
				Type:   sqltypes.Int64,
			}},
			VindexColumn: 1,
		},
	}, {
		inTable: t2,
		inRule:  &binlogdatapb.Rule{Match: "/t1/"},
	}, {
		inTable: t1,
		inRule:  &binlogdatapb.Rule{Match: "/*/"},
		outErr:  "error parsing regexp: missing argument to repetition operator: `*`",
	}, {
		inTable: t2,
		inRule:  &binlogdatapb.Rule{Match: "/.*/", Filter: "-80"},
		outErr:  `no vschema definition for table t2`,
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
		inRule:  &binlogdatapb.Rule{Match: "t1", Filter: "select id, val from t1 where id=1"},
		outErr:  `unsupported where clause:  where id = 1`,
	}, {
		inTable: t1,
		inRule:  &binlogdatapb.Rule{Match: "t1", Filter: "select id, val from t1 where max(id)"},
		outErr:  `unsupported where clause:  where max(id)`,
	}, {
		inTable: t1,
		inRule:  &binlogdatapb.Rule{Match: "t1", Filter: "select id, val from t1 where in_keyrange(id)"},
		outErr:  `unsupported: id`,
	}, {
		inTable: t1,
		inRule:  &binlogdatapb.Rule{Match: "t1", Filter: "select id, val from t1 where in_keyrange(*, 'hash', '-80')"},
		outErr:  `unexpected: *`,
	}, {
		inTable: t1,
		inRule:  &binlogdatapb.Rule{Match: "t1", Filter: "select id, val from t1 where in_keyrange(1, 'hash', '-80')"},
		outErr:  `unexpected: 1`,
	}, {
		inTable: t1,
		inRule:  &binlogdatapb.Rule{Match: "t1", Filter: "select id, val from t1 where in_keyrange(none, 'hash', '-80')"},
		outErr:  `keyrange expression does not reference a column in the select list: none`,
	}, {
		inTable: t1,
		inRule:  &binlogdatapb.Rule{Match: "t1", Filter: "select id, val from t1 where in_keyrange(id, 'lookup', '-80')"},
		outErr:  `vindex must be Unique and Functional to be used for VReplication: lookup`,
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
		outErr:  `column none not found in table t1`,
	}, {
		inTable: t1,
		inRule:  &binlogdatapb.Rule{Match: "t1", Filter: "select id, val, max(val) from t1"},
		outErr:  `unsupported: max(val)`,
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
		plan, err := buildPlan(tcase.inTable, testKSChema, &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{tcase.inRule},
		})
		if plan != nil {
			plan.Table = nil
			plan.Vindex = nil
			plan.KeyRange = nil
			if !reflect.DeepEqual(tcase.outPlan, plan) {
				t.Errorf("Plan(%v, %v):\n%v, want\n%v", tcase.inTable, tcase.inRule, plan, tcase.outPlan)
			}
		} else if tcase.outPlan != nil {
			t.Errorf("Plan(%v, %v):\nnil, want\n%v", tcase.inTable, tcase.inRule, tcase.outPlan)
		}
		gotErr := ""
		if err != nil {
			gotErr = err.Error()
		}
		if gotErr != tcase.outErr {
			t.Errorf("Plan(%v, %v) err: %v, want %v", tcase.inTable, tcase.inRule, err, tcase.outErr)
		}

	}
}
