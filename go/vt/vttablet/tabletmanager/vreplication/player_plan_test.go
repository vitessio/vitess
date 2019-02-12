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

package vreplication

import (
	"encoding/json"
	"testing"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/sqlparser"
)

func TestPlayerPlan(t *testing.T) {
	testcases := []struct {
		input *binlogdatapb.Filter
		plan  *PlayerPlan
		err   string
	}{{
		// Regular expression
		input: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match: "/.*",
			}},
		},
		plan: &PlayerPlan{
			VStreamFilter: &binlogdatapb.Filter{
				Rules: []*binlogdatapb.Rule{{
					Match: "/.*",
				}},
			},
			TablePlans: map[string]*TablePlan{},
		},
	}, {
		// '*' expression
		input: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "t1",
				Filter: "select * from t2",
			}},
		},
		plan: &PlayerPlan{
			VStreamFilter: &binlogdatapb.Filter{
				Rules: []*binlogdatapb.Rule{{
					Match:  "t2",
					Filter: "select * from t2",
				}},
			},
			TablePlans: map[string]*TablePlan{
				"t2": {
					Name: "t1",
				},
			},
		},
	}, {
		// Explicit columns
		input: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "t1",
				Filter: "select c1, c2 from t2",
			}},
		},
		plan: &PlayerPlan{
			VStreamFilter: &binlogdatapb.Filter{
				Rules: []*binlogdatapb.Rule{{
					Match:  "t2",
					Filter: "select c1, c2 from t2",
				}},
			},
			TablePlans: map[string]*TablePlan{
				"t2": {
					Name: "t1",
					ColExprs: []*ColExpr{{
						ColName: sqlparser.NewColIdent("c1"),
						ColNum:  0,
					}, {
						ColName: sqlparser.NewColIdent("c2"),
						ColNum:  1,
					}},
				},
			},
		},
	}, {
		// func expr
		input: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "t1",
				Filter: "select hour(c1) as hc1, day(c2) as dc2 from t2",
			}},
		},
		plan: &PlayerPlan{
			VStreamFilter: &binlogdatapb.Filter{
				Rules: []*binlogdatapb.Rule{{
					Match:  "t2",
					Filter: "select hour(c1) as hc1, day(c2) as dc2 from t2",
				}},
			},
			TablePlans: map[string]*TablePlan{
				"t2": {
					Name: "t1",
					ColExprs: []*ColExpr{{
						ColName: sqlparser.NewColIdent("hc1"),
						ColNum:  0,
					}, {
						ColName: sqlparser.NewColIdent("dc2"),
						ColNum:  1,
					}},
				},
			},
		},
	}, {
		// count expr
		input: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "t1",
				Filter: "select hour(c1) as hc1, count(*) as c, day(c2) as dc2 from t2",
			}},
		},
		plan: &PlayerPlan{
			VStreamFilter: &binlogdatapb.Filter{
				Rules: []*binlogdatapb.Rule{{
					Match:  "t2",
					Filter: "select hour(c1) as hc1, day(c2) as dc2 from t2",
				}},
			},
			TablePlans: map[string]*TablePlan{
				"t2": {
					Name: "t1",
					ColExprs: []*ColExpr{{
						ColName: sqlparser.NewColIdent("hc1"),
						ColNum:  0,
					}, {
						ColName:   sqlparser.NewColIdent("c"),
						Operation: OpCount,
					}, {
						ColName: sqlparser.NewColIdent("dc2"),
						ColNum:  1,
					}},
				},
			},
		},
	}, {
		// sum expr
		input: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "t1",
				Filter: "select hour(c1) as hc1, sum(c3) as s, day(c2) as dc2 from t2",
			}},
		},
		plan: &PlayerPlan{
			VStreamFilter: &binlogdatapb.Filter{
				Rules: []*binlogdatapb.Rule{{
					Match:  "t2",
					Filter: "select hour(c1) as hc1, c3, day(c2) as dc2 from t2",
				}},
			},
			TablePlans: map[string]*TablePlan{
				"t2": {
					Name: "t1",
					ColExprs: []*ColExpr{{
						ColName: sqlparser.NewColIdent("hc1"),
						ColNum:  0,
					}, {
						ColName:   sqlparser.NewColIdent("s"),
						ColNum:    1,
						Operation: OpSum,
					}, {
						ColName: sqlparser.NewColIdent("dc2"),
						ColNum:  2,
					}},
				},
			},
		},
	}, {
		// partial group by
		input: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "t1",
				Filter: "select c1, c2, c3 from t2 group by c3, c1",
			}},
		},
		plan: &PlayerPlan{
			VStreamFilter: &binlogdatapb.Filter{
				Rules: []*binlogdatapb.Rule{{
					Match:  "t2",
					Filter: "select c1, c2, c3 from t2",
				}},
			},
			TablePlans: map[string]*TablePlan{
				"t2": {
					Name: "t1",
					ColExprs: []*ColExpr{{
						ColName:   sqlparser.NewColIdent("c1"),
						ColNum:    0,
						IsGrouped: true,
					}, {
						ColName: sqlparser.NewColIdent("c2"),
						ColNum:  1,
					}, {
						ColName:   sqlparser.NewColIdent("c3"),
						ColNum:    2,
						IsGrouped: true,
					}},
					OnInsert: InsertOndup,
				},
			},
		},
	}, {
		// full group by
		input: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "t1",
				Filter: "select c1, c2, c3 from t2 group by c3, c1, c2",
			}},
		},
		plan: &PlayerPlan{
			VStreamFilter: &binlogdatapb.Filter{
				Rules: []*binlogdatapb.Rule{{
					Match:  "t2",
					Filter: "select c1, c2, c3 from t2",
				}},
			},
			TablePlans: map[string]*TablePlan{
				"t2": {
					Name: "t1",
					ColExprs: []*ColExpr{{
						ColName:   sqlparser.NewColIdent("c1"),
						ColNum:    0,
						IsGrouped: true,
					}, {
						ColName:   sqlparser.NewColIdent("c2"),
						ColNum:    1,
						IsGrouped: true,
					}, {
						ColName:   sqlparser.NewColIdent("c3"),
						ColNum:    2,
						IsGrouped: true,
					}},
					OnInsert: InsertIgnore,
				},
			},
		},
	}, {
		// syntax error
		input: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "t1",
				Filter: "bad query",
			}},
		},
		err: "syntax error at position 4 near 'bad'",
	}, {
		// not a select
		input: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "t1",
				Filter: "update t1 set val=1",
			}},
		},
		err: "unexpected: update t1 set val = 1",
	}, {
		// no distinct
		input: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "t1",
				Filter: "select distinct c1 from t1",
			}},
		},
		err: "unexpected: select distinct c1 from t1",
	}, {
		// no ',' join
		input: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "t1",
				Filter: "select * from t1, t2",
			}},
		},
		err: "unexpected: select * from t1, t2",
	}, {
		// no join
		input: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "t1",
				Filter: "select * from t1 join t2",
			}},
		},
		err: "unexpected: select * from t1 join t2",
	}, {
		// no subqueries
		input: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "t1",
				Filter: "select * from (select * from t2) as a",
			}},
		},
		err: "unexpected: select * from (select * from t2) as a",
	}, {
		// cannot combine '*' with other
		input: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "t1",
				Filter: "select *, c1 from t1",
			}},
		},
		err: "unexpected: select *, c1 from t1",
	}, {
		// cannot combine '*' with other (different code path)
		input: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "t1",
				Filter: "select c1, * from t1",
			}},
		},
		err: "unexpected: *",
	}, {
		// no distinct in func
		input: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "t1",
				Filter: "select hour(distinct c1) from t1",
			}},
		},
		err: "unexpected: hour(distinct c1)",
	}, {
		// funcs need alias
		input: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "t1",
				Filter: "select hour(c1) from t1",
			}},
		},
		err: "expression needs an alias: hour(c1)",
	}, {
		// only count(*)
		input: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "t1",
				Filter: "select count(c1) as c from t1",
			}},
		},
		err: "only count(*) is supported: count(c1)",
	}, {
		// no sum(*)
		input: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "t1",
				Filter: "select sum(*) as c from t1",
			}},
		},
		err: "unexpected: sum(*)",
	}, {
		// no complex expr in sum
		input: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "t1",
				Filter: "select sum(a + b) as c from t1",
			}},
		},
		err: "unexpected: sum(a + b)",
	}, {
		// unsupported func
		input: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "t1",
				Filter: "select foo(a) as c from t1",
			}},
		},
		err: "unexpected: foo(a)",
	}, {
		// no complex expr in select
		input: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "t1",
				Filter: "select a + b from t1",
			}},
		},
		err: "unexpected: a + b",
	}, {
		// no complex expr in group by
		input: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "t1",
				Filter: "select a from t1 group by a + 1",
			}},
		},
		err: "unexpected: a + 1",
	}, {
		// group by does not reference alias
		input: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "t1",
				Filter: "select a as b from t1 group by a",
			}},
		},
		err: "group by expression does not reference an alias in the select list: a",
	}, {
		// cannot group by aggr
		input: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "t1",
				Filter: "select count(*) as a from t1 group by a",
			}},
		},
		err: "group by expression is not allowed to reference an aggregate expression: a",
	}}

	for _, tcase := range testcases {
		plan, err := buildPlayerPlan(tcase.input)
		gotPlan, _ := json.Marshal(plan)
		wantPlan, _ := json.Marshal(tcase.plan)
		if string(gotPlan) != string(wantPlan) {
			t.Errorf("Filter(%v):\n%s, want\n%s", tcase.input, gotPlan, wantPlan)
		}
		gotErr := ""
		if err != nil {
			gotErr = err.Error()
		}
		if gotErr != tcase.err {
			t.Errorf("Filter err(%v): %s, want %v", tcase.input, gotErr, tcase.err)
		}
	}
}
