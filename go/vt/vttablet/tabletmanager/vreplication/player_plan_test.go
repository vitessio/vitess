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
	"sort"
	"testing"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/sqlparser"
)

func (pp *PlayerPlan) MarshalJSON() ([]byte, error) {
	var targets []string
	for k := range pp.TargetTables {
		targets = append(targets, k)
	}
	sort.Strings(targets)
	v := struct {
		VStreamFilter *binlogdatapb.Filter
		TargetTables  []string
		TablePlans    map[string]*TablePlan
	}{
		VStreamFilter: pp.VStreamFilter,
		TargetTables:  targets,
		TablePlans:    pp.TablePlans,
	}
	return json.Marshal(&v)
}

func (tp *TablePlan) MarshalJSON() ([]byte, error) {
	v := struct {
		Name         string
		SendRule     string
		PKReferences []string               `json:",omitempty"`
		Insert       *sqlparser.ParsedQuery `json:",omitempty"`
		Update       *sqlparser.ParsedQuery `json:",omitempty"`
		Delete       *sqlparser.ParsedQuery `json:",omitempty"`
	}{
		Name:         tp.Name,
		SendRule:     tp.SendRule.Match,
		PKReferences: tp.PKReferences,
		Insert:       tp.Insert,
		Update:       tp.Update,
		Delete:       tp.Delete,
	}
	return json.Marshal(&v)
}

type TestPlayerPlan struct {
	VStreamFilter *binlogdatapb.Filter
	TargetTables  []string
	TablePlans    map[string]*TestTablePlan
}

type TestTablePlan struct {
	Name         string
	SendRule     string
	PKReferences []string `json:",omitempty"`
	Insert       string   `json:",omitempty"`
	Update       string   `json:",omitempty"`
	Delete       string   `json:",omitempty"`
}

func TestBuildPlayerPlan(t *testing.T) {
	testcases := []struct {
		input *binlogdatapb.Filter
		plan  *TestPlayerPlan
		err   string
	}{{
		// Regular expression
		input: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match: "/.*",
			}},
		},
		plan: &TestPlayerPlan{
			VStreamFilter: &binlogdatapb.Filter{
				Rules: []*binlogdatapb.Rule{{
					Match:  "t1",
					Filter: "select * from t1",
				}},
			},
			TargetTables: []string{"t1"},
			TablePlans: map[string]*TestTablePlan{
				"t1": {
					Name:     "t1",
					SendRule: "t1",
				},
			},
		},
	}, {
		// '*' expression
		input: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "t1",
				Filter: "select * from t2",
			}},
		},
		plan: &TestPlayerPlan{
			VStreamFilter: &binlogdatapb.Filter{
				Rules: []*binlogdatapb.Rule{{
					Match:  "t2",
					Filter: "select * from t2",
				}},
			},
			TargetTables: []string{"t1"},
			TablePlans: map[string]*TestTablePlan{
				"t2": {
					Name:     "t1",
					SendRule: "t2",
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
		plan: &TestPlayerPlan{
			VStreamFilter: &binlogdatapb.Filter{
				Rules: []*binlogdatapb.Rule{{
					Match:  "t2",
					Filter: "select c1, c2 from t2",
				}},
			},
			TargetTables: []string{"t1"},
			TablePlans: map[string]*TestTablePlan{
				"t2": {
					Name:         "t1",
					SendRule:     "t2",
					PKReferences: []string{"c1"},
					Insert:       "insert into t1 set c1=:a_c1, c2=:a_c2",
					Update:       "update t1 set c2=:a_c2 where c1=:b_c1",
					Delete:       "delete from t1 where c1=:b_c1",
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
		plan: &TestPlayerPlan{
			VStreamFilter: &binlogdatapb.Filter{
				Rules: []*binlogdatapb.Rule{{
					Match:  "t2",
					Filter: "select c1, c2, c3 from t2",
				}},
			},
			TargetTables: []string{"t1"},
			TablePlans: map[string]*TestTablePlan{
				"t2": {
					Name:         "t1",
					SendRule:     "t2",
					PKReferences: []string{"c1"},
					Insert:       "insert into t1 set c1=:a_c1, c2=:a_c2, c3=:a_c3 on duplicate key update c2=:a_c2",
					Update:       "update t1 set c2=:a_c2 where c1=:b_c1",
					Delete:       "update t1 set c2=null where c1=:b_c1",
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
		plan: &TestPlayerPlan{
			VStreamFilter: &binlogdatapb.Filter{
				Rules: []*binlogdatapb.Rule{{
					Match:  "t2",
					Filter: "select c1, c2, c3 from t2",
				}},
			},
			TargetTables: []string{"t1"},
			TablePlans: map[string]*TestTablePlan{
				"t2": {
					Name:         "t1",
					SendRule:     "t2",
					PKReferences: []string{"c1"},
					Insert:       "insert ignore into t1 set c1=:a_c1, c2=:a_c2, c3=:a_c3",
					Update:       "insert ignore into t1 set c1=:a_c1, c2=:a_c2, c3=:a_c3",
				},
			},
		},
	}, {
		input: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "t1",
				Filter: "select foo(a) as c1, b c2 from t1",
			}},
		},
		plan: &TestPlayerPlan{
			VStreamFilter: &binlogdatapb.Filter{
				Rules: []*binlogdatapb.Rule{{
					Match:  "t1",
					Filter: "select a, b from t1",
				}},
			},
			TargetTables: []string{"t1"},
			TablePlans: map[string]*TestTablePlan{
				"t1": {
					Name:         "t1",
					SendRule:     "t1",
					PKReferences: []string{"a"},
					Insert:       "insert into t1 set c1=foo(:a_a), c2=:a_b",
					Update:       "update t1 set c2=:a_b where c1=(foo(:b_a))",
					Delete:       "delete from t1 where c1=(foo(:b_a))",
				},
			},
		},
	}, {
		input: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "t1",
				Filter: "select a + b as c1, c as c2 from t1",
			}},
		},
		plan: &TestPlayerPlan{
			VStreamFilter: &binlogdatapb.Filter{
				Rules: []*binlogdatapb.Rule{{
					Match:  "t1",
					Filter: "select a, b, c from t1",
				}},
			},
			TargetTables: []string{"t1"},
			TablePlans: map[string]*TestTablePlan{
				"t1": {
					Name:         "t1",
					SendRule:     "t1",
					PKReferences: []string{"a", "b"},
					Insert:       "insert into t1 set c1=:a_a + :a_b, c2=:a_c",
					Update:       "update t1 set c2=:a_c where c1=(:b_a + :b_b)",
					Delete:       "delete from t1 where c1=(:b_a + :b_b)",
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

	tableKeys := map[string][]string{
		"t1": {"c1"},
	}

	for _, tcase := range testcases {
		plan, err := buildPlayerPlan(tcase.input, tableKeys)
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
