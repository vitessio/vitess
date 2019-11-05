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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"vitess.io/vitess/go/sqltypes"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

type TestReplicatorPlan struct {
	VStreamFilter *binlogdatapb.Filter
	TargetTables  []string
	TablePlans    map[string]*TestTablePlan
}

type TestTablePlan struct {
	TargetName   string
	SendRule     string
	PKReferences []string `json:",omitempty"`
	InsertFront  string   `json:",omitempty"`
	InsertValues string   `json:",omitempty"`
	InsertOnDup  string   `json:",omitempty"`
	Insert       string   `json:",omitempty"`
	Update       string   `json:",omitempty"`
	Delete       string   `json:",omitempty"`
}

func TestBuildPlayerPlan(t *testing.T) {
	testcases := []struct {
		input  *binlogdatapb.Filter
		plan   *TestReplicatorPlan
		planpk *TestReplicatorPlan
		err    string
	}{{
		// Regular expression
		input: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match: "/.*",
			}},
		},
		plan: &TestReplicatorPlan{
			VStreamFilter: &binlogdatapb.Filter{
				Rules: []*binlogdatapb.Rule{{
					Match:  "t1",
					Filter: "select * from t1",
				}},
			},
			TargetTables: []string{"t1"},
			TablePlans: map[string]*TestTablePlan{
				"t1": {
					TargetName: "t1",
					SendRule:   "t1",
				},
			},
		},
		planpk: &TestReplicatorPlan{
			VStreamFilter: &binlogdatapb.Filter{
				Rules: []*binlogdatapb.Rule{{
					Match:  "t1",
					Filter: "select * from t1",
				}},
			},
			TargetTables: []string{"t1"},
			TablePlans: map[string]*TestTablePlan{
				"t1": {
					TargetName: "t1",
					SendRule:   "t1",
				},
			},
		},
	}, {
		// Regular with keyrange
		input: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "/.*",
				Filter: "-80",
			}},
		},
		plan: &TestReplicatorPlan{
			VStreamFilter: &binlogdatapb.Filter{
				Rules: []*binlogdatapb.Rule{{
					Match:  "t1",
					Filter: "select * from t1 where in_keyrange('-80')",
				}},
			},
			TargetTables: []string{"t1"},
			TablePlans: map[string]*TestTablePlan{
				"t1": {
					TargetName: "t1",
					SendRule:   "t1",
				},
			},
		},
		planpk: &TestReplicatorPlan{
			VStreamFilter: &binlogdatapb.Filter{
				Rules: []*binlogdatapb.Rule{{
					Match:  "t1",
					Filter: "select * from t1 where in_keyrange('-80')",
				}},
			},
			TargetTables: []string{"t1"},
			TablePlans: map[string]*TestTablePlan{
				"t1": {
					TargetName: "t1",
					SendRule:   "t1",
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
		plan: &TestReplicatorPlan{
			VStreamFilter: &binlogdatapb.Filter{
				Rules: []*binlogdatapb.Rule{{
					Match:  "t2",
					Filter: "select * from t2",
				}},
			},
			TargetTables: []string{"t1"},
			TablePlans: map[string]*TestTablePlan{
				"t2": {
					TargetName: "t1",
					SendRule:   "t2",
				},
			},
		},
		planpk: &TestReplicatorPlan{
			VStreamFilter: &binlogdatapb.Filter{
				Rules: []*binlogdatapb.Rule{{
					Match:  "t2",
					Filter: "select * from t2",
				}},
			},
			TargetTables: []string{"t1"},
			TablePlans: map[string]*TestTablePlan{
				"t2": {
					TargetName: "t1",
					SendRule:   "t2",
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
		plan: &TestReplicatorPlan{
			VStreamFilter: &binlogdatapb.Filter{
				Rules: []*binlogdatapb.Rule{{
					Match:  "t2",
					Filter: "select c1, c2 from t2",
				}},
			},
			TargetTables: []string{"t1"},
			TablePlans: map[string]*TestTablePlan{
				"t2": {
					TargetName:   "t1",
					SendRule:     "t2",
					PKReferences: []string{"c1"},
					InsertFront:  "insert into t1(c1,c2)",
					InsertValues: "(:a_c1,:a_c2)",
					Insert:       "insert into t1(c1,c2) values (:a_c1,:a_c2)",
					Update:       "update t1 set c2=:a_c2 where c1=:b_c1",
					Delete:       "delete from t1 where c1=:b_c1",
				},
			},
		},
		planpk: &TestReplicatorPlan{
			VStreamFilter: &binlogdatapb.Filter{
				Rules: []*binlogdatapb.Rule{{
					Match:  "t2",
					Filter: "select c1, c2, pk1, pk2 from t2",
				}},
			},
			TargetTables: []string{"t1"},
			TablePlans: map[string]*TestTablePlan{
				"t2": {
					TargetName:   "t1",
					SendRule:     "t2",
					PKReferences: []string{"c1", "pk1", "pk2"},
					InsertFront:  "insert into t1(c1,c2)",
					InsertValues: "(:a_c1,:a_c2)",
					Insert:       "insert into t1(c1,c2) select :a_c1, :a_c2 from dual where (:a_pk1,:a_pk2) <= (1,'aaa')",
					Update:       "update t1 set c2=:a_c2 where c1=:b_c1 and (:b_pk1,:b_pk2) <= (1,'aaa')",
					Delete:       "delete from t1 where c1=:b_c1 and (:b_pk1,:b_pk2) <= (1,'aaa')",
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
		plan: &TestReplicatorPlan{
			VStreamFilter: &binlogdatapb.Filter{
				Rules: []*binlogdatapb.Rule{{
					Match:  "t2",
					Filter: "select c1, c2, c3 from t2",
				}},
			},
			TargetTables: []string{"t1"},
			TablePlans: map[string]*TestTablePlan{
				"t2": {
					TargetName:   "t1",
					SendRule:     "t2",
					PKReferences: []string{"c1"},
					InsertFront:  "insert into t1(c1,c2,c3)",
					InsertValues: "(:a_c1,:a_c2,:a_c3)",
					InsertOnDup:  "on duplicate key update c2=values(c2)",
					Insert:       "insert into t1(c1,c2,c3) values (:a_c1,:a_c2,:a_c3) on duplicate key update c2=values(c2)",
					Update:       "update t1 set c2=:a_c2 where c1=:b_c1",
					Delete:       "update t1 set c2=null where c1=:b_c1",
				},
			},
		},
		planpk: &TestReplicatorPlan{
			VStreamFilter: &binlogdatapb.Filter{
				Rules: []*binlogdatapb.Rule{{
					Match:  "t2",
					Filter: "select c1, c2, c3, pk1, pk2 from t2",
				}},
			},
			TargetTables: []string{"t1"},
			TablePlans: map[string]*TestTablePlan{
				"t2": {
					TargetName:   "t1",
					SendRule:     "t2",
					PKReferences: []string{"c1", "pk1", "pk2"},
					InsertFront:  "insert into t1(c1,c2,c3)",
					InsertValues: "(:a_c1,:a_c2,:a_c3)",
					InsertOnDup:  "on duplicate key update c2=values(c2)",
					Insert:       "insert into t1(c1,c2,c3) select :a_c1, :a_c2, :a_c3 from dual where (:a_pk1,:a_pk2) <= (1,'aaa') on duplicate key update c2=values(c2)",
					Update:       "update t1 set c2=:a_c2 where c1=:b_c1 and (:b_pk1,:b_pk2) <= (1,'aaa')",
					Delete:       "update t1 set c2=null where c1=:b_c1 and (:b_pk1,:b_pk2) <= (1,'aaa')",
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
		plan: &TestReplicatorPlan{
			VStreamFilter: &binlogdatapb.Filter{
				Rules: []*binlogdatapb.Rule{{
					Match:  "t2",
					Filter: "select c1, c2, c3 from t2",
				}},
			},
			TargetTables: []string{"t1"},
			TablePlans: map[string]*TestTablePlan{
				"t2": {
					TargetName:   "t1",
					SendRule:     "t2",
					PKReferences: []string{"c1"},
					InsertFront:  "insert ignore into t1(c1,c2,c3)",
					InsertValues: "(:a_c1,:a_c2,:a_c3)",
					Insert:       "insert ignore into t1(c1,c2,c3) values (:a_c1,:a_c2,:a_c3)",
					Update:       "insert ignore into t1(c1,c2,c3) values (:a_c1,:a_c2,:a_c3)",
				},
			},
		},
		planpk: &TestReplicatorPlan{
			VStreamFilter: &binlogdatapb.Filter{
				Rules: []*binlogdatapb.Rule{{
					Match:  "t2",
					Filter: "select c1, c2, c3, pk1, pk2 from t2",
				}},
			},
			TargetTables: []string{"t1"},
			TablePlans: map[string]*TestTablePlan{
				"t2": {
					TargetName:   "t1",
					SendRule:     "t2",
					PKReferences: []string{"c1", "pk1", "pk2"},
					InsertFront:  "insert ignore into t1(c1,c2,c3)",
					InsertValues: "(:a_c1,:a_c2,:a_c3)",
					Insert:       "insert ignore into t1(c1,c2,c3) select :a_c1, :a_c2, :a_c3 from dual where (:a_pk1,:a_pk2) <= (1,'aaa')",
					Update:       "insert ignore into t1(c1,c2,c3) select :a_c1, :a_c2, :a_c3 from dual where (:a_pk1,:a_pk2) <= (1,'aaa')",
				},
			},
		},
	}, {
		input: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "t1",
				Filter: "select foo(a) as c1, foo(a, b) as c2, c c3 from t1",
			}},
		},
		plan: &TestReplicatorPlan{
			VStreamFilter: &binlogdatapb.Filter{
				Rules: []*binlogdatapb.Rule{{
					Match:  "t1",
					Filter: "select a, b, c from t1",
				}},
			},
			TargetTables: []string{"t1"},
			TablePlans: map[string]*TestTablePlan{
				"t1": {
					TargetName:   "t1",
					SendRule:     "t1",
					PKReferences: []string{"a"},
					InsertFront:  "insert into t1(c1,c2,c3)",
					InsertValues: "(foo(:a_a),foo(:a_a, :a_b),:a_c)",
					Insert:       "insert into t1(c1,c2,c3) values (foo(:a_a),foo(:a_a, :a_b),:a_c)",
					Update:       "update t1 set c2=foo(:a_a, :a_b), c3=:a_c where c1=(foo(:b_a))",
					Delete:       "delete from t1 where c1=(foo(:b_a))",
				},
			},
		},
		planpk: &TestReplicatorPlan{
			VStreamFilter: &binlogdatapb.Filter{
				Rules: []*binlogdatapb.Rule{{
					Match:  "t1",
					Filter: "select a, b, c, pk1, pk2 from t1",
				}},
			},
			TargetTables: []string{"t1"},
			TablePlans: map[string]*TestTablePlan{
				"t1": {
					TargetName:   "t1",
					SendRule:     "t1",
					PKReferences: []string{"a", "pk1", "pk2"},
					InsertFront:  "insert into t1(c1,c2,c3)",
					InsertValues: "(foo(:a_a),foo(:a_a, :a_b),:a_c)",
					Insert:       "insert into t1(c1,c2,c3) select foo(:a_a), foo(:a_a, :a_b), :a_c from dual where (:a_pk1,:a_pk2) <= (1,'aaa')",
					Update:       "update t1 set c2=foo(:a_a, :a_b), c3=:a_c where c1=(foo(:b_a)) and (:b_pk1,:b_pk2) <= (1,'aaa')",
					Delete:       "delete from t1 where c1=(foo(:b_a)) and (:b_pk1,:b_pk2) <= (1,'aaa')",
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
		plan: &TestReplicatorPlan{
			VStreamFilter: &binlogdatapb.Filter{
				Rules: []*binlogdatapb.Rule{{
					Match:  "t1",
					Filter: "select a, b, c from t1",
				}},
			},
			TargetTables: []string{"t1"},
			TablePlans: map[string]*TestTablePlan{
				"t1": {
					TargetName:   "t1",
					SendRule:     "t1",
					PKReferences: []string{"a", "b"},
					InsertFront:  "insert into t1(c1,c2)",
					InsertValues: "(:a_a + :a_b,:a_c)",
					Insert:       "insert into t1(c1,c2) values (:a_a + :a_b,:a_c)",
					Update:       "update t1 set c2=:a_c where c1=(:b_a + :b_b)",
					Delete:       "delete from t1 where c1=(:b_a + :b_b)",
				},
			},
		},
		planpk: &TestReplicatorPlan{
			VStreamFilter: &binlogdatapb.Filter{
				Rules: []*binlogdatapb.Rule{{
					Match:  "t1",
					Filter: "select a, b, c, pk1, pk2 from t1",
				}},
			},
			TargetTables: []string{"t1"},
			TablePlans: map[string]*TestTablePlan{
				"t1": {
					TargetName:   "t1",
					SendRule:     "t1",
					PKReferences: []string{"a", "b", "pk1", "pk2"},
					InsertFront:  "insert into t1(c1,c2)",
					InsertValues: "(:a_a + :a_b,:a_c)",
					Insert:       "insert into t1(c1,c2) select :a_a + :a_b, :a_c from dual where (:a_pk1,:a_pk2) <= (1,'aaa')",
					Update:       "update t1 set c2=:a_c where c1=(:b_a + :b_b) and (:b_pk1,:b_pk2) <= (1,'aaa')",
					Delete:       "delete from t1 where c1=(:b_a + :b_b) and (:b_pk1,:b_pk2) <= (1,'aaa')",
				},
			},
		},
	}, {
		// Keywords as names.
		input: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "t1",
				Filter: "select c1, c2, `primary` from `primary`",
			}},
		},
		plan: &TestReplicatorPlan{
			VStreamFilter: &binlogdatapb.Filter{
				Rules: []*binlogdatapb.Rule{{
					Match:  "primary",
					Filter: "select c1, c2, `primary` from `primary`",
				}},
			},
			TargetTables: []string{"t1"},
			TablePlans: map[string]*TestTablePlan{
				"primary": {
					TargetName:   "t1",
					SendRule:     "primary",
					PKReferences: []string{"c1"},
					InsertFront:  "insert into t1(c1,c2,`primary`)",
					InsertValues: "(:a_c1,:a_c2,:a_primary)",
					Insert:       "insert into t1(c1,c2,`primary`) values (:a_c1,:a_c2,:a_primary)",
					Update:       "update t1 set c2=:a_c2, `primary`=:a_primary where c1=:b_c1",
					Delete:       "delete from t1 where c1=:b_c1",
				},
			},
		},
		planpk: &TestReplicatorPlan{
			VStreamFilter: &binlogdatapb.Filter{
				Rules: []*binlogdatapb.Rule{{
					Match:  "primary",
					Filter: "select c1, c2, `primary`, pk1, pk2 from `primary`",
				}},
			},
			TargetTables: []string{"t1"},
			TablePlans: map[string]*TestTablePlan{
				"primary": {
					TargetName:   "t1",
					SendRule:     "primary",
					PKReferences: []string{"c1", "pk1", "pk2"},
					InsertFront:  "insert into t1(c1,c2,`primary`)",
					InsertValues: "(:a_c1,:a_c2,:a_primary)",
					Insert:       "insert into t1(c1,c2,`primary`) select :a_c1, :a_c2, :a_primary from dual where (:a_pk1,:a_pk2) <= (1,'aaa')",
					Update:       "update t1 set c2=:a_c2, `primary`=:a_primary where c1=:b_c1 and (:b_pk1,:b_pk2) <= (1,'aaa')",
					Delete:       "delete from t1 where c1=:b_c1 and (:b_pk1,:b_pk2) <= (1,'aaa')",
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
				Filter: "select hour(distinct c1) as a from t1",
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
		// sum should have only one argument
		input: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "t1",
				Filter: "select sum(a, b) as c from t1",
			}},
		},
		err: "unexpected: sum(a, b)",
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

	copyState := map[string]*sqltypes.Result{
		"t1": sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"pk1|pk2",
				"int64|varchar",
			),
			"1|aaa",
		),
	}

	for _, tcase := range testcases {
		plan, err := buildReplicatorPlan(tcase.input, tableKeys, nil)
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

		plan, err = buildReplicatorPlan(tcase.input, tableKeys, copyState)
		if err != nil {
			continue
		}
		gotPlan, _ = json.Marshal(plan)
		wantPlan, _ = json.Marshal(tcase.planpk)
		if string(gotPlan) != string(wantPlan) {
			t.Errorf("Filter(%v,copyState):\n%s, want\n%s", tcase.input, gotPlan, wantPlan)
		}
	}
}

func TestBuildPlayerPlanNoDup(t *testing.T) {
	tableKeys := map[string][]string{
		"t1": {"c1"},
		"t2": {"c2"},
	}
	input := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "t1",
			Filter: "select * from t",
		}, {
			Match:  "t2",
			Filter: "select * from t",
		}},
	}
	_, err := buildReplicatorPlan(input, tableKeys, nil)
	want := "more than one target for source table t"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("buildReplicatorPlan err: %v, must contain: %v", err, want)
	}
}

func TestBuildPlayerPlanExclude(t *testing.T) {
	tableKeys := map[string][]string{
		"t1": {"c1"},
		"t2": {"c2"},
	}
	input := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "t2",
			Filter: "exclude",
		}, {
			Match:  "/.*",
			Filter: "",
		}},
	}
	plan, err := buildReplicatorPlan(input, tableKeys, nil)
	assert.NoError(t, err)

	want := &TestReplicatorPlan{
		VStreamFilter: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "t1",
				Filter: "select * from t1",
			}},
		},
		TargetTables: []string{"t1"},
		TablePlans: map[string]*TestTablePlan{
			"t1": {
				TargetName: "t1",
				SendRule:   "t1",
			},
		},
	}

	gotPlan, _ := json.Marshal(plan)
	wantPlan, _ := json.Marshal(want)
	assert.Equal(t, string(gotPlan), string(wantPlan))
}
