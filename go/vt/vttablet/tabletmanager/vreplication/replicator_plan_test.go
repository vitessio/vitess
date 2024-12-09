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
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/bytes2"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/sqlparser"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vttablet "vitess.io/vitess/go/vt/vttablet/common"
)

type TestReplicatorPlan struct {
	VStreamFilter *binlogdatapb.Filter
	TargetTables  []string
	TablePlans    map[string]*TestTablePlan
}

type TestTablePlan struct {
	TargetName   string
	SendRule     string
	InsertFront  string   `json:",omitempty"`
	InsertValues string   `json:",omitempty"`
	InsertOnDup  string   `json:",omitempty"`
	Insert       string   `json:",omitempty"`
	Update       string   `json:",omitempty"`
	Delete       string   `json:",omitempty"`
	PKReferences []string `json:",omitempty"`
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
					InsertOnDup:  " on duplicate key update c2=values(c2)",
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
					InsertOnDup:  " on duplicate key update c2=values(c2)",
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
					Filter: "select a, a, b, c from t1",
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
					Filter: "select a, a, b, c, pk1, pk2 from t1",
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
		input: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "t1",
				Filter: "select c1, convert(c using utf8mb4) as c2 from t1",
			}},
		},
		plan: &TestReplicatorPlan{
			VStreamFilter: &binlogdatapb.Filter{
				Rules: []*binlogdatapb.Rule{{
					Match:  "t1",
					Filter: "select c1, convert(c using utf8mb4) as c2 from t1",
				}},
			},
			TargetTables: []string{"t1"},
			TablePlans: map[string]*TestTablePlan{
				"t1": {
					TargetName:   "t1",
					SendRule:     "t1",
					PKReferences: []string{"c1"},
					InsertFront:  "insert into t1(c1,c2)",
					InsertValues: "(:a_c1,convert(:a_c using utf8mb4))",
					Insert:       "insert into t1(c1,c2) values (:a_c1,convert(:a_c using utf8mb4))",
					Update:       "update t1 set c2=convert(:a_c using utf8mb4) where c1=:b_c1",
					Delete:       "delete from t1 where c1=:b_c1",
				},
			},
		},
		planpk: &TestReplicatorPlan{
			VStreamFilter: &binlogdatapb.Filter{
				Rules: []*binlogdatapb.Rule{{
					Match:  "t1",
					Filter: "select c1, convert(c using utf8mb4) as c2, pk1, pk2 from t1",
				}},
			},
			TargetTables: []string{"t1"},
			TablePlans: map[string]*TestTablePlan{
				"t1": {
					TargetName:   "t1",
					SendRule:     "t1",
					PKReferences: []string{"c1", "pk1", "pk2"},
					InsertFront:  "insert into t1(c1,c2)",
					InsertValues: "(:a_c1,convert(:a_c using utf8mb4))",
					Insert:       "insert into t1(c1,c2) select :a_c1, convert(:a_c using utf8mb4) from dual where (:a_pk1,:a_pk2) <= (1,'aaa')",
					Update:       "update t1 set c2=convert(:a_c using utf8mb4) where c1=:b_c1 and (:b_pk1,:b_pk2) <= (1,'aaa')",
					Delete:       "delete from t1 where c1=:b_c1 and (:b_pk1,:b_pk2) <= (1,'aaa')",
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
		// keyspace_id
		input: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "t1",
				Filter: "select c1, c2, keyspace_id() ksid from t1",
			}},
		},
		plan: &TestReplicatorPlan{
			VStreamFilter: &binlogdatapb.Filter{
				Rules: []*binlogdatapb.Rule{{
					Match:  "t1",
					Filter: "select c1, c2, keyspace_id() from t1",
				}},
			},
			TargetTables: []string{"t1"},
			TablePlans: map[string]*TestTablePlan{
				"t1": {
					TargetName:   "t1",
					SendRule:     "t1",
					PKReferences: []string{"c1"},
					InsertFront:  "insert into t1(c1,c2,ksid)",
					InsertValues: "(:a_c1,:a_c2,:a_keyspace_id)",
					Insert:       "insert into t1(c1,c2,ksid) values (:a_c1,:a_c2,:a_keyspace_id)",
					Update:       "update t1 set c2=:a_c2, ksid=:a_keyspace_id where c1=:b_c1",
					Delete:       "delete from t1 where c1=:b_c1",
				},
			},
		},
		planpk: &TestReplicatorPlan{
			VStreamFilter: &binlogdatapb.Filter{
				Rules: []*binlogdatapb.Rule{{
					Match:  "t1",
					Filter: "select c1, c2, keyspace_id(), pk1, pk2 from t1",
				}},
			},
			TargetTables: []string{"t1"},
			TablePlans: map[string]*TestTablePlan{
				"t1": {
					TargetName:   "t1",
					SendRule:     "t1",
					PKReferences: []string{"c1", "pk1", "pk2"},
					InsertFront:  "insert into t1(c1,c2,ksid)",
					InsertValues: "(:a_c1,:a_c2,:a_keyspace_id)",
					Insert:       "insert into t1(c1,c2,ksid) select :a_c1, :a_c2, :a_keyspace_id from dual where (:a_pk1,:a_pk2) <= (1,'aaa')",
					Update:       "update t1 set c2=:a_c2, ksid=:a_keyspace_id where c1=:b_c1 and (:b_pk1,:b_pk2) <= (1,'aaa')",
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
		err: "failed to build table replication plan for t1 table: syntax error at position 4 near 'bad' in query: bad query",
	}, {
		// not a select
		input: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "t1",
				Filter: "update t1 set val = 1",
			}},
		},
		err: "failed to build table replication plan for t1 table: unsupported non-select statement in query: update t1 set val = 1",
	}, {
		// no distinct
		input: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "t1",
				Filter: "select distinct c1 from t1",
			}},
		},
		err: "failed to build table replication plan for t1 table: unsupported distinct clause in query: select distinct c1 from t1",
	}, {
		// no ',' join
		input: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "t1",
				Filter: "select * from t1, t2",
			}},
		},
		err: "failed to build table replication plan for t1 table: unsupported multi-table usage in query: select * from t1, t2",
	}, {
		// no join
		input: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "t1",
				Filter: "select * from t1 join t2",
			}},
		},
		err: "failed to build table replication plan for t1 table: unsupported from expression (*sqlparser.JoinTableExpr) in query: select * from t1 join t2",
	}, {
		// no subqueries
		input: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "t1",
				Filter: "select * from (select * from t2) as a",
			}},
		},
		err: "failed to build table replication plan for t1 table: unsupported from source (*sqlparser.DerivedTable) in query: select * from (select * from t2) as a",
	}, {
		// cannot combine '*' with other
		input: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "t1",
				Filter: "select *, c1 from t1",
			}},
		},
		err: "failed to build table replication plan for t1 table: unsupported mix of '*' and columns in query: select *, c1 from t1",
	}, {
		// cannot combine '*' with other (different code path)
		input: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "t1",
				Filter: "select c1, * from t1",
			}},
		},
		err: "failed to build table replication plan for t1 table: invalid expression: * in query: select c1, * from t1",
	}, {
		// no distinct in func
		input: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "t1",
				Filter: "select hour(distinct c1) as a from t1",
			}},
		},
		err: "failed to build table replication plan for t1 table: syntax error at position 21 near 'distinct' in query: select hour(distinct c1) as a from t1",
	}, {
		// funcs need alias
		input: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "t1",
				Filter: "select hour(c1) from t1",
			}},
		},
		err: "failed to build table replication plan for t1 table: expression needs an alias: hour(c1) in query: select hour(c1) from t1",
	}, {
		// only count(*)
		input: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "t1",
				Filter: "select count(c1) as c from t1",
			}},
		},
		err: "failed to build table replication plan for t1 table: only count(*) is supported: count(c1) in query: select count(c1) as c from t1",
	}, {
		// no sum(*)
		input: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "t1",
				Filter: "select sum(*) as c from t1",
			}},
		},
		err: "failed to build table replication plan for t1 table: syntax error at position 13 in query: select sum(*) as c from t1",
	}, {
		// sum should have only one argument
		input: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "t1",
				Filter: "select sum(a, b) as c from t1",
			}},
		},
		err: "failed to build table replication plan for t1 table: syntax error at position 14 in query: select sum(a, b) as c from t1",
	}, {
		// no complex expr in sum
		input: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "t1",
				Filter: "select sum(a + b) as c from t1",
			}},
		},
		err: "failed to build table replication plan for t1 table: unsupported non-column name in sum clause: sum(a + b) in query: select sum(a + b) as c from t1",
	}, {
		// no complex expr in group by
		input: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "t1",
				Filter: "select a from t1 group by a + 1",
			}},
		},
		err: "failed to build table replication plan for t1 table: unsupported non-column name or alias in group by clause: a + 1 in query: select a from t1 group by a + 1",
	}, {
		// group by does not reference alias
		input: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "t1",
				Filter: "select a as b from t1 group by a",
			}},
		},
		err: "failed to build table replication plan for t1 table: group by expression does not reference an alias in the select list: a in query: select a as b from t1 group by a",
	}, {
		// cannot group by aggr
		input: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "t1",
				Filter: "select count(*) as a from t1 group by a",
			}},
		},
		err: "failed to build table replication plan for t1 table: group by expression is not allowed to reference an aggregate expression: a in query: select count(*) as a from t1 group by a",
	}}

	PrimaryKeyInfos := map[string][]*ColumnInfo{
		"t1": {&ColumnInfo{Name: "c1", IsPK: true}},
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

	vttablet.InitVReplicationConfigDefaults()
	for _, tcase := range testcases {
		vr := &vreplicator{
			workflowConfig: vttablet.DefaultVReplicationConfig,
		}
		plan, err := vr.buildReplicatorPlan(getSource(tcase.input), PrimaryKeyInfos, nil, binlogplayer.NewStats(), collations.MySQL8(), sqlparser.NewTestParser())
		gotErr := ""
		if err != nil {
			gotErr = err.Error()
		}
		require.Equal(t, tcase.err, gotErr, "Filter err(%v): %s, want %v", tcase.input, gotErr, tcase.err)
		gotPlan, _ := json.Marshal(plan)
		wantPlan, _ := json.Marshal(tcase.plan)
		require.Equal(t, string(wantPlan), string(gotPlan), "Filter(%v):\n%s, want\n%s", tcase.input, gotPlan, wantPlan)
		plan, err = vr.buildReplicatorPlan(getSource(tcase.input), PrimaryKeyInfos, copyState, binlogplayer.NewStats(), collations.MySQL8(), sqlparser.NewTestParser())
		if err != nil {
			continue
		}
		gotPlan, _ = json.Marshal(plan)
		wantPlan, _ = json.Marshal(tcase.planpk)
		require.Equal(t, string(wantPlan), string(gotPlan), "Filter(%v,copyState):\n%s, want\n%s", tcase.input, gotPlan, wantPlan)
	}
}

func getSource(filter *binlogdatapb.Filter) *binlogdatapb.BinlogSource {
	return &binlogdatapb.BinlogSource{Filter: filter}
}

func TestBuildPlayerPlanNoDup(t *testing.T) {
	PrimaryKeyInfos := map[string][]*ColumnInfo{
		"t1": {&ColumnInfo{Name: "c1"}},
		"t2": {&ColumnInfo{Name: "c2"}},
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
	vr := &vreplicator{
		workflowConfig: vttablet.DefaultVReplicationConfig,
	}
	_, err := vr.buildReplicatorPlan(getSource(input), PrimaryKeyInfos, nil, binlogplayer.NewStats(), collations.MySQL8(), sqlparser.NewTestParser())
	want := "more than one target for source table t"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("buildReplicatorPlan err: %v, must contain: %v", err, want)
	}
}

func TestBuildPlayerPlanExclude(t *testing.T) {
	PrimaryKeyInfos := map[string][]*ColumnInfo{
		"t1": {&ColumnInfo{Name: "c1"}},
		"t2": {&ColumnInfo{Name: "c2"}},
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
	vr := &vreplicator{
		workflowConfig: vttablet.DefaultVReplicationConfig,
	}
	plan, err := vr.buildReplicatorPlan(getSource(input), PrimaryKeyInfos, nil, binlogplayer.NewStats(), collations.MySQL8(), sqlparser.NewTestParser())
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

func TestAppendFromRow(t *testing.T) {
	testCases := []struct {
		name    string
		tp      *TablePlan
		row     *querypb.Row
		want    string
		wantErr string
	}{
		{
			name: "simple",
			tp: &TablePlan{
				BulkInsertValues: sqlparser.BuildParsedQuery("values (%a, %a, %a)",
					":c1", ":c2", ":c3",
				),
				Fields: []*querypb.Field{
					{Name: "c1", Type: querypb.Type_INT32},
					{Name: "c2", Type: querypb.Type_INT32},
					{Name: "c3", Type: querypb.Type_INT32},
				},
			},
			row: sqltypes.RowToProto3(
				[]sqltypes.Value{
					sqltypes.NewInt64(1),
					sqltypes.NewInt64(2),
					sqltypes.NewInt64(3),
				},
			),
			want: "values (1, 2, 3)",
		},
		{
			name: "too few fields",
			tp: &TablePlan{
				BulkInsertValues: sqlparser.BuildParsedQuery("values (%a, %a, %a)",
					":c1", ":c2", ":c3",
				),
				Fields: []*querypb.Field{
					{Name: "c1", Type: querypb.Type_INT32},
					{Name: "c2", Type: querypb.Type_INT32},
				},
			},
			wantErr: "wrong number of fields: got 2 fields for 3 bind locations",
		},
		{
			name: "skip half",
			tp: &TablePlan{
				BulkInsertValues: sqlparser.BuildParsedQuery("values (%a, %a, %a, %a)",
					":c1", ":c2", ":c4", ":c8",
				),
				Fields: []*querypb.Field{
					{Name: "c1", Type: querypb.Type_INT32},
					{Name: "c2", Type: querypb.Type_INT32},
					{Name: "c3", Type: querypb.Type_INT32},
					{Name: "c4", Type: querypb.Type_INT32},
					{Name: "c5", Type: querypb.Type_INT32},
					{Name: "c6", Type: querypb.Type_INT32},
					{Name: "c7", Type: querypb.Type_INT32},
					{Name: "c8", Type: querypb.Type_INT32},
				},
				FieldsToSkip: map[string]bool{
					"c3": true,
					"c5": true,
					"c6": true,
					"c7": true,
				},
			},
			row: sqltypes.RowToProto3(
				[]sqltypes.Value{
					sqltypes.NewInt64(1),
					sqltypes.NewInt64(2),
					sqltypes.NewInt64(3),
					sqltypes.NewInt64(4),
					sqltypes.NewInt64(5),
					sqltypes.NewInt64(6),
					sqltypes.NewInt64(7),
					sqltypes.NewInt64(8),
				},
			),
			want: "values (1, 2, 4, 8)",
		},
		{
			name: "skip all but one",
			tp: &TablePlan{
				BulkInsertValues: sqlparser.BuildParsedQuery("values (%a)",
					":c4",
				),
				Fields: []*querypb.Field{
					{Name: "c1", Type: querypb.Type_INT32},
					{Name: "c2", Type: querypb.Type_INT32},
					{Name: "c3", Type: querypb.Type_INT32},
					{Name: "c4", Type: querypb.Type_INT32},
					{Name: "c5", Type: querypb.Type_INT32},
					{Name: "c6", Type: querypb.Type_INT32},
					{Name: "c7", Type: querypb.Type_INT32},
					{Name: "c8", Type: querypb.Type_INT32},
				},
				FieldsToSkip: map[string]bool{
					"c1": true,
					"c2": true,
					"c3": true,
					"c5": true,
					"c6": true,
					"c7": true,
					"c8": true,
				},
			},
			row: sqltypes.RowToProto3(
				[]sqltypes.Value{
					sqltypes.NewInt64(1),
					sqltypes.NewInt64(2),
					sqltypes.NewInt64(3),
					sqltypes.NewInt64(4),
					sqltypes.NewInt64(5),
					sqltypes.NewInt64(6),
					sqltypes.NewInt64(7),
					sqltypes.NewInt64(8),
				},
			),
			want: "values (4)",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			bb := &bytes2.Buffer{}
			err := tc.tp.appendFromRow(bb, tc.row)
			if tc.wantErr != "" {
				require.EqualError(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.want, bb.String())
			}
		})
	}
}
