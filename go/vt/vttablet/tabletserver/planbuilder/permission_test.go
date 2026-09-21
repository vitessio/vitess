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

package planbuilder

import (
	"testing"

	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/tableacl"
)

func TestBuildPermissions(t *testing.T) {
	tcases := []struct {
		input        string
		output       []Permission
		undetermined bool
	}{{
		input: "select * from t",
		output: []Permission{{
			TableName: "t",
			Role:      tableacl.READER,
		}},
	}, {
		input: "select * from t1 union select * from t2",
		output: []Permission{{
			TableName: "t1",
			Role:      tableacl.READER,
		}, {
			TableName: "t2",
			Role:      tableacl.READER,
		}},
	}, {
		input: "insert into t values()",
		output: []Permission{{
			TableName: "t",
			Role:      tableacl.WRITER,
		}},
	}, {
		input: "update t set a=1",
		output: []Permission{{
			TableName: "t",
			Role:      tableacl.WRITER,
		}},
	}, {
		input: "delete from t",
		output: []Permission{{
			TableName: "t",
			Role:      tableacl.WRITER,
		}},
	}, {
		input:  "set a=1",
		output: nil,
	}, {
		input:  "show variables like 'a%'",
		output: nil,
	}, {
		input:  "describe select * from t",
		output: nil,
	}, {
		input: "create table t",
		output: []Permission{{
			TableName: "t",
			Role:      tableacl.ADMIN,
		}},
	}, {
		input: "rename table t1 to t2",
		output: []Permission{{
			TableName: "t1",
			Role:      tableacl.ADMIN,
		}, {
			TableName: "t2",
			Role:      tableacl.ADMIN,
		}},
	}, {
		input: "flush tables t1, t2",
		output: []Permission{{
			TableName: "t1",
			Role:      tableacl.ADMIN,
		}, {
			TableName: "t2",
			Role:      tableacl.ADMIN,
		}},
	}, {
		input: "drop table t",
		output: []Permission{{
			TableName: "t",
			Role:      tableacl.ADMIN,
		}},
	}, {
		input:        "repair t",
		output:       nil,
		undetermined: true,
	}, {
		input: "select (select a from t2) from t1",
		output: []Permission{{
			TableName: "t1",
			Role:      tableacl.READER,
		}, {
			TableName: "t2",
			Role:      tableacl.READER,
		}},
	}, {
		input: "insert into t1 values((select a from t2), 1)",
		output: []Permission{{
			TableName: "t1",
			Role:      tableacl.WRITER,
		}, {
			TableName: "t2",
			Role:      tableacl.READER,
		}},
	}, {
		input: "update t1 set a = (select b from t2)",
		output: []Permission{{
			TableName: "t1",
			Role:      tableacl.WRITER,
		}, {
			TableName: "t2",
			Role:      tableacl.READER,
		}},
	}, {
		input: "delete from t1 where a = (select b from t2)",
		output: []Permission{{
			TableName: "t1",
			Role:      tableacl.WRITER,
		}, {
			TableName: "t2",
			Role:      tableacl.READER,
		}},
	}, {
		input: "select * from t1, t2",
		output: []Permission{{
			TableName: "t1",
			Role:      tableacl.READER,
		}, {
			TableName: "t2",
			Role:      tableacl.READER,
		}},
	}, {
		input: "select * from (t1, t2)",
		output: []Permission{{
			TableName: "t1",
			Role:      tableacl.READER,
		}, {
			TableName: "t2",
			Role:      tableacl.READER,
		}},
	}, {
		input: "update t1 join t2 on a=b set c=d",
		output: []Permission{{
			TableName: "t1",
			Role:      tableacl.WRITER,
		}, {
			TableName: "t2",
			Role:      tableacl.WRITER,
		}},
	}, {
		input: "update (select * from t1) as a join t2 on a=b set c=d",
		output: []Permission{{
			TableName: "t2",
			Role:      tableacl.WRITER,
		}, {
			TableName: "t1", // derived table in update or delete needs reader permission as they cannot be modified.
		}},
	}, {
		input: "select next 10 values from seq",
		output: []Permission{{
			TableName: "seq",
			Role:      tableacl.WRITER,
		}},
	}, {
		input: "with t as (select count(*) as a from user) select a from t",
		output: []Permission{{
			TableName: "user",
			Role:      tableacl.READER,
		}},
	}, {
		input: "with d as (select id, count(*) as a from user) select d.a from music join d on music.user_id = d.id group by 1",
		output: []Permission{{
			TableName: "music",
			Role:      tableacl.READER,
		}, {
			TableName: "user",
			Role:      tableacl.READER,
		}},
	}, {
		input: "WITH t1 AS ( SELECT id FROM t2 ) SELECT * FROM t1 JOIN ks.t1 AS t3",
		output: []Permission{{
			TableName: "t1",
			Role:      tableacl.READER,
		}, {
			TableName: "t2",
			Role:      tableacl.READER,
		}},
	}, {
		input: "WITH RECURSIVE t1 (n) AS ( SELECT id from t2 UNION ALL SELECT n + 1 FROM t1 WHERE n < 5 ) SELECT * FROM t1 JOIN t1 AS t3",
		output: []Permission{{
			TableName: "t2",
			Role:      tableacl.READER,
		}},
	}, {
		// A parenthesized arm's WITH stays visible to the arms after it.
		input: "(with t1 as (select count(*) as a from user) select a from t1) union  select * from t1",
		output: []Permission{{
			TableName: "user",
			Role:      tableacl.READER,
		}},
	}, {
		// A non-recursive CTE is not visible inside its own definition, so the
		// reference in the CTE body is the real base table and must require a
		// READER permission. See GHSA-mv22-c3rp-c6m4.
		input: "with secret as (select * from secret) select * from secret",
		output: []Permission{{
			TableName: "secret",
			Role:      tableacl.READER,
		}},
	}, {
		// A non-recursive CTE that shadows a real table only shadows it for the
		// consumer, not for the CTE's own body.
		input: "with t as (select * from t where id in (select id from u)) select * from t",
		output: []Permission{{
			TableName: "t",
			Role:      tableacl.READER,
		}, {
			TableName: "u",
			Role:      tableacl.READER,
		}},
	}, {
		// An earlier sibling CTE is visible inside a later sibling's body and
		// carries no permission; the real table it wraps does.
		input: "with a as (select * from real1), b as (select * from a) select * from b",
		output: []Permission{{
			TableName: "real1",
			Role:      tableacl.READER,
		}},
	}, {
		// A recursive CTE may reference itself, so the self-reference in its own
		// body is the CTE and carries no permission.
		input: "with recursive t as (select * from real1 union all select * from t) select * from t",
		output: []Permission{{
			TableName: "real1",
			Role:      tableacl.READER,
		}},
	}, {
		// The consumer query sees its CTEs everywhere in its own block, not
		// only in the top-level FROM: a derived table reading the CTE carries
		// no permission, even if a base table of the same name exists.
		input: "with t as (select * from real1) select * from (select * from t) as s",
		output: []Permission{{
			TableName: "real1",
			Role:      tableacl.READER,
		}},
	}, {
		// A scalar subquery in the consumer's select list sees the CTE.
		input: "with t as (select * from real1) select (select max(id) from t) from real2",
		output: []Permission{{
			TableName: "real2",
			Role:      tableacl.READER,
		}, {
			TableName: "real1",
			Role:      tableacl.READER,
		}},
	}, {
		// A subquery in the consumer's WHERE sees the CTE.
		input: "with t as (select * from real1) select * from real2 where id in (select id from t)",
		output: []Permission{{
			TableName: "real2",
			Role:      tableacl.READER,
		}, {
			TableName: "real1",
			Role:      tableacl.READER,
		}},
	}, {
		// Both arms of a union see the CTE declared on the union.
		input: "with t as (select * from real1) select * from t union select * from t",
		output: []Permission{{
			TableName: "real1",
			Role:      tableacl.READER,
		}},
	}, {
		// A subquery in a DELETE's WHERE sees the CTE declared on the DELETE.
		input: "with t as (select * from real1) delete from real2 where id in (select id from t)",
		output: []Permission{{
			TableName: "real2",
			Role:      tableacl.WRITER,
		}, {
			TableName: "real1",
			Role:      tableacl.READER,
		}},
	}, {
		// From the first parenthesized union arm that declares its own WITH
		// onward, MySQL no longer resolves the union's leading CTEs: a
		// same-named reference there is the real table and requires its
		// permission. The arms before that one still see the leading CTEs.
		input: "with t as (select * from real1) select * from t union all (with t as (select * from t) select * from t)",
		output: []Permission{{
			TableName: "real1",
			Role:      tableacl.READER,
		}, {
			TableName: "t",
			Role:      tableacl.READER,
		}},
	}, {
		// The arm's own WITH need not shadow anything for the leading CTEs to
		// become invisible; the arm's consumer reads the real table.
		input: "with t as (select * from real1) select * from t union all (with s as (select 1 as id) select * from t)",
		output: []Permission{{
			TableName: "real1",
			Role:      tableacl.READER,
		}, {
			TableName: "t",
			Role:      tableacl.READER,
		}, {
			TableName: "dual",
			Role:      tableacl.READER,
		}},
	}, {
		// A plain arm after the arm with the WITH reads the real table too,
		// while the plain arm before it still reads the CTE.
		input: "with t as (select * from real1) select * from t union all (with s as (select 1 as id) select id from s) union all (select * from t)",
		output: []Permission{{
			TableName: "real1",
			Role:      tableacl.READER,
		}, {
			TableName: "dual",
			Role:      tableacl.READER,
		}, {
			TableName: "t",
			Role:      tableacl.READER,
		}},
	}, {
		// The union's own ORDER BY is walked too, with the enclosing scopes.
		input: "with t as (select id from real1) select id from t union all (with s as (select 1 as id) select id from s) order by (select max(id) from real2)",
		output: []Permission{{
			TableName: "real1",
			Role:      tableacl.READER,
		}, {
			TableName: "dual",
			Role:      tableacl.READER,
		}, {
			TableName: "real2",
			Role:      tableacl.READER,
		}},
	}, {
		// The ORDER BY of a parenthesized union arm sees the CTEs its last
		// arm saw, here the leading one.
		input: "with t as (select id from real1) select id from t union all (select id from real2 union all select id from real2 order by (select max(id) from t))",
		output: []Permission{{
			TableName: "real1",
			Role:      tableacl.READER,
		}, {
			TableName: "real2",
			Role:      tableacl.READER,
		}, {
			TableName: "real2",
			Role:      tableacl.READER,
		}},
	}, {
		// From an arm with its own WITH onward, that WITH's names replace the
		// leading CTEs for the later arms and the union's own ORDER BY.
		input: "select id from real1 union all (with c as (select id from secret) select id from c) union all select id from c order by (select max(id) from c)",
		output: []Permission{{
			TableName: "real1",
			Role:      tableacl.READER,
		}, {
			TableName: "secret",
			Role:      tableacl.READER,
		}},
	}, {
		// A later arm with its own WITH replaces them again.
		input: "select id from real1 union all (with c as (select id from secret) select id from c) union all (with d as (select id from real2) select id from d) union all select id from c",
		output: []Permission{{
			TableName: "real1",
			Role:      tableacl.READER,
		}, {
			TableName: "secret",
			Role:      tableacl.READER,
		}, {
			TableName: "real2",
			Role:      tableacl.READER,
		}, {
			TableName: "c",
			Role:      tableacl.READER,
		}},
	}, {
		// A parenthesized nested union with its own WITH leaves behind
		// whatever its chain ended with.
		input: "(with c as (select id from secret) select id from c union all (with d as (select id from real2) select id from d)) union all select id from d union all select id from c",
		output: []Permission{{
			TableName: "secret",
			Role:      tableacl.READER,
		}, {
			TableName: "real2",
			Role:      tableacl.READER,
		}, {
			TableName: "c",
			Role:      tableacl.READER,
		}},
	}, {
		// A nested union without a WITH is transparent: an arm with its own
		// WITH inside it replaces the leading CTEs for the arms after it.
		input: "with c as (select id from real1) select id from c union all ((with e as (select id from real2) select id from e) union all select id from c)",
		output: []Permission{{
			TableName: "real1",
			Role:      tableacl.READER,
		}, {
			TableName: "real2",
			Role:      tableacl.READER,
		}, {
			TableName: "c",
			Role:      tableacl.READER,
		}},
	}, {
		// ON DUPLICATE KEY UPDATE sees the CTEs the inserted rows' last arm
		// saw: the SELECT's own WITH here, and the WITH arm's names once an
		// arm declares one.
		input: "insert into tgt(id, x) with t as (select 1 as id) select id, id from t on duplicate key update x = (select max(id) from t)",
		output: []Permission{{
			TableName: "tgt",
			Role:      tableacl.WRITER,
		}, {
			TableName: "dual",
			Role:      tableacl.READER,
		}},
	}, {
		input: "insert into tgt(id, x) with t as (select id from real1) select 1, id from t union all (with s as (select 1 as id) select 1, id from s) on duplicate key update x = (select max(x) from t)",
		output: []Permission{{
			TableName: "tgt",
			Role:      tableacl.WRITER,
		}, {
			TableName: "real1",
			Role:      tableacl.READER,
		}, {
			TableName: "dual",
			Role:      tableacl.READER,
		}, {
			TableName: "t",
			Role:      tableacl.READER,
		}},
	}, {
		// Same with an arm declaring its own WITH elsewhere in the chain.
		input: "with t as (select id from real1) select id from t union all (select id from real2 union all select id from real2 order by (select max(id) from secret)) union all (with s as (select 1 as id) select id from s)",
		output: []Permission{{
			TableName: "real1",
			Role:      tableacl.READER,
		}, {
			TableName: "real2",
			Role:      tableacl.READER,
		}, {
			TableName: "real2",
			Role:      tableacl.READER,
		}, {
			TableName: "secret",
			Role:      tableacl.READER,
		}, {
			TableName: "dual",
			Role:      tableacl.READER,
		}},
	}, {
		// A union's own ORDER BY sees its leading CTEs while no arm has hidden
		// them, and so does the ORDER BY of a parenthesized single-select arm.
		input: "with t as (select id from real1) select id from t union all select id from real2 order by (select max(id) from t)",
		output: []Permission{{
			TableName: "real1",
			Role:      tableacl.READER,
		}, {
			TableName: "real2",
			Role:      tableacl.READER,
		}},
	}, {
		input: "with t as (select id from real1) select id from t union all (select id from real2 order by (select max(id) from t))",
		output: []Permission{{
			TableName: "real1",
			Role:      tableacl.READER,
		}, {
			TableName: "real2",
			Role:      tableacl.READER,
		}},
	}, {
		// A union inside a derived table sees the consumer's CTEs in its own
		// ORDER BY; it is not a union arm.
		input: "with t as (select id from real1) select * from (select id from real2 union all select id from real2 order by (select max(id) from t)) as d",
		output: []Permission{{
			TableName: "real1",
			Role:      tableacl.READER,
		}, {
			TableName: "real2",
			Role:      tableacl.READER,
		}, {
			TableName: "real2",
			Role:      tableacl.READER,
		}},
	}, {
		// When the arm with the WITH comes first, no arm sees the leading CTEs.
		input: "with t as (select * from real1) (with s as (select 1 as id) select * from t) union all select * from t",
		output: []Permission{{
			TableName: "real1",
			Role:      tableacl.READER,
		}, {
			TableName: "t",
			Role:      tableacl.READER,
		}, {
			TableName: "dual",
			Role:      tableacl.READER,
		}, {
			TableName: "t",
			Role:      tableacl.READER,
		}},
	}, {
		// Only the union's own leading WITH is affected. A CTE from an
		// enclosing query block stays visible in every arm, including inside
		// the arm's own CTE bodies.
		input: "with t as (select * from real1), s as (select * from t union all (with t as (select * from t) select * from t)) select * from s",
		output: []Permission{{
			TableName: "real1",
			Role:      tableacl.READER,
		}},
	}, {
		// Same for a union inside a derived table of the consumer.
		input: "with t as (select * from real1) select * from (select * from t union all (with s as (select 1 as id) select * from t)) as d",
		output: []Permission{{
			TableName: "real1",
			Role:      tableacl.READER,
		}, {
			TableName: "dual",
			Role:      tableacl.READER,
		}},
	}, {
		// A CTE joined into a multi-table UPDATE is a read source, never a
		// write target; it carries no WRITER permission of its own.
		input: "with t as (select * from real1) update real2 join t on real2.id = t.id set real2.x = 8",
		output: []Permission{{
			TableName: "real2",
			Role:      tableacl.WRITER,
		}, {
			TableName: "real1",
			Role:      tableacl.READER,
		}},
	}, {
		// Same for a multi-table DELETE, in both syntaxes.
		input: "with t as (select * from real1) delete real2 from real2 join t on real2.id = t.id",
		output: []Permission{{
			TableName: "real2",
			Role:      tableacl.WRITER,
		}, {
			TableName: "real1",
			Role:      tableacl.READER,
		}},
	}, {
		// MySQL refuses a CTE as the target of an UPDATE or DELETE, so a
		// same-named CTE derives no WRITER for the shadowed table: nothing can
		// be written through that name. The body's read still needs READER.
		input: "with t as (select * from t) delete from t",
		output: []Permission{{
			TableName: "t",
			Role:      tableacl.READER,
		}},
	}, {
		input: "with t as (select * from t) update t set x = 1",
		output: []Permission{{
			TableName: "t",
			Role:      tableacl.READER,
		}},
	}, {
		input: "with t as (select * from real1) delete from real2 using real2 join t on real2.id = t.id",
		output: []Permission{{
			TableName: "real2",
			Role:      tableacl.WRITER,
		}, {
			TableName: "real1",
			Role:      tableacl.READER,
		}},
	}, {
		// A later sibling is not visible in an earlier body, so the
		// reference there is the real table.
		input: "with a as (select * from b), b as (select * from real1) select * from a",
		output: []Permission{{
			TableName: "b",
			Role:      tableacl.READER,
		}, {
			TableName: "real1",
			Role:      tableacl.READER,
		}},
	}, {
		// A nested WITH inside a CTE's own body does not make the outer CTE
		// visible to itself either.
		input: "with a as (with b as (select * from a) select * from b) select * from a",
		output: []Permission{{
			TableName: "a",
			Role:      tableacl.READER,
		}},
	}, {
		// The self-reference is closed on INSERT ... SELECT as well.
		input: "insert into real2 with real1 as (select * from real1) select * from real1",
		output: []Permission{{
			TableName: "real2",
			Role:      tableacl.WRITER,
		}, {
			TableName: "real1",
			Role:      tableacl.READER,
		}},
	}, {
		// And on a WITH declared by a derived table or an IN subquery.
		input: "select * from (with t as (select * from t) select * from t) as d",
		output: []Permission{{
			TableName: "t",
			Role:      tableacl.READER,
		}},
	}, {
		input: "select * from real2 where id in (with t as (select id from t) select id from t)",
		output: []Permission{{
			TableName: "real2",
			Role:      tableacl.READER,
		}, {
			TableName: "t",
			Role:      tableacl.READER,
		}},
	}, {
		// Statements whose tables the parser discards derive no permission
		// and are flagged instead, so the executor can fail closed on them.
		input:        "do (select * from t)",
		undetermined: true,
	}, {
		input:        "optimize table t",
		undetermined: true,
	}, {
		input:        "call proc()",
		undetermined: true,
	}, {
		input:        "load data infile 'x' into table t",
		undetermined: true,
	}}

	for _, tcase := range tcases {
		t.Run(tcase.input, func(t *testing.T) {
			stmt, err := sqlparser.NewTestParser().Parse(tcase.input)
			if err != nil {
				t.Fatal(err)
			}
			got, undetermined := BuildPermissions(stmt)
			utils.MustMatch(t, tcase.output, got)
			utils.MustMatch(t, tcase.undetermined, undetermined)
		})
	}
}
