// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlparser

import "testing"

func TestValid(t *testing.T) {
	validSQL := []struct {
		input  string
		output string
	}{{
		input: "select 1 from t",
	}, {
		input: "select .1 from t",
	}, {
		input: "select 1.2e1 from t",
	}, {
		input: "select 1.2e+1 from t",
	}, {
		input: "select 1.2e-1 from t",
	}, {
		input: "select 08.3 from t",
	}, {
		input: "select -1 from t where b = -2",
	}, {
		input:  "select - -1 from t",
		output: "select 1 from t",
	}, {
		input:  "select 1 from t // aa",
		output: "select 1 from t",
	}, {
		input:  "select 1 from t -- aa",
		output: "select 1 from t",
	}, {
		input: "select /* simplest */ 1 from t",
	}, {
		input:  "select /* keyword col */ `By` from t",
		output: "select /* keyword col */ `by` from t",
	}, {
		input: "select /* double star **/ 1 from t",
	}, {
		input: "select /* double */ /* comment */ 1 from t",
	}, {
		input:  "select /* back-quote */ 1 from `t`",
		output: "select /* back-quote */ 1 from t",
	}, {
		input:  "select /* back-quote keyword */ 1 from `By`",
		output: "select /* back-quote keyword */ 1 from `By`",
	}, {
		input: "select /* @ */ @@a from b",
	}, {
		input: "select /* \\0 */ '\\0' from a",
	}, {
		input:  "select 1 /* drop this comment */ from t",
		output: "select 1 from t",
	}, {
		input: "select /* union */ 1 from t union select 1 from t",
	}, {
		input: "select /* double union */ 1 from t union select 1 from t union select 1 from t",
	}, {
		input: "select /* union all */ 1 from t union all select 1 from t",
	}, {
		input: "select /* minus */ 1 from t minus select 1 from t",
	}, {
		input: "select /* except */ 1 from t except select 1 from t",
	}, {
		input: "select /* intersect */ 1 from t intersect select 1 from t",
	}, {
		input: "select /* distinct */ distinct 1 from t",
	}, {
		input: "select /* for update */ 1 from t for update",
	}, {
		input: "select /* lock in share mode */ 1 from t lock in share mode",
	}, {
		input: "select /* select list */ 1, 2 from t",
	}, {
		input: "select /* * */ * from t",
	}, {
		input:  "select /* column alias */ a b from t",
		output: "select /* column alias */ a as b from t",
	}, {
		input: "select /* column alias with as */ a as b from t",
	}, {
		input:  "select /* keyword column alias */ a as `By` from t",
		output: "select /* keyword column alias */ a as `by` from t",
	}, {
		input: "select /* a.* */ a.* from t",
	}, {
		input:  "select next value for t",
		output: "select next value from t",
	}, {
		input: "select next value from t",
	}, {
		input: "select /* `By`.* */ `By`.* from t",
	}, {
		input: "select /* select with bool expr */ a = b from t",
	}, {
		input: "select /* case_when */ case when a = b then c end from t",
	}, {
		input: "select /* case_when_else */ case when a = b then c else d end from t",
	}, {
		input: "select /* case_when_when_else */ case when a = b then c when b = d then d else d end from t",
	}, {
		input: "select /* case */ case aa when a = b then c end from t",
	}, {
		input: "select /* parenthesis */ 1 from (t)",
	}, {
		input: "select /* parenthesis multi-table */ 1 from (t1, t2)",
	}, {
		input: "select /* table list */ 1 from t1, t2",
	}, {
		input: "select /* parenthessis in table list 1 */ 1 from (t1), t2",
	}, {
		input: "select /* parenthessis in table list 2 */ 1 from t1, (t2)",
	}, {
		input: "select /* use */ 1 from t1 use index (a) where b = 1",
	}, {
		input:  "select /* keyword index */ 1 from t1 use index (`By`) where b = 1",
		output: "select /* keyword index */ 1 from t1 use index (`by`) where b = 1",
	}, {
		input: "select /* ignore */ 1 from t1 as t2 ignore index (a), t3 use index (b) where b = 1",
	}, {
		input: "select /* use */ 1 from t1 as t2 use index (a), t3 use index (b) where b = 1",
	}, {
		input: "select /* force */ 1 from t1 as t2 force index (a), t3 force index (b) where b = 1",
	}, {
		input:  "select /* table alias */ 1 from t t1",
		output: "select /* table alias */ 1 from t as t1",
	}, {
		input: "select /* table alias with as */ 1 from t as t1",
	}, {
		input: "select /* keyword table alias */ 1 from t as `By`",
	}, {
		input: "select /* join */ 1 from t1 join t2",
	}, {
		input: "select /* join on */ 1 from t1 join t2 on a = b",
	}, {
		input:  "select /* inner join */ 1 from t1 inner join t2",
		output: "select /* inner join */ 1 from t1 join t2",
	}, {
		input:  "select /* cross join */ 1 from t1 cross join t2",
		output: "select /* cross join */ 1 from t1 join t2",
	}, {
		input: "select /* straight_join */ 1 from t1 straight_join t2",
	}, {
		input: "select /* straight_join on */ 1 from t1 straight_join t2 on a = b",
	}, {
		input: "select /* left join */ 1 from t1 left join t2 on a = b",
	}, {
		input:  "select /* left outer join */ 1 from t1 left outer join t2 on a = b",
		output: "select /* left outer join */ 1 from t1 left join t2 on a = b",
	}, {
		input: "select /* right join */ 1 from t1 right join t2 on a = b",
	}, {
		input:  "select /* right outer join */ 1 from t1 right outer join t2 on a = b",
		output: "select /* right outer join */ 1 from t1 right join t2 on a = b",
	}, {
		input: "select /* natural join */ 1 from t1 natural join t2",
	}, {
		input: "select /* natural left join */ 1 from t1 natural left join t2",
	}, {
		input:  "select /* natural left outer join */ 1 from t1 natural left join t2",
		output: "select /* natural left outer join */ 1 from t1 natural left join t2",
	}, {
		input: "select /* natural right join */ 1 from t1 natural right join t2",
	}, {
		input:  "select /* natural right outer join */ 1 from t1 natural right join t2",
		output: "select /* natural right outer join */ 1 from t1 natural right join t2",
	}, {
		input: "select /* join on */ 1 from t1 join t2 on a = b",
	}, {
		input: "select /* s.t */ 1 from s.t",
	}, {
		input: "select /* keyword schema & table name */ 1 from `By`.`bY`",
	}, {
		input: "select /* select in from */ 1 from (select 1 from t) as a",
	}, {
		input:  "select /* select in from with no as */ 1 from (select 1 from t) a",
		output: "select /* select in from with no as */ 1 from (select 1 from t) as a",
	}, {
		input: "select /* where */ 1 from t where a = b",
	}, {
		input: "select /* and */ 1 from t where a = b and a = c",
	}, {
		input: "select /* or */ 1 from t where a = b or a = c",
	}, {
		input: "select /* not */ 1 from t where not a = b",
	}, {
		input: "select /* bool is */ 1 from t where a = b is null",
	}, {
		input: "select /* bool is not */ 1 from t where a = b is not false",
	}, {
		input: "select /* true */ 1 from t where true",
	}, {
		input: "select /* false */ 1 from t where false",
	}, {
		input: "select /* exists */ 1 from t where exists (select 1 from t)",
	}, {
		input: "select /* keyrange */ 1 from t where keyrange(1, 2)",
	}, {
		input: "select /* (boolean) */ 1 from t where not (a = b)",
	}, {
		input: "select /* in value list */ 1 from t where a in (b, c)",
	}, {
		input: "select /* in select */ 1 from t where a in (select 1 from t)",
	}, {
		input: "select /* not in */ 1 from t where a not in (b, c)",
	}, {
		input: "select /* like */ 1 from t where a like b",
	}, {
		input: "select /* not like */ 1 from t where a not like b",
	}, {
		input: "select /* regexp */ 1 from t where a regexp b",
	}, {
		input: "select /* not regexp */ 1 from t where a not regexp b",
	}, {
		input:  "select /* rlike */ 1 from t where a rlike b",
		output: "select /* rlike */ 1 from t where a regexp b",
	}, {
		input:  "select /* not rlike */ 1 from t where a not rlike b",
		output: "select /* not rlike */ 1 from t where a not regexp b",
	}, {
		input: "select /* between */ 1 from t where a between b and c",
	}, {
		input: "select /* not between */ 1 from t where a not between b and c",
	}, {
		input: "select /* is null */ 1 from t where a is null",
	}, {
		input: "select /* is not null */ 1 from t where a is not null",
	}, {
		input: "select /* is true */ 1 from t where a is true",
	}, {
		input: "select /* is not true */ 1 from t where a is not true",
	}, {
		input: "select /* is false */ 1 from t where a is false",
	}, {
		input: "select /* is not false */ 1 from t where a is not false",
	}, {
		input: "select /* < */ 1 from t where a < b",
	}, {
		input: "select /* <= */ 1 from t where a <= b",
	}, {
		input: "select /* >= */ 1 from t where a >= b",
	}, {
		input: "select /* > */ 1 from t where a > b",
	}, {
		input: "select /* != */ 1 from t where a != b",
	}, {
		input:  "select /* <> */ 1 from t where a <> b",
		output: "select /* <> */ 1 from t where a != b",
	}, {
		input: "select /* <=> */ 1 from t where a <=> b",
	}, {
		input: "select /* != */ 1 from t where a != b",
	}, {
		input: "select /* single value expre list */ 1 from t where a in (b)",
	}, {
		input: "select /* select as a value expression */ 1 from t where a = (select a from t)",
	}, {
		input: "select /* parenthesised value */ 1 from t where a = (b)",
	}, {
		input: "select /* over-parenthesize */ ((1)) from t where ((a)) in (((1))) and ((a, b)) in ((((1, 1))), ((2, 2)))",
	}, {
		input: "select /* dot-parenthesize */ (a.b) from t where (b.c) = 2",
	}, {
		input: "select /* & */ 1 from t where a = b & c",
	}, {
		input: "select /* & */ 1 from t where a = b & c",
	}, {
		input: "select /* | */ 1 from t where a = b | c",
	}, {
		input: "select /* ^ */ 1 from t where a = b ^ c",
	}, {
		input: "select /* + */ 1 from t where a = b + c",
	}, {
		input: "select /* - */ 1 from t where a = b - c",
	}, {
		input: "select /* * */ 1 from t where a = b * c",
	}, {
		input: "select /* / */ 1 from t where a = b / c",
	}, {
		input: "select /* % */ 1 from t where a = b % c",
	}, {
		input: "select /* << */ 1 from t where a = b << c",
	}, {
		input: "select /* >> */ 1 from t where a = b >> c",
	}, {
		input:  "select /* % no space */ 1 from t where a = b%c",
		output: "select /* % no space */ 1 from t where a = b % c",
	}, {
		input: "select /* u+ */ 1 from t where a = +b",
	}, {
		input: "select /* u- */ 1 from t where a = -b",
	}, {
		input: "select /* u~ */ 1 from t where a = ~b",
	}, {
		input: "select /* empty function */ 1 from t where a = b()",
	}, {
		input: "select /* function with 1 param */ 1 from t where a = b(c)",
	}, {
		input: "select /* function with many params */ 1 from t where a = b(c, d)",
	}, {
		input: "select /* if as func */ 1 from t where a = if(b)",
	}, {
		input: "select /* function with distinct */ count(distinct a) from t",
	}, {
		input: "select /* a */ a from t",
	}, {
		input: "select /* a.b */ a.b from t",
	}, {
		input:  "select /* keyword a.b */ `By`.`bY` from t",
		output: "select /* keyword a.b */ `By`.`by` from t",
	}, {
		input: "select /* string */ 'a' from t",
	}, {
		input:  "select /* double quoted string */ \"a\" from t",
		output: "select /* double quoted string */ 'a' from t",
	}, {
		input:  "select /* quote quote in string */ 'a''a' from t",
		output: "select /* quote quote in string */ 'a\\'a' from t",
	}, {
		input:  "select /* double quote quote in string */ \"a\"\"a\" from t",
		output: "select /* double quote quote in string */ 'a\\\"a' from t",
	}, {
		input:  "select /* quote in double quoted string */ \"a'a\" from t",
		output: "select /* quote in double quoted string */ 'a\\'a' from t",
	}, {
		input: "select /* backslash quote in string */ 'a\\'a' from t",
	}, {
		input: "select /* literal backslash in string */ 'a\\\\na' from t",
	}, {
		input: "select /* all escapes */ '\\0\\'\\\"\\b\\n\\r\\t\\Z\\\\' from t",
	}, {
		input:  "select /* non-escape */ '\\x' from t",
		output: "select /* non-escape */ 'x' from t",
	}, {
		input: "select /* unescaped backslash */ '\\n' from t",
	}, {
		input: "select /* value argument */ :a from t",
	}, {
		input: "select /* value argument with digit */ :a1 from t",
	}, {
		input: "select /* value argument with dot */ :a.b from t",
	}, {
		input:  "select /* positional argument */ ? from t",
		output: "select /* positional argument */ :v1 from t",
	}, {
		input:  "select /* multiple positional arguments */ ?, ? from t",
		output: "select /* multiple positional arguments */ :v1, :v2 from t",
	}, {
		input: "select /* list arg */ * from t where a in ::list",
	}, {
		input: "select /* list arg not in */ * from t where a not in ::list",
	}, {
		input: "select /* null */ null from t",
	}, {
		input: "select /* octal */ 010 from t",
	}, {
		input: "select /* hex */ 0xf0 from t",
	}, {
		input: "select /* hex caps */ 0xF0 from t",
	}, {
		input: "select /* float */ 0.1 from t",
	}, {
		input: "select /* group by */ 1 from t group by a",
	}, {
		input: "select /* having */ 1 from t having a = b",
	}, {
		input:  "select /* simple order by */ 1 from t order by a",
		output: "select /* simple order by */ 1 from t order by a asc",
	}, {
		input: "select /* order by asc */ 1 from t order by a asc",
	}, {
		input: "select /* order by desc */ 1 from t order by a desc",
	}, {
		input: "select /* limit a */ 1 from t limit a",
	}, {
		input: "select /* limit a,b */ 1 from t limit a, b",
	}, {
		input:  "select /* binary unary */ a- -b from t",
		output: "select /* binary unary */ a - -b from t",
	}, {
		input: "select /* - - */ - -b from t",
	}, {
		input: "select /* interval */ adddate('2008-01-02', interval 31 day) from t",
	}, {
		input: "select /* dual */ 1 from dual",
	}, {
		input:  "select /* Dual */ 1 from Dual",
		output: "select /* Dual */ 1 from dual",
	}, {
		input:  "select /* DUAL */ 1 from Dual",
		output: "select /* DUAL */ 1 from dual",
	}, {
		input: "insert /* simple */ into a values (1)",
	}, {
		input: "insert /* a.b */ into a.b values (1)",
	}, {
		input: "insert /* multi-value */ into a values (1, 2)",
	}, {
		input: "insert /* multi-value list */ into a values (1, 2), (3, 4)",
	}, {
		input:  "insert /* set */ into a set a = 1, a.b = 2",
		output: "insert /* set */ into a(a, a.b) values (1, 2)",
	}, {
		input: "insert /* value expression list */ into a values (a + 1, 2 * 3)",
	}, {
		input: "insert /* column list */ into a(a, b) values (1, 2)",
	}, {
		input: "insert /* qualified column list */ into a(a, a.b) values (1, 2)",
	}, {
		input: "insert /* select */ into a select b, c from d",
	}, {
		input: "insert /* on duplicate */ into a values (1, 2) on duplicate key update b = func(a), c = d",
	}, {
		input: "update /* simple */ a set b = 3",
	}, {
		input: "update /* a.b */ a.b set b = 3",
	}, {
		input: "update /* b.c */ a set b.c = 3",
	}, {
		input: "update /* list */ a set b = 3, c = 4",
	}, {
		input: "update /* expression */ a set b = 3 + 4",
	}, {
		input: "update /* where */ a set b = 3 where a = b",
	}, {
		input: "update /* order */ a set b = 3 order by c desc",
	}, {
		input: "update /* limit */ a set b = 3 limit c",
	}, {
		input: "delete /* simple */ from a",
	}, {
		input: "delete /* a.b */ from a.b",
	}, {
		input: "delete /* where */ from a where a = b",
	}, {
		input: "delete /* order */ from a order by b desc",
	}, {
		input: "delete /* limit */ from a limit b",
	}, {
		input: "set /* simple */ a = 3",
	}, {
		input: "set /* list */ a = 3, b = 4",
	}, {
		input:  "alter ignore table a add foo",
		output: "alter table a",
	}, {
		input:  "alter table a add foo",
		output: "alter table a",
	}, {
		input:  "alter table `By` add foo",
		output: "alter table `By`",
	}, {
		input:  "alter table a alter foo",
		output: "alter table a",
	}, {
		input:  "alter table a change foo",
		output: "alter table a",
	}, {
		input:  "alter table a modify foo",
		output: "alter table a",
	}, {
		input:  "alter table a drop foo",
		output: "alter table a",
	}, {
		input:  "alter table a disable foo",
		output: "alter table a",
	}, {
		input:  "alter table a enable foo",
		output: "alter table a",
	}, {
		input:  "alter table a order foo",
		output: "alter table a",
	}, {
		input:  "alter table a default foo",
		output: "alter table a",
	}, {
		input:  "alter table a discard foo",
		output: "alter table a",
	}, {
		input:  "alter table a import foo",
		output: "alter table a",
	}, {
		input:  "alter table a rename b",
		output: "rename table a b",
	}, {
		input:  "alter table `By` rename `bY`",
		output: "rename table `By` `bY`",
	}, {
		input:  "alter table a rename to b",
		output: "rename table a b",
	}, {
		input: "create table a",
	}, {
		input: "create table `by`",
	}, {
		input:  "create table if not exists a",
		output: "create table a",
	}, {
		input:  "create index a on b",
		output: "alter table b",
	}, {
		input:  "create unique index a on b",
		output: "alter table b",
	}, {
		input:  "create unique index a using foo on b",
		output: "alter table b",
	}, {
		input:  "create view a",
		output: "create table a",
	}, {
		input:  "alter view a",
		output: "alter table a",
	}, {
		input:  "drop view a",
		output: "drop table a",
	}, {
		input: "drop table a",
	}, {
		input:  "drop table if exists a",
		output: "drop table a",
	}, {
		input:  "drop view if exists a",
		output: "drop table a",
	}, {
		input:  "drop index b on a",
		output: "alter table a",
	}, {
		input:  "analyze table a",
		output: "alter table a",
	}, {
		input:  "show foobar",
		output: "other",
	}, {
		input:  "describe foobar",
		output: "other",
	}, {
		input:  "explain foobar",
		output: "other",
	}}
	for _, tcase := range validSQL {
		if tcase.output == "" {
			tcase.output = tcase.input
		}
		tree, err := Parse(tcase.input)
		if err != nil {
			t.Errorf("input: %s, err: %v", tcase.input, err)
			continue
		}
		out := String(tree)
		if out != tcase.output {
			t.Errorf("out: %s, want %s", out, tcase.output)
		}
		// This test just exercises the tree walking functionality.
		// There's no way automated way to verify that a node calls
		// all its children. But we can examine code coverage and
		// ensure that all WalkSubtree functions were called.
		Walk(func(node SQLNode) (bool, error) {
			return true, nil
		}, tree)
	}
}

func TestCaseSensitivity(t *testing.T) {
	validSQL := []struct {
		input  string
		output string
	}{{
		input: "create table A",
	}, {
		input:  "create index b on A",
		output: "alter table A",
	}, {
		input:  "alter table A foo",
		output: "alter table A",
	}, {
		input:  "alter table A rename to B",
		output: "rename table A B",
	}, {
		input:  "rename table A to B",
		output: "rename table A B",
	}, {
		input: "drop table B",
	}, {
		input:  "drop index b on A",
		output: "alter table A",
	}, {
		input: "select a from B",
	}, {
		input:  "select A as B from C",
		output: "select a as b from C",
	}, {
		input: "select B.* from c",
	}, {
		input:  "select B.A from c",
		output: "select B.a from c",
	}, {
		input: "select * from B as C",
	}, {
		input: "select * from A.B",
	}, {
		input: "update A set b = 1",
	}, {
		input: "update A.B set b = 1",
	}, {
		input:  "select A() from b",
		output: "select a() from b",
	}, {
		input:  "select A(B, C) from b",
		output: "select a(b, c) from b",
	}, {
		input:  "select A(distinct B, C) from b",
		output: "select a(distinct b, c) from b",
	}, {
		input:  "select IF(B, C) from b",
		output: "select if(b, c) from b",
	}, {
		input:  "select * from b use index (A)",
		output: "select * from b use index (a)",
	}, {
		input:  "insert into A(A, B) values (1, 2)",
		output: "insert into A(a, b) values (1, 2)",
	}, {
		input:  "CREATE TABLE A",
		output: "create table A",
	}, {
		input:  "create view A",
		output: "create table a",
	}, {
		input:  "alter view A",
		output: "alter table a",
	}, {
		input:  "drop view A",
		output: "drop table a",
	}}
	for _, tcase := range validSQL {
		if tcase.output == "" {
			tcase.output = tcase.input
		}
		tree, err := Parse(tcase.input)
		if err != nil {
			t.Errorf("input: %s, err: %v", tcase.input, err)
			continue
		}
		out := String(tree)
		if out != tcase.output {
			t.Errorf("out: %s, want %s", out, tcase.output)
		}
	}
}

func TestErrors(t *testing.T) {
	invalidSQL := []struct {
		input  string
		output string
	}{{
		input:  "select !8 from t",
		output: "syntax error at position 9 near '!'",
	}, {
		input:  "select $ from t",
		output: "syntax error at position 9 near '$'",
	}, {
		input:  "select : from t",
		output: "syntax error at position 9 near ':'",
	}, {
		input:  "select 078 from t",
		output: "syntax error at position 11 near '078'",
	}, {
		input:  "select `1a` from t",
		output: "syntax error at position 9 near '1'",
	}, {
		input:  "select `:table` from t",
		output: "syntax error at position 9 near ':'",
	}, {
		input:  "select `table:` from t",
		output: "syntax error at position 14 near 'table'",
	}, {
		input:  "select 'aa\\",
		output: "syntax error at position 12 near 'aa'",
	}, {
		input:  "select 'aa",
		output: "syntax error at position 12 near 'aa'",
	}, {
		input:  "select * from t where :1 = 2",
		output: "syntax error at position 24 near ':'",
	}, {
		input:  "select * from t where :. = 2",
		output: "syntax error at position 24 near ':'",
	}, {
		input:  "select * from t where ::1 = 2",
		output: "syntax error at position 25 near '::'",
	}, {
		input:  "select * from t where ::. = 2",
		output: "syntax error at position 25 near '::'",
	}, {
		input:  "update a set c = values(1)",
		output: "syntax error at position 24 near 'values'",
	}, {
		input:  "update a set c = last_insert_id(1)",
		output: "syntax error at position 32 near 'last_insert_id'",
	}, {
		input: "select(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F" +
			"(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(" +
			"F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F" +
			"(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(" +
			"F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F" +
			"(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(" +
			"F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F" +
			"(F(F(F(F(F(F(F(F(F(F(F(F(",
		output: "max nesting level reached at position 406",
	}, {
		input: "select(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F" +
			"(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(" +
			"F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F" +
			"(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(" +
			"F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F" +
			"(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(" +
			"F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F" +
			"(F(F(F(F(F(F(F(F(F(F(F(",
		output: "syntax error at position 405",
	}, {
		input:  "select /* aa",
		output: "syntax error at position 13 near '/* aa'",
	}, {
		// This construct is considered invalid due to a grammar conflict.
		input:  "insert into a select * from b join c on duplicate key update d=e",
		output: "syntax error at position 50 near 'duplicate'",
	}, {
		input:  "select * from a left join b",
		output: "syntax error at position 29",
	}, {
		input:  "select * from a natural join b on c = d",
		output: "syntax error at position 34 near 'on'",
	}, {
		input:  "select next id from a",
		output: "expecting value after next at position 23",
	}}
	for _, tcase := range invalidSQL {
		if tcase.output == "" {
			tcase.output = tcase.input
		}
		_, err := Parse(tcase.input)
		if err == nil || err.Error() != tcase.output {
			t.Errorf("%s: %v, want %s", tcase.input, err, tcase.output)
		}
	}
}

func BenchmarkParse1(b *testing.B) {
	sql := "select 'abcd', 20, 30.0, eid from a where 1=eid and name='3'"
	for i := 0; i < b.N; i++ {
		ast, err := Parse(sql)
		if err != nil {
			b.Fatal(err)
		}
		_ = String(ast)
	}
}

func BenchmarkParse2(b *testing.B) {
	sql := "select aaaa, bbb, ccc, ddd, eeee, ffff, gggg, hhhh, iiii from tttt, ttt1, ttt3 where aaaa = bbbb and bbbb = cccc and dddd+1 = eeee group by fff, gggg having hhhh = iiii and iiii = jjjj order by kkkk, llll limit 3, 4"
	for i := 0; i < b.N; i++ {
		ast, err := Parse(sql)
		if err != nil {
			b.Fatal(err)
		}
		_ = String(ast)
	}
}
