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
		input:  "select 1",
		output: "select 1 from dual",
	}, {
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
		input:  "select 1 from t # aa",
		output: "select 1 from t",
	}, {
		input:  "select 1 --aa\nfrom t",
		output: "select 1 from t",
	}, {
		input:  "select 1 #aa\nfrom t",
		output: "select 1 from t",
	}, {
		input: "select /* simplest */ 1 from t",
	}, {
		input: "select /* double star **/ 1 from t",
	}, {
		input: "select /* double */ /* comment */ 1 from t",
	}, {
		input: "select /* back-quote keyword */ `By` from t",
	}, {
		input: "select /* back-quote num */ `2a` from t",
	}, {
		input: "select /* back-quote . */ `a.b` from t",
	}, {
		input: "select /* back-quote back-quote */ `a``b` from t",
	}, {
		input:  "select /* back-quote unnecessary */ 1 from `t`",
		output: "select /* back-quote unnecessary */ 1 from t",
	}, {
		input:  "select /* back-quote idnum */ 1 from `a1`",
		output: "select /* back-quote idnum */ 1 from a1",
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
		input: "select /* union distinct */ 1 from t union distinct select 1 from t",
	}, {
		input:  "(select /* union parenthesized select */ 1 from t order by a) union select 1 from t",
		output: "(select /* union parenthesized select */ 1 from t order by a asc) union select 1 from t",
	}, {
		input: "select /* union parenthesized select 2 */ 1 from t union (select 1 from t)",
	}, {
		input:  "select /* union order by */ 1 from t union select 1 from t order by a",
		output: "select /* union order by */ 1 from t union select 1 from t order by a asc",
	}, {
		input:  "select /* union order by limit lock */ 1 from t union select 1 from t order by a limit 1 for update",
		output: "select /* union order by limit lock */ 1 from t union select 1 from t order by a asc limit 1 for update",
	}, {
		input: "select /* union with limit on lhs */ 1 from t limit 1 union select 1 from t",
	}, {
		input:  "(select id, a from t order by id limit 1) union (select id, b as a from s order by id limit 1) order by a limit 1",
		output: "(select id, a from t order by id asc limit 1) union (select id, b as a from s order by id asc limit 1) order by a asc limit 1",
	}, {
		input: "select a from (select 1 as a from tbl1 union select 2 from tbl2) as t",
	}, {
		input: "select * from t1 join (select * from t2 union select * from t3) as t",
	}, {
		input: "select * from t1 where col in (select 1 from dual union select 2 from dual)",
	}, {
		input: "select * from t1 where exists (select a from t2 union select b from t3)",
	}, {
		input: "select /* distinct */ distinct 1 from t",
	}, {
		input: "select /* straight_join */ straight_join 1 from t",
	}, {
		input: "select /* for update */ 1 from t for update",
	}, {
		input: "select /* lock in share mode */ 1 from t lock in share mode",
	}, {
		input: "select /* select list */ 1, 2 from t",
	}, {
		input: "select /* * */ * from t",
	}, {
		input: "select /* a.* */ a.* from t",
	}, {
		input: "select /* a.b.* */ a.b.* from t",
	}, {
		input:  "select /* column alias */ a b from t",
		output: "select /* column alias */ a as b from t",
	}, {
		input: "select /* column alias with as */ a as b from t",
	}, {
		input: "select /* keyword column alias */ a as `By` from t",
	}, {
		input:  "select /* column alias as string */ a as \"b\" from t",
		output: "select /* column alias as string */ a as b from t",
	}, {
		input:  "select /* column alias as string without as */ a \"b\" from t",
		output: "select /* column alias as string without as */ a as b from t",
	}, {
		input: "select /* a.* */ a.* from t",
	}, {
		input:  "select next value for t",
		output: "select next 1 values from t",
	}, {
		input:  "select next value from t",
		output: "select next 1 values from t",
	}, {
		input: "select next 10 values from t",
	}, {
		input: "select next :a values from t",
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
		input: "select /* keyword index */ 1 from t1 use index (`By`) where b = 1",
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
		input:  "select /* string table alias */ 1 from t as 't1'",
		output: "select /* string table alias */ 1 from t as t1",
	}, {
		input:  "select /* string table alias without as */ 1 from t 't1'",
		output: "select /* string table alias without as */ 1 from t as t1",
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
		input:  "select /* && */ 1 from t where a = b && a = c",
		output: "select /* && */ 1 from t where a = b and a = c",
	}, {
		input: "select /* or */ 1 from t where a = b or a = c",
	}, {
		input:  "select /* || */ 1 from t where a = b || a = c",
		output: "select /* || */ 1 from t where a = b or a = c",
	}, {
		input: "select /* not */ 1 from t where not a = b",
	}, {
		input: "select /* ! */ 1 from t where a = !1",
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
		input: "select /* like escape */ 1 from t where a like b escape '!'",
	}, {
		input: "select /* not like */ 1 from t where a not like b",
	}, {
		input: "select /* not like escape */ 1 from t where a not like b escape '$'",
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
		input: "select /* div */ 1 from t where a = b div c",
	}, {
		input:  "select /* MOD */ 1 from t where a = b MOD c",
		output: "select /* MOD */ 1 from t where a = b % c",
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
		input: "select /* -> */ a.b -> 'ab' from t",
	}, {
		input: "select /* -> */ a.b ->> 'ab' from t",
	}, {
		input: "select /* empty function */ 1 from t where a = b()",
	}, {
		input: "select /* function with 1 param */ 1 from t where a = b(c)",
	}, {
		input: "select /* function with many params */ 1 from t where a = b(c, d)",
	}, {
		input: "select /* function with distinct */ count(distinct a) from t",
	}, {
		input: "select /* if as func */ 1 from t where a = if(b)",
	}, {
		input: "select /* current_timestamp as func */ current_timestamp() from t",
	}, {
		input: "select /* mod as func */ a from tab where mod(b, 2) = 0",
	}, {
		input: "select /* database as func no param */ database() from t",
	}, {
		input: "select /* database as func 1 param */ database(1) from t",
	}, {
		input: "select /* a */ a from t",
	}, {
		input: "select /* a.b */ a.b from t",
	}, {
		input: "select /* a.b.c */ a.b.c from t",
	}, {
		input: "select /* keyword a.b */ `By`.`bY` from t",
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
		input:  "select /* hex */ x'f0A1' from t",
		output: "select /* hex */ X'f0A1' from t",
	}, {
		input: "select /* hex caps */ X'F0a1' from t",
	}, {
		input: "select /* 0x */ 0xf0 from t",
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
		input: "select /* order by null */ 1 from t order by null",
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
		input: "select /* binary binary */ binary  binary b from t",
	}, {
		input: "select /* binary ~ */ binary  ~b from t",
	}, {
		input: "select /* ~ binary */ ~ binary b from t",
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
		input: "select /* column as bool in where */ a from t where b",
	}, {
		input: "select /* OR of columns in where */ * from t where a or b",
	}, {
		input: "select /* OR of mixed columns in where */ * from t where a = 5 or b and c is not null",
	}, {
		input: "select /* OR in select columns */ (a or b) from t where c = 5",
	}, {
		input: "select /* bool as select value */ a, true from t",
	}, {
		input: "select /* bool column in ON clause */ * from t join s on t.id = s.id and s.foo where t.bar",
	}, {
		input: "select /* bool in order by */ * from t order by a is null or b asc",
	}, {
		input: "select /* string in case statement */ if(max(case a when 'foo' then 1 else 0 end) = 1, 'foo', 'bar') as foobar from t",
	}, {
		input: "select /* dual */ 1 from dual",
	}, {
		input: "select /* dual */ 1 from dual",
	}, {
		input: "insert /* simple */ into a values (1)",
	}, {
		input: "insert /* a.b */ into a.b values (1)",
	}, {
		input: "insert /* multi-value */ into a values (1, 2)",
	}, {
		input: "insert /* multi-value list */ into a values (1, 2), (3, 4)",
	}, {
		input: "insert /* no values */ into a values ()",
	}, {
		input:  "insert /* set */ into a set a = 1, b = 2",
		output: "insert /* set */ into a(a, b) values (1, 2)",
	}, {
		input: "insert /* value expression list */ into a values (a + 1, 2 * 3)",
	}, {
		input: "insert /* column list */ into a(a, b) values (1, 2)",
	}, {
		input: "insert /* qualified column list */ into a(a, b) values (1, 2)",
	}, {
		input:  "insert /* qualified columns */ into t (t.a, t.b) values (1, 2)",
		output: "insert /* qualified columns */ into t(a, b) values (1, 2)",
	}, {
		input: "insert /* select */ into a select b, c from d",
	}, {
		input:  "insert /* no cols & paren select */ into a(select * from t)",
		output: "insert /* no cols & paren select */ into a select * from t",
	}, {
		input:  "insert /* cols & paren select */ into a(a,b,c) (select * from t)",
		output: "insert /* cols & paren select */ into a(a, b, c) select * from t",
	}, {
		input: "insert /* cols & union with paren select */ into a(b, c) (select d, e from f) union (select g from h)",
	}, {
		input: "insert /* on duplicate */ into a values (1, 2) on duplicate key update b = func(a), c = d",
	}, {
		input: "insert /* bool in insert value */ into a values (1, true, false)",
	}, {
		input: "insert /* bool in on duplicate */ into a values (1, 2) on duplicate key update b = false, c = d",
	}, {
		input: "insert /* bool expression on duplicate */ into a values (1, 2) on duplicate key update b = func(a), c = a > d",
	}, {
		input: "update /* simple */ a set b = 3",
	}, {
		input: "update /* a.b */ a.b set b = 3",
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
		input: "update /* bool in update */ a set b = true",
	}, {
		input: "update /* bool expr in update */ a set b = 5 > 2",
	}, {
		input: "update /* bool in update where */ a set b = 5 where c",
	}, {
		input: "update /* table qualifier */ a set a.b = 3",
	}, {
		input: "update /* table qualifier */ a set t.a.b = 3",
	}, {
		input:  "update /* table alias */ tt aa set aa.cc = 3",
		output: "update /* table alias */ tt as aa set aa.cc = 3",
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
		input:  "alter table a rename as b",
		output: "rename table a b",
	}, {
		input:  "alter table a rename index foo to bar",
		output: "alter table a",
	}, {
		input:  "alter table a rename key foo to bar",
		output: "alter table a",
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
		input:  "create fulltext index a using foo on b",
		output: "alter table b",
	}, {
		input:  "create spatial index a using foo on b",
		output: "alter table b",
	}, {
		input:  "create view a",
		output: "create table a",
	}, {
		input:  "create or replace view a",
		output: "create table a",
	}, {
		input:  "alter view a",
		output: "alter table a",
	}, {
		input:  "drop view a",
		output: "drop table a",
	}, {
		input:  "drop table a",
		output: "drop table a",
	}, {
		input:  "drop table if exists a",
		output: "drop table if exists a",
	}, {
		input:  "drop view if exists a",
		output: "drop table if exists a",
	}, {
		input:  "drop index b on a",
		output: "alter table a",
	}, {
		input:  "analyze table a",
		output: "alter table a",
	}, {
		input:  "show databases",
		output: "show databases",
	}, {
		input:  "show tables",
		output: "show tables",
	}, {
		input:  "show vitess_keyspaces",
		output: "show vitess_keyspaces",
	}, {
		input:  "show vitess_shards",
		output: "show vitess_shards",
	}, {
		input:  "show vschema_tables",
		output: "show vschema_tables",
	}, {
		input:  "show create database",
		output: "show unsupported",
	}, {
		input:  "show foobar",
		output: "show unsupported",
	}, {
		input:  "describe foobar",
		output: "other",
	}, {
		input:  "explain foobar",
		output: "other",
	}, {
		input:  "truncate foo",
		output: "other",
	}, {
		input: "select /* EQ true */ 1 from t where a = true",
	}, {
		input: "select /* EQ false */ 1 from t where a = false",
	}, {
		input: "select /* NE true */ 1 from t where a != true",
	}, {
		input: "select /* NE false */ 1 from t where a != false",
	}, {
		input: "select /* LT true */ 1 from t where a < true",
	}, {
		input: "select /* LT false */ 1 from t where a < false",
	}, {
		input: "select /* GT true */ 1 from t where a > true",
	}, {
		input: "select /* GT false */ 1 from t where a > false",
	}, {
		input: "select /* LE true */ 1 from t where a <= true",
	}, {
		input: "select /* LE false */ 1 from t where a <= false",
	}, {
		input: "select /* GE true */ 1 from t where a >= true",
	}, {
		input: "select /* GE false */ 1 from t where a >= false",
	}, {
		input:  "select * from t order by a collate utf8_general_ci",
		output: "select * from t order by a collate utf8_general_ci asc",
	}, {
		input: "select k collate latin1_german2_ci as k1 from t1 order by k1 asc",
	}, {
		input: "select * from t group by a collate utf8_general_ci",
	}, {
		input: "select MAX(k collate latin1_german2_ci) from t1",
	}, {
		input: "select distinct k collate latin1_german2_ci from t1",
	}, {
		input: "select * from t1 where 'Müller' collate latin1_german2_ci = k",
	}, {
		input: "select * from t1 where k like 'Müller' collate latin1_german2_ci",
	}, {
		input: "select k from t1 group by k having k = 'Müller' collate latin1_german2_ci",
	}, {
		input: "select k from t1 join t2 order by a collate latin1_german2_ci asc, b collate latin1_german2_ci asc",
	}, {
		input:  "select k collate 'latin1_german2_ci' as k1 from t1 order by k1 asc",
		output: "select k collate latin1_german2_ci as k1 from t1 order by k1 asc",
	}, {
		input:  "select /* drop trailing semicolon */ 1 from dual;",
		output: "select /* drop trailing semicolon */ 1 from dual",
	}, {
		input: "select /* cache directive */ sql_no_cache 'foo' from t",
	}, {
		input: "select binary 'a' = 'A' from t",
	}, {
		input: "select match(a) against ('foo') from t",
	}, {
		input: "select match(a1, a2) against ('foo' in natural language mode with query expansion) from t",
	}, {
		input: "select title from video as v where match(v.title, v.tag) against ('DEMO' in boolean mode)",
	}, {
		input: "select name, group_concat(score) from t group by name",
	}, {
		input: "select name, group_concat(distinct id, score order by id desc separator ':') from t group by name",
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
		input:  "alter table A convert",
		output: "alter table A",
	}, {
		// View names get lower-cased.
		input:  "alter view A foo",
		output: "alter table a",
	}, {
		input:  "alter table A rename to B",
		output: "rename table A B",
	}, {
		input:  "rename table A to B",
		output: "rename table A B",
	}, {
		input:  "drop table B",
		output: "drop table B",
	}, {
		input:  "drop table if exists B",
		output: "drop table if exists B",
	}, {
		input:  "drop index b on A",
		output: "alter table A",
	}, {
		input: "select a from B",
	}, {
		input: "select A as B from C",
	}, {
		input: "select B.* from c",
	}, {
		input: "select B.A from c",
	}, {
		input: "select * from B as C",
	}, {
		input: "select * from A.B",
	}, {
		input: "update A set b = 1",
	}, {
		input: "update A.B set b = 1",
	}, {
		input: "select A() from b",
	}, {
		input: "select A(B, C) from b",
	}, {
		input: "select A(distinct B, C) from b",
	}, {
		// IF is an exception. It's always lower-cased.
		input:  "select IF(B, C) from b",
		output: "select if(B, C) from b",
	}, {
		input: "select * from b use index (A)",
	}, {
		input: "insert into A(A, B) values (1, 2)",
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
	}, {
		input:  "drop view if exists A",
		output: "drop table if exists a",
	}, {
		input:  "select /* lock in SHARE MODE */ 1 from t lock in SHARE MODE",
		output: "select /* lock in SHARE MODE */ 1 from t lock in share mode",
	}, {
		input:  "select next VALUE from t",
		output: "select next 1 values from t",
	}, {
		input: "select /* use */ 1 from t1 use index (A) where b = 1",
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

func TestKeywords(t *testing.T) {
	validSQL := []struct {
		input  string
		output string
	}{{
		input:  "select current_timestamp",
		output: "select current_timestamp() from dual",
	}, {
		input: "update t set a = current_timestamp()",
	}, {
		input:  "select a, current_date from t",
		output: "select a, current_date() from t",
	}, {
		input:  "insert into t(a, b) values (current_date, current_date())",
		output: "insert into t(a, b) values (current_date(), current_date())",
	}, {
		input: "select * from t where a > utc_timestmp()",
	}, {
		input:  "update t set b = utc_timestamp + 5",
		output: "update t set b = utc_timestamp() + 5",
	}, {
		input:  "select utc_time, utc_date",
		output: "select utc_time(), utc_date() from dual",
	}, {
		input:  "select 1 from dual where localtime > utc_time",
		output: "select 1 from dual where localtime() > utc_time()",
	}, {
		input:  "update t set a = localtimestamp(), b = utc_timestamp",
		output: "update t set a = localtimestamp(), b = utc_timestamp()",
	}, {
		input: "insert into t(a) values (unix_timestamp)",
	}, {
		input: "select replace(a, 'foo', 'bar') from t",
	}, {
		input: "update t set a = replace('1234', '2', '1')",
	}, {
		input: "insert into t(a, b) values ('foo', 'bar') on duplicate key update a = replace(hex('foo'), 'f', 'b')",
	}, {
		input: "update t set a = left('1234', 3)",
	}, {
		input: "select left(a, 5) from t",
	}, {
		input: "update t set d = adddate(date('2003-12-31 01:02:03'), interval 5 days)",
	}, {
		input: "insert into t(a, b) values (left('foo', 1), 'b')",
	}, {
		input: "insert /* qualified function */ into t(a, b) values (test.PI(), 'b')",
	}, {
		input:  "select /* keyword in qualified id */ * from t join z on t.key = z.key",
		output: "select /* keyword in qualified id */ * from t join z on t.`key` = z.`key`",
	}, {
		input:  "select /* non-reserved keywords as unqualified cols */ date, view, offset from t",
		output: "select /* non-reserved keywords as unqualified cols */ `date`, `view`, `offset` from t",
	}, {
		input:  "select /* share and mode as cols */ share, mode from t where share = 'foo'",
		output: "select /* share and mode as cols */ `share`, `mode` from t where `share` = 'foo'",
	}, {
		input:  "select /* unused keywords as cols */ write, varying from t where trailing = 'foo'",
		output: "select /* unused keywords as cols */ `write`, `varying` from t where `trailing` = 'foo'",
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

func TestConvert(t *testing.T) {
	validSQL := []struct {
		input  string
		output string
	}{{
		input:  "select cast('abc' as date) from t",
		output: "select convert('abc', date) from t",
	}, {
		input: "select convert('abc', binary(4)) from t",
	}, {
		input: "select convert('abc', binary) from t",
	}, {
		input: "select convert('abc', char character set binary) from t",
	}, {
		input: "select convert('abc', char(4) ascii) from t",
	}, {
		input: "select convert('abc', char unicode) from t",
	}, {
		input: "select convert('abc', char(4)) from t",
	}, {
		input: "select convert('abc', char) from t",
	}, {
		input: "select convert('abc', nchar(4)) from t",
	}, {
		input: "select convert('abc', nchar) from t",
	}, {
		input: "select convert('abc', signed) from t",
	}, {
		input:  "select convert('abc', signed integer) from t",
		output: "select convert('abc', signed) from t",
	}, {
		input: "select convert('abc', unsigned) from t",
	}, {
		input:  "select convert('abc', unsigned integer) from t",
		output: "select convert('abc', unsigned) from t",
	}, {
		input: "select convert('abc', decimal(3, 4)) from t",
	}, {
		input: "select convert('abc', decimal(4)) from t",
	}, {
		input: "select convert('abc', decimal) from t",
	}, {
		input: "select convert('abc', date) from t",
	}, {
		input: "select convert('abc', time(4)) from t",
	}, {
		input: "select convert('abc', time) from t",
	}, {
		input: "select convert('abc', datetime(9)) from t",
	}, {
		input: "select convert('abc', datetime) from t",
	}, {
		input: "select convert('abc', json) from t",
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

	invalidSQL := []struct {
		input  string
		output string
	}{{
		input:  "select convert('abc' as date) from t",
		output: "syntax error at position 24 near 'as'",
	}, {
		input:  "select convert from t",
		output: "syntax error at position 20 near 'from'",
	}, {
		input:  "select cast('foo', decimal) from t",
		output: "syntax error at position 19",
	}, {
		input:  "select convert('abc', datetime(4+9)) from t",
		output: "syntax error at position 34",
	}, {
		input:  "select convert('abc', decimal(4+9)) from t",
		output: "syntax error at position 33",
	}}

	for _, tcase := range invalidSQL {
		_, err := Parse(tcase.input)
		if err == nil || err.Error() != tcase.output {
			t.Errorf("%s: %v, want %s", tcase.input, err, tcase.output)
		}
	}
}

func TestErrors(t *testing.T) {
	invalidSQL := []struct {
		input  string
		output string
	}{{
		input:  "select $ from t",
		output: "syntax error at position 9 near '$'",
	}, {
		input:  "select : from t",
		output: "syntax error at position 9 near ':'",
	}, {
		input:  "select 0xH from t",
		output: "syntax error at position 10 near '0x'",
	}, {
		input:  "select x'78 from t",
		output: "syntax error at position 12 near '78'",
	}, {
		input:  "select x'777' from t",
		output: "syntax error at position 14 near '777'",
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
		output: "syntax error at position 26 near '1'",
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
		output: "syntax error at position 54 near 'key'",
	}, {
		input:  "select * from a left join b",
		output: "syntax error at position 29",
	}, {
		input:  "select * from a natural join b on c = d",
		output: "syntax error at position 34 near 'on'",
	}, {
		input:  "select next id from a",
		output: "expecting value after next at position 15 near 'id'",
	}, {
		input:  "select next 1+1 values from a",
		output: "syntax error at position 15",
	}, {
		input:  "insert into a values (select * from b)",
		output: "syntax error at position 29 near 'select'",
	}, {
		input:  "select database",
		output: "syntax error at position 17",
	}, {
		input:  "select mod from t",
		output: "syntax error at position 16 near 'from'",
	}, {
		input:  "select 1 from t where div 5",
		output: "syntax error at position 26 near 'div'",
	}, {
		input:  "select 1 from t where binary",
		output: "syntax error at position 30",
	}, {
		input:  "update (select id from foo) subqalias set id = 4",
		output: "syntax error at position 9",
	}, {
		input:  "select match(a1, a2) against ('foo' in boolean mode with query expansion) from t",
		output: "syntax error at position 57 near 'with'",
	}, {
		input:  "select /* reserved keyword as unqualified column */ * from t where key = 'test'",
		output: "syntax error at position 71 near 'key'",
	}, {
		input:  "select /* vitess-reserved keyword as unqualified column */ * from t where escape = 'test'",
		output: "syntax error at position 81 near 'escape'",
	}, {
		input:  "(select /* parenthesized select */ * from t)",
		output: "syntax error at position 46",
	}, {
		input:  "select * from t where id = ((select a from t1 union select b from t2) order by a limit 1)",
		output: "syntax error at position 76 near 'order'",
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
