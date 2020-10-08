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

package sqlparser

import (
	"bufio"
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	validSQL = []struct {
		input  string
		output string
	}{{
		input:  "select 1",
		output: "select 1 from dual",
	}, {
		input: "select 1 from t",
	}, {
		input:  "select * from information_schema.columns",
		output: "select * from information_schema.`columns`",
	}, {
		input:  "select * from information_schema.processlist",
		output: "select * from information_schema.`processlist`",
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
		input: "select a from t",
	}, {
		input: "select $ from t",
	}, {
		input: "select a.b as a$b from $test$",
	}, {
		input:  "select 1 from t // aa\n",
		output: "select 1 from t",
	}, {
		input:  "select 1 from t -- aa\n",
		output: "select 1 from t",
	}, {
		input:  "select 1 from t # aa\n",
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
		// Ensure this doesn't generate: ""select * from t1 join t2 on a = b join t3 on a = b".
		input: "select * from t1 join t2 on a = b join t3",
	}, {
		input: "select * from t1 where col in (select 1 from dual union select 2 from dual)",
	}, {
		input: "select * from t1 where exists (select a from t2 union select b from t3)",
	}, {
		input: "select 1 from dual union select 2 from dual union all select 3 from dual union select 4 from dual union all select 5 from dual",
	}, {
		input: "(select 1 from dual) order by 1 asc limit 2",
	}, {
		input: "(select 1 from dual order by 1 desc) order by 1 asc limit 2",
	}, {
		input: "(select 1 from dual)",
	}, {
		input: "((select 1 from dual))",
	}, {
		input: "select 1 from (select 1 from dual) as t",
	}, {
		input: "select 1 from (select 1 from dual union select 2 from dual) as t",
	}, {
		input: "select 1 from ((select 1 from dual) union select 2 from dual) as t",
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
		input: "select /* use */ 1 from t1 use index () where b = 1",
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
		input: "select /* join on */ 1 from t1 join t2 using (a)",
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
		input: "select /* left join */ 1 from t1 left join t2 using (a)",
	}, {
		input:  "select /* left outer join */ 1 from t1 left outer join t2 on a = b",
		output: "select /* left outer join */ 1 from t1 left join t2 on a = b",
	}, {
		input:  "select /* left outer join */ 1 from t1 left outer join t2 using (a)",
		output: "select /* left outer join */ 1 from t1 left join t2 using (a)",
	}, {
		input: "select /* right join */ 1 from t1 right join t2 on a = b",
	}, {
		input: "select /* right join */ 1 from t1 right join t2 using (a)",
	}, {
		input:  "select /* right outer join */ 1 from t1 right outer join t2 on a = b",
		output: "select /* right outer join */ 1 from t1 right join t2 on a = b",
	}, {
		input:  "select /* right outer join */ 1 from t1 right outer join t2 using (a)",
		output: "select /* right outer join */ 1 from t1 right join t2 using (a)",
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
		input: "select /* join using */ 1 from t1 join t2 using (a)",
	}, {
		input: "select /* join using (a, b, c) */ 1 from t1 join t2 using (a, b, c)",
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
		input: "select /* false on left */ 1 from t where false = 0",
	}, {
		input: "select /* exists */ 1 from t where exists (select 1 from t)",
	}, {
		input:  "select /* (boolean) */ 1 from t where not (a = b)",
		output: "select /* (boolean) */ 1 from t where not a = b",
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
		input:  "select /* parenthesised value */ 1 from t where a = (b)",
		output: "select /* parenthesised value */ 1 from t where a = b",
	}, {
		input:  "select /* over-parenthesize */ ((1)) from t where ((a)) in (((1))) and ((a, b)) in ((((1, 1))), ((2, 2)))",
		output: "select /* over-parenthesize */ 1 from t where a in (1) and (a, b) in ((1, 1), (2, 2))",
	}, {
		input:  "select /* dot-parenthesize */ (a.b) from t where (b.c) = 2",
		output: "select /* dot-parenthesize */ a.b from t where b.c = 2",
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
		input:  "select count(distinctrow(1)) from (select (1) from dual union all select 1 from dual) a",
		output: "select count(distinct 1) from (select 1 from dual union all select 1 from dual) as a",
	}, {
		input: "select /* if as func */ 1 from t where a = if(b)",
	}, {
		input: "select /* current_timestamp */ current_timestamp() from t",
	}, {
		input: "select /* current_timestamp as func */ current_timestamp() from t",
	}, {
		input: "select /* current_timestamp with fsp */ current_timestamp(3) from t",
	}, {
		input: "select /* current_date */ current_date() from t",
	}, {
		input: "select /* current_date as func */ current_date() from t",
	}, {
		input: "select /* current_time */ current_time() from t",
	}, {
		input: "select /* current_time as func */ current_time() from t",
	}, {
		input: "select /* current_time with fsp */ current_time(1) from t",
	}, {
		input: "select /* utc_timestamp */ utc_timestamp() from t",
	}, {
		input: "select /* utc_timestamp as func */ utc_timestamp() from t",
	}, {
		input: "select /* utc_timestamp with fsp */ utc_timestamp(0) from t",
	}, {
		input: "select /* utc_time */ utc_time() from t",
	}, {
		input: "select /* utc_time as func */ utc_time() from t",
	}, {
		input: "select /* utc_time with fsp */ utc_time(4) from t",
	}, {
		input: "select /* utc_date */ utc_date() from t",
	}, {
		input: "select /* utc_date as func */ utc_date() from t",
	}, {
		input: "select /* localtime */ localtime() from t",
	}, {
		input: "select /* localtime as func */ localtime() from t",
	}, {
		input: "select /* localtime with fsp */ localtime(5) from t",
	}, {
		input: "select /* localtimestamp */ localtimestamp() from t",
	}, {
		input: "select /* localtimestamp as func */ localtimestamp() from t",
	}, {
		input: "select /* localtimestamp with fsp */ localtimestamp(7) from t",
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
		input:  "select /* bit literal */ b'0101' from t",
		output: "select /* bit literal */ B'0101' from t",
	}, {
		input: "select /* bit literal caps */ B'010011011010' from t",
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
		input: "select /* interval keyword */ adddate('2008-01-02', interval 1 year) from t",
	}, {
		input:  "select /* TIMESTAMPADD */ TIMESTAMPADD(MINUTE, 1, '2008-01-04') from t",
		output: "select /* TIMESTAMPADD */ timestampadd(MINUTE, 1, '2008-01-04') from t",
	}, {
		input:  "select /* TIMESTAMPDIFF */ TIMESTAMPDIFF(MINUTE, '2008-01-02', '2008-01-04') from t",
		output: "select /* TIMESTAMPDIFF */ timestampdiff(MINUTE, '2008-01-02', '2008-01-04') from t",
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
		input:  "select /* OR in select columns */ (a or b) from t where c = 5",
		output: "select /* OR in select columns */ a or b from t where c = 5",
	}, {
		input: "select /* XOR of columns in where */ * from t where a xor b",
	}, {
		input: "select /* XOR of mixed columns in where */ * from t where a = 5 xor b and c is not null",
	}, {
		input:  "select /* XOR in select columns */ (a xor b) from t where c = 5",
		output: "select /* XOR in select columns */ a xor b from t where c = 5",
	}, {
		input:  "select /* XOR in select columns */ * from t where (1 xor c1 > 0)",
		output: "select /* XOR in select columns */ * from t where 1 xor c1 > 0",
	}, {
		input: "select /* bool as select value */ a, true from t",
	}, {
		input: "select /* bool column in ON clause */ * from t join s on t.id = s.id and s.foo where t.bar",
	}, {
		input: "select /* bool in order by */ * from t order by a is null or b asc",
	}, {
		input: "select /* string in case statement */ if(max(case a when 'foo' then 1 else 0 end) = 1, 'foo', 'bar') as foobar from t",
	}, {
		input:  "/*!show databases*/",
		output: "show databases",
	}, {
		input:  "select /*!40101 * from*/ t",
		output: "select * from t",
	}, {
		input:  "select /*! * from*/ t",
		output: "select * from t",
	}, {
		input:  "select /*!* from*/ t",
		output: "select * from t",
	}, {
		input:  "select /*!401011 from*/ t",
		output: "select 1 from t",
	}, {
		input: "select /* dual */ 1 from dual",
	}, {
		input:  "select * from (select 'tables') tables",
		output: "select * from (select 'tables' from dual) as `tables`",
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
		input:  "insert /* set default */ into a set a = default, b = 2",
		output: "insert /* set default */ into a(a, b) values (default, 2)",
	}, {
		input: "insert /* value expression list */ into a values (a + 1, 2 * 3)",
	}, {
		input: "insert /* default */ into a values (default, 2 * 3)",
	}, {
		input: "insert /* column list */ into a(a, b) values (1, 2)",
	}, {
		input: "insert into a(a, b) values (1, ifnull(null, default(b)))",
	}, {
		input: "insert /* qualified column list */ into a(a, b) values (1, 2)",
	}, {
		input:  "insert /* qualified columns */ into t (t.a, t.b) values (1, 2)",
		output: "insert /* qualified columns */ into t(a, b) values (1, 2)",
	}, {
		input: "insert /* select */ into a select b, c from d",
	}, {
		input:  "insert /* it accepts columns with keyword action */ into a(action, b) values (1, 2)",
		output: "insert /* it accepts columns with keyword action */ into a(`action`, b) values (1, 2)",
	}, {
		input: "insert /* no cols & paren select */ into a (select * from t)",
	}, {
		input: "insert /* cols & paren select */ into a(a, b, c) (select * from t)",
	}, {
		input: "insert /* cols & union with paren select */ into a(b, c) (select d, e from f) union (select g from h)",
	}, {
		input: "insert /* on duplicate */ into a values (1, 2) on duplicate key update b = func(a), c = d",
	}, {
		input: "insert /* bool in insert value */ into a values (1, true, false)",
	}, {
		input: "insert /* bool in on duplicate */ into a values (1, 2) on duplicate key update b = false, c = d",
	}, {
		input: "insert /* bool in on duplicate */ into a values (1, 2, 3) on duplicate key update b = values(b), c = d",
	}, {
		input: "insert /* bool in on duplicate */ into a values (1, 2, 3) on duplicate key update b = values(a.b), c = d",
	}, {
		input: "insert /* bool expression on duplicate */ into a values (1, 2) on duplicate key update b = func(a), c = a > d",
	}, {
		input: "insert into user(username, `status`) values ('Chuck', default(`status`))",
	}, {
		input:  "insert into user(format, tree, vitess) values ('Chuck', 42, 'Barry')",
		output: "insert into user(`format`, `tree`, `vitess`) values ('Chuck', 42, 'Barry')",
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
		input:  "update (select id from foo) subqalias set id = 4",
		output: "update (select id from foo) as subqalias set id = 4",
	}, {
		input:  "update foo f, bar b set f.id = b.id where b.name = 'test'",
		output: "update foo as f, bar as b set f.id = b.id where b.name = 'test'",
	}, {
		input:  "update foo f join bar b on f.name = b.name set f.id = b.id where b.name = 'test'",
		output: "update foo as f join bar as b on f.name = b.name set f.id = b.id where b.name = 'test'",
	}, {
		input: "update /* ignore */ ignore a set b = 3",
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
		input: "delete a from a join b on a.id = b.id where b.name = 'test'",
	}, {
		input: "delete a, b from a, b where a.id = b.id and b.name = 'test'",
	}, {
		input: "delete /* simple */ ignore from a",
	}, {
		input: "delete ignore from a",
	}, {
		input: "delete /* limit */ ignore from a",
	}, {
		input:  "delete from a1, a2 using t1 as a1 inner join t2 as a2 where a1.id=a2.id",
		output: "delete a1, a2 from t1 as a1 join t2 as a2 where a1.id = a2.id",
	}, {
		input: "set /* simple */ a = 3",
	}, {
		input: "set #simple\n b = 4",
	}, {
		input: "set character_set_results = utf8",
	}, {
		input: "set @@session.autocommit = true",
	}, {
		input: "set @@session.`autocommit` = true",
	}, {
		input: "set @@session.'autocommit' = true",
	}, {
		input: "set @@session.\"autocommit\" = true",
	}, {
		input:  "set @@session.autocommit = ON",
		output: "set @@session.autocommit = 'on'",
	}, {
		input:  "set @@session.autocommit= OFF",
		output: "set @@session.autocommit = 'off'",
	}, {
		input:  "set autocommit = on",
		output: "set autocommit = 'on'",
	}, {
		input:  "set autocommit = off",
		output: "set autocommit = 'off'",
	}, {
		input:  "set names utf8 collate foo",
		output: "set names 'utf8'",
	}, {
		input:  "set names utf8 collate 'foo'",
		output: "set names 'utf8'",
	}, {
		input:  "set character set utf8",
		output: "set charset 'utf8'",
	}, {
		input:  "set character set 'utf8'",
		output: "set charset 'utf8'",
	}, {
		input:  "set character set \"utf8\"",
		output: "set charset 'utf8'",
	}, {
		input:  "set charset default",
		output: "set charset default",
	}, {
		input:  "set session wait_timeout = 3600",
		output: "set session wait_timeout = 3600",
	}, {
		input:  "set session wait_timeout = 3600, session autocommit = off",
		output: "set session wait_timeout = 3600, session autocommit = 'off'",
	}, {
		input:  "set session wait_timeout = 3600, @@global.autocommit = off",
		output: "set session wait_timeout = 3600, @@global.autocommit = 'off'",
	}, {
		input: "set /* list */ a = 3, b = 4",
	}, {
		input: "set /* mixed list */ a = 3, names 'utf8', charset 'ascii', b = 4",
	}, {
		input: "set session transaction isolation level repeatable read",
	}, {
		input: "set transaction isolation level repeatable read",
	}, {
		input: "set global transaction isolation level repeatable read",
	}, {
		input: "set transaction isolation level repeatable read",
	}, {
		input: "set transaction isolation level read committed",
	}, {
		input: "set transaction isolation level read uncommitted",
	}, {
		input: "set transaction isolation level serializable",
	}, {
		input: "set transaction read write",
	}, {
		input: "set transaction read only",
	}, {
		input: "set tx_read_only = 1",
	}, {
		input: "set tx_read_only = 0",
	}, {
		input: "set transaction_read_only = 1",
	}, {
		input: "set transaction_read_only = 0",
	}, {
		input: "set tx_isolation = 'repeatable read'",
	}, {
		input: "set tx_isolation = 'read committed'",
	}, {
		input: "set tx_isolation = 'read uncommitted'",
	}, {
		input: "set tx_isolation = 'serializable'",
	}, {
		input: "set sql_safe_updates = 0",
	}, {
		input: "set sql_safe_updates = 1",
	}, {
		input: "set @variable = 42",
	}, {
		input: "set @period.variable = 42",
	}, {
		input:  "alter table a add foo",
		output: "alter table a",
	}, {
		input:  "alter table a add spatial key foo (column1)",
		output: "alter table a",
	}, {
		input:  "alter table a add unique key foo (column1)",
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
		output: "rename table a to b",
	}, {
		input:  "alter table `By` rename `bY`",
		output: "rename table `By` to `bY`",
	}, {
		input:  "alter table a rename to b",
		output: "rename table a to b",
	}, {
		input:  "alter table a rename as b",
		output: "rename table a to b",
	}, {
		input:  "alter table a rename index foo to bar",
		output: "alter table a",
	}, {
		input:  "alter table a rename key foo to bar",
		output: "alter table a",
	}, {
		input:  "alter table e auto_increment = 20",
		output: "alter table e",
	}, {
		input:  "alter table e character set = 'ascii'",
		output: "alter table e",
	}, {
		input:  "alter table e default character set = 'ascii'",
		output: "alter table e",
	}, {
		input:  "alter table e comment = 'hello'",
		output: "alter table e",
	}, {
		input:  "alter table a reorganize partition b into (partition c values less than (?), partition d values less than (maxvalue))",
		output: "alter table a reorganize partition b into (partition c values less than (:v1), partition d values less than (maxvalue))",
	}, {
		input:  "alter table a partition by range (id) (partition p0 values less than (10), partition p1 values less than (maxvalue))",
		output: "alter table a",
	}, {
		input:  "alter table a add column id int",
		output: "alter table a",
	}, {
		input:  "alter table a add index idx (id)",
		output: "alter table a",
	}, {
		input:  "alter table a add fulltext index idx (id)",
		output: "alter table a",
	}, {
		input:  "alter table a add spatial index idx (id)",
		output: "alter table a",
	}, {
		input:  "alter table a add foreign key",
		output: "alter table a",
	}, {
		input:  "alter table a add primary key",
		output: "alter table a",
	}, {
		input:  "alter table a add constraint",
		output: "alter table a",
	}, {
		input:  "alter table a add id",
		output: "alter table a",
	}, {
		input:  "alter table a drop column id int",
		output: "alter table a",
	}, {
		input:  "alter table a drop partition p2712",
		output: "alter table a",
	}, {
		input:  "alter table a drop index idx (id)",
		output: "alter table a",
	}, {
		input:  "alter table a drop fulltext index idx (id)",
		output: "alter table a",
	}, {
		input:  "alter table a drop spatial index idx (id)",
		output: "alter table a",
	}, {
		input:  "alter table a add check ch_1",
		output: "alter table a",
	}, {
		input:  "alter table a drop check ch_1",
		output: "alter table a",
	}, {
		input:  "alter table a drop foreign key",
		output: "alter table a",
	}, {
		input:  "alter table a drop primary key",
		output: "alter table a",
	}, {
		input:  "alter table a drop constraint",
		output: "alter table a",
	}, {
		input:  "alter table a drop id",
		output: "alter table a",
	}, {
		input:  "alter database d default character set = charset",
		output: "alter database d",
	}, {
		input:  "alter database d character set = charset",
		output: "alter database d",
	}, {
		input:  "alter database d default collate = collation",
		output: "alter database d",
	}, {
		input:  "alter database d collate = collation",
		output: "alter database d",
	}, {
		input:  "alter schema d default character set = charset",
		output: "alter database d",
	}, {
		input:  "alter schema d character set = charset",
		output: "alter database d",
	}, {
		input:  "alter schema d default collate = collation",
		output: "alter database d",
	}, {
		input:  "alter schema d collate = collation",
		output: "alter database d",
	}, {
		input: "create table a",
	}, {
		input:  "create table a (\n\t`a` int\n)",
		output: "create table a (\n\ta int\n)",
	}, {
		input: "create table `by` (\n\t`by` char\n)",
	}, {
		input:  "create table if not exists a (\n\t`a` int\n)",
		output: "create table a (\n\ta int\n)",
	}, {
		input:  "create table a ignore me this is garbage",
		output: "create table a",
	}, {
		input:  "create table a (a int, b char, c garbage)",
		output: "create table a",
	}, {
		input:  "create table a (b1 bool not null primary key, b2 boolean not null)",
		output: "create table a (\n\tb1 bool not null primary key,\n\tb2 boolean not null\n)",
	}, {
		input: "alter vschema create vindex hash_vdx using hash",
	}, {
		input: "alter vschema create vindex keyspace.hash_vdx using hash",
	}, {
		input: "alter vschema create vindex lookup_vdx using lookup with owner=user, table=name_user_idx, from=name, to=user_id",
	}, {
		input: "alter vschema create vindex xyz_vdx using xyz with param1=hello, param2='world', param3=123",
	}, {
		input: "alter vschema drop vindex hash_vdx",
	}, {
		input: "alter vschema drop vindex ks.hash_vdx",
	}, {
		input: "alter vschema add table a",
	}, {
		input: "alter vschema add table ks.a",
	}, {
		input: "alter vschema add sequence a_seq",
	}, {
		input: "alter vschema add sequence ks.a_seq",
	}, {
		input: "alter vschema on a add auto_increment id using a_seq",
	}, {
		input: "alter vschema on ks.a add auto_increment id using a_seq",
	}, {
		input: "alter vschema drop table a",
	}, {
		input: "alter vschema drop table ks.a",
	}, {
		input: "alter vschema on a add vindex hash (id)",
	}, {
		input: "alter vschema on ks.a add vindex hash (id)",
	}, {
		input:  "alter vschema on a add vindex `hash` (`id`)",
		output: "alter vschema on a add vindex hash (id)",
	}, {
		input:  "alter vschema on `ks`.a add vindex `hash` (`id`)",
		output: "alter vschema on ks.a add vindex hash (id)",
	}, {
		input:  "alter vschema on a add vindex hash (id) using `hash`",
		output: "alter vschema on a add vindex hash (id) using hash",
	}, {
		input: "alter vschema on a add vindex `add` (`add`)",
	}, {
		input: "alter vschema on a add vindex hash (id) using hash",
	}, {
		input:  "alter vschema on a add vindex hash (id) using `hash`",
		output: "alter vschema on a add vindex hash (id) using hash",
	}, {
		input: "alter vschema on user add vindex name_lookup_vdx (name) using lookup_hash with owner=user, table=name_user_idx, from=name, to=user_id",
	}, {
		input:  "alter vschema on user2 add vindex name_lastname_lookup_vdx (name,lastname) using lookup with owner=`user`, table=`name_lastname_keyspace_id_map`, from=`name,lastname`, to=`keyspace_id`",
		output: "alter vschema on user2 add vindex name_lastname_lookup_vdx (name, lastname) using lookup with owner=user, table=name_lastname_keyspace_id_map, from=name,lastname, to=keyspace_id",
	}, {
		input: "alter vschema on a drop vindex hash",
	}, {
		input: "alter vschema on ks.a drop vindex hash",
	}, {
		input:  "alter vschema on a drop vindex `hash`",
		output: "alter vschema on a drop vindex hash",
	}, {
		input:  "alter vschema on a drop vindex hash",
		output: "alter vschema on a drop vindex hash",
	}, {
		input:  "alter vschema on a drop vindex `add`",
		output: "alter vschema on a drop vindex `add`",
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
		input:  "rename table a to b",
		output: "rename table a to b",
	}, {
		input:  "rename table a to b, b to c",
		output: "rename table a to b, b to c",
	}, {
		input:  "drop view a",
		output: "drop table a",
	}, {
		input:  "drop table a",
		output: "drop table a",
	}, {
		input:  "drop table a, b",
		output: "drop table a, b",
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
		output: "otherread",
	}, {
		input:  "flush tables",
		output: "flush",
	}, {
		input:  "flush tables with read lock",
		output: "flush",
	}, {
		input:  "show binary logs",
		output: "show binary logs",
	}, {
		input:  "show binlog events",
		output: "show binlog",
	}, {
		input:  "show character set",
		output: "show charset",
	}, {
		input:  "show character set like '%foo'",
		output: "show charset like '%foo'",
	}, {
		input:  "show charset",
		output: "show charset",
	}, {
		input:  "show charset like '%foo'",
		output: "show charset like '%foo'",
	}, {
		input:  "show charset where 'charset' = 'utf8'",
		output: "show charset where 'charset' = 'utf8'",
	}, {
		input:  "show charset where 'charset' = '%foo'",
		output: "show charset where 'charset' = '%foo'",
	}, {
		input:  "show collation",
		output: "show collation",
	}, {
		input:  "show collation where `Charset` = 'utf8' and `Collation` = 'utf8_bin'",
		output: "show collation where `Charset` = 'utf8' and `Collation` = 'utf8_bin'",
	}, {
		input:  "show create database d",
		output: "show create database",
	}, {
		input:  "show create event e",
		output: "show create event",
	}, {
		input:  "show create function f",
		output: "show create function",
	}, {
		input:  "show create procedure p",
		output: "show create procedure",
	}, {
		input:  "show create table t",
		output: "show create table t",
	}, {
		input:  "show create trigger t",
		output: "show create trigger",
	}, {
		input:  "show create user u",
		output: "show create user",
	}, {
		input:  "show create view v",
		output: "show create view",
	}, {
		input:  "show databases",
		output: "show databases",
	}, {
		input:  "show schemas",
		output: "show schemas",
	}, {
		input:  "show engine INNODB",
		output: "show engine",
	}, {
		input:  "show engines",
		output: "show engines",
	}, {
		input:  "show storage engines",
		output: "show storage",
	}, {
		input:  "show errors",
		output: "show errors",
	}, {
		input:  "show events",
		output: "show events",
	}, {
		input:  "show function code func",
		output: "show function",
	}, {
		input:  "show function status",
		output: "show function",
	}, {
		input:  "show grants for 'root@localhost'",
		output: "show grants",
	}, {
		input: "show index from t",
	}, {
		input: "show indexes from t",
	}, {
		input: "show keys from t",
	}, {
		input:  "show master status",
		output: "show master",
	}, {
		input:  "show open tables",
		output: "show open",
	}, {
		input:  "show plugins",
		output: "show plugins",
	}, {
		input:  "show privileges",
		output: "show privileges",
	}, {
		input:  "show procedure code p",
		output: "show procedure",
	}, {
		input:  "show procedure status",
		output: "show procedure",
	}, {
		input:  "show processlist",
		output: "show processlist",
	}, {
		input:  "show full processlist",
		output: "show processlist",
	}, {
		input:  "show profile cpu for query 1",
		output: "show profile",
	}, {
		input:  "show profiles",
		output: "show profiles",
	}, {
		input:  "show relaylog events",
		output: "show relaylog",
	}, {
		input:  "show slave hosts",
		output: "show slave",
	}, {
		input:  "show slave status",
		output: "show slave",
	}, {
		input:  "show status",
		output: "show status",
	}, {
		input:  "show global status",
		output: "show global status",
	}, {
		input:  "show session status",
		output: "show session status",
	}, {
		input:  "show table status",
		output: "show table",
	}, {
		input: "show tables",
	}, {
		input: "show tables like '%keyspace%'",
	}, {
		input: "show tables where 1 = 0",
	}, {
		input: "show tables from a",
	}, {
		input: "show tables from a where 1 = 0",
	}, {
		input: "show tables from a like '%keyspace%'",
	}, {
		input: "show full tables",
	}, {
		input: "show full tables from a",
	}, {
		input:  "show full tables in a",
		output: "show full tables from a",
	}, {
		input: "show full tables from a like '%keyspace%'",
	}, {
		input: "show full tables from a where 1 = 0",
	}, {
		input: "show full tables like '%keyspace%'",
	}, {
		input: "show full tables where 1 = 0",
	}, {
		input: "show full columns from a like '%'",
	}, {
		input: "show full columns from messages from test_keyspace like '%'",
	}, {
		input: "show full fields from a like '%'",
	}, {
		input: "show fields from a like '%'",
	}, {
		input:  "show triggers",
		output: "show triggers",
	}, {
		input:  "show variables",
		output: "show variables",
	}, {
		input:  "show global variables",
		output: "show global variables",
	}, {
		input:  "show session variables",
		output: "show session variables",
	}, {
		input: "show vitess_keyspaces",
	}, {
		input: "show vitess_keyspaces like '%'",
	}, {
		input: "show vitess_shards",
	}, {
		input: "show vitess_shards like '%'",
	}, {
		input: "show vitess_tablets",
	}, {
		input: "show vitess_tablets like '%'",
	}, {
		input: "show vitess_tablets where hostname = 'some-tablet'",
	}, {
		input: "show vschema tables",
	}, {
		input: "show vschema vindexes",
	}, {
		input: "show vschema vindexes on t",
	}, {
		input:  "show warnings",
		output: "show warnings",
	}, {
		input:  "select warnings from t",
		output: "select `warnings` from t",
	}, {
		input:  "show foobar",
		output: "show foobar",
	}, {
		input:  "show foobar like select * from table where syntax is 'ignored'",
		output: "show foobar",
	}, {
		input:  "use db",
		output: "use db",
	}, {
		input:  "use duplicate",
		output: "use `duplicate`",
	}, {
		input:  "use `ks:-80@master`",
		output: "use `ks:-80@master`",
	}, {
		input:  "use @replica",
		output: "use `@replica`",
	}, {
		input:  "use ks@replica",
		output: "use `ks@replica`",
	}, {
		input:  "describe select * from t",
		output: "explain select * from t",
	}, {
		input:  "desc select * from t",
		output: "explain select * from t",
	}, {
		input:  "desc foobar",
		output: "otherread",
	}, {
		input:  "explain t1",
		output: "otherread",
	}, {
		input:  "explain t1 col",
		output: "otherread",
	}, {
		input: "explain select * from t",
	}, {
		input: "explain format = traditional select * from t",
	}, {
		input: "explain analyze select * from t",
	}, {
		input: "explain format = tree select * from t",
	}, {
		input: "explain format = json select * from t",
	}, {
		input: "explain format = vitess select * from t",
	}, {
		input:  "describe format = vitess select * from t",
		output: "explain format = vitess select * from t",
	}, {
		input: "explain delete from t",
	}, {
		input: "explain insert into t(col1, col2) values (1, 2)",
	}, {
		input: "explain update t set col = 2",
	}, {
		input:  "truncate table foo",
		output: "truncate table foo",
	}, {
		input:  "truncate foo",
		output: "truncate table foo",
	}, {
		input:  "repair foo",
		output: "otheradmin",
	}, {
		input:  "optimize foo",
		output: "otheradmin",
	}, {
		input:  "lock tables foo",
		output: "otheradmin",
	}, {
		input:  "unlock tables foo",
		output: "otheradmin",
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
		input: "select distinct sql_no_cache 'foo' from t",
	}, {
		input:  "select sql_no_cache distinct 'foo' from t",
		output: "select distinct sql_no_cache 'foo' from t",
	}, {
		input:  "select sql_no_cache straight_join distinct 'foo' from t",
		output: "select distinct sql_no_cache straight_join 'foo' from t",
	}, {
		input:  "select straight_join distinct sql_no_cache 'foo' from t",
		output: "select distinct sql_no_cache straight_join 'foo' from t",
	}, {
		input:  "select sql_calc_found_rows 'foo' from t",
		output: "select sql_calc_found_rows 'foo' from t",
	}, {
		input: "select binary 'a' = 'A' from t",
	}, {
		input: "select 1 from t where foo = _binary 'bar'",
	}, {
		input: "select 1 from t where foo = _utf8 'bar' and bar = _latin1 'sjösjuk'",
	}, {
		input:  "select 1 from t where foo = _binary'bar'",
		output: "select 1 from t where foo = _binary 'bar'",
	}, {
		input: "select 1 from t where foo = _utf8mb4 'bar'",
	}, {
		input:  "select 1 from t where foo = _utf8mb4'bar'",
		output: "select 1 from t where foo = _utf8mb4 'bar'",
	}, {
		input: "select match(a) against ('foo') from t",
	}, {
		input: "select match(a1, a2) against ('foo' in natural language mode with query expansion) from t",
	}, {
		input:  "select database()",
		output: "select database() from dual",
	}, {
		input:  "select schema()",
		output: "select schema() from dual",
	}, {
		input: "select title from video as v where match(v.title, v.tag) against ('DEMO' in boolean mode)",
	}, {
		input: "select name, group_concat(score) from t group by name",
	}, {
		input: "select name, group_concat(distinct id, score order by id desc separator ':') from t group by name",
	}, {
		input: "select name, group_concat(distinct id, score order by id desc separator ':' limit 1) from t group by name",
	}, {
		input: "select name, group_concat(distinct id, score order by id desc separator ':' limit 10, 2) from t group by name",
	}, {
		input: "select * from t partition (p0)",
	}, {
		input: "select * from t partition (p0, p1)",
	}, {
		input: "select e.id, s.city from employees as e join stores partition (p1) as s on e.store_id = s.id",
	}, {
		input: "select truncate(120.3333, 2) from dual",
	}, {
		input: "update t partition (p0) set a = 1",
	}, {
		input: "insert into t partition (p0) values (1, 'asdf')",
	}, {
		input: "insert into t1 select * from t2 partition (p0)",
	}, {
		input: "replace into t partition (p0) values (1, 'asdf')",
	}, {
		input: "delete from t partition (p0) where a = 1",
	}, {
		input: "stream * from t",
	}, {
		input: "vstream * from t",
	}, {
		input: "stream /* comment */ * from t",
	}, {
		input: "begin",
	}, {
		input:  "begin;",
		output: "begin",
	}, {
		input:  "start transaction",
		output: "begin",
	}, {
		input: "commit",
	}, {
		input: "rollback",
	}, {
		input: "create database test_db",
	}, {
		input:  "create schema test_db",
		output: "create database test_db",
	}, {
		input: "create database if not exists test_db",
	}, {
		input:  "create schema if not exists test_db",
		output: "create database if not exists test_db",
	}, {
		input: "drop database test_db",
	}, {
		input:  "drop schema test_db",
		output: "drop database test_db",
	}, {
		input: "drop database if exists test_db",
	}, {
		input:  "delete a.*, b.* from tbl_a a, tbl_b b where a.id = b.id and b.name = 'test'",
		output: "delete a, b from tbl_a as a, tbl_b as b where a.id = b.id and b.name = 'test'",
	}, {
		input:  "select distinctrow a.* from (select (1) from dual union all select 1 from dual) a",
		output: "select distinct a.* from (select 1 from dual union all select 1 from dual) as a",
	}, {
		input: "select `weird function name`() from t",
	}, {
		input: "select status() from t", // should not escape function names that are keywords
	}, {
		input: "select * from `weird table name`",
	}, {
		input:  "SHOW FULL TABLES FROM `jiradb` LIKE 'AO_E8B6CC_ISSUE_MAPPING'",
		output: "show full tables from jiradb like 'AO_E8B6CC_ISSUE_MAPPING'",
	}, {
		input:  "SHOW FULL COLUMNS FROM AO_E8B6CC_ISSUE_MAPPING FROM jiradb LIKE '%'",
		output: "show full columns from AO_E8B6CC_ISSUE_MAPPING from jiradb like '%'",
	}, {
		input:  "SHOW KEYS FROM `AO_E8B6CC_ISSUE_MAPPING` FROM `jiradb`",
		output: "show keys from AO_E8B6CC_ISSUE_MAPPING from jiradb",
	}, {
		input:  "SHOW CREATE TABLE `jiradb`.`AO_E8B6CC_ISSUE_MAPPING`",
		output: "show create table jiradb.AO_E8B6CC_ISSUE_MAPPING",
	}, {
		input:  "SHOW INDEX FROM `AO_E8B6CC_ISSUE_MAPPING` FROM `jiradb`",
		output: "show index from AO_E8B6CC_ISSUE_MAPPING from jiradb",
	}, {
		input:  "SHOW FULL TABLES FROM `jiradb` LIKE '%'",
		output: "show full tables from jiradb like '%'",
	}, {
		input:  "SHOW EXTENDED INDEX FROM `AO_E8B6CC_PROJECT_MAPPING` FROM `jiradb`",
		output: "show extended index from AO_E8B6CC_PROJECT_MAPPING from jiradb",
	}, {
		input:  "SHOW EXTENDED KEYS FROM `AO_E8B6CC_ISSUE_MAPPING` FROM `jiradb`",
		output: "show extended keys from AO_E8B6CC_ISSUE_MAPPING from jiradb",
	}, {
		input:  "SHOW CREATE TABLE `jiradb`.`AO_E8B6CC_ISSUE_MAPPING`",
		output: "show create table jiradb.AO_E8B6CC_ISSUE_MAPPING",
	}, {
		input:  "SHOW INDEXES FROM `AO_E8B6CC_ISSUE_MAPPING` FROM `jiradb`",
		output: "show indexes from AO_E8B6CC_ISSUE_MAPPING from jiradb",
	}, {
		input:  "SHOW FULL TABLES FROM `jiradb` LIKE '%'",
		output: "show full tables from jiradb like '%'",
	}, {
		input:  "SHOW EXTENDED INDEXES FROM `AO_E8B6CC_PROJECT_MAPPING` FROM `jiradb`",
		output: "show extended indexes from AO_E8B6CC_PROJECT_MAPPING from jiradb",
	}, {
		input:  "SHOW EXTENDED INDEXES IN `AO_E8B6CC_PROJECT_MAPPING` IN `jiradb`",
		output: "show extended indexes from AO_E8B6CC_PROJECT_MAPPING from jiradb",
	}, {
		input:  "do 1",
		output: "otheradmin",
	}, {
		input:  "do funcCall(), 2 = 1, 3 + 1",
		output: "otheradmin",
	}, {
		input: "savepoint a",
	}, {
		input: "savepoint `@@@;a`",
	}, {
		input: "rollback to a",
	}, {
		input: "rollback to `@@@;a`",
	}, {
		input:  "rollback work to a",
		output: "rollback to a",
	}, {
		input:  "rollback to savepoint a",
		output: "rollback to a",
	}, {
		input:  "rollback work to savepoint a",
		output: "rollback to a",
	}, {
		input: "release savepoint a",
	}, {
		input: "release savepoint `@@@;a`",
	}}
)

func TestValid(t *testing.T) {
	for _, tcase := range validSQL {
		t.Run(tcase.input, func(t *testing.T) {
			if tcase.output == "" {
				tcase.output = tcase.input
			}
			tree, err := Parse(tcase.input)
			require.NoError(t, err, tcase.input)
			out := String(tree)
			if tcase.output != out {
				t.Errorf("Parsing failed. \nExpected/Got:\n%s\n%s", tcase.output, out)
			}
			// This test just exercises the tree walking functionality.
			// There's no way automated way to verify that a node calls
			// all its children. But we can examine code coverage and
			// ensure that all walkSubtree functions were called.
			Walk(func(node SQLNode) (bool, error) {
				return true, nil
			}, tree)
		})
	}
}

// Ensure there is no corruption from using a pooled yyParserImpl in Parse.
func TestParallelValid(t *testing.T) {
	parallelism := 100
	numIters := 1000

	wg := sync.WaitGroup{}
	wg.Add(parallelism)
	for i := 0; i < parallelism; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < numIters; j++ {
				tcase := validSQL[rand.Intn(len(validSQL))]
				if tcase.output == "" {
					tcase.output = tcase.input
				}
				tree, err := Parse(tcase.input)
				if err != nil {
					t.Errorf("Parse(%q) err: %v, want nil", tcase.input, err)
					continue
				}
				out := String(tree)
				if out != tcase.output {
					t.Errorf("Parse(%q) = %q, want: %q", tcase.input, out, tcase.output)
				}
			}
		}()
	}
	wg.Wait()
}

func TestInvalid(t *testing.T) {
	invalidSQL := []struct {
		input string
		err   string
	}{{
		input: "select a, b from (select * from tbl) sort by a",
		err:   "syntax error",
	}, {
		input: "/*!*/",
		err:   "empty statement",
	}}

	for _, tcase := range invalidSQL {
		_, err := Parse(tcase.input)
		if err == nil {
			t.Errorf("Parse invalid query(%q), got: nil, want: %s...", tcase.input, tcase.err)
		}
		if err != nil && !strings.Contains(err.Error(), tcase.err) {
			t.Errorf("Parse invalid query(%q), got: %v, want: %s...", tcase.input, err, tcase.err)
		}
	}
}

func TestCaseSensitivity(t *testing.T) {
	validSQL := []struct {
		input  string
		output string
	}{{
		input:  "create table A (\n\t`B` int\n)",
		output: "create table A (\n\tB int\n)",
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
		output: "rename table A to B",
	}, {
		input: "rename table A to B",
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
		input:  "CREATE TABLE A (\n\t`A` int\n)",
		output: "create table A (\n\tA int\n)",
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
		input: "update t set a = current_timestamp(5)",
	}, {
		input:  "select a, current_date from t",
		output: "select a, current_date() from t",
	}, {
		input:  "insert into t(a, b) values (current_date, current_date())",
		output: "insert into t(a, b) values (current_date(), current_date())",
	}, {
		input: "select * from t where a > utc_timestmp()",
	}, {
		input: "select * from t where a > utc_timestamp(4)",
	}, {
		input:  "update t set b = utc_timestamp + 5",
		output: "update t set b = utc_timestamp() + 5",
	}, {
		input:  "select utc_time, utc_date, utc_time(6)",
		output: "select utc_time(), utc_date(), utc_time(6) from dual",
	}, {
		input:  "select 1 from dual where localtime > utc_time",
		output: "select 1 from dual where localtime() > utc_time()",
	}, {
		input:  "select 1 from dual where localtime(2) > utc_time(1)",
		output: "select 1 from dual where localtime(2) > utc_time(1)",
	}, {
		input:  "update t set a = localtimestamp(), b = utc_timestamp",
		output: "update t set a = localtimestamp(), b = utc_timestamp()",
	}, {
		input:  "update t set a = localtimestamp(10), b = utc_timestamp(13)",
		output: "update t set a = localtimestamp(10), b = utc_timestamp(13)",
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
	}, {
		input:  "select status from t",
		output: "select `status` from t",
	}, {
		input:  "select Status from t",
		output: "select `Status` from t",
	}, {
		input:  "select variables from t",
		output: "select `variables` from t",
	}, {
		input:  "select Variables from t",
		output: "select `Variables` from t",
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
	}, {
		input: "select convert('abc' using ascii) from t",
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
	}, {
		input:  "/* a comment */",
		output: "empty statement",
	}, {
		input:  "set transaction isolation level 12345",
		output: "syntax error at position 38 near '12345'",
	}}

	for _, tcase := range invalidSQL {
		_, err := Parse(tcase.input)
		if err == nil || err.Error() != tcase.output {
			t.Errorf("%s: %v, want %s", tcase.input, err, tcase.output)
		}
	}
}

func TestIntoOutfileS3(t *testing.T) {
	validSQL := []struct {
		input  string
		output string
	}{{
		input:  "select * from t order by name limit 100 into outfile s3 'out_file_name'",
		output: "select * from t order by name asc limit 100 into outfile s3 'out_file_name'",
	}, {
		input: "select * from (select * from t union select * from t2) as t3 where t3.name in (select col from t4) into outfile s3 'out_file_name'",
	}, {
		// Invalid queries but these are parsed and errors caught in planbuilder
		input: "select * from t limit 100 into outfile s3 'out_file_name' union select * from t2",
	}, {
		input: "select * from (select * from t into outfile s3 'inner_outfile') as t2 into outfile s3 'out_file_name'",
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
		input:  "set transaction isolation level 12345",
		output: "syntax error at position 38 near '12345'",
	}}

	for _, tcase := range invalidSQL {
		_, err := Parse(tcase.input)
		if err == nil || err.Error() != tcase.output {
			t.Errorf("%s: %v, want %s", tcase.input, err, tcase.output)
		}
	}
}

func TestPositionedErr(t *testing.T) {
	invalidSQL := []struct {
		input  string
		output PositionedErr
	}{{
		input:  "select convert('abc' as date) from t",
		output: PositionedErr{"syntax error", 24, []byte("as")},
	}, {
		input:  "select convert from t",
		output: PositionedErr{"syntax error", 20, []byte("from")},
	}, {
		input:  "select cast('foo', decimal) from t",
		output: PositionedErr{"syntax error", 19, nil},
	}, {
		input:  "select convert('abc', datetime(4+9)) from t",
		output: PositionedErr{"syntax error", 34, nil},
	}, {
		input:  "select convert('abc', decimal(4+9)) from t",
		output: PositionedErr{"syntax error", 33, nil},
	}, {
		input:  "set transaction isolation level 12345",
		output: PositionedErr{"syntax error", 38, []byte("12345")},
	}, {
		input:  "select * from a left join b",
		output: PositionedErr{"syntax error", 28, nil},
	}, {
		input:  "select a from (select * from tbl)",
		output: PositionedErr{"syntax error", 34, nil},
	}}

	for _, tcase := range invalidSQL {
		tkn := NewStringTokenizer(tcase.input)
		_, err := ParseNext(tkn)

		if posErr, ok := err.(PositionedErr); !ok {
			t.Errorf("%s: %v expected PositionedErr, got (%T) %v", tcase.input, err, err, tcase.output)
		} else if posErr.Pos != tcase.output.Pos || !bytes.Equal(posErr.Near, tcase.output.Near) || err.Error() != tcase.output.Error() {
			t.Errorf("%s: %v, want: %v", tcase.input, err, tcase.output)
		}
	}
}

func TestSubStr(t *testing.T) {

	validSQL := []struct {
		input  string
		output string
	}{{
		input: `select substr('foobar', 1) from t`,
	}, {
		input: "select substr(a, 1, 6) from t",
	}, {
		input:  "select substring(a, 1) from t",
		output: "select substr(a, 1) from t",
	}, {
		input:  "select substring(a, 1, 6) from t",
		output: "select substr(a, 1, 6) from t",
	}, {
		input:  "select substr(a from 1 for 6) from t",
		output: "select substr(a, 1, 6) from t",
	}, {
		input:  "select substring(a from 1 for 6) from t",
		output: "select substr(a, 1, 6) from t",
	}, {
		input:  `select substr("foo" from 1 for 2) from t`,
		output: `select substr('foo', 1, 2) from t`,
	}, {
		input:  `select substring("foo", 1, 2) from t`,
		output: `select substr('foo', 1, 2) from t`,
	}, {
		input:  `select substr(substr("foo" from 1 for 2), 1, 2) from t`,
		output: `select substr(substr('foo', 1, 2), 1, 2) from t`,
	}, {
		input:  `select substr(substring("foo", 1, 2), 3, 4) from t`,
		output: `select substr(substr('foo', 1, 2), 3, 4) from t`,
	}, {
		input:  `select substring(substr("foo", 1), 2) from t`,
		output: `select substr(substr('foo', 1), 2) from t`,
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

func TestCreateTable(t *testing.T) {
	validSQL := []string{
		// test all the data types and options
		"create table t (\n" +
			"	col_bit bit,\n" +
			"	col_tinyint tinyint auto_increment,\n" +
			"	col_tinyint3 tinyint(3) unsigned,\n" +
			"	col_smallint smallint,\n" +
			"	col_smallint4 smallint(4) zerofill,\n" +
			"	col_mediumint mediumint,\n" +
			"	col_mediumint5 mediumint(5) unsigned not null,\n" +
			"	col_int int,\n" +
			"	col_int10 int(10) not null,\n" +
			"	col_integer integer comment 'this is an integer',\n" +
			"	col_bigint bigint,\n" +
			"	col_bigint10 bigint(10) zerofill not null default 10,\n" +
			"	col_real real,\n" +
			"	col_real2 real(1,2) not null default 1.23,\n" +
			"	col_double double,\n" +
			"	col_double2 double(3,4) not null default 1.23,\n" +
			"	col_float float,\n" +
			"	col_float2 float(3,4) not null default 1.23,\n" +
			"	col_decimal decimal,\n" +
			"	col_decimal2 decimal(2),\n" +
			"	col_decimal3 decimal(2,3),\n" +
			"	col_numeric numeric,\n" +
			"	col_numeric2 numeric(2),\n" +
			"	col_numeric3 numeric(2,3),\n" +
			"	col_date date,\n" +
			"	col_time time,\n" +
			"	col_timestamp timestamp,\n" +
			"	col_datetime datetime,\n" +
			"	col_year year,\n" +
			"	col_char char,\n" +
			"	col_char2 char(2),\n" +
			"	col_char3 char(3) character set ascii,\n" +
			"	col_char4 char(4) character set ascii collate ascii_bin,\n" +
			"	col_varchar varchar,\n" +
			"	col_varchar2 varchar(2),\n" +
			"	col_varchar3 varchar(3) character set ascii,\n" +
			"	col_varchar4 varchar(4) character set ascii collate ascii_bin,\n" +
			"	col_binary binary,\n" +
			"	col_varbinary varbinary(10),\n" +
			"	col_tinyblob tinyblob,\n" +
			"	col_blob blob,\n" +
			"	col_mediumblob mediumblob,\n" +
			"	col_longblob longblob,\n" +
			"	col_tinytext tinytext,\n" +
			"	col_text text,\n" +
			"	col_mediumtext mediumtext,\n" +
			"	col_longtext longtext,\n" +
			"	col_text text character set ascii collate ascii_bin,\n" +
			"	col_json json,\n" +
			"	col_enum enum('a', 'b', 'c', 'd'),\n" +
			"	col_enum2 enum('a', 'b', 'c', 'd') character set ascii,\n" +
			"	col_enum3 enum('a', 'b', 'c', 'd') collate ascii_bin,\n" +
			"	col_enum4 enum('a', 'b', 'c', 'd') character set ascii collate ascii_bin,\n" +
			"	col_set set('a', 'b', 'c', 'd'),\n" +
			"	col_set2 set('a', 'b', 'c', 'd') character set ascii,\n" +
			"	col_set3 set('a', 'b', 'c', 'd') collate ascii_bin,\n" +
			"	col_set4 set('a', 'b', 'c', 'd') character set ascii collate ascii_bin,\n" +
			"	col_geometry1 geometry,\n" +
			"	col_geometry2 geometry not null,\n" +
			"	col_point1 point,\n" +
			"	col_point2 point not null,\n" +
			"	col_linestring1 linestring,\n" +
			"	col_linestring2 linestring not null,\n" +
			"	col_polygon1 polygon,\n" +
			"	col_polygon2 polygon not null,\n" +
			"	col_geometrycollection1 geometrycollection,\n" +
			"	col_geometrycollection2 geometrycollection not null,\n" +
			"	col_multipoint1 multipoint,\n" +
			"	col_multipoint2 multipoint not null,\n" +
			"	col_multilinestring1 multilinestring,\n" +
			"	col_multilinestring2 multilinestring not null,\n" +
			"	col_multipolygon1 multipolygon,\n" +
			"	col_multipolygon2 multipolygon not null\n" +
			")",

		// test defining indexes separately
		"create table t (\n" +
			"	id int auto_increment,\n" +
			"	username varchar,\n" +
			"	email varchar,\n" +
			"	full_name varchar,\n" +
			"	geom point not null,\n" +
			"	status_nonkeyword varchar,\n" +
			"	primary key (id),\n" +
			"	spatial key geom (geom),\n" +
			"	unique key by_username (username),\n" +
			"	unique by_username2 (username),\n" +
			"	unique index by_username3 (username),\n" +
			"	index by_status (status_nonkeyword),\n" +
			"	key by_full_name (full_name)\n" +
			")",

		// test that indexes support USING <id>
		"create table t (\n" +
			"	id int auto_increment,\n" +
			"	username varchar,\n" +
			"	email varchar,\n" +
			"	full_name varchar,\n" +
			"	status_nonkeyword varchar,\n" +
			"	primary key (id) using BTREE,\n" +
			"	unique key by_username (username) using HASH,\n" +
			"	unique by_username2 (username) using OTHER,\n" +
			"	unique index by_username3 (username) using XYZ,\n" +
			"	index by_status (status_nonkeyword) using PDQ,\n" +
			"	key by_full_name (full_name) using OTHER\n" +
			")",
		// test other index options
		"create table t (\n" +
			"	id int auto_increment,\n" +
			"	username varchar,\n" +
			"	email varchar,\n" +
			"	primary key (id) comment 'hi',\n" +
			"	unique key by_username (username) key_block_size 8,\n" +
			"	unique index by_username4 (username) comment 'hi' using BTREE,\n" +
			"	unique index by_username4 (username) using BTREE key_block_size 4 comment 'hi'\n" +
			")",

		// multi-column indexes
		"create table t (\n" +
			"	id int auto_increment,\n" +
			"	username varchar,\n" +
			"	email varchar,\n" +
			"	full_name varchar,\n" +
			"	a int,\n" +
			"	b int,\n" +
			"	c int,\n" +
			"	primary key (id, username),\n" +
			"	unique key by_abc (a, b, c),\n" +
			"	unique key (a, b, c),\n" +
			"	key by_email (email(10), username)\n" +
			")",

		// foreign keys
		"create table t (\n" +
			"	id int auto_increment,\n" +
			"	username varchar,\n" +
			"	k int,\n" +
			"	Z int,\n" +
			"	primary key (id, username),\n" +
			"	key by_email (email(10), username),\n" +
			"	constraint second_ibfk_1 foreign key (k, j) references simple (a, b),\n" +
			"	constraint second_ibfk_1 foreign key (k, j) references simple (a, b) on delete restrict,\n" +
			"	constraint second_ibfk_1 foreign key (k, j) references simple (a, b) on delete no action,\n" +
			"	constraint second_ibfk_1 foreign key (k, j) references simple (a, b) on delete cascade on update set default,\n" +
			"	constraint second_ibfk_1 foreign key (k, j) references simple (a, b) on delete set default on update set null,\n" +
			"	constraint second_ibfk_1 foreign key (k, j) references simple (a, b) on delete set null on update restrict,\n" +
			"	constraint second_ibfk_1 foreign key (k, j) references simple (a, b) on update no action,\n" +
			"	constraint second_ibfk_1 foreign key (k, j) references simple (a, b) on update cascade\n" +
			")",

		// table options
		"create table t (\n" +
			"	id int auto_increment\n" +
			") engine InnoDB,\n" +
			"  auto_increment 123,\n" +
			"  avg_row_length 1,\n" +
			"  default character set utf8mb4,\n" +
			"  character set latin1,\n" +
			"  checksum 0,\n" +
			"  default collate binary,\n" +
			"  collate ascii_bin,\n" +
			"  comment 'this is a comment',\n" +
			"  compression 'zlib',\n" +
			"  connection 'connect_string',\n" +
			"  data directory 'absolute path to directory',\n" +
			"  delay_key_write 1,\n" +
			"  encryption 'n',\n" +
			"  index directory 'absolute path to directory',\n" +
			"  insert_method no,\n" +
			"  key_block_size 1024,\n" +
			"  max_rows 100,\n" +
			"  min_rows 10,\n" +
			"  pack_keys 0,\n" +
			"  password 'sekret',\n" +
			"  row_format default,\n" +
			"  stats_auto_recalc default,\n" +
			"  stats_persistent 0,\n" +
			"  stats_sample_pages 1,\n" +
			"  tablespace tablespace_name storage disk,\n" +
			"  tablespace tablespace_name\n",

		// boolean columns
		"create table t (\n" +
			"	bi bigint not null primary key,\n" +
			"	b1 bool not null,\n" +
			"	b2 boolean\n" +
			")",
	}
	for _, sql := range validSQL {
		sql = strings.TrimSpace(sql)
		tree, err := ParseStrictDDL(sql)
		if err != nil {
			t.Errorf("input: %s, err: %v", sql, err)
			continue
		}
		got := String(tree.(*DDL))

		if sql != got {
			t.Errorf("want:\n%s\ngot:\n%s", sql, got)
		}
	}

	sql := "create table t garbage"
	_, err := Parse(sql)
	if err != nil {
		t.Errorf("input: %s, err: %v", sql, err)
	}

	tree, err := ParseStrictDDL(sql)
	if tree != nil || err == nil {
		t.Errorf("ParseStrictDDL unexpectedly accepted input %s", sql)
	}

	testCases := []struct {
		input  string
		output string
	}{{
		// test key_block_size
		input: "create table t (\n" +
			"	id int auto_increment,\n" +
			"	username varchar,\n" +
			"	unique key by_username (username) key_block_size 8,\n" +
			"	unique key by_username2 (username) key_block_size=8,\n" +
			"	unique by_username3 (username) key_block_size = 4\n" +
			")",
		output: "create table t (\n" +
			"	id int auto_increment,\n" +
			"	username varchar,\n" +
			"	unique key by_username (username) key_block_size 8,\n" +
			"	unique key by_username2 (username) key_block_size 8,\n" +
			"	unique by_username3 (username) key_block_size 4\n" +
			")",
	}, {
		// test defaults
		input: "create table t (\n" +
			"	i1 int default 1,\n" +
			"	i2 int default null,\n" +
			"	f1 float default 1.23,\n" +
			"	s1 varchar default 'c',\n" +
			"	s2 varchar default 'this is a string',\n" +
			"	s3 varchar default null,\n" +
			"	s4 timestamp default current_timestamp,\n" +
			"	s5 bit(1) default B'0'\n" +
			")",
		output: "create table t (\n" +
			"	i1 int default 1,\n" +
			"	i2 int default null,\n" +
			"	f1 float default 1.23,\n" +
			"	s1 varchar default 'c',\n" +
			"	s2 varchar default 'this is a string',\n" +
			"	`s3` varchar default null,\n" +
			"	s4 timestamp default current_timestamp(),\n" +
			"	s5 bit(1) default B'0'\n" +
			")",
	}, {
		// test non_reserved word in column name
		input: "create table t (\n" +
			"	repair int\n" +
			")",
		output: "create table t (\n" +
			"	`repair` int\n" +
			")",
	}, {
		// test key field options
		input: "create table t (\n" +
			"	id int auto_increment primary key,\n" +
			"	username varchar unique key,\n" +
			"	email varchar unique,\n" +
			"	full_name varchar key,\n" +
			"	time1 timestamp on update current_timestamp,\n" +
			"	time2 timestamp default current_timestamp on update current_timestamp\n" +
			")",
		output: "create table t (\n" +
			"	id int auto_increment primary key,\n" +
			"	username varchar unique key,\n" +
			"	email varchar unique,\n" +
			"	full_name varchar key,\n" +
			"	time1 timestamp on update current_timestamp(),\n" +
			"	time2 timestamp default current_timestamp() on update current_timestamp()\n" +
			")",
	}, {
		// test current_timestamp with and without ()
		input: "create table t (\n" +
			"	time1 timestamp default current_timestamp,\n" +
			"	time2 timestamp default current_timestamp(),\n" +
			"	time3 timestamp default current_timestamp on update current_timestamp,\n" +
			"	time4 timestamp default current_timestamp() on update current_timestamp(),\n" +
			"	time5 timestamp(3) default current_timestamp(3) on update current_timestamp(3)\n" +
			")",
		output: "create table t (\n" +
			"	time1 timestamp default current_timestamp(),\n" +
			"	time2 timestamp default current_timestamp(),\n" +
			"	time3 timestamp default current_timestamp() on update current_timestamp(),\n" +
			"	time4 timestamp default current_timestamp() on update current_timestamp(),\n" +
			"	time5 timestamp(3) default current_timestamp(3) on update current_timestamp(3)\n" +
			")",
	}, {
		// test utc_timestamp with and without ()
		input: "create table t (\n" +
			"	time1 timestamp default utc_timestamp,\n" +
			"	time2 timestamp default utc_timestamp(),\n" +
			"	time3 timestamp default utc_timestamp on update utc_timestamp,\n" +
			"	time4 timestamp default utc_timestamp() on update utc_timestamp(),\n" +
			"	time5 timestamp(4) default utc_timestamp(4) on update utc_timestamp(4)\n" +
			")",
		output: "create table t (\n" +
			"	time1 timestamp default utc_timestamp(),\n" +
			"	time2 timestamp default utc_timestamp(),\n" +
			"	time3 timestamp default utc_timestamp() on update utc_timestamp(),\n" +
			"	time4 timestamp default utc_timestamp() on update utc_timestamp(),\n" +
			"	time5 timestamp(4) default utc_timestamp(4) on update utc_timestamp(4)\n" +
			")",
	}, {
		// test utc_time with and without ()
		input: "create table t (\n" +
			"	time1 timestamp default utc_time,\n" +
			"	time2 timestamp default utc_time(),\n" +
			"	time3 timestamp default utc_time on update utc_time,\n" +
			"	time4 timestamp default utc_time() on update utc_time(),\n" +
			"	time5 timestamp(5) default utc_time(5) on update utc_time(5)\n" +
			")",
		output: "create table t (\n" +
			"	time1 timestamp default utc_time(),\n" +
			"	time2 timestamp default utc_time(),\n" +
			"	time3 timestamp default utc_time() on update utc_time(),\n" +
			"	time4 timestamp default utc_time() on update utc_time(),\n" +
			"	time5 timestamp(5) default utc_time(5) on update utc_time(5)\n" +
			")",
	}, {
		// test utc_date with and without ()
		input: "create table t (\n" +
			"	time1 timestamp default utc_date,\n" +
			"	time2 timestamp default utc_date(),\n" +
			"	time3 timestamp default utc_date on update utc_date,\n" +
			"	time4 timestamp default utc_date() on update utc_date()\n" +
			")",
		output: "create table t (\n" +
			"	time1 timestamp default utc_date(),\n" +
			"	time2 timestamp default utc_date(),\n" +
			"	time3 timestamp default utc_date() on update utc_date(),\n" +
			"	time4 timestamp default utc_date() on update utc_date()\n" +
			")",
	}, {
		// test localtime with and without ()
		input: "create table t (\n" +
			"	time1 timestamp default localtime,\n" +
			"	time2 timestamp default localtime(),\n" +
			"	time3 timestamp default localtime on update localtime,\n" +
			"	time4 timestamp default localtime() on update localtime(),\n" +
			"	time5 timestamp(6) default localtime(6) on update localtime(6)\n" +
			")",
		output: "create table t (\n" +
			"	time1 timestamp default localtime(),\n" +
			"	time2 timestamp default localtime(),\n" +
			"	time3 timestamp default localtime() on update localtime(),\n" +
			"	time4 timestamp default localtime() on update localtime(),\n" +
			"	time5 timestamp(6) default localtime(6) on update localtime(6)\n" +
			")",
	}, {
		// test localtimestamp with and without ()
		input: "create table t (\n" +
			"	time1 timestamp default localtimestamp,\n" +
			"	time2 timestamp default localtimestamp(),\n" +
			"	time3 timestamp default localtimestamp on update localtimestamp,\n" +
			"	time4 timestamp default localtimestamp() on update localtimestamp(),\n" +
			"	time5 timestamp(1) default localtimestamp(1) on update localtimestamp(1)\n" +
			")",
		output: "create table t (\n" +
			"	time1 timestamp default localtimestamp(),\n" +
			"	time2 timestamp default localtimestamp(),\n" +
			"	time3 timestamp default localtimestamp() on update localtimestamp(),\n" +
			"	time4 timestamp default localtimestamp() on update localtimestamp(),\n" +
			"	time5 timestamp(1) default localtimestamp(1) on update localtimestamp(1)\n" +
			")",
	}, {
		// test current_date with and without ()
		input: "create table t (\n" +
			"	time1 timestamp default current_date,\n" +
			"	time2 timestamp default current_date(),\n" +
			"	time3 timestamp default current_date on update current_date,\n" +
			"	time4 timestamp default current_date() on update current_date()\n" +
			")",
		output: "create table t (\n" +
			"	time1 timestamp default current_date(),\n" +
			"	time2 timestamp default current_date(),\n" +
			"	time3 timestamp default current_date() on update current_date(),\n" +
			"	time4 timestamp default current_date() on update current_date()\n" +
			")",
	}, {
		// test current_time with and without ()
		input: "create table t (\n" +
			"	time1 timestamp default current_time,\n" +
			"	time2 timestamp default current_time(),\n" +
			"	time3 timestamp default current_time on update current_time,\n" +
			"	time4 timestamp default current_time() on update current_time(),\n" +
			"	time5 timestamp(2) default current_time(2) on update current_time(2)\n" +
			")",
		output: "create table t (\n" +
			"	time1 timestamp default current_time(),\n" +
			"	time2 timestamp default current_time(),\n" +
			"	time3 timestamp default current_time() on update current_time(),\n" +
			"	time4 timestamp default current_time() on update current_time(),\n" +
			"	time5 timestamp(2) default current_time(2) on update current_time(2)\n" +
			")",
	},
	}
	for _, tcase := range testCases {
		tree, err := ParseStrictDDL(tcase.input)
		if err != nil {
			t.Errorf("input: %s, err: %v", tcase.input, err)
			continue
		}
		if got, want := String(tree.(*DDL)), tcase.output; got != want {
			t.Errorf("Parse(%s):\n%s, want\n%s", tcase.input, got, want)
		}
	}
}

func TestCreateTableLike(t *testing.T) {
	normal := "create table a like b"
	testCases := []struct {
		input  string
		output string
	}{
		{
			"create table a like b",
			normal,
		},
		{
			"create table a (like b)",
			normal,
		},
		{
			"create table ks.a like unsharded_ks.b",
			"create table ks.a like unsharded_ks.b",
		},
	}
	for _, tcase := range testCases {
		tree, err := ParseStrictDDL(tcase.input)
		if err != nil {
			t.Errorf("input: %s, err: %v", tcase.input, err)
			continue
		}
		if got, want := String(tree.(*DDL)), tcase.output; got != want {
			t.Errorf("Parse(%s):\n%s, want\n%s", tcase.input, got, want)
		}
	}
}

func TestCreateTableEscaped(t *testing.T) {
	testCases := []struct {
		input  string
		output string
	}{{
		input: "create table `a`(`id` int, primary key(`id`))",
		output: "create table a (\n" +
			"\tid int,\n" +
			"\tprimary key (id)\n" +
			")",
	}, {
		input: "create table `insert`(`update` int, primary key(`delete`))",
		output: "create table `insert` (\n" +
			"\t`update` int,\n" +
			"\tprimary key (`delete`)\n" +
			")",
	}}
	for _, tcase := range testCases {
		tree, err := ParseStrictDDL(tcase.input)
		if err != nil {
			t.Errorf("input: %s, err: %v", tcase.input, err)
			continue
		}
		if got, want := String(tree.(*DDL)), tcase.output; got != want {
			t.Errorf("Parse(%s):\n%s, want\n%s", tcase.input, got, want)
		}
	}
}

var (
	invalidSQL = []struct {
		input        string
		output       string
		excludeMulti bool // Don't use in the ParseNext multi-statement parsing tests.
	}{{
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
		output: "syntax error at position 404",
	}, {
		// This construct is considered invalid due to a grammar conflict.
		input:  "insert into a select * from b join c on duplicate key update d=e",
		output: "syntax error at position 54 near 'key'",
	}, {
		input:  "select * from a left join b",
		output: "syntax error at position 28",
	}, {
		input:  "select * from a natural join b on c = d",
		output: "syntax error at position 34 near 'on'",
	}, {
		input:  "select * from a natural join b using (c)",
		output: "syntax error at position 37 near 'using'",
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
		output: "syntax error at position 16",
	}, {
		input:  "select mod from t",
		output: "syntax error at position 16 near 'from'",
	}, {
		input:  "select 1 from t where div 5",
		output: "syntax error at position 26 near 'div'",
	}, {
		input:  "select 1 from t where binary",
		output: "syntax error at position 29",
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
		input:  "select * from t where id = ((select a from t1 union select b from t2) order by a limit 1)",
		output: "syntax error at position 76 near 'order'",
	}, {
		input:  "select /* straight_join using */ 1 from t1 straight_join t2 using (a)",
		output: "syntax error at position 66 near 'using'",
	}, {
		input:        "select 'aa",
		output:       "syntax error at position 11 near 'aa'",
		excludeMulti: true,
	}, {
		input:        "select 'aa\\",
		output:       "syntax error at position 12 near 'aa'",
		excludeMulti: true,
	}, {
		input:        "select /* aa",
		output:       "syntax error at position 13 near '/* aa'",
		excludeMulti: true,
	}, {
		// non_reserved keywords are currently not permitted everywhere
		input:        "create database repair",
		output:       "syntax error at position 23 near 'repair'",
		excludeMulti: true,
	}}
)

func TestErrors(t *testing.T) {
	for _, tcase := range invalidSQL {
		t.Run(tcase.input, func(t *testing.T) {
			_, err := Parse(tcase.input)
			require.Error(t, err, tcase.output)
			require.Equal(t, err.Error(), tcase.output)
		})
	}
}

// TestSkipToEnd tests that the skip to end functionality
// does not skip past a ';'. If any tokens exist after that, Parse
// should return an error.
func TestSkipToEnd(t *testing.T) {
	testcases := []struct {
		input  string
		output string
	}{{
		// This is the case where the partial ddl will be reset
		// because of a premature ';'.
		input:  "create table a(id; select * from t",
		output: "syntax error at position 19",
	}, {
		// Partial DDL should get reset for valid DDLs also.
		input:  "create table a(id int); select * from t",
		output: "syntax error at position 31 near 'select'",
	}, {
		// Partial DDL does not get reset here. But we allow the
		// DDL only if there are no new tokens after skipping to end.
		input:  "create table a bb cc; select * from t",
		output: "extra characters encountered after end of DDL: 'select'",
	}, {
		// Test that we don't step at ';' inside strings.
		input:  "create table a bb 'a;'; select * from t",
		output: "extra characters encountered after end of DDL: 'select'",
	}}
	for _, tcase := range testcases {
		_, err := Parse(tcase.input)
		if err == nil || err.Error() != tcase.output {
			t.Errorf("%s: %v, want %s", tcase.input, err, tcase.output)
		}
	}
}

func TestParseDjangoQueries(t *testing.T) {

	file, err := os.Open("./test_queries/django_queries.txt")
	if err != nil {
		t.Errorf(" Error: %v", err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {

		_, err := Parse(string(scanner.Text()))
		if err != nil {
			t.Error(scanner.Text())
			t.Errorf(" Error: %v", err)
		}
	}
}

// Benchmark run on 6/23/17, prior to improvements:
// BenchmarkParse1-4         100000             16334 ns/op
// BenchmarkParse2-4          30000             44121 ns/op

// Benchmark run on 9/3/18, comparing pooled parser performance.
//
// benchmark                     old ns/op     new ns/op     delta
// BenchmarkNormalize-4          2540          2533          -0.28%
// BenchmarkParse1-4             18269         13330         -27.03%
// BenchmarkParse2-4             46703         41255         -11.67%
// BenchmarkParse2Parallel-4     22246         20707         -6.92%
// BenchmarkParse3-4             4064743       4083135       +0.45%
//
// benchmark                     old allocs     new allocs     delta
// BenchmarkNormalize-4          27             27             +0.00%
// BenchmarkParse1-4             75             74             -1.33%
// BenchmarkParse2-4             264            263            -0.38%
// BenchmarkParse2Parallel-4     176            175            -0.57%
// BenchmarkParse3-4             360            361            +0.28%
//
// benchmark                     old bytes     new bytes     delta
// BenchmarkNormalize-4          821           821           +0.00%
// BenchmarkParse1-4             22776         2307          -89.87%
// BenchmarkParse2-4             28352         7881          -72.20%
// BenchmarkParse2Parallel-4     25712         5235          -79.64%
// BenchmarkParse3-4             6352082       6336307       -0.25%

const (
	sql1 = "select 'abcd', 20, 30.0, eid from a where 1=eid and name='3'"
	sql2 = "select aaaa, bbb, ccc, ddd, eeee, ffff, gggg, hhhh, iiii from tttt, ttt1, ttt3 where aaaa = bbbb and bbbb = cccc and dddd+1 = eeee group by fff, gggg having hhhh = iiii and iiii = jjjj order by kkkk, llll limit 3, 4"
)

func BenchmarkParse1(b *testing.B) {
	sql := sql1
	for i := 0; i < b.N; i++ {
		ast, err := Parse(sql)
		if err != nil {
			b.Fatal(err)
		}
		_ = String(ast)
	}
}

func BenchmarkParse2(b *testing.B) {
	sql := sql2
	for i := 0; i < b.N; i++ {
		ast, err := Parse(sql)
		if err != nil {
			b.Fatal(err)
		}
		_ = String(ast)
	}
}

func BenchmarkParse2Parallel(b *testing.B) {
	sql := sql2
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ast, err := Parse(sql)
			if err != nil {
				b.Fatal(err)
			}
			_ = ast
		}
	})
}

var benchQuery string

func init() {
	// benchQuerySize is the approximate size of the query.
	benchQuerySize := 1000000

	// Size of value is 1/10 size of query. Then we add
	// 10 such values to the where clause.
	var baseval bytes.Buffer
	for i := 0; i < benchQuerySize/100; i++ {
		// Add an escape character: This will force the upcoming
		// tokenizer improvement to still create a copy of the string.
		// Then we can see if avoiding the copy will be worth it.
		baseval.WriteString("\\'123456789")
	}

	var buf bytes.Buffer
	buf.WriteString("select a from t1 where v = 1")
	for i := 0; i < 10; i++ {
		fmt.Fprintf(&buf, " and v%d = \"%d%s\"", i, i, baseval.String())
	}
	benchQuery = buf.String()
}

func BenchmarkParse3(b *testing.B) {
	for i := 0; i < b.N; i++ {
		if _, err := Parse(benchQuery); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkParseBigQuery(b *testing.B) {
	b.ReportAllocs()
	quer := "select 'hyena' like case when case when (false or false) is true then not (false or false) when (true xor true) and -334 not in (-396, -345, -9, -380, 569, -171) then 'bluebird' like 'pony' is false when 672 in (-152, -271, 947 * 409) then -595 when case when false then true when false then false else 269 end between case -169 when -612 then 'cicada' when true then 'chamois' when false then 188 when true then 171 when true then true end and case 399 when 'camel' then 670 when true then false when 66 then false when 'buzzard' then false else -757 end then 'reptile' when -695 < 458 is not true then case 'stud' when case 29 when 'dolphin' then 'fish' when true then false else 923 end then not false when 'hagfish' = 'platypus' then true is not false when -426 < -322 then 'buck' when 'fly' like 'bulldog' then 522 % -631 when -217 then case when true then true when false then false else 'grub' end when case when false then 395 when true then true when false then true when true then true when true then 'serval' when true then false end then -386 <= 673 end when true and false and 227 in (206, -137, -91, -217, -589) then -476 - 817 not between 657 >> 182 and 158 else 'toad' end = 'sloth' then 'magpie' <=> case 'sturgeon' when 'slug' = case when false then true when false then 'anemone' when true then false when true then true when true then 'trout' when false then false else 'raven' end then not false xor not false when case when not true then case when false then true when true then 'flea' when false then 'foxhound' else 'primate' end when 'toucan' <= 'macaw' then 11 >= -311 when false xor false then case 'burro' when true then true when 'tomcat' then -457 when 'toad' then true when 'bull' then false when true then 'doberman' when false then false else 'bat' end when 'gator' <= 'donkey' then true or true when false xor false then case 'drake' when false then false when false then true when false then 521 when true then 62 when true then 'halibut' when true then false else 'quagga' end when true xor false then -46 else case when true then false when true then -88 when true then 184 when false then true end end then case when true then 'magpie' when true then false end / (-48 / 475) when -432 <= 118 is not false then 57 >= 77 xor 193 != -316 end when case 75 when not true is not false then -892 not in (47, 29, -436, -180) or -663 >= 371 when case 449 when false then -832 when false then true when true then false when true then true else 246 end in (608 ^ 125, 616, 414, case -72 when 'opossum' then 'slug' when true then -399 else 302 end, -411 * 448) then (true or false) xor 211 in (-362, -662, -198, -971, 121, -105) when -292 in (220, -589) is not true then case 'elephant' when false then false when 754 then 'pegasus' else 'heron' end like case 'moray' when false then -696 when true then -64 else 'imp' end else 259 div case 619 when true then 'mole' when 'tarpon' then true else 47 end end in (-372, case -191 when true then true when true then false when false then false when false then 638 when 549 then true when true then 96 end % (352 | 555) << case when true is not false then 'lamb' when true or true then case when true then true when false then false when false then false when false then -453 when true then false when false then false end when 644 between 534 and -503 then 609 in (-633, -412) when 341 between 131 and 552 then case -136 when 'oarfish' then true when 'stud' then true else -239 end when true or false then 'sole' = 'starfish' end, case when true xor true then case 456 when false then false when true then 209 when 'chigger' then 'snake' end when -850 between -196 and -140 then 'moccasin' like 'vulture' else case when false then false when true then true when false then 'insect' else 176 end end << (-36 * 257 & case -213 when true then false when false then false when false then -368 when -342 then 'starfish' end), case when 'griffon' like 'tomcat' then 'herring' >= 'deer' when false and false then case when false then true when false then false when false then false when false then false else 'bobcat' end when -22 != -750 then 715 else 855 end & 268 * 728) then case when case case when false then true when false then true end when case 'fly' when true then true when false then true end then false is not false when 'jaguar' then false or true when case 'feline' when 151 then 'possum' when true then true when false then false end then case 247 when false then true when false then false when true then -299 when true then 213 else 599 end end >= case 'salmon' when 'turkey' < 'orca' then 233 between -461 and 90 when not true then false or true else 'chigger' end then 'mustang' when not 254 between -323 and -319 or 'boar' like 'wolf' xor true and true then 'antelope' else 'hen' end when case case case when false then 682 when true then false when false then 'silkworm' when true then true when true then false when true then true else 12 end when false xor true then 135 when false is true then false is null else -54 end when case -109 when 'vulture' then 40 when false then true when 'grouper' then true when -126 then false when true then -138 else 29 end in (-5, 631, -170, case when false then false when true then -306 when false then -272 else 79 end, -613, -197 / -294) then case -464 when false then true when true then true when false then -241 end not between case -400 when false then 411 when -72 then true when 'hookworm' then 114 end and 424 * 105 when 586 < 195 then case when 'polliwog' < 'beetle' then case 'reindeer' when true then false when 219 then 'crane' when 17 then 'minnow' when 'monkey' then false when 'alpaca' then 'toucan' end when false is not false then -66 between 210 and 78 end else -533 end in (418, case when 603 between -284 and -389 then not (false or false) when -326 > 282 div 182 then case -166 when 375 then 'sparrow' when true then true when true then false when 'monkfish' then 516 when false then 'shrimp' when false then false end != -85 & 369 else case when false or true then 'sailfish' > 'roughy' when true is true then false xor true when 485 not in (-256, -25) then -138 in (-878, 399, 71, 452, -362, -243) when 'sunbird' like 'wolf' then true or false when -263 between 174 and 390 then 'salmon' when not true then true xor false else -295 end end, case when (false or true) xor false and true then 221 > 734 * -131 when true and false and -550 between 428 and -853 then not not false end) then not case -98 when true then false when true then true end not in (303 / -334, case -36 when true then true when 'maggot' then true when false then false when false then false end) xor (not true xor 'skunk' <= 'bluejay' or 237 not in (case 656 when true then false when true then 'goldfish' when false then 686 when 3 then false when true then 'clam' else 47 end, -376, -580, 196 div 48, case when false then true when false then false end, 14)) when -10 << (161 & 51) between -646 and case -295 % -740 when false or true then false and false when 'adder' then true or false when not true then 'mudfish' when 36 << -771 then 'lark' when 33 then case when true then true when false then 287 else 88 end end and case 675 << -42 when 'firefly' like 'sole' then false xor true when 'dove' then 324 when -222 not in (673, -426, 574, -501, 783) then -284 when true and true then 'flounder' when -660 << -141 then 3 & 410 end not between (58 & -354) ^ (816 + -425) and 345 then not 190 not between -763 and case when false and false then false and true when not true then 'cow' like 'horse' when 375 >= -546 then 'labrador' like 'lizard' when true or false then 'sloth' <= 'malamute' end when -223 not between case 532 + -293 when 'mole' then true and false when 'molly' then 384 else case 447 when 'bluebird' then true when true then true else 898 end end and case -185 when false xor true then 'yeti' > 'shad' when 'buzzard' then 'calf' >= 'goose' when 'weevil' then false is true when 'worm' then true is null when 'bluebird' <= 'anemone' then false xor false else -225 end xor not case when false then 679 when true then true when false then 'polliwog' else 'satyr' end like case when true then true when true then 'albacore' else 'raccoon' end then ((true or true) xor (true xor false)) and case when false then false when true then true when true then true when true then 284 when true then 55 when false then false else 109 end <= -55 % 39 xor ('hermit' != case 'sparrow' when 86 then false when false then 'cardinal' when true then true end xor 'llama' like 'marmot' is false) when (-695 ^ -295 in (-417, case when false then false when false then true when true then true when false then false when true then false end, 0, 755 div -544) or case 806 when false then 112 when false then false when false then 318 when true then -169 end between case -73 when false then false when -438 then false when 563 then false when 14 then 'pigeon' when true then true end and case -174 when 'penguin' then true when true then -175 when false then true when true then true else -315 end) is true then 'sheep' else case case case when 221 in (274, 9, 72, 194, -104, -370) then case 14 when -283 then true when -369 then 331 when 217 then false when 'drum' then true when false then true when true then true end when -331 not in (62, 354, 118) then 716 not between -221 and 201 when -183 between 121 and 377 then 480 when 404 in (245, 109, -197, 86, -62) then false is not false else case when false then 'seasnail' when false then -603 when false then true when false then 'salmon' end end when 'elf' > 'tarpon' is not true then -514 when -537 between 326 and 42 and (false xor true) then (false xor false) and -233 not between 176 and -97 when case 'fly' when -268 ^ 876 then 224 != 305 when -574 not between -104 and 389 then case when false then true when true then true when true then false end when -732 ^ 0 then case 'skylark' when 'ram' then true when false then 69 when true then 'goldfish' when -547 then false else 'anteater' end when false xor true then 'dolphin' when -470 != 668 then not false else case when false then false when true then false when true then true when false then false end end then 'louse' = 'pipefish' is false end when 'turkey' then case 'swift' when case case when true then false when true then -117 when false then 78 when false then false when false then false when true then true else 'porpoise' end when false xor true then true xor false when -144 >> -117 then 618 in (636, 278) when -237 >= 333 then 'swine' when -82 not between 434 and -201 then false or true when 'sunbird' >= 'gull' then 101 in (-487, 104, -58, -585, -294, 250) else 'gator' end then case when true xor true then 36 not between -300 and 48 when false and false then case when false then true when false then true when false then false end when not true then -466 in (46, 764) when 'eagle' like 'rhino' then false xor true when 'tetra' like 'cricket' then 'wombat' like 'troll' when false is not null then -181 ^ 1 else case 'bulldog' when true then -70 when -36 then false when -155 then true end end when not true xor (false xor true) then 593 not between 96 and 128 or 'gator' like 'ant' when 35 + 643 <=> -538 - -116 then 'stinkbug' when (true xor false) and 'terrier' like 'gecko' then 326 & 317 <=> case when true then true when false then true when true then true when false then false when true then false when false then true else -409 end when -233 ^ -353 not in (-63 % -740, case 34 when 148 then true when true then 661 end, case -345 when false then false when 'lobster' then true else -366 end, -101, 123 << 369) then case -742 when 'cow' then 'lionfish' when 'boar' then true else 449 end < case 722 when -664 then false when true then true else -731 end else 'caribou' end when -308 then 459 in (-230, case when true then true when false then false when true then -515 end & -242, 320) when case 90 when 'chow' then 'bengal' when true then false when true then true when -368 then -871 end * 243 in (case when false and true then 'walleye' when true and true then 'snake' like 'chamois' end, case 4 & -72 when 'alien' like 'swine' then 'bat' when true is not true then 'walrus' else -418 ^ 853 end) then case when -151 not between 616 and -103 then case when true then false when true then false when false then false else 'stinkbug' end when 'cod' like 'haddock' then false or true when 'rooster' like 'crawdad' then -17 = 188 when true or false then false is not true end >= 'lark' when (309 not between 182 and 320 xor (true xor false)) and 567 not in (case 11 when 401 then false when true then true when true then false end, case when true then true when false then true end, case when true then false when false then -34 when false then true when false then 321 else -774 end, case when true then true when false then false else 393 end, 409) then -142 % case when true then true when false then true when true then 677 when false then false when false then true end = case when 11 <=> -368 then 76 between -775 and -369 when 309 = -459 then 'snipe' <= 'tetra' end else case case when 355 between 347 and 92 then -635 when 288 not in (727, 412, -241, -195) then -102 << 603 else case when false then false when false then false when true then -715 when true then false when false then true else 'frog' end end when -166 > 204 div -879 then case when false is false then 11 << 128 when true or true then 'sparrow' like 'toucan' when false and false then case when true then 360 when true then true end when -419 <= -387 then not false end when not 'fish' like 'bear' then -605 not between 25 | 475 and -312 / 440 when not true xor not false then case when true then 'goblin' when true then false else -334 end between 856 and 676 end end end xor case case case when 184 + -37 not in (case -34 when false then true when 294 then 'kid' when true then 'oyster' when 228 then 'rattler' when 'kid' then -413 else -121 end, -568, case 699 when false then 'rooster' when true then false when false then 319 else -329 end, 22, 452 div -430) then -144 <=> -334 and 347 != -413 when (not true) is true then 68 not between 163 and -379 xor false and false end when case case when false then true when true then true when true then false when false then -503 else 'kit' end when 418 between -621 and 92 then 'dane' < 'prawn' when case -678 when true then false when false then 'reindeer' else -375 end then -525 between -9 and 516 when not true then false and false when 'turtle' < 'bird' then true and false end like 'weasel' then case 'buffalo' when false is true and 'hippo' != 'bass' then case 'mammoth' when true then 'iguana' when 'ant' then true when false then -23 when true then false when true then 'cockatoo' when 900 then true end like 'skink' when case 266 when 'tiger' then true when 384 then false when -272 then 'starfish' when true then 25 when true then true when 'boxer' then true end | case when true then false when true then true when false then false else 24 end then (-49 | 821) + 37 / 0 when 'spaniel' then case when false then 'buck' when true then true when false then false else 57 end % case when false then 'tomcat' when false then -66 when false then 'scorpion' when true then 'boxer' when false then true else 383 end else 'hawk' end when case when -17 > case 77 when -83 then true when false then 186 end then -95 between -464 and -5 xor true and false when case 'rattler' when true then false when 'quagga' then true when false then false else 'oyster' end != case when false then 944 when false then true when true then true when true then false when true then false when false then 'jawfish' end then 426 in (-368 + -164, 253, 257 & 498, case -56 when -617 then false when -658 then true end) else case case 646 when true then true when 313 then 9 end when 373 in (552, -317) then -134 not in (209, -128, 398, 726, 669, -320) when not true then 'whale' <=> 'mammal' when 40 not in (156, 482) then -314 != 53 when 836 >= -317 then -3 not between -83 and -450 when -249 >> -347 then true or false else case when true then false when true then 296 when true then 'cardinal' else 721 end end end then case 'boxer' when case when true then -306 when true then true when false then false when true then false else 572 end not between 51 * 192 and -77 div -589 then 269 in (-107, -546, 332, -467, -378, -501) is not false when false xor false or false and false then (not true) is false when (true is false) is not null then 'possum' when 'tapir' then not (false or false) end when -112 = 538 then 'parrot' <=> 'titmouse' when 'mastodon' then not true and (false and false) or 263 not in (479, -122 * 436, 335 - 512, -311 / 405, -343 + 745, -602 ^ -171) when not 'woodcock' <= case 'ocelot' when 'buzzard' then false when 'warthog' then -17 when true then true when 'pika' then true when true then true else 'lioness' end then -550 when (not true) is not false or 'aardvark' like 'sponge' then 778 else case case when 'imp' like 'panda' then case when true then 'gecko' when false then true when false then true when false then 'snake' when true then false when false then true else 'stingray' end when not false then -636 when true or true then case when true then true when false then true end else 'raven' end when -339 - 101 < 38 * 219 then 886 when 'tetra' like 'bear' xor 516 not in (129, -316, 493, -312, 173, -88) then not 'caiman' like 'chimp' when case -839 when true then 'gannet' when false then 'whippet' when false then false when false then false when false then false when true then 'turkey' else 303 end in (5, 663, 179 - 113) then not 'firefly' like 'tiger' when not -358 not in (20, 188, 84) then (false and true) is not true when (true or false) is not null then true and true xor (false xor false) else case case 'gnat' when false then 'cowbird' when true then false else 'chow' end when 482 then false xor false when case 'tick' when 'lobster' then 'woodcock' when 'mongoose' then false end then true or true when 256 not between 373 and -437 then case 'oriole' when true then false when false then false when true then -597 end when 46 in (-613, 337, -120, 484, 738, -195) then false xor true else 'pelican' end end end when not (154 not in (31, -165, -366, -96, 420) and (true xor true)) and case case 'starfish' when 'akita' then true when true then false when -327 then 84 end when true or false then 838 & -276 when true and true then case when false then false when false then false when true then 'viper' else 'swan' end when false or true then true and true when case 'catfish' when 'skunk' then false when 'gopher' then false when true then false when true then -102 when false then false else 'bluebird' end then -578 in (-569, -262, 22, 93, -766, 830) when not true then true is false end >= case when 'lobster' like 'snapper' then -199 != 436 when false or true then case when true then 15 when true then true else 'asp' end when -386 in (472, 130, 1, -29) then 'elf' like 'glowworm' when 'sunbeam' > 'goose' then 'leech' like 'swan' when -693 = 441 then 'hermit' like 'gazelle' when false or false then false or true end then case when 'man' like 'lemur' or -432 in (192, case -386 when false then 'impala' when 'husky' then false when true then true when true then false when false then false end, -214, 163 + -657, 90 & -539) then 656 - case when false then 'calf' when true then true else 236 end in (741, -546) when 'platypus' like 'dragon' then 'catfish' <=> 'cheetah' or -445 >> -422 <= case when true then true when true then true when true then 299 else -571 end when 15 not in (-477 % 479, 512 ^ -119, -189 % 114, 482 + -153, 533) is false then case 371 when 261 then false when 'labrador' then 'barnacle' when 'monkfish' then true when true then true when 'anchovy' then 419 else -238 end & case when true then true when true then -653 when false then true when true then 'buffalo' when false then true when false then 'panda' end between -418 and case -369 >> -614 when true and false then not true when case 'goose' when -498 then false when false then true when -222 then false else 'bedbug' end then true xor true when 'leech' like 'possum' then not false else 94 end end when not case case when false then true when false then false when false then 'jaybird' when true then true else 'shrimp' end when 'woodcock' then not true when case 538 when -281 then false when 262 then false when false then 'ewe' when false then true when 'swine' then 132 when -3 then false end then false or true when -673 between -374 and 121 then true or true when 'ewe' like 'ocelot' then -187 when 198 not in (128, -718) then case when false then true when true then -24 when false then false else 34 end when false or true then 510 not in (-506, -13, -35, -147, -483) else case 'gibbon' when false then true when 561 then false when false then -448 when true then true when false then true else 'squid' end end >= case 'vervet' when 'crayfish' <=> 'goldfish' then false and false when 'bedbug' != 'stud' then true xor false when not false then false or true when 'woodcock' like 'ghoul' then true or false when case when false then false when true then 340 when true then false when false then true when true then 456 when false then true end then 'narwhal' like 'snail' when 'lacewing' then case when false then true when false then false end else 'kit' end then case -431 when case when false then false when false then 'bug' when true then 'gopher' when false then 633 else -37 end then 'bird' != 'bedbug' when 697 in (838, 832, 181, -117) then 'cub' <= 'starfish' when 17 not in (-83, 267, 102, -68, 451) then -523 not in (-644, -15) when 250 then -720 not between -7 and 450 when case when false then false when false then true when false then 'albacore' when false then 'dove' when false then -412 end then 'foal' = 'mongrel' else case -176 when true then true when true then -655 when true then true end end in (case when false then -387 when false then false when false then true when false then true end / case when true then 'fawn' when false then 'grouper' when true then 'catfish' when false then true end, -583, 35) is true when case 600 when true then false when true then 'frog' when true then false when 'monitor' then false when true then 'rodent' end between -183 >> 572 and case -163 when true then true when false then false when false then false end or case 303 when 'badger' then 'sheepdog' when true then 'oryx' when false then false when -171 then 46 when true then false end between 389 & -7 and 93 % 451 or (case 'snapper' when true then false when -428 then 'panda' when true then true when true then false else 'heron' end like case when true then false when true then true else 'unicorn' end or case 133 when 'chimp' then false when true then true when true then true when true then 'llama' when true then true when 593 then 622 end not in (case when false then true when false then true when true then 'urchin' when true then false when false then false else 120 end, case -441 when false then false when true then true when 'ladybug' then true when true then true end)) then not ((true xor false or -185 <=> -41) and 'barnacle' like 'puma' is true) else case when 220 not in (case when true then 'calf' when false then 'tomcat' when false then 684 when false then -312 else 363 end, case when true then 176 when true then true when true then false end) or case when true then true when true then false when false then 'magpie' when true then 218 when false then true end not between case -36 when true then true when 'dory' then true when false then false when false then false end and case when false then true when false then 'skylark' when true then true end then case when false then true when true then 'penguin' when true then 'barnacle' end | -79 in (221, -147 % 163, case when true then 330 when false then false when false then false when true then false else -505 end ^ -602) when -690 != -30 then -155 <= -532 and true is true or 488 div -217 between case -66 when true then true when 113 then 'emu' else 227 end and 438 div 617 else case case when 29 not in (-634, 374, -487, 713) then not true when 34 between -398 and -288 then -364 = 322 else case when true then 'ray' when false then false when true then 83 when false then 243 when false then true when false then 'gnu' else 'grouper' end end when case when false then false when false then 'mullet' end < 'tomcat' then 480 / -451 - (-81 << -227) when 681 not between case when true then false when false then false else -410 end and -10 then case 'boxer' when 207 then true when true then false when -137 then -83 when true then false when true then false when 386 then true end like 'tahr' when case when true then false when true then false when false then false end in (case 31 when true then false when false then true when false then 182 when 'wildcat' then false when false then false when 'pony' then 'stud' else 70 end, 49 div 355, -408, 438 >> -647) then -690 not between 126 and -308 xor false and false when case 'stork' when case 'wasp' when 'kid' then false when 'kitten' then -626 when false then true when -125 then false end then 21 <=> -369 when false or false then false is not false else 'finch' end then case when true then true when true then 'kitten' when true then true when false then false else -808 end != -228 end end end like case 'ostrich' when 146 << 163 not between case -559 when true then 'emu' when 'ape' then false when 'jaybird' then true when true then true when true then 'adder' end and 346 >> -650 and 468 ^ 3 not in (-58 - -491, -1) or (-239 between -155 and 587) is not false then case 'midge' when -346 div -618 then -469 between -626 and -403 when false xor false then true and false when -712 not in (-559, -492, -236, 328, 840, -851) then case when false then true when false then true when true then false else 150 end end != 'hyena' is true when case when case when true then 'beagle' when false then true when true then 'koala' when false then true end << 698 <=> (320 - 15) div -16 then (-127 > -616 and -379 <= 149) is false when case -174 when 533 then true when -162 then 'ibex' when true then false when false then true when true then -125 end <= case when false then true when true then false else -729 end xor case 127 when false then 93 when true then true when true then true end <=> case when true then false when true then false when false then false when false then true else -553 end then -70 ^ (-202 | 327) % 181 when -255 + -227 > -603 - 421 xor (true xor false xor (true xor false)) then case case -343 when 681 then 802 when true then false when 'polliwog' then true when true then true when false then 'shark' when 157 then true else 469 end when not true then case 'weevil' when true then -220 when true then 'dingo' when false then 10 when true then false end when 'phoenix' like 'aphid' then case when false then false when false then false when false then 486 else 16 end when true and false then -50 >= -332 when false is true then 'redbird' when 'drum' then 'lamb' >= 'humpback' else case 114 when true then false when true then true when true then true when false then true end end > case -334 when false and true then false or false when 'airedale' then 'mantis' = 'leech' when 'eft' like 'turkey' then not false else -158 end end then case when case case when false then false when true then true else 784 end when 'seasnail' like 'bat' then case when true then true when true then -147 else 339 end when 116 in (-463, 292, -765, -336) then -776 not between 457 and 64 end between 171 | -417 / -126 and 701 then 115 when -165 <=> case when true then 'stud' when false then 294 end & -420 + 574 then (false or false) and -181 = -164 xor case 'treefrog' when true then true when 244 then true when false then -220 when true then true when false then false end <= 'alpaca' when 101 not between 578 and case 435 ^ -18 when false is not true then true xor true when true and false then case when false then -435 when false then true when false then -968 when true then true end when 592 between 110 and 279 then case 'tortoise' when true then -521 when true then false when false then false when true then false when true then false end when false is not true then 'cheetah' != 'urchin' when 'elk' then false or true end then case when case -591 when true then 'shad' when true then -409 when -16 then false when 'leopard' then 283 when true then 'monarch' end not between -84 | 210 and -930 then case when true then true when false then true when true then true when true then -119 when false then 506 when true then false else 'shad' end like case when false then false when true then -480 when false then true when true then 'sponge' when true then false when true then false end when 511 = 514 or true xor false then not -707 > 209 when -526 & 125 >= case 14 when 637 then false when true then false when 'snail' then 'satyr' when false then true when 433 then 'mongrel' end then 'lamb' like 'ringtail' and not false when -12 < -577 then 'molly' <= 'dove' when 25 in (case when true then true when true then true when false then false when false then false when true then true when true then true end, case 389 when true then 'heron' when -400 then false when true then 903 when true then true else 375 end, case when true then 'panda' when false then false when false then -768 end, 815 ^ 666, case when true then -793 when false then false end, -413 >> -155) then -314 when 'rhino' <= 'reindeer' and not true then true xor true or 'rabbit' like 'labrador' else case case when true then -169 when false then false end when false xor false then case when false then false when true then false when true then false when true then false when false then 'cow' when true then false else -750 end when -540 < 535 then 379 when false and true then -302 << 115 when 'mongrel' like 'jaguar' then not true else case 207 when true then true when 'giraffe' then 'hagfish' when false then 82 when false then 149 else 659 end end end when case case 'walrus' when 267 then false when false then true when true then false when true then -67 else 'wildcat' end when 178 then -242 ^ 142 when -622 not in (715, -709, -215, -627) then -329 when 306 then true and true when case when false then 'flamingo' when true then 661 when true then 'porpoise' when true then false when true then false else 'urchin' end then false is true when true xor false then case 'ibex' when 'rodent' then true when false then false when 'moth' then false else 'muskrat' end else 'ladybug' end > 'shepherd' then case when 547 - -445 between case when true then false when false then true when false then false when true then false when false then true else -799 end and -20 then case when true then false when false then false else 'quetzal' end <= 'skylark' when (false or true) xor 'cockatoo' <=> 'bedbug' then 524 >= -115 - 144 when case when false then true when false then true when false then true when true then false end not in (-363, -65, case -135 when true then false when -130 then true when 'boxer' then 'bengal' when true then 'loon' else 422 end, -34, -199 * -112) then case case 280 when -401 then true when true then false when false then 'ghost' when true then false else 653 end when not true then 'kid' when -448 in (365, -377) then true is not null end when 326 in (case -262 when false then true when false then false when true then -84 when 'piglet' then true when false then true when true then false else -110 end, -45, case when true then false when false then 84 else -386 end) then -396 & 103 between 357 and 295 when 130 >> -542 in (465, case -479 when 'stork' then false when true then 'ibex' when true then false when -283 then 'gnu' end) then (true xor false) is false end when case when true is not false then 433 != -32 when not true then -646 not between -734 and -116 when 'lacewing' != 'squid' then 119 * -51 when 'iguana' like 'oarfish' then false xor false when 536 < 236 then -231 end >= case -432 % 199 when 'civet' > 'hagfish' then true is not false when false and false then not true when not false then false and false when 444 not in (559, 154) then -626 when case when true then false when true then false when false then true when false then false when false then false else 'muskrat' end then 'gorilla' else 184 << -343 end then case 'deer' when false and false then 'pipefish' < 'redfish' when false xor true then true or true end = 'lemming' when case case when true then true when true then 'insect' else 'elf' end when true is not null then 'dolphin' like 'buffalo' when -449 <= -96 then true is true when case when false then true when false then 'kit' when false then false when false then false end then -113 when 'lamprey' then 762 in (177, 695, 176) end <= 'yak' then 'polecat' <= 'spider' or not -90 in (0, -578, -94, 369, -42) else case when 'stork' like 'starling' then not false when false xor false then 'kangaroo' when true xor false then -161 >> 107 end - -86 end when 167 div -243 then 'gecko' when case -374 when case 312 when false then true when 538 then true when true then 'salmon' when false then -67 when true then true else -912 end > -98 div -392 then false or false or (true or true) when case when false is null then true or false when 'aardvark' != 'gnu' then 'calf' like 'oriole' when -429 != -109 then 645 div 288 when true and true then 'thrush' like 'orca' when 'grouse' = 'orca' then 'stingray' > 'rat' when false xor false then true and false else 'serval' end then (-856 between -192 and -529) is not true when true and true and 'fox' like 'goat' then -178 when 'dane' = case 'shiner' when false then true when false then true end then case 'hyena' when 477 then true when true then true when true then false when 495 then 195 when false then true end like case when false then false when true then false end when case case when true then true when true then true else 'crow' end when false or true then 656 not between -141 and -304 when not false then case -333 when 'seasnail' then -144 when false then false when false then true when true then false when 'wildcat' then 'parrot' when true then false else 257 end else 'goose' end then not true xor 481 not between 370 and 27 when 160 then 'koala' else case 89 - 516 when true and false then not true when case when false then true when true then true when true then false when false then -386 when false then true when false then true end then 'parakeet' like 'mink' when not false then not true when case when false then 685 when true then false when false then false else 601 end then case 328 when 'seahorse' then 'unicorn' when true then true when true then true else 90 end when case when false then false when true then true when true then 'mole' when true then false when false then false end then case 642 when false then true when false then -244 when true then 'maggot' when false then 112 end when not false then case 'wren' when 'mackerel' then 143 when 7 then true when true then 'treefrog' when -233 then 'leopard' end end end not in (case -494 - -810 when (false and true) is not null then 'monitor' when -17 >> 214 in (case -25 when 201 then false when 'treefrog' then true when -50 then -274 when true then true when -250 then false end, 323 + 629, -313, case when true then false when true then true when true then true when true then -290 end) then case 92 when -749 then true when true then true when true then true when true then false when true then false else -217 end * -326 else case when false then true when false then false when false then 'toucan' else -502 end * -442 end, -581) then case when false then false when true then true when false then 'drake' when true then 'krill' when false then -228 when false then true else 769 end - -142 + case -310 when 'moth' then false when true then true when 'swan' then 857 when 'hermit' then false end / (-609 & 181) << (case when not false then 96 in (-48, 38, 221, 112) when not true then true and false else 223 end >> -6 ^ case when false then 'manatee' when true then false when false then false when true then true when true then false when true then -42 else 225 end) when ('martin' < 'jennet' xor not true or not false and 411 not between -4 and 166) xor 252 between case -88 when true is true then not true when -814 <=> 478 then not false when true is not null then 'grub' > 'dinosaur' when case 'beetle' when false then -179 when false then true when false then -342 when -212 then true when 608 then true when -830 then true end then -93 else -197 div -104 end and case -382 when 251 * -214 then -436 between 786 and 133 when 'bison' > 'dog' then true is not true when -376 >= -318 then true and false end then 277 when case -314 when true then true when true then 8 when 'calf' then false end | 436 | case when 'corgi' >= 'manatee' then 'rhino' > 'hippo' when 'bullfrog' >= 'skylark' then -403 >= 166 when false is not null then 421 when -192 >= 344 then 'squid' when 'dove' like 'kitten' then case when true then false when true then true when true then true end when not true then 37 >= -77 end not between (-49 >> (474 << -495)) - case case when true then 'grouper' when true then false when true then false else 45 end when false and false then not true when not true then false or false when -236 not between 354 and -315 then case when false then false when true then true when true then -243 end when 359 then false or false end and case when case when true then false when false then false when true then false when true then true when false then -100 else 166 end between -4 and 506 then -57 between 642 and case when true then 'piranha' when false then false when true then false when true then 'drum' when false then 'coyote' else 31 end when 505 * -44 not in (114, 498 << -689, 446 << 719, 235, 481, -481 + 3) then case 'jennet' when true then 'swine' when false then 'rabbit' when false then false when -604 then 'snipe' else 'grub' end like 'hornet' when 'swan' <= 'swift' and 'werewolf' like 'pony' then -153 when true is not null xor 697 not in (180, -14, 287, 519) then not false is not true else case when -903 <=> 305 then case -669 when 'dane' then true when true then true when 'husky' then true when -359 then -236 end when true is not false then true is not null when 599 in (-274, -873) then true or false when false xor false then 571 ^ 242 else -198 + -366 end end then not (true xor true) and case 'ghost' when 'robin' then 158 when false then true when true then true else 'rat' end < case when true then -335 when false then 'osprey' when false then 'mouse' when false then true end xor not -274 - 576 > -389 ^ 319 end from t where col = case ((343 | case -803 when 'mollusk' < 'porpoise' then 'shad' when -326 not between 147 and 336 then 'guppy' >= 'goshawk' when 'cowbird' then 'swift' like 'grouper' when not true then 'eel' when 'gibbon' < 'malamute' then false or true else 267 end) % -566 - -151) div 366 when case when case when true then 'tahr' when false then false when true then 310 end != -858 xor (true and true xor false and false) xor case when true then 'parakeet' when true then false when true then false when true then true when true then true when false then 'goose' else 168 end / (387 % -305) not between -374 and case 852 when true then true when true then true when false then 'cobra' when false then true else 165 end << -239 then case case case when true then false when false then true when false then false when true then true when true then true when false then false end when true or false then case 'tadpole' when true then true when 670 then -692 when false then -896 when 'raccoon' then false when 918 then -699 else 'magpie' end when -496 then true and true when -221 not in (595, -61, -485, -638, -284) then 'drake' <=> 'slug' when 'squid' like 'cougar' then 'horse' <=> 'quail' when case when true then false when true then false when true then false when true then false when true then true end then true or true when 212 - 232 then not false end when false xor false xor 91 between 417 and 377 then 'mudfish' = 'snapper' and -100 > 268 when not (true and false) then 'reindeer' when 228 % 157 not in (-848, case when true then -8 when false then true when false then false when false then -641 end, 361 + -383) then 15 not between -243 and case when false then 'pig' when true then false when false then false else 212 end when 492 not between -766 and -34 or (true or false) then 403 + -95 in (-620 | 342, -571 >> 20) end < 516 >> case when false and true then false or false when 'viper' != 'mongoose' then true or false when not false then -148 not in (845, 148, -169, -48, 92) when true and false then -367 not in (84, -156, -237, 742, 777, -275) when false and true then false is not false when 148 between -281 and 619 then 'firefly' like 'muskox' end when case case when 638 >= 440 then -456 when -149 between 4 and 71 then case when true then true when false then 'humpback' else 397 end when 'oriole' != 'asp' then true xor false when 'longhorn' like 'pup' then true is false end when case when -416 in (-69, -102, -268) then 'seasnail' != 'crab' when false or true then 231 in (776, -416) when 51 between 104 and 944 then -157 when true or true then 'tortoise' like 'mantis' end then 36 & -659 between case when true then 'teal' when false then true when true then false when true then true when false then 'falcon' else 224 end and case -435 when true then false when true then false else -535 end when case when true then true when true then false when true then false when true then true when false then false else -120 end <=> 255 << -270 then 'mollusk' when -40 then (false or true) is not true when 98 not in (case when true then false when false then false when true then true when true then false else 608 end, case -75 when true then false when false then true when true then true when true then -593 end, 84 << -110, case -627 when false then false when false then 'stork' end, -261 + 288) then -626 when not 'zebra' like 'sparrow' then 322 not in (-173, -809 * -46, -389, -103, 331 - -585, -116) when -358 + 714 then true xor true or true and true end like 'reindeer' then case -147 div case -392 when 'sunbeam' then false when false then 'lizard' when 148 then false end when not true is not true then 'lobster' != 'sparrow' and (true and false) when case when false then false when true then true when true then true when true then -325 when false then false else -483 end <=> 676 then 348 / -627 - 39 end <= -380 when case case when 'skylark' like 'yeti' then not false when false xor true then not false when 167 not between -559 and 275 then 1 > -202 when 205 <=> -156 then true xor true when 'rat' like 'bobcat' then case 563 when false then false when true then 'beagle' when false then true when 'sheep' then 123 else 113 end else case when true then 'chimp' when true then 889 else 101 end end when case 466 / 623 when case when true then false when true then true when false then 686 when true then true when false then false when false then 'sparrow' else 167 end then 'stallion' = 'jennet' when true xor false then true xor false when 674 & -509 then not true when not false then true or false when 'guinea' >= 'alpaca' then 96 >> 93 when -520 <=> 889 then case 'sturgeon' when 'troll' then 'bug' when true then false end end then case -293 when true then true when true then true else 300 end between case when false then false when true then false when true then -294 when true then true when true then false when true then false else 177 end and 813 when case 'phoenix' when 'wren' then false when false then false when true then true else 'snipe' end like case 'manatee' when 118 then false when false then true when false then 'seasnail' when 'coral' then false when true then false else 'python' end then 283 - 66 not between case when false then false when false then true when true then false when true then false when true then true end and case when true then 'termite' when true then true when false then true else -131 end else case when true xor false then -257 & -479 when -584 not in (-499, -47, 141) then 601 <= 64 end end not in (-303, case case when true then 'guinea' when false then true when false then true when false then true when false then 'ringtail' when true then true else 374 end | -65 when case when false then false when false then true when true then false when false then 'stinkbug' else -758 end not in (417 * 161, case when true then 'kite' when false then 'pup' when true then -285 end, case 365 when false then true when false then true when 'muskox' then true else -441 end, case when true then false when false then false end, 365, -856) then case case when true then true when true then 'hyena' end when case -247 when 'monitor' then 'collie' when true then false when false then 680 when 'boa' then false when false then true end then 'ladybug' when 'gnu' like 'dogfish' then case when false then true when true then true when true then false end when 'swan' like 'terrier' then case when false then 'rooster' when false then true when false then false end else 'katydid' end when case case 'opossum' when true then 147 when true then -407 else 'barnacle' end when -2 + -426 then false or true when not true then case 'cattle' when 'bison' then true when true then false when true then false when -653 then false when 'mongrel' then -910 when true then false end else 'pipefish' end then case 'emu' when 411 = -388 then false or false when true xor false then true or false when case when false then true when true then -322 else -732 end then -408 != 879 when -929 <=> -627 then case 'turtle' when false then false when -337 then false when -739 then 'mackerel' when true then false when false then false when true then true else 'ocelot' end when -641 not between -324 and -526 then case -625 when 356 then true when true then true when false then 651 when -71 then 37 else 316 end end when false and false xor false is true then 161 between case -335 when false then false when false then 'platypus' when 'walrus' then false when true then false else -353 end and -5 + 87 when case -159 when true then 'phoenix' when 'reindeer' then true when true then false else 183 end != case 77 when -117 then 'civet' when false then true when false then 'lioness' end then 152 div -278 not in (case 337 when 'cod' then 'sole' when false then 'piranha' when true then -599 when false then true when 'cat' then -289 else 148 end, 992 & 141) when not 449 between 69 and -75 then 'moth' when case 'lemur' when false then false when false then 'flea' when 'wolf' then false when 'jackass' then false else 'kit' end <=> 'escargot' then not -434 not between -376 and -11 end, -478) then 123 div (83 % 502) not between 633 / -675 + (605 + -62) and case when false then false when true then false end ^ (952 + 141) or (-375 not in (-219, 171) xor (false xor false)) is true when case when not 298 <= -121 then not (false or true) when -394 | -248 not between case when false then true when true then true end and case 352 when false then -299 when -415 then false when -43 then true when false then true else 446 end then -261 when case when true then 'filly' when false then true when true then -764 when false then true when true then -157 when true then -576 else 525 end in (case when true then true when false then -361 when true then -34 when true then true when false then true end, case when true then false when true then true when false then false when true then 'arachnid' when false then false when true then false else -214 end, 104) then case when -67 <= -576 then true xor false when true or true then false is null else 'pig' end end >= -56 then case when case 387 when case -404 when 82 then 52 when true then -521 when -66 then 'aphid' when true then -405 end then true or false when -267 <= 476 then not false when case -325 when true then true when 284 then false when true then true when false then true when -573 then 'squirrel' else -254 end then true xor false when true or true then case when true then 'lacewing' when false then false when true then 'lynx' when true then false when true then false when false then false end when case when false then 'cub' when true then false when false then true else 'warthog' end then false and true when 'wolf' then 'racer' else 94 - -456 end not between case case when false then false when false then true when true then false when true then true when true then true end when 'mutt' < 'pegasus' then 'burro' when 110 in (-356, 182) then 'molly' when 'elf' < 'drake' then 'dogfish' > 'akita' end and case -591 when false is null then -579 < 663 when -84 <=> 223 then 325 when 'crane' <=> 'monkfish' then 'gannet' >= 'crab' when true is true then -97 != -133 when 363 then false xor false when true and true then case 'seal' when true then true when true then true when -147 then true when 16 then false else 'guppy' end end then (445 != -409 or not false) xor (-316 not between 80 and 732 xor not false) when case 'pony' when false then false when 796 then false when true then false when 'titmouse' then false when true then false else 'mayfly' end != 'coyote' or (false xor false) and 273 < 185 then case (212 << 191) + -743 % -52 when false is false xor false and false then 'starfish' when case when 443 != 69 then false is not false when false xor false then -838 >= 129 when 'bat' like 'eel' then not true when -544 < -519 then false xor true when not true then case when true then false when true then false when true then false when true then 'starfish' when false then false end else case 'spaniel' when false then 'herring' when 'buzzard' then true when 'foxhound' then false when false then -177 when true then false when 'hermit' then true else 'marlin' end end then 103 between -14 and case -52 when true then false when false then 437 when true then true when true then false when true then true when true then true else -128 end end when ('ram' like 'hare' and -192 > 787) is not null then 'lamb' like 'osprey' when -945 not between 183 and 894 and case when false then false when true then false when false then true end like 'baboon' then 'caribou' when 678 > -456 xor 'warthog' like 'sawfish' xor (296 <=> -363 or true is null) then 'beagle' else 'sheepdog' end when (not (334 <=> 97 xor true is false)) is not null then case when 'fowl' > 'tuna' and 'ewe' != 'condor' then -75 when not false and 41 <= -19 then case 'sailfish' when true then 'sole' when 'crow' then false end = 'aphid' when 'kit' > 'kangaroo' is true then -136 % -1 + (699 << -158) when (true xor false) and 720 between -622 and -431 then not (false xor false) when case when true then 704 when true then true when false then 'shrimp' end not between 220 * 184 and 375 >> 465 then not -231 <=> 736 end < 'grackle' else case case when not true then true xor true when not true then 'egret' when true or true then true xor true when 'guinea' like 'earwig' then 'poodle' like 'lark' else -452 / 584 end / -192 when 'cattle' then case when 'falcon' <= 'skink' then -634 when not false then 'spider' >= 'jawfish' when 154 in (147, -97) then -212 not between 160 and 195 when -237 between -223 and -219 then true or true when 'goshawk' like 'molly' then case when false then false when false then true when false then false else 'honeybee' end when 'quagga' like 'sponge' then false and false end >= 283 when 'kodiak' > case 'platypus' when false then true when 'baboon' then 'tarpon' else 'jaguar' end and not -492 in (-614, -475, 61) then case when true then false when false then false when true then 'lizard' else -448 end * case when true then true when false then -292 when false then -170 when true then true when false then false when true then true end <= case 91 << -42 when -429 != 143 then -208 in (248, -63) when -327 < 930 then 'badger' when true is not null then -530 in (257, -671, 125, -92, 321, 32) when 260 then 'coral' <= 'wolf' end when case when 121 + 136 in (case when false then -26 when true then true when false then false else -179 end, case when false then true when true then false when false then false when false then true when true then true when true then -145 end) then 'beagle' when case when false then false when false then 'albacore' when true then 'starfish' when true then true when true then true when false then true else -132 end between 655 and -40 & 163 then 'bluebird' < 'ladybird' xor true is null when -600 % -359 not between case when true then true when false then true when false then false when true then true when false then true when false then false else 264 end and -395 * -3 then 135 else 'chigger' end then case 'magpie' when case 288 when false then 'aardvark' when true then false when false then false when false then true when true then false when false then false end >= 299 then case when -397 not between -611 and 385 then true xor false when true or false then case when false then 'anteater' when false then true when false then true when true then 'pigeon' when false then 'hare' when true then false else 574 end when 'kingfish' like 'wasp' then case when false then false when true then true when false then false when true then false when false then false else 'dinosaur' end when 190 between 248 and -545 then true or true when 283 <=> 879 then -271 | 105 else 845 | -302 end when 'crow' then 104 << 291 = 202 when case 'hermit' when false then false when -302 then true when false then true when false then true when true then false when true then false end like 'heron' then true xor true xor not true else 'ape' end when 'labrador' then 'macaw' > case when -4 between 138 and 46 then not false when -112 > -428 then case when false then true when true then true when true then 'treefrog' when false then true when false then false when false then false end when 677 = -33 then 'yak' else case when true then 'stinkbug' when true then true end end else case when false xor false then 'bluebird' <= 'raccoon' when -269 not in (-88, -406, -183) then true xor true when 'falcon' >= 'hookworm' then not true when 'leopard' like 'mustang' then false is null when not false then -215 not between 705 and -382 else -101 & 300 end << 593 div 42 + -724 div -163 end end between 5 and 285 div case when not false or true xor true then 'panda' < 'lamb' when 'antelope' like 'elk' then 'molly' like case when true then false when false then true when false then true when true then false when true then -222 else 'reindeer' end when not true is not false then 689 <=> -133 and true is not false when case -412 when false then false when false then 81 else -453 end not between 780 % 65 and 751 then case when true then true when true then true else -439 end <=> -236 >> 105 when case -515 when true then false when true then true when -103 then true when true then false when 891 then 'herring' end between -248 ^ -71 and 120 then 'mastodon' else 147 end ^ case 3 ^ -62 / 882 when (false and false) is not null then case when true then 'mollusk' when true then true when false then 'mole' else 26 end not between 81 and case 765 when 353 then false when false then true when -25 then false when 'titmouse' then true when false then 'antelope' when false then false else 80 end when case 'sunbeam' when false then true when false then 'jaybird' when true then -410 when true then true end != 'seal' then case case when true then false when true then false when false then 'flamingo' when false then true when true then true else 'leech' end when false is false then -330 << 715 when case when true then true when false then false when false then true when true then 36 when false then true else 'lemming' end then 'mako' else 'goshawk' end when (false xor false) is null then 324 in (case -575 when 'calf' then true when false then false when 'monitor' then false when true then true when false then true when false then 219 end, case 249 when true then true when true then false else 200 end) when (184 between 272 and 242) is true then 2 >> 14 < case -149 when false then false when false then 'chicken' end else case when -735 between -772 and 200 then true and false when false is not false then true or false when false and false then 410 not in (859, -346, -63, -289, 179, -453) else -45 + -196 end end then case when -85 in (-792, case -60 >> -221 when case 'martin' when 'bunny' then true when false then true when 'bullfrog' then false when false then 'wallaby' when true then 'foal' end then 'tiger' >= 'shrew' when 228 in (-572, 182) then 338 in (83, 25, 593, 467, -330, -3) when 'starfish' like 'grackle' then 'flea' else case -253 when false then 662 when 'cardinal' then 'kodiak' when true then false else -36 end end >> -224, case -55 when case 'yak' when -561 + -4 then 'parakeet' when not false then 'raven' when -511 between 188 and -526 then case 'bear' when true then false when 'termite' then true when -609 then true when 'ibex' then true end when 181 != -349 then case when false then false when true then true when false then true when false then 'ocelot' when false then true when true then false end when case when true then 'shrew' when false then true when true then true when true then false when true then true end then 'tarpon' else case 'swan' when -137 then false when false then true end end then case when not false then true and false when 'moose' like 'gelding' then 'imp' when 'pika' != 'hookworm' then true or false end when 416 between 274 and 215 * 407 then -571 when case when false is null then case when true then 192 when false then false else 'monkey' end when true and true then case when true then -44 when true then false when true then false end when 69 not in (-348, 339, -32, -283) then false xor true when 'newt' like 'macaque' then 235 >= 290 when 'pegasus' >= 'monster' then true xor true when not true then 743 between -576 and 353 else case 'whale' when false then true when true then false when false then true when 'boar' then false when true then false when false then true else 'fish' end end then -104 <= -225 or not false when case -347 / 103 when 'hookworm' <= 'anemone' then not true when 10 not in (462, -113, 70, 115) then false or false when 540 >> 237 then 'eagle' when 345 != 63 then 'bass' > 'javelin' when 'monitor' like 'unicorn' then -377 not in (48, 394, -580, 160) else -661 end then case -429 when true then -431 when true then 'eft' end between -616 and -522 >> -138 else -65 << case 107 when 705 then false when false then 'meerkat' when false then 'ox' when false then 'monitor' when -195 then 'guinea' when 'wombat' then 'shad' end end, -187) or case -155 when false xor false then not true when false and false then 'filly' = 'yeti' when true xor true then 757 when 'jaybird' >= 'coral' then 461 when 'labrador' then not true end between -799 * -447 % case 179 when true then true when true then true when true then true end and case -736 when false then true when -472 then false when false then true end % (124 << -297) and -27 not in (case -559 when false then 'javelin' when 'coyote' then true when false then false when false then true else -708 end % -463, 398) then case when not case when 366 not between 326 and 428 then true xor false when -262 in (225, -29, -709, -248) then case 'gopher' when true then -238 when 537 then 'swine' when true then false when false then false when 'gibbon' then false when 747 then 'buffalo' end when true and false then case when false then false when true then false when false then -129 when false then false when true then true end else -620 << -697 end not between case 223 % -572 when 'serval' like 'martin' then false and false when -293 >> -16 then not true end and case when false and true then -297 in (822, -387) when false xor true then false or false when true xor true then -27 between 399 and -101 when false and false then 'lark' else 145 end then case 'mudfish' when case 243 when -240 then true when -354 then false when true then true when true then false when true then true when true then true end not between 114 and 239 div 143 then 220 >> 505 << 112 when not not false then case when false then false when true then true when false then false when true then 'badger' end like 'marlin' when 9 between -318 >> 316 and 267 then 'drake' like 'mako' or 'ghost' <= 'marmot' when case 652 when false then false when true then false when false then true when false then 'snipe' when true then true when -14 then 'raptor' end <= case -574 when false then true when 'asp' then 'cowbird' when true then false when 'ox' then 56 when true then false when false then 'gecko' else -282 end then -422 / 906 between case when true then 'sunbeam' when false then false end and -119 when true xor false or -546 between -42 and 227 then case when -43 between 283 and 644 then case 'impala' when -434 then true when 'ant' then false when true then false else 'scorpion' end when 207 >= -572 then -73 between -144 and 206 when 221 between 387 and 328 then 'rhino' like 'ghost' when 372 >= -509 then true is not false when 'pony' != 'blowfish' then false or true end else 'marten' end = 'earwig' when 463 != case when false then false when true then true when true then true when true then true end is null xor (-21 < -791 and -304 <=> 394 xor case when true then 77 when true then true when true then true when true then false when false then false when true then true end != 215) then (-84 not between -381 % 459 and case when true then false when false then -204 end) is not false and case when 'pup' > 'slug' then true xor false when -776 in (-131, -493, 177, -347) then 'aardvark' else case when true then false when false then false when false then -360 when true then false when true then 'iguana' when true then false else 105 end end in (-94 div -228 - -205, 666, 153 << 17 >> 225 + -399, (781 | 532) - case 498 when 'louse' then false when 685 then -24 when false then false end) when case when 'ewe' < 'cow' or -15 between -332 and -127 then not (false xor false) when 21 not in (185, -546, -484, 38, -5) xor -298 in (41, 937, -223) then case 'sloth' when 'kodiak' then false and false when -73 / 443 then -455 not in (-161, 366) when true is not null then true or false when 'rabbit' then not false else case 'dassie' when true then 'crappie' when true then true when false then 171 when true then 14 when 'coral' then true when 'zebra' then 'hippo' else 'baboon' end end when not (false xor true) then false and true and 'yeti' = 'hamster' else case 'pika' when 'bull' >= 'toad' then 'monkey' > 'mammoth' when 'anteater' like 'sponge' then not true else case 'bear' when true then false when 209 then 'tiger' end end end > case when (true xor true) and (false xor false) then -94 not in (244, -669, 518, -299, 573) or true xor true when 552 & -129 not in (514 & 845, case 75 when true then true when false then false when false then true when false then true when false then true end, -137) then (false xor true) is not false when 'chipmunk' like case 'silkworm' when true then true when 'hermit' then true when false then true when true then false when true then false when 'mollusk' then false else 'caiman' end then -630 when case -760 when true then 'rattler' when true then false when false then true when false then 'polliwog' when true then -515 when false then 'burro' else 253 end in (181 div 316, 369 << 110, 836 << 13, -491 ^ 632, case when false then 'raven' when false then false when true then true when false then true end, 614 - 333) then case case when true then false when true then false when true then false when true then 'blowfish' end when 9 not in (-503, 48) then -324 < -292 when false is true then false and true when true is true then 219 not between 121 and -502 when 815 not in (15, 359) then 458 not in (-626, 36, 23, -2, -94, -477) end when case 617 when false then -570 when false then true when true then -228 end not between -140 and 215 then case 805 when true then 'rodent' when true then false when true then 'pegasus' when 'pika' then 258 else -789 end < -472 when -470 in (-141, -531, 517, 438) or 'osprey' <= 'grubworm' then 'bream' end then (-129 & case when true then 'wasp' when true then true when false then true when false then true when false then 'titmouse' when false then -485 end) << case when false xor false then true or false when not true then 'monarch' when true xor true then -232 not between -144 and -29 when -486 between 471 and 208 then -362 not in (72, 432, 164, -629, 537) when false and true then -259 in (297, -339, 59) else -448 ^ -706 end = case when 'pelican' <=> case 'mastodon' when false then true when -406 then true when false then 143 when 84 then false else 'troll' end then 'roughy' = 'panda' and (false xor true) when true xor true or -431 between -292 and -222 then case 778 when 'marmot' then true when -110 then true when 'weevil' then true else 272 end >= 504 when not (true xor false) then not 'calf' like 'feline' when -297 not between -185 and -321 or (true or true) then not (true or false) else 275 >> (247 >> 109) end end when 91 not in (case when false or true then 'aardvark' like 'beetle' when 'pug' like 'bream' then 22 = 602 when 269 not in (-124, 645, -197, -110, -388, 110) then -349 not in (-457, 51, -555, 428, -112) when false is null then 'crow' <= 'lynx' when 'oarfish' like 'cougar' then -230 != -419 when not false then 164 >= 422 else case 688 when false then false when false then false else 366 end end * case 746 * 2 when -860 in (-438, 45, 637) then false or false when -126 & -9 then true is not false when -207 between -212 and -830 then 'fish' like 'albacore' else 588 | 346 end div (-148 % -127 << 585), -66, case when 671 not between case when true then 142 when true then 281 when true then false when true then false when true then false end and -177 xor ('goose' != 'civet' or 531 not in (-40, 376)) then 213 - ((-377 & -160) >> case -767 when false then 136 when false then -86 when true then false end) when 533 ^ -493 ^ case when true then false when true then false else 76 end <= case when not true then case -448 when 'duckling' then true when true then false end when true or true then true xor true when 271 != -294 then true xor false when false or false then false is not false when false is false then case when true then true when true then true when false then 'sawfly' when true then true end else 783 end then -164 else -150 end, case case when 89 * 4 = -499 then 'snapper' when 1 between 328 << 832 and case when true then false when true then true else 311 end then case 'sunbird' when -278 not in (349, -214, -160, -413) then -283 << 390 when 178 then true xor true when not true then 'sailfish' when 'giraffe' then 'silkworm' when -392 <= 618 then 47 between 591 and -292 when 279 then false and false else 'mutt' end end when case when true then false when false then 'muskrat' when false then true else 26 end * case -362 when true then true when true then 'ape' when false then true when 223 then false when false then false when true then -114 else 102 end > case case when false then true when false then false end when true and true then -209 when -236 in (29, 458) then case when false then -512 when true then true when false then true else 'maggot' end when 'titmouse' then -505 else -704 | -292 end then case when true xor true then false is null when true and true then not true when false is false then 177 * 627 when 'bison' <=> 'bee' then 134 * 240 when true xor true then true and true when 446 = 253 then -412 end in (-52, -10 / -672, 798, case -162 when not false then -408 != 95 when 424 then true and true when true or false then true or true when not true then true or false end, case when true is false then case when true then false when false then false when false then false when false then false when false then false when true then true else -395 end when 'lionfish' like 'javelin' then 'pug' < 'cowbird' when 'pigeon' < 'griffon' then false or true when true is true then 62 when false or true then 324 in (-531, 317, -84, -124, 423) else case when true then false when true then false end end, 30) when not case when false then false when true then false when true then true end = 'cobra' then 'skunk' when case when not 'sponge' like 'hagfish' then 'wombat' <=> 'bluejay' xor 738 not in (488, 227, -367, -292, 362, -605) when not (false or false) then 'cod' when not (true and true) then (-46 << -120) % (-851 / 74) when (-490 not between 312 and 224) is true then not (true and true) else 'grub' end then case case case 334 when false then true when 'piranha' then false end when not true then 'fish' like 'zebra' when true xor false then -573 <= 162 when 'grizzly' like 'hyena' then 'cockatoo' = 'terrapin' when not true then 'termite' like 'hawk' when true xor true then 'imp' != 'dog' when 'roughy' like 'parakeet' then -365 else 544 end when (false xor true) is false then case when -109 not between 98 and -47 then true xor false when false or false then 'lamb' = 'cow' end when -178 div -245 = -30 then case when false then false when true then false when false then 700 when false then 'mallard' when false then 'terrapin' end between case when true then true when false then false when false then false when false then false when false then true else -441 end and 541 when (true xor true) is null then (false and true) is true else case 516 when 441 then 751 when -290 then false when false then false when 'moccasin' then false end + case 181 when false then 'lark' when true then false when false then -413 else -373 end end when -421 then case when 'malamute' >= 'flamingo' then -264 when 'goose' >= 'halibut' then 'sole' like 'longhorn' when not true then 503 when 'moose' <=> 'stag' then not false else 36 end <= case -320 when true then false when false then 'mallard' else -789 end << -564 when case 122 >> -290 when case when false then true when false then true when false then true when false then true when false then false when false then false end then case 44 when 'dane' then false when false then 'dolphin' else -792 end when true is null then false is true when false xor true then false xor false when not true then 'shark' when true is not true then 'horse' like 'seagull' else 204 | -48 end not in (case when false is not false then -427 < 89 when not false then not false when false xor true then case when false then false when true then false end when 'hookworm' like 'muskrat' then 258 end, case -78 ^ -108 when true or false then false or true when 421 > 30 then 'chicken' like 'guinea' when case when false then true when true then false when true then 'sunfish' when false then false else 'halibut' end then 'skylark' end, -197, -157 div -93 << 591, -35 * -5 % case -632 when 759 then false when false then 'anchovy' when 'bunny' then 392 when 'lamb' then false else -340 end) then -473 >= (-295 & -77) << case when false then true when false then true when false then false end when case when -448 != -290 then false xor true when true or false then false or true when 'halibut' != 'piranha' then case 'sunbird' when false then true when true then true when false then 620 when true then true else 'humpback' end when false xor true then true xor true when 'horse' >= 'snipe' then true is not true else case 'mastiff' when 'yak' then false when true then 'leopard' end end like 'vulture' then 'worm' else case -427 when 445 not in (68, case -6 when false then true when true then false when true then false else 339 end) then 222 = -149 or false and true when case 'terrier' when true then true when true then 86 end like 'liger' then not true is true when 'serval' like case when true then false when false then true when true then false when true then false when true then true end then -425 not in (-802, 636, 824, 335, -36) is true when 'cougar' like 'hare' then -412 + -314 not between case when true then -15 when false then true when true then false when true then true when false then false end and -4 div 147 else -813 end end, 152 / -314 << case case when 'oyster' like 'albacore' then case -89 when 'walleye' then 'tuna' when true then false when true then 62 when true then -47 when 'quetzal' then true end when -153 <= 225 then -270 << 396 when true and false then -479 not in (-455, 138) when false is not true then false and false when not true then true xor true end when 'bulldog' then 'beetle' when -217 in (case 38 when false then 654 when false then true when false then true when true then 'ewe' when 'marmoset' then true end, -391, -424 / -55) then 309 * 701 when not true is not true then 'thrush' else -411 ^ (65 | -162) end) then -27 when case -218 when case -19 when false then 'parrot' when 80 then true when false then true when 'foxhound' then -31 when false then -202 when 50 then 104 else -7 end then 465 <=> -54 when true is true then -53 in (-669, 157) when 'bonefish' then false or true when 393 not in (-268, -338, -71, 269, -275) then 41 between 231 and -313 end >= case case -457 when false then false when true then true when 'mammal' then 'wasp' when 'koala' then false when true then true else 84 end when false is false then case when false then false when false then false end when true is true then -418 < -221 when true xor false then case when false then true when true then 'python' end when -831 not in (154, -641) then 507 between -527 and -463 when true and true then 831 not between -152 and -57 when 178 then 'cheetah' = 'snake' else case when false then true when false then false end end is not false or case -198 when 'kid' then true when true then true when true then 'pigeon' when false then 558 when false then 'dolphin' end & 351 div 331 != case -137 & -490 when 343 then 'seahorse' like 'bird' when 508 then -369 - -260 end and ('halibut' > 'hagfish' xor (false or false or false and false)) then case when case 'crane' when 832 not in (-205, -388, 354, 561, 16, 118) then not true when case 'bluebird' when 'squid' then true when true then 394 when false then false when true then false when false then true end then case 293 when true then false when -360 then true when 'gibbon' then true when true then 'joey' when false then -136 when true then false else 116 end when 'magpie' like 'pelican' then 'silkworm' > 'crow' else case 'swift' when true then false when false then true when false then false when true then false when false then 'mammal' end end < 'dinosaur' or -37 in (case when 'crab' <= 'buck' then 897 in (176, -140, 38, 755, -204, 215) when not false then not false when -207 between -146 and 383 then false or false when false xor false then 'narwhal' like 'mite' when true xor false then false xor false when false xor true then case 'griffon' when true then 'walrus' when true then false end end, (-266 % 407) ^ -229) then (case 506 when 'tuna' then true when false then 756 when false then false when true then 'clam' end != case 415 when false then 'squirrel' when 'pug' then 318 when false then false when false then false when -19 then false when -299 then 'ewe' end or case when true then true when true then false end like 'monarch') and 'anemone' like 'pelican' when ('honeybee' < 'grizzly' xor not true or -303 <= -333 is not true) and (not 521 between -354 and -820 xor -471 in (-756 << -304, 231 / -614, case -422 when false then 'swift' when 'ringtail' then true else 286 end, 161)) then case when true then true when false then true else -15 end + (-487 - -374) between 226 + -267 ^ -46 and -298 xor case -42 ^ 5 when -383 then not false when true or false then not true when true or false then -237 between 748 and 230 when -92 != -312 then true is not true when 'macaque' then not false else -704 end between -272 and -234 div -148 div case when false then 'horse' when false then false when true then -281 when true then 73 end else 'hornet' end when case 17 when -185 < case -659 when 'humpback' < 'sunbird' then 'giraffe' >= 'crayfish' when 188 between -175 and 286 then not true when case when false then false when true then -75 when false then false when true then false end then -447 in (181, -523, -206, 357, -513) when not true then 'wildcat' >= 'kiwi' when true or true then 'mole' when 230 not in (-318, -300, 118) then 354 >= 569 else 386 end then ('macaw' like 'quail' and (true xor true)) is false when case when false is null or true is not true then (true is null) is not true when 'jackal' like 'mollusk' xor (true xor true) then case case -6 when true then 120 when false then 225 when false then true when 'cardinal' then true when true then true when -776 then 'hookworm' end when false and true then false xor true when -262 >> 600 then 'tetra' = 'osprey' when 'grizzly' then case when true then 'toucan' when false then 'polliwog' else 84 end when -301 % 509 then 257 not between 53 and 316 when 4 ^ 371 then 340 != -214 end when (true xor false) is not true then case when true then false when false then false when false then false when false then true end % (29 & 140) when 516 << 295 between case -135 when 418 then true when false then true else 110 end and 0 then (true or false) xor (false or true) when not -506 between 245 and 778 then 'lioness' like 'eft' end then 'starfish' like 'coyote' or (true xor true or 'bluegill' like 'pangolin') when case case case when false then true when true then true when false then true when true then false when true then false else -13 end when not true then -254 between 99 and -109 when true or true then false xor false end when case 47 when false then true when false then true when true then 'midge' when 'bee' then 245 else -832 end ^ 519 then -815 ^ 121 & 196 when case when false then 'narwhal' when true then -586 when false then true when false then false end >> case when true then true when false then -245 else -397 end then case 'turtle' when false then 'narwhal' when 24 then 'shad' when true then true when true then 'goose' else 'whippet' end <= 'ibex' when case 'sparrow' when 'doberman' then true when 247 then true when true then 'kite' when 81 then -295 else 'mongrel' end like case when true then true when false then true end then (true or true) is null when not (false and true) then case when true then -206 when true then 'basilisk' when false then false when true then true when true then false when true then false end = -21 % -707 when false is false and -320 > 742 then -257 not in (-24, 63, -527, 575, -72, -668) is false else case when 'bluegill' like 'dove' then true xor false when false is true then true or true else -52 & 275 end end then case 381 when case when 'mammal' like 'stud' then 'owl' like 'buzzard' when true or false then case when false then 'ferret' when false then false when true then false when true then false end when false or true then false is not false when true and true then false and true when true or false then false is not false when true xor false then 727 end then case -406 when false then false when -707 then 'dane' else -2 end >> -739 when 'ewe' = 'corgi' then 'woodcock' like 'sturgeon' is null when 802 not in (270, 369, -248) and -630 <=> -464 then case -313 when 'oriole' then false when true then -514 when false then false when false then -779 when false then 803 when 'guppy' then false end not in (case when true then true when false then 76 when false then false end, case -204 when 'fly' then false when false then false end, -676, -535) when -322 / -487 in (case when false then -19 when false then true when true then false when true then false when true then true when false then false else 228 end, case -550 when true then true when -233 then true when true then false when false then 'dory' when true then -543 when false then -409 else -199 end) then 'molly' like 'worm' xor not true when case 'tomcat' when true then 'lizard' when 'crawdad' then 'aphid' when 285 then true when true then 'cobra' when false then true when true then true end like 'cod' then not 'kite' like 'finch' when case 'ladybird' when 205 >> -190 then case 'monkey' when false then 'osprey' when true then -169 when true then false when true then false else 'swan' end when 534 not between -20 and -218 then not false when 'tarpon' then 'skink' when case 'hippo' when true then true when true then 63 when false then 'jaybird' else 'worm' end then -359 & 562 end then case case when true then -677 when false then true when false then 'doberman' when false then true when true then true when false then false end when case 'barnacle' when false then -219 when false then true when false then false when true then false when true then true when false then true end then false is not false when false xor false then -509 % -689 when true and false then false xor true when 347 != -722 then 419 when -4 in (772, 357, 400, 21, 83) then 'escargot' <= 'rat' else -636 end end when case when -853 in (199, -454, 131, -1, -36, -636) then 327 when true xor true then 'kid' when true and true then case 'moray' when true then true when false then true when false then true when -293 then 'hawk' else 'sturgeon' end when not true then 'anemone' when not false then true xor false end | -152 then 'redbird' = case when -336 = 158 then true and true when -458 between -269 and 169 then -54 & 458 else 'cardinal' end end in (case 8 when case -312 when not true is not true then 'bluegill' like 'camel' is not null when case when 551 between -622 and 414 then not true when 'lemur' >= 'terrapin' then 'gnu' != 'drum' when false xor true then 'blowfish' <= 'hedgehog' when 'phoenix' = 'oyster' then true or false when false or false then 'sheep' end then case -760 when false then 'flea' when -286 then false end < -555 when 270 % 867 not between 307 << 98 and case -5 when true then false when -217 then false when false then true else 298 end then 54 when 20 <= -732 xor not false then 'skylark' when 278 not between case -509 when false then -278 when true then true end and case 524 when true then false when 483 then false when true then false when 109 then true when false then 22 else -765 end then 'rabbit' else case case when false then true when false then true when true then 378 end when case when true then 'mantis' when false then 'basilisk' when false then false when true then true else -410 end then false or true when -608 in (-88, -178, -88, -219) then 295 in (22, -68, -909, 141, 328, -426) end end then case 'elf' when 'bluebird' <=> 'eel' then not true when case 527 when false then false when true then true end then 'killdeer' != 'hamster' when true is null then true is not true when 'snail' then case 785 when false then false when false then true when false then false end when case -283 when false then 'termite' when 'gator' then true when true then false when false then false when 26 then 'porpoise' when true then false end then case when true then true when false then 'kangaroo' else 'mastodon' end when not false then false or false end > case when 'mastiff' like 'gecko' then 298 when not false then not true when 435 not between 184 and 174 then not false when -399 != 642 then -895 end when -337 then 42 not between 509 and 185 or true and false or -64 in (347, -325) xor -173 between 416 and 673 else (case when true then 'monarch' when true then -181 else -508 end << 239) div case when not false then -207 > -271 when false or false then 401 end end, -13 % 135, case when not (true and true or 737 not between -307 and -20) then -650 when (-335 | -351) & case -219 when 906 then false when false then false when false then true when -485 then true when true then true else -161 end in (64, case case when true then -335 when false then false when true then 'scorpion' else 422 end when not false then false is not true when true and false then case when false then -71 when true then true when true then 'hound' when false then false end else case -169 when 53 then true when 'seahorse' then true end end, case 384 - -478 when 'mouse' then true or false when -176 < -42 then case 'grouse' when false then true when 'zebra' then false when true then false end when 'wombat' like 'malamute' then 2 not between 14 and 541 when -328 % 62 then 'koala' like 'perch' when 'piranha' then 'skink' end, -344, 75, 168) then -25 in (-30, case 484 / -872 when 840 not in (-371, -344, 334, 106) then case when false then true when true then true when false then 636 when true then false else 277 end when case -490 when false then 636 when true then false else 0 end then 'sunbeam' like 'seagull' when 'lacewing' then case when true then true when true then true when false then true when true then 290 else 'orca' end when -210 then 446 > -351 else -251 >> 341 end) end, case when false then false when false then 'coyote' else 284 end / (-99 >> -513) / -374 | 421, (case -48 when false then 'turtle' when false then true when true then false when false then -171 when true then false when 'deer' then false else -69 end div case -209 when true then true when -59 then true when false then 'bird' when false then -13 else 2 end & -574) div -464) then 343 / -785 < case -455 when 'buck' like 'raccoon' then case when false and false then 'bat' when 'calf' like 'griffon' then not true when 454 between 186 and -506 then case -594 when 'platypus' then false when 'kitten' then true when 'lark' then true end when not true then case when true then true when false then true when false then 'cricket' end when true and true then 'crappie' when -55 in (324, 422) then -497 != -465 end when 324 then 25 % 174 in (771, -120 - -630, 824 << 196, case when true then true when true then true end) when case when false then true when true then 166 when true then true when false then true when false then true end not between -367 * -162 and 553 div 83 then -46 when -493 >= case -259 when 102 then false when true then true when true then 'walrus' when false then false when false then true end then false or true or -727 <= 372 when 'crab' like case 'swine' when true then 'cardinal' when true then false when false then 'sturgeon' when false then 'dove' when false then false when true then -162 else 'reptile' end then (151 + 303) * case 181 when 'elk' then true when false then -611 end end is true when case when 275 ^ -153 ^ case when false then 442 when true then false when true then true when false then true when true then true end != -536 then case when not false is not false then true is null and -277 not in (-847, -116, -649, -248, 69, -744) when case when false then false when false then false when true then 524 when true then true when true then 54 end > case when false then 'rabbit' when true then false when false then -158 when false then false when false then 342 when false then false else 'colt' end then 'mutt' like case when true then 'serval' when false then false when false then 'porpoise' when false then true when false then 52 end when not 'snail' like 'stallion' then (not false) is not true else 144 & 69 div 122 end when 57 & (169 | -560) != -561 then case case 'kiwi' when case when true then true when false then -380 when true then true when true then false end then false xor false when 'alien' != 'puma' then 77 >= 250 when false is not null then case when true then -250 when true then 'tuna' else 'weevil' end when -182 between 512 and 19 then -770 <= -461 end when 'krill' like 'mastiff' xor true is false then 471 = -451 or 'coyote' != 'bird' when case 'cheetah' when false then 'drum' when false then true end <=> case when true then true when true then true when false then false else 'fox' end then 'krill' when -425 | case -90 when true then true when true then false when true then true else 196 end then 102 >> 159 between -127 and case -216 when false then 'bug' when true then false when true then true end else 'foal' end when case when 'fowl' > 'grub' then case when true then 'lark' when false then true when false then true when false then false end when 'buffalo' like 'doberman' then false xor true when not false then case when true then true when true then true end when -374 in (-250, -107, -194, -579) then not true when not true then 483 != -146 when -842 not between -50 and -52 then -756 between -186 and 574 end between case case when false then false when false then true when false then true when false then false end when 'stud' >= 'lionfish' then not false when true xor true then 523 not between 21 and 57 when 'louse' != 'silkworm' then not true when -100 <= -698 then 219 > -558 when 'seagull' like 'monarch' then case 817 when true then false when true then 'salmon' when true then false end end and case when 'ibex' like 'parakeet' then 'scorpion' when false and true then -27 != -766 when -500 in (112, -444) then -357 when 397 between -642 and 717 then 'joey' when 'badger' like 'anteater' then -590 when true and false then true and true else 315 end then (true or true) is true or not false xor true is true end <=> case -680 when 711 < case 426 >> 648 when not true then case 'shark' when -314 then -117 when -466 then false when false then false when 'polecat' then true end when 694 not in (109, -272) then -4 else case 150 when false then -358 when true then false when 'muskrat' then false when false then true when true then 'chigger' when 'buzzard' then 'oriole' end end then case when case -138 when 139 then 524 when true then 'mite' when 378 then false when false then true when 'starling' then -364 else -216 end <= case -152 when true then -51 when false then 171 end then -946 not between -1 and -199 xor (false xor false) when (true and false) is not false then case when true then false when false then false else 103 end | -80 >> -660 end when not (true or true or 'mongrel' > 'seal') then -61 & -738 != -244 & case -162 when false then true when true then false when -23 then 246 when true then true when true then true when true then true else -224 end end then not (13 * -363 | 296) / case 598 >> 478 when false xor false then 202 when 'cub' then 'mongoose' else 74 | 609 end not in (case case when false then true when false then false when true then true when true then 'reindeer' when true then 674 end div 436 when case 42 when true then true when true then 'crawdad' end not between case when false then false when false then true when true then false when false then false when false then true when true then true else -392 end and 385 then (false and false) is false when case case 'cobra' when 'buck' then false when true then true when false then false when 'caiman' then true when 'hyena' then false else 'bear' end when 173 then 745 not between 7 and 752 when true is true then case when true then true when true then true when false then 45 when true then true when true then true when true then true else 'starling' end when case 'monkfish' when 'platypus' then 'heron' when true then 'beetle' when -371 then false when false then false when true then 'dragon' when false then false else 'blowfish' end then true xor false when case when false then true when true then false end then -73 >= 218 when true or false then true xor true end then case 'teal' when false then false when true then 134 when true then false when true then true when true then true when -701 then true end like 'monkfish' when case when false then 'grouper' when true then 196 end = 'hamster' then 'oryx' when 17 >= 213 | 133 then 'tapir' = 'zebra' is false end, case when case when false then 'guinea' when false then true else 'alpaca' end < case when true then false when false then true when true then false when true then 301 else 'urchin' end then -206 * 402 not between case 132 when false then false when 'tomcat' then true when false then 'redfish' end and case 130 when -512 then 'coral' when 'tortoise' then true when 'griffon' then 774 when true then 204 when true then false when true then -1 else 625 end when case when true then 'humpback' when false then -576 when true then false when false then true else 'elf' end like 'bulldog' then 'possum' when 'sole' like 'buzzard' xor (false or true) then false or false or 506 < 775 when not 'doberman' like 'monkey' then true is false and 'kit' like 'blowfish' when case when false then false when true then -337 when true then -155 else 372 end between 380 | 528 and -142 << -40 then case 'roughy' when -310 % 316 then false or false when 36 div 663 then not true when true and true then 'caribou' <= 'ferret' when -678 != -683 then -64 else 'ladybug' end when 370 not between -752 + -607 and case when true then false when false then 247 when true then true when false then false when true then false end then not -123 > -297 end) when case case 242 when true then false when true then true when false then false when false then false when false then true else -782 end when -75 then not false when case when true then true when false then true else 513 end then false or true end * (-102 div 393 ^ -136) % case when case when true then true when false then false when false then false when true then true end in (282 / 161, case 402 when true then false when false then false end, -448) then -27 in (case when true then 'imp' when false then true else -208 end, -44 % -871, -41 & 263) when 'kingfish' != 'werewolf' or -540 <= -447 then 100 not in (-133, -210 - -321, -562 | 558, case when false then false when true then true when false then -360 when true then -243 when false then -435 end) when case when true then false when true then false when false then true end like case when true then false when false then false when true then true when false then -54 else 'slug' end then 'ibex' end not between 120 and (case -247 + -145 when 597 / -678 then 'bedbug' like 'barnacle' when 447 < -185 then 'cobra' when 161 >> 889 then 90 - 446 when case 'elk' when false then false when false then false when false then -238 end then -104 in (54, -382) when 'goat' then -26 between -276 and 61 when false and false then 776 end & 247) div case when 623 between 527 and 75 then case 'sturgeon' when -254 then 'chicken' when false then true when 'adder' then 136 when false then true else 'boar' end <= 'wren' when -499 <=> 814 xor (true or false) then 'ram' <=> case 'asp' when 'ox' then false when -420 then false end end then not not not (422 between 415 and 526 or true xor false) end when (case when case when true then true when true then 'seal' when false then true when false then false else 'bengal' end != case 'molly' when true then false when 'glider' then true when -87 then 125 when 'garfish' then -704 when false then true end then not 'heron' like 'pig' when 'condor' <= 'lamb' then -320 when true is not null or false xor false then 795 ^ -107 not between -227 * -393 and 785 >> -668 when case -145 when false then true when true then 105 when true then false when false then -676 when false then 'koi' else 611 end not in (434 - -597, -249 / -217, -317, case when true then false when false then true when true then 272 when false then true when false then true end, 491, -476 & 616) then (-473 not between 2 and -117) is true end ^ case when case when false then 718 when true then true when true then 63 when true then 'liger' when true then true when true then 'drake' else 464 end >= 329 then 166 between case -133 when false then 'lemur' when true then 154 when true then 'beagle' end and -37 when 'shark' like 'squirrel' or -230 in (314, 194, -178) then case when false then -388 when true then -870 when false then true when true then false else 'bullfrog' end >= case when true then false when false then 'doberman' when false then 12 when false then false when false then true when false then 'boar' end end not between 45 and case when 177 < case when false then true when true then false when false then false when true then true else -344 end is true then -153 in (case when false then false when false then 'barnacle' when true then 'alien' end * case when true then false when false then true when true then false end, case when false then -170 when true then true else -159 end div case 491 when true then true when true then false when false then true else -520 end) when 704 - 453 = 638 + 624 and 165 < 646 then case when true then false when true then true when true then 739 end << 457 = case case when true then true when true then 539 when false then true end when true xor false then not true when -499 then not false when 'starfish' then not false else 268 end when ((false or false) and 'osprey' < 'quagga') is not true then -779 > case when false then false when false then false when true then false else 212 end xor case when false then false when false then true when false then false when false then false when false then true when false then true end in (case when false then true when true then false end, 30, -927 % 202) when 9 ^ -184 | case 741 when true then 'buffalo' when 287 then 'wahoo' when 'monkey' then 392 when false then true else -241 end in (262 / 583 / -674 ^ 416, -190, (-279 % -4) ^ case 207 when 185 then false when 'pig' then true end) then case 'maggot' when case when false then -545 when true then true when false then true when true then false when false then 'sunbird' else -703 end <= 209 + 594 then case when true then 'spaniel' when true then -115 end like 'lobster' when 'racer' like case 'impala' when true then -1 when false then false else 'lab' end then 'jaybird' when -291 in (-173, -394) and (true or true) then case when true then true when false then 'swift' when true then false else 92 end in (179 >> 441, -188, case 130 when 207 then -343 when true then false when 'bullfrog' then false when true then true when true then false when true then 'minnow' end) when 'quetzal' then -195 + 281 between case when false then true when false then false else 295 end and case when true then 'moccasin' when false then true when false then false when false then false when true then false end end when not not 482 in (240, -28) then (-394 not between -249 and -368 xor 101 != -594) is not false when case 656 when false then 'bluejay' when false then true when 'swan' then true when true then 'pig' when true then -221 end > -350 - 25 and ('mule' <= 'zebra' or true is not null) then (false is not true or 'ladybird' like 'viper') xor 452 ^ -43 <= -361 else -491 % case when false is null then 279 + -326 when 55 not between 308 and 650 then -875 between 79 and -439 when not true then 'hare' <=> 'wombat' end end) is null then case when case when 396 > 13 or 109 in (286, 134, -169, 266) then case when false then 677 when true then true when true then 'piglet' else 142 end not between -600 and -486 << -789 when 'sponge' like 'platypus' then 'minnow' when 4 <=> 481 then 'albacore' else case case when false then true when false then 682 when true then true when false then 780 when false then true else -568 end when 'anemone' like 'gelding' then -441 % 244 when 108 in (-27, 342, 19, -19) then true xor true else -104 end end between 601 and 135 or 154 & case case -177 when 'kite' then true when 'starling' then true when -103 then 428 when false then false when false then true when true then 107 end when false and true then 91 <= -158 when 'elf' like 'crab' then 'grizzly' <=> 'bobcat' when case 'whale' when -98 then true when false then true else 'doberman' end then 'rodent' when 261 between 132 and -23 then true or false else -212 end in ((case when false then true when true then 254 when true then true end & 805) / -54, 849 / case when 'grackle' like 'satyr' then false xor false when not false then 314 <=> 465 when 216 not in (482, -612) then false is not null when false or false then true xor true when true and false then 'prawn' like 'prawn' when true xor true then 'phoenix' <= 'mongoose' end, case when case 363 when true then true when 'kingfish' then 'oriole' when false then 678 end > case 156 when 'imp' then false when true then false when false then false when 'buzzard' then 28 when false then 'doberman' end then 'narwhal' when case when true then true when true then 220 when false then false when false then 227 end in (case when false then false when false then false end, -146 * 45, 260 ^ 182, 136) then case when not false then case -442 when true then true when true then false else 141 end when true is true then 393 != 390 when true or true then 640 | 918 end end, -53, case when 187 not in (180, case when true then -42 when false then true when true then false when false then false when true then 'swine' end, -709 >> 266, 192 % 536, 16 & 590, case when false then true when true then false else -295 end) then case when true then true when false then true when false then true when true then 'pig' when true then false when false then 'joey' else 'jennet' end like case when true then false when true then true when true then 'jackal' end when -153 % -7 between -69 div -886 and case when true then true when false then 265 when true then -17 when false then true when false then -36 end then case when 707 in (-728, -239, -640, -403, -12, -457) then 'shiner' when not false then true is not false when not true then not false when true xor true then case when true then false when false then true when false then false else 'malamute' end when true is not null then false xor false end end) then (('rodent' like 'cub' is not null or case -340 when -724 then false when true then true else -51 end between case -478 when false then 'lionfish' when true then true when -348 then true when false then true else -306 end and 538) and (false is null or not true) is not false) is not null when not 'rhino' != case 'ant' when 'rooster' like 'chigger' then case when false then true when false then false end when -321 not between 80 and 489 then true and true when 908 not in (387, 139, 74, -392, 78, -112) then -207 when true is true then not true else 'beetle' end and not -653 not between 74 & 652 and case case when true then false when false then 'scorpion' when false then false end when false xor false then false and true when 'marmoset' <= 'treefrog' then true xor true when -230 in (245, -28, 8, 127, -38) then 'pup' end then case case when 97 not between -3 / case when false then true when true then false when false then false when false then 353 when false then 'tiger' when false then 'seal' else 73 end and -23 % 422 * case when true then 'monster' when true then false else -245 end then case when -231 between 439 and -545 then case 381 when false then 738 when 'swan' then true end when 'bream' like 'foxhound' then -563 else 267 end in (case when -405 not between 313 and 93 then 'feline' when not true then 199 when false is not null then -193 in (-773, -927) when -134 between -146 and -528 then true or true when 'arachnid' like 'gazelle' then case -776 when true then false when true then -146 when true then true else -483 end when true and false then 'antelope' <=> 'woodcock' end, -145 + -611 >> case 70 when 868 then true when 584 then true when true then 'goldfish' when 'slug' then true end, 448) when case 578 when true then false when 138 then false end not in (case when true then false when true then 'ox' else 462 end, 80 % -668, case 10 when false then false when true then true when true then 'duck' when true then -316 end, case when true then true when true then false when true then true when false then true when false then -173 else -247 end, case -50 when 'hookworm' then 'bedbug' when 'slug' then 295 end, -68 ^ -162) and (-231 not in (-52, -234, -609, 106) or false is not true) then case when true then false when false then 641 when false then 607 when false then true else 'donkey' end like 'redbird' xor case when false then 35 when false then true else 'minnow' end < 'kingfish' when (-578 not between 123 and -50 or 22 not between 167 and -18) and ('elf' < 'feline' or 518 between -162 and -456) then -383 in ((-375 >> -196) - 711 ^ -405, 213) when not (not true) is not true then 'moth' like case 'mosquito' when 562 between 936 and 16 then true xor false when case 'grouse' when true then false when true then 'hornet' when false then -371 when 496 then true else 'lemming' end then not false when 'collie' then false is false when false or true then 'cardinal' end end when case when -93 div -214 | 137 ^ 655 <=> -164 then case when 'badger' like 'hare' then case when true xor true then case 'terrapin' when 403 then false when false then true when false then true end when not false then 560 << 745 when true xor true then case -36 when false then false when true then true else -507 end when 'jay' like 'flamingo' then not false when true or false then 'stud' > 'piglet' when false or false then 'meerkat' end when false xor false or 'oarfish' like 'sole' then case when true xor false then 'troll' like 'mayfly' when true and false then 170 end when 'sunbird' like case 'kid' when false then true when true then 'cheetah' when true then false when false then true else 'yeti' end then -40 < -16 or (false or true) when -880 != 620 is not true then case 'yak' when false then 'chamois' when true then 'grouse' when true then 'dingo' when 'crawdad' then 750 when true then -625 end >= 'condor' when 'fish' like 'flounder' then -188 when not false and 384 = 338 then 'dodo' like 'bunny' end when 'ram' like 'pheasant' is null or -241 - -791 = 261 / -9 then case -61 when -672 then false when true then true when true then true else 279 end in (case -467 when true then -519 when false then false when true then true when -210 then -158 when false then false when 545 then true else -104 end, 186 - -352, -416 ^ -617, 99 % -771, -641 - -175, -571 | 270) xor (not true or false is not true) when 476 <=> 63 div 137 xor case when true then 'boa' when false then 435 else -144 end not between -927 % 95 and 75 >> -432 then not not true is false when 205 * 221 - (526 >> -46) not in (case 183 when 919 <= 261 then true or true when -95 | 72 then 'seahorse' like 'lion' when false is not null then case when true then false when true then 'escargot' end end, -442 >> case 54 when true then true when true then true when false then 'adder' end, 85, -287 >> -408 << -521 / 281, 174) then 'rooster' like 'rabbit' is true xor (130 <= -533 or not false) when 'mudfish' like 'airedale' xor not (false or true) then case when true or false then not true when true and false then false is true when 'humpback' like 'sloth' then -244 when false and true then 'bonefish' like 'muskrat' end < 303 end then case when false xor true or -947 != 290 then 'lacewing' when case 'sculpin' when true then true when false then false when 408 then false when 'louse' then false when false then true when false then 132 else 'tiger' end <=> 'lynx' then 'pup' like 'monkey' when (28 between 641 and 18) is true then false is false xor not false when case when false then false when false then 160 when false then 'arachnid' else 'ladybird' end like case 'werewolf' when false then true when true then false else 'slug' end then 'dolphin' <=> 'falcon' is not true when -114 not between -60 * -782 and -259 then 32 between 616 | 293 and 769 when not true or 'dane' <= 'panda' then case when -172 in (487, -38, 320, -324, 236, 180) then 2 >= -547 when 'elf' >= 'grackle' then -141 >> 174 when 'cod' like 'raccoon' then 'drum' else case 'lark' when true then true when true then -354 when true then false when false then true else 'caribou' end end end * case 84 / -474 when 'blowfish' like 'snake' or -426 not in (-126, -101) then 'poodle' when 'poodle' like case 'sole' when -365 then -17 when true then false else 'goldfish' end then case 'primate' when 646 then true when false then true when true then false end < 'yeti' when case when 'husky' like 'sawfish' then not false when 249 in (19, -12, -309, 532) then 'donkey' when 153 < 246 then -520 not between 266 and 754 end then not -389 between -54 and -146 when true and false or true xor true then 64 not in (-67 / -22, 430 << 144, -337 << 72, 250 ^ -611, case 175 when true then false when 'quail' then 'eft' else 436 end, 302 - -476) end when 44 / -517 < -892 ^ 65 then 'gull' when 'muskrat' like case when 318 - -70 in (616 | 774, -153, -557 * -391) then case when true then false when false then true when false then 'catfish' when true then true end not between -621 and -177 when 'turtle' like 'joey' and (false xor true) then 'falcon' else 'anemone' end then case when true is null then false xor false when 'basilisk' < 'calf' then not false when false and true then not false when not true then -143 in (-590, -457, -478, -564, -15) else case when false then 'yeti' when false then true when false then true when true then true when true then false end end != case when 23 <=> 77 then case 'reptile' when false then 'swan' when 'chipmunk' then true when true then 'mantis' when true then false when false then 'pug' end when not false then 'pig' when 'foal' like 'labrador' then 'humpback' >= 'cobra' when 'goshawk' >= 'cat' then 278 else case when true then false when false then true when true then true when true then 300 when true then -292 else 352 end end and 'halibut' >= case when false and true then true xor true when true xor true then 35 not between -509 and 464 when 606 = -741 then false and true when 32 not between -372 and -755 then true and false when 860 between -475 and -395 then false or false when true and true then 304 + 0 else case 'panther' when 'phoenix' then false when 503 then true else 'bear' end end when not -201 + -307 between 27 and case when 182 in (128, 79, -448, 164, -605, 443) then false is not false when 'roughy' > 'heron' then case when false then false when false then -456 when false then true when false then 'heron' when true then true when false then -331 end end then (not true or (true or true) or 43 <=> -332 - -419) and ('sheepdog' >= case when true then 21 when false then 236 end and -288 <=> case when false then 'buffalo' when true then false when true then false end) end when not (false and false) or case when true then true when false then true when true then true when true then 188 when true then true else 640 end <=> -173 or ('pony' like 'pug' is not null or case 72 when true then true when false then true when false then false when 'drake' then 'snapper' when 8 then true else 544 end between case when true then 281 when false then 'swine' when false then false end and case when true then false when false then 568 end) or case when 227 in (-50, 101, -860) then case 171 when true then 'turkey' when true then true when true then false when 'baboon' then 'marten' when false then false when 526 then false end when false is false then 'eel' when -48 in (-491, 196, -642, 483) then not false when true or false then 672 >> -479 when false and true then 'grub' else -123 end div (case -951 when false then false when 'gazelle' then true when true then 'moccasin' end + -426) < case when 'haddock' like 'arachnid' or false and true then case -626 when false then true when true then true when 451 then 141 when false then false when true then true else 238 end + -129 when 'clam' <=> 'cat' and (true or true) then -36 between -35 and 383 xor false and true when case 436 when false then 'roughy' when true then 716 end between -4 and 110 then 'herring' when case when false then true when true then 'mutt' when true then true when false then false when false then -615 else -43 end in (93, case 466 when 'airedale' then true when false then true when 128 then true when true then false when -413 then 'dingo' when false then false end) then case -154 when 301 then 'penguin' when true then true else 491 end >> -235 end then case when not (true xor true) then case 'grubworm' when not true then case 'dogfish' when 189 then true when true then 'grizzly' when true then true when true then -187 when true then false end when case 'mantis' when false then false when 'tapir' then false when true then true when 575 then -537 when false then false else 'mackerel' end then case -70 when 'horse' then true when -47 then 'stag' when 56 then true when 'labrador' then -523 when true then 'mako' end when case 'polliwog' when true then false when 'swan' then 'amoeba' when 'clam' then -512 when 141 then true end then -36 % 72 when -682 * 186 then false or false when -342 not between 330 and -212 then case when false then true when false then false when false then true when false then 43 end when -826 in (-535, -160, -102) then case -90 when 'puma' then 303 when true then 'tahr' when 'marmot' then 'buffalo' when true then true when false then true else 373 end end when 'dingo' like 'mammoth' is not false then -706 = 73 or true xor false when not 'cod' = 'heron' then not false xor 132 not in (-520, -586, 245, -266) when 'tetra' <=> case when true then false when false then false when false then false else 'mudfish' end then 'katydid' like 'cheetah' or -347 between 0 and -57 when case when true then false when true then false when true then -155 end not between 807 and -327 then -61 div -659 not in (135 - -188, -318 >> 135, case -250 when false then false when false then 'tiger' when false then true when false then 364 else 189 end) when not false or 'moccasin' >= 'seasnail' then case 373 when false then true when -191 then 653 when true then true when 'pelican' then false when true then true else 72 end not in (-209 >> 407, -542 % 531, case when false then 114 when true then false when true then true end, 742 * -763) else -244 ^ 42 * case when true then -80 when true then false when true then 14 when false then true when true then false end end / -93 not between -87 >> 125 % (case -44 when false then false when true then true when true then -547 end + case when true then 'sunfish' when false then true when false then -527 when false then 'monkey' else -242 end) and 812 when case when 273 not in (-501, case when 'kit' <= 'stork' then 'crane' when 'impala' = 'trout' then 'sawfish' like 'kangaroo' when -208 <= -214 then 'bee' = 'cicada' when 'donkey' >= 'beagle' then case when true then false when false then -580 when false then 'jawfish' when false then -576 when false then false when false then 666 end else case -524 when false then false when 'bluegill' then true when false then false when false then false else -311 end end, 159) then case case -251 when false then false when true then false when false then true end when 'falcon' then case when true then false when true then false when false then false when true then 'ladybird' else 'porpoise' end when not true then -820 in (256, 59, -117, -619, -755) when 397 = 409 then false and true when 'husky' like 'parrot' then -300 not between 671 and 170 when true or true then 'aardvark' end > case when false then 'foxhound' when false then 'werewolf' when false then 'buzzard' when true then true end ^ 341 when 'chigger' > 'snail' and case 245 when false then true when true then false when false then false else -126 end > case when true then -222 when false then false when false then 130 when true then false end then (case -616 when true then 775 when false then true when true then 104 when 'monkey' then true when true then true else -153 end not between 36 and case when true then false when true then 'sculpin' when false then false when true then 401 when false then false else 562 end) is not false when 339 in (case when -459 not in (-383, 29, -233) then false or false when not true then 'cowbird' when not true then case 'reptile' when 'quetzal' then true when -854 then 'dove' when 'whale' then true when true then false when true then -161 when 874 then 338 else 'porpoise' end when 609 in (542, -52) then 722 >> -68 end, case when 'flounder' like 'cod' then -393 not between 580 and 350 when 'seasnail' < 'scorpion' then -426 when false xor true then false xor false when true xor false then case -48 when 221 then true when false then false when true then false when false then false when false then true when true then true else 28 end end, (372 & 84) ^ case when true then false when false then false when false then false when false then 218 when true then true when true then false end, case when 'penguin' <= 'duck' then case when false then true when false then true when true then false when false then true when false then false end when false is not null then 'shrimp' when true and false then -191 in (-854, 426, 165, 342, -1) when true is false then 'mackerel' when true and true then 345 not between -465 and 75 else 406 end, case when false then false when true then true when true then 142 when false then false when false then true when false then true end * case 569 when 286 then false when true then true else 85 end) then -287 & -263 > case when true then 'koala' when false then false when false then 270 else -277 end or (not true or not true) when case case -340 when false then true when false then true when true then false when true then true when false then 398 when -224 then -31 else -109 end when true or true then false xor true when -486 not between -390 and -2 then true or false when 210 in (-435, 193) then 'leopard' like 'wildcat' when -544 in (-66, -255) then 'opossum' else 20 ^ 949 end in (509, -537) then case 129 * -220 % (-516 >> 606) when -19 / -179 between 24 >> -677 and case 763 when false then 517 when 'opossum' then false end then 1 between 249 >> -292 and case 358 when false then 'roughy' when -709 then -67 when true then false when 'sparrow' then 58 when false then false when false then false else -148 end when 477 not between 581 / -477 and -28 then 'moth' <=> case 'flea' when true then false when 'filly' then true when true then true when true then 'basilisk' else 'urchin' end when 'boa' <=> 'newt' and 136 between -432 and 39 then not 'swan' like 'hen' when case case when true then true when false then 'jennet' when true then true when false then true when false then false else 159 end when false is false then 'monkfish' <= 'dove' when true and false then case when false then true when true then false when false then 368 end when 321 < -472 then case 'longhorn' when true then -64 when true then true when true then 226 when -76 then false when 499 then true when -796 then true else 'squirrel' end end then 263 in (244, -972, -949, -656, -648, -25) and 24 not in (151, -551, 130) when case -495 when not true then not false when 50 then false is not null when false is true then 640 <= -608 when false or false then false is true else 274 end then case when false then true when true then true when false then false else 130 end not in (329, case -521 when true then true when 'crawdad' then 'dassie' when 'mastodon' then false else 709 end, case 681 when false then true when true then false when true then true else 125 end, case when false then true when false then -87 when false then 'warthog' when true then 566 when false then 245 end) else case -559 when not true then true or true when 64 in (79, -327, -35, -92, 613) then true and false else -384 << 19 end end end like case 'woodcock' when not (not true or -344 not between 475 and 45) then -24 not between -431 and case when false then 'alien' when false then true when true then true when true then true when false then 599 end | case when false then -556 when false then false when false then false when true then true else -146 end when 'reindeer' like 'locust' and -306 >= 253 or 81 not between 435 >> 464 and case when false then true when false then false when true then true when true then true when true then 572 else 203 end then not 'dinosaur' = 'sparrow' is not null when not (not false) is null then (true or true or -153 = 506) is not false when -41 div case case 289 when 'rabbit' then 421 when false then false when false then true when false then false when true then false when 125 then true else 85 end when 760 not in (-245, 206, 713, -55) then not false when case when true then 'coral' when true then 'baboon' when false then false end then case when false then false when true then true when true then false when false then true when false then 'gnat' else -359 end when -453 > 73 then 154 between -212 and 0 end then 'midge' else case when 'vervet' like 'dassie' is not false then case when true xor false then -249 when 'duck' >= 'oryx' then 'reindeer' when true is false then true or false when 'tortoise' like 'louse' then -304 & 3 when -299 < 923 then case when true then true when true then true when true then true when false then false else 'collie' end when -420 = 476 then 135 % -597 end when 'halibut' like 'shrew' and (false or false) then -240 = case when false then 113 when false then false when true then true when false then 439 else -601 end when case when true then false when true then 136 when false then false when false then true when true then true when true then true else 'rodent' end != case when false then true when true then 'sunbird' when true then -262 else 'ant' end then not (false or true) when -317 between 347 * -50 and 27 % 199 then -36 div 93 not in (-430, -543, 301 + 197, 608 >> -39, -198 & -980, case when false then 396 when true then -279 when true then true else 178 end) when 'kitten' = 'shark' or 'malamute' like 'mosquito' then (true or true) and -250 not between -5 and 717 else 'slug' end end then case when (true xor false or (true or false)) xor not true and (true or false) then not 414 in (201, 122, 109, -356, 359) or 'oarfish' like 'roughy' is null when case when false is not false then false xor false when false xor true then 'man' else 'bobcat' end > 'mako' then 386 in (-686 | -681 | 702, 206, -368, 645) end >= case when 376 - case when true then true when false then false when false then -396 when true then false when false then false when false then 487 end < -80 | -254 then case when 208 between 34 and -230 then 'drake' when 576 not between 355 and -206 then -177 not between -143 and -707 when false or false then not false else -323 end != case 77 when -613 then 853 when 'gorilla' < 'beagle' then case when false then 'osprey' when true then 'crow' else 'corgi' end when 'troll' like 'heron' then case 'flea' when true then false when true then false when -68 then false when true then -384 when true then false when 'tetra' then 'penguin' else 'griffon' end when true is null then false is not true else 284 end when (false or false) is not true or not -135 in (-649, -311, 80, -6) then -379 < -324 or -596 % -189 = case -957 when 'goldfish' then false when true then true when 78 then false when 732 then 'moray' when true then -469 end else 'mastodon' end when case when -39 div -874 between case when true then false when false then false when false then false when true then true when true then false end and 319 / 97 or 403 between -396 and 303 and -241 not in (27, 149, 217, 735, 154, -31) then case -252 when -153 between 963 and -175 then case 'ram' when true then true when true then true when false then false when true then false end when 'cricket' then false and false when -111 not between 156 and -182 then -190 not in (100, -44, -284) when not true then 'goat' when case 'goshawk' when true then false when true then true when true then false when true then true when false then false end then 560 when -701 > -502 then case 'crow' when true then 'anteater' when -58 then true end end != case 344 when true then true when true then false when 320 then false end + case -534 when false then false when -402 then 94 when false then true when 'gopher' then true when true then -92 when false then 'muskrat' else -217 end when case -351 | 335 when 'parrot' then true is null when 166 not in (-661, -830, -388, -176, -40) then 593 != -61 when 6 >= -503 then not false when -737 between -351 and 661 then 'stingray' like 'slug' when not false then not false end in (217 * -122 div -643 ^ -654, -835 div (337 / -219), case case 753 when true then true when 887 then true end when not true then case when false then true when false then true else 'mastiff' end when case 'chamois' when true then false when 'cattle' then true when -229 then false when 'gannet' then true else 'akita' end then case 358 when true then 296 when false then true when 'raccoon' then false when 580 then false when true then false else -449 end when false is not null then true and true when 736 not between 278 and 120 then true xor true when 185 then true and true when true or true then -563 end, 27, -616) then -26 when case when true then 'squirrel' when true then false else -558 end >> 210 not in (case when 696 != -428 then 'caiman' <= 'kingfish' when 28 >= -346 then false and false end, -345, 84, case when 'fly' <= 'mite' then not false when 804 not in (139, -596, -433, 378) then -265 not in (-471, -68, 813, 101) when -376 <= -245 then false xor true else case when true then -476 when true then false when false then true when true then false when true then -418 when true then false else -749 end end, -572 + 65, case -374 when not true then 325 between 128 and 81 when case -286 when false then false when true then true when 'primate' then true else -624 end then case 607 when 817 then false when false then true when -418 then 293 when true then true when true then 288 when 'sculpin' then true end else case -88 when true then true when false then false end end) then case case -75 when true then 136 when 'earwig' then false when true then false end when 833 then false is null when -955 = -213 then not true else case 855 when false then true when false then 846 when -445 then false when false then true when false then false end end not between 430 and 396 when case 'stingray' when 'kite' then not false when -332 then 'javelin' when not true then true or true when 'unicorn' = 'tiger' then 'seasnail' like 'shark' when -425 not in (-90, -425, 296, 301, -972, 2) then not false end <=> case when true is not false then case 'bonefish' when 276 then 851 when true then 181 when 'caribou' then 347 when true then false when false then false when false then 30 else 'osprey' end when -358 between 208 and 360 then 477 between 750 and 742 when true xor true then 'buck' like 'pheasant' when 'marmot' < 'gnat' then 36 * -841 when -90 between -437 and -300 then 466 not between -322 and -402 when not false then -194 in (262, -547, -266, 551, 119) end then 'reptile' <=> case when 261 between -31 and -306 then false is not true when false or true then true and true else case when false then false when true then 293 else 'anteater' end end when -239 * 749 not between 177 and -20 | 287 and 'gopher' <=> case when true then 418 when false then false else 'seahorse' end then case 'wombat' when false then true when true then 103 when 'hen' then true else 'guppy' end <=> case 'mudfish' when true then true when true then false when true then 122 when false then true else 'cod' end or true xor true xor (true xor false) end like 'woodcock' then case when (-330 between -697 ^ -272 and 407 % -129) is null then case (650 << 773) / -84 when case 32 - -74 when -458 <= -277 then -113 / -254 when 'kite' like 'blowfish' then not false when 350 then 'cowbird' like 'chipmunk' when -421 >= -344 then 'firefly' < 'vulture' when 'stallion' then case 'terrier' when false then true when false then true when 'wallaby' then false when true then true when true then false else 'muskrat' end when true xor true then false is not null else -498 / 336 end then -524 not in (-597, -5) when case when false then false when true then false when false then 527 when true then 'wolf' when false then 222 end <=> case 'amoeba' when false then true when true then false when false then false when 'owl' then 642 else 'snail' end then case 'baboon' when case when true then false when true then true when true then true end then 'ewe' > 'man' when 'swan' < 'humpback' then 149 = -495 end when case when 756 not between -636 and 349 then false or false when false is not null then -595 = 454 when 140 between 282 and 93 then not true when 'mustang' > 'basilisk' then -445 % 128 end then 'antelope' when -658 << case when false then false when true then true when false then true when false then false else -164 end then case when false then false when true then false when false then -795 when true then true when true then 'molly' end like 'raven' when case 'monkfish' when true then false when false then false when 167 then 610 else 'perch' end like 'escargot' then case when true then true when false then false when true then false else 310 end not between case -60 when 300 then false when false then 'cougar' when false then 188 when -299 then 127 when 'octopus' then false else 30 end and 327 else 101 end when case when 'slug' > 'ibex' then case 'python' when true then false when false then 948 when false then false end when not false then 'raccoon' <= 'malamute' when 696 not between 313 and 123 then false and true when 843 <=> 531 then case 'oryx' when -592 then 'stud' when false then true when false then true end end not in (-327 div 37 div case when false then 'gecko' when false then 'heron' when true then true when false then true end, case when 81 <=> -452 then false or false when true and false then 73 & 67 when -281 >= -483 then 'mantis' < 'owl' when -86 between 391 and 4 then 427 in (174, 363) when -577 in (-692, 183, 579, 273, 302, -246) then 309 when false xor false then case when true then false when false then 'mackerel' when false then false else 244 end else -386 | -869 end) then (33 <= -348 or false is not true) is not true end = 'prawn' end when case when not (-303 - 217 = case 434 when false then -437 when false then true when false then false when true then false else -141 end or (-129 not between -329 and 282 or -315 <=> 316)) then (case when 397 not in (-457, -508) then 212 when 34 not in (312, -248, -612, -276) then false xor false when false and true then 382 <= -330 when -171 not between -953 and 47 then 837 >> 614 end between 380 and case case when false then false when true then false when false then false when false then false when true then false end when 155 not between -336 and -41 then 'bluejay' when false and false then -672 = -213 when 'turkey' like 'horse' then -554 in (-958, -304, 233, 107, 896, -48) end) is not null when not (not true xor 'crayfish' like 'shrew') and (case -743 when true then 497 when true then 111 end not in (case when true then false when true then false when true then false when false then true end, 181, -368 / 181, case when false then true when true then true when false then false else 506 end, case when false then false when false then true when true then true when false then true when true then true when false then false end, 82) or (false xor true or -470 in (-264, 263, -447))) then -290 & case when false or false then case when true then true when true then true when false then true when false then 412 when true then 501 when true then 'worm' else -78 end when false xor true then true or false when 102 between 276 and -485 then case when true then false when true then false else 'amoeba' end when -696 < -548 then 'manatee' like 'platypus' else -156 end - -51 when case when 'cardinal' like 'egret' and (true xor false) then 323 between -545 and -507 xor true and false when not (true and false) then 52 in (case 8 when true then true when true then 'leech' when true then 'collie' end, case when true then false when false then false when true then false when true then false when false then 'bluegill' else -324 end, case when true then 'mammal' when true then -246 when false then true when true then false end, 52 << -73, -746 % 5) when -866 not in (case -657 when 'tick' then 'caribou' when false then true when -498 then false when true then true when true then 160 else 84 end, case when true then true when false then -463 end) then case 294 when 'possum' then false when false then false when true then -336 when true then false when 355 then true end >> case -715 when false then 'toad' when false then true when false then 234 when false then true when false then 'sailfish' else 393 end when case -579 when true then true when false then true end not between 637 ^ 385 and -614 % -495 then 477 >> 37 not between -71 and -610 - 425 end > case when case when false then 31 when true then false else 'drake' end != case when false then 64 when false then true when true then false when true then true when false then true end then 'fox' when case 'mako' when true then true when 'eagle' then true end >= case 'hedgehog' when false then 'moose' when true then true when -794 then true when 'cicada' then false when false then true end then not (false and false) when 'seagull' = 'rhino' xor 'gannet' like 'spider' then case when false xor true then case when true then 'moray' when false then 'cattle' when false then true end when false xor false then 246 > 493 when 'crow' >= 'reindeer' then false and false when true or true then true is not true when false is true then 'jaguar' else -560 / -557 end when case when true then true when false then false when true then false when false then false end like 'flounder' then case when true then -579 when false then true when true then 455 when false then 'kingfish' when true then false when true then true else 621 end <= case -326 when true then 'pheasant' when true then false when true then false when false then false when false then false end when 98 << 722 >= -48 then case when true xor false then not false when false xor true then false is false when 464 between -515 and 45 then case -45 when -265 then true when false then false when false then false else -292 end when 'lark' > 'redfish' then 973 between -360 and -21 when 'starfish' > 'javelin' then 'monkfish' when 'louse' like 'manatee' then case when true then false when true then false when false then true when false then false end end when case -33 when 'skink' then true when 'grubworm' then false when true then true when false then false when false then 'imp' else -268 end not in (518 % 611, 123 / 433) then not false and false is not false else 'cockatoo' end then (false and false or false xor true) is not true xor 14 not between case 689 when true then 129 when false then false when true then 472 when false then true else 619 end - 545 and case -337 | 364 when not true then false is not true when 7 not between 359 and 172 then case when true then true when false then 491 when true then false when false then true when false then 'lab' when false then true else 707 end else -531 / 129 end when 285 between 519 % case -477 when true then 144 when true then true when false then 609 when false then true when true then true when 'moose' then false end and 112 / -11 or case when false or false then -358 not between -358 and 309 when 425 != -26 then false is true else 54 div 43 end between 623 and -529 then -88 between 269 and 444 div (case 248 when true then 'boar' when 'newt' then false when 86 then false when false then true when true then false end << case when true then true when false then -602 when true then true when false then true when true then true when false then true else -741 end) when case when 3 - 117 in (-665 - 477, -198 ^ -801, 163 div 383, case when true then -632 when true then 'meerkat' when false then 'cicada' end, case when true then true when true then false when true then true when false then true else -320 end, case -77 when true then false when true then -104 when false then true when true then false end) then 'wildcat' != 'bobcat' when not 392 != -7 then 36 not in (-169, 319, 362, -668, 627, -398) and not true else 'shrimp' end > 'racer' then 26 < -770 is true else 'goose' end <= case when not case 'wren' when true then false when false then true when false then true when false then true when 'hamster' then 'falcon' when -85 then true end like 'warthog' xor (true or false or not true) is true then 'arachnid' when (case 'alien' when false then true when false then false when 'parrot' then 'shark' when 'humpback' then false when 'egret' then false else 'mudfish' end >= case 'foal' when true then false when -77 then true when -313 then true end is not null) is true then case -185 when 122 != 62 then case 38 when 247 then true when 157 then false when true then true end when 'mantis' then case when false then 'jay' when false then true when false then false when true then false end when 119 between 23 and 769 then case 'shark' when true then -70 when false then false when -97 then true end when true or true then case when true then false when false then true when false then true when false then true when true then false end else -145 end & case when not true then not false when true or false then case when true then true when true then true when false then 'martin' end when 'beagle' like 'zebra' then -243 when true is true then -577 between -185 and 211 when not false then false is not null else -143 end not in (182, 477, case when false and false then 58 / 691 when false xor false then true is false end % case -309 * 259 when case -269 when 'sculpin' then true when 'snipe' then 201 when true then 'kangaroo' when true then 12 when true then false when false then false else -40 end then 'ox' like 'duck' when case 'jackass' when false then false when 'mongrel' then true when true then true when true then false when -121 then false end then 'fish' <=> 'lionfish' when -420 in (-699, 111, 83) then 'pup' = 'whale' else case when false then 'woodcock' when true then true else -682 end end) when case case when false then false when true then true when true then 'anchovy' when false then true else 'filly' end when -286 not between 138 and -152 then case when true then true when false then true when false then -132 when false then false end when 'panda' like 'phoenix' then 'llama' like 'ghost' when 'swan' != 'falcon' then 295 / 385 when 567 then case -642 when false then -456 when 'bass' then false when true then true when true then true when true then true end when 307 >> -8 then case when false then 'primate' when true then false end end like 'anemone' and ((182 in (-336, -437) xor 'tadpole' >= 'koi') and 801 in (613 >> 192, -153 + 171, case 185 when true then true when true then 'bass' when false then false end, case -509 when 2 then 'bull' when false then false when -269 then false when false then false end)) then -547 + case when true is not false then 334 > 43 when false and true then -38 between 6 and -135 else -154 % 158 end % case when 14 in (-479, 797) then not false when not false then case -167 when false then 'killdeer' when true then 'foal' end when false is not true then 246 when 'lizard' <=> 'drum' then 'chamois' like 'cat' when 81 between -517 and 464 then false or false end when -284 in (case 183 when false then false when 'dragon' then false end, case -720 when 'escargot' then 602 when true then true when false then -46 when false then 'hound' end, case when true then false when false then -434 else 800 end) is null and not (true and true xor false is not true) then case case when not true then true is not null when 61 in (672, 209, -457, -25, 714) then -367 in (-54, 333, 870, 62, -187, 547) when false is false then 'koala' when true and true then 'frog' end when -540 << 144 in (448, 891 | 31, -244) then case when 170 <=> 424 then case when true then false when false then 281 when false then -216 when true then -20 when true then true when true then 409 end when -520 not in (636, -396, 342, -390, -884) then true and false when 'donkey' < 'werewolf' then case 670 when -199 then false when true then true when false then false when 'penguin' then false when 'magpie' then false end end when 9 between -6 and 97 then 103 << 406 in (295, 540 / -55, -251 ^ -308) when case when true then false when true then -347 else 198 end in (case when true then true when true then true when true then true when false then false else -101 end, -331 >> 211, 160) then 'glider' like 'koi' when case case when false then 'krill' when false then false when true then true when true then true else 'crab' end when case when false then false when false then -585 when false then false when true then 'anchovy' when false then false end then -405 when case when true then true when false then false when true then true else 'elf' end then case 'snake' when false then true when false then false when 'stag' then 351 when -237 then false end when 'insect' = 'adder' then -273 not between 134 and -121 else case 'primate' when 'sheepdog' then 'sparrow' when false then false when 'boxer' then true when 741 then false else 'worm' end end then -270 != -65 or true xor false else -499 end not between -359 | case case 1 when false then false when false then true when false then false when true then true when false then -105 when true then 74 end when 'man' then true is null when true xor false then not false end and case when true is not false then 'shark' when 76 not in (494, 757) then 83 >> -382 when 'adder' <=> 'marmot' then true is null when -50 not in (-212, -624) then false or true when 232 != 55 then 717 >> -767 when -410 not in (-95, 284, 410, 388) then -349 << 101 else case -843 when true then -1 when false then false when true then false when true then -350 when false then 672 end end ^ (case when false then 'collie' when true then true when true then true end ^ 456) end then case case 'sheepdog' when case 'sparrow' when false xor true or 'spider' = 'parakeet' then case when 513 not in (-838, 361, -136, 61, -488) then -121 between -473 and 409 when not false then false xor false when not false then 76 not between 226 and 159 when false xor true then case when true then -280 when false then true when true then true when false then false when false then false when false then 'killdeer' end end when false is not true xor false is false then 'ostrich' != 'koi' is false when case when 708 between -97 and 574 then case when false then false when false then true when true then false when false then 'mustang' when false then false when false then -156 else 707 end when false xor false then 'trout' when 'cicada' > 'wolf' then 408 end then 'pup' = 'loon' when 'kite' like 'cattle' and 'flounder' like 'basilisk' then 'sponge' when case when 14 not in (-402, -761, 425, -343, 699) then not false when 'goose' like 'narwhal' then 'mustang' else case when true then 'guinea' when true then -42 when true then 'horse' when false then true end end then case when false then true when true then false when true then false when false then false when true then -83 end <= case when true then false when true then true else 'titmouse' end when 'skylark' >= 'sheep' then 195 & -443 not in (-181, 81, -399, -602 & 852, 630, case -515 when false then false when -376 then false when true then -25 end) else 'prawn' end then (357 between 756 and case 307 when 355 then false when false then 'elf' end) is false when case when 398 <=> -128 then -124 - -272 when not false then true or true when false xor false then -269 < -343 when 336 = 312 then 295 div -182 end not in (-254 % 85 + case when false then -787 when false then false when true then false when true then true when true then false when true then true else 268 end, -521 % (-344 + -244), 75, -488 div 403, -289) then 'kitten' when case case when false then -624 when true then false when true then 'monarch' when true then true when false then -188 when true then 'horse' end when 390 not between 296 and -83 then 207 | -135 when 'grubworm' then false and false when false xor false then 678 not in (474, 222) when 'killdeer' >= 'slug' then 'gannet' like 'rat' when true or false then 'stingray' > 'chipmunk' end between -630 and -644 then -585 % 280 in (875, -70 & 354, -423 + 286, case when true then true when false then false when true then true when false then false when false then 'wahoo' end) or not true xor not true when case 817 when case when true then true when false then 'panther' when true then true end between case -859 when false then false when 'pigeon' then -154 else 114 end and 288 then case 'buzzard' when 'shrimp' then true when 'sturgeon' then true when false then -607 else 'bullfrog' end like case when false then 'mouse' when false then 31 when false then true when false then true when true then -481 when true then 231 else 'escargot' end when -176 << -135 != 892 % -441 then case -133 when false then true when false then -655 when 'yeti' then false else -174 end % -465 ^ 35 when (false or true) is not false then 174 in (-35, case -670 when 'stingray' then 'sparrow' when -180 then false else 447 end, -87, 365 - 938) else -369 >> -990 | 210 << -843 end then case when false or true then not true when false or true then 706 not between 610 and 98 when false xor true then case when true then false when true then false when false then false when true then true end when false and false then 'dog' < 'seasnail' end << (-173 | 431) % case when false then false when false then true else -627 end when 'tortoise' then case when true and true then 281 when -674 in (177, 97, -400) then not false when not false then case when false then false when true then -88 end else case 'owl' when 'treefrog' then false when 'duckling' then true when -634 then false end end like case case when false then 'silkworm' when false then true end when true is true then false is not null when 213 >> -857 then -181 in (-268, 497, -273) when 'mudfish' != 'bison' then -5 not in (21, 431, 215, 543, -783, 338) when -128 not in (-371, -914, 210, 479, 562) then not true else case 'quagga' when 'polliwog' then 'mantis' when true then 'dogfish' when true then true when true then true when 'tuna' then -82 end end when case when false then false when true then 'goose' when true then false when true then 658 end like case when false then true when true then true when true then -854 when false then 303 else 'squirrel' end and (-540 between -112 and -85 and 'feline' <= 'raptor') then -199 * 2 in (-302, 281, 326 * 89, -168) is not false end when 'lark' then (-88 div 734 | 35 * 520) << 137 not between case when not 'ladybug' like 'bison' then 'aardvark' like 'corgi' or (false or false) when -581 not between -40 and 274 or 17 between -5 and 72 then 519 not between 250 and 156 and (false or false) when (true or true) and 'piranha' like 'chimp' then case when true then -31 when false then false when true then false when false then true when true then 'oriole' end in (121, -795 << 83, case when true then true when true then true else 228 end) when 'mule' like case 'oryx' when false then true when false then true end then 40 << (14 & 23) when true and true xor 240 not between -29 and -37 then 577 + 151 not between case -395 when true then false when false then true when true then 'halibut' end and 185 << 189 when 'gnu' = case when false then 432 when true then false when false then 225 else 'catfish' end then 548 <= 681 or false is null else 259 end and -246 when 'beagle' like 'oriole' xor true is false xor (false or false or 310 between -469 and -82) or -385 between case -421 when true then 306 when -253 then true when false then -67 when true then false when 201 then false end ^ case when false then true when true then true when false then false end and -61 then 'liger' when -405 * ((-770 - 85) % -573) not in (case -43 when 197 in (-767, 138, 654, 58) then 653 > -566 when false xor true then not false when -398 % 355 then 57 end % -325, 416) then 123 when 'ape' <= case when 14 in (-97, 310, -658) or false is null then -494 in (case 714 when true then true when true then false when 'feline' then 'sheep' when false then false when true then true else -546 end, -435 div 325) when 470 > -841 xor -177 = -248 then case 131 + -468 when 'adder' <=> 'terrier' then true and false when false xor false then 190 not between 807 and -212 end when not (true and false) then 221 | 513 not in (-664 div -47, -482 + -757, -405, -614 - -363) end then (573 & -870) << case when false then false when false then true when false then true else 224 end >> (-260 << 438) between case when -463 << 141 between case -306 when true then false when false then 21 else 206 end and case -392 when true then 'grub' when false then true when 600 then 101 when -234 then false when false then true else -439 end then case 'bird' when not false then 'bengal' like 'duck' when 689 <=> -541 then -448 + -194 when 'hawk' then true is false when 'crayfish' like 'elk' then -386 != 47 when case when true then false when false then true when true then false end then false xor false when -21 not between -188 and 702 then true or true end when case 'hagfish' when true then 198 when false then -119 when false then -679 when true then 'bobcat' when 43 then 'honeybee' else 'shrew' end > 'racer' then (not false) is not null when -364 <=> -130 xor true is not false then case 'catfish' when false and false then 'louse' when 353 then 'skunk' when true xor false then case 'gibbon' when 'anemone' then true when -94 then false when 'kitten' then true end end when true and true or -75 < -435 then -342 end and -144 when case case case when 49 not between -178 and 714 then -225 | 993 when 482 not in (-604, -620, 312) then false xor true when 'salmon' < 'stork' then 'civet' when not false then true and false when true and false then case when false then false when true then 315 when true then -205 when false then true when false then true else 352 end when false and true then -175 in (447, 547, 294, 261) else case 'krill' when true then 'akita' when -630 then false when 706 then 'ostrich' when true then 678 when false then -264 end end when case 479 when true then false when false then 269 when true then true when 'weevil' then 'halibut' when 'elk' then false when 'sheep' then 386 end % -51 then case case 'parrot' when 407 then true when false then -388 when 'frog' then false else 'sheep' end when true or true then 56 - -291 when not true then case when true then true when true then 'porpoise' when false then false when true then false when false then true end when 'shark' like 'seahorse' then not true when -217 not in (-306, -484) then case when true then true when false then true when true then false when false then false when false then true else 'tapir' end when -168 div 303 then 654 between -27 and -630 else case when true then 'narwhal' when false then false when false then 743 when true then false when true then 'shad' when false then true end end when -347 then true xor true or 37 between -44 and -232 when case case 'elk' when true then 'rabbit' when 772 then true when true then true when true then 'hippo' else 'pony' end when true or false then case when false then true when true then true when true then true when false then 'lacewing' end when -737 in (129, -583, -33) then case 'hermit' when false then -559 when true then true when true then true when -352 then true when true then -137 end when -111 not in (-288, 927, 381, 128, 268, -59) then 448 when case when true then false when true then 'warthog' when false then 'shark' when true then false when false then false else 847 end then false or false when true xor false then -316 in (-721, 305, 27, -666, 324) end then 547 not in (-314, -48, -761, 382, -408) and (false xor true) when not 845 not in (-318, -336, 82, 378, -138, 230) then -383 << 65 % 441 end when 'lionfish' then 229 when 881 then not 'colt' > 'snipe' xor not 'silkworm' like 'mosquito' when 89 not in (401, 465, -132, 489, 15) is not null and case when false then true when true then true when true then false when false then -342 when true then false when true then false else 90 end between 509 and -258 >> -327 then -614 not in (case when not true then true and true when true xor true then 'emu' end, -180, case when not false then 24 not between -264 and -279 when false or true then case when false then true when true then -399 when false then false when false then 'alien' else 'bison' end when 'bat' < 'goat' then -188 & 121 else -224 + 162 end) else 'swan' end then case 'lemming' when 403 then case case case 'heron' when 'gecko' then -351 when true then 923 when false then false when true then 'condor' else 'scorpion' end when -103 not between -53 and -140 then 'hermit' like 'hare' when true xor false then -28 not between 222 and 45 when 491 < 234 then 'satyr' like 'bluejay' when false is not false then case when true then false when true then true when false then false when false then -260 when true then true when true then true end end when 'racer' != 'arachnid' and -798 not between 544 and -319 then 'alien' like 'deer' is null when false is true or false xor true then (false or false) is not false when case 519 when 'foxhound' then 'puma' when 'terrapin' then false when 'albacore' then true when true then true else -206 end >> 572 then 39 + -273 when case when false then 77 when false then false when true then false when true then true end = -457 then 'finch' < case 'lamb' when false then true when true then true end when 'gator' like 'pelican' then 186 end when case when -728 <= 529 then not true when -706 <=> 269 then case when true then false when false then 'bullfrog' end when false and true then true and true when true and true then 855 >= 235 when 698 >= 349 then 'insect' < 'coyote' else case when false then 'louse' when false then -15 when false then false when true then false when true then 705 when true then 325 end end in (52, case when 'stork' like 'gibbon' then true and false when -367 not in (259, 87, 737, -323) then -321 / -163 when false xor true then -347 <=> 839 when -188 not between 737 and 307 then 'ghoul' when -26 in (-249, -350, -308, 113, -85, -246) then -616 else -439 end, 804 + case when false then false when true then true when false then -892 when true then -41 else -662 end) then 'porpoise' >= 'raven' when 149 > case -475 when 'piranha' then true when true then false when -45 then true when false then false else -688 end is not true then case when true then true when false then false when false then true when true then true end - case when false then false when false then true when false then -183 when true then true else -78 end < case -350 when false then false when false then -299 when true then true when false then false else 157 end * -496 when (true or false or 94 not in (-128, 386, 76, -410)) and -912 | -116 not between 199 >> -691 and case 835 when true then -223 when true then true when true then 248 when 'griffon' then true when false then true when true then false else 416 end then case 'lemur' when case case 'vulture' when false then false when true then 'mule' end when 'redbird' then case 'stag' when 'toucan' then true when false then 'husky' when false then false when false then true when true then true else 'snake' end when false is null then false and true when 'roughy' then case -836 when true then false when true then false else -213 end else 'firefly' end then case when true then 'fawn' when false then false else -881 end between -959 | -59 and 503 when 49 not in (185, -4, -81, 44) xor false is true then case when true then true when false then false end / (-316 - -462) when 'amoeba' then case -85 when false then false when true then true when 'grouse' then false end >= -28 % -118 end when 151 << 31 in (759 / case when true then -61 when false then 'clam' when true then true when true then 295 end, case when true then false when false then true when false then true when false then 760 when true then true when false then 597 else -761 end << case when true then true when false then false when false then false when false then true when true then true when false then 'penguin' else 668 end, case -526 when false xor true then 'chimp' when true xor true then 'meerkat' when false xor true then 'jackass' when -270 > 648 then -442 | 375 when 'dassie' <=> 'anemone' then 459 / 564 when 'basilisk' < 'mullet' then -496 <=> -482 else -255 end, 391 ^ -224, case when 'quetzal' like 'dory' then -108 between -187 and 375 when false and true then case when true then false when true then true when true then false when false then -693 when false then true end end) then case 'condor' when true is false and -165 != 166 then 'rabbit' = 'mako' xor 'starling' like 'cougar' when case case when true then 'sloth' when true then false else -391 end when not false then true or false when true is true then -126 in (-7, 801, 339) when false is null then 690 - 98 when case when true then 62 when false then false when false then false when true then true end then false is not true else case when false then -378 when false then true else 251 end end then case 88 when 'hippo' then 'platypus' when 'gorilla' then true when false then 'donkey' else 615 end & 40 when 'glider' like 'bird' xor not false then 755 >> -202 in (365 div -53, 9, case when false then true when false then false when false then false else 342 end, -89 << -740, case 67 when true then false when true then 308 when false then true end, 599 * 867) when not 195 in (-345, 285, -478, 505) then -80 % -175 when 'parrot' <=> 'osprey' then case 417 when true then 'cougar' when false then 'satyr' when false then true when true then 41 else -229 end >= 191 else case when 'squid' like 'beetle' then case -406 when true then 119 when 'grub' then true when false then false when 183 then 'emu' when true then true when true then -513 else 181 end when 875 <=> 74 then 'feline' like 'ray' when true xor true then true and false when -228 not in (99, -458, -207, -482, -354, -437) then case 317 when true then false when true then false when 'jackal' then false when true then -318 else -288 end when 634 in (-101, -665, 265, -309) then false or false else case when false then true when false then false else 'skylark' end end end end when (-140 & -591) % (252 div 239) + -692 between 146 div case -178 << 367 when 'bonefish' like 'tarpon' then 'dragon' when not true then case 'satyr' when true then true when true then false when true then true when true then true when 'deer' then -563 when false then 95 end when -429 then case when true then 553 when false then true when true then true end when false xor true then false or true when true and false then 134 not between -15 and 14 when false and true then 'pup' > 'scorpion' else -721 end and 818 then 'hog' end >= case 'mongrel' when ('boar' != case when false then 'stallion' when false then false when true then false when false then false end xor (false xor false xor 'sunfish' like 'camel')) and (-111 between case when true then true when false then true when true then true else -649 end and -301 or -131 | -236 in (-124 div -327, -48 ^ -816, 318 & -408, 2 / 14, 83, -134)) then 'reindeer' >= case 'quagga' when not 'viper' like 'hare' then 'rodent' when 'turkey' then not true xor 467 != 744 when not (true and false) then case 'moth' when 'dassie' then true when true then false when false then true when true then true when false then false else 'escargot' end <= case 'foal' when true then false when false then false when 'raven' then true when true then false else 'donkey' end when not false xor 'fox' = 'labrador' then -661 % 784 in (case when true then true when true then false when true then true else -800 end, 351 * -121, case -456 when 'reindeer' then false when 'honeybee' then true when false then true end) end when not 'raccoon' <= 'raven' then 268 when case case 'scorpion' when case -400 when true then 'monitor' when true then true when true then true end then not true when 'kangaroo' > 'sloth' then 'corgi' like 'wallaby' when case when true then true when true then false else 'rooster' end then 'slug' = 'collie' when 'pegasus' then true is true when not true then true or false when false and false then -572 between 437 and -798 else case 'crab' when false then false when false then 'mallard' when false then false when true then true when -863 then 'goblin' else 'husky' end end when -103 then not false is false when -43 div case when false then false when true then false when false then false when false then true when true then false end then true is not null or true and true when -599 ^ 361 then -393 not in (501, 523, 361, -192, -395, -231) is not null else 'walleye' end = 'primate' then 'chicken' >= 'aphid' and 'kite' >= 'beetle' or 'panda' like 'unicorn' is null when case 301 when case 'lemming' when -363 then true when false then false when true then true when -11 then 'cattle' end then not false when case when true then 10 when true then -23 when true then true when true then true end then -717 not in (-242, -135, -582, -50) when true or true then -28 not in (-645, -9, 354, 370, -2) when not false then 'hookworm' end ^ 6 >= case -548 when 285 div 557 div (-511 + -686) then 'grubworm' when 'mongrel' <=> case when false then false when false then false else 'stallion' end then 'opossum' when -678 not between 21 & 260 and case 140 when -253 then true when false then true when true then -511 when false then false end then 'shrimp' like case when false then 'pelican' when false then false when true then -412 when true then true when true then false else 'dory' end when (not false) is not false then false xor false or 'glider' >= 'foal' when case when true then true when false then true end >= case when true then -338 when false then 'hare' when true then -168 else 71 end then not (false xor false) when not 'aardvark' like 'gelding' then not false and 'mosquito' <=> 'garfish' else -139 * -514 << case when true then false when true then 'dove' when false then false when false then 'pig' else -474 end end then case when true and true then 310 not between 147 and -30 when true or false then not true when 'mallard' <=> 'peacock' then 'octopus' when true or true then 'sparrow' like 'wasp' when not true then 'warthog' when not false then true and false else -449 end not between 190 >> case -692 when false then -489 when 'rodent' then false when false then 'mosquito' when false then 'quail' when false then false end and case 618 when 17 then 'quetzal' when true then true else 111 end | 72 + -346 and (-557 & 150 not in (-587 * 543, 22 ^ 780, -56 >> -350, -192, -239 << 476, 199 ^ -718) or false is not true xor not true) else 'crab' end when (case 'mouse' when case when true then true when true then true when true then true end >= case 'man' when false then -290 when 'scorpion' then true when true then -158 when 552 then true when true then true else 'pangolin' end then (-85 between -67 and 298) is not false when -128 then case when false then true when false then false when false then false when false then -239 else 'prawn' end like 'seagull' when case when false then 'kite' when false then 260 when true then true when true then false else 'shrimp' end like 'akita' then -69 div 73 in (case -126 when true then false when true then false when false then true else 607 end, -136 & 5, 386, -275) when 444 - -730 < -785 then case -165 when true then true when 'bear' then false end > case -556 when false then true when false then true when -409 then true when false then true when true then 128 else -348 end else case when false and true then 'adder' like 'weevil' when 117 != -271 then false and false when false or true then 'gelding' like 'baboon' else 'cockatoo' end end like case case 'anchovy' when 605 | 230 then 'anteater' when 'dinosaur' then case 'ladybird' when true then false when false then false when 355 then true when false then true else 'grub' end when -132 | -490 then 41 not between -556 and -92 else 'deer' end when case 'dory' when -93 in (250, 86) then 658 <=> 474 when true and true then true is not false when case when false then -431 when true then false when false then 'hornet' when true then true when true then 'buzzard' when false then true end then -670 not in (-329, -406) when case when false then -181 when false then true when true then -444 when false then false else 'longhorn' end then 'finch' else 'ocelot' end then case when false or true then 'ladybird' when 128 between 445 and 9 then 'pangolin' else case when true then true when false then 'alien' when false then true else 'bat' end end when (15 + 517) ^ case when false then false when true then false when true then false end then (false and true) is null when 'chicken' like 'catfish' then -398 when -402 << -766 != 366 then not -701 between 440 and -199 when 544 between -137 + -834 and -124 then case when true xor false then case -334 when true then true when true then 'malamute' when 249 then true when false then 'mouse' end when -208 not in (183, 301) then 'bengal' <=> 'redbird' else -30 end when 'sheep' then case case 'sheep' when 'elk' then true when true then 'duck' when 'seahorse' then true when true then false else 'crappie' end when false is null then true and true when 'tarpon' like 'rhino' then 358 / 232 when case 191 when true then true when true then true when 662 then true end then true or true when 'jaybird' <= 'hedgehog' then 'sheepdog' <= 'redbird' when 'buzzard' != 'grouse' then 'liger' like 'goat' end end or (case when not false then case 'caribou' when true then true when false then false when true then -637 when false then true end when -254 <= 671 then case when false then false when false then false when false then true when false then true when false then true when true then 'turkey' else 'aphid' end end like case 'oyster' when 594 <= -713 then 9 = 526 when case when true then false when false then true else 'escargot' end then case -107 when 'jay' then true when true then false else 451 end end or case when 176 = -15 then -258 when true is not null then -855 between -354 and 320 when false or false then false and true end not in (70, -110, case case 167 when 337 then -780 when false then true else -327 end when 'cardinal' like 'lion' then -191 not in (276, 721, 68, -141, 8, 173) when 'monitor' then -346 in (-17, -323, 712) when 'lamb' then true is not false when 'falcon' then 'filly' like 'mallard' when -505 not in (-180, 313, 675, 41, 12) then 'mastiff' like 'gator' end, 103 ^ case 663 when true then true when false then false when 187 then 303 else 507 end, -79 ^ 556 / 155))) xor (not case 'caribou' when 'woodcock' then true xor false when 379 = 211 then true and false end like 'dinosaur' or case case case when false then -297 when true then 'imp' else 820 end when case 880 when 'teal' then 'reptile' when 'pig' then true when 'werewolf' then false when -316 then true when 'phoenix' then true when true then true end then true is false when not false then case 'dinosaur' when true then true when true then false when true then false else 'bedbug' end else 157 + 50 end when 'serval' then 'mackerel' when case 'malamute' when case when true then true when true then true when false then 629 when false then false when true then false end then false xor true when 604 not between -350 and -263 then true and true when 457 in (-716, -282, 61, -609, -398) then -285 not between 736 and 103 when 'cougar' then case 'earwig' when true then true when 'condor' then false when 208 then 'pigeon' when -641 then -293 when true then false end when false xor false then not false else 'chipmunk' end then -73 < -709 | -520 when not not false then not (true xor false) when -237 between case -111 when true then true when false then 67 when false then 'dingo' when true then -188 else -233 end and -632 / 481 then case case when false then false when true then true end when 187 between -740 and -620 then 186 + -122 when 'rattler' like 'sloth' then case when false then true when false then 182 when true then false when true then true else -51 end when 735 in (454, 733, 80, 816, 3, -377) then case -571 when -618 then false when -57 then true when 622 then false else 607 end when case when true then 'tapir' when false then false when false then true when true then false when true then true end then not false else 'bonefish' end when 'drake' then case when false then 408 when false then false when false then false else 36 end < 556 when -467 not in (460 div 451, 566, -38, 370 & -480, 233, -354) then 'lemur' <= 'longhorn' is false else case 349 * 87 when case 'crayfish' when false then 'garfish' when true then false when false then true when false then false when true then true end then 'silkworm' > 'bream' when -100 != -89 then 888 < 510 when false xor true then case 'earwig' when false then 135 when -180 then -495 when false then true when false then false when false then true end else case when false then 219 when true then 'cricket' when true then 414 when false then 'jackal' when false then true when true then 'gecko' end end end not in ((-344 >> 634 / -702) * case when 'mastodon' <= 'prawn' then 'mullet' when -39 not in (88, -126, 757, -418) then -553 = -95 when true or true then not false when 'bird' <=> 'mole' then -408 + 420 else -512 end, case -194 when true then false when true then false end div case -270 when 'dingo' then 'insect' when true then false when false then true when false then false when 243 then false when false then false end div case case 98 when true then false when true then false when -267 then true when 'mullet' then -306 when false then true when true then 'mule' end when false is null then -624 between -493 and -60 when false or false then -106 * 300 when 695 not in (716, -342) then 'hyena' when -473 between 173 and -442 then 'kingfish' when 'asp' then false is true else 161 % -151 end, case (-507 >> -214) % case when true then true when false then -64 when false then 'katydid' when false then false when true then false when false then -742 else -439 end when case when false then 243 when false then -512 when false then 'wallaby' else 29 end not in (case 167 when true then false when 'lynx' then 'macaque' when true then -41 when false then false end, 218) then false is null or 20 = 359 when case when false then -463 when false then 'gecko' when false then true when false then true when true then 'clam' when false then true end like 'buck' then not 'gorilla' <= 'goose' when -768 not between 170 and 256 << 250 then -174 when case when false then false when true then false when false then false when true then false when false then false else 71 end not in (297, -109 >> -279, case when true then false when true then false when false then 'alpaca' when true then 'dove' when true then true else 124 end, 93, -742 | -559) then 426 - case 292 when false then true when 'pika' then 456 end else case when true then false when false then true when true then true when false then false when true then true else 371 end div (-181 / 265) end, case when false then true when true then true when true then false when true then 'termite' end div (131 | 469) + 164)) then case when 803 not in (case when 'basilisk' >= 'puma' xor not not false then 'troll' like 'piranha' when (481 | 75) % -206 >= case -363 % -38 when true xor false then true and true when 'pegasus' <= 'turkey' then not false when -190 then 176 not between -727 and 513 when false and false then 'herring' like 'boar' when case when false then true when false then true when true then false else -211 end then 'crawdad' != 'yeti' when false xor true then true and true else 363 & -727 end then 'snake' = 'magpie' is null end, case 560 & case 103 when not false then 810 not in (-216, -80, 307, 147) when 'sunbeam' then case when false then 593 when false then 378 when true then -266 when false then true when true then true else -13 end else 268 * -234 end when case 'sparrow' when false then true when true then true end >= case 'humpback' when false then true when false then true when true then true when false then -812 when true then false when true then true else 'yeti' end is not false then 117 - 242 when case when not 211 >= -324 then case when true xor true then case when true then true when false then false when true then -264 when false then false end when true xor true then case when true then true when false then 356 when false then 'shrimp' when true then -234 when true then false when false then -57 end when 19 not between -362 and 341 then 'snail' <= 'werewolf' when true is not false then 'terrapin' like 'dinosaur' else 'viper' end when 'bobcat' like 'barnacle' then -180 ^ 310 between case -35 when 518 then false when false then 314 when true then false when true then false end and -323 % 45 when 36 in (340, -330, 294, -377) is not null then case 'leech' when not false then case when false then -53 when true then false when true then true when false then true when false then 340 end when 339 & 514 then 65 * -588 when -641 not in (-216, -122, -586, 558, -471) then 'dragon' like 'ladybug' when 493 & -65 then case when false then false when false then 'parakeet' end end else case 'hookworm' when -359 between -267 and -647 then not false when 'prawn' then case -17 when true then false when true then 'mudfish' when 'unicorn' then 762 when true then false end when false or false then 'moth' like 'toucan' when true xor false then case when false then 'monitor' when true then true when true then false when true then 'zebra' when true then false when false then false else -628 end when 283 <=> 496 then -237 in (3, -496, -46) else 'caiman' end end then 'quetzal' end, 703, case when case when 'pheasant' >= 'unicorn' then true xor false when 756 not between 486 and 520 then -58 not in (-72, -267, -97, 495, -165, 758) end between 818 and case -575 when false xor false then true or true when false and true then 193 not in (273, 322, 126, 790, -69, 779) when 649 between -259 and 175 then false is true else -459 / 914 end then 'horse' like 'newt' when -308 div 83 <= -171 and -661 in (229, -123, case 283 when 5 then false when true then false when 'racer' then -435 else 705 end, -617) then -253 between 501 and 647 - (-663 & -147) when 'orca' <=> case when false then true when false then true when true then false else 'wallaby' end is not true then 184 in (case case -26 when true then false when true then true when 532 then -70 when false then false when false then false end when not true then false or true when 'monkey' < 'man' then false and false when 'caiman' then 'crow' > 'feline' when false is null then -360 when 435 <= -25 then false or true when 'buffalo' <=> 'anteater' then 'bream' <=> 'molly' end, 553 & -98 | -490, -248, case when false or true then false is not true when 553 = -45 then true or true else -490 end, 572 div -2 + (184 + 677), (21 & -80) << -768) when -262 in (702 << 835, 757, -75 div 722, -216 / -352, 132) xor false is not true and (true and true) then case -437 when false is not null then 'macaw' <=> 'amoeba' when 'escargot' then 570 | -343 when false and false then not true when -293 << -203 then case -264 when -189 then false when false then 'shrew' when true then -284 when true then true end when false and false then 196 when 465 ^ 5 then true xor false else 538 - -303 end - 159 when 'troll' <=> 'ostrich' then -770 when case when not false then 305 not in (288, 453, -288) when 320 != 532 then not true when 'mole' > 'wren' then -98 when 27 not in (396, 27) then 261 > -96 else -632 end >= -70 then not (-24 not in (109, 465, 865, -528, 553, 585) xor false and true) else case -283 when true then false when false then 33 when false then 'hare' when 'camel' then 'dogfish' end - 334 + case -117 when 'grouse' then false when 'malamute' then true when false then false when true then true end % 274 end) then case when case -198 when 'crab' then case -100 when -629 in (105, 106, 28, 344) then case 'goat' when true then 117 when true then true when -50 then 'parrot' when true then true when -390 then true end when false is false then case when false then 'ghoul' when false then true when true then 'stork' when true then false end end when case 'stallion' when 'dinosaur' like 'camel' then false or true when 102 between -738 and -580 then 'leopard' when not true then false and false when 'anemone' then true xor true when 'parakeet' like 'manatee' then -93 ^ -390 end then 'kiwi' >= 'jackass' is true when case 'kingfish' when false then false when -337 then false when -595 then false when true then false else 'wahoo' end like 'loon' then (-491 << -280) * 37 when -603 * 660 & 327 then case -280 when false then -235 when true then false when false then 388 when false then true when false then true end between -225 >> -58 and -789 when 'bluebird' then -325 >> -82 in (189, -275, 552, -75 ^ 266) when not false or 'midge' < 'mako' then 71 <=> 50 else -722 end <= case when not (false or true) then 'elk' <=> 'lacewing' or true and false when case 'fowl' when true then -544 when false then 272 else 'tapir' end = 'beagle' then case when 0 != -655 then 'boxer' like 'dodo' when 'killdeer' <=> 'malamute' then 'ant' = 'bulldog' when 'gnu' >= 'earwig' then -369 in (-296, 386, -323) end when case when true then false when false then true when false then true when false then 'bat' else 'eel' end like case when false then false when true then false when true then -248 when false then false when true then 21 when true then 'catfish' else 'urchin' end then case case 'sparrow' when false then true when 185 then true when -633 then false when true then true end when not false then true and false when false or false then 261 != -281 end when false xor true xor 915 >= -251 then 446 when case when false then true when true then 'zebra' when false then 'camel' when false then false when false then false when false then 'raven' end not between 461 >> 792 and -113 + -660 then case 'primate' when false then false when 294 then true end >= 'collie' end then not ((true xor false) and 'ghoul' like 'dory' or case when true then false when true then 'grubworm' when true then 'wallaby' when true then true when true then -150 else 'monkfish' end like 'gull') when case 'weevil' when 'iguana' like 'moray' then true and false when true xor true then -8 when 'kit' then 'man' like 'panda' when false xor true then case 'piranha' when false then true when 'oryx' then 'whale' when true then true end when not true then -377 not in (577, 427, 390, -111, -99) when case when false then false when true then true when false then true when false then false when true then true when true then 679 else 553 end then 879 not in (38, 354, -132, -304, 315, -208) end >= 'deer' and (true is true and 'marlin' like 'elf' or case 'hen' when true then 399 when 'hound' then true when true then true when false then false when 'moose' then 757 when -943 then true else 'flea' end like case 'muskox' when false then true when false then false when false then false when true then true else 'barnacle' end) then 'jennet' >= 'jaguar' xor ('crayfish' like case 'marmot' when false then false when false then -87 when true then 583 else 'skunk' end or not 'mouse' > 'weasel') when 'hen' >= case when false xor false then true or false when false is not null then -131 not between -248 and -591 when 'collie' like 'mayfly' then true is true when false or true then 'turkey' like 'halibut' when -595 not in (312, 17, 279) then case 'cattle' when 'dove' then -603 when false then true when false then 'ox' when false then -311 else 'mackerel' end end xor case 186 - -84 when -445 then 177 when 'spider' then 'snake' when false or true then 394 > -316 when -425 != 722 then case when false then true when true then -316 else 314 end when not false then 494 end not in (-580, case when false then -233 when true then 'bison' when true then true when true then false when true then true when true then true else -155 end + case -605 when true then false when true then true when -142 then false when true then false when 'sparrow' then true when true then 'cobra' end, 284, case when -376 not in (-502, 228, -555, 353, -451, 521) then true and false when 265 not between 854 and 79 then -851 between -380 and -517 when false is not null then case 'gnat' when true then 'clam' when true then true when false then true when 401 then true else 'bass' end else case when true then true when true then 289 when true then -539 when false then true else 889 end end, case 24 when 886 not between -203 and -146 then case when false then false when true then true when false then false when false then true when false then 'shepherd' when false then false end when 'terrier' like 'giraffe' then true and true end) then -541 in (case -99 when not false is null then case when -28 <=> 401 then case 'bonefish' when true then true when true then -443 when true then false else 'jackal' end when not true then 'imp' when 'goldfish' like 'flounder' then true and true when false and false then case -691 when -390 then true when 'flea' then 623 when 'grub' then false end when true and true then 286 >= 404 when true xor true then true and false else 'satyr' end when 'hen' <= 'hagfish' then case 'raven' when not true then not true when not true then -878 when 153 not between -304 and 525 then case 'rooster' when 'quetzal' then false when -559 then false when 'moose' then false end when false xor false then case 67 when false then -458 when false then true when false then true end when 'goat' then not false else 'ray' end when not -545 not in (-591, -306, 219, 575, -61, 77) then 'shad' != 'porpoise' xor 'tadpole' like 'lynx' when case 'terrapin' when true then false when 150 then true when true then false when false then false when 'raven' then false when true then true else 'moth' end like case 'barnacle' when false then true when 105 then false when -279 then true when -734 then true else 'oyster' end then 393 / 38 in (-479, -599 >> -357, -109, 318 % 314) when not -53 between 193 and 437 then case when false then true when false then false when false then true when true then 'tarpon' when false then true when true then true else 'bird' end like 'boa' else -146 end, case case when true then true when true then true when true then 'ostrich' when false then false end when true xor true then case 'parrot' when true then false when true then true when 'porpoise' then false when true then true when true then 745 else 'elf' end when case when true then true when true then true end then not false else 764 | -22 end >> case when true xor false then true or true when not false then -368 end, case when true and false xor 12 between -24 and -295 then 'squid' <=> 'possum' xor 'sturgeon' != 'deer' when -321 > -185 >> -375 then 570 = 208 or not true when 296 | -457 in (-321 - -48, -901, case -671 when false then 'heron' when false then 'mosquito' when true then true when false then -527 when 'cattle' then true end) then 'rhino' end, case when 161 % 347 not between -112 - -53 and case -111 when true then false when true then 'mackerel' when false then false when 'pig' then false else 180 end then not false and not false when 603 - -609 <= case when false then true when true then true when true then false when false then true when true then -41 end then (false is not false) is true else case -384 when true then 710 when false then true end & -852 end, case case when not true then case 'foxhound' when false then true when 222 then true when false then true when false then true when 'emu' then false end when false or true then 'kite' when false and true then 440 << 288 when false and true then 'cattle' like 'shepherd' else case -429 when 'flea' then false when true then -966 when 'coral' then true end end when -523 between -68 and -284 xor 274 < -210 then 779 <= 78 and (false xor true) when not (true or true) then 182 when case when 313 <= -391 then not true when 'cat' <= 'dassie' then 67 in (-356, 882) else case -6 when true then true when true then true when false then true when 'ghoul' then true end end then 210 end, case when (false xor true) is not null then 'tuna' when true and false and 'eel' < 'crayfish' then 94 & -61 when 649 <=> -130 and (true xor false) then (-165 & -18) + (-466 << -170) when (true is not true) is false then true xor true or 'sturgeon' = 'guppy' when 'cow' < 'kangaroo' and true is null then 'shad' like case 'cicada' when 'rooster' then false when true then false when 'bream' then true when 115 then 'chimp' else 'giraffe' end else -45 end) end when case when (-797 <= -631 and -573 not between 89 and 537) is not false then case 'jay' when -13 >= 751 then 'mosquito' <= 'lioness' when case 494 when true then true when -133 then 215 when true then true when false then true when false then false when false then 'sparrow' else -817 end then not true when 'lobster' <= 'griffon' then -177 not in (-648, 417) else 'husky' end like 'hippo' when 'drake' != case 'pigeon' when 'monkfish' then 'escargot' when 214 then -526 when true then true when false then true when false then false when true then -131 end or true and true xor -304 < -28 then -85 else case 'firefly' when case when true then true when true then -445 else 'alpaca' end like case 'kiwi' when 'humpback' then false when false then false when 'buck' then true when false then -405 when true then true when 'sailfish' then true end then case 13 when case 'gazelle' when true then 'gannet' when 672 then 'humpback' when true then true end then 'wahoo' when case when false then true when true then -291 when true then -81 when true then false when false then true when true then 'bream' else 'reptile' end then false or true when not false then true xor false when false and true then -185 * -180 when 'stag' then 'horse' else -377 + 452 end when (502 & 49) * case when false then true when false then false when true then true end then false xor true xor 423 between -708 and 234 when case when true then true when true then 'possum' when true then true when false then false when false then 'longhorn' when true then false else 'mongoose' end <=> case when false then 'oriole' when false then true else 'bug' end then 221 >> 81 not between case -527 when false then true when 'cattle' then false when true then false else 391 end and 864 & -69 when case when not false then -137 when 107 in (243, 134, -845) then true or false when false xor true then 117 not in (447, 164, 89, 531, 385) else case when true then 'silkworm' when true then 'egret' when true then false when true then false when true then false end end then -547 when 'foal' <= 'zebra' and 'cheetah' like 'dingo' then 'lark' end end > 'anemone' then 'alpaca' like case 'cougar' when 34 between case when false then true when false then false else -105 end and -255 - 317 xor (true xor false xor false is true) then case 'tortoise' when false xor true then 'seasnail' = 'arachnid' when 'elephant' like 'boar' then case 'goblin' when true then true when false then true when -278 then 'beetle' else 'louse' end when false or false then -21 when 'pheasant' <=> 'condor' then 114 not between 99 and 20 when 196 * 497 then true is false when 165 <= 107 then false is true end like case 'anchovy' when 'dodo' < 'deer' then 207 in (109, -428) when 72 != 476 then false xor false when case when false then false when false then -623 when false then true else -50 end then case when false then false when true then true when true then false when false then false when true then false when false then 180 else 'sponge' end when false and false then case 'deer' when true then 'zebra' when false then false when true then 'teal' when -444 then -327 end when -644 * -952 then 'hedgehog' <= 'deer' end when case when true xor true then false is not true when not false then false and true when true xor false then false or false end not in (795, case -298 when 333 in (116, 206, -335, 53, -311) then true and false when not false then not true when false xor true then 'hog' like 'tortoise' when 'quetzal' = 'pika' then 318 not in (594, -373) when not true then 'burro' end, (-432 | 246) + (-686 - -140), case when false and false then case 'cardinal' when 'grub' then true when true then 185 else 'elk' end when -369 in (509, 195) then 94 in (483, -392, -348, -450) end, (-238 - -610) div case when true then 'bluegill' when true then true when true then false when true then true when true then 'orca' when true then 'squirrel' else 717 end, case when 'condor' < 'dove' then 'bison' like 'opossum' when -328 not between -198 and 492 then true is true else -96 / 305 end) then not (false xor true) xor (not false or true xor false) end when case when not false then not true when false or true then not false when true or false then 'hyena' like 'goblin' when false or true then 'rodent' when 'mastiff' < 'snapper' then false is null when -86 between -399 and -179 then true xor true end ^ (-489 - case -192 when false then 'boa' when true then 458 when true then 274 when true then true when false then true when false then false else -170 end) not in (-114, -67, case 750 when 'akita' like 'kid' and 716 > -70 then not true xor 615 between -195 and 260 when case when false then true when false then 'krill' when true then 'hawk' when true then 'magpie' else -317 end between -495 and -96 then not (true xor true) when case -221 when false then 'haddock' when false then true when 'sturgeon' then true when true then 'glowworm' when -96 then 'gelding' when true then -265 end not in (-339, case when false then false when false then true when false then false end, -27 - 133, 423) then case when false then true when true then false when true then 'monkey' when false then false when true then 863 else -312 end in (case -51 when 'lamprey' then false when false then 695 when false then -636 when false then -50 when false then 'foxhound' end, case 231 when true then 'crane' when false then true when false then -34 when true then true end) end) xor -643 <=> case 193 % 403 when 'dragon' then 192 not in (-592, -749, -341, -654) when -776 then 615 end - -310 then case case when -401 between 512 and -280 and (false xor true) xor ('terrapin' <= 'hookworm' or not true) then case when 'grubworm' like 'egret' then case when false then 'javelin' when false then true when false then 'silkworm' when true then false when false then false when false then true else 'eft' end when 'goldfish' like 'gannet' then 'platypus' like 'gobbler' when 'glider' like 'mullet' then false and false when 'elf' >= 'python' then not true when -395 not in (139, -100, -29) then 411 = -142 when false and true then true or true else -260 end = case case when true then 'burro' when false then true end when true and false then 'salmon' when 401 not between 477 and -409 then 'pipefish' like 'duckling' when -317 <= 52 then true and false when true is false then 'parakeet' != 'fowl' when true is false then false is true when 813 in (2, -333, 50, -555) then false is null end when case -455 div 142 when 'leech' >= 'sunbeam' then false xor false when false xor true then case 869 when true then true when 'lemur' then true else -148 end when true is true then 'seasnail' when 4 <=> 10 then true is false when not false then 'weasel' like 'cow' when case 'anchovy' when true then false when 369 then false when 'llama' then true when false then false when false then false when 'lobster' then false end then case 'beagle' when true then false when true then true when 'louse' then 'gnu' when -3 then false when false then 'joey' when 'horse' then true else 'ray' end else -109 >> -660 end != case when false and false then true xor true when false is not false then 881 in (-385, 662) when -333 < -747 then true and true when 30 in (907, 199, 623) then 822 when 'python' = 'fly' then true xor true end then not -432 not in (-61, -564, -168, -881, 137, 410) and case 'chimp' when false then true when 'snail' then false when false then false when 111 then false when 118 then 142 else 'oryx' end <=> case when false then false when true then true when false then false when true then false when true then true end when 'prawn' like 'trout' is not true then -158 in (-16, -222, -256, -651) xor true and false xor case when true then true when true then true when true then 'silkworm' when true then true when false then false else 379 end not in (184 << 457, case 169 when false then 'liger' when true then true when true then 180 when false then -699 else -36 end, case -158 when false then -177 when true then 'sole' when false then -346 when 'earwig' then true when true then true when false then true else 83 end, case when true then false when false then 'urchin' when false then true when false then 'killdeer' when false then 469 end, -60 << 15) when 596 between case when -574 != -271 then -511 <=> 211 when 331 in (-48, -139, 178, -213) then true xor false else 136 * 750 end and case -46 when 94 between -605 and -205 then true or false when not false then case 315 when false then false when true then true when false then false when false then true when false then true when false then false else -916 end else case when false then false when true then 'cougar' when false then false when true then 'jennet' when false then true when true then 'walrus' end end then 'robin' when 'grouper' like 'grubworm' is true then not ('calf' != 'grub' xor (true xor true)) else 317 end when (229 <=> -496 is not true xor 'gelding' like 'egret') is not false then -259 when -303 not in (-215 * 69, case when 'cobra' <=> case 'egret' when 'flamingo' then true when true then false when true then false when false then true when false then 'blowfish' when 'louse' then 'alien' end then 'python' != 'mammoth' and 'trout' like 'seasnail' when false is not null or 440 in (433, -848) then case when -176 not in (-172, 502, 96, -135, 89) then 642 >= -610 when 102 in (-280, 238) then 354 when 'flamingo' like 'toucan' then -794 not between -225 and -172 when 'camel' <=> 'thrush' then 'joey' != 'rhino' else case when true then true when false then false when true then 602 when false then true else 'whippet' end end when not -448 between 371 and 542 then -400 div 518 >> 256 when not false xor 'hare' = 'glider' then not (false xor true) when not 'swan' like 'goblin' then false or true or -85 <= 426 when -847 = -425 or -133 not between -240 and 325 then 764 between -327 and -4 xor not true end, 487, -281 ^ (344 | -634)) then not case case 'katydid' when 'goldfish' then -104 when false then true when true then false when false then -576 when 426 then false else 'crappie' end when -503 between -188 and -151 then not true when -161 > -449 then 211 >= -533 when -667 not between 189 and -123 then 38 when 471 then 'hawk' like 'dinosaur' when 'elk' then 481 not in (-126, -310, -463, -764, -728, 338) when 22 = -44 then -721 not between -602 and -116 else 'mule' end >= 'calf' when 'llama' like 'opossum' then 'bobcat' when not not not -699 in (-14, 607, -262, -266, 157, -584) then 'prawn' when 'lemur' then 'fawn' like case when case when false then false when true then 236 end like 'boa' then not not true when case when false then 'tetra' when true then 'earwig' end not in (-28 & 757, 424 / 167, case -661 when true then false when -62 then 'longhorn' when 'aardvark' then false when false then true when 'hermit' then true end) then 665 <= case 505 when false then true when false then true when true then -139 when true then true when false then true when false then false end when case when true then -289 when true then false when false then true else 'weevil' end <=> case 'insect' when false then true when true then -112 when true then false else 'tetra' end then -544 not in (case -860 when false then -13 when true then false when 400 then 171 when false then false end, -60 + 381, -759 & -466, -462) when 'warthog' like 'liger' then -186 | case -8 when true then true when true then 'flea' end when 'hookworm' like 'cat' xor 'grizzly' != 'mollusk' then 'cow' <= 'lamb' and false is not null when (375 between -527 and -610) is null then -218 in (case -535 when -321 then 'shad' when 'crane' then 118 when false then false else 576 end, case when true then true when false then 555 when false then -327 else -553 end, -107, -126 ^ -168, 452) else 'calf' end when case case when false or true then false is null when 'crayfish' > 'hound' then true xor true when true or false then false xor false when 176 not in (274, -480) then 288 <=> 233 when not true then true and true else 'ostrich' end when 'snapper' <=> 'weevil' xor (false xor true) then not false xor true is not null when 194 between -619 and case when false then false when true then false when true then true when false then false when false then true when true then false end then 391 > case when false then 'peacock' when true then 68 when false then 'monster' when false then 'pelican' when true then 'horse' when true then 764 else 398 end when (true and true) is not false then case 'hamster' when false then 'vulture' when false then -36 end like case when true then true when true then 'cat' else 'martin' end when case -294 when false then true when true then true end = -243 & -571 then (false or true) xor -13 not between -182 and 504 when case when true then 'lioness' when false then true when false then false when false then false end >= 'gibbon' then case case 297 when true then 'maggot' when true then 170 when false then true when false then true when true then false else 112 end when not true then case when false then false when false then 327 else 'flamingo' end when not true then 'herring' > 'drum' when not false then not true end else 'beetle' end like case 'gopher' when case 'crab' when -45 then true when false then 306 when 'slug' then true when false then false when true then 458 when 751 then 'moth' end like 'catfish' then case 'terrapin' when false then true when -299 then true when true then false when true then false else 'mosquito' end like 'tetra' when case when false then false when false then true when true then true else -136 end not between -20 >> 709 and -109 << -203 then false xor true xor 663 in (607, -75, -30, 59, 692, -201) when -562 then -123 - -630 = 244 when 41 not in (-650 >> 610, 728, case when false then false when false then true when true then 'urchin' when false then 'arachnid' when false then 'mudfish' when true then -16 end, -198, 517 div -220, 92 >> -532) then 'alien' when case when true then 88 when false then -488 when false then -204 when false then 76 when true then true else 'eel' end like 'dane' then not (true and false) end then case when 113 < -34 or 'antelope' <=> 'goblin' then 'ant' like 'rodent' when 'pheasant' > 'mullet' and 'crane' like 'calf' then (true or true) is not null when 'starling' = 'squid' xor 541 > -195 then case case when false then true when true then true when false then -695 when false then true when true then false when true then true end when 'pug' then 'sunfish' when true or false then case when false then -288 when false then 347 when true then 'sunfish' when false then false when false then 'oriole' else 234 end when -52 in (-140, 105, 211, -238, -1) then 'robin' > 'asp' when case when false then -317 when true then 154 when true then true when false then false end then not true when not false then -692 when not false then false xor false end end > -171 end when case 174 when (true and false or 126 not between -652 and 87) and not -673 in (307, -642, -629) then not ('glider' <=> 'mastiff' and 744 <= -277) when not ('seagull' like 'emu' or -35 in (574, -292, -319, -515, 528, -234)) then case 'vervet' when false then true when 'moth' then true when 'dane' then false when true then false when false then true when false then 340 end like case 'mongrel' when false then 579 when false then 'puma' when true then 503 end xor (false or false or false is null) when case case when true xor true then 242 between 363 and 149 when 53 = -541 then 'seasnail' when true or true then -446 ^ 172 when true is not null then 467 not between 254 and 831 when 'cougar' like 'calf' then true xor true end when 'sunbeam' then not (true xor true) when case when false then false when true then true when true then 620 when false then -361 when true then false when true then 'elf' else 'mosquito' end like case when true then false when true then true when true then false when true then true end then 262 not between -327 and -257 xor 117 not in (-951, -254, -373, -33) when 367 < -349 or 'mammal' like 'kitten' then case when false is false then 338 <= -24 when not false then 'hare' when -65 not in (24, -573, -670, -272, 301) then not false when true and true then 725 != 214 when false and true then not true when 'silkworm' <= 'killdeer' then not false else case when true then false when true then false when true then true when false then true end end else case 'cod' when true and false then 'martin' when 'antelope' != 'sloth' then not false when false xor true then false or true else 'doberman' end end then -823 + -479 end in (case when 216 in (-51 << 92 >> (127 | 356), case -285 when case 'hen' when false then 'shad' when -110 then false else 'aardvark' end then 'meerkat' when false is false then 'mako' when false xor true then true and false when false or true then -40 not in (204, 335, -146, -824, -97) when false and false then false or false when false or false then case when true then true when false then true when true then false when false then false when true then false when true then 'dassie' end else case when false then -450 when false then true else -156 end end, case case -388 when 'chamois' then false when false then false when true then false when false then false end when 'duckling' <= 'mastiff' then -249 when 'bird' > 'woodcock' then false is not false when -682 then false xor true when 'weevil' then case when false then -362 when true then false when false then true when false then 'macaw' end when false or true then 'swift' else case 822 when false then false when false then 482 when false then false when true then false else -623 end end, 89) then (-162 <= -132 or 143 in (-34, -74, 808, 262)) and not 581 < -462 when case when 53 != -123 then true xor false when 'honeybee' != 'tortoise' then 213 between 633 and 404 when -57 not in (-547, -293, 395, -542) then false xor false when 'kangaroo' >= 'prawn' then -620 end not in (170 / 475 << -179, case when true and false then true xor true when true xor false then case when false then true when false then false else 'urchin' end when 189 not in (-432, -32, 303, 275) then false and true when true or true then case 'snake' when true then -296 when false then false when true then -806 when true then 'slug' when 'minnow' then false when false then true else 'python' end when 506 not between -292 and 285 then case -631 when false then false when false then true when false then false end when 'quagga' <= 'koala' then 'hound' else -120 end, case when true xor true then 'newt' like 'boar' when not false then true and true when 'shad' like 'seal' then false is not false when 'troll' <= 'kangaroo' then true is not false when true is not false then case when true then true when true then true else -77 end else case 416 when -151 then false when -63 then false end end, 630, case when -413 <=> -298 then 83 when 'tarpon' like 'primate' then case 'werewolf' when 'dog' then 'sturgeon' when 'camel' then 'owl' end when -296 in (-38, -829, -553) then true or true when 'egret' <=> 'ewe' then 'parakeet' else 213 end) then not case when false then false when true then false when true then -331 when false then true end between -19 % 347 and 23 * -495 when ('honeybee' = 'husky' is true) is not false then not true or false xor true or case 'sunbird' when false then false when false then false when -150 then -611 when true then true end like 'hamster' when ('muskrat' like 'anemone' or (true or true)) is true then ('ant' like 'herring' and 'shrimp' <= 'platypus') is not false when 'mudfish' = case when 136 not between 62 and 114 then case when true then false when false then 119 end when true and true then case when true then 265 when true then true when false then 113 when true then true when false then true end end then 'mastodon' like 'jaybird' is not null when case 187 % -56 when -418 in (17, 312, 86, -191, -440) then -231 not between 186 and -13 when -286 - -181 then false and true when true or false then 'macaw' like 'orca' else case 129 when 'turkey' then false when 'bluejay' then true when false then true else -314 end end between case when 'dingo' like 'lark' then false is not false when 'stinkbug' like 'snapper' then 'cat' when 'gelding' < 'terrier' then not true when 'eel' like 'kodiak' then true and true when -201 between -360 and -96 then true or false else case -79 when false then true when true then true when true then true else 71 end end and -641 * 422 div (-151 & -90) then 'monkfish' = 'goblin' and true is false or 'husky' >= 'phoenix' end, -188, (case when false is not false then not false when 'pika' like 'monkfish' then -712 < -142 else case when true then true when true then true when false then true else -199 end end << 45) + (case 69 + -153 when case 'clam' when false then false when false then -718 end then not false when 257 ^ -753 then 'swift' when false xor true then 23 when -289 between 314 and -549 then true is not null else 701 << -270 end - case when 'walrus' like 'ray' then true or true when true and true then case when true then 'swan' when true then false when false then true when false then true when true then false else -705 end end), -613 + 403 >> case when false xor false then -564 div -254 when 465 not between 255 and 400 then false and false else -496 * -196 end + case case when true then 'parakeet' when true then false when true then true else 234 end when false xor true then -84 not between -86 and 647 when true is null then case 'chipmunk' when true then false when true then -355 when -801 then 'stag' when false then true else 'doe' end when 'lemming' like 'corgi' then 'primate' like 'dingo' when 'katydid' = 'lizard' then case when false then true when false then true when true then false when true then true when true then false when true then -837 end when true is true then case when false then true when true then true when true then 'tiger' end end, case -174 when (-215 + -56) div (249 div -263) - case case when false then 135 when false then false when true then false when true then true when true then 'mink' when false then true else -682 end when not true then false xor false when false xor false then 347 ^ 814 when case 'viper' when 'grubworm' then true when false then true when 8 then false when true then -17 when false then false end then 'tahr' like 'shad' end then case when false then false when false then true when true then 'ringtail' when true then 'gazelle' when true then 'bonefish' end between 826 and 234 and 'amoeba' != case 'mayfly' when true then false when 'oarfish' then true else 'bedbug' end when case -80 when true or false then 448 not in (774, -200) when not true then 540 in (696, 112) when 'phoenix' then 'mongrel' when -213 in (296, 156, -273) then not false when -499 div -306 then case 45 when false then false when true then -370 end else 127 | 994 end between -454 and case when 'beetle' <=> 'mustang' then 563 * 741 when true is not false then -89 in (-574, -675, 327, -72) when false or true then -297 << -195 when false or true then true and true else case when false then true when false then false end end then 753 when case when -633 < 267 - -135 then -917 between 314 and -272 or 'skink' like 'lion' when (false or true) and (true xor true) then 'woodcock' != 'narwhal' when -414 - 539 not between 141 ^ -71 and case when false then -886 when true then false when false then false when true then false else 58 end then false xor true xor not true when 'jawfish' > 'goat' xor false is false then case 'gorilla' when 877 then true when true then 'cat' when false then true end like case when true then 'cricket' when false then false end when case 462 when false then 'elf' when -1 then 'finch' end != -183 then 'skink' when 'ibex' like 'guppy' then -76 in (-433, -105, 10, -204, 146, 364) xor 560 not in (-206, -628) else 'roughy' end then case 'sculpin' when true then true when true then false when true then true when 'dragon' then 618 when true then 'parakeet' else 'unicorn' end like 'goblin' and case when false then 'magpie' when false then 519 when false then true when false then false when false then true when true then true end = case 'snake' when true then false when false then -456 when 'jay' then false when 796 then true when -273 then false else 'monster' end when 'airedale' then not -12 | 320 between -210 and -262 - -156 when case case when false then false when true then 613 when false then false when false then false else 123 end when true is not false then case when false then true when true then true when false then true when false then false end when case when true then true when true then true when true then 214 when true then true when true then false when false then 'coral' else 'chipmunk' end then -847 << -533 when 'bug' like 'teal' then -264 between 6 and 210 when -331 / -292 then false and false when true and true then 'ram' < 'grizzly' else -671 end between case case when true then 'swine' when false then -217 else -851 end when 533 <= -160 then 'kite' like 'escargot' when -384 > -48 then 'ringtail' > 'piglet' when false and true then 399 when true and true then true and false end and -396 >> 529 % 337 then 'oarfish' like 'unicorn' else case -690 when false xor false then -174 % 215 when 'sunbird' >= 'sawfly' then 'perch' when false is false then 299 not in (27, 103, -621, 94) when case when false then false when true then true when false then false when false then true when false then false when true then true end then not false when 'cub' like 'eft' then 'spaniel' when 'piranha' then case 'dove' when true then false when -495 then false when false then -791 when 'molly' then true when true then 'fly' end else 200 ^ 411 end * -165 end, -937) then ('dane' < 'zebra' and (true xor false xor (false xor true)) or case 'glider' when case when true then 5 when true then true when true then 'phoenix' when false then false end then false or false when 'mule' > 'weasel' then false or true when false and true then false xor false when -462 not between 456 and 578 then 170 <= -48 else 'jawfish' end like 'bullfrog') and not ('humpback' like 'herring' xor 'duck' like case when false then true when true then 'fawn' when false then true when true then 432 when false then false when false then false else 'puma' end) when -916 not between case when 'rat' <=> 'bobcat' then -266 not between 294 and -242 when false xor false then case when false then true when false then true when true then false when true then 225 when true then false when true then -243 else 29 end when 'sailfish' like 'cowbird' then case when true then true when false then 'akita' else -313 end when 'squirrel' like 'parrot' then false is not true else case when true then false when true then true when false then false when false then 'snapper' when false then 456 when false then 720 else -325 end end ^ (case 525 when 'coyote' then true when false then true when true then false else -417 end << 184) * -70 and -624 then 342 != -55 when 'raptor' like case 'griffon' when 97 > 472 is null xor true is false and false is false then case when -68 = -477 then case 111 when true then true when true then true when true then true when false then true end when not false then true and false when false or false then true is not null when true xor true then true or false else 255 & -494 end < case when -241 between -13 and -107 then true and false when -228 != -424 then 'goat' = 'manatee' else case -121 when -567 then false when false then 'leech' when true then false end end when case -389 when false then true when false then 136 end - -402 in (-649, -690) then 'albacore' like case when 406 not in (-205, 124, -276) then true xor true when 'weasel' < 'salmon' then not true when 'ostrich' like 'flounder' then false is not true else 'mongrel' end when 'cricket' <=> case when false then false when false then 'panda' when true then true when true then 183 else 'lizard' end or -39 between 435 and case -721 when 162 then 'platypus' when true then true when true then -567 when true then false else 291 end then 861 between 223 | 79 and -576 div 90 or (false and true or true xor false) when 356 then -277 / (-435 | -758) >> 386 else case when (false xor false) is not false then 'ferret' like case 'cicada' when false then 430 when true then 'dragon' when true then false when false then false when true then true else 'drake' end when not true and true is not true then (true or true) and true is not null when false is not null or 'walleye' < 'anchovy' then (false xor false) is true when 408 not between -727 and 236 xor -349 between 88 and -164 then -438 << 341 between -411 & 520 and -616 else 'lionfish' end end then 'mollusk' end when case when 'colt' like 'asp' then case case when not 'opossum' like 'stallion' xor -504 != -381 and -446 not in (266, 228) then case 381 when true then 'kodiak' when true then false when false then 416 end in (-100, case when true then true when true then false when false then true when true then -300 end, 260 ^ 524, -207 << -213) is false when ((not true) is not null) is not false then 130 between -540 and case when false then -708 when true then true when false then false else 122 end % case -341 when true then true when false then true when 'kitten' then false when 'joey' then 'ape' when true then 'falcon' when 161 then 278 end when case 'sunfish' when case -747 when true then 'bonefish' when false then false when -505 then -383 end then false xor false when case when false then true when true then 'ewe' when true then true when false then true when false then 8 end then 'python' like 'maggot' when -140 not in (-643, -561, -236, 624, -270, 612) then false xor false when true xor false then true is false when case 'locust' when true then true when true then false when false then true when true then false end then case 'meerkat' when true then false when false then false when true then false when true then false when -498 then 347 else 'magpie' end end like 'tuna' then case when false then 51 when true then false when false then 'monarch' else 'coyote' end = case 'starfish' when 463 then 'skylark' when 249 then true when 'eagle' then true when false then true when -575 then -500 when -285 then 'doe' end xor false and false and 243 not between -519 and -62 end when -107 <= case when (true or false) and -411 not in (-285, -311) then false or true or false and true when not false is not false then case when true and false then false xor false when 'lab' like 'poodle' then 'stallion' when true and true then 'terrapin' <= 'starfish' when true xor false then false or true end when false and false and (false xor true) then -620 & 539 in (71, -37 >> -344, -8 | -87, case 183 when -97 then false when true then 'monkfish' else 729 end, case when true then false when false then false when true then -654 when false then false when false then true else -37 end) when (100 between -139 and 813) is not null then case case when false then false when true then 'cockatoo' when true then false else -140 end when -100 then not true when 'tarpon' then -844 in (-286, -313) when 114 then 447 not in (-273, -664, -311, 301) when case 'hyena' when true then true when false then 191 when false then true when false then false when true then 27 else 'goblin' end then case -672 when false then false when true then 195 end when 241 not between -415 and -778 then 94 end when 258 - -744 not in (-220 << -349, -198 & 40, 366) then 'bat' like 'cockatoo' when -543 not between 605 and -329 xor false is true then 231 in (-893, -280 ^ 422, -632, -171 >> -267, 15, -298 * -329) end then case when false xor true then not false when false and true then -238 - 489 end like 'kit' or case when false and true then 'sheepdog' like 'crow' when -270 in (631, -95, -109) then 'boar' = 'gecko' else case -200 when true then -265 when false then true when 521 then 'buck' when true then 402 when 8 then false when true then false else -15 end end in (-580, -243, -146) when (true and true and 'hagfish' != 'possum' and -521 div 111 <=> case 30 when false then 'labrador' when 'iguana' then true when true then false when true then 237 end) is not null then case (-580 | -33) / (-246 << 233) when 'killdeer' like 'amoeba' then 683 not in (-276, -396, -2, -816, -535) is null when case when false then 'filly' when false then 'boxer' when true then true else 'hedgehog' end != 'flea' then 115 when case when false then true when false then false when true then -90 when true then false when false then -325 when true then true end not between case when true then true when false then 'squirrel' when true then false else -462 end and 116 + -194 then -76 div 290 | case when true then 'cricket' when false then 202 when false then true when false then false when true then false end when not -201 not between -518 and -52 then -773 in (30, 159) xor false and false end not in (case case when false or true then false or false when false and false then false xor false when -4 != 44 then 'swift' != 'vervet' end when not 'liger' like 'cheetah' then 'jackal' when case when true then false when false then false when false then true when true then true else 'yak' end != 'buck' then 'amoeba' like 'ewe' xor -258 between -381 and -513 end, 903) when 113 between 39 and 108 / -936 | case 246 when true then true when true then false when 23 then false end & case -249 when true then 'sculpin' when true then 'heron' when 'wildcat' then false end then case when -311 >= -276 then -517 & -232 when true or false then 'viper' when not true then case 322 when false then false when false then 109 when 'mouse' then false when true then true when 'mite' then -350 else -335 end when 'krill' != 'platypus' then -265 in (240, -499, -568, -516) when not false then -859 in (-751, 60) when 'raven' like 'haddock' then case when false then true when true then false when false then false end end ^ (case -585 when false then 'tick' when true then false when 'glowworm' then -280 when false then -185 end | case 39 when true then 175 when true then false when true then 35 when false then true else 40 end) not in ((-462 + case -228 when false then 107 when 'orca' then true when true then false when false then false when 802 then 218 when true then false end) * (75 << 25), case when not false and (true or true) then case when false then 'loon' when false then -267 when true then false when false then false end not in (-137, case when true then false when true then true end) when case when false then 'swan' when false then 360 else 589 end between case 444 when false then true when false then -420 end and case when true then true when false then false when false then -297 when true then true end then 357 in (-386 | -26, case when true then false when true then false when true then 'shark' when false then true when false then true when false then 'cougar' end, -423, case -49 when false then true when 'eft' then false else -338 end) when (false or true) is null then not false or true is not false end, case when 118 not between 451 and -374 then 'panda' != 'louse' when false and true then -620 not in (-125, 742, -406, -351, 223, -468) when -811 between 143 and 234 then 'parakeet' like 'ladybird' end div case when not false then case when true then true when false then 'guppy' else 'viper' end when true or false then case when true then true when false then 623 when true then -414 when false then 'spaniel' when true then 'ocelot' else -319 end end, -222, -257) when -13 then case -181 when false then true when false then true when false then true else 97 end >> case 465 when true then false when false then false when false then false when false then 'lemming' when true then -175 end in (case when 228 < -370 then case 'yeti' when true then true when true then true else 'mustang' end when not false then false and false when 716 not in (-206, 21, -818, -99, 19, 454) then true xor true when -253 not between -243 and -336 then false or false end, case when not false then 728 >= -370 when true and false then not true when 325 in (-36, -111, -141, -531, 213, 87) then 'jawfish' end, case when true or false then true or false when true or false then 583 not between 535 and -228 when true or true then true and false when 'orca' != 'baboon' then true or false end, case when true or false then true and false when false is null then -423 not in (603, -54, 719, 45, -164, 5) when not true then 906 in (-745, 40, 145, 229, -111) when 347 != 63 then case 'stallion' when true then false when true then true when true then true when true then true when true then false else 'zebra' end when true xor false then 471 not in (85, -237, 17, 700, 125) else 655 * -610 end) is true when -45 | case when true then 74 when true then 'guinea' when true then 'primate' when false then false when false then 'loon' when false then false else 417 end != (250 | -164) << 230 xor 'cub' like 'shiner' then case case when false or false then not true when -338 > 118 then -330 when 2 not between 35 and -746 then 28 <= 755 when -727 not in (44, -545, 253, 553, -840) then case -472 when true then true when true then 361 when false then false when true then true when false then -914 end when 'alien' like 'tarpon' then -95 not between 288 and 304 else case when false then true when true then true else 70 end end << (case -11 when false then 221 when false then false when true then true end >> case when true then false when false then true when false then 'bluejay' when false then false when false then -34 end) when case when not false xor 245 not between 452 and 294 then -347 - -195 <=> case -26 when false then true when -622 then false when true then 398 when 'elephant' then true when true then false else 125 end when 215 - 322 > case 405 when true then 'eel' when false then false when false then 'griffon' end then 'ox' like 'mustang' when 'porpoise' like 'pelican' is not null then 'lemming' when 'orca' > 'garfish' xor false and true then not -429 not between 0 and -336 when not -78 >= 328 then 'alien' when case when true then true when false then 'clam' when false then -83 else 'gelding' end like case 'kiwi' when true then 'guinea' when false then -211 when true then false end then not not false end then case 389 when -347 then case case 167 when true then 'mastiff' when true then 'cockatoo' when true then true when false then false end when false is not null then true and true when -477 in (-793, 361, 916, 486) then 'filly' when -93 then true or true else case when false then true when true then true when true then true when true then false else 20 end end when not -86 > -911 then 'pheasant' < 'kiwi' when 'flounder' < case when false then false when true then 64 when true then true when false then 'garfish' when false then -27 else 'wolf' end then case when true then true when true then true when false then true when true then true end / -272 else -299 | 477 - 313 end when (not false) is true or -854 & -346 in (774 << -278, 536, -584, case when true then false when false then 859 when false then true when true then 'boxer' end, -133 & -135, case -780 when true then true when true then 9 when false then false when true then 'lamprey' end) then case 'locust' when false xor false or false and false then 'haddock' when case when true xor false then case -159 when false then false when 'seal' then false when true then true end when true is not true then 'tetra' like 'iguana' when not true then case when true then 'loon' when true then 29 end when -14 not in (-26, -232) then false or true when 287 >= -149 then 190 % -14 when false or false then 'ladybug' != 'ant' else 'labrador' end then false or false or true is null when -250 <=> 136 or 'chipmunk' != 'bison' then 145 div -391 + (-226 - 383) end else case when 607 not between case when false then true when false then true when false then -38 else 190 end and case when false then false when false then true end then case -114 when false then false when false then 'grub' end not between -145 & 567 and 639 + 259 when (-903 between 490 and 110) is not null then 23 - -350 < 282 when 143 >= 493 is not false then 414 >= 176 is not false when 'mudfish' like case when false then 'boar' when true then true when false then true when true then false when true then true end then 'anemone' like case when true then true when false then false else 'swan' end else 268 end end when not case when false and false then false xor true when 'snipe' = 'moth' then -555 when true and false then -81 between -341 and 180 when false or false then 'stork' > 'parrot' end like case when not true then 134 = 523 when not true then case 'dove' when true then 'shiner' when true then -129 when true then true when true then true when false then true else 'cheetah' end end then case when (172 >= 281 or 'cat' like 'colt') is not true then (true xor true) and true is false and 81 between case when false then 'crayfish' when false then -453 when true then -81 when false then true when true then false when true then false else 120 end and -159 when case when true xor false then 429 when not true then 'iguana' when 87 in (-295, -350, 700, -665) then 140 not between 51 and 273 when -82 in (-105, -595, 323, 476) then 'earwig' like 'seasnail' when true and false then true xor true when not true then -248 between -612 and 122 else case 'ray' when true then true when false then false else 'stallion' end end != 'ostrich' then 76 >= -527 and true is not true xor not 'mustang' like 'eagle' when not 174 >> 225 not in (-277, 153 ^ -315, -202, 534, 286 - -171) then 26 > -275 % 143 and (198 in (-691, 487, 314, -44, -64, -327) and -253 not in (423, 196, -91, 649)) when -8 * 363 not in (413, case when false then false when false then true when true then -115 else -183 end, 199) and (false is not false and 'sloth' >= 'bluegill') then -299 < case when true then -87 when true then false when false then 'tadpole' when false then false when false then true else -588 end xor -928 not in (case when true then -148 when true then false else 72 end, 650 * 451, 29 << -128, -415, case -642 when -25 then 'ladybug' when true then true when 'mutt' then 'midge' when true then false when 'lab' then 'chicken' when false then true end) when (-290 not in (-140, -63, -175, -135, 798, -90) or -479 between -7 and 477) xor (true and false xor (false xor true)) then 'orca' when -443 in (707, -648, 653 + 26 ^ 287, -42 / case -445 when true then false when false then false when 'tetra' then 69 when 276 then false when false then true when false then true end, 453, case -550 when true is null then -91 not in (-140, 537, 6) when -123 in (-389, 274, 585) then -557 in (-434, -313, -683, -439, -7) when false or true then 'leech' when 'griffon' then case when true then 126 when false then false when false then true when true then 'possum' when true then false when true then true end when case when true then true when false then true when true then true when true then false when true then false when true then 677 else 'polecat' end then true xor true when -170 not between 286 and 765 then case when true then true when true then 155 when true then true else 'buzzard' end else -27 end) then case when not 'meerkat' like 'bear' then 764 >> -330 between -45 and 81 << 377 when case 'cockatoo' when 'imp' then 'cockatoo' when false then 'duck' when 'yeti' then true when false then -401 when false then true else 'manatee' end like case 'racer' when false then true when true then false end then 276 >> 547 in (637 & -37, 361, -745, -176 * -715, 313, case when false then false when false then false when true then -69 when false then 147 else 270 end) when -679 & -235 not in (case when false then false when true then 792 when false then true when true then -144 end, -483 ^ 581, 602 >> -346, 348, case -624 when false then false when false then true end) then case case when false then false when false then false when true then true end when true or false then 'gazelle' like 'ostrich' when 'warthog' then true and true when 41 then 117 between 215 and 57 when true xor true then case 'koi' when true then -182 when true then 'colt' when true then true else 'grizzly' end when 310 then -510 >> 176 end when case when true then false when false then 'crappie' when false then 'stingray' else -463 end <= 733 / -202 then 'grizzly' end end else 'pika' end when (not true is null or -443 << 308 in (case when true then 'falcon' when true then -563 when true then 168 when false then false when true then false end, -713 div -261, case when false then 263 when true then false end, -208 << -368)) and (128 <= -131 and ('krill' like 'anteater' xor -393 <=> -141)) or case when case 'kid' when 'treefrog' then true when false then -61 when -68 then false when false then false when 286 then false end like 'lab' then case case 'gobbler' when true then true when 'piranha' then false when true then true when 432 then 581 when true then 456 end when -851 in (166, 258) then not false when 'bird' then false is not null when false is not false then not false when false is false then 'snail' when -365 between 41 and 467 then 438 between -491 and 145 when 'raven' like 'bluegill' then 'flounder' end when -439 * 402 < case when true then true when false then -183 end then 'snake' like 'spaniel' when case 'aphid' when false then 113 when true then false when true then 'swan' when true then true when false then -580 when 'halibut' then 'garfish' else 'lemming' end like case when true then false when true then true when false then false when false then true end then case -166 when 290 not between -304 and -256 then 'scorpion' like 'tiger' when 'vulture' like 'cockatoo' then false or false when false is not true then true xor false when 'snapper' then false is null else 236 / 700 end when (true and true) is true then 'dog' like 'anteater' is not false end < -761 then (case when case when false then false when true then false when true then 'tarpon' when false then true else -105 end != 287 then case -416 % -50 when false is not true then false or true when not true then false or false when 54 < -397 then 880 end when case when false then false when false then 'toad' else 'koi' end = 'fawn' then case when true is not true then 3 > 427 when not true then 166 not between -267 and -781 when true and false then case when true then false when true then true when false then true else 'seal' end end when case 499 when false then 'pig' when false then false end < -54 ^ -913 then 381 div -153 not between case -180 when false then true when 'lacewing' then -409 end and -493 when false and true and 372 in (-669, -702, 130, -966) then 'coyote' > 'midge' or (false or true) else 307 end between (-366 << -325) * 220 % case case -238 when 422 then true when true then 519 when false then false when true then true when 68 then false end when 'sunbird' = 'shrimp' then false or true when 'bat' <= 'mongrel' then -523 != 186 when case when true then true when true then false else 'teal' end then case 359 when 'lizard' then 587 when 414 then false end when 'mule' like 'weasel' then case 106 when true then -85 when 163 then true when false then 519 when 'hare' then 'drum' when -233 then false when true then true else 68 end end and 118) is not true when 271 >> case when case -162 when true then true when false then -182 when true then -212 when false then false when false then true end between -359 >> -306 and 239 then 226 div (554 << -661) when (413 not between -547 and -264) is not null then (107 not between -91 and -426) is not true end = 279 then case -316 - case when false then 'sloth' when false then 'troll' when false then false when false then false when false then false when true then true else 236 end when 264 > 549 << -147 then 'collie' like 'locust' when 'chipmunk' then not 51 <= 737 when case -475 when false then true when 'anchovy' then false when false then true when -319 then true when 'alien' then false when 461 then true end <=> 599 then 448 between 107 and -82 when case -113 >> 239 when case when false then 203 when true then -488 else 109 end then case 815 when -77 then 'beetle' when false then 'fawn' when true then true when -572 then false when 506 then 'caiman' when 'firefly' then true end when case when false then true when true then true when true then false else 'mongoose' end then false and true when false is true then 'skylark' like 'weevil' when 'sunbeam' like 'fly' then -279 between -133 and -50 end then -396 | 30 in (239 / -14, 466, 86 + 72, 290 ^ -776) when 368 = case -588 when 'unicorn' then true when true then true when true then true when 'ox' then -363 when true then false when true then true else -514 end then 39 >= -535 when (286 between -456 and 55) is null then 'gopher' else case when false xor true then -679 in (-117, -10, 771, 326, 614) when true and false then case 'boar' when 'cockatoo' then false when false then 'mongrel' when 'buzzard' then -69 when 296 then false when true then 'jennet' else 'feline' end when -547 = -115 then true xor false end end >> case when not not true then -608 <= 255 or 'moth' < 'eft' when true xor true or 'dove' != 'monarch' then 'wildcat' like 'badger' else 512 end & (-182 - -177) div (case when false then true when false then 'thrush' end ^ case when true then 6 when false then 224 end & -49 + 291 ^ -776) when -101 = case when case when true then true when true then false when true then false when true then true end like 'bluebird' then (false or true) is null when case 444 when true then true when true then 'wren' when 'minnow' then false when false then -290 else -97 end not between case 179 when 316 then 'molly' when 267 then false else 165 end and -225 then 'cub' < 'troll' or not true else -884 div -92 << case when true then true when false then 'bunny' else 111 end end or 'lynx' > case when not true then false or false when 'quagga' like 'seagull' then 16 >= 231 when -126 <=> 195 then -849 not between -332 and -35 when 504 > 28 then 'labrador' when not true then false xor false end is true then not case -363 when true then 'adder' when 'whale' then true when false then true when true then 'parrot' when true then false else -131 end % case 295 when false then 640 when false then false when true then 'sailfish' when -40 then false when false then false when false then 'lioness' else 232 end + case case when true then 'walrus' when false then true when false then 194 else 37 end when 'falcon' then 'bluebird' like 'penguin' when 796 in (525, -229, 264, -39) then false xor true when case -751 when true then -210 when true then false when true then 'horse' when true then false when 'louse' then 181 when true then -21 else 415 end then 63 in (32, 49, 134, 249) else case when false then true when false then 'eagle' when true then true when true then false when false then true when false then -368 else 7 end end <= case case case when true then -553 when false then true when false then 'osprey' when false then false when false then 'mako' when true then false end when 'tarpon' then not false when case when true then 695 when false then 'mite' when false then -200 else 'stallion' end then -717 <= -282 when true or true then -45 not in (-462, -3, 88) when 594 << 512 then true and true when 'fawn' <=> 'sheep' then case 'treefrog' when false then true when true then false when true then true when false then 447 end else -705 end when not (false and true) then case when true then true when false then true when true then false when false then -323 when false then false end in (case when false then 'bullfrog' when true then true when false then false when false then true when false then true when true then 17 else -322 end, -5 | 346, 88 * 287) when case when 'bobcat' like 'mastiff' then 'chicken' > 'doberman' when -50 <= 188 then case when false then true when true then false when false then true when true then false when false then -446 when false then 'koi' else -159 end end then false is true or false xor false when -44 then -272 when 'chow' then case case 'sawfish' when true then false when -588 then 378 when false then true when true then 'drake' when false then -501 when true then true end when -273 between -54 and -458 then -260 not in (-186, -488) when 237 then true or true when 'penguin' like 'chigger' then not true when true xor false then 0 not in (5, 392, -180) when false or false then -577 not in (57, 9, -174) when false and false then 'buffalo' else 'eft' end when 182 then 'joey' else case when -164 >= -613 then false or false when 510 <= -329 then 'gnat' when -295 > -148 then not false when false xor true then true or false when false xor false then -1 not in (616, -272, -296) when 'mongrel' >= 'shepherd' then -921 ^ -386 end end when case when case case 'urchin' when -99 then true when false then -24 when 'shiner' then 'gar' end when false xor false then 'wahoo' like 'moose' when 'cattle' < 'muskox' then case -307 when 'viper' then false when -87 then false when 318 then -261 when true then true when 'polliwog' then false else -158 end end like 'iguana' then case when 168 in (82 << -317, 279 + 120, case 32 when true then false when false then 'worm' end, -200 << 167, 627) then case when 'sunbird' like 'shiner' then 'starling' <=> 'mole' when true is not null then case 'rhino' when false then 184 when false then false when true then true else 'hare' end when true xor false then 'boar' when 456 not between 508 and -597 then -126 when true and true then case when false then false when false then true else 'labrador' end end when not (false xor true) then 202 in (-25, 341) and 'elf' like 'burro' when not false is false then case when true then true when true then true when false then false when true then true when false then true end like 'llama' when case 'platypus' when true then 'foxhound' when 'airedale' then false when false then false when false then true when false then false end like case 'perch' when false then true when 517 then false when false then false when true then 'chicken' when false then true when 'cicada' then false else 'honeybee' end then not not true when 'drum' like 'anchovy' and 'bobcat' like 'hermit' then case 'jawfish' when false then false when 'stinkbug' then true else 'koi' end > case 'mammoth' when 83 then -861 when true then -599 when 'muskrat' then true end when 'ibex' <= 'reindeer' then 'sloth' like 'snail' and not false end when case 'snake' when true then false when false then true when false then 'foal' else 'boar' end like 'yak' is not null then 'gull' end like case 'mastodon' when case 'mouse' when 'beetle' like case when false then 'moccasin' when true then true else 'jaybird' end then -433 when false or false or true xor false then 'chamois' <=> case 'lacewing' when 339 then true when false then true else 'raven' end when case when true then 'weasel' when false then true when false then true when true then true when true then true when true then 'cattle' end > case when false then false when true then false when true then 41 when false then true else 99 end then 'spider' when true and true or false and true then 21 not in (67 >> -592, -36, 917 % 245, 216 + -226, -205 >> 355) else 'opossum' end then case when 84 >= case when false then false when true then -802 when false then 'rodent' when false then false when false then true when true then true end then 726 << case when true then false when false then false when false then -405 when false then -208 when true then true when false then 'gazelle' else -105 end when not (false xor false) then (not false) is not false when -207 in (-78, 757, case when true then -750 when true then true when true then true else -379 end) then not 'bream' like 'ghoul' when 208 in (-637, 61, -623, -366) and 282 not between 503 and -405 then case 'hornet' when 260 then -257 when false then 'goshawk' when true then false else 'monitor' end <= 'ant' else case 'locust' when 'toucan' like 'sunbird' then 'hippo' > 'moccasin' when 123 in (-552, 395, -455, -677, 192) then 'gannet' like 'fish' when 164 between 350 and 218 then 'haddock' end end when case when 737 in (-834, -229) is null then case when true then true when true then false else 'ghoul' end = case when false then -723 when false then false end when 'penguin' like 'monkey' xor true and true then 'terrier' >= case 'koala' when 617 then true when false then true when 'troll' then false when true then -36 when true then false when false then false end when 'dingo' like 'fawn' then 212 between 401 and -747 when 'rat' like 'sailfish' or not true then -89 ^ (92 << -224) when 518 >> -181 in (-431 | 424, 194 - -557, 23, -392 >> -279, -187 - 86) then 'lobster' else case when true xor false then 213 not in (199, -198, 223, -24) when 'lionfish' like 'primate' then 'whippet' else case 'owl' when true then 'cod' when false then true when false then false when false then -403 when false then 'locust' end end end then -392 when 'reptile' then 530 not between case case -22 when true then true when false then false when 'arachnid' then true when 'herring' then false else 118 end when 'colt' <=> 'grizzly' then -458 < -346 when 336 < 236 then 'lacewing' = 'stallion' when 301 then 'imp' <= 'coyote' else case when true then false when true then 198 when true then true when true then true when true then 'wolf' else 43 end end and -839 when 'possum' then 'bird' <= case when not false then true or true when false or true then not false end end then case when 'lark' != 'gopher' then 'squid' like 'horse' when (-267 & -135) / case when false then true when false then false when true then false when false then true when true then true when false then false else -321 end not between case when 'satyr' like 'glider' then 334 not between 766 and -182 when 'cat' like 'aphid' then case 387 when true then true when false then false when true then 'mutt' else -580 end end and case case when false then true when false then true when false then 93 when false then 'lemur' else -168 end when true is true then false or false when true or false then 77 > -541 when case when true then true when true then true when false then 'pegasus' when false then -119 when true then false when false then true end then true and true else -69 end then case -415 when -100 then true when true then true when 659 then false when -391 then true when false then 'bird' end <=> -6 >> -796 is not true when not case when false then false when false then false when false then true when false then true when true then true when true then true end like 'puma' then (true is true and 'pangolin' like 'goblin') is not false when not -18 > -115 xor (false or false) and (true or true) then 'bengal' when 'orca' != 'cowbird' then case -92 when -561 not between -54 and -855 then 'koala' < 'roughy' when 134 ^ -370 then true xor true when 275 then 504 div 147 when 'muskrat' >= 'dinosaur' then 'sunbird' like 'humpback' when case when true then false when true then true when true then 'dassie' when true then 'chigger' when false then false end then not false end / (-555 + 78) end not between -378 and case case (5 + 128) * (-709 | -464) when 'mongoose' like 'slug' xor (true or true) then case case when false then false when false then 'seagull' else 'squirrel' end when 347 not in (-253, -347, -415, 431, 116, 365) then true or true when 'bulldog' <=> 'gecko' then 'frog' when not false then false or true when 'dolphin' then not false when 'monster' <= 'fish' then not false else 'insect' end when case when false then 'monitor' when false then 'gibbon' else -105 end + -226 then 869 else 233 * 239 end when case when -246 > -375 then false xor false when not false then 'goshawk' like 'crow' when 839 in (443, 421) then false is true else -159 ^ 294 end >= 468 then 'kitten' like 'sculpin' when 'liger' like case when -547 >= 69 then 'piglet' >= 'catfish' when 'weasel' like 'frog' then false is null when true is false then 272 << -428 when false and true then false or false when not false then false and true else 'lynx' end then case 'ant' when 'wolf' like 'tapir' or false xor false then not 111 not in (395, 310) when (true and true) is null then 'pheasant' like 'anchovy' when 456 not in (-478, 275, 424, -453, -156, 271) or -515 <= 543 then (false xor true) and -24 <=> -174 when not (false and true) then (false and true) is not false when 'fawn' like case when false then 'aardvark' when false then true when true then true when true then false else 'wombat' end then 587 else case when true or false then -78 + -519 when -435 not between -275 and 762 then 'polliwog' <= 'whippet' when true or true then case when true then true when false then true when true then 2 when true then 600 end when false and false then 'skylark' like 'crawdad' when 'clam' = 'owl' then false is null else 'deer' end end when case when false is not false then 'bee' >= 'dragon' when true xor false then false and true else 'macaque' end like case when false is not true then -485 not in (462, -396, -23, 364) when 'koi' != 'shrew' then not true when false is not null then -513 >= -248 when 553 not in (187, 521, 118, 336) then -107 in (-598, 491, 393, 468, 194, -555) else case when true then false when true then true when true then true when true then false end end then case when 572 between -235 and -281 then 413 & 210 when -219 in (29, 282, -643, 277, 410) then 50 not in (-682, -189, -85) when false or true then 'macaw' >= 'raccoon' when true and false then -305 in (-230, 98, 126, 149) when false and true then 145 <=> -527 when 218 != 200 then 'doe' <= 'rhino' end like 'sponge' when (673 + 348) / (64 % -313) in (case -64 when false then 'sturgeon' when false then false when true then 'tapir' end + 29, case when 'pigeon' like 'longhorn' then -131 in (-637, -181, -473) when false xor true then 246 ^ 113 when not true then false or true when -269 between -182 and 176 then false or true end, 749, case case 341 when false then 'possum' when true then true when -144 then true when false then true end when 'dolphin' then not true when -275 then -53 not between -140 and -513 end) then case case 'collie' when case when false then true when false then 700 else 'egret' end then case 249 when true then 'blowfish' when false then false end when true or false then case when true then 'lioness' when false then true when false then 'elf' when true then true when false then false else -471 end when case 'penguin' when false then 'barnacle' when 602 then true when true then 'dodo' when true then true when false then -454 when false then -326 end then 62 when 'chow' then true or false when 'leech' > 'emu' then 'ox' when false and true then true and false end when 'adder' then case when 127 < -395 then false and false when false and true then 415 when -257 between -212 and 611 then false or false end when -189 + -470 div -600 then case -444 << 88 when false or true then case -226 when true then false when true then 'reptile' when true then true else -584 end when case when false then true when true then false when true then -183 when true then true else -73 end then 388 not in (323, 266, -175, 237, -199, -655) when -33 <= 126 then case when false then true when false then true end when false and false then -776 not in (366, 286) end else 'bream' end end when 'muskrat' > case case 'moth' when case -251 when true then false when false then -887 when true then false when false then 541 when false then 'bluejay' when false then -142 else -110 end then 430 >= 104 when 'perch' = 'kiwi' then 'oyster' < 'phoenix' when false or false then case when false then 'unicorn' when false then 'gecko' when true then 221 when false then 317 when false then true when true then 142 end when case 'ghoul' when 'alien' then false when 143 then true when false then -60 else 'kitten' end then 530 <= -535 else 'dragon' end when 'crappie' like 'goat' or true is false then not 184 not between 463 and 9 when 510 not between -371 and -122 xor 'grackle' like 'grackle' then 577 when case 263 when true then true when true then 337 when -193 then true when true then 568 when true then false end + 350 / 507 then -298 & 394 when true xor true xor true is not null then -447 - -151 >= -117 & 394 end and -435 ^ 885 not between case when not 'chigger' <= 'locust' then case when not false then 146 in (-261, -78, 253, -320, 321) when 'unicorn' <= 'sculpin' then true is not true else case 'amoeba' when 'raccoon' then 'piglet' when 'chicken' then false when true then false else 'guppy' end end when 'finch' > 'man' is not true then 'yak' when case 'goose' when true then true when false then 'fowl' when true then true end like 'ibex' then (false or true) xor (true or true) end and case 352 & -326 << 490 when 424 / -60 <=> 688 then case -580 when 480 between 772 and -13 then 57 not between -408 and -390 when true or false then case when true then false when false then 'ox' when true then 'arachnid' when false then false else 'turtle' end when 'prawn' then true xor false when -362 = 789 then case when true then true when true then true else 'bonefish' end when 53 not in (-473, 180, 194, 402) then true xor true else -5 % -722 end when case when false then false when false then true end like case when false then 'catfish' when true then true when false then true when false then true when false then false end then not 273 between -92 and -153 when -32 then 'racer' > 'reptile' when (not true) is not null then case when true then false when true then true when true then false when true then -394 when true then true when false then false else -619 end in (case -284 when false then -32 when false then true when 329 then 42 end, -395 div 125, 334 & -397, case when false then false when false then false when false then true when true then false when true then true when true then true else -647 end, case -206 when 659 then true when true then true end, -192) when case 120 when true then 'cod' when true then 311 when 'oryx' then false end not in (-293 | -102, case 209 when true then true when false then true when true then 'collie' when false then false else 137 end, -113 ^ -301, -159 + 172) then (true and false) is false else case when false xor true then 'goshawk' != 'vervet' when 274 != -137 then 65 when 13 < 6 then 'barnacle' like 'marmot' when not false then 'grub' like 'boa' when false is true then -396 not between -171 and -643 end end then -204 >= case when not ('mule' like 'bulldog' and (true or true)) then case case when 'pipefish' like 'buzzard' then true is true when false and true then 'pika' != 'buzzard' when true is not null then -587 between 187 and -275 else 15 end when 'donkey' then case -668 when 'moccasin' then 'dinosaur' when 178 then 'crayfish' when 'rabbit' then 'gopher' when true then true when true then false else 22 end not in (-247, case when false then true when false then -253 when true then 107 end, case 80 when true then true when true then true when true then 497 end, case -125 when 'falcon' then -126 when 'mallard' then false when true then 'falcon' when false then false else 567 end, -238 + -58) when 337 <=> case -180 when false then -14 when 129 then 95 when false then true when 'oarfish' then true when true then 281 end then case -187 when true then true when 'gnu' then 'sponge' when true then -447 else 642 end % (769 - 839) when 'emu' then 'mammoth' when 26 << 216 >= -635 then 'finch' when -340 >= -690 is not null then case 603 when false then false when false then true when false then false end & 95 + 375 else 25 >> -329 / -849 end when ('sponge' != 'fish' or false and false) is true then case when true xor true xor true and false then not 'impala' like 'husky' when not 313 > 336 then 'newt' when case -276 when false then true when true then true else -525 end between case when false then 'caribou' when false then true when true then false when false then 'condor' when false then true end and 7 >> 35 then 404 * 113 >= -734 when -24 <=> case when false then true when true then 34 else -120 end then not (false and true) when not 44 != -712 then true and false xor 377 <=> -542 end when 59 not between case when true then true when true then 'dinosaur' when false then false when true then false end div 15 and -44 then case 56 when false then 243 when 'cod' then -566 end % case when false then 'mullet' when false then 'quail' end > 61 - case when false then false when false then -251 when false then -424 when false then 'salmon' when false then true when false then false end when -233 <=> -286 is false or case 'labrador' when true then 'escargot' when 'seasnail' then true else 'lioness' end like case 'bear' when 'badger' then true when true then true when false then true when true then false else 'bream' end then -599 else case 246 when -40 not in (-661, -649) and -665 between 368 and 247 then case when 'cougar' like 'javelin' then -772 - 78 when true is null then 215 not in (-154, -306, 312, -359) when false or true then case when false then 'barnacle' when false then true when true then 'starfish' when false then false end when not true then true is not null when false and false then 'tiger' like 'deer' end when 'trout' like 'parrot' then 'raccoon' else case when 381 not in (-270, 697) then 'mantis' like 'fawn' when not true then -472 + 600 when true and false then 378 ^ 257 when 'griffon' like 'coyote' then -68 between -41 and 353 when false or false then -114 = -105 end end end else 'macaw' end then 'parrot' like case when case 'panda' when false is not null then 537 when 817 + -67 then false or true when 63 not between 95 and 162 then 'eft' >= 'lark' when case 'collie' when false then 'crayfish' when 740 then true when 'buffalo' then false when true then false when false then false when false then 'perch' end then case when true then true when false then true when false then true when true then false when true then -475 end when -85 between 947 and 130 then true xor false end like 'monitor' then case case when false then true when false then 760 when true then true when true then 'penguin' when false then true when false then true else -79 end when -291 + -534 then -370 between -436 and -155 when false or false then -290 | 179 when -610 between 178 and 31 then not false when true and true then case when true then false when true then false when true then false when false then -12 else -4 end when not false then true and false else -225 end != case -660 when false then 281 when false then 'locust' when false then -116 when false then false else 56 end >> 425 when not 'swine' like 'bear' then case when 527 < -300 then -78 in (-29, 590, 140, 49) when -264 between -143 and 243 then case when true then true when false then false when true then 'egret' end when -351 in (-660, 290, -164, -280) then -4 not between 502 and 72 when not false then 60 end <= 'heron' end and case when 152 & -677 = 271 - -347 xor not 'labrador' >= 'walrus' then -569 when (not true or 104 < 23) xor 115 ^ 171 between 552 + 145 and -3 then 464 when case case when false then 'tahr' when false then -213 when true then false when false then 'ladybug' when false then false when false then false else 'bee' end when 'ram' then true or true when true xor true then false and false when case when false then -140 when false then true else 7 end then true is not null else 'heron' end = 'troll' then 'hound' like 'sawfly' or 111 not in (276, 28, 311, -87, 115, -725) or (-289 not between -160 and 52 or not true) when case 'man' when true then -254 when false then true when false then 'turtle' when -560 then true end != 'bull' and 741 * -269 >= 41 then case when not false then 'cub' <=> 'aardvark' when 'gopher' like 'katydid' then false is not true end in (400, 392 div 362 | -188) end < case when case case 281 when false then false when true then true when false then false when false then false when 'anemone' then true when false then true else -65 end when not true then case when true then true when true then false when true then true when true then true else -16 end when 598 then -121 < -87 when false xor true then 836 not between -667 and -388 else 326 + 551 end not between case when -211 = 36 then -142 & 40 when 'mongoose' like 'quagga' then true and true when true is true then 'cobra' when 'quetzal' > 'mutt' then true and true when false xor false then false is not null end and case -250 & 710 when 'tadpole' then case 415 when false then 'racer' when -278 then 'marten' when true then true when 'vervet' then false when false then 60 else -66 end when -197 not between 264 and 77 then true xor false when 'basilisk' then -130 % -854 when 'man' then 90 & -177 end then -121 not in (275, case when true then 'tadpole' when true then -379 when true then true when true then false when false then true end, case -133 when -836 then 'foal' when 761 then 438 when 37 then 'mako' end, 439 * -47, -141) or 112 not between -936 div -391 and -407 * -538 when 'aphid' <= 'marmot' xor case 153 when 'tuna' then false when false then -288 when 664 then true else -418 end not in (-274, case -713 when false then false when false then true when 138 then false when 'louse' then false else -31 end) then case when true or true then 'bobcat' like 'owl' when not true then 6 << 624 when 536 between -317 and -522 then true xor false when 'monarch' like 'stud' then 806 else -558 % 457 end not between case when true then true when true then true when false then false end | -527 * 163 and 220 when case -676 when true then true when -679 then false when 'duckling' then false when 'cicada' then false else 716 end = -629 and 'lamprey' >= case when true then 'crappie' when true then false when false then true when true then true when false then false when true then false end then 'insect' <=> 'stallion' is null or (false or true) and -398 != 131 when case 9 | -63 when case -301 when true then false when false then true when true then false when true then true else -321 end then true xor false when case when true then false when false then true when true then false when true then false when false then false else 'crawdad' end then 570 between 671 and 484 when not true then -385 not in (92, 568, 242) when 234 not between -91 and 5 then 200 / 607 when false and true then false xor true when false is false then false and false else -491 end in (-387, 292 % -424, -166 * (15 >> -153), case when false xor true then not true when true xor true then true and true when -212 = -697 then 'gazelle' like 'moose' else case when true then 'chow' when false then true when true then -254 end end, 442, -802) then case case when not false then case when true then false when true then false when false then true else -120 end when false or true then 'cowbird' when false is not false then case 318 when -429 then true when false then false when false then true end when 'joey' > 'loon' then false xor true end when not -2 between -443 and -115 then case 'killdeer' when false then 'snipe' when false then 726 when false then false when false then 557 when false then 'treefrog' when -204 then true end <= case 'ram' when true then 'barnacle' when 196 then true when true then true end when 19 not between 240 and 103 or true and false then 274 != -101 xor 'bream' like 'mantis' when 'camel' then 'oarfish' when -767 between 277 and 251 then -156 end else 37 end end"
	for i := 0; i < b.N; i++ {
		if _, err := Parse(quer); err != nil {
			b.Fatal(err)
		}
	}
}
