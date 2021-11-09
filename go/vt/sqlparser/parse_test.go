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
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type parseTest struct {
	input                string
	output               string
	serializeSelectExprs bool
}

var (
	validSQL = []parseTest{
		{
			input:  "select 1",
			output: "select 1 from dual",
		}, {
			input: "select 1 from t",
		}, {
			input: "select a, b from t",
		}, {
			input:  "select a,  b from t",
			output: "select a, b from t",
		}, {
			input:  "select a,b from t",
			output: "select a, b from t",
		}, {
			input:  "select `a`, `'b'` from t",
			output: "select a, `'b'` from t",
		}, {
			input:  `select "'ain't'", '"hello"' from t`,
			output: `select 'ain't', "hello" from t`,
		}, {
			input: `select "1" + "2" from t`,
		}, {
			input: `select '1' + "2" from t`,
		}, {
			input:  "select * from information_schema.columns",
			output: "select * from information_schema.`columns`",
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
			output: "select - -1 from t",
		}, {
			input:  "select -   -1 from t",
			output: "select -   -1 from t",
		}, {
			input:                "select - -1 from t",
			output:               "select 1 from t",
			serializeSelectExprs: true, // not a bug, we are testing that - -1 becomes 1
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
			input:                "select 1 --aa\nfrom t",
			output:               "select 1 from t",
			serializeSelectExprs: true,
		}, {
			input:                "select 1 #aa\nfrom t",
			output:               "select 1 from t",
			serializeSelectExprs: true,
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
			input:                "select /* \\0 */ '\\0' from a",
			serializeSelectExprs: true,
		}, {
			input:  "select /* \\0 */ '\\0' from a",
			output: "select /* \\0 */ \\0 from a",
		}, {
			input:                "select 1 /* drop this comment */ from t",
			output:               "select 1 from t",
			serializeSelectExprs: true,
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
			input: "with test as (select 1 from dual), test_two as (select 2 from dual) select * from test, test_two union all (with b as (with c as (select 1, 2 from dual) select * from c) select * from b)",
		}, {
			input:  "select /* union order by */ 1 from t union select 1 from t order by a",
			output: "select /* union order by */ 1 from t union select 1 from t order by a asc",
		}, {
			input:  "select /* union order by limit lock */ 1 from t union select 1 from t order by a limit 1 for update",
			output: "select /* union order by limit lock */ 1 from t union select 1 from t order by a asc limit 1 for update",
		}, {
			input:  "(select id, a from t order by id limit 1) union (select id, b as a from s order by id limit 1) order by a limit 1",
			output: "(select id, a from t order by id asc limit 1) union (select id, b as a from s order by id asc limit 1) order by a asc limit 1",
		}, {
			input: "select a from (select 1 as a from tbl1 union select 2 from tbl2) as t",
		}, {
			input: "select a from (select 1 as a from tbl1 union select 2 from tbl2) as t (a, b)",
		}, {
			input: "select a from (values row(1, 2), row('a', 'b')) as t (a, b)",
		}, {
			input: "select a from (values row(1, 2), row('a', 'b')) as t1 join (values row(3, 4), row('c', 'd')) as t2",
		}, {
			input: "select a from (values row(1, 2), row('a', 'b')) as t1 (w, x) join (values row(3, 4), row('c', 'd')) as t2 (y, z)",
		}, {
			// TODO: These forms are not yet supported due to a grammar conflict
			// 	input: "values row(1, 2), row('a', 'b')",
			// }, {
			// 	input: "values row(1, 2), row('a', 'b') union values row(3, 4), row('c', 'd')",
			// }, {
			input: "select * from t1 join (select * from t2 union select * from t3) as t",
		}, {
			// Ensure this doesn't generate: ""select * from t1 join t2 on a = b join t3 on a = b".
			input: "select * from t1 join t2 on a = b join t3",
		}, {
			input: "select * from t1 where col in (select 1 from dual union select 2 from dual)",
		}, {
			input: "select * from t1 where exists (select a from t2 union select b from t3)",
		}, {
			input: "select /* distinct */ distinct 1 from t",
		}, {
			input:  "select all col from t",
			output: "select col from t",
		}, {
			input:                "select /* straight_join */ straight_join 1 from t",
			serializeSelectExprs: true,
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
			input:  "select /* column alias */ a as b from t",
			output: "select /* column alias */ a as b from t",
		}, {
			input:                "select /* column alias */ a b from t",
			output:               "select /* column alias */ a as b from t",
			serializeSelectExprs: true,
		}, {
			input:  "select t.Date as Date from t",
			output: "select t.`Date` as `Date` from t",
		}, {
			input:  "select t.col as YeAr from t",
			output: "select t.col as `YeAr` from t",
		}, {
			input: "select /* column alias with as */ a as b from t",
		}, {
			input: "select /* keyword column alias */ a as `By` from t",
		}, {
			input:  "select /* column alias as string */ a as \"b\" from t",
			output: "select /* column alias as string */ a as b from t",
		}, {
			input:                "select /* column alias as string without as */ a \"b\" from t",
			output:               "select /* column alias as string without as */ a as b from t",
			serializeSelectExprs: true,
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
			input:                "select /* `By`.* */ `By`.* from t",
			serializeSelectExprs: true,
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
			input: "select /* use */ 1 from t1 as of '2019-01-01' use index (a) where b = 1",
		}, {
			input: "select /* keyword index */ 1 from t1 as of '2019-01-01' use index (`By`) where b = 1",
		}, {
			input: "select /* ignore */ 1 from t1 as of '2019-01-01' as t2 ignore index (a), t3 use index (b) where b = 1",
		}, {
			input: "select /* use */ 1 from t1 as of '2019-01-01' as t2 use index (a), t3 as of '2019-01-02' use index (b) where b = 1",
		}, {
			input: "select /* force */ 1 from t1 as of '2019-01-01' as t2 force index (a), t3 force index (b) where b = 1",
		}, {
			input:  "select /* table alias */ 1 from t as of '2019-01-01' t1",
			output: "select /* table alias */ 1 from t as of '2019-01-01' as t1",
		}, {
			input: "select /* table alias with as */ 1 from t as of '2019-01-01' as t1",
		}, {
			input:  "select /* string table alias */ 1 from t as of '2019-01-01' as 't1'",
			output: "select /* string table alias */ 1 from t as of '2019-01-01' as t1",
		}, {
			input:  "select /* string table alias without as */ 1 from t as of '2019-01-01' 't1'",
			output: "select /* string table alias without as */ 1 from t as of '2019-01-01' as t1",
		}, {
			input: "select /* keyword table alias */ 1 from t as of '2019-01-01' as `By`",
		}, {
			input: "select /* join */ 1 from t1 join t2",
		}, {
			input: "select /* join on */ 1 from t1 join t2 on a = b",
		}, {
			input: "select /* join */ 1 from t1 as of '2019-01-01' as t3 join t2 as t4",
		}, {
			input: "select /* join on */ 1 from t1 as of '2019-01-01' as t3 join t2 as of '2019-01-01' as t4 on a = b",
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
			input: "with cte1 as (select a from b) select * from cte1",
		}, {
			input: "with cte1 as (select a from b), cte2 as (select c from d) select * from cte1 join cte2",
		}, {
			input: "with cte1 (x, y) as (select a from b) select * from cte1",
		}, {
			input: "with cte1 (w, x) as (select a from b), cte2 (y, z) as (select c from d) select * from cte1 join cte2",
		}, {
			input: "with cte1 (w, x) as (select a from b) select a, (with cte2 (y, z) as (select c from d) select y from cte2) from cte1",
		}, {
			input: "with cte1 (w, x) as (select a from b) select a from cte1 join (with cte2 (y, z) as (select c from d) select * from cte2) as sub1 where a = b",
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
			input:  "select /* a.b */ `a`.`b` from t",
			output: "select /* a.b */ a.b from t",
		}, {
			input: "select /* a.b.c */ a.b.c from t",
		}, {
			input: "select /* keyword a.b */ `By`.`bY` from t",
		}, {
			input:  "select /* string */ 'a' from t",
			output: "select /* string */ a from t",
		}, {
			input:                "select /* double quoted string */ \"a\" from t",
			output:               "select /* double quoted string */ 'a' from t",
			serializeSelectExprs: true,
		}, {
			input:  "select /* double quoted string */ \"a\" from t",
			output: "select /* double quoted string */ a from t",
		}, {
			input:                "select /* quote quote in string */ 'a''a' from t",
			output:               "select /* quote quote in string */ 'a\\'a' from t",
			serializeSelectExprs: true,
		}, {
			input:                "select /* double quote quote in string */ \"a\"\"a\" from t",
			output:               "select /* double quote quote in string */ 'a\\\"a' from t",
			serializeSelectExprs: true,
		}, {
			input:                "select /* quote in double quoted string */ \"a'a\" from t",
			output:               "select /* quote in double quoted string */ 'a\\'a' from t",
			serializeSelectExprs: true,
		}, {
			input:  "select /* quote in double quoted string */ \"a'a\" from t",
			output: "select /* quote in double quoted string */ a'a from t",
		}, {
			input:  "select /* backslash quote in string */ 'a\\'a' from t",
			output: "select /* backslash quote in string */ a\\'a from t",
		}, {
			input:  "select /* literal backslash in string */ 'a\\\\na' from t",
			output: "select /* literal backslash in string */ a\\\\na from t",
		}, {
			input:  "select /* all escapes */ '\\0\\'\\\"\\b\\n\\r\\t\\Z\\\\' from t",
			output: "select /* all escapes */ \\0\\'\\\"\\b\\n\\r\\t\\Z\\\\ from t",
		}, {
			input:                "select /* non-escape */ '\\x' from t",
			output:               "select /* non-escape */ 'x' from t",
			serializeSelectExprs: true,
		}, {
			input:  "select /* non-escape */ '\\x' from t",
			output: "select /* non-escape */ \\x from t",
		}, {
			input:  "select /* unescaped backslash */ '\n' from t",
			output: "select /* unescaped backslash */ \n from t",
		}, {
			input:  "select /* escaped backslash */ '\\n' from t",
			output: "select /* escaped backslash */ \\n from t",
		}, {
			input: "select /* value argument */ :a from t",
		}, {
			input: "select /* value argument with digit */ :a1 from t",
		}, {
			input: "select /* value argument with dot */ :a.b from t",
		}, {
			input:                "select /* positional argument */ ? from t",
			output:               "select /* positional argument */ :v1 from t",
			serializeSelectExprs: true,
		}, {
			input: "select /* positional argument */ ? from t",
		}, {
			input:                "select /* positional argument */ ? from t limit ?",
			output:               "select /* positional argument */ :v1 from t limit :v2",
			serializeSelectExprs: true,
		}, {
			input:                "select /* multiple positional arguments */ ?, ? from t",
			output:               "select /* multiple positional arguments */ :v1, :v2 from t",
			serializeSelectExprs: true,
		}, {
			input: "select /* list arg */ * from t where a in ::list",
		}, {
			input: "select /* list arg not in */ * from t where a not in ::list",
		}, {
			input: "select /* null */ null from t",
		}, {
			input: "select /* octal */ 010 from t",
		}, {
			input:                "select /* hex */ x'f0A1' from t",
			output:               "select /* hex */ X'f0A1' from t",
			serializeSelectExprs: true,
		}, {
			input: "select /* hex */ x'f0A1' from t",
		}, {
			input: "select /* hex caps */ X'F0a1' from t",
		}, {
			input:                "select /* bit literal */ b'0101' from t",
			output:               "select /* bit literal */ B'0101' from t",
			serializeSelectExprs: true,
		}, {
			input: "select /* bit literal */ b'0101' from t",
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
			input: "select /* limit a */ 1 from t limit 3",
		}, {
			input: "select /* limit a,b */ 1 from t limit 4, 5",
		}, {
			input:                "select /* binary unary */ a- -b from t",
			output:               "select /* binary unary */ a - -b from t",
			serializeSelectExprs: true,
		}, {
			input: "select /* binary unary */ a- -b from t",
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
			input: "select /* TIMESTAMPADD */ TIMESTAMPADD(MINUTE, 1, '2008-01-04') from t",
		}, {
			input: "select /* TIMESTAMPDIFF */ TIMESTAMPDIFF(MINUTE, '2008-01-02', '2008-01-04') from t",
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
			input:                "select * from (select 'tables') tables",
			output:               "select * from (select 'tables' from dual) as `tables`",
			serializeSelectExprs: true,
		}, {
			input:  "select * from (select 'tables') tables",
			output: "select * from (select tables from dual) as `tables`",
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
			input: "insert /* bool in on duplicate */ into a values (1, 2, 3) on duplicate key update b = values(b), c = d",
		}, {
			input: "insert /* bool in on duplicate */ into a values (1, 2, 3) on duplicate key update b = values(a.b), c = d",
		}, {
			input: "insert /* bool expression on duplicate */ into a values (1, 2) on duplicate key update b = func(a), c = a > d",
		}, {
			input:  "insert into A(A, B) values (';', '''')",
			output: "insert into A(A, B) values (';', '\\'')",
		}, {
			input:  "CREATE TABLE A (\n\t`A` int\n)",
			output: "create table A (\n\tA int\n)",
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
			input: "update /* limit */ a set b = 3 limit 100",
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
			input: "delete /* limit */ from a limit 100",
		}, {
			input: "delete a from a join b on a.id = b.id where b.name = 'test'",
		}, {
			input: "delete a, b from a, b where a.id = b.id and b.name = 'test'",
		}, {
			input:  "delete from a1, a2 using t1 as a1 inner join t2 as a2 where a1.id=a2.id",
			output: "delete a1, a2 from t1 as a1 join t2 as a2 where a1.id = a2.id",
		}, {
			input: "savepoint abc",
		}, {
			input:  "savepoint `ab_cd`",
			output: "savepoint ab_cd",
		}, {
			input: "rollback to abc",
		}, {
			input:  "rollback work to abc",
			output: "rollback to abc",
		}, {
			input:  "rollback to savepoint abc",
			output: "rollback to abc",
		}, {
			input:  "rollback work to savepoint abc",
			output: "rollback to abc",
		}, {
			input:  "rollback work to savepoint `ab_cd`",
			output: "rollback to ab_cd",
		}, {
			input: "release savepoint abc",
		}, {
			input:  "release savepoint `ab_cd`",
			output: "release savepoint ab_cd",
		}, {
			input: "set /* simple */ a = 3",
		}, {
			input: "set #simple\n @b = 4",
		}, {
			input: "set #simple\n b = 4",
		}, {
			input: "set character_set_results = utf8",
		}, {
			input:  "set @@session.autocommit = true",
			output: "set session autocommit = true",
		}, {
			input:  "set @@session.`autocommit` = true",
			output: "set session `autocommit` = true",
		}, {
			input:  "set @@session.autocommit = ON",
			output: "set session autocommit = 'ON'",
		}, {
			input:  "set @@session.autocommit= OFF",
			output: "set session autocommit = 'OFF'",
		}, {
			input:  "set session autocommit = ON",
			output: "set session autocommit = 'ON'",
		}, {
			input:  "set global autocommit = OFF",
			output: "set global autocommit = 'OFF'",
		}, {
			input:  "set @@global.optimizer_prune_level = 1",
			output: "set global optimizer_prune_level = 1",
		}, {
			input: "set global optimizer_prune_level = 1",
		}, {
			input:  "set @@persist.optimizer_prune_level = 1",
			output: "set persist optimizer_prune_level = 1",
		}, {
			input: "set persist optimizer_prune_level = 1",
		}, {
			input:  "set @@persist_only.optimizer_prune_level = 1",
			output: "set persist_only optimizer_prune_level = 1",
		}, {
			input: "set persist_only optimizer_prune_level = 1",
		}, {
			input:  "set @@local.optimizer_prune_level = 1",
			output: "set session optimizer_prune_level = 1",
		}, {
			input:  "set local optimizer_prune_level = 1",
			output: "set session optimizer_prune_level = 1",
		}, {
			input:  "set @@optimizer_prune_level = 1",
			output: "set session optimizer_prune_level = 1",
		}, {
			input: "set session optimizer_prune_level = 1",
		}, {
			input:  "set @@optimizer_prune_level = 1, @@global.optimizer_search_depth = 62",
			output: "set session optimizer_prune_level = 1, global optimizer_search_depth = 62",
		}, {
			input:  "set @@GlObAl.optimizer_prune_level = 1",
			output: "set global optimizer_prune_level = 1",
		}, {
			input: "set @user.var = 1",
		}, {
			input: "set @user.var.name = 1",
		}, {
			input:  "set autocommit = on",
			output: "set autocommit = 'on'",
		}, {
			input:  "set autocommit = off",
			output: "set autocommit = 'off'",
		}, {
			input:  "set autocommit = off, foo = 1",
			output: "set autocommit = 'off', foo = 1",
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
			input: "signal some_condition_name",
		}, {
			input: "signal sqlstate value '45000'",
		}, {
			input:  "signal sqlstate '45000'",
			output: "signal sqlstate value '45000'",
		}, {
			input: "signal sqlstate value '45000' set message_text = 'ouch!'",
		}, {
			input: "signal sqlstate value '45000' set class_origin = 'abc', subclass_origin = 'def', message_text = 'ghi', " +
				"mysql_errno = 123, constraint_catalog = 'jkl', constraint_schema = 'mno', constraint_name = 'pqr', " +
				"catalog_name = 'stu', schema_name = 'vwx', table_name = 'yz0', column_name = '123', cursor_name = '456'",
		}, {
			input: "signal some_condition_name set message_text = 'the text of the best'",
		}, {
			input: "signal some_condition_name set class_origin = 'abc', subclass_origin = 'def', message_text = 'ghi', " +
				"mysql_errno = 123, constraint_catalog = 'jkl', constraint_schema = 'mno', constraint_name = 'pqr', " +
				"catalog_name = 'stu', schema_name = 'vwx', table_name = 'yz0', column_name = '123', cursor_name = '456'",
		}, {
			input: "resignal",
		}, {
			input: "resignal some_condition_name",
		}, {
			input: "resignal sqlstate value '45000'",
		}, {
			input:  "resignal sqlstate '45000'",
			output: "resignal sqlstate value '45000'",
		}, {
			input: "resignal sqlstate value '45000' set message_text = 'ouch!'",
		}, {
			input: "resignal sqlstate value '45000' set class_origin = 'abc', subclass_origin = 'def', message_text = 'ghi', " +
				"mysql_errno = 123, constraint_catalog = 'jkl', constraint_schema = 'mno', constraint_name = 'pqr', " +
				"catalog_name = 'stu', schema_name = 'vwx', table_name = 'yz0', column_name = '123', cursor_name = '456'",
		}, {
			input: "resignal some_condition_name set message_text = 'the text of the best'",
		}, {
			input: "resignal some_condition_name set class_origin = 'abc', subclass_origin = 'def', message_text = 'ghi', " +
				"mysql_errno = 123, constraint_catalog = 'jkl', constraint_schema = 'mno', constraint_name = 'pqr', " +
				"catalog_name = 'stu', schema_name = 'vwx', table_name = 'yz0', column_name = '123', cursor_name = '456'",
		}, {
			input: "resignal set message_text = 'the text of the best'",
		}, {
			input: "resignal set class_origin = 'abc', subclass_origin = 'def', message_text = 'ghi', " +
				"mysql_errno = 123, constraint_catalog = 'jkl', constraint_schema = 'mno', constraint_name = 'pqr', " +
				"catalog_name = 'stu', schema_name = 'vwx', table_name = 'yz0', column_name = '123', cursor_name = '456'",
		}, {
			input:  "alter ignore table a add foo int",
			output: "alter table a add column (\n\tfoo int\n)",
		}, {
			input:  "alter table a add foo int",
			output: "alter table a add column (\n\tfoo int\n)",
		}, {
			input:  "alter table a add spatial key foo (column1)",
			output: "alter table a add spatial index foo (column1)",
		}, {
			input:  "alter table a add unique key foo (column1)",
			output: "alter table a add unique index foo (column1)",
		}, {
			input:  "alter table `By` add foo int",
			output: "alter table `By` add column (\n\tfoo int\n)",
		}, {
			input:  "alter table a drop foo",
			output: "alter table a drop column foo",
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
			output: "alter table a rename to b",
		}, {
			input:  "alter table `By` rename `bY`",
			output: "alter table `By` rename to `bY`",
		}, {
			input: "alter table a rename to b",
		}, {
			input:  "alter table a rename as b",
			output: "alter table a rename to b",
		}, {
			input:  "alter table a rename index foo to bar",
			output: "alter table a rename index foo to bar",
		}, {
			input:  "alter table a rename key foo to bar",
			output: "alter table a rename index foo to bar",
		}, {
			input:  "alter table e auto_increment = 20",
			output: "alter table e",
		}, {
			input:  "alter table e auto_increment 20",
			output: "alter table e",
		}, {
			input:  "alter table e auto_increment = 20.0",
			output: "alter table e",
		}, {
			input:  "alter table e auto_increment 20.0",
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
			output: "alter table a add column (\n\tid int\n)",
		}, {
			input: "alter table a add index idx (id)",
		}, {
			input: "alter table a add fulltext index idx (id)",
		}, {
			input: "alter table a add spatial index idx (id)",
		}, {
			input:  "alter table a add foreign key (x) references y(z)",
			output: "alter table a add foreign key (x) references y (z)",
		}, {
			input: "alter table a add primary key (a, b)",
		}, {
			input: "alter table a add constraint a_pk primary key (a, b)",
		}, {
			input: "alter table a add constraint a_pk primary key (value)",
		}, {
			input: "alter table a add primary key (value)",
		}, {
			input: "alter table a drop primary key",
		}, {
			input: "alter table a drop column id",
		}, {
			input:  "alter table a drop partition p2712",
			output: "alter table a",
		}, {
			input: "alter table a drop index idx",
		}, {
			input:  "alter table a add constraint check (b > 0)",
			output: "alter table a add check (b > 0)",
		}, {
			input:  "alter table a add constraint check (b > 0) enforced",
			output: "alter table a add check (b > 0)",
		}, {
			input:  "alter table a add constraint check (b > 0) not enforced",
			output: "alter table a add check (b > 0) not enforced",
		}, {
			input: "alter table a add constraint ch_1 check (b > 0)",
		}, {
			input:  "alter table a add constraint ch_1 check (b > 0) enforced",
			output: "alter table a add constraint ch_1 check (b > 0)",
		}, {
			input: "alter table a add constraint ch_1 check (b > 0) not enforced",
		}, {
			input: "alter table a add check (b > 0)",
		}, {
			input: "alter table a drop check ch_1",
		}, {
			input: "alter table a drop check status",
		}, {
			input: "alter table a drop constraint status",
		}, {
			input: "alter table a drop foreign key fk_something",
		}, {
			input: "alter table a drop constraint b",
		}, {
			input:  "alter table a drop id",
			output: "alter table a drop column id",
		}, {
			input: "create table a",
		}, {
			input:  "create table a (\n\t`a` int\n)",
			output: "create table a (\n\ta int\n)",
		}, {
			input: "create table `by` (\n\t`by` char\n)",
		}, {
			input:  "create table if not exists a (\n\t`a` int\n)",
			output: "create table if not exists a (\n\ta int\n)",
		}, {
			input:  "create table a ignore me this is garbage",
			output: "create table a",
		}, {
			input:  "create table a (a int, b char, c garbage)",
			output: "create table a",
		}, {
			input: "alter table a rename column a to b",
		}, {
			input:  "alter table a rename column a as b",
			output: "alter table a rename column a to b",
		}, {
			input:  "create table a (b1 bool not null primary key, b2 boolean not null)",
			output: "create table a (\n\tb1 bool not null primary key,\n\tb2 boolean not null\n)",
		}, {
			input:  "create temporary table a (b1 bool not null primary key, b2 boolean not null)",
			output: "create temporary table a (\n\tb1 bool not null primary key,\n\tb2 boolean not null\n)",
		}, {
			input:  "create temporary table if not exists a (\n\t`a` int\n)",
			output: "create temporary table if not exists a (\n\ta int\n)",
		}, {
			input:  "create index a on b (id)",
			output: "alter table b add index a (id)",
		}, {
			input:  "create index a on b (foo(6) desc, foo asc)",
			output: "alter table b add index a (foo(6) desc, foo)",
		}, {
			input:  "create unique index a on b (id)",
			output: "alter table b add unique index a (id)",
		}, {
			input:  "create unique index a using btree on b (id)",
			output: "alter table b add unique index a using btree (id)",
		}, {
			input:  "create fulltext index a using btree on b (id)",
			output: "alter table b add fulltext index a using btree (id)",
		}, {
			input:  "create spatial index a using btree on b (id)",
			output: "alter table b add spatial index a using btree (id)",
		}, {
			input:  "create view a as select current_timestamp()",
			output: "create view a as select current_timestamp() from dual",
		}, {
			input:  "create view a_view as select * from table_1 join table_2 on table_1.table_2_id_fk = table_2.id where city = 'my city'",
			output: "create view a_view as select * from table_1 join table_2 on table_1.table_2_id_fk = table_2.id where city = 'my city'",
		}, {
			input:  "create or replace view a as select current_timestamp()",
			output: "create or replace view a as select current_timestamp() from dual",
		}, {
			input: "create trigger t1 before update on foo for each row precedes bar update xxy set baz = 1 where a = b",
		}, {
			input: "create trigger t1 after delete on foo for each row delete from xxy where old.y = z",
		}, { //TODO: figure out why `SET SESSION sys_var = x` does not work when set directly on the trigger (works in BEGIN/END block)
			input:  "create trigger t1 after delete on foo for each row set @@sum = @@sum + old.b",
			output: "create trigger t1 after delete on foo for each row set session sum = @@sum + old.b",
		}, {
			input: "create trigger t1 before insert on foo for each row set new.x = new.x + 1",
		}, {
			input: "create trigger t1 after insert on foo for each row update xxy set y = new.x",
		}, {
			input: "create trigger t1 before delete on foo for each row follows baz update xxy set x = old.y",
		}, {
			input:  "create definer = me trigger t1 before delete on foo for each row follows baz update xxy set x = old.y",
			output: "create trigger t1 before delete on foo for each row follows baz update xxy set x = old.y",
		}, {
			input:  "create definer=me trigger t1 before delete on foo for each row follows baz update xxy set x = old.y",
			output: "create trigger t1 before delete on foo for each row follows baz update xxy set x = old.y",
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
			output: "drop view a",
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
			input:  "drop table a cascade",
			output: "drop table a",
		}, {
			input:  "drop table b restrict",
			output: "drop table b",
		}, {
			input:  "drop table b        ",
			output: "drop table b",
		}, {
			input:  "drop view if exists a",
			output: "drop view if exists a",
		}, {
			input:  "drop index b on a",
			output: "alter table a drop index b",
		}, {
			input:  "analyze table a",
			output: "alter table a",
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
			input:  "show charset where `Charset` like 'utf8'",
			output: "show charset where `Charset` like 'utf8'",
		}, {
			input:  "show collation",
			output: "show collation",
		}, {
			input:  "show collation where `Charset` = 'utf8' and `Collation` = 'utf8_bin'",
			output: "show collation where `Charset` = 'utf8' and `Collation` = 'utf8_bin'",
		}, {
			input:  "show collation like 'utf8%'",
			output: "show collation where `collation` like 'utf8%'",
		}, {
			input: "show create database d",
		}, {
			input: "show create schema d",
		}, {
			input: "show create database if not exists d",
		}, {
			input: "show create schema if not exists d",
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
			output: "show create trigger t",
		}, {
			input:  "show create user u",
			output: "show create user",
		}, {
			input:  "show create view v",
			output: "show create view v",
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
			input: "show function status",
		}, {
			input: "show function status where Name = 'hi'",
		}, {
			input: "show function status like 'hi'",
		}, {
			input:  "show grants for 'root@localhost'",
			output: "show grants",
		}, {
			input: "show index from tbl",
		}, {
			input:  "show indexes from tbl",
			output: "show index from tbl",
		}, {
			input:  "show keys from tbl",
			output: "show index from tbl",
		}, {
			input:  "show index in tbl",
			output: "show index from tbl",
		}, {
			input:  "show indexes in tbl",
			output: "show index from tbl",
		}, {
			input:  "show keys in tbl",
			output: "show index from tbl",
		}, {
			input: "show index from tbl from db",
		}, {
			input:  "show indexes from tbl from db",
			output: "show index from tbl from db",
		}, {
			input:  "show keys from tbl from db",
			output: "show index from tbl from db",
		}, {
			input:  "show index in tbl in db",
			output: "show index from tbl from db",
		}, {
			input:  "show indexes in tbl in db",
			output: "show index from tbl from db",
		}, {
			input:  "show keys in tbl in db",
			output: "show index from tbl from db",
		}, {
			input: "show index from tbl where Key_name = 'key'",
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
			input: "show procedure status",
		}, {
			input: "show procedure status where Name = 'hi'",
		}, {
			input: "show procedure status like 'hi'",
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
			output: "show table status",
		}, {
			input:  "show table status from mydb",
			output: "show table status from mydb",
		}, {
			input:  "show table status from mydb LIKE 't1'",
			output: "show table status from mydb like 't1'",
		}, {
			input:  "show table status LIKE 't1'",
			output: "show table status like 't1'",
		}, {
			input:  "show table status where name='t1'",
			output: "show table status where name = 't1'",
		}, {
			input: "show tables",
		}, {
			input: "show tables as of 123",
		}, {
			input: "show tables like '%keyspace%'",
		}, {
			input: "show tables as of 123 like '%keyspace%'",
		}, {
			input: "show tables where 1 = 0",
		}, {
			input: "show tables as of 'abc' where 1 = 0",
		}, {
			input: "show tables from a",
		}, {
			input: "show tables from a as of 'abc'",
		}, {
			input: "show tables from a where 1 = 0",
		}, {
			input: "show tables from a as of 123 where 1 = 0",
		}, {
			input: "show tables from a like '%keyspace%'",
		}, {
			input: "show tables from a as of 'abc' like '%keyspace%'",
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
			input: "show triggers from dbname",
		}, {
			input:  "show triggers in dbname",
			output: "show triggers from dbname",
		}, {
			input: "show triggers like 'pattern'",
		}, {
			input: "show triggers where v = 'x'",
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
			input:  "show warnings",
			output: "show warnings",
		}, {
			input:                "select warnings from t",
			output:               "select `warnings` from t",
			serializeSelectExprs: true,
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
			input:  "describe foobar",
			output: "show columns on foobar",
		}, {
			input:  "desc foobar",
			output: "show columns on foobar",
		}, {
			input: "explain select * from foobar",
		}, {
			input: "explain format = tree select * from foobar",
		}, {
			input: "explain analyze select * from foobar",
		}, {
			input: "explain update foobar set foo = bar",
		}, {
			input: "explain delete from foobar where foo = bar",
		}, {
			input: "explain insert into foobar values (1, 2, 3)",
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
			input:                "select k collate latin1_german2_ci as k1 from t1 order by k1 asc",
			serializeSelectExprs: true,
		}, {
			input: "select * from t group by a collate utf8_general_ci",
		}, {
			input: "select MAX(k collate latin1_german2_ci) from t1",
		}, {
			input: "select MaX(k collate latin1_german2_ci) from t1",
		}, {
			input: "select MAX(distinct k) from t1",
		}, {
			input:  "select MAX(distinct k) as min from t1",
			output: "select MAX(distinct k) as `min` from t1",
		}, {
			input:  "select MIn(distinct k) as Max from t1",
			output: "select MIn(distinct k) as `Max` from t1",
		}, {
			input: "select avg(distinct k) from t1",
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
			input:                "select k collate 'latin1_german2_ci' as k1 from t1 order by k1 asc",
			output:               "select k collate latin1_german2_ci as k1 from t1 order by k1 asc",
			serializeSelectExprs: true,
		}, {
			input:  "select /* drop trailing semicolon */ 1 from dual;",
			output: "select /* drop trailing semicolon */ 1 from dual",
		}, {
			input:  "select /* cache directive */ sql_no_cache 'foo' from t",
			output: "select /* cache directive */ sql_no_cache foo from t",
		}, {
			input:  "select /* sql_calc_rows directive */ sql_calc_found_rows 'foo' from t",
			output: "select /* sql_calc_rows directive */ sql_calc_found_rows foo from t",
		}, {
			input:  "select /* cache and sql_calc_rows directive */ sql_no_cache sql_calc_found_rows 'foo' from t",
			output: "select /* cache and sql_calc_rows directive */ sql_no_cache sql_calc_found_rows foo from t",
		}, {
			input: "select binary 'a' = 'A' from t",
		}, {
			input: "select 1 from t where foo = _binary 'bar'",
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
			input: "select title from video as v where match(v.title, v.tag) against ('DEMO' in boolean mode)",
		}, {
			input: "select name, group_concat(score) from t group by name",
		}, {
			input: `select concAt(  "a",    "b", "c"  ) from t group by name`,
		}, {
			input: "select name, group_concat(distinct id, score order by id desc separator ':') from t group by name",
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
			input: "select name, dense_rank() over () from t",
		}, {
			input:  "select name, avg(a) over (partition by b) as avg from t",
			output: "select name, avg(a) over (partition by b) as `avg` from t",
		}, {
			input: "select name, bit_and(a) over (partition by b) from t",
		}, {
			input: "select name, bit_or(a) over (partition by b) from t",
		}, {
			input: "select name, bit_xor(a) over (partition by b) from t",
		}, {
			input: "select name, count(distinct a) over (partition by b) from t",
		}, {
			input:  "select name, count(a) over (partition by b) as count from t",
			output: "select name, count(a) over (partition by b) as `count` from t",
		}, {
			input: "select name, json_arrayagg(a) over (partition by b) from t",
		}, {
			input: "select name, json_objectagg(a) over (partition by b) from t",
		}, {
			input: "select name, max(a) over (partition by b) from t",
		}, {
			input: "select name, min(a) over (partition by b) from t",
		}, {
			input: "select name, stddev_pop(a) over (partition by b) from t",
		}, {
			input: "select name, stddev(a) over (partition by b) from t",
		}, {
			input: "select name, std(a) over (partition by b) from t",
		}, {
			input: "select name, stddev_samp(a) over (partition by b) from t",
		}, {
			input: "select name, sum(a) over (partition by b) from t",
		}, {
			input:  "select name, sum(distinct a) over (partition by b) as SUM from t",
			output: "select name, sum(distinct a) over (partition by b) as `SUM` from t",
		}, {
			input: "select name, var_pop(a) over (partition by b) from t",
		}, {
			input: "select name, variance(a) over (partition by b) from t",
		}, {
			input: "select name, cume_dist() over (partition by b) from t",
		}, {
			input: "select name, cume_dist() over (partition by b) - 1 in (1, 2) as included from t",
		}, {
			input: "select name, cume_dist() over (partition by b) = dense_rank() over () as included from t",
		}, {
			input: "select name, dense_rank() over (partition by b) from t",
		}, {
			input: "select name, first_value(a) over (partition by b) from t",
		}, {
			input: "select name, lag(a) over (partition by b) from t",
		}, {
			input: "select name, last_value(a) over (partition by b) from t",
		}, {
			input: "select name, lead(a) over (partition by b) from t",
		}, {
			input: "select name, nth_value(a) over (partition by b) from t",
		}, {
			input: "select name, ntile() over (partition by b) from t",
		}, {
			input: "select name, percent_rank() over (partition by b) from t",
		}, {
			input: "select name, rank() over (partition by b) from t",
		}, {
			input: "select name, row_number() over (partition by b) from t",
		}, {
			input: "select name, dense_rank() over (partition by b) from t",
		}, {
			input: "select name, dense_rank() over (partition by b order by c asc) from t",
		}, {
			input: "select name, cume_dist() over (partition by b order by c asc) from t",
		}, {
			input: "select name, first_value(a) over (partition by b order by c asc) from t",
		}, {
			input: "select name, lag(a) over (partition by b order by c asc) from t",
		}, {
			input: "select name, last_value(a) over (partition by b order by c asc) from t",
		}, {
			input: "select name, lead(a) over (partition by b order by c asc) from t",
		}, {
			input: "select name, nth_value(a) over (partition by b order by c asc) from t",
		}, {
			input: "select name, ntile() over (partition by b order by c asc) from t",
		}, {
			input: "select name, percent_rank() over (partition by b order by c asc) from t",
		}, {
			input: "select name, rank() over (partition by b order by c asc) from t",
		}, {
			input: "select name, row_number() over (partition by b order by c asc) from t",
		}, {
			input: "select name, dense_rank() over (order by b) from t",
		}, {
			input: "select name, dense_rank() over (partition by b order by c) from t",
		}, {
			input: "select name, dense_rank() over (partition by b order by c), lag(d) over (order by e desc) from t",
		}, {
			input: "select name, dense_rank() over window_name from t",
		}, {
			input: `SELECT pk,
					(SELECT max(pk) FROM one_pk WHERE pk < opk.pk) as max,
					(SELECT min(pk) FROM one_pk WHERE pk > opk.pk) as min
					FROM one_pk opk
					WHERE (SELECT min(pk) FROM one_pk WHERE pk > opk.pk) IS NOT NULL
					ORDER BY max`,
			output: "select pk, (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) as `max`," +
				" (SELECT min(pk) FROM one_pk WHERE pk > opk.pk) as `min` " +
				"from one_pk as opk " +
				"where (select min(pk) from one_pk where pk > opk.pk) " +
				"is not null order by `max` asc",
		}, {
			input:  "select i, s as max from mytable group by max",
			output: "select i, s as `max` from mytable group by `max`",
		}, {
			input:  "select i, s as max from mytable MAx",
			output: "select i, s as `max` from mytable as `MAx`",
		},
		// {
		// 	// TODO: for this to work we need a keyword-safe version of expression in sql.y
		// 	// input: `select i, s as max from mytable group by max having max = "hello"`,
		// },
		{
			input: "stream * from t",
		}, {
			input: "stream /* comment */ * from t",
		}, {
			input: "begin",
		}, {
			input:  "start transaction",
			output: "begin",
		}, {
			input:  "start transaction read only",
			output: "begin read only",
		}, {
			input:  "start transaction read write",
			output: "begin read write",
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
			input:  "create database if not exists test_db",
			output: "create database if not exists test_db",
		}, {
			input: "drop database test_db",
		}, {
			input:  "drop schema test_db",
			output: "drop database test_db",
		}, {
			input:  "drop database if exists test_db",
			output: "drop database if exists test_db",
		}, {
			input: "drop trigger trigger1",
		}, {
			input: "drop trigger if exists t2",
		}, {
			input:  "create table t (c int not null default 0 on update current_timestamp() auto_increment comment 'a comment here' unique)",
			output: "create table t (\n\tc int not null default 0 on update current_timestamp() auto_increment comment 'a comment here' unique\n)",
		}, {
			input:  "create table t (c INT NOT NULL DEFAULT 0 ON UPDATE current_timestamp() AUTO_INCREMENT COMMENT 'a comment here' UNIQUE)",
			output: "create table t (\n\tc INT not null default 0 on update current_timestamp() auto_increment comment 'a comment here' unique\n)",
		}, {
			// Same input with options backwards.
			input:  "create table t (c int unique comment 'a comment here' auto_increment on update current_timestamp() default 0 not null)",
			output: "create table t (\n\tc int not null default 0 on update current_timestamp() auto_increment comment 'a comment here' unique\n)",
		}, {
			// Transpose pairs in original
			input:  "create table t (c int default 0 not null auto_increment on update current_timestamp() unique comment 'a comment here')",
			output: "create table t (\n\tc int not null default 0 on update current_timestamp() auto_increment comment 'a comment here' unique\n)",
		}, {
			// Transpose pairs in reversed
			input:  "create table t (c int comment 'a comment here' unique on update current_timestamp() auto_increment not null default 0)",
			output: "create table t (\n\tc int not null default 0 on update current_timestamp() auto_increment comment 'a comment here' unique\n)",
		}, {
			// Those tests for ALTER TABLE ADD (...
			input:  "alter table t add (c int not null default 0 on update current_timestamp() auto_increment comment 'a comment here' unique)",
			output: "alter table t add column (\n\tc int not null default 0 on update current_timestamp() auto_increment comment 'a comment here' unique\n)",
		}, {
			input:  "alter table t add (c int unique comment 'a comment here' auto_increment on update current_timestamp() default 0 not null)",
			output: "alter table t add column (\n\tc int not null default 0 on update current_timestamp() auto_increment comment 'a comment here' unique\n)",
		}, {
			input:  "alter table t add (c int default 0 not null auto_increment on update current_timestamp() unique comment 'a comment here')",
			output: "alter table t add column (\n\tc int not null default 0 on update current_timestamp() auto_increment comment 'a comment here' unique\n)",
		}, {
			input:  "alter table t add (c int comment 'a comment here' unique on update current_timestamp() auto_increment not null default 0)",
			output: "alter table t add column (\n\tc int not null default 0 on update current_timestamp() auto_increment comment 'a comment here' unique\n)",
		}, {
			// Those tests for ALTER TABLE ADD COLUMN name ...
			input:  "alter table t add column c int not null default 0 on update current_timestamp() auto_increment comment 'a comment here' unique",
			output: "alter table t add column (\n\tc int not null default 0 on update current_timestamp() auto_increment comment 'a comment here' unique\n)",
		}, {
			input:  "alter table t add column c int unique comment 'a comment here' auto_increment on update current_timestamp() default 0 not null",
			output: "alter table t add column (\n\tc int not null default 0 on update current_timestamp() auto_increment comment 'a comment here' unique\n)",
		}, {
			input:  "alter table t add column c int unique comment 'a comment here' auto_increment on update current_timestamp() default 0 not null after foo",
			output: "alter table t add column (\n\tc int not null default 0 on update current_timestamp() auto_increment comment 'a comment here' unique\n) after foo",
		}, {
			input:  "alter table t add column c int unique comment 'a comment here' auto_increment on update current_timestamp() default 0 not null first",
			output: "alter table t add column (\n\tc int not null default 0 on update current_timestamp() auto_increment comment 'a comment here' unique\n) first",
		}, {
			input:  "alter table t add column c int default 0 not null auto_increment on update current_timestamp() unique comment 'a comment here'",
			output: "alter table t add column (\n\tc int not null default 0 on update current_timestamp() auto_increment comment 'a comment here' unique\n)",
		}, {
			input:  "alter table t add column c int comment 'a comment here' unique on update current_timestamp() auto_increment not null default 0",
			output: "alter table t add column (\n\tc int not null default 0 on update current_timestamp() auto_increment comment 'a comment here' unique\n)",
		}, {
			input:  "alter table t change foo bar int not null auto_increment first",
			output: "alter table t change column foo (\n\tbar int not null auto_increment\n) first",
		}, {
			input:  "alter table a modify foo int unique comment 'a comment here' auto_increment on update current_timestamp() default 0 not null after bar",
			output: "alter table a modify column foo (\n\tfoo int not null default 0 on update current_timestamp() auto_increment comment 'a comment here' unique\n) after bar",
		}, {
			input: "alter table t add column c int unique comment 'a comment here' auto_increment on update current_timestamp() default 0 not null," +
				" change foo bar int not null auto_increment first," +
				" reorganize partition b into (partition c values less than (:v1), partition d values less than (maxvalue))," +
				" add spatial index idx (id)",
			output: `alter table t add column (
	c int not null default 0 on update current_timestamp() auto_increment comment 'a comment here' unique
), change column foo (
	bar int not null auto_increment
) first, reorganize partition b into (partition c values less than (:v1), partition d values less than (maxvalue)), add spatial index idx (id)`,
		}, {
			input:  "alter table t alter foo set default 5",
			output: "alter table t alter column foo set default 5",
		}, {
			input:  "alter table t alter foo set default replace(uuid(),'-','')",
			output: "alter table t alter column foo set default replace(uuid(), '-', '')",
		}, {
			input: "alter table t alter column foo set default now()",
		}, {
			input:  "alter table t alter foo drop default",
			output: "alter table t alter column foo drop default",
		}, {
			input: "alter table t alter column foo drop default",
		}, {
			input: "alter table t modify value float(53) not null",
			output: "alter table t modify column `value` (\n" +
				"\t`value` float(53) not null\n" +
				")",
		}, {
			input:  "delete a.*, b.* from tbl_a a, tbl_b b where a.id = b.id and b.name = 'test'",
			output: "delete a, b from tbl_a as a, tbl_b as b where a.id = b.id and b.name = 'test'",
		}, {
			input: "call f1",
		}, {
			input:  "call f1()",
			output: "call f1",
		}, {
			input:  "call f1 ()",
			output: "call f1",
		}, {
			input: "call f1(x)",
		}, {
			input: "call f1(@x, @y)",
		}, {
			input: "call f1(now(), rand())",
		}, {
			input: "drop procedure p1",
		}, {
			input: "drop procedure if exists p1",
		}, {
			input:  "create procedure p1() select rand()",
			output: "create procedure p1 () select rand() from dual",
		}, {
			input:  "create procedure p1() language sql deterministic sql security invoker select 1+1",
			output: "create procedure p1 () language sql deterministic sql security invoker select 1 + 1 from dual",
		}, {
			input:  "create definer = me procedure p1(v1 int) select now()",
			output: "create definer = me procedure p1 (in v1 int) select now() from dual",
		}, {
			input:  "create definer = me procedure p1(v1 int) comment 'some_comment' not deterministic select now()",
			output: "create definer = me procedure p1 (in v1 int) comment 'some_comment' not deterministic select now() from dual",
		},
	}
	// Any tests that contain multiple statements within the body (such as BEGIN/END blocks) should go here.
	// validSQL is used by TestParseNextValid, which expects a semicolon to mean the end of a full statement.
	// Multi-statement bodies do not follow this expectation, hence they are excluded from TestParseNextValid.
	validMultiStatementSql = []parseTest{
		{
			input:  "create procedure p1 (in v1 int, inout v2 char(2), out v3 datetime) begin select rand() * 10; end",
			output: "create procedure p1 (in v1 int, inout v2 char(2), out v3 datetime) begin\nselect rand() * 10 from dual;\nend",
		}, {
			input:  "create procedure p1(v1 datetime)\nif rand() < 1 then select rand();\nend if",
			output: "create procedure p1 (in v1 datetime) if rand() < 1 then select rand() from dual;\nend if",
		}, {
			input: `create procedure p1(n double, m double)
begin
	set @s = '';
	if n = m then set @s = 'equals';
	else
		if n > m then set @s = 'greater';
		else set @s = 'less';
		end if;
		set @s = concat('is ', @s, ' than');
	end if;
	set @s = concat(n, ' ', @s, ' ', m, '.');
	select @s;
end`,
			output: "create procedure p1 (in n double, in m double) begin\nset @s = '';\nif n = m then set @s = 'equals';\nelse if n > m then set @s = 'greater';\nelse set @s = 'less';\nend if; set @s = concat('is ', @s, ' than');\nend if;\nset @s = concat(n, ' ', @s, ' ', m, '.');\nselect @s from dual;\nend",
		}, { // DECLARE statements are only allowed inside of BEGIN/END blocks
			input: `create procedure p1 () begin
declare cond_name condition for 1002;
end`,
		}, {
			input: `create procedure p1 () begin
declare cond_name condition for sqlstate '45000';
end`,
			output: `create procedure p1 () begin
declare cond_name condition for sqlstate value '45000';
end`,
		}, {
			input: `create procedure p1 () begin
declare cond_name condition for sqlstate value '45000';
end`,
		}, {
			input: `create procedure p1 () begin
declare cur_name cursor for select id, vals from test.t1;
end`,
		}, {
			input: `create procedure p1 () begin
declare cur_name cursor for select i from test.t2;
end`,
		}, {
			input: `create procedure p1 () begin
declare continue handler for sqlstate '45000', sqlstate value '45000' insert into test.t1 values (1, 1);
end`,
			output: `create procedure p1 () begin
declare continue handler for sqlstate value '45000', sqlstate value '45000' insert into test.t1 values (1, 1);
end`,
		}, {
			input: `create procedure p1 () begin
declare exit handler for sqlwarning, not found, sqlexception select i from test.t2;
end`,
		}, {
			input: `create procedure p1 () begin
declare undo handler for 1004, cond_name select i from test.t2;
end`,
		}, {
			input: `create procedure p1 () begin
declare x int;
end`,
		}, {
			input: `create procedure p1 () begin
declare y datetime default now();
end`,
		}, {
			input: `create procedure p1 () begin
declare x, y, z varchar(200) character set uft8mb4 default 'hi';
end`,
		},
	}
)

func TestValid(t *testing.T) {
	validSQL = append(validSQL, validMultiStatementSql...)
	for _, tcase := range validSQL {
		runParseTestCase(t, tcase)
	}
}

// Skipped tests for queries where the select expression can't accurately be captured because of comments
func TestBrokenCommentSelection(t *testing.T) {
	testcases := []parseTest{{
		input:  "select 1 --aa\nfrom t",
		output: "select 1 from t",
	}, {
		input:  "select 1 #aa\nfrom t",
		output: "select 1 from t",
	}, {
		input:  "select concat(a, -- this is a\n b -- this is b\n) from t",
		output: "select concat(a, b) from t",
	}, {
		input:  "select concat( /*comment*/ a, b) from t",
		output: "select concat(  a, b) from t",
	}, {
		input:  "select 1 /* drop this comment */ from t",
		output: "select 1 from t",
	}, {
		input:  "select 1, 2 /* drop this comment */, 3 from t",
		output: "select 1, 2, 3 from t",
	},
	}

	for _, tcase := range testcases {
		t.Run(tcase.input, func(t *testing.T) {
			t.Skip()
			runParseTestCase(t, tcase)
		})
	}
}

func assertTestcaseOutput(t *testing.T, tcase parseTest, tree Statement) {
	// For tests that require it, clear the InputExpression of selected expressions so they print their reproduced
	// values, rather than the input values. In most cases this is due to a bug in parsing, and there should be a
	// skipped test. But in some cases it's intentional, to test the behavior of parser logic.
	if tcase.serializeSelectExprs {
		tree.walkSubtree(func(node SQLNode) (kontinue bool, err error) {
			if ae, ok := node.(*AliasedExpr); ok {
				ae.InputExpression = ""
			}
			return true, nil
		})
	}

	out := String(tree)
	assert.Equal(t, tcase.output, out)
}

var ignoreWhitespaceTests = []parseTest{
	// TODO: this is a test of BEGIN .. END syntax, not triggers. Would be better to isolate it
	{
		input: `create trigger t1 before delete on foo for each row follows baz 
							begin
								set session foo = old.x;
                set session bar = new.y;
                update baz.t set a = @@foo + @@bar where z = old.x;
              end`,
	},
	{
		input: `create trigger t1 before delete on foo for each row follows baz 
							begin
								set session foo = old.x;
								begin
									set session boo = new.z;
                end;
                set session bar = new.y;
                update baz.t set a = @@foo + @@bar where z = old.x;
              end`,
	},
	{
		// TODO: this is a test of parsing case statements, not triggers. would be better to isolate it
		input: `create trigger t1 before delete on foo for each row 
							begin
								case old.y
									when 1 then select a + 1 from c;
									when 0 then update a set b = 2; delete from z;
								end case;
							end`,
	}, {
		input: `create trigger t1 before delete on foo for each row 
							begin
								case old.y
									when 1 then select a + 1 from c;
								end case;
							end`,
	}, {
		input: `create trigger t1 before delete on foo for each row 
							begin
								case old.x
									when old.y then set session var = 1;
									when 0 then update a set b = 2; delete from z;
									else select true from dual; delete from x;
								end case;
							end`,
	}, {
		// TODO: this is a test of parsing if statements, not triggers. would be better to isolate it
		input: `create trigger t1 before delete on foo for each row 
							begin
								if old.y > 0 then 
									select a + 1 from c;
									update b set c = 1;
								elseif old.y < 0 then 
									delete from z;
								elseif new.foo > rand() then
									set session autocommit = 1;
								else 
									insert into z values (1, 2, 3);
								end if;
							end`,
	}, {
		input: `create trigger t1 before delete on foo for each row
							begin
								if old.y > 0 then 
									select a + 1 from c;
									update b set c = 1;
								elseif new.foo > rand() then
									set session autocommit = 1;
								else
									insert into z values (1, 2, 3);
								end if;
							end`,
	}, {
		input: `create trigger t1 before delete on foo for each row 
							begin
								if old.y > 0 then
									select a + 1 from c;
									update b set c = 1;
								else 
									insert into z values (1, 2, 3);
								end if;
							end`,
	}, {
		input: `create trigger t1 before delete on foo for each row
							begin
								if old.y > 0 then 
									select a + 1 from c; update b set c = 1;
								end if;
							end`,
	},
}

func TestValidIgnoreWhitespace(t *testing.T) {
	for _, tcase := range ignoreWhitespaceTests {
		t.Run(tcase.input, func(t *testing.T) {
			if tcase.output == "" {
				tcase.output = tcase.input
			}
			tree, err := Parse(tcase.input)
			if err != nil {
				t.Errorf("Parse(%q) err: %v, want nil", tcase.input, err)
				return
			}
			out := String(tree)
			normalize := regexp.MustCompile("\\s+")
			normalizedOut := normalize.ReplaceAllLiteralString(out, " ")
			expectedOut := normalize.ReplaceAllLiteralString(tcase.output, " ")

			if normalizedOut != expectedOut {
				t.Errorf("Parse(%q) = %q, want: %q", tcase.input, normalizedOut, expectedOut)
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

func TestCreateViewSelectPosition(t *testing.T) {
	cases := []struct {
		query string
		sel   string
	}{{
		query: "create view a as select current_timestamp()",
		sel:   "select current_timestamp()",
	}, {
		query: "create view a as select /* comment */ 2 + 2 from dual",
		sel:   "select /* comment */ 2 + 2 from dual",
	}}
	for _, tcase := range cases {
		tree, err := Parse(tcase.query)
		if err != nil {
			t.Errorf("Parse(%q) err: %v", tcase.query, err)
		}
		ddl, ok := tree.(*DDL)
		if !ok {
			t.Errorf("Expected DDL when parsing (%q)", tcase.query)
		}
		sel := tcase.query[ddl.SubStatementPositionStart:ddl.SubStatementPositionEnd]
		if sel != tcase.sel {
			t.Errorf("expected select to be %q, got %q", tcase.sel, sel)
		}
	}
}

// Ensure there is no corruption from using a pooled yyParserImpl in Parse.
func TestValidParallel(t *testing.T) {
	validSQL = append(validSQL, validMultiStatementSql...)
	parallelism := 100
	numIters := 1000

	wg := sync.WaitGroup{}
	wg.Add(parallelism)
	for i := 0; i < parallelism; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < numIters; j++ {
				// can't run each test in its own test case, there are so many it bogs down an IDE
				tcase := validSQL[rand.Intn(len(validSQL))]
				if tcase.output == "" {
					tcase.output = tcase.input
				}
				tree, err := Parse(tcase.input)
				require.NoError(t, err)

				assertTestcaseOutput(t, tcase, tree)

				// This test just exercises the tree walking functionality.
				// There's no way automated way to verify that a node calls
				// all its children. But we can examine code coverage and
				// ensure that all walkSubtree functions were called.
				Walk(func(node SQLNode) (bool, error) {
					return true, nil
				}, tree)

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
		input: "select a from (select * from tbl)",
		err:   "Every derived table must have its own alias",
	}, {
		input: "select a, b from (select * from tbl) sort by a",
		err:   "syntax error",
	}, {
		input: "with test as (select 1), test_two as (select 2) select * from test, test_two union all with b as (select 1, 2) select * from b",
		err:   "syntax error",
	}, {
		input: "select * from test order by a union select * from test",
		err:   "syntax error",
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

	invalidDDL := []struct {
		input string
		err   string
	}{{
		input: "create table t (c int not null default 0 on update current_timestamp() auto_increment comment 'a comment here' unique null)",
		err:   "cannot include NULL / NOT NULL more than once at position 123 near 'null'",
	}, {
		input: "create table t (c int not null default 0 on update current_timestamp() auto_increment comment 'a comment here' unique primary key)",
		err:   "cannot include more than one key option for a column definition at position 130 near 'key'",
	}, {
		input: "create table t (c int not null default 0 on update current_timestamp() auto_increment comment 'a comment here' unique comment 'another')",
		err:   "cannot include more than one comment for a column definition at position 136 near 'another'",
	}, {
		input: "create table t (c int not null default 0 on update current_timestamp() auto_increment comment 'a comment here' unique auto_increment)",
		err:   "cannot include AUTO_INCREMENT more than once at position 133 near 'auto_increment'",
	}, {
		input: "create table t (c int not null default 0 on update current_timestamp() auto_increment comment 'a comment here' unique on update utc_timestamp())",
		err:   "cannot include ON UPDATE more than once at position 144",
	}, {
		input: "create table t (c int not null default 0 on update current_timestamp() auto_increment comment 'a comment here' unique default 1)",
		err:   "cannot include DEFAULT more than once at position 129",
	}, {
		input: "create table t (c not null int default 0 on update current_timestamp() auto_increment comment 'a comment here' unique)",
		err:   "syntax error at position 22 near 'not'",
	}, {
		input: "create table t (c default 0 int on update current_timestamp() auto_increment comment 'a comment here' unique)",
		err:   "syntax error at position 26 near 'default'",
	}, {
		input: "alter table t add (c int not null default 0 on update current_timestamp() auto_increment comment 'a comment here' unique null)",
		err:   "cannot include NULL / NOT NULL more than once at position 126 near 'null'",
	}, {
		input: "alter table t add (c int not null default 0 on update current_timestamp() auto_increment comment 'a comment here' unique primary key)",
		err:   "cannot include more than one key option for a column definition at position 133 near 'key'",
	}, {
		input: "alter table t add (c int not null default 0 on update current_timestamp() auto_increment comment 'a comment here' unique comment 'another')",
		err:   "cannot include more than one comment for a column definition at position 139 near 'another'",
	}, {
		input: "alter table t add (c int not null default 0 on update current_timestamp() auto_increment comment 'a comment here' unique auto_increment)",
		err:   "cannot include AUTO_INCREMENT more than once at position 136 near 'auto_increment'",
	}, {
		input: "alter table t add (c int not null default 0 on update current_timestamp() auto_increment comment 'a comment here' unique on update utc_timestamp())",
		err:   "cannot include ON UPDATE more than once at position 147",
	}, {
		input: "alter table t add (c int not null default 0 on update current_timestamp() auto_increment comment 'a comment here' unique default 1)",
		err:   "cannot include DEFAULT more than once at position 132",
	}, {
		input: "alter table t add (c not null int default 0 on update current_timestamp() auto_increment comment 'a comment here' unique)",
		err:   "syntax error at position 25 near 'not'",
	}, {
		input: "alter table t add (c default 0 int on update current_timestamp() auto_increment comment 'a comment here' unique)",
		err:   "syntax error at position 29 near 'default'",
	}}
	for _, tcase := range invalidDDL {
		_, err := ParseStrictDDL(tcase.input)
		if err == nil {
			t.Errorf("Parse invalid DDL(%q), got: nil, want: %s...", tcase.input, tcase.err)
		}
		if err != nil && !strings.Contains(err.Error(), tcase.err) {
			t.Errorf("Parse invalid DDL(%q), got: %v, want: %s...", tcase.input, err, tcase.err)
		}
	}
}

func TestCaseSensitivity(t *testing.T) {
	validSQL := []parseTest{
		{
			input:  "create table A (\n\t`B` int\n)",
			output: "create table A (\n\tB int\n)",
		}, {
			input:  "create index b on A (ID)",
			output: "alter table A add index b (ID)",
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
			input: "alter table A rename to B",
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
			output: "alter table A drop index b",
		}, {
			input: "select a from B",
		}, {
			input: "select A as B from C",
		}, {
			input: "select B.* from c",
		}, {
			input:                "select B.A from c",
			serializeSelectExprs: true,
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
			input: "update A.B set foo.b = 1, c = 2, baz.foo.c = baz.b",
		}, {
			input: "select A() from b",
		}, {
			input: "select A(B, C) from b",
		}, {
			input: "select A(distinct B, C) from b",
		}, {
			input:                "select A(ALL B, C) from b",
			output:               "select A(B, C) from b",
			serializeSelectExprs: true,
		}, {
			input:  "select A(ALL B, C) from b",
			output: "select A(ALL B, C) from b",
		}, {
			input: "select IF(B, C) from b",
		}, {
			input: "select * from b use index (A)",
		}, {
			input: "insert into A(A, B) values (1, 2)",
		}, {
			input:  "create view A as select current_timestamp()",
			output: "create view a as select current_timestamp() from dual",
		}, {
			input:  "alter view A",
			output: "alter table a",
		}, {
			input:  "drop view A",
			output: "drop view a",
		}, {
			input:  "drop view if exists A",
			output: "drop view if exists a",
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
		runParseTestCase(t, tcase)
	}
}

func TestKeywords(t *testing.T) {
	validSQL := []parseTest{
		{
			input:                "select current_timestamp",
			output:               "select current_timestamp() from dual",
			serializeSelectExprs: true,
		}, {
			input:  "select current_TIMESTAMP",
			output: "select current_TIMESTAMP from dual",
		}, {
			input: "update t set a = current_timestamp()",
		}, {
			input: "update t set a = current_timestamp(5)",
		}, {
			input:                "select a, current_date from t",
			output:               "select a, current_date() from t",
			serializeSelectExprs: true,
		}, {
			input:  "select a, current_DATE from t",
			output: "select a, current_DATE from t",
		}, {
			input:                "select a, current_user from t",
			output:               "select a, current_user() from t",
			serializeSelectExprs: true,
		}, {
			input: "select a, current_USER from t",
		}, {
			input: "select a, Current_USER(     ) from t",
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
			input:                "select utc_time, utc_date, utc_time(6)",
			output:               "select utc_time(), utc_date(), utc_time(6) from dual",
			serializeSelectExprs: true,
		}, {
			input:  "select utc_TIME, UTC_date, utc_time(6)",
			output: "select utc_TIME, UTC_date, utc_time(6) from dual",
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
			input:  "select /* unused keywords as cols */ write, virtual from t where varcharacter = 'foo'",
			output: "select /* unused keywords as cols */ `write`, `virtual` from t where `varcharacter` = 'foo'",
		}, {
			input:  "insert into x (status) values (42)",
			output: "insert into x(`status`) values (42)",
		}, {
			input:  "update x set status = 32 where status = 42",
			output: "update x set status = 32 where `status` = 42",
		}, {
			input:  "delete from x where status = 32",
			output: "delete from x where `status` = 32",
		}, {
			input:                "select status from t",
			output:               "select `status` from t",
			serializeSelectExprs: true,
		}, {
			input:                "select variables from t",
			output:               "select `variables` from t",
			serializeSelectExprs: true,
		}}

	for _, tcase := range validSQL {
		runParseTestCase(t, tcase)
	}
}

func runParseTestCase(t *testing.T, tcase parseTest) bool {
	return t.Run(tcase.input, func(t *testing.T) {
		if tcase.output == "" {
			tcase.output = tcase.input
		}
		tree, err := Parse(tcase.input)
		require.NoError(t, err)

		assertTestcaseOutput(t, tcase, tree)

		// This test just exercises the tree walking functionality.
		// There's no way automated way to verify that a node calls
		// all its children. But we can examine code coverage and
		// ensure that all walkSubtree functions were called.
		Walk(func(node SQLNode) (bool, error) {
			return true, nil
		}, tree)
	})
}

func TestConvert(t *testing.T) {
	validSQL := []parseTest{
		{
			input:                "select cast('abc' as date) from t",
			output:               "select convert('abc', date) from t",
			serializeSelectExprs: true,
		}, {
			input: "select cast('abc' as date) from t",
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
			input:                "select convert('abc', signed integer) from t",
			output:               "select convert('abc', signed) from t",
			serializeSelectExprs: true,
		}, {
			input:  "select convert('abc', signed) from t",
			output: "select convert('abc', signed) from t",
		}, {
			input: "select convert('abc', unsigned) from t",
		}, {
			input:                "select convert('abc', unsigned integer) from t",
			output:               "select convert('abc', unsigned) from t",
			serializeSelectExprs: true,
		}, {
			input:  "select convert('abc', unsigned) from t",
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
		runParseTestCase(t, tcase)
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
		output: "syntax error at position 19 near 'foo'",
	}, {
		input:  "select convert('abc', datetime(4+9)) from t",
		output: "syntax error at position 34 near '4'",
	}, {
		input:  "select convert('abc', decimal(4+9)) from t",
		output: "syntax error at position 33 near '4'",
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

func TestSubStr(t *testing.T) {

	// serializeSelectExprs here is not a bug: these are tests that various substring forms get parsed correctly
	validSQL := []parseTest{{
		input: `select substr('foobar', 1) from t`,
	}, {
		input: "select substr(a, 1, 6) from t",
	}, {
		input:  "select substring(a, 1) from t",
		output: "select substring(a, 1) from t",
	}, {
		input:  "select substring(a, 1, 6) from t",
		output: "select substring(a, 1, 6) from t",
	}, {
		input:                "select substring(a from 1 for 6) from t",
		output:               "select substr(a, 1, 6) from t",
		serializeSelectExprs: true,
	}, {
		input: "select substring(a from 1 for 6) from t",
	}, {
		input:                "select substring(a from 1 for 6) from t",
		output:               "select substr(a, 1, 6) from t",
		serializeSelectExprs: true,
	}, {
		input: "select substring(a from 1 for 6) from t",
	}, {
		input: "select substring(a from 1  for   6) from t",
	}, {
		input:                `select substr("foo" from 1 for 2) from t`,
		output:               `select substr('foo', 1, 2) from t`,
		serializeSelectExprs: true,
	}, {
		input:                `select substring("foo", 1, 2) from t`,
		output:               `select substring('foo', 1, 2) from t`,
		serializeSelectExprs: true,
	}, {
		input:                `select substr(substr("foo" from 1 for 2), 1, 2) from t`,
		output:               `select substr(substr('foo', 1, 2), 1, 2) from t`,
		serializeSelectExprs: true,
	}, {
		input: `select substr(substr("foo" from 1 for 2), 1, 2) from t`,
	}, {
		input:                `select substr(substring("foo", 1, 2), 3, 4) from t`,
		output:               `select substr(substring('foo', 1, 2), 3, 4) from t`,
		serializeSelectExprs: true,
	}, {
		input:                `select substr(substr("foo", 1), 2) from t`,
		output:               `select substr(substr('foo', 1), 2) from t`,
		serializeSelectExprs: true,
	}}

	for _, tcase := range validSQL {
		runParseTestCase(t, tcase)
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
			"	col_double3 double precision not null default 1.23,\n" +
			"	col_float float,\n" +
			"	col_float2 float(3,4) not null default 1.23,\n" +
			"	col_float3 float(3) not null default 1.23,\n" +
			"	col_decimal decimal,\n" +
			"	col_decimal2 decimal(2),\n" +
			"	col_decimal3 decimal(2,3),\n" +
			"	col_dec dec,\n" +
			"	col_dec2 dec(2),\n" +
			"	col_dec3 dec(2,3),\n" +
			"	col_fixed fixed,\n" +
			"	col_fixed2 fixed(2),\n" +
			"	col_fixed3 fixed(2,3),\n" +
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
			"	col_character character,\n" +
			"	col_character2 character(2),\n" +
			"	col_character3 character(3) character set ascii,\n" +
			"	col_character4 character(4) character set ascii collate ascii_bin,\n" +
			"	col_nchar nchar,\n" +
			"	col_nchar2 nchar(2),\n" +
			"	col_national_char national char,\n" +
			"	col_national_char2 national char(2),\n" +
			"	col_national_character national character,\n" +
			"	col_national_character2 national character(2),\n" +
			"	col_varchar varchar,\n" +
			"	col_varchar2 varchar(2),\n" +
			"	col_varchar3 varchar(3) character set ascii,\n" +
			"	col_varchar4 varchar(4) character set ascii collate ascii_bin,\n" +
			"	col_character_varying character varying,\n" +
			"	col_character_varying2 character varying(2),\n" +
			"	col_character_varying3 character varying(3) character set ascii,\n" +
			"	col_character_varying4 character varying(4) character set ascii collate ascii_bin,\n" +
			"	col_nvarchar nvarchar,\n" +
			"	col_nvarchar2 nvarchar(2),\n" +
			"	col_national_varchar national varchar,\n" +
			"	col_national_varchar2 national varchar(2),\n" +
			"	col_national_character_varying national character varying,\n" +
			"	col_national_character_varying2 national character varying(2),\n" +
			"	col_binary binary,\n" +
			"	col_varbinary varbinary(10),\n" +
			"	col_tinyblob tinyblob,\n" +
			"	col_blob blob,\n" +
			"	col_mediumblob mediumblob,\n" +
			"	col_longblob longblob,\n" +
			"	col_tinytext tinytext,\n" +
			"	col_text text,\n" +
			"	col_mediumtext mediumtext,\n" +
			"	col_long long,\n" +
			"	col_long_varchar long varchar,\n" +
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

		// check constraint
		"create table t (\n" +
			"	id int auto_increment,\n" +
			"	username varchar,\n" +
			"	a int,\n" +
			"	b int,\n" +
			"	check (b in (0, 1)),\n" +
			"	constraint a_positive check (a > 0),\n" +
			"	check (a > b)\n" +
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
			"	s3 varchar default null,\n" +
			"	s4 timestamp default current_timestamp(),\n" +
			"	s5 bit(1) default B'0'\n" +
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
		// test inline check constraint
		input: "create table t (\n" +
			"	a int,\n" +
			"	b int constraint b_positive check (b > 0)\n" +
			")",
		output: "create table t (\n" +
			"	a int,\n" +
			"	b int,\n" +
			"	constraint b_positive check (b > 0)\n" +
			")",
	}, {
		// test initial table constraint
		input: "create table t (\n" +
			"	check (a <> b),\n" +
			"	a int,\n" +
			"	b int\n" +
			")",
		output: "create table t (\n" +
			"	a int,\n" +
			"	b int,\n" +
			"	check (a != b)\n" +
			")",
	}, {
		input: "create table t (\n" +
			"	id int,\n" +
			"	status int,\n" +
			"	constraint status check (a > 0)\n" +
			")",
		output: "create table t (\n" +
			"	id int,\n" +
			"	`status` int,\n" +
			"	constraint status check (a > 0)\n" +
			")",
	}, {
		input: "create table t (\n" +
			"	id int,\n" +
			"	status int,\n" +
			"	constraint status check (status in (0, 1))\n" +
			")",
		output: "create table t (\n" +
			"	id int,\n" +
			"	`status` int,\n" +
			"	constraint status check (`status` in (0, 1))\n" +
			")",
	}, {
		// we don't support named primary keys currently
		input: "create table t (\n" +
			"	id int auto_increment,\n" +
			"	username varchar,\n" +
			"	a int,\n" +
			"	b int,\n" +
			"	constraint a_positive primary key (a, b)\n" +
			")",
		output: "create table t (\n" +
			"	id int auto_increment,\n" +
			"	username varchar,\n" +
			"	a int,\n" +
			"	b int,\n" +
			"	primary key (a, b)\n" +
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
		t.Run(tcase.input, func(t *testing.T) {
			tree, err := ParseStrictDDL(tcase.input)
			if err != nil {
				t.Errorf("input: %s, err: %v", tcase.input, err)
				return
			}
			if got, want := String(tree.(*DDL)), tcase.output; got != want {
				t.Errorf("Parse(%s):\n%s, want\n%s", tcase.input, got, want)
			}
		})
	}

	nonsupportedKeywords := []string{
		"sql_cache",
		"cume_dist",
		"last_value",
		"percent_rank",
		"lag",
		"first_value",
		"column",
		"long",
		"sql_no_cache",
		"current_user",
		"row",
		"lead",
		"full",
		"nvarchar",
		"_binary",
		"dec",
		"all",
		"processlist",
		"dense_rank",
		"analyze",
		"format",
		"ntile",
		"cast",
		"follows",
		"group_concat",
		"nth_value",
		"_utf8mb4",
		"row_number",
		"rank",
		"infile",
		"escaped",
		"terminated",
		"enclosed",
	}
	nonsupported := map[string]bool{}
	for _, x := range nonsupportedKeywords {
		nonsupported[x] = true
	}

	for key := range keywords {
		//input := fmt.Sprintf("create table t {key} bigint)")
		input := fmt.Sprintf("create table t (\n\t`%s` bigint\n)", key)
		output := fmt.Sprintf("create table t (\n\t`%s` bigint\n)", key)
		t.Run(input, func(t *testing.T) {
			if _, ok := nonsupported[key]; ok {
				t.Skipf("Keyword currently not supported as a column name: %s", key)
			}
			tree, err := ParseStrictDDL(input)
			if err != nil {
				t.Errorf("input: %s, err: %v", input, err)
				return
			}
			if got, want := String(tree.(*DDL)), output; got != want {
				t.Errorf("Parse(%s):\n%s, want\n%s", input, got, want)
			}
		})
	}
}

func TestLoadData(t *testing.T) {
	testCases := []struct {
		input  string
		output string
	}{{
		// test with simple file
		input:  "LOAD DATA INFILE 'x.txt' INTO TABLE c",
		output: "load data infile 'x.txt' into table c",
	}, {
		input:  "LOAD DATA INFILE '~/Desktop/x.txt' INTO TABLE c",
		output: "load data infile '~/Desktop/x.txt' into table c",
	}, {
		input:  "LOAD DATA LOCAL INFILE ':SOURCE:9fa1415b62a44b53b86cffbccb210b51' INTO TABLE test",
		output: "load data local infile ':SOURCE:9fa1415b62a44b53b86cffbccb210b51' into table test",
	}, {
		input:  "LOAD DATA LOCAL INFILE ':SOURCE:9fa1415b62a44b53b86cffbccb210b51' INTO TABLE test PARTITION (id)",
		output: "load data local infile ':SOURCE:9fa1415b62a44b53b86cffbccb210b51' into table test partition (id)",
	}, {
		input:  "LOAD DATA LOCAL INFILE ':SOURCE:9fa1415b62a44b53b86cffbccb210b51' INTO TABLE test PARTITION (id) CHARACTER SET UTF8MB4",
		output: "load data local infile ':SOURCE:9fa1415b62a44b53b86cffbccb210b51' into table test partition (id) character set UTF8MB4",
	}, {
		input:  "LOAD DATA LOCAL INFILE ':SOURCE:9fa1415b62a44b53b86cffbccb210b51' INTO TABLE test FIELDS TERMINATED BY ''",
		output: "load data local infile ':SOURCE:9fa1415b62a44b53b86cffbccb210b51' into table test fields terminated by ''",
	}, {
		input:  "LOAD DATA LOCAL INFILE ':SOURCE:9fa1415b62a44b53b86cffbccb210b51' INTO TABLE test PARTITION (id) CHARACTER SET UTF8MB4 FIELDS TERMINATED BY '' ESCAPED BY ''",
		output: "load data local infile ':SOURCE:9fa1415b62a44b53b86cffbccb210b51' into table test partition (id) character set UTF8MB4 fields terminated by '' escaped by ''",
	}, {
		input:  "LOAD DATA LOCAL INFILE ':SOURCE:9fa1415b62a44b53b86cffbccb210b51' INTO TABLE test PARTITION (id) FIELDS TERMINATED BY '' ENCLOSED BY '' ESCAPED BY ''",
		output: "load data local infile ':SOURCE:9fa1415b62a44b53b86cffbccb210b51' into table test partition (id) fields terminated by '' enclosed by '' escaped by ''",
	}, {
		input:  "LOAD DATA LOCAL INFILE 'y.txt' INTO TABLE test PARTITION (id) CHARACTER SET UTF8MB4 FIELDS TERMINATED BY '' OPTIONALLY ENCLOSED BY '' ESCAPED BY '' LINES TERMINATED BY ''",
		output: "load data local infile 'y.txt' into table test partition (id) character set UTF8MB4 fields terminated by '' optionally enclosed by '' escaped by '' lines terminated by ''",
	}, {
		input:  "LOAD DATA LOCAL INFILE 'l.csv' INTO TABLE test PARTITION (id) CHARACTER SET UTF8MB4 FIELDS TERMINATED BY '' ESCAPED BY '' LINES TERMINATED BY '' IGNORE 0 LINES (`pk`)",
		output: "load data local infile 'l.csv' into table test partition (id) character set UTF8MB4 fields terminated by '' escaped by '' lines terminated by '' ignore 0 lines (pk)",
	}, {
		input:  "LOAD DATA LOCAL INFILE 'l.csv' INTO TABLE test PARTITION (id) CHARACTER SET UTF8MB4 FIELDS TERMINATED BY '' ESCAPED BY '' LINES STARTING BY 'xxx' IGNORE 0 LINES (`pk`)",
		output: "load data local infile 'l.csv' into table test partition (id) character set UTF8MB4 fields terminated by '' escaped by '' lines starting by 'xxx' ignore 0 lines (pk)",
	}, {
		input:  "LOAD DATA LOCAL INFILE 'g.xlsx' INTO TABLE test PARTITION (id) CHARACTER SET UTF8MB4 FIELDS TERMINATED BY '' ESCAPED BY '' LINES TERMINATED BY '' (`id`)",
		output: "load data local infile 'g.xlsx' into table test partition (id) character set UTF8MB4 fields terminated by '' escaped by '' lines terminated by '' (id)",
	}, {
		input:  "LOAD DATA INFILE '/tmp/jokes.txt' INTO TABLE jokes FIELDS TERMINATED BY '' LINES TERMINATED BY '\n%%\n' (joke)",
		output: "load data infile '/tmp/jokes.txt' into table jokes fields terminated by '' lines terminated by '\n%%\n' (joke)",
	}, {
		input:  "LOAD DATA INFILE 'data.txt' INTO TABLE db2.my_table",
		output: "load data infile 'data.txt' into table db2.my_table",
	}, {
		input:  "LOAD DATA INFILE 'data.txt' INTO TABLE db2.my_table (c1, c2, c3)",
		output: "load data infile 'data.txt' into table db2.my_table (c1, c2, c3)",
	}, {
		input:  "LOAD DATA INFILE '/tmp/test.txt' INTO TABLE test IGNORE 1 LINES",
		output: "load data infile '/tmp/test.txt' into table test ignore 1 lines",
	}}
	for _, tcase := range testCases {
		p, err := Parse(tcase.input)
		require.NoError(t, err)
		if got, want := String(p), tcase.output; got != want {
			t.Errorf("Parse(%s):\n%s, want\n%s", tcase.input, got, want)
		}
	}
}

func TestTrim(t *testing.T) {
	testCases := []parseTest{
		{
			input:                `SELECT TRIM("foo")`,
			output:               `select trim(both ' ' from 'foo') from dual`,
			serializeSelectExprs: true,
		},
		{
			input:                `SELECT TRIM("bar" FROM "foo")`,
			output:               `select trim(both 'bar' from 'foo') from dual`,
			serializeSelectExprs: true,
		},
		{
			input:                `SELECT TRIM(LEADING "bar" FROM "foo")`,
			output:               `select trim(leading 'bar' from 'foo') from dual`,
			serializeSelectExprs: true,
		},
		{
			input:                `SELECT TRIM(TRAILING "bar" FROM "foo")`,
			output:               `select trim(trailing 'bar' from 'foo') from dual`,
			serializeSelectExprs: true,
		},
		{
			input:                `SELECT TRIM(BOTH "bar" FROM "foo")`,
			output:               `select trim(both 'bar' from 'foo') from dual`,
			serializeSelectExprs: true,
		},
		{
			input:                `SELECT TRIM(TRIM("foobar"))`,
			output:               `select trim(both ' ' from trim(both ' ' from 'foobar')) from dual`,
			serializeSelectExprs: true,
		},
	}
	for _, tcase := range testCases {
		runParseTestCase(t, tcase)
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

func TestCreateTableSelect(t *testing.T) {
	testCases := []struct {
		input  string
		output string
	}{{
		input:  "create table `t` as select * from `uv`",
		output: "create table t as select * from uv",
	}, {
		input:  "create table `t` as select * from `uv` LIMIT 5",
		output: "create table t as select * from uv limit 5",
	}, {
		input:  "create table `t` select pk from `foo`",
		output: "create table t as select pk from foo",
	}}
	// TODO: Table Specs with CREATE SELECT need to be fixed
	//{
	//	input: "create table t (pk int) select val from foo",
	//	output: "create table t (\n" +
	//			"\tpk int\n" +
	//		    ") as select val from foo",
	//}, {
	//	input: "CREATE TEMPORARY TABLE t (INDEX my_index_name (tag, time), UNIQUE my_unique_index_name (order_number)) SELECT * FROM my_big_table WHERE my_val = 1",
	//	output: "create table t(\n" +
	//			"\tINDEX my_index_name (tag, time)\n" +
	//		    "\tUNIQUE my_unique_index_name (order_number)\n" +
	//			") as SELECT * FROM my_big_table WHERE my_val = 1",
	//}, {
	//	input: `CREATE TEMPORARY TABLE core.my_tmp_table (id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, value BIGINT UNSIGNED NOT NULL DEFAULT 0 UNIQUE, location VARCHAR(20) DEFAULT "NEEDS TO BE SET", country CHAR(2) DEFAULT "XX" COMMENT "Two-letter country code", INDEX my_index_name (location)) ENGINE=MyISAM SELECT * FROM core.my_big_table`,
	//	output: "create temporary table core.my_tmp_table (id\n" +
	//		    "\tint unsigned not null auto_increment primary key,\n" +
	//			"\tvalue bigint unsigned not null default 0 unique,\n" +
	//		    "\tlocation varchar(20) default \"need to be set\",\n" +
	//			"\tcountry char(2) default \"XX\" comment \"Two-letter country code\",\n" +
	//			"index my_index_name (location)\n" +
	//			")engine=MyISAM SELECT * FROM core.my_big_table",
	//},
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

func TestLocks(t *testing.T) {
	testCases := []struct {
		input  string
		output string
	}{{
		input:  "lock tables foo read",
		output: "lock tables foo read",
	}, {
		input:  "LOCK TABLES `t1` READ",
		output: "lock tables t1 read",
	}, {
		input:  "LOCK TABLES `mytable` as `t` WRITE",
		output: "lock tables mytable as t write",
	}, {
		input:  "LOCK TABLES t1 WRITE, t2 READ",
		output: "lock tables t1 write, t2 read",
	}, {
		input:  "LOCK TABLES t1 LOW_PRIORITY WRITE, t2 READ LOCAL",
		output: "lock tables t1 low_priority write, t2 read local",
	}, {
		input:  "LOCK TABLES t1 as table1 LOW_PRIORITY WRITE, t2 as table2 READ LOCAL",
		output: "lock tables t1 as table1 low_priority write, t2 as table2 read local",
	}, {
		input:  "UNLOCK TABLES",
		output: "unlock tables",
	}, {
		input:  "LOCK TABLES `people` READ /*!32311 LOCAL */",
		output: "lock tables people read local",
	}}
	for _, tcase := range testCases {
		p, err := Parse(tcase.input)
		require.NoError(t, err)
		if got, want := String(p), tcase.output; got != want {
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
		output: "max nesting level reached at position 406 near 'F'",
	}, {
		input: "select(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F" +
			"(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(" +
			"F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F" +
			"(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(" +
			"F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F" +
			"(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(" +
			"F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F" +
			"(F(F(F(F(F(F(F(F(F(F(F(",
		output: "syntax error at position 404 near 'F'",
	}, {
		// This construct is considered invalid due to a grammar conflict.
		input:  "insert into a select * from b join c on duplicate key update d=e",
		output: "syntax error at position 54 near 'key'",
	}, {
		input:  "select * from a left join b",
		output: "syntax error at position 28 near 'b'",
	}, {
		input:  "select * from a natural join b on c = d",
		output: "syntax error at position 34 near 'on'",
	}, {
		input:  "select * from a natural join b using (c)",
		output: "syntax error at position 37 near 'using'",
	}, {
		input:  "select next id from a",
		output: "syntax error at position 15 near 'id'",
	}, {
		input:  "select next 1+1 values from a",
		output: "syntax error at position 15 near '1'",
	}, {
		input:  "insert into a values (select * from b)",
		output: "syntax error at position 29 near 'select'",
	}, {
		input:  "select database",
		output: "syntax error at position 16 near 'database'",
	}, {
		input:  "select mod from t",
		output: "syntax error at position 16 near 'from'",
	}, {
		input:  "select 1 from t where div 5",
		output: "syntax error at position 26 near 'div'",
	}, {
		input:  "select 1 from t where binary",
		output: "syntax error at position 29 near 'binary'",
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
		output: "syntax error at position 45 near 't'",
	}, {
		input:  "select * from t where id = ((select a from t1 union select b from t2) order by a limit 1)",
		output: "syntax error at position 76 near 'order'",
	}, {
		input:  "select a, max(a as b) from t1",
		output: "syntax error at position 19 near 'as'",
	}, {
		input:  "select a, cume_dist() from t1",
		output: "syntax error at position 27 near 'from'",
	}, {
		input:  "select a, STD(distinct b) over () from t1",
		output: "syntax error at position 23 near 'distinct'",
	}, {
		input:  "select a, foo() over () from t1",
		output: "syntax error at position 21 near 'over'",
	}, {
		input:  "select name, cume_dist(a) over (partition by b) from t",
		output: "syntax error at position 25 near 'a'",
	}, {
		input:  "select name, dense_rank(a) over (partition by b) from t",
		output: "syntax error at position 26 near 'a'",
	}, {
		input:  "select name, ntile(a) over (partition by b) from t",
		output: "syntax error at position 21 near 'a'",
	}, {
		input:  "select name, percent_rank(a) over (partition by b) from t",
		output: "syntax error at position 28 near 'a'",
	}, {
		input:  "select name, rank(a) over (partition by b) from t",
		output: "syntax error at position 20 near 'a'",
	}, {
		input:  "select name, row_number(a) over (partition by b) from t",
		output: "syntax error at position 26 near 'a'",
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
		input:  "INSERT INTO TABLE a VALUES (1)",
		output: "syntax error at position 18 near 'TABLE'",
	}, {
		input:  "set @user.@var = true",
		output: "invalid user variable declaration `@var` at position 22 near 'true'",
	}, {
		input:  "set @user.var.@name = true",
		output: "invalid user variable declaration `@name` at position 27 near 'true'",
	}, {
		input:  "set @@session.'autocommit' = true",
		output: "invalid system variable declaration `'autocommit'` at position 34 near 'true'",
	}, {
		input:  "set @@session.\"autocommit\" = true",
		output: "invalid system variable declaration `\"autocommit\"` at position 34 near 'true'",
	}, {
		input:  "set @@unknown.autocommit = true",
		output: "invalid system variable declaration `autocommit` at position 32 near 'true'",
	}, {
		input:  "set @@session.@@autocommit = true",
		output: "invalid system variable declaration `@@autocommit` at position 34 near 'true'",
	}, {
		input:  "set xyz.@@autocommit = true",
		output: "invalid system variable declaration `@@autocommit` at position 28 near 'true'",
	}, {
		input:  "set @@session.@autocommit = true",
		output: "invalid user variable declaration `@autocommit` at position 33 near 'true'",
	}, {
		input:  "set xyz.@autocommit = true",
		output: "invalid user variable declaration `@autocommit` at position 27 near 'true'",
	}, {
		input:  "set @@session.validate_password.length = 1",
		output: "invalid system variable declaration `length` at position 43 near '1'",
	}, {
		input:  "set session.@@validate_password.length = 1",
		output: "invalid system variable declaration `@@validate_password.length` at position 43 near '1'",
	}, {
		input:  "set session.validate_password.@@length = 1",
		output: "invalid system variable declaration `@@length` at position 43 near '1'",
	}, {
		input:  "set something.@@validate_password.length = 1",
		output: "invalid system variable declaration `@@validate_password.length` at position 45 near '1'",
	}, {
		input:  "set something.validate_password.@@length = 1",
		output: "invalid system variable declaration `@@length` at position 45 near '1'",
	}, {
		input:  "set session @@autocommit = true",
		output: "invalid system variable name `@@autocommit` at position 32 near 'true'",
	}, {
		input:  "set session @autocommit = true",
		output: "invalid system variable name `@autocommit` at position 31 near 'true'",
	}, {
		input:  "set session @@session.autocommit = true",
		output: "invalid system variable name `@@session.autocommit` at position 40 near 'true'",
	}, {
		input:  "set session @@global.autocommit = true",
		output: "invalid system variable name `@@global.autocommit` at position 39 near 'true'",
	}, {
		input:  "set session other.@@autocommit = true",
		output: "invalid system variable declaration `@@autocommit` at position 38 near 'true'",
	}, {
		input:  "set session other.@autocommit = true",
		output: "invalid user variable declaration `@autocommit` at position 37 near 'true'",
	}, {
		input:  "select * from foo limit -100",
		output: "syntax error at position 26 near 'limit'",
	}, {
		input:  "select * from foo limit 1.0",
		output: "syntax error at position 28 near '1.0'",
	}, {
		input:  "select * from foo limit 1+1",
		output: "syntax error at position 27 near '1'",
	}, {
		input:  "select * from foo limit a",
		output: "syntax error at position 26 near 'a'",
	}, {
		input:  "select * from foo limit '100'",
		output: "syntax error at position 30 near '100'",
	}, {
		input:  "start transaction read",
		output: "syntax error at position 23 near 'read'",
	}, {
		input:  "drop table x CASAS",
		output: "syntax error at position 19 near 'CASAS'",
	},
	}
)

func TestErrors(t *testing.T) {
	for _, tcase := range invalidSQL {
		t.Run(tcase.input, func(t *testing.T) {
			_, err := ParseStrictDDL(tcase.input)
			assert.Equal(t, tcase.output, err.Error())
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
		output: "syntax error at position 19 near 'id'",
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
	defer file.Close()
	if err != nil {
		t.Errorf(" Error: %v", err)
	}
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
