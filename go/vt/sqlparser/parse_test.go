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
	input                      string
	output                     string
	useSelectExpressionLiteral bool
}

var (
	validSQL = []parseTest{
		{
			input:  "SELECT * FROM base.version;",
			output: "select * from base.`version`",
		},
		{
			input:  "CREATE VIEW version AS SELECT * FROM base.version;",
			output: "create view `version` as select * from base.`version`",
		},
		{
			input:  "create table t123 (c1 varchar(5) check (c1 in ('v1', 'v2')) NOT NULL);",
			output: "create table t123 (\n\tc1 varchar(5) not null,\n\tcheck (c1 in ('v1', 'v2'))\n)",
		},
		{
			input:  "INSERT INTO hourly_logins (applications_id, count, hour) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE count = count + VALUES(count)",
			output: "insert into hourly_logins(applications_id, `count`, `hour`) values (:v1, :v2, :v3) on duplicate key update count = `count` + values(`count`)",
		},
		{
			input:  "INSERT INTO hourly_logins (applications_id, count, hour) VALUES (?, ?, ?) AS dt ON DUPLICATE KEY UPDATE count = count + dt.count",
			output: "insert into hourly_logins(applications_id, `count`, `hour`) values (:v1, :v2, :v3) AS dt on duplicate key update count = `count` + dt.`count`",
		},
		{
			input:  "INSERT INTO hourly_logins (applications_id, count, hour) VALUES (?, ?, ?) AS dt(new_id, new_count, new_hour) ON DUPLICATE KEY UPDATE count = count + dt.count",
			output: "insert into hourly_logins(applications_id, `count`, `hour`) values (:v1, :v2, :v3) AS dt(new_id, new_count, new_hour) on duplicate key update count = `count` + dt.`count`",
		},
		{
			input:  "INSERT INTO hourly_logins (applications_id, count, hour) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE account = account + VALUES(account)",
			output: "insert into hourly_logins(applications_id, `count`, `hour`) values (:v1, :v2, :v3) on duplicate key update account = `account` + values(`account`)",
		},
		{
			// INVISIBLE should parse, but be a no-op (for now)
			input:  "create table t (pk int primary key, c1 int INVISIBLE)",
			output: "create table t (\n\tpk int primary key,\n\tc1 int\n)",
		},
		{
			// INVISIBLE should parse, but be a no-op (for now)
			input:  "alter table t add column c1 int INVISIBLE",
			output: "alter table t add column (\n\tc1 int\n)",
		},
		{
			input:  "ALTER TABLE webhook_events ADD COLUMN event varchar(255) DEFAULT NULL;",
			output: "alter table webhook_events add column (\n\t`event` varchar(255) default null\n)",
		},
		{
			input:  "CREATE TABLE webhook_events (pk int primary key, event varchar(255) DEFAULT NULL)",
			output: "create table webhook_events (\n\tpk int primary key,\n\t`event` varchar(255) default null\n)",
		},
		{
			input:  "alter table t add column fk int REFERENCES parent(id)",
			output: "alter table t add column (\n\tfk int references parent [id]\n)",
		},
		{
			input:  "CREATE table t (pk int primary key, fk int REFERENCES parent(id))",
			output: "create table t (\n\tpk int primary key,\n\tfk int references parent [id]\n)",
		},
		{
			input:  `Select 'a' "b" 'c'`,
			output: "select 'abc'",
		},
		{
			input:  `Select concat('a' "b" 'c', "de" 'f')`,
			output: "select concat('abc', 'def')",
		},
		{
			input:  "SET @foo = 'o' 'ne';",
			output: "set @foo = 'one'",
		},
		{
			input:  "SET @foo := 'o' 'ne';",
			output: "set @foo = 'one'",
		},
		{
			input:  "SET @foo = \"o\" \"ne\";",
			output: "set @foo = 'one'",
		},
		{
			input:  "SET @foo = 'o''ne';",
			output: "set @foo = 'o\\'ne'",
		},
		{
			input:  "SET @@GLOBAL.GTID_PURGED= /*!80000 '+'*/ 'beabe64c-9dc6-11ed-8021-a0f9021e8e70:1-126';",
			output: "set global GTID_PURGED = '+beabe64c-9dc6-11ed-8021-a0f9021e8e70:1-126'",
		},
		{
			input:  "SET @@GLOBAL.GTID_PURGED:= /*!80000 '+'*/ 'beabe64c-9dc6-11ed-8021-a0f9021e8e70:1-126';",
			output: "set global GTID_PURGED = '+beabe64c-9dc6-11ed-8021-a0f9021e8e70:1-126'",
		},
		{
			input: "show replicas",
		},
		{
			input: "show binary logs",
		},
		{
			input: "show binary log status",
		},
		{
			input:  "SHOW MASTER STATUS",
			output: "show binary log status",
		},
		{
			input: "start replica",
		},
		{
			input: "stop replica",
		},
		{
			input: "show replica status",
		},
		{
			input:  "change replication source to SOURCE_HOST = 'Host'",
			output: "change replication source to source_host = Host",
		},
		{
			input:  "change replication source to SOURCE_HOST = 'Host', SOURCE_AUTO_POSITION=0",
			output: "change replication source to source_host = Host, source_auto_position = 0",
		},
		{
			input:  "change replication source to SOURCE_HOST = 'host', SOURCE_PASSWORD='PaSSword', SOURCE_PORT=12345, source_user='root', SOURCE_CONNECT_RETRY=60, SOURCE_RETRY_COUNT=3",
			output: "change replication source to source_host = host, source_password = PaSSword, source_port = 12345, source_user = root, source_connect_retry = 60, source_retry_count = 3",
		},
		{
			input:  "change replication filter REPLICATE_DO_TABLE=(table1)",
			output: "change replication filter replicate_do_table = (table1)",
		},
		{
			input:  "change replication filter REPLICATE_DO_TABLE=(table1, db.table2, db.table3)",
			output: "change replication filter replicate_do_table = (table1, db.table2, db.table3)",
		},
		{
			input:  "change replication filter REPLICATE_DO_TABLE=(db1.t1, db2.t2), REPLICATE_IGNORE_TABLE=(t1)",
			output: "change replication filter replicate_do_table = (db1.t1, db2.t2), replicate_ignore_table = (t1)",
		},
		{
			input:  "change replication filter REPLICATE_DO_TABLE=(db1.t1, db2.t2), REPLICATE_IGNORE_TABLE=(db1.t1, db2.t2)",
			output: "change replication filter replicate_do_table = (db1.t1, db2.t2), replicate_ignore_table = (db1.t1, db2.t2)",
		},
		{
			input: "reset replica",
		},
		{
			input: "reset replica all",
		},
		{
			input:  "create database `db1` charset 'utf8mb4' collate 'utf8_bin';",
			output: "create database db1 charset utf8mb4 collate utf8_bin",
		},
		{
			input:  `create database db1 charset "utf8mb4" collate "utf8_bin";`,
			output: `create database db1 charset utf8mb4 collate utf8_bin`,
		},
		{
			// Technically, MySQL would require these reserved keywords to be backtick quoted, but it's convenient that
			// we can support unquoted reserved keyword identifiers when we can, so adding this assertion to prevent
			// this query from breaking. A similar query is used by sql-diff.bats runs.
			input:  "INSERT INTO test(pk, int, string, boolean, float, uint, uuid) values (1, 2, 'one', true, 5.0, 6, 100)",
			output: "insert into test(pk, `int`, string, `boolean`, `float`, uint, uuid) values (1, 2, 'one', true, 5.0, 6, 100)",
		},
		{
			input:  "select * from my_table_function()",
			output: "select * from my_table_function()",
		},
		{
			input:  "select * from my_table_function('foo')",
			output: "select * from my_table_function('foo')",
		},
		{
			input:  "select * from my_table_function('foo', 'bar')",
			output: "select * from my_table_function('foo', 'bar')",
		},
		{
			input:  "select * from my_table_function() my_table_func_alias",
			output: "select * from my_table_function() as my_table_func_alias",
		},
		{
			input:  "select * from my_table_function() as my_table_func_alias",
			output: "select * from my_table_function() as my_table_func_alias",
		},
		{
			input:  "select * from my_table_function() my_table_func_alias1, my_table_function() my_table_func_alias2",
			output: "select * from my_table_function() as my_table_func_alias1, my_table_function() as my_table_func_alias2",
		},
		{
			input:  "select * from my_table_function() my_table_func_alias1 join my_table_function() my_table_func_alias2",
			output: "select * from my_table_function() as my_table_func_alias1 join my_table_function() as my_table_func_alias2",
		},
		{
			input:  "select * from (select * from my_table_function() as tbl_alias) sq",
			output: "select * from (select * from my_table_function() as tbl_alias) as sq",
		},

		{
			input: "select 1",
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
			input:                      `select "'ain't'", '"hello"' from t`,
			output:                     "select '\\'ain\\'t\\'', '\\\"hello\\\"' from t",
			useSelectExpressionLiteral: true,
		}, {
			input:                      `select "1" + "2" from t`,
			output:                     "select '1' + '2' from t",
			useSelectExpressionLiteral: true,
		}, {
			input:                      `select '1' + "2" from t`,
			output:                     "select '1' + '2' from t",
			useSelectExpressionLiteral: true,
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
			input:                      "select - -1 from t",
			output:                     "select 1 from t",
			useSelectExpressionLiteral: true,
		}, {
			input:                      "select -   -1 from t",
			output:                     "select 1 from t",
			useSelectExpressionLiteral: true,
		}, {
			input:  "select - -1 from t",
			output: "select 1 from t",
			// not a bug, we are testing that - -1 becomes 1
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
			input:                      "select /* \\0 */ '\\0' from a",
			output:                     "select /* \\0 */ '\\0' from a",
			useSelectExpressionLiteral: true,
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
			input:  "with test as (select 1 from dual), test_two as (select 2 from dual) select * from test, test_two union all (with b as (with c as (select 1, 2 from dual) select * from c) select * from b)",
			output: "with test as (select 1), test_two as (select 2) select * from test, test_two union all (with b as (with c as (select 1, 2) select * from c) select * from b)",
		}, {
			input:  "select /* union order by */ 1 from t union select 1 from t order by a",
			output: "select /* union order by */ 1 from t union select 1 from t order by a asc",
		}, {
			input:  "select /* union order by limit lock */ 1 from t union select 1 from t order by a limit 1 for update",
			output: "select /* union order by limit lock */ 1 from t union select 1 from t order by a asc limit 1 for update",
		},
		{
			input: "select 1 from t intersect select 1 from t",
		},
		{
			input: "select 1 from t intersect all select 1 from t",
		},
		{
			input: "select 1 from t intersect distinct select 1 from t",
		},
		{
			input: "select 1 from t except select 1 from t",
		},
		{
			input: "select 1 from t except all select 1 from t",
		},
		{
			input: "select 1 from t except distinct select 1 from t",
		},
		{
			input: "create table u as select 1 from t intersect all select 1 from t",
		},
		{
			input:  "(select id, a from t order by id limit 1) union (select id, b as a from s order by id limit 1) order by a limit 1",
			output: "(select id, a from t order by id asc limit 1) union (select id, b as a from s order by id asc limit 1) order by a asc limit 1",
		}, {
			input: "select a from (select 1 as a from tbl1 union select 2 from tbl2) as t",
		}, {
			input: "select a from (select 1 as a from tbl1 union select 2 from tbl2) as t (a, b)",
		},
		{
			input:  "select a from (values (1, 2), ('a', 'b')) as t (a, b)",
			output: "select a from (values row(1, 2), row('a', 'b')) as t (a, b)",
		},
		{
			input:  "select a from (values (1, 2), row('a', 'b')) as t (a, b)",
			output: "select a from (values row(1, 2), row('a', 'b')) as t (a, b)",
		},
		{
			input:  "select a from (values row(1, 2), ('a', 'b')) as t (a, b)",
			output: "select a from (values row(1, 2), row('a', 'b')) as t (a, b)",
		},
		{
			input: "select a from (values row(1, 2), row('a', 'b')) as t (a, b)",
		},
		{
			input: "select a from (values row(1, 2), row('a', 'b')) as t1 join (values row(3, 4), row('c', 'd')) as t2",
		},
		{
			input: "select a from (values row(1, 2), row('a', 'b')) as t1 (w, x) join (values row(3, 4), row('c', 'd')) as t2 (y, z)",
		},
		{
			input: "select a from (values row(1, 2), row('a', 'b')) as t1 (w, x) join lateral (values row(3, 4), row('c', 'd')) as t2 (y, z)",
		},
		{
			input:  "values row(1, 3), row(2, 2), row(3, 1)",
			output: "select * from (values row(1, 3), row(2, 2), row(3, 1))",
		},
		{
			input:  "values (1, 3), (2, 2), (3, 1)",
			output: "select * from (values row(1, 3), row(2, 2), row(3, 1))",
		},
		{
			input:  "values (1, 3), row(2, 2), (3, 1)",
			output: "select * from (values row(1, 3), row(2, 2), row(3, 1))",
		},
		{
			input:  "values (1, 3), (2, 2), (3, 1) order by 1",
			output: "select * from (values row(1, 3), row(2, 2), row(3, 1)) order by 1 asc",
		},
		{
			input:  "values (1, 3), (2, 2), (3, 1) order by 1 asc",
			output: "select * from (values row(1, 3), row(2, 2), row(3, 1)) order by 1 asc",
		},
		{
			input:  "values (1, 3), (2, 2), (3, 1) order by 1 desc",
			output: "select * from (values row(1, 3), row(2, 2), row(3, 1)) order by 1 desc",
		},
		{
			input:  "values (1, 3), (2, 2), (3, 1) limit 1",
			output: "select * from (values row(1, 3), row(2, 2), row(3, 1)) limit 1",
		},
		{
			input:  "values (1, 3), (2, 2), (3, 1) order by 2 limit 2",
			output: "select * from (values row(1, 3), row(2, 2), row(3, 1)) order by 2 asc limit 2",
		},
		{
			input:  "(((values (1, 3), (2, 2), (3, 1) order by 2 limit 2)))",
			output: "select * from (values row(1, 3), row(2, 2), row(3, 1)) order by 2 asc limit 2",
		},
		{
			input: "select a from t1, lateral (select b from t2) as sq",
		},
		{
			input: "select a from t1 join lateral (select b from t2) as sq",
		},
		{
			input: "select a from t1 natural join lateral (select b from t2) as sq",
		},
		{
			input:  "select a from t1 join lateral (select b from t2) sq",
			output: "select a from t1 join lateral (select b from t2) as sq",
		},
		{
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
			input: "select * from a where exists (select * from b where a.x = b.x)",
		}, {
			input: "select * from a where not exists (select * from b where a.x = b.x)",
		}, {
			input:  "select * from t1 where col in (select 1 from dual union select 2 from dual)",
			output: "select * from t1 where col in (select 1 union select 2)",
		}, {
			input: "select * from t1 where exists (select a from t2 union select b from t3)",
		},
		{
			input: "select /* distinct */ distinct 1 from t",
		},
		{
			input:  "select all col from t",
			output: "select all col from t",
		},
		{
			input: "select /* straight_join */ straight_join 1 from t",
		},
		{
			input:  "select sql_calc_found_rows distinct * from t",
			output: "select distinct sql_calc_found_rows * from t",
		},
		{
			input:  "select distinct sql_calc_found_rows * from t",
			output: "select distinct sql_calc_found_rows * from t",
		},
		{
			input:  "select distinct sql_calc_found_rows distinct * from t",
			output: "select distinct sql_calc_found_rows * from t",
		},
		{
			input:  "select sql_cache distinct sql_calc_found_rows straight_join * from t",
			output: "select distinct straight_join sql_calc_found_rows sql_cache * from t",
		},
		{
			input:  "select straight_join sql_calc_found_rows all sql_no_cache * from t",
			output: "select all straight_join sql_calc_found_rows sql_no_cache * from t",
		},
		{
			input:  "select sql_cache distinct sql_calc_found_rows straight_join straight_join sql_calc_found_rows distinct sql_cache * from t",
			output: "select distinct straight_join sql_calc_found_rows sql_cache * from t",
		},
		{
			input: "select /* for update */ 1 from t for update",
		}, {
			input: "select /* skip locked */ 1 from t for update skip locked",
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
			input:  "select /* column alias */ a b from t",
			output: "select /* column alias */ a as b from t",
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
			input: "select /* keyword table alias */ 1 from t as `By`",
		}, {
			input: "select /* use */ 1 from t1 as of '2019-01-01' use index (a) where b = 1",
		}, {
			input:  "select /* use */ 1 from t1 for system_time as of '2019-01-01' use index (`By`) where b = 1",
			output: "select /* use */ 1 from t1 as of '2019-01-01' use index (`By`) where b = 1",
		}, {
			input:  "select /* use */ 1 from t1 for system_time from '2019-01-01' to '2020-01-01' use index (`By`) where b = 1",
			output: "select /* use */ 1 from t1 for system_time from '2019-01-01' to '2020-01-01' use index (`By`) where b = 1",
		}, {
			input:  "select /* use */ 1 from t1 for system_time between '2019-01-01' and '2020-01-01' use index (`By`) where b = 1",
			output: "select /* use */ 1 from t1 for system_time between '2019-01-01' and '2020-01-01' use index (`By`) where b = 1",
		}, {
			input:  "select /* use */ 1 from t1 for system_time contained in ('2019-01-01', '2020-01-01') use index (`By`) where b = 1",
			output: "select /* use */ 1 from t1 for system_time contained in ('2019-01-01', '2020-01-01') use index (`By`) where b = 1",
		}, {
			input:  "select /* use */ 1 from t1 for system_time all use index (`By`) where b = 1",
			output: "select /* use */ 1 from t1 for system_time all use index (`By`) where b = 1",
		}, {
			input:  "select /* use */ 1 from t1 for version as of '2019-01-01' use index (`By`) where b = 1",
			output: "select /* use */ 1 from t1 as of '2019-01-01' use index (`By`) where b = 1",
		}, {
			input:  "select /* use */ 1 from t1 for version from '2019-01-01' to '2020-01-01' use index (`By`) where b = 1",
			output: "select /* use */ 1 from t1 for system_time from '2019-01-01' to '2020-01-01' use index (`By`) where b = 1",
		}, {
			input:  "select /* use */ 1 from t1 for version between '2019-01-01' and '2020-01-01' use index (`By`) where b = 1",
			output: "select /* use */ 1 from t1 for system_time between '2019-01-01' and '2020-01-01' use index (`By`) where b = 1",
		}, {
			input:  "select /* use */ 1 from t1 for version contained in ('2019-01-01', '2020-01-01') use index (`By`) where b = 1",
			output: "select /* use */ 1 from t1 for system_time contained in ('2019-01-01', '2020-01-01') use index (`By`) where b = 1",
		}, {
			input:  "select /* use */ 1 from t1 for version all use index (`By`) where b = 1",
			output: "select /* use */ 1 from t1 for system_time all use index (`By`) where b = 1",
		}, {
			input:  "select /* use */ 1 from t1 versions from '2019-01-01' to '2020-01-01' use index (`By`) where b = 1",
			output: "select /* use */ 1 from t1 for system_time from '2019-01-01' to '2020-01-01' use index (`By`) where b = 1",
		}, {
			input:  "select /* use */ 1 from t1 versions between '2019-01-01' and '2020-01-01' use index (`By`) where b = 1",
			output: "select /* use */ 1 from t1 for system_time between '2019-01-01' and '2020-01-01' use index (`By`) where b = 1",
		}, {
			input:  "select /* use */ 1 from t1 versions contained in ('2019-01-01', '2020-01-01') use index (`By`) where b = 1",
			output: "select /* use */ 1 from t1 for system_time contained in ('2019-01-01', '2020-01-01') use index (`By`) where b = 1",
		}, {
			input:  "select /* use */ 1 from t1 versions all use index (`By`) where b = 1",
			output: "select /* use */ 1 from t1 for system_time all use index (`By`) where b = 1",
		}, {
			input:  "select /* use */ 1 from t1 for system_time as of '2019-01-01'",
			output: "select /* use */ 1 from t1 as of '2019-01-01'",
		}, {
			input: "select /* keyword index */ 1 from t1 as of '2019-01-01' use index (`By`) where b = 1",
		}, {
			input: "select /* ignore */ 1 from t1 as of '2019-01-01' as t2 ignore index (a), t3 use index (b) where b = 1",
		}, {
			input: "select /* use */ 1 from t1 as of '2019-01-01' as t2 use index (a), t3 as of '2019-01-02' use index (b) where b = 1",
		}, {
			input:  "select /* use */ 1 from t1 for system_time as of '2019-01-01' as t2 use index (a), t3 as of '2019-01-02' use index (b) where b = 1",
			output: "select /* use */ 1 from t1 as of '2019-01-01' as t2 use index (a), t3 as of '2019-01-02' use index (b) where b = 1",
		}, {
			input: "select /* force */ 1 from t1 as of '2019-01-01' as t2 force index (a), t3 force index (b) where b = 1",
		}, {
			input:  "select /* table alias */ 1 from t as of '2019-01-01' t1",
			output: "select /* table alias */ 1 from t as of '2019-01-01' as t1",
		}, {
			input: "select /* table alias with as */ 1 from t as of '2019-01-01' as t1",
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
			input: "select /* full outer join */ * from a full outer join b on a.x = b.x",
		}, {
			input:  "select /* full outer join */ * from a full join b on a.x = b.x",
			output: "select /* full outer join */ * from a full outer join b on a.x = b.x",
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
			input:  "with t as (select (1) from dual) select sum(n) from t",
			output: "with t as (select (1)) select sum(n) from t",
		}, {
			input:  "with recursive t (n) as (select (1) from dual union all select n + 1 from t where n < 100) select sum(n) from t",
			output: "with recursive t (n) as (select (1) union all select n + 1 from t where n < 100) select sum(n) from t",
		}, {
			input:  "with recursive t (n) as (select (1) from dual union select n + 1 from t where n < 100) select sum(n) from t",
			output: "with recursive t (n) as (select (1) union select n + 1 from t where n < 100) select sum(n) from t",
		}, {
			input:  "with recursive a as (select 1 union select 2) select 10 union select 20",
			output: "with recursive a as (select 1 union select 2) select 10 union select 20",
		}, {
			input: "with cte1 as (select a from b) update c set d = e",
		}, {
			input: "with recursive cte1 as (select a from b) update c set d = e",
		}, {
			input: "with cte1 as (select a from b) delete from d where e = f",
		}, {
			input: "with recursive cte1 as (select a from b) delete from d where e = f",
		}, {
			input: "with cte1 as (select a from b) insert into c select * from cte1",
		}, {
			input: "with recursive cte1 as (select a from b) insert into c select * from cte1",
		}, {
			input: "select /* s.t */ 1 from s.t",
		}, {
			input:  "select /* s.keyword */ 1 from s.account",
			output: "select /* s.keyword */ 1 from s.`account`",
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
			input: "select /* xor */ 1 from t where a = b xor a = c",
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
		},
		{
			input:  "select /* current_date */ current_date from t",
			output: "select /* current_date */ current_date() from t",
		},
		{
			input: "select /* current_date as func */ current_date() from t",
		},
		{
			input:  "select /* current_time */ current_time from t",
			output: "select /* current_time */ current_time(0) from t",
		},
		{
			input:  "select /* current_time as func */ current_time() from t",
			output: "select /* current_time as func */ current_time(0) from t",
		},
		{
			input: "select /* current_time with fsp */ current_time(1) from t",
		},
		{
			input:  "select /* current_timestamp */ current_timestamp from t",
			output: "select /* current_timestamp */ current_timestamp(0) from t",
		},
		{
			input:  "select /* current_timestamp as func */ current_timestamp() from t",
			output: "select /* current_timestamp as func */ current_timestamp(0) from t",
		},
		{
			input: "select /* current_timestamp with fsp */ current_timestamp(3) from t",
		},
		{
			input:  "select /* localtime */ localtime from t",
			output: "select /* localtime */ localtime(0) from t",
		},
		{
			input:  "select /* localtime as func */ localtime() from t",
			output: "select /* localtime as func */ localtime(0) from t",
		},
		{
			input: "select /* localtime with fsp */ localtime(5) from t",
		},
		{
			input:  "select /* localtimestamp */ localtimestamp from t",
			output: "select /* localtimestamp */ localtimestamp(0) from t",
		},
		{
			input:  "select /* localtimestamp as func */ localtimestamp() from t",
			output: "select /* localtimestamp as func */ localtimestamp(0) from t",
		},
		{
			input: "select /* localtimestamp with fsp */ localtimestamp(7) from t",
		},
		{
			input:  "select /* utc_date */ utc_date from t",
			output: "select /* utc_date */ utc_date() from t",
		},
		{
			input: "select /* utc_date as func */ utc_date() from t",
		},
		{
			input:  "select /* utc_time */ utc_time from t",
			output: "select /* utc_time */ utc_time(0) from t",
		},
		{
			input:  "select /* utc_time as func */ utc_time() from t",
			output: "select /* utc_time as func */ utc_time(0) from t",
		},
		{
			input: "select /* utc_time with fsp */ utc_time(4) from t",
		},
		{
			input:  "select /* utc_timestamp */ utc_timestamp from t",
			output: "select /* utc_timestamp */ utc_timestamp(0) from t",
		},
		{
			input:  "select /* utc_timestamp as func */ utc_timestamp() from t",
			output: "select /* utc_timestamp as func */ utc_timestamp(0) from t",
		},
		{
			input: "select /* utc_timestamp with fsp */ utc_timestamp(0) from t",
		},
		{
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
			input:                      "select /* string */ 'a' from t",
			output:                     "select /* string */ 'a' from t",
			useSelectExpressionLiteral: true,
		}, {
			input:  "select /* double quoted string */ \"a\" from t",
			output: "select /* double quoted string */ 'a' from t",
		}, {
			input:                      "select /* double quoted string */ \"a\" from t",
			output:                     "select /* double quoted string */ 'a' from t",
			useSelectExpressionLiteral: true,
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
			input:  "select /* quote in double quoted string */ \"a'a\" from t",
			output: "select /* quote in double quoted string */ 'a\\'a' from t",

			useSelectExpressionLiteral: true,
		}, {
			input:                      "select /* backslash quote in string */ 'a\\'a' from t",
			output:                     "select /* backslash quote in string */ 'a\\'a' from t",
			useSelectExpressionLiteral: true,
		}, {
			input:  "select /* literal backslash in string */ 'a\\\\na' from t",
			output: "select /* literal backslash in string */ 'a\\\\na' from t",

			useSelectExpressionLiteral: true,
		}, {
			input:                      "select /* all escapes */ '\\0\\'\\\"\\b\\n\\r\\t\\Z\\\\' from t",
			output:                     "select /* all escapes */ '\\0\\'\\\"\\b\\n\\r\\t\\Z\\\\' from t",
			useSelectExpressionLiteral: true,
		}, {
			input:  "select /* non-escape */ '\\x' from t",
			output: "select /* non-escape */ 'x' from t",
		}, {
			input:                      "select /* non-escape */ '\\x' from t",
			output:                     "select /* non-escape */ 'x' from t",
			useSelectExpressionLiteral: true,
		}, {
			input:                      "select /* unescaped backslash */ '\n' from t",
			output:                     "select /* unescaped backslash */ '\\n' from t",
			useSelectExpressionLiteral: true,
		}, {
			input:                      "select /* escaped backslash */ '\\n' from t",
			output:                     "select /* escaped backslash */ '\\n' from t",
			useSelectExpressionLiteral: true,
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
			input:                      "select /* positional argument */ ? from t",
			output:                     "select /* positional argument */ :v1 from t",
			useSelectExpressionLiteral: true,
		}, {
			input:  "select /* positional argument */ ? from t limit ?",
			output: "select /* positional argument */ :v1 from t limit :v2",
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
			input:                      "select /* hex */ x'f0A1' from t",
			output:                     "select /* hex */ X'f0A1' from t",
			useSelectExpressionLiteral: true,
		}, {
			input: "select /* hex caps */ X'F0a1' from t",
		}, {
			input:  "select /* bit literal */ b'0101' from t",
			output: "select /* bit literal */ B'0101' from t",
		}, {
			input:                      "select /* bit literal */ b'0101' from t",
			output:                     "select /* bit literal */ B'0101' from t",
			useSelectExpressionLiteral: true,
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
			input:  "select /* binary unary */ a- -b from t",
			output: "select /* binary unary */ a - -b from t",
		}, {
			input:                      "select /* binary unary */ a- -b from t",
			output:                     "select /* binary unary */ a - -b from t",
			useSelectExpressionLiteral: true,
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
			input:                      "select /* TIMESTAMPADD */ TIMESTAMPADD(MINUTE, 1, '2008-01-04') from t",
			output:                     "select /* TIMESTAMPADD */ timestampadd(MINUTE, 1, '2008-01-04') from t",
			useSelectExpressionLiteral: true,
		}, {
			input:                      "select /* TIMESTAMPDIFF */ TIMESTAMPDIFF(MINUTE, '2008-01-02', '2008-01-04') from t",
			output:                     "select /* TIMESTAMPDIFF */ timestampdiff(MINUTE, '2008-01-02', '2008-01-04') from t",
			useSelectExpressionLiteral: true,
		}, {
			input:  "select /* dual */ 1 from dual",
			output: "select /* dual */ 1",
		}, {
			input:  "select /* Dual */ 1 from Dual",
			output: "select /* Dual */ 1",
		}, {
			input:  "select /* DUAL */ 1 from Dual",
			output: "select /* DUAL */ 1",
		}, {
			input: "select /* column as bool in where */ a from t where b",
		}, {
			input: "select /* OR of columns in where */ * from t where a or b",
		}, {
			input: "select /* XOR of columns in where */ * from t where a xor b",
		}, {
			input: "select /* OR of mixed columns in where */ * from t where a = 5 or b and c is not null",
		}, {
			input: "select /* XOR of mixed columns in where */ * from t where a = 5 xor b and c is not null",
		}, {
			input: "select /* XOR of mixed columns in where */ * from t where a = 5 xor b or d = 3 and c is not null",
		}, {
			input: "select /* OR in select columns */ (a or b) from t where c = 5",
		}, {
			input: "select /* XOR in select columns */ (a xor b) from t where c = 5",
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
			input:  "select /* dual */ 1 from dual",
			output: "select /* dual */ 1",
		}, {
			input:  "select * from (select 'tables') tables",
			output: "select * from (select 'tables') as `tables`",
		}, {
			input:                      "select * from (select 'tables') tables",
			output:                     "select * from (select 'tables') as `tables`",
			useSelectExpressionLiteral: true,
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
			input:  "insert /* set default */ into a set a := default, b := 2",
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
			input:  "insert into a(a, b) value (1, ifnull(null, default(b)))",
			output: "insert into a(a, b) values (1, ifnull(null, default(b)))",
		},
		{
			input:  "insert into a value (1, ifnull(null, default(b)))",
			output: "insert into a values (1, ifnull(null, default(b)))",
		},
		{
			input:  "insert into xy values row (1, 1)",
			output: "insert into xy values (1, 1)",
		},
		{
			input:  "insert into xy values row (1, 1), row (2, 2)",
			output: "insert into xy values (1, 1), (2, 2)",
		},
		{
			input:  "insert into xy values (1, 1), row (2, 2)",
			output: "insert into xy values (1, 1), (2, 2)",
		},
		{
			input:  "insert into xy values row (1, 1), (2, 2)",
			output: "insert into xy values (1, 1), (2, 2)",
		},
		{
			input:  "insert into xy ((( values row (1, 1), row (2, 2) )))",
			output: "insert into xy values (1, 1), (2, 2)",
		},
		{
			input:  "insert into xy values row()",
			output: "insert into xy values ()",
		},
		{
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
			input:  "update /* simple */ a set b := 3",
			output: "update /* simple */ a set b = 3",
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
			output: "update foo as f, bar as b set f.id = b.id where b.`name` = 'test'",
		}, {
			input:  "update foo f join bar b on f.name = b.name set f.id = b.id where b.name = 'test'",
			output: "update foo as f join bar as b on f.`name` = b.`name` set f.id = b.id where b.`name` = 'test'",
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
			input:  "delete a from a join b on a.id = b.id where b.name = 'test'",
			output: "delete a from a join b on a.id = b.id where b.`name` = 'test'",
		}, {
			input:  "delete a, b from a, b where a.id = b.id and b.name = 'test'",
			output: "delete a, b from a, b where a.id = b.id and b.`name` = 'test'",
		},
		{
			input:  "delete from a1, a2 using t1 as a1 inner join t2 as a2 where a1.id=a2.id",
			output: "delete a1, a2 from t1 as a1 join t2 as a2 where a1.id = a2.id",
		},
		{
			input: "savepoint abc",
		}, {
			input:  "savepoint `ab_cd`",
			output: "savepoint ab_cd",
		}, {
			input: "rollback",
		}, {
			input:  "rollback work",
			output: "rollback",
		}, {
			input:  "rollback work and chain",
			output: "rollback",
		}, {
			input:  "rollback work and no chain",
			output: "rollback",
		}, {
			input:  "rollback work release",
			output: "rollback",
		}, {
			input:  "rollback work no release",
			output: "rollback",
		}, {
			input:  "rollback work and chain release",
			output: "rollback",
		}, {
			input:  "rollback work and chain no release",
			output: "rollback",
		}, {
			input:  "rollback work and no chain release",
			output: "rollback",
		}, {
			input:  "rollback work and no chain no release",
			output: "rollback",
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
		},
		{
			input:  "set @@`version` = true",
			output: "set session version = true",
		},
		{
			input:  "select @@`version` = true",
			output: "select @@`version` = true",
		},
		{
			input:  "set @@session.autocommit = true",
			output: "set session autocommit = true",
		},
		{
			input:  "set @@session.`autocommit` = true",
			output: "set session autocommit = true",
		},
		{
			input:  "select @@session.`autocommit` = true",
			output: "select @@session.`autocommit` = true",
		},
		{
			input:  "set @@session.autocommit = ON",
			output: "set session autocommit = 'ON'",
		},
		{
			input:  "set @@session.autocommit= OFF",
			output: "set session autocommit = 'OFF'",
		},
		{
			input:  "set session autocommit = ON",
			output: "set session autocommit = 'ON'",
		},
		{
			input:  "set session autocommit := ON",
			output: "set session autocommit = 'ON'",
		},
		{
			input:  "set global autocommit = OFF",
			output: "set global autocommit = 'OFF'",
		},
		{
			input:  "set @@global.optimizer_prune_level = 1",
			output: "set global optimizer_prune_level = 1",
		},
		{
			input: "set global optimizer_prune_level = 1",
		},
		{
			input:  "set @@persist.optimizer_prune_level = 1",
			output: "set persist optimizer_prune_level = 1",
		},
		{
			input: "set persist optimizer_prune_level = 1",
		},
		{
			input:  "set @@persist_only.optimizer_prune_level = 1",
			output: "set persist_only optimizer_prune_level = 1",
		},
		{
			input: "set persist_only optimizer_prune_level = 1",
		},
		{
			input:  "set @@local.optimizer_prune_level = 1",
			output: "set session optimizer_prune_level = 1",
		},
		{
			input:  "set local optimizer_prune_level = 1",
			output: "set session optimizer_prune_level = 1",
		},
		{
			input:  "set @@optimizer_prune_level = 1",
			output: "set session optimizer_prune_level = 1",
		},
		{
			input: "set session optimizer_prune_level = 1",
		},
		{
			input:  "set @@optimizer_prune_level = 1, @@global.optimizer_search_depth = 62",
			output: "set session optimizer_prune_level = 1, global optimizer_search_depth = 62",
		},
		{
			input:  "set @@GlObAl.optimizer_prune_level = 1",
			output: "set global optimizer_prune_level = 1",
		},
		{
			input: "set @user.var = 1",
		},
		{
			input: "set @user.var.name = 1",
		},
		{
			input:  "set @user.var.name := 1",
			output: "set @user.var.name = 1",
		},
		{
			input:  "set @`user var` = 1",
			output: "set @user var = 1",
		},
		{
			input:  "select @`user var`",
			output: "select @`user var`",
		},
		{
			input:  "set @user.`var` = 1",
			output: "set @user.var = 1",
		},
		{
			input:  "select @user.`var`",
			output: "select @user.var",
		},
		{
			input:  "set @`user`.`var` = 1",
			output: "set @`user`.var = 1",
		},
		{
			input:  "select @`user`.`var`",
			output: "select @`user`.var",
		},
		{
			input:  "set @abc.def.`ghi` = 300",
			output: "set @abc.def.ghi = 300",
		},
		{
			input:  "select @abc.def.`ghi`",
			output: "select @abc.def.ghi",
		},
		{
			input:  "set autocommit = on",
			output: "set autocommit = 'on'",
		},
		{
			input:  "set autocommit = off",
			output: "set autocommit = 'off'",
		},
		{
			input:  "set autocommit = off, foo = 1",
			output: "set autocommit = 'off', foo = 1",
		},
		{
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
		},
		{
			input:  "alter table a add partition (partition p values less than (val))",
		},
		{
			input:  "alter table a drop partition p",
		},
		{
			input:  "alter table a discard partition p tablespace",
		},
		{
			input:  "alter table a discard partition all tablespace",
		},
		{
			input:  "alter table a import partition p tablespace",
		},
		{
			input:  "alter table a import partition all tablespace",
		},
		{
			input:  "alter table a truncate partition p tablespace",
		},
		{
			input:  "alter table a truncate partition all tablespace",
		},
		{
			input:  "alter table a coalesce partition 5",
		},
		{
			input:  "alter table a reorganize partition b into (partition c values less than (?), partition d values less than (maxvalue))",
			output: "alter table a reorganize partition b into (partition c values less than (:v1), partition d values less than (maxvalue))",
		},
		{
			input:  "alter table a exchange partition p with table t",
			output: "alter table a exchange partition p with table t without validation",
		},
		{
			input:  "alter table a exchange partition p with table t with validation",
		},
		{
			input:  "alter table a exchange partition p with table t without validation",
		},
		{
			input:  "alter table a analyze partition p",
		},
		{
			input:  "alter table a analyze partition all",
		},
		{
			input:  "alter table a optimize partition p",
		},
		{
			input:  "alter table a optimize partition all",
		},
		{
			input:  "alter table a rebuild partition p",
		},
		{
			input:  "alter table a rebuild partition all",
		},
		{
			input:  "alter table a repair partition p",
		},
		{
			input:  "alter table a repair partition all",
		},
		{
			input:  "alter table a remove partitioning",
		},
		{
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
			input:  "alter table a add constraint foreign key (x) references y(z)",
			output: "alter table a add foreign key (x) references y (z)",
		}, {
			input:  "alter table a add constraint abc foreign key country_code (country_code) REFERENCES premium_country (country_code)",
			output: "alter table a add constraint abc foreign key country_code (country_code) references premium_country (country_code)",
		}, {
			input:  "alter table a add constraint abc foreign key country_code (country_code) REFERENCES premium_country (country_code) on delete cascade",
			output: "alter table a add constraint abc foreign key country_code (country_code) references premium_country (country_code) on delete cascade",
		}, {
			input:  "alter table a add constraint abc foreign key country_code (country_code) REFERENCES premium_country (country_code) on update set null",
			output: "alter table a add constraint abc foreign key country_code (country_code) references premium_country (country_code) on update set null",
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
		},
		{
			input: "alter table a drop foreign key fk_something",
		},
		{
			input: "alter table a drop constraint b",
		},
		{
			input: "alter table a rename constraint foreign key oldfk to newfk",
		},
		{
			input: "alter table a rename constraint check oldchk to newchk",
		},
		{
			input: "alter table a rename constraint oldcon to newcon",
		},
		{
			input:  "alter table a drop id",
			output: "alter table a drop column id",
		}, {
			input:  "alter table a disable keys",
			output: "alter table a disable keys",
		}, {
			input:  "alter table a enable keys",
			output: "alter table a enable keys",
		}, {
			input:  "alter table t add primary key `foo` (`id`)",
			output: "alter table t add primary key (id)",
		}, {
			input:  "create table a (\n\t`a` int\n)",
			output: "create table a (\n\ta int\n)",
		}, {
			input: "create table `by` (\n\t`by` char\n)",
		}, {
			input:  "create table if not exists a (\n\t`a` int\n)",
			output: "create table if not exists a (\n\ta int\n)",
		}, {
			input: "alter table a rename column a to b",
		}, {
			input:  "alter table a rename column a as b",
			output: "alter table a rename column a to b",
		}, {
			input:  "create table t1 (id serial primary key, c1 text not null);",
			output: "create table t1 (\n\tid bigint not null auto_increment primary key,\n\tc1 text not null\n)",
		}, {
			input:  "create table t1 (id int primary key unique, c1 varchar(111) not null);",
			output: "create table t1 (\n\tid int primary key,\n\tc1 varchar(111) not null\n)",
		}, {
			input:  "create table t1 (id int primary key unique key, c1 varchar(111) not null);",
			output: "create table t1 (\n\tid int primary key,\n\tc1 varchar(111) not null\n)",
		}, {
			input:  "create table t1 (id int primary key primary key, c1 varchar(111) not null);",
			output: "create table t1 (\n\tid int primary key,\n\tc1 varchar(111) not null\n)",
		}, {
			input:  "create table t1 (id int unique unique unique unique, c1 varchar(111) not null);",
			output: "create table t1 (\n\tid int unique,\n\tc1 varchar(111) not null\n)",
		}, {
			input:  "create table a (b1 bool not null primary key, b2 boolean not null)",
			output: "create table a (\n\tb1 bool not null primary key,\n\tb2 boolean not null\n)",
		}, {
			input:  "CREATE TABLE `dolt_test`.`a` (`id` INT, `b` DOUBLE, PRIMARY KEY (`id`), INDEX `c` (`b` ASC) INVISIBLE)",
			output: "create table dolt_test.a (\n\tid INT,\n\tb DOUBLE,\n\tPRIMARY KEY (id),\n\tINDEX c (b) INVISIBLE\n)",
		}, {
			input:  "CREATE TABLE `dolt_test`.`a` (`id` INT, `b` DOUBLE, PRIMARY KEY (`id`), INDEX `c` (`b` ASC) VISIBLE)",
			output: "create table dolt_test.a (\n\tid INT,\n\tb DOUBLE,\n\tPRIMARY KEY (id),\n\tINDEX c (b) VISIBLE\n)",
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
			input:  "CREATE INDEX a ON b (id)",
			output: "alter table b add index a (id)",
		}, {
			input:  "create index a on b (foo(6) desc, foo asc)",
			output: "alter table b add index a (foo(6) desc, foo)",
		}, {
			input:  "CREATE INDEX `c` on `dolt_test`.`a`(`b` ASC) INVISIBLE",
			output: "alter table dolt_test.a add index c (b) INVISIBLE",
		}, {
			input:  "CREATE INDEX `c` on `dolt_test`.`a`(`b` ASC) VISIBLE",
			output: "alter table dolt_test.a add index c (b) VISIBLE",
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
			input:  "create ALGORITHM=UNDEFINED DEFINER=`UserName`@`localhost` SQL SECURITY DEFINER view a as select current_timestamp()",
			output: "create algorithm = undefined definer = `UserName`@`localhost` sql security definer view a as select current_timestamp(0)",
		}, {
			input:  "create ALGORITHM=UNDEFINED SQL SECURITY DEFINER view a as select current_timestamp()",
			output: "create algorithm = undefined sql security definer view a as select current_timestamp(0)",
		}, {
			input:  "create ALGORITHM=UNDEFINED DEFINER=UserName@localhost view a as select current_timestamp()",
			output: "create algorithm = undefined definer = `UserName`@`localhost` view a as select current_timestamp(0)",
		}, {
			input:  "create ALGORITHM=MERGE DEFINER=UserName@localhost SQL SECURITY INVOKER view a as select current_timestamp()",
			output: "create algorithm = merge definer = `UserName`@`localhost` sql security invoker view a as select current_timestamp(0)",
		}, {
			input:  "create ALGORITHM=TEMPTABLE DEFINER=UserName@localhost SQL SECURITY DEFINER view a as select current_timestamp()",
			output: "create algorithm = temptable definer = `UserName`@`localhost` sql security definer view a as select current_timestamp(0)",
		}, {
			input:  "create DEFINER=`nameUser`@`localhost` SQL SECURITY DEFINER view a as select current_timestamp()",
			output: "create definer = `nameUser`@`localhost` sql security definer view a as select current_timestamp(0)",
		}, {
			input:  "create SQL SECURITY INVOKER view a as select current_timestamp()",
			output: "create sql security invoker view a as select current_timestamp(0)",
		}, {
			input:  "CREATE VIEW a AS SELECT current_timestamp()",
			output: "create view a as select current_timestamp(0)",
		}, {
			input:  "create view a_view as select * from table_1 join table_2 on table_1.table_2_id_fk = table_2.id where city = 'my city'",
			output: "create view a_view as select * from table_1 join table_2 on table_1.table_2_id_fk = table_2.id where city = 'my city'",
		}, {
			input:  "CREATE OR REPLACE VIEW a AS SELECT current_timestamp()",
			output: "create or replace view a as select current_timestamp(0)",
		},
		{
			input: "create trigger t1 before update on foo for each row precedes bar update xxy set baz = 1 where a = b",
		},
		{
			input: "create trigger t2 before update on foo for each row precedes bar call myStoredProc(foo)",
		},
		{
			input: "create trigger dbName.trigger1 before update on foo for each row precedes bar update xxy set baz = 1 where a = b",
		},
		{
			input: "create trigger t1 after delete on foo for each row delete from xxy where old.y = z",
		},
		{ //TODO: figure out why `SET SESSION sys_var = x` does not work when set directly on the trigger (works in BEGIN/END block)
			input:  "create trigger t1 after delete on foo for each row set @@sum = @@sum + old.b",
			output: "create trigger t1 after delete on foo for each row set session sum = @@sum + old.b",
		},
		{
			input: "create trigger t1 before insert on foo for each row set new.x = new.x + 1",
		},
		{
			input: "create trigger t1 after insert on foo for each row update xxy set y = new.x",
		},
		{
			input: "create trigger t1 before delete on foo for each row follows baz update xxy set x = old.y",
		},
		{
			input:  "create trigger t1 before insert on foo for each row begin end",
			output: "create trigger t1 before insert on foo for each row begin\nend",
		},
		{
			input:  "create DEFINER=`root`@`localhost` trigger t1 before delete on foo for each row follows baz update xxy set x = old.y",
			output: "create definer = `root`@`localhost` trigger t1 before delete on foo for each row follows baz update xxy set x = old.y",
		},
		{
			input:  "create definer = me trigger t1 before delete on foo for each row follows baz update xxy set x = old.y",
			output: "create definer = `me`@`%` trigger t1 before delete on foo for each row follows baz update xxy set x = old.y",
		},
		{
			input:  "create definer=me trigger t1 before delete on foo for each row follows baz update xxy set x = old.y",
			output: "create definer = `me`@`%` trigger t1 before delete on foo for each row follows baz update xxy set x = old.y",
		},
		{
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
			output: "analyze table a",
		}, {
			input:  "analyze table a, b, c",
			output: "analyze table a, b, c",
		}, {
			input:  "analyze table a update histogram on (x, y) using data '{\"buckets\": [[0,10],[10,20]]}'",
			output: "analyze table a update histogram on (x, y) using data '{\\\"buckets\\\": [[0,10],[10,20]]}'",
		}, {
			input:  "analyze table a drop histogram on (x, y)",
			output: "analyze table a drop histogram on (x, y)",
		}, {
			input:  "analyze table a update histogram on x, y using data '{\"buckets\": [[0,10],[10,20]]}'",
			output: "analyze table a update histogram on (x, y) using data '{\\\"buckets\\\": [[0,10],[10,20]]}'",
		}, {
			input:  "analyze table a drop histogram on x, y",
			output: "analyze table a drop histogram on (x, y)",
		}, {
			input:  "prepare stmt from 'select sqrt(pow(?,2) + pow(?,2)) as hypotenuse'",
			output: "prepare stmt from 'select sqrt(pow(?,2) + pow(?,2)) as hypotenuse'",
		}, {
			input:  "prepare stmt from @a",
			output: "prepare stmt from @a",
		}, {
			input:  "execute stmt",
			output: "execute stmt",
		}, {
			input:  "execute stmt using @a, @b",
			output: "execute stmt using @a, @b",
		}, {
			input:  "execute action",
			output: "execute action",
		}, {
			input:  "deallocate prepare stmt",
			output: "deallocate prepare stmt",
		}, {
			input:  "drop prepare stmt",
			output: "deallocate prepare stmt",
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
			input:  "show create procedure t",
			output: "show create procedure t",
		}, {
			input:  "show create table t",
			output: "show create table t",
		}, {
			input:  "show create table t as of 'version'",
			output: "show create table t as of 'version'",
		}, {
			input:  "show create table t for system_time as of 'version'",
			output: "show create table t as of 'version'",
		}, {
			input:  "show create trigger t",
			output: "show create trigger t",
		}, {
			input:  "show create view v",
			output: "show create view v",
		}, {
			input:  "show databases",
			output: "show databases",
		}, {
			input:  "show databases like 'something'",
			output: "show databases like 'something'",
		}, {
			input:  "show databases where `database` = 'something'",
			output: "show databases where `database` = 'something'",
		}, {
			input:  "show schemas",
			output: "show schemas",
		}, {
			input:  "show engines",
			output: "show engines",
		}, {
			input:  "show errors",
			output: "show errors",
		}, {
			input: "show function status",
		}, {
			input: "show function status where `Name` = 'hi'",
		}, {
			input: "show function status like 'hi'",
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
			input:  "show plugins",
			output: "show plugins",
		}, {
			input: "show procedure status",
		}, {
			input: "show procedure status where `Name` = 'hi'",
		}, {
			input: "show procedure status like 'hi'",
		}, {
			input:  "show processlist",
			output: "show processlist",
		}, {
			input:  "show full processlist",
			output: "show full processlist",
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
			input:  "show session status LIKE 'Ssl_cipher'",
			output: "show session status like 'Ssl_cipher'",
		}, {
			input:  "show session status LIKE '%x'",
			output: "show session status like '%x'",
		}, {
			input:  "show session status where value > 5",
			output: "show session status where `value` > 5",
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
			output: "show table status where `name` = 't1'",
		}, {
			input: "show tables",
		}, {
			input: "show tables as of 123",
		}, {
			input:  "show tables for system_time as of 123",
			output: "show tables as of 123",
		}, {
			input: "show tables like '%keyspace%'",
		}, {
			input: "show tables as of 123 like '%keyspace%'",
		}, {
			input:  "show tables for system_time as of 123 like '%keyspace%'",
			output: "show tables as of 123 like '%keyspace%'",
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
			input:  "show TRIGGERS where v = 'x'",
			output: "show triggers where v = 'x'",
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
			input:  "show variables like 'max_join_size'",
			output: "show variables like 'max_join_size'",
		}, {
			input:  "show global variables like '%size%'",
			output: "show global variables like '%size%'",
		}, {
			input:  "show session variables like '%size%'",
			output: "show session variables like '%size%'",
		}, {
			input:  "show variables where Variable_name = 'auto_generate_certs'",
			output: "show variables where Variable_name = 'auto_generate_certs'",
		}, {
			input:  "show global variables where `Value` = 'ON'",
			output: "show global variables where `Value` = 'ON'",
		}, {
			input:  "show session variables where Variable_name like '%dir%' and `Value` like '/usr/%'",
			output: "show session variables where Variable_name like '%dir%' and `Value` like '/usr/%'",
		}, {
			input:  "show warnings",
			output: "show warnings",
		}, {
			input:  "show warnings limit 10",
			output: "show warnings limit 10",
		}, {
			input:  "show warnings limit 10, 10",
			output: "show warnings limit 10, 10",
		}, {
			input:  "show count(*) warnings",
			output: "show count(*) warnings",
		}, {
			input:  "show count ( * ) warnings",
			output: "show count(*) warnings",
		}, {
			input:  "show errors",
			output: "show errors",
		}, {
			input:  "show errors limit 10",
			output: "show errors limit 10",
		}, {
			input:  "show errors limit 10, 10",
			output: "show errors limit 10, 10",
		}, {
			input:  "show count(*) errors",
			output: "show count(*) errors",
		}, {
			input:  "show count ( * ) errors",
			output: "show count(*) errors",
		}, {
			input:  "select warnings from t",
			output: "select `warnings` from t",
		}, {
			input:  "use db",
			output: "use db",
		}, {
			input:  "use db/branch",
			output: "use `db/branch`",
		}, {
			input:  "use duplicate",
			output: "use `duplicate`",
		}, {
			input:  "use `ks:-80@master`",
			output: "use `ks:-80@master`",
		}, {
			input:  "describe foobar",
			output: "show columns from foobar",
		}, {
			input:  "desc foobar",
			output: "show columns from foobar",
		}, {
			input:  "explain foobar",
			output: "show columns from foobar",
		}, {
			input:  "describe a as of 'foo'",
			output: "show columns from a as of 'foo'",
		}, {
			input:  "describe a for system_time as of 'foo'",
			output: "show columns from a as of 'foo'",
		}, {
			input:  "describe a as of func('foo')",
			output: "show columns from a as of func('foo')",
		}, {
			input:  "show columns from a as of 'foo'",
			output: "show columns from a as of 'foo'",
		}, {
			input:  "show columns from a for system_time as of 'foo'",
			output: "show columns from a as of 'foo'",
		}, {
			input: "explain select * from foobar",
		}, {
			input: "explain format = tree select * from foobar",
		}, {
			input: "explain analyze select * from foobar",
		}, {
			input:  "explain extended select * from foobar",
			output: "explain select * from foobar",
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
			input:  "select k collate 'latin1_german2_ci' as k1 from t1 order by k1 asc",
			output: "select k collate latin1_german2_ci as k1 from t1 order by k1 asc",
		}, {
			input:  "select /* drop trailing semicolon */ 1 from dual;",
			output: "select /* drop trailing semicolon */ 1",
		}, {
			input:                      "select /* cache directive */ sql_no_cache 'foo' from t",
			output:                     "select /* cache directive */ sql_no_cache 'foo' from t",
			useSelectExpressionLiteral: true,
		}, {
			input:                      "select /* sql_calc_rows directive */ sql_calc_found_rows 'foo' from t",
			output:                     "select /* sql_calc_rows directive */ sql_calc_found_rows 'foo' from t",
			useSelectExpressionLiteral: true,
		}, {
			input:                      "select /* cache and sql_calc_rows directive */ sql_no_cache sql_calc_found_rows 'foo' from t",
			output:                     "select /* cache and sql_calc_rows directive */ sql_calc_found_rows sql_no_cache 'foo' from t",
			useSelectExpressionLiteral: true,
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
			input: "select `name`, group_concat(score) from t group by `name`",
		}, {
			input:                      "select concAt(  \"a\",    \"b\", \"c\"  ) from t group by `name`",
			output:                     "select concAt('a', 'b', 'c') from t group by `name`",
			useSelectExpressionLiteral: true,
		}, {
			input: "select `name`, group_concat(distinct id, score order by id desc separator ':') from t group by `name`",
		}, {
			input: "select `name`, group_concat(distinct id, score order by id desc separator '') from t group by `name`",
		}, {
			input: "select * from t partition (p0)",
		}, {
			input: "select * from t partition (p0, p1)",
		}, {
			input: "select e.id, s.city from employees as e join stores partition (p1) as s on e.store_id = s.id",
		}, {
			input:  "select truncate(120.3333, 2) from dual",
			output: "select truncate(120.3333, 2)",
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
			input: "select `name`, dense_rank() over () from t",
		}, {
			input:  "select name, avg(a) over (partition by b) as avg from t",
			output: "select `name`, avg(a) over (partition by b) as `avg` from t",
		}, {
			input: "select `name`, bit_and(a) over (partition by b) from t",
		}, {
			input: "select `name`, bit_or(a) over (partition by b) from t",
		}, {
			input: "select `name`, bit_xor(a) over (partition by b) from t",
		}, {
			input: "select `name`, count(distinct a) over (partition by b) from t",
		}, {
			input:  "select name, count(a) over (partition by b) as count from t",
			output: "select `name`, count(a) over (partition by b) as `count` from t",
		}, {
			input: "select `name`, json_arrayagg(a) over (partition by b) from t",
		}, {
			input: "select `name`, json_objectagg(a) over (partition by b) from t",
		}, {
			input: "select `name`, max(a) over (partition by b) from t",
		}, {
			input: "select `name`, min(a) over (partition by b) from t",
		}, {
			input: "select `name`, stddev_pop(a) over (partition by b) from t",
		}, {
			input: "select `name`, stddev(a) over (partition by b) from t",
		}, {
			input: "select `name`, std(a) over (partition by b) from t",
		}, {
			input: "select `name`, stddev_samp(a) over (partition by b) from t",
		}, {
			input: "select `name`, sum(a) over (partition by b) from t",
		}, {
			input:  "select name, sum(distinct a) over (partition by b) as SUM from t",
			output: "select `name`, sum(distinct a) over (partition by b) as `SUM` from t",
		}, {
			input: "select `name`, var_pop(a) over (partition by b) from t",
		}, {
			input: "select `name`, variance(a) over (partition by b) from t",
		}, {
			input: "select `name`, cume_dist() over (partition by b) from t",
		}, {
			input: "select `name`, cume_dist() over (partition by b) - 1 in (1, 2) as included from t",
		}, {
			input: "select `name`, cume_dist() over (partition by b) = dense_rank() over () as included from t",
		}, {
			input: "select `name`, dense_rank() over (partition by b) from t",
		}, {
			input: "select `name`, first_value(a) over (partition by b) from t",
		}, {
			input: "select `name`, lag(a) over (partition by b) from t",
		}, {
			input: "select `name`, last_value(a) over (partition by b) from t",
		}, {
			input: "select `name`, lead(a) over (partition by b) from t",
		}, {
			input: "select `name`, nth_value(a) over (partition by b) from t",
		}, {
			input: "select `name`, ntile() over (partition by b) from t",
		}, {
			input: "select `name`, percent_rank() over (partition by b) from t",
		}, {
			input: "select `name`, rank() over (partition by b) from t",
		}, {
			input: "select `name`, row_number() over (partition by b) from t",
		}, {
			input: "select `name`, dense_rank() over (partition by b) from t",
		}, {
			input: "select `name`, dense_rank() over (partition by b order by c asc) from t",
		}, {
			input: "select `name`, cume_dist() over (partition by b order by c asc) from t",
		}, {
			input: "select `name`, first_value(a) over (partition by b order by c asc) from t",
		}, {
			input: "select `name`, lag(a) over (partition by b order by c asc) from t",
		}, {
			input: "select `name`, last_value(a) over (partition by b order by c asc) from t",
		}, {
			input: "select `name`, lead(a) over (partition by b order by c asc) from t",
		}, {
			input: "select `name`, nth_value(a) over (partition by b order by c asc) from t",
		}, {
			input: "select `name`, ntile() over (partition by b order by c asc) from t",
		}, {
			input: "select `name`, percent_rank() over (partition by b order by c asc) from t",
		}, {
			input: "select `name`, rank() over (partition by b order by c asc) from t",
		}, {
			input: "select `name`, row_number() over (partition by b order by c asc) from t",
		}, {
			input: "select `name`, dense_rank() over ( order by b asc) from t",
		}, {
			input: "select `name`, dense_rank() over (partition by b order by c asc) from t",
		}, {
			input: "select `name`, dense_rank() over (partition by b order by c asc), lag(d) over ( order by e desc) from t",
		}, {
			input: "select `name`, dense_rank() over ( order by y asc ROWS CURRENT ROW) from t",
		}, {
			input: "select `name`, dense_rank() over (partition by x ROWS CURRENT ROW) from t",
		}, {
			input: "select `name`, dense_rank() over (partition by x order by y asc ROWS CURRENT ROW) from t",
		}, {
			input: "select `name`, row_number() over (partition by x order by y asc ROWS 2 PRECEDING) from t",
		}, {
			input: "select `name`, row_number() over (partition by x ROWS UNBOUNDED PRECEDING) from t",
		}, {
			input: "select `name`, row_number() over (partition by x ROWS interval 5 DAY PRECEDING) from t",
		}, {
			input: "select `name`, row_number() over (partition by x ROWS interval '2:30' MINUTE_SECOND PRECEDING) from t",
		}, {
			input: "select `name`, row_number() over (partition by x order by y asc ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) from t",
		}, {
			input: "select `name`, dense_rank() over (partition by x ROWS BETWEEN CURRENT ROW AND CURRENT ROW) from t",
		}, {
			input: "select `name`, dense_rank() over (partition by x ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) from t",
		}, {
			input: "select `name`, row_number() over (partition by x ROWS BETWEEN interval 5 DAY PRECEDING AND CURRENT ROW) from t",
		}, {
			input: "select `name`, row_number() over (partition by x ROWS BETWEEN interval '2:30' MINUTE_SECOND PRECEDING AND CURRENT ROW) from t",
		}, {
			input: "select `name`, dense_rank() over (partition by x RANGE CURRENT ROW) from t",
		}, {
			input: "select `name`, dense_rank() over (partition by x RANGE 2 PRECEDING) from t",
		}, {
			input: "select `name`, dense_rank() over (partition by x RANGE UNBOUNDED PRECEDING) from t",
		}, {
			input: "select `name`, row_number() over (partition by x RANGE interval 5 DAY PRECEDING) from t",
		}, {
			input: "select `name`, row_number() over (partition by x RANGE interval '2:30' MINUTE_SECOND PRECEDING) from t",
		}, {
			input: "select `name`, dense_rank() over (partition by x RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) from t",
		}, {
			input: "select `name`, dense_rank() over (partition by x RANGE BETWEEN CURRENT ROW AND CURRENT ROW) from t",
		}, {
			input: "select `name`, dense_rank() over (partition by x RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) from t",
		}, {
			input: "select `name`, row_number() over (partition by x RANGE BETWEEN interval 5 DAY PRECEDING AND CURRENT ROW) from t",
		}, {
			input: "select `name`, row_number() over (partition by x RANGE BETWEEN interval '2:30' MINUTE_SECOND PRECEDING AND CURRENT ROW) from t",
		}, {
			input: "select `name`, dense_rank() over (w1 partition by x) from t window w1 as ( order by y asc)",
		}, {
			input: "select `name`, dense_rank() over (w1 partition by x), count(*) over w2 from t window w1 as ( order by y asc), w2 as (w1 partition by x)",
		}, {
			input: "select `name`, dense_rank() over w3 from t window w1 as (w2), w2 as (), w3 as (w1)",
		}, {
			input: "select `name`, dense_rank() over window_name from t",
		}, {
			input:  "with a as (select (1) from dual) select name, dense_rank() over window_name from a",
			output: "with a as (select (1)) select `name`, dense_rank() over window_name from a",
		}, {
			input: "select `name`, dense_rank() over (w1 partition by x) from t window w1 as (ROWS UNBOUNDED PRECEDING)",
		}, {
			input: "select `name`, dense_rank() over (w1 partition by x) from t window w1 as (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
		}, {
			input: `SELECT pk,
					(SELECT max(pk) FROM one_pk WHERE pk < opk.pk) as max,
					(SELECT min(pk) FROM one_pk WHERE pk > opk.pk) as min
					FROM one_pk opk
					WHERE (SELECT min(pk) FROM one_pk WHERE pk > opk.pk) IS NOT NULL
					ORDER BY max`,
			useSelectExpressionLiteral: true,
			output: "select pk, (select max(pk) from one_pk where pk < opk.pk) `max`," +
				" (select min(pk) from one_pk where pk > opk.pk) `min` " +
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
			input:  "begin",
			output: "start transaction",
		}, {
			input:  "begin work",
			output: "start transaction",
		}, {
			input:  "start transaction",
			output: "start transaction",
		}, {
			input:  "start transaction read only",
			output: "start transaction read only",
		}, {
			input:  "start transaction read write",
			output: "start transaction read write",
		}, {
			input: "commit",
		}, {
			input:  "commit work",
			output: "commit",
		}, {
			input:  "commit work and chain",
			output: "commit",
		}, {
			input:  "commit work and no chain",
			output: "commit",
		}, {
			input:  "commit work release",
			output: "commit",
		}, {
			input:  "commit work no release",
			output: "commit",
		}, {
			input:  "commit work and chain release",
			output: "commit",
		}, {
			input:  "commit work and chain no release",
			output: "commit",
		}, {
			input:  "commit work and no chain release",
			output: "commit",
		}, {
			input:  "commit work and no chain no release",
			output: "commit",
		}, {
			input: "create database test_db",
		}, {
			input:  "create schema test_db",
		}, {
			input:  "create database if not exists test_db",
		}, {
			input: "alter database test_db character set utf8mb3",
		}, {
			input: "alter database test_db collate utf8mb3_bin",
		}, {
			input: "alter database test_db character set utf8mb3 collate utf8mb3_bin",
		}, {
			input: "alter database character set utf8mb3",
		}, {
			input: "alter database collate utf8mb3_bin",
		}, {
			input: "alter database character set utf8mb3 collate utf8mb3_bin",
		}, {
			input: "drop database test_db",
		}, {
			input:  "drop schema test_db",
		}, {
			input:  "drop database if exists test_db",
		}, {
			input: "drop trigger trigger1",
		}, {
			input: "drop trigger if exists t2",
		}, {
			input: "drop trigger dbName.trigger2",
		}, {
			input: "drop trigger if exists dbName.trigger3",
		}, {
			input:  "create table t (c int not null default 0 on update current_timestamp() auto_increment comment 'a comment here' unique)",
			output: "create table t (\n\tc int not null default 0 on update current_timestamp(0) auto_increment comment 'a comment here' unique\n)",
		}, {
			input:  "create table t (c int null default 0 on update current_timestamp() auto_increment comment 'a comment here' unique)",
			output: "create table t (\n\tc int default 0 on update current_timestamp(0) auto_increment comment 'a comment here' unique\n)",
		}, {
			input:  "create table t (c INT NOT NULL DEFAULT 0 ON UPDATE current_timestamp() AUTO_INCREMENT COMMENT 'a comment here' UNIQUE)",
			output: "create table t (\n\tc INT not null default 0 on update current_timestamp(0) auto_increment comment 'a comment here' unique\n)",
		}, {
			// Same input with options backwards.
			input:  "create table t (c int unique comment 'a comment here' auto_increment on update current_timestamp() default 0 not null)",
			output: "create table t (\n\tc int not null default 0 on update current_timestamp(0) auto_increment comment 'a comment here' unique\n)",
		}, {
			// Transpose pairs in original
			input:  "create table t (c int default 0 not null auto_increment on update current_timestamp() unique comment 'a comment here')",
			output: "create table t (\n\tc int not null default 0 on update current_timestamp(0) auto_increment comment 'a comment here' unique\n)",
		}, {
			// Transpose pairs in reversed
			input:  "create table t (c int comment 'a comment here' unique on update current_timestamp() auto_increment not null default 0)",
			output: "create table t (\n\tc int not null default 0 on update current_timestamp(0) auto_increment comment 'a comment here' unique\n)",
		}, {
			// Those tests for ALTER TABLE ADD (...
			input:  "alter table t add (c int not null default 0 on update current_timestamp() auto_increment comment 'a comment here' unique)",
			output: "alter table t add column (\n\tc int not null default 0 on update current_timestamp(0) auto_increment comment 'a comment here' unique\n)",
		}, {
			input:  "alter table t add (c int unique comment 'a comment here' auto_increment on update current_timestamp() default 0 not null)",
			output: "alter table t add column (\n\tc int not null default 0 on update current_timestamp(0) auto_increment comment 'a comment here' unique\n)",
		}, {
			input:  "alter table t add (c int default 0 not null auto_increment on update current_timestamp() unique comment 'a comment here')",
			output: "alter table t add column (\n\tc int not null default 0 on update current_timestamp(0) auto_increment comment 'a comment here' unique\n)",
		}, {
			input:  "alter table t add (c int comment 'a comment here' unique on update current_timestamp() auto_increment not null default 0)",
			output: "alter table t add column (\n\tc int not null default 0 on update current_timestamp(0) auto_increment comment 'a comment here' unique\n)",
		}, {
			// Those tests for ALTER TABLE ADD COLUMN name ...
			input:  "alter table t add column c int not null default 0 on update current_timestamp() auto_increment comment 'a comment here' unique",
			output: "alter table t add column (\n\tc int not null default 0 on update current_timestamp(0) auto_increment comment 'a comment here' unique\n)",
		}, {
			input:  "alter table t add column c int unique comment 'a comment here' auto_increment on update current_timestamp() default 0 not null",
			output: "alter table t add column (\n\tc int not null default 0 on update current_timestamp(0) auto_increment comment 'a comment here' unique\n)",
		}, {
			input:  "alter table t add column c int unique comment 'a comment here' auto_increment on update current_timestamp() default 0 not null after foo",
			output: "alter table t add column (\n\tc int not null default 0 on update current_timestamp(0) auto_increment comment 'a comment here' unique\n) after foo",
		}, {
			input:  "alter table t add column c int unique comment 'a comment here' auto_increment on update current_timestamp() default 0 not null first",
			output: "alter table t add column (\n\tc int not null default 0 on update current_timestamp(0) auto_increment comment 'a comment here' unique\n) first",
		}, {
			input:  "alter table t add column c int default 0 not null auto_increment on update current_timestamp() unique comment 'a comment here'",
			output: "alter table t add column (\n\tc int not null default 0 on update current_timestamp(0) auto_increment comment 'a comment here' unique\n)",
		}, {
			input:  "alter table t add column c int comment 'a comment here' unique on update current_timestamp() auto_increment not null default 0",
			output: "alter table t add column (\n\tc int not null default 0 on update current_timestamp(0) auto_increment comment 'a comment here' unique\n)",
		}, {
			input:  "alter table t change foo bar int not null auto_increment first",
			output: "alter table t change column foo (\n\tbar int not null auto_increment\n) first",
		}, {
			input:  "alter table test change v1 v2 varchar(255) character set utf8mb4 binary not null",
			output: "alter table test change column v1 (\n\tv2 varchar(255) character set utf8mb4 binary not null\n)",
		}, {
			input:  "alter table a modify foo int unique comment 'a comment here' auto_increment on update current_timestamp() default 0 not null after bar",
			output: "alter table a modify column foo (\n\tfoo int not null default 0 on update current_timestamp(0) auto_increment comment 'a comment here' unique\n) after bar",
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
			output: "delete a, b from tbl_a as a, tbl_b as b where a.id = b.id and b.`name` = 'test'",
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
			input: "call f1(a) as of '2023-10-10'",
		}, {
			input:  "call f1(a) for system_time as of '2023-10-10'",
			output: "call f1(a) as of '2023-10-10'",
		}, {
			input: "call f1(now(), rand()) as of 'uo5qcl722f891g6aisqp3ma7r41s4ha4'",
		}, {
			input: "drop procedure p1",
		}, {
			input: "drop procedure if exists p1",
		}, {
			input: "drop procedure dbName.p1",
		}, {
			input:  "CREATE DEFINER=`root`@`localhost` PROCEDURE p2() SELECT RAND()",
			output: "create definer = `root`@`localhost` procedure p2 () select RAND()",
		}, {
			input:  "create procedure mydb.p1() select rand()",
			output: "create procedure mydb.p1 () select rand()",
		}, {
			input:  "create procedure p1() select rand()",
			output: "create procedure p1 () select rand()",
		}, {
			input:  "create procedure p1() language sql deterministic sql security invoker select 1+1",
			output: "create procedure p1 () language sql deterministic sql security invoker select 1 + 1",
		}, {
			input:  "call p1()",
			output: "call p1",
		}, {
			input:  "call mydb.p1()",
			output: "call mydb.p1",
		}, {
			input:  "create definer = me procedure p1(v1 int) select now()",
			output: "create definer = `me`@`%` procedure p1 (in v1 int) select now()",
		}, {
			input:  "create definer = me procedure p1(v1 int) comment 'some_comment' not deterministic select now()",
			output: "create definer = `me`@`%` procedure p1 (in v1 int) comment 'some_comment' not deterministic select now()",
		}, {
			input:                      "SELECT FORMAT(45124,2) FROM test",
			output:                     "select FORMAT(45124, 2) from test",
			useSelectExpressionLiteral: true,
		}, {
			input:                      "SELECT FORMAT(45124,2,'de_DE') FROM test",
			output:                     "select FORMAT(45124, 2, 'de_DE') from test",
			useSelectExpressionLiteral: true,
		}, {
			input:  "CREATE USER UserName@localhost",
			output: "create user `UserName`@`localhost`",
		}, {
			input:  "CREATE USER UserName@localhost IDENTIFIED BY 'some_auth'",
			output: "create user `UserName`@`localhost` identified by 'some_auth'",
		}, {
			input:  "CREATE USER UserName@localhost IDENTIFIED BY RANDOM PASSWORD AND IDENTIFIED WITH some_plugin",
			output: "create user `UserName`@`localhost` identified by random password and identified with some_plugin",
		}, {
			input:  "CREATE USER UserName@localhost IDENTIFIED WITH some_plugin INITIAL AUTHENTICATION IDENTIFIED BY RANDOM PASSWORD",
			output: "create user `UserName`@`localhost` identified with some_plugin initial authentication identified by random password",
		}, {
			input:  "CREATE USER UserName1@localhost IDENTIFIED BY 'some_auth1', UserName2@localhost IDENTIFIED BY 'some_auth2' DEFAULT ROLE role1, role2@localhost",
			output: "create user `UserName1`@`localhost` identified by 'some_auth1', `UserName2`@`localhost` identified by 'some_auth2' default role `role1`@`%`, `role2`@`localhost`",
		}, {
			input:  "CREATE USER UserName@localhost REQUIRE NONE",
			output: "create user `UserName`@`localhost`",
		}, {
			input:  "CREATE USER UserName@localhost REQUIRE X509",
			output: "create user `UserName`@`localhost` require X509",
		}, {
			input:  "CREATE USER UserName@localhost REQUIRE SUBJECT 'some_subject' AND ISSUER 'some_issuer'",
			output: "create user `UserName`@`localhost` require issuer 'some_issuer' and subject 'some_subject'",
		}, {
			input:  "CREATE USER UserName@localhost WITH MAX_CONNECTIONS_PER_HOUR 3 MAX_QUERIES_PER_HOUR 20",
			output: "create user `UserName`@`localhost` with max_queries_per_hour 20 max_connections_per_hour 3",
		}, {
			input:  "CREATE USER UserName@localhost PASSWORD EXPIRE NEVER ACCOUNT LOCK PASSWORD HISTORY 5",
			output: "create user `UserName`@`localhost` password expire never password history 5 account lock",
		}, {
			input:  "CREATE USER UserName@localhost PASSWORD_LOCK_TIME UNBOUNDED ACCOUNT LOCK PASSWORD REUSE INTERVAL 90 DAY ACCOUNT UNLOCK",
			output: "create user `UserName`@`localhost` password reuse interval 90 day password_lock_time unbounded",
		}, {
			input:  "CREATE USER UserName@localhost COMMENT 'hello'",
			output: "create user `UserName`@`localhost` attribute '{\"comment\": \"hello\"}'",
		}, {
			input:  "CREATE USER UserName@localhost COMMENT 'text\"here'",
			output: "create user `UserName`@`localhost` attribute '{\"comment\": \"text\\\"here\"}'",
		}, {
			input:  "CREATE USER UserName@localhost ATTRIBUTE '{\"attr\": \"attr_text\"}'",
			output: "create user `UserName`@`localhost` attribute '{\"attr\": \"attr_text\"}'",
		}, {
			input:  "CREATE USER 'UserName'@'%' IDENTIFIED WITH 'caching_sha2_password' AS 'xyz0123'",
			output: "create user `UserName`@`%` identified with caching_sha2_password as 'xyz0123'",
		}, {
			input:  "ALTER USER IF EXISTS foo@bar IDENTIFIED BY 'password1';",
			output: "alter user if exists `foo`@`bar` identified by 'password1'",
		}, {
			input:  "ALTER USER foo@bar IDENTIFIED BY 'password1';",
			output: "alter user `foo`@`bar` identified by 'password1'",
		}, {
			input:  "ALTER USER foo@bar IDENTIFIED BY RANDOM PASSWORD;",
			output: "alter user `foo`@`bar` identified by random password",
		}, {
			input:  "ALTER USER foo@bar IDENTIFIED WITH some_plugin;",
			output: "alter user `foo`@`bar` identified with some_plugin",
		}, {
			input:  "ALTER USER foo@bar IDENTIFIED WITH some_plugin BY 'auth_string';",
			output: "alter user `foo`@`bar` identified with some_plugin by 'auth_string'",
		}, {
			input:  "RENAME USER UserName1@localhost TO UserName2@localhost, UserName3 TO UserName4",
			output: "rename user `UserName1`@`localhost` to `UserName2`@`localhost`, `UserName3`@`%` to `UserName4`@`%`",
		}, {
			input:  "DROP USER UserName",
			output: "drop user `UserName`@`%`",
		}, {
			input:  "DROP USER 'UserName'",
			output: "drop user `UserName`@`%`",
		}, {
			input:  `DROP USER "UserName"`,
			output: "drop user `UserName`@`%`",
		}, {
			input:  `DROP USER "User@Name"`,
			output: "drop user `User@Name`@`%`",
		}, {
			input:  "DROP USER UserName@localhost",
			output: "drop user `UserName`@`localhost`",
		}, {
			input:  "DROP USER UserName@`localhost`",
			output: "drop user `UserName`@`localhost`",
		}, {
			input:  `DROP USER "UserName"@localhost`,
			output: "drop user `UserName`@`localhost`",
		}, {
			input:  "DROP USER 'UserName'@'localhost'",
			output: "drop user `UserName`@`localhost`",
		}, {
			input:  "DROP USER 'User@Name'@`local@host`",
			output: "drop user `User@Name`@`local@host`",
		}, {
			input:  "DROP USER `User``Name`",
			output: "drop user `User``Name`@`%`",
		}, {
			input:  "DROP USER ''",
			output: "drop user ``@`%`",
		}, {
			input:  `DROP USER ""`,
			output: "drop user ``@`%`",
		}, {
			input:  "DROP USER ``",
			output: "drop user ``@`%`",
		}, {
			input:  "DROP USER ''@localhost",
			output: "drop user ``@`localhost`",
		}, {
			input:  "DROP USER ''@",
			output: "drop user ``@``",
		}, {
			input:  "DROP USER UserName1, UserName2",
			output: "drop user `UserName1`@`%`, `UserName2`@`%`",
		}, {
			input:  "DROP USER IF EXISTS UserName",
			output: "drop user if exists `UserName`@`%`",
		}, {
			input:  "DROP USER IF EXISTS 'UserName'@'localhost'",
			output: "drop user if exists `UserName`@`localhost`",
		}, {
			input:  "DROP USER IF EXISTS UserName1, UserName2",
			output: "drop user if exists `UserName1`@`%`, `UserName2`@`%`",
		}, {
			input:  "DROP USER IF EXISTS UserName1, `UserName2`@'localhost'",
			output: "drop user if exists `UserName1`@`%`, `UserName2`@`localhost`",
		}, {
			input:  `DROP USER IF EXISTS "UserName1", "UserName2"@'localhost'`,
			output: "drop user if exists `UserName1`@`%`, `UserName2`@`localhost`",
		}, {
			input:  `DROP USER IF EXISTS UserName1@localhost, 'UserName2'@"localhost"`,
			output: "drop user if exists `UserName1`@`localhost`, `UserName2`@`localhost`",
		}, {
			input:  "CREATE ROLE role1",
			output: "create role `role1`@`%`",
		}, {
			input:  "CREATE ROLE role1, role2@localhost",
			output: "create role `role1`@`%`, `role2`@`localhost`",
		}, {
			input:  "CREATE ROLE IF NOT EXISTS role1",
			output: "create role if not exists `role1`@`%`",
		}, {
			input:  "CREATE ROLE IF NOT EXISTS role1, role2@localhost",
			output: "create role if not exists `role1`@`%`, `role2`@`localhost`",
		}, {
			input:  "DROP ROLE role1",
			output: "drop role `role1`@`%`",
		}, {
			input:  "DROP ROLE role1, role2@localhost",
			output: "drop role `role1`@`%`, `role2`@`localhost`",
		}, {
			input:  "DROP ROLE IF EXISTS role1",
			output: "drop role if exists `role1`@`%`",
		}, {
			input:  "DROP ROLE IF EXISTS role1, role2@localhost",
			output: "drop role if exists `role1`@`%`, `role2`@`localhost`",
		}, {
			input:  "GRANT GRANT OPTION ON * TO UserName",
			output: "grant grant option on * to `UserName`@`%`",
		}, {
			input:  "GRANT ALL PRIVILEGES ON * TO UserName",
			output: "grant all on * to `UserName`@`%`",
		}, {
			input:  "GRANT ALL ON * TO UserName",
			output: "grant all on * to `UserName`@`%`",
		}, {
			input:  "GRANT ALL ON *.* TO UserName",
			output: "grant all on *.* to `UserName`@`%`",
		}, {
			input:  "GRANT ALL ON db.* TO UserName",
			output: "grant all on `db`.* to `UserName`@`%`",
		}, {
			input:  "GRANT ALL ON db.tbl TO UserName",
			output: "grant all on `db`.`tbl` to `UserName`@`%`",
		}, {
			input:  "GRANT ALL ON `db`.`tbl` TO UserName",
			output: "grant all on `db`.`tbl` to `UserName`@`%`",
		}, {
			input:  "GRANT ALL ON tbl TO UserName",
			output: "grant all on `tbl` to `UserName`@`%`",
		}, {
			input:  "GRANT ALL ON TABLE tbl TO UserName",
			output: "grant all on table `tbl` to `UserName`@`%`",
		}, {
			input:  "GRANT SELECT (col1, col2), UPDATE (col2) ON db.tbl TO UserName",
			output: "grant select (`col1`, `col2`), update (`col2`) on `db`.`tbl` to `UserName`@`%`",
		}, {
			input: "GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, RELOAD, SHUTDOWN, PROCESS, " +
				"FILE, REFERENCES, INDEX, ALTER, SHOW DATABASES, SUPER, CREATE TEMPORARY TABLES, LOCK TABLES, " +
				"EXECUTE, REPLICATION SLAVE, REPLICATION CLIENT, CREATE VIEW, SHOW VIEW, CREATE ROUTINE, " +
				"ALTER ROUTINE, CREATE USER, EVENT, TRIGGER, CREATE TABLESPACE, CREATE ROLE, DROP ROLE ON *.* TO " +
				"`UserName`@`%` WITH GRANT OPTION",
			output: "grant select, insert, update, delete, create, drop, reload, shutdown, process, " +
				"file, references, index, alter, show databases, super, create temporary tables, lock tables, " +
				"execute, replication slave, replication client, create view, show view, create routine, " +
				"alter routine, create user, event, trigger, create tablespace, create role, drop role on *.* to " +
				"`UserName`@`%` with grant option",
		}, {
			input:  "GRANT ALL ON tbl TO UserName1@localhost, UserName2",
			output: "grant all on `tbl` to `UserName1`@`localhost`, `UserName2`@`%`",
		}, {
			input:  "GRANT ALL ON tbl TO UserName WITH GRANT OPTION",
			output: "grant all on `tbl` to `UserName`@`%` with grant option",
		}, {
			input:  "GRANT ALL ON tbl TO UserName AS OtherUser",
			output: "grant all on `tbl` to `UserName`@`%` as `OtherUser`@`%`",
		}, {
			input:  "GRANT ALL ON tbl TO UserName AS OtherUser WITH ROLE ALL",
			output: "grant all on `tbl` to `UserName`@`%` as `OtherUser`@`%` with role all",
		}, {
			input:  "GRANT ALL ON tbl TO UserName AS OtherUser WITH ROLE ALL EXCEPT NotThisRole",
			output: "grant all on `tbl` to `UserName`@`%` as `OtherUser`@`%` with role all except `NotThisRole`@`%`",
		}, {
			input:  "GRANT Role1 TO UserName",
			output: "grant `Role1`@`%` to `UserName`@`%`",
		}, {
			input:  "GRANT Role1, Role2 TO UserName1, UserName2",
			output: "grant `Role1`@`%`, `Role2`@`%` to `UserName1`@`%`, `UserName2`@`%`",
		}, {
			input:  "GRANT Role1 TO UserName WITH ADMIN OPTION",
			output: "grant `Role1`@`%` to `UserName`@`%` with admin option",
		}, {
			input:  "GRANT PROXY ON UserName TO Role1, Role2",
			output: "grant proxy on `UserName`@`%` to `Role1`@`%`, `Role2`@`%`",
		}, {
			input:  "GRANT PROXY ON UserName TO Role1, Role2 WITH GRANT OPTION",
			output: "grant proxy on `UserName`@`%` to `Role1`@`%`, `Role2`@`%` with grant option",
		}, {
			input:  "GRANT REPLICATION_SLAVE_ADMIN, GROUP_REPLICATION_ADMIN, BINLOG_ADMIN ON *.* TO 'u1'@'localhost'",
			output: "grant replication_slave_admin, group_replication_admin, binlog_admin on *.* to `u1`@`localhost`",
		}, {
			input:  "REVOKE ALL ON * FROM UserName",
			output: "revoke all on * from `UserName`@`%`",
		}, {
			input:  "REVOKE ALL ON *.* FROM UserName",
			output: "revoke all on *.* from `UserName`@`%`",
		}, {
			input:  "REVOKE ALL ON db.* FROM UserName",
			output: "revoke all on `db`.* from `UserName`@`%`",
		}, {
			input:  "REVOKE ALL ON db.tbl FROM UserName",
			output: "revoke all on `db`.`tbl` from `UserName`@`%`",
		}, {
			input:  "REVOKE ALL ON `db`.`tbl` FROM UserName",
			output: "revoke all on `db`.`tbl` from `UserName`@`%`",
		}, {
			input:  "REVOKE ALL ON tbl FROM UserName",
			output: "revoke all on `tbl` from `UserName`@`%`",
		}, {
			input:  "REVOKE ALL ON TABLE tbl FROM UserName",
			output: "revoke all on table `tbl` from `UserName`@`%`",
		}, {
			input:  "REVOKE SELECT (col1, col2), UPDATE (col2) ON db.tbl FROM UserName",
			output: "revoke select (`col1`, `col2`), update (`col2`) on `db`.`tbl` from `UserName`@`%`",
		}, {
			input:  "REVOKE ALL ON tbl FROM UserName1@localhost, UserName2",
			output: "revoke all on `tbl` from `UserName1`@`localhost`, `UserName2`@`%`",
		}, {
			input:  "REVOKE ALL, GRANT OPTION FROM UserName",
			output: "revoke all privileges, grant option from `UserName`@`%`",
		}, {
			input:  "REVOKE ALL PRIVILEGES, GRANT OPTION FROM UserName",
			output: "revoke all privileges, grant option from `UserName`@`%`",
		}, {
			input:  "REVOKE Role1 FROM UserName",
			output: "revoke `Role1`@`%` from `UserName`@`%`",
		}, {
			input:  "REVOKE Role1, Role2 FROM UserName1, UserName2",
			output: "revoke `Role1`@`%`, `Role2`@`%` from `UserName1`@`%`, `UserName2`@`%`",
		}, {
			input:  "REVOKE PROXY ON UserName FROM Role1, Role2",
			output: "revoke proxy on `UserName`@`%` from `Role1`@`%`, `Role2`@`%`",
		}, {
			input:  "REVOKE PROXY ON UserName FROM Role1, Role2",
			output: "revoke proxy on `UserName`@`%` from `Role1`@`%`, `Role2`@`%`",
		}, {
			input:  "FLUSH PRIVILEGES",
			output: "flush privileges",
		}, {
			input:  "FLUSH BINARY LOGS",
			output: "flush binary logs",
		}, {
			input:  "FLUSH USER_RESOURCES",
			output: "flush user_resources",
		}, {
			input:  "FLUSH RELAY LOGS",
			output: "flush relay logs",
		}, {
			input:  "FLUSH LOCAL RELAY LOGS FOR CHANNEL 'connections'",
			output: "flush local relay logs for channel connections",
		}, {
			input:  "FLUSH LOCAL reLay lOgs FOR CHANNEL 'connections'",
			output: "flush local relay logs for channel connections",
		}, {
			input:  "FLUSH LOCAL OPTIMIZER_COSTS",
			output: "flush local optimizer_costs",
		}, {
			input:  "FLUSH NO_WRITE_TO_BINLOG HOSTS",
			output: "flush no_write_to_binlog hosts",
		}, {
			input:  "FLUSH TABLE `inventory`.`customers` WITH READ LOCK",
			output: "flush table inventory.customers with read lock",
		}, {
			input:  "FLUSH TABLES `inventory`.`customers` WITH READ LOCK",
			output: "flush tables inventory.customers with read lock",
		}, {
			input:  "FLUSH TABLES `inventory`.`customers`",
			output: "flush tables inventory.customers",
		}, {
			input:  "FLUSH TABLE table1, foo.table2 WITH READ LOCK",
			output: "flush table table1, foo.table2 with read lock",
		}, {
			input:  "FLUSH TABLES table1, foo.table2 WITH READ LOCK",
			output: "flush tables table1, foo.table2 with read lock",
		}, {
			input:  "SHOW GRANTS",
			output: "show grants",
		}, {
			input:  "SHOW GRANTS FOR UserName",
			output: "show grants for `UserName`@`%`",
		}, {
			input:  "SHOW GRANTS FOR Current_User",
			output: "show grants for Current_User()",
		}, {
			input:  "SHOW GRANTS FOR Current_User()",
			output: "show grants for Current_User()",
		}, {
			input:  "SHOW GRANTS FOR UserName USING role1, role2",
			output: "show grants for `UserName`@`%` using `role1`@`%`, `role2`@`%`",
		}, {
			input:  "SHOW PRIVILEGES",
			output: "show privileges",
		}, {
			input:  "kill query 123",
			output: "kill query 123",
		}, {
			input:  "kill connection 423",
			output: "kill connection 423",
		}, {
			input:  "SELECT * FROM information_schema.processlist",
			output: "select * from information_schema.`processlist`",
		}, {
			input:  "CREATE DATABASE `dolt_testing` DEFAULT CHARACTER SET latin1",
			output: "create database dolt_testing default character set latin1",
		}, {
			input:  "CREATE DATABASE `dolt_testing` DEFAULT CHARACTER SET=latin1",
			output: "create database dolt_testing default character set latin1",
		}, {
			input:  "CREATE DATABASE `dolt_testing` DEFAULT CHARSET latin1",
			output: "create database dolt_testing default charset latin1",
		}, {
			input:  "CREATE DATABASE `dolt_testing` DEFAULT COLLATE latin1_general_ci",
			output: "create database dolt_testing default collate latin1_general_ci",
		}, {
			input:  "CREATE DATABASE `dolt_testing` COLLATE latin1_general_ci CHARACTER SET latin1",
			output: "create database dolt_testing collate latin1_general_ci character set latin1",
		}, {
			input:  "CREATE DATABASE `dolt_testing` DEFAULT COLLATE cp1257_lithuanian_ci",
			output: "create database dolt_testing default collate cp1257_lithuanian_ci",
		}, {
			input:  "CREATE DATABASE `dolt_testing` DEFAULT CHARACTER SET latin1 DEFAULT COLLATE latin1_general_ci",
			output: "create database dolt_testing default character set latin1 default collate latin1_general_ci",
		}, {
			input:  "CREATE DATABASE IF NOT EXISTS `test` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT ENCRYPTION='N'",
			output: "create database if not exists test default character set utf8mb4 collate utf8mb4_0900_ai_ci default encryption N",
		}, {
			input:  "CREATE DATABASE `somedb` CHARACTER SET binary CHARSET binary COLLATE binary collate binary encryption 'n' encryption 'n'",
			output: "create database somedb character set binary charset binary collate binary collate binary encryption n encryption n",
		}, {
			input:  "create table test (pk varchar(255) collate 'utf8_unicode_ci')",
			output: "create table test (\n\tpk varchar(255) collate utf8_unicode_ci\n)",
		}, {
			input:  "create table test (pk varchar(255) collate utf8_unicode_ci)",
			output: "create table test (\n\tpk varchar(255) collate utf8_unicode_ci\n)",
		}, {
			input:  "create table test (pk varchar(255)) collate 'utf8_unicode_ci'",
			output: "create table test (\n\tpk varchar(255)\n) collate utf8_unicode_ci",
		}, {
			input:  "create table test (pk varchar(255)) collate utf8_unicode_ci",
			output: "create table test (\n\tpk varchar(255)\n) collate utf8_unicode_ci",
		}, {
			input:  "create table test (pk varchar(255) charset binary)",
			output: "create table test (\n\tpk varchar(255) character set binary\n)",
		}, {
			input:  "create table test (pk varchar(255) charset utf8mb4)",
			output: "create table test (\n\tpk varchar(255) character set utf8mb4\n)",
		}, {
			input:  "create table test (pk varchar(255) character set binary)",
			output: "create table test (\n\tpk varchar(255) character set binary\n)",
		}, {
			input:  "create table test (pk varchar(255) character set utf8mb4)",
			output: "create table test (\n\tpk varchar(255) character set utf8mb4\n)",
		}, {
			input:  "create table test (pk varchar(255) collate binary)",
			output: "create table test (\n\tpk varchar(255) collate binary\n)",
		}, {
			input:  "create table test (pk varchar(255) collate utf8_unicode_ci)",
			output: "create table test (\n\tpk varchar(255) collate utf8_unicode_ci\n)",
		}, {
			input:  "create table test (pk varchar(255) charset binary collate binary)",
			output: "create table test (\n\tpk varchar(255) character set binary collate binary\n)",
		}, {
			input:  "create table test (pk varchar(255) charset utf8mb4 collate utf8_unicode_ci)",
			output: "create table test (\n\tpk varchar(255) character set utf8mb4 collate utf8_unicode_ci\n)",
		}, {
			input:  "create table test (pk varchar(255) character set binary collate binary)",
			output: "create table test (\n\tpk varchar(255) character set binary collate binary\n)",
		}, {
			input:  "create table test (pk varchar(255) character set utf8mb4 collate utf8_unicode_ci)",
			output: "create table test (\n\tpk varchar(255) character set utf8mb4 collate utf8_unicode_ci\n)",
		}, {
			input:  "select * from current",
			output: "select * from `current`",
		}, {
			input:  "select * from prev as current",
			output: "select * from prev as `current`",
		}, {
			input:  "select current from table1",
			output: "select `current` from table1",
		}, {
			input:  "select prev as current from table1",
			output: "select prev as `current` from table1",
		}, {
			input:  "CREATE TABLE mytable (h int DEFAULT (date_format(now(),_utf8mb4'%Y')))",
			output: "create table mytable (\n\th int default (date_format(now(), _utf8mb4 '%Y'))\n)",
		}, {
			input:  "CREATE TABLE mytable (pk int NOT NULL, col2 varchar(20) NOT NULL DEFAULT 'sometext', PRIMARY KEY (pk), CONSTRAINT status CHECK (col2 like _utf8mb4'%sometext%'))",
			output: "create table mytable (\n\tpk int not null,\n\tcol2 varchar(20) not null default 'sometext',\n\tPRIMARY KEY (pk),\n\tconstraint status check (col2 like _utf8mb4 '%sometext%')\n)",
		}, {
			input:  "create table t (pk int not null, primary key `pk_id` (`pk`))",
			output: "create table t (\n\tpk int not null,\n\tprimary key (pk)\n)",
		}, {
			input:  "create table t (password int not null, primary key (password))",
			output: "create table t (\n\t`password` int not null,\n\tprimary key (`password`)\n)",
		}, {
			input:  "create table t (pk int not null, constraint `mykey` primary key `pk_id` (`pk`))",
			output: "create table t (\n\tpk int not null,\n\tprimary key (pk)\n)",
		},
		{
			input:  "alter table t default character set utf8mb4",
			output: "alter table t character set utf8mb4",
		},
		{
			input:  "alter table t character set = utf8mb4",
			output: "alter table t character set utf8mb4",
		},
		{
			input:  "alter table t character set := utf8mb4",
			output: "alter table t character set utf8mb4",
		},
		{
			input:  "alter table t character set utf8mb4 collate utf8mb4_0900_bin",
			output: "alter table t character set utf8mb4 collate utf8mb4_0900_bin",
		},
		{
			input:  "alter table t character set 'utf8mb4' collate = 'utf8mb4_0900_bin'",
			output: "alter table t character set utf8mb4 collate utf8mb4_0900_bin",
		},
		{
			input:  "alter table t collate utf8mb4_0900_bin",
			output: "alter table t collate utf8mb4_0900_bin",
		},
		{
			input:  "alter table t collate = 'utf8mb4_0900_bin'",
			output: "alter table t collate utf8mb4_0900_bin",
		},
		{
			input:  "alter table t collate = 'utf8mb4_0900_bin'",
			output: "alter table t collate utf8mb4_0900_bin",
		},
		{
			input:  "alter table t convert to character set utf8mb4",
			output: "alter table t character set utf8mb4",
		},
		{
			input:  "alter table t convert to character set utf8mb4 collate utf8mb4_0900_bin",
			output: "alter table t character set utf8mb4 collate utf8mb4_0900_bin",
		},
		{
			input:  "SELECT _utf8mb4'abc'",
			output: "select _utf8mb4 'abc'",
		}, {
			input:  "SELECT _latin1 X'4D7953514C'",
			output: "select _latin1 X'4D7953514C'",
		}, {
			input:  "SELECT _utf8mb4'abc' COLLATE utf8mb4_danish_ci",
			output: "select _utf8mb4 'abc' collate utf8mb4_danish_ci",
		}, {
			input:  "CREATE TABLE engine_cost (cost_name varchar(64) NOT NULL PRIMARY KEY, default_value float GENERATED ALWAYS AS ((case cost_name when _utf8mb3'io_block_read_cost' then 1.0 when _utf8mb3'memory_block_read_cost' then 0.25 else NULL end)) VIRTUAL)",
			output: "create table engine_cost (\n\tcost_name varchar(64) not null primary key,\n\tdefault_value float generated always as ((case cost_name when _utf8mb3 'io_block_read_cost' then 1.0 when _utf8mb3 'memory_block_read_cost' then 0.25 else null end)) virtual\n)",
		}, {
			input:  "CREATE VIEW myview AS SELECT concat(a.first_name, _utf8mb4 ' ', a.last_name) AS name, if(a.active, _utf8mb4 'active', _utf8mb4 '') AS notes FROM a",
			output: "create view myview as select concat(a.first_name, _utf8mb4 ' ', a.last_name) as `name`, if(a.active, _utf8mb4 'active', _utf8mb4 '') as notes from a",
		}, {
			input:  "create view v_today(today) as select CURRENT_DATE()",
			output: "create view v_today(today) as select CURRENT_DATE()",
		}, {
			input:  "create or replace view v_today(today) as select CURRENT_DATE()",
			output: "create or replace view v_today(today) as select CURRENT_DATE()",
		}, {
			input:  "create view v_today(today) as select CURRENT_DATE() WITH CHECK OPTION",
			output: "create view v_today(today) as select CURRENT_DATE() with cascaded check option",
		}, {
			input:  "create view v_today(today) as select CURRENT_DATE() WITH CASCADED CHECK OPTION",
			output: "create view v_today(today) as select CURRENT_DATE() with cascaded check option",
		}, {
			input:  "create view v_today(today) as select CURRENT_DATE() WITH LOCAL CHECK OPTION",
			output: "create view v_today(today) as select CURRENT_DATE() with local check option",
		}, {
			input: "select 1 into @aaa",
		}, {
			input:  "select now() into @late where now() > '2019-04-04 13:25:44'",
			output: "select now() where now() > '2019-04-04 13:25:44' into @late",
		}, {
			input:  "SELECT now() WHERE now() > '2019-04-04 13:25:44' INTO @late",
			output: "select now() where now() > '2019-04-04 13:25:44' into @late",
		},
		{
			input:  "SELECT * FROM (VALUES ROW(2,4,8)) AS t INTO @x,@y,@z",
			output: "select * from (values row(2, 4, 8)) as t into @x, @y, @z",
		},
		{
			input:  "SELECT * FROM (VALUES ROW(2,4,8)) AS t(a,b,c) INTO @x,@y,@z",
			output: "select * from (values row(2, 4, 8)) as t (a, b, c) into @x, @y, @z",
		},
		{
			input:  "SELECT * FROM (VALUES (2,4,8)) AS t INTO @x,@y,@z",
			output: "select * from (values row(2, 4, 8)) as t into @x, @y, @z",
		},
		{
			input:  "SELECT * FROM (VALUES (2,4,8)) AS t(a,b,c) INTO @x,@y,@z",
			output: "select * from (values row(2, 4, 8)) as t (a, b, c) into @x, @y, @z",
		},
		{
			input:  "SELECT * FROM (VALUES (1, 2, 3), (4, 5, 6), (7, 8, 9)) t",
			output: "select * from (values row(1, 2, 3), row(4, 5, 6), row(7, 8, 9)) as t",
		},
		{
			input:  "SELECT id FROM mytable ORDER BY id DESC LIMIT 1 INTO @myvar",
			output: "select id from mytable order by id desc limit 1 into @myvar",
		}, {
			input:  "SELECT id INTO @myvar FROM mytable GROUP BY id LIMIT 1",
			output: "select id from mytable group by id limit 1 into @myvar",
		}, {
			input:  "SELECT m.id, t.category FROM mytable m JOIN testtable t on m.id = t.id LIMIT 1 INTO @myId, @myCategory",
			output: "select m.id, t.category from mytable as m join testtable as t on m.id = t.id limit 1 into @myId, @myCategory",
		}, {
			input:  "select * from foo limit a",
			output: "select * from foo limit a",
		}, {
			input:  "select * from foo limit a ,a",
			output: "select * from foo limit a, a",
		}, {
			input:  "select * from foo limit a offset a",
			output: "select * from foo limit a, a",
		}, {
			input:  "SELECT id FROM mytable UNION select id FROM testtable LIMIT 1 INTO @myId",
			output: "select id from mytable union select id from testtable limit 1 into @myId",
		}, {
			input:  "SELECT id FROM mytable UNION select id FROM testtable UNION select id FROM othertable LIMIT 1 INTO @myId",
			output: "select id from mytable union select id from testtable union select id from othertable limit 1 into @myId",
		},
		{
			input:  "SELECT 1 INTO OUTFILE 'x.txt'",
			output: "select 1 into outfile 'x.txt'",
		},
		{
			input:  "SELECT * FROM (VALUES ROW(2,4,8),ROW(1,2,3)) AS t(a,b,c) INTO OUTFILE 'myfile.txt'",
			output: "select * from (values row(2, 4, 8), row(1, 2, 3)) as t (a, b, c) into outfile 'myfile.txt'",
		},
		{
			input:  "SELECT id INTO OUTFILE 'myfile.txt' FROM mytable ORDER BY id DESC",
			output: "select id from mytable order by id desc into outfile 'myfile.txt'",
		},
		{
			input:  "SELECT * FROM (VALUES ROW(2,4,8)) AS t INTO DUMPFILE 'even.dump'",
			output: "select * from (values row(2, 4, 8)) as t into dumpfile 'even.dump'",
		},
		{
			input:  "SELECT id INTO DUMPFILE 'dump.txt' FROM mytable ORDER BY id DESC LIMIT 15",
			output: "select id from mytable order by id desc limit 15 into dumpfile 'dump.txt'",
		},
		{
			input: "select * from tbl into outfile 'outfile.txt' fields terminated by 'a'",
		},
		{
			input:  "select * from tbl into outfile 'outfile.txt' columns terminated by 'a'",
			output: "select * from tbl into outfile 'outfile.txt' fields terminated by 'a'",
		},
		{
			input: "select * from tbl into outfile 'outfile.txt' fields terminated by 'a' enclosed by 'b'",
		},
		{
			input: "select * from tbl into outfile 'outfile.txt' fields terminated by 'a' optionally enclosed by 'b'",
		},
		{
			input: "select * from tbl into outfile 'outfile.txt' fields terminated by 'a' escaped by 'c'",
		},
		{
			input: "select * from tbl into outfile 'outfile.txt' fields terminated by 'a' enclosed by 'b' escaped by 'c'",
		},
		{
			input: "select * from tbl into outfile 'outfile.txt' fields terminated by 'a' optionally enclosed by 'b' escaped by 'c'",
		},
		{
			input: "select * from tbl into outfile 'outfile.txt' lines terminated by 'd'",
		},
		{
			input: "select * from tbl into outfile 'outfile.txt' lines starting by 'd' terminated by 'e'",
		},
		{
			input: "select * from tbl into outfile 'outfile.txt' fields terminated by 'a' lines terminated by 'd'",
		},
		{
			input: "select * from tbl into outfile 'outfile.txt' fields terminated by 'a' enclosed by 'b' lines terminated by 'd'",
		},
		{
			input: "select * from tbl into outfile 'outfile.txt' fields terminated by 'a' escaped by 'c' lines terminated by 'd'",
		},
		{
			input: "select * from tbl into outfile 'outfile.txt' fields terminated by 'a' optionally enclosed by 'b' escaped by 'c' lines terminated by 'd'",
		},
		{
			input: "select * from tbl into outfile 'outfile.txt' character set binary fields terminated by 'a'",
		},
		{
			input:  "table tbl into outfile 'outfile.txt' fields terminated by 'a' optionally enclosed by 'b' escaped by 'c' lines terminated by 'd'",
			output: "select * from tbl into outfile 'outfile.txt' fields terminated by 'a' optionally enclosed by 'b' escaped by 'c' lines terminated by 'd'",
		},
		{
			input:  "CREATE PROCEDURE proc (IN p_store_id INT, OUT current INT) SELECT COUNT(*) INTO current FROM inventory WHERE store_id = p_store_id",
			output: "create procedure proc (in p_store_id INT, out current INT) select COUNT(*) from inventory where store_id = p_store_id into `current`",
		}, {
			input:  "CREATE PROCEDURE proc (IN p_store_id INT, OUT current INT) SELECT COUNT(*) FROM inventory WHERE store_id = p_store_id INTO current",
			output: "create procedure proc (in p_store_id INT, out current INT) select COUNT(*) from inventory where store_id = p_store_id into `current`",
		}, {
			input:  "CREATE PROCEDURE proc (IN p_store_id INT, OUT code INT, OUT amount INT) SELECT id, quantity INTO code, amount FROM inventory WHERE store_id = p_store_id",
			output: "create procedure proc (in p_store_id INT, out code INT, out amount INT) select id, quantity from inventory where store_id = p_store_id into code, amount",
		}, {
			input:  "CREATE PROCEDURE proc (IN p_store_id INT, INOUT amount INT) SELECT COUNT(*) FROM inventory WHERE store_id = p_store_id AND quantity = amount INTO amount",
			output: "create procedure proc (in p_store_id INT, inout amount INT) select COUNT(*) from inventory where store_id = p_store_id and quantity = amount into amount",
		}, {
			input:  "CREATE PROCEDURE new_proc(IN t VARCHAR(100)) SELECT id, name FROM mytable WHERE id < 100 AND name = t INTO OUTFILE 'logs.txt'",
			output: "create procedure new_proc (in t VARCHAR(100)) select id, `name` from mytable where id < 100 and `name` = t into outfile 'logs.txt'",
		}, {
			input:  "CREATE PROCEDURE proc (IN p_store_id INT) SELECT * FROM inventory WHERE store_id = p_store_id INTO DUMPFILE 'dumpfile.txt'",
			output: "create procedure proc (in p_store_id INT) select * from inventory where store_id = p_store_id into dumpfile 'dumpfile.txt'",
		}, {
			input:  "CREATE TABLE t (id INT PRIMARY KEY, col1 GEOMETRY SRID 0)",
			output: "create table t (\n\tid INT primary key,\n\tcol1 GEOMETRY srid 0\n)",
		}, {
			input:  "CREATE TABLE t (id INT PRIMARY KEY, col1 POLYGON NULL SRID 0)",
			output: "create table t (\n\tid INT primary key,\n\tcol1 POLYGON srid 0\n)",
		}, {
			input:  "CREATE TABLE t (id INT PRIMARY KEY, col1 LINESTRING NULL SRID 0 COMMENT 'my comment')",
			output: "create table t (\n\tid INT primary key,\n\tcol1 LINESTRING srid 0 comment 'my comment'\n)",
		}, {
			input:  "CREATE TABLE t (id INT PRIMARY KEY, col1 GEOMETRYCOLLECTION NOT NULL SRID 0)",
			output: "create table t (\n\tid INT primary key,\n\tcol1 GEOMETRYCOLLECTION not null srid 0\n)",
		},
		{
			input:  "ALTER TABLE t ADD COLUMN col1 POINT NOT NULL SRID 0 DEFAULT (POINT(1, 2))",
			output: "alter table t add column (\n\tcol1 POINT not null srid 0 default (POINT(1, 2))\n)",
		},
		{
			input:  "ALTER TABLE t MODIFY COLUMN col1 POINT NOT NULL DEFAULT (POINT(1, 2)) SRID 1234",
			output: "alter table t modify column col1 (\n\tcol1 POINT not null srid 1234 default (POINT(1, 2))\n)",
		},
		{
			input:  "ALTER TABLE t modify col1 varchar(255) NOT NULL COLLATE 'utf8mb4_0900_ai_ci'",
			output: "alter table t modify column col1 (\n\tcol1 varchar(255) collate utf8mb4_0900_ai_ci not null\n)",
		},
		{
			input:  "ALTER TABLE t modify col1 varchar(255) COLLATE 'utf8mb4_0900_ai_ci' NOT NULL",
			output: "alter table t modify column col1 (\n\tcol1 varchar(255) collate utf8mb4_0900_ai_ci not null\n)",
		},
		{
			input:  "CREATE TABLE t (col1 BIGINT PRIMARY KEY, col2 DOUBLE DEFAULT -1.1)",
			output: "create table t (\n\tcol1 BIGINT primary key,\n\tcol2 DOUBLE default -1.1\n)",
		}, {
			input:  "CREATE TABLE t (col1 BIGINT PRIMARY KEY, col2 BIGINT DEFAULT -1)",
			output: "create table t (\n\tcol1 BIGINT primary key,\n\tcol2 BIGINT default -1\n)",
		}, {
			input:  "CREATE TABLE `dual` (id int)",
			output: "create table `dual` (\n\tid int\n)",
		}, {
			input:  "DROP TABLE `dual`",
			output: "drop table `dual`",
		},
		{
			input:  "CREATE TABLE `t4` (`pk` int NOT NULL, `_tinytext` tinytext, `_text` text, `_longtext` longtext, `_mediumtext` mediumtext, PRIMARY KEY (`pk`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;",
			output: "create table t4 (\n\tpk int not null,\n\t_tinytext tinytext,\n\t_text text,\n\t_longtext longtext,\n\t_mediumtext mediumtext,\n\tPRIMARY KEY (pk)\n) ENGINE InnoDB CHARACTER SET utf8mb3",
		},
		{
			input:  "CREATE TABLE `t` (pk int) ENGINE=InnoDB DEFAULT CHARSET=binary;",
			output: "create table t (\n\tpk int\n) ENGINE InnoDB CHARACTER SET binary",
		},
		{
			input:  "CREATE TABLE `t` (pk int) ENGINE=InnoDB CHARACTER SET=binary;",
			output: "create table t (\n\tpk int\n) ENGINE InnoDB CHARACTER SET binary",
		},
		{
			input:  "CREATE TABLE test (\n  data varchar(5) NULL DEFAULT _utf8 \"KZPVD\"\n) ENGINE = InnoDB DEFAULT CHARACTER SET = utf8mb4 DEFAULT COLLATE = UTF8MB4_BIN;",
			output: "create table test (\n\t`data` varchar(5) default _utf8mb3 'KZPVD'\n) ENGINE InnoDB CHARACTER SET utf8mb4 COLLATE UTF8MB4_BIN",
		}, {
			input: "select EXTRACT(DAY from '2020-10-1')",
		}, {
			input:  "DROP EVENT event1",
			output: "drop event event1",
		}, {
			input:  "DROP EVENT IF EXISTS event1",
			output: "drop event if exists event1",
		}, {
			input:  "CREATE EVENT event1 ON SCHEDULE AT '2006-02-10 23:59:00' DO INSERT INTO test.totals VALUES (NOW())",
			output: "create event event1 on schedule at '2006-02-10 23:59:00' do insert into test.totals values (NOW())",
		},
		{
			input:  "CREATE DEFINER = `root`@`localhost` EVENT event1 ON SCHEDULE AT CURRENT_TIMESTAMP + INTERVAL 3 WEEK + INTERVAL 2 DAY DO INSERT INTO mytable VALUES (NOW())",
			output: "create definer = `root`@`localhost` event event1 on schedule at CURRENT_TIMESTAMP(0) + interval 3 WEEK + interval 2 DAY do insert into mytable values (NOW())",
		},
		{
			input:  "CREATE EVENT event1 ON SCHEDULE EVERY 1 MINUTE ENDS CURRENT_TIMESTAMP + INTERVAL 3 HOUR ON COMPLETION PRESERVE disable COMMENT 'new event' DO INSERT INTO mytable VALUES (1)",
			output: "create event event1 on schedule every 1 MINUTE ends CURRENT_TIMESTAMP(0) + interval 3 HOUR on completion preserve disable comment 'new event' do insert into mytable values (1)",
		},
		{
			input:  "CREATE EVENT event1 ON SCHEDULE EVERY 1 MINUTE STARTS CURRENT_TIMESTAMP + INTERVAL 2 HOUR ENDS CURRENT_TIMESTAMP + INTERVAL 3 HOUR ON COMPLETION NOT PRESERVE enable DO INSERT INTO mytable VALUES (1)",
			output: "create event event1 on schedule every 1 MINUTE starts CURRENT_TIMESTAMP(0) + interval 2 HOUR ends CURRENT_TIMESTAMP(0) + interval 3 HOUR enable do insert into mytable values (1)",
		},
		{
			input:  "CREATE EVENT e_call_myproc ON SCHEDULE AT CURRENT_TIMESTAMP + INTERVAL 1 DAY disable on slave DO CALL myproc(5, 27)",
			output: "create event e_call_myproc on schedule at CURRENT_TIMESTAMP(0) + interval 1 DAY disable on slave do call myproc(5, 27)",
		},
		{
			input:  "CREATE EVENT e_call_myproc ON SCHEDULE AT CURRENT_TIMESTAMP DO CALL myproc(5, 27)",
			output: "create event e_call_myproc on schedule at CURRENT_TIMESTAMP(0) do call myproc(5, 27)",
		},
		{
			input:  "CREATE EVENT e_call_myproc ON SCHEDULE AT CURRENT_TIMESTAMP DISABLE DO CALL myproc(5, 27)",
			output: "create event e_call_myproc on schedule at CURRENT_TIMESTAMP(0) disable do call myproc(5, 27)",
		},
		{
			input:  "SHOW EVENTS",
			output: "show events",
		}, {
			input:  "SHOW EVENTS FROM dbName",
			output: "show events from dbName",
		}, {
			input:  "SHOW EVENTS IN dbName",
			output: "show events from dbName",
		}, {
			input:  "SHOW EVENTS IN dbName LIKE 'pattern'",
			output: "show events from dbName like 'pattern'",
		}, {
			input:  "SHOW EVENTS FROM dbName WHERE status = 'enabled'",
			output: "show events from dbName where `status` = 'enabled'",
		}, {
			input:  "SHOW CREATE EVENT myevent",
			output: "show create event myevent",
		}, {
			input:  "ALTER EVENT myevent ON SCHEDULE AT CURRENT_TIMESTAMP;",
			output: "alter event myevent on schedule at CURRENT_TIMESTAMP(0)",
		}, {
			input:  "ALTER EVENT myevent ON COMPLETION NOT PRESERVE",
			output: "alter event myevent on completion not preserve",
		}, {
			input:  "ALTER EVENT myevent RENAME TO event1",
			output: "alter event myevent rename to event1",
		}, {
			input:  "ALTER EVENT mydb.myevent RENAME TO newdb.event1",
			output: "alter event mydb.myevent rename to newdb.event1",
		}, {
			input:  "ALTER EVENT myevent ENABLE",
			output: "alter event myevent enable",
		}, {
			input:  "ALTER EVENT myevent COMMENT 'add comment'",
			output: "alter event myevent comment 'add comment'",
		}, {
			input:  "ALTER EVENT myevent DO INSERT INTO mytable values (1)",
			output: "alter event myevent do insert into mytable values (1)",
		}, {
			input:  "ALTER EVENT myevent ON SCHEDULE EVERY 1 MINUTE STARTS CURRENT_TIMESTAMP DO INSERT INTO mytable values (1)",
			output: "alter event myevent on schedule every 1 MINUTE starts CURRENT_TIMESTAMP(0) do insert into mytable values (1)",
		}, {
			input:  "ALTER EVENT myevent RENAME TO new_event DISABLE COMMENT 'renaming and disabling the event'",
			output: "alter event myevent rename to new_event disable comment 'renaming and disabling the event'",
		}, {
			input:  "ALTER DEFINER = `newuser`@`localhost` EVENT myevent ON COMPLETION NOT PRESERVE;",
			output: "alter definer = `newuser`@`localhost` event myevent on completion not preserve",
		}, {
			input:  "SELECT * FROM information_schema.events;",
			output: "select * from information_schema.`events`",
		}, {
			input:  "SELECT Event.name AS event FROM Event ORDER BY Event.name",
			output: "select `Event`.`name` as `event` from `Event` order by `Event`.`name` asc",
		}, {
			input:  "ALTER TABLE webhook_events ADD COLUMN event varchar(255) DEFAULT NULL;",
			output: "alter table webhook_events add column (\n\t`event` varchar(255) default null\n)",
		},
		// Create Spatial Reference System Statements
		{
			input: "create spatial reference system 1234\n" +
				"name 'name'\n" +
				"definition 'definition'\n" +
				"organization 'organization' identified by 4321\n" +
				"description 'description'",
		},
		{
			input: "create spatial reference system if not exists 1234\n" +
				"name 'name'\n" +
				"definition 'definition'\n" +
				"organization 'organization' identified by 4321\n" +
				"description 'description'",
		},
		{
			input: "create or replace spatial reference system 1234\n" +
				"name 'name'\n" +
				"definition 'definition'\n" +
				"organization 'organization' identified by 4321\n" +
				"description 'description'",
		},
		{
			input: "create spatial reference system 1234\n" +
				"organization 'organization' identified by 4321\n" +
				"definition 'definition'\n" +
				"name 'name'\n" +
				"description 'description'",
			output: "create spatial reference system 1234\n" +
				"name 'name'\n" +
				"definition 'definition'\n" +
				"organization 'organization' identified by 4321\n" +
				"description 'description'",
		},
		{
			input: "create table t (i int) secondary_engine=rapid",
			output: "create table t (\n" +
				"\ti int\n" +
				") secondary_engine rapid",
		},
		{
			input: "create table t (i int) secondary_engine='rapid'",
			output: "create table t (\n" +
				"\ti int\n" +
				") secondary_engine rapid",
		},
		{
			input: "create table t (i int) secondary_engine=NULL",
			output: "create table t (\n" +
				"\ti int\n" +
				") secondary_engine NULL",
		},
		{
			input: "create table t (i int) secondary_engine NULL",
			output: "create table t (\n" +
				"\ti int\n" +
				") secondary_engine NULL",
		},
		// No-op alter statements
		{
			input: "alter table t alter constraint name enforced",
			output: "alter table t",
		},
		{
			input: "alter table t alter check name enforced",
			output: "alter table t",
		},
		{
			input: "alter table t alter constraint name not enforced",
			output: "alter table t",
		},
		{
			input: "alter table t alter check name not enforced",
			output: "alter table t",
		},
		{
			input: "alter table t algorithm default",
			output: "alter table t",
		},
		{
			input: "alter table t algorithm instant",
			output: "alter table t",
		},
		{
			input: "alter table t algorithm inplace",
			output: "alter table t",
		},
		{
			input: "alter table t algorithm copy",
			output: "alter table t",
		},
		{
			input: "alter table t algorithm = default",
			output: "alter table t",
		},
		{
			input: "alter table t algorithm = instant",
			output: "alter table t",
		},
		{
			input: "alter table t algorithm = inplace",
			output: "alter table t",
		},
		{
			input: "alter table t algorithm = copy",
			output: "alter table t",
		},
		{
			input: "alter table t alter index name visible",
			output: "alter table t",
		},
		{
			input: "alter table t alter index name invisible",
			output: "alter table t",
		},
		{
			input: "alter table t discard tablespace",
			output: "alter table t",
		},
		{
			input: "alter table t import tablespace",
			output: "alter table t",
		},
		{
			input: "alter table t force",
			output: "alter table t",
		},
		{
			input: "alter table t lock default",
			output: "alter table t",
		},
		{
			input: "alter table t lock none",
			output: "alter table t",
		},
		{
			input: "alter table t lock shared",
			output: "alter table t",
		},
		{
			input: "alter table t lock exclusive",
			output: "alter table t",
		},
		{
			input: "alter table t lock = default",
			output: "alter table t",
		},
		{
			input: "alter table t lock = none",
			output: "alter table t",
		},
		{
			input: "alter table t lock = shared",
			output: "alter table t",
		},
		{
			input: "alter table t lock = exclusive",
			output: "alter table t",
		},
		{
			input:  "alter table t autoextend_size='asdf'",
			output: "alter table t",
		},
		{
			input:  "alter table t auto_increment=1",
			output: "alter table t",
		},
		{
			input:  "alter table t avg_row_length=1",
			output: "alter table t",
		},
		{
			input:  "alter table t checksum 1",
			output: "alter table t",
		},
		{
			input:  "alter table t checksum = 1",
			output: "alter table t",
		},
		{
			input:  "alter table t table_checksum 1",
			output: "alter table t",
		},
		{
			input:  "alter table t table_checksum = 1",
			output: "alter table t",
		},
		{
			input:  "alter table t comment='asdf'",
			output: "alter table t",
		},
		{
			input:  "alter table t compression='asdf'",
			output: "alter table t",
		},
		{
			input:  "alter table t connection='asdf'",
			output: "alter table t",
		},
		{
			input:  "alter table t data directory = 'asdf'",
			output: "alter table t",
		},
		{
			input:  "alter table t index directory = 'asdf'",
			output: "alter table t",
		},
		{
			input:  "alter table t delay_key_write = 123",
			output: "alter table t",
		},
		{
			input:  "alter table t encryption='asdf'",
			output: "alter table t",
		},
		{
			input:  "alter table t engine=eng",
			output: "alter table t",
		},
		{
			input:  "alter table t engine_attribute='asdf'",
			output: "alter table t",
		},
		{
			input:  "alter table t insert_method last",
			output: "alter table t",
		},
		{
			input:  "alter table t insert_method=last",
			output: "alter table t",
		},
		{
			input:  "alter table t key_block_size='asdf'",
			output: "alter table t",
		},
		{
			input:  "alter table t max_rows='asdf'",
			output: "alter table t",
		},
		{
			input:  "alter table t min_rows='asdf'",
			output: "alter table t",
		},
		{
			input:  "alter table t pack_keys=123",
			output: "alter table t",
		},
		{
			input:  "alter table t password='asdf'",
			output: "alter table t",
		},
		{
			input:  "alter table t row_format=dynamic",
			output: "alter table t",
		},
		{
			input:  "alter table t secondary_engine=rapid",
			output: "alter table t",
		},
		{
			input:  "alter table t secondary_engine rapid",
			output: "alter table t",
		},
		{
			input:  "alter table t secondary_engine=NULL",
			output: "alter table t",
		},
		{
			input:  "alter table t secondary_engine NULL",
			output: "alter table t",
		},
		{
			input: "create table t (i int) secondary_engine_attribute='rapid'",
			output: "create table t (\n" +
				"\ti int\n" +
				") secondary_engine_attribute rapid",
		},
		{
			input: "create table t (i int) secondary_engine_attribute='rapid'",
			output: "create table t (\n" +
				"\ti int\n" +
				") secondary_engine_attribute rapid",
		},
		{
			input:  "alter table t secondary_engine_attribute='rapid'",
			output: "alter table t",
		},
		{
			input:  "alter table t secondary_engine_attribute 'rapid'",
			output: "alter table t",
		},
		{
			input:  "alter table t stats_auto_recalc = default",
			output: "alter table t",
		},
		{
			input:  "alter table t stats_persistent = default",
			output: "alter table t",
		},
		{
			input:  "alter table t stats_sample_pages = 'asdf'",
			output: "alter table t",
		},
		{
			input:  "alter table t tablespace asdf",
			output: "alter table t",
		},
		{
			input:  "alter table t tablespace asdf storage disk",
			output: "alter table t",
		},
		{
			input:  "alter table t tablespace asdf storage memory",
			output: "alter table t",
		},
		{
			input:  "alter table t union = (a, b, c)",
			output: "alter table t",
		},

		{
			input:  "alter table t modify col varchar(20) not null, algorithm = inplace, lock = none;",
			output: "alter table t modify column col (\n" +
				"\tcol varchar(20) not null\n" +
				"),,",
		},
		{
			input:  "table t",
			output: "select * from t",
		},
		{
			input: "create table t (i int) checksum=0",
			output: "create table t (\n" +
				"\ti int\n" +
				") checksum 0",
		},
		{
			input: "create table t (i int) checksum 100",
			output: "create table t (\n" +
				"\ti int\n" +
				") checksum 100",
		},
		{
			input: "create table t (i int) table_checksum=0",
			output: "create table t (\n" +
				"\ti int\n" +
				") CHECKSUM 0",
		},
		{
			input: "create table t (i int) table_checksum 100",
			output: "create table t (\n" +
				"\ti int\n" +
				") CHECKSUM 100",
		},
		{
			input: "create table t (i int) union (a, b, c)",
			output: "create table t (\n" +
				"\ti int\n" +
				") union (a,b,c)",
		},
		{
			input: "create table t (i int) union (`a`, `b`, `c`)",
			output: "create table t (\n" +
				"\ti int\n" +
				") union (a,b,c)",
		},
		{
			input:  "alter table t union=(a, b, c)",
			output: "alter table t",
		},
		{
			input: "create table t (i int) insert_method last",
			output: "create table t (\n" +
				"\ti int\n" +
				") insert_method last",
		},
		{
			input: "create table t (i int) insert_method=last",
			output: "create table t (\n" +
				"\ti int\n" +
				") insert_method last",
		},

		// Date, Time, and Timestamp literals
		{
			input:  "select date '2020-10-01'",
			output: "select '2020-10-01'",
		},
		{
			input:  "select time '2020-10-01'",
			output: "select '2020-10-01'",
		},
		{
			input:  "select timestamp '2020-10-01'",
			output: "select '2020-10-01'",
		},

		{
			input:  "select date'2020-10-01'",
			output: "select '2020-10-01'",
		},
		{
			input:  "select time'2020-10-01'",
			output: "select '2020-10-01'",
		},
		{
			input:  "select timestamp'2020-10-01'",
			output: "select '2020-10-01'",
		},

		{
			input:  "select (date '2020-10-01')",
			output: "select ('2020-10-01')",
		},
		{
			input:  "select (time '2020-10-01')",
			output: "select ('2020-10-01')",
		},
		{
			input:  "select (timestamp '2020-10-01')",
			output: "select ('2020-10-01')",
		},

		{
			input:  "insert into t values (date '2020-10-01'), (time '2020-10-01'), (timestamp '2020-10-01')",
			output: "insert into t values ('2020-10-01'), ('2020-10-01'), ('2020-10-01')",
		},
		{
			input:  "select * from (values row(date '2020-10-01', time '12:34:56', timestamp '2001-02-03 12:34:56')) t;",
			output: "select * from (values row('2020-10-01', '12:34:56', '2001-02-03 12:34:56')) as t",
		},
	}

	// Any tests that contain multiple statements within the body (such as BEGIN/END blocks) should go here.
	// validSQL is used by TestParseNextValid, which expects a semicolon to mean the end of a full statement.
	// Multi-statement bodies do not follow this expectation, hence they are excluded from TestParseNextValid.
	validMultiStatementSql = []parseTest{
		{
			input:  "create procedure p1 (in v1 int, inout v2 char(2), out v3 datetime) begin select rand() * 10; end",
			output: "create procedure p1 (in v1 int, inout v2 char(2), out v3 datetime) begin\nselect rand() * 10;\nend",
		}, {
			input:  "create procedure p1(v1 datetime)\nif rand() < 1 then select rand();\nend if",
			output: "create procedure p1 (in v1 datetime) if rand() < 1 then select rand();\nend if",
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
			output: "create procedure p1 (in n double, in m double) begin\nset @s = '';\nif n = m then set @s = 'equals';\nelse if n > m then set @s = 'greater';\nelse set @s = 'less';\nend if; set @s = concat('is ', @s, ' than');\nend if;\nset @s = concat(n, ' ', @s, ' ', m, '.');\nselect @s;\nend",
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
		}, {
			input: `create procedure proc1 (IN p_store_id INT, OUT p_film_count INT) READS SQL DATA BEGIN
SELECT COUNT(*) FROM inventory WHERE store_id = p_store_id;
SET p_film_count = 44;
END`,
			output: "create procedure proc1 (in p_store_id INT, out p_film_count INT) reads sql data begin\nselect COUNT(*) from inventory where store_id = p_store_id;\nset p_film_count = 44;\nend",
		}, {
			input: `CREATE DEFINER=root@localhost PROCEDURE film_not_in_stock(IN p_film_id INT, IN p_store_id INT, OUT p_film_count INT)
    READS SQL DATA
BEGIN
     SELECT inventory_id FROM inventory WHERE film_id = p_film_id AND store_id = p_store_id;
     SELECT COUNT(*) FROM inventory WHERE film_id = p_film_id AND store_id = p_store_id INTO p_film_count;
END`,
			output: "create definer = `root`@`localhost` procedure film_not_in_stock (in p_film_id INT, in p_store_id INT, out p_film_count INT) reads sql data begin\nselect inventory_id from inventory where film_id = p_film_id and store_id = p_store_id;\nselect COUNT(*) from inventory where film_id = p_film_id and store_id = p_store_id into p_film_count;\nend",
		},
		{
			input:  "with a(j) as (select 1), b(i) as (select 2) (select j from a union select i from b order by j desc limit 1) union select j from a;",
			output: "with a (j) as (select 1), b (i) as (select 2) (select j from a union select i from b order by j desc limit 1) union select j from a",
		},
		{
			input:  "with a(j) as (select 1) ( with c(k) as (select 3) select k from c union select 6) union select k from c;",
			output: "with a (j) as (select 1) (with c (k) as (select 3) select k from c union select 6) union select k from c",
		},
		{
			input:  "with a(j) as (select 1) ( with c(k) as (select 3) select (select k from c union select 6 limit 1) as b) union select k from c;",
			output: "with a (j) as (select 1) (with c (k) as (select 3) select (select k from c union select 6 limit 1) as b) union select k from c",
		},
		{
			input: `CREATE PROCEDURE testproc() BEGIN
  DECLARE done INT DEFAULT FALSE;
  DECLARE a CHAR(16);
  DECLARE b, c INT;
  DECLARE cur1 CURSOR FOR SELECT id,data FROM test.t1;
  DECLARE cur2 CURSOR FOR SELECT i FROM test.t2;
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

  OPEN cur1;
  OPEN cur2;

  FETCH cur1 INTO a, b;
  FETCH NEXT FROM cur2 INTO c;
  IF b < c THEN
    INSERT INTO test.t3 VALUES (a,b);
  ELSE
    INSERT INTO test.t3 VALUES (a,c);
  END IF;

  CLOSE cur1;
  CLOSE cur2;
END`,
			output: `create procedure testproc () begin
declare done INT default false;
declare a CHAR(16);
declare b, c INT;
declare cur1 cursor for select id, ` + "`data`" + ` from test.t1;
declare cur2 cursor for select i from test.t2;
declare continue handler for not found set done = true;
open cur1;
open cur2;
fetch cur1 into a, b;
fetch cur2 into c;
if b < c then insert into test.t3 values (a, b);
else insert into test.t3 values (a, c);
end if;
close cur1;
close cur2;
end`,
		},
		{
			input: `CREATE PROCEDURE testproc() BEGIN
  DECLARE done INT DEFAULT FALSE;
  DECLARE a CHAR(16);
  DECLARE b INT;
  DECLARE cur1 CURSOR FOR SELECT id, data FROM test.t1;
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

  OPEN cur1;

  read_loop : LOOP
    FETCH cur1 INTO a, b;
    IF done THEN
      LEAVE read_loop;
    END IF;
    IF a < b THEN
      INSERT INTO test.t3 VALUES (a,b);
    ELSE
      ITERATE read_loop;
    END IF;
  END LOOP;

  BEGIN
    DECLARE EXIT HANDLER FOR NOT FOUND BEGIN END;
    LOOP
      FETCH cur1 INTO a, b;
    END LOOP;
  END;

  empty_loop: LOOP
    LEAVE empty_loop;
  END LOOP empty_loop;

  CLOSE cur1;
END`,
			output: `create procedure testproc () begin
declare done INT default false;
declare a CHAR(16);
declare b INT;
declare cur1 cursor for select id, ` + "`data`" + ` from test.t1;
declare continue handler for not found set done = true;
open cur1;
read_loop: loop
fetch cur1 into a, b;
if done then leave read_loop;
end if;
if a < b then insert into test.t3 values (a, b);
else iterate read_loop;
end if;
end loop read_loop;
begin
declare exit handler for not found begin
end;
loop
fetch cur1 into a, b;
end loop;
end;
empty_loop: loop
leave empty_loop;
end loop empty_loop;
close cur1;
end`,
		},
		{
			input: `CREATE PROCEDURE testproc() BEGIN
  DECLARE a INT DEFAULT 10;

  REPEAT
    WHILE a < 5 DO
      BEGIN
      END;
    END WHILE;
  UNTIL a > 5
  END REPEAT;

  repeat1: REPEAT
    while1: WHILE a < 6 DO
      begin1: BEGIN
      END;
    END WHILE;
  UNTIL a > 6
  END REPEAT;

  repeat2: REPEAT
    while2: WHILE a < 7 DO
      begin2: BEGIN
      END begin2;
    END WHILE while2;
  UNTIL a > 7
  END REPEAT repeat2;
END`,
			output: `create procedure testproc () begin
declare a INT default 10;
repeat
while a < 5 do
begin
end;
end while;
until a > 5
end repeat;
repeat1: repeat
while1: while a < 6 do
begin1: begin
end;
end while;
until a > 6
end repeat;
repeat2: repeat
while2: while a < 7 do
begin2: begin
end;
end while;
until a > 7
end repeat;
end`,
		},
		{
			input: `CREATE PROCEDURE testproc() BEGIN
  DECLARE a INT DEFAULT 1;
  CASE a
    WHEN 1 THEN SET a = 10;
    WHEN 2 THEN SET a = 20;
    ELSE SET a = 30;
  END CASE;
  CASE
    WHEN a = 1 THEN SET a = 10;
    WHEN a = 2 THEN SET a = 20;
    ELSE SET a = 30;
  END CASE;
END`,
			output: `create procedure testproc () begin
declare a INT default 1;
case a
when 1 then set a = 10;
when 2 then set a = 20;
else set a = 30;
end case;
case
when a = 1 then set a = 10;
when a = 2 then set a = 20;
else set a = 30;
end case;
end`,
		},
	}

	// validAnsiQuotesSQL contains SQL statements that are valid when the ANSI_QUOTES SQL mode is enabled. This
	// mode treats double quotes (and backticks) as identifier quotes, and single quotes as string quotes.
	validAnsiQuotesSQL = []parseTest{
		{
			input:  `select "count", "foo", "bar" from t order by "COUNT"`,
			output: "select `count`, foo, bar from t order by `COUNT` asc",
		},
		{
			input:  `INSERT INTO hourly_logins ("applications_id", "count", "hour") VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE "count" = "count" + VALUES(count)`,
			output: "insert into hourly_logins(applications_id, `count`, `hour`) values (:v1, :v2, :v3) on duplicate key update count = `count` + values(`count`)",
		},
		{
			input:  `CREATE TABLE "webhook_events" ("pk" int primary key, "event" varchar(255) DEFAULT NULL)`,
			output: "create table webhook_events (\n\tpk int primary key,\n\t`event` varchar(255) default null\n)",
		},
		{
			input:  `with "test" as (select 1 from "dual"), "test_two" as (select 2 from "dual") select * from "test", "test_two" union all (with "b" as (with "c" as (select 1, 2 from "dual") select * from "c") select * from "b")`,
			output: "with test as (select 1 from `dual`), test_two as (select 2 from `dual`) select * from test, test_two union all (with b as (with c as (select 1, 2 from `dual`) select * from c) select * from b)",
		},
		{
			input:  `select '"' from t order by "foo"`,
			output: `select '\"' from t order by foo asc`,
		},
		{
			// Assert that quote escaping is the same as when ANSI_QUOTES is off
			input:  `select '''foo'''`,
			output: `select '\'foo\''`,
		},
		{
			// Assert that quote escaping is the same as when ANSI_QUOTES is off
			input:  `select """""""foo"""""""`,
			output: "select `\"\"\"foo\"\"\"`",
		},
	}
)

// TestSingleSQL is a helper function to test a single SQL statement.
func TestSingleSQL(t *testing.T) {
	t.Skip()
	tests := []parseTest{
		{
			input:  "select @`user var`",
			output: "select @`user var`",
		},
	}
	for _, tcase := range tests {
		runParseTestCase(t, tcase)
	}
}

func TestValid(t *testing.T) {
	validSQL = append(validSQL, validMultiStatementSql...)
	for _, tcase := range validSQL {
		runParseTestCase(t, tcase)
	}
}

func TestAnsiQuotesMode(t *testing.T) {
	parserOptions := ParserOptions{AnsiQuotes: true}
	for _, tcase := range validAnsiQuotesSQL {
		runParseTestCaseWithParserOptions(t, tcase, parserOptions)
	}
	for _, tcase := range invalidAnsiQuotesSQL {
		t.Run(tcase.input, func(t *testing.T) {
			_, err := ParseWithOptions(tcase.input, parserOptions)
			require.NotNil(t, err)
			assert.Equal(t, tcase.output, err.Error())
		})
	}
}

func TestSingle(t *testing.T) {
	validSQL = append(validSQL, validMultiStatementSql...)
	for _, tcase := range validSQL {
		if tcase.input == "select /* use */ 1 from t1 for system_time as of '2019-01-01'" {
			runParseTestCase(t, tcase)
		}
	}
}

func TestGeneratedColumns(t *testing.T) {
	tests := []parseTest{
		{
			input:  "create table t (i int, j int as (i + 1))",
			output: "create table t (\n\ti int,\n\tj int generated always as (i + 1) virtual\n)",
		},
		{
			input:  "create table t (i int, j int as (i + 1) virtual)",
			output: "create table t (\n\ti int,\n\tj int generated always as (i + 1) virtual\n)",
		},
		{
			input:  "create table t (i int, j int as (i + 1) stored)",
			output: "create table t (\n\ti int,\n\tj int generated always as (i + 1) stored\n)",
		},
		{
			input:  "create table t (i int, j int generated always as (i + 1))",
			output: "create table t (\n\ti int,\n\tj int generated always as (i + 1) virtual\n)",
		},
		{
			input:  "create table t (i int, j int generated always as (i + 1) virtual)",
			output: "create table t (\n\ti int,\n\tj int generated always as (i + 1) virtual\n)",
		},
		{
			input:  "create table t (i int, j int generated always as (i + 1) stored)",
			output: "create table t (\n\ti int,\n\tj int generated always as (i + 1) stored\n)",
		},
	}
	for _, tcase := range tests {
		runParseTestCase(t, tcase)
	}
}

// Will throw syntax errors, but shouldn't
func TestNotWorkingIdentifiersStartingWithNumbers(t *testing.T) {
	tests := []parseTest{
		{
			input:  "insert into mydb.2b values (1)",
			output: "insert into mydb.`2b` values (1)",
		}, {
			input:  "insert into 1a.2b values (1)",
			output: "insert into `1a`.`2b` values (1)",
		}, {
			input:  "insert into 1a.2b(3c) values (1)",
			output: "insert into `1a`.`2b`(`3c`) values (1)",
		},
	}
	for _, tcase := range tests {
		t.Skip()
		runParseTestCase(t, tcase)
	}
}

func TestParsingIdentifiersStartingWithNumbers(t *testing.T) {
	tests := []parseTest{
		{
			input:  "create database 1a",
			output: "create database 1a",
		},
		{
			input:  "create table 1a (i int)",
			output: "create table `1a` (\n\ti int\n)",
		},
		{
			input:  "create table t (1a int)",
			output: "create table t (\n\t`1a` int\n)",
		},
		{
			input:  "create table t (123456a int)",
			output: "create table t (\n\t`123456a` int\n)",
		},
		{
			input:  "create table t (1a int primary key, 2b int)",
			output: "create table t (\n\t`1a` int primary key,\n\t`2b` int\n)",
		},
		{
			input:  "alter table t add column 1a int",
			output: "alter table t add column (\n\t`1a` int\n)",
		},
		{
			input:  "alter table t drop column 1a",
			output: "alter table t drop column `1a`",
		},
		{
			input:  "update t set 1a = 5",
			output: "update t set 1a = 5",
		},
		{
			input:  "insert into t (1a) values (1)",
			output: "insert into t(`1a`) values (1)",
		},
		{
			input:  "select 0xH from t",
			output: "select `0xH` from t",
		},
	}
	for _, tcase := range tests {
		runParseTestCase(t, tcase)
	}
}

func TestParseOne(t *testing.T) {
	type tc struct {
		input     string
		remainder string
	}
	cases := []tc{
		{
			"select 1; select 64 * 10;",
			"select 64 * 10;",
		},
		{
			"select 1",
			"",
		},
		{
			"select 1 -- trailing comment",
			"",
		},
		{
			"select 1; -- another trailing comment",
			"-- another trailing comment",
		},
		{
			"select 1    \t\t\n\r\n\t  \t\n",
			"",
		},
		{
			`create trigger t1 before delete on foo for each row follows baz
			begin
				set session foo = old.x;
				set session bar = new.y;
				update baz.t set a = @@foo + @@bar where z = old.x;
			end
			`,
			"",
		},
		{
			`create trigger t1 before delete on foo for each row follows baz
			begin
				set session foo = old.x;
				set session bar = new.y;
				update baz.t set a = @@foo + @@bar where z = old.x;
			end;
			select 1 from dual;`,
			"\t\t\tselect 1 from dual;",
		},
		{
			"\t\t\tselect 1 from dual;",
			"",
		},
		{
			input:     `/*!50800 create view a as select 2 from  dual    */  `,
			remainder: "",
		},
		{
			input:     `/*! create view a as select 2 from  dual    */  ; select * from a`,
			remainder: "select * from a",
		},
	}
	for _, c := range cases {
		t.Run(c.input, func(t *testing.T) {
			stmt, resti, err := ParseOne(c.input)
			require.NoError(t, err)
			require.NotNil(t, stmt)
			var rest string
			if resti < len(c.input) {
				rest = c.input[resti:]
			}
			require.Equal(t, c.remainder, rest)
		})
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
	if tree, ok := tree.(WalkableSQLNode); ok {
		tree.walkSubtree(func(node SQLNode) (kontinue bool, err error) {
			if ae, ok := node.(*AliasedExpr); !tcase.useSelectExpressionLiteral && ok {
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
		output: "create trigger t1 before delete on foo for each row begin case old.x when old.y then set session var = 1; when 0 then update a set b = 2; delete from z; else select true; delete from x; end case; end",
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
	}, {
		// https://github.com/dolthub/dolt/issues/5452
		input: `
/*!50003 CREATE*/ /*!50017 DEFINER=root@localhost*/ /*!50003 TRIGGER forecast_db.before_ai_suggested_task_label_delete
    BEFORE DELETE
    ON forecast_db.ai_suggested_task_label
    FOR EACH ROW
BEGIN
    IF OLD.delete != (1)
    THEN
        SET @message_text =
                CONCAT('CANNOT DELETE ai_suggested_task_label IF DELETE FLAG IS NOT SET FOR: ', CAST(OLD.id AS CHAR));
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = @message_text;
    END IF;
    INSERT INTO forecast_db_events.event_ai_suggested_task_label
        (action, id,created_at,updated_at,created_by,updated_by,company_id,label_id,task_id,delete)
    VALUES ("delete", OLD.id,OLD.created_at,OLD.updated_at,OLD.created_by,OLD.updated_by,OLD.company_id,OLD.label_id,OLD.task_id,OLD.delete);
    END */
`,
		output: "create definer = `root`@`localhost` trigger forecast_db.before_ai_suggested_task_label_delete " +
			"before delete on forecast_db.ai_suggested_task_label for each row begin\n" +
			"if OLD.`delete` != (1) then " +
			"set @message_text = CONCAT('CANNOT DELETE ai_suggested_task_label IF DELETE FLAG IS NOT SET FOR: ', CAST(OLD.id as CHAR)); " +
			"signal sqlstate value '45000' set message_text = @message_text;\n" +
			"end if;\n" +
			"insert into forecast_db_events.event_ai_suggested_task_label(`action`, id, created_at, updated_at, created_by, updated_by, company_id, label_id, task_id, `delete`) " +
			"values ('delete', OLD.id, OLD.created_at, OLD.updated_at, OLD.created_by, OLD.updated_by, OLD.company_id, OLD.label_id, OLD.task_id, OLD.`delete`);\n" +
			"end",
	},
}

func TestValidIgnoreWhitespace(t *testing.T) {
	for _, tcase := range ignoreWhitespaceTests {
		t.Run(tcase.input, func(t *testing.T) {
			if tcase.output == "" {
				tcase.output = tcase.input
			}
			tree, err := Parse(tcase.input)
			require.NoError(t, err)

			out := String(tree)
			normalize := regexp.MustCompile("\\s+")
			normalizedOut := normalize.ReplaceAllLiteralString(out, " ")
			expectedOut := normalize.ReplaceAllLiteralString(tcase.output, " ")
			assert.Equal(t, expectedOut, normalizedOut)

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

// This was a sysbench test I was failing.
// There are strange requirements for the positions of the Tokenizer especially when handling special comments like this
// For the record: this test was never failing as a unit test (only when running dolt as a server?)
// Regardless, we don't appear to have any tests with really long queries, so I'm leaving it.
func TestLongQueries(t *testing.T) {
	query := "/*! CREATE TABLE sbtest1 (\n             \tid INT NOT NULL,\n             \ttiny_int_col TINYINT NOT NULL,\n             \tunsigned_tiny_int_col TINYINT UNSIGNED NOT NULL,\n             \tsmall_int_col SMALLINT NOT NULL,\n             \tunsigned_small_int_col SMALLINT UNSIGNED NOT NULL,\n             \tmedium_int_col MEDIUMINT NOT NULL,\n             \tunsigned_medium_int_col MEDIUMINT UNSIGNED NOT NULL,\n             \tint_col INT NOT NULL,\n             \tunsigned_int_col INT UNSIGNED NOT NULL,\n             \tbig_int_col BIGINT NOT NULL,\n             \tunsigned_big_int_col BIGINT UNSIGNED NOT NULL,\n             \tdecimal_col DECIMAL NOT NULL,\n             \tfloat_col FLOAT NOT NULL,\n             \tdouble_col DOUBLE NOT NULL,\n             \tbit_col BIT NOT NULL,\n             \tchar_col CHAR NOT NULL,\n             \tvar_char_col VARCHAR(64) NOT NULL,\n             \ttiny_text_col TINYTEXT NOT NULL,\n             \ttext_col TEXT NOT NULL,\n             \tmedium_text_col MEDIUMTEXT NOT NULL,\n             \tlong_text_col LONGTEXT NOT NULL,\n             \tenum_col ENUM('val0', 'val1', 'val2') NOT NULL,\n             \tset_col SET('val0', 'val1', 'val2') NOT NULL,\n             \tdate_col DATE NOT NULL,\n             \ttime_col TIME NOT NULL,\n             \tdatetime_col DATETIME NOT NULL,\n             \ttimestamp_col TIMESTAMP NOT NULL,\n             \tyear_col YEAR NOT NULL,\n             \tPRIMARY KEY(id),\n             \tINDEX (big_int_col)\n             ) */"
	_, err := Parse(query)
	assert.NoError(t, err)
}

// Some DDL statements need to record the start and end indexes of a substatement within the original query string.
// These are tests of that functionality, which is tricky to get right in the presence of certain piece of functionality
// like MySQL's special comments.
func TestDDLSelectPosition(t *testing.T) {
	cases := []struct {
		query string
		sel   string
	}{
		{
			query: "create view a as select current_timestamp()",
			sel:   "select current_timestamp()",
		}, {
			query: "create view a as select /* comment */ 2 + 2 from dual",
			sel:   "select /* comment */ 2 + 2 from dual",
		}, {
			query: "/*! create view a as select 2 from dual */",
			sel:   "select 2 from dual",
		}, {
			query: "/*! create view a as select 2 from dual */  ",
			sel:   "select 2 from dual",
		}, {
			query: "/*! create view a as select 2 from  dual    */  ",
			sel:   "select 2 from  dual",
		}, {
			query: "/*!12345 create view a as select 2 from dual */",
			sel:   "select 2 from dual",
		}, {
			query: "/*!50001 CREATE VIEW `some_view` as SELECT 1 AS `x`*/",
			sel:   "SELECT 1 AS `x`",
		}, {
			query: "create or replace view a as select current_timestamp()",
			sel:   "select current_timestamp()",
		}, {
			query: "create or replace view a as select /* comment */ 2 + 2 from dual",
			sel:   "select /* comment */ 2 + 2 from dual",
		}, {
			query: "/*! create or replace view a as select 2 from dual */",
			sel:   "select 2 from dual",
		}, {
			query: "/*! create or replace view a as select 2 from dual */  ",
			sel:   "select 2 from dual",
		}, {
			query: "/*! create or replace view a as select 2 from  dual    */  ",
			sel:   "select 2 from  dual",
		}, {
			query: "/*!12345 create or replace view a as select 2 from dual */",
			sel:   "select 2 from dual",
		}, {
			query: "/*!50001 CREATE OR REPLACE VIEW `some_view` as SELECT 1 AS `x`*/",
			sel:   "SELECT 1 AS `x`",
		}, {
			query: `create procedure p1(n double, m double)
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
			sel: `begin
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
		}, {
			query: "create procedure p1() language sql deterministic sql security invoker select 1+1",
			sel:   "select 1+1",
		}, {
			query: "create procedure p1 (in v1 int, inout v2 char(2), out v3 datetime) begin select rand() * 10; end",
			sel:   "begin select rand() * 10; end",
		}, {
			query: `/*!50400 create procedure p1(n double, m double)
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
end   */  `,
			sel: `begin
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
		}, {
			query: ` /*! create procedure p1(n double, m double)
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
end*/ `,
			sel: `begin
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
		}, {
			query: "/*!50040   create procedure p1() language sql deterministic sql security invoker select 1+1 */",
			sel:   "select 1+1",
		}, {
			query: "/*! create procedure p1 (in v1 int, inout v2 char(2), out v3 datetime) begin select rand() * 10; end */",
			sel:   "begin select rand() * 10; end",
		}, {
			query: "create trigger t1 before update on foo for each row precedes bar update xxy set baz = 1 where a = b",
			sel:   "update xxy set baz = 1 where a = b",
		}, {
			query: "create definer = me trigger t1 before delete on foo for each row follows baz update xxy set x = old.y",
			sel:   "update xxy set x = old.y",
		}, {
			query: `create trigger t1 before delete on foo for each row follows baz
			begin
				set session foo = old.x;
				set session bar = new.y;
				update baz.t set a = @@foo + @@bar where z = old.x;
			end`,
			sel: `			begin
				set session foo = old.x;
				set session bar = new.y;
				update baz.t set a = @@foo + @@bar where z = old.x;
			end`,
		}, {
			query: "/*! create trigger t1 before update on foo for each row precedes bar update xxy set baz = 1 where a = b */",
			sel:   "update xxy set baz = 1 where a = b",
		}, {
			query: "/*!50040 create definer = me trigger t1 before delete on foo for each row follows baz update xxy set x = old.y */ ",
			sel:   "update xxy set x = old.y",
		}, {
			query: `/*!50604 create trigger t1 before delete on foo for each row follows baz
			begin
				set session foo = old.x;
				set session bar = new.y;
				update baz.t set a = @@foo + @@bar where z = old.x;
			end    */`,
			sel: `			begin
				set session foo = old.x;
				set session bar = new.y;
				update baz.t set a = @@foo + @@bar where z = old.x;
			end`,
		},
	}

	for _, tcase := range cases {
		t.Run(tcase.query, func(t *testing.T) {
			tree, err := Parse(tcase.query)
			require.Nil(t, err)

			ddl, ok := tree.(*DDL)
			require.True(t, ok, "Expected DDL when parsing (%q)", tcase.query)
			require.True(t, ddl.SubStatementPositionStart < ddl.SubStatementPositionEnd, "substatement indexes out of order")

			sel := tcase.query[ddl.SubStatementPositionStart:ddl.SubStatementPositionEnd]
			if sel != tcase.sel {
				require.Equal(t, tcase.sel, sel)
			}
		})

		// Also run all these test cases with the ParseOne function (used to execute multiple queries in a single request)
		// to make sure they appropriately consume the correct amount of buffer
		t.Run(tcase.query+" ParseOne", func(t *testing.T) {
			tree, remainder, err := ParseOne(tcase.query)
			require.Nil(t, err)
			require.Equal(t, len(tcase.query)+1, remainder)

			ddl, ok := tree.(*DDL)
			require.True(t, ok, "Expected DDL when parsing (%q)", tcase.query)
			require.True(t, ddl.SubStatementPositionStart < ddl.SubStatementPositionEnd, "substatement indexes out of order")

			sel := tcase.query[ddl.SubStatementPositionStart:ddl.SubStatementPositionEnd]
			if sel != tcase.sel {
				require.Equal(t, tcase.sel, sel)
			}
		})
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
				require.NoError(t, err, tcase.input)

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
		skip  bool
	}{
		{
			input: "SET @foo = `o` `ne`;",
			err:   "syntax error",
		},
		{
			input: "select '1' '2",
			err:   "syntax error",
		},
		{
			input: "CHANGE REPLICATION FILTER",
			err:   "syntax error",
		},
		{
			input: "change replication filter REPLICATE_DO_TABLE=()",
			err:   "syntax error",
		},
		{
			input: "CHANGE REPLICATION SOURCE TO",
			err:   "syntax error",
		},
		{
			input: "select a from (select * from tbl)",
			err:   "Every derived table must have its own alias",
		},
		{
			input: "select a, b from (select * from tbl) sort by a",
			err:   "syntax error",
		},
		{
			input: "with test as (select 1), test_two as (select 2) select * from test, test_two union all with b as (select 1, 2) select * from b",
			err:   "syntax error",
		},
		{
			input: "select * from test order by a union select * from test",
			err:   "syntax error",
		},
		{
			input: "select current_timestamp(-1)",
			err:   "syntax error",
		},
		{
			input: "select current_timestamp(1 + 1)",
			err:   "syntax error",
		},
		{
			input: "select current_timestamp('abc')",
			err:   "syntax error",
		},
		{
			input: "select current_timestamp(1.0)",
			err:   "syntax error",
		},
		{
			input: "select current_timestamp(now())",
			err:   "syntax error",
		},
		{
			input: "select current_timestamp(i)",
			err:   "syntax error",
		},
		{
			input: "select current_timestamp(@a)",
			err:   "syntax error",
		},
		{
			input: "select current_timestamp(null)",
			err:   "syntax error",
		},
		{
			input: "create spatial reference system 1234\n" +
				"name 'name'\n" +
				"name 'name'",
			err: "multiple definitions of attribute name",
		},
		{
			input: "create spatial reference system 1234\n" +
				"definition 'definition'\n" +
				"definition 'definition'\n",
			err: "multiple definitions of attribute definition",
		},
		{
			input: "create spatial reference system 1234\n" +
				"organization 'organization' identified by 4321\n" +
				"organization 'organization' identified by 4321",
			err: "multiple definitions of attribute organization",
		},
		{
			input: "create spatial reference system 1234\n" +
				"description 'description'\n" +
				"description 'description'",
			err: "multiple definitions of attribute description",
		},
		{
			input: "create or replace spatial reference system 1234\n" +
				"name 'name'\n" +
				"name 'name'",
			err: "multiple definitions of attribute name",
		},
		{
			input: "create spatial reference system if not exists 1234\n" +
				"name 'name'\n" +
				"name 'name'",
			err: "multiple definitions of attribute name",
		},
		{
			input: "select * from t join lateral t2",
			err:   "syntax error",
		},
		{
			input: "select * from t join lateral (select * from t2)",
			err:   "Every derived table must have its own alias",
		},
		{
			input: "select distinct all * from t",
			err:   "incorrect usage of DISTINCT and ALL",
		},
		{
			input: "select sql_cache sql_no_cache * from t",
			err:   "incorrect usage of SQL_CACHE and SQL_NO_CACHE",
		},
		{
			input: "select * from tbl into outfile 'outfile.txt' fields terminated by 'a' fields terminated by 'a'",
			err:   "syntax error",
		},
		{
			input: "select * from tbl into outfile 'outfile.txt' enclosed by 'b'",
			err:   "syntax error",
		},
		{
			input: "select * from tbl into outfile 'outfile.txt' escaped by 'c'",
			err:   "syntax error",
		},
		{
			input: "select * from tbl into outfile 'outfile.txt' enclosed by 'b' fields terminated by 'a'",
			err:   "syntax error",
		},
		{
			input: "select * from tbl into outfile 'outfile.txt' escaped by 'c' fields terminated by 'a'",
			err:   "syntax error",
		},
		{
			input: "select * from tbl into outfile 'outfile.txt' lines terminated by 'd' fields terminated by 'a'",
			err:   "syntax error",
		},

		{
			// TODO: should work
			input: "select * from tbl into outfile 'outfile.txt' fields terminated by 'a' enclosed by 'b' enclosed by 'b'",
			err:   "syntax error",
		},
		{
			// TODO: should work
			input: "select * from tbl into outfile 'outfile.txt' fields terminated by 'a' optionally enclosed by 'b' optionally enclosed by 'b'",
			err:   "syntax error",
		},
		{
			// TODO: should work
			input: "select * from tbl into outfile 'outfile.txt' fields terminated by 'a' escaped by 'c' escaped by 'c'",
			err:   "syntax error",
		},
		{
			// TODO: should work
			input: "select * from tbl into outfile 'outfile.txt' lines terminated by 'e' starting by 'd'",
			err:   "syntax error",
		},
		{
			// TODO: should work
			input: "select * from tbl into outfile 'outfile.txt' lines starting by 'd' terminated by 'e' starting by 'd' terminated by 'e'",
			err:   "syntax error",
		},

		{
			input: "select date 20010203",
			err:   "syntax error",
		},
		{
			input: "select date concat('2001-', '02-', '03')",
			err:   "syntax error",
		},
	}

	for _, tcase := range invalidSQL {
		t.Run(tcase.input, func(t *testing.T) {
			_, err := Parse(tcase.input)
			if err == nil {
				t.Errorf("Parse invalid query(%q), got: nil, want: %s...", tcase.input, tcase.err)
			}
			if err != nil && !strings.Contains(err.Error(), tcase.err) {
				t.Errorf("Parse invalid query(%q), got: %v, want: %s...", tcase.input, err, tcase.err)
			}
		})
	}

	invalidDDL := []struct {
		input string
		err   string
	}{{
		input: "create table t (c int not null default 0 on update current_timestamp() auto_increment comment 'a comment here' fulltext key primary key)",
		err:   "cannot include more than one key option for a column definition at position 136 near 'key'",
	}, {
		input: "create table t (c int not null default 0 on update current_timestamp() auto_increment comment 'a comment here' unique comment 'another')",
		err:   "cannot include more than one comment for a column definition at position 136 near 'another'",
	}, {
		input: "create table t (c int not null default 0 on update current_timestamp() auto_increment comment 'a comment here' unique auto_increment)",
		err:   "cannot include AUTO_INCREMENT more than once at position 133 near 'auto_increment'",
	}, {
		input: "create table t (c int not null default 0 on update current_timestamp() auto_increment comment 'a comment here' unique on update utc_timestamp())",
		err:   "syntax error at position 142 near 'utc_timestamp'",
	}, {
		input: "create table t (c int not null default 0 on update current_timestamp() auto_increment comment 'a comment here' unique default 1)",
		err:   "cannot include DEFAULT more than once at position 128",
	}, {
		input: "create table t (c not null int default 0 on update current_timestamp() auto_increment comment 'a comment here' unique)",
		err:   "syntax error at position 22 near 'not'",
	}, {
		input: "create table t (c default 0 int on update current_timestamp() auto_increment comment 'a comment here' unique)",
		err:   "syntax error at position 26 near 'default'",
	}, {
		input: "alter table t add (c int not null default 0 on update current_timestamp() auto_increment comment 'a comment here' fulltext key primary key)",
		err:   "cannot include more than one key option for a column definition at position 139 near 'key'",
	}, {
		input: "alter table t add (c int not null default 0 on update current_timestamp() auto_increment comment 'a comment here' unique comment 'another')",
		err:   "cannot include more than one comment for a column definition at position 139 near 'another'",
	}, {
		input: "alter table t add (c int not null default 0 on update current_timestamp() auto_increment comment 'a comment here' unique auto_increment)",
		err:   "cannot include AUTO_INCREMENT more than once at position 136 near 'auto_increment'",
	}, {
		input: "alter table t add (c int not null default 0 on update current_timestamp() auto_increment comment 'a comment here' unique on update utc_timestamp())",
		err:   "syntax error at position 145 near 'utc_timestamp'",
	}, {
		input: "alter table t add (c int not null default 0 on update current_timestamp() auto_increment comment 'a comment here' unique default 1)",
		err:   "cannot include DEFAULT more than once at position 131",
	}, {
		input: "alter table t add (c not null int default 0 on update current_timestamp() auto_increment comment 'a comment here' unique)",
		err:   "syntax error at position 25 near 'not'",
	}, {
		input: "alter table t add (c default 0 int on update current_timestamp() auto_increment comment 'a comment here' unique)",
		err:   "syntax error at position 29 near 'default'",
	}, {
		input: "create role ''@localhost",
		err:   "the anonymous user is not a valid role name",
	}, {
		input: "CREATE USER UserName@localhost REQUIRE SUBJECT 'some_subject1' AND SUBJECT 'some_subject2'",
		err:   "invalid tls options",
	}, {
		input: "CREATE USER UserName@localhost REQUIRE SSL AND X509",
		err:   "invalid tls options",
	}}
	for _, tcase := range invalidDDL {
		_, err := Parse(tcase.input)
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
			input: "select B.A from c",
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
			input:  "select A(ALL B, C) from b",
			output: "select A(B, C) from b",
		}, {
			input:                      "select A(ALL B, C) from b",
			output:                     "select A(B, C) from b",
			useSelectExpressionLiteral: true,
		}, {
			input: "select IF(B, C) from b",
		}, {
			input: "select * from b use index (A)",
		}, {
			input: "insert into A(A, B) values (1, 2)",
		}, {
			input:  "create view A as select current_timestamp()",
			output: "create view a as select current_timestamp(0)",
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

// TODO: come up with some automated way to generate test cases for all keywords
func TestKeywords(t *testing.T) {
	validSQL := []parseTest{
		{
			input:  "select current_timestamp",
			output: "select current_timestamp(0)",
		},
		{
			input:                      "select current_TIMESTAMP",
			output:                     "select current_TIMESTAMP(0)",
			useSelectExpressionLiteral: true,
		},
		{
			input:  "update t set a = current_timestamp()",
			output: "update t set a = current_timestamp(0)",
		},
		{
			input: "update t set a = current_timestamp(5)",
		},
		{
			input:  "select a, current_date from t",
			output: "select a, current_date() from t",
		}, {
			input:                      "select a, current_DATE from t",
			output:                     "select a, current_DATE() from t",
			useSelectExpressionLiteral: true,
		}, {
			input:  "select a, current_user from t",
			output: "select a, current_user() from t",
		}, {
			input:                      "select a, current_USER from t",
			output:                     "select a, current_USER() from t",
			useSelectExpressionLiteral: true,
		}, {
			input:                      "select a, Current_USER(     ) from t",
			output:                     "select a, Current_USER() from t",
			useSelectExpressionLiteral: true,
		}, {
			input:  "insert into t(a, b) values (current_date, current_date())",
			output: "insert into t(a, b) values (current_date(), current_date())",
		}, {
			input:  "insert into t(a, b) values ('a', 'b')",
			output: "insert into t(a, b) values ('a', 'b')",
		}, {
			input:  "insert into t() values ()",
			output: "insert into t() values ()",
		}, {
			input:  "insert into t() values (), (), ()",
			output: "insert into t() values (), (), ()",
		}, {
			input: "select * from t where a > utc_timestmp()",
		}, {
			input: "select * from t where a > utc_timestamp(4)",
		}, {
			input:  "update t set b = utc_timestamp + 5",
			output: "update t set b = utc_timestamp(0) + 5",
		}, {
			input:  "select utc_time, utc_date, utc_time(6)",
			output: "select utc_time(0), utc_date(), utc_time(6)",
		}, {
			input:                      "select utc_TIME, UTC_date, utc_time(6)",
			output:                     "select utc_TIME(0), UTC_date(), utc_time(6)",
			useSelectExpressionLiteral: true,
		}, {
			input:  "select 1 from dual where localtime > utc_time",
			output: "select 1 where localtime(0) > utc_time(0)",
		}, {
			input:  "select 1 from dual where localtime(2) > utc_time(1)",
			output: "select 1 where localtime(2) > utc_time(1)",
		}, {
			input:  "update t set a = localtimestamp(), b = utc_timestamp",
			output: "update t set a = localtimestamp(0), b = utc_timestamp(0)",
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
			input:  "insert into x (status) values (42)",
			output: "insert into x(`status`) values (42)",
		}, {
			input:  "update x set status = 32 where status = 42",
			output: "update x set status = 32 where `status` = 42",
		}, {
			input:  "delete from x where status = 32",
			output: "delete from x where `status` = 32",
		}, {
			input:  "select status from t",
			output: "select `status` from t",
		}, {
			input:  "select comment from t",
			output: "select `comment` from t",
		}, {
			input:  "select table_commment AS Comment FROM information_schema.TABLES",
			output: "select table_commment as `Comment` from information_schema.`TABLES`",
		}, {
			input:  "select 1 as comment",
			output: "select 1 as `comment`",
		}, {
			input:  "select variables from t",
			output: "select `variables` from t",
		}, {
			input:  "select 1 as found",
			output: "select 1 as `found`",
		}, {
			input:  "select found from t",
			output: "select `found` from t",
		}, {
			input:  "insert into t (found) values (42)",
			output: "insert into t(`found`) values (42)",
		}, {
			input:  "update x set found = 32 where found = 42",
			output: "update x set found = 32 where `found` = 42",
		}, {
			input:  "delete from x where found = 32",
			output: "delete from x where `found` = 32",
		}, {
			input:  "select 1 as event",
			output: "select 1 as `event`",
		}, {
			input:  "select event from t",
			output: "select `event` from t",
		}, {
			input:  "insert into t (event) values (42)",
			output: "insert into t(`event`) values (42)",
		}, {
			input:  "update x set event = 32 where event = 42",
			output: "update x set event = 32 where `event` = 42",
		}, {
			input:  "delete from x where event = 32",
			output: "delete from x where `event` = 32",
		}}

	for _, tcase := range validSQL {
		runParseTestCase(t, tcase)
	}
}

// runParseTestCase runs the specific test case, |tcase|, using the default parser options.
func runParseTestCase(t *testing.T, tcase parseTest) bool {
	return runParseTestCaseWithParserOptions(t, tcase, ParserOptions{})
}

// runParseTestCaseWithParserOptions runs the specific test case, |tcase|, using the specified parser
// options, |options|, to control any parser behaviors.
func runParseTestCaseWithParserOptions(t *testing.T, tcase parseTest, options ParserOptions) bool {
	return t.Run(tcase.input, func(t *testing.T) {
		if tcase.output == "" {
			tcase.output = tcase.input
		}
		tree, err := ParseWithOptions(tcase.input, options)
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

// TestFunctionCalls validates that every MySQL built-in function parses correctly. List of functions found here:
// https://dev.mysql.com/doc/refman/8.0/en/built-in-function-reference.html
// Some functions are required to have a certain number or type of arguments because they are defined as reserved words.
// Others are not, which means they parse correctly but lead to semantically meaningless results. That's fine for the
// purpose of this test: we just want to make sure that all built-in functions parse correctly and aren't broken by
// grammar changes.
func TestFunctionCalls(t *testing.T) {
	queries := []string{
		"select ABS() from dual",
		"select ACOS() from dual",
		"select ADDDATE() from dual",
		"select ADDTIME() from dual",
		"select AES_DECRYPT() from dual",
		"select AES_ENCRYPT() from dual",
		"select ANY_VALUE() from dual",
		"select ASCII() from dual",
		"select ASIN() from dual",
		"select ATAN() from dual",
		"select ATAN2() from dual",
		"select AVG(col) from dual",
		"select BENCHMARK() from dual",
		"select BIN() from dual",
		"select BIN_TO_UUID() from dual",
		"select BIT_AND(col) from dual",
		"select BIT_COUNT() from dual",
		"select BIT_LENGTH() from dual",
		"select BIT_OR(col) from dual",
		"select BIT_XOR(col) from dual",
		"select CAN_ACCESS_COLUMN() from dual",
		"select CAN_ACCESS_DATABASE() from dual",
		"select CAN_ACCESS_TABLE() from dual",
		"select CAN_ACCESS_USER() from dual",
		"select CAN_ACCESS_VIEW() from dual",
		"select CEIL() from dual",
		"select CEILING() from dual",
		"select CHAR(77, 121, 83, 81, '76') from dual",
		"select CHAR_LENGTH() from dual",
		"select CHARACTER_LENGTH() from dual",
		"select CHARSET() from dual",
		"select COALESCE() from dual",
		"select COERCIBILITY() from dual",
		"select COLLATION() from dual",
		"select COMPRESS() from dual",
		"select CONCAT() from dual",
		"select CONCAT_WS() from dual",
		"select CONNECTION_ID() from dual",
		"select CONV() from dual",
		"select CONVERT('abc', binary) from dual",
		"select CONVERT(foo, DOUBLE)",
		"select CONVERT(foo, FLOAT)",
		"select CONVERT_TZ() from dual",
		"select COS() from dual",
		"select COT() from dual",
		"select COUNT(col) from dual",
		"select COUNT(distinct col) from dual",
		"select CRC32() from dual",
		"select CUME_DIST() over mywindow from dual",
		"select CURDATE() from dual",
		"select CURRENT_DATE() from dual",
		"select CURRENT_ROLE() from dual",
		"select CURRENT_TIME(0) from dual",
		"select CURRENT_TIMESTAMP(0) from dual",
		"select CURRENT_USER() from dual",
		"select CURTIME() from dual",
		"select DATABASE() from dual",
		"select DATE() from dual",
		"select DATE_ADD() from dual",
		"select DATE_FORMAT() from dual",
		"select DATE_SUB() from dual",
		"select DATEDIFF() from dual",
		"select DAY() from dual",
		"select DAYNAME() from dual",
		"select DAYOFMONTH() from dual",
		"select DAYOFWEEK() from dual",
		"select DAYOFYEAR() from dual",
		"update mytable set a = default(b)",
		"select DEGREES() from dual",
		"select DENSE_RANK() over mywindow from dual",
		"select ELT() from dual",
		"select EXP() from dual",
		"select EXPORT_SET() from dual",
		"select EXTRACT(DAY from '2023-03-03') from dual",
		"select ExtractValue() from dual",
		"select FIELD() from dual",
		"select FIND_IN_SET() from dual",
		"select FIRST_VALUE(col) over mywindow from dual",
		"select FLOOR() from dual",
		"select FORMAT(col) from dual",
		"select FORMAT_BYTES() from dual",
		"select FORMAT_PICO_TIME() from dual",
		"select FOUND_ROWS() from dual",
		"select FROM_BASE64() from dual",
		"select FROM_DAYS() from dual",
		"select FROM_UNIXTIME() from dual",
		"select GeomCollection() from dual",
		"select GeometryCollection() from dual",
		"select GET_DD_COLUMN_PRIVILEGES() from dual",
		"select GET_DD_CREATE_OPTIONS() from dual",
		"select GET_DD_INDEX_SUB_PART_LENGTH() from dual",
		"select GET_FORMAT() from dual",
		"select GET_LOCK() from dual",
		"select GREATEST() from dual",
		"select group_concat(col) from dual",
		"select GROUPING(col) from dual",
		"select GTID_SUBSET() from dual",
		"select GTID_SUBTRACT() from dual",
		"select HEX() from dual",
		"select HOUR() from dual",
		"select ICU_VERSION() from dual",
		"select IF(col) from dual",
		"select IFNULL() from dual",
		"select INET_ATON() from dual",
		"select INET_NTOA() from dual",
		"select INET6_ATON() from dual",
		"select INET6_NTOA() from dual",
		"select INSERT(col) from dual",
		"select INSTR() from dual",
		"select INTERNAL_AUTO_INCREMENT() from dual",
		"select INTERNAL_AVG_ROW_LENGTH() from dual",
		"select INTERNAL_CHECK_TIME() from dual",
		"select INTERNAL_CHECKSUM() from dual",
		"select INTERNAL_DATA_FREE() from dual",
		"select INTERNAL_DATA_LENGTH() from dual",
		"select INTERNAL_DD_CHAR_LENGTH() from dual",
		"select INTERNAL_GET_COMMENT_OR_ERROR() from dual",
		"select INTERNAL_GET_ENABLED_ROLE_JSON() from dual",
		"select INTERNAL_GET_HOSTNAME() from dual",
		"select INTERNAL_GET_USERNAME() from dual",
		"select INTERNAL_GET_VIEW_WARNING_OR_ERROR() from dual",
		"select INTERNAL_INDEX_COLUMN_CARDINALITY() from dual",
		"select INTERNAL_INDEX_LENGTH() from dual",
		"select INTERNAL_IS_ENABLED_ROLE() from dual",
		"select INTERNAL_IS_MANDATORY_ROLE() from dual",
		"select INTERNAL_KEYS_DISABLED() from dual",
		"select INTERNAL_MAX_DATA_LENGTH() from dual",
		"select INTERNAL_TABLE_ROWS() from dual",
		"select INTERNAL_UPDATE_TIME() from dual",
		"select IS_FREE_LOCK() from dual",
		"select IS_IPV4() from dual",
		"select IS_IPV4_COMPAT() from dual",
		"select IS_IPV4_MAPPED() from dual",
		"select IS_IPV6() from dual",
		"select IS_USED_LOCK() from dual",
		"select IS_UUID() from dual",
		"select ISNULL() from dual",
		"select JSON_ARRAY() from dual",
		"select JSON_ARRAY_APPEND() from dual",
		"select JSON_ARRAY_INSERT() from dual",
		"select JSON_ARRAYAGG(col) from dual",
		"select JSON_CONTAINS() from dual",
		"select JSON_CONTAINS_PATH() from dual",
		"select JSON_DEPTH() from dual",
		"select JSON_EXTRACT() from dual",
		"select JSON_INSERT() from dual",
		"select JSON_KEYS() from dual",
		"select JSON_LENGTH() from dual",
		"select JSON_MERGE() from dual",
		"select JSON_MERGE_PATCH() from dual",
		"select JSON_MERGE_PRESERVE() from dual",
		"select JSON_OBJECT() from dual",
		"select JSON_OBJECTAGG(col) from dual",
		"select JSON_OVERLAPS() from dual",
		"select JSON_PRETTY() from dual",
		"select JSON_QUOTE() from dual",
		"select JSON_REMOVE() from dual",
		"select JSON_REPLACE() from dual",
		"select JSON_SCHEMA_VALID() from dual",
		"select JSON_SCHEMA_VALIDATION_REPORT() from dual",
		"select JSON_SEARCH() from dual",
		"select JSON_SET() from dual",
		"select JSON_STORAGE_FREE() from dual",
		"select JSON_STORAGE_SIZE() from dual",
		"select JSON_TYPE() from dual",
		"select JSON_UNQUOTE() from dual",
		"select JSON_VALID() from dual",
		"select JSON_VALUE() from dual",
		"select LAG(col) over mywindow from dual",
		"select LAST_DAY() from dual",
		"select LAST_INSERT_ID() from dual",
		"select LAST_VALUE(col) over mywindow from dual",
		"select LCASE() from dual",
		"select LEAD(col) over mywindow from dual",
		"select LEAST() from dual",
		"select LEFT('abc', 1) from dual",
		"select LENGTH() from dual",
		"select LineString() from dual",
		"select LN() from dual",
		"select LOAD_FILE() from dual",
		"select LOCALTIME(0) from dual",
		"select LOCALTIMESTAMP(0) from dual",
		"select LOCATE() from dual",
		"select LOG() from dual",
		"select LOG10() from dual",
		"select LOG2() from dual",
		"select LOWER() from dual",
		"select LPAD() from dual",
		"select LTRIM() from dual",
		"select MAKE_SET() from dual",
		"select MAKEDATE() from dual",
		"select MAKETIME() from dual",
		"select MASTER_POS_WAIT() from dual",
		"select match(a1, a2) against ('foo' in natural language mode with query expansion) from t",
		"select MAX(col1) from dual",
		"select MBRContains() from dual",
		"select MBRCoveredBy() from dual",
		"select MBRCovers() from dual",
		"select MBRDisjoint() from dual",
		"select MBREquals() from dual",
		"select MBRIntersects() from dual",
		"select MBROverlaps() from dual",
		"select MBRTouches() from dual",
		"select MBRWithin() from dual",
		"select MD5() from dual",
		"select MICROSECOND() from dual",
		"select MID() from dual",
		"select MIN(col) from dual",
		"select MINUTE() from dual",
		"select MOD(col) from dual",
		"select MONTH() from dual",
		"select MONTHNAME() from dual",
		"select MultiLineString() from dual",
		"select MultiPoint() from dual",
		"select MultiPolygon() from dual",
		"select NAME_CONST() from dual",
		"select NOW() from dual",
		"select NTH_VALUE(col) over mywindow from dual",
		"select NTILE() over mywindow from dual",
		"select NULLIF() from dual",
		"select OCT() from dual",
		"select OCTET_LENGTH() from dual",
		"select ORD() from dual",
		"select PERCENT_RANK() over mywindow from dual",
		"select PERIOD_ADD() from dual",
		"select PERIOD_DIFF() from dual",
		"select PI() from dual",
		"select Point() from dual",
		"select Polygon() from dual",
		"select POW() from dual",
		"select POWER() from dual",
		"select PS_CURRENT_THREAD_ID() from dual",
		"select PS_THREAD_ID() from dual",
		"select QUARTER() from dual",
		"select QUOTE() from dual",
		"select RADIANS() from dual",
		"select RAND() from dual",
		"select RANDOM_BYTES() from dual",
		"select RANK() over mywindow from dual",
		"select REGEXP_INSTR() from dual",
		"select REGEXP_LIKE() from dual",
		"select REGEXP_REPLACE() from dual",
		"select REGEXP_SUBSTR() from dual",
		"select RELEASE_ALL_LOCKS() from dual",
		"select RELEASE_LOCK() from dual",
		"select REPEAT('a', 2) from dual",
		"select REPLACE('a', 'b') from dual",
		"select REVERSE() from dual",
		"select RIGHT('b', 1) from dual",
		"select ROLES_GRAPHML() from dual",
		"select ROUND() from dual",
		"select ROW_COUNT() from dual",
		"select ROW_NUMBER() over mywindow from dual",
		"select RPAD() from dual",
		"select RTRIM() from dual",
		"select SCHEMA() from dual",
		"select SEC_TO_TIME() from dual",
		"select SECOND() from dual",
		"select SESSION_USER() from dual",
		"select SHA1() from dual",
		"select SHA2() from dual",
		"select SIGN() from dual",
		"select SIN() from dual",
		"select SLEEP() from dual",
		"select SOUNDEX() from dual",
		"select SOURCE_POS_WAIT() from dual",
		"select SPACE() from dual",
		"select SQRT() from dual",
		"select ST_Area() from dual",
		"select ST_AsBinary() from dual",
		"select ST_AsGeoJSON() from dual",
		"select ST_AsText() from dual",
		"select ST_Buffer() from dual",
		"select ST_Buffer_Strategy() from dual",
		"select ST_Centroid() from dual",
		"select ST_Collect() from dual",
		"select ST_Contains() from dual",
		"select ST_ConvexHull() from dual",
		"select ST_Crosses() from dual",
		"select ST_Difference() from dual",
		"select ST_Dimension() from dual",
		"select ST_Disjoint() from dual",
		"select ST_Distance() from dual",
		"select ST_Distance_Sphere() from dual",
		"select ST_EndPoint() from dual",
		"select ST_Envelope() from dual",
		"select ST_Equals() from dual",
		"select ST_ExteriorRing() from dual",
		"select ST_FrechetDistance() from dual",
		"select ST_GeoHash() from dual",
		"select ST_GeomCollFromText() from dual",
		"select ST_GeomCollFromWKB() from dual",
		"select ST_GeometryN() from dual",
		"select ST_GeometryType() from dual",
		"select ST_GeomFromGeoJSON() from dual",
		"select ST_GeomFromText() from dual",
		"select ST_GeomFromWKB() from dual",
		"select ST_HausdorffDistance() from dual",
		"select ST_InteriorRingN() from dual",
		"select ST_Intersection() from dual",
		"select ST_Intersects() from dual",
		"select ST_IsClosed() from dual",
		"select ST_IsEmpty() from dual",
		"select ST_IsSimple() from dual",
		"select ST_IsValid() from dual",
		"select ST_LatFromGeoHash() from dual",
		"select ST_Latitude() from dual",
		"select ST_Length() from dual",
		"select ST_LineFromText() from dual",
		"select ST_LineFromWKB() from dual",
		"select ST_LineInterpolatePoint() from dual",
		"select ST_LineInterpolatePoints() from dual",
		"select ST_LongFromGeoHash() from dual",
		"select ST_Longitude() from dual",
		"select ST_MakeEnvelope() from dual",
		"select ST_MLineFromText() from dual",
		"select ST_MultiLineStringFromText() from dual",
		"select ST_MLineFromWKB() from dual",
		"select ST_MultiLineStringFromWKB() from dual",
		"select ST_MPointFromText() from dual",
		"select ST_MultiPointFromText() from dual",
		"select ST_MPointFromWKB() from dual",
		"select ST_MultiPointFromWKB() from dual",
		"select ST_MPolyFromText() from dual",
		"select ST_MultiPolygonFromText() from dual",
		"select ST_MPolyFromWKB() from dual",
		"select ST_MultiPolygonFromWKB() from dual",
		"select ST_NumGeometries() from dual",
		"select ST_NumInteriorRing() from dual",
		"select ST_NumInteriorRings() from dual",
		"select ST_NumPoints() from dual",
		"select ST_Overlaps() from dual",
		"select ST_PointAtDistance() from dual",
		"select ST_PointFromGeoHash() from dual",
		"select ST_PointFromText() from dual",
		"select ST_PointFromWKB() from dual",
		"select ST_PointN() from dual",
		"select ST_PolyFromText() from dual",
		"select ST_PolyFromWKB() from dual",
		"select ST_PolygonFromWKB() from dual",
		"select ST_Simplify() from dual",
		"select ST_SRID() from dual",
		"select ST_StartPoint() from dual",
		"select ST_SwapXY() from dual",
		"select ST_SymDifference() from dual",
		"select ST_Touches() from dual",
		"select ST_Transform() from dual",
		"select ST_Union() from dual",
		"select ST_Validate() from dual",
		"select ST_Within() from dual",
		"select ST_X() from dual",
		"select ST_Y() from dual",
		"select STATEMENT_DIGEST() from dual",
		"select STATEMENT_DIGEST_TEXT() from dual",
		"select STD(col) from dual",
		"select STDDEV(col) from dual",
		"select STDDEV_POP(col) from dual",
		"select STDDEV_SAMP(col) from dual",
		"select STR_TO_DATE() from dual",
		"select STRCMP() from dual",
		"select SUBDATE() from dual",
		"select SUBSTR('a', 'b') from dual",
		"select SUBSTRING('b', 'c') from dual",
		"select SUBSTRING_INDEX() from dual",
		"select SUBTIME() from dual",
		"select SUM(col) from dual",
		"select SYSDATE() from dual",
		"select SYSTEM_USER() from dual",
		"select TAN() from dual",
		"select TIME() from dual",
		"select TIME_FORMAT() from dual",
		"select TIME_TO_SEC() from dual",
		"select TIMEDIFF() from dual",
		"select TIMESTAMP() from dual",
		"select timestampadd(MONTH, 1, col) from dual",
		"select timestampdiff(MONTH, 1, col) from dual",
		"select TO_BASE64() from dual",
		"select TO_DAYS() from dual",
		"select TO_SECONDS() from dual",
		"select trim(both ' ' from 'a') from dual",
		"select TRUNCATE() from dual",
		"select UCASE() from dual",
		"select UNCOMPRESS() from dual",
		"select UNCOMPRESSED_LENGTH() from dual",
		"select UNHEX() from dual",
		"select UNIX_TIMESTAMP() from dual",
		"select UpdateXML() from dual",
		"select UPPER() from dual",
		"select USER() from dual",
		"select UTC_DATE() from dual",
		"select UTC_TIME(0) from dual",
		"select UTC_TIMESTAMP(0) from dual",
		"select UUID() from dual",
		"select UUID_SHORT() from dual",
		"select UUID_TO_BIN() from dual",
		"select VALIDATE_PASSWORD_STRENGTH() from dual",
		"select VAR_POP(col) from dual",
		"select VAR_SAMP(col) from dual",
		"select VARIANCE(col) from dual",
		"select VERSION() from dual",
		"select WAIT_FOR_EXECUTED_GTID_SET() from dual",
		"select WAIT_UNTIL_SQL_THREAD_AFTER_GTIDS() from dual",
		"select WEEK() from dual",
		"select WEEKDAY() from dual",
		"select WEEKOFYEAR() from dual",
		"select WEIGHT_STRING() from dual",
		"select YEAR() from dual",
		"select YEARWEEK() from dual",
		"select CHAR(77, 121, 83, 81, '76' USING utf8mb4) from dual",
	}

	// Functions where the input doesn't match the output. Prefer query tests above when possible.
	testCases := []parseTest{
		{
			input:  "select CAST(1 as datetime) from dual",
			output: "select CAST(1 as datetime)",
		},
		{
			input:  "select LOCALTIMESTAMP from dual",
			output: "select LOCALTIMESTAMP(0)",
		},
		{
			input:  "SELECT CAST(foo AS DOUBLE)",
			output: "select CAST(foo as DOUBLE)",
		},
		{
			input:  "SELECT CAST(foo AS DOUBLE PRECISION)",
			output: "select CAST(foo as DOUBLE)",
		},
		{
			input:  "SELECT CAST(foo AS REAL)",
			output: "select CAST(foo as REAL)",
		},
		{
			input:  "SELECT CAST(foo AS FLOAT)",
			output: "select CAST(foo as FLOAT)",
		},
		{
			input:  "SELECT CAST(foo AS CHARACTER)",
			output: "select CAST(foo as CHAR)",
		},
		{
			input:  "SELECT CAST(foo AS CHARACTER(100))",
			output: "select CAST(foo as CHAR(100))",
		},
		{
			input:  "SELECT POSITION('abc' in 'xyz')",
			output: "select LOCATE('abc', 'xyz')",
		},
		{
			input:  "SELECT POSITION(1 + 1 in foo)",
			output: "select LOCATE(1 + 1, foo)",
		},
	}

	// Unimplemented or broken functionality
	skippedTestCases := []parseTest{
		{
			// INTERVAL function produces a grammar conflict
			input: "select INTERVAL(col1, col2) from dual",
		},
		{
			// not implemented
			input: `select SELECT 17 MEMBER OF('[23, "abc", 17, "ab", 10]'); from dual`,
		},
		{
			input:  "alter table a check partition p",
		},
		{
			input:  "alter table a check partition all",
		},
	}

	for _, query := range queries {
		test := parseTest{
			input:  query,
			output: strings.Replace(query, " from dual", "", -1),
		}
		runParseTestCase(t, test)
	}

	for _, test := range testCases {
		runParseTestCase(t, test)
	}

	for _, test := range skippedTestCases {
		t.Run(test.input, func(t *testing.T) {
			t.Skip()
		})
	}
}

func TestConvert(t *testing.T) {
	validSQL := []parseTest{
		{
			input: "select cast('abc' as date) from t",
		},
		{
			input: "select cast('abc' as year) from t",
		},
		{
			input:                      "select cast('abc' as date) from t",
			useSelectExpressionLiteral: true,
		},
		{
			input: "select convert('abc', binary(4)) from t",
		},
		{
			input: "select convert('abc', binary) from t",
		},
		{
			input: "select convert('abc', char character set binary) from t",
		},
		{
			input: "select convert('abc', char(4) ascii) from t",
		},
		{
			input: "select convert('abc', char unicode) from t",
		},
		{
			input: "select convert('abc', char(4)) from t",
		},
		{
			input: "select convert('abc', char) from t",
		},
		{
			input: "select convert('abc', nchar(4)) from t",
		},
		{
			input: "select convert('abc', nchar) from t",
		},
		{
			input: "select convert('abc', signed) from t",
		},
		{
			input:  "select convert('abc', signed integer) from t",
			output: "select convert('abc', signed) from t",
		},
		{
			input:  "select convert('abc', signed) from t",
			output: "select convert('abc', signed) from t",
		},
		{
			input: "select convert('abc', unsigned) from t",
		},
		{
			input:  "select convert('abc', unsigned integer) from t",
			output: "select convert('abc', unsigned) from t",
		},
		{
			input:  "select convert('abc', unsigned) from t",
			output: "select convert('abc', unsigned) from t",
		},
		{
			input: "select convert('abc', decimal(3, 4)) from t",
		},
		{
			input: "select convert('abc', decimal(4)) from t",
		},
		{
			input: "select convert('abc', decimal) from t",
		},
		{
			input: "select convert('abc', date) from t",
		},
		{
			input: "select convert('abc', time(4)) from t",
		},
		{
			input: "select convert('abc', time) from t",
		},
		{
			input: "select convert('abc', datetime(9)) from t",
		},
		{
			input: "select convert('abc', datetime) from t",
		},
		{
			input: "select convert('abc', json) from t",
		},
		{
			input: "select convert('abc' using ascii) from t",
		},
		{
			input: "select convert('abc', year) from t",
		},
	}

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

	// various substring forms get parsed correctly
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
		input:  "select substring(a from 1 for 6) from t",
		output: "select substr(a, 1, 6) from t",
	}, {
		input:                      "select substring(a from 1 for 6) from t",
		output:                     "select substr(a, 1, 6) from t",
		useSelectExpressionLiteral: true,
	}, {
		input:  "select substring(a from 1 for 6) from t",
		output: "select substr(a, 1, 6) from t",
	}, {
		input:                      "select substring(a from 1 for 6) from t",
		output:                     "select substr(a, 1, 6) from t",
		useSelectExpressionLiteral: true,
	}, {
		input:                      "select substring(a from 1  for   6) from t",
		output:                     "select substr(a, 1, 6) from t",
		useSelectExpressionLiteral: true,
	}, {
		input:  `select substr("foo" from 1 for 2) from t`,
		output: `select substr('foo', 1, 2) from t`,
	}, {
		input:  `select substring("foo", 1, 2) from t`,
		output: `select substring('foo', 1, 2) from t`,
	}, {
		input:  `select substr(substr("foo" from 1 for 2), 1, 2) from t`,
		output: `select substr(substr('foo', 1, 2), 1, 2) from t`,
	}, {
		input:                      `select substr(substr("foo" from 1 for 2), 1, 2) from t`,
		output:                     "select substr(substr('foo', 1, 2), 1, 2) from t",
		useSelectExpressionLiteral: true,
	}, {
		input:  `select substr(substring("foo", 1, 2), 3, 4) from t`,
		output: `select substr(substring('foo', 1, 2), 3, 4) from t`,
	}, {
		input:  `select substr(substr("foo", 1), 2) from t`,
		output: `select substr(substr('foo', 1), 2) from t`,
	}}

	for _, tcase := range validSQL {
		runParseTestCase(t, tcase)
	}
}

var sampleColumns = []string{
	"	col_bit bit",
	"	col_tinyint tinyint auto_increment",
	"	col_tinyint3 tinyint(3) unsigned",
	"	col_smallint smallint",
	"	col_smallint4 smallint(4) zerofill",
	"	col_mediumint mediumint",
	"	col_mediumint5 mediumint(5) unsigned not null",
	"	col_int int",
	"	col_int10 int(10) not null",
	"	col_integer integer comment 'this is an integer'",
	"	col_bigint bigint",
	"	col_bigint10 bigint(10) zerofill not null default 10",
	"	col_real real",
	"	col_real2 real(1,2) not null default 1.23",
	"	col_double double",
	"	col_double2 double(3,4) not null default 1.23",
	"	col_double3 double precision not null default 1.23",
	"	col_float float",
	"	col_float2 float(3,4) not null default 1.23",
	"	col_float3 float(3) not null default 1.23",
	"	col_decimal decimal",
	"	col_decimal2 decimal(2)",
	"	col_decimal3 decimal(2,3)",
	"	col_dec dec",
	"	col_dec2 dec(2)",
	"	col_dec3 dec(2,3)",
	"	col_fixed fixed",
	"	col_fixed2 fixed(2)",
	"	col_fixed3 fixed(2,3)",
	"	col_numeric numeric",
	"	col_numeric2 numeric(2)",
	"	col_numeric3 numeric(2,3)",
	"	col_date date",
	"	col_time time",
	"	col_timestamp timestamp",
	"	col_datetime datetime",
	"	col_year year",
	"	col_char char",
	"	col_char2 char(2)",
	"	col_char3 char(3) character set ascii",
	"	col_char4 char(4) character set ascii collate ascii_bin",
	"	col_character character",
	"	col_character2 character(2)",
	"	col_character3 character(3) character set ascii",
	"	col_character4 character(4) character set ascii collate ascii_bin",
	"	col_char_varying char varying(2)",
	"	col_char_varying2 char varying(10) character set utf8",
	"	col_nchar nchar",
	"	col_nchar2 nchar(2)",
	"	col_nchar_varchar nchar varchar(2)",
	"	col_nchar_varying nchar varying(2)",
	"	col_national_char national char",
	"	col_national_char2 national char(2)",
	"	col_national_character national character",
	"	col_national_character2 national character(2)",
	"	col_national_char_varying national char varying(2)",
	"	col_varchar varchar",
	"	col_varchar2 varchar(2)",
	"	col_varchar3 varchar(3) character set ascii",
	"	col_varchar4 varchar(4) character set ascii collate ascii_bin",
	"	col_varchar5 varchar(5) character set ascii binary",
	"	col_varcharMax varchar(MAX)",
	"	col_character_varying character varying",
	"	col_character_varying2 character varying(2)",
	"	col_character_varying3 character varying(3) character set ascii",
	"	col_character_varying4 character varying(4) character set ascii collate ascii_bin",
	"	col_nvarchar nvarchar",
	"	col_nvarchar2 nvarchar(2)",
	"	col_national_varchar national varchar",
	"	col_national_varchar2 national varchar(2)",
	"	col_national_character_varying national character varying",
	"	col_national_character_varying2 national character varying(2)",
	"	col_binary binary",
	"	col_varbinary varbinary(10)",
	"	col_tinyblob tinyblob",
	"	col_blob blob",
	"	col_mediumblob mediumblob",
	"	col_longblob longblob",
	"	col_tinytext tinytext",
	"	col_text text",
	"	col_mediumtext mediumtext",
	"	col_long long",
	"	col_long_varchar long varchar",
	"	col_longtext longtext",
	"	col_text text character set ascii collate ascii_bin",
	"	col_json json",
	"	col_enum enum('a', 'b', 'c', 'd')",
	"	col_enum2 enum('a', 'b', 'c', 'd') character set ascii",
	"	col_enum3 enum('a', 'b', 'c', 'd') collate ascii_bin",
	"	col_enum4 enum('a', 'b', 'c', 'd') character set ascii collate ascii_bin",
	"	col_set set('a', 'b', 'c', 'd')",
	"	col_set2 set('a', 'b', 'c', 'd') character set ascii",
	"	col_set3 set('a', 'b', 'c', 'd') collate ascii_bin",
	"	col_set4 set('a', 'b', 'c', 'd') character set ascii collate ascii_bin",
}

var sampleGeoColumns = []string{
	"	col_geometry1 geometry",
	"	col_geometry2 geometry not null",
	"	col_point1 point",
	"	col_point2 point not null",
	"	col_linestring1 linestring",
	"	col_linestring2 linestring not null",
	"	col_polygon1 polygon",
	"	col_polygon2 polygon not null",
	"	col_geometrycollection1 geometrycollection",
	"	col_geometrycollection2 geometrycollection not null",
	"	col_multipoint1 multipoint",
	"	col_multipoint2 multipoint not null",
	"	col_multilinestring1 multilinestring",
	"	col_multilinestring2 multilinestring not null",
	"	col_multipolygon1 multipolygon",
	"	col_multipolygon2 multipolygon not null",
}

func TestCreateTable(t *testing.T) {
	var createStatement strings.Builder
	createStatement.WriteString("create table t (\n")
	first := true
	for _, col := range sampleColumns {
		if !first {
			createStatement.WriteString(",\n")
		}
		first = false
		createStatement.WriteString(col)
	}
	for _, col := range sampleGeoColumns {
		createStatement.WriteString(",\n")
		createStatement.WriteString(col)
	}
	createStatement.WriteString("\n)")

	sql := "create table t garbage"
	tree, err := Parse(sql)
	if tree != nil || err == nil {
		t.Errorf("Parse unexpectedly accepted input %s", sql)
	}

	testCases := []parseTest{
		// test all the data types and options
		{
			input: createStatement.String(),
		},
		{
			// test defining indexes separately
			input: "create table t (\n" +
				"	id int auto_increment,\n" +
				"	username varchar,\n" +
				"	email varchar,\n" +
				"	full_name varchar,\n" +
				"	geom point not null,\n" +
				"	status_nonkeyword varchar,\n" +
				"	primary key (id),\n" +
				"	spatial key geom (geom),\n" +
				"	fulltext key fts (full_name),\n" +
				"	unique key by_username (username),\n" +
				"	unique by_username2 (username),\n" +
				"	unique index by_username3 (username),\n" +
				"	index by_status (status_nonkeyword),\n" +
				"	key by_full_name (full_name)\n" +
				")",
		},
		{
			// test that indexes support USING <id>
			input: "create table t (\n" +
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
		},
		{
			// test other index options
			input: "create table t (\n" +
				"	id int auto_increment,\n" +
				"	username varchar,\n" +
				"	email varchar,\n" +
				"	primary key (id) comment 'hi',\n" +
				"	unique key by_username (username) key_block_size 8,\n" +
				"	unique index by_username4 (username) comment 'hi' using BTREE,\n" +
				"	unique index by_username4 (username) using BTREE key_block_size 4 comment 'hi'\n" +
				")",
		},
		{
			// multi-column indexes
			input: "create table t (\n" +
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
		},
		{
			// foreign keys
			input: "create table t (\n" +
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
		},
		{
			// check constraint
			input: "create table t (\n" +
				"	id int auto_increment,\n" +
				"	username varchar,\n" +
				"	a int,\n" +
				"	b int,\n" +
				"	check (b in (0, 1)),\n" +
				"	constraint a_positive check (a > 0),\n" +
				"	check (a > b)\n" +
				")",
		},
		{
			// table options
			input: "create table t (\n" +
				"\tid int auto_increment\n" +
				") engine InnoDB" +
				" auto_increment 123" +
				" avg_row_length 1" +
				" character set utf8mb4" +
				" character set latin1" +
				" checksum 0" +
				" default collate binary" +
				" collate ascii_bin" +
				" comment 'this is a comment'" +
				" compression 'zlib'" +
				" connection 'connect_string'" +
				" data directory 'absolute path to directory'" +
				" delay_key_write 1" +
				" encryption 'n'" +
				" index directory 'absolute path to directory'" +
				" insert_method no" +
				" key_block_size 1024" +
				" max_rows 100" +
				" min_rows 10" +
				" pack_keys 0" +
				" password 'sekret'" +
				" row_format default" +
				" stats_auto_recalc default" +
				" stats_persistent 0" +
				" stats_sample_pages 1" +
				" tablespace tablespace_name storage disk" +
				" tablespace tablespace_name",
			output: "create table t (\n" +
				"\tid int auto_increment\n" +
				") engine InnoDB" +
				" auto_increment 123" +
				" avg_row_length 1" +
				" character set utf8mb4" +
				" character set latin1" +
				" checksum 0" +
				" collate binary" +
				" collate ascii_bin" +
				" comment this is a comment" +
				" compression zlib" +
				" connection connect_string" +
				" data directory absolute path to directory" +
				" delay_key_write 1" +
				" encryption n" +
				" index directory absolute path to directory" +
				" insert_method no" +
				" key_block_size 1024" +
				" max_rows 100" +
				" min_rows 10" +
				" pack_keys 0" +
				" password sekret" +
				" row_format default" +
				" stats_auto_recalc default" +
				" stats_persistent 0" +
				" stats_sample_pages 1" +
				" tablespace tablespace_name storage disk" +
				" tablespace tablespace_name",
		},
		{
			// table options out of order
			input: "create table t (\n" +
				"	id int auto_increment\n" +
				") engine InnoDB" +
				" avg_row_length 1" +
				" character set utf8mb4" +
				" auto_increment 123" +
				" character set latin1",
		},
		{
			// passing hex nums to table options
			input: "create table t (\n" +
				"	id int auto_increment\n" +
				") engine InnoDB,\n" +
				"  auto_increment 0x1,\n" +
				"  avg_row_length 0x2,\n" +
				"  checksum 0x3,\n" +
				"  delay_key_write 0x4,\n" +
				"  key_block_size 0x5,\n" +
				"  max_rows 0x6,\n" +
				"  min_rows 0x7,\n" +
				"  pack_keys 0x8,\n" +
				"  stats_auto_recalc 0x1,\n" +
				"  stats_persistent 0x9,\n" +
				"  stats_sample_pages 0x10\n",
			output: "create table t (\n" +
				"\tid int auto_increment\n" +
				") engine InnoDB" +
				" auto_increment 0x1" +
				" avg_row_length 0x2" +
				" checksum 0x3" +
				" delay_key_write 0x4" +
				" key_block_size 0x5" +
				" max_rows 0x6" +
				" min_rows 0x7" +
				" pack_keys 0x8" +
				" stats_auto_recalc 0x1" +
				" stats_persistent 0x9" +
				" stats_sample_pages 0x10",
		},
		{
			// passing floats to table options
			input: "create table t (\n" +
				"	id int auto_increment\n" +
				") engine InnoDB,\n" +
				"  auto_increment 4.1,\n" +
				"  avg_row_length 4.2,\n" +
				"  checksum 4.3,\n" +
				"  delay_key_write 4.4,\n" +
				"  key_block_size 4.5,\n" +
				"  max_rows 4.6,\n" +
				"  min_rows 4.7,\n" +
				"  pack_keys 4.8,\n" +
				"  stats_persistent 4.9,\n" +
				"  stats_sample_pages 4.01\n",
			output: "create table t (\n" +
				"\tid int auto_increment\n" +
				") engine InnoDB" +
				" auto_increment 4.1" +
				" avg_row_length 4.2" +
				" checksum 4.3" +
				" delay_key_write 4.4" +
				" key_block_size 4.5" +
				" max_rows 4.6" +
				" min_rows 4.7" +
				" pack_keys 4.8" +
				" stats_persistent 4.9" +
				" stats_sample_pages 4.01",
		},
		{
			// boolean columns
			input: "create table t (\n" +
				"	bi bigint not null primary key,\n" +
				"	b1 bool not null,\n" +
				"	b2 boolean\n" +
				")",
		},
		{
			// generated by serial
			input: "create table t (\n" +
				"	id bigint not null auto_increment unique,\n" +
				"	a bigint not null\n" +
				")",
		},
		{
			// Tet varchar (MAX) syntax
			input:  "create table t (username varchar(MAX))",
			output: "create table t (\n\tusername varchar(MAX)\n)",
		},
		{
			// Test the signed keyword – as the default for numeric types, it is a no-op
			input:  "create table t (pk int signed primary key)",
			output: "create table t (\n\tpk int primary key\n)",
		},
		{
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
		},
		{
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
				"	s4 timestamp default current_timestamp(0),\n" +
				"	s5 bit(1) default B'0'\n" +
				")",
		},
		{
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
				"	time1 timestamp on update current_timestamp(0),\n" +
				"	time2 timestamp default current_timestamp(0) on update current_timestamp(0)\n" +
				")",
		},
		{
			// test alternate key syntax
			input: "create table t (\n" +
				"	id int,\n" +
				"	full_name varchar,\n" +
				"	constraint unique key (full_name),\n" +
				"	constraint unique index named (full_name),\n" +
				"	constraint namedx unique (full_name),\n" +
				"	constraint pk primary key (id)\n" +
				")",
			output: "create table t (\n" +
				"	id int,\n" +
				"	full_name varchar,\n" +
				"	unique key (full_name),\n" +
				"	unique index named (full_name),\n" +
				"	unique  namedx (full_name),\n" +
				"	primary key (id)\n" +
				")",
		},
		{
			input: "create table t (\n" +
				"	id int,\n" +
				"	constraint primary key (id)\n" +
				")",
			output: "create table t (\n" +
				"	id int,\n" +
				"	primary key (id)\n" +
				")",
		},
		{
			input: "create table t (\n" +
				"	id int,\n" +
				"	constraint abc primary key def (id)\n" +
				")",
			output: "create table t (\n" +
				"	id int,\n" +
				"	primary key (id)\n" +
				")",
		},
		{
			// test now
			input: "create table t (\n" +
				"	time1 timestamp default now(),\n" +
				"	time2 timestamp default now() on update now(),\n" +
				"	time3 timestamp(3) default now(3) on update now(3)\n" +
				")",
			output: "create table t (\n" +
				"	time1 timestamp default now(),\n" +
				"	time2 timestamp default now() on update now(),\n" +
				"	time3 timestamp(3) default now(3) on update now(3)\n" +
				")",
		},
		{
			// test current_timestamp with and without ()
			input: "create table t (\n" +
				"	time1 timestamp default current_timestamp,\n" +
				"	time2 timestamp default current_timestamp(),\n" +
				"	time3 timestamp default current_timestamp on update current_timestamp,\n" +
				"	time4 timestamp default current_timestamp() on update current_timestamp(),\n" +
				"	time5 timestamp(3) default current_timestamp(3) on update current_timestamp(3)\n" +
				")",
			output: "create table t (\n" +
				"	time1 timestamp default current_timestamp(0),\n" +
				"	time2 timestamp default current_timestamp(0),\n" +
				"	time3 timestamp default current_timestamp(0) on update current_timestamp(0),\n" +
				"	time4 timestamp default current_timestamp(0) on update current_timestamp(0),\n" +
				"	time5 timestamp(3) default current_timestamp(3) on update current_timestamp(3)\n" +
				")",
		},
		{
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
		},
		{
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
		},
		{
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
		},
		{
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
		},
		{
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
		},
		{
			// test localtime with and without ()
			input: "create table t (\n" +
				"	time1 timestamp default localtime,\n" +
				"	time2 timestamp default localtime(),\n" +
				"	time3 timestamp default localtime on update localtime,\n" +
				"	time4 timestamp default localtime() on update localtime(),\n" +
				"	time5 timestamp(6) default localtime(6) on update localtime(6)\n" +
				")",
			output: "create table t (\n" +
				"	time1 timestamp default localtime(0),\n" +
				"	time2 timestamp default localtime(0),\n" +
				"	time3 timestamp default localtime(0) on update localtime(0),\n" +
				"	time4 timestamp default localtime(0) on update localtime(0),\n" +
				"	time5 timestamp(6) default localtime(6) on update localtime(6)\n" +
				")",
		},
		{
			// test localtimestamp with and without ()
			input: "create table t (\n" +
				"	time1 timestamp default localtimestamp,\n" +
				"	time2 timestamp default localtimestamp(),\n" +
				"	time3 timestamp default localtimestamp on update localtimestamp,\n" +
				"	time4 timestamp default localtimestamp() on update localtimestamp(),\n" +
				"	time5 timestamp(1) default localtimestamp(1) on update localtimestamp(1)\n" +
				")",
			output: "create table t (\n" +
				"	time1 timestamp default localtimestamp(0),\n" +
				"	time2 timestamp default localtimestamp(0),\n" +
				"	time3 timestamp default localtimestamp(0) on update localtimestamp(0),\n" +
				"	time4 timestamp default localtimestamp(0) on update localtimestamp(0),\n" +
				"	time5 timestamp(1) default localtimestamp(1) on update localtimestamp(1)\n" +
				")",
		},
		{
			input: "create table t (\n" +
				"	id serial not null,\n" +
				"	a bigint not null\n" +
				")",
			output: "create table t (\n" +
				"	id bigint not null auto_increment unique,\n" +
				"	a bigint not null\n" +
				")",
		},

		// partition options
		{
			input: "create table t (\n" +
				"\ti int\n" +
				") partition by range (store_id) (\n" +
				"partition p0 values less than (6),\n" +
				"partition p1 values less than (11),\n" +
				"partition p2 values less than (16),\n" +
				"partition p3 values less than (21)\n" +
				")",
		},
		{
			input: "create table t (\n" +
				"\ti int\n" +
				") partition by hash ('values')",
		},
		{
			input: "create table t (\n" +
				"\ti int\n" +
				") partition by hash (col)",
		},
		{
			input: "create table t (\n" +
				"\ti int\n" +
				") partition by linear hash (col)",
		},
		{
			input: "create table t (\n" +
				"\ti int\n" +
				") partition by KEY (col)",
		},
		{
			input: "create table t (\n" +
				"\ti int\n" +
				") partition by KEY ALGORITHM = 7 (col)",
		},
		{
			input: "create table t (\n" +
				"\ti int\n" +
				") partition by linear KEY ALGORITHM = 7 (col)",
		},
		{
			input: "create table t (\n" +
				"\ti int\n" +
				") partition by RANGE (col)",
		},
		{
			input: "create table t (\n" +
				"\ti int\n" +
				") partition by RANGE (i + j)",
		},
		{
			input: "create table t (\n" +
				"\ti int\n" +
				") partition by RANGE (month(i))",
		},
		{
			input: "create table t (\n" +
				"\ti int\n" +
				") partition by RANGE (concat(i))",
		},
		{
			input: "create table t (\n" +
				"\ti int\n" +
				") partition by RANGE COLUMNS (c1, c2, c3)",
			output: "create table t (\n" +
				"\ti int\n" +
				") partition by RANGE (c1, c2, c3)",
		},
		{
			input: "create table t (\n" +
				"\ti int\n" +
				") partition by LIST (col)",
		},
		{
			input: "create table t (\n" +
				"\ti int\n" +
				") partition by LIST (i + j)",
		},
		{
			input: "create table t (\n" +
				"\ti int\n" +
				") partition by LIST (month(i))",
		},
		{
			input: "create table t (\n" +
				"\ti int\n" +
				") partition by LIST (concat(i))",
		},
		{
			input: "create table t (\n" +
				"\ti int\n" +
				") partition by LIST COLUMNS (c1, c2, c3)",
			output: "create table t (\n" +
				"\ti int\n" +
				") partition by LIST (c1, c2, c3)",
		},
		{
			input: "create table t (\n" +
				"\ti int\n" +
				") partition by linear hash (a) partitions 20",
		},
		{
			input: "create table t (\n" +
				"\ti int\n" +
				") partition by linear hash (a) partitions 10 subpartition by linear hash (b) subpartitions 20",
		},
		{
			input: "create table t (\n" +
				"\ti int\n)" +
				"ROW_FORMAT=DYNAMIC",
			output: "create table t (\n" +
				"\ti int\n) " +
				"ROW_FORMAT DYNAMIC",
		},
		{
			input: "create table t (\n" +
				"\ti int\n" +
				") engine innodb " +
				"auto_increment 123 " +
				"avg_row_length 1 " +
				"character set utf8mb4 " +
				"partition by linear hash (a) partitions 10 " +
				"subpartition by linear hash (b) subpartitions 20",
		},
		{
			input: "create table t (\n" +
				"\ti1 int1,\n" +
				"\ti2 int2,\n" +
				"\ti3 int3,\n" +
				"\ti4 int4,\n" +
				"\ti8 int8,\n" +
				"\ti11 int1(1),\n" +
				"\ti22 int2(2),\n" +
				"\ti33 int3(3),\n" +
				"\ti44 int4(4),\n" +
				"\ti88 int8(8))",
			output: "create table t (\n" +
				"\ti1 tinyint,\n" +
				"\ti2 smallint,\n" +
				"\ti3 mediumint,\n" +
				"\ti4 int,\n" +
				"\ti8 bigint,\n" +
				"\ti11 tinyint(1),\n" +
				"\ti22 smallint(2),\n" +
				"\ti33 mediumint(3),\n" +
				"\ti44 int(4),\n" +
				"\ti88 bigint(8)\n" +
				")",
		},
	}
	for _, tcase := range testCases {
		runParseTestCase(t, tcase)
	}

	// these are keywords in our grammar due to their special syntax, but they should parse unquoted in MySQL
	nonsupportedKeywords := []string{
		"comment",
		"cast",
		"position",
		"trim",
	}
	nonsupported := map[string]bool{}
	for _, x := range nonsupportedKeywords {
		nonsupported[x] = true
	}

	for key := range keywords {
		input := fmt.Sprintf("create table t (\n\t`%s` bigint\n)", key)
		output := fmt.Sprintf("create table t (\n\t`%s` bigint\n)", key)
		t.Run(input, func(t *testing.T) {
			if _, ok := nonsupported[key]; ok {
				input = fmt.Sprintf("create table %s (\n\t%s bigint\n)", key, key)
				output = fmt.Sprintf("create table `%s` (\n\t`%s` bigint\n)", key, key)
			}
			tree, err := Parse(input)
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
		input:  "LOAD DATA INFILE 'x.txt' IGNORE INTO TABLE c",
		output: "load data infile 'x.txt' ignore into table c",
	}, {
		input:  "LOAD DATA INFILE 'x.txt' REPLACE INTO TABLE c",
		output: "load data infile 'x.txt' replace into table c",
	}, {
		input:  "LOAD DATA INFILE '~/Desktop/x.txt' INTO TABLE c",
		output: "load data infile '~/Desktop/x.txt' into table c",
	}, {
		input:  "LOAD DATA LOCAL INFILE ':SOURCE:9fa1415b62a44b53b86cffbccb210b51' INTO TABLE test",
		output: "load data local infile ':SOURCE:9fa1415b62a44b53b86cffbccb210b51' into table test",
	}, {
		input:  "LOAD DATA LOCAL INFILE ':SOURCE:9fa1415b62a44b53b86cffbccb210b51' IGNORE INTO TABLE test",
		output: "load data local infile ':SOURCE:9fa1415b62a44b53b86cffbccb210b51' ignore into table test",
	}, {
		input:  "LOAD DATA LOCAL INFILE ':SOURCE:9fa1415b62a44b53b86cffbccb210b51' REPLACE INTO TABLE test",
		output: "load data local infile ':SOURCE:9fa1415b62a44b53b86cffbccb210b51' replace into table test",
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
		input:  "LOAD DATA LOCAL INFILE 'l.csv' INTO TABLE test PARTITION (id) CHARACTER SET UTF8MB4 FIELDS TERMINATED BY '' ESCAPED BY '' LINES STARTING BY 'xxx' IGNORE 0 ROWS (`pk`)",
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
	}, {
		input:  "LOAD DATA INFILE '/tmp/test.txt' INTO TABLE test IGNORE 1 ROWS",
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

func TestEscape(t *testing.T) {
	testCases := []parseTest{
		{
			input:  `SELECT * FROM test WHERE col LIKE "%$_%" ESCAPE "$"`,
			output: `select * from test where col like '%$_%' escape '$'`,
		},
	}
	for _, tcase := range testCases {
		runParseTestCase(t, tcase)
	}
}

func TestTrim(t *testing.T) {
	testCases := []parseTest{
		{
			input:  `SELECT TRIM("foo")`,
			output: "select trim(both ' ' from 'foo')",
		},
		{
			input:  `SELECT TRIM("bar" FROM "foo")`,
			output: "select trim(both 'bar' from 'foo')",
		},
		{
			input:  `SELECT TRIM(LEADING "bar" FROM "foo")`,
			output: "select trim(leading 'bar' from 'foo')",
		},
		{
			input:  `SELECT TRIM(TRAILING "bar" FROM "foo")`,
			output: "select trim(trailing 'bar' from 'foo')",
		},
		{
			input:  `SELECT TRIM(BOTH "bar" FROM "foo")`,
			output: "select trim(both 'bar' from 'foo')",
		},
		{
			input:  `SELECT TRIM(TRIM("foobar"))`,
			output: "select trim(both ' ' from trim(both ' ' from 'foobar'))",
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
		tree, err := Parse(tcase.input)
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
		tree, err := Parse(tcase.input)
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
		tree, err := Parse(tcase.input)
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
		input:  "SET @foo = `o` `ne`;",
		output: "syntax error at position 20 near 'ne'",
	}, {
		input:  "use db/",
		output: "syntax error at position 8 near 'db'",
	}, {
		input:  "select $ from t",
		output: "syntax error at position 9 near '$'",
	}, {
		input:  "select : from t",
		output: "syntax error at position 9 near ':'",
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
		output: "syntax error at position 16 near '@@session.'",
	}, {
		input:  "set xyz.@@autocommit = true",
		output: "invalid system variable declaration `@@autocommit` at position 28 near 'true'",
	}, {
		input:  "set @@session.@autocommit = true",
		output: "syntax error at position 16 near '@@session.'",
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
		input:  "select * from foo limit '100'",
		output: "syntax error at position 30 near '100'",
	}, {
		input:  "start transaction read",
		output: "syntax error at position 23 near 'read'",
	}, {
		input:  "drop table x CASAS",
		output: "syntax error at position 19 near 'CASAS'",
	}, {
		input:  "drop user UserName1 UserName2",
		output: "syntax error at position 30 near 'UserName2'",
	}, {
		input:  "drop user `UserName1@`localhost",
		output: "syntax error at position 32 near 'localhost'",
	}, {
		input:  "drop user insert@table",
		output: "syntax error at position 17 near 'insert'",
	}, {
		input:  "show session status like asd",
		output: "syntax error at position 29 near 'asd'",
	}, {
		input:  "create table 1 (i int)",
		output: "syntax error at position 15 near '1'",
	}, {
		input:  "create table t (1 int)",
		output: "syntax error at position 18 near '1'",
	}, {
		input:  "update 1 set x = 1",
		output: "syntax error at position 9 near '1'",
	}, {
		input:  "update t set 1 = 1",
		output: "syntax error at position 15 near '1'",
	}, {
		input:  "alter table 1 add column (i int)",
		output: "syntax error at position 14 near '1'",
	}, {
		input:  "alter table t add column (1 int)",
		output: "syntax error at position 28 near '1'",
	}, {
		input:  "alter table t drop column 1",
		output: "syntax error at position 28 near '1'",
	}, {
		input:  "insert into 1 (i, j) values (1)",
		output: "syntax error at position 14 near '1'",
	}, {
		input:  "insert into t (1, j) values (1)",
		output: "syntax error at position 17 near '1'",
	}, {
		input:  "insert into t (i, 1) values (1)",
		output: "syntax error at position 20 near '1'",
	}, {
		input:  "insert into t values (1.a)",
		output: "syntax error at position 25 near '1.'",
	}, {
		input:  "insert into t values (1.1a)",
		output: "syntax error at position 26 near '1.1'",
	}, {
		input:  "insert into t values (1234.1a)",
		output: "syntax error at position 29 near '1234.1'",
	}, {
		input:  "SELECT id, name INTO @idVar FROM mytable INTO @nameVar",
		output: "Multiple INTO clauses in one query block at position 55 near '@nameVar'",
	}, {
		input:  "select 1 from dual into @myvar union select 2 from dual",
		output: "syntax error at position 37 near 'union'",
	}, {
		input:  "select id from mytable union select id from testtable union select id into @myvar from othertable",
		output: "INTO clause is not allowed at position 98 near 'othertable'",
	}, {
		input:  "create view invalidView as select id from mytable into @myview",
		output: "INTO clause is not allowed at position 63 near '@myview'",
	}, {
		input:  "select * from t1 where exists (select a from t2 union select b from t3 into @myvar)",
		output: "INTO clause is not allowed at position 84 near '@myvar'",
	}, {
		input:  "insert into a select * into @a from b",
		output: "INTO clause is not allowed at position 38 near 'b'",
	}, {
		input:  "create table t (id int primary key, col1 geometry SRID -1)",
		output: "syntax error at position 57 near 'SRID'",
	}, {
		input:  "create table t (id int primary key, col1 geometry null SRID 0 default null SRID 4236)",
		output: "cannot include SRID more than once at position 85 near '4236'",
	}, {
		input:  "create table dual (id int)",
		output: "syntax error at position 18 near 'dual'",
	}, {
		input:  "drop table dual",
		output: "syntax error at position 16 near 'dual'",
	}, {
		input:        "CREATE PROCEDURE testproc() BEGIN begin1: BEGIN END begin2; END",
		output:       "End-label begin2 without match at position 59 near 'begin2'",
		excludeMulti: true,
	}, {
		input:        "CREATE PROCEDURE testproc() BEGIN loop1: LOOP BEGIN END; END LOOP loop2; END",
		output:       "End-label loop2 without match at position 72 near 'loop2'",
		excludeMulti: true,
	}, {
		input:        "CREATE PROCEDURE testproc() BEGIN repeat1: REPEAT BEGIN END; UNTIL a > 7 END REPEAT repeat2; END",
		output:       "End-label repeat2 without match at position 92 near 'repeat2'",
		excludeMulti: true,
	}, {
		input:        "CREATE PROCEDURE testproc() BEGIN while1: WHILE a > 7 DO BEGIN END; END WHILE while2; END",
		output:       "End-label while2 without match at position 85 near 'while2'",
		excludeMulti: true,
	}, {
		input:  "ALTER EVENT myevent",
		output: "You have an error in your SQL syntax; At least one event field to alter needs to be defined at position 20 near 'myevent'",
	}, {
		input:  "ALTER DEFINER = `newuser`@`localhost` EVENT myevent",
		output: "You have an error in your SQL syntax; At least one event field to alter needs to be defined at position 52 near 'myevent'",
	},
	}

	invalidSRIDQueries = func() (queries []parseTest) {
		for _, column := range sampleColumns {
			var statement strings.Builder
			statement.WriteString("create table t (id int primary key, ")
			statement.WriteString(column)
			statement.WriteString(" SRID 0)")
			queries = append(queries, parseTest{
				input:  statement.String(),
				output: "cannot define SRID for non spatial types",
			})
		}
		return
	}()

	// invalidAnsiQuotesSQL contains invalid SQL statements that use ANSI_QUOTES mode.
	invalidAnsiQuotesSQL = []parseTest{
		{
			// Assert that the two identifier quotes do not match each other
			input:  "select \"foo`",
			output: "syntax error at position 13 near 'foo`'",
		},
		{
			// Assert that the two identifier quotes do not match each other
			input:  "select `bar\"",
			output: "syntax error at position 13 near 'bar\"'",
		},
		{
			// Assert that single and double quotes do not auto concatenate in ANSI_QUOTES mode
			input:  "select 'a' \"b\" 'c'",
			output: "syntax error at position 19 near 'c'",
		},
	}
)

func TestErrors(t *testing.T) {
	for _, tcase := range invalidSQL {
		t.Run(tcase.input, func(t *testing.T) {
			_, err := Parse(tcase.input)
			assert.Equal(t, tcase.output, err.Error())
		})
	}
	for _, tcase := range invalidSRIDQueries {
		t.Run(tcase.input, func(t *testing.T) {
			_, err := Parse(tcase.input)
			assert.Contains(t, err.Error(), tcase.output)
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
		output: "syntax error at position 18 near 'bb'",
	}, {
		// Test that we don't step at ';' inside strings.
		input:  "create table a bb 'a;'; select * from t",
		output: "syntax error at position 18 near 'bb'",
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
		_, err := Parse(scanner.Text())
		if err != nil {
			t.Error(scanner.Text())
			t.Errorf(" Error: %v", err)
		}
	}
}

// not reserved in mysql
var correctlyDoParse = []string{
	"account",
	"action",
	"active",
	"admin",
	"after",
	"against",
	"aggregate",
	"algorithm",
	"always",
	"analyse",
	"any",
	"array",
	"ascii",
	"at",
	"attribute",
	"authentication",
	"autoextend_size",
	"auto_increment",
	"avg",
	"avg_row_length",
	"backup",
	"begin",
	"binlog",
	"bit",
	"block",
	"bool",
	"boolean",
	"btree",
	"buckets",
	"byte",
	"cache",
	"cascaded",
	"catalog_name",
	"chain",
	"challenge_response",
	"changed",
	"channel",
	"charset",
	"checksum",
	"cipher",
	"class_origin",
	"client",
	"clone",
	"close",
	"coalesce",
	"code",
	"collation",
	"columns",
	"column_format",
	"column_name",
	"comment",
	"commit",
	"committed",
	"compact",
	"completion",
	"component",
	"compressed",
	"compression",
	"concurrent",
	"connection",
	"consistent",
	"constraint_catalog",
	"constraint_name",
	"constraint_schema",
	"contained",
	"contains",
	"context",
	"cpu",
	"current",
	"cursor_name",
	"data",
	"datafile",
	"date",
	"datetime",
	"day",
	"deallocate",
	"default_auth",
	"definer",
	"definition",
	"delay_key_write",
	"description",
	"des_key_file",
	"diagnostics",
	"directory",
	"disable",
	"discard",
	"disk",
	"do",
	"dumpfile",
	"duplicate",
	"dynamic",
	"enable",
	"encryption",
	"end",
	"ends",
	"enforced",
	"engine",
	"engines",
	"engine_attribute",
	"enum",
	"error",
	"errors",
	"event",
	"events",
	"every",
	"exchange",
	"exclude",
	"execute",
	"expansion",
	"expire",
	"export",
	"extended",
	"extent_size",
	"factor",
	"failed_login_attempts",
	"fast",
	"faults",
	"fields",
	"file",
	"file_block_size",
	"filter",
	"finish",
	"first",
	"fixed",
	"flush",
	"following",
	"follows",
	"format",
	"found",
	"full",
	"general",
	"geomcollection",
	"geometry",
	"geometrycollection",
	"get_format",
	"get_master_public_key",
	"get_source_public_key",
	"global",
	"grants",
	"group_replication",
	"gtid_only",
	"handler",
	"hash",
	"help",
	"histogram",
	"history",
	"host",
	"hosts",
	"hour",
	"identified",
	"ignore_server_ids",
	"import",
	"inactive",
	"indexes",
	"initial",
	"initial_size",
	"initiate",
	"insert_method",
	"install",
	"instance",
	"invisible",
	"invoker",
	"io",
	"io_thread",
	"ipc",
	"isolation",
	"issuer",
	"json",
	"json_value",
	"keyring",
	"key_block_size",
	"language",
	"last",
	"leaves",
	"less",
	"level",
	"linestring",
	"list",
	"local",
	"locked",
	"locks",
	"logfile",
	"logs",
	"master",
	"master_auto_position",
	"master_compression_algorithms",
	"master_connect_retry",
	"master_delay",
	"master_heartbeat_period",
	"master_host",
	"master_log_file",
	"master_log_pos",
	"master_password",
	"master_port",
	"master_public_key_path",
	"master_retry_count",
	"master_server_id",
	"master_ssl",
	"master_ssl_ca",
	"master_ssl_capath",
	"master_ssl_cert",
	"master_ssl_cipher",
	"master_ssl_crl",
	"master_ssl_crlpath",
	"master_ssl_key",
	"master_tls_ciphersuites",
	"master_tls_version",
	"master_user",
	"master_zstd_compression_level",
	"max_connections_per_hour",
	"max_queries_per_hour",
	"max_rows",
	"max_size",
	"max_updates_per_hour",
	"max_user_connections",
	"medium",
	"member",
	"memory",
	"merge",
	"message_text",
	"microsecond",
	"migrate",
	"minute",
	"min_rows",
	"mode",
	"modify",
	"month",
	"multilinestring",
	"multipoint",
	"multipolygon",
	"mutex",
	"mysql_errno",
	"name",
	"names",
	"national",
	"nchar",
	"ndb",
	"ndbcluster",
	"nested",
	"network_namespace",
	"never",
	"new",
	"no",
	"nodegroup",
	"none",
	"nowait",
	"no_wait",
	"nulls",
	"number",
	"nvarchar",
	"offset",
	"oj",
	"old",
	"one",
	"only",
	"open",
	"optional",
	"options",
	"ordinality",
	"organization",
	"others",
	"owner",
	"pack_keys",
	"page",
	"parser",
	"partial",
	"partitioning",
	"partitions",
	"password",
	"password_lock_time",
	"path",
	"persist",
	"persist_only",
	"phase",
	"plugin",
	"plugins",
	"plugin_dir",
	"point",
	"polygon",
	"port",
	"precedes",
	"preceding",
	"prepare",
	"preserve",
	"prev",
	"privileges",
	"privilege_checks_user",
	"process",
	"processlist",
	"profile",
	"profiles",
	"proxy",
	"quarter",
	"query",
	"quick",
	"random",
	"read_only",
	"rebuild",
	"recover",
	"redofile",
	"redo_buffer_size",
	"redundant",
	"reference",
	"registration",
	"relay",
	"relaylog",
	"relay_log_file",
	"relay_log_pos",
	"relay_thread",
	"reload",
	"remote",
	"remove",
	"reorganize",
	"repair",
	"repeatable",
	"replica",
	"replicas",
	"replicate_do_db",
	"replicate_do_table",
	"replicate_ignore_db",
	"replicate_ignore_table",
	"replicate_rewrite_db",
	"replicate_wild_do_table",
	"replicate_wild_ignore_table",
	"replication",
	"require_row_format",
	"reset",
	"resource",
	"respect",
	"restart",
	"restore",
	"resume",
	"retain",
	"returned_sqlstate",
	"returning",
	"returns",
	"reuse",
	"reverse",
	"role",
	"rollback",
	"rollup",
	"rotate",
	"routine",
	"row_count",
	"row_format",
	"rtree",
	"savepoint",
	"schedule",
	"schema_name",
	"second",
	"secondary",
	"secondary_engine",
	"secondary_engine_attribute",
	"secondary_load",
	"secondary_unload",
	"security",
	"serial",
	"serializable",
	"server",
	"session",
	"share",
	"shutdown",
	"signed",
	"simple",
	"skip",
	"slave",
	"slow",
	"snapshot",
	"socket",
	"some",
	"soname",
	"sounds",
	"source",
	"source_auto_position",
	"source_bind",
	"source_compression_algorithms",
	"source_connect_retry",
	"source_delay",
	"source_heartbeat_period",
	"source_host",
	"source_log_file",
	"source_log_pos",
	"source_password",
	"source_port",
	"source_public_key_path",
	"source_retry_count",
	"source_ssl",
	"source_ssl_ca",
	"source_ssl_capath",
	"source_ssl_cert",
	"source_ssl_cipher",
	"source_ssl_crl",
	"source_ssl_crlpath",
	"source_ssl_key",
	"source_ssl_verify_server_cert",
	"source_tls_ciphersuites",
	"source_tls_version",
	"source_user",
	"source_zstd_compression_level",
	"sql_after_gtids",
	"sql_after_mts_gaps",
	"sql_before_gtids",
	"sql_buffer_result",
	"sql_thread",
	"sql_tsi_day",
	"sql_tsi_hour",
	"sql_tsi_minute",
	"sql_tsi_month",
	"sql_tsi_quarter",
	"sql_tsi_second",
	"sql_tsi_week",
	"sql_tsi_year",
	"srid",
	"stacked",
	"start",
	"starts",
	"stats_auto_recalc",
	"stats_persistent",
	"stats_sample_pages",
	"status",
	"stop",
	"storage",
	"stream",
	"string",
	"subclass_origin",
	"subject",
	"subpartition",
	"subpartitions",
	"super",
	"suspend",
	"swaps",
	"switches",
	"tables",
	"tablespace",
	"table_checksum",
	"table_name",
	"temporary",
	"temptable",
	"text",
	"than",
	"thread_priority",
	"ties",
	"time",
	"timestamp",
	"timestampadd",
	"timestampdiff",
	"tls",
	"transaction",
	"triggers",
	"truncate",
	"type",
	"types",
	"unbounded",
	"uncommitted",
	"undefined",
	"undofile",
	"undo_buffer_size",
	"unicode",
	"uninstall",
	"unknown",
	"unregister",
	"until",
	"upgrade",
	"user",
	"user_resources",
	"use_frm",
	"validation",
	"value",
	"variables",
	"vcpu",
	"version",
	"versions",
	"view",
	"visible",
	"wait",
	"warnings",
	"week",
	"weight_string",
	"without",
	"work",
	"wrapper",
	"x509",
	"xa",
	"xid",
	"xml",
	"year",
	"zone",
}

// reserved in mysql
var correctlyDontParse = []string{
	"accessible",
	"add",
	"all",
	"alter",
	"analyze",
	"and",
	"as",
	"asc",
	"asensitive",
	"before",
	"between",
	"bigint",
	"binary",
	"blob",
	"both",
	"by",
	"call",
	"cascade",
	"case",
	"change",
	"char",
	"character",
	"check",
	"collate",
	"column",
	"condition",
	"constraint",
	"continue",
	"convert",
	"create",
	"cross",
	"cube",
	"cume_dist",
	"current_date",
	"current_time",
	"current_timestamp",
	"current_user",
	"cursor",
	"database",
	"databases",
	"day_hour",
	"day_microsecond",
	"day_minute",
	"day_second",
	"dec",
	"decimal",
	"declare",
	"default",
	"delayed",
	"delete",
	"dense_rank",
	"desc",
	"describe",
	"deterministic",
	"distinct",
	"distinctrow",
	"div",
	"double",
	"drop",
	"dual",
	"each",
	"else",
	"elseif",
	"empty",
	"enclosed",
	"escaped",
	"except",
	"exists",
	"exit",
	"explain",
	"false",
	"fetch",
	"first_value",
	"float",
	"float4",
	"float8",
	"for",
	"force",
	"foreign",
	"from",
	"fulltext",
	"function",
	"generated",
	"get",
	"grant",
	"group",
	"grouping",
	"groups",
	"having",
	"high_priority",
	"hour_microsecond",
	"hour_minute",
	"hour_second",
	"if",
	"ignore",
	"in",
	"index",
	"infile",
	"inner",
	"inout",
	"insensitive",
	"insert",
	"int",
	"int1",
	"int2",
	"int3",
	"int4",
	"int8",
	"integer",
	"interval",
	"into",
	"io_after_gtids",
	"io_before_gtids",
	"is",
	"iterate",
	"join",
	"json_table",
	"key",
	"keys",
	"kill",
	"lag",
	"last_value",
	"lateral",
	"lead",
	"leading",
	"leave",
	"left",
	"like",
	"limit",
	"linear",
	"lines",
	"load",
	"localtime",
	"localtimestamp",
	"lock",
	"long",
	"longblob",
	"longtext",
	"loop",
	"low_priority",
	"master_bind",
	"master_ssl_verify_server_cert",
	"match",
	"maxvalue",
	"mediumblob",
	"mediumint",
	"mediumtext",
	"middleint",
	"minute_microsecond",
	"mod",
	"modifies",
	"natural",
	"not",
	"no_write_to_binlog",
	"nth_value",
	"ntile",
	"null",
	"numeric",
	"of",
	"on",
	"optimize",
	"optimizer_costs",
	"option",
	"optionally",
	"or",
	"order",
	"out",
	"outer",
	"outfile",
	"over",
	"partition",
	"percent_rank",
	"precision",
	"primary",
	"procedure",
	"purge",
	"range",
	"rank",
	"read",
	"reads",
	"read_write",
	"real",
	"recursive",
	"references",
	"regexp",
	"release",
	"rename",
	"repeat",
	"replace",
	"require",
	"resignal",
	"restrict",
	"return",
	"revoke",
	"right",
	"rlike",
	"row",
	"rows",
	"row_number",
	"schema",
	"schemas",
	"second_microsecond",
	"select",
	"sensitive",
	"separator",
	"set",
	"show",
	"signal",
	"smallint",
	"spatial",
	"specific",
	"sql",
	"sqlexception",
	"sqlstate",
	"sqlwarning",
	"sql_big_result",
	"sql_calc_found_rows",
	"sql_small_result",
	"ssl",
	"starting",
	"stored",
	"straight_join",
	"system",
	"table",
	"terminated",
	"then",
	"tinyblob",
	"tinyint",
	"tinytext",
	"to",
	"trailing",
	"trigger",
	"true",
	"undo",
	"union",
	"unique",
	"unlock",
	"unsigned",
	"update",
	"usage",
	"use",
	"using",
	"utc_date",
	"utc_time",
	"utc_timestamp",
	"values",
	"varbinary",
	"varchar",
	"varcharacter",
	"varying",
	"virtual",
	"when",
	"where",
	"while",
	"window",
	"with",
	"write",
	"xor",
	"year_month",
	"zerofill",
}

// not reserved in mysql
var incorrectlyDontParse = []string{
	"escape",
	"next",
	"off",
	"sql_cache",
	"sql_no_cache",
}

// reserved in mysql
var incorrectlyParse = []string{
	"dual",
	"minute_second",
}

// TestKeywordsCorrectlyParse ensures that certain keywords can be parsed by a series of edit queries.
func TestKeywordsCorrectlyParse(t *testing.T) {
	tests := []string{
		"SELECT 1 as %s",
		"INSERT INTO t_t (%s) VALUES ('one')",
		"INSERT INTO t_t (pk, %s) VALUES (1, 'one')",
		"INSERT INTO t_t (pk, %s, c1) VALUES (1, 'one', 1)",
		"DELETE FROM t where %s=1",
		"UPDATE t SET %s=1",
		"CREATE TABLE t(%s int)",
		"CREATE TABLE t(foo int, %s int)",
		"CREATE TABLE t(foo int, %s int, foo int)",
		"CREATE TABLE foo (i int, primary key (%s))",
		"CREATE TABLE %s(i int)",
		"SELECT * FROM t ORDER BY t.%s",
		"SELECT %s.c FROM t",
		"ALTER TABLE t DROP CONSTRAINT %s",
		"ALTER TABLE t DROP CHECK %s",
		"ALTER TABLE t DROP COLUMN %s",
	}

	multikeyword_tests := []string{
		"ALTER TABLE t RENAME COLUMN %s TO %s",
	}

	for _, kw := range correctlyDoParse {
		for _, query := range tests {
			test := fmt.Sprintf(query, kw)
			t.Run(test, func(t *testing.T) {
				_, err := Parse(test)
				assert.NoError(t, err)
			})
		}
		for _, query := range multikeyword_tests {
			test := fmt.Sprintf(query, kw, kw)
			t.Run(test, func(t *testing.T) {
				_, err := Parse(test)
				assert.NoError(t, err)
			})
		}
	}
}

func TestReservedKeywordsParseWhenQualified(t *testing.T) {
	tcTest := "SELECT * FROM t ORDER BY %s.%s"
	sTest := "SELECT %s.%s FROM t"

	tests := []string{tcTest, sTest}

	// these are reserved keywords that don't work even when qualified
	badReservedKeywords := map[string]bool{
		"all":                 true,
		"distinct":            true,
		"div":                 true,
		"key":                 true,
		"select":              true,
		"sql_calc_found_rows": true,
		"straight_join":       true,
		"when":                true,
		"dual":                true,
	}

	for _, kw := range correctlyDontParse {
		for _, query := range tests {
			test := fmt.Sprintf(query, kw, kw)
			t.Run(test, func(t *testing.T) {
				if badReservedKeywords[kw] {
					t.Skip("this reserved word doesn't work when qualified")
				}
				_, err := Parse(test)
				assert.NoError(t, err)
			})
		}
	}
}

// TestKeywordsCorrectlyDontParse ensures certain keywords should not be parsed in certain queries.
func TestKeywordsCorrectlyDontParse(t *testing.T) {
	aliasTest := "SELECT 1 as %s"
	iTest := "INSERT INTO t (%s) VALUES (1)"
	dTest := "DELETE FROM t where %s=1"
	uTest := "UPDATE t SET %s=1"
	cTest := "CREATE TABLE t(%s int)"
	tTest := "CREATE TABLE %s(i int)"

	// these are reserved keywords that are also values, so they can be used in conditions
	validConditionReservedKeywords := map[string]bool{
		"current_date":      true,
		"current_time":      true,
		"current_timestamp": true,
		"current_user":      true,
		"false":             true,
		"localtime":         true,
		"localtimestamp":    true,
		"null":              true,
		"true":              true,
		"utc_date":          true,
		"utc_time":          true,
		"utc_timestamp":     true,
	}

	tests := []string{aliasTest, iTest, dTest, uTest, cTest, tTest}

	for _, kw := range correctlyDontParse {
		for _, query := range tests {
			if query == dTest && validConditionReservedKeywords[kw] {
				continue
			}
			test := fmt.Sprintf(query, kw)
			t.Run(test, func(t *testing.T) {
				_, err := Parse(test)
				if err == nil {
					// If we can successfully parse a MySQL reserved keyword as an identifier without needing backtick
					// quoting, just skip this test so we have a record of it, instead of failing the test. This allows
					// us to track the difference, but we don't want to prevent being able to use reserved keywords
					// without quotes if we can easily support them.
					t.Skip()
				} else {
					assert.Error(t, err)
				}
			})
		}
	}
}

// TestKeywordsIncorrectlyDoParse documents bad behavior where the parser is incorrectly parsing a keyword that should error.
func TestKeywordsIncorrectlyDoParse(t *testing.T) {
	aliasTest := "SELECT 1 as %s"
	iTest := "INSERT INTO t (%s) VALUES (1)"
	dTest := "DELETE FROM t where %s=1"
	uTest := "UPDATE t SET %s=1"
	cTest := "CREATE TABLE t(%s int)"

	tests := []string{aliasTest, iTest, dTest, uTest, cTest}

	for _, kw := range incorrectlyParse {
		for _, query := range tests {
			test := fmt.Sprintf(query, kw)
			t.Run(test, func(t *testing.T) {
				t.Skip()
				_, err := Parse(test)
				assert.Error(t, err)
			})
		}
	}
}

// TestKeywordsIncorrectlyDontParse documents behavior where the parser is incorrectly throwing an error for a valid keyword.
func TestKeywordsIncorrectlyDontParse(t *testing.T) {
	aliasTest := "SELECT 1 as %s"
	iTest := "INSERT INTO t (%s) VALUES (1)"
	dTest := "DELETE FROM t where %s=1"
	uTest := "UPDATE t SET %s=1"
	cTest := "CREATE TABLE t(%s int)"

	tests := []string{aliasTest, iTest, dTest, uTest, cTest}

	for _, kw := range incorrectlyDontParse {
		for _, query := range tests {
			test := fmt.Sprintf(query, kw)
			t.Run(test, func(t *testing.T) {
				t.Skip("delete doesn't work for these words yet")
				_, err := Parse(test)
				assert.NoError(t, err)
			})
		}
	}
}

func TestJSONTable(t *testing.T) {
	validSQL := []parseTest{
		{
			input: `
SELECT *
FROM
	JSON_TABLE(
		'[{"a":1},{"a":2}]',
		"$[*]" COLUMNS(
			x varchar(100) path "$.a"
		)
	) as tt;`,
			output: "select * from json_table('[{\\\"a\\\":1},{\\\"a\\\":2}]', '$[*]' columns(x varchar(100) path '$.a')) as tt",
		},
		{
			input: `
SELECT *
FROM
	JSON_TABLE(
		'[{"a":1, "b":2},{"a":3, "b":4}]',
		"$[*]" COLUMNS(
			x varchar(100) path "$.a",
			y varchar(100) path "$.b"
		)
	) as tt;`,
			output: "select * from json_table('[{\\\"a\\\":1, \\\"b\\\":2},{\\\"a\\\":3, \\\"b\\\":4}]', '$[*]' columns(x varchar(100) path '$.a', y varchar(100) path '$.b')) as tt",
		},
		{
			input: `
SELECT *
FROM
	JSON_TABLE(
		concat('[{},','{}]'),
		"$[*]" COLUMNS(
			x varchar(100) path "$.a",
			y varchar(100) path "$.b"
		)
	) as t;
	`,
			output: "select * from json_table(concat('[{},', '{}]'), '$[*]' columns(" +
				"x varchar(100) path '$.a', " +
				"y varchar(100) path '$.b'" +
				")) as t",
		},
		{
			input: `
SELECT *
FROM
	JSON_TABLE(
		123,
		"$[*]" COLUMNS(
			x varchar(100) path "$.a",
			y varchar(100) path "$.b"
		)
	) as t;
	`,
			output: "select * from json_table(123, '$[*]' columns(" +
				"x varchar(100) path '$.a', " +
				"y varchar(100) path '$.b'" +
				")) as t",
		},
		{
			input: `
SELECT *
FROM
	JSON_TABLE(
		'[{"a":1},{"a":2}]',
		"$[*]" COLUMNS(
			x varchar(100) path "$.a"
		)
	) t1
JOIN
	json_table(
		'[{"a":1},{"a":2}]',
		"$[*]" COLUMNS(
			x varchar(100) path "$.a"
		)
	) t2;`,
			output: "select * from json_table('[{\\\"a\\\":1},{\\\"a\\\":2}]', '$[*]' columns(" +
				"x varchar(100) path '$.a'" +
				")) as t1 join json_table('[{\\\"a\\\":1},{\\\"a\\\":2}]', '$[*]' columns(" +
				"x varchar(100) path '$.a'" +
				")) as t2",
		},
		{
			input: `
SELECT *
FROM
	JSON_TABLE(
		'[{"a":1},{"a":2}]',
		"$[*]" COLUMNS(
			x varchar(100) path "$.a"
		)
	) t
JOIN
	tt;`,
			output: "select * from json_table('[{\\\"a\\\":1},{\\\"a\\\":2}]', '$[*]' columns(" +
				"x varchar(100) path '$.a'" +
				")) as t join tt",
		},
		{
			input: `
SELECT *
FROM
	t
JOIN
	JSON_TABLE(
		'[{"a":1},{"a":2}]',
		"$[*]" COLUMNS(
			x varchar(100) path "$.a"
		)
	) tt;`,
			output: "select * from t join json_table('[{\\\"a\\\":1},{\\\"a\\\":2}]', '$[*]' columns(" +
				"x varchar(100) path '$.a'" +
				")) as tt",
		},
		{
			input: `
SELECT *
FROM
	JSON_TABLE(
		'[{"a":1},{"a":2}]',
		"$[*]" COLUMNS(
			x varchar(100) path "$.a"
		)
	) t1
UNION
SELECT *
FROM
	json_table(
		'[{"b":1},{"b":2}]',
		"$[*]" columns(
			y varchar(100) path "$.b"
		)
	) t2;`,
			output: "select * from json_table('[{\\\"a\\\":1},{\\\"a\\\":2}]', '$[*]' columns(" +
				"x varchar(100) path '$.a'" +
				")) as t1 union select * from json_table('[{\\\"b\\\":1},{\\\"b\\\":2}]', '$[*]' columns(" +
				"y varchar(100) path '$.b'" +
				")) as t2",
		},
		{
			input: `SELECT * FROM t WHERE i in (SELECT x FROM JSON_TABLE('[{"a":1},{"a":2}]', "$[*]" COLUMNS(x VARCHAR(100) PATH "$.a")) AS tt);`,
			output: "select * from t where i in (select x from json_table('[{\\\"a\\\":1},{\\\"a\\\":2}]', '$[*]' columns(" +
				"x VARCHAR(100) path '$.a'" +
				")) as tt)",
		},
		{
			input: `
SELECT x, y
FROM
	JSON_TABLE(
		'[{"a":1},{"a":2}]',
		"$[*]" COLUMNS(
			x varchar(100) path "$.a"
		)
	) t1,
	JSON_TABLE(
		'[{"b":3},{"b":4}]',
		"$[*]" COLUMNS(
			y varchar(100) path "$.b"
		)
	) t2;`,
			output: "select x, y from json_table('[{\\\"a\\\":1},{\\\"a\\\":2}]', '$[*]' columns(" +
				"x varchar(100) path '$.a'" +
				")) as t1, json_table('[{\\\"b\\\":3},{\\\"b\\\":4}]', '$[*]' columns(" +
				"y varchar(100) path '$.b'" +
				")) as t2",
		},

		// FOR ORDINALITY TESTS
		{
			input: `SELECT * FROM JSON_TABLE('{}', '$' COLUMNS( pk FOR ORDINALITY, c1 INT PATH '$.c1')) as jt;`,
			output: "select * from json_table('{}', '$' columns(" +
				"pk FOR ORDINALITY, " +
				"c1 INT path '$.c1'" +
				")) as jt",
		},

		// EXISTS TESTS
		{
			input: `SELECT * FROM JSON_TABLE('{}', '$' COLUMNS( c1 INT EXISTS PATH '$.c1')) as jt;`,
			output: "select * from json_table('{}', '$' columns(" +
				"c1 INT exists path '$.c1'" +
				")) as jt",
		},

		// ON EMPTY TESTS
		{
			input: `SELECT * FROM JSON_TABLE('{}', '$' COLUMNS( c1 INT PATH '$.c1' NULL ON EMPTY )) as jt;`,
			output: "select * from json_table('{}', '$' columns(" +
				"c1 INT path '$.c1' DEFAULT null on empty" +
				")) as jt",
		},
		{
			input: `SELECT * FROM JSON_TABLE('{}', '$' COLUMNS( c1 INT PATH '$.c1' DEFAULT 1 ON EMPTY )) as jt;`,
			output: "select * from json_table('{}', '$' columns(" +
				"c1 INT path '$.c1' DEFAULT 1 on empty" +
				")) as jt",
		},
		{
			input: `SELECT * FROM JSON_TABLE('{}', '$' COLUMNS( c1 INT PATH '$.c1' DEFAULT '1' ON EMPTY )) as jt;`,
			output: "select * from json_table('{}', '$' columns(" +
				"c1 INT path '$.c1' DEFAULT '1' on empty" +
				")) as jt",
		},
		{
			input: `SELECT * FROM JSON_TABLE('{}', '$' COLUMNS( c1 INT PATH '$.c1' DEFAULT '{"abc": 123}' ON EMPTY )) as jt;`,
			output: "select * from json_table('{}', '$' columns(" +
				"c1 INT path '$.c1' DEFAULT '{\\\"abc\\\": 123}' on empty" +
				")) as jt",
		},
		{
			input: `SELECT * FROM JSON_TABLE('{}', '$' COLUMNS( c1 INT PATH '$.c1' ERROR ON EMPTY )) as jt;`,
			output: "select * from json_table('{}', '$' columns(" +
				"c1 INT path '$.c1' error on empty" +
				")) as jt",
		},

		// ON ERROR TESTS
		{
			input: `SELECT * FROM JSON_TABLE('{}', '$' COLUMNS( c1 INT PATH '$.c1' NULL ON ERROR )) as jt;`,
			output: "select * from json_table('{}', '$' columns(" +
				"c1 INT path '$.c1' DEFAULT null on error " +
				")) as jt",
		},
		{
			input: `SELECT * FROM JSON_TABLE('{}', '$' COLUMNS( c1 INT PATH '$.c1' DEFAULT 1 ON ERROR )) as jt;`,
			output: "select * from json_table('{}', '$' columns(" +
				"c1 INT path '$.c1' DEFAULT 1 on error " +
				")) as jt",
		},
		{
			input: `SELECT * FROM JSON_TABLE('{}', '$' COLUMNS( c1 INT PATH '$.c1' DEFAULT '1' ON ERROR )) as jt;`,
			output: "select * from json_table('{}', '$' columns(" +
				"c1 INT path '$.c1' DEFAULT '1' on error " +
				")) as jt",
		},
		{
			input: `SELECT * FROM JSON_TABLE('{}', '$' COLUMNS( c1 INT PATH '$.c1' DEFAULT '{"abc": 123}' ON ERROR )) as jt;`,
			output: "select * from json_table('{}', '$' columns(" +
				"c1 INT path '$.c1' DEFAULT '{\\\"abc\\\": 123}' on error " +
				")) as jt",
		},
		{
			input: `SELECT * FROM JSON_TABLE('{}', '$' COLUMNS( c1 INT PATH '$.c1' ERROR ON ERROR )) as jt;`,
			output: "select * from json_table('{}', '$' columns(" +
				"c1 INT path '$.c1' error on error" +
				")) as jt",
		},

		// NESTED PATH TESTS
		{
			input: `SELECT * FROM JSON_TABLE('{}', '$' columns(NESTED PATH '$' columns (b INT PATH '$'))) AS jt;`,
			output: "select * from json_table('{}', '$' columns(" +
				"nested path '$' columns(" +
				"b INT path '$'" +
				")" +
				")) as jt",
		},
		// TODO: MySQL doesn't parse this, but their docs say they do.
		// https://dev.mysql.com/doc/refman/8.0/en/json-table-functions.html#:~:text=NESTED%20PATH%20(or%20simply%20NESTED%3B%20PATH%20is%20optional)
		{
			input: `SELECT * FROM JSON_TABLE('{}', '$' columns(NESTED '$' columns (b INT PATH '$'))) AS jt;`,
			output: "select * from json_table('{}', '$' columns(" +
				"nested path '$' columns(" +
				"b INT path '$'" +
				")" +
				")) as jt",
		},
		{
			input: `SELECT * FROM JSON_TABLE('{}', '$' columns(NESTED PATH '$' columns (a INT PATH '$', b INT PATH '$'))) AS jt;`,
			output: "select * from json_table('{}', '$' columns(nested path '$' columns(" +
				"a INT path '$', " +
				"b INT path '$'" +
				")" +
				")) as jt",
		},
		{
			input: `SELECT * FROM  JSON_TABLE('{}', 'root_path' COLUMNS( a INT PATH 'a_path', NESTED PATH 'b_path' COLUMNS (NESTED PATH '$' COLUMNS (b1 INT PATH 'b1_path')))) AS jt;`,
			output: "select * from json_table('{}', 'root_path' columns(" +
				"a INT path 'a_path', " +
				"nested path 'b_path' columns(" +
				"nested path '$' columns(" +
				"b1 INT path 'b1_path'" +
				")" +
				")" +
				")) as jt",
		},
	}

	for _, tcase := range validSQL {
		runParseTestCase(t, tcase)
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
