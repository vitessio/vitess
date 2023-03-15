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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildParsedQuery(t *testing.T) {
	testcases := []struct {
		in   string
		args []any
		out  string
	}{{
		in:  "select * from tbl",
		out: "select * from tbl",
	}, {
		in:  "select * from tbl where b=4 or a=3",
		out: "select * from tbl where b=4 or a=3",
	}, {
		in:  "select * from tbl where b = 4 or a = 3",
		out: "select * from tbl where b = 4 or a = 3",
	}, {
		in:   "select * from tbl where name='%s'",
		args: []any{"xyz"},
		out:  "select * from tbl where name='xyz'",
	}}

	for _, tc := range testcases {
		t.Run(tc.in, func(t *testing.T) {
			parsed := BuildParsedQuery(tc.in, tc.args...)
			assert.Equal(t, tc.out, parsed.Query)
		})
	}
}

func TestCanonicalOutput(t *testing.T) {
	testcases := []struct {
		input     string
		canonical string
	}{
		{
			"create table t(id int)",
			"CREATE TABLE `t` (\n\t`id` int\n)",
		},
		{
			"create algorithm = merge sql security definer view a (b,c,d) as select * from e with cascaded check option",
			"CREATE ALGORITHM = merge SQL SECURITY DEFINER VIEW `a`(`b`, `c`, `d`) AS SELECT * FROM `e` WITH CASCADED CHECK OPTION",
		},
		{
			"create or replace algorithm = temptable definer = a@b.c.d sql security definer view a(b,c,d) as select * from e with local check option",
			"CREATE OR REPLACE ALGORITHM = temptable DEFINER = a@`b.c.d` SQL SECURITY DEFINER VIEW `a`(`b`, `c`, `d`) AS SELECT * FROM `e` WITH LOCAL CHECK OPTION",
		},
		{
			"create table `a`(`id` int, primary key(`id`))",
			"CREATE TABLE `a` (\n\t`id` int,\n\tPRIMARY KEY (`id`)\n)",
		},
		{
			"create table `a`(`id` int unsigned, primary key(`id`))",
			"CREATE TABLE `a` (\n\t`id` int unsigned,\n\tPRIMARY KEY (`id`)\n)",
		},
		{
			"create table `a`(`id` int zerofill, primary key(`id`))",
			"CREATE TABLE `a` (\n\t`id` int zerofill,\n\tPRIMARY KEY (`id`)\n)",
		},
		{
			"create table `a`(`id` int primary key)",
			"CREATE TABLE `a` (\n\t`id` int PRIMARY KEY\n)",
		},
		{
			"create table a (id int not null auto_increment, v varchar(32) default null, v2 varchar(62) charset utf8mb4 collate utf8mb4_0900_ai_ci, key v_idx(v(16)))",
			"CREATE TABLE `a` (\n\t`id` int NOT NULL AUTO_INCREMENT,\n\t`v` varchar(32) DEFAULT NULL,\n\t`v2` varchar(62) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,\n\tKEY `v_idx` (`v`(16))\n)",
		},
		{
			"create table a (id int not null primary key, dt datetime default current_timestamp)",
			"CREATE TABLE `a` (\n\t`id` int NOT NULL PRIMARY KEY,\n\t`dt` datetime DEFAULT current_timestamp()\n)",
		},
		{
			"create table `insert`(`update` int, primary key(`delete`))",
			"CREATE TABLE `insert` (\n\t`update` int,\n\tPRIMARY KEY (`delete`)\n)",
		},
		{
			"alter table a engine=InnoDB",
			"ALTER TABLE `a` ENGINE InnoDB",
		},
		{
			"create table a (v varchar(32)) engine=InnoDB",
			"CREATE TABLE `a` (\n\t`v` varchar(32)\n) ENGINE InnoDB",
		},
		{
			"create table a (id int not null primary key) engine InnoDB, charset utf8mb4, collate utf8mb4_0900_ai_ci partition by range (`id`) (partition `p10` values less than(10) engine InnoDB tablespace foo)",
			"CREATE TABLE `a` (\n\t`id` int NOT NULL PRIMARY KEY\n) ENGINE InnoDB,\n  CHARSET utf8mb4,\n  COLLATE utf8mb4_0900_ai_ci\nPARTITION BY RANGE (`id`)\n(PARTITION `p10` VALUES LESS THAN (10) ENGINE InnoDB TABLESPACE foo)",
		},
		{
			"create table a (id int not null primary key) engine InnoDB, charset utf8mb4, collate utf8mb4_0900_ai_ci partition by range (`id`) (partition `p10` values less than maxvalue)",
			"CREATE TABLE `a` (\n\t`id` int NOT NULL PRIMARY KEY\n) ENGINE InnoDB,\n  CHARSET utf8mb4,\n  COLLATE utf8mb4_0900_ai_ci\nPARTITION BY RANGE (`id`)\n(PARTITION `p10` VALUES LESS THAN MAXVALUE)",
		},
		{
			"alter table a comment='a b c'",
			"ALTER TABLE `a` COMMENT 'a b c'",
		},
		{
			"alter table a add column c char not null default 'x'",
			"ALTER TABLE `a` ADD COLUMN `c` char NOT NULL DEFAULT 'x'",
		},
		{
			"alter table t2 modify column id bigint unsigned primary key",
			"ALTER TABLE `t2` MODIFY COLUMN `id` bigint unsigned PRIMARY KEY",
		},
		{
			"alter table t1 modify column a int first, modify column b int after a",
			"ALTER TABLE `t1` MODIFY COLUMN `a` int FIRST, MODIFY COLUMN `b` int AFTER `a`",
		},
		{
			"alter table t2 rename column foo to bar",
			"ALTER TABLE `t2` RENAME COLUMN `foo` TO `bar`",
		},
		{
			"alter table t1 drop key `PRIMARY`, add primary key (id,n)",
			"ALTER TABLE `t1` DROP KEY `PRIMARY`, ADD PRIMARY KEY (`id`, `n`)",
		},
		{
			"alter table t1 drop foreign key f",
			"ALTER TABLE `t1` DROP FOREIGN KEY `f`",
		},
		{
			"alter table t1 add constraint f foreign key (i) references parent (id) match simple on delete cascade on update set null",
			"ALTER TABLE `t1` ADD CONSTRAINT `f` FOREIGN KEY (`i`) REFERENCES `parent` (`id`) MATCH SIMPLE ON DELETE CASCADE ON UPDATE SET NULL",
		},
		{
			"alter table t1 remove partitioning",
			"ALTER TABLE `t1` REMOVE PARTITIONING",
		},
		{
			"alter table t1 partition by hash (id) partitions 5",
			"ALTER TABLE `t1` \nPARTITION BY HASH (`id`) PARTITIONS 5",
		},
		{
			"alter table t1 partition by list (id) (partition p1 values in (11, 21), partition p2 values in (12, 22))",
			"ALTER TABLE `t1` \nPARTITION BY LIST (`id`)\n(PARTITION `p1` VALUES IN (11, 21),\n PARTITION `p2` VALUES IN (12, 22))",
		},
		{
			"alter table t1 row_format=compressed, character set=utf8",
			"ALTER TABLE `t1` ROW_FORMAT COMPRESSED, CHARSET utf8",
		},
		{
			"create table a (id int primary key) row_format=compressed, character set=utf8mb4 collate=utf8mb4_0900_ai_ci",
			"CREATE TABLE `a` (\n\t`id` int PRIMARY KEY\n) ROW_FORMAT COMPRESSED,\n  CHARSET utf8mb4,\n  COLLATE utf8mb4_0900_ai_ci",
		},
		{
			"create table a (e enum('red','green','blue','orange','yellow'))",
			"CREATE TABLE `a` (\n\t`e` enum('red', 'green', 'blue', 'orange', 'yellow')\n)",
		},
		{
			"create table a (e set('red','green','blue','orange','yellow'))",
			"CREATE TABLE `a` (\n\t`e` set('red', 'green', 'blue', 'orange', 'yellow')\n)",
		},
		{
			"create table entries (uid varchar(53) not null, namespace varchar(254) not null, spec json default null, primary key (namespace, uid), key entries_spec_updatedAt ((json_value(spec, _utf8mb4 '$.updatedAt'))))",
			"CREATE TABLE `entries` (\n\t`uid` varchar(53) NOT NULL,\n\t`namespace` varchar(254) NOT NULL,\n\t`spec` json DEFAULT NULL,\n\tPRIMARY KEY (`namespace`, `uid`),\n\tKEY `entries_spec_updatedAt` ((JSON_VALUE(`spec`, _utf8mb4 '$.updatedAt')))\n)",
		},
		{
			"create table identifiers (id binary(16) not null default (uuid_to_bin(uuid(),true)))",
			"CREATE TABLE `identifiers` (\n\t`id` binary(16) NOT NULL DEFAULT (uuid_to_bin(uuid(), true))\n)",
		},
		{
			"create table t (\n\tid int auto_increment,\n\tusername varchar column_format dynamic\n)",
			"CREATE TABLE `t` (\n\t`id` int AUTO_INCREMENT,\n\t`username` varchar COLUMN_FORMAT DYNAMIC\n)",
		},
		{
			"create table t (\n\tid int auto_increment,\n\tusername varchar visible\n)",
			"CREATE TABLE `t` (\n\t`id` int AUTO_INCREMENT,\n\t`username` varchar VISIBLE\n)",
		},
		{
			"create table t (\n\tid int auto_increment,\n\tusername varchar engine_attribute '{}' secondary_engine_attribute '{}'\n)",
			"CREATE TABLE `t` (\n\t`id` int AUTO_INCREMENT,\n\t`username` varchar ENGINE_ATTRIBUTE '{}' SECONDARY_ENGINE_ATTRIBUTE '{}'\n)",
		},
		{
			"create table t (p point srid 0, g geometry not null srid 4326)",
			"CREATE TABLE `t` (\n\t`p` point SRID 0,\n\t`g` geometry NOT NULL SRID 4326\n)",
		},
		{
			"select regexp_replace('abc def ghi', '[a-z]+', 'X', 1, 3, 'c'), REGEXP_LIKE('dog cat dog', 'dog'), REGEXP_SUBSTR('abc def ghi', '[a-z]+', 1), REGEXP_INSTR('aa aaa aaaa aaaa aaaa aaaa', 'a{4}',1), 'Michael!' RLIKE '.*' from dual",
			"SELECT REGEXP_REPLACE('abc def ghi', '[a-z]+', 'X', 1, 3, 'c'), REGEXP_LIKE('dog cat dog', 'dog'), REGEXP_SUBSTR('abc def ghi', '[a-z]+', 1), REGEXP_INSTR('aa aaa aaaa aaaa aaaa aaaa', 'a{4}', 1), 'Michael!' REGEXP '.*' FROM `dual`",
		},
		{
			"select not regexp_replace('abc def ghi', '[a-z]+', 'X', 1, 3, 'c'), not regexp_like('dog cat dog', 'dog'), not regexp_substr('abc def ghi', '[a-z]+', 1), not regexp_instr('aa aaa aaaa aaaa aaaa aaaa', 'a{4}',1), 'Michael!' not rlike '.*' from dual",
			"SELECT NOT REGEXP_REPLACE('abc def ghi', '[a-z]+', 'X', 1, 3, 'c'), NOT REGEXP_LIKE('dog cat dog', 'dog'), NOT REGEXP_SUBSTR('abc def ghi', '[a-z]+', 1), NOT REGEXP_INSTR('aa aaa aaaa aaaa aaaa aaaa', 'a{4}', 1), 'Michael!' NOT REGEXP '.*' FROM `dual`",
		},
		{
			"revert /* vt+ foo */ vitess_migration '9aecb3b4_b8a9_11ec_929a_0a43f95f28a3'",
			"REVERT /* vt+ foo */ VITESS_MIGRATION '9aecb3b4_b8a9_11ec_929a_0a43f95f28a3'",
		},
		{
			"alter vitess_migration '9aecb3b4_b8a9_11ec_929a_0a43f95f28a3' throttle",
			"ALTER VITESS_MIGRATION '9aecb3b4_b8a9_11ec_929a_0a43f95f28a3' throttle",
		},
		{
			"select count(a) from t",
			"SELECT COUNT(`a`) FROM `t`",
		},
		{
			"select var_pop(a) from products",
			"SELECT VAR_POP(`a`) FROM `products`",
		},
		{
			"select /* function with distinct */ count(distinct a) from t",
			"SELECT /* function with distinct */ COUNT(DISTINCT `a`) FROM `t`",
		},
		{
			"select char(77, 121, 83, 81, '76' using utf8mb4) from dual",
			"SELECT CHAR(77, 121, 83, 81, '76' USING utf8mb4) FROM `dual`",
		},
		{
			"create table t1 (id int primary key, name tinytext not null, fulltext key name_ft(name) with parser ngram)",
			"CREATE TABLE `t1` (\n\t`id` int PRIMARY KEY,\n\t`name` tinytext NOT NULL,\n\tFULLTEXT KEY `name_ft` (`name`) WITH PARSER ngram\n)",
		},
		{
			"select convert('abc' using utf8mb4)",
			"SELECT CONVERT('abc' USING utf8mb4) FROM `dual`",
		},
		{
			"select point(4, 5)",
			"SELECT POINT(4, 5) FROM `dual`",
		},
		{
			"create table x(location geometry default (point(7.0, 3.0)))",
			"CREATE TABLE `x` (\n\t`location` geometry DEFAULT (POINT(7.0, 3.0))\n)",
		},
		{
			"create table x(location geometry default (linestring(point(7.0, 3.0), point(7.0, 3.0))))",
			"CREATE TABLE `x` (\n\t`location` geometry DEFAULT (LINESTRING(POINT(7.0, 3.0), POINT(7.0, 3.0)))\n)",
		},
		{
			"create table x(a geometry default (polygon(linestring(point(7.0, 3.0), point(7.0, 3.0)))))",
			"CREATE TABLE `x` (\n\t`a` geometry DEFAULT (POLYGON(LINESTRING(POINT(7.0, 3.0), POINT(7.0, 3.0))))\n)",
		},
		{
			"alter vschema create vindex lookup_vdx using lookup with owner=user, table=name_user_idx, from=name, to=user_id",
			"ALTER VSCHEMA CREATE VINDEX `lookup_vdx` USING `lookup` WITH owner=user, table=name_user_idx, from=name, to=user_id",
		},
		{
			"create table tb (id varbinary(100) default X'4d7953514c')",
			"CREATE TABLE `tb` (\n\t`id` varbinary(100) DEFAULT X'4d7953514c'\n)",
		},
		{
			"select grp, group_concat(c order by c asc separator 'foo') from t1 group by grp",
			"SELECT `grp`, GROUP_CONCAT(`c` ORDER BY `c` ASC SEPARATOR 'foo') FROM `t1` GROUP BY `grp`",
		},
		{
			"create table t (id int, info JSON, INDEX zips((CAST(info->'$.field' AS unsigned array))))",
			"CREATE TABLE `t` (\n\t`id` int,\n\t`info` JSON,\n\tINDEX `zips` ((CAST(`info` -> '$.field' AS unsigned array)))\n)",
		},
		{
			"select 1 from t1 into outfile 'test/t1.txt'",
			"SELECT 1 FROM `t1` INTO OUTFILE 'test/t1.txt'",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.input, func(t *testing.T) {
			tree, err := Parse(tc.input)
			require.NoError(t, err, tc.input)

			out := CanonicalString(tree)
			require.Equal(t, tc.canonical, out, "bad serialization")

			// Make sure we've generated a valid query!
			rereadStmt, err := Parse(out)
			require.NoError(t, err, out)
			out = CanonicalString(rereadStmt)
			require.Equal(t, tc.canonical, out, "bad serialization")
		})
	}
}
