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
			"CREATE ALGORITHM = MERGE SQL SECURITY DEFINER VIEW `a`(`b`, `c`, `d`) AS SELECT * FROM `e` WITH CASCADED CHECK OPTION",
		},
		{
			"create or replace algorithm = temptable definer = a@b.c.d sql security definer view a(b,c,d) as select * from e with local check option",
			"CREATE OR REPLACE ALGORITHM = TEMPTABLE DEFINER = a@`b.c.d` SQL SECURITY DEFINER VIEW `a`(`b`, `c`, `d`) AS SELECT * FROM `e` WITH LOCAL CHECK OPTION",
		},
		{
			"create table `a`(`id` int, primary key(`id`))",
			"CREATE TABLE `a` (\n\t`id` int,\n\tPRIMARY KEY (`id`)\n)",
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
			"CREATE TABLE `a` (\n\t`id` int NOT NULL PRIMARY KEY,\n\t`dt` datetime DEFAULT CURRENT_TIMESTAMP()\n)",
		},
		{
			"create table `insert`(`update` int, primary key(`delete`))",
			"CREATE TABLE `insert` (\n\t`update` int,\n\tPRIMARY KEY (`delete`)\n)",
		},
		{
			"alter table a engine=innodb",
			"ALTER TABLE `a` ENGINE INNODB",
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
			"ALTER TABLE `t2` MODIFY COLUMN `id` bigint UNSIGNED PRIMARY KEY",
		},
		{
			"alter table t1 modify column a int first, modify column b int after a",
			"ALTER TABLE `t1` MODIFY COLUMN `a` int FIRST, MODIFY COLUMN `b` int AFTER `a`",
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
			"alter table t1 add constraint f foreign key (i) references parent (id) on delete cascade on update set null",
			"ALTER TABLE `t1` ADD CONSTRAINT `f` FOREIGN KEY (`i`) REFERENCES `parent` (`id`) ON DELETE CASCADE ON UPDATE SET NULL",
		},
		{
			"alter table t1 remove partitioning",
			"ALTER TABLE `t1` REMOVE PARTITIONING",
		},
		{
			"alter table t1 partition by hash (id) partitions 5",
			"ALTER TABLE `t1` PARTITION BY HASH (`id`) PARTITIONS 5",
		},
		{
			"alter table t1 partition by list (id) (partition p1 values in (11, 21), partition p2 values in (12, 22))",
			"ALTER TABLE `t1` PARTITION BY LIST (`id`) (PARTITION `p1` VALUES IN (11, 21), PARTITION `p2` VALUES IN (12, 22))",
		},
		{
			"alter table t1 row_format=compressed, character set=utf8",
			"ALTER TABLE `t1` ROW_FORMAT COMPRESSED, CHARSET utf8",
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
