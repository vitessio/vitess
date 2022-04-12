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

func TestParseFlags(t *testing.T) {
	type flags struct {
		escape bool
		upcase bool
	}

	testcases := []struct {
		in     string
		output map[flags]string
	}{
		{
			"create table t(id int)",
			map[flags]string{
				{escape: true, upcase: true}:  "CREATE TABLE `t` (\n\t`id` int\n)",
				{escape: false, upcase: true}: "CREATE TABLE t (\n\tid int\n)",
				{escape: true, upcase: false}: "create table `t` (\n\t`id` int\n)",
			},
		},
		{
			"create algorithm = merge sql security definer view a (b,c,d) as select * from e with cascaded check option",
			map[flags]string{
				{escape: true, upcase: true}: "CREATE ALGORITHM = MERGE SQL SECURITY DEFINER VIEW `a`(`b`, `c`, `d`) AS SELECT * FROM `e` WITH CASCADED CHECK OPTION",
			},
		},
		{
			"create or replace algorithm = temptable definer = a@b.c.d sql security definer view a(b,c,d) as select * from e with local check option",
			map[flags]string{
				{escape: true, upcase: true}: "CREATE OR REPLACE ALGORITHM = TEMPTABLE DEFINER = a@`b.c.d` SQL SECURITY DEFINER VIEW `a`(`b`, `c`, `d`) AS SELECT * FROM `e` WITH LOCAL CHECK OPTION",
			},
		},
		{
			"create table `a`(`id` int, primary key(`id`))",
			map[flags]string{
				{escape: true, upcase: true}: "CREATE TABLE `a` (\n\t`id` int,\n\tPRIMARY KEY (`id`)\n)",
			},
		},
		{
			"create table `a`(`id` int primary key)",
			map[flags]string{
				{escape: true, upcase: true}: "CREATE TABLE `a` (\n\t`id` int PRIMARY KEY\n)",
			},
		},
		{
			"create table a (id int not null auto_increment, v varchar(32) default null, v2 varchar(62) charset utf8mb4 collate utf8mb4_0900_ai_ci, key v_idx(v(16)))",
			map[flags]string{
				{escape: true, upcase: true}: "CREATE TABLE `a` (\n\t`id` int NOT NULL AUTO_INCREMENT,\n\t`v` varchar(32) DEFAULT NULL,\n\t`v2` varchar(62) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,\n\tKEY `v_idx` (`v`(16))\n)",
			},
		},
		{
			"create table a (id int not null primary key, dt datetime default current_timestamp)",
			map[flags]string{
				{escape: true, upcase: true}: "CREATE TABLE `a` (\n\t`id` int NOT NULL PRIMARY KEY,\n\t`dt` datetime DEFAULT CURRENT_TIMESTAMP()\n)",
			},
		},
		{
			"create table `insert`(`update` int, primary key(`delete`))",
			map[flags]string{
				{escape: true, upcase: true}: "CREATE TABLE `insert` (\n\t`update` int,\n\tPRIMARY KEY (`delete`)\n)",
			},
		},
		{
			"alter table a engine=innodb",
			map[flags]string{
				{escape: true, upcase: true}: "ALTER TABLE `a` ENGINE INNODB",
			},
		},
		{
			"alter table a comment='a b c'",
			map[flags]string{
				{escape: true, upcase: true}: "ALTER TABLE `a` COMMENT 'a b c'",
			},
		},
		{
			"alter table a add column c char not null default 'x'",
			map[flags]string{
				{escape: true, upcase: true}: "ALTER TABLE `a` ADD COLUMN `c` char NOT NULL DEFAULT 'x'",
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.in, func(t *testing.T) {
			tree, err := Parse(tc.in)
			if err != nil {
				t.Fatalf("failed to parse %q: %v", tc.in, err)
			}
			require.NoError(t, err, tc.in)

			for flags, expected := range tc.output {
				buf := NewTrackedBuffer(nil)
				if flags.escape {
					buf.SetEscapeAllIdentifiers(true)
				}
				if flags.upcase {
					buf.SetUpperCase(true)
				}
				buf.Myprintf("%v", tree)

				out := buf.String()
				if out != expected {
					t.Errorf("bad serialization.\nwant: %s\n got: %s", expected, out)
				}
			}
		})
	}
}
