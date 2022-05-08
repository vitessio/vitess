/*
Copyright 2022 The Vitess Authors.

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

package schemadiff

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"
)

func TestCreateTableDiff(t *testing.T) {
	tt := []struct {
		name     string
		from     string
		to       string
		fromName string
		toName   string
		diff     string
		cdiff    string
		isError  bool
		errorMsg string
		autoinc  int
		rotation int
	}{
		{
			name: "identical",
			from: "create table t (id int primary key)",
			to:   "create table t (id int primary key)",
		},
		{
			name: "identical 2",
			from: "create table t (id int, primary key(id))",
			to:   "create table t (id int, primary key(id))",
		},
		{
			name: "identical, spacing",
			from: "create   table     t    (id int   primary  key)",
			to: `create table t (
						id int primary key
					)`,
		},
		{
			name: "identical, name change",
			from: "create table t1 (id int PRIMARY KEY)",
			to:   "create table t2 (id int primary key)",
		},
		{
			name: "identical, case change",
			from: "create table t (id int PRIMARY KEY)",
			to:   "create table t (id int primary key)",
		},
		{
			name: "identical, case change on target",
			from: "create table t (id int primary key)",
			to:   "create table t (id int PRIMARY KEY)",
		},
		{
			name: "identical, case and qualifiers",
			from: "CREATE table `t` (`id` int primary key)",
			to:   "create TABLE t (id int primary key)",
		},
		{
			name: "identical, case and qualifiers 2",
			from: "CREATE table t (`id` int primary key)",
			to:   "create TABLE `t` (id int primary key)",
		},
		{
			name: "identical, case and column qualifiers",
			from: "CREATE table t (`id` int primary key, i int not null default 0)",
			to:   "create TABLE t (id int primary key, `i` int not null default 0)",
		},
		{
			name:  "added column",
			from:  "create table t1 (`id` int primary key)",
			to:    "create table t2 (id int primary key, `i` int not null default 0)",
			diff:  "alter table t1 add column i int not null default 0",
			cdiff: "ALTER TABLE `t1` ADD COLUMN `i` int NOT NULL DEFAULT 0",
		},
		{
			name:     "dropped column",
			from:     "create table t1 (id int primary key, `i` int not null default 0)",
			to:       "create table t2 (`id` int primary key)",
			diff:     "alter table t1 drop column i",
			cdiff:    "ALTER TABLE `t1` DROP COLUMN `i`",
			fromName: "t1",
			toName:   "t2",
		},
		{
			name:  "modified column",
			from:  "create table t1 (id int primary key, `i` int not null default 0)",
			to:    "create table t2 (id int primary key, `i` bigint unsigned default null)",
			diff:  "alter table t1 modify column i bigint unsigned",
			cdiff: "ALTER TABLE `t1` MODIFY COLUMN `i` bigint unsigned",
		},
		{
			name:  "added column, dropped column, modified column",
			from:  "create table t1 (id int primary key, `i` int not null default 0, c char(3) default '')",
			to:    "create table t2 (id int primary key, ts timestamp null, `i` bigint unsigned default null)",
			diff:  "alter table t1 drop column c, modify column i bigint unsigned, add column ts timestamp null after id",
			cdiff: "ALTER TABLE `t1` DROP COLUMN `c`, MODIFY COLUMN `i` bigint unsigned, ADD COLUMN `ts` timestamp NULL AFTER `id`",
		},
		// columns, reordering
		{
			name:  "reorder column",
			from:  "create table t1 (id int primary key, a int, b int, c int, d int)",
			to:    "create table t2 (id int primary key, a int, c int, b int, d int)",
			diff:  "alter table t1 modify column c int after a",
			cdiff: "ALTER TABLE `t1` MODIFY COLUMN `c` int AFTER `a`",
		},
		{
			name:  "reorder column, far jump",
			from:  "create table t1 (id int primary key, a int, b int, c int, d int)",
			to:    "create table t2 (a int, b int, c int, d int, id int primary key)",
			diff:  "alter table t1 modify column id int primary key after d",
			cdiff: "ALTER TABLE `t1` MODIFY COLUMN `id` int PRIMARY KEY AFTER `d`",
		},
		{
			name:  "reorder column, far jump, another reorder",
			from:  "create table t1 (id int primary key, a int, b int, c int, d int)",
			to:    "create table t2 (a int, c int, b int, d int, id int primary key)",
			diff:  "alter table t1 modify column c int after a, modify column id int primary key after d",
			cdiff: "ALTER TABLE `t1` MODIFY COLUMN `c` int AFTER `a`, MODIFY COLUMN `id` int PRIMARY KEY AFTER `d`",
		},
		{
			name:  "reorder column, far jump, another reorder 2",
			from:  "create table t1 (id int primary key, a int, b int, c int, d int)",
			to:    "create table t2 (c int, a int, b int, d int, id int primary key)",
			diff:  "alter table t1 modify column c int first, modify column id int primary key after d",
			cdiff: "ALTER TABLE `t1` MODIFY COLUMN `c` int FIRST, MODIFY COLUMN `id` int PRIMARY KEY AFTER `d`",
		},
		{
			name:  "reorder column, far jump, another reorder 3",
			from:  "create table t1 (id int primary key, a int, b int, c int, d int, e int, f int)",
			to:    "create table t2 (a int, c int, b int, d int, id int primary key, e int, f int)",
			diff:  "alter table t1 modify column c int after a, modify column id int primary key after d",
			cdiff: "ALTER TABLE `t1` MODIFY COLUMN `c` int AFTER `a`, MODIFY COLUMN `id` int PRIMARY KEY AFTER `d`",
		},
		{
			name:  "reorder column, far jump, another reorder, removed columns",
			from:  "create table t1 (id int primary key, a int, b int, c int, d int, e int, f int, g int)",
			to:    "create table t2 (a int, c int, f int, e int, id int primary key, g int)",
			diff:  "alter table t1 drop column b, drop column d, modify column f int after c, modify column id int primary key after e",
			cdiff: "ALTER TABLE `t1` DROP COLUMN `b`, DROP COLUMN `d`, MODIFY COLUMN `f` int AFTER `c`, MODIFY COLUMN `id` int PRIMARY KEY AFTER `e`",
		},
		{
			name:  "two reorders",
			from:  "create table t1 (id int primary key, a int, b int, c int, d int, e int, f int)",
			to:    "create table t2 (id int primary key, b int, a int, c int, e int, d int, f int)",
			diff:  "alter table t1 modify column b int after id, modify column e int after c",
			cdiff: "ALTER TABLE `t1` MODIFY COLUMN `b` int AFTER `id`, MODIFY COLUMN `e` int AFTER `c`",
		},
		{
			name:  "two reorders, added and removed columns",
			from:  "create table t1 (id int primary key, a int, b int, c int, d int, e int, f int)",
			to:    "create table t2 (g int, id int primary key, h int, b int, a int, i int, e int, d int, j int, f int, k int)",
			diff:  "alter table t1 drop column c, modify column b int after id, modify column e int after a, add column g int first, add column h int after id, add column i int after a, add column j int after d, add column k int",
			cdiff: "ALTER TABLE `t1` DROP COLUMN `c`, MODIFY COLUMN `b` int AFTER `id`, MODIFY COLUMN `e` int AFTER `a`, ADD COLUMN `g` int FIRST, ADD COLUMN `h` int AFTER `id`, ADD COLUMN `i` int AFTER `a`, ADD COLUMN `j` int AFTER `d`, ADD COLUMN `k` int",
		},
		{
			name:  "reorder column and change data type",
			from:  "create table t1 (id int primary key, a int, b int, c int, d int)",
			to:    "create table t2 (id int primary key, a int, c bigint, b int, d int)",
			diff:  "alter table t1 modify column c bigint after a",
			cdiff: "ALTER TABLE `t1` MODIFY COLUMN `c` bigint AFTER `a`",
		},
		{
			name:  "reorder column, first",
			from:  "create table t1 (id int primary key, a int, b int, c int, d int)",
			to:    "create table t2 (c int, id int primary key, a int, b int, d int)",
			diff:  "alter table t1 modify column c int first",
			cdiff: "ALTER TABLE `t1` MODIFY COLUMN `c` int FIRST",
		},
		{
			name:  "add multiple columns",
			from:  "create table t1 (id int primary key, a int)",
			to:    "create table t2 (id int primary key, a int, b int, c int, d int)",
			diff:  "alter table t1 add column b int, add column c int, add column d int",
			cdiff: "ALTER TABLE `t1` ADD COLUMN `b` int, ADD COLUMN `c` int, ADD COLUMN `d` int",
		},
		{
			name:  "added column in middle",
			from:  "create table t1 (id int primary key, a int, b int, c int, d int)",
			to:    "create table t2 (id int primary key, a int, b int, x int, c int, d int)",
			diff:  "alter table t1 add column x int after b",
			cdiff: "ALTER TABLE `t1` ADD COLUMN `x` int AFTER `b`",
		},
		{
			name:  "added multiple column in middle",
			from:  "create table t1 (id int primary key, a int)",
			to:    "create table t2 (w int, x int, id int primary key, y int, a int, z int)",
			diff:  "alter table t1 add column w int first, add column x int after w, add column y int after id, add column z int",
			cdiff: "ALTER TABLE `t1` ADD COLUMN `w` int FIRST, ADD COLUMN `x` int AFTER `w`, ADD COLUMN `y` int AFTER `id`, ADD COLUMN `z` int",
		},
		{
			name:  "added column first, reorder column",
			from:  "create table t1 (id int primary key, a int)",
			to:    "create table t2 (x int, a int, id int primary key)",
			diff:  "alter table t1 modify column a int first, add column x int first",
			cdiff: "ALTER TABLE `t1` MODIFY COLUMN `a` int FIRST, ADD COLUMN `x` int FIRST",
		},
		{
			name:  "added column in middle, add column on end, reorder column",
			from:  "create table t1 (id int primary key, a int, b int, c int, d int)",
			to:    "create table t2 (id int primary key, a int, b int, x int, d int, c int, y int)",
			diff:  "alter table t1 modify column d int after b, add column x int after b, add column y int",
			cdiff: "ALTER TABLE `t1` MODIFY COLUMN `d` int AFTER `b`, ADD COLUMN `x` int AFTER `b`, ADD COLUMN `y` int",
		},
		{
			name:  "added column in middle, add column on end, reorder column 2",
			from:  "create table t1 (id int primary key, a int, b int, c int, d int)",
			to:    "create table t2 (id int primary key, a int, c int, x int, b int, d int, y int)",
			diff:  "alter table t1 modify column c int after a, add column x int after c, add column y int",
			cdiff: "ALTER TABLE `t1` MODIFY COLUMN `c` int AFTER `a`, ADD COLUMN `x` int AFTER `c`, ADD COLUMN `y` int",
		},
		// keys
		{
			name:  "added key",
			from:  "create table t1 (`id` int primary key, i int)",
			to:    "create table t2 (id int primary key, `i` int, key `i_idx` (i))",
			diff:  "alter table t1 add key i_idx (i)",
			cdiff: "ALTER TABLE `t1` ADD KEY `i_idx` (`i`)",
		},
		{
			name:  "added key without name",
			from:  "create table t1 (`id` int primary key, i int)",
			to:    "create table t2 (id int primary key, `i` int, key (i))",
			diff:  "alter table t1 add key i (i)",
			cdiff: "ALTER TABLE `t1` ADD KEY `i` (`i`)",
		},
		{
			name:  "added key without name, conflicting name",
			from:  "create table t1 (`id` int primary key, i int, key i(i))",
			to:    "create table t2 (id int primary key, `i` int, key i(i), key (i))",
			diff:  "alter table t1 add key i_2 (i)",
			cdiff: "ALTER TABLE `t1` ADD KEY `i_2` (`i`)",
		},
		{
			name:  "added key without name, conflicting name 2",
			from:  "create table t1 (`id` int primary key, i int, key i(i), key i_2(i))",
			to:    "create table t2 (id int primary key, `i` int, key i(i), key i_2(i), key (i))",
			diff:  "alter table t1 add key i_3 (i)",
			cdiff: "ALTER TABLE `t1` ADD KEY `i_3` (`i`)",
		},
		{
			name:  "added column and key",
			from:  "create table t1 (`id` int primary key)",
			to:    "create table t2 (id int primary key, `i` int, key `i_idx` (i))",
			diff:  "alter table t1 add column i int, add key i_idx (i)",
			cdiff: "ALTER TABLE `t1` ADD COLUMN `i` int, ADD KEY `i_idx` (`i`)",
		},
		{
			name:  "modify column primary key",
			from:  "create table t1 (`id` int)",
			to:    "create table t2 (id int primary key)",
			diff:  "alter table t1 modify column id int primary key",
			cdiff: "ALTER TABLE `t1` MODIFY COLUMN `id` int PRIMARY KEY",
		},
		{
			name:  "added primary key",
			from:  "create table t1 (`id` int)",
			to:    "create table t2 (id int, primary key(id))",
			diff:  "alter table t1 add primary key (id)",
			cdiff: "ALTER TABLE `t1` ADD PRIMARY KEY (`id`)",
		},
		{
			name:  "dropped primary key",
			from:  "create table t1 (id int, primary key(id))",
			to:    "create table t2 (id int)",
			diff:  "alter table t1 drop key `PRIMARY`",
			cdiff: "ALTER TABLE `t1` DROP KEY `PRIMARY`",
		},
		{
			name:  "dropped key",
			from:  "create table t1 (`id` int primary key, i int, key i_idx(i))",
			to:    "create table t2 (`id` int primary key, i int)",
			diff:  "alter table t1 drop key i_idx",
			cdiff: "ALTER TABLE `t1` DROP KEY `i_idx`",
		},
		{
			name:  "dropped key 2",
			from:  "create table t1 (`id` int, i int, primary key (id), key i_idx(i))",
			to:    "create table t1 (`id` int, i int, primary key (id))",
			diff:  "alter table t1 drop key i_idx",
			cdiff: "ALTER TABLE `t1` DROP KEY `i_idx`",
		},
		{
			name:  "modified key",
			from:  "create table t1 (`id` int primary key, i int, key i_idx(i))",
			to:    "create table t2 (`id` int primary key, i int, key i_idx(i, id))",
			diff:  "alter table t1 drop key i_idx, add key i_idx (i, id)",
			cdiff: "ALTER TABLE `t1` DROP KEY `i_idx`, ADD KEY `i_idx` (`i`, `id`)",
		},
		{
			name:  "modified primary key",
			from:  "create table t1 (`id` int, i int, primary key(id), key i_idx(i))",
			to:    "create table t2 (`id` int, i int, primary key(id, i),key i_idx(`i`))",
			diff:  "alter table t1 drop key `PRIMARY`, add primary key (id, i)",
			cdiff: "ALTER TABLE `t1` DROP KEY `PRIMARY`, ADD PRIMARY KEY (`id`, `i`)",
		},
		{
			name: "reordered key, no diff",
			from: "create table t1 (`id` int primary key, i int, key i_idx(i), key i2_idx(i, `id`))",
			to:   "create table t2 (`id` int primary key, i int, key i2_idx (`i`, id), key i_idx ( i ) )",
		},
		{
			name: "reordered key, no diff, 2",
			from: "create table t1 (`id` int, i int, primary key(id), key i_idx(i), key i2_idx(i, `id`))",
			to:   "create table t2 (`id` int, i int, key i2_idx (`i`, id), key i_idx ( i ), primary key(id) )",
		},
		{
			name: "reordered key, no diff 3",
			from: "CREATE TABLE `pets` (`id` int, `name` VARCHAR(255), `login` VARCHAR(255), PRIMARY KEY (`id`), KEY (`login`), KEY (`name`) )",
			to:   "CREATE TABLE `pets` (`id` int, `name` VARCHAR(255), `login` VARCHAR(255), PRIMARY KEY (`id`), KEY (`name`), KEY (`login`) )",
		},
		{
			name: "reordered key, no diff 4",
			from: "CREATE TABLE `pets` (`id` int, `name` VARCHAR(255), `login` VARCHAR(255), PRIMARY KEY (`id`), KEY login (login, name), KEY (`login`), KEY (`name`) )",
			to:   "CREATE TABLE `pets` (`id` int, `name` VARCHAR(255), `login` VARCHAR(255), PRIMARY KEY (`id`), KEY (`name`), KEY (`login`), KEY login (login, name) )",
		},
		{
			name:  "reordered key, add key",
			from:  "create table t1 (`id` int primary key, i int, key i_idx(i), key i2_idx(i, `id`))",
			to:    "create table t2 (`id` int primary key, i int, key i2_idx (`i`, id), key i_idx3(id), key i_idx ( i ) )",
			diff:  "alter table t1 add key i_idx3 (id)",
			cdiff: "ALTER TABLE `t1` ADD KEY `i_idx3` (`id`)",
		},
		// foreign keys
		{
			name:  "drop foreign key",
			from:  "create table t1 (id int primary key, i int, constraint f foreign key (i) references parent(id))",
			to:    "create table t2 (id int primary key, i int)",
			diff:  "alter table t1 drop foreign key f",
			cdiff: "ALTER TABLE `t1` DROP FOREIGN KEY `f`",
		},
		{
			name:  "add foreign key",
			from:  "create table t1 (id int primary key, i int)",
			to:    "create table t2 (id int primary key, i int, constraint f foreign key (i) references parent(id))",
			diff:  "alter table t1 add constraint f foreign key (i) references parent (id)",
			cdiff: "ALTER TABLE `t1` ADD CONSTRAINT `f` FOREIGN KEY (`i`) REFERENCES `parent` (`id`)",
		},
		{
			name: "identical foreign key",
			from: "create table t1 (id int primary key, i int, constraint f foreign key (i) references parent(id) on delete cascade)",
			to:   "create table t2 (id int primary key, i int, constraint f foreign key (i) references parent(id) on delete cascade)",
			diff: "",
		},
		{
			name:  "modify foreign key",
			from:  "create table t1 (id int primary key, i int, constraint f foreign key (i) references parent(id) on delete cascade)",
			to:    "create table t2 (id int primary key, i int, constraint f foreign key (i) references parent(id) on delete set null)",
			diff:  "alter table t1 drop foreign key f, add constraint f foreign key (i) references parent (id) on delete set null",
			cdiff: "ALTER TABLE `t1` DROP FOREIGN KEY `f`, ADD CONSTRAINT `f` FOREIGN KEY (`i`) REFERENCES `parent` (`id`) ON DELETE SET NULL",
		},
		{
			name:  "drop and add foreign key",
			from:  "create table t1 (id int primary key, i int, constraint f foreign key (i) references parent(id) on delete cascade)",
			to:    "create table t2 (id int primary key, i int, constraint f2 foreign key (i) references parent(id) on delete set null)",
			diff:  "alter table t1 drop foreign key f, add constraint f2 foreign key (i) references parent (id) on delete set null",
			cdiff: "ALTER TABLE `t1` DROP FOREIGN KEY `f`, ADD CONSTRAINT `f2` FOREIGN KEY (`i`) REFERENCES `parent` (`id`) ON DELETE SET NULL",
		},
		{
			name: "ignore different foreign key order",
			from: "create table t1 (id int primary key, i int, constraint f foreign key (i) references parent(id) on delete restrict, constraint f2 foreign key (i2) references parent2(id) on delete restrict)",
			to:   "create table t2 (id int primary key, i int, constraint f2 foreign key (i2) references parent2(id) on delete restrict, constraint f foreign key (i) references parent(id) on delete restrict)",
			diff: "",
		},
		// partitions
		{
			name:  "identical partitioning",
			from:  "create table t1 (id int primary key) partition by hash (id) partitions 4",
			to:    "create table t1 (id int primary key, a int) partition by hash (id) partitions 4",
			diff:  "alter table t1 add column a int",
			cdiff: "ALTER TABLE `t1` ADD COLUMN `a` int",
		},
		{
			name:  "remove partitioning",
			from:  "create table t1 (id int primary key) partition by hash (id) partitions 4",
			to:    "create table t1 (id int primary key, a int)",
			diff:  "alter table t1 add column a int remove partitioning",
			cdiff: "ALTER TABLE `t1` ADD COLUMN `a` int REMOVE PARTITIONING",
		},
		{
			name:  "remove partitioning 2",
			from:  "create table t1 (id int primary key) partition by hash (id) partitions 4",
			to:    "create table t1 (id int primary key)",
			diff:  "alter table t1 remove partitioning",
			cdiff: "ALTER TABLE `t1` REMOVE PARTITIONING",
		},
		{
			name:  "change partitioning hash",
			from:  "create table t1 (id int primary key) partition by hash (id) partitions 4",
			to:    "create table t1 (id int primary key) partition by hash (id) partitions 5",
			diff:  "alter table t1 partition by hash (id) partitions 5",
			cdiff: "ALTER TABLE `t1` PARTITION BY HASH (`id`) PARTITIONS 5",
		},
		{
			name:  "change partitioning key",
			from:  "create table t1 (id int primary key) partition by key (id) partitions 2",
			to:    "create table t1 (id int primary key) partition by hash (id) partitions 5",
			diff:  "alter table t1 partition by hash (id) partitions 5",
			cdiff: "ALTER TABLE `t1` PARTITION BY HASH (`id`) PARTITIONS 5",
		},
		{
			name:  "change partitioning list",
			from:  "create table t1 (id int primary key) partition by key (id) partitions 2",
			to:    "create table t1 (id int primary key) partition by list (id) (partition p1 values in(11,21), partition p2 values in (12,22))",
			diff:  "alter table t1 partition by list (id) (partition p1 values in (11, 21), partition p2 values in (12, 22))",
			cdiff: "ALTER TABLE `t1` PARTITION BY LIST (`id`) (PARTITION `p1` VALUES IN (11, 21), PARTITION `p2` VALUES IN (12, 22))",
		},
		{
			name:  "change partitioning range: rotate",
			from:  "create table t1 (id int primary key) partition by range (id) (partition p1 values less than (10), partition p2 values less than (20), partition p3 values less than (30))",
			to:    "create table t1 (id int primary key) partition by range (id) (partition p2 values less than (20), partition p3 values less than (30), partition p4 values less than (40))",
			diff:  "alter table t1 partition by range (id) (partition p2 values less than (20), partition p3 values less than (30), partition p4 values less than (40))",
			cdiff: "ALTER TABLE `t1` PARTITION BY RANGE (`id`) (PARTITION `p2` VALUES LESS THAN (20), PARTITION `p3` VALUES LESS THAN (30), PARTITION `p4` VALUES LESS THAN (40))",
		},
		{
			name:     "change partitioning range: ignore rotate",
			from:     "create table t1 (id int primary key) partition by range (id) (partition p1 values less than (10), partition p2 values less than (20), partition p3 values less than (30))",
			to:       "create table t1 (id int primary key) partition by range (id) (partition p2 values less than (20), partition p3 values less than (30), partition p4 values less than (40))",
			rotation: RangeRotationIgnore,
		},
		{
			name:     "change partitioning range: ignore rotate, not a rotation",
			from:     "create table t1 (id int primary key) partition by range (id) (partition p1 values less than (10), partition p2 values less than (20), partition p3 values less than (30))",
			to:       "create table t1 (id int primary key) partition by range (id) (partition p2 values less than (25), partition p3 values less than (30), partition p4 values less than (40))",
			rotation: RangeRotationIgnore,
			diff:     "alter table t1 partition by range (id) (partition p2 values less than (25), partition p3 values less than (30), partition p4 values less than (40))",
			cdiff:    "ALTER TABLE `t1` PARTITION BY RANGE (`id`) (PARTITION `p2` VALUES LESS THAN (25), PARTITION `p3` VALUES LESS THAN (30), PARTITION `p4` VALUES LESS THAN (40))",
		},
		{
			name:     "change partitioning range: ignore rotate, not a rotation 2",
			from:     "create table t1 (id int primary key) partition by range (id) (partition p1 values less than (10), partition p2 values less than (20), partition p3 values less than (30))",
			to:       "create table t1 (id int primary key) partition by range (id) (partition p2 values less than (20), partition p3 values less than (35), partition p4 values less than (40))",
			rotation: RangeRotationIgnore,
			diff:     "alter table t1 partition by range (id) (partition p2 values less than (20), partition p3 values less than (35), partition p4 values less than (40))",
			cdiff:    "ALTER TABLE `t1` PARTITION BY RANGE (`id`) (PARTITION `p2` VALUES LESS THAN (20), PARTITION `p3` VALUES LESS THAN (35), PARTITION `p4` VALUES LESS THAN (40))",
		},
		{
			name:     "change partitioning range: ignore rotate, not a rotation 3",
			from:     "create table t1 (id int primary key) partition by range (id) (partition p1 values less than (10), partition p2 values less than (20), partition p3 values less than (30))",
			to:       "create table t1 (id int primary key) partition by range (id) (partition p2 values less than (20), partition pX values less than (30), partition p4 values less than (40))",
			rotation: RangeRotationIgnore,
			diff:     "alter table t1 partition by range (id) (partition p2 values less than (20), partition pX values less than (30), partition p4 values less than (40))",
			cdiff:    "ALTER TABLE `t1` PARTITION BY RANGE (`id`) (PARTITION `p2` VALUES LESS THAN (20), PARTITION `pX` VALUES LESS THAN (30), PARTITION `p4` VALUES LESS THAN (40))",
		},
		{
			name:     "change partitioning range: ignore rotate, not a rotation 4",
			from:     "create table t1 (id int primary key) partition by range (id) (partition p1 values less than (10), partition p2 values less than (20), partition p3 values less than (30))",
			to:       "create table t1 (id int primary key) partition by range (id) (partition pX values less than (20), partition p3 values less than (30), partition p4 values less than (40))",
			rotation: RangeRotationIgnore,
			diff:     "alter table t1 partition by range (id) (partition pX values less than (20), partition p3 values less than (30), partition p4 values less than (40))",
			cdiff:    "ALTER TABLE `t1` PARTITION BY RANGE (`id`) (PARTITION `pX` VALUES LESS THAN (20), PARTITION `p3` VALUES LESS THAN (30), PARTITION `p4` VALUES LESS THAN (40))",
		},
		{
			name:     "change partitioning range: ignore rotate, nothing shared",
			from:     "create table t1 (id int primary key) partition by range (id) (partition p1 values less than (10), partition p2 values less than (20), partition p3 values less than (30))",
			to:       "create table t1 (id int primary key) partition by range (id) (partition p4 values less than (40), partition p5 values less than (50), partition p6 values less than (60))",
			rotation: RangeRotationIgnore,
			diff:     "alter table t1 partition by range (id) (partition p4 values less than (40), partition p5 values less than (50), partition p6 values less than (60))",
			cdiff:    "ALTER TABLE `t1` PARTITION BY RANGE (`id`) (PARTITION `p4` VALUES LESS THAN (40), PARTITION `p5` VALUES LESS THAN (50), PARTITION `p6` VALUES LESS THAN (60))",
		},
		{
			name:     "change partitioning range: ignore rotate, no names shared, definitions shared",
			from:     "create table t1 (id int primary key) partition by range (id) (partition p1 values less than (10), partition p2 values less than (20), partition p3 values less than (30))",
			to:       "create table t1 (id int primary key) partition by range (id) (partition pA values less than (20), partition pB values less than (30), partition pC values less than (40))",
			rotation: RangeRotationIgnore,
			diff:     "alter table t1 partition by range (id) (partition pA values less than (20), partition pB values less than (30), partition pC values less than (40))",
			cdiff:    "ALTER TABLE `t1` PARTITION BY RANGE (`id`) (PARTITION `pA` VALUES LESS THAN (20), PARTITION `pB` VALUES LESS THAN (30), PARTITION `pC` VALUES LESS THAN (40))",
		},

		//
		// table options
		{
			name: "same options, no diff 1",
			from: "create table t1 (id int primary key) row_format=compressed",
			to:   "create table t1 (id int primary key) row_format=compressed",
		},
		{
			name: "same options, no diff 2",
			from: "create table t1 (id int primary key) row_format=compressed, character set=utf8",
			to:   "create table t1 (id int primary key) row_format=compressed, character set=utf8",
		},
		{
			name: "same options, no diff 3",
			from: "create table t1 (id int primary key) row_format=compressed, character set=utf8",
			to:   "create table t1 (id int primary key) row_format=compressed, charset=utf8",
		},
		{
			name: "reordered options, no diff",
			from: "create table t1 (id int primary key) row_format=compressed character set=utf8",
			to:   "create table t1 (id int primary key) character set=utf8, row_format=compressed",
		},
		{
			name:  "add table option 1",
			from:  "create table t1 (id int primary key)",
			to:    "create table t1 (id int primary key) row_format=compressed",
			diff:  "alter table t1 row_format COMPRESSED",
			cdiff: "ALTER TABLE `t1` ROW_FORMAT COMPRESSED",
		},
		{
			name:  "add table option 2",
			from:  "create table t1 (id int primary key) character set=utf8",
			to:    "create table t1 (id int primary key) character set=utf8, row_format=compressed",
			diff:  "alter table t1 row_format COMPRESSED",
			cdiff: "ALTER TABLE `t1` ROW_FORMAT COMPRESSED",
		},
		{
			name:  "add table option 3",
			from:  "create table t1 (id int primary key) character set=utf8",
			to:    "create table t1 (id int primary key) row_format=compressed, character set=utf8",
			diff:  "alter table t1 row_format COMPRESSED",
			cdiff: "ALTER TABLE `t1` ROW_FORMAT COMPRESSED",
		},
		{
			name:  "add table option 3",
			from:  "create table t1 (id int primary key) character set=utf8",
			to:    "create table t1 (id int primary key) row_format=compressed, character set=utf8, checksum=1",
			diff:  "alter table t1 row_format COMPRESSED checksum 1",
			cdiff: "ALTER TABLE `t1` ROW_FORMAT COMPRESSED CHECKSUM 1",
		},
		{
			name:  "modify table option 1",
			from:  "create table t1 (id int primary key) character set=utf8",
			to:    "create table t1 (id int primary key) character set=utf8mb4",
			diff:  "alter table t1 charset utf8mb4",
			cdiff: "ALTER TABLE `t1` CHARSET utf8mb4",
		},
		{
			name:  "modify table option 2",
			from:  "create table t1 (id int primary key) charset=utf8",
			to:    "create table t1 (id int primary key) character set=utf8mb4",
			diff:  "alter table t1 charset utf8mb4",
			cdiff: "ALTER TABLE `t1` CHARSET utf8mb4",
		},
		{
			name:  "modify table option 3",
			from:  "create table t1 (id int primary key) character set=utf8",
			to:    "create table t1 (id int primary key) charset=utf8mb4",
			diff:  "alter table t1 charset utf8mb4",
			cdiff: "ALTER TABLE `t1` CHARSET utf8mb4",
		},
		{
			name:  "modify table option 4",
			from:  "create table t1 (id int primary key) character set=utf8",
			to:    "create table t1 (id int primary key) row_format=compressed, character set=utf8mb4, checksum=1",
			diff:  "alter table t1 charset utf8mb4 row_format COMPRESSED checksum 1",
			cdiff: "ALTER TABLE `t1` CHARSET utf8mb4 ROW_FORMAT COMPRESSED CHECKSUM 1",
		},
		{
			name:  "remove table option 1",
			from:  "create table t1 (id int primary key) row_format=compressed",
			to:    "create table t1 (id int primary key) ",
			diff:  "alter table t1 row_format DEFAULT",
			cdiff: "ALTER TABLE `t1` ROW_FORMAT DEFAULT",
		},
		{
			name:  "remove table option 2",
			from:  "create table t1 (id int primary key) CHECKSUM=1",
			to:    "create table t1 (id int primary key) ",
			diff:  "alter table t1 CHECKSUM 0",
			cdiff: "ALTER TABLE `t1` CHECKSUM 0",
		},
		{
			name:  "remove table option 3",
			from:  "create table t1 (id int primary key) checksum=1",
			to:    "create table t1 (id int primary key) ",
			diff:  "alter table t1 checksum 0",
			cdiff: "ALTER TABLE `t1` CHECKSUM 0",
		},
		{
			name:  "remove table option 4",
			from:  "create table t1 (id int auto_increment primary key) KEY_BLOCK_SIZE=16 COMPRESSION='zlib'",
			to:    "create table t2 (id int auto_increment primary key)",
			diff:  "alter table t1 KEY_BLOCK_SIZE 0 COMPRESSION ''",
			cdiff: "ALTER TABLE `t1` KEY_BLOCK_SIZE 0 COMPRESSION ''",
		},
		{
			name:  "add, modify and remove table option",
			from:  "create table t1 (id int primary key) engine=innodb, charset=utf8, checksum=1",
			to:    "create table t1 (id int primary key) row_format=compressed, engine=innodb, charset=utf8mb4",
			diff:  "alter table t1 checksum 0 charset utf8mb4 row_format COMPRESSED",
			cdiff: "ALTER TABLE `t1` CHECKSUM 0 CHARSET utf8mb4 ROW_FORMAT COMPRESSED",
		},
		{
			name: "ignore AUTO_INCREMENT addition",
			from: "create table t1 (id int auto_increment primary key)",
			to:   "create table t2 (id int auto_increment primary key) AUTO_INCREMENT=300",
		},
		{
			name:    "apply AUTO_INCREMENT addition",
			from:    "create table t1 (id int auto_increment primary key)",
			to:      "create table t2 (id int auto_increment primary key) AUTO_INCREMENT=300",
			autoinc: AutoIncrementApplyHigher,
			diff:    "alter table t1 AUTO_INCREMENT 300",
			cdiff:   "ALTER TABLE `t1` AUTO_INCREMENT 300",
		},
		{
			name: "ignore AUTO_INCREMENT removal",
			from: "create table t1 (id int auto_increment primary key) AUTO_INCREMENT=300",
			to:   "create table t2 (id int auto_increment primary key)",
		},
		{
			name:    "ignore AUTO_INCREMENT removal 2",
			from:    "create table t1 (id int auto_increment primary key) AUTO_INCREMENT=300",
			to:      "create table t2 (id int auto_increment primary key)",
			autoinc: AutoIncrementApplyHigher,
		},
		{
			name: "ignore AUTO_INCREMENT change",
			from: "create table t1 (id int auto_increment primary key) AUTO_INCREMENT=100",
			to:   "create table t2 (id int auto_increment primary key) AUTO_INCREMENT=300",
		},
		{
			name:    "apply AUTO_INCREMENT change",
			from:    "create table t1 (id int auto_increment primary key) AUTO_INCREMENT=100",
			to:      "create table t2 (id int auto_increment primary key) AUTO_INCREMENT=300",
			autoinc: AutoIncrementApplyHigher,
			diff:    "alter table t1 AUTO_INCREMENT 300",
			cdiff:   "ALTER TABLE `t1` AUTO_INCREMENT 300",
		},
		{
			name:    "ignore AUTO_INCREMENT decrease",
			from:    "create table t1 (id int auto_increment primary key) AUTO_INCREMENT=300",
			to:      "create table t2 (id int auto_increment primary key) AUTO_INCREMENT=100",
			autoinc: AutoIncrementApplyHigher,
		},
		{
			name:    "apply AUTO_INCREMENT decrease",
			from:    "create table t1 (id int auto_increment primary key) AUTO_INCREMENT=300",
			to:      "create table t2 (id int auto_increment primary key) AUTO_INCREMENT=100",
			autoinc: AutoIncrementApplyAlways,
			diff:    "alter table t1 AUTO_INCREMENT 100",
			cdiff:   "ALTER TABLE `t1` AUTO_INCREMENT 100",
		},
		{
			name:  `change table charset`,
			from:  "create table t (id int primary key, t1 varchar(128) default null, t2 varchar(128) not null, t3 tinytext charset latin1, t4 tinytext charset latin1) default charset=utf8",
			to:    "create table t (id int primary key, t1 varchar(128) not null, t2 varchar(128) not null, t3 tinytext, t4 tinytext charset latin1) default charset=utf8mb4",
			diff:  "alter table t modify column t1 varchar(128) not null, modify column t2 varchar(128) not null, modify column t3 tinytext, charset utf8mb4",
			cdiff: "ALTER TABLE `t` MODIFY COLUMN `t1` varchar(128) NOT NULL, MODIFY COLUMN `t2` varchar(128) NOT NULL, MODIFY COLUMN `t3` tinytext, CHARSET utf8mb4",
		},
		{
			name:  "normalized unsigned attribute",
			from:  "create table t1 (id int primary key)",
			to:    "create table t1 (id int unsigned primary key)",
			diff:  "alter table t1 modify column id int unsigned primary key",
			cdiff: "ALTER TABLE `t1` MODIFY COLUMN `id` int unsigned PRIMARY KEY",
		},
		{
			name:  "normalized ENGINE InnoDB value",
			from:  "create table t1 (id int primary key) character set=utf8",
			to:    "create table t1 (id int primary key) engine=innodb, character set=utf8",
			diff:  "alter table t1 engine InnoDB",
			cdiff: "ALTER TABLE `t1` ENGINE InnoDB",
		},
		{
			name:  "normalized ENGINE MyISAM value",
			from:  "create table t1 (id int primary key) character set=utf8",
			to:    "create table t1 (id int primary key) engine=myisam, character set=utf8",
			diff:  "alter table t1 engine MyISAM",
			cdiff: "ALTER TABLE `t1` ENGINE MyISAM",
		},
		{
			name:  "normalized ENGINE MEMORY value",
			from:  "create table t1 (id int primary key) character set=utf8",
			to:    "create table t1 (id int primary key) engine=memory, character set=utf8",
			diff:  "alter table t1 engine MEMORY",
			cdiff: "ALTER TABLE `t1` ENGINE MEMORY",
		},
		{
			name:  "normalized CHARSET value",
			from:  "create table t1 (id int primary key) engine=innodb",
			to:    "create table t1 (id int primary key) engine=innodb, character set=UTF8MB4",
			diff:  "alter table t1 charset utf8mb4",
			cdiff: "ALTER TABLE `t1` CHARSET utf8mb4",
		},
		{
			name:  "normalized CHARSET utf8 value",
			from:  "create table t1 (id int primary key) engine=innodb",
			to:    "create table t1 (id int primary key) engine=innodb, character set=UTF8",
			diff:  "alter table t1 charset utf8mb3",
			cdiff: "ALTER TABLE `t1` CHARSET utf8mb3",
		},
		{
			name:  "normalized COLLATE value",
			from:  "create table t1 (id int primary key) engine=innodb",
			to:    "create table t1 (id int primary key) engine=innodb, collate=UTF8_BIN",
			diff:  "alter table t1 collate utf8_bin",
			cdiff: "ALTER TABLE `t1` COLLATE utf8_bin",
		},
	}
	standardHints := DiffHints{}
	for _, ts := range tt {
		t.Run(ts.name, func(t *testing.T) {
			fromStmt, err := sqlparser.Parse(ts.from)
			require.NoError(t, err)
			fromCreateTable, ok := fromStmt.(*sqlparser.CreateTable)
			require.True(t, ok)

			toStmt, err := sqlparser.Parse(ts.to)
			require.NoError(t, err)
			toCreateTable, ok := toStmt.(*sqlparser.CreateTable)
			require.True(t, ok)

			c := NewCreateTableEntity(fromCreateTable)
			other := NewCreateTableEntity(toCreateTable)
			hints := standardHints
			hints.AutoIncrementStrategy = ts.autoinc
			hints.RangeRotationStrategy = ts.rotation
			alter, err := c.Diff(other, &hints)
			switch {
			case ts.isError:
				require.Error(t, err)
				if ts.errorMsg != "" {
					assert.Contains(t, err.Error(), ts.errorMsg)
				}
			case ts.diff == "":
				assert.NoError(t, err)
				assert.True(t, alter.IsEmpty(), "expected empty diff, found changes")
				if !alter.IsEmpty() {
					t.Logf("statements[0]: %v", alter.StatementString())
				}
			default:
				assert.NoError(t, err)
				require.NotNil(t, alter)
				assert.False(t, alter.IsEmpty(), "expected changes, found empty diff")

				{
					diff := alter.StatementString()
					assert.Equal(t, ts.diff, diff)
					// validate we can parse back the statement
					_, err := sqlparser.Parse(diff)
					assert.NoError(t, err)

					// Validate "from/to" entities
					eFrom, eTo := alter.Entities()
					if ts.fromName != "" {
						assert.Equal(t, ts.fromName, eFrom.Name())
					}
					if ts.toName != "" {
						assert.Equal(t, ts.toName, eTo.Name())
					}

					{ // Validate "apply()" on "from" converges with "to"
						applied, err := c.Apply(alter)
						assert.NoError(t, err)
						require.NotNil(t, applied)
						appliedDiff, err := eTo.Diff(applied, &hints)
						require.NoError(t, err)
						assert.True(t, appliedDiff.IsEmpty(), "expected empty diff, found changes: %v.\nc=%v\n,alter=%v\n,eTo=%v\napplied=%v\n",
							appliedDiff.CanonicalStatementString(),
							c.Create().CanonicalStatementString(),
							alter.CanonicalStatementString(),
							eTo.Create().CanonicalStatementString(),
							applied.Create().CanonicalStatementString(),
						)
					}
				}
				{
					cdiff := alter.CanonicalStatementString()
					assert.Equal(t, ts.cdiff, cdiff)
					_, err := sqlparser.Parse(cdiff)
					assert.NoError(t, err)
				}

			}
		})
	}
}

func TestValidate(t *testing.T) {
	tt := []struct {
		name      string
		from      string
		to        string
		alter     string
		expectErr error
	}{
		{
			name:  "add column",
			from:  "create table t (id int primary key)",
			alter: "alter table t add column i int",
			to:    "create table t (id int primary key, i int)",
		},
		{
			name:  "add key",
			from:  "create table t (id int primary key, i int)",
			alter: "alter table t add key i_idx(i)",
			to:    "create table t (id int primary key, i int, key i_idx(i))",
		},
		{
			name:  "add column and key",
			from:  "create table t (id int primary key)",
			alter: "alter table t add column i int, add key i_idx(i)",
			to:    "create table t (id int primary key, i int, key i_idx(i))",
		},
		{
			name:      "add key, missing column",
			from:      "create table t (id int primary key, i int)",
			alter:     "alter table t add key j_idx(j)",
			expectErr: ErrInvalidColumnInKey,
		},
		{
			name:      "add key, missing column 2",
			from:      "create table t (id int primary key, i int)",
			alter:     "alter table t add key j_idx(j, i)",
			expectErr: ErrInvalidColumnInKey,
		},
		{
			name:  "drop column, ok",
			from:  "create table t (id int primary key, i int, i2 int, key i_idx(i))",
			alter: "alter table t drop column i2",
			to:    "create table t (id int primary key, i int, key i_idx(i))",
		},
		{
			name:  "drop column, affect keys",
			from:  "create table t (id int primary key, i int, key i_idx(i))",
			alter: "alter table t drop column i",
			to:    "create table t (id int primary key)",
		},
		{
			name:  "drop column, affect keys 2",
			from:  "create table t (id int primary key, i int, i2 int, key i_idx(i, i2))",
			alter: "alter table t drop column i",
			to:    "create table t (id int primary key, i2 int, key i_idx(i2))",
		},
		{
			name:  "drop column, affect keys 3",
			from:  "create table t (id int primary key, i int, i2 int, key i_idx(i, i2))",
			alter: "alter table t drop column i2",
			to:    "create table t (id int primary key, i int, key i_idx(i))",
		},
		{
			name:  "drop column, affect keys 4",
			from:  "create table t (id int primary key, i int, i2 int, key some_key(id, i), key i_idx(i, i2))",
			alter: "alter table t drop column i2",
			to:    "create table t (id int primary key, i int, key some_key(id, i), key i_idx(i))",
		},
		{
			name:  "add multiple keys, multi columns, ok",
			from:  "create table t (id int primary key, i1 int, i2 int, i3 int)",
			alter: "alter table t add key i12_idx(i1, i2), add key i32_idx(i3, i2), add key i21_idx(i2, i1)",
			to:    "create table t (id int primary key, i1 int, i2 int, i3 int, key i12_idx(i1, i2), key i32_idx(i3, i2), key i21_idx(i2, i1))",
		},
		{
			name:      "add multiple keys, multi columns, missing column",
			from:      "create table t (id int primary key, i1 int, i2 int, i4 int)",
			alter:     "alter table t add key i12_idx(i1, i2), add key i32_idx(i3, i2), add key i21_idx(i2, i1)",
			expectErr: ErrInvalidColumnInKey,
		},
		{
			name:  "nullable timestamp",
			from:  "create table t (id int primary key, t datetime)",
			alter: "alter table t modify column t timestamp null",
			to:    "create table t (id int primary key, t timestamp null)",
		},
	}
	hints := DiffHints{}
	for _, ts := range tt {
		t.Run(ts.name, func(t *testing.T) {
			stmt, err := sqlparser.Parse(ts.from)
			require.NoError(t, err)
			fromCreateTable, ok := stmt.(*sqlparser.CreateTable)
			require.True(t, ok)

			stmt, err = sqlparser.Parse(ts.alter)
			require.NoError(t, err)
			alterTable, ok := stmt.(*sqlparser.AlterTable)
			require.True(t, ok)

			from := NewCreateTableEntity(fromCreateTable)
			a := &AlterTableEntityDiff{from: from, alterTable: alterTable}
			applied, err := from.Apply(a)
			if ts.expectErr != nil {
				assert.Error(t, err)
				assert.True(t, errors.Is(err, ts.expectErr))
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, applied)

				c, ok := applied.(*CreateTableEntity)
				require.True(t, ok)
				applied = c.normalize()

				stmt, err := sqlparser.Parse(ts.to)
				require.NoError(t, err)
				toCreateTable, ok := stmt.(*sqlparser.CreateTable)
				require.True(t, ok)

				to := NewCreateTableEntity(toCreateTable)
				diff, err := applied.Diff(to, &hints)
				require.NoError(t, err)
				assert.Empty(t, diff, "diff found: %v.\napplied: %v\nto: %v", diff.CanonicalStatementString(), applied.Create().CanonicalStatementString(), to.Create().CanonicalStatementString())
			}
		})
	}
}

func TestNormalize(t *testing.T) {
	tt := []struct {
		name string
		from string
		to   string
	}{
		{
			name: "basic table",
			from: "create table t (id int primary key, i int)",
			to:   "CREATE TABLE `t` (\n\t`id` int PRIMARY KEY,\n\t`i` int\n)",
		},
		{
			name: "removes default null",
			from: "create table t (id int primary key, i int default null)",
			to:   "CREATE TABLE `t` (\n\t`id` int PRIMARY KEY,\n\t`i` int\n)",
		},
		{
			name: "keeps not exist",
			from: "create table if not exists t (id int primary key, i int)",
			to:   "CREATE TABLE IF NOT EXISTS `t` (\n\t`id` int PRIMARY KEY,\n\t`i` int\n)",
		},

		{
			name: "timestamp null",
			from: "create table t (id int primary key, t timestamp null)",
			to:   "CREATE TABLE `t` (\n\t`id` int PRIMARY KEY,\n\t`t` timestamp NULL\n)",
		},
		{
			name: "timestamp default null",
			from: "create table t (id int primary key, t timestamp default null)",
			to:   "CREATE TABLE `t` (\n\t`id` int PRIMARY KEY,\n\t`t` timestamp NULL\n)",
		},
		{
			name: "uses lowercase type",
			from: "create table t (id INT primary key, i INT default null)",
			to:   "CREATE TABLE `t` (\n\t`id` int PRIMARY KEY,\n\t`i` int\n)",
		},
		{
			name: "removes default signed",
			from: "create table t (id int signed primary key, i int signed)",
			to:   "CREATE TABLE `t` (\n\t`id` int PRIMARY KEY,\n\t`i` int\n)",
		},
		{
			name: "removes int sizes",
			from: "create table t (id int primary key, i int(11) default null)",
			to:   "CREATE TABLE `t` (\n\t`id` int PRIMARY KEY,\n\t`i` int\n)",
		},
		{
			name: "removes zerofill and maps to unsigned",
			from: "create table t (id int primary key, i int zerofill default null)",
			to:   "CREATE TABLE `t` (\n\t`id` int PRIMARY KEY,\n\t`i` int unsigned\n)",
		},
		{
			name: "removes int sizes case insensitive",
			from: "create table t (id int primary key, i INT(11) default null)",
			to:   "CREATE TABLE `t` (\n\t`id` int PRIMARY KEY,\n\t`i` int\n)",
		},
		{
			name: "removes matching charset",
			from: "create table t (id int signed primary key, v varchar(255) charset utf8mb4) charset utf8mb4",
			to:   "CREATE TABLE `t` (\n\t`id` int PRIMARY KEY,\n\t`v` varchar(255)\n) CHARSET utf8mb4",
		},
		{
			name: "removes matching case insensitive charset",
			from: "create table t (id int signed primary key, v varchar(255) charset UTF8MB4) charset utf8mb4",
			to:   "CREATE TABLE `t` (\n\t`id` int PRIMARY KEY,\n\t`v` varchar(255)\n) CHARSET utf8mb4",
		},
		{
			name: "removes matching collation if default",
			from: "create table t (id int signed primary key, v varchar(255) collate utf8mb4_0900_ai_ci) collate utf8mb4_0900_ai_ci",
			to:   "CREATE TABLE `t` (\n\t`id` int PRIMARY KEY,\n\t`v` varchar(255)\n) COLLATE utf8mb4_0900_ai_ci",
		},
		{
			name: "removes matching collation case insensitive if default",
			from: "create table t (id int signed primary key, v varchar(255) collate UTF8MB4_0900_AI_CI) collate utf8mb4_0900_ai_ci",
			to:   "CREATE TABLE `t` (\n\t`id` int PRIMARY KEY,\n\t`v` varchar(255)\n) COLLATE utf8mb4_0900_ai_ci",
		},
		{
			name: "removes matching charset & collation if default",
			from: "create table t (id int signed primary key, v varchar(255) charset utf8mb4 collate utf8mb4_0900_ai_ci) charset utf8mb4 collate utf8mb4_0900_ai_ci",
			to:   "CREATE TABLE `t` (\n\t`id` int PRIMARY KEY,\n\t`v` varchar(255)\n) CHARSET utf8mb4,\n  COLLATE utf8mb4_0900_ai_ci",
		},
		{
			name: "sets collation for non default collation at table level",
			from: "create table t (id int signed primary key, v varchar(255) charset utf8mb4) charset utf8mb4 collate utf8mb4_0900_bin",
			to:   "CREATE TABLE `t` (\n\t`id` int PRIMARY KEY,\n\t`v` varchar(255) COLLATE utf8mb4_0900_ai_ci\n) CHARSET utf8mb4,\n  COLLATE utf8mb4_0900_bin",
		},
		{
			name: "does not add collation for a non default collation at table level",
			from: "create table t (id int signed primary key, v varchar(255)) charset utf8mb4 collate utf8mb4_0900_bin",
			to:   "CREATE TABLE `t` (\n\t`id` int PRIMARY KEY,\n\t`v` varchar(255)\n) CHARSET utf8mb4,\n  COLLATE utf8mb4_0900_bin",
		},
		{
			name: "cleans up collation at the column level if it matches the tabel level and both are given",
			from: "create table t (id int signed primary key, v varchar(255) collate utf8mb4_0900_bin) charset utf8mb4 collate utf8mb4_0900_bin",
			to:   "CREATE TABLE `t` (\n\t`id` int PRIMARY KEY,\n\t`v` varchar(255)\n) CHARSET utf8mb4,\n  COLLATE utf8mb4_0900_bin",
		},
		{
			name: "cleans up charset and collation at the column level if it matches the tabel level and both are given",
			from: "create table t (id int signed primary key, v varchar(255) charset utf8mb4 collate utf8mb4_0900_bin) charset utf8mb4 collate utf8mb4_0900_bin",
			to:   "CREATE TABLE `t` (\n\t`id` int PRIMARY KEY,\n\t`v` varchar(255)\n) CHARSET utf8mb4,\n  COLLATE utf8mb4_0900_bin",
		},
		{
			name: "keeps existing collation even if default for non default collation at table level",
			from: "create table t (id int signed primary key, v varchar(255) charset utf8mb4 collate utf8mb4_0900_ai_ci) charset utf8mb4 collate utf8mb4_0900_bin",
			to:   "CREATE TABLE `t` (\n\t`id` int PRIMARY KEY,\n\t`v` varchar(255) COLLATE utf8mb4_0900_ai_ci\n) CHARSET utf8mb4,\n  COLLATE utf8mb4_0900_bin",
		},
		{
			name: "keeps existing collation even if another non default collation",
			from: "create table t (id int signed primary key, v varchar(255) charset utf8mb4 collate utf8mb4_german2_ci) charset utf8mb4 collate utf8mb4_0900_bin",
			to:   "CREATE TABLE `t` (\n\t`id` int PRIMARY KEY,\n\t`v` varchar(255) COLLATE utf8mb4_german2_ci\n) CHARSET utf8mb4,\n  COLLATE utf8mb4_0900_bin",
		},
		{
			name: "maps utf8 to utf8mb3",
			from: "create table t (id int signed primary key, v varchar(255) charset utf8 collate utf8_general_ci) charset utf8 collate utf8_general_ci",
			to:   "CREATE TABLE `t` (\n\t`id` int PRIMARY KEY,\n\t`v` varchar(255)\n) CHARSET utf8mb3,\n  COLLATE utf8_general_ci",
		},
		{
			name: "lowercase table options for charset and collation",
			from: "create table t (id int signed primary key, v varchar(255) charset utf8 collate utf8_general_ci) charset UTF8 collate UTF8_GENERAL_CI",
			to:   "CREATE TABLE `t` (\n\t`id` int PRIMARY KEY,\n\t`v` varchar(255)\n) CHARSET utf8mb3,\n  COLLATE utf8_general_ci",
		},
		{
			name: "drops existing collation if it matches table default at column level for non default charset",
			from: "create table t (id int signed primary key, v varchar(255) charset utf8mb3 collate utf8_unicode_ci) charset utf8mb3 collate utf8_unicode_ci",
			to:   "CREATE TABLE `t` (\n\t`id` int PRIMARY KEY,\n\t`v` varchar(255)\n) CHARSET utf8mb3,\n  COLLATE utf8_unicode_ci",
		},
		{
			name: "correct case table options for engine",
			from: "create table t (id int signed primary key) engine innodb",
			to:   "CREATE TABLE `t` (\n\t`id` int PRIMARY KEY\n) ENGINE InnoDB",
		},
		{
			name: "correct case for engine in partitions",
			from: "create table a (id int not null primary key) engine InnoDB, charset utf8mb4, collate utf8mb4_0900_ai_ci partition by range (`id`) (partition `p10` values less than(10) engine innodb)",
			to:   "CREATE TABLE `a` (\n\t`id` int NOT NULL PRIMARY KEY\n) ENGINE InnoDB,\n  CHARSET utf8mb4,\n  COLLATE utf8mb4_0900_ai_ci PARTITION BY RANGE (`id`) (PARTITION `p10` VALUES LESS THAN (10) ENGINE InnoDB)",
		},
	}
	for _, ts := range tt {
		t.Run(ts.name, func(t *testing.T) {
			stmt, err := sqlparser.Parse(ts.from)
			require.NoError(t, err)
			fromCreateTable, ok := stmt.(*sqlparser.CreateTable)
			require.True(t, ok)

			from := NewCreateTableEntity(fromCreateTable)
			assert.Equal(t, ts.to, sqlparser.CanonicalString(from))
		})
	}
}
