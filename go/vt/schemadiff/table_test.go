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
		diff     string
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
			name: "added column",
			from: "create table t1 (`id` int primary key)",
			to:   "create table t2 (id int primary key, `i` int not null default 0)",
			diff: "alter table t1 add column i int not null default 0",
		},
		{
			name: "dropped column",
			from: "create table t1 (id int primary key, `i` int not null default 0)",
			to:   "create table t2 (`id` int primary key)",
			diff: "alter table t1 drop column i",
		},
		{
			name: "modified column",
			from: "create table t1 (id int primary key, `i` int not null default 0)",
			to:   "create table t2 (id int primary key, `i` bigint unsigned default null)",
			diff: "alter table t1 modify column i bigint unsigned default null",
		},
		{
			name: "added column, dropped column, modified column",
			from: "create table t1 (id int primary key, `i` int not null default 0, c char(3) default '')",
			to:   "create table t2 (id int primary key, ts timestamp null, `i` bigint unsigned default null)",
			diff: "alter table t1 drop column c, modify column i bigint unsigned default null, add column ts timestamp null after id",
		},
		{
			name: "reorder column",
			from: "create table t1 (id int primary key, a int, b int, c int, d int)",
			to:   "create table t2 (id int primary key, a int, c int, b int, d int)",
			diff: "alter table t1 modify column c int after a",
		},
		{
			name: "reorder column, far jump",
			from: "create table t1 (id int primary key, a int, b int, c int, d int)",
			to:   "create table t2 (a int, b int, c int, d int, id int primary key)",
			diff: "alter table t1 modify column a int first, modify column b int after a, modify column c int after b, modify column d int after c",
		},
		{
			name: "reorder column, far jump, another reorder",
			from: "create table t1 (id int primary key, a int, b int, c int, d int)",
			to:   "create table t2 (a int, c int, b int, d int, id int primary key)",
			diff: "alter table t1 modify column a int first, modify column c int after a, modify column b int after c, modify column d int after b",
		},
		{
			name: "reorder column, far jump, another reorder 2",
			from: "create table t1 (id int primary key, a int, b int, c int, d int)",
			to:   "create table t2 (c int, a int, b int, d int, id int primary key)",
			diff: "alter table t1 modify column c int first, modify column a int after c, modify column b int after a, modify column d int after b",
		},
		{
			name: "reorder column, far jump, another reorder, extra columns",
			from: "create table t1 (id int primary key, a int, b int, c int, d int, e int, f int)",
			to:   "create table t2 (a int, c int, b int, d int, id int primary key, e int, f int)",
			diff: "alter table t1 modify column a int first, modify column c int after a, modify column b int after c, modify column d int after b",
		},
		{
			name: "reorder column, far jump, another reorder, removed columns",
			from: "create table t1 (id int primary key, a int, b int, c int, d int, e int, f int, g int)",
			to:   "create table t2 (a int, c int, f int, e int, id int primary key, g int)",
			diff: "alter table t1 drop column b, drop column d, modify column a int first, modify column c int after a, modify column f int after c, modify column e int after f",
		},
		{
			name: "two reorders",
			from: "create table t1 (id int primary key, a int, b int, c int, d int, e int, f int)",
			to:   "create table t2 (id int primary key, b int, a int, c int, e int, d int, f int)",
			diff: "alter table t1 modify column b int after id, modify column e int after c",
		},
		{
			name: "reorder column and change data type",
			from: "create table t1 (id int primary key, a int, b int, c int, d int)",
			to:   "create table t2 (id int primary key, a int, c bigint, b int, d int)",
			diff: "alter table t1 modify column c bigint after a",
		},
		{
			name: "reorder column, first",
			from: "create table t1 (id int primary key, a int, b int, c int, d int)",
			to:   "create table t2 (c int, id int primary key, a int, b int, d int)",
			diff: "alter table t1 modify column c int first",
		},
		{
			name: "add multiple columns",
			from: "create table t1 (id int primary key, a int)",
			to:   "create table t2 (id int primary key, a int, b int, c int, d int)",
			diff: "alter table t1 add column b int, add column c int, add column d int",
		},
		{
			name: "added column in middle",
			from: "create table t1 (id int primary key, a int, b int, c int, d int)",
			to:   "create table t2 (id int primary key, a int, b int, x int, c int, d int)",
			diff: "alter table t1 add column x int after b",
		},
		{
			name: "added multiple column in middle",
			from: "create table t1 (id int primary key, a int)",
			to:   "create table t2 (w int, x int, id int primary key, y int, a int, z int)",
			diff: "alter table t1 add column w int first, add column x int after w, add column y int after id, add column z int",
		},
		{
			name: "added column first, reorder column",
			from: "create table t1 (id int primary key, a int)",
			to:   "create table t2 (x int, a int, id int primary key)",
			diff: "alter table t1 modify column a int first, add column x int first",
		},
		{
			name: "added column in middle, add column on end, reorder column",
			from: "create table t1 (id int primary key, a int, b int, c int, d int)",
			to:   "create table t2 (id int primary key, a int, b int, x int, d int, c int, y int)",
			diff: "alter table t1 modify column d int after b, add column x int after b, add column y int",
		},
		{
			name: "added column in middle, add column on end, reorder column 2",
			from: "create table t1 (id int primary key, a int, b int, c int, d int)",
			to:   "create table t2 (id int primary key, a int, c int, x int, b int, d int, y int)",
			diff: "alter table t1 modify column c int after a, add column x int after c, add column y int",
		},
		// keys
		{
			name: "added key",
			from: "create table t1 (`id` int primary key, i int)",
			to:   "create table t2 (id int primary key, `i` int, key `i_idx` (i))",
			diff: "alter table t1 add key i_idx (i)",
		},
		{
			name: "added column and key",
			from: "create table t1 (`id` int primary key)",
			to:   "create table t2 (id int primary key, `i` int, key `i_idx` (i))",
			diff: "alter table t1 add column i int, add key i_idx (i)",
		},
		{
			name: "modify column primary key",
			from: "create table t1 (`id` int)",
			to:   "create table t2 (id int primary key)",
			diff: "alter table t1 modify column id int primary key",
		},
		{
			name: "added primary key",
			from: "create table t1 (`id` int)",
			to:   "create table t2 (id int, primary key(id))",
			diff: "alter table t1 add primary key (id)",
		},
		{
			name: "dropped key",
			from: "create table t1 (`id` int primary key, i int, key i_idx(i))",
			to:   "create table t2 (`id` int primary key, i int)",
			diff: "alter table t1 drop key i_idx",
		},
		{
			name: "dropped key 2",
			from: "create table t1 (`id` int, i int, primary key (id), key i_idx(i))",
			to:   "create table t1 (`id` int, i int, primary key (id))",
			diff: "alter table t1 drop key i_idx",
		},
		{
			name: "modified key",
			from: "create table t1 (`id` int primary key, i int, key i_idx(i))",
			to:   "create table t2 (`id` int primary key, i int, key i_idx(i, id))",
			diff: "alter table t1 drop key i_idx, add key i_idx (i, id)",
		},
		{
			name: "modified primary key",
			from: "create table t1 (`id` int, i int, primary key(id), key i_idx(i))",
			to:   "create table t2 (`id` int, i int, primary key(id, i),key i_idx(`i`))",
			diff: "alter table t1 drop key `PRIMARY`, add primary key (id, i)",
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
			name: "reordered key, add key",
			from: "create table t1 (`id` int primary key, i int, key i_idx(i), key i2_idx(i, `id`))",
			to:   "create table t2 (`id` int primary key, i int, key i2_idx (`i`, id), key i_idx3(id), key i_idx ( i ) )",
			diff: "alter table t1 add key i_idx3 (id)",
		},
		// foreign keys
		{
			name: "drop foreign key",
			from: "create table t1 (id int primary key, i int, constraint f foreign key (i) references parent(id))",
			to:   "create table t2 (id int primary key, i int)",
			diff: "alter table t1 drop foreign key f",
		},
		{
			name: "add foreign key",
			from: "create table t1 (id int primary key, i int)",
			to:   "create table t2 (id int primary key, i int, constraint f foreign key (i) references parent(id))",
			diff: "alter table t1 add constraint f foreign key (i) references parent (id)",
		},
		{
			name: "identical foreign key",
			from: "create table t1 (id int primary key, i int, constraint f foreign key (i) references parent(id) on delete cascade)",
			to:   "create table t2 (id int primary key, i int, constraint f foreign key (i) references parent(id) on delete cascade)",
			diff: "",
		},
		{
			name: "modify foreign key",
			from: "create table t1 (id int primary key, i int, constraint f foreign key (i) references parent(id) on delete cascade)",
			to:   "create table t2 (id int primary key, i int, constraint f foreign key (i) references parent(id) on delete set null)",
			diff: "alter table t1 drop foreign key f, add constraint f foreign key (i) references parent (id) on delete set null",
		},
		{
			name: "drop and add foreign key",
			from: "create table t1 (id int primary key, i int, constraint f foreign key (i) references parent(id) on delete cascade)",
			to:   "create table t2 (id int primary key, i int, constraint f2 foreign key (i) references parent(id) on delete set null)",
			diff: "alter table t1 drop foreign key f, add constraint f2 foreign key (i) references parent (id) on delete set null",
		},
		{
			name: "ignore different foreign key order",
			from: "create table t1 (id int primary key, i int, constraint f foreign key (i) references parent(id) on delete restrict, constraint f2 foreign key (i2) references parent2(id) on delete restrict)",
			to:   "create table t2 (id int primary key, i int, constraint f2 foreign key (i2) references parent2(id) on delete restrict, constraint f foreign key (i) references parent(id) on delete restrict)",
			diff: "",
		},
		// partitions
		{
			name: "identical partitioning",
			from: "create table t1 (id int primary key) partition by hash (id) partitions 4",
			to:   "create table t1 (id int primary key, a int) partition by hash (id) partitions 4",
			diff: "alter table t1 add column a int",
		},
		{
			name: "remove partitioning",
			from: "create table t1 (id int primary key) partition by hash (id) partitions 4",
			to:   "create table t1 (id int primary key, a int)",
			diff: "alter table t1 add column a int remove partitioning",
		},
		{
			name: "remove partitioning 2",
			from: "create table t1 (id int primary key) partition by hash (id) partitions 4",
			to:   "create table t1 (id int primary key)",
			diff: "alter table t1 remove partitioning",
		},
		{
			name: "change partitioning hash",
			from: "create table t1 (id int primary key) partition by hash (id) partitions 4",
			to:   "create table t1 (id int primary key) partition by hash (id) partitions 5",
			diff: "alter table t1 partition by hash (id) partitions 5",
		},
		{
			name: "change partitioning key",
			from: "create table t1 (id int primary key) partition by key (id) partitions 2",
			to:   "create table t1 (id int primary key) partition by hash (id) partitions 5",
			diff: "alter table t1 partition by hash (id) partitions 5",
		},
		{
			name: "change partitioning list",
			from: "create table t1 (id int primary key) partition by key (id) partitions 2",
			to:   "create table t1 (id int primary key) partition by list (id) (partition p1 values in(11,21), partition p2 values in (12,22))",
			diff: "alter table t1 partition by list (id) (partition p1 values in (11, 21), partition p2 values in (12, 22))",
		},
		{
			name: "change partitioning range: rotate",
			from: "create table t1 (id int primary key) partition by range (id) (partition p1 values less than (10), partition p2 values less than (20), partition p3 values less than (30))",
			to:   "create table t1 (id int primary key) partition by range (id) (partition p2 values less than (20), partition p3 values less than (30), partition p4 values less than (40))",
			diff: "alter table t1 partition by range (id) (partition p2 values less than (20), partition p3 values less than (30), partition p4 values less than (40))",
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
		},
		{
			name:     "change partitioning range: ignore rotate, not a rotation 2",
			from:     "create table t1 (id int primary key) partition by range (id) (partition p1 values less than (10), partition p2 values less than (20), partition p3 values less than (30))",
			to:       "create table t1 (id int primary key) partition by range (id) (partition p2 values less than (20), partition p3 values less than (35), partition p4 values less than (40))",
			rotation: RangeRotationIgnore,
			diff:     "alter table t1 partition by range (id) (partition p2 values less than (20), partition p3 values less than (35), partition p4 values less than (40))",
		},
		{
			name:     "change partitioning range: ignore rotate, not a rotation 3",
			from:     "create table t1 (id int primary key) partition by range (id) (partition p1 values less than (10), partition p2 values less than (20), partition p3 values less than (30))",
			to:       "create table t1 (id int primary key) partition by range (id) (partition p2 values less than (20), partition pX values less than (30), partition p4 values less than (40))",
			rotation: RangeRotationIgnore,
			diff:     "alter table t1 partition by range (id) (partition p2 values less than (20), partition pX values less than (30), partition p4 values less than (40))",
		},
		{
			name:     "change partitioning range: ignore rotate, not a rotation 4",
			from:     "create table t1 (id int primary key) partition by range (id) (partition p1 values less than (10), partition p2 values less than (20), partition p3 values less than (30))",
			to:       "create table t1 (id int primary key) partition by range (id) (partition pX values less than (20), partition p3 values less than (30), partition p4 values less than (40))",
			rotation: RangeRotationIgnore,
			diff:     "alter table t1 partition by range (id) (partition pX values less than (20), partition p3 values less than (30), partition p4 values less than (40))",
		},
		{
			name:     "change partitioning range: ignore rotate, nothing shared",
			from:     "create table t1 (id int primary key) partition by range (id) (partition p1 values less than (10), partition p2 values less than (20), partition p3 values less than (30))",
			to:       "create table t1 (id int primary key) partition by range (id) (partition p4 values less than (40), partition p5 values less than (50), partition p6 values less than (60))",
			rotation: RangeRotationIgnore,
			diff:     "alter table t1 partition by range (id) (partition p4 values less than (40), partition p5 values less than (50), partition p6 values less than (60))",
		},
		{
			name:     "change partitioning range: ignore rotate, no names shared, definitions shared",
			from:     "create table t1 (id int primary key) partition by range (id) (partition p1 values less than (10), partition p2 values less than (20), partition p3 values less than (30))",
			to:       "create table t1 (id int primary key) partition by range (id) (partition pA values less than (20), partition pB values less than (30), partition pC values less than (40))",
			rotation: RangeRotationIgnore,
			diff:     "alter table t1 partition by range (id) (partition pA values less than (20), partition pB values less than (30), partition pC values less than (40))",
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
			name: "add table option 1",
			from: "create table t1 (id int primary key)",
			to:   "create table t1 (id int primary key) row_format=compressed",
			diff: "alter table t1 row_format compressed",
		},
		{
			name: "add table option 2",
			from: "create table t1 (id int primary key) character set=utf8",
			to:   "create table t1 (id int primary key) character set=utf8, row_format=compressed",
			diff: "alter table t1 row_format compressed",
		},
		{
			name: "add table option 3",
			from: "create table t1 (id int primary key) character set=utf8",
			to:   "create table t1 (id int primary key) row_format=compressed, character set=utf8",
			diff: "alter table t1 row_format compressed",
		},
		{
			name: "add table option 3",
			from: "create table t1 (id int primary key) character set=utf8",
			to:   "create table t1 (id int primary key) row_format=compressed, character set=utf8, checksum=1",
			diff: "alter table t1 row_format compressed checksum 1",
		},
		{
			name: "modify table option 1",
			from: "create table t1 (id int primary key) character set=utf8",
			to:   "create table t1 (id int primary key) character set=utf8mb4",
			diff: "alter table t1 charset utf8mb4",
		},
		{
			name: "modify table option 2",
			from: "create table t1 (id int primary key) charset=utf8",
			to:   "create table t1 (id int primary key) character set=utf8mb4",
			diff: "alter table t1 charset utf8mb4",
		},
		{
			name: "modify table option 3",
			from: "create table t1 (id int primary key) character set=utf8",
			to:   "create table t1 (id int primary key) charset=utf8mb4",
			diff: "alter table t1 charset utf8mb4",
		},
		{
			name: "modify table option 4",
			from: "create table t1 (id int primary key) character set=utf8",
			to:   "create table t1 (id int primary key) row_format=compressed, character set=utf8mb4, checksum=1",
			diff: "alter table t1 charset utf8mb4 row_format compressed checksum 1",
		},
		{
			name: "remove table option 1",
			from: "create table t1 (id int primary key) row_format=compressed",
			to:   "create table t1 (id int primary key) ",
			diff: "alter table t1 row_format DEFAULT",
		},
		{
			name: "remove table option 2",
			from: "create table t1 (id int primary key) CHECKSUM=1",
			to:   "create table t1 (id int primary key) ",
			diff: "alter table t1 CHECKSUM 0",
		},
		{
			name: "remove table option 3",
			from: "create table t1 (id int primary key) checksum=1",
			to:   "create table t1 (id int primary key) ",
			diff: "alter table t1 checksum 0",
		},
		{
			name: "remove table option 4",
			from: "create table t1 (id int auto_increment primary key) KEY_BLOCK_SIZE=16 COMPRESSION='zlib'",
			to:   "create table t2 (id int auto_increment primary key)",
			diff: "alter table t1 KEY_BLOCK_SIZE 0 COMPRESSION ''",
		},
		{
			name: "add, modify and remove table option",
			from: "create table t1 (id int primary key) engine=innodb, charset=utf8, checksum=1",
			to:   "create table t1 (id int primary key) row_format=compressed, engine=innodb, charset=utf8mb4",
			diff: "alter table t1 checksum 0 charset utf8mb4 row_format compressed",
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
		},
		{
			name: `change table charset`,
			from: "create table t (id int primary key, t1 varchar(128) default null, t2 varchar(128) not null, t3 tinytext charset latin1, t4 tinytext charset latin1) default charset=utf8",
			to:   "create table t (id int primary key, t1 varchar(128) not null, t2 varchar(128) not null, t3 tinytext, t4 tinytext charset latin1) default charset=utf8mb4",
			diff: "alter table t modify column t1 varchar(128) not null, modify column t2 varchar(128) not null, modify column t3 tinytext, charset utf8mb4",
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
				diff := alter.StatementString()
				assert.Equal(t, ts.diff, diff)
			}
		})
	}
}
