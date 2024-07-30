/*
Copyright 2024 The Vitess Authors.

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

package onlineddl

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/schemadiff"
	"vitess.io/vitess/go/vt/vtenv"
)

func TestRevertible(t *testing.T) {

	type revertibleTestCase struct {
		name       string
		fromSchema string
		toSchema   string
		// expectProblems              bool
		removedForeignKeyNames      string
		removedUniqueKeyNames       string
		droppedNoDefaultColumnNames string
		expandedColumnNames         string
	}

	var testCases = []revertibleTestCase{
		{
			name:       "identical schemas",
			fromSchema: `id int primary key, i1 int not null default 0`,
			toSchema:   `id int primary key, i2 int not null default 0`,
		},
		{
			name:       "different schemas, nothing to note",
			fromSchema: `id int primary key, i1 int not null default 0, unique key i1_uidx(i1)`,
			toSchema:   `id int primary key, i1 int not null default 0, i2 int not null default 0, unique key i1_uidx(i1)`,
		},
		{
			name:                  "removed non-nullable unique key",
			fromSchema:            `id int primary key, i1 int not null default 0, unique key i1_uidx(i1)`,
			toSchema:              `id int primary key, i2 int not null default 0`,
			removedUniqueKeyNames: `i1_uidx`,
		},
		{
			name:                  "removed nullable unique key",
			fromSchema:            `id int primary key, i1 int default null, unique key i1_uidx(i1)`,
			toSchema:              `id int primary key, i2 int default null`,
			removedUniqueKeyNames: `i1_uidx`,
		},
		{
			name:                  "expanding unique key removes unique constraint",
			fromSchema:            `id int primary key, i1 int default null, unique key i1_uidx(i1)`,
			toSchema:              `id int primary key, i1 int default null, unique key i1_uidx(i1, id)`,
			removedUniqueKeyNames: `i1_uidx`,
		},
		{
			name:                  "expanding unique key prefix removes unique constraint",
			fromSchema:            `id int primary key, v varchar(100) default null, unique key v_uidx(v(20))`,
			toSchema:              `id int primary key, v varchar(100) default null, unique key v_uidx(v(21))`,
			removedUniqueKeyNames: `v_uidx`,
		},
		{
			name:                  "reducing unique key does not remove unique constraint",
			fromSchema:            `id int primary key, i1 int default null, unique key i1_uidx(i1, id)`,
			toSchema:              `id int primary key, i1 int default null, unique key i1_uidx(i1)`,
			removedUniqueKeyNames: ``,
		},
		{
			name:       "reducing unique key does not remove unique constraint",
			fromSchema: `id int primary key, v varchar(100) default null, unique key v_uidx(v(21))`,
			toSchema:   `id int primary key, v varchar(100) default null, unique key v_uidx(v(20))`,
		},
		{
			name:                   "removed foreign key",
			fromSchema:             "id int primary key, i int, constraint some_fk_1 foreign key (i) references parent (id) on delete cascade",
			toSchema:               "id int primary key, i int",
			removedForeignKeyNames: "some_fk_1",
		},

		{
			name:       "renamed foreign key",
			fromSchema: "id int primary key, i int, constraint f1 foreign key (i) references parent (id) on delete cascade",
			toSchema:   "id int primary key, i int, constraint f2 foreign key (i) references parent (id) on delete cascade",
		},
		{
			name:                        "remove column without default",
			fromSchema:                  `id int primary key, i1 int not null, i2 int not null default 0, i3 int default null`,
			toSchema:                    `id int primary key, i4 int not null default 0`,
			droppedNoDefaultColumnNames: `i1`,
		},
		{
			name:                "expanded: nullable",
			fromSchema:          `id int primary key, i1 int not null, i2 int default null`,
			toSchema:            `id int primary key, i1 int default null, i2 int not null`,
			expandedColumnNames: `i1`,
		},
		{
			name:                "expanded: longer text",
			fromSchema:          `id int primary key, i1 int default null, v1 varchar(40) not null, v2 varchar(5), v3 varchar(3)`,
			toSchema:            `id int primary key, i1 int not null, v1 varchar(100) not null, v2 char(3), v3 char(5)`,
			expandedColumnNames: `v1,v3`,
		},
		{
			name:                "expanded: int numeric precision and scale",
			fromSchema:          `id int primary key, i1 int, i2 tinyint, i3 mediumint, i4 bigint`,
			toSchema:            `id int primary key, i1 int, i2 mediumint, i3 int, i4 tinyint`,
			expandedColumnNames: `i2,i3`,
		},
		{
			name:                "expanded: floating point",
			fromSchema:          `id int primary key, i1 int, n2 bigint, n3 bigint, n4 float, n5 double`,
			toSchema:            `id int primary key, i1 int, n2 float, n3 double, n4 double, n5 float`,
			expandedColumnNames: `n2,n3,n4`,
		},
		{
			name:                "expanded: decimal numeric precision and scale",
			fromSchema:          `id int primary key, i1 int, d1 decimal(10,2), d2 decimal (10,2), d3 decimal (10,2)`,
			toSchema:            `id int primary key, i1 int, d1 decimal(11,2), d2 decimal (9,1), d3 decimal (10,3)`,
			expandedColumnNames: `d1,d3`,
		},
		{
			name:                "expanded: signed, unsigned",
			fromSchema:          `id int primary key, i1 bigint signed, i2 int unsigned, i3 bigint unsigned`,
			toSchema:            `id int primary key, i1 int signed, i2 int signed, i3 int signed`,
			expandedColumnNames: `i2,i3`,
		},
		{
			name:                "expanded: signed, unsigned: range",
			fromSchema:          `id int primary key, i1 int signed, i2 bigint signed, i3 int signed`,
			toSchema:            `id int primary key, i1 int unsigned, i2 int unsigned, i3 bigint unsigned`,
			expandedColumnNames: `i1,i3`,
		},
		{
			name:                "expanded: datetime precision",
			fromSchema:          `id int primary key, dt1 datetime, ts1 timestamp, ti1 time, dt2 datetime(3), dt3 datetime(6), ts2 timestamp(3)`,
			toSchema:            `id int primary key, dt1 datetime(3), ts1 timestamp(6), ti1 time(3), dt2 datetime(6), dt3 datetime(3), ts2 timestamp`,
			expandedColumnNames: `dt1,ts1,ti1,dt2`,
		},
		{
			name:                "expanded: strange data type changes",
			fromSchema:          `id int primary key, dt1 datetime, ts1 timestamp, i1 int, d1 date, e1 enum('a', 'b')`,
			toSchema:            `id int primary key, dt1 char(32), ts1 varchar(32), i1 tinytext, d1 char(2), e1 varchar(2)`,
			expandedColumnNames: `dt1,ts1,i1,d1,e1`,
		},
		{
			name:                "expanded: temporal types",
			fromSchema:          `id int primary key, t1 time, t2 timestamp, t3 date, t4 datetime, t5 time, t6 date`,
			toSchema:            `id int primary key, t1 datetime, t2 datetime, t3 timestamp, t4 timestamp, t5 timestamp, t6 datetime`,
			expandedColumnNames: `t1,t2,t3,t5,t6`,
		},
		{
			name:                "expanded: character sets",
			fromSchema:          `id int primary key, c1 char(3) charset utf8, c2 char(3) charset utf8mb4, c3 char(3) charset ascii, c4 char(3) charset utf8mb4, c5 char(3) charset utf8, c6 char(3) charset latin1`,
			toSchema:            `id int primary key, c1 char(3) charset utf8mb4, c2 char(3) charset utf8, c3 char(3) charset utf8, c4 char(3) charset ascii, c5 char(3) charset utf8, c6 char(3) charset utf8mb4`,
			expandedColumnNames: `c1,c3,c6`,
		},
		{
			name:                "expanded: enum",
			fromSchema:          `id int primary key, e1 enum('a', 'b'), e2 enum('a', 'b'), e3 enum('a', 'b'),      e4 enum('a', 'b'), e5 enum('a', 'b'),      e6 enum('a', 'b'), e7 enum('a', 'b'), e8 enum('a', 'b')`,
			toSchema:            `id int primary key, e1 enum('a', 'b'), e2 enum('a'),      e3 enum('a', 'b', 'c'), e4 enum('a', 'x'), e5 enum('a', 'x', 'b'), e6 enum('b'),      e7 varchar(1), e8 tinyint`,
			expandedColumnNames: `e3,e4,e5,e6,e7,e8`,
		},
		{
			name:                "expanded: set",
			fromSchema:          `id int primary key, e1 set('a', 'b'), e2 set('a', 'b'), e3 set('a', 'b'), e4 set('a', 'b'), e5 set('a', 'b'), e6 set('a', 'b'), e7 set('a', 'b'), e8 set('a', 'b')`,
			toSchema:            `id int primary key, e1 set('a', 'b'), e2 set('a'), e3 set('a', 'b', 'c'), e4 set('a', 'x'), e5 set('a', 'x', 'b'), e6 set('b'), e7 varchar(1), e8 tinyint`,
			expandedColumnNames: `e3,e4,e5,e6,e7,e8`,
		},
	}

	var (
		createTableWrapper = `CREATE TABLE t (%s)`
	)

	senv := schemadiff.NewTestEnv()
	venv := vtenv.NewTestEnv()
	diffHints := &schemadiff.DiffHints{}
	for _, tcase := range testCases {
		t.Run(tcase.name, func(t *testing.T) {
			tcase.fromSchema = fmt.Sprintf(createTableWrapper, tcase.fromSchema)
			sourceTableEntity, err := schemadiff.NewCreateTableEntityFromSQL(senv, tcase.fromSchema)
			require.NoError(t, err)

			tcase.toSchema = fmt.Sprintf(createTableWrapper, tcase.toSchema)
			targetTableEntity, err := schemadiff.NewCreateTableEntityFromSQL(senv, tcase.toSchema)
			require.NoError(t, err)

			diff, err := sourceTableEntity.TableDiff(targetTableEntity, diffHints)
			require.NoError(t, err)

			v, err := NewVRepl(
				venv,
				"7cee19dd_354b_11eb_82cd_f875a4d24e90",
				"ks",
				"0",
				"mydb",
				sourceTableEntity.CreateTable,
				targetTableEntity.CreateTable,
				diff.AlterTable(),
				false,
			)
			require.NoError(t, err)

			err = v.analyzeAlter()
			require.NoError(t, err)
			err = v.analyzeTables()
			require.NoError(t, err)

			toStringSlice := func(s string) []string {
				if s == "" {
					return []string{}
				}
				return strings.Split(s, ",")
			}
			assert.Equal(t, toStringSlice(tcase.removedForeignKeyNames), v.analysis.RemovedForeignKeyNames)
			assert.Equal(t, toStringSlice(tcase.removedUniqueKeyNames), v.analysis.RemovedUniqueKeys.Names())
			assert.Equal(t, toStringSlice(tcase.droppedNoDefaultColumnNames), v.analysis.DroppedNoDefaultColumns.Names())
			assert.Equal(t, toStringSlice(tcase.expandedColumnNames), v.analysis.ExpandedColumns.Names())
		})
	}
}
