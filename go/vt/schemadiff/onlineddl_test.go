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

func TestUniqueKeysForOnlineDDL(t *testing.T) {
	table := `
	create table t (
		idsha varchar(64),
		col1 int,
		col2 int not null,
		col3 bigint not null default 3,
		col4 smallint not null,
		f float not null,
		v varchar(32) not null,
		primary key (idsha),
		unique key ukidsha (idsha),
		unique key uk1 (col1),
		unique key uk2 (col2),
		unique key uk3 (col3),
		key k1 (col1),
		key kf (f),
		key k1f (col1, f),
		key kv (v),
		unique key ukv (v),
		unique key ukvprefix (v(10)),
		unique key uk2vprefix (col2, v(10)),
		unique key uk1f (col1, f),
		unique key uk41 (col4, col1),
		unique key uk42 (col4, col2)
	)`
	env := NewTestEnv()
	createTableEntity, err := NewCreateTableEntityFromSQL(env, table)
	require.NoError(t, err)
	err = createTableEntity.validate()
	require.NoError(t, err)

	keys := PrioritizedUniqueKeys(createTableEntity)
	require.NotEmpty(t, keys)
	names := make([]string, 0, len(keys))
	for _, key := range keys {
		names = append(names, key.Name())
	}
	expect := []string{
		"PRIMARY",
		"uk42",
		"uk2",
		"uk3",
		"ukidsha",
		"ukv",
		"uk2vprefix",
		"ukvprefix",
		"uk41",
		"uk1",
		"uk1f",
	}
	assert.Equal(t, expect, names)
}

func TestRemovedForeignKeyNames(t *testing.T) {
	env := NewTestEnv()

	tcases := []struct {
		before string
		after  string
		names  []string
	}{
		{
			before: "create table t (id int primary key)",
			after:  "create table t (id2 int primary key, i int)",
		},
		{
			before: "create table t (id int primary key)",
			after:  "create table t2 (id2 int primary key, i int)",
		},
		{
			before: "create table t (id int primary key, i int, constraint f foreign key (i) references parent (id) on delete cascade)",
			after:  "create table t (id int primary key, i int, constraint f foreign key (i) references parent (id) on delete cascade)",
		},
		{
			before: "create table t (id int primary key, i int, constraint f1 foreign key (i) references parent (id) on delete cascade)",
			after:  "create table t (id int primary key, i int, constraint f2 foreign key (i) references parent (id) on delete cascade)",
		},
		{
			before: "create table t (id int primary key, i int, constraint f foreign key (i) references parent (id) on delete cascade)",
			after:  "create table t (id int primary key, i int)",
			names:  []string{"f"},
		},
		{
			before: "create table t (id int primary key, i int, i2 int, constraint f1 foreign key (i) references parent (id) on delete cascade, constraint fi2 foreign key (i2) references parent (id) on delete cascade)",
			after:  "create table t (id int primary key, i int, i2 int, constraint f2 foreign key (i) references parent (id) on delete cascade)",
			names:  []string{"fi2"},
		},
		{
			before: "create table t1 (id int primary key, i int, constraint `check1` CHECK ((`i` < 5)))",
			after:  "create table t2 (id int primary key, i int)",
		},
	}
	for _, tcase := range tcases {
		t.Run(tcase.before, func(t *testing.T) {
			before, err := NewCreateTableEntityFromSQL(env, tcase.before)
			require.NoError(t, err)
			err = before.validate()
			require.NoError(t, err)

			after, err := NewCreateTableEntityFromSQL(env, tcase.after)
			require.NoError(t, err)
			err = after.validate()
			require.NoError(t, err)

			names, err := RemovedForeignKeyNames(before, after)
			assert.NoError(t, err)
			assert.Equal(t, tcase.names, names)
		})
	}
}

func TestGetAlterTableAnalysis(t *testing.T) {
	tcases := []struct {
		alter    string
		renames  map[string]string
		drops    map[string]bool
		isrename bool
		autoinc  bool
	}{
		{
			alter: "alter table t add column t int, engine=innodb",
		},
		{
			alter:   "alter table t add column t int, change ts ts timestamp, engine=innodb",
			renames: map[string]string{"ts": "ts"},
		},
		{
			alter:   "alter table t AUTO_INCREMENT=7",
			autoinc: true,
		},
		{
			alter:   "alter table t add column t int, change ts ts timestamp, auto_increment=7 engine=innodb",
			renames: map[string]string{"ts": "ts"},
			autoinc: true,
		},
		{
			alter:   "alter table t  add column t int, change ts ts timestamp, CHANGE f `f` float, engine=innodb",
			renames: map[string]string{"ts": "ts", "f": "f"},
		},
		{
			alter:   `alter table t add column b bigint, change f fl float, change i count int, engine=innodb`,
			renames: map[string]string{"f": "fl", "i": "count"},
		},
		{
			alter:   "alter table t add column b bigint, change column `f` fl float, change `i` `count` int, engine=innodb",
			renames: map[string]string{"f": "fl", "i": "count"},
		},
		{
			alter:   "alter table t add column b bigint, change column `f` fl float, change `i` `count` int, change ts ts timestamp, engine=innodb",
			renames: map[string]string{"f": "fl", "i": "count", "ts": "ts"},
		},
		{
			alter: "alter table t drop column b",
			drops: map[string]bool{"b": true},
		},
		{
			alter: "alter table t drop column b, drop key c_idx, drop column `d`",
			drops: map[string]bool{"b": true, "d": true},
		},
		{
			alter: "alter table t drop column b, drop key c_idx, drop column `d`, drop `e`, drop primary key, drop foreign key fk_1",
			drops: map[string]bool{"b": true, "d": true, "e": true},
		},
		{
			alter:    "alter table t rename as something_else",
			isrename: true,
		},
		{
			alter:    "alter table t drop column b, rename as something_else",
			isrename: true,
			drops:    map[string]bool{"b": true},
		},
	}
	for _, tcase := range tcases {
		t.Run(tcase.alter, func(t *testing.T) {
			if tcase.renames == nil {
				tcase.renames = make(map[string]string)
			}
			if tcase.drops == nil {
				tcase.drops = make(map[string]bool)
			}
			stmt, err := sqlparser.NewTestParser().ParseStrictDDL(tcase.alter)
			require.NoError(t, err)
			alter, ok := stmt.(*sqlparser.AlterTable)
			require.True(t, ok)

			analysis := GetAlterTableAnalysis(alter)
			require.NotNil(t, analysis)
			assert.Equal(t, tcase.isrename, analysis.IsRenameTable)
			assert.Equal(t, tcase.autoinc, analysis.IsAutoIncrementChangeRequested)
			assert.Equal(t, tcase.renames, analysis.ColumnRenameMap)
			assert.Equal(t, tcase.drops, analysis.DroppedColumnsMap)
		})
	}
}

func TestAnalyzeSharedColumns(t *testing.T) {
	sourceTable := `
		create table t (
			id int,
			cint int,
			cgen1 int generated always as (cint + 1) stored,
			cgen2 int generated always as (cint + 2) stored,
			cchar char(1),
			cremoved int not null default 7,
			cnullable int,
			cnodefault int not null,
			extra1 int,
			primary key (id)
		)
	`
	targetTable := `
		create table t (
			id int,
			cint int,
			cgen1 int generated always as (cint + 1) stored,
			cchar_alternate char(1),
			cnullable int,
			cnodefault int not null,
			extra2 int,
			primary key (id)
		)
	`
	tcases := []struct {
		name                                    string
		sourceTable                             string
		targetTable                             string
		renameMap                               map[string]string
		expectSourceSharedColNames              []string
		expectTargetSharedColNames              []string
		expectDroppedSourceNonGeneratedColNames []string
		expectSharedColumnsMap                  map[string]string
	}{
		{
			name:                                    "rename map empty",
			renameMap:                               map[string]string{},
			expectSourceSharedColNames:              []string{"id", "cint", "cnullable", "cnodefault"},
			expectTargetSharedColNames:              []string{"id", "cint", "cnullable", "cnodefault"},
			expectDroppedSourceNonGeneratedColNames: []string{"cchar", "cremoved", "extra1"},
			expectSharedColumnsMap:                  map[string]string{"id": "id", "cint": "cint", "cnullable": "cnullable", "cnodefault": "cnodefault"},
		},
		{
			name:                                    "renamed column",
			renameMap:                               map[string]string{"cchar": "cchar_alternate"},
			expectSourceSharedColNames:              []string{"id", "cint", "cchar", "cnullable", "cnodefault"},
			expectTargetSharedColNames:              []string{"id", "cint", "cchar_alternate", "cnullable", "cnodefault"},
			expectDroppedSourceNonGeneratedColNames: []string{"cremoved", "extra1"},
			expectSharedColumnsMap:                  map[string]string{"id": "id", "cint": "cint", "cchar": "cchar_alternate", "cnullable": "cnullable", "cnodefault": "cnodefault"},
		},
	}

	env := NewTestEnv()
	alterTableAnalysis := GetAlterTableAnalysis(nil) // empty
	for _, tcase := range tcases {
		t.Run(tcase.name, func(t *testing.T) {
			if tcase.sourceTable == "" {
				tcase.sourceTable = sourceTable
			}
			if tcase.targetTable == "" {
				tcase.targetTable = targetTable
			}
			if tcase.renameMap != nil {
				alterTableAnalysis.ColumnRenameMap = tcase.renameMap
			}

			sourceEntity, err := NewCreateTableEntityFromSQL(env, tcase.sourceTable)
			require.NoError(t, err)
			err = sourceEntity.validate()
			require.NoError(t, err)

			targetEntity, err := NewCreateTableEntityFromSQL(env, tcase.targetTable)
			require.NoError(t, err)
			err = targetEntity.validate()
			require.NoError(t, err)

			sourceSharedCols, targetSharedCols, droppedNonGeneratedCols, sharedColumnsMap := AnalyzeSharedColumns(
				NewColumnDefinitionEntityList(sourceEntity.ColumnDefinitionEntities()),
				NewColumnDefinitionEntityList(targetEntity.ColumnDefinitionEntities()),
				alterTableAnalysis,
			)
			assert.Equal(t, tcase.expectSourceSharedColNames, sourceSharedCols.Names())
			assert.Equal(t, tcase.expectTargetSharedColNames, targetSharedCols.Names())
			assert.Equal(t, tcase.expectDroppedSourceNonGeneratedColNames, droppedNonGeneratedCols.Names())
			assert.Equal(t, tcase.expectSharedColumnsMap, sharedColumnsMap)
		})
	}
}
