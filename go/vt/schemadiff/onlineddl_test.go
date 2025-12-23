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

package schemadiff

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/capabilities"
	"vitess.io/vitess/go/vt/sqlparser"
)

func TestGetConstraintType(t *testing.T) {
	{
		typ := GetConstraintType(&sqlparser.CheckConstraintDefinition{})
		assert.Equal(t, CheckConstraintType, typ)
	}
	{
		typ := GetConstraintType(&sqlparser.ForeignKeyDefinition{})
		assert.Equal(t, ForeignKeyConstraintType, typ)
	}
}

func TestPrioritizedUniqueKeys(t *testing.T) {
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
	names := make([]string, 0, len(keys.Entities))
	for _, key := range keys.Entities {
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
			if tcase.names == nil {
				tcase.names = []string{}
			}
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

			analysis := OnlineDDLAlterTableAnalysis(alter)
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
	alterTableAnalysis := OnlineDDLAlterTableAnalysis(nil) // empty
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
				sourceEntity.ColumnDefinitionEntitiesList(),
				targetEntity.ColumnDefinitionEntitiesList(),
				alterTableAnalysis,
			)
			assert.Equal(t, tcase.expectSourceSharedColNames, sourceSharedCols.Names())
			assert.Equal(t, tcase.expectTargetSharedColNames, targetSharedCols.Names())
			assert.Equal(t, tcase.expectDroppedSourceNonGeneratedColNames, droppedNonGeneratedCols.Names())
			assert.Equal(t, tcase.expectSharedColumnsMap, sharedColumnsMap)
		})
	}
}

func TestKeyAtLeastConstrainedAs(t *testing.T) {
	env := NewTestEnv()
	sourceTable := `
		create table source_table (
			id int,
			c1 int,
			c2 int,
			c3 int,
			c9 int,
			v varchar(32),
			primary key (id),
			unique key uk1 (c1),
			unique key uk2 (c2),
			unique key uk3 (c3),
			unique key uk9 (c9),
			unique key uk12 (c1, c2),
			unique key uk13 (c1, c3),
			unique key uk23 (c2, c3),
			unique key uk123 (c1, c2, c3),
			unique key uk21 (c2, c1),
			unique key ukv (v),
			unique key ukv3 (v(3)),
			unique key ukv5 (v(5)),
			unique key uk2v5 (c2, v(5))
		)`
	targetTable := `
		create table target_table (
			id int,
			c1 int,
			c2 int,
			c3_renamed int,
			v varchar(32),
			primary key (id),
			unique key uk1 (c1),
			unique key uk2 (c2),
			unique key uk3 (c3_renamed),
			unique key uk12 (c1, c2),
			unique key uk13 (c1, c3_renamed),
			unique key uk23 (c2, c3_renamed),
			unique key uk123 (c1, c2, c3_renamed),
			unique key uk21 (c2, c1),
			unique key ukv (v),
			unique key ukv3 (v(3)),
			unique key ukv5 (v(5)),
			unique key uk2v5 (c2, v(5))
		)`
	renameMap := map[string]string{
		"c3": "c3_renamed",
	}
	tcases := []struct {
		sourceKey string
		targetKey string
		renameMap map[string]string
		expect    bool
	}{
		{
			sourceKey: "uk1",
			targetKey: "uk1",
			expect:    true,
		},
		{
			sourceKey: "uk2",
			targetKey: "uk2",
			expect:    true,
		},
		{
			sourceKey: "uk3",
			targetKey: "uk3",
			expect:    false, // c3 is renamed
		},
		{
			sourceKey: "uk2",
			targetKey: "uk1",
			expect:    false,
		},
		{
			sourceKey: "uk12",
			targetKey: "uk1",
			expect:    false,
		},
		{
			sourceKey: "uk1",
			targetKey: "uk12",
			expect:    true,
		},
		{
			sourceKey: "uk1",
			targetKey: "uk21",
			expect:    true,
		},
		{
			sourceKey: "uk12",
			targetKey: "uk21",
			expect:    true,
		},
		{
			sourceKey: "uk123",
			targetKey: "uk21",
			expect:    false,
		},
		{
			sourceKey: "uk123",
			targetKey: "uk123",
			expect:    false, // c3 is renamed
		},
		{
			sourceKey: "uk1",
			targetKey: "uk123",
			expect:    true, // c3 is renamed but not referenced
		},
		{
			sourceKey: "uk21",
			targetKey: "uk123",
			expect:    true, // c3 is renamed but not referenced
		},
		{
			sourceKey: "uk9",
			targetKey: "uk123",
			expect:    false, // c9 not in target
		},
		{
			sourceKey: "uk3",
			targetKey: "uk3",
			renameMap: renameMap,
			expect:    true,
		},
		{
			sourceKey: "uk123",
			targetKey: "uk123",
			renameMap: renameMap,
			expect:    true,
		},
		{
			sourceKey: "uk3",
			targetKey: "uk123",
			renameMap: renameMap,
			expect:    true,
		},
		{
			sourceKey: "ukv",
			targetKey: "ukv",
			expect:    true,
		},
		{
			sourceKey: "ukv3",
			targetKey: "ukv3",
			expect:    true,
		},
		{
			sourceKey: "ukv",
			targetKey: "ukv3",
			expect:    false,
		},
		{
			sourceKey: "ukv5",
			targetKey: "ukv3",
			expect:    false,
		},
		{
			sourceKey: "ukv3",
			targetKey: "ukv5",
			expect:    true,
		},
		{
			sourceKey: "ukv3",
			targetKey: "ukv",
			expect:    true,
		},
		{
			sourceKey: "uk2",
			targetKey: "uk2v5",
			expect:    true,
		},
		{
			sourceKey: "ukv5",
			targetKey: "uk2v5",
			expect:    true,
		},
		{
			sourceKey: "ukv3",
			targetKey: "uk2v5",
			expect:    true,
		},
		{
			sourceKey: "ukv",
			targetKey: "uk2v5",
			expect:    false,
		},
		{
			sourceKey: "uk2v5",
			targetKey: "ukv5",
			expect:    false,
		},
	}

	sourceEntity, err := NewCreateTableEntityFromSQL(env, sourceTable)
	require.NoError(t, err)
	err = sourceEntity.validate()
	require.NoError(t, err)
	sourceKeys := sourceEntity.IndexDefinitionEntitiesMap()

	targetEntity, err := NewCreateTableEntityFromSQL(env, targetTable)
	require.NoError(t, err)
	err = targetEntity.validate()
	require.NoError(t, err)
	targetKeys := targetEntity.IndexDefinitionEntitiesMap()

	for _, tcase := range tcases {
		t.Run(tcase.sourceKey+"/"+tcase.targetKey, func(t *testing.T) {
			if tcase.renameMap == nil {
				tcase.renameMap = make(map[string]string)
			}
			sourceKey := sourceKeys[tcase.sourceKey]
			require.NotNil(t, sourceKey)

			targetKey := targetKeys[tcase.targetKey]
			require.NotNil(t, targetKey)

			result := KeyAtLeastConstrainedAs(sourceKey, targetKey, tcase.renameMap)
			assert.Equal(t, tcase.expect, result)
		})
	}
}

func TestIntroducedUniqueConstraints(t *testing.T) {
	env := NewTestEnv()
	tcases := []struct {
		sourceTable      string
		targetTable      string
		expectIntroduced []string
		expectRemoved    []string
	}{
		{
			sourceTable: `
				create table source_table (
					id int,
					c1 int,
					c2 int,
					c3 int,
					primary key (id),
					unique key uk1 (c1),
					unique key uk2 (c2),
					unique key uk31 (c3, c1),
					key k1 (c1)
				)`,
			targetTable: `
				create table source_table (
					id int,
					c1 int,
					c2 int,
					c3 int,
					primary key (id),
					unique key uk1 (c1),
					unique key uk3 (c3),
					unique key uk31_alias (c3, c1),
					key k2 (c2)
				)`,
			expectIntroduced: []string{"uk3"},
			expectRemoved:    []string{"uk2"},
		},
		{
			sourceTable: `
				create table source_table (
					id int,
					c1 int,
					c2 int,
					c3 int,
					primary key (id),
					unique key uk1 (c1),
					unique key uk2 (c2),
					unique key uk31 (c3, c1),
					key k1 (c1)
				)`,
			targetTable: `
				create table source_table (
					id int,
					c1 int,
					c2 int,
					c3 int,
					primary key (id),
					unique key uk1 (c1),
					unique key uk3 (c3),
					key k2 (c2)
				)`,
			expectIntroduced: []string{"uk3"}, // uk31 (c3, c1) not considered removed because the new "uk3" is even more constrained
			expectRemoved:    []string{"uk2"},
		},
		{
			sourceTable: `
				create table source_table (
					id int,
					c1 int,
					c2 int,
					v varchar(128),
					primary key (id),
					unique key uk12 (c1, c2),
					unique key ukv5 (v(5)),
					key k1 (c1)
				)`,
			targetTable: `
				create table source_table (
					id int,
					c1 int,
					c2 int,
					c3 int,
					v varchar(128),
					primary key (id),
					unique key uk1v2 (c1, v(2)),
					unique key uk1v7 (c1, v(7)),
					unique key ukv3 (v(3)),
					key k2 (c2)
				)`,
			expectIntroduced: []string{"uk1v2", "ukv3"},
			expectRemoved:    []string{"uk12"},
		},
	}
	for _, tcase := range tcases {
		t.Run("", func(t *testing.T) {
			sourceEntity, err := NewCreateTableEntityFromSQL(env, tcase.sourceTable)
			require.NoError(t, err)
			err = sourceEntity.validate()
			require.NoError(t, err)
			sourceUniqueKeys := PrioritizedUniqueKeys(sourceEntity)

			targetEntity, err := NewCreateTableEntityFromSQL(env, tcase.targetTable)
			require.NoError(t, err)
			err = targetEntity.validate()
			require.NoError(t, err)
			targetUniqueKeys := PrioritizedUniqueKeys(targetEntity)

			introduced := IntroducedUniqueConstraints(sourceUniqueKeys, targetUniqueKeys, nil)
			assert.Equal(t, tcase.expectIntroduced, introduced.Names())
		})
	}
}

func TestUniqueKeysCoveredByColumns(t *testing.T) {
	env := NewTestEnv()
	table := `
		create table t (
			id int,
			c1 int not null,
			c2 int not null,
			c3 int not null,
			c9 int,
			v varchar(32) not null,
			primary key (id),
			unique key uk1 (c1),
			unique key uk3 (c3),
			unique key uk9 (c9),
			key k3 (c3),
			unique key uk12 (c1, c2),
			unique key uk13 (c1, c3),
			unique key uk23 (c2, c3),
			unique key uk123 (c1, c2, c3),
			unique key uk21 (c2, c1),
			unique key ukv (v),
			unique key ukv3 (v(3)),
			unique key uk2v5 (c2, v(5)),
			unique key uk3v (c3, v)
		)
	`
	tcases := []struct {
		columns []string
		expect  []string
	}{
		{
			columns: []string{"id"},
			expect:  []string{"PRIMARY"},
		},
		{
			columns: []string{"c1"},
			expect:  []string{"uk1"},
		},
		{
			columns: []string{"id", "c1"},
			expect:  []string{"PRIMARY", "uk1"},
		},
		{
			columns: []string{"c1", "id"},
			expect:  []string{"PRIMARY", "uk1"},
		},
		{
			columns: []string{"c9"},
			expect:  []string{}, // nullable column
		},
		{
			columns: []string{"v"},
			expect:  []string{"ukv"},
		},
		{
			columns: []string{"v", "c9"},
			expect:  []string{"ukv"},
		},
		{
			columns: []string{"v", "c2"},
			expect:  []string{"ukv"},
		},
		{
			columns: []string{"v", "c2", "c3"},
			expect:  []string{"uk3", "uk23", "uk3v", "ukv"},
		},
		{
			columns: []string{"id", "c1", "c2", "c3", "v"},
			expect:  []string{"PRIMARY", "uk1", "uk3", "uk12", "uk13", "uk23", "uk21", "uk3v", "uk123", "ukv"},
		},
	}

	entity, err := NewCreateTableEntityFromSQL(env, table)
	require.NoError(t, err)
	err = entity.validate()
	require.NoError(t, err)
	tableColumns := entity.ColumnDefinitionEntitiesList()
	tableKeys := PrioritizedUniqueKeys(entity)
	assert.Equal(t, []string{
		"PRIMARY",
		"uk1",
		"uk3",
		"uk12",
		"uk13",
		"uk23",
		"uk21",
		"uk3v",
		"uk123",
		"ukv",
		"uk2v5",
		"ukv3",
		"uk9",
	}, tableKeys.Names())

	for _, tcase := range tcases {
		t.Run(strings.Join(tcase.columns, ","), func(t *testing.T) {
			columns := []*ColumnDefinitionEntity{}
			for _, tcaseCol := range tcase.columns {
				col := tableColumns.GetColumn(tcaseCol)
				require.NotNil(t, col)
				columns = append(columns, col)
			}
			columnsList := NewColumnDefinitionEntityList(columns)

			covered := IterationKeysByColumns(tableKeys, columnsList)
			assert.Equal(t, tcase.expect, covered.Names())
		})
	}
}

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
		{
			name:       "index with expression",
			fromSchema: "id int, primary key (id), key idx1 ((id + 1))",
			toSchema:   "id int, primary key (id), key idx2 ((id + 2))",
		},
		{
			name:       "remove unique index with expression, add another, skip both",
			fromSchema: "id int, i int, primary key (id), unique key idx1 ((id + 1))",
			toSchema:   "id int, i int, primary key (id), unique key idx2 ((i  + 2))",
		},
	}

	var (
		createTableWrapper = `CREATE TABLE t (%s)`
	)

	env := NewTestEnv()
	diffHints := &DiffHints{}
	for _, tcase := range testCases {
		t.Run(tcase.name, func(t *testing.T) {
			tcase.fromSchema = fmt.Sprintf(createTableWrapper, tcase.fromSchema)
			sourceTableEntity, err := NewCreateTableEntityFromSQL(env, tcase.fromSchema)
			require.NoError(t, err)

			tcase.toSchema = fmt.Sprintf(createTableWrapper, tcase.toSchema)
			targetTableEntity, err := NewCreateTableEntityFromSQL(env, tcase.toSchema)
			require.NoError(t, err)

			diff, err := sourceTableEntity.TableDiff(targetTableEntity, diffHints)
			require.NoError(t, err)
			alterTableAnalysis := OnlineDDLAlterTableAnalysis(diff.AlterTable())

			analysis, err := OnlineDDLMigrationTablesAnalysis(sourceTableEntity, targetTableEntity, alterTableAnalysis)
			require.NoError(t, err)

			toStringSlice := func(s string) []string {
				if s == "" {
					return []string{}
				}
				return strings.Split(s, ",")
			}
			assert.Equal(t, toStringSlice(tcase.removedForeignKeyNames), analysis.RemovedForeignKeyNames)
			assert.Equal(t, toStringSlice(tcase.removedUniqueKeyNames), analysis.RemovedUniqueKeys.Names())
			assert.Equal(t, toStringSlice(tcase.droppedNoDefaultColumnNames), analysis.DroppedNoDefaultColumns.Names())
			assert.Equal(t, toStringSlice(tcase.expandedColumnNames), analysis.ExpandedColumns.Names())
		})
	}
}

func TestValidateAndEditCreateTableStatement(t *testing.T) {
	tt := []struct {
		name                string
		query               string
		allowForeignKeys    bool
		expectError         string
		countConstraints    int
		expectConstraintMap map[string]string
	}{
		{
			name: "table with FK, not allowed",
			query: `
				create table onlineddl_test (
						id int auto_increment,
						i int not null,
						parent_id int not null,
						primary key(id),
						constraint test_ibfk foreign key (parent_id) references onlineddl_test_parent (id) on delete no action
					)
				`,
			expectError: ErrForeignKeyFound.Error(),
		},
		{
			name: "table with FK, allowed",
			query: `
				create table onlineddl_test (
						id int auto_increment,
						i int not null,
						parent_id int not null,
						primary key(id),
						constraint test_ibfk foreign key (parent_id) references onlineddl_test_parent (id) on delete no action
					)
				`,
			allowForeignKeys:    true,
			countConstraints:    1,
			expectConstraintMap: map[string]string{"test_ibfk": "test_ibfk_2wtivm6zk4lthpz14g9uoyaqk"},
		},
		{
			name: "table with default FK name, strip table name",
			query: `
			create table onlineddl_test (
					id int auto_increment,
					i int not null,
					parent_id int not null,
					primary key(id),
					constraint onlineddl_test_ibfk_1 foreign key (parent_id) references onlineddl_test_parent (id) on delete no action
				)
			`,
			allowForeignKeys: true,
			countConstraints: 1,
			// we want 'onlineddl_test_' to be stripped out:
			expectConstraintMap: map[string]string{"onlineddl_test_ibfk_1": "ibfk_1_2wtivm6zk4lthpz14g9uoyaqk"},
		},
		{
			name: "table with anonymous FK, allowed",
			query: `
				create table onlineddl_test (
						id int auto_increment,
						i int not null,
						parent_id int not null,
						primary key(id),
						foreign key (parent_id) references onlineddl_test_parent (id) on delete no action
					)
				`,
			allowForeignKeys:    true,
			countConstraints:    1,
			expectConstraintMap: map[string]string{"": "fk_2wtivm6zk4lthpz14g9uoyaqk"},
		},
		{
			name: "table with CHECK constraints",
			query: `
				create table onlineddl_test (
						id int auto_increment,
						i int not null,
						primary key(id),
						constraint check_1 CHECK ((i >= 0)),
						constraint check_2 CHECK ((i <> 5)),
						constraint check_3 CHECK ((i >= 0)),
						constraint chk_1111033c1d2d5908bf1f956ba900b192_check_4 CHECK ((i >= 0))
					)
				`,
			countConstraints: 4,
			expectConstraintMap: map[string]string{
				"check_1": "check_1_7dbssrkwdaxhdunwi5zj53q83",
				"check_2": "check_2_ehg3rtk6ejvbxpucimeess30o",
				"check_3": "check_3_0se0t8x98mf8v7lqmj2la8j9u",
				"chk_1111033c1d2d5908bf1f956ba900b192_check_4": "chk_1111033c1d2d5908bf1f956ba900b192_c_0c2c3bxi9jp4evqrct44wg3xh",
			},
		},
		{
			name: "table with both FOREIGN and CHECK constraints",
			query: `
				create table onlineddl_test (
						id int auto_increment,
						i int not null,
						primary key(id),
						constraint check_1 CHECK ((i >= 0)),
						constraint test_ibfk foreign key (parent_id) references onlineddl_test_parent (id) on delete no action,
						constraint chk_1111033c1d2d5908bf1f956ba900b192_check_4 CHECK ((i >= 0))
					)
				`,
			allowForeignKeys: true,
			countConstraints: 3,
			expectConstraintMap: map[string]string{
				"check_1": "check_1_7dbssrkwdaxhdunwi5zj53q83",
				"chk_1111033c1d2d5908bf1f956ba900b192_check_4": "chk_1111033c1d2d5908bf1f956ba900b192_c_0se0t8x98mf8v7lqmj2la8j9u",
				"test_ibfk": "test_ibfk_2wtivm6zk4lthpz14g9uoyaqk",
			},
		},
	}
	env := NewTestEnv()
	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := env.Parser().ParseStrictDDL(tc.query)
			require.NoError(t, err)
			createTable, ok := stmt.(*sqlparser.CreateTable)
			require.True(t, ok)

			table := "onlineddl_test"
			baseUUID := "a5a563da_dc1a_11ec_a416_0a43f95f28a3"
			constraintMap, err := ValidateAndEditCreateTableStatement(table, baseUUID, createTable, tc.allowForeignKeys)
			if tc.expectError != "" {
				assert.ErrorContains(t, err, tc.expectError)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tc.expectConstraintMap, constraintMap)

			uniqueConstraintNames := map[string]bool{}
			err = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
				switch node := node.(type) {
				case *sqlparser.ConstraintDefinition:
					uniqueConstraintNames[node.Name.String()] = true
				}
				return true, nil
			}, createTable)
			assert.NoError(t, err)
			assert.Equal(t, tc.countConstraints, len(uniqueConstraintNames))
			assert.Equalf(t, tc.countConstraints, len(constraintMap), "got contraints: %v", constraintMap)
		})
	}
}

func TestValidateAndEditAlterTableStatement(t *testing.T) {
	capableOf := func(capability capabilities.FlavorCapability) (bool, error) {
		switch capability {
		case
			capabilities.InstantDDLXtrabackupCapability,
			capabilities.InstantDDLFlavorCapability,
			capabilities.InstantAddLastColumnFlavorCapability,
			capabilities.InstantAddDropVirtualColumnFlavorCapability,
			capabilities.InstantAddDropColumnFlavorCapability,
			capabilities.InstantChangeColumnDefaultFlavorCapability,
			capabilities.InstantChangeColumnVisibilityCapability,
			capabilities.InstantExpandEnumCapability:
			return true, nil
		}
		return false, nil
	}
	incapableOf := func(capability capabilities.FlavorCapability) (bool, error) {
		return false, nil
	}

	tt := []struct {
		alter     string
		capableOf capabilities.CapableOf
		m         map[string]string
		expect    []string
	}{
		{
			alter:     "alter table t add column i int",
			capableOf: incapableOf,
			expect:    []string{"alter table t add column i int, algorithm = copy"},
		},
		{
			alter:     "alter table t add column i int",
			capableOf: capableOf,
			expect:    []string{"alter table t add column i int"},
		},
		{
			alter:  "alter table t add column i int, add fulltext key name1_ft (name1)",
			expect: []string{"alter table t add column i int, add fulltext key name1_ft (name1)"},
		},
		{
			alter:  "alter table t add column i int, add fulltext key name1_ft (name1), add fulltext key name2_ft (name2)",
			expect: []string{"alter table t add column i int, add fulltext key name1_ft (name1)", "alter table t add fulltext key name2_ft (name2)"},
		},
		{
			alter:  "alter table t add fulltext key name0_ft (name0), add column i int, add fulltext key name1_ft (name1), add fulltext key name2_ft (name2)",
			expect: []string{"alter table t add fulltext key name0_ft (name0), add column i int", "alter table t add fulltext key name1_ft (name1)", "alter table t add fulltext key name2_ft (name2)"},
		},
		{
			alter:  "alter table t add constraint check (id != 1)",
			expect: []string{"alter table t add constraint chk_aulpn7bjeortljhguy86phdn9 check (id != 1)"},
		},
		{
			alter:  "alter table t add constraint t_chk_1 check (id != 1)",
			expect: []string{"alter table t add constraint chk_1_aulpn7bjeortljhguy86phdn9 check (id != 1)"},
		},
		{
			alter:  "alter table t add constraint some_check check (id != 1)",
			expect: []string{"alter table t add constraint some_check_aulpn7bjeortljhguy86phdn9 check (id != 1)"},
		},
		{
			alter:  "alter table t add constraint some_check check (id != 1), add constraint another_check check (id != 2)",
			expect: []string{"alter table t add constraint some_check_aulpn7bjeortljhguy86phdn9 check (id != 1), add constraint another_check_4fa197273p3w96267pzm3gfi3 check (id != 2)"},
		},
		{
			alter:  "alter table t add foreign key (parent_id) references onlineddl_test_parent (id) on delete no action",
			expect: []string{"alter table t add constraint fk_6fmhzdlya89128u5j3xapq34i foreign key (parent_id) references onlineddl_test_parent (id) on delete no action"},
		},
		{
			alter:  "alter table t add constraint myfk foreign key (parent_id) references onlineddl_test_parent (id) on delete no action",
			expect: []string{"alter table t add constraint myfk_6fmhzdlya89128u5j3xapq34i foreign key (parent_id) references onlineddl_test_parent (id) on delete no action"},
		},
		{
			// strip out table name
			alter:  "alter table t add constraint t_ibfk_1 foreign key (parent_id) references onlineddl_test_parent (id) on delete no action",
			expect: []string{"alter table t add constraint ibfk_1_6fmhzdlya89128u5j3xapq34i foreign key (parent_id) references onlineddl_test_parent (id) on delete no action"},
		},
		{
			// stript out table name
			alter:  "alter table t add constraint t_ibfk_1 foreign key (parent_id) references onlineddl_test_parent (id) on delete no action",
			expect: []string{"alter table t add constraint ibfk_1_6fmhzdlya89128u5j3xapq34i foreign key (parent_id) references onlineddl_test_parent (id) on delete no action"},
		},
		{
			alter:  "alter table t add constraint t_ibfk_1 foreign key (parent_id) references onlineddl_test_parent (id) on delete no action, add constraint some_check check (id != 1)",
			expect: []string{"alter table t add constraint ibfk_1_6fmhzdlya89128u5j3xapq34i foreign key (parent_id) references onlineddl_test_parent (id) on delete no action, add constraint some_check_aulpn7bjeortljhguy86phdn9 check (id != 1)"},
		},
		{
			alter: "alter table t drop foreign key t_ibfk_1",
			m: map[string]string{
				"t_ibfk_1": "ibfk_1_aaaaaaaaaaaaaa",
			},
			expect: []string{"alter table t drop foreign key ibfk_1_aaaaaaaaaaaaaa"},
		},
	}

	env := NewTestEnv()
	for _, tc := range tt {
		t.Run(tc.alter, func(t *testing.T) {
			if tc.capableOf == nil {
				tc.capableOf = capableOf
			}
			stmt, err := env.Parser().ParseStrictDDL(tc.alter)
			require.NoError(t, err)
			alterTable, ok := stmt.(*sqlparser.AlterTable)
			require.True(t, ok)

			m := map[string]string{}
			for k, v := range tc.m {
				m[k] = v
			}
			baseUUID := "a5a563da_dc1a_11ec_a416_0a43f95f28a3"
			tableName := "t"
			alters, err := ValidateAndEditAlterTableStatement(tableName, baseUUID, tc.capableOf, alterTable, m)
			assert.NoError(t, err)
			var altersStrings []string
			for _, alter := range alters {
				altersStrings = append(altersStrings, sqlparser.String(alter))
			}
			assert.Equal(t, tc.expect, altersStrings)
		})
	}
}

func TestAddInstantAlgorithm(t *testing.T) {
	tt := []struct {
		alter  string
		expect string
	}{
		{
			alter:  "alter table t add column i2 int not null",
			expect: "ALTER TABLE `t` ADD COLUMN `i2` int NOT NULL, ALGORITHM = INSTANT",
		},
		{
			alter:  "alter table t add column i2 int not null, lock=none",
			expect: "ALTER TABLE `t` ADD COLUMN `i2` int NOT NULL, LOCK NONE, ALGORITHM = INSTANT",
		},
		{
			alter:  "alter table t add column i2 int not null, algorithm=inplace",
			expect: "ALTER TABLE `t` ADD COLUMN `i2` int NOT NULL, ALGORITHM = INSTANT",
		},
		{
			alter:  "alter table t add column i2 int not null, algorithm=inplace, lock=none",
			expect: "ALTER TABLE `t` ADD COLUMN `i2` int NOT NULL, ALGORITHM = INSTANT, LOCK NONE",
		},
	}
	env := NewTestEnv()
	for _, tc := range tt {
		t.Run(tc.alter, func(t *testing.T) {
			stmt, err := env.Parser().ParseStrictDDL(tc.alter)
			require.NoError(t, err)
			alterTable, ok := stmt.(*sqlparser.AlterTable)
			require.True(t, ok)

			AddInstantAlgorithm(alterTable)
			alterInstant := sqlparser.CanonicalString(alterTable)

			assert.Equal(t, tc.expect, alterInstant)

			stmt, err = env.Parser().ParseStrictDDL(alterInstant)
			require.NoError(t, err)
			_, ok = stmt.(*sqlparser.AlterTable)
			require.True(t, ok)
		})
	}
}

func TestDuplicateCreateTable(t *testing.T) {
	baseUUID := "a5a563da_dc1a_11ec_a416_0a43f95f28a3"
	allowForeignKeys := true

	tcases := []struct {
		sql           string
		newName       string
		expectSQL     string
		expectMapSize int
	}{
		{
			sql:       "create table t (id int primary key)",
			newName:   "mytable",
			expectSQL: "create table mytable (\n\tid int primary key\n)",
		},
		{
			sql:           "create table t (id int primary key, i int, constraint f foreign key (i) references parent (id) on delete cascade)",
			newName:       "mytable",
			expectSQL:     "create table mytable (\n\tid int primary key,\n\ti int,\n\tconstraint f_ewl7lthyax2xxocpib3hyyvxx foreign key (i) references parent (id) on delete cascade\n)",
			expectMapSize: 1,
		},
		{
			sql:           "create table self (id int primary key, i int, constraint f foreign key (i) references self (id))",
			newName:       "mytable",
			expectSQL:     "create table mytable (\n\tid int primary key,\n\ti int,\n\tconstraint f_6tlv90d9gcf4h6roalfnkdhar foreign key (i) references mytable (id)\n)",
			expectMapSize: 1,
		},
		{
			sql:     "create table self (id int primary key, i1 int, i2 int, constraint f1 foreign key (i1) references self (id), constraint f1 foreign key (i2) references parent (id))",
			newName: "mytable",
			expectSQL: `create table mytable (
	id int primary key,
	i1 int,
	i2 int,
	constraint f1_95jpox7sx4w0cv7dpspzmjbxu foreign key (i1) references mytable (id),
	constraint f1_1fg002b1cuqoavgti5zp8pu91 foreign key (i2) references parent (id)
)`,
			expectMapSize: 1,
		},
	}
	env := NewTestEnv()
	for _, tcase := range tcases {
		t.Run(tcase.sql, func(t *testing.T) {
			stmt, err := env.Parser().ParseStrictDDL(tcase.sql)
			require.NoError(t, err)
			originalCreateTable, ok := stmt.(*sqlparser.CreateTable)
			require.True(t, ok)
			require.NotNil(t, originalCreateTable)
			newCreateTable, constraintMap, err := DuplicateCreateTable(originalCreateTable, baseUUID, tcase.newName, allowForeignKeys)
			assert.NoError(t, err)
			assert.NotNil(t, newCreateTable)
			assert.NotNil(t, constraintMap)

			newSQL := sqlparser.String(newCreateTable)
			assert.Equal(t, tcase.expectSQL, newSQL)
			assert.Equal(t, tcase.expectMapSize, len(constraintMap))
		})
	}
}
