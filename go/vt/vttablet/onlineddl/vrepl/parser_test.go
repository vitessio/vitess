/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/
/*
Copyright 2021 The Vitess Authors.

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

package vrepl

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"
)

func alterTableStatement(t *testing.T, sql string) *sqlparser.AlterTable {
	stmt, err := sqlparser.NewTestParser().ParseStrictDDL(sql)
	require.NoError(t, err)
	alter, ok := stmt.(*sqlparser.AlterTable)
	require.True(t, ok)
	return alter
}

func TestParseAlterStatement(t *testing.T) {
	statement := "alter table t add column t int, engine=innodb"
	alterStatement := alterTableStatement(t, statement)
	parser := NewAlterTableParser()
	parser.AnalyzeAlter(alterStatement)
	assert.False(t, parser.HasNonTrivialRenames())
	assert.False(t, parser.IsAutoIncrementDefined())
}

func TestParseAlterStatementTrivialRename(t *testing.T) {
	statement := "alter table t add column t int, change ts ts timestamp, engine=innodb"
	alterStatement := alterTableStatement(t, statement)
	parser := NewAlterTableParser()
	parser.AnalyzeAlter(alterStatement)
	assert.False(t, parser.HasNonTrivialRenames())
	assert.False(t, parser.IsAutoIncrementDefined())
	assert.Equal(t, len(parser.columnRenameMap), 1)
	assert.Equal(t, parser.columnRenameMap["ts"], "ts")
}

func TestParseAlterStatementWithAutoIncrement(t *testing.T) {
	statements := []string{
		"auto_increment=7",
		"auto_increment = 7",
		"AUTO_INCREMENT = 71",
		"AUTO_INCREMENT   23",
		"AUTO_INCREMENT 23",
		"add column t int, change ts ts timestamp, auto_increment=7 engine=innodb",
		"add column t int, change ts ts timestamp, auto_increment =7 engine=innodb",
		"add column t int, change ts ts timestamp, AUTO_INCREMENT = 7 engine=innodb",
		"add column t int, change ts ts timestamp, engine=innodb auto_increment=73425",
		"add column t int, change ts ts timestamp, engine=innodb, auto_increment=73425",
		"add column t int, change ts ts timestamp, engine=innodb, auto_increment 73425",
		"add column t int, change ts ts timestamp, engine innodb, auto_increment 73425",
		"add column t int, change ts ts timestamp, engine innodb auto_increment 73425",
	}
	for _, statement := range statements {
		parser := NewAlterTableParser()
		statement := "alter table t " + statement
		alterStatement := alterTableStatement(t, statement)
		parser.AnalyzeAlter(alterStatement)
		assert.True(t, parser.IsAutoIncrementDefined())
	}
}

func TestParseAlterStatementTrivialRenames(t *testing.T) {
	statement := "alter table t  add column t int, change ts ts timestamp, CHANGE f `f` float, engine=innodb"
	alterStatement := alterTableStatement(t, statement)
	parser := NewAlterTableParser()
	parser.AnalyzeAlter(alterStatement)
	assert.False(t, parser.HasNonTrivialRenames())
	assert.False(t, parser.IsAutoIncrementDefined())
	assert.Equal(t, len(parser.columnRenameMap), 2)
	assert.Equal(t, parser.columnRenameMap["ts"], "ts")
	assert.Equal(t, parser.columnRenameMap["f"], "f")
}

func TestParseAlterStatementNonTrivial(t *testing.T) {
	statements := []string{
		`add column b bigint, change f fl float, change i count int, engine=innodb`,
		"add column b bigint, change column `f` fl float, change `i` `count` int, engine=innodb",
		"add column b bigint, change column `f` fl float, change `i` `count` int, change ts ts timestamp, engine=innodb",
		`change
		  f fl float,
			CHANGE COLUMN i
			  count int, engine=innodb`,
	}

	for _, statement := range statements {
		statement := "alter table t " + statement
		alterStatement := alterTableStatement(t, statement)
		parser := NewAlterTableParser()
		parser.AnalyzeAlter(alterStatement)
		assert.False(t, parser.IsAutoIncrementDefined())
		renames := parser.GetNonTrivialRenames()
		assert.Equal(t, len(renames), 2)
		assert.Equal(t, renames["i"], "count")
		assert.Equal(t, renames["f"], "fl")
	}
}

func TestParseAlterStatementDroppedColumns(t *testing.T) {
	{
		parser := NewAlterTableParser()
		statement := "alter table t drop column b"
		alterStatement := alterTableStatement(t, statement)
		parser.AnalyzeAlter(alterStatement)
		assert.Equal(t, len(parser.droppedColumns), 1)
		assert.True(t, parser.droppedColumns["b"])
	}
	{
		parser := NewAlterTableParser()
		statement := "alter table t drop column b, drop key c_idx, drop column `d`"
		alterStatement := alterTableStatement(t, statement)
		parser.AnalyzeAlter(alterStatement)
		assert.Equal(t, len(parser.droppedColumns), 2)
		assert.True(t, parser.droppedColumns["b"])
		assert.True(t, parser.droppedColumns["d"])
	}
	{
		parser := NewAlterTableParser()
		statement := "alter table t drop column b, drop key c_idx, drop column `d`, drop `e`, drop primary key, drop foreign key fk_1"
		alterStatement := alterTableStatement(t, statement)
		parser.AnalyzeAlter(alterStatement)
		assert.Equal(t, len(parser.droppedColumns), 3)
		assert.True(t, parser.droppedColumns["b"])
		assert.True(t, parser.droppedColumns["d"])
		assert.True(t, parser.droppedColumns["e"])
	}
}

func TestParseAlterStatementRenameTable(t *testing.T) {
	tt := []struct {
		alter    string
		isRename bool
	}{
		{
			alter: "alter table t drop column b",
		},
		{
			alter:    "alter table t rename as something_else",
			isRename: true,
		},
		{
			alter:    "alter table t rename to something_else",
			isRename: true,
		},
		{
			alter:    "alter table t drop column b, rename as something_else",
			isRename: true,
		},
		{
			alter:    "alter table t engine=innodb, rename as something_else",
			isRename: true,
		},
		{
			alter:    "alter table t rename as something_else, engine=innodb",
			isRename: true,
		},
	}
	for _, tc := range tt {
		t.Run(tc.alter, func(t *testing.T) {
			parser := NewAlterTableParser()
			alterStatement := alterTableStatement(t, tc.alter)
			parser.AnalyzeAlter(alterStatement)
			assert.Equal(t, tc.isRename, parser.isRenameTable)
		})
	}
}
