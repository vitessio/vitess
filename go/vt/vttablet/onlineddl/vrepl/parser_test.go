/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package vrepl

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseAlterStatement(t *testing.T) {
	statement := "add column t int, engine=innodb"
	parser := NewAlterTableParser()
	err := parser.ParseAlterStatement(statement)
	assert.NoError(t, err)
	assert.Equal(t, parser.alterStatementOptions, statement)
	assert.False(t, parser.HasNonTrivialRenames())
	assert.False(t, parser.IsAutoIncrementDefined())
}

func TestParseAlterStatementTrivialRename(t *testing.T) {
	statement := "add column t int, change ts ts timestamp, engine=innodb"
	parser := NewAlterTableParser()
	err := parser.ParseAlterStatement(statement)
	assert.NoError(t, err)
	assert.Equal(t, parser.alterStatementOptions, statement)
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
		"add column t int, change ts ts timestamp, auto_increment=7 engine=innodb",
		"add column t int, change ts ts timestamp, auto_increment =7 engine=innodb",
		"add column t int, change ts ts timestamp, AUTO_INCREMENT = 7 engine=innodb",
		"add column t int, change ts ts timestamp, engine=innodb auto_increment=73425",
	}
	for _, statement := range statements {
		parser := NewAlterTableParser()
		err := parser.ParseAlterStatement(statement)
		assert.NoError(t, err)
		assert.Equal(t, parser.alterStatementOptions, statement)
		assert.True(t, parser.IsAutoIncrementDefined())
	}
}

func TestParseAlterStatementTrivialRenames(t *testing.T) {
	statement := "add column t int, change ts ts timestamp, CHANGE f `f` float, engine=innodb"
	parser := NewAlterTableParser()
	err := parser.ParseAlterStatement(statement)
	assert.NoError(t, err)
	assert.Equal(t, parser.alterStatementOptions, statement)
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
		parser := NewAlterTableParser()
		err := parser.ParseAlterStatement(statement)
		assert.NoError(t, err)
		assert.False(t, parser.IsAutoIncrementDefined())
		assert.Equal(t, parser.alterStatementOptions, statement)
		renames := parser.GetNonTrivialRenames()
		assert.Equal(t, len(renames), 2)
		assert.Equal(t, renames["i"], "count")
		assert.Equal(t, renames["f"], "fl")
	}
}

func TestTokenizeAlterStatement(t *testing.T) {
	parser := NewAlterTableParser()
	{
		alterStatement := "add column t int"
		tokens, _ := parser.tokenizeAlterStatement(alterStatement)
		assert.True(t, reflect.DeepEqual(tokens, []string{"add column t int"}))
	}
	{
		alterStatement := "add column t int, change column i int"
		tokens, _ := parser.tokenizeAlterStatement(alterStatement)
		assert.True(t, reflect.DeepEqual(tokens, []string{"add column t int", "change column i int"}))
	}
	{
		alterStatement := "add column t int, change column i int 'some comment'"
		tokens, _ := parser.tokenizeAlterStatement(alterStatement)
		assert.True(t, reflect.DeepEqual(tokens, []string{"add column t int", "change column i int 'some comment'"}))
	}
	{
		alterStatement := "add column t int, change column i int 'some comment, with comma'"
		tokens, _ := parser.tokenizeAlterStatement(alterStatement)
		assert.True(t, reflect.DeepEqual(tokens, []string{"add column t int", "change column i int 'some comment, with comma'"}))
	}
	{
		alterStatement := "add column t int, add column d decimal(10,2)"
		tokens, _ := parser.tokenizeAlterStatement(alterStatement)
		assert.True(t, reflect.DeepEqual(tokens, []string{"add column t int", "add column d decimal(10,2)"}))
	}
	{
		alterStatement := "add column t int, add column e enum('a','b','c')"
		tokens, _ := parser.tokenizeAlterStatement(alterStatement)
		assert.True(t, reflect.DeepEqual(tokens, []string{"add column t int", "add column e enum('a','b','c')"}))
	}
	{
		alterStatement := "add column t int(11), add column e enum('a','b','c')"
		tokens, _ := parser.tokenizeAlterStatement(alterStatement)
		assert.True(t, reflect.DeepEqual(tokens, []string{"add column t int(11)", "add column e enum('a','b','c')"}))
	}
}

func TestSanitizeQuotesFromAlterStatement(t *testing.T) {
	parser := NewAlterTableParser()
	{
		alterStatement := "add column e enum('a','b','c')"
		strippedStatement := parser.sanitizeQuotesFromAlterStatement(alterStatement)
		assert.Equal(t, strippedStatement, "add column e enum('','','')")
	}
	{
		alterStatement := "change column i int 'some comment, with comma'"
		strippedStatement := parser.sanitizeQuotesFromAlterStatement(alterStatement)
		assert.Equal(t, strippedStatement, "change column i int ''")
	}
}

func TestParseAlterStatementDroppedColumns(t *testing.T) {

	{
		parser := NewAlterTableParser()
		statement := "drop column b"
		err := parser.ParseAlterStatement(statement)
		assert.NoError(t, err)
		assert.Equal(t, len(parser.droppedColumns), 1)
		assert.True(t, parser.droppedColumns["b"])
	}
	{
		parser := NewAlterTableParser()
		statement := "drop column b, drop key c_idx, drop column `d`"
		err := parser.ParseAlterStatement(statement)
		assert.NoError(t, err)
		assert.Equal(t, parser.alterStatementOptions, statement)
		assert.Equal(t, len(parser.droppedColumns), 2)
		assert.True(t, parser.droppedColumns["b"])
		assert.True(t, parser.droppedColumns["d"])
	}
	{
		parser := NewAlterTableParser()
		statement := "drop column b, drop key c_idx, drop column `d`, drop `e`, drop primary key, drop foreign key fk_1"
		err := parser.ParseAlterStatement(statement)
		assert.NoError(t, err)
		assert.Equal(t, len(parser.droppedColumns), 3)
		assert.True(t, parser.droppedColumns["b"])
		assert.True(t, parser.droppedColumns["d"])
		assert.True(t, parser.droppedColumns["e"])
	}
	{
		parser := NewAlterTableParser()
		statement := "drop column b, drop bad statement, add column i int"
		err := parser.ParseAlterStatement(statement)
		assert.NoError(t, err)
		assert.Equal(t, len(parser.droppedColumns), 1)
		assert.True(t, parser.droppedColumns["b"])
	}
}

func TestParseAlterStatementRenameTable(t *testing.T) {

	{
		parser := NewAlterTableParser()
		statement := "drop column b"
		err := parser.ParseAlterStatement(statement)
		assert.NoError(t, err)
		assert.False(t, parser.isRenameTable)
	}
	{
		parser := NewAlterTableParser()
		statement := "rename as something_else"
		err := parser.ParseAlterStatement(statement)
		assert.NoError(t, err)
		assert.True(t, parser.isRenameTable)
	}
	{
		parser := NewAlterTableParser()
		statement := "drop column b, rename as something_else"
		err := parser.ParseAlterStatement(statement)
		assert.NoError(t, err)
		assert.Equal(t, parser.alterStatementOptions, statement)
		assert.True(t, parser.isRenameTable)
	}
	{
		parser := NewAlterTableParser()
		statement := "engine=innodb rename as something_else"
		err := parser.ParseAlterStatement(statement)
		assert.NoError(t, err)
		assert.True(t, parser.isRenameTable)
	}
	{
		parser := NewAlterTableParser()
		statement := "rename as something_else, engine=innodb"
		err := parser.ParseAlterStatement(statement)
		assert.NoError(t, err)
		assert.True(t, parser.isRenameTable)
	}
}

func TestParseAlterStatementExplicitTable(t *testing.T) {

	{
		parser := NewAlterTableParser()
		statement := "drop column b"
		err := parser.ParseAlterStatement(statement)
		assert.NoError(t, err)
		assert.Equal(t, parser.explicitSchema, "")
		assert.Equal(t, parser.explicitTable, "")
		assert.Equal(t, parser.alterStatementOptions, "drop column b")
		assert.True(t, reflect.DeepEqual(parser.alterTokens, []string{"drop column b"}))
	}
	{
		parser := NewAlterTableParser()
		statement := "alter table tbl drop column b"
		err := parser.ParseAlterStatement(statement)
		assert.NoError(t, err)
		assert.Equal(t, parser.explicitSchema, "")
		assert.Equal(t, parser.explicitTable, "tbl")
		assert.Equal(t, parser.alterStatementOptions, "drop column b")
		assert.True(t, reflect.DeepEqual(parser.alterTokens, []string{"drop column b"}))
	}
	{
		parser := NewAlterTableParser()
		statement := "alter table `tbl` drop column b"
		err := parser.ParseAlterStatement(statement)
		assert.NoError(t, err)
		assert.Equal(t, parser.explicitSchema, "")
		assert.Equal(t, parser.explicitTable, "tbl")
		assert.Equal(t, parser.alterStatementOptions, "drop column b")
		assert.True(t, reflect.DeepEqual(parser.alterTokens, []string{"drop column b"}))
	}
	{
		parser := NewAlterTableParser()
		statement := "alter table `scm with spaces`.`tbl` drop column b"
		err := parser.ParseAlterStatement(statement)
		assert.NoError(t, err)
		assert.Equal(t, parser.explicitSchema, "scm with spaces")
		assert.Equal(t, parser.explicitTable, "tbl")
		assert.Equal(t, parser.alterStatementOptions, "drop column b")
		assert.True(t, reflect.DeepEqual(parser.alterTokens, []string{"drop column b"}))
	}
	{
		parser := NewAlterTableParser()
		statement := "alter table `scm`.`tbl with spaces` drop column b"
		err := parser.ParseAlterStatement(statement)
		assert.NoError(t, err)
		assert.Equal(t, parser.explicitSchema, "scm")
		assert.Equal(t, parser.explicitTable, "tbl with spaces")
		assert.Equal(t, parser.alterStatementOptions, "drop column b")
		assert.True(t, reflect.DeepEqual(parser.alterTokens, []string{"drop column b"}))
	}
	{
		parser := NewAlterTableParser()
		statement := "alter table `scm`.tbl drop column b"
		err := parser.ParseAlterStatement(statement)
		assert.NoError(t, err)
		assert.Equal(t, parser.explicitSchema, "scm")
		assert.Equal(t, parser.explicitTable, "tbl")
		assert.Equal(t, parser.alterStatementOptions, "drop column b")
		assert.True(t, reflect.DeepEqual(parser.alterTokens, []string{"drop column b"}))
	}
	{
		parser := NewAlterTableParser()
		statement := "alter table scm.`tbl` drop column b"
		err := parser.ParseAlterStatement(statement)
		assert.NoError(t, err)
		assert.Equal(t, parser.explicitSchema, "scm")
		assert.Equal(t, parser.explicitTable, "tbl")
		assert.Equal(t, parser.alterStatementOptions, "drop column b")
		assert.True(t, reflect.DeepEqual(parser.alterTokens, []string{"drop column b"}))
	}
	{
		parser := NewAlterTableParser()
		statement := "alter table scm.tbl drop column b"
		err := parser.ParseAlterStatement(statement)
		assert.NoError(t, err)
		assert.Equal(t, parser.explicitSchema, "scm")
		assert.Equal(t, parser.explicitTable, "tbl")
		assert.Equal(t, parser.alterStatementOptions, "drop column b")
		assert.True(t, reflect.DeepEqual(parser.alterTokens, []string{"drop column b"}))
	}
	{
		parser := NewAlterTableParser()
		statement := "alter table scm.tbl drop column b, add index idx(i)"
		err := parser.ParseAlterStatement(statement)
		assert.NoError(t, err)
		assert.Equal(t, parser.explicitSchema, "scm")
		assert.Equal(t, parser.explicitTable, "tbl")
		assert.Equal(t, parser.alterStatementOptions, "drop column b, add index idx(i)")
		assert.True(t, reflect.DeepEqual(parser.alterTokens, []string{"drop column b", "add index idx(i)"}))
	}
}
