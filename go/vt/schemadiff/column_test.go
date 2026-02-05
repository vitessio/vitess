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
	"testing"

	"golang.org/x/exp/maps"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestColumnFunctions(t *testing.T) {
	table := `
	create table t (
		id int,
		col1 int,
		col2 int not null,
		col3 int default null,
		col4 int default 1,
		COL5 int default 1,
		ts1 timestamp,
		ts2 timestamp(3) null,
		ts3 timestamp(6) not null,
		primary key (id)
	)`
	env := NewTestEnv()
	createTableEntity, err := NewCreateTableEntityFromSQL(env, table)
	require.NoError(t, err)
	m := createTableEntity.ColumnDefinitionEntitiesMap()
	for _, col := range m {
		col.SetExplicitDefaultAndNull()
		err := col.SetExplicitCharsetCollate()
		require.NoError(t, err)
	}

	t.Run("nullable", func(t *testing.T) {
		assert.False(t, m["id"].IsNullable())
		assert.True(t, m["col1"].IsNullable())
		assert.False(t, m["col2"].IsNullable())
		assert.True(t, m["col3"].IsNullable())
		assert.True(t, m["col4"].IsNullable())
		assert.True(t, m["col5"].IsNullable())
		assert.True(t, m["ts1"].IsNullable())
		assert.True(t, m["ts2"].IsNullable())
		assert.False(t, m["ts3"].IsNullable())
	})
	t.Run("default null", func(t *testing.T) {
		assert.False(t, m["id"].IsDefaultNull())
		assert.True(t, m["col1"].IsDefaultNull())
		assert.False(t, m["col2"].IsDefaultNull())
		assert.True(t, m["col3"].IsDefaultNull())
		assert.False(t, m["col4"].IsDefaultNull())
		assert.False(t, m["col5"].IsDefaultNull())
		assert.True(t, m["ts1"].IsDefaultNull())
		assert.True(t, m["ts2"].IsDefaultNull())
		assert.False(t, m["ts3"].IsDefaultNull())
	})
	t.Run("has default", func(t *testing.T) {
		assert.False(t, m["id"].HasDefault())
		assert.True(t, m["col1"].HasDefault())
		assert.False(t, m["col2"].HasDefault())
		assert.True(t, m["col3"].HasDefault())
		assert.True(t, m["col4"].HasDefault())
		assert.True(t, m["col5"].HasDefault())
		assert.True(t, m["ts1"].HasDefault())
		assert.True(t, m["ts2"].HasDefault())
		assert.False(t, m["ts3"].HasDefault())
	})
}

func TestExpands(t *testing.T) {
	tcases := []struct {
		source  string
		target  string
		expands bool
		msg     string
	}{
		{
			source: "int",
			target: "int",
		},
		{
			source: "int",
			target: "smallint",
		},
		{
			source: "int",
			target: "smallint unsigned",
		},
		{
			source:  "int unsigned",
			target:  "tinyint",
			expands: true,
			msg:     "source is unsigned, target is signed",
		},
		{
			source:  "int unsigned",
			target:  "tinyint signed",
			expands: true,
			msg:     "source is unsigned, target is signed",
		},
		{
			source: "int",
			target: "tinyint",
		},
		{
			source:  "int",
			target:  "bigint",
			expands: true,
			msg:     "increased integer range",
		},
		{
			source:  "int",
			target:  "bigint unsigned",
			expands: true,
			msg:     "increased integer range",
		},
		{
			source:  "int",
			target:  "int unsigned",
			expands: true,
			msg:     "target unsigned value exceeds source unsigned value",
		},
		{
			source:  "int unsigned",
			target:  "int",
			expands: true,
			msg:     "source is unsigned, target is signed",
		},
		{
			source: "int",
			target: "int default null",
		},
		{
			source: "int default null",
			target: "int",
		},
		{
			source: "int",
			target: "int not null",
		},
		{
			source:  "int not null",
			target:  "int",
			expands: true,
			msg:     "target is NULL-able, source is not",
		},
		{
			source:  "int not null",
			target:  "int default null",
			expands: true,
			msg:     "target is NULL-able, source is not",
		},
		{
			source: "float",
			target: "int",
		},
		{
			source:  "int",
			target:  "float",
			expands: true,
			msg:     "target is floating point, source is not",
		},
		{
			source:  "float",
			target:  "double",
			expands: true,
			msg:     "increased floating point range",
		},
		{
			source:  "decimal(5,2)",
			target:  "float",
			expands: true,
			msg:     "target is floating point, source is not",
		},
		{
			source:  "int",
			target:  "decimal",
			expands: true,
			msg:     "target is decimal, source is not",
		},
		{
			source:  "int",
			target:  "decimal(5,2)",
			expands: true,
			msg:     "increased length",
		},
		{
			source:  "int",
			target:  "decimal(5,0)",
			expands: true,
			msg:     "increased length",
		},
		{
			source: "decimal(5,2)", // 123.45
			target: "decimal(3,2)", // 1.23
		},
		{
			source: "decimal(5,2)", // 123.45
			target: "decimal(4,1)", // 123.4
		},
		{
			source:  "decimal(5,2)", // 123.45
			target:  "decimal(5,1)", // 1234.5
			expands: true,
			msg:     "increased decimal range",
		},
		{
			source: "char(7)",
			target: "char(7)",
		},
		{
			source: "char(7)",
			target: "varchar(7)",
		},
		{
			source: "char(7)",
			target: "varchar(5)",
		},
		{
			source:  "char(5)",
			target:  "varchar(7)",
			expands: true,
			msg:     "increased length",
		},
		{
			source:  "varchar(5)",
			target:  "char(7)",
			expands: true,
			msg:     "increased length",
		},
		{
			source: "tinytext",
			target: "tinytext",
		},
		{
			source: "tinytext",
			target: "tinyblob",
		},
		{
			source: "mediumtext",
			target: "tinytext",
		},
		{
			source: "mediumblob",
			target: "tinytext",
		},
		{
			source:  "tinytext",
			target:  "text",
			expands: true,
			msg:     "increased blob range",
		},
		{
			source:  "tinytext",
			target:  "mediumblob",
			expands: true,
			msg:     "increased blob range",
		},
		{
			source: "timestamp",
			target: "timestamp",
		},
		{
			source: "timestamp",
			target: "time",
		},
		{
			source: "datetime",
			target: "timestamp",
		},
		{
			source: "datetime",
			target: "date",
		},
		{
			source:  "time",
			target:  "timestamp",
			expands: true,
			msg:     "target is expanded data type of source",
		},
		{
			source:  "timestamp",
			target:  "datetime",
			expands: true,
			msg:     "target is expanded data type of source",
		},
		{
			source:  "date",
			target:  "datetime",
			expands: true,
			msg:     "target is expanded data type of source",
		},
		{
			source:  "timestamp",
			target:  "timestamp(3)",
			expands: true,
			msg:     "increased length",
		},
		{
			source:  "timestamp",
			target:  "timestamp(6)",
			expands: true,
			msg:     "increased length",
		},
		{
			source:  "timestamp(3)",
			target:  "timestamp(6)",
			expands: true,
			msg:     "increased length",
		},
		{
			source: "timestamp(6)",
			target: "timestamp(3)",
		},
		{
			source: "timestamp(6)",
			target: "timestamp",
		},
		{
			source:  "timestamp",
			target:  "time(3)",
			expands: true,
			msg:     "increased length",
		},
		{
			source:  "datetime",
			target:  "time(3)",
			expands: true,
			msg:     "increased length",
		},
		{
			source: "enum('a','b')",
			target: "enum('a','b')",
		},
		{
			source: "enum('a','b')",
			target: "enum('a')",
		},
		{
			source:  "enum('a','b')",
			target:  "enum('b')",
			expands: true,
			msg:     "target enum/set expands or reorders source enum/set",
		},
		{
			source:  "enum('a','b')",
			target:  "enum('a','b','c')",
			expands: true,
			msg:     "target enum/set expands or reorders source enum/set",
		},
		{
			source:  "enum('a','b')",
			target:  "enum('a','x')",
			expands: true,
			msg:     "target enum/set expands or reorders source enum/set",
		},
		{
			source: "set('a','b')",
			target: "set('a','b')",
		},
		{
			source:  "set('a','b')",
			target:  "set('a','b','c')",
			expands: true,
			msg:     "target enum/set expands or reorders source enum/set",
		},
	}
	env := NewTestEnv()
	for _, tcase := range tcases {
		t.Run(tcase.source+" -> "+tcase.target, func(t *testing.T) {
			fromCreateTableSQL := fmt.Sprintf("create table t (col %s)", tcase.source)
			from, err := NewCreateTableEntityFromSQL(env, fromCreateTableSQL)
			require.NoError(t, err)

			toCreateTableSQL := fmt.Sprintf("create table t (col %s)", tcase.target)
			to, err := NewCreateTableEntityFromSQL(env, toCreateTableSQL)
			require.NoError(t, err)

			require.Len(t, from.ColumnDefinitionEntities(), 1)
			fromCol := from.ColumnDefinitionEntities()[0]
			require.Len(t, to.ColumnDefinitionEntities(), 1)
			toCol := to.ColumnDefinitionEntities()[0]

			expands, message := ColumnChangeExpandsDataRange(fromCol, toCol)
			assert.Equal(t, tcase.expands, expands, message)
			if expands {
				require.NotEmpty(t, tcase.msg, message)
			}
			assert.Contains(t, message, tcase.msg)
		})
	}
}

func TestColumnDefinitionEntityList(t *testing.T) {
	table := `
	create table t (
		id int,
		col1 int,
		Col2 int not null,
		primary key (id)
	)`
	env := NewTestEnv()
	createTableEntity, err := NewCreateTableEntityFromSQL(env, table)
	require.NoError(t, err)
	entities := createTableEntity.ColumnDefinitionEntities()
	require.NotEmpty(t, entities)
	list := NewColumnDefinitionEntityList(entities)
	assert.NotNil(t, list.GetColumn("id"))
	assert.NotNil(t, list.GetColumn("col1"))
	assert.NotNil(t, list.GetColumn("Col2"))
	assert.NotNil(t, list.GetColumn("col2")) // we also allow lower case
	assert.Nil(t, list.GetColumn("COL2"))
	assert.Nil(t, list.GetColumn("ID"))
	assert.Nil(t, list.GetColumn("Col1"))
	assert.Nil(t, list.GetColumn("col3"))
}

func TestColumnDefinitionEntityListSubset(t *testing.T) {
	table1 := `
	create table t (
		ID int,
		col1 int,
		Col2 int not null,
		primary key (id)
	)`
	table2 := `
	create table t (
		id int,
		Col1 int,
		primary key (id)
	)`
	env := NewTestEnv()
	createTableEntity1, err := NewCreateTableEntityFromSQL(env, table1)
	require.NoError(t, err)
	entities1 := createTableEntity1.ColumnDefinitionEntities()
	require.NotEmpty(t, entities1)
	list1 := NewColumnDefinitionEntityList(entities1)

	createTableEntity2, err := NewCreateTableEntityFromSQL(env, table2)
	require.NoError(t, err)
	entities2 := createTableEntity2.ColumnDefinitionEntities()
	require.NotEmpty(t, entities2)
	list2 := NewColumnDefinitionEntityList(entities2)

	assert.True(t, list1.Contains(list2))
	assert.False(t, list2.Contains(list1))
}

func TestColumnDefinitionEntity(t *testing.T) {
	table1 := `
	create table t (
		it int,
		e enum('a','b','c'),
		primary key (id)
	)`
	env := NewTestEnv()
	createTableEntity1, err := NewCreateTableEntityFromSQL(env, table1)
	require.NoError(t, err)
	entities1 := createTableEntity1.ColumnDefinitionEntities()
	require.NotEmpty(t, entities1)
	list1 := NewColumnDefinitionEntityList(entities1)

	t.Run("enum", func(t *testing.T) {
		enumCol := list1.GetColumn("e")
		require.NotNil(t, enumCol)
		assert.Equal(t, []string{"'a'", "'b'", "'c'"}, enumCol.EnumValues())

		{
			ordinalsMap := enumCol.EnumValuesOrdinals()
			assert.ElementsMatch(t, []int{1, 2, 3}, maps.Values(ordinalsMap))
			assert.ElementsMatch(t, []string{"'a'", "'b'", "'c'"}, maps.Keys(ordinalsMap))
		}
		{
			valuesMap := enumCol.EnumOrdinalValues()
			assert.ElementsMatch(t, []int{1, 2, 3}, maps.Keys(valuesMap))
			assert.ElementsMatch(t, []string{"'a'", "'b'", "'c'"}, maps.Values(valuesMap))
		}
	})
}
