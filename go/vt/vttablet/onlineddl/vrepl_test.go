/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package onlineddl

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtenv"
)

func TestReadTableColumns(t *testing.T) {
	env := vtenv.NewTestEnv()

	tcases := []struct {
		name      string
		create    string
		cols      []string
		generated []string
		pk        []string
	}{
		{
			name: "simple",
			create: `create table t (
				id int,
				primary key (id)
			)`,
			cols:      []string{"id"},
			generated: []string{},
			pk:        []string{"id"},
		},
		{
			name: "complex",
			create: `create table t (
				id int,
				col1 int,
				col2 int,
				col3 int generated always as (col1 + 1) stored,
				col4 int generated always as (col1 + 1) virtual,
				col5 int,
				primary key (id, col1),
				unique key (id, col5),
				unique key (col2, col5),
				key (col5)
			)`,
			cols:      []string{"id", "col1", "col2", "col3", "col4", "col5"},
			generated: []string{"col3", "col4"},
			pk:        []string{"id", "col1"},
		},
	}
	for _, tcase := range tcases {
		t.Run(tcase.name, func(t *testing.T) {
			stmt, err := env.Parser().ParseStrictDDL(tcase.create)
			require.NoError(t, err)
			createTable, ok := stmt.(*sqlparser.CreateTable)
			require.True(t, ok)
			cols, virtual, pk, err := readTableColumns(createTable)
			assert.NoError(t, err)
			assert.Equal(t, tcase.cols, cols.Names())
			assert.Equal(t, tcase.generated, virtual.Names())
			assert.Equal(t, tcase.pk, pk.Names())
		})
	}
}

func TestReadAutoIncrement(t *testing.T) {
	env := vtenv.NewTestEnv()

	tcases := []struct {
		name   string
		create string
		expect uint64
	}{
		{
			name: "none",
			create: `create table t (
				id int,
				primary key (id)
			)`,
			expect: 0,
		},
		{
			name: "simple",
			create: `create table t (
				id int auto_increment,
				primary key (id)
			) auto_increment=123`,
			expect: 123,
		},
		{
			name: "multiple opts",
			create: `create table t (
				id int auto_increment,
				primary key (id)
			) engine=innodb, charset=utf8mb4 auto_increment=123 row_format=compressed`,
			expect: 123,
		},
	}
	for _, tcase := range tcases {
		t.Run(tcase.name, func(t *testing.T) {
			stmt, err := env.Parser().ParseStrictDDL(tcase.create)
			require.NoError(t, err)
			createTable, ok := stmt.(*sqlparser.CreateTable)
			require.True(t, ok)
			autoIncrement, err := readAutoIncrement(createTable)
			assert.NoError(t, err)
			assert.EqualValues(t, tcase.expect, autoIncrement)
		})
	}
}
