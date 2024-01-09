package schemadiff

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/capabilities"
	"vitess.io/vitess/go/vt/sqlparser"
)

func TestAlterTableCapableOfInstantDDL(t *testing.T) {
	capableOf := func(capability capabilities.FlavorCapability) (bool, error) {
		switch capability {
		case
			capabilities.InstantDDLFlavorCapability,
			capabilities.InstantAddLastColumnFlavorCapability,
			capabilities.InstantAddDropVirtualColumnFlavorCapability,
			capabilities.InstantAddDropColumnFlavorCapability,
			capabilities.InstantChangeColumnDefaultFlavorCapability,
			capabilities.InstantExpandEnumCapability:
			return true, nil
		}
		return false, nil
	}
	incapableOf := func(capability capabilities.FlavorCapability) (bool, error) {
		return false, nil
	}
	parser := sqlparser.NewTestParser()

	tcases := []struct {
		name                      string
		create                    string
		alter                     string
		expectCapableOfInstantDDL bool
		capableOf                 capabilities.CapableOf
	}{
		{
			name:                      "add column",
			create:                    "create table t1 (id int, i1 int)",
			alter:                     "alter table t1 add column i2 int",
			expectCapableOfInstantDDL: true,
		},
		{
			name:                      "add last column",
			create:                    "create table t1 (id int, i1 int)",
			alter:                     "alter table t1 add column i2 int after i1",
			expectCapableOfInstantDDL: true,
		},
		{
			name:                      "add mid column",
			create:                    "create table t1 (id int, i1 int)",
			alter:                     "alter table t1 add column i2 int after id",
			expectCapableOfInstantDDL: true,
		},
		{
			name:                      "add mid column, incapable",
			create:                    "create table t1 (id int, i1 int)",
			alter:                     "alter table t1 add column i2 int after id",
			capableOf:                 incapableOf,
			expectCapableOfInstantDDL: false,
		},
		{
			name:                      "drop virtual column",
			create:                    "create table t(id int, i1 int not null, i2 int generated always as (i1 + 1) virtual, primary key(id))",
			alter:                     "alter table t drop column i2",
			expectCapableOfInstantDDL: true,
		},
		{
			name:                      "drop stored virtual column",
			create:                    "create table t(id int, i1 int not null, i2 int generated always as (i1 + 1) stored, primary key(id))",
			alter:                     "alter table t drop column i2",
			expectCapableOfInstantDDL: true,
		},
		{
			name:                      "drop mid column",
			create:                    "create table t(id int, i1 int not null, i2 int not null, primary key(id))",
			alter:                     "alter table t drop column i1",
			expectCapableOfInstantDDL: true,
		},
		{
			name:                      "fail due to row_format=compressed",
			create:                    "create table t(id int, i1 int not null, i2 int not null, primary key(id)) row_format=compressed",
			alter:                     "alter table t drop column i1",
			expectCapableOfInstantDDL: false,
		},
		{
			name:                      "add two columns",
			create:                    "create table t(id int, i1 int not null, primary key(id))",
			alter:                     "alter table t add column i2 int not null after id, add column i3 int not null",
			expectCapableOfInstantDDL: true,
		},
		{
			name:                      "multiple add/drop columns",
			create:                    "create table t(id int, i1 int not null, primary key(id))",
			alter:                     "alter table t add column i2 int not null after id, add column i3 int not null, drop column i1",
			expectCapableOfInstantDDL: true,
		},
		// change/remove column default
		{
			name:                      "set a default column value",
			create:                    "create table t(id int, i1 int not null, primary key(id))",
			alter:                     "alter table t modify column i1 int not null default 0",
			expectCapableOfInstantDDL: true,
		},
		{
			name:                      "change a default column value",
			create:                    "create table t(id int, i1 int not null, primary key(id))",
			alter:                     "alter table t modify column i1 int not null default 3",
			expectCapableOfInstantDDL: true,
		},
		{
			name:                      "change default column value to null",
			create:                    "create table t(id int, i1 int not null, primary key(id))",
			alter:                     "alter table t modify column i1 int default null",
			expectCapableOfInstantDDL: false,
		},
		{
			name:                      "fail because on top of changing the default value, the datatype is changed, too",
			create:                    "create table t(id int, i1 int not null, primary key(id))",
			alter:                     "alter table t modify column i1 bigint not null default 3",
			expectCapableOfInstantDDL: false,
		},
		{
			name:                      "set column dfault value to null",
			create:                    "create table t(id int, i1 int, primary key(id))",
			alter:                     "alter table t modify column i1 int default null",
			expectCapableOfInstantDDL: true,
		},
		// enum/set:
		{
			name:                      "change enum default value",
			create:                    "create table t(id int, c1 enum('a', 'b', 'c'), primary key(id))",
			alter:                     "alter table t modify column c1 enum('a', 'b', 'c') default 'b'",
			expectCapableOfInstantDDL: true,
		},
		{
			name:                      "enum append",
			create:                    "create table t(id int, c1 enum('a', 'b', 'c'), primary key(id))",
			alter:                     "alter table t modify column c1 enum('a', 'b', 'c', 'd')",
			expectCapableOfInstantDDL: true,
		},
		{
			name:                      "enum append with changed default",
			create:                    "create table t(id int, c1 enum('a', 'b', 'c') default 'a', primary key(id))",
			alter:                     "alter table t modify column c1 enum('a', 'b', 'c', 'd') default 'd'",
			expectCapableOfInstantDDL: true,
		},
		{
			name:                      "enum: fail insert in middle",
			create:                    "create table t(id int, c1 enum('a', 'b', 'c'), primary key(id))",
			alter:                     "alter table t modify column c1 enum('a', 'b', 'x', 'c')",
			expectCapableOfInstantDDL: false,
		},
		{
			name:                      "enum: fail change",
			create:                    "create table t(id int, c1 enum('a', 'b', 'c'), primary key(id))",
			alter:                     "alter table t modify column c1 enum('a', 'x', 'c')",
			expectCapableOfInstantDDL: false,
		},
		{
			name:                      "set: append",
			create:                    "create table t(id int, c1 set('a', 'b', 'c'), primary key(id))",
			alter:                     "alter table t modify column c1 set('a', 'b', 'c', 'd')",
			expectCapableOfInstantDDL: true,
		},
		{
			name:                      "fail set append when over threshold", // (increase from 8 to 9 values => storage goes from 1 byte to 2 bytes)
			create:                    "create table t(id int, c1 set('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'), primary key(id))",
			alter:                     "alter table t modify column c1 set('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i')",
			expectCapableOfInstantDDL: false,
		},
	}
	for _, tcase := range tcases {
		t.Run(tcase.name, func(t *testing.T) {
			if tcase.capableOf == nil {
				tcase.capableOf = capableOf
			}
			createTable, err := parser.Parse(tcase.create)
			require.NoError(t, err, "failed to parse a CREATE TABLE statement from %q", tcase.create)
			createTableStmt, ok := createTable.(*sqlparser.CreateTable)
			require.True(t, ok)

			alterTable, err := parser.Parse(tcase.alter)
			require.NoError(t, err, "failed to parse a ALTER TABLE statement from %q", tcase.alter)
			alterTableStmt, ok := alterTable.(*sqlparser.AlterTable)
			require.True(t, ok)

			isCapableOf, err := AlterTableCapableOfInstantDDL(alterTableStmt, createTableStmt, tcase.capableOf)
			require.NoError(t, err)
			assert.Equal(t, tcase.expectCapableOfInstantDDL, isCapableOf)
		})
	}
}
