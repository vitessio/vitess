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

/*
Functionality of this Executor is tested in go/test/endtoend/onlineddl/...
*/

package onlineddl

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/sqlparser"
)

func TestAnalyzeInstantDDL(t *testing.T) {
	tt := []struct {
		version     string
		create      string
		alter       string
		expectError bool
		instant     bool
	}{
		// add/drop columns
		{
			version: "5.7.28",
			create:  "create table t(id int, i1 int not null, primary key(id))",
			alter:   "alter table t add column i2 int not null",
			instant: false,
		},
		{
			version: "8.0.21",
			create:  "create table t(id int, i1 int not null, primary key(id))",
			alter:   "alter table t add column i2 int not null",
			instant: true,
		},
		{
			version: "8.0.21",
			create:  "create table t(id int, i1 int not null, primary key(id))",
			alter:   "alter table t add column i2 int not null, add column i3 int not null",
			instant: true,
		},
		{
			// fail add mid column in older versions
			version: "8.0.21",
			create:  "create table t(id int, i1 int not null, primary key(id))",
			alter:   "alter table t add column i2 int not null after id",
			instant: false,
		},
		{
			version: "8.0.21",
			create:  "create table t(id int, i1 int not null, primary key(id))",
			alter:   "alter table t drop column i1",
			instant: false,
		},
		{
			// drop virtual column
			version: "8.0.21",
			create:  "create table t(id int, i1 int not null, i2 int generated always as (i1 + 1) virtual, primary key(id))",
			alter:   "alter table t drop column i2",
			instant: true,
		},
		{
			version: "8.0.21",
			create:  "create table t(id int, i1 int not null, i2 int generated always as (i1 + 1) stored, primary key(id))",
			alter:   "alter table t drop column i2",
			instant: false,
		},
		{
			// add mid column
			version: "8.0.29",
			create:  "create table t(id int, i1 int not null, primary key(id))",
			alter:   "alter table t add column i2 int not null after id",
			instant: true,
		},
		{
			// drop mid column
			version: "8.0.29",
			create:  "create table t(id int, i1 int not null, i2 int not null, primary key(id))",
			alter:   "alter table t drop column i1",
			instant: true,
		},
		{
			// fail due to row_format=compressed
			version: "8.0.29",
			create:  "create table t(id int, i1 int not null, i2 int not null, primary key(id)) row_format=compressed",
			alter:   "alter table t drop column i1",
			instant: false,
		},
		{
			version: "8.0.29",
			create:  "create table t(id int, i1 int not null, primary key(id))",
			alter:   "alter table t add column i2 int not null after id, add column i3 int not null",
			instant: true,
		},
		{
			version: "8.0.29",
			create:  "create table t(id int, i1 int not null, primary key(id))",
			alter:   "alter table t add column i2 int not null after id, add column i3 int not null, drop column i1",
			instant: true,
		},
		// change/remove column default
		{
			// set a default value
			version: "8.0.21",
			create:  "create table t(id int, i1 int not null, primary key(id))",
			alter:   "alter table t modify column i1 int not null default 0",
			instant: true,
		},
		{
			// change a default value
			version: "8.0.21",
			create:  "create table t(id int, i1 int not null, primary key(id))",
			alter:   "alter table t modify column i1 int not null default 3",
			instant: true,
		},
		{
			// change default value to null
			version: "8.0.21",
			create:  "create table t(id int, i1 int not null, primary key(id))",
			alter:   "alter table t modify column i1 int default null",
			instant: false,
		},
		{
			// fail because on top of changing the default value, the datatype is changed, too
			version: "8.0.21",
			create:  "create table t(id int, i1 int not null, primary key(id))",
			alter:   "alter table t modify column i1 bigint not null default 3",
			instant: false,
		},
		{
			version: "8.0.21",
			create:  "create table t(id int, i1 int, primary key(id))",
			alter:   "alter table t modify column i1 int default 0",
			instant: true,
		},
		{
			version: "8.0.21",
			create:  "create table t(id int, i1 int, primary key(id))",
			alter:   "alter table t modify column i1 int default null",
			instant: true,
		},
		// enum/set:
		{
			// enum same, with changed default
			version: "8.0.21",
			create:  "create table t(id int, c1 enum('a', 'b', 'c'), primary key(id))",
			alter:   "alter table t modify column c1 enum('a', 'b', 'c') default 'b'",
			instant: true,
		},
		{
			// enum append
			version: "8.0.21",
			create:  "create table t(id int, c1 enum('a', 'b', 'c'), primary key(id))",
			alter:   "alter table t modify column c1 enum('a', 'b', 'c', 'd')",
			instant: true,
		},
		{
			// enum append with changed default
			version: "8.0.21",
			create:  "create table t(id int, c1 enum('a', 'b', 'c') default 'a', primary key(id))",
			alter:   "alter table t modify column c1 enum('a', 'b', 'c', 'd') default 'd'",
			instant: true,
		},
		{
			// fail insert in middle
			version: "8.0.21",
			create:  "create table t(id int, c1 enum('a', 'b', 'c'), primary key(id))",
			alter:   "alter table t modify column c1 enum('a', 'b', 'x', 'c')",
			instant: false,
		},
		{
			// fail change
			version: "8.0.21",
			create:  "create table t(id int, c1 enum('a', 'b', 'c'), primary key(id))",
			alter:   "alter table t modify column c1 enum('a', 'x', 'c')",
			instant: false,
		},
		{
			// set append
			version: "8.0.21",
			create:  "create table t(id int, c1 set('a', 'b', 'c'), primary key(id))",
			alter:   "alter table t modify column c1 set('a', 'b', 'c', 'd')",
			instant: true,
		},
		{
			// fail set append when over threshold (increase from 8 to 9 values => storage goes from 1 byte to 2 bytes)
			version: "8.0.21",
			create:  "create table t(id int, c1 set('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'), primary key(id))",
			alter:   "alter table t modify column c1 set('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i')",
			instant: false,
		},
	}
	for _, tc := range tt {
		name := tc.version + " " + tc.create
		t.Run(name, func(t *testing.T) {
			stmt, err := sqlparser.ParseStrictDDL(tc.create)
			require.NoError(t, err)
			createTable, ok := stmt.(*sqlparser.CreateTable)
			require.True(t, ok)

			stmt, err = sqlparser.ParseStrictDDL(tc.alter)
			require.NoError(t, err)
			alterTable, ok := stmt.(*sqlparser.AlterTable)
			require.True(t, ok)

			_, capableOf, _ := mysql.GetFlavor(tc.version, nil)
			plan, err := AnalyzeInstantDDL(alterTable, createTable, capableOf)
			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				if tc.instant {
					require.NotNil(t, plan)
					assert.Equal(t, instantDDLSpecialOperation, plan.operation)
				} else {
					require.Nil(t, plan)
				}
			}
		})
	}
}
