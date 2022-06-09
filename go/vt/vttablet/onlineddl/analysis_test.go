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
			version: "8.0.29",
			create:  "create table t(id int, i1 int not null, primary key(id))",
			alter:   "alter table t add column i2 int not null after id",
			instant: true,
		},
		{
			version: "8.0.29",
			create:  "create table t(id int, i1 int not null, i2 int not null, primary key(id))",
			alter:   "alter table t drop column i1",
			instant: true,
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
			version: "8.0.21",
			create:  "create table t(id int, i1 int not null, primary key(id))",
			alter:   "alter table t modify column i1 int not null default 0",
			instant: true,
		},
		{
			version: "8.0.21",
			create:  "create table t(id int, i1 int not null, primary key(id))",
			alter:   "alter table t modify column i1 int not null default 3",
			instant: true,
		},
		{
			version: "8.0.21",
			create:  "create table t(id int, i1 int not null, primary key(id))",
			alter:   "alter table t modify column i1 int default null",
			instant: false,
		},
		{
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
