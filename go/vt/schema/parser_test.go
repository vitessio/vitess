/*
Copyright 2019 The Vitess Authors.

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

package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseAlterTableOptions(t *testing.T) {
	type expect struct {
		schema, table, options string
	}
	tests := map[string]expect{
		"add column i int, drop column d":                               {schema: "", table: "", options: "add column i int, drop column d"},
		"  add column i int, drop column d  ":                           {schema: "", table: "", options: "add column i int, drop column d"},
		"alter table t add column i int, drop column d":                 {schema: "", table: "t", options: "add column i int, drop column d"},
		"alter    table   t      add column i int, drop column d":       {schema: "", table: "t", options: "add column i int, drop column d"},
		"alter table `t` add column i int, drop column d":               {schema: "", table: "t", options: "add column i int, drop column d"},
		"alter table `scm`.`t` add column i int, drop column d":         {schema: "scm", table: "t", options: "add column i int, drop column d"},
		"alter table `scm`.t add column i int, drop column d":           {schema: "scm", table: "t", options: "add column i int, drop column d"},
		"alter table scm.`t` add column i int, drop column d":           {schema: "scm", table: "t", options: "add column i int, drop column d"},
		"alter table scm.t add column i int, drop column d":             {schema: "scm", table: "t", options: "add column i int, drop column d"},
		"  alter       table   scm.`t` add column i int, drop column d": {schema: "scm", table: "t", options: "add column i int, drop column d"},
		"ALTER  table scm.t ADD COLUMN i int, DROP COLUMN d":            {schema: "scm", table: "t", options: "ADD COLUMN i int, DROP COLUMN d"},
		"ALTER TABLE scm.t ADD COLUMN i int, DROP COLUMN d":             {schema: "scm", table: "t", options: "ADD COLUMN i int, DROP COLUMN d"},
	}
	for query, expect := range tests {
		schema, table, options := ParseAlterTableOptions(query)
		assert.Equal(t, expect.schema, schema)
		assert.Equal(t, expect.table, table)
		assert.Equal(t, expect.options, options)
	}
}

func TestReplaceTableNameInCreateTableStatement(t *testing.T) {
	replacementTableName := `my_table`
	tt := []struct {
		stmt    string
		expect  string
		isError bool
	}{
		{
			stmt:    "CREATE TABLE tbl (id int)",
			isError: true,
		},
		{
			stmt:   "CREATE TABLE `tbl` (id int)",
			expect: "CREATE TABLE `my_table` (id int)",
		},
		{
			stmt:   "CREATE     TABLE     `tbl`    (id int)",
			expect: "CREATE     TABLE     `my_table`    (id int)",
		},
		{
			stmt:   "create table `tbl` (id int)",
			expect: "create table `my_table` (id int)",
		},
		{
			stmt:    "CREATE TABLE `schema`.`tbl` (id int)",
			isError: true,
		},
		{
			stmt:    "CREATE TABLE IF NOT EXISTS `tbl` (id int)",
			isError: true,
		},
	}
	for _, ts := range tt {
		t.Run(ts.stmt, func(*testing.T) {
			result, err := ReplaceTableNameInCreateTableStatement(ts.stmt, replacementTableName)
			if ts.isError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, ts.expect, result)
			}
		})
	}
}

func TestLegacyParseRevertUUID(t *testing.T) {

	{
		uuid, err := legacyParseRevertUUID("revert 4e5dcf80_354b_11eb_82cd_f875a4d24e90")
		assert.NoError(t, err)
		assert.Equal(t, "4e5dcf80_354b_11eb_82cd_f875a4d24e90", uuid)
	}
	{
		_, err := legacyParseRevertUUID("revert 4e5dcf80_354b_11eb_82cd_f875a4")
		assert.Error(t, err)
	}
	{
		_, err := legacyParseRevertUUID("revert vitess_migration '4e5dcf80_354b_11eb_82cd_f875a4d24e90'")
		assert.Error(t, err)
	}
}
