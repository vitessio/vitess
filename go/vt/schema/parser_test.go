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
)

func TestParseAlterTableOptions(t *testing.T) {
	type expect struct {
		schema, table, options string
	}
	tests := map[string]expect{
		"add column i int, drop column d":                                                                       {schema: "", table: "", options: "add column i int, drop column d"},
		"  add column i int, drop column d  ":                                                                   {schema: "", table: "", options: "add column i int, drop column d"},
		"alter table t add column i int, drop column d":                                                         {schema: "", table: "t", options: "add column i int, drop column d"},
		"alter    table   t      add column i int, drop column d":                                               {schema: "", table: "t", options: "add column i int, drop column d"},
		"alter table `t` add column i int, drop column d":                                                       {schema: "", table: "t", options: "add column i int, drop column d"},
		"alter table `scm`.`t` add column i int, drop column d":                                                 {schema: "scm", table: "t", options: "add column i int, drop column d"},
		"alter table `scm`.t add column i int, drop column d":                                                   {schema: "scm", table: "t", options: "add column i int, drop column d"},
		"alter table scm.`t` add column i int, drop column d":                                                   {schema: "scm", table: "t", options: "add column i int, drop column d"},
		"alter table scm.t add column i int, drop column d":                                                     {schema: "scm", table: "t", options: "add column i int, drop column d"},
		"alter with_ghost table scm.t add column i int, drop column d":                                          {schema: "scm", table: "t", options: "add column i int, drop column d"},
		"  alter   with_ghost   table   scm.`t` add column i int, drop column d":                                {schema: "scm", table: "t", options: "add column i int, drop column d"},
		"alter with_pt table scm.t add column i int, drop column d":                                             {schema: "scm", table: "t", options: "add column i int, drop column d"},
		"alter with_ghost '--some-option=5 --another-option=false' table scm.t add column i int, drop column d": {schema: "scm", table: "t", options: "add column i int, drop column d"},
	}
	for query, expect := range tests {
		schema, table, options := ParseAlterTableOptions(query)
		if schema != expect.schema {
			t.Errorf("schema: %+v, want:%+v", schema, expect.schema)
		}
		if table != expect.table {
			t.Errorf("table: %+v, want:%+v", table, expect.table)
		}
		if options != expect.options {
			t.Errorf("options: %+v, want:%+v", options, expect.options)
		}
	}
}

func TestRemoveOnlineDDLHints(t *testing.T) {
	tests := map[string]string{
		"ALTER TABLE my_table DROP COLUMN i":                              "ALTER TABLE `my_table` DROP COLUMN i",
		"   ALTER     TABLE    my_table     DROP COLUMN i":                "ALTER TABLE `my_table` DROP COLUMN i",
		"ALTER WITH_GHOST TABLE my_table DROP COLUMN i":                   "ALTER TABLE `my_table` DROP COLUMN i",
		"ALTER WITH_PT TABLE `my_table` DROP COLUMN i":                    "ALTER TABLE `my_table` DROP COLUMN i",
		"ALTER WITH_PT TABLE scm.`my_table` DROP COLUMN i":                "ALTER TABLE `scm`.`my_table` DROP COLUMN i",
		"ALTER WITH_PT TABLE `scm`.`my_table` DROP COLUMN i":              "ALTER TABLE `scm`.`my_table` DROP COLUMN i",
		"ALTER    WITH_GHOST   TABLE   `scm`.`my_table`    DROP COLUMN i": "ALTER TABLE `scm`.`my_table` DROP COLUMN i",
	}
	for query, expect := range tests {
		normalizedQuery := RemoveOnlineDDLHints(query)
		if normalizedQuery != expect {
			t.Errorf("got: %+v, want:%+v", normalizedQuery, expect)
		}
	}
}
