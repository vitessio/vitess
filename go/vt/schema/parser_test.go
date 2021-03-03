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
	"strings"
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

func TestNormalizeOnlineDDL(t *testing.T) {
	type expect struct {
		sqls    []string
		isError bool
	}
	tests := map[string]expect{
		"alter table t add column i int, drop column d": {sqls: []string{"alter table t add column i int, drop column d"}},
		"create table t (id int primary key)":           {sqls: []string{"create table t (id int primary key)"}},
		"drop table t":                                  {sqls: []string{"drop table t"}},
		"drop table if exists t":                        {sqls: []string{"drop table if exists t"}},
		"drop table t1, t2, t3":                         {sqls: []string{"drop table t1", "drop table t2", "drop table t3"}},
		"drop table if exists t1, t2, t3":               {sqls: []string{"drop table if exists t1", "drop table if exists t2", "drop table if exists t3"}},
		"create index i_idx on t(id)":                   {sqls: []string{"alter table t add index i_idx (id)"}},
		"create index i_idx on t(name(12))":             {sqls: []string{"alter table t add index i_idx (`name`(12))"}},
		"create index i_idx on t(id, `ts`, name(12))":   {sqls: []string{"alter table t add index i_idx (id, ts, `name`(12))"}},
		"create unique index i_idx on t(id)":            {sqls: []string{"alter table t add unique index i_idx (id)"}},
		"create index i_idx using btree on t(id)":       {sqls: []string{"alter table t add index i_idx (id) using btree"}},
		"create index with syntax error i_idx on t(id)": {isError: true},
		"select * from t":                               {isError: true},
		"drop database t":                               {isError: true},
	}
	for query, expect := range tests {
		t.Run(query, func(t *testing.T) {
			normalized, err := NormalizeOnlineDDL(query)
			if expect.isError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				sqls := []string{}
				for _, n := range normalized {
					sql := n.SQL
					sql = strings.ReplaceAll(sql, "\n", "")
					sql = strings.ReplaceAll(sql, "\t", "")
					sqls = append(sqls, sql)
				}
				assert.Equal(t, expect.sqls, sqls)
			}
		})
	}
}
