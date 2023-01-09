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
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sqlparser"
)

func TestVexecUpdateTemplates(t *testing.T) {
	{
		match, err := sqlparser.QueryMatchesTemplates("select 1 from dual", vexecUpdateTemplates)
		assert.NoError(t, err)
		assert.False(t, match)
	}
	queries := []string{
		`update _vt.schema_migrations set migration_status='cancel-all' where mysql_schema='vt_commerce'`,
		`update _vt.schema_migrations set migration_status = 'cancel-all' where migration_uuid='a5a563da_dc1a_11ec_a416_0a43f95f28a3' and mysql_schema = 'vt_commerce'`,
		`update _vt.schema_migrations set migration_status = 'cancel-all' where migration_uuid='a5a563da_dc1a_11ec_a416_0a43f95f28a3' and mysql_schema = 'vt_commerce' and shard='0'`,
	}
	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			match, err := sqlparser.QueryMatchesTemplates(query, vexecUpdateTemplates)
			assert.NoError(t, err)
			assert.True(t, match)
		})
	}
}

func TestGetConstraintType(t *testing.T) {
	{
		typ := GetConstraintType(&sqlparser.CheckConstraintDefinition{})
		assert.Equal(t, CheckConstraintType, typ)
	}
	{
		typ := GetConstraintType(&sqlparser.ForeignKeyDefinition{})
		assert.Equal(t, ForeignKeyConstraintType, typ)
	}
}

func TestValidateAndEditCreateTableStatement(t *testing.T) {
	e := Executor{}
	tt := []struct {
		name             string
		query            string
		strategyOptions  string
		expectError      string
		countConstraints int
	}{
		{
			name: "table with FK, not allowed",
			query: `
				create table onlineddl_test (
						id int auto_increment,
						i int not null,
						parent_id int not null,
						primary key(id),
						constraint test_fk foreign key (parent_id) references onlineddl_test_parent (id) on delete no action
					)
				`,
			countConstraints: 1,
			expectError:      schema.ErrForeignKeyFound.Error(),
		},
		{
			name: "table with FK, allowed",
			query: `
				create table onlineddl_test (
						id int auto_increment,
						i int not null,
						parent_id int not null,
						primary key(id),
						constraint test_fk foreign key (parent_id) references onlineddl_test_parent (id) on delete no action
					)
				`,
			strategyOptions:  "--unsafe-allow-foreign-keys",
			countConstraints: 1,
		},
		{
			name: "table with anonymous FK, allowed",
			query: `
				create table onlineddl_test (
						id int auto_increment,
						i int not null,
						parent_id int not null,
						primary key(id),
						foreign key (parent_id) references onlineddl_test_parent (id) on delete no action
					)
				`,
			strategyOptions:  "--unsafe-allow-foreign-keys",
			countConstraints: 1,
		},
		{
			name: "table with CHECK constraints",
			query: `
				create table onlineddl_test (
						id int auto_increment,
						i int not null,
						primary key(id),
						constraint check_1 CHECK ((i >= 0)),
						constraint check_2 CHECK ((i <> 5)),
						constraint check_3 CHECK ((i >= 0)),
						constraint chk_1111033c1d2d5908bf1f956ba900b192_check_4 CHECK ((i >= 0))
					)
				`,
			countConstraints: 4,
		},
		{
			name: "table with both FOREIGN and CHECK constraints",
			query: `
				create table onlineddl_test (
						id int auto_increment,
						i int not null,
						primary key(id),
						constraint check_1 CHECK ((i >= 0)),
						constraint test_fk foreign key (parent_id) references onlineddl_test_parent (id) on delete no action,
						constraint chk_1111033c1d2d5908bf1f956ba900b192_check_4 CHECK ((i >= 0))
					)
				`,
			strategyOptions:  "--unsafe-allow-foreign-keys",
			countConstraints: 3,
		},
	}
	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := sqlparser.ParseStrictDDL(tc.query)
			require.NoError(t, err)
			createTable, ok := stmt.(*sqlparser.CreateTable)
			require.True(t, ok)

			onlineDDL := &schema.OnlineDDL{UUID: "a5a563da_dc1a_11ec_a416_0a43f95f28a3", Table: "onlineddl_test", Options: tc.strategyOptions}
			constraintMap, err := e.validateAndEditCreateTableStatement(context.Background(), onlineDDL, createTable)
			if tc.expectError != "" {
				require.Error(t, err)
				assert.ErrorContains(t, err, tc.expectError)
			} else {
				assert.NoError(t, err)
			}
			uniqueConstraintNames := map[string]bool{}
			err = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
				switch node := node.(type) {
				case *sqlparser.ConstraintDefinition:
					uniqueConstraintNames[node.Name.String()] = true
				}
				return true, nil
			}, createTable)
			assert.NoError(t, err)
			assert.Equal(t, tc.countConstraints, len(uniqueConstraintNames))
			assert.Equalf(t, tc.countConstraints, len(constraintMap), "got contraints: %v", constraintMap)
		})
	}
}

func TestValidateAndEditAlterTableStatement(t *testing.T) {
	e := Executor{}
	tt := []struct {
		alter  string
		expect []string
	}{
		{
			alter:  "alter table t add column i int",
			expect: []string{"alter table t add column i int"},
		},
		{
			alter:  "alter table t add column i int, add fulltext key name1_ft (name1)",
			expect: []string{"alter table t add column i int, add fulltext key name1_ft (name1)"},
		},
		{
			alter:  "alter table t add column i int, add fulltext key name1_ft (name1), add fulltext key name2_ft (name2)",
			expect: []string{"alter table t add column i int, add fulltext key name1_ft (name1)", "alter table t add fulltext key name2_ft (name2)"},
		},
		{
			alter:  "alter table t add fulltext key name0_ft (name0), add column i int, add fulltext key name1_ft (name1), add fulltext key name2_ft (name2)",
			expect: []string{"alter table t add fulltext key name0_ft (name0), add column i int", "alter table t add fulltext key name1_ft (name1)", "alter table t add fulltext key name2_ft (name2)"},
		},
		{
			alter:  "alter table t add constraint check (id != 1)",
			expect: []string{"alter table t add constraint chk_aulpn7bjeortljhguy86phdn9 check (id != 1)"},
		},
		{
			alter:  "alter table t add constraint t_chk_1 check (id != 1)",
			expect: []string{"alter table t add constraint chk_1_aulpn7bjeortljhguy86phdn9 check (id != 1)"},
		},
		{
			alter:  "alter table t add constraint some_check check (id != 1)",
			expect: []string{"alter table t add constraint some_check_aulpn7bjeortljhguy86phdn9 check (id != 1)"},
		},
		{
			alter:  "alter table t add constraint some_check check (id != 1), add constraint another_check check (id != 2)",
			expect: []string{"alter table t add constraint some_check_aulpn7bjeortljhguy86phdn9 check (id != 1), add constraint another_check_4fa197273p3w96267pzm3gfi3 check (id != 2)"},
		},
		{
			alter:  "alter table t add foreign key (parent_id) references onlineddl_test_parent (id) on delete no action",
			expect: []string{"alter table t add constraint fk_6fmhzdlya89128u5j3xapq34i foreign key (parent_id) references onlineddl_test_parent (id) on delete no action"},
		},
		{
			alter:  "alter table t add constraint myfk foreign key (parent_id) references onlineddl_test_parent (id) on delete no action",
			expect: []string{"alter table t add constraint myfk_6fmhzdlya89128u5j3xapq34i foreign key (parent_id) references onlineddl_test_parent (id) on delete no action"},
		},
		{
			alter:  "alter table t add constraint t_fk_1 foreign key (parent_id) references onlineddl_test_parent (id) on delete no action",
			expect: []string{"alter table t add constraint fk_1_6fmhzdlya89128u5j3xapq34i foreign key (parent_id) references onlineddl_test_parent (id) on delete no action"},
		},
		{
			alter:  "alter table t add constraint t_fk_1 foreign key (parent_id) references onlineddl_test_parent (id) on delete no action, add constraint some_check check (id != 1)",
			expect: []string{"alter table t add constraint fk_1_6fmhzdlya89128u5j3xapq34i foreign key (parent_id) references onlineddl_test_parent (id) on delete no action, add constraint some_check_aulpn7bjeortljhguy86phdn9 check (id != 1)"},
		},
	}
	for _, tc := range tt {
		t.Run(tc.alter, func(t *testing.T) {
			stmt, err := sqlparser.ParseStrictDDL(tc.alter)
			require.NoError(t, err)
			alterTable, ok := stmt.(*sqlparser.AlterTable)
			require.True(t, ok)

			m := map[string]string{}
			onlineDDL := &schema.OnlineDDL{UUID: "a5a563da_dc1a_11ec_a416_0a43f95f28a3", Table: "t", Options: "--unsafe-allow-foreign-keys"}
			alters, err := e.validateAndEditAlterTableStatement(context.Background(), onlineDDL, alterTable, m)
			assert.NoError(t, err)
			altersStrings := []string{}
			for _, alter := range alters {
				altersStrings = append(altersStrings, sqlparser.String(alter))
			}
			assert.Equal(t, tc.expect, altersStrings)
		})
	}
}

func TestAddInstantAlgorithm(t *testing.T) {
	e := Executor{}
	tt := []struct {
		alter  string
		expect string
	}{
		{
			alter:  "alter table t add column i2 int not null",
			expect: "ALTER TABLE `t` ADD COLUMN `i2` int NOT NULL, ALGORITHM = INSTANT",
		},
		{
			alter:  "alter table t add column i2 int not null, lock=none",
			expect: "ALTER TABLE `t` ADD COLUMN `i2` int NOT NULL, LOCK NONE, ALGORITHM = INSTANT",
		},
		{
			alter:  "alter table t add column i2 int not null, algorithm=inplace",
			expect: "ALTER TABLE `t` ADD COLUMN `i2` int NOT NULL, ALGORITHM = INSTANT",
		},
		{
			alter:  "alter table t add column i2 int not null, algorithm=inplace, lock=none",
			expect: "ALTER TABLE `t` ADD COLUMN `i2` int NOT NULL, ALGORITHM = INSTANT, LOCK NONE",
		},
	}
	for _, tc := range tt {
		t.Run(tc.alter, func(t *testing.T) {
			stmt, err := sqlparser.ParseStrictDDL(tc.alter)
			require.NoError(t, err)
			alterTable, ok := stmt.(*sqlparser.AlterTable)
			require.True(t, ok)

			e.addInstantAlgorithm(alterTable)
			alterInstant := sqlparser.CanonicalString(alterTable)

			assert.Equal(t, tc.expect, alterInstant)

			stmt, err = sqlparser.ParseStrictDDL(alterInstant)
			require.NoError(t, err)
			_, ok = stmt.(*sqlparser.AlterTable)
			require.True(t, ok)
		})
	}
}
