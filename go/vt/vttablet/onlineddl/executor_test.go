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

func TestValidateAndEditCreateTableStatement(t *testing.T) {
	e := Executor{}
	tt := []struct {
		name             string
		query            string
		expectError      bool
		countConstraints int
	}{
		{
			name: "table with FK",
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
			expectError:      true,
		},
		{
			name: "table with FK",
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
	}
	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := sqlparser.ParseStrictDDL(tc.query)
			require.NoError(t, err)
			createTable, ok := stmt.(*sqlparser.CreateTable)
			require.True(t, ok)

			onlineDDL := &schema.OnlineDDL{UUID: "a5a563da_dc1a_11ec_a416_0a43f95f28a3", Table: "onlineddl_test"}
			constraintMap, err := e.validateAndEditCreateTableStatement(context.Background(), onlineDDL, createTable)
			if tc.expectError {
				assert.Error(t, err)
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
			assert.Equal(t, tc.countConstraints, len(constraintMap))
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
