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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sqlparser"
)

var (
	testMySQLVersion = "8.0.34"
)

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
	e := Executor{
		env: tabletenv.NewEnv(vtenv.NewTestEnv(), nil, "ValidateAndEditCreateTableStatementTest"),
	}
	tt := []struct {
		name                string
		query               string
		strategyOptions     string
		expectError         string
		countConstraints    int
		expectConstraintMap map[string]string
	}{
		{
			name: "table with FK, not allowed",
			query: `
				create table onlineddl_test (
						id int auto_increment,
						i int not null,
						parent_id int not null,
						primary key(id),
						constraint test_ibfk foreign key (parent_id) references onlineddl_test_parent (id) on delete no action
					)
				`,
			expectError: schema.ErrForeignKeyFound.Error(),
		},
		{
			name: "table with FK, allowed",
			query: `
				create table onlineddl_test (
						id int auto_increment,
						i int not null,
						parent_id int not null,
						primary key(id),
						constraint test_ibfk foreign key (parent_id) references onlineddl_test_parent (id) on delete no action
					)
				`,
			strategyOptions:     "--unsafe-allow-foreign-keys",
			countConstraints:    1,
			expectConstraintMap: map[string]string{"test_ibfk": "test_ibfk_2wtivm6zk4lthpz14g9uoyaqk"},
		},
		{
			name: "table with default FK name, strip table name",
			query: `
			create table onlineddl_test (
					id int auto_increment,
					i int not null,
					parent_id int not null,
					primary key(id),
					constraint onlineddl_test_ibfk_1 foreign key (parent_id) references onlineddl_test_parent (id) on delete no action
				)
			`,
			strategyOptions:  "--unsafe-allow-foreign-keys",
			countConstraints: 1,
			// we want 'onlineddl_test_' to be stripped out:
			expectConstraintMap: map[string]string{"onlineddl_test_ibfk_1": "ibfk_1_2wtivm6zk4lthpz14g9uoyaqk"},
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
			strategyOptions:     "--unsafe-allow-foreign-keys",
			countConstraints:    1,
			expectConstraintMap: map[string]string{"": "fk_2wtivm6zk4lthpz14g9uoyaqk"},
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
			expectConstraintMap: map[string]string{
				"check_1": "check_1_7dbssrkwdaxhdunwi5zj53q83",
				"check_2": "check_2_ehg3rtk6ejvbxpucimeess30o",
				"check_3": "check_3_0se0t8x98mf8v7lqmj2la8j9u",
				"chk_1111033c1d2d5908bf1f956ba900b192_check_4": "chk_1111033c1d2d5908bf1f956ba900b192_c_0c2c3bxi9jp4evqrct44wg3xh",
			},
		},
		{
			name: "table with both FOREIGN and CHECK constraints",
			query: `
				create table onlineddl_test (
						id int auto_increment,
						i int not null,
						primary key(id),
						constraint check_1 CHECK ((i >= 0)),
						constraint test_ibfk foreign key (parent_id) references onlineddl_test_parent (id) on delete no action,
						constraint chk_1111033c1d2d5908bf1f956ba900b192_check_4 CHECK ((i >= 0))
					)
				`,
			strategyOptions:  "--unsafe-allow-foreign-keys",
			countConstraints: 3,
			expectConstraintMap: map[string]string{
				"check_1": "check_1_7dbssrkwdaxhdunwi5zj53q83",
				"chk_1111033c1d2d5908bf1f956ba900b192_check_4": "chk_1111033c1d2d5908bf1f956ba900b192_c_0se0t8x98mf8v7lqmj2la8j9u",
				"test_ibfk": "test_ibfk_2wtivm6zk4lthpz14g9uoyaqk",
			},
		},
	}
	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := e.env.Environment().Parser().ParseStrictDDL(tc.query)
			require.NoError(t, err)
			createTable, ok := stmt.(*sqlparser.CreateTable)
			require.True(t, ok)

			onlineDDL := &schema.OnlineDDL{UUID: "a5a563da_dc1a_11ec_a416_0a43f95f28a3", Table: "onlineddl_test", Options: tc.strategyOptions}
			constraintMap, err := e.validateAndEditCreateTableStatement(onlineDDL, createTable)
			if tc.expectError != "" {
				assert.ErrorContains(t, err, tc.expectError)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tc.expectConstraintMap, constraintMap)

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
	e := Executor{
		env: tabletenv.NewEnv(vtenv.NewTestEnv(), nil, "TestValidateAndEditAlterTableStatementTest"),
	}
	tt := []struct {
		alter        string
		mySQLVersion string
		m            map[string]string
		expect       []string
	}{
		{
			alter:        "alter table t add column i int",
			mySQLVersion: "8.0.29",
			expect:       []string{"alter table t add column i int, algorithm = copy"},
		},
		{
			alter:        "alter table t add column i int",
			mySQLVersion: "8.0.32",
			expect:       []string{"alter table t add column i int"},
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
			// strip out table name
			alter:  "alter table t add constraint t_ibfk_1 foreign key (parent_id) references onlineddl_test_parent (id) on delete no action",
			expect: []string{"alter table t add constraint ibfk_1_6fmhzdlya89128u5j3xapq34i foreign key (parent_id) references onlineddl_test_parent (id) on delete no action"},
		},
		{
			// stript out table name
			alter:  "alter table t add constraint t_ibfk_1 foreign key (parent_id) references onlineddl_test_parent (id) on delete no action",
			expect: []string{"alter table t add constraint ibfk_1_6fmhzdlya89128u5j3xapq34i foreign key (parent_id) references onlineddl_test_parent (id) on delete no action"},
		},
		{
			alter:  "alter table t add constraint t_ibfk_1 foreign key (parent_id) references onlineddl_test_parent (id) on delete no action, add constraint some_check check (id != 1)",
			expect: []string{"alter table t add constraint ibfk_1_6fmhzdlya89128u5j3xapq34i foreign key (parent_id) references onlineddl_test_parent (id) on delete no action, add constraint some_check_aulpn7bjeortljhguy86phdn9 check (id != 1)"},
		},
		{
			alter: "alter table t drop foreign key t_ibfk_1",
			m: map[string]string{
				"t_ibfk_1": "ibfk_1_aaaaaaaaaaaaaa",
			},
			expect: []string{"alter table t drop foreign key ibfk_1_aaaaaaaaaaaaaa"},
		},
	}

	for _, tc := range tt {
		t.Run(tc.alter, func(t *testing.T) {
			stmt, err := e.env.Environment().Parser().ParseStrictDDL(tc.alter)
			require.NoError(t, err)
			alterTable, ok := stmt.(*sqlparser.AlterTable)
			require.True(t, ok)

			m := map[string]string{}
			for k, v := range tc.m {
				m[k] = v
			}
			if tc.mySQLVersion == "" {
				tc.mySQLVersion = testMySQLVersion
			}
			capableOf := mysql.ServerVersionCapableOf(tc.mySQLVersion)
			onlineDDL := &schema.OnlineDDL{UUID: "a5a563da_dc1a_11ec_a416_0a43f95f28a3", Table: "t", Options: "--unsafe-allow-foreign-keys"}
			alters, err := e.validateAndEditAlterTableStatement(capableOf, onlineDDL, alterTable, m)
			assert.NoError(t, err)
			var altersStrings []string
			for _, alter := range alters {
				altersStrings = append(altersStrings, sqlparser.String(alter))
			}
			assert.Equal(t, tc.expect, altersStrings)
		})
	}
}

func TestAddInstantAlgorithm(t *testing.T) {
	e := Executor{
		env: tabletenv.NewEnv(vtenv.NewTestEnv(), nil, "AddInstantAlgorithmTest"),
	}
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
			stmt, err := e.env.Environment().Parser().ParseStrictDDL(tc.alter)
			require.NoError(t, err)
			alterTable, ok := stmt.(*sqlparser.AlterTable)
			require.True(t, ok)

			e.addInstantAlgorithm(alterTable)
			alterInstant := sqlparser.CanonicalString(alterTable)

			assert.Equal(t, tc.expect, alterInstant)

			stmt, err = e.env.Environment().Parser().ParseStrictDDL(alterInstant)
			require.NoError(t, err)
			_, ok = stmt.(*sqlparser.AlterTable)
			require.True(t, ok)
		})
	}
}

func TestDuplicateCreateTable(t *testing.T) {
	e := Executor{
		env: tabletenv.NewEnv(vtenv.NewTestEnv(), nil, "DuplicateCreateTableTest"),
	}
	ctx := context.Background()
	onlineDDL := &schema.OnlineDDL{UUID: "a5a563da_dc1a_11ec_a416_0a43f95f28a3", Table: "something", Strategy: "vitess", Options: "--unsafe-allow-foreign-keys"}

	tcases := []struct {
		sql           string
		newName       string
		expectSQL     string
		expectMapSize int
	}{
		{
			sql:       "create table t (id int primary key)",
			newName:   "mytable",
			expectSQL: "create table mytable (\n\tid int primary key\n)",
		},
		{
			sql:           "create table t (id int primary key, i int, constraint f foreign key (i) references parent (id) on delete cascade)",
			newName:       "mytable",
			expectSQL:     "create table mytable (\n\tid int primary key,\n\ti int,\n\tconstraint f_bjj16562shq086ozik3zf6kjg foreign key (i) references parent (id) on delete cascade\n)",
			expectMapSize: 1,
		},
	}
	for _, tcase := range tcases {
		t.Run(tcase.sql, func(t *testing.T) {
			originalCreateTable, newCreateTable, constraintMap, err := e.duplicateCreateTable(ctx, onlineDDL, tcase.sql, tcase.newName)
			assert.NoError(t, err)
			assert.NotNil(t, originalCreateTable)
			assert.NotNil(t, newCreateTable)
			assert.NotNil(t, constraintMap)

			newSQL := sqlparser.String(newCreateTable)
			assert.Equal(t, tcase.expectSQL, newSQL)
			assert.Equal(t, tcase.expectMapSize, len(constraintMap))
		})
	}
}

func TestShouldCutOverAccordingToBackoff(t *testing.T) {
	tcases := []struct {
		name string

		shouldForceCutOverIndicator bool
		forceCutOverAfter           time.Duration
		sinceReadyToComplete        time.Duration
		sinceLastCutoverAttempt     time.Duration
		cutoverAttempts             int64

		expectShouldCutOver      bool
		expectShouldForceCutOver bool
	}{
		{
			name:                "no reason why not, normal cutover",
			expectShouldCutOver: true,
		},
		{
			name:                "backoff",
			cutoverAttempts:     1,
			expectShouldCutOver: false,
		},
		{
			name:                "more backoff",
			cutoverAttempts:     3,
			expectShouldCutOver: false,
		},
		{
			name:                    "more backoff, since last cutover",
			cutoverAttempts:         3,
			sinceLastCutoverAttempt: time.Second,
			expectShouldCutOver:     false,
		},
		{
			name:                    "no backoff, long since last cutover",
			cutoverAttempts:         3,
			sinceLastCutoverAttempt: time.Hour,
			expectShouldCutOver:     true,
		},
		{
			name:                    "many attempts, long since last cutover",
			cutoverAttempts:         3000,
			sinceLastCutoverAttempt: time.Hour,
			expectShouldCutOver:     true,
		},
		{
			name:                        "force cutover",
			shouldForceCutOverIndicator: true,
			expectShouldCutOver:         true,
			expectShouldForceCutOver:    true,
		},
		{
			name:                        "force cutover overrides backoff",
			cutoverAttempts:             3,
			shouldForceCutOverIndicator: true,
			expectShouldCutOver:         true,
			expectShouldForceCutOver:    true,
		},
		{
			name:                     "backoff; cutover-after not in effect yet",
			cutoverAttempts:          3,
			forceCutOverAfter:        time.Second,
			expectShouldCutOver:      false,
			expectShouldForceCutOver: false,
		},
		{
			name:                     "backoff; cutover-after still not in effect yet",
			cutoverAttempts:          3,
			forceCutOverAfter:        time.Second,
			sinceReadyToComplete:     time.Millisecond,
			expectShouldCutOver:      false,
			expectShouldForceCutOver: false,
		},
		{
			name:                     "cutover-after overrides backoff",
			cutoverAttempts:          3,
			forceCutOverAfter:        time.Second,
			sinceReadyToComplete:     time.Second * 2,
			expectShouldCutOver:      true,
			expectShouldForceCutOver: true,
		},
		{
			name:                     "cutover-after overrides backoff, realistic value",
			cutoverAttempts:          300,
			sinceLastCutoverAttempt:  time.Minute,
			forceCutOverAfter:        time.Hour,
			sinceReadyToComplete:     time.Hour * 2,
			expectShouldCutOver:      true,
			expectShouldForceCutOver: true,
		},
	}
	for _, tcase := range tcases {
		t.Run(tcase.name, func(t *testing.T) {
			shouldCutOver, shouldForceCutOver := shouldCutOverAccordingToBackoff(
				tcase.shouldForceCutOverIndicator,
				tcase.forceCutOverAfter,
				tcase.sinceReadyToComplete,
				tcase.sinceLastCutoverAttempt,
				tcase.cutoverAttempts,
			)
			assert.Equal(t, tcase.expectShouldCutOver, shouldCutOver)
			assert.Equal(t, tcase.expectShouldForceCutOver, shouldForceCutOver)
		})
	}
}
