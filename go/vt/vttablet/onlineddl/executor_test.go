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

	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sqlparser"
)

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
		{
			sql:           "create table self (id int primary key, i int, constraint f foreign key (i) references self (id))",
			newName:       "mytable",
			expectSQL:     "create table mytable (\n\tid int primary key,\n\ti int,\n\tconstraint f_8aymb58nzb78l5jhq600veg6y foreign key (i) references mytable (id)\n)",
			expectMapSize: 1,
		},
		{
			sql:     "create table self (id int primary key, i1 int, i2 int, constraint f1 foreign key (i1) references self (id), constraint f1 foreign key (i2) references parent (id))",
			newName: "mytable",
			expectSQL: `create table mytable (
	id int primary key,
	i1 int,
	i2 int,
	constraint f1_1rlsg9yls1t91i35zq5gyeoq7 foreign key (i1) references mytable (id),
	constraint f1_59t4lvb1ncti6fxy27drad4jp foreign key (i2) references parent (id)
)`,
			expectMapSize: 1,
		},
	}
	for _, tcase := range tcases {
		t.Run(tcase.sql, func(t *testing.T) {
			stmt, err := e.env.Environment().Parser().ParseStrictDDL(tcase.sql)
			require.NoError(t, err)
			originalCreateTable, ok := stmt.(*sqlparser.CreateTable)
			require.True(t, ok)
			require.NotNil(t, originalCreateTable)
			newCreateTable, constraintMap, err := e.duplicateCreateTable(ctx, onlineDDL, originalCreateTable, tcase.newName)
			assert.NoError(t, err)
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
			name:                     "zero since ready",
			cutoverAttempts:          3,
			forceCutOverAfter:        time.Second,
			sinceReadyToComplete:     0,
			expectShouldCutOver:      false,
			expectShouldForceCutOver: false,
		},
		{
			name:                     "zero since read, zero cut-over-after",
			cutoverAttempts:          3,
			forceCutOverAfter:        0,
			sinceReadyToComplete:     0,
			expectShouldCutOver:      false,
			expectShouldForceCutOver: false,
		},
		{
			name:                     "microsecond",
			cutoverAttempts:          3,
			forceCutOverAfter:        time.Microsecond,
			sinceReadyToComplete:     time.Millisecond,
			expectShouldCutOver:      true,
			expectShouldForceCutOver: true,
		},
		{
			name:                     "microsecond, not ready",
			cutoverAttempts:          3,
			forceCutOverAfter:        time.Millisecond,
			sinceReadyToComplete:     time.Microsecond,
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
