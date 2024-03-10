/*
Copyright 2023 The Vitess Authors.

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

package schemadiff

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/vt/vtenv"
)

func TestPermutations(t *testing.T) {
	ctx := context.Background()
	tt := []struct {
		name               string
		fromQueries        []string
		toQueries          []string
		expectDiffs        int
		expectPermutations int
	}{
		{
			name: "no diff",
			fromQueries: []string{
				"create table t1 (id int primary key, info int not null);",
			},
			toQueries: []string{
				"create table t1 (id int primary key, info int not null);",
			},
			expectDiffs:        0,
			expectPermutations: 0,
		},
		{
			name: "single diff",
			fromQueries: []string{
				"create table t1 (id int primary key, info int not null);",
			},
			toQueries: []string{
				"create table t1 (id int primary key, info int not null, i int);",
			},
			expectDiffs:        1,
			expectPermutations: 1,
		},
		{
			name: "two diffs",
			fromQueries: []string{
				"create table t1 (id int primary key, info int not null);",
				"create view v1 as select id from t1",
			},
			toQueries: []string{
				"create table t1 (id int primary key, info int not null, i int);",
				"create view v1 as select id, info from t1",
			},
			expectDiffs:        2,
			expectPermutations: 2,
		},
		{
			name: "multiple diffs",
			fromQueries: []string{
				"create table t1 (id int primary key, info int not null);",
				"create table t2 (id int primary key, ts timestamp);",
				"create view v1 as select id from t1",
			},
			toQueries: []string{
				"create table t1 (id int primary key, info int not null, i int);",
				"create table t2 (id int primary key);",
				"create view v1 as select id, info from t1",
				"create view v2 as select id from t2",
			},
			expectDiffs:        4,
			expectPermutations: 8, // because CREATE VIEW does not permutate with TABLE operations
		},
		{
			name: "multiple drop view diffs",
			fromQueries: []string{
				"create table t1 (id int primary key, info int not null);",
				"create view v1 as select id from t1",
				"create view v2 as select id from v1",
				"create view v0 as select id from v2",
			},
			toQueries: []string{
				"create table t1 (id int primary key, info int not null);",
			},
			expectDiffs:        3,
			expectPermutations: 1, // because DROP VIEW don't permutate between themselves
		},
		{
			name: "multiple drop view diffs with ALTER TABLE",
			fromQueries: []string{
				"create table t1 (id int primary key, info int not null);",
				"create view v1 as select id from t1",
				"create view v2 as select id from v1",
				"create view v0 as select id from v2",
			},
			toQueries: []string{
				"create table t1 (id int primary key, info bigint not null);",
			},
			expectDiffs:        4,
			expectPermutations: 1, // because DROP VIEW don't permutate between themselves and with TABLE operations
		},
		{
			name: "multiple create view diffs",
			fromQueries: []string{
				"create table t1 (id int primary key, info int not null);",
			},
			toQueries: []string{
				"create table t1 (id int primary key, info int not null);",
				"create view v1 as select id from t1",
				"create view v2 as select id from v1",
				"create view v3 as select id from v2",
			},
			expectDiffs:        3,
			expectPermutations: 1, // because CREATE VIEW don't permutate between themselves
		},
		{
			name: "multiple create view diffs with ALTER TABLE",
			fromQueries: []string{
				"create table t1 (id int primary key, info int not null);",
			},
			toQueries: []string{
				"create table t1 (id int primary key, info bigint not null);",
				"create view v1 as select id from t1",
				"create view v2 as select id from v1",
				"create view v3 as select id from v2",
			},
			expectDiffs:        4,
			expectPermutations: 1, // because CREATE VIEW don't permutate between themselves and with TABLE operations
		},
		{
			name: "multiple create and drop view diffs with ALTER TABLE",
			fromQueries: []string{
				"create table t1 (id int primary key, info int not null);",
				"create view v101 as select id from t1",
				"create view v102 as select id from v101",
				"create view v103 as select id from v102",
			},
			toQueries: []string{
				"create table t1 (id int primary key, info bigint not null);",
				"create view v201 as select id from t1",
				"create view v202 as select id from v201",
				"create view v203 as select id from v202",
			},
			expectDiffs:        7,
			expectPermutations: 1, // because CREATE/DROP VIEW don't permutate between themselves and with TABLE operations
		},
	}
	hints := &DiffHints{RangeRotationStrategy: RangeRotationDistinctStatements}
	env := NewTestEnv()
	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {

			fromSchema, err := NewSchemaFromQueries(env, tc.fromQueries)
			require.NoError(t, err)
			require.NotNil(t, fromSchema)

			toSchema, err := NewSchemaFromQueries(env, tc.toQueries)
			require.NoError(t, err)
			require.NotNil(t, toSchema)

			schemaDiff, err := fromSchema.SchemaDiff(toSchema, hints)
			require.NoError(t, err)

			allDiffs := schemaDiff.UnorderedDiffs()
			require.Equal(t, tc.expectDiffs, len(allDiffs))

			toSingleString := func(diffs []EntityDiff) string {
				res := ""
				for _, diff := range diffs {
					res = res + diff.CanonicalStatementString() + ";"
				}
				return res
			}
			t.Run("no early break", func(t *testing.T) {
				iteration := 0
				allPerms := map[string]bool{}
				allPermsStatements := []string{}
				allDiffs := schemaDiff.UnorderedDiffs()
				originalSingleString := toSingleString(allDiffs)
				numEquals := 0
				earlyBreak, err := permutateDiffs(ctx, allDiffs, func(pdiffs []EntityDiff) (earlyBreak bool) {
					defer func() { iteration++ }()
					// cover all permutations
					singleString := toSingleString(pdiffs)
					allPerms[singleString] = true
					allPermsStatements = append(allPermsStatements, singleString)
					if originalSingleString == singleString {
						numEquals++
					}
					return false
				})
				assert.NoError(t, err)
				if len(allDiffs) > 0 {
					assert.Equal(t, numEquals, 1)
				}

				assert.False(t, earlyBreak)
				assert.Equalf(t, tc.expectPermutations, len(allPerms), "all perms: %v", strings.Join(allPermsStatements, "\n"))
			})
			t.Run("early break", func(t *testing.T) {
				allPerms := map[string]bool{}
				allDiffs := schemaDiff.UnorderedDiffs()
				originalSingleString := toSingleString(allDiffs)
				earlyBreak, err := permutateDiffs(ctx, allDiffs, func(pdiffs []EntityDiff) (earlyBreak bool) {
					// Single visit
					allPerms[toSingleString(pdiffs)] = true
					// First permutation should be the same as original
					require.Equal(t, originalSingleString, toSingleString(pdiffs))
					// early break; this callback function should not be invoked again
					return true
				})
				assert.NoError(t, err)
				if len(allDiffs) > 0 {
					assert.True(t, earlyBreak)
					assert.Equal(t, 1, len(allPerms))
				} else {
					// no diffs means no permutations, and no call to the callback function
					assert.False(t, earlyBreak)
					assert.Equal(t, 0, len(allPerms))
				}
			})
		})
	}
}

func TestPermutationsContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	allDiffs := []EntityDiff{&DropViewEntityDiff{}}
	earlyBreak, err := permutateDiffs(ctx, allDiffs, func(pdiffs []EntityDiff) (earlyBreak bool) {
		return false
	})
	assert.True(t, earlyBreak) // proves that termination was due to context cancel
	assert.Error(t, err)       // proves that termination was due to context cancel
}

func TestSchemaDiff(t *testing.T) {
	ctx := context.Background()
	var (
		createQueries = []string{
			"create table t1 (id int primary key, info int not null);",
			"create table t2 (id int primary key, ts timestamp);",
			"create view v1 as select id from t1",
		}
	)
	tt := []struct {
		name               string
		fromQueries        []string
		toQueries          []string
		expectDiffs        int
		expectDeps         int
		sequential         bool
		conflictingDiffs   int
		entityOrder        []string // names of tables/views in expected diff order
		mysqlServerVersion string
		instantCapability  InstantDDLCapability
		fkStrategy         int
		expectError        string
		expectOrderedError string
	}{
		{
			name:              "no change",
			toQueries:         createQueries,
			entityOrder:       []string{},
			instantCapability: InstantDDLCapabilityIrrelevant,
		},
		{
			name: "three unrelated changes",
			toQueries: []string{
				"create table t1 (id int primary key, info int not null, ts timestamp);",
				"create table t2 (id int primary key, ts timestamp, v varchar);",
				"create view v1 as select id from t1",
				"create view v2 as select 1 from dual",
			},
			expectDiffs:       3,
			entityOrder:       []string{"t1", "t2", "v2"},
			instantCapability: InstantDDLCapabilityPossible,
		},
		{
			name: "instant DDL possible on 8.0.32",
			toQueries: []string{
				"create table t1 (id int primary key, ts timestamp, info int not null);",
				"create table t2 (id int primary key, ts timestamp);",
				"create view v1 as select id from t1",
			},
			expectDiffs:       1,
			entityOrder:       []string{"t1"},
			instantCapability: InstantDDLCapabilityPossible,
		},
		{
			name: "instant DDL impossible on 8.0.17",
			toQueries: []string{
				"create table t1 (id int primary key, ts timestamp, info int not null);",
				"create table t2 (id int primary key, ts timestamp);",
				"create view v1 as select id from t1",
			},
			mysqlServerVersion: "8.0.17",
			expectDiffs:        1,
			entityOrder:        []string{"t1"},
			instantCapability:  InstantDDLCapabilityImpossible,
		},
		{
			name: "three unrelated changes 2",
			toQueries: []string{
				"create table t1 (id int primary key, info int not null);",
				"create table t2 (id int primary key, ts timestamp, v varchar);",
				"create view v2 as select 1 from dual",
			},
			expectDiffs:       3,
			entityOrder:       []string{"v1", "t2", "v2"},
			instantCapability: InstantDDLCapabilityPossible,
		},
		// Subsequent
		{
			name: "add one fulltext key",
			toQueries: []string{
				"create table t1 (id int primary key, info int not null);",
				"create table t2 (id int primary key, ts timestamp, v varchar, fulltext key ftk1 (v));",
				"create view v1 as select id from t1",
			},
			expectDiffs:       1,
			expectDeps:        0,
			entityOrder:       []string{"t2"},
			instantCapability: InstantDDLCapabilityImpossible,
		},
		{
			// MySQL limitation: you cannot add two FULLTEXT keys in a single statement. `schemadiff` complies
			// with that limitation and turns such a request into two distinct statements.
			name: "add two fulltext keys",
			toQueries: []string{
				"create table t1 (id int primary key, info int not null);",
				"create table t2 (id int primary key, ts timestamp, v varchar, fulltext key ftk1 (v), fulltext key ftk2 (v));",
				"create view v1 as select id from t1",
			},
			expectDiffs:       2,
			expectDeps:        1,
			sequential:        true,
			entityOrder:       []string{"t2", "t2"},
			instantCapability: InstantDDLCapabilityImpossible,
		},
		{
			name: "add partition",
			fromQueries: []string{
				"create table t1 (id int primary key, info int not null);",
				"create table t2 (id int primary key, ts timestamp) partition by range (id) (partition p0 values less than (0), partition p1 values less than (1));",
				"create view v1 as select id from t1",
			},
			toQueries: []string{
				"create table t1 (id int primary key, info int not null);",
				"create table t2 (id int primary key, ts timestamp) partition by range (id) (partition p0 values less than (0), partition p1 values less than (1), partition p2 values less than (2));",
				"create view v1 as select id from t1",
			},
			expectDiffs:       1,
			expectDeps:        0,
			entityOrder:       []string{"t2"},
			instantCapability: InstantDDLCapabilityImpossible,
		},
		{
			// In MySQL, you cannot ALTER TABLE ADD COLUMN ..., ADD PARTITION in a single statement
			name: "add column, add partition",
			fromQueries: []string{
				"create table t1 (id int primary key, info int not null);",
				"create table t2 (id int primary key, ts timestamp) partition by range (id) (partition p0 values less than (0), partition p1 values less than (1));",
				"create view v1 as select id from t1",
			},
			toQueries: []string{
				"create table t1 (id int primary key, info int not null);",
				"create table t2 (id int primary key, ts timestamp, v varchar) partition by range (id) (partition p0 values less than (0), partition p1 values less than (1), partition p2 values less than (2));",
				"create view v1 as select id from t1",
			},
			expectDiffs:       2,
			expectDeps:        1,
			sequential:        true,
			entityOrder:       []string{"t2", "t2"},
			instantCapability: InstantDDLCapabilityImpossible,
		},
		{
			name: "add view",
			toQueries: append(
				createQueries,
				"create view v2 as select id from t2",
			),
			expectDiffs:       1,
			entityOrder:       []string{"v2"},
			instantCapability: InstantDDLCapabilityIrrelevant,
		},
		{
			name: "add view, alter table",
			toQueries: []string{
				"create table t1 (id int primary key, info int not null);",
				"create table t2 (id int primary key, ts timestamp, v varchar);",
				"create view v1 as select id from t1",
				"create view v2 as select id from t2",
			},
			expectDiffs:       2,
			expectDeps:        1,
			entityOrder:       []string{"t2", "v2"},
			instantCapability: InstantDDLCapabilityPossible,
		},
		{
			name: "alter view, alter table",
			toQueries: []string{
				"create table t1 (the_id int primary key, info int not null);",
				"create table t2 (id int primary key, ts timestamp);",
				"create view v1 as select the_id from t1",
			},
			expectDiffs:       2,
			expectDeps:        1,
			entityOrder:       []string{"t1", "v1"},
			conflictingDiffs:  2,
			instantCapability: InstantDDLCapabilityImpossible,
		},
		{
			name: "alter table, add view",
			toQueries: []string{
				"create table t1 (id int primary key, info int not null);",
				"create table t2 (id int primary key, ts timestamp, v varchar);",
				"create view v1 as select id from t1",
				"create view v2 as select id, v from t2",
			},
			expectDiffs:       2,
			expectDeps:        1,
			entityOrder:       []string{"t2", "v2"},
			instantCapability: InstantDDLCapabilityPossible,
		},
		{
			name: "create view depending on 2 tables, alter table",
			toQueries: []string{
				"create table t1 (id int primary key, info int not null);",
				"create table t2 (id int primary key, ts timestamp, v varchar);",
				"create view v1 as select id from t1",
				"create view v2 as select info, v from t1, t2",
			},
			expectDiffs:       2,
			expectDeps:        1,
			entityOrder:       []string{"t2", "v2"},
			instantCapability: InstantDDLCapabilityPossible,
		},
		{
			name: "create view depending on 2 tables, alter other table",
			toQueries: []string{
				"create table t1 (id int primary key, info int not null, dt datetime);",
				"create table t2 (id int primary key, ts timestamp);",
				"create view v1 as select id from t1",
				// "create view v2 as select id from t1",
				"create view v2 as select info, ts from t1, t2",
				// "create view v2 as select info, ts from t1, t2",
			},
			expectDiffs:       2,
			expectDeps:        1,
			entityOrder:       []string{"t1", "v2"},
			instantCapability: InstantDDLCapabilityPossible,
		},
		{
			name: "create view depending on 2 tables, alter both tables",
			toQueries: []string{
				"create table t1 (id int primary key, info int not null, dt datetime);",
				"create table t2 (id int primary key, ts timestamp, v varchar);",
				"create view v1 as select id from t1",
				"create view v2 as select info, ts from t1, t2",
			},
			expectDiffs:       3,
			expectDeps:        2,
			entityOrder:       []string{"t1", "t2", "v2"},
			instantCapability: InstantDDLCapabilityPossible,
		},
		{
			name: "alter view depending on 2 tables, uses new column, alter tables",
			toQueries: []string{
				"create table t1 (id int primary key, info int not null, dt datetime);",
				"create table t2 (id int primary key, ts timestamp, v varchar);",
				"create view v1 as select id from t1",
				"create view v2 as select info, v from t1, t2",
			},
			expectDiffs:       3,
			expectDeps:        2,
			entityOrder:       []string{"t1", "t2", "v2"},
			instantCapability: InstantDDLCapabilityPossible,
		},
		{
			name: "drop view",
			toQueries: []string{
				"create table t1 (id int primary key, info int not null);",
				"create table t2 (id int primary key, ts timestamp);",
			},
			expectDiffs:       1,
			expectDeps:        0,
			entityOrder:       []string{"v1"},
			instantCapability: InstantDDLCapabilityIrrelevant,
		},
		{
			name: "drop view, alter dependent table",
			toQueries: []string{
				"create table t1 (id int primary key, info int not null, dt datetime);",
				"create table t2 (id int primary key, ts timestamp);",
			},
			expectDiffs:       2,
			expectDeps:        1,
			entityOrder:       []string{"v1", "t1"},
			instantCapability: InstantDDLCapabilityPossible,
		},
		{
			name: "drop view, drop dependent table",
			toQueries: []string{
				"create table t2 (id int primary key, ts timestamp);",
			},
			expectDiffs:       2,
			expectDeps:        1,
			entityOrder:       []string{"v1", "t1"},
			instantCapability: InstantDDLCapabilityIrrelevant,
		},
		{
			name: "drop view, drop unrelated table",
			toQueries: []string{
				"create table t1 (id int primary key, info int not null);",
			},
			expectDiffs:       2,
			expectDeps:        0,
			entityOrder:       []string{"v1", "t2"},
			instantCapability: InstantDDLCapabilityIrrelevant,
		},
		{
			name: "alter view, drop table",
			toQueries: []string{
				"create table t2 (id int primary key, ts timestamp);",
				"create view v1 as select id from t2",
			},
			expectDiffs:       2,
			expectDeps:        1,
			entityOrder:       []string{"v1", "t1"},
			instantCapability: InstantDDLCapabilityIrrelevant,
		},
		{
			name: "alter view, add view",
			toQueries: []string{
				"create table t1 (id int primary key, info int not null);",
				"create table t2 (id int primary key, ts timestamp);",
				"create view v1 as select id, info from t1",
				"create view v2 as select info from v1",
			},
			expectDiffs:       2,
			expectDeps:        1,
			entityOrder:       []string{"v1", "v2"},
			instantCapability: InstantDDLCapabilityIrrelevant,
		},
		{
			name: "alter view, add view, 2",
			toQueries: []string{
				"create table t1 (id int primary key, info int not null);",
				"create table t2 (id int primary key, ts timestamp);",
				"create view v1 as select id, ts from v2",
				"create view v2 as select id, ts from t2",
			},
			expectDiffs:       2,
			expectDeps:        1,
			entityOrder:       []string{"v2", "v1"},
			instantCapability: InstantDDLCapabilityIrrelevant,
		},
		{
			name: "alter table, alter view, add view",
			toQueries: []string{
				"create table t1 (id int primary key, info int not null);",
				"create table t2 (id int primary key, ts timestamp, v varchar);",
				"create view v1 as select ts from t2",
				"create view v2 as select v from t2",
			},
			expectDiffs:       3,
			expectDeps:        2,
			entityOrder:       []string{"t2", "v1", "v2"},
			instantCapability: InstantDDLCapabilityPossible,
		},
		{
			name: "alter table, alter view, impossible sequence",
			fromQueries: []string{
				"create table t1 (id int primary key, info int not null);",
				"create view v1 as select id, info from t1",
			},
			toQueries: []string{
				"create table t1 (id int primary key, newcol int not null);",
				"create view v1 as select id, newcol from t1",
			},
			expectDiffs:       2,
			expectDeps:        1,
			conflictingDiffs:  2,
			instantCapability: InstantDDLCapabilityPossible,
		},

		// FKs
		{
			name: "create table with fk",
			toQueries: append(
				createQueries,
				"create table t3 (id int primary key, ts timestamp, t1_id int, foreign key (t1_id) references t1 (id) on delete no action);",
			),
			expectDiffs:       1,
			entityOrder:       []string{"t3"},
			instantCapability: InstantDDLCapabilityIrrelevant,
		},
		{
			name: "create two tables with fk",
			toQueries: append(
				createQueries,
				"create table tp (id int primary key, info int not null);",
				"create table t3 (id int primary key, ts timestamp, tp_id int, foreign key (tp_id) references tp (id) on delete no action);",
			),
			expectDiffs:       2,
			expectDeps:        1,
			entityOrder:       []string{"tp", "t3"},
			sequential:        true,
			instantCapability: InstantDDLCapabilityIrrelevant,
		},
		{
			name: "create two tables valid fk cycle",
			toQueries: append(
				createQueries,
				"create table t11 (id int primary key, i int, constraint f1101 foreign key (i) references t12 (id) on delete restrict);",
				"create table t12 (id int primary key, i int, constraint f1201 foreign key (i) references t11 (id) on delete set null);",
			),
			expectDiffs:        2,
			expectDeps:         2,
			sequential:         true,
			fkStrategy:         ForeignKeyCheckStrategyStrict,
			expectOrderedError: "no valid applicable order for diffs",
		},
		{
			name: "create two tables valid fk cycle, fk ignore",
			toQueries: append(
				createQueries,
				"create table t12 (id int primary key, i int, constraint f1201 foreign key (i) references t11 (id) on delete set null);",
				"create table t11 (id int primary key, i int, constraint f1101 foreign key (i) references t12 (id) on delete restrict);",
			),
			expectDiffs:       2,
			expectDeps:        2,
			entityOrder:       []string{"t11", "t12"}, // Note that the tables were reordered lexicographically
			sequential:        true,
			instantCapability: InstantDDLCapabilityIrrelevant,
			fkStrategy:        ForeignKeyCheckStrategyIgnore,
		},
		{
			name: "add FK",
			toQueries: []string{
				"create table t1 (id int primary key, info int not null);",
				"create table t2 (id int primary key, ts timestamp, t1_id int, foreign key (t1_id) references t1 (id) on delete no action);",
				"create view v1 as select id from t1",
			},
			expectDiffs:       1,
			expectDeps:        0,
			entityOrder:       []string{"t2"},
			instantCapability: InstantDDLCapabilityImpossible,
		},
		{
			name: "add FK pointing to new table",
			toQueries: []string{
				"create table t1 (id int primary key, info int not null);",
				"create table t2 (id int primary key, ts timestamp, tp_id int, foreign key (tp_id) references tp (id) on delete no action);",
				"create table tp (id int primary key, info int not null);",
				"create view v1 as select id from t1",
			},
			expectDiffs:       2,
			expectDeps:        1,
			sequential:        true,
			entityOrder:       []string{"tp", "t2"},
			instantCapability: InstantDDLCapabilityImpossible,
		},
		{
			name: "add two valid fk cycle references",
			toQueries: []string{
				"create table t1 (id int primary key, info int not null, i int, constraint f1 foreign key (i) references t2 (id) on delete restrict);",
				"create table t2 (id int primary key, ts timestamp,      i int, constraint f2 foreign key (i) references t1 (id) on delete set null);",
				"create view v1 as select id from t1",
			},
			expectDiffs:       2,
			expectDeps:        2,
			sequential:        false,
			fkStrategy:        ForeignKeyCheckStrategyStrict,
			entityOrder:       []string{"t1", "t2"},
			instantCapability: InstantDDLCapabilityImpossible,
		},
		{
			name: "add a table and a valid fk cycle references",
			toQueries: []string{
				"create table t0 (id int primary key, info int not null, i int, constraint f1 foreign key (i) references t2 (id) on delete restrict);",
				"create table t1 (id int primary key, info int not null);",
				"create table t2 (id int primary key, ts timestamp,      i int, constraint f2 foreign key (i) references t0 (id) on delete set null);",
				"create view v1 as select id from t1",
			},
			expectDiffs:       2,
			expectDeps:        2,
			sequential:        true,
			fkStrategy:        ForeignKeyCheckStrategyStrict,
			entityOrder:       []string{"t0", "t2"},
			instantCapability: InstantDDLCapabilityImpossible,
		},
		{
			name: "add a table and a valid fk cycle references, lelxicographically desc",
			toQueries: []string{
				"create table t1 (id int primary key, info int not null);",
				"create table t2 (id int primary key, ts timestamp,      i int, constraint f2 foreign key (i) references t9 (id) on delete set null);",
				"create table t9 (id int primary key, info int not null, i int, constraint f1 foreign key (i) references t2 (id) on delete restrict);",
				"create view v1 as select id from t1",
			},
			expectDiffs:       2,
			expectDeps:        2,
			sequential:        true,
			fkStrategy:        ForeignKeyCheckStrategyStrict,
			entityOrder:       []string{"t9", "t2"},
			instantCapability: InstantDDLCapabilityImpossible,
		},
		{
			name: "add FK, unrelated alter",
			toQueries: []string{
				"create table t1 (id int primary key, info int not null, key info_idx(info));",
				"create table t2 (id int primary key, ts timestamp, t1_id int, foreign key (t1_id) references t1 (id) on delete no action);",
				"create view v1 as select id from t1",
			},
			expectDiffs:       2,
			expectDeps:        1,
			entityOrder:       []string{"t1", "t2"},
			instantCapability: InstantDDLCapabilityImpossible,
		},
		{
			name: "add FK, add unrelated column",
			toQueries: []string{
				"create table t1 (id int primary key, info int not null, dt datetime);",
				"create table t2 (id int primary key, ts timestamp, t1_id int, foreign key (t1_id) references t1 (id) on delete no action);",
				"create view v1 as select id from t1",
			},
			expectDiffs:       2,
			expectDeps:        1,
			entityOrder:       []string{"t1", "t2"},
			instantCapability: InstantDDLCapabilityImpossible,
		},
		{
			name: "add FK, alter unrelated column",
			toQueries: []string{
				"create table t1 (id int primary key, info bigint not null);",
				"create table t2 (id int primary key, ts timestamp, t1_id int, foreign key (t1_id) references t1 (id) on delete no action);",
				"create view v1 as select id from t1",
			},
			expectDiffs:       2,
			expectDeps:        1,
			entityOrder:       []string{"t1", "t2"},
			instantCapability: InstantDDLCapabilityImpossible,
		},
		{
			name: "add FK, alter referenced column",
			toQueries: []string{
				"create table t1 (id bigint primary key, info bigint not null);",
				"create table t2 (id int primary key, ts timestamp, t1_id bigint, foreign key (t1_id) references t1 (id) on delete no action);",
				"create view v1 as select id from t1",
			},
			expectDiffs:       2,
			expectDeps:        1,
			sequential:        true,
			entityOrder:       []string{"t1", "t2"},
			instantCapability: InstantDDLCapabilityImpossible,
		},
		{
			name: "add column. create FK table referencing new column",
			toQueries: []string{
				"create table t1 (id int primary key, info int not null, p int, key p_idx (p));",
				"create table t2 (id int primary key, ts timestamp);",
				"create view v1 as select id from t1",
				"create table t3 (id int primary key, ts timestamp, t1_p int, foreign key (t1_p) references t1 (p) on delete no action);",
			},
			expectDiffs:       2,
			expectDeps:        1,
			entityOrder:       []string{"t1", "t3"},
			sequential:        true,
			instantCapability: InstantDDLCapabilityImpossible,
		},
		{
			name: "add column. add FK referencing new column",
			toQueries: []string{
				"create table t1 (id int primary key, info int not null, p int, key p_idx (p));",
				"create table t2 (id int primary key, ts timestamp, t1_p int, foreign key (t1_p) references t1 (p) on delete no action);",
				"create view v1 as select id from t1",
			},
			expectDiffs:       2,
			expectDeps:        1,
			sequential:        true,
			entityOrder:       []string{"t1", "t2"},
			instantCapability: InstantDDLCapabilityImpossible,
		},
		{
			name: "add column. add FK referencing new column, alphabetically desc",
			toQueries: []string{
				"create table t1 (id int primary key, info int not null, t2_p int, foreign key (t2_p) references t2 (p) on delete no action);",
				"create table t2 (id int primary key, ts timestamp, p int, key p_idx (p));",
				"create view v1 as select id from t1",
			},
			expectDiffs:       2,
			expectDeps:        1,
			sequential:        true,
			entityOrder:       []string{"t2", "t1"},
			instantCapability: InstantDDLCapabilityImpossible,
		},
		{
			name: "add index on parent. add FK to index column",
			toQueries: []string{
				"create table t1 (id int primary key, info int not null, key info_idx(info));",
				"create table t2 (id int primary key, ts timestamp, t1_info int not null, constraint parent_info_fk foreign key (t1_info) references t1 (info));",
				"create view v1 as select id from t1",
			},
			expectDiffs:       2,
			expectDeps:        1,
			sequential:        true,
			entityOrder:       []string{"t1", "t2"},
			instantCapability: InstantDDLCapabilityImpossible,
		},
		{
			name: "add index on parent with existing index. add FK to index column",
			fromQueries: []string{
				"create table t1 (id int primary key, info int not null, key existing_info_idx(info));",
				"create table t2 (id int primary key, ts timestamp);",
				"create view v1 as select id from t1",
			},
			toQueries: []string{
				"create table t1 (id int primary key, info int not null, key existing_info_idx(info), key info_idx(info));",
				"create table t2 (id int primary key, ts timestamp, t1_info int not null, constraint parent_info_fk foreign key (t1_info) references t1 (info));",
				"create view v1 as select id from t1",
			},
			expectDiffs:       2,
			expectDeps:        1,
			sequential:        false,
			entityOrder:       []string{"t1", "t2"},
			instantCapability: InstantDDLCapabilityImpossible,
		},
		{
			name: "modify fk column types, fail",
			fromQueries: []string{
				"create table t1 (id int primary key);",
				"create table t2 (id int primary key, ts timestamp, t1_id int, foreign key (t1_id) references t1 (id) on delete no action);",
			},
			toQueries: []string{
				"create table t1 (id bigint primary key);",
				"create table t2 (id int primary key, ts timestamp, t1_id bigint, foreign key (t1_id) references t1 (id) on delete no action);",
			},
			expectDiffs:       2,
			expectDeps:        0,
			sequential:        false,
			conflictingDiffs:  1,
			instantCapability: InstantDDLCapabilityImpossible,
		},
		{
			name: "add hierarchical constraints",
			fromQueries: []string{
				"create table t1 (id int primary key, ref int, key ref_idx (ref));",
				"create table t2 (id int primary key, ref int, key ref_idx (ref));",
				"create table t3 (id int primary key, ref int, key ref_idx (ref));",
				"create table t4 (id int primary key, ref int, key ref_idx (ref));",
				"create table t5 (id int primary key, ref int, key ref_idx (ref));",
			},
			toQueries: []string{
				"create table t1 (id int primary key, ref int, key ref_idx (ref));",
				"create table t2 (id int primary key, ref int, key ref_idx (ref), foreign key (ref) references t1 (id) on delete no action);",
				"create table t3 (id int primary key, ref int, key ref_idx (ref), foreign key (ref) references t2 (id) on delete no action);",
				"create table t4 (id int primary key, ref int, key ref_idx (ref), foreign key (ref) references t3 (id) on delete no action);",
				"create table t5 (id int primary key, ref int, key ref_idx (ref), foreign key (ref) references t1 (id) on delete no action);",
			},
			expectDiffs:       4,
			expectDeps:        2, // t2<->t3, t3<->t4
			sequential:        false,
			entityOrder:       []string{"t2", "t3", "t4", "t5"},
			instantCapability: InstantDDLCapabilityImpossible,
		},
		{
			name: "drop fk",
			fromQueries: []string{
				"create table t1 (id int primary key, info int not null);",
				"create table t2 (id int primary key, ts timestamp, t1_id int, foreign key (t1_id) references t1 (id) on delete no action);",
				"create view v1 as select id from t1",
			},
			toQueries:         createQueries,
			expectDiffs:       1,
			expectDeps:        0,
			entityOrder:       []string{"t2"},
			instantCapability: InstantDDLCapabilityImpossible,
		},
		{
			name: "drop fk, drop table",
			fromQueries: []string{
				"create table t1 (id int primary key, info int not null);",
				"create table t2 (id int primary key, ts timestamp, t1_id int, foreign key (t1_id) references t1 (id) on delete no action);",
			},
			toQueries: []string{
				"create table t2 (id int primary key, ts timestamp, t1_id int);",
			},
			expectDiffs:       2,
			expectDeps:        1,
			entityOrder:       []string{"t2", "t1"},
			instantCapability: InstantDDLCapabilityImpossible,
		},
		{
			name: "drop fk, drop column",
			fromQueries: []string{
				"create table t1 (id int primary key, info int not null, p int, key p_idx (p));",
				"create table t2 (id int primary key, ts timestamp, t1_p int, foreign key (t1_p) references t1 (p) on delete no action);",
			},
			toQueries: []string{
				"create table t1 (id int primary key, info int not null);",
				"create table t2 (id int primary key, ts timestamp, t1_p int);",
			},
			expectDiffs:       2,
			expectDeps:        1,
			entityOrder:       []string{"t2", "t1"},
			instantCapability: InstantDDLCapabilityImpossible,
		},
		{
			name: "reverse fk",
			fromQueries: []string{
				"create table t1 (id int primary key, p int, key p_idx (p));",
				"create table t2 (id int primary key, p int, key p_idx (p), foreign key (p) references t1 (p) on delete no action);",
			},
			toQueries: []string{
				"create table t1 (id int primary key, p int, key p_idx (p), foreign key (p) references t2 (p) on delete no action);",
				"create table t2 (id int primary key, p int, key p_idx (p));",
			},
			expectDiffs:       2,
			expectDeps:        2,
			entityOrder:       []string{"t2", "t1"},
			instantCapability: InstantDDLCapabilityImpossible,
		},
		{
			name: "add and drop FK, add and drop column, impossible order",
			fromQueries: []string{
				"create table t1 (id int primary key, p int, key p_idx (p));",
				"create table t2 (id int primary key, p int, key p_idx (p), foreign key (p) references t1 (p) on delete no action);",
			},
			toQueries: []string{
				"create table t1 (id int primary key, q int, key q_idx (q));",
				"create table t2 (id int primary key, q int, key q_idx (q), foreign key (q) references t1 (q) on delete no action);",
			},
			expectDiffs:       2,
			expectDeps:        1,
			sequential:        true,
			conflictingDiffs:  2,
			instantCapability: InstantDDLCapabilityImpossible,
		},
		{
			name: "two identical foreign keys in table, drop one",
			fromQueries: []string{
				"create table parent (id int primary key)",
				"create table t1 (id int primary key, i int, key i_idex (i), constraint f1 foreign key (i) references parent(id), constraint f2 foreign key (i) references parent(id))",
			},
			toQueries: []string{
				"create table parent (id int primary key)",
				"create table t1 (id int primary key, i int, key i_idex (i), constraint f1 foreign key (i) references parent(id))",
			},
			expectDiffs:       1,
			expectDeps:        0,
			entityOrder:       []string{"t1"},
			instantCapability: InstantDDLCapabilityImpossible,
		},
		{
			name: "test",
			fromQueries: []string{
				"CREATE TABLE t1 (id bigint NOT NULL, name varchar(255), PRIMARY KEY (id))",
			},
			toQueries: []string{
				"CREATE TABLE t1 (id bigint NOT NULL, name varchar(255), PRIMARY KEY (id), KEY idx_name (name))",
				"CREATE TABLE t3 (id bigint NOT NULL, name varchar(255), t1_id bigint, PRIMARY KEY (id), KEY t1_id (t1_id), KEY nameidx (name), CONSTRAINT t3_ibfk_1 FOREIGN KEY (t1_id) REFERENCES t1 (id) ON DELETE CASCADE ON UPDATE CASCADE, CONSTRAINT t3_ibfk_2 FOREIGN KEY (name) REFERENCES t1 (name) ON DELETE CASCADE ON UPDATE CASCADE)",
			},
			expectDiffs:       2,
			expectDeps:        1,
			sequential:        true,
			entityOrder:       []string{"t1", "t3"},
			instantCapability: InstantDDLCapabilityImpossible,
		},
	}
	baseHints := &DiffHints{
		RangeRotationStrategy: RangeRotationDistinctStatements,
	}
	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			vtenv, err := vtenv.New(vtenv.Options{
				MySQLServerVersion: tc.mysqlServerVersion,
			})
			require.NoError(t, err)
			env := NewEnv(vtenv, collations.CollationUtf8mb4ID)

			if tc.fromQueries == nil {
				tc.fromQueries = createQueries
			}
			fromSchema, err := NewSchemaFromQueries(env, tc.fromQueries)
			require.NoError(t, err)
			require.NotNil(t, fromSchema)

			toSchema, err := NewSchemaFromQueries(env, tc.toQueries)
			require.NoError(t, err)
			require.NotNil(t, toSchema)

			hints := *baseHints
			hints.ForeignKeyCheckStrategy = tc.fkStrategy
			schemaDiff, err := fromSchema.SchemaDiff(toSchema, &hints)
			if tc.expectError != "" {
				assert.ErrorContains(t, err, tc.expectError)
				return
			}
			require.NoError(t, err)

			allDiffs := schemaDiff.UnorderedDiffs()
			allDiffsStatements := []string{}
			for _, diff := range allDiffs {
				allDiffsStatements = append(allDiffsStatements, diff.CanonicalStatementString())
			}
			assert.Equalf(t, tc.expectDiffs, len(allDiffs), "found diffs: %v", allDiffsStatements)

			deps := schemaDiff.AllDependenciess()
			depsKeys := []string{}
			for _, dep := range deps {
				depsKeys = append(depsKeys, dep.hashKey())
			}
			assert.Equalf(t, tc.expectDeps, len(deps), "found deps: %v", depsKeys)
			assert.Equal(t, tc.sequential, schemaDiff.HasSequentialExecutionDependencies())

			orderedDiffs, err := schemaDiff.OrderedDiffs(ctx)
			if tc.expectOrderedError != "" {
				assert.ErrorContains(t, err, tc.expectOrderedError)
				return
			}
			if tc.conflictingDiffs > 0 {
				assert.Error(t, err)
				impossibleOrderErr, ok := err.(*ImpossibleApplyDiffOrderError)
				assert.True(t, ok)
				conflictingDiffsStatements := []string{}
				for _, diff := range impossibleOrderErr.ConflictingDiffs {
					conflictingDiffsStatements = append(conflictingDiffsStatements, diff.CanonicalStatementString())
				}
				assert.Equalf(t, tc.conflictingDiffs, len(impossibleOrderErr.ConflictingDiffs), "found conflicting diffs: %+v\n diff statements=%+v", conflictingDiffsStatements, allDiffsStatements)
			} else {
				require.NoErrorf(t, err, "Unordered diffs: %v", allDiffsStatements)
			}
			diffStatementStrings := []string{}
			for _, diff := range orderedDiffs {
				diffStatementStrings = append(diffStatementStrings, diff.CanonicalStatementString())
			}
			if tc.conflictingDiffs == 0 {
				// validate that the order of diffs is as expected (we don't check for the full diff statement,
				// just for the order of affected tables/views)
				require.NotNil(t, tc.entityOrder) // making sure we explicitly specified expected order
				assert.Equalf(t, len(tc.entityOrder), len(orderedDiffs), "expected %d diffs/entities per %v", len(tc.entityOrder), tc.entityOrder)
				diffEntities := []string{}
				for _, diff := range orderedDiffs {
					diffEntities = append(diffEntities, diff.EntityName())
				}
				assert.Equalf(t, tc.entityOrder, diffEntities, "diffs: %v", strings.Join(diffStatementStrings, ";\n"))
			}
			for _, diff := range orderedDiffs {
				s := diff.CanonicalStatementString()
				// Internal integrity, while we're here: see that the equivalence relation has entries for all diffs.
				_, err := schemaDiff.r.ElementClass(s)
				require.NoError(t, err)
			}
			instantCapability := schemaDiff.InstantDDLCapability()
			assert.Equal(t, tc.instantCapability, instantCapability)
		})

	}
}
