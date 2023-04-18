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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPermutations(t *testing.T) {
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
			expectPermutations: 24,
		},
	}
	hints := &DiffHints{RangeRotationStrategy: RangeRotationDistinctStatements}
	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {

			fromSchema, err := NewSchemaFromQueries(tc.fromQueries)
			require.NoError(t, err)
			require.NotNil(t, fromSchema)

			toSchema, err := NewSchemaFromQueries(tc.toQueries)
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
				allDiffs := schemaDiff.UnorderedDiffs()
				originalSingleString := toSingleString(allDiffs)
				earlyBreak := permutateDiffs(allDiffs, func(pdiffs []EntityDiff) (earlyBreak bool) {
					// cover all permutations
					allPerms[toSingleString(pdiffs)] = true
					if iteration == 0 {
						// First permutation should be the same as original
						require.Equal(t, originalSingleString, toSingleString(pdiffs))
					} else {
						// rest of permutations must be different than original (later we also verify they are all unique)
						require.NotEqualf(t, originalSingleString, toSingleString(pdiffs), "in iteration %d", iteration)
					}
					iteration++
					return false
				})
				assert.False(t, earlyBreak)
				assert.Equal(t, tc.expectPermutations, len(allPerms))
			})
			t.Run("early break", func(t *testing.T) {
				allPerms := map[string]bool{}
				allDiffs := schemaDiff.UnorderedDiffs()
				originalSingleString := toSingleString(allDiffs)
				earlyBreak := permutateDiffs(allDiffs, func(pdiffs []EntityDiff) (earlyBreak bool) {
					// Single visit
					allPerms[toSingleString(pdiffs)] = true
					// First permutation should be the same as original
					require.Equal(t, originalSingleString, toSingleString(pdiffs))
					// early break; this callback function should not be invoked again
					return true
				})
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

func TestSchemaDiff(t *testing.T) {
	var (
		createQueries = []string{
			"create table t1 (id int primary key, info int not null);",
			"create table t2 (id int primary key, ts timestamp);",
			"create view v1 as select id from t1",
		}
	)
	tt := []struct {
		name             string
		fromQueries      []string
		toQueries        []string
		expectDiffs      int
		expectDeps       int
		sequential       bool
		conflictingDiffs int
		entityOrder      []string // names of tables/views in expected diff order
	}{
		{
			name:        "no change",
			toQueries:   createQueries,
			entityOrder: []string{},
		},
		{
			name: "three unrelated changes",
			toQueries: []string{
				"create table t1 (id int primary key, info int not null, ts timestamp);",
				"create table t2 (id int primary key, ts timestamp, v varchar);",
				"create view v1 as select id from t1",
				"create view v2 as select 1 from dual",
			},
			expectDiffs: 3,
			entityOrder: []string{"t1", "t2", "v2"},
		},
		{
			name: "three unrelated changes 2",
			toQueries: []string{
				"create table t1 (id int primary key, info int not null);",
				"create table t2 (id int primary key, ts timestamp, v varchar);",
				"create view v2 as select 1 from dual",
			},
			expectDiffs: 3,
			entityOrder: []string{"v1", "t2", "v2"},
		},
		// Subsequent
		{
			name: "add one fulltext key",
			toQueries: []string{
				"create table t1 (id int primary key, info int not null);",
				"create table t2 (id int primary key, ts timestamp, v varchar, fulltext key ftk1 (v));",
				"create view v1 as select id from t1",
			},
			expectDiffs: 1,
			expectDeps:  0,
			entityOrder: []string{"t2"},
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
			expectDiffs: 2,
			expectDeps:  1,
			sequential:  true,
			entityOrder: []string{"t2", "t2"},
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
			expectDiffs: 1,
			expectDeps:  0,
			entityOrder: []string{"t2"},
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
			expectDiffs: 2,
			expectDeps:  1,
			sequential:  true,
			entityOrder: []string{"t2", "t2"},
		},
		{
			name: "add view",
			toQueries: append(
				createQueries,
				"create view v2 as select id from t2",
			),
			expectDiffs: 1,
			entityOrder: []string{"v2"},
		},
		{
			name: "add view, alter table",
			toQueries: []string{
				"create table t1 (id int primary key, info int not null);",
				"create table t2 (id int primary key, ts timestamp, v varchar);",
				"create view v1 as select id from t1",
				"create view v2 as select id from t2",
			},
			expectDiffs: 2,
			expectDeps:  1,
			entityOrder: []string{"t2", "v2"},
		},
		{
			name: "alter view, alter table",
			toQueries: []string{
				"create table t1 (the_id int primary key, info int not null);",
				"create table t2 (id int primary key, ts timestamp);",
				"create view v1 as select the_id from t1",
			},
			expectDiffs:      2,
			expectDeps:       1,
			entityOrder:      []string{"t1", "v1"},
			conflictingDiffs: 2,
		},
		{
			name: "alter table, add view",
			toQueries: []string{
				"create table t1 (id int primary key, info int not null);",
				"create table t2 (id int primary key, ts timestamp, v varchar);",
				"create view v1 as select id from t1",
				"create view v2 as select id, v from t2",
			},
			expectDiffs: 2,
			expectDeps:  1,
			entityOrder: []string{"t2", "v2"},
		},
		{
			name: "create view depending on 2 tables, alter table",
			toQueries: []string{
				"create table t1 (id int primary key, info int not null);",
				"create table t2 (id int primary key, ts timestamp, v varchar);",
				"create view v1 as select id from t1",
				"create view v2 as select info, v from t1, t2",
			},
			expectDiffs: 2,
			expectDeps:  1,
			entityOrder: []string{"t2", "v2"},
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
			expectDiffs: 2,
			expectDeps:  1,
			entityOrder: []string{"t1", "v2"},
		},
		{
			name: "create view depending on 2 tables, alter both tables",
			toQueries: []string{
				"create table t1 (id int primary key, info int not null, dt datetime);",
				"create table t2 (id int primary key, ts timestamp, v varchar);",
				"create view v1 as select id from t1",
				// "create view v2 as select id from t1",
				"create view v2 as select info, ts from t1, t2",
				// "create view v2 as select info, ts from t1, t2",
			},
			expectDiffs: 3,
			expectDeps:  2,
			entityOrder: []string{"t1", "v2", "t2"},
		},
		{
			name: "alter view depending on 2 tables, uses new column, alter tables",
			toQueries: []string{
				"create table t1 (id int primary key, info int not null, dt datetime);",
				"create table t2 (id int primary key, ts timestamp, v varchar);",
				"create view v1 as select id from t1",
				"create view v2 as select info, v from t1, t2",
			},
			expectDiffs: 3,
			expectDeps:  2,
			entityOrder: []string{"t1", "t2", "v2"},
		},
		{
			name: "drop view",
			toQueries: []string{
				"create table t1 (id int primary key, info int not null);",
				"create table t2 (id int primary key, ts timestamp);",
			},
			expectDiffs: 1,
			expectDeps:  0,
			entityOrder: []string{"v1"},
		},
		{
			name: "drop view, alter dependent table",
			toQueries: []string{
				"create table t1 (id int primary key, info int not null, dt datetime);",
				"create table t2 (id int primary key, ts timestamp);",
			},
			expectDiffs: 2,
			expectDeps:  1,
			entityOrder: []string{"v1", "t1"},
		},
		{
			name: "drop view, drop dependent table",
			toQueries: []string{
				"create table t2 (id int primary key, ts timestamp);",
			},
			expectDiffs: 2,
			expectDeps:  1,
			entityOrder: []string{"v1", "t1"},
		},
		{
			name: "drop view, drop unrelated table",
			toQueries: []string{
				"create table t1 (id int primary key, info int not null);",
			},
			expectDiffs: 2,
			expectDeps:  0,
			entityOrder: []string{"v1", "t2"},
		},
		{
			name: "alter view, drop table",
			toQueries: []string{
				"create table t2 (id int primary key, ts timestamp);",
				"create view v1 as select id from t2",
			},
			expectDiffs: 2,
			expectDeps:  1,
			entityOrder: []string{"v1", "t1"},
		},
		{
			name: "alter view, add view",
			toQueries: []string{
				"create table t1 (id int primary key, info int not null);",
				"create table t2 (id int primary key, ts timestamp);",
				"create view v1 as select id, info from t1",
				"create view v2 as select info from v1",
			},
			expectDiffs: 2,
			expectDeps:  1,
			entityOrder: []string{"v1", "v2"},
		},
		{
			name: "alter view, add view, 2",
			toQueries: []string{
				"create table t1 (id int primary key, info int not null);",
				"create table t2 (id int primary key, ts timestamp);",
				"create view v1 as select id, ts from v2",
				"create view v2 as select id, ts from t2",
			},
			expectDiffs: 2,
			expectDeps:  1,
			entityOrder: []string{"v2", "v1"},
		},
		{
			name: "alter table, alter view, add view",
			toQueries: []string{
				"create table t1 (id int primary key, info int not null);",
				"create table t2 (id int primary key, ts timestamp, v varchar);",
				"create view v1 as select ts from t2",
				"create view v2 as select v from t2",
			},
			expectDiffs: 3,
			expectDeps:  2,
			entityOrder: []string{"t2", "v1", "v2"},
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
			expectDiffs:      2,
			expectDeps:       1,
			conflictingDiffs: 2,
		},

		// FKs
		{
			name: "create table with fk",
			toQueries: append(
				createQueries,
				"create table t3 (id int primary key, ts timestamp, t1_id int, foreign key (t1_id) references t1 (id) on delete no action);",
			),
			expectDiffs: 1,
			entityOrder: []string{"t3"},
		},
		{
			name: "create two table with fk",
			toQueries: append(
				createQueries,
				"create table tp (id int primary key, info int not null);",
				"create table t3 (id int primary key, ts timestamp, tp_id int, foreign key (tp_id) references tp (id) on delete no action);",
			),
			expectDiffs: 2,
			expectDeps:  1,
			entityOrder: []string{"tp", "t3"},
		},
		{
			name: "add FK",
			toQueries: []string{
				"create table t1 (id int primary key, info int not null);",
				"create table t2 (id int primary key, ts timestamp, t1_id int, foreign key (t1_id) references t1 (id) on delete no action);",
				"create view v1 as select id from t1",
			},
			expectDiffs: 1,
			expectDeps:  0,
			entityOrder: []string{"t2"},
		},
		{
			name: "add FK pointing to new table",
			toQueries: []string{
				"create table t1 (id int primary key, info int not null);",
				"create table t2 (id int primary key, ts timestamp, tp_id int, foreign key (tp_id) references tp (id) on delete no action);",
				"create table tp (id int primary key, info int not null);",
				"create view v1 as select id from t1",
			},
			expectDiffs: 2,
			expectDeps:  1,
			sequential:  true,
			entityOrder: []string{"tp", "t2"},
		},
		{
			name: "add FK, unrelated alter",
			toQueries: []string{
				"create table t1 (id int primary key, info int not null, key info_idx(info));",
				"create table t2 (id int primary key, ts timestamp, t1_id int, foreign key (t1_id) references t1 (id) on delete no action);",
				"create view v1 as select id from t1",
			},
			expectDiffs: 2,
			expectDeps:  1,
			entityOrder: []string{"t1", "t2"},
		},
		{
			name: "add FK, add unrelated column",
			toQueries: []string{
				"create table t1 (id int primary key, info int not null, dt datetime);",
				"create table t2 (id int primary key, ts timestamp, t1_id int, foreign key (t1_id) references t1 (id) on delete no action);",
				"create view v1 as select id from t1",
			},
			expectDiffs: 2,
			expectDeps:  1,
			entityOrder: []string{"t1", "t2"},
		},
		{
			name: "add FK, alter unrelated column",
			toQueries: []string{
				"create table t1 (id int primary key, info bigint not null);",
				"create table t2 (id int primary key, ts timestamp, t1_id int, foreign key (t1_id) references t1 (id) on delete no action);",
				"create view v1 as select id from t1",
			},
			expectDiffs: 2,
			expectDeps:  1,
			entityOrder: []string{"t1", "t2"},
		},
		{
			name: "add FK, alter referenced column",
			toQueries: []string{
				"create table t1 (id bigint primary key, info bigint not null);",
				"create table t2 (id int primary key, ts timestamp, t1_id bigint, foreign key (t1_id) references t1 (id) on delete no action);",
				"create view v1 as select id from t1",
			},
			expectDiffs: 2,
			expectDeps:  1,
			sequential:  true,
			entityOrder: []string{"t1", "t2"},
		},
		{
			name: "add column. create FK table referencing new column",
			toQueries: []string{
				"create table t1 (id int primary key, info int not null, p int, key p_idx (p));",
				"create table t2 (id int primary key, ts timestamp);",
				"create view v1 as select id from t1",
				"create table t3 (id int primary key, ts timestamp, t1_p int, foreign key (t1_p) references t1 (p) on delete no action);",
			},
			expectDiffs: 2,
			expectDeps:  1,
			entityOrder: []string{"t1", "t3"},
		},
		{
			name: "add column. add FK referencing new column",
			toQueries: []string{
				"create table t1 (id int primary key, info int not null, p int, key p_idx (p));",
				"create table t2 (id int primary key, ts timestamp, t1_p int, foreign key (t1_p) references t1 (p) on delete no action);",
				"create view v1 as select id from t1",
			},
			expectDiffs: 2,
			expectDeps:  1,
			sequential:  true,
			entityOrder: []string{"t1", "t2"},
		},
		{
			name: "add column. add FK referencing new column, alphabetically desc",
			toQueries: []string{
				"create table t1 (id int primary key, info int not null, t2_p int, foreign key (t2_p) references t2 (p) on delete no action);",
				"create table t2 (id int primary key, ts timestamp, p int, key p_idx (p));",
				"create view v1 as select id from t1",
			},
			expectDiffs: 2,
			expectDeps:  1,
			sequential:  true,
			entityOrder: []string{"t2", "t1"},
		},
		{
			name: "drop fk",
			fromQueries: []string{
				"create table t1 (id int primary key, info int not null);",
				"create table t2 (id int primary key, ts timestamp, t1_id int, foreign key (t1_id) references t1 (id) on delete no action);",
				"create view v1 as select id from t1",
			},
			toQueries:   createQueries,
			expectDiffs: 1,
			expectDeps:  0,
			entityOrder: []string{"t2"},
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
			expectDiffs: 2,
			expectDeps:  1,
			entityOrder: []string{"t2", "t1"},
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
			expectDiffs: 2,
			expectDeps:  1,
			entityOrder: []string{"t2", "t1"},
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
			expectDiffs: 2,
			expectDeps:  2,
			entityOrder: []string{"t2", "t1"},
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
			expectDiffs:      2,
			expectDeps:       1,
			sequential:       true,
			conflictingDiffs: 2,
		},
	}
	hints := &DiffHints{RangeRotationStrategy: RangeRotationDistinctStatements}
	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			if tc.fromQueries == nil {
				tc.fromQueries = createQueries
			}
			fromSchema, err := NewSchemaFromQueries(tc.fromQueries)
			require.NoError(t, err)
			require.NotNil(t, fromSchema)

			toSchema, err := NewSchemaFromQueries(tc.toQueries)
			require.NoError(t, err)
			require.NotNil(t, toSchema)

			schemaDiff, err := fromSchema.SchemaDiff(toSchema, hints)
			require.NoError(t, err)

			allDiffs := schemaDiff.UnorderedDiffs()
			allDiffsStatements := []string{}
			for _, diff := range allDiffs {
				allDiffsStatements = append(allDiffsStatements, diff.CanonicalStatementString())
			}
			assert.Equalf(t, tc.expectDiffs, len(allDiffs), "found diffs: %v", allDiffsStatements)

			deps := schemaDiff.AllDeps()
			depsKeys := []string{}
			for _, dep := range deps {
				depsKeys = append(depsKeys, dep.hashKey())
			}
			assert.Equalf(t, tc.expectDeps, len(deps), "found deps: %v", depsKeys)
			assert.Equal(t, tc.sequential, schemaDiff.HasSequentialExecutionDeps())

			orderedDiffs, err := schemaDiff.OrderedDiffs()
			if tc.conflictingDiffs > 0 {
				require.Greater(t, tc.conflictingDiffs, 1) // self integrity. If there's a conflict, then obviously there's at least two conflicting diffs (a single diff has nothing to conflict with)
				assert.Error(t, err)
				impossibleOrderErr, ok := err.(*ImpossibleApplyDiffOrderError)
				assert.True(t, ok)
				assert.Equal(t, tc.conflictingDiffs, len(impossibleOrderErr.ConflictingDiffs))
			} else {
				require.NoError(t, err)
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
		})
	}
}
