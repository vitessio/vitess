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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSchemaDiff(t *testing.T) {
	var (
		createQueries = []string{
			"create table t1 (id int primary key, info int not null);",
			"create table t2 (id int primary key, ts timestamp);",
			"create view v1 as select id from t1",
		}
	)
	tt := []struct {
		name        string
		fromQueries []string
		toQueries   []string
		expectDiffs int
		expectDeps  int
		sequential  bool
	}{
		{
			name:      "no change",
			toQueries: createQueries,
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
		},
		{
			name: "add two fulltext keys",
			toQueries: []string{
				"create table t1 (id int primary key, info int not null);",
				"create table t2 (id int primary key, ts timestamp, v varchar, fulltext key ftk1 (v), fulltext key ftk2 (v));",
				"create view v1 as select id from t1",
			},
			expectDiffs: 2,
			expectDeps:  1,
			sequential:  true,
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
		},
		{
			name: "add view",
			toQueries: append(
				createQueries,
				"create view v2 as select id from t2",
			),
			expectDiffs: 1,
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
		},
		{
			name: "alter view, alter table",
			toQueries: []string{
				"create table t1 (the_id int primary key, info int not null);",
				"create table t2 (id int primary key, ts timestamp);",
				"create view v1 as select the_id from t1",
			},
			expectDiffs: 2,
			expectDeps:  1,
		},
		{
			name: "alter view, alter table, add view",
			toQueries: []string{
				"create table t1 (id int primary key, info int not null);",
				"create table t2 (id int primary key, ts timestamp, v varchar);",
				"create view v1 as select id from t1",
				"create view v2 as select id, v from t2",
			},
			expectDiffs: 2,
			expectDeps:  1,
		},
		{
			name: "alter view (2 tables), alter table",
			toQueries: []string{
				"create table t1 (id int primary key, info int not null);",
				"create table t2 (id int primary key, ts timestamp, v varchar);",
				"create view v1 as select id from t1",
				"create view v2 as select info, v from t1, t2",
			},
			expectDiffs: 2,
			expectDeps:  1,
		},
		{
			name: "alter view (2 tables), alter tables",
			toQueries: []string{
				"create table t1 (id int primary key, info int not null, dt datetime);",
				"create table t2 (id int primary key, ts timestamp, v varchar);",
				"create view v1 as select id from t1",
				"create view v2 as select info, v from t1, t2",
			},
			expectDiffs: 3,
			expectDeps:  2,
		},
		{
			name: "drop view",
			toQueries: []string{
				"create table t1 (id int primary key, info int not null);",
				"create table t2 (id int primary key, ts timestamp);",
			},
			expectDiffs: 1,
			expectDeps:  0,
		},
		{
			name: "drop view, alter dependent table",
			toQueries: []string{
				"create table t1 (id int primary key, info int not null, dt datetime);",
				"create table t2 (id int primary key, ts timestamp);",
			},
			expectDiffs: 2,
			expectDeps:  1,
		},
		{
			name: "drop view, drop dependent table",
			toQueries: []string{
				"create table t2 (id int primary key, ts timestamp);",
			},
			expectDiffs: 2,
			expectDeps:  1,
		},
		{
			name: "drop view, drop unrelated table",
			toQueries: []string{
				"create table t1 (id int primary key, info int not null);",
			},
			expectDiffs: 2,
			expectDeps:  0,
		},
		{
			name: "alter view, drop table",
			toQueries: []string{
				"create table t2 (id int primary key, ts timestamp);",
				"create view v1 as select id from t2",
			},
			expectDiffs: 2,
			expectDeps:  1,
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
		},
		// FKs
		{
			name: "create table with fk",
			toQueries: append(
				createQueries,
				"create table t3 (id int primary key, ts timestamp, t1_id int, foreign key (t1_id) references t1 (id) on delete no action);",
			),
			expectDiffs: 1,
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
		},
		{
			name: "add FK to new table",
			toQueries: []string{
				"create table t1 (id int primary key, info int not null);",
				"create table t2 (id int primary key, ts timestamp, tp_id int, foreign key (tp_id) references tp (id) on delete no action);",
				"create table tp (id int primary key, info int not null);",
				"create view v1 as select id from t1",
			},
			expectDiffs: 2,
			expectDeps:  1,
			sequential:  true,
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

			allDiffs := schemaDiff.AllDiffs()
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
		})
	}
}
