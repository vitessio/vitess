/*
Copyright 2022 The Vitess Authors.

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

	"vitess.io/vitess/go/vt/sqlparser"
)

func TestDiffTables(t *testing.T) {
	tt := []struct {
		name     string
		from     string
		to       string
		diff     string
		cdiff    string
		fromName string
		toName   string
		action   string
		isError  bool
	}{
		{
			name: "identical",
			from: "create table t(id int primary key)",
			to:   "create table t(id int primary key)",
		},
		{
			name:     "change of columns",
			from:     "create table t(id int primary key)",
			to:       "create table t(id int primary key, i int)",
			diff:     "alter table t add column i int",
			cdiff:    "ALTER TABLE `t` ADD COLUMN `i` int",
			action:   "alter",
			fromName: "t",
			toName:   "t",
		},
		{
			name:   "create",
			to:     "create table t(id int primary key)",
			diff:   "create table t (\n\tid int primary key\n)",
			cdiff:  "CREATE TABLE `t` (\n\t`id` int PRIMARY KEY\n)",
			action: "create",
			toName: "t",
		},
		{
			name:     "drop",
			from:     "create table t(id int primary key)",
			diff:     "drop table t",
			cdiff:    "DROP TABLE `t`",
			action:   "drop",
			fromName: "t",
		},
		{
			name: "none",
		},
	}
	hints := &DiffHints{}
	for _, ts := range tt {
		t.Run(ts.name, func(t *testing.T) {
			var fromCreateTable *sqlparser.CreateTable
			if ts.from != "" {
				fromStmt, err := sqlparser.ParseStrictDDL(ts.from)
				assert.NoError(t, err)
				var ok bool
				fromCreateTable, ok = fromStmt.(*sqlparser.CreateTable)
				assert.True(t, ok)
			}
			var toCreateTable *sqlparser.CreateTable
			if ts.to != "" {
				toStmt, err := sqlparser.ParseStrictDDL(ts.to)
				assert.NoError(t, err)
				var ok bool
				toCreateTable, ok = toStmt.(*sqlparser.CreateTable)
				assert.True(t, ok)
			}
			// Testing two paths:
			// - one, just diff the "CREATE TABLE..." strings
			// - two, diff the CreateTable constructs
			// Technically, DiffCreateTablesQueries calls DiffTables,
			// but we expose both to users of this library. so we want to make sure
			// both work as expected irrespective of any relationship between them.
			dq, dqerr := DiffCreateTablesQueries(ts.from, ts.to, hints)
			d, err := DiffTables(fromCreateTable, toCreateTable, hints)
			switch {
			case ts.isError:
				assert.Error(t, err)
				assert.Error(t, dqerr)
			case ts.diff == "":
				assert.NoError(t, err)
				assert.NoError(t, dqerr)
				assert.Nil(t, d)
				assert.Nil(t, dq)
			default:
				assert.NoError(t, err)
				require.NotNil(t, d)
				require.False(t, d.IsEmpty())
				{
					diff := d.StatementString()
					assert.Equal(t, ts.diff, diff)
					action, err := DDLActionStr(d)
					assert.NoError(t, err)
					assert.Equal(t, ts.action, action)

					// validate we can parse back the statement
					_, err = sqlparser.ParseStrictDDL(diff)
					assert.NoError(t, err)

					eFrom, eTo := d.Entities()
					if ts.fromName != "" {
						assert.Equal(t, ts.fromName, eFrom.Name())
					}
					if ts.toName != "" {
						assert.Equal(t, ts.toName, eTo.Name())
					}
				}
				{
					canonicalDiff := d.CanonicalStatementString()
					assert.Equal(t, ts.cdiff, canonicalDiff)
					action, err := DDLActionStr(d)
					assert.NoError(t, err)
					assert.Equal(t, ts.action, action)

					// validate we can parse back the statement
					_, err = sqlparser.ParseStrictDDL(canonicalDiff)
					assert.NoError(t, err)
				}
				// let's also check dq, and also validate that dq's statement is identical to d's
				assert.NoError(t, dqerr)
				require.NotNil(t, dq)
				require.False(t, dq.IsEmpty())
				diff := dq.StatementString()
				assert.Equal(t, ts.diff, diff)
			}
		})
	}
}

func TestDiffViews(t *testing.T) {
	tt := []struct {
		name     string
		from     string
		to       string
		diff     string
		cdiff    string
		fromName string
		toName   string
		action   string
		isError  bool
	}{
		{
			name: "identical",
			from: "create view v1 as select a, b, c from t",
			to:   "create view v1 as select a, b, c from t",
		},
		{
			name:     "change of column list, qualifiers",
			from:     "create view v1 (col1, `col2`, `col3`) as select `a`, `b`, c from t",
			to:       "create view v1 (`col1`, col2, colother) as select a, b, `c` from t",
			diff:     "alter view v1(col1, col2, colother) as select a, b, c from t",
			cdiff:    "ALTER VIEW `v1`(`col1`, `col2`, `colother`) AS SELECT `a`, `b`, `c` FROM `t`",
			action:   "alter",
			fromName: "v1",
			toName:   "v1",
		},
		{
			name:   "create",
			to:     "create view v1 as select a, b, c from t",
			diff:   "create view v1 as select a, b, c from t",
			cdiff:  "CREATE VIEW `v1` AS SELECT `a`, `b`, `c` FROM `t`",
			action: "create",
			toName: "v1",
		},
		{
			name:     "drop",
			from:     "create view v1 as select a, b, c from t",
			diff:     "drop view v1",
			cdiff:    "DROP VIEW `v1`",
			action:   "drop",
			fromName: "v1",
		},
		{
			name: "none",
		},
	}
	hints := &DiffHints{}
	for _, ts := range tt {
		t.Run(ts.name, func(t *testing.T) {
			var fromCreateView *sqlparser.CreateView
			if ts.from != "" {
				fromStmt, err := sqlparser.ParseStrictDDL(ts.from)
				assert.NoError(t, err)
				var ok bool
				fromCreateView, ok = fromStmt.(*sqlparser.CreateView)
				assert.True(t, ok)
			}
			var toCreateView *sqlparser.CreateView
			if ts.to != "" {
				toStmt, err := sqlparser.ParseStrictDDL(ts.to)
				assert.NoError(t, err)
				var ok bool
				toCreateView, ok = toStmt.(*sqlparser.CreateView)
				assert.True(t, ok)
			}
			// Testing two paths:
			// - one, just diff the "CREATE TABLE..." strings
			// - two, diff the CreateTable constructs
			// Technically, DiffCreateTablesQueries calls DiffTables,
			// but we expose both to users of this library. so we want to make sure
			// both work as expected irrespective of any relationship between them.
			dq, dqerr := DiffCreateViewsQueries(ts.from, ts.to, hints)
			d, err := DiffViews(fromCreateView, toCreateView, hints)
			switch {
			case ts.isError:
				assert.Error(t, err)
				assert.Error(t, dqerr)
			case ts.diff == "":
				assert.NoError(t, err)
				assert.NoError(t, dqerr)
				assert.Nil(t, d)
				assert.Nil(t, dq)
			default:
				assert.NoError(t, err)
				require.NotNil(t, d)
				require.False(t, d.IsEmpty())
				{
					diff := d.StatementString()
					assert.Equal(t, ts.diff, diff)
					action, err := DDLActionStr(d)
					assert.NoError(t, err)
					assert.Equal(t, ts.action, action)

					// validate we can parse back the statement
					_, err = sqlparser.ParseStrictDDL(diff)
					assert.NoError(t, err)

					eFrom, eTo := d.Entities()
					if ts.fromName != "" {
						assert.Equal(t, ts.fromName, eFrom.Name())
					}
					if ts.toName != "" {
						assert.Equal(t, ts.toName, eTo.Name())
					}
				}
				{
					canonicalDiff := d.CanonicalStatementString()
					assert.Equal(t, ts.cdiff, canonicalDiff)
					action, err := DDLActionStr(d)
					assert.NoError(t, err)
					assert.Equal(t, ts.action, action)

					// validate we can parse back the statement
					_, err = sqlparser.ParseStrictDDL(canonicalDiff)
					assert.NoError(t, err)
				}

				// let's also check dq, and also validate that dq's statement is identical to d's
				assert.NoError(t, dqerr)
				require.NotNil(t, dq)
				require.False(t, dq.IsEmpty())
				diff := dq.StatementString()
				assert.Equal(t, ts.diff, diff)
			}
		})
	}
}

func TestDiffSchemas(t *testing.T) {
	tt := []struct {
		name        string
		from        string
		to          string
		diffs       []string
		cdiffs      []string
		expectError string
		tableRename int
	}{
		{
			name: "identical tables",
			from: "create table t(id int primary key)",
			to:   "create table t(id int primary key)",
		},
		{
			name: "change of table column",
			from: "create table t(id int primary key, v varchar(10))",
			to:   "create table t(id int primary key, v varchar(20))",
			diffs: []string{
				"alter table t modify column v varchar(20)",
			},
			cdiffs: []string{
				"ALTER TABLE `t` MODIFY COLUMN `v` varchar(20)",
			},
		},
		{
			name: "change of table column tinyint 1 to longer",
			from: "create table t(id int primary key, i tinyint(1))",
			to:   "create table t(id int primary key, i tinyint(2))",
			diffs: []string{
				"alter table t modify column i tinyint",
			},
			cdiffs: []string{
				"ALTER TABLE `t` MODIFY COLUMN `i` tinyint",
			},
		},
		{
			name: "change of table column tinyint 2 to 1",
			from: "create table t(id int primary key, i tinyint(2))",
			to:   "create table t(id int primary key, i tinyint(1))",
			diffs: []string{
				"alter table t modify column i tinyint(1)",
			},
			cdiffs: []string{
				"ALTER TABLE `t` MODIFY COLUMN `i` tinyint(1)",
			},
		},
		{
			name: "change of table columns, added",
			from: "create table t(id int primary key)",
			to:   "create table t(id int primary key, i int)",
			diffs: []string{
				"alter table t add column i int",
			},
			cdiffs: []string{
				"ALTER TABLE `t` ADD COLUMN `i` int",
			},
		},
		{
			name: "change with function",
			from: "create table identifiers (id binary(16) NOT NULL DEFAULT (uuid_to_bin(uuid(),true)))",
			to:   "create table identifiers (company_id mediumint unsigned NOT NULL, id binary(16) NOT NULL DEFAULT (uuid_to_bin(uuid(),true)))",
			diffs: []string{
				"alter table identifiers add column company_id mediumint unsigned not null first",
			},
			cdiffs: []string{
				"ALTER TABLE `identifiers` ADD COLUMN `company_id` mediumint unsigned NOT NULL FIRST",
			},
		},
		{
			name: "change within functional index",
			from: "create table t1 (id mediumint unsigned NOT NULL, deleted_at timestamp, primary key (id), unique key deleted_check (id, (if((deleted_at is null),0,NULL))))",
			to:   "create table t1 (id mediumint unsigned NOT NULL, deleted_at timestamp, primary key (id), unique key deleted_check (id, (if((deleted_at is not null),0,NULL))))",
			diffs: []string{
				"alter table t1 drop key deleted_check, add unique key deleted_check (id, (if(deleted_at is not null, 0, null)))",
			},
			cdiffs: []string{
				"ALTER TABLE `t1` DROP KEY `deleted_check`, ADD UNIQUE KEY `deleted_check` (`id`, (if(`deleted_at` IS NOT NULL, 0, NULL)))",
			},
		},
		{
			name: "change for a check",
			from: "CREATE TABLE `t` (`id` int NOT NULL, `test` int NOT NULL DEFAULT '0', PRIMARY KEY (`id`), CONSTRAINT `Check1` CHECK ((`test` >= 0)))",
			to:   "CREATE TABLE `t` (`id` int NOT NULL, `test` int NOT NULL DEFAULT '0', PRIMARY KEY (`id`), CONSTRAINT `RenamedCheck1` CHECK ((`test` >= 0)))",
			diffs: []string{
				"alter table t drop check Check1, add constraint RenamedCheck1 check (test >= 0)",
			},
			cdiffs: []string{
				"ALTER TABLE `t` DROP CHECK `Check1`, ADD CONSTRAINT `RenamedCheck1` CHECK (`test` >= 0)",
			},
		},
		{
			name: "not enforce a check",
			from: "CREATE TABLE `t` (`id` int NOT NULL, `test` int NOT NULL DEFAULT '0', PRIMARY KEY (`id`), CONSTRAINT `Check1` CHECK ((`test` >= 0)))",
			to:   "CREATE TABLE `t` (`id` int NOT NULL, `test` int NOT NULL DEFAULT '0', PRIMARY KEY (`id`), CONSTRAINT `Check1` CHECK ((`test` >= 0)) NOT ENFORCED)",
			diffs: []string{
				"alter table t alter check Check1 not enforced",
			},
			cdiffs: []string{
				"ALTER TABLE `t` ALTER CHECK `Check1` NOT ENFORCED",
			},
		},
		{
			name: "enforce a check",
			from: "CREATE TABLE `t` (`id` int NOT NULL, `test` int NOT NULL DEFAULT '0', PRIMARY KEY (`id`), CONSTRAINT `Check1` CHECK ((`test` >= 0)) NOT ENFORCED)",
			to:   "CREATE TABLE `t` (`id` int NOT NULL, `test` int NOT NULL DEFAULT '0', PRIMARY KEY (`id`), CONSTRAINT `Check1` CHECK ((`test` >= 0)))",
			diffs: []string{
				"alter table t alter check Check1 enforced",
			},
			cdiffs: []string{
				"ALTER TABLE `t` ALTER CHECK `Check1` ENFORCED",
			},
		},
		{
			name: "change of table columns, removed",
			from: "create table t(id int primary key, i int)",
			to:   "create table t(id int primary key)",
			diffs: []string{
				"alter table t drop column i",
			},
			cdiffs: []string{
				"ALTER TABLE `t` DROP COLUMN `i`",
			},
		},
		{
			name: "create table",
			to:   "create table t(id int primary key)",
			diffs: []string{
				"create table t (\n\tid int primary key\n)",
			},
			cdiffs: []string{
				"CREATE TABLE `t` (\n\t`id` int PRIMARY KEY\n)",
			},
		},
		{
			name: "create table 2",
			from: ";;; ; ;    ;;;",
			to:   "create table t(id int primary key)",
			diffs: []string{
				"create table t (\n\tid int primary key\n)",
			},
			cdiffs: []string{
				"CREATE TABLE `t` (\n\t`id` int PRIMARY KEY\n)",
			},
		},
		{
			name: "drop table",
			from: "create table t(id int primary key)",
			diffs: []string{
				"drop table t",
			},
			cdiffs: []string{
				"DROP TABLE `t`",
			},
		},
		{
			name: "create, alter, drop tables",
			from: "create table t1(id int primary key); create table t2(id int primary key); create table t3(id int primary key)",
			to:   "create table t4(id int primary key); create table t2(id bigint primary key); create table t3(id int primary key)",
			diffs: []string{
				"drop table t1",
				"alter table t2 modify column id bigint primary key",
				"create table t4 (\n\tid int primary key\n)",
			},
			cdiffs: []string{
				"DROP TABLE `t1`",
				"ALTER TABLE `t2` MODIFY COLUMN `id` bigint PRIMARY KEY",
				"CREATE TABLE `t4` (\n\t`id` int PRIMARY KEY\n)",
			},
		},
		{
			name: "identical tables: drop and create",
			from: "create table t1(id int primary key); create table t2(id int unsigned primary key);",
			to:   "create table t1(id int primary key); create table t3(id int unsigned primary key);",
			diffs: []string{
				"drop table t2",
				"create table t3 (\n\tid int unsigned primary key\n)",
			},
			cdiffs: []string{
				"DROP TABLE `t2`",
				"CREATE TABLE `t3` (\n\t`id` int unsigned PRIMARY KEY\n)",
			},
		},
		{
			name: "identical tables: heuristic rename",
			from: "create table t1(id int primary key); create table t2a(id int unsigned primary key);",
			to:   "create table t1(id int primary key); create table t2b(id int unsigned primary key);",
			diffs: []string{
				"rename table t2a to t2b",
			},
			cdiffs: []string{
				"RENAME TABLE `t2a` TO `t2b`",
			},
			tableRename: TableRenameHeuristicStatement,
		},
		{
			name: "identical tables: drop and create",
			from: "create table t1a(id int primary key); create table t2a(id int unsigned primary key); create table t3a(id smallint primary key); ",
			to:   "create table t1b(id bigint primary key); create table t2b(id int unsigned primary key); create table t3b(id int primary key); ",
			diffs: []string{
				"drop table t1a",
				"drop table t2a",
				"drop table t3a",
				"create table t1b (\n\tid bigint primary key\n)",
				"create table t2b (\n\tid int unsigned primary key\n)",
				"create table t3b (\n\tid int primary key\n)",
			},
			cdiffs: []string{
				"DROP TABLE `t1a`",
				"DROP TABLE `t2a`",
				"DROP TABLE `t3a`",
				"CREATE TABLE `t1b` (\n\t`id` bigint PRIMARY KEY\n)",
				"CREATE TABLE `t2b` (\n\t`id` int unsigned PRIMARY KEY\n)",
				"CREATE TABLE `t3b` (\n\t`id` int PRIMARY KEY\n)",
			},
		},
		{
			name: "identical tables: multiple heuristic rename",
			from: "create table t1a(id int primary key); create table t2a(id int unsigned primary key); create table t3a(id smallint primary key); ",
			to:   "create table t1b(id bigint primary key); create table t2b(id int unsigned primary key); create table t3b(id int primary key); ",
			diffs: []string{
				"drop table t3a",
				"create table t1b (\n\tid bigint primary key\n)",
				"rename table t1a to t3b",
				"rename table t2a to t2b",
			},
			cdiffs: []string{
				"DROP TABLE `t3a`",
				"CREATE TABLE `t1b` (\n\t`id` bigint PRIMARY KEY\n)",
				"RENAME TABLE `t1a` TO `t3b`",
				"RENAME TABLE `t2a` TO `t2b`",
			},
			tableRename: TableRenameHeuristicStatement,
		},
		// Views
		{
			name: "identical views",
			from: "create table t(id int); create view v1 as select * from t",
			to:   "create table t(id int); create view v1 as select * from t",
		},
		{
			name: "modified view",
			from: "create table t(id int); create view v1 as select * from t",
			to:   "create table t(id int); create view v1 as select id from t",
			diffs: []string{
				"alter view v1 as select id from t",
			},
			cdiffs: []string{
				"ALTER VIEW `v1` AS SELECT `id` FROM `t`",
			},
		},
		{
			name: "drop view",
			from: "create table t(id int); create view v1 as select * from t",
			to:   "create table t(id int);",
			diffs: []string{
				"drop view v1",
			},
			cdiffs: []string{
				"DROP VIEW `v1`",
			},
		},
		{
			name: "create view",
			from: "create table t(id int)",
			to:   "create table t(id int); create view v1 as select id from t",
			diffs: []string{
				"create view v1 as select id from t",
			},
			cdiffs: []string{
				"CREATE VIEW `v1` AS SELECT `id` FROM `t`",
			},
		},
		{
			name:        "create view: unresolved dependencies",
			from:        "create table t(id int)",
			to:          "create table t(id int); create view v1 as select id from t2",
			expectError: (&ViewDependencyUnresolvedError{View: "v1"}).Error(),
		},
		{
			name: "convert table to view",
			from: "create table t(id int); create table v1 (id int)",
			to:   "create table t(id int); create view v1 as select * from t",
			diffs: []string{
				"drop table v1",
				"create view v1 as select * from t",
			},
			cdiffs: []string{
				"DROP TABLE `v1`",
				"CREATE VIEW `v1` AS SELECT * FROM `t`",
			},
		},
		{
			name: "convert view to table",
			from: "create table t(id int); create view v1 as select * from t",
			to:   "create table t(id int); create table v1 (id int)",
			diffs: []string{
				"drop view v1",
				"create table v1 (\n\tid int\n)",
			},
			cdiffs: []string{
				"DROP VIEW `v1`",
				"CREATE TABLE `v1` (\n\t`id` int\n)",
			},
		},
		{
			name:        "unsupported statement",
			from:        "create table t(id int)",
			to:          "drop table t",
			expectError: (&UnsupportedStatementError{Statement: "DROP TABLE `t`"}).Error(),
		},
		{
			name: "create, alter, drop tables and views",
			from: "create view v1 as select * from t1; create table t1(id int primary key); create table t2(id int primary key); create view v2 as select * from t2; create table t3(id int primary key);",
			to:   "create view v0 as select * from v2, t2; create table t4(id int primary key); create view v2 as select id from t2; create table t2(id bigint primary key); create table t3(id int primary key)",
			diffs: []string{
				"drop table t1",
				"drop view v1",
				"alter table t2 modify column id bigint primary key",
				"alter view v2 as select id from t2",
				"create table t4 (\n\tid int primary key\n)",
				"create view v0 as select * from v2, t2",
			},
			cdiffs: []string{
				"DROP TABLE `t1`",
				"DROP VIEW `v1`",
				"ALTER TABLE `t2` MODIFY COLUMN `id` bigint PRIMARY KEY",
				"ALTER VIEW `v2` AS SELECT `id` FROM `t2`",
				"CREATE TABLE `t4` (\n\t`id` int PRIMARY KEY\n)",
				"CREATE VIEW `v0` AS SELECT * FROM `v2`, `t2`",
			},
		},
	}
	for _, ts := range tt {
		t.Run(ts.name, func(t *testing.T) {
			hints := &DiffHints{
				TableRenameStrategy: ts.tableRename,
			}
			diffs, err := DiffSchemasSQL(ts.from, ts.to, hints)
			if ts.expectError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), ts.expectError)
			} else {
				assert.NoError(t, err)

				statements := []string{}
				cstatements := []string{}
				for _, d := range diffs {
					statements = append(statements, d.StatementString())
					cstatements = append(cstatements, d.CanonicalStatementString())
				}
				if ts.diffs == nil {
					ts.diffs = []string{}
				}
				assert.Equal(t, ts.diffs, statements)
				if ts.cdiffs == nil {
					ts.cdiffs = []string{}
				}
				assert.Equal(t, ts.cdiffs, cstatements)

				// validate we can parse back the diff statements
				for _, s := range statements {
					_, err := sqlparser.ParseStrictDDL(s)
					assert.NoError(t, err)
				}
				for _, s := range cstatements {
					_, err := sqlparser.ParseStrictDDL(s)
					assert.NoError(t, err)
				}

				{
					// Validate "apply()" on "from" converges with "to"
					schema1, err := NewSchemaFromSQL(ts.from)
					assert.NoError(t, err)
					schema1SQL := schema1.ToSQL()

					schema2, err := NewSchemaFromSQL(ts.to)
					assert.NoError(t, err)
					applied, err := schema1.Apply(diffs)
					require.NoError(t, err)

					// validate schema1 unaffected by Apply
					assert.Equal(t, schema1SQL, schema1.ToSQL())

					appliedDiff, err := schema2.Diff(applied, hints)
					require.NoError(t, err)
					assert.Empty(t, appliedDiff)
					assert.Equal(t, schema2.ToQueries(), applied.ToQueries())
				}
			}
		})
	}
}

func TestSchemaApplyError(t *testing.T) {
	tt := []struct {
		name string
		from string
		to   string
	}{
		{
			name: "added table",
			to:   "create table t2(id int primary key)",
		},
		{
			name: "different tables",
			from: "create table t1(id int primary key)",
			to:   "create table t2(id int primary key)",
		},
		{
			name: "added table 2",
			from: "create table t1(id int primary key)",
			to:   "create table t1(id int primary key); create table t2(id int primary key)",
		},
		{
			name: "modified tables",
			from: "create table t(id int primary key, i int)",
			to:   "create table t(id int primary key)",
		},
		{
			name: "added view",
			from: "create table t(id int); create view v1 as select * from t",
			to:   "create table t(id int); create view v1 as select * from t; create view v2 as select * from t",
		},
	}
	hints := &DiffHints{}
	for _, ts := range tt {
		t.Run(ts.name, func(t *testing.T) {
			// Validate "apply()" on "from" converges with "to"
			schema1, err := NewSchemaFromSQL(ts.from)
			assert.NoError(t, err)
			schema2, err := NewSchemaFromSQL(ts.to)
			assert.NoError(t, err)

			{
				diffs, err := schema1.Diff(schema2, hints)
				assert.NoError(t, err)
				assert.NotEmpty(t, diffs)
				_, err = schema1.Apply(diffs)
				require.NoError(t, err)
				_, err = schema2.Apply(diffs)
				require.Error(t, err, "expected error applying to schema2. diffs: %v", diffs)
			}
			{
				diffs, err := schema2.Diff(schema1, hints)
				assert.NoError(t, err)
				assert.NotEmpty(t, diffs, "schema1: %v, schema2: %v", schema1.ToSQL(), schema2.ToSQL())
				_, err = schema2.Apply(diffs)
				require.NoError(t, err)
				_, err = schema1.Apply(diffs)
				require.Error(t, err, "applying diffs to schema1: %v", schema1.ToSQL())
			}
		})
	}
}
