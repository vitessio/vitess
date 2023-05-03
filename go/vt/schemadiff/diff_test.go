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
		hints    *DiffHints
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
			name:     "change of columns, boolean type",
			from:     "create table t(id int primary key)",
			to:       "create table t(id int primary key, i int, b boolean)",
			diff:     "alter table t add column i int, add column b tinyint(1)",
			cdiff:    "ALTER TABLE `t` ADD COLUMN `i` int, ADD COLUMN `b` tinyint(1)",
			action:   "alter",
			fromName: "t",
			toName:   "t",
		},
		{
			name: "alter columns from tinyint(1) to boolean",
			from: "create table t(id int primary key, b tinyint(1))",
			to:   "create table t(id int primary key, b boolean)",
		},
		{
			name: "alter columns from boolean to tinyint(1)",
			from: "create table t(id int primary key, b boolean)",
			to:   "create table t(id int primary key, b tinyint(1))",
		},
		{
			name:     "change of columns, boolean type, default true",
			from:     "create table t(id int primary key)",
			to:       "create table t(id int primary key, i int, b boolean default true)",
			diff:     "alter table t add column i int, add column b tinyint(1) default '1'",
			cdiff:    "ALTER TABLE `t` ADD COLUMN `i` int, ADD COLUMN `b` tinyint(1) DEFAULT '1'",
			action:   "alter",
			fromName: "t",
			toName:   "t",
		},
		{
			name:     "change of columns, boolean type, invalid default",
			from:     "create table t(id int primary key)",
			to:       "create table t(id int primary key, i int, b boolean default 'red')",
			diff:     "alter table t add column i int, add column b tinyint(1)",
			cdiff:    "ALTER TABLE `t` ADD COLUMN `i` int, ADD COLUMN `b` tinyint(1)",
			action:   "alter",
			fromName: "t",
			toName:   "t",
		},
		{
			name:   "create",
			to:     "create table t(id int primary key)",
			diff:   "create table t (\n\tid int,\n\tprimary key (id)\n)",
			cdiff:  "CREATE TABLE `t` (\n\t`id` int,\n\tPRIMARY KEY (`id`)\n)",
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
		{
			name:   "TableQualifierDeclared hint, to has qualifier",
			from:   "create table t1 (id int primary key, name int)",
			to:     "create table _vt.t1 (id int primary key, name bigint)",
			diff:   "alter table _vt.t1 modify column `name` bigint",
			cdiff:  "ALTER TABLE `_vt`.`t1` MODIFY COLUMN `name` bigint",
			action: "alter",
			hints: &DiffHints{
				TableQualifierHint: TableQualifierDeclared,
			},
		},
		{
			name:   "TableQualifierDeclared hint, from has qualifier",
			from:   "create table _vt.t1 (id int primary key, name int)",
			to:     "create table t1 (id int primary key, name bigint)",
			diff:   "alter table t1 modify column `name` bigint",
			cdiff:  "ALTER TABLE `t1` MODIFY COLUMN `name` bigint",
			action: "alter",
			hints: &DiffHints{
				TableQualifierHint: TableQualifierDeclared,
			},
		},
		{
			name:   "TableQualifierDefault, from has qualifier",
			from:   "create table _vt.t1 (id int primary key, name int)",
			to:     "create table t1 (id int primary key, name bigint)",
			diff:   "alter table _vt.t1 modify column `name` bigint",
			cdiff:  "ALTER TABLE `_vt`.`t1` MODIFY COLUMN `name` bigint",
			action: "alter",
		},
		{
			name:   "TableQualifierDefault, both have qualifiers",
			from:   "create table _vt.t1 (id int primary key, name int)",
			to:     "create table _vt.t1 (id int primary key, name bigint)",
			diff:   "alter table _vt.t1 modify column `name` bigint",
			cdiff:  "ALTER TABLE `_vt`.`t1` MODIFY COLUMN `name` bigint",
			action: "alter",
		},
		{
			name:   "TableQualifierDefault, create",
			to:     "create table _vt.t(id int primary key)",
			diff:   "create table _vt.t (\n\tid int,\n\tprimary key (id)\n)",
			cdiff:  "CREATE TABLE `_vt`.`t` (\n\t`id` int,\n\tPRIMARY KEY (`id`)\n)",
			action: "create",
			toName: "t",
		},
		{
			name:   "TableQualifierDeclared, create",
			to:     "create table _vt.t(id int primary key)",
			diff:   "create table _vt.t (\n\tid int,\n\tprimary key (id)\n)",
			cdiff:  "CREATE TABLE `_vt`.`t` (\n\t`id` int,\n\tPRIMARY KEY (`id`)\n)",
			action: "create",
			toName: "t",
			hints: &DiffHints{
				TableQualifierHint: TableQualifierDeclared,
			},
		},
		{
			name:     "TableQualifierDefault, drop",
			from:     "create table _vt.t(id int primary key)",
			diff:     "drop table _vt.t",
			cdiff:    "DROP TABLE `_vt`.`t`",
			action:   "drop",
			fromName: "t",
		},
		{
			name:     "TableQualifierDeclared, drop",
			from:     "create table _vt.t(id int primary key)",
			diff:     "drop table _vt.t",
			cdiff:    "DROP TABLE `_vt`.`t`",
			action:   "drop",
			fromName: "t",
			hints: &DiffHints{
				TableQualifierHint: TableQualifierDeclared,
			},
		},
	}
	for _, ts := range tt {
		t.Run(ts.name, func(t *testing.T) {
			var fromCreateTable *sqlparser.CreateTable
			hints := &DiffHints{}
			if ts.hints != nil {
				hints = ts.hints
			}
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
				"create table t (\n\tid int,\n\tprimary key (id)\n)",
			},
			cdiffs: []string{
				"CREATE TABLE `t` (\n\t`id` int,\n\tPRIMARY KEY (`id`)\n)",
			},
		},
		{
			name: "create table 2",
			from: ";;; ; ;    ;;;",
			to:   "create table t(id int primary key)",
			diffs: []string{
				"create table t (\n\tid int,\n\tprimary key (id)\n)",
			},
			cdiffs: []string{
				"CREATE TABLE `t` (\n\t`id` int,\n\tPRIMARY KEY (`id`)\n)",
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
				"alter table t2 modify column id bigint",
				"create table t4 (\n\tid int,\n\tprimary key (id)\n)",
			},
			cdiffs: []string{
				"DROP TABLE `t1`",
				"ALTER TABLE `t2` MODIFY COLUMN `id` bigint",
				"CREATE TABLE `t4` (\n\t`id` int,\n\tPRIMARY KEY (`id`)\n)",
			},
		},
		{
			name: "identical tables: drop and create",
			from: "create table t1(id int primary key); create table t2(id int unsigned primary key);",
			to:   "create table t1(id int primary key); create table t3(id int unsigned primary key);",
			diffs: []string{
				"drop table t2",
				"create table t3 (\n\tid int unsigned,\n\tprimary key (id)\n)",
			},
			cdiffs: []string{
				"DROP TABLE `t2`",
				"CREATE TABLE `t3` (\n\t`id` int unsigned,\n\tPRIMARY KEY (`id`)\n)",
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
			name: "drop and create all",
			from: "create table t1a(id int primary key); create table t2a(id int unsigned primary key); create table t3a(id smallint primary key); ",
			to:   "create table t1b(id bigint primary key); create table t2b(id int unsigned primary key); create table t3b(id int primary key); ",
			diffs: []string{
				"drop table t3a",
				"drop table t2a",
				"drop table t1a",
				"create table t1b (\n\tid bigint,\n\tprimary key (id)\n)",
				"create table t2b (\n\tid int unsigned,\n\tprimary key (id)\n)",
				"create table t3b (\n\tid int,\n\tprimary key (id)\n)",
			},
			cdiffs: []string{
				"DROP TABLE `t3a`",
				"DROP TABLE `t2a`",
				"DROP TABLE `t1a`",
				"CREATE TABLE `t1b` (\n\t`id` bigint,\n\tPRIMARY KEY (`id`)\n)",
				"CREATE TABLE `t2b` (\n\t`id` int unsigned,\n\tPRIMARY KEY (`id`)\n)",
				"CREATE TABLE `t3b` (\n\t`id` int,\n\tPRIMARY KEY (`id`)\n)",
			},
		},
		{
			name: "identical tables: multiple heuristic rename",
			from: "create table t1a(id int primary key); create table t2a(id int unsigned primary key); create table t3a(id smallint primary key); ",
			to:   "create table t1b(id bigint primary key); create table t2b(id int unsigned primary key); create table t3b(id int primary key); ",
			diffs: []string{
				"drop table t3a",
				"create table t1b (\n\tid bigint,\n\tprimary key (id)\n)",
				"rename table t2a to t2b",
				"rename table t1a to t3b",
			},
			cdiffs: []string{
				"DROP TABLE `t3a`",
				"CREATE TABLE `t1b` (\n\t`id` bigint,\n\tPRIMARY KEY (`id`)\n)",
				"RENAME TABLE `t2a` TO `t2b`",
				"RENAME TABLE `t1a` TO `t3b`",
			},
			tableRename: TableRenameHeuristicStatement,
		},
		// Foreign keys
		{
			name: "create tables with foreign keys, expect specific order",
			to:   "create table t7(id int primary key); create table t5 (id int primary key, i int, constraint f5 foreign key (i) references t7(id)); create table t4 (id int primary key, i int, constraint f4 foreign key (i) references t7(id));",
			diffs: []string{
				"create table t7 (\n\tid int,\n\tprimary key (id)\n)",
				"create table t4 (\n\tid int,\n\ti int,\n\tprimary key (id),\n\tkey f4 (i),\n\tconstraint f4 foreign key (i) references t7 (id)\n)",
				"create table t5 (\n\tid int,\n\ti int,\n\tprimary key (id),\n\tkey f5 (i),\n\tconstraint f5 foreign key (i) references t7 (id)\n)",
			},
			cdiffs: []string{
				"CREATE TABLE `t7` (\n\t`id` int,\n\tPRIMARY KEY (`id`)\n)",
				"CREATE TABLE `t4` (\n\t`id` int,\n\t`i` int,\n\tPRIMARY KEY (`id`),\n\tKEY `f4` (`i`),\n\tCONSTRAINT `f4` FOREIGN KEY (`i`) REFERENCES `t7` (`id`)\n)",
				"CREATE TABLE `t5` (\n\t`id` int,\n\t`i` int,\n\tPRIMARY KEY (`id`),\n\tKEY `f5` (`i`),\n\tCONSTRAINT `f5` FOREIGN KEY (`i`) REFERENCES `t7` (`id`)\n)",
			},
		},
		{
			name: "drop tables with foreign keys, expect specific order",
			from: "create table t7(id int primary key); create table t5 (id int primary key, i int, constraint f5 foreign key (i) references t7(id)); create table t4 (id int primary key, i int, constraint f4 foreign key (i) references t7(id));",
			diffs: []string{
				"drop table t5",
				"drop table t4",
				"drop table t7",
			},
			cdiffs: []string{
				"DROP TABLE `t5`",
				"DROP TABLE `t4`",
				"DROP TABLE `t7`",
			},
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
				"drop view v1",
				"drop table t1",
				"alter table t2 modify column id bigint",
				"alter view v2 as select id from t2",
				"create view v0 as select * from v2, t2",
				"create table t4 (\n\tid int,\n\tprimary key (id)\n)",
			},
			cdiffs: []string{
				"DROP VIEW `v1`",
				"DROP TABLE `t1`",
				"ALTER TABLE `t2` MODIFY COLUMN `id` bigint",
				"ALTER VIEW `v2` AS SELECT `id` FROM `t2`",
				"CREATE VIEW `v0` AS SELECT * FROM `v2`, `t2`",
				"CREATE TABLE `t4` (\n\t`id` int,\n\tPRIMARY KEY (`id`)\n)",
			},
		},
	}
	for _, ts := range tt {
		t.Run(ts.name, func(t *testing.T) {
			hints := &DiffHints{
				TableRenameStrategy: ts.tableRename,
			}
			diff, err := DiffSchemasSQL(ts.from, ts.to, hints)
			if ts.expectError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), ts.expectError)
			} else {
				assert.NoError(t, err)

				diffs, err := diff.OrderedDiffs()
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
					require.NoError(t, err)
					schema1SQL := schema1.ToSQL()

					schema2, err := NewSchemaFromSQL(ts.to)
					require.NoError(t, err)
					applied, err := schema1.Apply(diffs)
					require.NoError(t, err)

					// validate schema1 unaffected by Apply
					assert.Equal(t, schema1SQL, schema1.ToSQL())

					appliedDiff, err := schema2.SchemaDiff(applied, hints)
					require.NoError(t, err)
					assert.True(t, appliedDiff.Empty())
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
				diff, err := schema1.SchemaDiff(schema2, hints)
				require.NoError(t, err)
				diffs, err := diff.OrderedDiffs()
				assert.NoError(t, err)
				assert.NotEmpty(t, diffs)
				_, err = schema1.Apply(diffs)
				require.NoError(t, err)
				_, err = schema2.Apply(diffs)
				require.Error(t, err, "expected error applying to schema2. diffs: %v", diffs)
			}
			{
				diff, err := schema2.SchemaDiff(schema1, hints)
				require.NoError(t, err)
				diffs, err := diff.OrderedDiffs()
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
