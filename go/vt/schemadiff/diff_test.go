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
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtenv"
)

func TestDiffTables(t *testing.T) {
	env57, err := vtenv.New(vtenv.Options{MySQLServerVersion: "5.7.9"})
	require.NoError(t, err)
	tt := []struct {
		name        string
		from        string
		to          string
		diff        string
		cdiff       string
		fromName    string
		toName      string
		action      string
		expectError string
		hints       *DiffHints
		env         *Environment
		annotated   []string
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
			annotated: []string{
				" CREATE TABLE `t` (", " \t`id` int,", "+\t`i` int,", " \tPRIMARY KEY (`id`)", " )",
			},
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
			annotated: []string{
				"+CREATE TABLE `t` (", "+\t`id` int,", "+\tPRIMARY KEY (`id`)", "+)",
			},
		},
		{
			name:     "drop",
			from:     "create table t(id int primary key)",
			diff:     "drop table t",
			cdiff:    "DROP TABLE `t`",
			action:   "drop",
			fromName: "t",
			annotated: []string{
				"-CREATE TABLE `t` (", "-\t`id` int,", "-\tPRIMARY KEY (`id`)", "-)",
			},
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
		{
			name: "changing table level defaults with column specific settings, ignore charset",
			from: "create table t (a varchar(64) CHARACTER SET latin1 COLLATE latin1_bin) default charset=latin1",
			to:   "create table t (a varchar(64) CHARACTER SET latin1 COLLATE latin1_bin)",
			hints: &DiffHints{
				AlterTableAlgorithmStrategy: AlterTableAlgorithmStrategyCopy,
				TableCharsetCollateStrategy: TableCharsetCollateIgnoreAlways,
			},
		},
		{
			name: "changing table level defaults with column specific settings based on collation, ignore charset",
			from: "create table t (a varchar(64) COLLATE latin1_bin) default charset=utf8mb4",
			to:   "create table t (a varchar(64) CHARACTER SET latin1 COLLATE latin1_bin)",
			hints: &DiffHints{
				AlterTableAlgorithmStrategy: AlterTableAlgorithmStrategyCopy,
				TableCharsetCollateStrategy: TableCharsetCollateIgnoreAlways,
			},
		},
		{
			name: "error on unknown collation",
			from: "create table t (a varchar(64) COLLATE latin1_nonexisting) default charset=utf8mb4",
			to:   "create table t (a varchar(64) CHARACTER SET latin1 COLLATE latin1_bin)",
			hints: &DiffHints{
				AlterTableAlgorithmStrategy: AlterTableAlgorithmStrategyCopy,
				TableCharsetCollateStrategy: TableCharsetCollateIgnoreAlways,
			},
			expectError: (&UnknownColumnCollationCharsetError{Column: "a", Collation: "latin1_nonexisting"}).Error(),
		},
		{
			name: "error on unknown charset",
			from: "create table t (a varchar(64)) default charset=latin_nonexisting collate=''",
			to:   "create table t (a varchar(64) CHARACTER SET latin1 COLLATE latin1_bin)",
			hints: &DiffHints{
				AlterTableAlgorithmStrategy: AlterTableAlgorithmStrategyCopy,
			},
			expectError: (&UnknownColumnCharsetCollationError{Column: "a", Charset: "latin_nonexisting"}).Error(),
		},
		{
			name:     "changing table level defaults with column specific settings",
			from:     "create table t (a varchar(64) CHARACTER SET latin1 COLLATE latin1_bin) default charset=latin1",
			to:       "create table t (a varchar(64) CHARACTER SET latin1 COLLATE latin1_bin)",
			diff:     "alter table t charset utf8mb4, algorithm = COPY",
			cdiff:    "ALTER TABLE `t` CHARSET utf8mb4, ALGORITHM = COPY",
			action:   "alter",
			fromName: "t",
			hints: &DiffHints{
				AlterTableAlgorithmStrategy: AlterTableAlgorithmStrategyCopy,
				TableCharsetCollateStrategy: TableCharsetCollateStrict,
			},
		},
		{
			name:     "changing table level defaults with column specific settings, table already normalized",
			from:     "create table t (a varchar(64)) default charset=latin1",
			to:       "create table t (a varchar(64) CHARACTER SET latin1 COLLATE latin1_bin)",
			diff:     "alter table t modify column a varchar(64) character set latin1 collate latin1_bin, charset utf8mb4, algorithm = COPY",
			cdiff:    "ALTER TABLE `t` MODIFY COLUMN `a` varchar(64) CHARACTER SET latin1 COLLATE latin1_bin, CHARSET utf8mb4, ALGORITHM = COPY",
			action:   "alter",
			fromName: "t",
			hints: &DiffHints{
				AlterTableAlgorithmStrategy: AlterTableAlgorithmStrategyCopy,
				TableCharsetCollateStrategy: TableCharsetCollateStrict,
			},
		},
		{
			name:   "changing table level charset to default",
			from:   `create table t (i int) default charset=latin1`,
			to:     `create table t (i int)`,
			action: "alter",
			diff:   "alter table t charset utf8mb4",
			cdiff:  "ALTER TABLE `t` CHARSET utf8mb4",
		},
		{
			name: "no changes with normalization and utf8mb4",
			from: `CREATE TABLE IF NOT EXISTS tables
			(
				TABLE_SCHEMA varchar(64) NOT NULL,
				TABLE_NAME varchar(64) NOT NULL,
				CREATE_STATEMENT longtext,
				CREATE_TIME BIGINT,
				PRIMARY KEY (TABLE_SCHEMA, TABLE_NAME)
			) engine = InnoDB`,
			to: "CREATE TABLE `tables` (" +
				"`TABLE_SCHEMA` varchar(64) NOT NULL," +
				"`TABLE_NAME` varchar(64) NOT NULL," +
				"`CREATE_STATEMENT` longtext," +
				"`CREATE_TIME` bigint DEFAULT NULL," +
				"PRIMARY KEY (`TABLE_SCHEMA`,`TABLE_NAME`)" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			hints: &DiffHints{
				TableCharsetCollateStrategy: TableCharsetCollateIgnoreAlways,
			},
		},
		{
			name: "no changes with normalization and utf8mb3",
			from: `CREATE TABLE IF NOT EXISTS tables
			(
				TABLE_SCHEMA varchar(64) NOT NULL,
				TABLE_NAME varchar(64) NOT NULL,
				CREATE_STATEMENT longtext,
				CREATE_TIME BIGINT,
				PRIMARY KEY (TABLE_SCHEMA, TABLE_NAME)
			) engine = InnoDB`,
			to: "CREATE TABLE `tables` (" +
				"`TABLE_SCHEMA` varchar(64) NOT NULL," +
				"`TABLE_NAME` varchar(64) NOT NULL," +
				"`CREATE_STATEMENT` longtext," +
				"`CREATE_TIME` bigint DEFAULT NULL," +
				"PRIMARY KEY (`TABLE_SCHEMA`,`TABLE_NAME`)" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci",
			hints: &DiffHints{
				TableCharsetCollateStrategy: TableCharsetCollateIgnoreAlways,
			},
			env: NewEnv(env57, collations.CollationUtf8mb3ID),
		},
	}
	env := NewTestEnv()
	for _, ts := range tt {
		t.Run(ts.name, func(t *testing.T) {
			var fromCreateTable *sqlparser.CreateTable
			hints := EmptyDiffHints()
			if ts.hints != nil {
				hints = ts.hints
			}
			if ts.env != nil {
				env = ts.env
			}
			if ts.from != "" {
				fromStmt, err := env.Parser().ParseStrictDDL(ts.from)
				assert.NoError(t, err)
				var ok bool
				fromCreateTable, ok = fromStmt.(*sqlparser.CreateTable)
				assert.True(t, ok)
			}
			var toCreateTable *sqlparser.CreateTable
			if ts.to != "" {
				toStmt, err := env.Parser().ParseStrictDDL(ts.to)
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
			dq, dqerr := DiffCreateTablesQueries(env, ts.from, ts.to, hints)
			d, err := DiffTables(env, fromCreateTable, toCreateTable, hints)
			switch {
			case ts.expectError != "":
				assert.ErrorContains(t, err, ts.expectError)
				assert.ErrorContains(t, dqerr, ts.expectError)
			case ts.diff == "":
				assert.NoError(t, err)
				assert.NoError(t, dqerr)
				if !assert.Nil(t, d) {
					assert.Failf(t, "found unexpected diff", "%v", d.CanonicalStatementString())
				}
				if !assert.Nil(t, dq) {
					assert.Failf(t, "found unexpected diff", "%v", dq.CanonicalStatementString())
				}
			default:
				assert.NoError(t, err)
				require.NotNil(t, d)
				require.False(t, d.IsEmpty())
				t.Run("statement", func(t *testing.T) {
					diff := d.StatementString()
					assert.Equal(t, ts.diff, diff)
					action, err := DDLActionStr(d)
					assert.NoError(t, err)
					assert.Equal(t, ts.action, action)

					// validate we can parse back the statement
					_, err = env.Parser().ParseStrictDDL(diff)
					assert.NoError(t, err)

					eFrom, eTo := d.Entities()
					if ts.fromName != "" {
						assert.Equal(t, ts.fromName, eFrom.Name())
					}
					if ts.toName != "" {
						assert.Equal(t, ts.toName, eTo.Name())
					}
				})
				t.Run("canonical", func(t *testing.T) {
					canonicalDiff := d.CanonicalStatementString()
					assert.Equal(t, ts.cdiff, canonicalDiff)
					action, err := DDLActionStr(d)
					assert.NoError(t, err)
					assert.Equal(t, ts.action, action)

					// validate we can parse back the statement
					_, err = env.Parser().ParseStrictDDL(canonicalDiff)
					assert.NoError(t, err)
				})
				t.Run("annotations", func(t *testing.T) {
					from, to, unified := d.Annotated()
					require.NotNil(t, from)
					require.NotNil(t, to)
					require.NotNil(t, unified)
					if ts.annotated != nil {
						// Optional test for assorted scenarios.
						unifiedExport := unified.Export()
						assert.Equal(t, ts.annotated, strings.Split(unifiedExport, "\n"))
					}
				})
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
		name      string
		from      string
		to        string
		diff      string
		cdiff     string
		fromName  string
		toName    string
		action    string
		isError   bool
		annotated []string
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
			annotated: []string{
				"-CREATE VIEW `v1`(`col1`, `col2`, `col3`) AS SELECT `a`, `b`, `c` FROM `t`",
				"+CREATE VIEW `v1`(`col1`, `col2`, `colother`) AS SELECT `a`, `b`, `c` FROM `t`",
			},
		},
		{
			name:   "create",
			to:     "create view v1 as select a, b, c from t",
			diff:   "create view v1 as select a, b, c from t",
			cdiff:  "CREATE VIEW `v1` AS SELECT `a`, `b`, `c` FROM `t`",
			action: "create",
			toName: "v1",
			annotated: []string{
				"+CREATE VIEW `v1` AS SELECT `a`, `b`, `c` FROM `t`",
			},
		},
		{
			name:     "drop",
			from:     "create view v1 as select a, b, c from t",
			diff:     "drop view v1",
			cdiff:    "DROP VIEW `v1`",
			action:   "drop",
			fromName: "v1",
			annotated: []string{
				"-CREATE VIEW `v1` AS SELECT `a`, `b`, `c` FROM `t`",
			},
		},
		{
			name: "none",
		},
	}
	hints := EmptyDiffHints()
	env := NewTestEnv()
	for _, ts := range tt {
		t.Run(ts.name, func(t *testing.T) {
			var fromCreateView *sqlparser.CreateView
			if ts.from != "" {
				fromStmt, err := env.Parser().ParseStrictDDL(ts.from)
				assert.NoError(t, err)
				var ok bool
				fromCreateView, ok = fromStmt.(*sqlparser.CreateView)
				assert.True(t, ok)
			}
			var toCreateView *sqlparser.CreateView
			if ts.to != "" {
				toStmt, err := env.Parser().ParseStrictDDL(ts.to)
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
			dq, dqerr := DiffCreateViewsQueries(env, ts.from, ts.to, hints)
			d, err := DiffViews(env, fromCreateView, toCreateView, hints)
			switch {
			case ts.isError:
				assert.Error(t, err)
				assert.Error(t, dqerr)
			case ts.diff == "":
				assert.NoError(t, err)
				assert.NoError(t, dqerr)
				if !assert.Nil(t, d) {
					assert.Failf(t, "found unexpected diff", "%v", d.CanonicalStatementString())
				}
				if !assert.Nil(t, dq) {
					assert.Failf(t, "found unexpected diff", "%v", dq.CanonicalStatementString())
				}
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
					_, err = env.Parser().ParseStrictDDL(diff)
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
					_, err = env.Parser().ParseStrictDDL(canonicalDiff)
					assert.NoError(t, err)
				}
				if ts.annotated != nil {
					// Optional test for assorted scenarios.
					_, _, unified := d.Annotated()
					unifiedExport := unified.Export()
					assert.Equal(t, ts.annotated, strings.Split(unifiedExport, "\n"))
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
	ctx := context.Background()
	tt := []struct {
		name        string
		from        string
		to          string
		diffs       []string
		cdiffs      []string
		expectError string
		tableRename int
		annotated   []string
		fkStrategy  int
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
			annotated: []string{
				"+CREATE TABLE `t` (\n+\t`id` int,\n+\tPRIMARY KEY (`id`)\n+)",
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
			annotated: []string{
				"-CREATE TABLE `t` (\n-\t`id` int,\n-\tPRIMARY KEY (`id`)\n-)",
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
			annotated: []string{
				"-CREATE TABLE `t1` (\n-\t`id` int,\n-\tPRIMARY KEY (`id`)\n-)",
				" CREATE TABLE `t2` (\n-\t`id` int,\n+\t`id` bigint,\n \tPRIMARY KEY (`id`)\n )",
				"+CREATE TABLE `t4` (\n+\t`id` int,\n+\tPRIMARY KEY (`id`)\n+)",
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
			annotated: []string{
				"-CREATE TABLE `t2a` (\n-\t`id` int unsigned,\n-\tPRIMARY KEY (`id`)\n-)\n+CREATE TABLE `t2b` (\n+\t`id` int unsigned,\n+\tPRIMARY KEY (`id`)\n+)",
			},
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
		{
			name: "tables with irregular names",
			from: "create table `t.2`(id int primary key); create table t3(`i.d` int primary key)",
			to:   "create table `t.2` (id bigint primary key); create table t3(`i.d` int unsigned primary key)",
			diffs: []string{
				"alter table `t.2` modify column id bigint",
				"alter table t3 modify column `i.d` int unsigned",
			},
			cdiffs: []string{
				"ALTER TABLE `t.2` MODIFY COLUMN `id` bigint",
				"ALTER TABLE `t3` MODIFY COLUMN `i.d` int unsigned",
			},
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
			name: "create tables with foreign keys, with invalid fk reference",
			from: "create table t (id int primary key)",
			to: `
				create table t (id int primary key);
				create table t11 (id int primary key, i int, constraint f1101a foreign key (i) references t12 (id) on delete restrict);
				create table t12 (id int primary key, i int, constraint f1201a foreign key (i) references t9 (id) on delete set null);
			`,
			expectError: "table `t12` foreign key references nonexistent table `t9`",
		},
		{
			name: "create tables with foreign keys, with invalid fk reference",
			from: "create table t (id int primary key)",
			to: `
				create table t (id int primary key);
				create table t11 (id int primary key, i int, constraint f1101b foreign key (i) references t12 (id) on delete restrict);
				create table t12 (id int primary key, i int, constraint f1201b foreign key (i) references t9 (id) on delete set null);
			`,
			expectError: "table `t12` foreign key references nonexistent table `t9`",
			fkStrategy:  ForeignKeyCheckStrategyIgnore,
		},
		{
			name: "create tables with foreign keys, with valid cycle",
			from: "create table t (id int primary key)",
			to: `
				create table t (id int primary key);
				create table t11 (id int primary key, i int, constraint f1101c foreign key (i) references t12 (id) on delete restrict);
				create table t12 (id int primary key, i int, constraint f1201c foreign key (i) references t11 (id) on delete set null);
			`,
			diffs: []string{
				"create table t11 (\n\tid int,\n\ti int,\n\tprimary key (id),\n\tkey f1101c (i),\n\tconstraint f1101c foreign key (i) references t12 (id) on delete restrict\n)",
				"create table t12 (\n\tid int,\n\ti int,\n\tprimary key (id),\n\tkey f1201c (i),\n\tconstraint f1201c foreign key (i) references t11 (id) on delete set null\n)",
			},
			cdiffs: []string{
				"CREATE TABLE `t11` (\n\t`id` int,\n\t`i` int,\n\tPRIMARY KEY (`id`),\n\tKEY `f1101c` (`i`),\n\tCONSTRAINT `f1101c` FOREIGN KEY (`i`) REFERENCES `t12` (`id`) ON DELETE RESTRICT\n)",
				"CREATE TABLE `t12` (\n\t`id` int,\n\t`i` int,\n\tPRIMARY KEY (`id`),\n\tKEY `f1201c` (`i`),\n\tCONSTRAINT `f1201c` FOREIGN KEY (`i`) REFERENCES `t11` (`id`) ON DELETE SET NULL\n)",
			},
			fkStrategy: ForeignKeyCheckStrategyIgnore,
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
		{
			// Making sure schemadiff distinguishes between VIEWs with different casing
			name: "case insensitive views",
			from: "create view v1 as select * from t; create table t(id int primary key); create view V1 as select * from t",
			to:   "",
			diffs: []string{
				"drop view v1",
				"drop view V1",
				"drop table t",
			},
			cdiffs: []string{
				"DROP VIEW `v1`",
				"DROP VIEW `V1`",
				"DROP TABLE `t`",
			},
		},
	}
	env := NewTestEnv()
	for _, ts := range tt {
		t.Run(ts.name, func(t *testing.T) {
			hints := &DiffHints{
				TableRenameStrategy:     ts.tableRename,
				ForeignKeyCheckStrategy: ts.fkStrategy,
			}
			diff, err := DiffSchemasSQL(env, ts.from, ts.to, hints)
			if ts.expectError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), ts.expectError)
			} else {
				require.NoError(t, err)

				diffs, err := diff.OrderedDiffs(ctx)
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
					_, err := env.Parser().ParseStrictDDL(s)
					assert.NoError(t, err)
				}
				for _, s := range cstatements {
					_, err := env.Parser().ParseStrictDDL(s)
					assert.NoError(t, err)
				}

				if ts.annotated != nil {
					// Optional test for assorted scenarios.
					if assert.Equalf(t, len(diffs), len(ts.annotated), "%+v", cstatements) {
						for i, d := range diffs {
							_, _, unified := d.Annotated()
							assert.Equal(t, ts.annotated[i], unified.Export())
						}
					}
				}

				{
					// Validate "apply()" on "from" converges with "to"
					schema1, err := NewSchemaFromSQL(env, ts.from)
					require.NoError(t, err)
					schema1SQL := schema1.ToSQL()

					schema2, err := NewSchemaFromSQL(env, ts.to)
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
	ctx := context.Background()
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
	hints := EmptyDiffHints()
	env := NewTestEnv()
	for _, ts := range tt {
		t.Run(ts.name, func(t *testing.T) {
			// Validate "apply()" on "from" converges with "to"
			schema1, err := NewSchemaFromSQL(env, ts.from)
			assert.NoError(t, err)
			schema2, err := NewSchemaFromSQL(env, ts.to)
			assert.NoError(t, err)

			{
				diff, err := schema1.SchemaDiff(schema2, hints)
				require.NoError(t, err)
				diffs, err := diff.OrderedDiffs(ctx)
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
				diffs, err := diff.OrderedDiffs(ctx)
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

func TestEntityDiffByStatement(t *testing.T) {
	env := NewTestEnv()

	tcases := []struct {
		query          string
		valid          bool
		expectAnotated bool
	}{
		{
			query:          "create table t1(id int primary key)",
			valid:          true,
			expectAnotated: true,
		},
		{
			query: "alter table t1 add column i int",
			valid: true,
		},
		{
			query: "rename table t1 to t2",
			valid: true,
		},
		{
			query: "drop table t1",
			valid: true,
		},
		{
			query:          "create view v1 as select * from t1",
			valid:          true,
			expectAnotated: true,
		},
		{
			query: "alter view v1 as select * from t2",
			valid: true,
		},
		{
			query: "drop view v1",
			valid: true,
		},
		{
			query: "drop database d1",
			valid: false,
		},
		{
			query: "optimize table t1",
			valid: false,
		},
	}

	for _, tcase := range tcases {
		t.Run(tcase.query, func(t *testing.T) {
			stmt, err := env.Parser().ParseStrictDDL(tcase.query)
			require.NoError(t, err)
			entityDiff := EntityDiffByStatement(stmt)
			if !tcase.valid {
				require.Nil(t, entityDiff)
				return
			}
			require.NotNil(t, entityDiff)
			require.NotNil(t, entityDiff.Statement())
			require.Equal(t, stmt, entityDiff.Statement())

			annotatedFrom, annotatedTo, annotatedUnified := entityDiff.Annotated()
			// EntityDiffByStatement doesn't have real entities behind it, just a wrapper around a statement.
			// Therefore, there are no annotations.
			if tcase.expectAnotated {
				assert.NotNil(t, annotatedFrom)
				assert.NotNil(t, annotatedTo)
				assert.NotNil(t, annotatedUnified)
			} else {
				assert.Nil(t, annotatedFrom)
				assert.Nil(t, annotatedTo)
				assert.Nil(t, annotatedUnified)
			}
		})
	}
}
