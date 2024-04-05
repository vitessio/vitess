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
	"fmt"
	"math/rand/v2"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	vterrors "vitess.io/vitess/go/errors"

	"vitess.io/vitess/go/vt/sqlparser"
)

var schemaTestCreateQueries = []string{
	"create view v5 as select * from t1, (select * from v3) as some_alias",
	"create table t3(id int, type enum('foo', 'bar') NOT NULL DEFAULT 'foo')",
	"create table t1(id int)",
	"create view v6 as select * from v4",
	"create view v4 as select * from t2 as something_else, v3",
	"create table t2(id int)",
	"create table t5(id int)",
	"create view v2 as select * from v3, t2",
	"create view v1 as select * from v3",
	"create view v3 as select *, id+1 as id_plus, id+2 from t3 as t3",
	"create view v0 as select 1 from DUAL",
	"create view v9 as select 1",
}

var schemaTestExpectSortedNames = []string{
	"t1",
	"t2",
	"t3",
	"t5",
	"v0", // level 1 ("dual" is an implicit table)
	"v3", // level 1
	"v9", // level 1 (no source table)
	"v1", // level 2
	"v2", // level 2
	"v4", // level 2
	"v5", // level 2
	"v6", // level 3
}

var schemaTestExpectSortedTableNames = []string{
	"t1",
	"t2",
	"t3",
	"t5",
}

var schemaTestExpectSortedViewNames = []string{
	"v0", // level 1 ("dual" is an implicit table)
	"v3", // level 1
	"v9", // level 1 (no source table)
	"v1", // level 2
	"v2", // level 2
	"v4", // level 2
	"v5", // level 2
	"v6", // level 3
}

var schemaTestToSQL = "CREATE TABLE `t1` (\n\t`id` int\n);\nCREATE TABLE `t2` (\n\t`id` int\n);\nCREATE TABLE `t3` (\n\t`id` int,\n\t`type` enum('foo', 'bar') NOT NULL DEFAULT 'foo'\n);\nCREATE TABLE `t5` (\n\t`id` int\n);\nCREATE VIEW `v0` AS SELECT 1 FROM `dual`;\nCREATE VIEW `v3` AS SELECT *, `id` + 1 AS `id_plus`, `id` + 2 FROM `t3` AS `t3`;\nCREATE VIEW `v9` AS SELECT 1 FROM `dual`;\nCREATE VIEW `v1` AS SELECT * FROM `v3`;\nCREATE VIEW `v2` AS SELECT * FROM `v3`, `t2`;\nCREATE VIEW `v4` AS SELECT * FROM `t2` AS `something_else`, `v3`;\nCREATE VIEW `v5` AS SELECT * FROM `t1`, (SELECT * FROM `v3`) AS `some_alias`;\nCREATE VIEW `v6` AS SELECT * FROM `v4`;\n"

func TestNewSchemaFromQueries(t *testing.T) {
	schema, err := NewSchemaFromQueries(NewTestEnv(), schemaTestCreateQueries)
	assert.NoError(t, err)
	require.NotNil(t, schema)

	assert.Equal(t, schemaTestExpectSortedNames, schema.EntityNames())
	assert.Equal(t, schemaTestExpectSortedTableNames, schema.TableNames())
	assert.Equal(t, schemaTestExpectSortedViewNames, schema.ViewNames())
}

func TestNewSchemaFromSQL(t *testing.T) {
	schema, err := NewSchemaFromSQL(NewTestEnv(), strings.Join(schemaTestCreateQueries, ";"))
	assert.NoError(t, err)
	require.NotNil(t, schema)

	assert.Equal(t, schemaTestExpectSortedNames, schema.EntityNames())
	assert.Equal(t, schemaTestExpectSortedTableNames, schema.TableNames())
	assert.Equal(t, schemaTestExpectSortedViewNames, schema.ViewNames())
}

func TestNewSchemaFromQueriesWithDuplicate(t *testing.T) {
	// v2 already exists
	queries := append(schemaTestCreateQueries,
		"create view v2 as select * from v1, t2",
	)
	_, err := NewSchemaFromQueries(NewTestEnv(), queries)
	assert.Error(t, err)
	assert.EqualError(t, err, (&ApplyDuplicateEntityError{Entity: "v2"}).Error())
}

func TestNewSchemaFromQueriesUnresolved(t *testing.T) {
	// v8 does not exist
	queries := append(schemaTestCreateQueries,
		"create view v7 as select * from v8, t2",
	)
	schema, err := NewSchemaFromQueries(NewTestEnv(), queries)
	assert.Error(t, err)
	assert.EqualError(t, err, (&ViewDependencyUnresolvedError{View: "v7"}).Error())
	v := schema.sorted[len(schema.sorted)-1]
	assert.IsType(t, &CreateViewEntity{}, v)
	assert.Equal(t, "CREATE VIEW `v7` AS SELECT * FROM `v8`, `t2`", v.Create().CanonicalStatementString())
}

func TestNewSchemaFromQueriesUnresolvedAlias(t *testing.T) {
	// v8 does not exist
	queries := append(schemaTestCreateQueries,
		"create view v7 as select * from something_else as t1, t2",
	)
	_, err := NewSchemaFromQueries(NewTestEnv(), queries)
	assert.Error(t, err)
	assert.EqualError(t, err, (&ViewDependencyUnresolvedError{View: "v7"}).Error())
}

func TestNewSchemaFromQueriesViewFromDual(t *testing.T) {
	// Schema will not contain any tables, just a view selecting from DUAL
	queries := []string{
		"create view v20 as select 1 from dual",
	}
	_, err := NewSchemaFromQueries(NewTestEnv(), queries)
	assert.NoError(t, err)
}

func TestNewSchemaFromQueriesViewFromDualImplicit(t *testing.T) {
	// Schema will not contain any tables, just a view implicitly selecting from DUAL
	queries := []string{
		"create view v20 as select 1",
	}
	_, err := NewSchemaFromQueries(NewTestEnv(), queries)
	assert.NoError(t, err)
}

func TestNewSchemaFromQueriesLoop(t *testing.T) {
	// v7 and v8 depend on each other
	queries := append(schemaTestCreateQueries,
		"create view v7 as select * from v8, t2",
		"create view v8 as select * from t1, v7",
	)
	_, err := NewSchemaFromQueries(NewTestEnv(), queries)
	require.Error(t, err)
	err = vterrors.UnwrapFirst(err)
	assert.EqualError(t, err, (&ViewDependencyUnresolvedError{View: "v7"}).Error())
}

func TestToSQL(t *testing.T) {
	schema, err := NewSchemaFromQueries(NewTestEnv(), schemaTestCreateQueries)
	assert.NoError(t, err)
	require.NotNil(t, schema)

	sql := schema.ToSQL()
	assert.Equal(t, schemaTestToSQL, sql)
}

func TestCopy(t *testing.T) {
	schema, err := NewSchemaFromQueries(NewTestEnv(), schemaTestCreateQueries)
	assert.NoError(t, err)
	require.NotNil(t, schema)

	schemaClone := schema.copy()
	assert.Equal(t, schema, schemaClone)
	assert.Equal(t, schema.ToSQL(), schemaClone.ToSQL())
	assert.False(t, schema == schemaClone)
}

func TestGetViewDependentTableNames(t *testing.T) {
	tt := []struct {
		name   string
		view   string
		tables []string
	}{
		{
			view:   "create view v6 as select * from v4",
			tables: []string{"v4"},
		},
		{
			view:   "create view v2 as select * from v3, t2",
			tables: []string{"v3", "t2"},
		},
		{
			view:   "create view v3 as select * from t3 as t3",
			tables: []string{"t3"},
		},
		{
			view:   "create view v3 as select * from t3 as something_else",
			tables: []string{"t3"},
		},
		{
			view:   "create view v5 as select * from t1, (select * from v3) as some_alias",
			tables: []string{"t1", "v3"},
		},
		{
			view:   "create view v0 as select 1 from DUAL",
			tables: []string{"dual"},
		},
		{
			view:   "create view v9 as select 1",
			tables: []string{"dual"},
		},
	}
	for _, ts := range tt {
		t.Run(ts.view, func(t *testing.T) {
			stmt, err := sqlparser.NewTestParser().ParseStrictDDL(ts.view)
			require.NoError(t, err)
			createView, ok := stmt.(*sqlparser.CreateView)
			require.True(t, ok)

			tables := getViewDependentTableNames(createView)
			assert.Equal(t, ts.tables, tables)
		})
	}
}

func TestGetForeignKeyParentTableNames(t *testing.T) {
	tt := []struct {
		name   string
		table  string
		tables []string
	}{
		{
			table:  "create table t1 (id int primary key, i int, foreign key (i) references parent(id))",
			tables: []string{"parent"},
		},
		{
			table:  "create table t1 (id int primary key, i int, constraint f foreign key (i) references parent(id))",
			tables: []string{"parent"},
		},
		{
			table:  "create table t1 (id int primary key, i int, constraint f foreign key (i) references parent(id) on delete cascade)",
			tables: []string{"parent"},
		},
		{
			table:  "create table t1 (id int primary key, i int, i2 int, constraint f foreign key (i) references parent(id) on delete cascade, constraint f2 foreign key (i2) references parent2(id) on delete restrict)",
			tables: []string{"parent", "parent2"},
		},
		{
			table:  "create table t1 (id int primary key, i int, i2 int, constraint f foreign key (i) references parent(id) on delete cascade, constraint f2 foreign key (i2) references parent(id) on delete restrict)",
			tables: []string{"parent", "parent"},
		},
	}
	for _, ts := range tt {
		t.Run(ts.table, func(t *testing.T) {
			stmt, err := sqlparser.NewTestParser().ParseStrictDDL(ts.table)
			require.NoError(t, err)
			createTable, ok := stmt.(*sqlparser.CreateTable)
			require.True(t, ok)

			tables := getForeignKeyParentTableNames(createTable)
			assert.Equal(t, ts.tables, tables)
		})
	}
}

func TestTableForeignKeyOrdering(t *testing.T) {
	fkQueries := []string{
		"create table t11 (id int primary key, i int, key ix (i), constraint f12 foreign key (i) references t12(id) on delete restrict, constraint f20 foreign key (i) references t20(id) on delete restrict)",
		"create table t15(id int, primary key(id))",
		"create view v09 as select * from v13, t17",
		"create table t20 (id int primary key, i int, key ix (i), constraint f15 foreign key (i) references t15(id) on delete restrict)",
		"create view v13 as select * from t20",
		"create table t12 (id int primary key, i int, key ix (i), constraint f15 foreign key (i) references t15(id) on delete restrict)",
		"create table t17 (id int primary key, i int, key ix (i), constraint f11 foreign key (i) references t11(id) on delete restrict, constraint f15 foreign key (i) references t15(id) on delete restrict)",
		"create table t16 (id int primary key, i int, key ix (i), constraint f11 foreign key (i) references t11(id) on delete restrict, constraint f15 foreign key (i) references t15(id) on delete restrict)",
		"create table t14 (id int primary key, i int, key ix (i), constraint f14 foreign key (i) references t14(id) on delete restrict)",
	}
	expectSortedTableNames := []string{
		"t14",
		"t15",
		"t12",
		"t20",
		"t11",
		"t16",
		"t17",
	}
	expectSortedViewNames := []string{
		"v13",
		"v09",
	}
	schema, err := NewSchemaFromQueries(NewTestEnv(), fkQueries)
	require.NoError(t, err)
	assert.NotNil(t, schema)

	assert.Equal(t, append(expectSortedTableNames, expectSortedViewNames...), schema.EntityNames())
	assert.Equal(t, expectSortedTableNames, schema.TableNames())
	assert.Equal(t, expectSortedViewNames, schema.ViewNames())
}

func TestInvalidSchema(t *testing.T) {
	tt := []struct {
		schema    string
		expectErr error
	}{
		{
			schema: "create table t11 (id int primary key, i int, key ix(i), constraint f11 foreign key (i) references t11(id) on delete restrict)",
		},
		{
			schema: "create table t10(id int primary key); create table t11 (id int primary key, i int, key ix(i), constraint f10 foreign key (i) references t10(id) on delete restrict)",
		},
		{
			schema:    "create table t11 (id int primary key, i int, constraint f11 foreign key (i7) references t11(id) on delete restrict)",
			expectErr: &InvalidColumnInForeignKeyConstraintError{Table: "t11", Constraint: "f11", Column: "i7"},
		},
		{
			schema:    "create table t11 (id int primary key, i int, constraint f11 foreign key (i) references t11(id, i) on delete restrict)",
			expectErr: &ForeignKeyColumnCountMismatchError{Table: "t11", Constraint: "f11", ColumnCount: 1, ReferencedTable: "t11", ReferencedColumnCount: 2},
		},
		{
			schema:    "create table t11 (id int primary key, i1 int, i2 int, constraint f11 foreign key (i1, i2) references t11(i1) on delete restrict)",
			expectErr: &ForeignKeyColumnCountMismatchError{Table: "t11", Constraint: "f11", ColumnCount: 2, ReferencedTable: "t11", ReferencedColumnCount: 1},
		},
		{
			schema:    "create table t11 (id int primary key, i int, constraint f12 foreign key (i) references t12 (id) on delete restrict)",
			expectErr: &ForeignKeyNonexistentReferencedTableError{Table: "t11", ReferencedTable: "t12"},
		},
		{
			schema:    "create view v as select 1 as id from dual; create table t11 (id int primary key, i int, constraint fv foreign key (i) references v (id) on delete restrict)",
			expectErr: &ForeignKeyReferencesViewError{Table: "t11", ReferencedView: "v"},
		},
		{
			// t11 self loop
			schema: "create table t11 (id int primary key, i int, constraint f11 foreign key (i) references t11 (id) on delete restrict)",
		},
		{
			// t12<->t11
			schema: `
				create table t11 (id int primary key, i int, constraint f1103 foreign key (i) references t12 (id) on delete restrict);
				create table t12 (id int primary key, i int, constraint f1203 foreign key (i) references t11 (id) on delete restrict)
			`,
		},
		{
			// t12<->t11
			schema: `
				create table t11 (id int primary key, i int, constraint f1101 foreign key (i) references t12 (i) on delete restrict);
				create table t12 (id int primary key, i int, constraint f1201 foreign key (i) references t11 (i) on delete set null)
			`,
		},
		{
			// t10, t12<->t11
			schema: `
				create table t10(id int primary key);
				create table t11 (id int primary key, i int, constraint f1102 foreign key (i) references t12 (id) on delete restrict);
				create table t12 (id int primary key, i int, constraint f1202 foreign key (i) references t11 (id) on delete restrict)
			`,
		},
		{
			// t10, t12<->t11<-t13
			schema: `
				create table t10(id int primary key);
				create table t11 (id int primary key, i int, constraint f1104 foreign key (i) references t12 (id) on delete restrict);
				create table t12 (id int primary key, i int, constraint f1204 foreign key (i) references t11 (id) on delete restrict);
				create table t13 (id int primary key, i int, constraint f13 foreign key (i) references t11 (id) on delete restrict)`,
		},
		{
			//      t10
			//       ^
			//       |
			//t12<->t11<-t13
			schema: `
				create table t10(id int primary key);
				create table t11 (id int primary key, i int, i10 int, constraint f111205 foreign key (i) references t12 (id) on delete restrict, constraint f111005 foreign key (i10) references t10 (id) on delete restrict);
				create table t12 (id int primary key, i int, constraint f1205 foreign key (id) references t11 (i) on delete restrict);
				create table t13 (id int primary key, i int, constraint f1305 foreign key (i) references t11 (id) on delete restrict)
			`,
		},
		{
			// t10, t12<->t11<-t13<-t14
			schema: `
				create table t10(id int primary key);
				create table t11 (id int primary key, i int, i10 int, constraint f1106 foreign key (i) references t12 (id) on delete restrict, constraint f111006 foreign key (i10) references t10 (id) on delete restrict);
				create table t12 (id int primary key, i int, constraint f1206 foreign key (i) references t11 (id) on delete restrict);
				create table t13 (id int primary key, i int, constraint f1306 foreign key (i) references t11 (id) on delete restrict);
				create table t14 (id int primary key, i int, constraint f1406 foreign key (i) references t13 (id) on delete restrict)
			`,
		},
		{
			// t10, t12<-t11<-t13<-t12
			schema: `
				create table t10(id int primary key);
				create table t11 (id int primary key, i int, key i_idx (i), i10 int, constraint f1107 foreign key (i) references t12 (id), constraint f111007 foreign key (i10) references t10 (id));
				create table t12 (id int primary key, i int, key i_idx (i), constraint f1207 foreign key (id) references t13 (i));
				create table t13 (id int primary key, i int, key i_idx (i), constraint f1307 foreign key (i) references t11 (i));
			`,
		},
		{
			schema:    "create table t11 (id int primary key, i int, key ix(i), constraint f11 foreign key (i) references t11(id2) on delete restrict)",
			expectErr: &InvalidReferencedColumnInForeignKeyConstraintError{Table: "t11", Constraint: "f11", ReferencedTable: "t11", ReferencedColumn: "id2"},
		},
		{
			schema:    "create table t10(id int primary key); create table t11 (id int primary key, i int, key ix(i), constraint f10 foreign key (i) references t10(x) on delete restrict)",
			expectErr: &InvalidReferencedColumnInForeignKeyConstraintError{Table: "t11", Constraint: "f10", ReferencedTable: "t10", ReferencedColumn: "x"},
		},
		{
			schema:    "create table t10(id int primary key, i int); create table t11 (id int primary key, i int, key ix(i), constraint f10 foreign key (i) references t10(i) on delete restrict)",
			expectErr: &MissingForeignKeyReferencedIndexError{Table: "t11", Constraint: "f10", ReferencedTable: "t10"},
		},
		{
			schema:    "create table t10(id int primary key); create table t11 (id int primary key, i int unsigned, key ix(i), constraint f10 foreign key (i) references t10(id) on delete restrict)",
			expectErr: &ForeignKeyColumnTypeMismatchError{Table: "t11", Constraint: "f10", Column: "i", ReferencedTable: "t10", ReferencedColumn: "id"},
		},
		{
			schema:    "create table t10(id int primary key); create table t11 (id int primary key, i bigint, key ix(i), constraint f10 foreign key (i) references t10(id) on delete restrict)",
			expectErr: &ForeignKeyColumnTypeMismatchError{Table: "t11", Constraint: "f10", Column: "i", ReferencedTable: "t10", ReferencedColumn: "id"},
		},
		{
			schema:    "create table t10(id bigint primary key); create table t11 (id int primary key, i int, key ix(i), constraint f10 foreign key (i) references t10(id) on delete restrict)",
			expectErr: &ForeignKeyColumnTypeMismatchError{Table: "t11", Constraint: "f10", Column: "i", ReferencedTable: "t10", ReferencedColumn: "id"},
		},
		{
			schema:    "create table t10(id bigint primary key); create table t11 (id int primary key, i varchar(100), key ix(i), constraint f10 foreign key (i) references t10(id) on delete restrict)",
			expectErr: &ForeignKeyColumnTypeMismatchError{Table: "t11", Constraint: "f10", Column: "i", ReferencedTable: "t10", ReferencedColumn: "id"},
		},
		{
			schema:    "create table t10(id int primary key, pid int null, key (pid)); create table t11 (id int primary key, pid int unsigned, key ix(pid), constraint f10 foreign key (pid) references t10(pid))",
			expectErr: &ForeignKeyColumnTypeMismatchError{Table: "t11", Constraint: "f10", Column: "pid", ReferencedTable: "t10", ReferencedColumn: "pid"},
		},
		{
			// NULL vs NOT NULL should be fine
			schema: "create table t10(id int primary key, pid int null, key (pid)); create table t11 (id int primary key, pid int not null, key ix(pid), constraint f10 foreign key (pid) references t10(pid))",
		},
		{
			// NOT NULL vs NULL should be fine
			schema: "create table t10(id int primary key, pid int not null, key (pid)); create table t11 (id int primary key, pid int null, key ix(pid), constraint f10 foreign key (pid) references t10(pid))",
		},
		{
			// InnoDB allows different string length
			schema: "create table t10(id varchar(50) primary key); create table t11 (id int primary key, i varchar(100), key ix(i), constraint f10 foreign key (i) references t10(id) on delete restrict)",
		},
		{
			// explicit charset/collation
			schema: "create table t10(id varchar(50) charset utf8mb4 collate utf8mb4_0900_ai_ci primary key); create table t11 (id int primary key, i varchar(100) charset utf8mb4 collate utf8mb4_0900_ai_ci, key ix(i), constraint f10 foreign key (i) references t10(id) on delete restrict)",
		},
		{
			// allowed: varchar->char
			schema: "create table t10(id varchar(50) charset utf8mb4 collate utf8mb4_0900_ai_ci primary key); create table t11 (id int primary key, i char(100) charset utf8mb4 collate utf8mb4_0900_ai_ci, key ix(i), constraint f10 foreign key (i) references t10(id) on delete restrict)",
		},
		{
			// allowed: char->varchar
			schema: "create table t10(id char(50) charset utf8mb4 collate utf8mb4_0900_ai_ci primary key); create table t11 (id int primary key, i varchar(50) charset utf8mb4 collate utf8mb4_0900_ai_ci, key ix(i), constraint f10 foreign key (i) references t10(id) on delete restrict)",
		},
		{
			schema:    "create table t10(id varchar(50) charset utf8mb3 primary key); create table t11 (id int primary key, i varchar(100) charset utf8mb4, key ix(i), constraint f10 foreign key (i) references t10(id) on delete restrict)",
			expectErr: &ForeignKeyColumnTypeMismatchError{Table: "t11", Constraint: "f10", Column: "i", ReferencedTable: "t10", ReferencedColumn: "id"},
		},
		{
			schema:    "create table t10(id varchar(50) charset utf8mb4 collate utf8mb4_0900_ai_ci primary key); create table t11 (id int primary key, i varchar(100) charset utf8mb4 collate utf8mb4_general_ci, key ix(i), constraint f10 foreign key (i) references t10(id) on delete restrict)",
			expectErr: &ForeignKeyColumnTypeMismatchError{Table: "t11", Constraint: "f10", Column: "i", ReferencedTable: "t10", ReferencedColumn: "id"},
		},
		{
			schema:    "create table t10(id VARCHAR(50) charset utf8mb4 collate utf8mb4_0900_ai_ci primary key); create table t11 (id int primary key, i VARCHAR(100) charset utf8mb4 collate utf8mb4_general_ci, key ix(i), constraint f10 foreign key (i) references t10(id) on delete restrict)",
			expectErr: &ForeignKeyColumnTypeMismatchError{Table: "t11", Constraint: "f10", Column: "i", ReferencedTable: "t10", ReferencedColumn: "id"},
		},
	}
	for _, ts := range tt {
		t.Run(ts.schema, func(t *testing.T) {

			_, err := NewSchemaFromSQL(NewTestEnv(), ts.schema)
			if ts.expectErr == nil {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
				assert.EqualError(t, err, ts.expectErr.Error())
			}
		})
	}
}

func TestInvalidTableForeignKeyReference(t *testing.T) {
	{
		fkQueries := []string{
			"create table t10 (id int primary key)",
			"create table t11 (id int primary key, i int, constraint f12 foreign key (i) references t12(id) on delete restrict)",
			"create table t15(id int, primary key(id))",
		}
		s, err := NewSchemaFromQueries(NewTestEnv(), fkQueries)
		assert.Error(t, err)
		// Even though there's errors, we still expect the schema to have been created.
		assert.NotNil(t, s)
		// Even though t11 caused an error, we still expect the schema to have parsed all tables.
		assert.Equalf(t, 3, len(s.Entities()), "found: %+v", s.EntityNames())
		t11 := s.Table("t11")
		assert.NotNil(t, t11)
		// validate t11 table definition is complete, even though it was invalid.
		assert.Equal(t, "create table t11 (\n\tid int,\n\ti int,\n\tprimary key (id),\n\tkey f12 (i),\n\tconstraint f12 foreign key (i) references t12 (id) on delete restrict\n)", t11.Create().StatementString())
		assert.EqualError(t, err, (&ForeignKeyNonexistentReferencedTableError{Table: "t11", ReferencedTable: "t12"}).Error())
	}
	{
		fkQueries := []string{
			"create table t13 (id int primary key, i int, constraint f11 foreign key (i) references t11(id) on delete restrict)",
			"create table t11 (id int primary key, i int, constraint f12 foreign key (i) references t12(id) on delete restrict)",
			"create table t12 (id int primary key, i int, constraint f13 foreign key (i) references t13(id) on delete restrict)",
		}
		_, err := NewSchemaFromQueries(NewTestEnv(), fkQueries)
		assert.NoError(t, err)
	}
	{
		fkQueries := []string{
			"create table t13 (id int primary key, i int, constraint f11 foreign key (i) references t11(i) on delete restrict)",
			"create table t11 (id int primary key, i int, constraint f12 foreign key (i) references t12(i) on delete restrict)",
			"create table t12 (id int primary key, i int, constraint f13 foreign key (i) references t13(i) on delete restrict)",
		}
		_, err := NewSchemaFromQueries(NewTestEnv(), fkQueries)
		assert.NoError(t, err)
	}
	{
		fkQueries := []string{
			"create table t13 (id int primary key, i int, constraint f11 foreign key (i) references t11(id) on delete restrict)",
			"create table t11 (id int primary key, i int, constraint f0 foreign key (i) references t0(id) on delete restrict)",
			"create table t12 (id int primary key, i int, constraint f13 foreign key (i) references t13(id) on delete restrict)",
		}
		_, err := NewSchemaFromQueries(NewTestEnv(), fkQueries)
		assert.Error(t, err)
		assert.ErrorContains(t, err, (&ForeignKeyNonexistentReferencedTableError{Table: "t11", ReferencedTable: "t0"}).Error())
	}
	{
		fkQueries := []string{
			"create table t13 (id int primary key, i int, constraint f11 foreign key (i) references t11(id) on delete restrict, constraint f12 foreign key (i) references t12(id) on delete restrict)",
			"create table t11 (id int primary key, i int, constraint f0 foreign key (i) references t0(id) on delete restrict)",
			"create table t12 (id int primary key, i int, constraint f13 foreign key (i) references t13(id) on delete restrict)",
		}
		_, err := NewSchemaFromQueries(NewTestEnv(), fkQueries)
		assert.Error(t, err)
		assert.ErrorContains(t, err, (&ForeignKeyNonexistentReferencedTableError{Table: "t11", ReferencedTable: "t0"}).Error())
	}
}

func TestGetEntityColumnNames(t *testing.T) {
	var queries = []string{
		"create table t1(id int, state int, some char(5))",
		"create table t2(id int primary key, c char(5))",
		"create view v1 as select id as id from t1",
		"create view v2 as select 3+1 as `id`, state as state, some as thing from t1",
		"create view v3 as select `id` as `id`, state as state, thing as another from v2",
		"create view v4 as select 1 as `ok` from dual",
		"create view v5 as select 1 as `ok` from DUAL",
		"create view v6 as select ok as `ok` from v5",
		"create view v7 as select * from t1",
		"create view v8 as select * from v7",
		"create view v9 as select * from v8, v6",
		"create view va as select * from v6, v8",
		"create view vb as select *, now() from v8",
	}

	schema, err := NewSchemaFromQueries(NewTestEnv(), queries)
	require.NoError(t, err)
	require.NotNil(t, schema)

	expectedColNames := map[string]([]string){
		"t1": []string{"id", "state", "some"},
		"t2": []string{"id", "c"},
		"v1": []string{"id"},
		"v2": []string{"id", "state", "thing"},
		"v3": []string{"id", "state", "another"},
		"v4": []string{"ok"},
		"v5": []string{"ok"},
		"v6": []string{"ok"},
		"v7": []string{"id", "state", "some"},
		"v8": []string{"id", "state", "some"},
		"v9": []string{"id", "state", "some", "ok"},
		"va": []string{"ok", "id", "state", "some"},
		"vb": []string{"id", "state", "some", "now()"},
	}
	entities := schema.Entities()
	require.Equal(t, len(entities), len(expectedColNames))

	tcmap := newDeclarativeSchemaInformation(NewTestEnv())
	// we test by order of dependency:
	for _, e := range entities {
		tbl := e.Name()
		t.Run(tbl, func(t *testing.T) {
			identifiers, err := schema.getEntityColumnNames(tbl, tcmap)
			assert.NoError(t, err)
			names := []string{}
			for _, ident := range identifiers {
				names = append(names, ident.String())
			}
			// compare columns. We disregard order.
			expectNames := expectedColNames[tbl][:]
			sort.Strings(names)
			sort.Strings(expectNames)
			assert.Equal(t, expectNames, names)
			// emulate the logic that fills known columns for known entities:
			tcmap.addTable(tbl)
			for _, name := range names {
				tcmap.addColumn(tbl, name)
			}
		})
	}
}

func TestViewReferences(t *testing.T) {
	tt := []struct {
		name      string
		queries   []string
		expectErr error
	}{
		{
			name: "valid",
			queries: []string{
				"create table t1(id int, state int, some char(5))",
				"create table t2(id int primary key, c char(5))",
				"create view v1 as select id as id from t1",
				"create view v2 as select 3+1 as `id`, state as state, some as thing from t1",
				"create view v3 as select `id` as `id`, state as state, thing as another from v2",
				"create view v4 as select 1 as `ok` from dual",
				"create view v5 as select 1 as `ok` from DUAL",
				"create view v6 as select ok as `ok` from v5",
			},
		},
		{
			name: "valid WHERE",
			queries: []string{
				"create table t1(id int primary key, c char(5))",
				"create table t2(id int primary key, c char(5))",
				"create view v1 as select c from t1 where id=3",
			},
		},
		{
			name: "invalid unqualified referenced column",
			queries: []string{
				"create table t1(id int primary key, c char(5))",
				"create table t2(id int primary key, c char(5))",
				"create view v1 as select unexpected from t1",
			},
			expectErr: &InvalidColumnReferencedInViewError{View: "v1", Column: "unexpected"},
		},
		{
			name: "invalid unqualified referenced column in where clause",
			queries: []string{
				"create table t1(id int primary key, c char(5))",
				"create table t2(id int primary key, c char(5))",
				"create view v1 as select 1 from t1 where unexpected=3",
			},
			expectErr: &InvalidColumnReferencedInViewError{View: "v1", Column: "unexpected"},
		},
		{
			name: "valid qualified",
			queries: []string{
				"create table t1(id int primary key, c char(5))",
				"create table t2(id int primary key, c char(5))",
				"create view v1 as select t1.c from t1 where t1.id=3",
			},
		},
		{
			name: "valid qualified, multiple tables",
			queries: []string{
				"create table t1(id int primary key, c char(5))",
				"create table t2(id int primary key, c char(5))",
				"create view v1 as select t1.c from t1, t2 where t2.id=3",
			},
		},
		{
			name: "invalid unqualified, multiple tables",
			queries: []string{
				"create table t1(id int primary key, c char(5))",
				"create table t2(id int primary key, c char(5))",
				"create view v1 as select c from t1, t2 where t2.id=3",
			},
			expectErr: &InvalidColumnReferencedInViewError{View: "v1", Column: "c", Ambiguous: true},
		},
		{
			name: "invalid unqualified in WHERE clause, multiple tables",
			queries: []string{
				"create table t1(id int primary key, c char(5))",
				"create table t2(id int primary key, c char(5))",
				"create view v1 as select t2.c from t1, t2 where id=3",
			},
			expectErr: &InvalidColumnReferencedInViewError{View: "v1", Column: "id", Ambiguous: true},
		},
		{
			name: "valid unqualified, multiple tables",
			queries: []string{
				"create table t1(id int primary key, c char(5))",
				"create table t2(id int primary key, c char(5), only_in_t2 int)",
				"create view v1 as select only_in_t2 from t1, t2 where t1.id=3",
			},
		},
		{
			name: "valid unqualified in WHERE clause, multiple tables",
			queries: []string{
				"create table t1(id int primary key, c char(5))",
				"create table t2(id int primary key, c char(5), only_in_t2 int)",
				"create view v1 as select t1.id from t1, t2 where only_in_t2=3",
			},
		},
		{
			name: "valid cascaded views",
			queries: []string{
				"create table t1(id int primary key, c char(5))",
				"create view v1 as select id, c from t1 where id > 0",
				"create view v2 as select * from v1 where id > 0",
			},
		},
		{
			name: "valid cascaded views, column aliases",
			queries: []string{
				"create table t1(id int primary key, c char(5))",
				"create view v1 as select id, c as ch from t1 where id > 0",
				"create view v2 as select ch from v1 where id > 0",
			},
		},
		{
			name: "valid cascaded views, column aliases in WHERE",
			queries: []string{
				"create table t1(id int primary key, c char(5))",
				"create view v1 as select id as counter, c as ch from t1 where id > 0",
				"create view v2 as select ch from v1 where counter > 0",
			},
		},
		{
			name: "valid cascaded views, aliased expression",
			queries: []string{
				"create table t1(id int primary key, c char(5))",
				"create view v1 as select id+1 as counter, c as ch from t1 where id > 0",
				"create view v2 as select ch from v1 where counter > 0",
			},
		},
		{
			name: "valid cascaded views, non aliased expression",
			queries: []string{
				"create table t1(id int primary key, c char(5))",
				"create view v1 as select id+1, c as ch from t1 where id > 0",
				"create view v2 as select ch from v1 where `id + 1` > 0",
			},
		},
		{
			name: "cascaded views, invalid column aliases",
			queries: []string{
				"create table t1(id int primary key, c char(5))",
				"create view v1 as select id, c as ch from t1 where id > 0",
				"create view v2 as select c from v1 where id > 0",
			},
			expectErr: &InvalidColumnReferencedInViewError{View: "v2", Column: "c"},
		},
		{
			name: "cascaded views, column not in view",
			queries: []string{
				"create table t1(id int primary key, c char(5))",
				"create view v1 as select id from t1 where c='x'",
				"create view v2 as select c from v1 where id > 0",
			},
			expectErr: &InvalidColumnReferencedInViewError{View: "v2", Column: "c"},
		},
		{
			name: "complex cascade",
			queries: []string{
				"create table t1(id int primary key, c char(5))",
				"create table t2(id int primary key, n int, info int)",
				"create view v1 as select id, c as ch from t1 where id > 0",
				"create view v2 as select n as num, info from t2",
				"create view v3 as select num, v1.id, ch from v1 join v2 on v1.id = v2.num where info > 5",
			},
		},
		{
			name: "valid dual",
			queries: []string{
				"create table t1(id int primary key, c char(5))",
				"create view v1 as select 1 from dual",
			},
		},
		{
			name: "invalid dual column",
			queries: []string{
				"create table t1(id int primary key, c char(5))",
				"create view v1 as select id from dual",
			},
			expectErr: &InvalidColumnReferencedInViewError{View: "v1", Column: "id"},
		},
		{
			name: "invalid dual star",
			queries: []string{
				"create table t1(id int primary key, c char(5))",
				"create view v1 as select * from dual",
			},
			expectErr: &InvalidStarExprInViewError{View: "v1"},
		},
		{
			name: "valid star",
			queries: []string{
				"create table t1(id int primary key, c char(5))",
				"create view v1 as select * from t1 where id > 0",
			},
		},
		{
			name: "valid star, cascaded",
			queries: []string{
				"create table t1(id int primary key, c char(5))",
				"create view v1 as select t1.* from t1 where id > 0",
				"create view v2 as select * from v1 where id > 0",
			},
		},
		{
			name: "valid star, two tables, cascaded",
			queries: []string{
				"create table t1(id int primary key, c char(5))",
				"create table t2(id int primary key, ts timestamp)",
				"create view v1 as select t1.* from t1, t2 where t1.id > 0",
				"create view v2 as select * from v1 where c > 0",
			},
		},
		{
			name: "valid two star, two tables, cascaded",
			queries: []string{
				"create table t1(id int primary key, c char(5))",
				"create table t2(id int primary key, ts timestamp)",
				"create view v1 as select t1.*, t2.* from t1, t2 where t1.id > 0",
				"create view v2 as select * from v1 where c > 0 and ts is not null",
			},
		},
		{
			name: "valid unqualified star, cascaded",
			queries: []string{
				"create table t1(id int primary key, c char(5))",
				"create table t2(id int primary key, ts timestamp)",
				"create view v1 as select * from t1, t2 where t1.id > 0",
				"create view v2 as select * from v1 where c > 0 and ts is not null",
			},
		},
	}
	for _, ts := range tt {
		t.Run(ts.name, func(t *testing.T) {
			schema, err := NewSchemaFromQueries(NewTestEnv(), ts.queries)
			if ts.expectErr == nil {
				require.NoError(t, err)
				require.NotNil(t, schema)
			} else {
				require.Error(t, err)
				err = vterrors.UnwrapFirst(err)
				require.Equal(t, ts.expectErr, err, "received error: %v", err)
			}
		})
	}
}

// TestMassiveSchema loads thousands of tables into one schema, and thousands of tables, some of which are different, into another schema.
// It compares the two schemas.
// The objective of this test is to verify that execution time is _reasonable_. Since this will run in GitHub CI, which is very slow, we allow
// for 1 minute total for all operations.
func TestMassiveSchema(t *testing.T) {
	tableBase := `
		CREATE TABLE IF NOT EXISTS placeholder
		(
				id                    int              NOT NULL AUTO_INCREMENT,
				workflow              varbinary(1000)  DEFAULT NULL,
				source                mediumblob       NOT NULL,
				pos                   varbinary(10000) NOT NULL,
				stop_pos              varbinary(10000) DEFAULT NULL,
				max_tps               bigint           NOT NULL,
				max_replication_lag   bigint           NOT NULL,
				cell                  varbinary(1000)  DEFAULT NULL,
				tablet_types          varbinary(100)   DEFAULT NULL,
				time_updated          bigint           NOT NULL,
				transaction_timestamp bigint           NOT NULL,
				state                 varbinary(100)   NOT NULL,
				message               varbinary(1000)  DEFAULT NULL,
				db_name               varbinary(255)   NOT NULL,
				rows_copied           bigint           NOT NULL DEFAULT '0',
				tags                  varbinary(1024)  NOT NULL DEFAULT '',
				time_heartbeat        bigint           NOT NULL DEFAULT '0',
				workflow_type         int              NOT NULL DEFAULT '0',
				time_throttled        bigint           NOT NULL DEFAULT '0',
				component_throttled   varchar(255)     NOT NULL DEFAULT '',
				workflow_sub_type     int              NOT NULL DEFAULT '0',
				defer_secondary_keys  tinyint(1)       NOT NULL DEFAULT '0',
				PRIMARY KEY (id),
				KEY workflow_idx (workflow(64)),
				KEY time_heartbeat_idx (time_heartbeat)
		) ENGINE = InnoDB
	`
	// Remove a couple columns into a modified table
	modifiedTable := tableBase
	for _, s := range []string{
		"workflow              varbinary(1000)  DEFAULT NULL,\n",
		"KEY workflow_idx (workflow(64)),\n",
	} {
		require.Contains(t, tableBase, s)
		modifiedTable = strings.Replace(modifiedTable, s, "", -1)
	}
	require.NotEqual(t, tableBase, modifiedTable)

	var schema0 *Schema
	var schema1 *Schema
	var err error
	numTables := 8192
	modifyTables := 500
	countModifiedTables := 0
	tableNames := map[string]bool{}

	startTime := time.Now()

	// Load thousands of tables into each schema
	t.Run(fmt.Sprintf("load %d tables into schemas", numTables), func(t *testing.T) {
		modifiedTableIndexes := map[int]bool{}
		for i, index := range rand.Perm(numTables) {
			if i >= modifyTables {
				break
			}
			modifiedTableIndexes[index] = true
		}
		queries0 := make([]string, 0, numTables) // to be loaded into schema0
		queries1 := make([]string, 0, numTables) // to be loaded into schema1
		for i := 0; i < numTables; i++ {
			tableName := fmt.Sprintf("tbl_%05d", i)
			query := strings.Replace(tableBase, "placeholder", tableName, -1)
			queries0 = append(queries0, query)
			if modifiedTableIndexes[i] {
				// Some tables in schema1 are changed
				query = strings.Replace(modifiedTable, "placeholder", tableName, -1)
				countModifiedTables++
			}
			queries1 = append(queries1, query)
			tableNames[tableName] = true
		}
		schema0, err = NewSchemaFromQueries(NewTestEnv(), queries0)
		require.NoError(t, err)
		schema1, err = NewSchemaFromQueries(NewTestEnv(), queries1)
		require.NoError(t, err)

		require.Equal(t, countModifiedTables, modifyTables)
	})
	t.Run(fmt.Sprintf("validate loaded %d tables", numTables), func(t *testing.T) {
		for _, schema := range []*Schema{schema0, schema1} {
			entities := schema.Entities()
			assert.Equal(t, numTables, len(entities)) // all tables are there
			for _, e := range entities {
				_, ok := tableNames[e.Name()]
				assert.True(t, ok)
			}
		}
	})

	t.Run("evaluating diff", func(t *testing.T) {
		schemaDiff, err := schema0.SchemaDiff(schema1, EmptyDiffHints())
		require.NoError(t, err)
		diffs := schemaDiff.UnorderedDiffs()
		require.NotEmpty(t, diffs)
		require.Equal(t, len(diffs), countModifiedTables)
	})

	elapsed := time.Since(startTime)
	assert.Less(t, elapsed, time.Minute)
}
