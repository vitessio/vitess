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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"
)

var createQueries = []string{
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

var expectSortedNames = []string{
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

var expectSortedTableNames = []string{
	"t1",
	"t2",
	"t3",
	"t5",
}

var expectSortedViewNames = []string{
	"v0", // level 1 ("dual" is an implicit table)
	"v3", // level 1
	"v9", // level 1 (no source table)
	"v1", // level 2
	"v2", // level 2
	"v4", // level 2
	"v5", // level 2
	"v6", // level 3
}

var toSQL = "CREATE TABLE `t1` (\n\t`id` int\n);\nCREATE TABLE `t2` (\n\t`id` int\n);\nCREATE TABLE `t3` (\n\t`id` int,\n\t`type` enum('foo', 'bar') NOT NULL DEFAULT 'foo'\n);\nCREATE TABLE `t5` (\n\t`id` int\n);\nCREATE VIEW `v0` AS SELECT 1 FROM `dual`;\nCREATE VIEW `v3` AS SELECT *, `id` + 1 AS `id_plus`, `id` + 2 FROM `t3` AS `t3`;\nCREATE VIEW `v9` AS SELECT 1 FROM `dual`;\nCREATE VIEW `v1` AS SELECT * FROM `v3`;\nCREATE VIEW `v2` AS SELECT * FROM `v3`, `t2`;\nCREATE VIEW `v4` AS SELECT * FROM `t2` AS `something_else`, `v3`;\nCREATE VIEW `v5` AS SELECT * FROM `t1`, (SELECT * FROM `v3`) AS `some_alias`;\nCREATE VIEW `v6` AS SELECT * FROM `v4`;\n"

func TestNewSchemaFromQueries(t *testing.T) {
	schema, err := NewSchemaFromQueries(createQueries)
	assert.NoError(t, err)
	require.NotNil(t, schema)

	assert.Equal(t, expectSortedNames, schema.EntityNames())
	assert.Equal(t, expectSortedTableNames, schema.TableNames())
	assert.Equal(t, expectSortedViewNames, schema.ViewNames())
}

func TestNewSchemaFromSQL(t *testing.T) {
	schema, err := NewSchemaFromSQL(strings.Join(createQueries, ";"))
	assert.NoError(t, err)
	require.NotNil(t, schema)

	assert.Equal(t, expectSortedNames, schema.EntityNames())
	assert.Equal(t, expectSortedTableNames, schema.TableNames())
	assert.Equal(t, expectSortedViewNames, schema.ViewNames())
}

func TestNewSchemaFromQueriesWithDuplicate(t *testing.T) {
	// v2 already exists
	queries := append(createQueries,
		"create view v2 as select * from v1, t2",
	)
	_, err := NewSchemaFromQueries(queries)
	assert.Error(t, err)
	assert.EqualError(t, err, (&ApplyDuplicateEntityError{Entity: "v2"}).Error())
}

func TestNewSchemaFromQueriesUnresolved(t *testing.T) {
	// v8 does not exist
	queries := append(createQueries,
		"create view v7 as select * from v8, t2",
	)
	_, err := NewSchemaFromQueries(queries)
	assert.Error(t, err)
	assert.EqualError(t, err, (&ViewDependencyUnresolvedError{View: "v7"}).Error())
}

func TestNewSchemaFromQueriesUnresolvedAlias(t *testing.T) {
	// v8 does not exist
	queries := append(createQueries,
		"create view v7 as select * from something_else as t1, t2",
	)
	_, err := NewSchemaFromQueries(queries)
	assert.Error(t, err)
	assert.EqualError(t, err, (&ViewDependencyUnresolvedError{View: "v7"}).Error())
}

func TestNewSchemaFromQueriesLoop(t *testing.T) {
	// v7 and v8 depend on each other
	queries := append(createQueries,
		"create view v7 as select * from v8, t2",
		"create view v8 as select * from t1, v7",
	)
	_, err := NewSchemaFromQueries(queries)
	assert.Error(t, err)
	assert.EqualError(t, err, (&ViewDependencyUnresolvedError{View: "v7"}).Error())
}

func TestToSQL(t *testing.T) {
	schema, err := NewSchemaFromQueries(createQueries)
	assert.NoError(t, err)
	require.NotNil(t, schema)

	sql := schema.ToSQL()
	assert.Equal(t, toSQL, sql)
}

func TestCopy(t *testing.T) {
	schema, err := NewSchemaFromQueries(createQueries)
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
			stmt, err := sqlparser.ParseStrictDDL(ts.view)
			require.NoError(t, err)
			createView, ok := stmt.(*sqlparser.CreateView)
			require.True(t, ok)

			tables, err := getViewDependentTableNames(createView)
			assert.NoError(t, err)
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
			stmt, err := sqlparser.ParseStrictDDL(ts.table)
			require.NoError(t, err)
			createTable, ok := stmt.(*sqlparser.CreateTable)
			require.True(t, ok)

			tables, err := getForeignKeyParentTableNames(createTable)
			assert.NoError(t, err)
			assert.Equal(t, ts.tables, tables)
		})
	}
}

func TestTableForeignKeyOrdering(t *testing.T) {
	fkQueries := []string{
		"create table t11 (id int primary key, i int, constraint f12 foreign key (i) references t12(id) on delete restrict, constraint f20 foreign key (i) references t20(id) on delete restrict)",
		"create table t15(id int, primary key(id))",
		"create view v09 as select * from v13, t17",
		"create table t20 (id int primary key, i int, constraint f15 foreign key (i) references t15(id) on delete restrict)",
		"create view v13 as select * from t20",
		"create table t12 (id int primary key, i int, constraint f15 foreign key (i) references t15(id) on delete restrict)",
		"create table t17 (id int primary key, i int, constraint f11 foreign key (i) references t11(id) on delete restrict, constraint f15 foreign key (i) references t15(id) on delete restrict)",
		"create table t16 (id int primary key, i int, constraint f11 foreign key (i) references t11(id) on delete restrict, constraint f15 foreign key (i) references t15(id) on delete restrict)",
		"create table t14 (id int primary key, i int, constraint f14 foreign key (i) references t14(id) on delete restrict)",
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
	schema, err := NewSchemaFromQueries(fkQueries)
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
			schema: "create table t11 (id int primary key, i int, constraint f11 foreign key (i) references t11(id) on delete restrict)",
		},
		{
			schema: "create table t10(id int primary key); create table t11 (id int primary key, i int, constraint f10 foreign key (i) references t10(id) on delete restrict)",
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
			schema:    "create table t11 (id int primary key, i int, constraint f12 foreign key (i) references t12(id) on delete restrict)",
			expectErr: &ForeignKeyDependencyUnresolvedError{Table: "t11"},
		},
		{
			schema:    "create table t11 (id int primary key, i int, constraint f11 foreign key (i) references t11(id2) on delete restrict)",
			expectErr: &InvalidReferencedColumnInForeignKeyConstraintError{Table: "t11", Constraint: "f11", ReferencedTable: "t11", ReferencedColumn: "id2"},
		},
		{
			schema:    "create table t10(id int primary key); create table t11 (id int primary key, i int, constraint f10 foreign key (i) references t10(x) on delete restrict)",
			expectErr: &InvalidReferencedColumnInForeignKeyConstraintError{Table: "t11", Constraint: "f10", ReferencedTable: "t10", ReferencedColumn: "x"},
		},
		{
			schema:    "create table t10(id int primary key); create table t11 (id int primary key, i int unsigned, constraint f10 foreign key (i) references t10(id) on delete restrict)",
			expectErr: &ForeignKeyColumnTypeMismatchError{Table: "t11", Constraint: "f10", Column: "i", ReferencedTable: "t10", ReferencedColumn: "id"},
		},
		{
			schema:    "create table t10(id int primary key); create table t11 (id int primary key, i bigint, constraint f10 foreign key (i) references t10(id) on delete restrict)",
			expectErr: &ForeignKeyColumnTypeMismatchError{Table: "t11", Constraint: "f10", Column: "i", ReferencedTable: "t10", ReferencedColumn: "id"},
		},
		{
			schema:    "create table t10(id bigint primary key); create table t11 (id int primary key, i int, constraint f10 foreign key (i) references t10(id) on delete restrict)",
			expectErr: &ForeignKeyColumnTypeMismatchError{Table: "t11", Constraint: "f10", Column: "i", ReferencedTable: "t10", ReferencedColumn: "id"},
		},
		{
			schema:    "create table t10(id bigint primary key); create table t11 (id int primary key, i varchar(100), constraint f10 foreign key (i) references t10(id) on delete restrict)",
			expectErr: &ForeignKeyColumnTypeMismatchError{Table: "t11", Constraint: "f10", Column: "i", ReferencedTable: "t10", ReferencedColumn: "id"},
		},
		{
			// InnoDB allows different string length
			schema: "create table t10(id varchar(50) primary key); create table t11 (id int primary key, i varchar(100), constraint f10 foreign key (i) references t10(id) on delete restrict)",
		},
		{
			schema:    "create table t10(id varchar(50) charset utf8mb3 primary key); create table t11 (id int primary key, i varchar(100) charset utf8mb4, constraint f10 foreign key (i) references t10(id) on delete restrict)",
			expectErr: &ForeignKeyColumnTypeMismatchError{Table: "t11", Constraint: "f10", Column: "i", ReferencedTable: "t10", ReferencedColumn: "id"},
		},
	}
	for _, ts := range tt {
		t.Run(ts.schema, func(t *testing.T) {

			_, err := NewSchemaFromSQL(ts.schema)
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
			"create table t11 (id int primary key, i int, constraint f12 foreign key (i) references t12(id) on delete restrict)",
			"create table t15(id int, primary key(id))",
		}
		_, err := NewSchemaFromQueries(fkQueries)
		assert.Error(t, err)
		assert.EqualError(t, err, (&ForeignKeyDependencyUnresolvedError{Table: "t11"}).Error())
	}
	{
		fkQueries := []string{
			"create table t13 (id int primary key, i int, constraint f11 foreign key (i) references t11(id) on delete restrict)",
			"create table t11 (id int primary key, i int, constraint f12 foreign key (i) references t12(id) on delete restrict)",
			"create table t12 (id int primary key, i int, constraint f13 foreign key (i) references t13(id) on delete restrict)",
		}
		_, err := NewSchemaFromQueries(fkQueries)
		assert.Error(t, err)
		assert.EqualError(t, err, (&ForeignKeyDependencyUnresolvedError{Table: "t11"}).Error())
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
	}

	schema, err := NewSchemaFromQueries(queries)
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
	}
	for tbl, expectNames := range expectedColNames {
		identifiers, err := schema.getEntityColumnNames(tbl)
		assert.NoError(t, err)
		names := []string{}
		for _, ident := range identifiers {
			names = append(names, ident.String())
		}
		assert.Equal(t, expectNames, names)
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
			expectErr: &InvalidColumnReferencedInViewError{View: "v1", Column: "c", NonUnique: true},
		},
		{
			name: "invalid unqualified in WHERE clause, multiple tables",
			queries: []string{
				"create table t1(id int primary key, c char(5))",
				"create table t2(id int primary key, c char(5))",
				"create view v1 as select t2.c from t1, t2 where id=3",
			},
			expectErr: &InvalidColumnReferencedInViewError{View: "v1", Column: "id", NonUnique: true},
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
				"create view v3 as select num, v1.id, ch from v1 join v2 using (id) where info > 5",
			},
		},
		{
			name: "valid *",
			queries: []string{
				"create table t1(id int primary key, c char(5))",
				"create view v1 as select * from t1 where id > 0",
			},
		},
		{
			name: "valid *, cascaded",
			queries: []string{
				"create table t1(id int primary key, c char(5))",
				"create view v1 as select t1.* from t1 where id > 0",
				"create view v2 as select * from v1 where id > 0",
			},
		},
	}
	for _, ts := range tt {
		t.Run(ts.name, func(t *testing.T) {
			schema, err := NewSchemaFromQueries(ts.queries)
			if ts.expectErr == nil {
				require.NoError(t, err)
				require.NotNil(t, schema)
			} else {
				require.Equal(t, ts.expectErr, err, "received error: %v", err)
			}
		})
	}
}
