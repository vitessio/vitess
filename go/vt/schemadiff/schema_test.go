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
	"create view v3 as select * from t3 as t3",
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

var toSQL = "CREATE TABLE `t1` (\n\t`id` int\n);\nCREATE TABLE `t2` (\n\t`id` int\n);\nCREATE TABLE `t3` (\n\t`id` int,\n\t`type` enum('foo', 'bar') NOT NULL DEFAULT 'foo'\n);\nCREATE TABLE `t5` (\n\t`id` int\n);\nCREATE VIEW `v0` AS SELECT 1 FROM `dual`;\nCREATE VIEW `v3` AS SELECT * FROM `t3` AS `t3`;\nCREATE VIEW `v9` AS SELECT 1 FROM `dual`;\nCREATE VIEW `v1` AS SELECT * FROM `v3`;\nCREATE VIEW `v2` AS SELECT * FROM `v3`, `t2`;\nCREATE VIEW `v4` AS SELECT * FROM `t2` AS `something_else`, `v3`;\nCREATE VIEW `v5` AS SELECT * FROM `t1`, (SELECT * FROM `v3`) AS `some_alias`;\nCREATE VIEW `v6` AS SELECT * FROM `v4`;\n"

func TestNewSchemaFromQueries(t *testing.T) {
	schema, err := NewSchemaFromQueries(createQueries)
	assert.NoError(t, err)
	assert.NotNil(t, schema)

	assert.Equal(t, expectSortedNames, schema.EntityNames())
	assert.Equal(t, expectSortedTableNames, schema.TableNames())
	assert.Equal(t, expectSortedViewNames, schema.ViewNames())
}

func TestNewSchemaFromSQL(t *testing.T) {
	schema, err := NewSchemaFromSQL(strings.Join(createQueries, ";"))
	assert.NoError(t, err)
	assert.NotNil(t, schema)

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
	assert.NotNil(t, schema)

	sql := schema.ToSQL()
	assert.Equal(t, toSQL, sql)
}

func TestCopy(t *testing.T) {
	schema, err := NewSchemaFromQueries(createQueries)
	assert.NoError(t, err)
	assert.NotNil(t, schema)

	schemaClone := schema.copy()
	assert.Equal(t, schema, schemaClone)
	assert.Equal(t, schema.ToSQL(), schemaClone.ToSQL())
	assert.False(t, schema == schemaClone)
}
