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

package endtoend

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
)

var ctx = context.Background()

const (
	createSchemaCopyTable = `
CREATE TABLE _vt.schemacopy (
	table_schema varchar(64) NOT NULL,
	table_name varchar(64) NOT NULL,
	column_name varchar(64) NOT NULL,
	ordinal_position bigint(21) unsigned NOT NULL,
	character_set_name varchar(32) DEFAULT NULL,
	collation_name varchar(32) DEFAULT NULL,
	column_type longtext NOT NULL,
	column_key varchar(3) NOT NULL,
	PRIMARY KEY (table_schema, table_name, ordinal_position)
)`
	createDb = `create database if not exists _vt`

	insertIntoSchemaCopy = `insert _vt.schemacopy 
select table_schema, table_name, column_name, ordinal_position, character_set_name, collation_name, column_type, column_key 
from information_schema.columns 
where table_schema = "vttest"`

	cleanSchemaCopy = `delete from _vt.schemacopy`
	dropTestTable   = `drop table if exists product`

	createUserTable = `create table vttest.product (id bigint(20) primary key, name char(10) CHARACTER SET utf8 COLLATE utf8_unicode_ci, created bigint(20))`

	detectNewColumns = `
select 1
from information_schema.columns as ISC
	 left join _vt.schemacopy as c on 
		ISC.table_name = c.table_name and 
		ISC.table_schema=c.table_schema and 
		ISC.ordinal_position = c.ordinal_position
where ISC.table_schema = "vttest" AND c.table_schema is null
`

	detectChangeColumns = `
select 1
from information_schema.columns as ISC
	  join _vt.schemacopy as c on 
		ISC.table_name = c.table_name and 
		ISC.table_schema=c.table_schema and 
		ISC.ordinal_position = c.ordinal_position
where ISC.table_schema = "vttest" 
	AND (not(c.column_name <=> ISC.column_name) 
	OR not(ISC.character_set_name <=> c.character_set_name) 
	OR not(ISC.collation_name <=> c.collation_name) 
	OR not(ISC.column_type <=> c.column_type) 
	OR not(ISC.column_key <=> c.column_key))
`

	detectRemoveColumns = `
select 1
from information_schema.columns as ISC
	  right join _vt.schemacopy as c on 
		ISC.table_name = c.table_name and 
		ISC.table_schema=c.table_schema and 
		ISC.ordinal_position = c.ordinal_position
where c.table_schema = "vttest" AND ISC.table_schema is null
`

	detectChange = detectChangeColumns + " UNION " + detectNewColumns + " UNION " + detectRemoveColumns
)

func TestChangeSchemaIsNoticed(t *testing.T) {
	conn, err := mysql.Connect(ctx, &connParams)
	require.NoError(t, err)
	defer conn.Close()

	_, err = conn.ExecuteFetch(createDb, 1000, true)
	require.NoError(t, err)
	_, err = conn.ExecuteFetch(createSchemaCopyTable, 1000, true)
	require.NoError(t, err)

	tests := []struct {
		name    string
		changeQ string
	}{{
		name:    "add column",
		changeQ: "alter table vttest.product add column phone VARCHAR(15)",
	}, {
		name:    "rename column",
		changeQ: "alter table vttest.product change name firstname char(10)",
	}, {
		name:    "change column type",
		changeQ: "alter table vttest.product change name name char(100)",
	}, {
		name:    "remove column",
		changeQ: "alter table vttest.product drop column name",
	}, {
		name:    "remove last column",
		changeQ: "alter table vttest.product drop column created",
	}, {
		name:    "remove table",
		changeQ: "drop table product",
	}, {
		name:    "create table",
		changeQ: `create table vttest.new_table (id bigint(20) primary key)`,
	}, {
		name:    "change character set",
		changeQ: "alter table vttest.product change name name char(10) CHARACTER SET utf8mb4",
	}, {
		name:    "change collation",
		changeQ: "alter table vttest.product change name name char(10) COLLATE utf8_unicode_520_ci",
	}, {
		name:    "drop PK",
		changeQ: "alter table vttest.product drop primary key",
	}, {
		name:    "change PK",
		changeQ: "alter table vttest.product drop primary key, add primary key (name)",
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// reset schemacopy
			_, err := conn.ExecuteFetch(cleanSchemaCopy, 1000, true)
			require.NoError(t, err)
			_, err = conn.ExecuteFetch(dropTestTable, 1000, true)
			require.NoError(t, err)
			_, err = conn.ExecuteFetch(createUserTable, 1000, true)
			require.NoError(t, err)
			rs, err := conn.ExecuteFetch(insertIntoSchemaCopy, 1000, true)
			require.NoError(t, err)
			require.NotZero(t, rs.RowsAffected)

			// make sure no changes are detected
			rs, err = conn.ExecuteFetch(detectChange, 1000, true)
			require.NoError(t, err)
			require.Empty(t, rs.Rows)

			// make the schema change
			_, err = conn.ExecuteFetch(test.changeQ, 1000, true)
			require.NoError(t, err)

			// make sure the change is detected
			rs, err = conn.ExecuteFetch(detectChange, 1000, true)
			require.NoError(t, err)
			require.NotEmpty(t, rs.Rows)
		})

	}

}
