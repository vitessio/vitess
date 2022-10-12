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
	"fmt"
	"strings"
	"testing"

	"vitess.io/vitess/go/vt/sqlparser"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
)

var ctx = context.Background()

const (
	createUserTable = `create table vttest.product (id bigint(20) primary key, name char(10) CHARACTER SET utf8 COLLATE utf8_unicode_ci, created bigint(20))`
	dropTestTable   = `drop table if exists product`
)

func TestChangeSchemaIsNoticed(t *testing.T) {
	conn, err := mysql.Connect(ctx, &connParams)
	require.NoError(t, err)
	defer conn.Close()

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
	}, {
		name:    "two tables changes",
		changeQ: "create table vttest.new_table2 (id bigint(20) primary key);alter table vttest.product drop column name",
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// reset schemacopy
			_, err := conn.ExecuteFetch(mysql.ClearSchemaCopy, 1000, true)
			require.NoError(t, err)
			_, err = conn.ExecuteFetch(dropTestTable, 1000, true)
			require.NoError(t, err)
			_, err = conn.ExecuteFetch(createUserTable, 1000, true)
			require.NoError(t, err)
			rs, err := conn.ExecuteFetch(mysql.InsertIntoSchemaCopy, 1000, true)
			require.NoError(t, err)
			require.NotZero(t, rs.RowsAffected)

			// make sure no changes are detected
			rs, err = conn.ExecuteFetch(mysql.DetectSchemaChange, 1000, true)
			require.NoError(t, err)
			require.Empty(t, rs.Rows)

			for _, q := range strings.Split(test.changeQ, ";") {
				// make the schema change
				_, err = conn.ExecuteFetch(q, 1000, true)
				require.NoError(t, err)
			}

			// make sure the change is detected
			rs, err = conn.ExecuteFetch(mysql.DetectSchemaChange, 1000, true)
			require.NoError(t, err)
			require.NotEmpty(t, rs.Rows)

			var tables []string
			for _, row := range rs.Rows {
				apa := sqlparser.NewStrLiteral(row[0].ToString())
				tables = append(tables, "table_name = "+sqlparser.String(apa))
			}
			tableNamePredicates := strings.Join(tables, " OR ")
			del := fmt.Sprintf("%s AND %s", mysql.ClearSchemaCopy, tableNamePredicates)
			upd := fmt.Sprintf("%s AND %s", mysql.InsertIntoSchemaCopy, tableNamePredicates)

			_, err = conn.ExecuteFetch(del, 1000, true)
			require.NoError(t, err)
			_, err = conn.ExecuteFetch(upd, 1000, true)
			require.NoError(t, err)

			// make sure the change is detected
			rs, err = conn.ExecuteFetch(mysql.DetectSchemaChange, 1000, true)
			require.NoError(t, err)
			require.Empty(t, rs.Rows)
		})
	}
}
