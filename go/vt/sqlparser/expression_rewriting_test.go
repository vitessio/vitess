/*
Copyright 2019 The Vitess Authors.

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

package sqlparser

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type myTestCase struct {
	in, expected                       string
	liid, db, foundRows, udv, rowCount bool
}

func TestRewrites(in *testing.T) {
	tests := []myTestCase{
		{
			in:       "SELECT 42",
			expected: "SELECT 42",
			// no bindvar needs
		},
		{
			in:       "SELECT last_insert_id()",
			expected: "SELECT :__lastInsertId as `last_insert_id()`",
			liid:     true,
		},
		{
			in:       "SELECT database()",
			expected: "SELECT :__vtdbname as `database()`",
			db:       true,
		},
		{
			in:       "SELECT database() from test",
			expected: "SELECT database() from test",
			// no bindvar needs
		},
		{
			in:       "SELECT last_insert_id() as test",
			expected: "SELECT :__lastInsertId as test",
			liid:     true,
		},
		{
			in:       "SELECT last_insert_id() + database()",
			expected: "SELECT :__lastInsertId + :__vtdbname as `last_insert_id() + database()`",
			db:       true, liid: true,
		},
		{
			in:       "select (select database()) from test",
			expected: "select (select database() from dual) from test",
			// no bindvar needs
		},
		{
			in:       "select (select database() from dual) from test",
			expected: "select (select database() from dual) from test",
			// no bindvar needs
		},
		{
			in:       "select (select database() from dual) from dual",
			expected: "select (select :__vtdbname as `database()` from dual) as `(select database() from dual)` from dual",
			db:       true,
		},
		{
			in:       "select id from user where database()",
			expected: "select id from user where database()",
			// no bindvar needs
		},
		{
			in:       "select table_name from information_schema.tables where table_schema = database()",
			expected: "select table_name from information_schema.tables where table_schema = database()",
			// no bindvar needs
		},
		{
			in:       "select schema()",
			expected: "select :__vtdbname as `schema()`",
			db:       true,
		},
		{
			in:        "select found_rows()",
			expected:  "select :__vtfrows as `found_rows()`",
			foundRows: true,
		},
		{
			in:       "select @`x y`",
			expected: "select :__vtudvx_y as `@``x y``` from dual",
			udv:      true,
		},
		{
			in:       "select id from t where id = @x",
			expected: "select id from t where id = :__vtudvx",
			db:       false, udv: true,
		},
		{
			in:       "insert into t(id) values(@xyx)",
			expected: "insert into t(id) values(:__vtudvxyx)",
			db:       false, udv: true,
		},
		{
			in:       "select row_count()",
			expected: "select :__vtrcount as `row_count()`",
			rowCount: true,
		},
	}

	for _, tc := range tests {
		in.Run(tc.in, func(t *testing.T) {
			stmt, err := Parse(tc.in)
			require.NoError(t, err)

			result, err := RewriteAST(stmt)
			require.NoError(t, err)

			expected, err := Parse(tc.expected)
			require.NoError(t, err, "test expectation does not parse [%s]", tc.expected)

			s := String(expected)
			require.Equal(t, s, String(result.AST))
			require.Equal(t, tc.liid, result.NeedLastInsertID, "should need last insert id")
			require.Equal(t, tc.db, result.NeedDatabase, "should need database name")
			require.Equal(t, tc.foundRows, result.NeedFoundRows, "should need found rows")
			require.Equal(t, tc.rowCount, result.NeedRowCount, "should need row count")
		})
	}
}
