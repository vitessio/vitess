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
	in, expected string
	liid, db     bool
}

func TestRewrites(in *testing.T) {
	tests := []myTestCase{
		{
			in:       "SELECT 42",
			expected: "SELECT 42",
			db:       false, liid: false,
		},
		{
			in:       "SELECT last_insert_id()",
			expected: "SELECT :__lastInsertId as `last_insert_id()`",
			db:       false, liid: true,
		},
		{
			in:       "SELECT database()",
			expected: "SELECT :__vtdbname as `database()`",
			db:       true, liid: false,
		},
		{
			in:       "SELECT database() from test",
			expected: "SELECT database() from test",
			db:       false, liid: false,
		},
		{
			in:       "SELECT last_insert_id() as test",
			expected: "SELECT :__lastInsertId as test",
			db:       false, liid: true,
		},
		{
			in:       "SELECT last_insert_id() + database()",
			expected: "SELECT :__lastInsertId + :__vtdbname as `last_insert_id() + database()`",
			db:       true, liid: true,
		},
		{
			in:       "select (select database()) from test",
			expected: "select (select database() from dual) from test",
			db:       false, liid: false,
		},
		{
			in:       "select (select database() from dual) from test",
			expected: "select (select database() from dual) from test",
			db:       false, liid: false,
		},
		{
			in:       "select (select database() from dual) from dual",
			expected: "select (select :__vtdbname as `database()` from dual) as `(select database() from dual)` from dual",
			db:       true, liid: false,
		},
		{
			in:       "select id from user where database()",
			expected: "select id from user where database()",
			db:       false, liid: false,
		},
		{
			in:       "select table_name from information_schema.tables where table_schema = database()",
			expected: "select table_name from information_schema.tables where table_schema = database()",
			db:       false, liid: false,
		},
	}

	for _, tc := range tests {
		in.Run(tc.in, func(t *testing.T) {
			stmt, err := Parse(tc.in)
			require.NoError(t, err)

			result, err := RewriteAST(stmt)
			require.NoError(t, err)

			expected, err := Parse(tc.expected)
			require.NoError(t, err)

			s := String(expected)
			require.Equal(t, s, String(result.AST))
			require.Equal(t, tc.liid, result.NeedLastInsertID, "should need last insert id")
			require.Equal(t, tc.db, result.NeedDatabase, "should need database name")
		})
	}
}
