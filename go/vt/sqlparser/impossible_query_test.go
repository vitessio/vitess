/*
Copyright 2025 The Vitess Authors.

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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFormatImpossibleQuery_Select(t *testing.T) {
	parser := NewTestParser()

	testcases := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "basic select",
			input:    "select * from t",
			expected: "select * from t where 1 != 1",
		},
		{
			name:     "select with existing where",
			input:    "select * from t where id = 1",
			expected: "select * from t where 1 != 1",
		},
		{
			name:     "select with group by",
			input:    "select col from t group by col",
			expected: "select col from t where 1 != 1 group by col",
		},
		{
			name:     "select with multiple columns",
			input:    "select id, name from users",
			expected: "select id, `name` from users where 1 != 1",
		},
		{
			name:     "select with multiple tables",
			input:    "select * from t1, t2",
			expected: "select * from t1, t2 where 1 != 1",
		},
		{
			name:     "select with join",
			input:    "select * from t1 join t2 on t1.id = t2.id",
			expected: "select * from t1 join t2 on t1.id = t2.id where 1 != 1",
		},
		{
			name:     "select with where and group by",
			input:    "select col from t where id > 5 group by col",
			expected: "select col from t where 1 != 1 group by col",
		},
		{
			name:     "select with with clause",
			input:    "with cte as (select * from t) select * from cte",
			expected: "with cte as (select * from t) select * from cte where 1 != 1",
		},
		{
			name:     "select with multiple CTEs",
			input:    "with cte1 as (select * from t1), cte2 as (select * from t2) select * from cte1 join cte2 on cte1.id = cte2.id",
			expected: "with cte1 as (select * from t1) , cte2 as (select * from t2) select * from cte1 join cte2 on cte1.id = cte2.id where 1 != 1",
		},
		{
			name:     "select with recursive CTE",
			input:    "with recursive cte as (select id from t1 union select id + 1 from cte where id < 10) select * from cte",
			expected: "with recursive cte as (select id from t1 union select id + 1 from cte where id < 10) select * from cte where 1 != 1",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := parser.Parse(tc.input)
			require.NoError(t, err)

			buf := NewTrackedBuffer(nil)
			FormatImpossibleQuery(buf, stmt)

			assert.Equal(t, tc.expected, buf.String())
		})
	}
}

func TestFormatImpossibleQuery_Union(t *testing.T) {
	parser := NewTestParser()

	testcases := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple union",
			input:    "select * from t1 union select * from t2",
			expected: "select * from t1 where 1 != 1 union select * from t2 where 1 != 1",
		},
		{
			name:     "union all",
			input:    "select * from t1 union all select * from t2",
			expected: "select * from t1 where 1 != 1 union all select * from t2 where 1 != 1",
		},
		{
			name:     "union with parentheses",
			input:    "(select * from t1) union (select * from t2)",
			expected: "select * from t1 where 1 != 1 union select * from t2 where 1 != 1",
		},
		{
			name:     "nested union",
			input:    "select * from t1 union select * from t2 union select * from t3",
			expected: "select * from t1 where 1 != 1 union select * from t2 where 1 != 1 union select * from t3 where 1 != 1",
		},
		{
			name:     "union with where clauses",
			input:    "select * from t1 where id > 1 union select * from t2 where name = 'test'",
			expected: "select * from t1 where 1 != 1 union select * from t2 where 1 != 1",
		},
		{
			name:     "union with complex select",
			input:    "select id, name from users group by id union select id, title from posts",
			expected: "select id, `name` from users where 1 != 1 group by id union select id, title from posts where 1 != 1",
		},
		{
			name:     "union with with clause",
			input:    "with cte as (select * from t1) select * from cte union select * from t2",
			expected: "with cte as (select * from t1) select * from cte where 1 != 1 union select * from t2 where 1 != 1",
		},
		{
			name:     "union with multiple CTEs",
			input:    "with cte1 as (select * from t1), cte2 as (select * from t2) select * from cte1 union select * from cte2",
			expected: "with cte1 as (select * from t1) , cte2 as (select * from t2) select * from cte1 where 1 != 1 union select * from cte2 where 1 != 1",
		},
		{
			name:     "union with recursive CTE",
			input:    "with recursive cte as (select id from t1 union select id + 1 from cte where id < 10) select * from cte union select * from t2",
			expected: "with recursive cte as (select id from t1 union select id + 1 from cte where id < 10) select * from cte where 1 != 1 union select * from t2 where 1 != 1",
		},
		{
			name:     "union with CTE containing where clause",
			input:    "with cte as (select * from t1 where id > 5) select * from cte union select * from t2 where name = 'test'",
			expected: "with cte as (select * from t1 where id > 5) select * from cte where 1 != 1 union select * from t2 where 1 != 1",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := parser.Parse(tc.input)
			require.NoError(t, err)

			buf := NewTrackedBuffer(nil)
			FormatImpossibleQuery(buf, stmt)

			assert.Equal(t, tc.expected, buf.String())
		})
	}
}

func TestFormatImpossibleQuery_Other(t *testing.T) {
	parser := NewTestParser()

	testcases := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "insert statement",
			input:    "insert into t values (1, 'test')",
			expected: "insert into t values (1, 'test')",
		},
		{
			name:     "update statement",
			input:    "update t set name = 'test' where id = 1",
			expected: "update t set `name` = 'test' where id = 1",
		},
		{
			name:     "delete statement",
			input:    "delete from t where id = 1",
			expected: "delete from t where id = 1",
		},
		{
			name:     "create table statement",
			input:    "create table t (id int, name varchar(50))",
			expected: "create table t (\n\tid int,\n\t`name` varchar(50)\n)",
		},
		{
			name:     "show statement",
			input:    "show tables",
			expected: "show tables",
		},
		{
			name:     "describe statement",
			input:    "describe t",
			expected: "explain t",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := parser.Parse(tc.input)
			require.NoError(t, err)

			buf := NewTrackedBuffer(nil)
			FormatImpossibleQuery(buf, stmt)

			assert.Equal(t, tc.expected, buf.String())
		})
	}
}
