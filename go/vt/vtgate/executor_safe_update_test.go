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

package vtgate

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"vitess.io/vitess/go/vt/sqlparser"
)

func TestIsValidUpdateQuery(t *testing.T) {

	tests := []struct {
		name     string
		sql      string
		expected bool
	}{
		{
			name:     "UPDATE statement with WHERE clause",
			sql:      "UPDATE users SET name='John' WHERE id=1",
			expected: true,
		},
		{
			name:     "UPDATE statement without WHERE clause",
			sql:      "UPDATE users SET name='John'",
			expected: false,
		},
		{
			name:     "SELECT statement",
			sql:      "SELECT * FROM users",
			expected: true,
		},
		{
			name:     "INSERT statement",
			sql:      "INSERT INTO users (name) VALUES ('John')",
			expected: true,
		},
		{
			name:     "DELETE statement with WHERE clause",
			sql:      "DELETE FROM users WHERE id=1",
			expected: true,
		},
		{
			name:     "UPDATE statement with complex WHERE condition",
			sql:      "UPDATE users SET name='John', age=25 WHERE id=1 AND status='active'",
			expected: true,
		},
		{
			name:     "UPDATE statement with subquery in WHERE clause",
			sql:      "UPDATE users SET name='John' WHERE id IN (SELECT user_id FROM orders WHERE amount > 100)",
			expected: true,
		},
		// Multi-table UPDATE test cases
		{
			name:     "Multi-table UPDATE with WHERE clause",
			sql:      "UPDATE users u, orders o SET u.name='John', o.status='processed' WHERE u.id=o.user_id AND u.id=1",
			expected: true,
		},
		{
			name:     "Multi-table UPDATE without WHERE clause",
			sql:      "UPDATE users u, orders o SET u.name='John', o.status='processed'",
			expected: false,
		},
		{
			name:     "JOIN multi-table UPDATE with WHERE clause",
			sql:      "UPDATE users u JOIN orders o ON u.id=o.user_id SET u.name='John' WHERE o.amount > 100",
			expected: true,
		},
		{
			name:     "JOIN multi-table UPDATE without WHERE clause",
			sql:      "UPDATE users u JOIN orders o ON u.id=o.user_id SET u.name='John'",
			expected: false,
		},
		{
			name:     "JOIN multi-table UPDATE without WHERE clause",
			sql:      "UPDATE users u JOIN orders o USING(id) SET u.name='John'",
			expected: false,
		},
		{
			name:     "JOIN multi-table UPDATE without WHERE clause",
			sql:      "UPDATE users u JOIN orders o USING(id) SET u.name='John' WHERE o.amount > 100",
			expected: true,
		},
		{
			name:     "LEFT JOIN multi-table UPDATE with WHERE clause",
			sql:      "UPDATE users u LEFT JOIN orders o ON u.id=o.user_id SET u.name='John' WHERE o.amount IS NULL",
			expected: true,
		},
		// Multi-table DELETE test cases
		{
			name:     "Multi-table DELETE with WHERE clause",
			sql:      "DELETE u, o FROM users u, orders o WHERE u.id=o.user_id AND u.status='inactive'",
			expected: true,
		},
		{
			name:     "Multi-table DELETE without WHERE clause",
			sql:      "DELETE u, o FROM users u, orders o",
			expected: false,
		},
		{
			name:     "JOIN multi-table DELETE with WHERE clause",
			sql:      "DELETE u, o FROM users u JOIN orders o ON u.id=o.user_id WHERE o.amount < 0",
			expected: true,
		},
		{
			name:     "JOIN multi-table DELETE without WHERE clause",
			sql:      "DELETE u, o FROM users u JOIN orders o ON u.id=o.user_id",
			expected: false,
		},
		{
			name:     "JOIN multi-table DELETE without WHERE clause",
			sql:      "DELETE u, o FROM users u join orders o USING(id);",
			expected: false,
		},
		{
			name:     "JOIN multi-table DELETE with WHERE clause",
			sql:      "DELETE u, o FROM users u join orders o USING(id) WHERE o.amount < 0;",
			expected: true,
		},
		{
			name:     "Single table DELETE without WHERE clause",
			sql:      "DELETE FROM users",
			expected: false,
		},
		{
			name:     "Single table DELETE with complex WHERE clause",
			sql:      "DELETE FROM users WHERE id IN (SELECT user_id FROM orders WHERE created_at < '2020-01-01')",
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			parser := sqlparser.NewTestParser()
			stmt, err := parser.Parse(tt.sql)
			assert.NoError(t, err)

			result := true
			if !isValidDeleteOrUpdateQuery(stmt) {
				result = false
			}

			assert.Equal(t, tt.expected, result)
		})
	}
}
