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

package vtgate

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/callerid"
)

func TestAddCallerIDUserToQuery(t *testing.T) {
	tests := []struct {
		name     string
		ctx      context.Context
		sql      string
		expected string
	}{
		{
			name:     "valid caller ID with simple username",
			ctx:      callerid.NewContext(context.Background(), nil, callerid.NewImmediateCallerID("testuser")),
			sql:      "SELECT * FROM users",
			expected: "SELECT * FROM users/* user:testuser */",
		},
		{
			name:     "valid caller ID with username containing spaces",
			ctx:      callerid.NewContext(context.Background(), nil, callerid.NewImmediateCallerID("test user with spaces")),
			sql:      "SELECT * FROM users",
			expected: "SELECT * FROM users/* user:test */",
		},
		{
			name:     "valid caller ID with username containing multiple spaces",
			ctx:      callerid.NewContext(context.Background(), nil, callerid.NewImmediateCallerID("first second third")),
			sql:      "INSERT INTO table VALUES (1)",
			expected: "INSERT INTO table VALUES (1)/* user:first */",
		},
		{
			name:     "valid caller ID with empty username",
			ctx:      callerid.NewContext(context.Background(), nil, callerid.NewImmediateCallerID("")),
			sql:      "DELETE FROM table",
			expected: "DELETE FROM table/* user: */",
		},
		{
			name:     "nil caller ID",
			ctx:      callerid.NewContext(context.Background(), nil, nil),
			sql:      "UPDATE table SET col=1",
			expected: "UPDATE table SET col=1/* user: */",
		},
		{
			name:     "context without caller ID",
			ctx:      context.Background(),
			sql:      "SHOW TABLES",
			expected: "SHOW TABLES/* user: */",
		},
		{
			name:     "empty SQL query",
			ctx:      callerid.NewContext(context.Background(), nil, callerid.NewImmediateCallerID("user")),
			sql:      "",
			expected: "/* user:user */",
		},
		{
			name:     "complex SQL query with caller ID",
			ctx:      callerid.NewContext(context.Background(), nil, callerid.NewImmediateCallerID("complex_user")),
			sql:      "SELECT u.id, u.name FROM users u JOIN orders o ON u.id = o.user_id WHERE u.status = 'active'",
			expected: "SELECT u.id, u.name FROM users u JOIN orders o ON u.id = o.user_id WHERE u.status = 'active'/* user:complex_user */",
		},
		{
			name:     "SQL query with existing comment",
			ctx:      callerid.NewContext(context.Background(), nil, callerid.NewImmediateCallerID("user")),
			sql:      "SELECT * FROM table /* existing comment */",
			expected: "SELECT * FROM table /* existing comment *//* user:user */",
		},
		{
			name:     "username with leading/trailing spaces",
			ctx:      callerid.NewContext(context.Background(), nil, callerid.NewImmediateCallerID("  trimmed  ")),
			sql:      "SELECT 1",
			expected: "SELECT 1/* user: */",
		},
		{
			name:     "username that is only spaces",
			ctx:      callerid.NewContext(context.Background(), nil, callerid.NewImmediateCallerID("   ")),
			sql:      "SELECT 1",
			expected: "SELECT 1/* user: */",
		},
		{
			name:     "single character username",
			ctx:      callerid.NewContext(context.Background(), nil, callerid.NewImmediateCallerID("x")),
			sql:      "SELECT 1",
			expected: "SELECT 1/* user:x */",
		},
		{
			name:     "username with special characters",
			ctx:      callerid.NewContext(context.Background(), nil, callerid.NewImmediateCallerID("user@domain.com")),
			sql:      "SELECT 1",
			expected: "SELECT 1/* user:user@domain.com */",
		},
		{
			name:     "username with special characters and spaces",
			ctx:      callerid.NewContext(context.Background(), nil, callerid.NewImmediateCallerID("user@domain.com admin role")),
			sql:      "SELECT 1",
			expected: "SELECT 1/* user:user@domain.com */",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := addCallerIDUserToQuery(tt.ctx, tt.sql)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestAddCallerIDUserToQuery_EdgeCases tests additional edge cases to ensure 100% coverage
func TestAddCallerIDUserToQuery_EdgeCases(t *testing.T) {
	t.Run("multiline SQL query", func(t *testing.T) {
		ctx := callerid.NewContext(context.Background(), nil, callerid.NewImmediateCallerID("multiline_user"))
		sql := `SELECT *
FROM users
WHERE id = 1`
		expected := `SELECT *
FROM users
WHERE id = 1/* user:multiline_user */`
		result := addCallerIDUserToQuery(ctx, sql)
		assert.Equal(t, expected, result)
	})

	t.Run("SQL with newlines and tabs", func(t *testing.T) {
		ctx := callerid.NewContext(context.Background(), nil, callerid.NewImmediateCallerID("tab_user"))
		sql := "SELECT\t*\nFROM\ttable"
		expected := "SELECT\t*\nFROM\ttable/* user:tab_user */"
		result := addCallerIDUserToQuery(ctx, sql)
		assert.Equal(t, expected, result)
	})

	t.Run("very long username with spaces", func(t *testing.T) {
		longUsername := "very long username with many spaces and words that should be truncated at first space"
		ctx := callerid.NewContext(context.Background(), nil, callerid.NewImmediateCallerID(longUsername))
		sql := "SELECT 1"
		expected := "SELECT 1/* user:very */"
		result := addCallerIDUserToQuery(ctx, sql)
		assert.Equal(t, expected, result)
	})
}

// TestAddCallerIDUserToQuery_Consistency tests that the function consistently behaves the same way
func TestAddCallerIDUserToQuery_Consistency(t *testing.T) {
	ctx := callerid.NewContext(context.Background(), nil, callerid.NewImmediateCallerID("consistent_user"))
	sql := "SELECT * FROM table"
	expected := "SELECT * FROM table/* user:consistent_user */"

	// Call the function multiple times to ensure consistency
	for i := 0; i < 5; i++ {
		result := addCallerIDUserToQuery(ctx, sql)
		assert.Equal(t, expected, result, "Function should return consistent results on call %d", i+1)
	}
}
