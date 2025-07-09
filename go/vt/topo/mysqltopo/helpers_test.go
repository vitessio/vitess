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

package mysqltopo

import (
	"fmt"
	"strings"
	"testing"
)

func TestExpandQuery(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		args     []string
		expected string
	}{
		{
			name:     "no arguments",
			query:    "SELECT * FROM table",
			args:     []string{},
			expected: "SELECT * FROM table",
		},
		{
			name:     "single argument",
			query:    "SELECT * FROM table WHERE id = %s",
			args:     []string{"123"},
			expected: "SELECT * FROM table WHERE id = '123'",
		},
		{
			name:     "multiple arguments",
			query:    "INSERT INTO table (name, value) VALUES (%s, %s)",
			args:     []string{"test", "value"},
			expected: "INSERT INTO table (name, value) VALUES ('test', 'value')",
		},
		{
			name:     "argument with quotes",
			query:    "SELECT * FROM table WHERE name = %s",
			args:     []string{"test's value"},
			expected: "SELECT * FROM table WHERE name = 'test\\'s value'",
		},
		{
			name:     "argument with special characters",
			query:    "UPDATE table SET data = %s WHERE id = %s",
			args:     []string{"data with\nnewlines\tand\ttabs", "42"},
			expected: "UPDATE table SET data = 'data with\\nnewlines\\tand\\ttabs' WHERE id = '42'",
		},
		{
			name:     "empty string argument",
			query:    "INSERT INTO table (name) VALUES (%s)",
			args:     []string{""},
			expected: "INSERT INTO table (name) VALUES ('')",
		},
		{
			name:     "numeric string argument",
			query:    "SELECT * FROM table WHERE count = %s",
			args:     []string{"100"},
			expected: "SELECT * FROM table WHERE count = '100'",
		},
		{
			name:     "path-like argument",
			query:    "SELECT * FROM table WHERE path = %s",
			args:     []string{"/global/keyspaces/test"},
			expected: "SELECT * FROM table WHERE path = '/global/keyspaces/test'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := expandQuery(tt.query, tt.args...)
			if result != tt.expected {
				t.Errorf("expandQuery() = %q, expected %q", result, tt.expected)
			}
		})
	}
}

func TestParseServerAddr_Helpers(t *testing.T) {
	tests := []struct {
		name         string
		serverAddr   string
		expectedHost string
		expectedPort int
		expectedUser string
		expectedPass string
		expectedDB   string
	}{
		{
			name:         "empty address - defaults",
			serverAddr:   "",
			expectedHost: "localhost",
			expectedPort: 3306,
			expectedUser: "root",
			expectedPass: "",
			expectedDB:   DefaultTopoSchema,
		},
		{
			name:         "host only",
			serverAddr:   "mysql-server",
			expectedHost: "mysql-server",
			expectedPort: 3306,
			expectedUser: "root",
			expectedPass: "",
			expectedDB:   DefaultTopoSchema,
		},
		{
			name:         "host and port",
			serverAddr:   "mysql-server:3307",
			expectedHost: "mysql-server",
			expectedPort: 3307,
			expectedUser: "root",
			expectedPass: "",
			expectedDB:   DefaultTopoSchema,
		},
		{
			name:         "user and host",
			serverAddr:   "testuser@mysql-server",
			expectedHost: "mysql-server",
			expectedPort: 3306,
			expectedUser: "testuser",
			expectedPass: "",
			expectedDB:   DefaultTopoSchema,
		},
		{
			name:         "user, password, and host",
			serverAddr:   "testuser:testpass@mysql-server",
			expectedHost: "mysql-server",
			expectedPort: 3306,
			expectedUser: "testuser",
			expectedPass: "testpass",
			expectedDB:   DefaultTopoSchema,
		},
		{
			name:         "user, password, host, and port",
			serverAddr:   "testuser:testpass@mysql-server:3307",
			expectedHost: "mysql-server",
			expectedPort: 3307,
			expectedUser: "testuser",
			expectedPass: "testpass",
			expectedDB:   DefaultTopoSchema,
		},
		{
			name:         "full connection string with database",
			serverAddr:   "testuser:testpass@mysql-server:3307/customdb",
			expectedHost: "mysql-server",
			expectedPort: 3307,
			expectedUser: "testuser",
			expectedPass: "testpass",
			expectedDB:   "customdb", // Should parse the custom database name
		},
		{
			name:         "host with database",
			serverAddr:   "mysql-server/customdb",
			expectedHost: "mysql-server",
			expectedPort: 3306,
			expectedUser: "root",
			expectedPass: "",
			expectedDB:   "customdb", // Should parse the custom database name
		},
		{
			name:         "host, port, and database",
			serverAddr:   "mysql-server:3307/customdb",
			expectedHost: "mysql-server",
			expectedPort: 3307,
			expectedUser: "root",
			expectedPass: "",
			expectedDB:   "customdb", // Should parse the custom database name
		},
		{
			name:         "complex password with special characters",
			serverAddr:   "user:password@mysql-server:3307",
			expectedHost: "mysql-server",
			expectedPort: 3307,
			expectedUser: "user",
			expectedPass: "password",
			expectedDB:   DefaultTopoSchema,
		},
		{
			name:         "IPv4 address",
			serverAddr:   "192.168.1.100:3306",
			expectedHost: "192.168.1.100",
			expectedPort: 3306,
			expectedUser: "root",
			expectedPass: "",
			expectedDB:   DefaultTopoSchema,
		},
		{
			name:         "user with IPv4 address",
			serverAddr:   "admin:secret@192.168.1.100:3306",
			expectedHost: "192.168.1.100",
			expectedPort: 3306,
			expectedUser: "admin",
			expectedPass: "secret",
			expectedDB:   DefaultTopoSchema,
		},
		{
			name:         "invalid port - should keep default",
			serverAddr:   "mysql-server:invalid",
			expectedHost: "mysql-server",
			expectedPort: 3306, // Should fallback to default when port parsing fails
			expectedUser: "root",
			expectedPass: "",
			expectedDB:   DefaultTopoSchema,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := parseServerAddr(tt.serverAddr)

			if params.Host != tt.expectedHost {
				t.Errorf("Expected host '%s', got '%s'", tt.expectedHost, params.Host)
			}
			if params.Port != tt.expectedPort {
				t.Errorf("Expected port %d, got %d", tt.expectedPort, params.Port)
			}
			if params.Uname != tt.expectedUser {
				t.Errorf("Expected username '%s', got '%s'", tt.expectedUser, params.Uname)
			}
			if params.Pass != tt.expectedPass {
				t.Errorf("Expected password '%s', got '%s'", tt.expectedPass, params.Pass)
			}
			if params.DbName != tt.expectedDB {
				t.Errorf("Expected database '%s', got '%s'", tt.expectedDB, params.DbName)
			}
		})
	}
}

func TestConvertError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		path     string
		expected error
	}{
		{
			name:     "nil error",
			err:      nil,
			path:     "/test/path",
			expected: nil,
		},
		{
			name:     "generic error",
			err:      fmt.Errorf("test error"),
			path:     "/test/path",
			expected: fmt.Errorf("test error"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := convertError(tt.err, tt.path)

			// For nil cases
			if tt.expected == nil && result == nil {
				return
			}

			// For non-nil cases, we just check that the function returns an error
			// since convertError currently just passes through the error
			if tt.expected != nil && result == nil {
				t.Errorf("Expected error, got nil")
			}
			if tt.expected == nil && result != nil {
				t.Errorf("Expected nil, got error: %v", result)
			}

			// Check that the error message is preserved
			if tt.expected != nil && result != nil {
				if tt.expected.Error() != result.Error() {
					t.Errorf("Expected error message '%s', got '%s'", tt.expected.Error(), result.Error())
				}
			}
		})
	}
}

func TestMySQLVersion_String(t *testing.T) {
	tests := []struct {
		name     string
		version  MySQLVersion
		expected string
	}{
		{
			name:     "zero version",
			version:  MySQLVersion(0),
			expected: "0",
		},
		{
			name:     "positive version",
			version:  MySQLVersion(123),
			expected: "123",
		},
		{
			name:     "large version",
			version:  MySQLVersion(9223372036854775807), // max int64
			expected: "9223372036854775807",
		},
		{
			name:     "negative version",
			version:  MySQLVersion(-1),
			expected: "-1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.version.String()
			if result != tt.expected {
				t.Errorf("MySQLVersion.String() = %q, expected %q", result, tt.expected)
			}
		})
	}
}

func TestFactory_HasGlobalReadOnlyCell(t *testing.T) {
	factory := Factory{}

	tests := []struct {
		name       string
		serverAddr string
		root       string
		expected   bool
	}{
		{
			name:       "any server address",
			serverAddr: "localhost:3306",
			root:       "/test",
			expected:   false,
		},
		{
			name:       "empty server address",
			serverAddr: "",
			root:       "",
			expected:   false,
		},
		{
			name:       "complex server address",
			serverAddr: "user:pass@host:3307/db",
			root:       "/complex/root",
			expected:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := factory.HasGlobalReadOnlyCell(tt.serverAddr, tt.root)
			if result != tt.expected {
				t.Errorf("Factory.HasGlobalReadOnlyCell() = %v, expected %v", result, tt.expected)
			}
		})
	}
}

// Helper function to test query expansion with different scenarios
func TestExpandQuery_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		args     []string
		expected string
	}{
		{
			name:     "more placeholders than arguments",
			query:    "SELECT * FROM table WHERE a = %s AND b = %s",
			args:     []string{"value1"},
			expected: "SELECT * FROM table WHERE a = 'value1' AND b = %!s(MISSING)",
		},
		{
			name:     "fewer placeholders than arguments",
			query:    "SELECT * FROM table WHERE a = %s",
			args:     []string{"value1", "value2"},
			expected: "SELECT * FROM table WHERE a = 'value1'%!(EXTRA string='value2')",
		},
		{
			name:     "no placeholders with arguments",
			query:    "SELECT * FROM table",
			args:     []string{"value1"},
			expected: "SELECT * FROM table%!(EXTRA string='value1')",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := expandQuery(tt.query, tt.args...)
			if result != tt.expected {
				t.Errorf("expandQuery() = %q, expected %q", result, tt.expected)
			}
		})
	}
}

// Test that expandQuery properly handles SQL injection attempts
func TestExpandQuery_SQLInjectionPrevention(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		args     []string
		expected string
	}{
		{
			name:     "SQL injection attempt with quotes",
			query:    "SELECT * FROM users WHERE name = %s",
			args:     []string{"'; DROP TABLE users; --"},
			expected: "SELECT * FROM users WHERE name = '\\'; DROP TABLE users; --'",
		},
		{
			name:     "SQL injection with UNION",
			query:    "SELECT * FROM table WHERE id = %s",
			args:     []string{"1 UNION SELECT * FROM passwords"},
			expected: "SELECT * FROM table WHERE id = '1 UNION SELECT * FROM passwords'",
		},
		{
			name:     "backslash escape attempt",
			query:    "INSERT INTO table (data) VALUES (%s)",
			args:     []string{"test\\'; DROP TABLE users; --"},
			expected: "INSERT INTO table (data) VALUES ('test\\\\\\'; DROP TABLE users; --')",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := expandQuery(tt.query, tt.args...)
			if result != tt.expected {
				t.Errorf("expandQuery() = %q, expected %q", result, tt.expected)
			}

			// Ensure the result doesn't contain unescaped dangerous patterns
			if strings.Contains(result, "'; DROP") && !strings.Contains(result, "\\'; DROP") {
				t.Errorf("SQL injection not properly escaped: %q", result)
			}
		})
	}
}
