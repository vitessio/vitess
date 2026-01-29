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

package binlogacl

import (
	"testing"

	"github.com/stretchr/testify/assert"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestBinlogAcl(t *testing.T) {
	cdcUser := querypb.VTGateCallerID{Username: "cdcUser"}
	regularUser := querypb.VTGateCallerID{Username: "regularUser"}

	// By default no users are allowed in (empty string means no users authorized)
	assert.False(t, Authorized(&cdcUser), "user should not be authorized by default")
	assert.False(t, Authorized(&regularUser), "user should not be authorized by default")

	// Test wildcard - all users authorized
	AuthorizedBinlogUsers.Set(NewAuthorizedBinlogUsers("%"))

	assert.True(t, Authorized(&cdcUser), "user should be authorized with wildcard")
	assert.True(t, Authorized(&regularUser), "user should be authorized with wildcard")

	// Test user list - only specific users authorized
	AuthorizedBinlogUsers.Set(NewAuthorizedBinlogUsers("cdcUser, replicationUser, debeziumUser"))

	assert.True(t, Authorized(&cdcUser), "cdcUser should be authorized")
	assert.False(t, Authorized(&regularUser), "regularUser should not be authorized")

	// Test with spaces in user list
	AuthorizedBinlogUsers.Set(NewAuthorizedBinlogUsers("  cdcUser  ,  regularUser  "))

	assert.True(t, Authorized(&cdcUser), "cdcUser should be authorized (spaces trimmed)")
	assert.True(t, Authorized(&regularUser), "regularUser should be authorized (spaces trimmed)")

	// Revert to baseline state for other tests
	AuthorizedBinlogUsers.Set(NewAuthorizedBinlogUsers(""))

	// Confirm back to default state
	assert.False(t, Authorized(&cdcUser), "user should not be authorized after reset")
	assert.False(t, Authorized(&regularUser), "user should not be authorized after reset")
}

func TestNewAuthorizedBinlogUsers(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		allowAll bool
		expected map[string]struct{}
	}{
		{
			name:     "empty string - no users",
			input:    "",
			allowAll: false,
			expected: map[string]struct{}{},
		},
		{
			name:     "wildcard - all users",
			input:    "%",
			allowAll: true,
			expected: map[string]struct{}{},
		},
		{
			name:     "single user",
			input:    "cdcUser",
			allowAll: false,
			expected: map[string]struct{}{"cdcUser": {}},
		},
		{
			name:     "multiple users",
			input:    "user1,user2,user3",
			allowAll: false,
			expected: map[string]struct{}{"user1": {}, "user2": {}, "user3": {}},
		},
		{
			name:     "users with spaces",
			input:    "  user1  ,  user2  ",
			allowAll: false,
			expected: map[string]struct{}{"user1": {}, "user2": {}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := NewAuthorizedBinlogUsers(tc.input)
			assert.Equal(t, tc.allowAll, result.allowAll)
			assert.Equal(t, tc.expected, result.acl)
			assert.Equal(t, tc.input, result.String())
		})
	}
}
