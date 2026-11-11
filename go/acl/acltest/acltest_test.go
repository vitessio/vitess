/*
Copyright 2026 The Vitess Authors.

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

package acltest

import (
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/acl"
)

// TestSetMode checks that importing the package makes the test policy the
// active one, and that each mode answers acl.CheckAccessHTTP as documented.
func TestSetMode(t *testing.T) {
	req := httptest.NewRequest("GET", "/", nil)
	roles := []string{acl.ADMIN, acl.DEBUGGING, acl.MONITORING, "custom"}

	tests := []struct {
		mode    Mode
		allowed map[string]bool
	}{
		{AllowAll, map[string]bool{acl.ADMIN: true, acl.DEBUGGING: true, acl.MONITORING: true, "custom": true}},
		{DenyAll, map[string]bool{}},
		{ReadOnly, map[string]bool{acl.DEBUGGING: true, acl.MONITORING: true}},
	}
	for _, tt := range tests {
		t.Run(tt.mode.String(), func(t *testing.T) {
			SetMode(t, tt.mode)
			for _, role := range roles {
				err := acl.CheckAccessHTTP(req, role)
				if tt.allowed[role] {
					assert.NoError(t, err, role)
				} else {
					assert.Error(t, err, role)
				}
			}
		})
	}

	// SetMode's cleanup restores the default.
	assert.NoError(t, acl.CheckAccessHTTP(req, acl.ADMIN))
}
