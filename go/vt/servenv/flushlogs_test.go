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

package servenv

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/acl/acltest"
)

// TestFlushLogsHandler checks that /debug/flushlogs requires the ADMIN role:
// it works without a restrictive policy and is refused under both read-only
// (which only grants DEBUGGING and MONITORING) and deny-all.
func TestFlushLogsHandler(t *testing.T) {
	tests := []struct {
		mode     acltest.Mode
		wantCode int
		wantBody string
	}{
		{acltest.AllowAll, http.StatusOK, "flushed"},
		{acltest.ReadOnly, http.StatusForbidden, ""},
		{acltest.DenyAll, http.StatusForbidden, ""},
	}
	for _, tt := range tests {
		t.Run(tt.mode.String(), func(t *testing.T) {
			acltest.SetMode(t, tt.mode)

			rec := httptest.NewRecorder()
			flushLogsHandler(rec, httptest.NewRequest(http.MethodGet, "/debug/flushlogs", nil))

			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.wantCode == http.StatusOK {
				assert.Equal(t, tt.wantBody, rec.Body.String())
			} else {
				// The handler must stop after the error and write nothing else.
				assert.Equal(t, acltest.DeniedBody(tt.mode), rec.Body.String())
			}
		})
	}
}
