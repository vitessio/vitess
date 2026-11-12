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

package opentsdb

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/acl/acltest"
)

// TestDebugHandlerACL checks that /debug/opentsdb requires the DEBUGGING role:
// it is served without a restrictive policy and under read-only, and refused
// under deny-all.
func TestDebugHandlerACL(t *testing.T) {
	prevURI := openTSDBURI
	openTSDBURI = "http://localhost:4242"
	t.Cleanup(func() { openTSDBURI = prevURI })

	b, err := newBackend("test")
	require.NoError(t, err)

	tests := []struct {
		mode     acltest.Mode
		wantCode int
	}{
		{acltest.AllowAll, http.StatusOK},
		{acltest.ReadOnly, http.StatusOK},
		{acltest.DenyAll, http.StatusForbidden},
	}
	for _, tt := range tests {
		t.Run(tt.mode.String(), func(t *testing.T) {
			acltest.SetMode(t, tt.mode)

			rec := httptest.NewRecorder()
			b.debugHandler(rec, httptest.NewRequest(http.MethodGet, "/debug/opentsdb", nil))

			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.wantCode == http.StatusOK {
				assert.Equal(t, "application/json; charset=utf-8", rec.Header().Get("Content-Type"))
			} else {
				// The handler must stop after the error and write nothing else.
				assert.Equal(t, acltest.DeniedBody(tt.mode), rec.Body.String())
			}
		})
	}
}
