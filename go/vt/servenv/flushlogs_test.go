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
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/acl/acltest"
	"vitess.io/vitess/go/vt/logutil"
)

// TestFlushLogsHandler checks that /debug/flushlogs requires the DEBUGGING
// role: it flushes without a restrictive policy and under read-only, and is
// refused without flushing under deny-all.
func TestFlushLogsHandler(t *testing.T) {
	var flushes atomic.Int64
	logutil.OnFlush(func() { flushes.Add(1) })

	tests := []struct {
		mode      acltest.Mode
		wantCode  int
		wantFlush bool
	}{
		{acltest.AllowAll, http.StatusOK, true},
		{acltest.ReadOnly, http.StatusOK, true},
		{acltest.DenyAll, http.StatusForbidden, false},
	}
	for _, tt := range tests {
		t.Run(tt.mode.String(), func(t *testing.T) {
			acltest.SetMode(t, tt.mode)
			before := flushes.Load()

			rec := httptest.NewRecorder()
			flushLogsHandler(rec, httptest.NewRequest(http.MethodGet, "/debug/flushlogs", nil))

			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.wantFlush {
				assert.Equal(t, "flushed", rec.Body.String())
				assert.Equal(t, before+1, flushes.Load())
			} else {
				// The handler must stop after the error: no output, no flush.
				assert.Equal(t, acltest.DeniedBody(tt.mode), rec.Body.String())
				assert.Equal(t, before, flushes.Load())
			}
		})
	}
}
