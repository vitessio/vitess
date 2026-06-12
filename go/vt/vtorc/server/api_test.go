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

package server

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/vt/vtorc/inst"
)

func TestGetACLPermissionLevelForAPI(t *testing.T) {
	tests := []struct {
		apiEndpoint string
		want        string
	}{
		{
			apiEndpoint: problemsAPI,
			want:        acl.MONITORING,
		}, {
			apiEndpoint: errantGTIDsAPI,
			want:        acl.MONITORING,
		}, {
			apiEndpoint: disableGlobalRecoveriesAPI,
			want:        acl.ADMIN,
		}, {
			apiEndpoint: enableGlobalRecoveriesAPI,
			want:        acl.ADMIN,
		}, {
			apiEndpoint: detectionAnalysisAPI,
			want:        acl.MONITORING,
		}, {
			apiEndpoint: healthAPI,
			want:        acl.MONITORING,
		}, {
			apiEndpoint: configAPI,
			want:        acl.MONITORING,
		}, {
			apiEndpoint: shardQuorumAPI,
			want:        acl.MONITORING,
		}, {
			apiEndpoint: "gibberish",
			want:        acl.ADMIN,
		},
	}
	for _, tt := range tests {
		t.Run(tt.apiEndpoint, func(t *testing.T) {
			got := getACLPermissionLevelForAPI(tt.apiEndpoint)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestShardQuorumAPIHandlerEmpty(t *testing.T) {
	inst.ResetShardPeerHealthForTest()
	rec := httptest.NewRecorder()
	shardQuorumAPIHandler(rec)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/json; charset=utf-8", rec.Header().Get("Content-Type"))
	assert.JSONEq(t, "[]", rec.Body.String())
}
