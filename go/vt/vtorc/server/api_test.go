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
