package server

import (
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/acl"
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
			apiEndpoint: disableGlobalRecoveriesAPI,
			want:        acl.ADMIN,
		}, {
			apiEndpoint: enableGlobalRecoveriesAPI,
			want:        acl.ADMIN,
		}, {
			apiEndpoint: replicationAnalysisAPI,
			want:        acl.MONITORING,
		}, {
			apiEndpoint: healthAPI,
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
