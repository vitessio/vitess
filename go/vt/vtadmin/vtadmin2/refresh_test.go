/*
Copyright 2026 The Vitess Authors.

Licensed under the Apache License, Version 2.0 the "License";
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vtadmin2

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

type refreshFakeServer struct {
	fakeVTAdminServer
}

func (f *refreshFakeServer) GetClusters(ctx context.Context, req *vtadminpb.GetClustersRequest) (*vtadminpb.GetClustersResponse, error) {
	return &vtadminpb.GetClustersResponse{}, nil
}

func (f *refreshFakeServer) GetWorkflows(ctx context.Context, req *vtadminpb.GetWorkflowsRequest) (*vtadminpb.GetWorkflowsResponse, error) {
	return &vtadminpb.GetWorkflowsResponse{}, nil
}

func TestRefreshMetaRenderedInHead(t *testing.T) {
	fake := &refreshFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/workflows?refresh=30", nil)
	s.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	headEnd := indexOf(body, "</head>")
	metaIdx := indexOf(body, `<meta http-equiv="refresh" content="30">`)
	require.NotEqual(t, -1, metaIdx, "refresh meta not rendered")
	assert.Less(t, metaIdx, headEnd, "refresh meta must be in <head>")
	assert.NotContains(t, body, "<noscript>")
}

func TestRefreshMinIntervalEnforced(t *testing.T) {
	fake := &refreshFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/workflows?refresh=1", nil)
	s.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), `content="10"`)
}

func TestRefreshRestrictedToVolatilePages(t *testing.T) {
	fake := &refreshFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/clusters?refresh=30", nil)
	s.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), "http-equiv=\"refresh\"")
}

func indexOf(s, substr string) int {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}
