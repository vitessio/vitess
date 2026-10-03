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

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

type debugJSONFakeServer struct {
	fakeVTAdminServer
}

func (f *debugJSONFakeServer) GetTablets(ctx context.Context, req *vtadminpb.GetTabletsRequest) (*vtadminpb.GetTabletsResponse, error) {
	tablet := &vtadminpb.Tablet{
		Cluster: &vtadminpb.Cluster{Id: "local"},
		Tablet: &topodatapb.Tablet{
			Alias:    &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
			Keyspace: "commerce",
			Shard:    "0",
		},
	}
	return &vtadminpb.GetTabletsResponse{Tablets: []*vtadminpb.Tablet{tablet}}, nil
}

func TestDebugJSONRendersPageData(t *testing.T) {
	fake := &debugJSONFakeServer{}
	s, err := NewServer(fake, Options{EnableDebugJSON: true})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/tablets?format=json", nil)
	s.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Header().Get("Content-Type"), "application/json")
	assert.Contains(t, rec.Body.String(), "zone1")
}

func TestDebugJSONDisabledByDefault(t *testing.T) {
	fake := &debugJSONFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/tablets?format=json", nil)
	s.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Header().Get("Content-Type"), "application/json")
}
