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
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

type tabletActionsFakeServer struct {
	fakeVTAdminServer

	startReplicationReq         *vtadminpb.StartReplicationRequest
	stopReplicationReq          *vtadminpb.StopReplicationRequest
	refreshReplicationSourceReq *vtadminpb.RefreshTabletReplicationSourceRequest
	setReadOnlyReq              *vtadminpb.SetReadOnlyRequest
	setReadWriteReq             *vtadminpb.SetReadWriteRequest
	deleteTabletReq             *vtadminpb.DeleteTabletRequest
	pingTabletReq               *vtadminpb.PingTabletRequest
	refreshStateReq             *vtadminpb.RefreshStateRequest
	runHealthCheckReq           *vtadminpb.RunHealthCheckRequest
}

func (f *tabletActionsFakeServer) GetTablet(ctx context.Context, req *vtadminpb.GetTabletRequest) (*vtadminpb.Tablet, error) {
	return &vtadminpb.Tablet{
		Cluster: &vtadminpb.Cluster{Id: testClusterID},
		Tablet: &topodatapb.Tablet{
			Alias:    &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
			Hostname: "tablet-100",
			Keyspace: "commerce",
			Shard:    "0",
			Type:     topodatapb.TabletType_REPLICA,
		},
		State: vtadminpb.Tablet_SERVING,
	}, nil
}

func (f *tabletActionsFakeServer) StartReplication(ctx context.Context, req *vtadminpb.StartReplicationRequest) (*vtadminpb.StartReplicationResponse, error) {
	f.startReplicationReq = req
	return &vtadminpb.StartReplicationResponse{}, nil
}

func (f *tabletActionsFakeServer) StopReplication(ctx context.Context, req *vtadminpb.StopReplicationRequest) (*vtadminpb.StopReplicationResponse, error) {
	f.stopReplicationReq = req
	return &vtadminpb.StopReplicationResponse{}, nil
}

func (f *tabletActionsFakeServer) RefreshTabletReplicationSource(ctx context.Context, req *vtadminpb.RefreshTabletReplicationSourceRequest) (*vtadminpb.RefreshTabletReplicationSourceResponse, error) {
	f.refreshReplicationSourceReq = req
	return &vtadminpb.RefreshTabletReplicationSourceResponse{}, nil
}

func (f *tabletActionsFakeServer) SetReadOnly(ctx context.Context, req *vtadminpb.SetReadOnlyRequest) (*vtadminpb.SetReadOnlyResponse, error) {
	f.setReadOnlyReq = req
	return &vtadminpb.SetReadOnlyResponse{}, nil
}

func (f *tabletActionsFakeServer) SetReadWrite(ctx context.Context, req *vtadminpb.SetReadWriteRequest) (*vtadminpb.SetReadWriteResponse, error) {
	f.setReadWriteReq = req
	return &vtadminpb.SetReadWriteResponse{}, nil
}

func (f *tabletActionsFakeServer) DeleteTablet(ctx context.Context, req *vtadminpb.DeleteTabletRequest) (*vtadminpb.DeleteTabletResponse, error) {
	f.deleteTabletReq = req
	return &vtadminpb.DeleteTabletResponse{}, nil
}

func (f *tabletActionsFakeServer) PingTablet(ctx context.Context, req *vtadminpb.PingTabletRequest) (*vtadminpb.PingTabletResponse, error) {
	f.pingTabletReq = req
	return &vtadminpb.PingTabletResponse{}, nil
}

func (f *tabletActionsFakeServer) RefreshState(ctx context.Context, req *vtadminpb.RefreshStateRequest) (*vtadminpb.RefreshStateResponse, error) {
	f.refreshStateReq = req
	return &vtadminpb.RefreshStateResponse{Status: "serving"}, nil
}

func (f *tabletActionsFakeServer) RunHealthCheck(ctx context.Context, req *vtadminpb.RunHealthCheckRequest) (*vtadminpb.RunHealthCheckResponse, error) {
	f.runHealthCheckReq = req
	return &vtadminpb.RunHealthCheckResponse{Status: "healthy"}, nil
}

func newTabletActionsTestServer(t *testing.T, fake *tabletActionsFakeServer, readOnly bool) *Server {
	t.Helper()
	s, err := NewServer(fake, Options{ReadOnly: readOnly})
	require.NoError(t, err)
	return s
}

const tabletActionBase = "/tablet/local/zone1-0000000100"

func postTabletAction(t *testing.T, s *Server, path string, form url.Values) *httptest.ResponseRecorder {
	t.Helper()
	return postShardForm(t, s, path, form)
}

func TestTabletActionsCallAPIWithAlias(t *testing.T) {
	tests := []struct {
		action   string
		verify   func(t *testing.T, fake *tabletActionsFakeServer)
		formFunc func(f url.Values)
	}{
		{
			action: "/start_replication",
			verify: func(t *testing.T, fake *tabletActionsFakeServer) {
				require.NotNil(t, fake.startReplicationReq)
				assert.Equal(t, "zone1", fake.startReplicationReq.Alias.Cell)
				assert.Equal(t, uint32(100), fake.startReplicationReq.Alias.Uid)
				assert.Equal(t, []string{testClusterID}, fake.startReplicationReq.ClusterIds)
			},
		},
		{
			action: "/stop_replication",
			verify: func(t *testing.T, fake *tabletActionsFakeServer) {
				require.NotNil(t, fake.stopReplicationReq)
				assert.Equal(t, uint32(100), fake.stopReplicationReq.Alias.Uid)
			},
		},
		{
			action: "/refresh_replication_source",
			verify: func(t *testing.T, fake *tabletActionsFakeServer) {
				require.NotNil(t, fake.refreshReplicationSourceReq)
				assert.Equal(t, uint32(100), fake.refreshReplicationSourceReq.Alias.Uid)
			},
		},
		{
			action: "/set_read_only",
			verify: func(t *testing.T, fake *tabletActionsFakeServer) {
				require.NotNil(t, fake.setReadOnlyReq)
				assert.Equal(t, uint32(100), fake.setReadOnlyReq.Alias.Uid)
			},
		},
		{
			action: "/set_read_write",
			verify: func(t *testing.T, fake *tabletActionsFakeServer) {
				require.NotNil(t, fake.setReadWriteReq)
				assert.Equal(t, uint32(100), fake.setReadWriteReq.Alias.Uid)
			},
		},
		{
			action: "/ping",
			verify: func(t *testing.T, fake *tabletActionsFakeServer) {
				require.NotNil(t, fake.pingTabletReq)
				assert.Equal(t, uint32(100), fake.pingTabletReq.Alias.Uid)
			},
		},
		{
			action: "/refresh_state",
			verify: func(t *testing.T, fake *tabletActionsFakeServer) {
				require.NotNil(t, fake.refreshStateReq)
				assert.Equal(t, uint32(100), fake.refreshStateReq.Alias.Uid)
			},
		},
		{
			action: "/health_check",
			verify: func(t *testing.T, fake *tabletActionsFakeServer) {
				require.NotNil(t, fake.runHealthCheckReq)
				assert.Equal(t, uint32(100), fake.runHealthCheckReq.Alias.Uid)
			},
		},
		{
			action: "/delete",
			formFunc: func(f url.Values) {
				f.Set("allow_primary", "on")
			},
			verify: func(t *testing.T, fake *tabletActionsFakeServer) {
				require.NotNil(t, fake.deleteTabletReq)
				assert.Equal(t, uint32(100), fake.deleteTabletReq.Alias.Uid)
				assert.True(t, fake.deleteTabletReq.AllowPrimary)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.action, func(t *testing.T) {
			fake := &tabletActionsFakeServer{}
			s := newTabletActionsTestServer(t, fake, false)

			form := url.Values{}
			if tt.formFunc != nil {
				tt.formFunc(form)
			}
			rec := postTabletAction(t, s, tabletActionBase+tt.action, form)

			assert.Equal(t, http.StatusSeeOther, rec.Code)
			tt.verify(t, fake)
		})
	}
}

func TestTabletDeleteRedirectsToTabletsList(t *testing.T) {
	fake := &tabletActionsFakeServer{}
	s := newTabletActionsTestServer(t, fake, false)

	rec := postTabletAction(t, s, tabletActionBase+"/delete", url.Values{})

	assert.Equal(t, http.StatusSeeOther, rec.Code)
	assert.Equal(t, "/tablets", rec.Header().Get("Location"))
}

func TestTabletActionFlashCarriesResult(t *testing.T) {
	fake := &tabletActionsFakeServer{}
	s := newTabletActionsTestServer(t, fake, false)

	rec := postTabletAction(t, s, tabletActionBase+"/refresh_state", url.Values{})
	assert.Equal(t, http.StatusSeeOther, rec.Code)

	cookie := findCookie(rec, flashCookieName)
	require.NotNil(t, cookie)
	flash := decodeFlash(cookie.Value)
	require.NotNil(t, flash)
	assert.Equal(t, "tablet state refreshed: serving", flash.Message)
}

func TestTabletActionsReadOnly(t *testing.T) {
	actions := []string{
		"/start_replication", "/stop_replication", "/refresh_replication_source",
		"/set_read_only", "/set_read_write", "/delete", "/ping",
		"/refresh_state", "/health_check",
	}

	for _, action := range actions {
		t.Run(action, func(t *testing.T) {
			fake := &tabletActionsFakeServer{}
			s := newTabletActionsTestServer(t, fake, true)

			rec := postTabletAction(t, s, tabletActionBase+action, url.Values{})

			assert.Equal(t, http.StatusForbidden, rec.Code)
			assert.Nil(t, fake.deleteTabletReq)
			assert.Nil(t, fake.startReplicationReq)
		})
	}
}

func TestTabletDetailRendersActionsCard(t *testing.T) {
	fake := &tabletActionsFakeServer{}
	s := newTabletActionsTestServer(t, fake, false)

	token, rec := renderWithCSRF(t, s, tabletActionBase)
	body := rec.Body.String()
	assert.Contains(t, body, tabletActionBase+"/start_replication")
	assert.Contains(t, body, tabletActionBase+"/delete")
	assert.Contains(t, body, `name="allow_primary"`)

	// A POST using the exact rendered token/cookie pairing must get past
	// CSRF validation.
	rec = postFormWithCSRF(s, tabletActionBase+"/ping", token, url.Values{
		"csrf_token": {token},
	})
	assert.Equal(t, http.StatusSeeOther, rec.Code)
	require.NotNil(t, fake.pingTabletReq)
}

func TestTabletDetailHidesActionsWhenReadOnly(t *testing.T) {
	fake := &tabletActionsFakeServer{}
	s := newTabletActionsTestServer(t, fake, true)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, tabletActionBase, nil)
	s.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), tabletActionBase+"/start_replication")
}

func TestTabletActionInvalidAlias(t *testing.T) {
	fake := &tabletActionsFakeServer{}
	s := newTabletActionsTestServer(t, fake, false)

	rec := postTabletAction(t, s, "/tablet/local/not-an-alias/start_replication", url.Values{})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Nil(t, fake.startReplicationReq)
}
