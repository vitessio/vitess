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

package vtadmin2

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

type tabletFakeServer struct {
	fakeVTAdminServer
	getTabletRequest     *vtadminpb.GetTabletRequest
	getFullStatusRequest *vtadminpb.GetFullStatusRequest
	getTabletError       error
	getFullStatusError   error
	getFullStatusNil     bool
}

func (f *tabletFakeServer) GetTablet(ctx context.Context, req *vtadminpb.GetTabletRequest) (*vtadminpb.Tablet, error) {
	f.getTabletRequest = req
	if f.getTabletError != nil {
		return nil, f.getTabletError
	}
	return &vtadminpb.Tablet{
		Cluster: &vtadminpb.Cluster{Id: "local", Name: "Local"},
		Tablet: &topodatapb.Tablet{
			Alias:    &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
			Hostname: "tablet-100",
			Keyspace: "commerce",
			Shard:    "0",
			Type:     topodatapb.TabletType_PRIMARY,
		},
		State: vtadminpb.Tablet_SERVING,
	}, nil
}

func (f *tabletFakeServer) GetFullStatus(ctx context.Context, req *vtadminpb.GetFullStatusRequest) (*vtctldatapb.GetFullStatusResponse, error) {
	f.getFullStatusRequest = req
	if f.getFullStatusError != nil {
		return nil, f.getFullStatusError
	}
	if f.getFullStatusNil {
		return nil, nil
	}
	return &vtctldatapb.GetFullStatusResponse{
		Status: &replicationdatapb.FullStatus{ServerUuid: "server-uuid-1"},
	}, nil
}

func TestTabletDetailCallsServerAndRendersTablet(t *testing.T) {
	fake := &tabletFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/tablet/local/zone1-0000000100", nil)
	s.ServeHTTP(rec, req)

	require.NotNil(t, fake.getTabletRequest)
	assert.Equal(t, []string{"local"}, fake.getTabletRequest.GetClusterIds())
	require.NotNil(t, fake.getTabletRequest.GetAlias())
	assert.Equal(t, "zone1", fake.getTabletRequest.GetAlias().GetCell())
	assert.EqualValues(t, 100, fake.getTabletRequest.GetAlias().GetUid())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "zone1-0000000100")
	assert.Contains(t, rec.Body.String(), "tablet-100")
	assert.Contains(t, rec.Body.String(), "commerce")
}

func TestTabletFullStatusCallsServerAndRendersStatus(t *testing.T) {
	fake := &tabletFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/tablet/local/zone1-0000000100/full_status", nil)
	s.ServeHTTP(rec, req)

	require.NotNil(t, fake.getFullStatusRequest)
	assert.Equal(t, "local", fake.getFullStatusRequest.GetClusterId())
	require.NotNil(t, fake.getFullStatusRequest.GetAlias())
	assert.Equal(t, "zone1", fake.getFullStatusRequest.GetAlias().GetCell())
	assert.EqualValues(t, 100, fake.getFullStatusRequest.GetAlias().GetUid())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Full Status")
	assert.Contains(t, rec.Body.String(), "zone1-0000000100")
	assert.Contains(t, rec.Body.String(), "server-uuid-1")
}

func TestTabletDetailInvalidAliasReturnsBadRequest(t *testing.T) {
	fake := &tabletFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/tablet/local/not-a-valid-alias", nil)
	s.ServeHTTP(rec, req)

	assert.Nil(t, fake.getTabletRequest)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "not-a-valid-alias")
}

func TestTabletDetailNotFoundReturnsNotFound(t *testing.T) {
	fake := &tabletFakeServer{
		getTabletError: vterrors.Errorf(vtrpcpb.Code_NOT_FOUND, "tablet missing"),
	}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/tablet/local/zone1-0000000100", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Contains(t, rec.Body.String(), "tablet missing")
}

func TestTabletDetailInvalidBackendArgumentReturnsBadRequest(t *testing.T) {
	fake := &tabletFakeServer{
		getTabletError: vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "bad tablet request"),
	}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/tablet/local/zone1-0000000100", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "bad tablet request")
}

func TestTabletFullStatusNotFoundReturnsNotFound(t *testing.T) {
	fake := &tabletFakeServer{
		getFullStatusError: vterrors.Errorf(vtrpcpb.Code_NOT_FOUND, "full status missing"),
	}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/tablet/local/zone1-0000000100/full_status", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Contains(t, rec.Body.String(), "full status missing")
}

func TestTabletFullStatusNilResponseReturnsNotFound(t *testing.T) {
	fake := &tabletFakeServer{getFullStatusNil: true}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/tablet/local/zone1-0000000100/full_status", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestTabletFullStatusInvalidBackendArgumentReturnsBadRequest(t *testing.T) {
	fake := &tabletFakeServer{
		getFullStatusError: vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "bad full status request"),
	}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/tablet/local/zone1-0000000100/full_status", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "bad full status request")
}
