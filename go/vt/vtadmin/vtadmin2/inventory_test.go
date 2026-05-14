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

	mysqlctlpb "vitess.io/vitess/go/vt/proto/mysqlctl"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

type inventoryFakeServer struct {
	fakeVTAdminServer
	cellInfosRequest   *vtadminpb.GetCellInfosRequest
	cellAliasesRequest *vtadminpb.GetCellsAliasesRequest
	backupsRequest     *vtadminpb.GetBackupsRequest
}

func (f *inventoryFakeServer) GetGates(ctx context.Context, req *vtadminpb.GetGatesRequest) (*vtadminpb.GetGatesResponse, error) {
	return &vtadminpb.GetGatesResponse{Gates: []*vtadminpb.VTGate{{Cluster: &vtadminpb.Cluster{Id: "local", Name: "Local"}, Hostname: "vtgate-1"}}}, nil
}

func (f *inventoryFakeServer) GetVtctlds(ctx context.Context, req *vtadminpb.GetVtctldsRequest) (*vtadminpb.GetVtctldsResponse, error) {
	return &vtadminpb.GetVtctldsResponse{Vtctlds: []*vtadminpb.Vtctld{{Cluster: &vtadminpb.Cluster{Id: "local", Name: "Local"}, Hostname: "vtctld-1"}}}, nil
}

func (f *inventoryFakeServer) GetCellInfos(ctx context.Context, req *vtadminpb.GetCellInfosRequest) (*vtadminpb.GetCellInfosResponse, error) {
	f.cellInfosRequest = req
	return &vtadminpb.GetCellInfosResponse{CellInfos: []*vtadminpb.ClusterCellInfo{{Cluster: &vtadminpb.Cluster{Id: "local", Name: "Local"}, Name: "zone1"}}}, nil
}

func (f *inventoryFakeServer) GetCellsAliases(ctx context.Context, req *vtadminpb.GetCellsAliasesRequest) (*vtadminpb.GetCellsAliasesResponse, error) {
	f.cellAliasesRequest = req
	return &vtadminpb.GetCellsAliasesResponse{Aliases: []*vtadminpb.ClusterCellsAliases{{Cluster: &vtadminpb.Cluster{Id: "local", Name: "Local"}, Aliases: map[string]*topodatapb.CellsAlias{"regional": {Cells: []string{"zone1", "zone2"}}}}}}, nil
}

func (f *inventoryFakeServer) GetBackups(ctx context.Context, req *vtadminpb.GetBackupsRequest) (*vtadminpb.GetBackupsResponse, error) {
	f.backupsRequest = req
	return &vtadminpb.GetBackupsResponse{Backups: []*vtadminpb.ClusterBackup{{Cluster: &vtadminpb.Cluster{Id: "local", Name: "Local"}, Backup: &mysqlctlpb.BackupInfo{Name: "backup-1", Keyspace: "commerce", Shard: "0"}}}}, nil
}

func TestGatesPageRendersRows(t *testing.T) {
	s, err := NewServer(&inventoryFakeServer{}, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/gates", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "VTGates")
	assert.Contains(t, rec.Body.String(), "vtgate-1")
}

func TestVtctldsPageRendersRows(t *testing.T) {
	s, err := NewServer(&inventoryFakeServer{}, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/vtctlds", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Vtctlds")
	assert.Contains(t, rec.Body.String(), "vtctld-1")
}

func TestCellsPageRendersRows(t *testing.T) {
	s, err := NewServer(&inventoryFakeServer{}, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/cells", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Cells")
	assert.Contains(t, rec.Body.String(), "zone1")
}

func TestCellsPagePassesFilters(t *testing.T) {
	fake := &inventoryFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/cells?cluster_id=local&cell=zone1&names_only=true", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	require.NotNil(t, fake.cellInfosRequest)
	assert.Equal(t, []string{"local"}, fake.cellInfosRequest.GetClusterIds())
	assert.Equal(t, []string{"zone1"}, fake.cellInfosRequest.GetCells())
	assert.True(t, fake.cellInfosRequest.GetNamesOnly())
}

func TestCellsPageRejectsInvalidNamesOnly(t *testing.T) {
	fake := &inventoryFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/cells?names_only=not-bool", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "could not parse query parameter names_only")
	assert.Nil(t, fake.cellInfosRequest)
}

func TestCellsAliasesPageRendersRows(t *testing.T) {
	s, err := NewServer(&inventoryFakeServer{}, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/cells_aliases", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Cell Aliases")
	assert.Contains(t, rec.Body.String(), "regional")
	assert.Contains(t, rec.Body.String(), "zone1")
	assert.Contains(t, rec.Body.String(), "zone2")
}

func TestCellsAliasesPagePassesFilters(t *testing.T) {
	fake := &inventoryFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/cells_aliases?cluster_id=local", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	require.NotNil(t, fake.cellAliasesRequest)
	assert.Equal(t, []string{"local"}, fake.cellAliasesRequest.GetClusterIds())
}

func TestBackupsPageRendersRows(t *testing.T) {
	s, err := NewServer(&inventoryFakeServer{}, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/backups", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Backups")
	assert.Contains(t, rec.Body.String(), "backup-1")
	assert.Contains(t, rec.Body.String(), "commerce")
}

func TestBackupsPagePassesFilters(t *testing.T) {
	fake := &inventoryFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/backups?cluster_id=local&keyspace=commerce&keyspace_shard=commerce/0&limit=10&detailed=false&detailed_limit=2", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	require.NotNil(t, fake.backupsRequest)
	assert.Equal(t, []string{"local"}, fake.backupsRequest.GetClusterIds())
	assert.Equal(t, []string{"commerce"}, fake.backupsRequest.GetKeyspaces())
	assert.Equal(t, []string{"commerce/0"}, fake.backupsRequest.GetKeyspaceShards())
	require.NotNil(t, fake.backupsRequest.GetRequestOptions())
	assert.EqualValues(t, 10, fake.backupsRequest.GetRequestOptions().GetLimit())
	assert.False(t, fake.backupsRequest.GetRequestOptions().GetDetailed())
	assert.EqualValues(t, 2, fake.backupsRequest.GetRequestOptions().GetDetailedLimit())
}

func TestBackupsPageUsesDetailedDefaults(t *testing.T) {
	fake := &inventoryFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/backups", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	require.NotNil(t, fake.backupsRequest)
	require.NotNil(t, fake.backupsRequest.GetRequestOptions())
	assert.True(t, fake.backupsRequest.GetRequestOptions().GetDetailed())
	assert.EqualValues(t, 3, fake.backupsRequest.GetRequestOptions().GetDetailedLimit())
}

func TestBackupsPageRejectsInvalidQueryValues(t *testing.T) {
	fake := &inventoryFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/backups?limit=bad", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "could not parse query parameter limit")
	assert.Nil(t, fake.backupsRequest)
}
