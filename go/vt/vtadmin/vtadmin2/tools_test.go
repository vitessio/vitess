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
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

type toolsFakeServer struct {
	fakeVTAdminServer

	vtExplainRequest *vtadminpb.VTExplainRequest
	vtExplainError   error
	vtExplainNil     bool
	vExplainRequest  *vtadminpb.VExplainRequest
	vExplainError    error
	vExplainNil      bool
	getClustersError error
}

func (f *toolsFakeServer) VTExplain(ctx context.Context, req *vtadminpb.VTExplainRequest) (*vtadminpb.VTExplainResponse, error) {
	f.vtExplainRequest = req
	if f.vtExplainError != nil {
		return nil, f.vtExplainError
	}
	if f.vtExplainNil {
		return nil, nil
	}
	return &vtadminpb.VTExplainResponse{Response: "vtgate plan"}, nil
}

func (f *toolsFakeServer) VExplain(ctx context.Context, req *vtadminpb.VExplainRequest) (*vtadminpb.VExplainResponse, error) {
	f.vExplainRequest = req
	if f.vExplainError != nil {
		return nil, f.vExplainError
	}
	if f.vExplainNil {
		return nil, nil
	}
	return &vtadminpb.VExplainResponse{Response: "vttablet plan"}, nil
}

func (f *toolsFakeServer) GetClusters(ctx context.Context, req *vtadminpb.GetClustersRequest) (*vtadminpb.GetClustersResponse, error) {
	if f.getClustersError != nil {
		return nil, f.getClustersError
	}
	return &vtadminpb.GetClustersResponse{Clusters: []*vtadminpb.Cluster{{Id: "local", Name: "Local"}, {Id: "prod", Name: "Prod"}}}, nil
}

func (f *toolsFakeServer) GetKeyspaces(ctx context.Context, req *vtadminpb.GetKeyspacesRequest) (*vtadminpb.GetKeyspacesResponse, error) {
	return &vtadminpb.GetKeyspacesResponse{Keyspaces: []*vtadminpb.Keyspace{
		{Cluster: &vtadminpb.Cluster{Id: "local", Name: "Local"}, Keyspace: &vtctldatapb.Keyspace{Name: "commerce"}},
		{Cluster: &vtadminpb.Cluster{Id: "local", Name: "Local"}, Keyspace: &vtctldatapb.Keyspace{Name: "customer"}},
		{Cluster: &vtadminpb.Cluster{Id: "prod", Name: "Prod"}, Keyspace: &vtctldatapb.Keyspace{Name: "commerce_prod"}},
	}}, nil
}

func TestVTExplainFormOnlyRendersWithoutCallingBackend(t *testing.T) {
	fake := &toolsFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/vtexplain", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "VTExplain")
	assert.Contains(t, rec.Body.String(), `action="/vtexplain"`)
	assert.Contains(t, rec.Body.String(), `name="cluster_id"`)
	assert.Contains(t, rec.Body.String(), `name="keyspace"`)
	assert.Contains(t, rec.Body.String(), `name="sql"`)
	assert.Nil(t, fake.vtExplainRequest)
}

func TestVTExplainDefaultsClusterAndKeyspaceSelects(t *testing.T) {
	fake := &toolsFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/vtexplain", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), `<select name="cluster_id" required>`)
	assert.Contains(t, rec.Body.String(), `<option value="local" selected>Local (local)</option>`)
	assert.Contains(t, rec.Body.String(), `<select name="keyspace" required>`)
	assert.Contains(t, rec.Body.String(), `<option value="commerce" selected>commerce</option>`)
	assert.Nil(t, fake.vtExplainRequest)
}

func TestVTExplainPassesQueryFieldsAndRendersResponse(t *testing.T) {
	fake := &toolsFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/vtexplain?cluster_id=local&keyspace=commerce&sql=select+*+from+customer", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	require.NotNil(t, fake.vtExplainRequest)
	assert.Equal(t, "local", fake.vtExplainRequest.GetCluster())
	assert.Equal(t, "commerce", fake.vtExplainRequest.GetKeyspace())
	assert.Equal(t, "select * from customer", fake.vtExplainRequest.GetSql())
	assert.Contains(t, rec.Body.String(), "vtgate plan")
	assert.Contains(t, rec.Body.String(), "&#34;response&#34;")
}

func TestVTExplainRequiresClusterForSubmittedSQL(t *testing.T) {
	fake := &toolsFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/vtexplain?keyspace=commerce&sql=select+1", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "cluster")
	assert.Nil(t, fake.vtExplainRequest)
}

func TestVTExplainValidatesSubmissionBeforeLoadingFormOptions(t *testing.T) {
	fake := &toolsFakeServer{getClustersError: errors.New("cluster options failed")}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/vtexplain?keyspace=commerce&sql=select+1", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "cluster_id")
	assert.NotContains(t, rec.Body.String(), "cluster options failed")
	assert.Nil(t, fake.vtExplainRequest)
}

func TestVTExplainRejectsBlankSubmission(t *testing.T) {
	fake := &toolsFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/vtexplain?cluster_id=&keyspace=&sql=", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "cluster")
	assert.Nil(t, fake.vtExplainRequest)
}

func TestVTExplainRequiresKeyspaceForSubmittedSQL(t *testing.T) {
	fake := &toolsFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/vtexplain?cluster_id=local&sql=select+1", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "keyspace")
	assert.Nil(t, fake.vtExplainRequest)
}

func TestVTExplainRequiresSQLWhenOtherFieldsAreSupplied(t *testing.T) {
	fake := &toolsFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/vtexplain?cluster_id=local&keyspace=commerce", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "sql")
	assert.Nil(t, fake.vtExplainRequest)
}

func TestVTExplainRendersBackendError(t *testing.T) {
	fake := &toolsFakeServer{vtExplainError: errors.New("vtexplain backend failed")}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/vtexplain?cluster_id=local&keyspace=commerce&sql=select+1", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.Contains(t, rec.Body.String(), "vtexplain backend failed")
	require.NotNil(t, fake.vtExplainRequest)
}

func TestVTExplainReturnsNotFoundForNilResponse(t *testing.T) {
	fake := &toolsFakeServer{vtExplainNil: true}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/vtexplain?cluster_id=local&keyspace=commerce&sql=select+1", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
	require.NotNil(t, fake.vtExplainRequest)
}

func TestVTExplainHeadReturnsMethodNotAllowed(t *testing.T) {
	fake := &toolsFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodHead, "/vtexplain", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
	assert.Nil(t, fake.vtExplainRequest)
}

func TestVExplainFormOnlyRendersWithoutCallingBackend(t *testing.T) {
	fake := &toolsFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/vexplain", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "VExplain")
	assert.Contains(t, rec.Body.String(), `action="/vexplain"`)
	assert.Contains(t, rec.Body.String(), `name="cluster_id"`)
	assert.Contains(t, rec.Body.String(), `name="keyspace"`)
	assert.Contains(t, rec.Body.String(), `name="sql"`)
	assert.Nil(t, fake.vExplainRequest)
}

func TestVExplainDefaultsClusterAndKeyspaceSelects(t *testing.T) {
	fake := &toolsFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/vexplain", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), `<select name="cluster_id" required>`)
	assert.Contains(t, rec.Body.String(), `<option value="local" selected>Local (local)</option>`)
	assert.Contains(t, rec.Body.String(), `<select name="keyspace" required>`)
	assert.Contains(t, rec.Body.String(), `<option value="commerce" selected>commerce</option>`)
	assert.Nil(t, fake.vExplainRequest)
}

func TestVExplainPassesQueryFieldsAndRendersResponse(t *testing.T) {
	fake := &toolsFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/vexplain?cluster_id=local&keyspace=commerce&sql=select+*+from+customer", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	require.NotNil(t, fake.vExplainRequest)
	assert.Equal(t, "local", fake.vExplainRequest.GetClusterId())
	assert.Equal(t, "commerce", fake.vExplainRequest.GetKeyspace())
	assert.Equal(t, "select * from customer", fake.vExplainRequest.GetSql())
	assert.Contains(t, rec.Body.String(), "vttablet plan")
	assert.Contains(t, rec.Body.String(), "&#34;response&#34;")
}

func TestVExplainRequiresClusterIDForSubmittedSQL(t *testing.T) {
	fake := &toolsFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/vexplain?keyspace=commerce&sql=select+1", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "cluster_id")
	assert.Nil(t, fake.vExplainRequest)
}

func TestVExplainValidatesSubmissionBeforeLoadingFormOptions(t *testing.T) {
	fake := &toolsFakeServer{getClustersError: errors.New("cluster options failed")}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/vexplain?keyspace=commerce&sql=select+1", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "cluster_id")
	assert.NotContains(t, rec.Body.String(), "cluster options failed")
	assert.Nil(t, fake.vExplainRequest)
}

func TestVExplainRejectsBlankSubmission(t *testing.T) {
	fake := &toolsFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/vexplain?cluster_id=&keyspace=&sql=", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "cluster_id")
	assert.Nil(t, fake.vExplainRequest)
}

func TestVExplainRequiresKeyspaceForSubmittedSQL(t *testing.T) {
	fake := &toolsFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/vexplain?cluster_id=local&sql=select+1", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "keyspace")
	assert.Nil(t, fake.vExplainRequest)
}

func TestVExplainRequiresSQLWhenOtherFieldsAreSupplied(t *testing.T) {
	fake := &toolsFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/vexplain?cluster_id=local&keyspace=commerce", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "sql")
	assert.Nil(t, fake.vExplainRequest)
}

func TestVExplainRendersBackendError(t *testing.T) {
	fake := &toolsFakeServer{vExplainError: errors.New("vexplain backend failed")}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/vexplain?cluster_id=local&keyspace=commerce&sql=select+1", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.Contains(t, rec.Body.String(), "vexplain backend failed")
	require.NotNil(t, fake.vExplainRequest)
}

func TestVExplainReturnsNotFoundForNilResponse(t *testing.T) {
	fake := &toolsFakeServer{vExplainNil: true}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/vexplain?cluster_id=local&keyspace=commerce&sql=select+1", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
	require.NotNil(t, fake.vExplainRequest)
}

func TestVExplainHeadReturnsMethodNotAllowed(t *testing.T) {
	fake := &toolsFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodHead, "/vexplain", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
	assert.Nil(t, fake.vExplainRequest)
}
