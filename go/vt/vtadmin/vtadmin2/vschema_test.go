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

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

type vschemaFakeServer struct {
	fakeVTAdminServer
	getVSchemasRequest     *vtadminpb.GetVSchemasRequest
	getVSchemaRequest      *vtadminpb.GetVSchemaRequest
	getSrvKeyspacesRequest *vtadminpb.GetSrvKeyspacesRequest
	getSrvVSchemasRequest  *vtadminpb.GetSrvVSchemasRequest
	getVSchemasError       error
	getVSchemaError        error
	getSrvKeyspacesError   error
	getSrvVSchemasError    error
	getVSchemaNilResponse  bool
}

func (f *vschemaFakeServer) GetVSchemas(ctx context.Context, req *vtadminpb.GetVSchemasRequest) (*vtadminpb.GetVSchemasResponse, error) {
	f.getVSchemasRequest = req
	if f.getVSchemasError != nil {
		return nil, f.getVSchemasError
	}
	return &vtadminpb.GetVSchemasResponse{VSchemas: []*vtadminpb.VSchema{{Cluster: &vtadminpb.Cluster{Id: "local", Name: "Local"}, Name: "commerce", VSchema: &vschemapb.Keyspace{Tables: map[string]*vschemapb.Table{"customer": {}}}}}}, nil
}

func (f *vschemaFakeServer) GetVSchema(ctx context.Context, req *vtadminpb.GetVSchemaRequest) (*vtadminpb.VSchema, error) {
	f.getVSchemaRequest = req
	if f.getVSchemaError != nil {
		return nil, f.getVSchemaError
	}
	if f.getVSchemaNilResponse {
		return nil, nil
	}
	return &vtadminpb.VSchema{Cluster: &vtadminpb.Cluster{Id: "local", Name: "Local"}, Name: "commerce", VSchema: &vschemapb.Keyspace{Tables: map[string]*vschemapb.Table{"customer": {}}}}, nil
}

func (f *vschemaFakeServer) GetSrvKeyspaces(ctx context.Context, req *vtadminpb.GetSrvKeyspacesRequest) (*vtadminpb.GetSrvKeyspacesResponse, error) {
	f.getSrvKeyspacesRequest = req
	if f.getSrvKeyspacesError != nil {
		return nil, f.getSrvKeyspacesError
	}
	return &vtadminpb.GetSrvKeyspacesResponse{SrvKeyspaces: map[string]*vtctldatapb.GetSrvKeyspacesResponse{"commerce": {SrvKeyspaces: map[string]*topodatapb.SrvKeyspace{"zone1": {}}}}}, nil
}

func (f *vschemaFakeServer) GetSrvVSchemas(ctx context.Context, req *vtadminpb.GetSrvVSchemasRequest) (*vtadminpb.GetSrvVSchemasResponse, error) {
	f.getSrvVSchemasRequest = req
	if f.getSrvVSchemasError != nil {
		return nil, f.getSrvVSchemasError
	}
	return &vtadminpb.GetSrvVSchemasResponse{SrvVSchemas: []*vtadminpb.SrvVSchema{{Cell: "zone1", Cluster: &vtadminpb.Cluster{Id: "local", Name: "Local"}, SrvVSchema: &vschemapb.SrvVSchema{Keyspaces: map[string]*vschemapb.Keyspace{"commerce": {}}}}}}, nil
}

func TestVSchemasPageCallsServerAndRendersRows(t *testing.T) {
	fake := &vschemaFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/vschemas?cluster_id=local", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	require.NotNil(t, fake.getVSchemasRequest)
	assert.Equal(t, []string{"local"}, fake.getVSchemasRequest.GetClusterIds())
	assert.Contains(t, rec.Body.String(), "VSchema")
	assert.Contains(t, rec.Body.String(), "commerce")
	assert.Contains(t, rec.Body.String(), "href=\"/vschema/local/commerce\"")
}

func TestVSchemasPageBackendErrorReturnsInternalServerError(t *testing.T) {
	fake := &vschemaFakeServer{getVSchemasError: errors.New("vschemas backend failed")}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/vschemas?cluster_id=local", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.Contains(t, rec.Body.String(), "vschemas backend failed")
}

func TestVSchemaPageCallsServerAndRendersDetails(t *testing.T) {
	fake := &vschemaFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/vschema/local/commerce", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	require.NotNil(t, fake.getVSchemaRequest)
	assert.Equal(t, "local", fake.getVSchemaRequest.GetClusterId())
	assert.Equal(t, "commerce", fake.getVSchemaRequest.GetKeyspace())
	assert.Contains(t, rec.Body.String(), "commerce")
	assert.Contains(t, rec.Body.String(), "customer")
}

func TestVSchemaPageBackendErrorReturnsInternalServerError(t *testing.T) {
	fake := &vschemaFakeServer{getVSchemaError: errors.New("vschema backend failed")}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/vschema/local/commerce", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.Contains(t, rec.Body.String(), "vschema backend failed")
}

func TestVSchemaDetailNilResponseReturnsNotFound(t *testing.T) {
	fake := &vschemaFakeServer{getVSchemaNilResponse: true}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/vschema/local/commerce", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestSrvKeyspacesPageCallsServerAndRendersRows(t *testing.T) {
	fake := &vschemaFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/srvkeyspaces?cluster_id=local&cell=zone1", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	require.NotNil(t, fake.getSrvKeyspacesRequest)
	assert.Equal(t, []string{"local"}, fake.getSrvKeyspacesRequest.GetClusterIds())
	assert.Equal(t, []string{"zone1"}, fake.getSrvKeyspacesRequest.GetCells())
	assert.Contains(t, rec.Body.String(), "SrvKeyspaces")
	assert.Contains(t, rec.Body.String(), "commerce")
	assert.Contains(t, rec.Body.String(), "zone1")
}

func TestSrvKeyspacesPageBackendErrorReturnsInternalServerError(t *testing.T) {
	fake := &vschemaFakeServer{getSrvKeyspacesError: errors.New("srvkeyspaces backend failed")}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/srvkeyspaces?cluster_id=local&cell=zone1", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.Contains(t, rec.Body.String(), "srvkeyspaces backend failed")
}

func TestSrvVSchemasPageCallsServerAndRendersRows(t *testing.T) {
	fake := &vschemaFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/srvvschemas?cluster_id=local&cell=zone1", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	require.NotNil(t, fake.getSrvVSchemasRequest)
	assert.Equal(t, []string{"local"}, fake.getSrvVSchemasRequest.GetClusterIds())
	assert.Equal(t, []string{"zone1"}, fake.getSrvVSchemasRequest.GetCells())
	assert.Contains(t, rec.Body.String(), "SrvVSchemas")
	assert.Contains(t, rec.Body.String(), "zone1")
	assert.Contains(t, rec.Body.String(), "commerce")
}

func TestSrvVSchemasPageBackendErrorReturnsInternalServerError(t *testing.T) {
	fake := &vschemaFakeServer{getSrvVSchemasError: errors.New("srvvschemas backend failed")}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/srvvschemas?cluster_id=local&cell=zone1", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.Contains(t, rec.Body.String(), "srvvschemas backend failed")
}
