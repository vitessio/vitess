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

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

type pageFakeServer struct {
	fakeVTAdminServer
	getClustersCalled bool
}

func (f *pageFakeServer) GetClusters(ctx context.Context, req *vtadminpb.GetClustersRequest) (*vtadminpb.GetClustersResponse, error) {
	f.getClustersCalled = true
	return &vtadminpb.GetClustersResponse{Clusters: []*vtadminpb.Cluster{
		{Id: "local", Name: "Local"},
		{Id: "prod", Name: "Production"},
	}}, nil
}

func TestClustersPageCallsServerAndRendersRows(t *testing.T) {
	fake := &pageFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/clusters", nil)
	s.ServeHTTP(rec, req)

	assert.True(t, fake.getClustersCalled)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Clusters")
	assert.Contains(t, rec.Body.String(), "local")
	assert.Contains(t, rec.Body.String(), "Production")
	assert.Contains(t, rec.Body.String(), "data-table-filter")
}

func (f *pageFakeServer) GetKeyspaces(ctx context.Context, req *vtadminpb.GetKeyspacesRequest) (*vtadminpb.GetKeyspacesResponse, error) {
	return &vtadminpb.GetKeyspacesResponse{Keyspaces: []*vtadminpb.Keyspace{
		{
			Cluster:  &vtadminpb.Cluster{Id: "local", Name: "Local"},
			Keyspace: &vtctldatapb.Keyspace{Name: "commerce"},
			Shards: map[string]*vtctldatapb.Shard{
				"0": {Name: "0"},
			},
		},
	}}, nil
}

func (f *pageFakeServer) GetKeyspace(ctx context.Context, req *vtadminpb.GetKeyspaceRequest) (*vtadminpb.Keyspace, error) {
	return &vtadminpb.Keyspace{
		Cluster:  &vtadminpb.Cluster{Id: req.GetClusterId(), Name: "Local"},
		Keyspace: &vtctldatapb.Keyspace{Name: req.GetKeyspace()},
		Shards: map[string]*vtctldatapb.Shard{
			"-80": {Name: "-80"},
			"80-": {Name: "80-"},
		},
	}, nil
}

func TestKeyspacesPageRendersRowsAndCreateLink(t *testing.T) {
	s, err := NewServer(&pageFakeServer{}, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/keyspaces", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Keyspaces")
	assert.Contains(t, rec.Body.String(), "commerce")
	assert.Contains(t, rec.Body.String(), "/keyspace/local/commerce")
	assert.Contains(t, rec.Body.String(), "/keyspaces/create")
}

func TestKeyspaceDetailRendersShardNames(t *testing.T) {
	s, err := NewServer(&pageFakeServer{}, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/keyspace/local/commerce", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "commerce")
	assert.Contains(t, rec.Body.String(), "-80")
	assert.Contains(t, rec.Body.String(), "80-")
}

func (f *pageFakeServer) GetTablets(ctx context.Context, req *vtadminpb.GetTabletsRequest) (*vtadminpb.GetTabletsResponse, error) {
	return &vtadminpb.GetTabletsResponse{Tablets: []*vtadminpb.Tablet{
		{
			Cluster: &vtadminpb.Cluster{Id: "local", Name: "Local"},
			Tablet: &topodatapb.Tablet{
				Alias:    &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
				Hostname: "tablet-100",
				Keyspace: "commerce",
				Shard:    "0",
				Type:     topodatapb.TabletType_PRIMARY,
			},
			State: vtadminpb.Tablet_SERVING,
		},
	}}, nil
}

func (f *pageFakeServer) GetSchemas(ctx context.Context, req *vtadminpb.GetSchemasRequest) (*vtadminpb.GetSchemasResponse, error) {
	return &vtadminpb.GetSchemasResponse{Schemas: []*vtadminpb.Schema{
		{
			Cluster:  &vtadminpb.Cluster{Id: "local", Name: "Local"},
			Keyspace: "commerce",
			TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
				{Name: "customer", Schema: "create table customer(id bigint primary key)"},
			},
		},
	}}, nil
}

func TestTabletsPageRendersRows(t *testing.T) {
	s, err := NewServer(&pageFakeServer{}, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/tablets", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Tablets")
	assert.Contains(t, rec.Body.String(), "zone1-0000000100")
	assert.Contains(t, rec.Body.String(), "tablet-100")
	assert.Contains(t, rec.Body.String(), "PRIMARY")
}

func TestSchemasPageRendersTables(t *testing.T) {
	s, err := NewServer(&pageFakeServer{}, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/schemas", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Schemas")
	assert.Contains(t, rec.Body.String(), "commerce")
	assert.Contains(t, rec.Body.String(), "customer")
}
