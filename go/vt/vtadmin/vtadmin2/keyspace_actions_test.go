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

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

type keyspaceActionsFakeServer struct {
	fakeVTAdminServer

	validateKeyspaceReq        *vtadminpb.ValidateKeyspaceRequest
	validateSchemaKeyspaceReq  *vtadminpb.ValidateSchemaKeyspaceRequest
	validateVersionKeyspaceReq *vtadminpb.ValidateVersionKeyspaceRequest
	rebuildKeyspaceGraphReq    *vtadminpb.RebuildKeyspaceGraphRequest
	removeKeyspaceCellReq      *vtadminpb.RemoveKeyspaceCellRequest
	createShardReq             *vtadminpb.CreateShardRequest
	reloadSchemasReq           *vtadminpb.ReloadSchemasRequest
}

func (f *keyspaceActionsFakeServer) GetKeyspace(ctx context.Context, req *vtadminpb.GetKeyspaceRequest) (*vtadminpb.Keyspace, error) {
	return &vtadminpb.Keyspace{
		Cluster:  &vtadminpb.Cluster{Id: req.ClusterId},
		Keyspace: &vtctldatapb.Keyspace{Name: req.Keyspace},
		Shards:   map[string]*vtctldatapb.Shard{"0": {}},
	}, nil
}

func (f *keyspaceActionsFakeServer) ValidateKeyspace(ctx context.Context, req *vtadminpb.ValidateKeyspaceRequest) (*vtctldatapb.ValidateKeyspaceResponse, error) {
	f.validateKeyspaceReq = req
	return &vtctldatapb.ValidateKeyspaceResponse{}, nil
}

func (f *keyspaceActionsFakeServer) ValidateSchemaKeyspace(ctx context.Context, req *vtadminpb.ValidateSchemaKeyspaceRequest) (*vtctldatapb.ValidateSchemaKeyspaceResponse, error) {
	f.validateSchemaKeyspaceReq = req
	return &vtctldatapb.ValidateSchemaKeyspaceResponse{}, nil
}

func (f *keyspaceActionsFakeServer) ValidateVersionKeyspace(ctx context.Context, req *vtadminpb.ValidateVersionKeyspaceRequest) (*vtctldatapb.ValidateVersionKeyspaceResponse, error) {
	f.validateVersionKeyspaceReq = req
	return &vtctldatapb.ValidateVersionKeyspaceResponse{}, nil
}

func (f *keyspaceActionsFakeServer) RebuildKeyspaceGraph(ctx context.Context, req *vtadminpb.RebuildKeyspaceGraphRequest) (*vtadminpb.RebuildKeyspaceGraphResponse, error) {
	f.rebuildKeyspaceGraphReq = req
	return &vtadminpb.RebuildKeyspaceGraphResponse{}, nil
}

func (f *keyspaceActionsFakeServer) RemoveKeyspaceCell(ctx context.Context, req *vtadminpb.RemoveKeyspaceCellRequest) (*vtadminpb.RemoveKeyspaceCellResponse, error) {
	f.removeKeyspaceCellReq = req
	return &vtadminpb.RemoveKeyspaceCellResponse{}, nil
}

func (f *keyspaceActionsFakeServer) CreateShard(ctx context.Context, req *vtadminpb.CreateShardRequest) (*vtctldatapb.CreateShardResponse, error) {
	f.createShardReq = req
	return &vtctldatapb.CreateShardResponse{}, nil
}

func (f *keyspaceActionsFakeServer) ReloadSchemas(ctx context.Context, req *vtadminpb.ReloadSchemasRequest) (*vtadminpb.ReloadSchemasResponse, error) {
	f.reloadSchemasReq = req
	return &vtadminpb.ReloadSchemasResponse{}, nil
}

func newKeyspaceActionsTestServer(t *testing.T, fake *keyspaceActionsFakeServer, readOnly bool) *Server {
	t.Helper()
	s, err := NewServer(fake, Options{ReadOnly: readOnly})
	require.NoError(t, err)
	return s
}

const keyspaceActionBase = "/keyspace/local/commerce"

func TestKeyspaceActionsCallAPI(t *testing.T) {
	tests := []struct {
		action string
		verify func(t *testing.T, fake *keyspaceActionsFakeServer)
		form   func(f url.Values)
	}{
		{
			action: "/validate",
			form:   func(f url.Values) { f.Set("ping_tablets", "on") },
			verify: func(t *testing.T, fake *keyspaceActionsFakeServer) {
				require.NotNil(t, fake.validateKeyspaceReq)
				assert.Equal(t, testClusterID, fake.validateKeyspaceReq.ClusterId)
				assert.Equal(t, testKeyspace, fake.validateKeyspaceReq.Keyspace)
				assert.True(t, fake.validateKeyspaceReq.PingTablets)
			},
		},
		{
			action: "/validate_schema",
			verify: func(t *testing.T, fake *keyspaceActionsFakeServer) {
				require.NotNil(t, fake.validateSchemaKeyspaceReq)
				assert.Equal(t, testKeyspace, fake.validateSchemaKeyspaceReq.Keyspace)
			},
		},
		{
			action: "/validate_version",
			verify: func(t *testing.T, fake *keyspaceActionsFakeServer) {
				require.NotNil(t, fake.validateVersionKeyspaceReq)
				assert.Equal(t, testKeyspace, fake.validateVersionKeyspaceReq.Keyspace)
			},
		},
		{
			action: "/rebuild_graph",
			form:   func(f url.Values) { f.Set("cells", "zone1, zone2") },
			verify: func(t *testing.T, fake *keyspaceActionsFakeServer) {
				require.NotNil(t, fake.rebuildKeyspaceGraphReq)
				assert.Equal(t, testKeyspace, fake.rebuildKeyspaceGraphReq.Keyspace)
				assert.Equal(t, []string{"zone1", "zone2"}, fake.rebuildKeyspaceGraphReq.Cells)
			},
		},
		{
			action: "/remove_cell",
			form:   func(f url.Values) { f.Set("cell", "zone1") },
			verify: func(t *testing.T, fake *keyspaceActionsFakeServer) {
				require.NotNil(t, fake.removeKeyspaceCellReq)
				assert.Equal(t, "zone1", fake.removeKeyspaceCellReq.Cell)
			},
		},
		{
			action: "/create_shard",
			form:   func(f url.Values) { f.Set("shard", "-80") },
			verify: func(t *testing.T, fake *keyspaceActionsFakeServer) {
				require.NotNil(t, fake.createShardReq)
				assert.Equal(t, testClusterID, fake.createShardReq.ClusterId)
				assert.Equal(t, testKeyspace, fake.createShardReq.Options.Keyspace)
				assert.Equal(t, "-80", fake.createShardReq.Options.ShardName)
			},
		},
		{
			action: "/reload_schema",
			verify: func(t *testing.T, fake *keyspaceActionsFakeServer) {
				require.NotNil(t, fake.reloadSchemasReq)
				assert.Equal(t, []string{testClusterID}, fake.reloadSchemasReq.ClusterIds)
				assert.Equal(t, []string{testKeyspace}, fake.reloadSchemasReq.Keyspaces)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.action, func(t *testing.T) {
			fake := &keyspaceActionsFakeServer{}
			s := newKeyspaceActionsTestServer(t, fake, false)

			form := url.Values{}
			if tt.form != nil {
				tt.form(form)
			}
			rec := postShardForm(t, s, keyspaceActionBase+tt.action, form)

			assert.Equal(t, http.StatusSeeOther, rec.Code)
			// Creating a shard lands you on the new shard's detail page.
			expectedRedirect := keyspaceActionBase
			if tt.action == "/create_shard" {
				expectedRedirect = keyspaceActionBase + "/shard/-80"
			}
			assert.Equal(t, expectedRedirect, rec.Header().Get("Location"))
			tt.verify(t, fake)
		})
	}
}

func TestKeyspaceRemoveCellValidation(t *testing.T) {
	fake := &keyspaceActionsFakeServer{}
	s := newKeyspaceActionsTestServer(t, fake, false)

	rec := postShardForm(t, s, keyspaceActionBase+"/remove_cell", url.Values{})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Nil(t, fake.removeKeyspaceCellReq)
}

func TestKeyspaceCreateShardValidation(t *testing.T) {
	fake := &keyspaceActionsFakeServer{}
	s := newKeyspaceActionsTestServer(t, fake, false)

	rec := postShardForm(t, s, keyspaceActionBase+"/create_shard", url.Values{})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Nil(t, fake.createShardReq)
}

func TestKeyspaceActionsReadOnly(t *testing.T) {
	actions := []string{
		"/validate", "/validate_schema", "/validate_version",
		"/rebuild_graph", "/remove_cell", "/create_shard", "/reload_schema",
	}

	for _, action := range actions {
		t.Run(action, func(t *testing.T) {
			fake := &keyspaceActionsFakeServer{}
			s := newKeyspaceActionsTestServer(t, fake, true)

			rec := postShardForm(t, s, keyspaceActionBase+action, url.Values{})

			assert.Equal(t, http.StatusForbidden, rec.Code)
			assert.Nil(t, fake.validateKeyspaceReq)
			assert.Nil(t, fake.createShardReq)
		})
	}
}

func TestKeyspaceDetailRendersActionsCard(t *testing.T) {
	fake := &keyspaceActionsFakeServer{}
	s := newKeyspaceActionsTestServer(t, fake, false)

	token, rec := renderWithCSRF(t, s, keyspaceActionBase)
	body := rec.Body.String()
	assert.Contains(t, body, keyspaceActionBase+"/validate")
	assert.Contains(t, body, keyspaceActionBase+"/create_shard")
	assert.Contains(t, body, keyspaceActionBase+"/reload_schema")

	// A POST using the exact rendered token/cookie pairing must get past
	// CSRF validation.
	rec = postFormWithCSRF(s, keyspaceActionBase+"/reload_schema", token, url.Values{
		"csrf_token": {token},
	})
	assert.Equal(t, http.StatusSeeOther, rec.Code)
	require.NotNil(t, fake.reloadSchemasReq)
}

func TestKeyspaceDetailHidesActionsWhenReadOnly(t *testing.T) {
	fake := &keyspaceActionsFakeServer{}
	s := newKeyspaceActionsTestServer(t, fake, true)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, keyspaceActionBase, nil)
	s.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), keyspaceActionBase+"/validate")
}
