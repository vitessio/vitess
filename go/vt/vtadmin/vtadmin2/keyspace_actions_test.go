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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	logutilpb "vitess.io/vitess/go/vt/proto/logutil"
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
	reloadSchemasResp          *vtadminpb.ReloadSchemasResponse
	reloadSchemasNil           bool
	validateKeyspaceResp       *vtctldatapb.ValidateKeyspaceResponse
	validateSchemaResp         *vtctldatapb.ValidateSchemaKeyspaceResponse
	validateVersionResp        *vtctldatapb.ValidateVersionKeyspaceResponse
	validateKeyspaceNil        bool
	validateSchemaNil          bool
	validateVersionNil         bool
	rebuildGraphNil            bool
	removeCellNil              bool
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
	if f.validateKeyspaceNil {
		return nil, nil
	}
	if f.validateKeyspaceResp != nil {
		return f.validateKeyspaceResp, nil
	}
	return &vtctldatapb.ValidateKeyspaceResponse{}, nil
}

func (f *keyspaceActionsFakeServer) ValidateSchemaKeyspace(ctx context.Context, req *vtadminpb.ValidateSchemaKeyspaceRequest) (*vtctldatapb.ValidateSchemaKeyspaceResponse, error) {
	f.validateSchemaKeyspaceReq = req
	if f.validateSchemaNil {
		return nil, nil
	}
	if f.validateSchemaResp != nil {
		return f.validateSchemaResp, nil
	}
	return &vtctldatapb.ValidateSchemaKeyspaceResponse{}, nil
}

func (f *keyspaceActionsFakeServer) ValidateVersionKeyspace(ctx context.Context, req *vtadminpb.ValidateVersionKeyspaceRequest) (*vtctldatapb.ValidateVersionKeyspaceResponse, error) {
	f.validateVersionKeyspaceReq = req
	if f.validateVersionNil {
		return nil, nil
	}
	if f.validateVersionResp != nil {
		return f.validateVersionResp, nil
	}
	return &vtctldatapb.ValidateVersionKeyspaceResponse{}, nil
}

func (f *keyspaceActionsFakeServer) RebuildKeyspaceGraph(ctx context.Context, req *vtadminpb.RebuildKeyspaceGraphRequest) (*vtadminpb.RebuildKeyspaceGraphResponse, error) {
	f.rebuildKeyspaceGraphReq = req
	if f.rebuildGraphNil {
		return nil, nil
	}
	return &vtadminpb.RebuildKeyspaceGraphResponse{}, nil
}

func (f *keyspaceActionsFakeServer) RemoveKeyspaceCell(ctx context.Context, req *vtadminpb.RemoveKeyspaceCellRequest) (*vtadminpb.RemoveKeyspaceCellResponse, error) {
	f.removeKeyspaceCellReq = req
	if f.removeCellNil {
		return nil, nil
	}
	return &vtadminpb.RemoveKeyspaceCellResponse{}, nil
}

func (f *keyspaceActionsFakeServer) CreateShard(ctx context.Context, req *vtadminpb.CreateShardRequest) (*vtctldatapb.CreateShardResponse, error) {
	f.createShardReq = req
	return &vtctldatapb.CreateShardResponse{}, nil
}

func (f *keyspaceActionsFakeServer) ReloadSchemas(ctx context.Context, req *vtadminpb.ReloadSchemasRequest) (*vtadminpb.ReloadSchemasResponse, error) {
	f.reloadSchemasReq = req
	if f.reloadSchemasNil {
		return nil, nil
	}
	if f.reloadSchemasResp != nil {
		return f.reloadSchemasResp, nil
	}
	return &vtadminpb.ReloadSchemasResponse{
		KeyspaceResults: []*vtadminpb.ReloadSchemasResponse_KeyspaceResult{{
			Keyspace: &vtadminpb.Keyspace{
				Cluster:  &vtadminpb.Cluster{Id: testClusterID},
				Keyspace: &vtctldatapb.Keyspace{Name: testKeyspace},
			},
		}},
	}, nil
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
				assert.Equal(t, int32(10), fake.reloadSchemasReq.Concurrency)
				assert.True(t, fake.reloadSchemasReq.IncludePrimary)
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

func TestKeyspaceActionsUnauthorizedNilResponse(t *testing.T) {
	tests := []struct {
		action string
		form   url.Values
		setup  func(*keyspaceActionsFakeServer)
		called func(*keyspaceActionsFakeServer) any
	}{
		{
			action: "/validate",
			setup:  func(f *keyspaceActionsFakeServer) { f.validateKeyspaceNil = true },
			called: func(f *keyspaceActionsFakeServer) any { return f.validateKeyspaceReq },
		},
		{
			action: "/validate_schema",
			setup:  func(f *keyspaceActionsFakeServer) { f.validateSchemaNil = true },
			called: func(f *keyspaceActionsFakeServer) any { return f.validateSchemaKeyspaceReq },
		},
		{
			action: "/validate_version",
			setup:  func(f *keyspaceActionsFakeServer) { f.validateVersionNil = true },
			called: func(f *keyspaceActionsFakeServer) any { return f.validateVersionKeyspaceReq },
		},
		{
			action: "/rebuild_graph",
			setup:  func(f *keyspaceActionsFakeServer) { f.rebuildGraphNil = true },
			called: func(f *keyspaceActionsFakeServer) any { return f.rebuildKeyspaceGraphReq },
		},
		{
			action: "/remove_cell",
			form:   url.Values{"cell": {"zone1"}},
			setup:  func(f *keyspaceActionsFakeServer) { f.removeCellNil = true },
			called: func(f *keyspaceActionsFakeServer) any { return f.removeKeyspaceCellReq },
		},
		{
			action: "/reload_schema",
			setup:  func(f *keyspaceActionsFakeServer) { f.reloadSchemasNil = true },
			called: func(f *keyspaceActionsFakeServer) any { return f.reloadSchemasReq },
		},
	}

	for _, tt := range tests {
		t.Run(tt.action, func(t *testing.T) {
			fake := &keyspaceActionsFakeServer{}
			tt.setup(fake)
			s := newKeyspaceActionsTestServer(t, fake, false)

			form := url.Values{}
			if tt.form != nil {
				form = tt.form
			}
			rec := postShardForm(t, s, keyspaceActionBase+tt.action, form)

			assert.Equal(t, http.StatusBadRequest, rec.Code)
			assert.NotNil(t, tt.called(fake))
			assert.Contains(t, rec.Body.String(), "not authorized")
		})
	}
}

func TestKeyspaceReloadSchemaEmptyResponseUnauthorized(t *testing.T) {
	fake := &keyspaceActionsFakeServer{
		reloadSchemasResp: &vtadminpb.ReloadSchemasResponse{},
	}
	s := newKeyspaceActionsTestServer(t, fake, false)

	rec := postShardForm(t, s, keyspaceActionBase+"/reload_schema", url.Values{})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.NotEqual(t, keyspaceActionBase, rec.Header().Get("Location"))
	assert.Contains(t, rec.Body.String(), "not authorized")
}

func TestKeyspaceValidateSchemaFailureResultsDoNotFlashSuccess(t *testing.T) {
	fake := &keyspaceActionsFakeServer{
		validateSchemaResp: &vtctldatapb.ValidateSchemaKeyspaceResponse{
			ResultsByShard: map[string]*vtctldatapb.ValidateShardResponse{
				"0":   {Results: []string{"schemas differ"}},
				"-80": {Results: []string{"missing table"}},
			},
		},
	}
	s := newKeyspaceActionsTestServer(t, fake, false)

	rec := postShardForm(t, s, keyspaceActionBase+"/validate_schema", url.Values{})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.NotEqual(t, keyspaceActionBase, rec.Header().Get("Location"))
	assert.Contains(t, rec.Body.String(), "schemas differ")
	assert.Contains(t, rec.Body.String(), "missing table")
	assert.Less(t, strings.Index(rec.Body.String(), "missing table"), strings.Index(rec.Body.String(), "schemas differ"))
}

func TestKeyspaceValidateFailureResultsDoNotFlashSuccess(t *testing.T) {
	fake := &keyspaceActionsFakeServer{
		validateKeyspaceResp: &vtctldatapb.ValidateKeyspaceResponse{
			ResultsByShard: map[string]*vtctldatapb.ValidateShardResponse{
				"0": {Results: []string{"tablet unavailable"}},
			},
		},
	}
	s := newKeyspaceActionsTestServer(t, fake, false)

	rec := postShardForm(t, s, keyspaceActionBase+"/validate", url.Values{})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "tablet unavailable")
}

func TestKeyspaceValidateVersionFailureResultsDoNotFlashSuccess(t *testing.T) {
	fake := &keyspaceActionsFakeServer{
		validateVersionResp: &vtctldatapb.ValidateVersionKeyspaceResponse{
			ResultsByShard: map[string]*vtctldatapb.ValidateShardResponse{
				"0": {Results: []string{"version mismatch"}},
			},
		},
	}
	s := newKeyspaceActionsTestServer(t, fake, false)

	rec := postShardForm(t, s, keyspaceActionBase+"/validate_version", url.Values{})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "version mismatch")
}

func TestKeyspaceReloadSchemaFailureEventsDoNotFlashSuccess(t *testing.T) {
	fake := &keyspaceActionsFakeServer{
		reloadSchemasResp: &vtadminpb.ReloadSchemasResponse{
			KeyspaceResults: []*vtadminpb.ReloadSchemasResponse_KeyspaceResult{{
				Keyspace: &vtadminpb.Keyspace{
					Keyspace: &vtctldatapb.Keyspace{Name: testKeyspace},
				},
				Events: []*logutilpb.Event{{
					Level: logutilpb.Level_ERROR,
					Value: "ReloadSchema(commerce) failed",
				}},
			}},
		},
	}
	s := newKeyspaceActionsTestServer(t, fake, false)

	rec := postShardForm(t, s, keyspaceActionBase+"/reload_schema", url.Values{})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.NotEqual(t, keyspaceActionBase, rec.Header().Get("Location"))
	assert.Contains(t, rec.Body.String(), "reload schema failed")
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
