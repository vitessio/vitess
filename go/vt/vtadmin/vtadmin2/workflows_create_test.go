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
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

const createMoveTablesPath = "/workflows/movetables/create"

type workflowCreateFakeServer struct {
	fakeVTAdminServer
	moveTablesCreateReq  *vtadminpb.MoveTablesCreateRequest
	moveTablesErr        error
	reshardCreateReq     *vtadminpb.ReshardCreateRequest
	reshardErr           error
	materializeCreateReq *vtadminpb.MaterializeCreateRequest
	materializeErr       error
	applySchemaReq       *vtadminpb.ApplySchemaRequest
	applySchemaErr       error
}

func (f *workflowCreateFakeServer) GetClusters(ctx context.Context, req *vtadminpb.GetClustersRequest) (*vtadminpb.GetClustersResponse, error) {
	return &vtadminpb.GetClustersResponse{Clusters: []*vtadminpb.Cluster{
		{Id: testClusterID, Name: "Local"},
	}}, nil
}

func (f *workflowCreateFakeServer) GetKeyspaces(ctx context.Context, req *vtadminpb.GetKeyspacesRequest) (*vtadminpb.GetKeyspacesResponse, error) {
	ks := func(name string) *vtadminpb.Keyspace {
		return &vtadminpb.Keyspace{
			Cluster:  &vtadminpb.Cluster{Id: testClusterID},
			Keyspace: &vtctldatapb.Keyspace{Name: name},
		}
	}
	return &vtadminpb.GetKeyspacesResponse{Keyspaces: []*vtadminpb.Keyspace{
		ks("commerce"), ks("sales"),
	}}, nil
}

func (f *workflowCreateFakeServer) GetSchemas(ctx context.Context, req *vtadminpb.GetSchemasRequest) (*vtadminpb.GetSchemasResponse, error) {
	return &vtadminpb.GetSchemasResponse{Schemas: []*vtadminpb.Schema{
		{
			Cluster:  &vtadminpb.Cluster{Id: testClusterID},
			Keyspace: "commerce",
			TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
				{Name: "users"},
				{Name: "orders"},
			},
		},
		{
			Cluster:  &vtadminpb.Cluster{Id: testClusterID},
			Keyspace: "sales",
			TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
				{Name: "users"},
				{Name: "orders"},
			},
		},
	}}, nil
}

func (f *workflowCreateFakeServer) GetWorkflows(ctx context.Context, req *vtadminpb.GetWorkflowsRequest) (*vtadminpb.GetWorkflowsResponse, error) {
	return &vtadminpb.GetWorkflowsResponse{WorkflowsByCluster: map[string]*vtadminpb.ClusterWorkflows{}}, nil
}

func (f *workflowCreateFakeServer) MoveTablesCreate(ctx context.Context, req *vtadminpb.MoveTablesCreateRequest) (*vtctldatapb.WorkflowStatusResponse, error) {
	f.moveTablesCreateReq = req
	if f.moveTablesErr != nil {
		return nil, f.moveTablesErr
	}
	return &vtctldatapb.WorkflowStatusResponse{}, nil
}

func newWorkflowCreateTestServer(t *testing.T, fake *workflowCreateFakeServer, readOnly bool) *Server {
	t.Helper()
	s, err := NewServer(fake, Options{ReadOnly: readOnly})
	require.NoError(t, err)
	return s
}

func TestCreateMoveTablesFormRendersOptions(t *testing.T) {
	fake := &workflowCreateFakeServer{}
	s := newWorkflowCreateTestServer(t, fake, false)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, createMoveTablesPath+"?cluster_id=local&source_keyspace=commerce", nil)
	s.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()

	assert.Contains(t, body, `name="workflow"`)
	assert.Contains(t, body, `value="local"`)              // cluster option
	assert.Contains(t, body, `value="commerce"`)           // source keyspace option
	assert.Contains(t, body, `value="sales"`)              // target keyspace option
	assert.Contains(t, body, `name="table" value="users"`) // source tables to pick
	assert.Contains(t, body, `name="table" value="orders"`)
	assert.Contains(t, body, `name="all_tables"`)
	assert.Contains(t, body, `name="on_ddl"`)
	assert.Contains(t, body, `name="tablet_type" value="REPLICA"`)
	assert.Contains(t, body, `name="tablet_type" value="PRIMARY"`)
	assert.Contains(t, body, `name="auto_start"`)
	assert.Contains(t, body, `name="csrf_token"`)
	assert.Contains(t, body, "IGNORE") // on_ddl default option

	// CSRF cookie must be minted so the form can post.
	assert.NotNil(t, findCookie(rec, csrfCookieName))
}

func TestCreateMoveTablesFormReadOnly(t *testing.T) {
	fake := &workflowCreateFakeServer{}
	s := newWorkflowCreateTestServer(t, fake, true)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, createMoveTablesPath, nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusForbidden, rec.Code)
	assert.Nil(t, fake.moveTablesCreateReq)
}

func TestCreateMoveTablesPostRedirectsToWorkflowDetail(t *testing.T) {
	fake := &workflowCreateFakeServer{}
	s := newWorkflowCreateTestServer(t, fake, false)

	form := url.Values{
		"cluster_id":           {testClusterID},
		"workflow":             {"users_to_sales"},
		"source_keyspace":      {testKeyspace},
		"target_keyspace":      {"sales"},
		"table":                {"users", "orders"},
		"cells":                {"zone1, zone2"},
		"on_ddl":               {"EXEC"},
		"tablet_type":          {"REPLICA", "PRIMARY"},
		"source_time_zone":     {"UTC"},
		"auto_start":           {"on"},
		"defer_secondary_keys": {"on"},
	}
	rec := postShardForm(t, s, createMoveTablesPath, form)

	assert.Equal(t, http.StatusSeeOther, rec.Code)
	assert.Equal(t, "/workflow/local/sales/users_to_sales", rec.Header().Get("Location"))

	req := fake.moveTablesCreateReq
	require.NotNil(t, req)
	assert.Equal(t, testClusterID, req.ClusterId)

	inner := req.GetRequest()
	assert.Equal(t, "users_to_sales", inner.Workflow)
	assert.Equal(t, testKeyspace, inner.SourceKeyspace)
	assert.Equal(t, "sales", inner.TargetKeyspace)
	assert.ElementsMatch(t, []string{"users", "orders"}, inner.IncludeTables)
	assert.False(t, inner.AllTables)
	assert.Equal(t, []string{"zone1", "zone2"}, inner.Cells)
	assert.Equal(t, []topodatapb.TabletType{topodatapb.TabletType_REPLICA, topodatapb.TabletType_PRIMARY}, inner.TabletTypes)
	assert.Equal(t, "EXEC", inner.OnDdl)
	assert.Equal(t, "UTC", inner.SourceTimeZone)
	assert.True(t, inner.AutoStart)
	assert.True(t, inner.DeferSecondaryKeys)
}

func TestCreateMoveTablesPostAllTables(t *testing.T) {
	fake := &workflowCreateFakeServer{}
	s := newWorkflowCreateTestServer(t, fake, false)

	form := url.Values{
		"cluster_id":      {testClusterID},
		"workflow":        {"full_copy"},
		"source_keyspace": {testKeyspace},
		"target_keyspace": {"sales"},
		"all_tables":      {"on"},
	}
	rec := postShardForm(t, s, createMoveTablesPath, form)

	assert.Equal(t, http.StatusSeeOther, rec.Code)
	inner := fake.moveTablesCreateReq.GetRequest()
	require.NotNil(t, inner)
	assert.True(t, inner.AllTables)
	assert.Empty(t, inner.IncludeTables)
}

func TestCreateMoveTablesPostDefaults(t *testing.T) {
	fake := &workflowCreateFakeServer{}
	s := newWorkflowCreateTestServer(t, fake, false)

	form := url.Values{
		"cluster_id":      {testClusterID},
		"workflow":        {"minimal"},
		"source_keyspace": {testKeyspace},
		"target_keyspace": {"sales"},
		"table":           {"users"},
	}
	rec := postShardForm(t, s, createMoveTablesPath, form)

	assert.Equal(t, http.StatusSeeOther, rec.Code)
	inner := fake.moveTablesCreateReq.GetRequest()
	require.NotNil(t, inner)
	// SPA default when nothing else is selected.
	assert.Equal(t, "IGNORE", inner.OnDdl)
	assert.Empty(t, inner.Cells)
	assert.Empty(t, inner.TabletTypes)
	assert.False(t, inner.AutoStart) // not submitted: zero value, not assumed
}

func TestCreateMoveTablesPostValidation(t *testing.T) {
	valid := url.Values{
		"cluster_id":      {testClusterID},
		"workflow":        {"w"},
		"source_keyspace": {testKeyspace},
		"target_keyspace": {"sales"},
		"table":           {"users"},
	}

	cases := map[string]func(f url.Values){
		"missing workflow":     func(f url.Values) { f.Del("workflow") },
		"missing source":       func(f url.Values) { f.Del("source_keyspace") },
		"missing target":       func(f url.Values) { f.Del("target_keyspace") },
		"source equals target": func(f url.Values) { f.Set("target_keyspace", testKeyspace) },
		"no tables and no all": func(f url.Values) { f.Del("table") },
		"invalid tablet type":  func(f url.Values) { f.Set("tablet_type", "BOGUS") },
	}

	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			fake := &workflowCreateFakeServer{}
			s := newWorkflowCreateTestServer(t, fake, false)

			form := url.Values{}
			maps.Copy(form, valid)
			mutate(form)

			rec := postShardForm(t, s, createMoveTablesPath, form)

			assert.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Nil(t, fake.moveTablesCreateReq)
		})
	}
}

func TestCreateMoveTablesPostReadOnly(t *testing.T) {
	fake := &workflowCreateFakeServer{}
	s := newWorkflowCreateTestServer(t, fake, true)

	form := url.Values{
		"cluster_id":      {testClusterID},
		"workflow":        {"w"},
		"source_keyspace": {testKeyspace},
		"target_keyspace": {"sales"},
		"table":           {"users"},
	}
	rec := postShardForm(t, s, createMoveTablesPath, form)

	assert.Equal(t, http.StatusForbidden, rec.Code)
	assert.Nil(t, fake.moveTablesCreateReq)
}

func TestCreateMoveTablesPostInvalidCSRF(t *testing.T) {
	fake := &workflowCreateFakeServer{}
	s := newWorkflowCreateTestServer(t, fake, false)

	form := url.Values{
		"cluster_id":      {testClusterID},
		"workflow":        {"w"},
		"source_keyspace": {testKeyspace},
		"target_keyspace": {"sales"},
		"table":           {"users"},
		"csrf_token":      {"wrong"},
	}
	req := httptest.NewRequest(http.MethodPost, createMoveTablesPath, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{Name: csrfCookieName, Value: testCSRF})
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusForbidden, rec.Code)
	assert.Nil(t, fake.moveTablesCreateReq)
}

func (f *workflowCreateFakeServer) ReshardCreate(ctx context.Context, req *vtadminpb.ReshardCreateRequest) (*vtctldatapb.WorkflowStatusResponse, error) {
	f.reshardCreateReq = req
	if f.reshardErr != nil {
		return nil, f.reshardErr
	}
	return &vtctldatapb.WorkflowStatusResponse{}, nil
}

func TestWorkflowsListLinksToCreateMoveTables(t *testing.T) {
	fake := &workflowCreateFakeServer{}
	s := newWorkflowCreateTestServer(t, fake, false)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/workflows", nil)
	s.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), `href="`+createMoveTablesPath+`"`)
}

func (f *workflowCreateFakeServer) MaterializeCreate(ctx context.Context, req *vtadminpb.MaterializeCreateRequest) (*vtctldatapb.MaterializeCreateResponse, error) {
	f.materializeCreateReq = req
	if f.materializeErr != nil {
		return nil, f.materializeErr
	}
	return &vtctldatapb.MaterializeCreateResponse{}, nil
}

const createMaterializePath = "/workflows/materialize/create"

func TestCreateMaterializeFormRendersOptions(t *testing.T) {
	fake := &workflowCreateFakeServer{}
	s := newWorkflowCreateTestServer(t, fake, false)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, createMaterializePath+"?cluster_id=local&target_keyspace=sales", nil)
	s.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()

	assert.Contains(t, body, `name="workflow"`)
	assert.Contains(t, body, `name="source_keyspace"`)
	assert.Contains(t, body, `name="target_keyspace"`)
	assert.Contains(t, body, `name="table_settings"`)
	assert.Contains(t, body, `name="cell"`)
	assert.Contains(t, body, `name="tablet_type"`)
	assert.Contains(t, body, `name="reference_table" value="users"`)
	assert.Contains(t, body, `name="reference_table" value="orders"`)
	assert.Contains(t, body, `name="csrf_token"`)
	assert.NotNil(t, findCookie(rec, csrfCookieName))
}

func TestCreateMaterializeFormReadOnly(t *testing.T) {
	fake := &workflowCreateFakeServer{}
	s := newWorkflowCreateTestServer(t, fake, true)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, createMaterializePath, nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusForbidden, rec.Code)
	assert.Nil(t, fake.materializeCreateReq)
}

func TestCreateMaterializePostRedirectsToWorkflowDetail(t *testing.T) {
	fake := &workflowCreateFakeServer{}
	s := newWorkflowCreateTestServer(t, fake, false)

	form := url.Values{
		"cluster_id":                  {testClusterID},
		"workflow":                    {"sales_summary"},
		"source_keyspace":             {testKeyspace},
		"target_keyspace":             {"sales"},
		"table_settings":              {`{"target_table":"sales_summary"}`},
		"reference_table":             {"users"},
		"cell":                        {"zone1, zone2"},
		"tablet_type":                 {"REPLICA", "PRIMARY"},
		"tablet_selection_preference": {"on"},
		"stop_after_copy":             {"on"},
	}
	rec := postShardForm(t, s, createMaterializePath, form)

	assert.Equal(t, http.StatusSeeOther, rec.Code)
	assert.Equal(t, "/workflow/local/sales/sales_summary", rec.Header().Get("Location"))

	req := fake.materializeCreateReq
	require.NotNil(t, req)
	assert.Equal(t, testClusterID, req.ClusterId)
	assert.Equal(t, `{"target_table":"sales_summary"}`, req.TableSettings)

	settings := req.GetRequest().GetSettings()
	assert.Equal(t, "sales_summary", settings.Workflow)
	assert.Equal(t, testKeyspace, settings.SourceKeyspace)
	assert.Equal(t, "sales", settings.TargetKeyspace)
	assert.Equal(t, []string{"users"}, settings.ReferenceTables)
	assert.Equal(t, "zone1,zone2", settings.Cell)
	assert.Equal(t, "REPLICA,PRIMARY", settings.TabletTypes)
	assert.True(t, settings.StopAfterCopy)
	assert.Equal(t, tabletmanagerdatapb.TabletSelectionPreference_INORDER, settings.TabletSelectionPreference)
	assert.Equal(t, vtctldatapb.MaterializationIntent_CUSTOM, settings.MaterializationIntent)
}

func TestCreateMaterializePostDefaults(t *testing.T) {
	fake := &workflowCreateFakeServer{}
	s := newWorkflowCreateTestServer(t, fake, false)

	form := url.Values{
		"cluster_id":      {testClusterID},
		"workflow":        {"minimal"},
		"source_keyspace": {testKeyspace},
		"target_keyspace": {"sales"},
		"table_settings":  {"{}"},
	}
	rec := postShardForm(t, s, createMaterializePath, form)

	assert.Equal(t, http.StatusSeeOther, rec.Code)
	settings := fake.materializeCreateReq.GetRequest().GetSettings()
	assert.Empty(t, settings.Cell)
	assert.Empty(t, settings.TabletTypes)
	assert.Equal(t, tabletmanagerdatapb.TabletSelectionPreference_ANY, settings.TabletSelectionPreference)
	assert.False(t, settings.StopAfterCopy)
}

func TestCreateMaterializePostValidation(t *testing.T) {
	valid := url.Values{
		"cluster_id":      {testClusterID},
		"workflow":        {"w"},
		"source_keyspace": {testKeyspace},
		"target_keyspace": {"sales"},
		"table_settings":  {"{}"},
	}

	cases := map[string]func(f url.Values){
		"missing workflow":       func(f url.Values) { f.Del("workflow") },
		"missing source":         func(f url.Values) { f.Del("source_keyspace") },
		"missing target":         func(f url.Values) { f.Del("target_keyspace") },
		"missing table settings": func(f url.Values) { f.Del("table_settings") },
		"invalid tablet type":    func(f url.Values) { f.Set("tablet_type", "BOGUS") },
	}

	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			fake := &workflowCreateFakeServer{}
			s := newWorkflowCreateTestServer(t, fake, false)

			form := url.Values{}
			maps.Copy(form, valid)
			mutate(form)

			rec := postShardForm(t, s, createMaterializePath, form)

			assert.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Nil(t, fake.materializeCreateReq)
		})
	}
}

func (f *workflowCreateFakeServer) ApplySchema(ctx context.Context, req *vtadminpb.ApplySchemaRequest) (*vtctldatapb.ApplySchemaResponse, error) {
	f.applySchemaReq = req
	if f.applySchemaErr != nil {
		return nil, f.applySchemaErr
	}
	return &vtctldatapb.ApplySchemaResponse{UuidList: []string{"0b1c2d3e"}}, nil
}

const createMigrationPath = "/migrations/create"

func TestCreateMigrationFormRendersOptions(t *testing.T) {
	fake := &workflowCreateFakeServer{}
	s := newWorkflowCreateTestServer(t, fake, false)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, createMigrationPath+"?cluster_id=local&keyspace=commerce", nil)
	s.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()

	assert.Contains(t, body, `name="keyspace"`)
	assert.Contains(t, body, `name="sql"`)
	assert.Contains(t, body, `name="ddl_strategy"`)
	assert.Contains(t, body, `vitess`)
	assert.Contains(t, body, `name="batch_size"`)
	assert.Contains(t, body, `name="caller_id"`)
	assert.Contains(t, body, `name="migration_context"`)
	assert.Contains(t, body, `name="uuid_list"`)
	assert.Contains(t, body, `name="csrf_token"`)
	assert.NotNil(t, findCookie(rec, csrfCookieName))
}

func TestCreateMigrationFormReadOnly(t *testing.T) {
	fake := &workflowCreateFakeServer{}
	s := newWorkflowCreateTestServer(t, fake, true)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, createMigrationPath, nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusForbidden, rec.Code)
	assert.Nil(t, fake.applySchemaReq)
}

func TestCreateMigrationPostRedirectsToMigrations(t *testing.T) {
	fake := &workflowCreateFakeServer{}
	s := newWorkflowCreateTestServer(t, fake, false)

	form := url.Values{
		"cluster_id":        {testClusterID},
		"keyspace":          {testKeyspace},
		"sql":               {"ALTER TABLE users ADD COLUMN name varchar(128);\nALTER TABLE orders ADD COLUMN note varchar(255);"},
		"ddl_strategy":      {"vitess"},
		"batch_size":        {"10"},
		"caller_id":         {"admin"},
		"migration_context": {"add-name-columns"},
		"uuid_list":         {"uuid-1, uuid-2"},
	}
	rec := postShardForm(t, s, createMigrationPath, form)

	assert.Equal(t, http.StatusSeeOther, rec.Code)
	assert.Equal(t, "/migrations?keyspace=commerce&cluster_id=local", rec.Header().Get("Location"))

	req := fake.applySchemaReq
	require.NotNil(t, req)
	assert.Equal(t, testClusterID, req.ClusterId)
	assert.Equal(t, "admin", req.CallerId)
	// Raw multi-statement SQL is passed through; the vtadmin API layer splits it.
	assert.Equal(t, "ALTER TABLE users ADD COLUMN name varchar(128);\nALTER TABLE orders ADD COLUMN note varchar(255);", req.Sql)
	assert.Empty(t, req.GetRequest().Sql)

	inner := req.GetRequest()
	assert.Equal(t, testKeyspace, inner.Keyspace)
	assert.Equal(t, "vitess", inner.DdlStrategy)
	assert.Equal(t, int64(10), inner.BatchSize)
	assert.Equal(t, "add-name-columns", inner.MigrationContext)
	assert.Equal(t, []string{"uuid-1", "uuid-2"}, inner.UuidList)
}

func TestCreateMigrationPostValidation(t *testing.T) {
	valid := url.Values{
		"cluster_id":   {testClusterID},
		"keyspace":     {testKeyspace},
		"sql":          {"ALTER TABLE users ADD COLUMN name varchar(128);"},
		"ddl_strategy": {"vitess"},
	}

	cases := map[string]func(f url.Values){
		"missing keyspace": func(f url.Values) { f.Del("keyspace") },
		"missing sql":      func(f url.Values) { f.Del("sql") },
		"invalid batch":    func(f url.Values) { f.Set("batch_size", "ten") },
	}

	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			fake := &workflowCreateFakeServer{}
			s := newWorkflowCreateTestServer(t, fake, false)

			form := url.Values{}
			maps.Copy(form, valid)
			mutate(form)

			rec := postShardForm(t, s, createMigrationPath, form)

			assert.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Nil(t, fake.applySchemaReq)
		})
	}
}

func TestCreateMigrationPostReadOnly(t *testing.T) {
	fake := &workflowCreateFakeServer{}
	s := newWorkflowCreateTestServer(t, fake, true)

	form := url.Values{
		"cluster_id":   {testClusterID},
		"keyspace":     {testKeyspace},
		"sql":          {"ALTER TABLE users ADD COLUMN name varchar(128);"},
		"ddl_strategy": {"vitess"},
	}
	rec := postShardForm(t, s, createMigrationPath, form)

	assert.Equal(t, http.StatusForbidden, rec.Code)
	assert.Nil(t, fake.applySchemaReq)
}

func TestWorkflowsListLinksToCreateReshard(t *testing.T) {
	fake := &workflowCreateFakeServer{}
	s := newWorkflowCreateTestServer(t, fake, false)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/workflows", nil)
	s.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), `href="`+createReshardPath+`"`)
}

func TestCreateReshardFormMultiClusterPicksClusterFirst(t *testing.T) {
	fake := &multiClusterFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, createReshardPath, nil)
	s.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	// No form until a cluster is picked explicitly.
	assert.NotContains(t, rec.Body.String(), `name="workflow"`)
	assert.Contains(t, rec.Body.String(), "/workflows/reshard/create?cluster_id=local")
	assert.Contains(t, rec.Body.String(), "/workflows/reshard/create?cluster_id=prod")
}

type multiClusterFakeServer struct {
	workflowCreateFakeServer
}

func (f *multiClusterFakeServer) GetClusters(ctx context.Context, req *vtadminpb.GetClustersRequest) (*vtadminpb.GetClustersResponse, error) {
	return &vtadminpb.GetClustersResponse{Clusters: []*vtadminpb.Cluster{
		{Id: "local", Name: "Local"},
		{Id: "prod", Name: "Production"},
	}}, nil
}

func (f *multiClusterFakeServer) GetKeyspaces(ctx context.Context, req *vtadminpb.GetKeyspacesRequest) (*vtadminpb.GetKeyspacesResponse, error) {
	return &vtadminpb.GetKeyspacesResponse{Keyspaces: []*vtadminpb.Keyspace{}}, nil
}

const createReshardPath = "/workflows/reshard/create"

func TestCreateReshardFormRendersOptions(t *testing.T) {
	fake := &workflowCreateFakeServer{}
	s := newWorkflowCreateTestServer(t, fake, false)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, createReshardPath, nil)
	s.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()

	assert.Contains(t, body, `name="workflow"`)
	assert.Contains(t, body, `name="keyspace"`)
	assert.Contains(t, body, `name="source_shards"`)
	assert.Contains(t, body, `name="target_shards"`)
	assert.Contains(t, body, `name="cells"`)
	assert.Contains(t, body, `name="tablet_type" value="REPLICA"`)
	assert.Contains(t, body, `name="on_ddl"`)
	assert.Contains(t, body, `name="tablet_selection_preference"`)
	assert.Contains(t, body, `name="skip_schema_copy"`)
	assert.Contains(t, body, `name="stop_after_copy"`)
	assert.Contains(t, body, `name="defer_secondary_keys"`)
	assert.Contains(t, body, `name="auto_start"`)
	assert.Contains(t, body, `name="csrf_token"`)
	assert.NotNil(t, findCookie(rec, csrfCookieName))
}

func TestCreateReshardFormReadOnly(t *testing.T) {
	fake := &workflowCreateFakeServer{}
	s := newWorkflowCreateTestServer(t, fake, true)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, createReshardPath, nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusForbidden, rec.Code)
	assert.Nil(t, fake.reshardCreateReq)
}

func TestCreateReshardPostRedirectsToWorkflowDetail(t *testing.T) {
	fake := &workflowCreateFakeServer{}
	s := newWorkflowCreateTestServer(t, fake, false)

	form := url.Values{
		"cluster_id":                  {testClusterID},
		"workflow":                    {"reshard_zero"},
		"keyspace":                    {testKeyspace},
		"source_shards":               {"0"},
		"target_shards":               {"-80, 80-"},
		"cells":                       {"zone1, zone2"},
		"tablet_type":                 {"REPLICA", "PRIMARY"},
		"tablet_selection_preference": {"on"},
		"on_ddl":                      {"STOP"},
		"skip_schema_copy":            {"on"},
		"stop_after_copy":             {"on"},
		"defer_secondary_keys":        {"on"},
		"auto_start":                  {"on"},
	}
	rec := postShardForm(t, s, createReshardPath, form)

	assert.Equal(t, http.StatusSeeOther, rec.Code)
	assert.Equal(t, "/workflow/local/"+testKeyspace+"/reshard_zero", rec.Header().Get("Location"))

	req := fake.reshardCreateReq
	require.NotNil(t, req)
	assert.Equal(t, testClusterID, req.ClusterId)

	inner := req.GetRequest()
	assert.Equal(t, "reshard_zero", inner.Workflow)
	assert.Equal(t, testKeyspace, inner.Keyspace)
	assert.Equal(t, []string{"0"}, inner.SourceShards)
	assert.Equal(t, []string{"-80", "80-"}, inner.TargetShards)
	assert.Equal(t, []string{"zone1", "zone2"}, inner.Cells)
	assert.Equal(t, []topodatapb.TabletType{topodatapb.TabletType_REPLICA, topodatapb.TabletType_PRIMARY}, inner.TabletTypes)
	assert.Equal(t, tabletmanagerdatapb.TabletSelectionPreference_INORDER, inner.TabletSelectionPreference)
	assert.Equal(t, "STOP", inner.OnDdl)
	assert.True(t, inner.SkipSchemaCopy)
	assert.True(t, inner.StopAfterCopy)
	assert.True(t, inner.DeferSecondaryKeys)
	assert.True(t, inner.AutoStart)
}

func TestCreateReshardPostDefaults(t *testing.T) {
	fake := &workflowCreateFakeServer{}
	s := newWorkflowCreateTestServer(t, fake, false)

	form := url.Values{
		"cluster_id":    {testClusterID},
		"workflow":      {"minimal"},
		"keyspace":      {testKeyspace},
		"source_shards": {"-"},
		"target_shards": {"-80,80-"},
	}
	rec := postShardForm(t, s, createReshardPath, form)

	assert.Equal(t, http.StatusSeeOther, rec.Code)
	inner := fake.reshardCreateReq.GetRequest()
	require.NotNil(t, inner)
	assert.Equal(t, "IGNORE", inner.OnDdl)
	// Absent checkbox: zero value (ANY), not assumed to be INORDER.
	assert.Equal(t, tabletmanagerdatapb.TabletSelectionPreference_ANY, inner.TabletSelectionPreference)
	assert.False(t, inner.AutoStart)
}

func TestCreateReshardPostValidation(t *testing.T) {
	valid := url.Values{
		"cluster_id":    {testClusterID},
		"workflow":      {"w"},
		"keyspace":      {testKeyspace},
		"source_shards": {"0"},
		"target_shards": {"-80,80-"},
	}

	cases := map[string]func(f url.Values){
		"missing workflow":      func(f url.Values) { f.Del("workflow") },
		"missing keyspace":      func(f url.Values) { f.Del("keyspace") },
		"missing source shards": func(f url.Values) { f.Del("source_shards") },
		"missing target shards": func(f url.Values) { f.Del("target_shards") },
		"invalid tablet type":   func(f url.Values) { f.Set("tablet_type", "BOGUS") },
	}

	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			fake := &workflowCreateFakeServer{}
			s := newWorkflowCreateTestServer(t, fake, false)

			form := url.Values{}
			maps.Copy(form, valid)
			mutate(form)

			rec := postShardForm(t, s, createReshardPath, form)

			assert.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Nil(t, fake.reshardCreateReq)
		})
	}
}

func TestCreateReshardPostReadOnly(t *testing.T) {
	fake := &workflowCreateFakeServer{}
	s := newWorkflowCreateTestServer(t, fake, true)

	form := url.Values{
		"cluster_id":    {testClusterID},
		"workflow":      {"w"},
		"keyspace":      {testKeyspace},
		"source_shards": {"0"},
		"target_shards": {"-80,80-"},
	}
	rec := postShardForm(t, s, createReshardPath, form)

	assert.Equal(t, http.StatusForbidden, rec.Code)
	assert.Nil(t, fake.reshardCreateReq)
}
