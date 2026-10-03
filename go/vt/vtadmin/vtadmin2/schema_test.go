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

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

type schemaFakeServer struct {
	fakeVTAdminServer
	getSchemaReq  *vtadminpb.GetSchemaRequest
	getSchemaRes  *vtadminpb.Schema
	getSchemaErr  error
	getVSchemaReq *vtadminpb.GetVSchemaRequest
	getVSchemaRes *vtadminpb.VSchema
	getVSchemaErr error
	getSchemasReq *vtadminpb.GetSchemasRequest
}

func (f *schemaFakeServer) GetSchema(ctx context.Context, req *vtadminpb.GetSchemaRequest) (*vtadminpb.Schema, error) {
	f.getSchemaReq = req
	if f.getSchemaErr != nil {
		return nil, f.getSchemaErr
	}
	return f.getSchemaRes, nil
}

func (f *schemaFakeServer) GetSchemas(ctx context.Context, req *vtadminpb.GetSchemasRequest) (*vtadminpb.GetSchemasResponse, error) {
	f.getSchemasReq = req
	return &vtadminpb.GetSchemasResponse{Schemas: []*vtadminpb.Schema{f.getSchemaRes}}, nil
}

func (f *schemaFakeServer) GetVSchema(ctx context.Context, req *vtadminpb.GetVSchemaRequest) (*vtadminpb.VSchema, error) {
	f.getVSchemaReq = req
	if f.getVSchemaErr != nil {
		return nil, f.getVSchemaErr
	}
	return f.getVSchemaRes, nil
}

func newSchemaTestServer(t *testing.T, fake *schemaFakeServer) *Server {
	t.Helper()
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)
	return s
}

func getSchemaDetail(t *testing.T, s *Server, clusterID, keyspace, table string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/schema/"+clusterID+"/"+keyspace+"/"+table, nil)
	s.ServeHTTP(rec, req)
	return rec
}

func newSchemaFake() *schemaFakeServer {
	return &schemaFakeServer{
		getSchemaRes: &vtadminpb.Schema{
			Cluster:  &vtadminpb.Cluster{Id: testClusterID},
			Keyspace: testKeyspace,
			TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
				{
					Name:              "users",
					Schema:            "CREATE TABLE `users` (\n  `user_id` bigint NOT NULL,\n  PRIMARY KEY (`user_id`)\n)",
					Columns:           []string{"user_id", "email"},
					PrimaryKeyColumns: []string{"user_id"},
				},
				{Name: "orders", Schema: "CREATE TABLE `orders` ()"},
			},
		},
		getVSchemaRes: &vtadminpb.VSchema{
			Cluster: &vtadminpb.Cluster{Id: testClusterID},
			Name:    testKeyspace,
			VSchema: &vschemapb.Keyspace{
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"hash": {
						Type:   "hash",
						Params: map[string]string{"hash_order": "1"},
					},
					"lookup": {
						Type: "consistent_lookup_unique",
					},
				},
				Tables: map[string]*vschemapb.Table{
					"users": {
						ColumnVindexes: []*vschemapb.ColumnVindex{
							{Column: "user_id", Name: "hash"},
							{Columns: []string{"email"}, Name: "lookup"},
						},
					},
				},
			},
		},
	}
}

func TestSchemaDetailRendersDefinitionAndVindexes(t *testing.T) {
	fake := newSchemaFake()
	s := newSchemaTestServer(t, fake)

	rec := getSchemaDetail(t, s, testClusterID, testKeyspace, "users")

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()

	assert.Contains(t, body, "CREATE TABLE `users`")
	assert.Contains(t, body, "user_id")
	assert.Contains(t, body, "hash")
	assert.Contains(t, body, "hash_order")
	assert.Contains(t, body, "lookup")
	assert.Contains(t, body, "consistent_lookup_unique")
	assert.Contains(t, body, "Primary")
	assert.Contains(t, body, "/keyspace/"+testClusterID+"/"+testKeyspace)

	// Other tables' definitions must not appear.
	assert.NotContains(t, body, "CREATE TABLE `orders`")
}

func TestSchemaDetailRequestsSchemaAndVSchema(t *testing.T) {
	fake := newSchemaFake()
	s := newSchemaTestServer(t, fake)

	getSchemaDetail(t, s, testClusterID, testKeyspace, "users")

	require.NotNil(t, fake.getSchemaReq)
	assert.Equal(t, testClusterID, fake.getSchemaReq.ClusterId)
	assert.Equal(t, testKeyspace, fake.getSchemaReq.Keyspace)
	assert.Equal(t, "users", fake.getSchemaReq.Table)

	require.NotNil(t, fake.getVSchemaReq)
	assert.Equal(t, testClusterID, fake.getVSchemaReq.ClusterId)
	assert.Equal(t, testKeyspace, fake.getVSchemaReq.Keyspace)
}

func TestSchemaDetailWithoutVindexes(t *testing.T) {
	fake := newSchemaFake()
	fake.getVSchemaRes.VSchema = &vschemapb.Keyspace{Sharded: false}
	s := newSchemaTestServer(t, fake)

	rec := getSchemaDetail(t, s, testClusterID, testKeyspace, "users")

	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), "Vindexes")
	assert.Contains(t, rec.Body.String(), "CREATE TABLE `users`")
}

func TestSchemaDetailVSchemaErrorStillRenders(t *testing.T) {
	fake := newSchemaFake()
	fake.getVSchemaErr = vterrors.New(vtrpcpb.Code_INTERNAL, "vtctld unavailable")
	s := newSchemaTestServer(t, fake)

	rec := getSchemaDetail(t, s, testClusterID, testKeyspace, "users")

	// The table definition is the primary content; the page must still render
	// without the vindexes section when the VSchema cannot be fetched.
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "CREATE TABLE `users`")
	assert.NotContains(t, rec.Body.String(), "Vindexes")
}

func TestSchemaDetailTableNotFound(t *testing.T) {
	fake := newSchemaFake()
	s := newSchemaTestServer(t, fake)

	rec := getSchemaDetail(t, s, testClusterID, testKeyspace, "missing")

	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Contains(t, rec.Body.String(), "missing")
}

func TestSchemaDetailAPIError(t *testing.T) {
	fake := newSchemaFake()
	fake.getSchemaErr = vterrors.New(vtrpcpb.Code_NOT_FOUND, "no such keyspace")
	s := newSchemaTestServer(t, fake)

	rec := getSchemaDetail(t, s, testClusterID, "missing", "users")

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestSchemasListLinksToTableDetail(t *testing.T) {
	fake := newSchemaFake()
	s := newSchemaTestServer(t, fake)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/schemas", nil)
	s.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "/schema/"+testClusterID+"/"+testKeyspace+"/users")
	assert.Contains(t, body, "/schema/"+testClusterID+"/"+testKeyspace+"/orders")
	require.NotNil(t, fake.getSchemasReq)
	assert.Empty(t, fake.getSchemasReq.ClusterIds)
}
