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

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

type operationsFakeServer struct {
	fakeVTAdminServer

	getSchemaMigrationsRequest       *vtadminpb.GetSchemaMigrationsRequest
	getSchemaMigrationsError         error
	getUnresolvedTransactionsRequest *vtadminpb.GetUnresolvedTransactionsRequest
	getUnresolvedTransactionsError   error
	getUnresolvedTransactionsNil     bool
	getTransactionInfoRequest        *vtadminpb.GetTransactionInfoRequest
	getTransactionInfoError          error
	getTransactionInfoNil            bool
	getClustersError                 error
}

func (f *operationsFakeServer) GetSchemaMigrations(ctx context.Context, req *vtadminpb.GetSchemaMigrationsRequest) (*vtadminpb.GetSchemaMigrationsResponse, error) {
	f.getSchemaMigrationsRequest = req
	if f.getSchemaMigrationsError != nil {
		return nil, f.getSchemaMigrationsError
	}
	return &vtadminpb.GetSchemaMigrationsResponse{
		SchemaMigrations: []*vtadminpb.SchemaMigration{
			{
				Cluster: &vtadminpb.Cluster{Id: "local", Name: "Local"},
				SchemaMigration: &vtctldatapb.SchemaMigration{
					Uuid:     "mig-1",
					Keyspace: "commerce",
					Shard:    "0",
					Table:    "customer",
					Status:   vtctldatapb.SchemaMigration_COMPLETE,
				},
			},
		},
	}, nil
}

func (f *operationsFakeServer) GetClusters(ctx context.Context, req *vtadminpb.GetClustersRequest) (*vtadminpb.GetClustersResponse, error) {
	if f.getClustersError != nil {
		return nil, f.getClustersError
	}
	return &vtadminpb.GetClustersResponse{Clusters: []*vtadminpb.Cluster{{Id: "local", Name: "Local"}, {Id: "prod", Name: "Prod"}}}, nil
}

func (f *operationsFakeServer) GetKeyspaces(ctx context.Context, req *vtadminpb.GetKeyspacesRequest) (*vtadminpb.GetKeyspacesResponse, error) {
	return &vtadminpb.GetKeyspacesResponse{Keyspaces: []*vtadminpb.Keyspace{
		{Cluster: &vtadminpb.Cluster{Id: "local", Name: "Local"}, Keyspace: &vtctldatapb.Keyspace{Name: "commerce"}},
		{Cluster: &vtadminpb.Cluster{Id: "local", Name: "Local"}, Keyspace: &vtctldatapb.Keyspace{Name: "customer"}},
		{Cluster: &vtadminpb.Cluster{Id: "prod", Name: "Prod"}, Keyspace: &vtctldatapb.Keyspace{Name: "commerce_prod"}},
	}}, nil
}

func (f *operationsFakeServer) GetUnresolvedTransactions(ctx context.Context, req *vtadminpb.GetUnresolvedTransactionsRequest) (*vtctldatapb.GetUnresolvedTransactionsResponse, error) {
	f.getUnresolvedTransactionsRequest = req
	if f.getUnresolvedTransactionsError != nil {
		return nil, f.getUnresolvedTransactionsError
	}
	if f.getUnresolvedTransactionsNil {
		return nil, nil
	}
	return &vtctldatapb.GetUnresolvedTransactionsResponse{
		Transactions: []*querypb.TransactionMetadata{{Dtid: "dtid-1"}},
	}, nil
}

func (f *operationsFakeServer) GetTransactionInfo(ctx context.Context, req *vtadminpb.GetTransactionInfoRequest) (*vtctldatapb.GetTransactionInfoResponse, error) {
	f.getTransactionInfoRequest = req
	if f.getTransactionInfoError != nil {
		return nil, f.getTransactionInfoError
	}
	if f.getTransactionInfoNil {
		return nil, nil
	}
	return &vtctldatapb.GetTransactionInfoResponse{
		Metadata:    &querypb.TransactionMetadata{Dtid: "dtid-1"},
		ShardStates: []*vtctldatapb.ShardTransactionState{{Shard: "commerce/0", State: "PREPARE"}},
	}, nil
}

func TestMigrationsPagePassesFiltersAndRendersRows(t *testing.T) {
	fake := &operationsFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/migrations?cluster_id=local&cluster_id=prod&keyspace=commerce&uuid=mig-1", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	require.NotNil(t, fake.getSchemaMigrationsRequest)
	require.Len(t, fake.getSchemaMigrationsRequest.GetClusterRequests(), 2)
	clusterReq := fake.getSchemaMigrationsRequest.GetClusterRequests()[0]
	assert.Equal(t, "local", clusterReq.GetClusterId())
	require.NotNil(t, clusterReq.GetRequest())
	assert.Equal(t, "commerce", clusterReq.GetRequest().GetKeyspace())
	assert.Equal(t, "mig-1", clusterReq.GetRequest().GetUuid())
	clusterReq = fake.getSchemaMigrationsRequest.GetClusterRequests()[1]
	assert.Equal(t, "prod", clusterReq.GetClusterId())
	require.NotNil(t, clusterReq.GetRequest())
	assert.Equal(t, "commerce", clusterReq.GetRequest().GetKeyspace())
	assert.Equal(t, "mig-1", clusterReq.GetRequest().GetUuid())
	assert.Contains(t, rec.Body.String(), "Migrations")
	assert.Contains(t, rec.Body.String(), "mig-1")
	assert.Contains(t, rec.Body.String(), "commerce")
}

func TestMigrationsPageDefaultsClusterAndKeyspaceSelects(t *testing.T) {
	fake := &operationsFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/migrations", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), `<select name="cluster_id" required>`)
	assert.Contains(t, rec.Body.String(), `<option value="local" selected>Local (local)</option>`)
	assert.Contains(t, rec.Body.String(), `<select name="keyspace" required>`)
	assert.Contains(t, rec.Body.String(), `<option value="commerce" selected>commerce</option>`)
	assert.Nil(t, fake.getSchemaMigrationsRequest)
}

func TestMigrationsPageRejectsEmptyClusterID(t *testing.T) {
	fake := &operationsFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/migrations?cluster_id=&keyspace=commerce", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "cluster_id")
	assert.Nil(t, fake.getSchemaMigrationsRequest)
}

func TestMigrationsPageRequiresClusterID(t *testing.T) {
	fake := &operationsFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/migrations?keyspace=commerce", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "cluster_id")
	assert.Nil(t, fake.getSchemaMigrationsRequest)
}

func TestMigrationsPageValidatesSubmissionBeforeLoadingFormOptions(t *testing.T) {
	fake := &operationsFakeServer{getClustersError: errors.New("cluster options failed")}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/migrations?keyspace=commerce", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "cluster_id")
	assert.NotContains(t, rec.Body.String(), "cluster options failed")
	assert.Nil(t, fake.getSchemaMigrationsRequest)
}

func TestMigrationsPageRequiresKeyspace(t *testing.T) {
	fake := &operationsFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/migrations?cluster_id=local", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "keyspace")
	assert.Nil(t, fake.getSchemaMigrationsRequest)
}

func TestMigrationsPageRendersBackendError(t *testing.T) {
	fake := &operationsFakeServer{getSchemaMigrationsError: errors.New("migrations backend failed")}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/migrations?cluster_id=local&keyspace=commerce", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.Contains(t, rec.Body.String(), "migrations backend failed")
	require.NotNil(t, fake.getSchemaMigrationsRequest)
}

func TestTransactionsPagePassesFiltersAndRendersRows(t *testing.T) {
	fake := &operationsFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/transactions?cluster_id=local&keyspace=commerce&abandon_age=30", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	require.NotNil(t, fake.getUnresolvedTransactionsRequest)
	assert.Equal(t, "local", fake.getUnresolvedTransactionsRequest.GetClusterId())
	assert.Equal(t, "commerce", fake.getUnresolvedTransactionsRequest.GetKeyspace())
	assert.EqualValues(t, 30, fake.getUnresolvedTransactionsRequest.GetAbandonAge())
	assert.Contains(t, rec.Body.String(), "Transactions")
	assert.Contains(t, rec.Body.String(), "dtid-1")
	assert.Contains(t, rec.Body.String(), "/transaction/local/dtid-1/info")
}

func TestTransactionsPageDefaultsClusterAndKeyspaceSelects(t *testing.T) {
	fake := &operationsFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/transactions", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), `<select name="cluster_id" required>`)
	assert.Contains(t, rec.Body.String(), `<option value="local" selected>Local (local)</option>`)
	assert.Contains(t, rec.Body.String(), `<select name="keyspace" required>`)
	assert.Contains(t, rec.Body.String(), `<option value="commerce" selected>commerce</option>`)
	assert.Nil(t, fake.getUnresolvedTransactionsRequest)
}

func TestTransactionsPageRequiresKeyspace(t *testing.T) {
	fake := &operationsFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/transactions?cluster_id=local", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "keyspace")
	assert.Nil(t, fake.getUnresolvedTransactionsRequest)
}

func TestTransactionsPageRequiresClusterID(t *testing.T) {
	fake := &operationsFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/transactions?keyspace=commerce", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "cluster_id")
	assert.Nil(t, fake.getUnresolvedTransactionsRequest)
}

func TestTransactionsPageValidatesSubmissionBeforeLoadingFormOptions(t *testing.T) {
	fake := &operationsFakeServer{getClustersError: errors.New("cluster options failed")}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/transactions?keyspace=commerce", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "cluster_id")
	assert.NotContains(t, rec.Body.String(), "cluster options failed")
	assert.Nil(t, fake.getUnresolvedTransactionsRequest)
}

func TestTransactionsPageRendersBackendError(t *testing.T) {
	fake := &operationsFakeServer{getUnresolvedTransactionsError: errors.New("transactions backend failed")}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/transactions?cluster_id=local&keyspace=commerce", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.Contains(t, rec.Body.String(), "transactions backend failed")
	require.NotNil(t, fake.getUnresolvedTransactionsRequest)
}

func TestTransactionsPageReturnsNotFoundForNilResponse(t *testing.T) {
	fake := &operationsFakeServer{getUnresolvedTransactionsNil: true}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/transactions?cluster_id=local&keyspace=commerce", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
	require.NotNil(t, fake.getUnresolvedTransactionsRequest)
}

func TestTransactionsPageRejectsInvalidAbandonAge(t *testing.T) {
	fake := &operationsFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/transactions?cluster_id=local&keyspace=commerce&abandon_age=bad", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "abandon_age")
	assert.Nil(t, fake.getUnresolvedTransactionsRequest)
}

func TestTransactionInfoPageUsesRouteValuesAndRendersDetail(t *testing.T) {
	fake := &operationsFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/transaction/local/dtid-1/info", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	require.NotNil(t, fake.getTransactionInfoRequest)
	assert.Equal(t, "local", fake.getTransactionInfoRequest.GetClusterId())
	require.NotNil(t, fake.getTransactionInfoRequest.GetRequest())
	assert.Equal(t, "dtid-1", fake.getTransactionInfoRequest.GetRequest().GetDtid())
	assert.Contains(t, rec.Body.String(), "Transaction")
	assert.Contains(t, rec.Body.String(), "dtid-1")
	assert.Contains(t, rec.Body.String(), "commerce/0")
}

func TestTransactionInfoPageRendersBackendError(t *testing.T) {
	fake := &operationsFakeServer{getTransactionInfoError: errors.New("transaction info backend failed")}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/transaction/local/dtid-1/info", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.Contains(t, rec.Body.String(), "transaction info backend failed")
	require.NotNil(t, fake.getTransactionInfoRequest)
}

func TestTransactionInfoPageReturnsNotFoundForNilResponse(t *testing.T) {
	fake := &operationsFakeServer{getTransactionInfoNil: true}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/transaction/local/dtid-1/info", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
	require.NotNil(t, fake.getTransactionInfoRequest)
}
