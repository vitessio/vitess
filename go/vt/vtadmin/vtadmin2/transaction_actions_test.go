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

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

type transactionActionsFakeServer struct {
	fakeVTAdminServer
	concludeTransactionReq *vtadminpb.ConcludeTransactionRequest
}

func (f *transactionActionsFakeServer) GetUnresolvedTransactions(ctx context.Context, req *vtadminpb.GetUnresolvedTransactionsRequest) (*vtctldatapb.GetUnresolvedTransactionsResponse, error) {
	return &vtctldatapb.GetUnresolvedTransactionsResponse{
		Transactions: []*querypb.TransactionMetadata{
			{Dtid: "transaction-id-1"},
		},
	}, nil
}

func (f *transactionActionsFakeServer) GetClusters(ctx context.Context, req *vtadminpb.GetClustersRequest) (*vtadminpb.GetClustersResponse, error) {
	return &vtadminpb.GetClustersResponse{Clusters: []*vtadminpb.Cluster{
		{Id: testClusterID, Name: "Local"},
	}}, nil
}

func (f *transactionActionsFakeServer) GetKeyspaces(ctx context.Context, req *vtadminpb.GetKeyspacesRequest) (*vtadminpb.GetKeyspacesResponse, error) {
	return &vtadminpb.GetKeyspacesResponse{Keyspaces: []*vtadminpb.Keyspace{
		{
			Cluster:  &vtadminpb.Cluster{Id: testClusterID},
			Keyspace: &vtctldatapb.Keyspace{Name: "commerce"},
		},
	}}, nil
}

func (f *transactionActionsFakeServer) ConcludeTransaction(ctx context.Context, req *vtadminpb.ConcludeTransactionRequest) (*vtctldatapb.ConcludeTransactionResponse, error) {
	f.concludeTransactionReq = req
	return &vtctldatapb.ConcludeTransactionResponse{}, nil
}

func TestTransactionConclude(t *testing.T) {
	fake := &transactionActionsFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	form := url.Values{}
	rec := postShardForm(t, s, "/transaction/local/transaction-id-1/conclude", form)

	assert.Equal(t, http.StatusSeeOther, rec.Code)
	assert.Equal(t, "/transactions?cluster_id=local", rec.Header().Get("Location"))

	req := fake.concludeTransactionReq
	require.NotNil(t, req)
	assert.Equal(t, testClusterID, req.ClusterId)
	assert.Equal(t, "transaction-id-1", req.Dtid)
}

func TestTransactionConcludeReadOnly(t *testing.T) {
	fake := &transactionActionsFakeServer{}
	s, err := NewServer(fake, Options{ReadOnly: true})
	require.NoError(t, err)

	rec := postShardForm(t, s, "/transaction/local/transaction-id-1/conclude", url.Values{})

	assert.Equal(t, http.StatusForbidden, rec.Code)
	assert.Nil(t, fake.concludeTransactionReq)
}

func TestTransactionsListRendersConcludeAction(t *testing.T) {
	fake := &transactionActionsFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/transactions?cluster_id=local&keyspace=commerce&abandon_age=600", nil)
	s.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "transaction-id-1")
	assert.Contains(t, rec.Body.String(), "/transaction/local/transaction-id-1/conclude")
}
