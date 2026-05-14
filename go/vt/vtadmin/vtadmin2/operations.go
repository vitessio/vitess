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
	"net/http"
	"slices"
	"strconv"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

type (
	transactionsData struct {
		ClusterID    string
		Transactions []*querypb.TransactionMetadata
	}

	transactionInfoData struct {
		ClusterID string
		Dtid      string
		Response  *vtctldatapb.GetTransactionInfoResponse
	}
)

func (s *Server) schemaMigrations(w http.ResponseWriter, r *http.Request) {
	clusterIDs := queryValues(r, "cluster_id")
	if len(clusterIDs) == 0 {
		s.renderError(w, r, http.StatusBadRequest, "Migrations", vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "cluster_id query parameter is required"))
		return
	}
	if slices.Contains(clusterIDs, "") {
		s.renderError(w, r, http.StatusBadRequest, "Migrations", vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "cluster_id query parameter is required"))
		return
	}
	keyspace := queryValue(r, "keyspace")
	if keyspace == "" {
		s.renderError(w, r, http.StatusBadRequest, "Migrations", vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "keyspace query parameter is required"))
		return
	}
	clusterRequests := make([]*vtadminpb.GetSchemaMigrationsRequest_ClusterRequest, 0, len(clusterIDs))
	for _, clusterID := range clusterIDs {
		clusterRequests = append(clusterRequests, &vtadminpb.GetSchemaMigrationsRequest_ClusterRequest{
			ClusterId: clusterID,
			Request: &vtctldatapb.GetSchemaMigrationsRequest{
				Keyspace: keyspace,
				Uuid:     queryValue(r, "uuid"),
			},
		})
	}

	resp, err := s.api.GetSchemaMigrations(r.Context(), &vtadminpb.GetSchemaMigrationsRequest{
		ClusterRequests: clusterRequests,
	})
	if err != nil {
		s.renderError(w, r, http.StatusInternalServerError, "Migrations", err)
		return
	}

	s.render(w, r, http.StatusOK, "migrations.html", PageData{
		Title:  "Migrations",
		Active: "migrations",
		Data:   resp.GetSchemaMigrations(),
	})
}

func (s *Server) transactions(w http.ResponseWriter, r *http.Request) {
	clusterID := queryValue(r, "cluster_id")
	if clusterID == "" {
		s.renderError(w, r, http.StatusBadRequest, "Transactions", vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "cluster_id query parameter is required"))
		return
	}
	keyspace := queryValue(r, "keyspace")
	if keyspace == "" {
		s.renderError(w, r, http.StatusBadRequest, "Transactions", vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "keyspace query parameter is required"))
		return
	}
	abandonAge, err := parseQueryInt64(r, "abandon_age", 0)
	if err != nil {
		s.renderError(w, r, http.StatusBadRequest, "Transactions", err)
		return
	}

	resp, err := s.api.GetUnresolvedTransactions(r.Context(), &vtadminpb.GetUnresolvedTransactionsRequest{
		ClusterId:  clusterID,
		Keyspace:   keyspace,
		AbandonAge: abandonAge,
	})
	if err != nil {
		s.renderError(w, r, http.StatusInternalServerError, "Transactions", err)
		return
	}
	if resp == nil {
		http.NotFound(w, r)
		return
	}

	s.render(w, r, http.StatusOK, "transactions.html", PageData{
		Title:  "Transactions",
		Active: "transactions",
		Data: transactionsData{
			ClusterID:    clusterID,
			Transactions: resp.GetTransactions(),
		},
	})
}

func (s *Server) transactionInfo(w http.ResponseWriter, r *http.Request) {
	resp, err := s.api.GetTransactionInfo(r.Context(), &vtadminpb.GetTransactionInfoRequest{
		ClusterId: r.PathValue("cluster_id"),
		Request: &vtctldatapb.GetTransactionInfoRequest{
			Dtid: r.PathValue("dtid"),
		},
	})
	if err != nil {
		s.renderError(w, r, http.StatusInternalServerError, "Transaction", err)
		return
	}
	if resp == nil {
		http.NotFound(w, r)
		return
	}

	s.render(w, r, http.StatusOK, "transaction.html", PageData{
		Title:  "Transaction",
		Active: "transactions",
		Data: transactionInfoData{
			ClusterID: r.PathValue("cluster_id"),
			Dtid:      r.PathValue("dtid"),
			Response:  resp,
		},
	})
}

func parseQueryInt64(r *http.Request, name string, defaultVal int64) (int64, error) {
	param := queryValue(r, name)
	if param == "" {
		return defaultVal, nil
	}
	val, err := strconv.ParseInt(param, 10, 64)
	if err != nil {
		return defaultVal, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "could not parse query parameter %s (= %s) into int64 value", name, param)
	}
	return val, nil
}
