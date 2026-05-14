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

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

type (
	vtExplainData struct {
		Cluster  string
		Keyspace string
		SQL      string
		Response *vtadminpb.VTExplainResponse
	}

	vExplainData struct {
		ClusterID string
		Keyspace  string
		SQL       string
		Response  *vtadminpb.VExplainResponse
	}
)

func (s *Server) vtExplain(w http.ResponseWriter, r *http.Request) {
	clusterID := queryValue(r, "cluster_id")
	keyspace := queryValue(r, "keyspace")
	sql := queryValue(r, "sql")
	data := vtExplainData{Cluster: clusterID, Keyspace: keyspace, SQL: sql}
	if len(r.URL.Query()) == 0 {
		s.render(w, r, http.StatusOK, "vtexplain.html", PageData{Title: "VTExplain", Active: "vtexplain", Data: data})
		return
	}
	if clusterID == "" {
		s.renderError(w, r, http.StatusBadRequest, "VTExplain", vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "cluster_id query parameter is required"))
		return
	}
	if keyspace == "" {
		s.renderError(w, r, http.StatusBadRequest, "VTExplain", vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "keyspace query parameter is required"))
		return
	}
	if sql == "" {
		s.renderError(w, r, http.StatusBadRequest, "VTExplain", vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "sql query parameter is required"))
		return
	}

	resp, err := s.api.VTExplain(r.Context(), &vtadminpb.VTExplainRequest{
		Cluster:  clusterID,
		Keyspace: keyspace,
		Sql:      sql,
	})
	if err != nil {
		s.renderError(w, r, http.StatusInternalServerError, "VTExplain", err)
		return
	}
	if resp == nil {
		http.NotFound(w, r)
		return
	}

	data.Response = resp
	s.render(w, r, http.StatusOK, "vtexplain.html", PageData{Title: "VTExplain", Active: "vtexplain", Data: data})
}

func (s *Server) vExplain(w http.ResponseWriter, r *http.Request) {
	clusterID := queryValue(r, "cluster_id")
	keyspace := queryValue(r, "keyspace")
	sql := queryValue(r, "sql")
	data := vExplainData{ClusterID: clusterID, Keyspace: keyspace, SQL: sql}
	if len(r.URL.Query()) == 0 {
		s.render(w, r, http.StatusOK, "vexplain.html", PageData{Title: "VExplain", Active: "vexplain", Data: data})
		return
	}
	if clusterID == "" {
		s.renderError(w, r, http.StatusBadRequest, "VExplain", vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "cluster_id query parameter is required"))
		return
	}
	if keyspace == "" {
		s.renderError(w, r, http.StatusBadRequest, "VExplain", vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "keyspace query parameter is required"))
		return
	}
	if sql == "" {
		s.renderError(w, r, http.StatusBadRequest, "VExplain", vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "sql query parameter is required"))
		return
	}

	resp, err := s.api.VExplain(r.Context(), &vtadminpb.VExplainRequest{
		ClusterId: clusterID,
		Keyspace:  keyspace,
		Sql:       sql,
	})
	if err != nil {
		s.renderError(w, r, http.StatusInternalServerError, "VExplain", err)
		return
	}
	if resp == nil {
		http.NotFound(w, r)
		return
	}

	data.Response = resp
	s.render(w, r, http.StatusOK, "vexplain.html", PageData{Title: "VExplain", Active: "vexplain", Data: data})
}
