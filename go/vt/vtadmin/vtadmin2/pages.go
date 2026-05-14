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

	"github.com/gorilla/mux"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

func (s *Server) clusters(w http.ResponseWriter, r *http.Request) {
	resp, err := s.api.GetClusters(r.Context(), &vtadminpb.GetClustersRequest{})
	if err != nil {
		s.renderError(w, r, http.StatusInternalServerError, "Clusters", err)
		return
	}

	s.render(w, r, http.StatusOK, "clusters.html", PageData{
		Title:  "Clusters",
		Active: "clusters",
		Data:   resp.GetClusters(),
	})
}

func (s *Server) keyspaces(w http.ResponseWriter, r *http.Request) {
	resp, err := s.api.GetKeyspaces(r.Context(), &vtadminpb.GetKeyspacesRequest{
		ClusterIds: r.URL.Query()["cluster_id"],
	})
	if err != nil {
		s.renderError(w, r, http.StatusInternalServerError, "Keyspaces", err)
		return
	}

	s.render(w, r, http.StatusOK, "keyspaces.html", PageData{
		Title:  "Keyspaces",
		Active: "keyspaces",
		Data:   resp.GetKeyspaces(),
	})
}

func (s *Server) keyspace(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	ks, err := s.api.GetKeyspace(r.Context(), &vtadminpb.GetKeyspaceRequest{
		ClusterId: vars["cluster_id"],
		Keyspace:  vars["name"],
	})
	if err != nil {
		s.renderError(w, r, http.StatusInternalServerError, "Keyspace", err)
		return
	}
	if ks == nil {
		http.NotFound(w, r)
		return
	}

	s.render(w, r, http.StatusOK, "keyspace.html", PageData{
		Title:  keyspaceName(ks),
		Active: "keyspaces",
		Data:   ks,
	})
}

func (s *Server) tablets(w http.ResponseWriter, r *http.Request) {
	resp, err := s.api.GetTablets(r.Context(), &vtadminpb.GetTabletsRequest{
		ClusterIds: r.URL.Query()["cluster_id"],
	})
	if err != nil {
		s.renderError(w, r, http.StatusInternalServerError, "Tablets", err)
		return
	}

	s.render(w, r, http.StatusOK, "tablets.html", PageData{
		Title:  "Tablets",
		Active: "tablets",
		Data:   resp.GetTablets(),
	})
}

func (s *Server) schemas(w http.ResponseWriter, r *http.Request) {
	resp, err := s.api.GetSchemas(r.Context(), &vtadminpb.GetSchemasRequest{
		ClusterIds: r.URL.Query()["cluster_id"],
	})
	if err != nil {
		s.renderError(w, r, http.StatusInternalServerError, "Schemas", err)
		return
	}

	s.render(w, r, http.StatusOK, "schemas.html", PageData{
		Title:  "Schemas",
		Active: "schemas",
		Data:   resp.GetSchemas(),
	})
}

func (s *Server) createKeyspaceForm(w http.ResponseWriter, r *http.Request) {
	if s.opts.ReadOnly {
		s.renderReadOnly(w, r)
		return
	}

	resp, err := s.api.GetClusters(r.Context(), &vtadminpb.GetClustersRequest{})
	if err != nil {
		s.renderError(w, r, http.StatusInternalServerError, "Create keyspace", err)
		return
	}

	s.render(w, r, http.StatusOK, "keyspace_create.html", PageData{
		Title:  "Create keyspace",
		Active: "keyspaces",
		Data:   resp.GetClusters(),
	})
}
