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
)

func (s *Server) vschemas(w http.ResponseWriter, r *http.Request) {
	resp, err := s.api.GetVSchemas(r.Context(), &vtadminpb.GetVSchemasRequest{
		ClusterIds: queryValues(r, "cluster_id"),
	})
	if err != nil {
		s.renderError(w, r, http.StatusInternalServerError, "VSchema", err)
		return
	}

	s.render(w, r, http.StatusOK, "vschemas.html", PageData{
		Title:  "VSchema",
		Active: "vschemas",
		Data:   resp.GetVSchemas(),
	})
}

func (s *Server) vschema(w http.ResponseWriter, r *http.Request) {
	keyspace := r.PathValue("keyspace")
	resp, err := s.api.GetVSchema(r.Context(), &vtadminpb.GetVSchemaRequest{
		ClusterId: r.PathValue("cluster_id"),
		Keyspace:  keyspace,
	})
	if err != nil {
		s.renderError(w, r, http.StatusInternalServerError, keyspace, err)
		return
	}
	if resp == nil {
		http.NotFound(w, r)
		return
	}

	s.render(w, r, http.StatusOK, "vschema.html", PageData{
		Title:  keyspace,
		Active: "vschemas",
		Data:   resp,
	})
}

func (s *Server) srvKeyspaces(w http.ResponseWriter, r *http.Request) {
	resp, err := s.api.GetSrvKeyspaces(r.Context(), &vtadminpb.GetSrvKeyspacesRequest{
		ClusterIds: queryValues(r, "cluster_id"),
		Cells:      queryValues(r, "cell"),
	})
	if err != nil {
		s.renderError(w, r, http.StatusInternalServerError, "SrvKeyspaces", err)
		return
	}

	s.render(w, r, http.StatusOK, "srvkeyspaces.html", PageData{
		Title:  "SrvKeyspaces",
		Active: "srvkeyspaces",
		Data:   resp,
	})
}

func (s *Server) srvVSchemas(w http.ResponseWriter, r *http.Request) {
	resp, err := s.api.GetSrvVSchemas(r.Context(), &vtadminpb.GetSrvVSchemasRequest{
		ClusterIds: queryValues(r, "cluster_id"),
		Cells:      queryValues(r, "cell"),
	})
	if err != nil {
		s.renderError(w, r, http.StatusInternalServerError, "SrvVSchemas", err)
		return
	}

	s.render(w, r, http.StatusOK, "srvvschemas.html", PageData{
		Title:  "SrvVSchemas",
		Active: "srvvschemas",
		Data:   resp.GetSrvVSchemas(),
	})
}
