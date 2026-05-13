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
	"net/url"
	"strings"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

func (s *Server) createKeyspace(w http.ResponseWriter, r *http.Request) {
	if s.opts.ReadOnly {
		s.renderReadOnly(w, r)
		return
	}

	if err := r.ParseForm(); err != nil {
		s.renderFormError(w, r, "Create keyspace", err.Error())
		return
	}

	clusterID := strings.TrimSpace(r.Form.Get("cluster_id"))
	name := strings.TrimSpace(r.Form.Get("name"))
	if clusterID == "" {
		s.renderFormError(w, r, "Create keyspace", "cluster is required")
		return
	}
	if name == "" {
		s.renderFormError(w, r, "Create keyspace", "keyspace name is required")
		return
	}

	_, err := s.api.CreateKeyspace(r.Context(), &vtadminpb.CreateKeyspaceRequest{
		ClusterId: clusterID,
		Options: &vtctldatapb.CreateKeyspaceRequest{
			Name:              name,
			Force:             r.Form.Get("force") == "on",
			AllowEmptyVSchema: r.Form.Get("allow_empty_v_schema") == "on",
			DurabilityPolicy:  strings.TrimSpace(r.Form.Get("durability_policy")),
			SidecarDbName:     strings.TrimSpace(r.Form.Get("sidecar_db_name")),
		},
	})
	if err != nil {
		s.renderFormError(w, r, "Create keyspace", err.Error())
		return
	}

	redirectWithFlash(w, r, "/keyspace/"+url.PathEscape(clusterID)+"/"+url.PathEscape(name), Flash{
		Kind:    "success",
		Message: "created keyspace " + name,
	})
}

func (s *Server) renderFormError(w http.ResponseWriter, r *http.Request, title string, message string) {
	w.WriteHeader(http.StatusBadRequest)
	s.render(w, r, "index.html", PageData{
		Title: title,
		Flash: &Flash{
			Kind:    "error",
			Message: message,
		},
	})
}

func redirectWithFlash(w http.ResponseWriter, r *http.Request, target string, flash Flash) {
	values := url.Values{}
	values.Set("flash", flash.Kind)
	values.Set("message", flash.Message)
	if strings.Contains(target, "?") {
		target += "&" + values.Encode()
	} else {
		target += "?" + values.Encode()
	}
	http.Redirect(w, r, target, http.StatusSeeOther)
}
