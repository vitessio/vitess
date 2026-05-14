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
	"errors"
	"net/http"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	vtadminerrors "vitess.io/vitess/go/vt/vtadmin/errors"
	"vitess.io/vitess/go/vt/vterrors"
)

type (
	workflowStatusData struct {
		ClusterID string
		Keyspace  string
		Name      string
		Response  *vtctldatapb.WorkflowStatusResponse
	}

	vdiffShowData struct {
		ClusterID string
		Keyspace  string
		Workflow  string
		Arg       string
		Response  *vtadminpb.VDiffShowResponse
	}
)

func (s *Server) workflows(w http.ResponseWriter, r *http.Request) {
	activeOnly, err := parseQueryBool(r, "active_only", false)
	if err != nil {
		s.renderError(w, r, http.StatusBadRequest, "Workflows", err)
		return
	}

	resp, err := s.api.GetWorkflows(r.Context(), &vtadminpb.GetWorkflowsRequest{
		ClusterIds:      queryValues(r, "cluster_id"),
		Keyspaces:       queryValues(r, "keyspace"),
		IgnoreKeyspaces: queryValues(r, "ignore_keyspace"),
		ActiveOnly:      activeOnly,
	})
	if err != nil {
		s.renderError(w, r, http.StatusInternalServerError, "Workflows", err)
		return
	}

	s.render(w, r, http.StatusOK, "workflows.html", PageData{
		Title:  "Workflows",
		Active: "workflows",
		Data:   resp.GetWorkflowsByCluster(),
	})
}

func (s *Server) workflow(w http.ResponseWriter, r *http.Request) {
	activeOnly, err := parseQueryBool(r, "active_only", false)
	if err != nil {
		s.renderError(w, r, http.StatusBadRequest, r.PathValue("name"), err)
		return
	}

	resp, err := s.api.GetWorkflow(r.Context(), &vtadminpb.GetWorkflowRequest{
		ClusterId:  r.PathValue("cluster_id"),
		Keyspace:   r.PathValue("keyspace"),
		Name:       r.PathValue("name"),
		ActiveOnly: activeOnly,
	})
	if err != nil {
		if errors.Is(err, vtadminerrors.ErrNoWorkflow) {
			s.renderError(w, r, http.StatusNotFound, r.PathValue("name"), err)
			return
		}
		s.renderError(w, r, http.StatusInternalServerError, r.PathValue("name"), err)
		return
	}
	if resp == nil {
		http.NotFound(w, r)
		return
	}

	s.render(w, r, http.StatusOK, "workflow.html", PageData{
		Title:  r.PathValue("name"),
		Active: "workflows",
		Data:   resp,
	})
}

func (s *Server) workflowStatus(w http.ResponseWriter, r *http.Request) {
	resp, err := s.api.GetWorkflowStatus(r.Context(), &vtadminpb.GetWorkflowStatusRequest{
		ClusterId: r.PathValue("cluster_id"),
		Keyspace:  r.PathValue("keyspace"),
		Name:      r.PathValue("name"),
	})
	if err != nil {
		s.renderError(w, r, http.StatusInternalServerError, "Workflow Status", err)
		return
	}
	if resp == nil {
		http.NotFound(w, r)
		return
	}

	s.render(w, r, http.StatusOK, "workflow_status.html", PageData{
		Title:  "Workflow Status",
		Active: "workflows",
		Data: workflowStatusData{
			ClusterID: r.PathValue("cluster_id"),
			Keyspace:  r.PathValue("keyspace"),
			Name:      r.PathValue("name"),
			Response:  resp,
		},
	})
}

func (s *Server) vdiffShow(w http.ResponseWriter, r *http.Request) {
	keyspace := queryValue(r, "keyspace")
	if keyspace == "" {
		s.renderError(w, r, http.StatusBadRequest, "VDiff", vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "keyspace query parameter is required"))
		return
	}
	workflow := queryValue(r, "workflow")
	if workflow == "" {
		s.renderError(w, r, http.StatusBadRequest, "VDiff", vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "workflow query parameter is required"))
		return
	}
	arg := queryValue(r, "arg")
	if arg == "" {
		arg = "last"
	}

	resp, err := s.api.VDiffShow(r.Context(), &vtadminpb.VDiffShowRequest{
		ClusterId: r.PathValue("cluster_id"),
		Request: &vtctldatapb.VDiffShowRequest{
			TargetKeyspace: keyspace,
			Workflow:       workflow,
			Arg:            arg,
		},
	})
	if err != nil {
		s.renderError(w, r, http.StatusInternalServerError, "VDiff", err)
		return
	}
	if resp == nil {
		http.NotFound(w, r)
		return
	}

	s.render(w, r, http.StatusOK, "vdiff_show.html", PageData{
		Title:  "VDiff",
		Active: "workflows",
		Data: vdiffShowData{
			ClusterID: r.PathValue("cluster_id"),
			Keyspace:  keyspace,
			Workflow:  workflow,
			Arg:       arg,
			Response:  resp,
		},
	})
}
