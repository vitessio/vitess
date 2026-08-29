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
	"net/http"
	"net/url"
	"strings"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

// Switch traffic direction values used by WorkflowSwitchTrafficRequest.
// These correspond to TrafficSwitchDirection in go/vt/vtctl/workflow.
const (
	switchDirectionForward  = int32(0)
	switchDirectionBackward = int32(1)
)

// beginWorkflowAction is the shared preflight for workflow mutation handlers.
// It returns the parsed route values; ok is false when the preflight has
// already rendered an error response.
func (s *Server) beginWorkflowAction(w http.ResponseWriter, r *http.Request, title string) (clusterID, keyspace, workflow string, ok bool) {
	if !s.beginFormAction(w, r, title) {
		return "", "", "", false
	}
	return r.PathValue("cluster_id"), r.PathValue("keyspace"), r.PathValue("name"), true
}

func workflowDetailPath(clusterID, keyspace, workflow string) string {
	return "/workflow/" + pathEscape(clusterID) + "/" + pathEscape(keyspace) + "/" + pathEscape(workflow)
}

func (s *Server) workflowStart(w http.ResponseWriter, r *http.Request) {
	const title = "Start workflow"
	clusterID, keyspace, workflow, ok := s.beginWorkflowAction(w, r, title)
	if !ok {
		return
	}

	_, err := s.api.StartWorkflow(r.Context(), &vtadminpb.StartWorkflowRequest{
		ClusterId: clusterID,
		Keyspace:  keyspace,
		Workflow:  workflow,
	})
	if err != nil {
		s.renderFormErrorErr(w, r, title, err)
		return
	}

	redirectWithFlash(w, r, workflowDetailPath(clusterID, keyspace, workflow), Flash{
		Kind:    "success",
		Message: "started workflow " + workflow,
	})
}

func (s *Server) workflowStop(w http.ResponseWriter, r *http.Request) {
	const title = "Stop workflow"
	clusterID, keyspace, workflow, ok := s.beginWorkflowAction(w, r, title)
	if !ok {
		return
	}

	_, err := s.api.StopWorkflow(r.Context(), &vtadminpb.StopWorkflowRequest{
		ClusterId: clusterID,
		Keyspace:  keyspace,
		Workflow:  workflow,
	})
	if err != nil {
		s.renderFormErrorErr(w, r, title, err)
		return
	}

	redirectWithFlash(w, r, workflowDetailPath(clusterID, keyspace, workflow), Flash{
		Kind:    "success",
		Message: "stopped workflow " + workflow,
	})
}

func (s *Server) workflowCancel(w http.ResponseWriter, r *http.Request) {
	const title = "Cancel workflow"
	clusterID, keyspace, workflow, ok := s.beginWorkflowAction(w, r, title)
	if !ok {
		return
	}

	_, err := s.api.WorkflowDelete(r.Context(), &vtadminpb.WorkflowDeleteRequest{
		ClusterId: clusterID,
		Request: &vtctldatapb.WorkflowDeleteRequest{
			Keyspace: keyspace,
			Workflow: workflow,
			KeepData: new(r.Form.Get("keep_data") == "on"),
			Shards:   r.Form["shard"],
		},
	})
	if err != nil {
		s.renderFormErrorErr(w, r, title, err)
		return
	}

	redirectWithFlash(w, r, "/workflows", Flash{
		Kind:    "success",
		Message: "cancelled workflow " + workflow,
	})
}

func (s *Server) workflowComplete(w http.ResponseWriter, r *http.Request) {
	const title = "Complete workflow"
	clusterID, keyspace, workflow, ok := s.beginWorkflowAction(w, r, title)
	if !ok {
		return
	}

	_, err := s.api.MoveTablesComplete(r.Context(), &vtadminpb.MoveTablesCompleteRequest{
		ClusterId: clusterID,
		Request: &vtctldatapb.MoveTablesCompleteRequest{
			Workflow:         workflow,
			TargetKeyspace:   keyspace,
			KeepData:         new(r.Form.Get("keep_data") == "on"),
			KeepRoutingRules: r.Form.Get("keep_routing_rules") == "on",
			RenameTables:     r.Form.Get("rename_tables") == "on",
		},
	})
	if err != nil {
		s.renderFormErrorErr(w, r, title, err)
		return
	}

	redirectWithFlash(w, r, "/workflows", Flash{
		Kind:    "success",
		Message: "completed workflow " + workflow,
	})
}

func (s *Server) workflowSwitchTraffic(w http.ResponseWriter, r *http.Request, direction int32) {
	title := "Switch traffic"
	if direction == switchDirectionBackward {
		title = "Reverse traffic"
	}

	clusterID, keyspace, workflow, ok := s.beginWorkflowAction(w, r, title)
	if !ok {
		return
	}

	tabletTypes, err := parseTabletTypes(r.Form["tablet_type"])
	if err != nil {
		s.renderFormErrorErr(w, r, title, err)
		return
	}

	timeout, err := parseShardFormDuration(r, "timeout")
	if err != nil {
		s.renderFormErrorErr(w, r, title, err)
		return
	}

	maxLag, err := parseShardFormDuration(r, "max_replication_lag_allowed")
	if err != nil {
		s.renderFormErrorErr(w, r, title, err)
		return
	}

	// Reverse traffic cannot enable reverse replication (it IS the reverse).
	enableReverseReplication := direction == switchDirectionForward && r.Form.Get("enable_reverse_replication") == "on"

	_, err = s.api.WorkflowSwitchTraffic(r.Context(), &vtadminpb.WorkflowSwitchTrafficRequest{
		ClusterId: clusterID,
		Request: &vtctldatapb.WorkflowSwitchTrafficRequest{
			Keyspace:                  keyspace,
			Workflow:                  workflow,
			TabletTypes:               tabletTypes,
			Cells:                     splitFormList(r.Form.Get("cells")),
			EnableReverseReplication:  enableReverseReplication,
			Direction:                 direction,
			Timeout:                   timeout,
			MaxReplicationLagAllowed:  maxLag,
			InitializeTargetSequences: r.Form.Get("initialize_target_sequences") == "on",
			Force:                     r.Form.Get("force") == "on",
			DryRun:                    r.Form.Get("dry_run") == "on",
		},
	})
	if err != nil {
		s.renderFormErrorErr(w, r, title, err)
		return
	}

	message := "switched traffic for workflow " + workflow
	if direction == switchDirectionBackward {
		message = "reversed traffic for workflow " + workflow
	}
	if r.Form.Get("dry_run") == "on" {
		message += " (dry run)"
	}

	redirectWithFlash(w, r, workflowDetailPath(clusterID, keyspace, workflow), Flash{
		Kind:    "success",
		Message: message,
	})
}

func (s *Server) workflowSwitchTrafficForward(w http.ResponseWriter, r *http.Request) {
	s.workflowSwitchTraffic(w, r, switchDirectionForward)
}

func (s *Server) workflowSwitchTrafficReverse(w http.ResponseWriter, r *http.Request) {
	s.workflowSwitchTraffic(w, r, switchDirectionBackward)
}

func (s *Server) workflowVDiffCreate(w http.ResponseWriter, r *http.Request) {
	const title = "Create VDiff"
	clusterID, keyspace, workflow, ok := s.beginWorkflowAction(w, r, title)
	if !ok {
		return
	}

	_, err := s.api.VDiffCreate(r.Context(), &vtadminpb.VDiffCreateRequest{
		ClusterId: clusterID,
		Request: &vtctldatapb.VDiffCreateRequest{
			Workflow:       workflow,
			TargetKeyspace: keyspace,
			Uuid:           strings.TrimSpace(r.Form.Get("uuid")),
			SourceCells:    splitFormList(r.Form.Get("source_cells")),
			TargetCells:    splitFormList(r.Form.Get("target_cells")),
			Tables:         splitFormList(r.Form.Get("tables")),
			AutoRetry:      r.Form.Get("auto_retry") == "on",
			Wait:           r.Form.Get("wait") == "on",
		},
	})
	if err != nil {
		s.renderFormErrorErr(w, r, title, err)
		return
	}

	redirect := "/vdiff/" + pathEscape(clusterID) + "/show?" +
		"cluster_id=" + url.QueryEscape(clusterID) +
		"&keyspace=" + url.QueryEscape(keyspace) +
		"&workflow=" + url.QueryEscape(workflow)

	if u := strings.TrimSpace(r.Form.Get("uuid")); u != "" {
		redirect += "&uuid=" + url.QueryEscape(u)
	}

	redirectWithFlash(w, r, redirect, Flash{
		Kind:    "success",
		Message: "created VDiff for workflow " + workflow,
	})
}
