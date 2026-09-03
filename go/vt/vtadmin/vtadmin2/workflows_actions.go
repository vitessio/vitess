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
	"cmp"
	"net/http"
	"net/url"
	"slices"
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

	resp, err := s.api.StartWorkflow(r.Context(), &vtadminpb.StartWorkflowRequest{
		ClusterId: clusterID,
		Keyspace:  keyspace,
		Workflow:  workflow,
	})
	if err != nil {
		s.renderFormErrorErr(w, r, title, err)
		return
	}
	if resp == nil {
		s.renderFormError(w, r, title, "not authorized to start workflow")
		return
	}

	s.redirectWithFlash(w, r, workflowDetailPath(clusterID, keyspace, workflow), Flash{
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

	resp, err := s.api.StopWorkflow(r.Context(), &vtadminpb.StopWorkflowRequest{
		ClusterId: clusterID,
		Keyspace:  keyspace,
		Workflow:  workflow,
	})
	if err != nil {
		s.renderFormErrorErr(w, r, title, err)
		return
	}
	if resp == nil {
		s.renderFormError(w, r, title, "not authorized to stop workflow")
		return
	}

	s.redirectWithFlash(w, r, workflowDetailPath(clusterID, keyspace, workflow), Flash{
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

	var keepData *bool
	if r.Form.Get("keep_data") == "on" {
		keepData = new(true)
	}

	_, err := s.api.WorkflowDelete(r.Context(), &vtadminpb.WorkflowDeleteRequest{
		ClusterId: clusterID,
		Request: &vtctldatapb.WorkflowDeleteRequest{
			Keyspace: keyspace,
			Workflow: workflow,
			KeepData: keepData,
			Shards:   r.Form["shard"],
		},
	})
	if err != nil {
		s.renderFormErrorErr(w, r, title, err)
		return
	}

	s.redirectWithFlash(w, r, "/workflows", Flash{
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

	wf, err := s.api.GetWorkflow(r.Context(), &vtadminpb.GetWorkflowRequest{
		ClusterId: clusterID,
		Keyspace:  keyspace,
		Name:      workflow,
	})
	if err != nil {
		s.renderFormErrorErr(w, r, title, err)
		return
	}
	if wf == nil || wf.GetWorkflow() == nil {
		s.renderFormError(w, r, title, "not authorized to complete workflow")
		return
	}
	if !workflowSupportsTrafficSwitch(wf.GetWorkflow().GetWorkflowType()) {
		s.renderFormError(w, r, title, "complete is only supported for MoveTables and Reshard workflows")
		return
	}

	status, err := s.api.GetWorkflowStatus(r.Context(), &vtadminpb.GetWorkflowStatusRequest{
		ClusterId: clusterID,
		Keyspace:  keyspace,
		Name:      workflow,
	})
	if err != nil {
		s.renderFormErrorErr(w, r, title, err)
		return
	}
	if !workflowTrafficFullySwitched(status.GetTrafficState()) {
		s.renderFormError(w, r, title, "cannot complete workflow until traffic is fully switched")
		return
	}

	_, err = s.api.MoveTablesComplete(r.Context(), &vtadminpb.MoveTablesCompleteRequest{
		ClusterId: clusterID,
		Request: &vtctldatapb.MoveTablesCompleteRequest{
			Workflow:         workflow,
			TargetKeyspace:   keyspace,
			KeepData:         formOptionalKeepData(r.Form),
			KeepRoutingRules: r.Form.Get("keep_routing_rules") == "on",
			RenameTables:     r.Form.Get("rename_tables") == "on",
		},
	})
	if err != nil {
		s.renderFormErrorErr(w, r, title, err)
		return
	}

	s.redirectWithFlash(w, r, "/workflows", Flash{
		Kind:    "success",
		Message: "completed workflow " + workflow,
	})
}

func workflowSupportsTrafficSwitch(workflowType string) bool {
	return workflowType == "MoveTables" || workflowType == "Reshard"
}

func workflowTrafficFullySwitched(trafficState string) bool {
	if strings.Contains(trafficState, "Not Switched") || strings.Contains(trafficState, "partially") {
		return false
	}
	return strings.Contains(trafficState, "Writes Switched")
}

func formOptionalKeepData(form url.Values) *bool {
	values := form["keep_data"]
	if len(values) == 0 {
		return nil
	}
	if slices.Contains(values, "on") {
		return new(true)
	}
	return new(false)
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

	wf, err := s.api.GetWorkflow(r.Context(), &vtadminpb.GetWorkflowRequest{
		ClusterId: clusterID,
		Keyspace:  keyspace,
		Name:      workflow,
	})
	if err != nil {
		s.renderFormErrorErr(w, r, title, err)
		return
	}
	if wf == nil || wf.GetWorkflow() == nil {
		s.renderFormError(w, r, title, "not authorized to "+strings.ToLower(title))
		return
	}
	if !workflowSupportsTrafficSwitch(wf.GetWorkflow().GetWorkflowType()) {
		s.renderFormError(w, r, title, "traffic switching is only supported for MoveTables and Reshard workflows")
		return
	}

	// Match the vtctldclient CLI: an omitted tablet-types selection defaults
	// to PRIMARY, REPLICA, and RDONLY. An empty list can return success while
	// switching no traffic at all.
	submittedTypes := r.Form["tablet_type"]
	if len(submittedTypes) == 0 {
		submittedTypes = []string{"PRIMARY", "REPLICA", "RDONLY"}
	}
	tabletTypes, err := parseTabletTypes(submittedTypes)
	if err != nil {
		s.renderFormErrorErr(w, r, title, err)
		return
	}

	timeout, err := parseShardFormDuration(r, "timeout")
	if err != nil {
		s.renderFormErrorErr(w, r, title, err)
		return
	}
	if timeout != nil && timeout.GetSeconds() < 1 {
		s.renderFormError(w, r, title, "timeout value must be at least 1 second")
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

	s.redirectWithFlash(w, r, workflowDetailPath(clusterID, keyspace, workflow), Flash{
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

	resp, err := s.api.VDiffCreate(r.Context(), &vtadminpb.VDiffCreateRequest{
		ClusterId: clusterID,
		Request: &vtctldatapb.VDiffCreateRequest{
			Workflow:       workflow,
			TargetKeyspace: keyspace,
			Uuid:           strings.TrimSpace(r.Form.Get("uuid")),
			SourceCells:    splitFormList(r.Form.Get("source_cells")),
			TargetCells:    splitFormList(r.Form.Get("target_cells")),
			Tables:         splitFormList(r.Form.Get("tables")),
			AutoRetry:      r.Form.Get("auto_retry") == "on",
			// Wait is intentionally not offered: a synchronous VDiff can outlive
			// the HTTP request. Create the VDiff and poll the show page instead.
		},
	})
	if err != nil {
		s.renderFormErrorErr(w, r, title, err)
		return
	}
	if resp == nil {
		s.renderFormError(w, r, title, "not authorized to create VDiff")
		return
	}

	redirect := "/vdiff/" + pathEscape(clusterID) + "/show?" +
		"cluster_id=" + url.QueryEscape(clusterID) +
		"&keyspace=" + url.QueryEscape(keyspace) +
		"&workflow=" + url.QueryEscape(workflow)

	if u := cmp.Or(resp.GetUUID(), strings.TrimSpace(r.Form.Get("uuid"))); u != "" {
		redirect += "&arg=" + url.QueryEscape(u)
	}

	s.redirectWithFlash(w, r, redirect, Flash{
		Kind:    "success",
		Message: "created VDiff for workflow " + workflow,
	})
}
