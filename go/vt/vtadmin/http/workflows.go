/*
Copyright 2021 The Vitess Authors.

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

package http

import (
	"context"
	"encoding/json"

	"vitess.io/vitess/go/vt/vtadmin/errors"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

// GetWorkflow implements the http wrapper for the VTAdminServer.GetWorkflow
// method.
//
// Its route is /workflow/{cluster_id}/{keyspace}/{name}[?active_only=].
func GetWorkflow(ctx context.Context, r Request, api *API) *JSONResponse {
	vars := r.Vars()

	activeOnly, err := r.ParseQueryParamAsBool("active_only", false)
	if err != nil {
		return NewJSONResponse(nil, err)
	}

	workflow, err := api.server.GetWorkflow(ctx, &vtadminpb.GetWorkflowRequest{
		ClusterId:  vars["cluster_id"],
		Keyspace:   vars["keyspace"],
		Name:       vars["name"],
		ActiveOnly: activeOnly,
	})

	return NewJSONResponse(workflow, err)
}

// GetWorkflows implements the http wrapper for the VTAdminServer.GetWorkflows
// method.
//
// Its route is /workflows, with query params:
// - cluster_id: repeated, cluster IDs
// - active_only
// - keyspace: repeated
// - ignore_keyspace: repeated
func GetWorkflows(ctx context.Context, r Request, api *API) *JSONResponse {
	query := r.URL.Query()

	activeOnly, err := r.ParseQueryParamAsBool("active_only", false)
	if err != nil {
		return NewJSONResponse(nil, err)
	}

	workflows, err := api.server.GetWorkflows(ctx, &vtadminpb.GetWorkflowsRequest{
		ClusterIds:      query["cluster_id"],
		Keyspaces:       query["keyspace"],
		IgnoreKeyspaces: query["ignore_keyspace"],
		ActiveOnly:      activeOnly,
	})

	return NewJSONResponse(workflows, err)
}

// GetWorkflowStatus implements the http wrapper for the VTAdminServer.GetWorkflowStatus
// method.
//
// Its route is /workflow/{cluster_id}/{keyspace}/{name}/status
func GetWorkflowStatus(ctx context.Context, r Request, api *API) *JSONResponse {
	vars := r.Vars()

	workflowStatus, err := api.server.GetWorkflowStatus(ctx, &vtadminpb.GetWorkflowStatusRequest{
		ClusterId: vars["cluster_id"],
		Keyspace:  vars["keyspace"],
		Name:      vars["name"],
	})

	return NewJSONResponse(workflowStatus, err)
}

// StartWorkflow implements the http wrapper for the VTAdminServer.StartWorkflow
// method.
//
// Its route is /workflow/{cluster_id}/{keyspace}/{name}/start
func StartWorkflow(ctx context.Context, r Request, api *API) *JSONResponse {
	vars := r.Vars()

	res, err := api.server.StartWorkflow(ctx, &vtadminpb.StartWorkflowRequest{
		ClusterId: vars["cluster_id"],
		Keyspace:  vars["keyspace"],
		Workflow:  vars["name"],
	})

	return NewJSONResponse(res, err)
}

// StopWorkflow implements the http wrapper for the VTAdminServer.StopWorkflow
// method.
//
// Its route is /workflow/{cluster_id}/{keyspace}/{name}/stop
func StopWorkflow(ctx context.Context, r Request, api *API) *JSONResponse {
	vars := r.Vars()

	res, err := api.server.StopWorkflow(ctx, &vtadminpb.StopWorkflowRequest{
		ClusterId: vars["cluster_id"],
		Keyspace:  vars["keyspace"],
		Workflow:  vars["name"],
	})

	return NewJSONResponse(res, err)
}

// MoveTablesCreate implements the http wrapper for the VTAdminServer.MoveTablesCreate
// method.
//
// Its route is /workflow/{cluster_id}/movetables
func MoveTablesCreate(ctx context.Context, r Request, api *API) *JSONResponse {
	vars := r.Vars()
	decoder := json.NewDecoder(r.Body)
	defer r.Body.Close()

	var req vtctldatapb.MoveTablesCreateRequest
	if err := decoder.Decode(&req); err != nil {
		return NewJSONResponse(nil, &errors.BadRequest{
			Err: err,
		})
	}

	res, err := api.server.MoveTablesCreate(ctx, &vtadminpb.MoveTablesCreateRequest{
		ClusterId: vars["cluster_id"],
		Request:   &req,
	})

	return NewJSONResponse(res, err)
}
