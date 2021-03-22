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

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
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
// - cluster: repeated, cluster IDs
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
		ClusterIds:      query["cluster"],
		Keyspaces:       query["keyspace"],
		IgnoreKeyspaces: query["ignore_keyspace"],
		ActiveOnly:      activeOnly,
	})

	return NewJSONResponse(workflows, err)
}
