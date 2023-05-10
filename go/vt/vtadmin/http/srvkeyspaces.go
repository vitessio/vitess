/*
Copyright 2023 The Vitess Authors.

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

	"github.com/gorilla/mux"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

// GetSrvKeyspaces implements the http wrapper for the /api/srvkeyspaces route.
func GetSrvKeyspaces(ctx context.Context, r Request, api *API) *JSONResponse {
	query := r.URL.Query()

	sks, err := api.server.GetSrvKeyspaces(ctx, &vtadminpb.GetSrvKeyspacesRequest{
		Cells:      query["cell"],
		ClusterIds: query["cluster_id"],
	})

	return NewJSONResponse(sks, err)
}

// GetSrvKeyspace implements the http wrapper for the /api/srvkeyspaces/{cluster_id}/{name} route.
func GetSrvKeyspace(ctx context.Context, r Request, api *API) *JSONResponse {
	query := r.URL.Query()
	vars := mux.Vars(r.Request)

	sk, err := api.server.GetSrvKeyspace(ctx, &vtadminpb.GetSrvKeyspaceRequest{
		Cells:     query["cell"],
		Keyspace:  vars["name"],
		ClusterId: vars["cluster_id"],
	})

	return NewJSONResponse(sk, err)
}
