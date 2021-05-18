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

	"github.com/gorilla/mux"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

// GetKeyspace implements the http wrapper for /keyspace/{cluster_id}/{name}.
func GetKeyspace(ctx context.Context, r Request, api *API) *JSONResponse {
	vars := mux.Vars(r.Request)
	keyspace, err := api.server.GetKeyspace(ctx, &vtadminpb.GetKeyspaceRequest{
		ClusterId: vars["cluster_id"],
		Keyspace:  vars["name"],
	})

	return NewJSONResponse(keyspace, err)
}

// GetKeyspaces implements the http wrapper for /keyspaces[?cluster=[&cluster=]].
func GetKeyspaces(ctx context.Context, r Request, api *API) *JSONResponse {
	keyspaces, err := api.server.GetKeyspaces(ctx, &vtadminpb.GetKeyspacesRequest{
		ClusterIds: r.URL.Query()["cluster"],
	})

	return NewJSONResponse(keyspaces, err)
}
