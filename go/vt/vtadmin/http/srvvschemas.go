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

// GetSrvVSchema implements the http wrapper for the /api/srvvschema/{cluster_id}/{cell} route.
func GetSrvVSchema(ctx context.Context, r Request, api *API) *JSONResponse {
	vars := r.Vars()

	sv, err := api.server.GetSrvVSchema(ctx, &vtadminpb.GetSrvVSchemaRequest{
		Cell:      vars["cell"],
		ClusterId: vars["cluster_id"],
	})

	return NewJSONResponse(sv, err)
}

// GetSrvVSchemas implements the http wrapper for the /api/srvvschemas route.
func GetSrvVSchemas(ctx context.Context, r Request, api *API) *JSONResponse {
	query := r.URL.Query()

	svs, err := api.server.GetSrvVSchemas(ctx, &vtadminpb.GetSrvVSchemasRequest{
		Cells:      query["cell"],
		ClusterIds: query["cluster_id"],
	})

	return NewJSONResponse(svs, err)
}
