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

// GetVSchema implements the http wrapper for the
// /vschema/{cluster_id}/{keyspace} route.
func GetVSchema(ctx context.Context, r Request, api *API) *JSONResponse {
	vars := r.Vars()

	vschema, err := api.server.GetVSchema(ctx, &vtadminpb.GetVSchemaRequest{
		ClusterId: vars["cluster_id"],
		Keyspace:  vars["keyspace"],
	})

	return NewJSONResponse(vschema, err)
}

// GetVSchemas implements the http wrapper for the
// /vschemas[?cluster=[&cluster=]] route.
func GetVSchemas(ctx context.Context, r Request, api *API) *JSONResponse {
	vschemas, err := api.server.GetVSchemas(ctx, &vtadminpb.GetVSchemasRequest{
		ClusterIds: r.URL.Query()["cluster"],
	})

	return NewJSONResponse(vschemas, err)
}
