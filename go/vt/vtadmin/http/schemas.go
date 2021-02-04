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

// GetSchemas implements the http wrapper for /schemas[?cluster=[&cluster=]
func GetSchemas(ctx context.Context, r Request, api *API) *JSONResponse {
	schemas, err := api.server.GetSchemas(ctx, &vtadminpb.GetSchemasRequest{
		ClusterIds: r.URL.Query()["cluster"],
	})

	return NewJSONResponse(schemas, err)
}
