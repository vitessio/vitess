/*
Copyright 2020 The Vitess Authors.

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

// GetTablets implements the http wrapper for /tablets[?cluster=[&cluster=]].
func GetTablets(ctx context.Context, r Request, api *API) *JSONResponse {
	tablets, err := api.server.GetTablets(ctx, &vtadminpb.GetTabletsRequest{
		ClusterIds: r.URL.Query()["cluster"],
	})

	return NewJSONResponse(tablets, err)
}

// GetTablet implements the http wrapper for /tablet/{tablet}[?cluster=[&cluster=]].
func GetTablet(ctx context.Context, r Request, api *API) *JSONResponse {
	vars := r.Vars()

	tablet, err := api.server.GetTablet(ctx, &vtadminpb.GetTabletRequest{
		Hostname:   vars["tablet"],
		ClusterIds: r.URL.Query()["cluster"],
	})

	return NewJSONResponse(tablet, err)
}
