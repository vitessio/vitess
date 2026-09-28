/*
Copyright 2022 The Vitess Authors.

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

// GetCellInfos implements the http wrapper for the
// /cells[?cluster=[&cluster_id=...]?cell=[&cell=...]&names_only=(true|false)] route.
func GetCellInfos(ctx context.Context, r Request, api *API) *JSONResponse {
	namesOnly, err := r.ParseQueryParamAsBool("names_only", false)
	if err != nil {
		return NewJSONResponse(nil, err)
	}

	cellInfos, err := api.server.GetCellInfos(ctx, &vtadminpb.GetCellInfosRequest{
		ClusterIds: r.URL.Query()["cluster_id"],
		Cells:      r.URL.Query()["cell"],
		NamesOnly:  namesOnly,
	})
	return NewJSONResponse(cellInfos, err)
}

// GetCellsAliases implements the http wrapper for the
// /cells_aliases[?cluster_id=[&cluster_id=]] route.
func GetCellsAliases(ctx context.Context, r Request, api *API) *JSONResponse {
	cellsAliases, err := api.server.GetCellsAliases(ctx, &vtadminpb.GetCellsAliasesRequest{
		ClusterIds: r.URL.Query()["cluster_id"],
	})

	return NewJSONResponse(cellsAliases, err)
}
