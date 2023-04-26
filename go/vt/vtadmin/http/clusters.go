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

	"github.com/gorilla/mux"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
	"vitess.io/vitess/go/vt/vtadmin/errors"
)

// GetClusters implements the http wrapper for /clusters
func GetClusters(ctx context.Context, r Request, api *API) *JSONResponse {
	clusters, err := api.server.GetClusters(ctx, &vtadminpb.GetClustersRequest{})
	return NewJSONResponse(clusters, err)
}

// Validate implements the http wrapper for /cluster/{cluster_id}/validate
func Validate(ctx context.Context, r Request, api *API) *JSONResponse {
	vars := mux.Vars(r.Request)
	decoder := json.NewDecoder(r.Body)
	defer r.Body.Close()

	var result struct {
		PingTablets bool `json:"pingTablets"`
	}

	if err := decoder.Decode(&result); err != nil {
		return NewJSONResponse(nil, &errors.BadRequest{
			Err: err,
		})
	}

	resp, err := api.server.Validate(ctx, &vtadminpb.ValidateRequest{
		ClusterId:   vars["cluster_id"],
		PingTablets: result.PingTablets,
	})
	return NewJSONResponse(resp, err)
}

// GetTopologyPath implements the http wrapper for /cluster/{cluster_id}/topology
//
// Query params:
// - path: string
func GetTopologyPath(ctx context.Context, r Request, api *API) *JSONResponse {
	vars := r.Vars()
	query := r.URL.Query()

	result, err := api.server.GetTopologyPath(ctx, &vtadminpb.GetTopologyPathRequest{
		ClusterId: vars["cluster_id"],
		Path:      query["path"][0],
	})
	return NewJSONResponse(result, err)
}
