/*
Copyright 2024 The Vitess Authors.

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

// VDiffCreate implements the http wrapper for the
// /vdiff/{cluster_id} route.
func VDiffCreate(ctx context.Context, r Request, api *API) *JSONResponse {
	vars := r.Vars()

	decoder := json.NewDecoder(r.Body)
	defer r.Body.Close()

	var req vtctldatapb.VDiffCreateRequest
	if err := decoder.Decode(&req); err != nil {
		return NewJSONResponse(nil, &errors.BadRequest{
			Err: err,
		})
	}

	res, err := api.server.VDiffCreate(ctx, &vtadminpb.VDiffCreateRequest{
		ClusterId: vars["cluster_id"],
		Request:   &req,
	})

	return NewJSONResponse(res, err)
}

// VDiffShow implements the http wrapper for the
// /vdiff/{cluster_id}/show route.
func VDiffShow(ctx context.Context, r Request, api *API) *JSONResponse {
	vars := r.Vars()

	decoder := json.NewDecoder(r.Body)
	defer r.Body.Close()

	var req vtctldatapb.VDiffShowRequest
	if err := decoder.Decode(&req); err != nil {
		return NewJSONResponse(nil, &errors.BadRequest{
			Err: err,
		})
	}

	res, err := api.server.VDiffShow(ctx, &vtadminpb.VDiffShowRequest{
		ClusterId: vars["cluster_id"],
		Request:   &req,
	})

	return NewJSONResponse(res, err)
}
