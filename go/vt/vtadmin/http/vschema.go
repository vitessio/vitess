/*
Copyright 2025 The Vitess Authors.

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

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	"vitess.io/vitess/go/vt/vtadmin/errors"
)

// VSchemaPublish implements the http wrapper for the
// /vschema/{cluster_id}/publish route.
func VSchemaPublish(ctx context.Context, r Request, api *API) *JSONResponse {
	vars := r.Vars()

	decoder := json.NewDecoder(r.Body)
	defer r.Body.Close()

	var req vtctldatapb.VSchemaPublishRequest
	if err := decoder.Decode(&req); err != nil {
		return NewJSONResponse(nil, &errors.BadRequest{
			Err: err,
		})
	}

	resp, err := api.server.VSchemaPublish(ctx, &vtadminpb.VSchemaPublishRequest{
		ClusterId: vars["cluster_id"],
		Request:   &req,
	})

	return NewJSONResponse(resp, err)
}

// VSchemaAddVindex implements the http wrapper for the
// /vschema/{cluster_id}/add_vindex route.
func VSchemaAddVindex(ctx context.Context, r Request, api *API) *JSONResponse {
	vars := r.Vars()

	decoder := json.NewDecoder(r.Body)
	defer r.Body.Close()

	var req vtctldatapb.VSchemaAddVindexRequest
	if err := decoder.Decode(&req); err != nil {
		return NewJSONResponse(nil, &errors.BadRequest{
			Err: err,
		})
	}

	resp, err := api.server.VSchemaAddVindex(ctx, &vtadminpb.VSchemaAddVindexRequest{
		ClusterId: vars["cluster_id"],
		Request:   &req,
	})

	return NewJSONResponse(resp, err)
}

// VSchemaRemoveVindex implements the http wrapper for the
// /vschema/{cluster_id}/remove_vindex route.
func VSchemaRemoveVindex(ctx context.Context, r Request, api *API) *JSONResponse {
	vars := r.Vars()

	decoder := json.NewDecoder(r.Body)
	defer r.Body.Close()

	var req vtctldatapb.VSchemaRemoveVindexRequest
	if err := decoder.Decode(&req); err != nil {
		return NewJSONResponse(nil, &errors.BadRequest{
			Err: err,
		})
	}

	resp, err := api.server.VSchemaRemoveVindex(ctx, &vtadminpb.VSchemaRemoveVindexRequest{
		ClusterId: vars["cluster_id"],
		Request:   &req,
	})

	return NewJSONResponse(resp, err)
}

// VSchemaAddLookupVindex implements the http wrapper for the
// /vschema/{cluster_id}/add_lookup_vindex route.
func VSchemaAddLookupVindex(ctx context.Context, r Request, api *API) *JSONResponse {
	vars := r.Vars()

	decoder := json.NewDecoder(r.Body)
	defer r.Body.Close()

	var req vtctldatapb.VSchemaAddLookupVindexRequest
	if err := decoder.Decode(&req); err != nil {
		return NewJSONResponse(nil, &errors.BadRequest{
			Err: err,
		})
	}

	resp, err := api.server.VSchemaAddLookupVindex(ctx, &vtadminpb.VSchemaAddLookupVindexRequest{
		ClusterId: vars["cluster_id"],
		Request:   &req,
	})

	return NewJSONResponse(resp, err)
}

// VSchemaAddTables implements the http wrapper for the
// /vschema/{cluster_id}/add_tables route.
func VSchemaAddTables(ctx context.Context, r Request, api *API) *JSONResponse {
	vars := r.Vars()

	decoder := json.NewDecoder(r.Body)
	defer r.Body.Close()

	var req vtctldatapb.VSchemaAddTablesRequest
	if err := decoder.Decode(&req); err != nil {
		return NewJSONResponse(nil, &errors.BadRequest{
			Err: err,
		})
	}

	resp, err := api.server.VSchemaAddTables(ctx, &vtadminpb.VSchemaAddTablesRequest{
		ClusterId: vars["cluster_id"],
		Request:   &req,
	})

	return NewJSONResponse(resp, err)
}

// VSchemaRemoveTables implements the http wrapper for the
// /vschema/{cluster_id}/remove_tables route.
func VSchemaRemoveTables(ctx context.Context, r Request, api *API) *JSONResponse {
	vars := r.Vars()

	decoder := json.NewDecoder(r.Body)
	defer r.Body.Close()

	var req vtctldatapb.VSchemaRemoveTablesRequest
	if err := decoder.Decode(&req); err != nil {
		return NewJSONResponse(nil, &errors.BadRequest{
			Err: err,
		})
	}

	resp, err := api.server.VSchemaRemoveTables(ctx, &vtadminpb.VSchemaRemoveTablesRequest{
		ClusterId: vars["cluster_id"],
		Request:   &req,
	})

	return NewJSONResponse(resp, err)
}

// VSchemaSetPrimaryVindex implements the http wrapper for the
// /vschema/{cluster_id}/set_primary_vindex route.
func VSchemaSetPrimaryVindex(ctx context.Context, r Request, api *API) *JSONResponse {
	vars := r.Vars()

	decoder := json.NewDecoder(r.Body)
	defer r.Body.Close()

	var req vtctldatapb.VSchemaSetPrimaryVindexRequest
	if err := decoder.Decode(&req); err != nil {
		return NewJSONResponse(nil, &errors.BadRequest{
			Err: err,
		})
	}

	resp, err := api.server.VSchemaSetPrimaryVindex(ctx, &vtadminpb.VSchemaSetPrimaryVindexRequest{
		ClusterId: vars["cluster_id"],
		Request:   &req,
	})

	return NewJSONResponse(resp, err)
}

// VSchemaSetSequence implements the http wrapper for the
// /vschema/{cluster_id}/set_sequence route.
func VSchemaSetSequence(ctx context.Context, r Request, api *API) *JSONResponse {
	vars := r.Vars()

	decoder := json.NewDecoder(r.Body)
	defer r.Body.Close()

	var req vtctldatapb.VSchemaSetSequenceRequest
	if err := decoder.Decode(&req); err != nil {
		return NewJSONResponse(nil, &errors.BadRequest{
			Err: err,
		})
	}

	resp, err := api.server.VSchemaSetSequence(ctx, &vtadminpb.VSchemaSetSequenceRequest{
		ClusterId: vars["cluster_id"],
		Request:   &req,
	})

	return NewJSONResponse(resp, err)
}

// VSchemaSetReference implements the http wrapper for the
// /vschema/{cluster_id}/set_reference route.
func VSchemaSetReference(ctx context.Context, r Request, api *API) *JSONResponse {
	vars := r.Vars()

	decoder := json.NewDecoder(r.Body)
	defer r.Body.Close()

	var req vtctldatapb.VSchemaSetReferenceRequest
	if err := decoder.Decode(&req); err != nil {
		return NewJSONResponse(nil, &errors.BadRequest{
			Err: err,
		})
	}

	resp, err := api.server.VSchemaSetReference(ctx, &vtadminpb.VSchemaSetReferenceRequest{
		ClusterId: vars["cluster_id"],
		Request:   &req,
	})

	return NewJSONResponse(resp, err)
}
