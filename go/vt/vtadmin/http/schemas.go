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

// FindSchema implements the http wrapper for the
// /schema/{table}[?cluster=[&cluster=]] route.
func FindSchema(ctx context.Context, r Request, api *API) *JSONResponse {
	vars := r.Vars()
	query := r.URL.Query()

	sizeOpts, err := getTableSizeOpts(r)
	if err != nil {
		return NewJSONResponse(nil, err)
	}

	schema, err := api.server.FindSchema(ctx, &vtadminpb.FindSchemaRequest{
		Table:            vars["table"],
		ClusterIds:       query["cluster"],
		TableSizeOptions: sizeOpts,
	})

	return NewJSONResponse(schema, err)
}

// GetSchema implements the http wrapper for the
// /schema/{cluster_id}/{keyspace}/{table} route.
func GetSchema(ctx context.Context, r Request, api *API) *JSONResponse {
	vars := r.Vars()

	sizeOpts, err := getTableSizeOpts(r)
	if err != nil {
		return NewJSONResponse(nil, err)
	}

	schema, err := api.server.GetSchema(ctx, &vtadminpb.GetSchemaRequest{
		ClusterId:        vars["cluster_id"],
		Keyspace:         vars["keyspace"],
		Table:            vars["table"],
		TableSizeOptions: sizeOpts,
	})

	return NewJSONResponse(schema, err)
}

// GetSchemas implements the http wrapper for the /schemas[?cluster=[&cluster=]
// route.
func GetSchemas(ctx context.Context, r Request, api *API) *JSONResponse {
	sizeOpts, err := getTableSizeOpts(r)
	if err != nil {
		return NewJSONResponse(nil, err)
	}

	schemas, err := api.server.GetSchemas(ctx, &vtadminpb.GetSchemasRequest{
		ClusterIds:       r.URL.Query()["cluster"],
		TableSizeOptions: sizeOpts,
	})

	return NewJSONResponse(schemas, err)
}

func getTableSizeOpts(r Request) (*vtadminpb.GetSchemaTableSizeOptions, error) {
	aggregateSizes, err := r.ParseQueryParamAsBool("aggregate_sizes", true)
	if err != nil {
		return nil, err
	}

	return &vtadminpb.GetSchemaTableSizeOptions{
		AggregateSizes: aggregateSizes,
	}, nil
}
