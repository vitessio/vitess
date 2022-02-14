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

	includeNonServingShards, err := r.ParseQueryParamAsBool("include_non_serving_shards", false)
	if err != nil {
		return nil, err
	}

	return &vtadminpb.GetSchemaTableSizeOptions{
		AggregateSizes:          aggregateSizes,
		IncludeNonServingShards: includeNonServingShards,
	}, nil
}
