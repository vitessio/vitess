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
