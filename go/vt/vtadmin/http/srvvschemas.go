package http

import (
	"context"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

// GetSrvVSchema implements the http wrapper for the /api/srvvschema/{cluster_id}/{cell} route.
func GetSrvVSchema(ctx context.Context, r Request, api *API) *JSONResponse {
	vars := r.Vars()

	sv, err := api.server.GetSrvVSchema(ctx, &vtadminpb.GetSrvVSchemaRequest{
		Cell:      vars["cell"],
		ClusterId: vars["cluster_id"],
	})

	return NewJSONResponse(sv, err)
}

// GetSrvVSchemas implements the http wrapper for the /api/srvvschemas route.
func GetSrvVSchemas(ctx context.Context, r Request, api *API) *JSONResponse {
	query := r.URL.Query()

	svs, err := api.server.GetSrvVSchemas(ctx, &vtadminpb.GetSrvVSchemasRequest{
		Cells:      query["cell"],
		ClusterIds: query["cluster"],
	})

	return NewJSONResponse(svs, err)
}
