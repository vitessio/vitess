package http

import (
	"context"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

// GetGates implements the http wrapper for /gates[?cluster=[&cluster=]].
func GetGates(ctx context.Context, r Request, api *API) *JSONResponse {
	gates, err := api.server.GetGates(ctx, &vtadminpb.GetGatesRequest{
		ClusterIds: r.URL.Query()["cluster"],
	})

	return NewJSONResponse(gates, err)
}
