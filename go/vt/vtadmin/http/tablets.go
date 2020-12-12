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
