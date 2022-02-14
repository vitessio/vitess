package http

import (
	"context"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

// GetVtctlds implements the http wrapper for /vtctlds[?cluster=[&cluster=]].
func GetVtctlds(ctx context.Context, r Request, api *API) *JSONResponse {
	vtctlds, err := api.server.GetVtctlds(ctx, &vtadminpb.GetVtctldsRequest{
		ClusterIds: r.URL.Query()["cluster"],
	})

	return NewJSONResponse(vtctlds, err)
}
