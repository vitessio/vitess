package http

import (
	"context"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

// GetClusters implements the http wrapper for /clusters
func GetClusters(ctx context.Context, r Request, api *API) *JSONResponse {
	clusters, err := api.server.GetClusters(ctx, &vtadminpb.GetClustersRequest{})
	return NewJSONResponse(clusters, err)
}
