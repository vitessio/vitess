package http

import (
	"context"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

// VTExplain implements the http wrapper for /vtexplain?cluster=&keyspace=&sql=
func VTExplain(ctx context.Context, r Request, api *API) *JSONResponse {
	query := r.URL.Query()
	res, err := api.server.VTExplain(ctx, &vtadminpb.VTExplainRequest{
		Cluster:  query.Get("cluster"),
		Keyspace: query.Get("keyspace"),
		Sql:      query.Get("sql"),
	})
	return NewJSONResponse(res, err)
}
