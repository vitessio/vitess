package http

import (
	"context"

	"vitess.io/vitess/go/vt/concurrency"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

// GetBackups implements the http wrapper for /backups[?cluster=[&cluster=]].
func GetBackups(ctx context.Context, r Request, api *API) *JSONResponse {
	query := r.URL.Query()

	rec := concurrency.AllErrorRecorder{} // Aggregate any BadRequest type errors

	limit, err := r.ParseQueryParamAsUint32("limit", 0)
	if err != nil {
		rec.RecordError(err)
	}

	detailed, err := r.ParseQueryParamAsBool("detailed", true)
	if err != nil {
		rec.RecordError(err)
	}

	detailedLimit, err := r.ParseQueryParamAsUint32("detailed_limit", 3)
	if err != nil {
		rec.RecordError(err)
	}

	if rec.HasErrors() {
		return NewJSONResponse(nil, rec.Error())
	}

	backups, err := api.server.GetBackups(ctx, &vtadminpb.GetBackupsRequest{
		ClusterIds:     query["cluster"],
		Keyspaces:      query["keyspace"],
		KeyspaceShards: query["keyspace_shard"],
		RequestOptions: &vtctldatapb.GetBackupsRequest{
			Limit:         limit,
			Detailed:      detailed,
			DetailedLimit: detailedLimit,
		},
	})

	return NewJSONResponse(backups, err)
}
