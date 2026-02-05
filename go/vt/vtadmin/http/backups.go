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

	"vitess.io/vitess/go/vt/concurrency"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

// GetBackups implements the http wrapper for /backups[?cluster_id=[&cluster_id=]].
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
		ClusterIds:     query["cluster_id"],
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
