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

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

// GetShardReplicationPositions implements the http wrapper for /shard_replication_positions.
// Query params:
//   - cluster: repeated, cluster ID
//   - keyspace: repeated, keyspace names
//   - keyspace_shard: repeated, keyspace shard names
func GetShardReplicationPositions(ctx context.Context, r Request, api *API) *JSONResponse {
	query := r.URL.Query()

	resp, err := api.server.GetShardReplicationPositions(ctx, &vtadminpb.GetShardReplicationPositionsRequest{
		ClusterIds:     query["cluster"],
		Keyspaces:      query["keyspace"],
		KeyspaceShards: query["keyspace_shard"],
	})

	return NewJSONResponse(resp, err)
}
