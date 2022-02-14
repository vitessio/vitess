package http

import (
	"context"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

// GetShardReplicationPositions implements the http wrapper for /shard_replication_positions.
// Query params:
//	- cluster: repeated, cluster ID
// 	- keyspace: repeated, keyspace names
//	- keyspace_shard: repeated, keyspace shard names
func GetShardReplicationPositions(ctx context.Context, r Request, api *API) *JSONResponse {
	query := r.URL.Query()

	resp, err := api.server.GetShardReplicationPositions(ctx, &vtadminpb.GetShardReplicationPositionsRequest{
		ClusterIds:     query["cluster"],
		Keyspaces:      query["keyspace"],
		KeyspaceShards: query["keyspace_shard"],
	})

	return NewJSONResponse(resp, err)
}
