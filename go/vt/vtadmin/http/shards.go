package http

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/gorilla/mux"
	"k8s.io/apimachinery/pkg/util/sets"

	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtadmin/errors"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

// CreateShard implements the http wrapper for POST /shards/{cluster_id}.
func CreateShard(ctx context.Context, r Request, api *API) *JSONResponse {
	vars := mux.Vars(r.Request)
	decoder := json.NewDecoder(r.Body)
	defer r.Body.Close()

	var req vtctldatapb.CreateShardRequest
	if err := decoder.Decode(&req); err != nil {
		return NewJSONResponse(nil, &errors.BadRequest{
			Err: err,
		})
	}

	resp, err := api.server.CreateShard(ctx, &vtadminpb.CreateShardRequest{
		ClusterId: vars["cluster_id"],
		Options:   &req,
	})
	return NewJSONResponse(resp, err)
}

// DeleteShards implements the http wrapper for DELETE /shards/{cluster_id}.
// Query params:
// - keyspace_shard: required, repeated list of keyspace/shards to delete.
// - recursive: bool
// - even_if_serving: bool
func DeleteShards(ctx context.Context, r Request, api *API) *JSONResponse {
	vars := mux.Vars(r.Request)
	recursive, err := r.ParseQueryParamAsBool("recursive", false)
	if err != nil {
		return NewJSONResponse(nil, err)
	}

	evenIfServing, err := r.ParseQueryParamAsBool("even_if_serving", false)
	if err != nil {
		return NewJSONResponse(nil, err)
	}

	shardList := r.URL.Query()["keyspace_shard"]
	shardList = sets.NewString(shardList...).List()
	shards := make([]*vtctldatapb.Shard, len(shardList))
	for i, kss := range shardList {
		ks, shard, err := topoproto.ParseKeyspaceShard(kss)
		if err != nil {
			return NewJSONResponse(nil, &errors.BadRequest{
				Err: fmt.Errorf("%w: parsing %s at position %d", err, kss, i),
			})
		}

		shards[i] = &vtctldatapb.Shard{
			Keyspace: ks,
			Name:     shard,
			Shard:    &topodatapb.Shard{},
		}
	}

	if len(shards) == 0 {
		return NewJSONResponse(nil, &errors.BadRequest{
			Err: fmt.Errorf("must specify at least one keyspace_shard to delete (got %+v)", shardList),
		})
	}

	resp, err := api.server.DeleteShards(ctx, &vtadminpb.DeleteShardsRequest{
		ClusterId: vars["cluster_id"],
		Options: &vtctldatapb.DeleteShardsRequest{
			Shards:        shards,
			Recursive:     recursive,
			EvenIfServing: evenIfServing,
		},
	})
	return NewJSONResponse(resp, err)
}
