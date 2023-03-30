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
	"encoding/json"
	"fmt"

	"github.com/gorilla/mux"

	"vitess.io/vitess/go/sets"
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
	shardList = sets.List(sets.New[string](shardList...))
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

// EmergencyFailoverShard implements the http wrapper for
// POST /shard/{cluster_id}/{keyspace}/{shard}/emergency_failover.
//
// Query params: none
//
// POST body is unmarshalled as vtctldatapb.EmergencyReparentShardRequest, but
// the Keyspace and Shard fields are ignored (coming instead from the route).
func EmergencyFailoverShard(ctx context.Context, r Request, api *API) *JSONResponse {
	decoder := json.NewDecoder(r.Body)
	defer r.Body.Close()

	var options vtctldatapb.EmergencyReparentShardRequest
	if err := decoder.Decode(&options); err != nil {
		return NewJSONResponse(nil, &errors.BadRequest{
			Err: err,
		})
	}

	vars := r.Vars()
	options.Keyspace = vars["keyspace"]
	options.Shard = vars["shard"]

	result, err := api.server.EmergencyFailoverShard(ctx, &vtadminpb.EmergencyFailoverShardRequest{
		ClusterId: vars["cluster_id"],
		Options:   &options,
	})
	return NewJSONResponse(result, err)
}

// PlannedFailoverShard implements the http wrapper for
// POST /shard/{cluster_id}/{keyspace}/{shard}/planned_failover.
//
// Query params: none
//
// POST body is unmarshalled as vtctldatapb.PlannedReparentShardRequest, but
// the Keyspace and Shard fields are ignored (coming instead from the route).
func PlannedFailoverShard(ctx context.Context, r Request, api *API) *JSONResponse {
	decoder := json.NewDecoder(r.Body)
	defer r.Body.Close()

	var options vtctldatapb.PlannedReparentShardRequest
	if err := decoder.Decode(&options); err != nil {
		return NewJSONResponse(nil, &errors.BadRequest{
			Err: err,
		})
	}

	vars := r.Vars()
	options.Keyspace = vars["keyspace"]
	options.Shard = vars["shard"]

	result, err := api.server.PlannedFailoverShard(ctx, &vtadminpb.PlannedFailoverShardRequest{
		ClusterId: vars["cluster_id"],
		Options:   &options,
	})
	return NewJSONResponse(result, err)
}

// ReloadSchemaShard implements the http wrapper for
// PUT /shard/{cluster_id}/{keyspace}/{shard}/reload_schema_shard
//
// Query params: none
//
// Body params:
// - wait_position: string
// - include_primary: bool
// - concurrency: uint32
func ReloadSchemaShard(ctx context.Context, r Request, api *API) *JSONResponse {
	decoder := json.NewDecoder(r.Body)
	defer r.Body.Close()

	var params struct {
		WaitPosition   string `json:"wait_position"`
		IncludePrimary bool   `json:"include_primary"`
		Concurrency    uint32 `json:"concurrency"`
	}

	if err := decoder.Decode(&params); err != nil {
		return NewJSONResponse(nil, &errors.BadRequest{
			Err: err,
		})
	}

	vars := r.Vars()

	result, err := api.server.ReloadSchemaShard(ctx, &vtadminpb.ReloadSchemaShardRequest{
		ClusterId:      vars["cluster_id"],
		Keyspace:       vars["keyspace"],
		Shard:          vars["shard"],
		Concurrency:    params.Concurrency,
		IncludePrimary: params.IncludePrimary,
		WaitPosition:   params.WaitPosition,
	})
	return NewJSONResponse(result, err)
}

// ValidateShard implements the http wrapper for
// PUT /shard/{cluster_id}/{keyspace}/{shard}/validate
//
// Query params: none
//
// Body params:
// - ping_tablets: bool
func ValidateShard(ctx context.Context, r Request, api *API) *JSONResponse {
	decoder := json.NewDecoder(r.Body)
	defer r.Body.Close()

	var params struct {
		PingTablets bool `json:"ping_tablets"`
	}

	if err := decoder.Decode(&params); err != nil {
		return NewJSONResponse(nil, &errors.BadRequest{
			Err: err,
		})
	}

	vars := r.Vars()

	result, err := api.server.ValidateShard(ctx, &vtadminpb.ValidateShardRequest{
		ClusterId:   vars["cluster_id"],
		Keyspace:    vars["keyspace"],
		Shard:       vars["shard"],
		PingTablets: params.PingTablets,
	})

	return NewJSONResponse(result, err)
}

// ValidateVersionShard implements the http wrapper for
// PUT /shard/{cluster_id}/{keyspace}/{shard}/validate_version
//
// Query params: none
//
// Body params: none
func ValidateVersionShard(ctx context.Context, r Request, api *API) *JSONResponse {
	vars := r.Vars()

	result, err := api.server.ValidateVersionShard(ctx, &vtadminpb.ValidateVersionShardRequest{
		ClusterId: vars["cluster_id"],
		Keyspace:  vars["keyspace"],
		Shard:     vars["shard"],
	})

	return NewJSONResponse(result, err)
}
