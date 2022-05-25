/*
Copyright 2020 The Vitess Authors.

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

	alias, err := vars.GetTabletAlias("tablet")
	if err != nil {
		return NewJSONResponse(nil, err)
	}

	tablet, err := api.server.GetTablet(ctx, &vtadminpb.GetTabletRequest{
		Alias:      alias,
		ClusterIds: r.URL.Query()["cluster"],
	})

	return NewJSONResponse(tablet, err)
}

func DeleteTablet(ctx context.Context, r Request, api *API) *JSONResponse {
	vars := r.Vars()

	alias, err := vars.GetTabletAlias("tablet")
	if err != nil {
		return NewJSONResponse(nil, err)
	}

	allowPrimary, err := r.ParseQueryParamAsBool("allow_primary", false)
	if err != nil {
		return NewJSONResponse(nil, err)
	}

	deleted, err := api.server.DeleteTablet(ctx, &vtadminpb.DeleteTabletRequest{
		Alias:        alias,
		AllowPrimary: allowPrimary,
		ClusterIds:   r.URL.Query()["cluster"],
	})

	return NewJSONResponse(deleted, err)
}

// PingTablet checks that the specified tablet is awake and responding to RPCs. This command can be blocked by other in-flight operations.
func PingTablet(ctx context.Context, r Request, api *API) *JSONResponse {
	vars := r.Vars()

	alias, err := vars.GetTabletAlias("tablet")
	if err != nil {
		return NewJSONResponse(nil, err)
	}

	ping, err := api.server.PingTablet(ctx, &vtadminpb.PingTabletRequest{
		Alias:      alias,
		ClusterIds: r.URL.Query()["cluster"],
	})

	return NewJSONResponse(ping, err)
}

// RefreshState reloads the tablet record on the specified tablet.
func RefreshState(ctx context.Context, r Request, api *API) *JSONResponse {
	vars := r.Vars()

	alias, err := vars.GetTabletAlias("tablet")
	if err != nil {
		return NewJSONResponse(nil, err)
	}

	result, err := api.server.RefreshState(ctx, &vtadminpb.RefreshStateRequest{
		Alias:      alias,
		ClusterIds: r.URL.Query()["cluster"],
	})

	return NewJSONResponse(result, err)
}

// ReparentTablet reparents a tablet to the current primary in the shard. This
// only works if the current replica position matches the last known reparent
// action.
func ReparentTablet(ctx context.Context, r Request, api *API) *JSONResponse {
	vars := r.Vars()

	alias, err := vars.GetTabletAlias("tablet")
	if err != nil {
		return NewJSONResponse(nil, err)
	}

	result, err := api.server.ReparentTablet(ctx, &vtadminpb.ReparentTabletRequest{
		Alias:      alias,
		ClusterIds: r.URL.Query()["cluster"],
	})

	return NewJSONResponse(result, err)
}

// RunHealthCheck runs a healthcheck on the tablet and returns the result.
func RunHealthCheck(ctx context.Context, r Request, api *API) *JSONResponse {
	vars := r.Vars()

	alias, err := vars.GetTabletAlias("tablet")
	if err != nil {
		return NewJSONResponse(nil, err)
	}

	result, err := api.server.RunHealthCheck(ctx, &vtadminpb.RunHealthCheckRequest{
		Alias:      alias,
		ClusterIds: r.URL.Query()["cluster"],
	})

	return NewJSONResponse(result, err)
}

// SetReadOnly sets the tablet to read only mode
func SetReadOnly(ctx context.Context, r Request, api *API) *JSONResponse {
	vars := r.Vars()

	alias, err := vars.GetTabletAlias("tablet")
	if err != nil {
		return NewJSONResponse(nil, err)
	}

	result, err := api.server.SetReadOnly(ctx, &vtadminpb.SetReadOnlyRequest{
		Alias:      alias,
		ClusterIds: r.URL.Query()["cluster"],
	})

	return NewJSONResponse(result, err)
}

// SetReadWrite sets the tablet to read write mode
func SetReadWrite(ctx context.Context, r Request, api *API) *JSONResponse {
	vars := r.Vars()

	alias, err := vars.GetTabletAlias("tablet")
	if err != nil {
		return NewJSONResponse(nil, err)
	}

	result, err := api.server.SetReadWrite(ctx, &vtadminpb.SetReadWriteRequest{
		Alias:      alias,
		ClusterIds: r.URL.Query()["cluster"],
	})

	return NewJSONResponse(result, err)
}

// StartReplication starts replication on the specified tablet.
func StartReplication(ctx context.Context, r Request, api *API) *JSONResponse {
	vars := r.Vars()

	alias, err := vars.GetTabletAlias("tablet")
	if err != nil {
		return NewJSONResponse(nil, err)
	}

	result, err := api.server.StartReplication(ctx, &vtadminpb.StartReplicationRequest{
		Alias:      alias,
		ClusterIds: r.URL.Query()["cluster"],
	})

	return NewJSONResponse(result, err)
}

// StartReplication stops replication on the specified tablet.
func StopReplication(ctx context.Context, r Request, api *API) *JSONResponse {
	vars := r.Vars()

	alias, err := vars.GetTabletAlias("tablet")
	if err != nil {
		return NewJSONResponse(nil, err)
	}

	result, err := api.server.StopReplication(ctx, &vtadminpb.StopReplicationRequest{
		Alias:      alias,
		ClusterIds: r.URL.Query()["cluster"],
	})

	return NewJSONResponse(result, err)
}

// TabletExternallyReparented implements the http wrapper for POST /tablet/{tablet}/tablet_externally_reparented.
//
// Query params:
// - `cluster`: repeated list of clusterIDs to limit the request to.
//
// POST body is unused; this endpoint takes no additional options.
func TabletExternallyReparented(ctx context.Context, r Request, api *API) *JSONResponse {
	vars := r.Vars()

	alias, err := vars.GetTabletAlias("tablet")
	if err != nil {
		return NewJSONResponse(nil, err)
	}

	result, err := api.server.TabletExternallyReparented(ctx, &vtadminpb.TabletExternallyReparentedRequest{
		Alias:      alias,
		ClusterIds: r.URL.Query()["cluster"],
	})
	return NewJSONResponse(result, err)
}
