/*
Copyright 2024 The Vitess Authors.

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
	"io"

	"github.com/gorilla/mux"

	"vitess.io/vitess/go/vt/vtadmin/errors"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

// ApplySchema implements the http wrapper for POST /migration/{cluster_id}/{keyspace}/.
func ApplySchema(ctx context.Context, r Request, api *API) *JSONResponse {
	decoder := json.NewDecoder(r.Body)
	defer r.Body.Close()

	var req vtctldatapb.ApplySchemaRequest
	if err := decoder.Decode(&req); err != nil {
		return NewJSONResponse(nil, &errors.BadRequest{
			Err: err,
		})
	}

	vars := mux.Vars(r.Request)
	req.Keyspace = vars["keyspace"]

	resp, err := api.server.ApplySchema(ctx, &vtadminpb.ApplySchemaRequest{
		ClusterId: vars["cluster_id"],
		Request:   &req,
	})

	return NewJSONResponse(resp, err)
}

// CancelSchemaMigration implements the http wrapper for /migration/{cluster_id}/{keyspace}/cancel[?uuid].
func CancelSchemaMigration(ctx context.Context, r Request, api *API) *JSONResponse {
	vars := mux.Vars(r.Request)

	resp, err := api.server.CancelSchemaMigration(ctx, &vtadminpb.CancelSchemaMigrationRequest{
		ClusterId: vars["cluster_id"],
		Request: &vtctldatapb.CancelSchemaMigrationRequest{
			Keyspace: vars["keyspace"],
			Uuid:     r.URL.Query().Get("uuid"),
		},
	})

	return NewJSONResponse(resp, err)
}

// CleanupSchemaMigration implements the http wrapper for /migration/{cluster_id}/{keyspace}/cleanup[?uuid].
func CleanupSchemaMigration(ctx context.Context, r Request, api *API) *JSONResponse {
	vars := mux.Vars(r.Request)

	resp, err := api.server.CleanupSchemaMigration(ctx, &vtadminpb.CleanupSchemaMigrationRequest{
		ClusterId: vars["cluster_id"],
		Request: &vtctldatapb.CleanupSchemaMigrationRequest{
			Keyspace: vars["keyspace"],
			Uuid:     r.URL.Query().Get("uuid"),
		},
	})

	return NewJSONResponse(resp, err)
}

// CompleteSchemaMigration implements the http wrapper for /migration/{cluster_id}/{keyspace}/complete[?uuid].
func CompleteSchemaMigration(ctx context.Context, r Request, api *API) *JSONResponse {
	vars := mux.Vars(r.Request)

	resp, err := api.server.CompleteSchemaMigration(ctx, &vtadminpb.CompleteSchemaMigrationRequest{
		ClusterId: vars["cluster_id"],
		Request: &vtctldatapb.CompleteSchemaMigrationRequest{
			Keyspace: vars["keyspace"],
			Uuid:     r.URL.Query().Get("uuid"),
		},
	})

	return NewJSONResponse(resp, err)
}

// GetSchemaMigrations implements the http wrapper for /migrations/.
func GetSchemaMigrations(ctx context.Context, r Request, api *API) *JSONResponse {
	decoder := json.NewDecoder(r.Body)
	defer r.Body.Close()

	var req vtadminpb.GetSchemaMigrationsRequest
	if err := decoder.Decode(&req); err != nil && err != io.EOF {
		return NewJSONResponse(nil, &errors.BadRequest{
			Err: err,
		})
	}

	resp, err := api.server.GetSchemaMigrations(ctx, &req)
	return NewJSONResponse(resp, err)
}

// LaunchSchemaMigration implements the http wrapper for /migration/{cluster_id}/{keyspace}/launch[?uuid].
func LaunchSchemaMigration(ctx context.Context, r Request, api *API) *JSONResponse {
	vars := mux.Vars(r.Request)

	resp, err := api.server.LaunchSchemaMigration(ctx, &vtadminpb.LaunchSchemaMigrationRequest{
		ClusterId: vars["cluster_id"],
		Request: &vtctldatapb.LaunchSchemaMigrationRequest{
			Keyspace: vars["keyspace"],
			Uuid:     r.URL.Query().Get("uuid"),
		},
	})

	return NewJSONResponse(resp, err)
}

// RetrySchemaMigration implements the http wrapper for /migration/{cluster_id}/{keyspace}/retry[?uuid].
func RetrySchemaMigration(ctx context.Context, r Request, api *API) *JSONResponse {
	vars := mux.Vars(r.Request)

	resp, err := api.server.RetrySchemaMigration(ctx, &vtadminpb.RetrySchemaMigrationRequest{
		ClusterId: vars["cluster_id"],
		Request: &vtctldatapb.RetrySchemaMigrationRequest{
			Keyspace: vars["keyspace"],
			Uuid:     r.URL.Query().Get("uuid"),
		},
	})

	return NewJSONResponse(resp, err)
}
