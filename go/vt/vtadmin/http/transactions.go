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

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

// GetUnresolvedTransactions implements the http wrapper for the
// /transactions/{cluster_id}/{keyspace}[?abandon_age=] route.
func GetUnresolvedTransactions(ctx context.Context, r Request, api *API) *JSONResponse {
	vars := r.Vars()

	abandonAge, err := r.ParseQueryParamAsInt64("abandon_age", 0)
	if err != nil {
		return NewJSONResponse(nil, err)
	}

	res, err := api.server.GetUnresolvedTransactions(ctx, &vtadminpb.GetUnresolvedTransactionsRequest{
		ClusterId:  vars["cluster_id"],
		Keyspace:   vars["keyspace"],
		AbandonAge: abandonAge,
	})

	return NewJSONResponse(res, err)
}

// ConcludeTransaction implements the http wrapper for the
// /transaction/{cluster_id}/{dtid}/conclude route.
func ConcludeTransaction(ctx context.Context, r Request, api *API) *JSONResponse {
	vars := r.Vars()

	res, err := api.server.ConcludeTransaction(ctx, &vtadminpb.ConcludeTransactionRequest{
		ClusterId: vars["cluster_id"],
		Dtid:      vars["dtid"],
	})

	return NewJSONResponse(res, err)
}
