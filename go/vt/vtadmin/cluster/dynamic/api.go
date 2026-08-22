/*
Copyright 2026 The Vitess Authors.

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

package dynamic

import (
	"context"
	"net/http"

	"vitess.io/vitess/go/vt/vtadmin/cluster"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

type (
	// API is the interface dynamic APIs must implement.
	// It is implemented by vtadmin.API.
	API interface {
		vtadminpb.VTAdminServer
		WithCluster(c *cluster.Cluster, id string) API
		Handler() http.Handler
	}

	contextAPI interface {
		API
		WithClusterContext(ctx context.Context, c *cluster.Cluster, id string) API
	}
)

func withCluster(ctx context.Context, api API, c *cluster.Cluster, id string) API {
	if api, ok := api.(contextAPI); ok {
		return api.WithClusterContext(ctx, c, id)
	}
	return api.WithCluster(c, id)
}
