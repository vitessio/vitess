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

package vtadmin

import (
	"context"

	"vitess.io/vitess/go/vt/vtadmin/cluster"
	"vitess.io/vitess/go/vt/vtadmin/rbac"
)

// debugAPI wraps a vtadmin API for use in the vtadmin/http/debug package
// endpoints.
type debugAPI struct {
	api *API
}

// Cluster is part of the debug.API interface.
func (dapi *debugAPI) Cluster(ctx context.Context, id string) (*cluster.Cluster, bool) {
	dapi.api.clusterMu.Lock()
	defer dapi.api.clusterMu.Unlock()

	c, ok := dapi.api.clusterMap[id]
	if !ok || !dapi.api.authz.IsAuthorized(ctx, c.ID, rbac.ClusterResource, rbac.GetAction) {
		return nil, false
	}

	return c, true
}

// Clusters is part of the debug.API interface.
func (dapi *debugAPI) Clusters(ctx context.Context) []*cluster.Cluster {
	dapi.api.clusterMu.Lock()
	defer dapi.api.clusterMu.Unlock()

	clusters := make([]*cluster.Cluster, 0, len(dapi.api.clusters))
	for _, c := range dapi.api.clusters {
		if dapi.api.authz.IsAuthorized(ctx, c.ID, rbac.ClusterResource, rbac.GetAction) {
			clusters = append(clusters, c)
		}
	}

	return clusters
}
