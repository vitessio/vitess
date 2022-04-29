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

import "vitess.io/vitess/go/vt/vtadmin/cluster"

// debugAPI wraps a vtadmin API for use in the vtadmin/http/debug package
// endpoints.
type debugAPI struct {
	api *API
}

// Cluster is part of the debug.API interface.
func (dapi *debugAPI) Cluster(id string) (*cluster.Cluster, bool) {
	dapi.api.clusterMu.Lock()
	defer dapi.api.clusterMu.Unlock()

	c, ok := dapi.api.clusterMap[id]
	return c, ok
}

// Clusters is part of the debug.API interface.
func (dapi *debugAPI) Clusters() []*cluster.Cluster {
	dapi.api.clusterMu.Lock()
	defer dapi.api.clusterMu.Unlock()

	return dapi.api.clusters
}
