package vtadmin

import "vitess.io/vitess/go/vt/vtadmin/cluster"

// debugAPI wraps a vtadmin API for use in the vtadmin/http/debug package
// endpoints.
type debugAPI struct {
	api *API
}

// Cluster is part of the debug.API interface.
func (dapi *debugAPI) Cluster(id string) (*cluster.Cluster, bool) {
	c, ok := dapi.api.clusterMap[id]
	return c, ok
}

// Clusters is part of the debug.API interface.
func (dapi *debugAPI) Clusters() []*cluster.Cluster { return dapi.api.clusters }
