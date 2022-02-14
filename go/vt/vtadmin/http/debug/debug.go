package debug

import "vitess.io/vitess/go/vt/vtadmin/cluster"

// API defines the interface needed to provide debug info for a vtadmin.API.
// This is implemented by a private wrapper struct in package vtadmin, to
// prevent debug needs from polluting the public interface of the actual API.
type API interface {
	// Cluster returns a cluster by id, with the same semantics as a map lookup.
	Cluster(id string) (*cluster.Cluster, bool)
	// Clusters returns a slice of all clusters in the API.
	Clusters() []*cluster.Cluster
}
