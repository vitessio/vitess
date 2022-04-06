package dynamic

import (
	"encoding/base64"
	"encoding/json"
	"errors"

	"vitess.io/vitess/go/vt/vtadmin/cluster"
)

// DiscoveryImpl is the name of the discovery implementation used by clusters
// unpacked from ClusterFromString.
const DiscoveryImpl = "dynamic"

// ErrNoID is returned from ClusterFromString when a cluster spec has a missing
// or empty id.
var ErrNoID = errors.New("cannot have cluster without an id")

// ClusterJSON is a struct to unmarshal json strings into dynamic cluster
// configurations.
type ClusterJSON struct {
	Name string `json:"name,omitempty"`
}

// ClusterFromString returns a cluster ID and possibly fully-usable Cluster
// from a base64-encoded JSON spec.
//
// If an error occurs decoding or unmarshalling the string, this function
// returns, respectively, the empty string, a nil Cluster, and a non-nil error.
//
// If the string can be decoded and unmarshalled, then the id will be non-empty,
// but errors may still be returned from instantiating the cluster, for example
// if the configuration in the JSON spec is invalid.
//
// Therefore, callers should handle the return values as follows:
//
//		c, id, err := dynamic.ClusterFromString(s)
//		if id == "" {
//			// handle err, do not use `c`.
//		}
//		if err != nil {
//			// log error. if desired, lookup the existing cluster with ID: `id`
//		}
//		// Use `c` (or existing cluster with ID == `id`) based on the dynamic cluster
//		api.WithCluster(c, id).DoAThing()
//
func ClusterFromString(s string) (c *cluster.Cluster, id string, err error) {
	dec, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, "", err
	}

	var spec ClusterJSON
	if err := json.Unmarshal(dec, &spec); err != nil {
		return nil, "", err
	}

	id = spec.Name
	if id == "" {
		return nil, "", ErrNoID
	}
	cfg := &cluster.Config{
		ID:            id,
		Name:          id,
		DiscoveryImpl: DiscoveryImpl,
		DiscoveryFlagsByImpl: cluster.FlagsByImpl{
			DiscoveryImpl: map[string]string{
				"discovery": string(dec),
			},
		},
	}
	c, err = cfg.Cluster()

	return c, id, err
}
