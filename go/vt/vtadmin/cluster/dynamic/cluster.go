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
	"encoding/base64"
	"strings"

	"vitess.io/vitess/go/vt/vtadmin/cluster"
)

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
//	c, id, err := dynamic.ClusterFromString(ctx, s)
//	if id == "" {
//		// handle err, do not use `c`.
//	}
//	if err != nil {
//		// log error. if desired, lookup the existing cluster with ID: `id`
//	}
//	// Use `c` (or existing cluster with ID == `id`) based on the dynamic cluster
//	api.WithCluster(c, id).DoAThing()
func ClusterFromString(ctx context.Context, s string) (c *cluster.Cluster, id string, err error) {
	cfg, id, err := cluster.LoadConfig(base64.NewDecoder(base64.StdEncoding, strings.NewReader(s)), "json")
	if err != nil {
		return nil, id, err
	}

	c, err = cfg.Cluster(ctx)
	return c, id, err
}
