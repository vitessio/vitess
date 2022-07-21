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
