/*
Copyright 2020 The Vitess Authors.

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

package sort

import (
	"sort"

	"vitess.io/vitess/go/vt/vtadmin/cluster"
)

// ClustersBy provides an interface to sort Clusters by arbitrary comparison.
type ClustersBy func(c1, c2 *cluster.Cluster) bool

// Sort sorts a slice of Clusters by the given comparison function.
func (by ClustersBy) Sort(clusters []*cluster.Cluster) {
	sorter := &clusterSorter{
		clusters: clusters,
		by:       by,
	}
	sort.Sort(sorter)
}

type clusterSorter struct {
	clusters []*cluster.Cluster
	by       func(c1, c2 *cluster.Cluster) bool
}

func (s clusterSorter) Len() int           { return len(s.clusters) }
func (s clusterSorter) Swap(i, j int)      { s.clusters[i], s.clusters[j] = s.clusters[j], s.clusters[i] }
func (s clusterSorter) Less(i, j int) bool { return s.by(s.clusters[i], s.clusters[j]) }
