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
