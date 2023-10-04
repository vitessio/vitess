/*
 Copyright 2017 GitHub Inc.

 Licensed under MIT License. See https://github.com/github/freno/blob/master/LICENSE
*/

package mysql

import (
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/base"
)

// ClusterTablet combines a cluster name with a tablet alias
type ClusterTablet struct {
	ClusterName string
	Alias       string
}

// GetClusterTablet creates a GetClusterTablet object
func GetClusterTablet(clusterName string, alias string) ClusterTablet {
	return ClusterTablet{ClusterName: clusterName, Alias: alias}
}

// TabletResultMap maps a cluster-tablet to a result
type TabletResultMap map[ClusterTablet]base.MetricResult

// Inventory has the operational data about probes, their metrics, and relevant configuration
type Inventory struct {
	ClustersProbes       map[string](Probes)
	IgnoreHostsCount     map[string]int
	IgnoreHostsThreshold map[string]float64
	TabletMetrics        TabletResultMap
}

// NewInventory creates a Inventory
func NewInventory() *Inventory {
	inventory := &Inventory{
		ClustersProbes:       make(map[string](Probes)),
		IgnoreHostsCount:     make(map[string]int),
		IgnoreHostsThreshold: make(map[string]float64),
		TabletMetrics:        make(map[ClusterTablet]base.MetricResult),
	}
	return inventory
}
