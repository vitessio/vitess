/*
 Copyright 2017 GitHub Inc.

 Licensed under MIT License. See https://github.com/github/freno/blob/master/LICENSE
*/

package mysql

import (
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/base"
)

// ClusterInstanceKey combines a cluster name with an instance key
type ClusterInstanceKey struct {
	ClusterName string
	Key         InstanceKey
}

// GetClusterInstanceKey creates a ClusterInstanceKey object
func GetClusterInstanceKey(clusterName string, key *InstanceKey) ClusterInstanceKey {
	return ClusterInstanceKey{ClusterName: clusterName, Key: *key}
}

// InstanceMetricResultMap maps a cluster-instance to a result
type InstanceMetricResultMap map[ClusterInstanceKey]base.MetricResult

// Inventory has the operational data about probes, their metrics, and relevant configuration
type Inventory struct {
	ClustersProbes       map[string](*Probes)
	IgnoreHostsCount     map[string]int
	IgnoreHostsThreshold map[string]float64
	InstanceKeyMetrics   InstanceMetricResultMap
}

// NewInventory creates a Inventory
func NewInventory() *Inventory {
	inventory := &Inventory{
		ClustersProbes:       make(map[string](*Probes)),
		IgnoreHostsCount:     make(map[string]int),
		IgnoreHostsThreshold: make(map[string]float64),
		InstanceKeyMetrics:   make(map[ClusterInstanceKey]base.MetricResult),
	}
	return inventory
}
