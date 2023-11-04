/*
 Copyright 2017 GitHub Inc.

 Licensed under MIT License. See https://github.com/github/freno/blob/master/LICENSE
*/

package mysql

import (
	"fmt"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// Probe is the minimal configuration required to connect to a MySQL server
type Probe struct {
	Key             InstanceKey
	MetricQuery     string
	Tablet          *topodatapb.Tablet
	TabletHost      string
	TabletPort      int
	CacheMillis     int
	QueryInProgress int64
}

// Probes maps instances to probe(s)
type Probes map[InstanceKey](*Probe)

// ClusterProbes has the probes for a specific cluster
type ClusterProbes struct {
	ClusterName          string
	IgnoreHostsCount     int
	IgnoreHostsThreshold float64
	InstanceProbes       *Probes
}

// NewProbes creates Probes
func NewProbes() *Probes {
	return &Probes{}
}

// NewProbe creates Probe
func NewProbe() *Probe {
	config := &Probe{
		Key: InstanceKey{},
	}
	return config
}

// String returns a human readable string of this struct
func (p *Probe) String() string {
	return fmt.Sprintf("%s, tablet=%s:%d", p.Key.DisplayString(), p.TabletHost, p.TabletPort)
}

// Equals checks if this probe has same instance key as another
func (p *Probe) Equals(other *Probe) bool {
	return p.Key.Equals(&other.Key)
}
