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
	Alias           string
	MetricQuery     string
	Tablet          *topodatapb.Tablet
	CacheMillis     int
	QueryInProgress int64
}

// Probes maps tablet aliases to probe(s)
type Probes map[string](*Probe)

// ClusterProbes has the probes for a specific cluster
type ClusterProbes struct {
	ClusterName          string
	IgnoreHostsCount     int
	IgnoreHostsThreshold float64
	TabletProbes         Probes
}

// NewProbes creates Probes
func NewProbes() Probes {
	return Probes{}
}

// NewProbe creates Probe
func NewProbe() *Probe {
	return &Probe{}
}

// String returns a human readable string of this struct
func (p *Probe) String() string {
	return fmt.Sprintf("probe alias=%s", p.Alias)
}
