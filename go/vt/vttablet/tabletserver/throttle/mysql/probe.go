/*
 Copyright 2017 GitHub Inc.

 Licensed under MIT License. See https://github.com/github/freno/blob/master/LICENSE
*/

package mysql

import (
	"fmt"
	"net"
)

const maxPoolConnections = 3
const maxIdleConnections = 3
const timeoutMillis = 1000

// Probe is the minimal configuration required to connect to a MySQL server
type Probe struct {
	Key             InstanceKey
	User            string
	Password        string
	MetricQuery     string
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

// DuplicateCredentials creates a new connection config with given key and with same credentials as this config
func (p *Probe) DuplicateCredentials(key InstanceKey) *Probe {
	config := &Probe{
		Key:      key,
		User:     p.User,
		Password: p.Password,
	}
	return config
}

// Duplicate duplicates this probe, including credentials
func (p *Probe) Duplicate() *Probe {
	return p.DuplicateCredentials(p.Key)
}

// String returns a human readable string of this struct
func (p *Probe) String() string {
	return fmt.Sprintf("%s, user=%s", p.Key.DisplayString(), p.User)
}

// Equals checks if this probe has same instance key as another
func (p *Probe) Equals(other *Probe) bool {
	return p.Key.Equals(&other.Key)
}

// GetDBUri returns the DB URI for the mysql server indicated by this probe
func (p *Probe) GetDBUri(databaseName string) string {
	hostname := p.Key.Hostname
	var ip = net.ParseIP(hostname)
	if (ip != nil) && (ip.To4() == nil) {
		// Wrap IPv6 literals in square brackets
		hostname = fmt.Sprintf("[%s]", hostname)
	}
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?interpolateParams=true&charset=utf8mb4,utf8,latin1&timeout=%dms", p.User, p.Password, hostname, p.Key.Port, databaseName, timeoutMillis)
}
