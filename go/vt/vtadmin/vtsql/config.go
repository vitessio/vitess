package vtsql

import "vitess.io/vitess/go/vt/vtadmin/cluster/discovery"

// Config represents the options that modify the behavior of a vtqsl.VTGateProxy.
type Config struct {
	Discovery     discovery.Discovery
	DiscoveryTags []string
	Credentials   Credentials
}
