package main

import (
	"github.com/youtube/vitess/go/vt/discovery"
	"github.com/youtube/vitess/go/vt/servenv"
	_ "github.com/youtube/vitess/go/vt/status"
	"github.com/youtube/vitess/go/vt/vtgate"
	"github.com/youtube/vitess/go/vt/vtgate/gateway"
)

// For use by plugins which wish to avoid racing when registering status page parts.
var onStatusRegistered func()

func addStatusParts(vtg *vtgate.VTGate) {
	servenv.AddStatusPart("VSchema", vtgate.VSchemaTemplate, func() interface{} {
		return vtg.VSchemaStats()
	})
	servenv.AddStatusPart("Topology Cache", vtgate.TopoTemplate, func() interface{} {
		return resilientSrvTopoServer.CacheStatus()
	})
	servenv.AddStatusPart("Gateway Status", gateway.StatusTemplate, func() interface{} {
		return vtg.GetGatewayCacheStatus()
	})
	servenv.AddStatusPart("Health Check Cache", discovery.HealthCheckTemplate, func() interface{} {
		return healthCheck.CacheStatus()
	})
	if onStatusRegistered != nil {
		onStatusRegistered()
	}
}
