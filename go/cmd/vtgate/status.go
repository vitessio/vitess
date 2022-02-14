package main

import (
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/srvtopo"
	_ "vitess.io/vitess/go/vt/status"
	"vitess.io/vitess/go/vt/vtgate"
)

func addStatusParts(vtg *vtgate.VTGate) {
	servenv.AddStatusPart("Executor", vtgate.ExecutorTemplate, func() interface{} {
		return nil
	})
	servenv.AddStatusPart("VSchema", vtgate.VSchemaTemplate, func() interface{} {
		return vtg.VSchemaStats()
	})
	servenv.AddStatusFuncs(srvtopo.StatusFuncs)
	servenv.AddStatusPart("Topology Cache", srvtopo.TopoTemplate, func() interface{} {
		return resilientServer.CacheStatus()
	})
	servenv.AddStatusPart("Gateway Status", vtgate.StatusTemplate, func() interface{} {
		return vtg.GetGatewayCacheStatus()
	})
	if vtgate.UsingLegacyGateway() {
		servenv.AddStatusPart("Health Check Cache", discovery.LegacyHealthCheckTemplate, func() interface{} {
			return legacyHealthCheck.CacheStatus()
		})
	} else {
		servenv.AddStatusPart("Health Check Cache", discovery.HealthCheckTemplate, func() interface{} {
			return vtg.Gateway().TabletsCacheStatus()
		})
	}
}
