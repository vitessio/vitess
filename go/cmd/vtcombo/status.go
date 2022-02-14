package main

import (
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/vtgate"

	_ "vitess.io/vitess/go/vt/status"
)

func addStatusParts(vtg *vtgate.VTGate) {
	// Override and reparse the template string so we can point tablet server urls to a local path.
	*discovery.TabletURLTemplateString = "{{.NamedStatusURL}}"
	discovery.ParseTabletURLTemplateFromFlag()

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
	servenv.AddStatusPart("Health Check Cache", discovery.HealthCheckTemplate, func() interface{} {
		return vtg.Gateway().TabletsCacheStatus()
	})
}
