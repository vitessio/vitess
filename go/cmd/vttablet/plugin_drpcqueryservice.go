package main

import (
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vttablet/drpcqueryservice"
	"vitess.io/vitess/go/vt/vttablet/tabletserver"
)

func init() {
	tabletserver.RegisterFunctions = append(tabletserver.RegisterFunctions, func(qsc tabletserver.Controller) {
		if servenv.DRPCCheckServiceMap("queryservice") {
			drpcqueryservice.Register(servenv.DRPCMux, qsc.QueryService())
		}
	})
}
