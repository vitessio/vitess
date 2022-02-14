package main

import (
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vtctl/grpcvtctlserver"
)

func init() {
	servenv.OnRun(func() {
		if servenv.GRPCCheckServiceMap("vtctl") {
			grpcvtctlserver.StartServer(servenv.GRPCServer, ts)
		}
	})
}
