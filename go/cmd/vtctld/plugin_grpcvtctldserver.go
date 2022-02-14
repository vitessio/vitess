package main

import (
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vtctl/grpcvtctldserver"
)

func init() {
	servenv.OnRun(func() {
		if servenv.GRPCCheckServiceMap("vtctld") {
			grpcvtctldserver.StartServer(servenv.GRPCServer, ts)
		}
	})
}
