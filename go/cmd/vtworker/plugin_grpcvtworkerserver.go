package main

import (
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/worker/grpcvtworkerserver"
)

func init() {
	servenv.OnRun(func() {
		if servenv.GRPCCheckServiceMap("vtworker") {
			grpcvtworkerserver.StartServer(servenv.GRPCServer, wi)
		}
	})
}
