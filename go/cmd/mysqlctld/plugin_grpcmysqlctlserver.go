package main

// Import and register the gRPC mysqlctl server

import (
	"vitess.io/vitess/go/vt/mysqlctl/grpcmysqlctlserver"
	"vitess.io/vitess/go/vt/servenv"
)

func init() {
	servenv.InitServiceMap("grpc", "mysqlctl")
	servenv.OnRun(func() {
		if servenv.GRPCCheckServiceMap("mysqlctl") {
			grpcmysqlctlserver.StartServer(servenv.GRPCServer, cnf, mysqld)
		}
	})
}
