// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

// Import and register the gRPC mysqlctl server

import (
	"github.com/youtube/vitess/go/vt/mysqlctl/grpcmysqlctlserver"
	"github.com/youtube/vitess/go/vt/servenv"
)

func init() {
	servenv.InitServiceMap("grpc", "mysqlctl")
	servenv.OnRun(func() {
		if servenv.GRPCCheckServiceMap("mysqlctl") {
			grpcmysqlctlserver.StartServer(servenv.GRPCServer, mysqld)
		}
	})
}
