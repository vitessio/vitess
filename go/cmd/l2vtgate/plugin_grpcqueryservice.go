// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

// Imports and register the gRPC queryservice server

import (
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/tabletserver/grpcqueryservice"
	"github.com/youtube/vitess/go/vt/tabletserver/queryservice"
	"github.com/youtube/vitess/go/vt/vtgate/l2vtgate"
)

func init() {
	servenv.RegisterGRPCFlags()
	l2vtgate.RegisterL2VTGates = append(l2vtgate.RegisterL2VTGates, func(qs queryservice.QueryService) {
		if servenv.GRPCCheckServiceMap("queryservice") {
			grpcqueryservice.Register(servenv.GRPCServer, qs)
		}
	})
}
