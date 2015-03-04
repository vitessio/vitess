// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package servenv

import (
	"flag"
	"fmt"
	"net"

	"google.golang.org/grpc"

	log "github.com/golang/glog"
)

// This file handles gRPC server, on its own port.
// Clients register servers, based on service map:
//
// servenv.RegisterGRPCFlags()
// servenv.OnRun(func() {
//   if servenv.GRPCCheckServiceMap("XXX") {
//     pb.RegisterXXX(servenv.GRPCServer, XXX)
//   }
// }

var (
	// GRPCPort is the port to listen on for gRPC. If not set or zero, don't listen.
	GRPCPort *int

	// GRPCServer is the global server to serve gRPC.
	GRPCServer = grpc.NewServer()
)

func serveGRPC() {
	// skip if not registered
	if GRPCPort == nil || *GRPCPort == 0 {
		return
	}

	// listen on the port
	log.Infof("Listening for gRPC calls on port %v", *GRPCPort)
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", *GRPCPort))
	if err != nil {
		log.Fatalf("Cannot listen on port %v for gRPC: %v", *GRPCPort, err)
	}

	// and serve on it
	go GRPCServer.Serve(listener)
}

// RegisterGRPCFlags registers the right command line flag to enable gRPC
func RegisterGRPCFlags() {
	GRPCPort = flag.Int("grpc_port", 0, "Port to listen on for gRPC calls")
}

// GRPCCheckServiceMap returns if we should register a gRPC service
// (and also logs how to enable / disable it)
func GRPCCheckServiceMap(name string) bool {
	// Silently fail individual services if gRPC is not enabled in the first place
	if GRPCPort == nil || *GRPCPort == 0 {
		return false
	}

	// then check ServiceMap
	if ServiceMap["grpc-"+name] {
		log.Infof("Registering %v for gRPC, disable it with -grpc-%v service_map parameter", name, name)
		return true
	}
	log.Infof("Not registering %v for gRPC, enable it with grpc-%v service_map parameter", name, name)
	return false
}
