// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package servenv

import (
	"flag"
	"fmt"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/servenv/grpcutils"
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
//
// Note servenv.GRPCServer can only be used in servenv.OnRun,
// and not before, as it is initialized right before calling OnRun.
var (
	// GRPCPort is the port to listen on for gRPC. If not set or zero, don't listen.
	GRPCPort *int

	// GRPCCert is the cert to use if TLS is enabled
	GRPCCert *string

	// GRPCKey is the key to use if TLS is enabled
	GRPCKey *string

	// GRPCCA is the CA to use if TLS is enabled
	GRPCCA *string

	// GRPCMaxMessageSize is the maximum message size which the gRPC server will
	// accept. Larger messages will be rejected.
	GRPCMaxMessageSize *int

	// GRPCServer is the global server to serve gRPC.
	GRPCServer *grpc.Server
)

// isGRPCEnabled returns true if gRPC server is set
func isGRPCEnabled() bool {
	if GRPCPort != nil && *GRPCPort != 0 {
		return true
	}

	if SocketFile != nil && *SocketFile != "" {
		return true
	}

	return false
}

// createGRPCServer create the gRPC server we will be using.
// It has to be called after flags are parsed, but before
// services register themselves.
func createGRPCServer() {
	// skip if not registered
	if !isGRPCEnabled() {
		log.Infof("Skipping gRPC server creation")
		return
	}

	var opts []grpc.ServerOption
	if GRPCPort != nil && *GRPCCert != "" && *GRPCKey != "" {
		config, err := grpcutils.TLSServerConfig(*GRPCCert, *GRPCKey, *GRPCCA)
		if err != nil {
			log.Fatalf("Failed to log gRPC cert/key/ca: %v", err)
		}

		// create the creds server options
		creds := credentials.NewTLS(config)
		opts = []grpc.ServerOption{grpc.Creds(creds)}
	}
	// Override the default max message size (which is 4 MiB in gRPC 1.0.0).
	// Large messages can occur when users try to insert very big rows. If they
	// hit the limit, they'll see the following error:
	// grpc: received message length XXXXXXX exceeding the max size 4194304
	// Note: For gRPC 1.0.0 it's sufficient to set the limit on the server only
	// because it's not enforced on the client side.
	if GRPCMaxMessageSize != nil {
		opts = append(opts, grpc.MaxMsgSize(*GRPCMaxMessageSize))
	}

	GRPCServer = grpc.NewServer(opts...)
}

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
	GRPCCert = flag.String("grpc_cert", "", "certificate to use, requires grpc_key, enables TLS")
	GRPCKey = flag.String("grpc_key", "", "key to use, requires grpc_cert, enables TLS")
	GRPCCA = flag.String("grpc_ca", "", "ca to use, requires TLS, and enforces client cert check")
	// Note: We're using 4 MiB as default value because that's the default in the
	// gRPC 1.0.0 Go server.
	GRPCMaxMessageSize = flag.Int("grpc_max_message_size", 4*1024*1024, "Maximum allowed RPC message size. Larger messages will be rejected by gRPC with the error 'exceeding the max size'.")
}

// GRPCCheckServiceMap returns if we should register a gRPC service
// (and also logs how to enable / disable it)
func GRPCCheckServiceMap(name string) bool {
	// Silently fail individual services if gRPC is not enabled in
	// the first place (either on a grpc port or on the socket file)
	if !isGRPCEnabled() {
		return false
	}

	// then check ServiceMap
	return CheckServiceMap("grpc", name)
}
