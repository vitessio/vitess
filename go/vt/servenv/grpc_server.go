/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package servenv

import (
	"flag"
	"fmt"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"math"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/grpccommon"
	"github.com/youtube/vitess/go/vt/vttls"
	"golang.org/x/net/context"
	"google.golang.org/grpc/keepalive"
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
	GRPCPort = flag.Int("grpc_port", 0, "Port to listen on for gRPC calls")

	// GRPCCert is the cert to use if TLS is enabled
	GRPCCert = flag.String("grpc_cert", "", "certificate to use, requires grpc_key, enables TLS")

	// GRPCKey is the key to use if TLS is enabled
	GRPCKey = flag.String("grpc_key", "", "key to use, requires grpc_cert, enables TLS")

	// GRPCCA is the CA to use if TLS is enabled
	GRPCCA = flag.String("grpc_ca", "", "ca to use, requires TLS, and enforces client cert check")

	// GRPCAuth which auth plugin to use (at the moment now only static is supported)
	GRPCAuth = flag.String("grpc_auth_mode", "", "Which auth plugin implementation to use (eg: static)")

	// GRPCServer is the global server to serve gRPC.
	GRPCServer *grpc.Server

	// GRPCMaxConnectionAge is the maximum age of a client connection, before GoAway is sent.
	// This is useful for L4 loadbalancing to ensure rebalancing after scaling.
	GRPCMaxConnectionAge = flag.Duration("grpc_max_connection_age", time.Duration(math.MaxInt64), "Maximum age of a client connection before GoAway is sent.")

	// GRPCMaxConnectionAgeGrace is an additional grace period after GRPCMaxConnectionAge, after which
	// connections are forcibly closed.
	GRPCMaxConnectionAgeGrace = flag.Duration("grpc_max_connection_age_grace", time.Duration(math.MaxInt64), "Additional grace period after grpc_max_connection_age, after which connections are forcibly closed.")

	authPlugin Authenticator
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
		config, err := vttls.ServerConfig(*GRPCCert, *GRPCKey, *GRPCCA)
		if err != nil {
			log.Exitf("Failed to log gRPC cert/key/ca: %v", err)
		}

		// create the creds server options
		creds := credentials.NewTLS(config)
		opts = []grpc.ServerOption{grpc.Creds(creds)}
	}
	// Override the default max message size for both send and receive
	// (which is 4 MiB in gRPC 1.0.0).
	// Large messages can occur when users try to insert or fetch very big
	// rows. If they hit the limit, they'll see the following error:
	// grpc: received message length XXXXXXX exceeding the max size 4194304
	// Note: For gRPC 1.0.0 it's sufficient to set the limit on the server only
	// because it's not enforced on the client side.
	log.Infof("Setting grpc max message size to %d", *grpccommon.MaxMessageSize)
	opts = append(opts, grpc.MaxRecvMsgSize(*grpccommon.MaxMessageSize))
	opts = append(opts, grpc.MaxSendMsgSize(*grpccommon.MaxMessageSize))

	if GRPCMaxConnectionAge != nil {
		ka := keepalive.ServerParameters{
			MaxConnectionAge: *GRPCMaxConnectionAge,
		}
		if GRPCMaxConnectionAgeGrace != nil {
			ka.MaxConnectionAgeGrace = *GRPCMaxConnectionAgeGrace
		}
		opts = append(opts, grpc.KeepaliveParams(ka))
	}

	if *GRPCAuth != "" {
		log.Infof("enabling auth plugin %v", *GRPCAuth)
		pluginInitializer := GetAuthenticator(*GRPCAuth)
		authPluginImpl, err := pluginInitializer()
		if err != nil {
			log.Fatalf("Failed to load auth plugin: %v", err)
		}
		authPlugin = authPluginImpl
		opts = append(opts, grpc.StreamInterceptor(streamInterceptor))
		opts = append(opts, grpc.UnaryInterceptor(unaryInterceptor))
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
		log.Exitf("Cannot listen on port %v for gRPC: %v", *GRPCPort, err)
	}

	// and serve on it
	// NOTE: Before we call Serve(), all services must have registered themselves
	//       with "GRPCServer". This is the case because go/vt/servenv/run.go
	//       runs all OnRun() hooks after createGRPCServer() and before
	//       serveGRPC(). If this was not the case, the binary would crash with
	//       the error "grpc: Server.RegisterService after Server.Serve".
	go GRPCServer.Serve(listener)

	OnTermSync(func() {
		log.Info("Initiated graceful stop of gRPC server")
		GRPCServer.GracefulStop()
		log.Info("gRPC server stopped")
	})
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

func streamInterceptor(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	newCtx, err := authPlugin.Authenticate(stream.Context(), info.FullMethod)

	if err != nil {
		return err
	}

	wrapped := WrapServerStream(stream)
	wrapped.WrappedContext = newCtx
	return handler(srv, wrapped)
}

func unaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	newCtx, err := authPlugin.Authenticate(ctx, info.FullMethod)
	if err != nil {
		return nil, err
	}

	return handler(newCtx, req)
}

// WrappedServerStream is based on the service stream wrapper from: https://github.com/grpc-ecosystem/go-grpc-middleware
type WrappedServerStream struct {
	grpc.ServerStream
	WrappedContext context.Context
}

// Context returns the wrapper's WrappedContext, overwriting the nested grpc.ServerStream.Context()
func (w *WrappedServerStream) Context() context.Context {
	return w.WrappedContext
}

// WrapServerStream returns a ServerStream that has the ability to overwrite context.
func WrapServerStream(stream grpc.ServerStream) *WrappedServerStream {
	if existing, ok := stream.(*WrappedServerStream); ok {
		return existing
	}
	return &WrappedServerStream{ServerStream: stream, WrappedContext: stream.Context()}
}
