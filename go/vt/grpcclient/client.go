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

// Package grpcclient contains utility methods for gRPC client implementations
// to use. It also supports plug-in authentication.
package grpcclient

import (
	"flag"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"

	"github.com/grpc-ecosystem/go-grpc-prometheus"

	"vitess.io/vitess/go/vt/grpccommon"
	"vitess.io/vitess/go/vt/vttls"

	"vitess.io/vitess/go/vt/log"
)

var (
	keepaliveTime    = flag.Duration("grpc_keepalive_time", 0, "After a duration of this time if the client doesn't see any activity it pings the server to see if the transport is still alive.")
	keepaliveTimeout = flag.Duration("grpc_keepalive_timeout", 0, "After having pinged for keepalive check, the client waits for a duration of Timeout and if no activity is seen even after that the connection is closed.")
)

// FailFast is a self-documenting type for the grpc.FailFast.
type FailFast bool

// grpcDialOptions is a registry of functions that append grpcDialOption to use when dialing a service
var grpcDialOptions []func(opts []grpc.DialOption) ([]grpc.DialOption, error)

// RegisterGRPCDialOptions registers an implementation of AuthServer.
func RegisterGRPCDialOptions(grpcDialOptionsFunc func(opts []grpc.DialOption) ([]grpc.DialOption, error)) {
	grpcDialOptions = append(grpcDialOptions, grpcDialOptionsFunc)
}

// Dial creates a grpc connection to the given target.
// failFast is a non-optional parameter because callers are required to specify
// what that should be.
func Dial(target string, failFast FailFast, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	grpccommon.EnableTracingOpt()
	newopts := []grpc.DialOption{
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(*grpccommon.MaxMessageSize),
			grpc.MaxCallSendMsgSize(*grpccommon.MaxMessageSize),
			grpc.FailFast(bool(failFast)),
		),
	}

	if *keepaliveTime != 0 || *keepaliveTimeout != 0 {
		kp := keepalive.ClientParameters{
			// After a duration of this time if the client doesn't see any activity it pings the server to see if the transport is still alive.
			Time: *keepaliveTime,
			// After having pinged for keepalive check, the client waits for a duration of Timeout and if no activity is seen even after that
			// the connection is closed. (This will eagerly fail inflight grpc requests even if they don't have timeouts.)
			Timeout:             *keepaliveTimeout,
			PermitWithoutStream: true,
		}
		newopts = append(newopts, grpc.WithKeepaliveParams(kp))
	}

	if *grpccommon.InitialConnWindowSize != 0 {
		newopts = append(newopts, grpc.WithInitialConnWindowSize(int32(*grpccommon.InitialConnWindowSize)))
	}

	if *grpccommon.InitialWindowSize != 0 {
		newopts = append(newopts, grpc.WithInitialWindowSize(int32(*grpccommon.InitialWindowSize)))
	}

	newopts = append(newopts, opts...)
	var err error
	for _, grpcDialOptionInitializer := range grpcDialOptions {
		newopts, err = grpcDialOptionInitializer(newopts)
		if err != nil {
			log.Fatalf("There was an error initializing client grpc.DialOption: %v", err)
		}
	}

	if *grpccommon.EnableGRPCPrometheus {
		newopts = append(newopts, grpc.WithUnaryInterceptor(grpc_prometheus.UnaryClientInterceptor))
		newopts = append(newopts, grpc.WithStreamInterceptor(grpc_prometheus.StreamClientInterceptor))
	}

	return grpc.Dial(target, newopts...)
}

// SecureDialOption returns the gRPC dial option to use for the
// given client connection. It is either using TLS, or Insecure if
// nothing is set.
func SecureDialOption(cert, key, ca, name string) (grpc.DialOption, error) {
	// No security options set, just return.
	if (cert == "" || key == "") && ca == "" {
		return grpc.WithInsecure(), nil
	}

	// Load the config.
	config, err := vttls.ClientConfig(cert, key, ca, name)
	if err != nil {
		return nil, err
	}

	// Create the creds server options.
	creds := credentials.NewTLS(config)
	return grpc.WithTransportCredentials(creds), nil
}
