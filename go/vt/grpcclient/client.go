/*
Copyright 2019 The Vitess Authors.

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
	"context"
	"crypto/tls"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/spf13/pflag"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/grpccommon"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vttls"
)

var (
	keepaliveTime         = 10 * time.Second
	keepaliveTimeout      = 10 * time.Second
	initialConnWindowSize int
	initialWindowSize     int

	// every vitess binary that makes grpc client-side calls.
	grpcclientBinaries = []string{
		"vtadmin",
		"vtbackup",
		"vtbench",
		"vtclient",
		"vtctl",
		"vtctlclient",
		"vtctld",
		"vtctldclient",
		"vtgate",
		"vtgateclienttest",
		"vtgr",
		"vtorc",
		"vttablet",
		"vttestserver",
	}
)

func registerFlags(fs *pflag.FlagSet) {
	fs.DurationVar(&keepaliveTime, "grpc_keepalive_time", keepaliveTime, "After a duration of this time, if the client doesn't see any activity, it pings the server to see if the transport is still alive.")
	fs.DurationVar(&keepaliveTimeout, "grpc_keepalive_timeout", keepaliveTimeout, "After having pinged for keepalive check, the client waits for a duration of Timeout and if no activity is seen even after that the connection is closed.")
	fs.IntVar(&initialConnWindowSize, "grpc_initial_conn_window_size", initialConnWindowSize, "gRPC initial connection window size")
	fs.IntVar(&initialWindowSize, "grpc_initial_window_size", initialWindowSize, "gRPC initial window size")
}

func init() {
	for _, cmd := range grpcclientBinaries {
		servenv.OnParseFor(cmd, registerFlags)
	}
}

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
	return DialContext(context.Background(), target, failFast, opts...)
}

// DialContext creates a grpc connection to the given target. Setup steps are
// covered by the context deadline, and, if WithBlock is specified in the dial
// options, connection establishment steps are covered by the context as well.
//
// failFast is a non-optional parameter because callers are required to specify
// what that should be.
func DialContext(ctx context.Context, target string, failFast FailFast, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	grpccommon.EnableTracingOpt()
	newopts := []grpc.DialOption{
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(*grpccommon.MaxMessageSize),
			grpc.MaxCallSendMsgSize(*grpccommon.MaxMessageSize),
			grpc.WaitForReady(bool(!failFast)),
		),
	}

	if keepaliveTime != 0 || keepaliveTimeout != 0 {
		kp := keepalive.ClientParameters{
			// After a duration of this time if the client doesn't see any activity it pings the server to see if the transport is still alive.
			Time: keepaliveTime,
			// After having pinged for keepalive check, the client waits for a duration of Timeout and if no activity is seen even after that
			// the connection is closed. (This will eagerly fail inflight grpc requests even if they don't have timeouts.)
			Timeout:             keepaliveTimeout,
			PermitWithoutStream: true,
		}
		newopts = append(newopts, grpc.WithKeepaliveParams(kp))
	}

	if initialConnWindowSize != 0 {
		newopts = append(newopts, grpc.WithInitialConnWindowSize(int32(initialConnWindowSize)))
	}

	if initialWindowSize != 0 {
		newopts = append(newopts, grpc.WithInitialWindowSize(int32(initialWindowSize)))
	}

	newopts = append(newopts, opts...)
	var err error
	for _, grpcDialOptionInitializer := range grpcDialOptions {
		newopts, err = grpcDialOptionInitializer(newopts)
		if err != nil {
			log.Fatalf("There was an error initializing client grpc.DialOption: %v", err)
		}
	}

	newopts = append(newopts, interceptors()...)

	return grpc.DialContext(ctx, target, newopts...)
}

func interceptors() []grpc.DialOption {
	builder := &clientInterceptorBuilder{}
	if *grpccommon.EnableGRPCPrometheus {
		builder.Add(grpc_prometheus.StreamClientInterceptor, grpc_prometheus.UnaryClientInterceptor)
	}
	trace.AddGrpcClientOptions(builder.Add)
	return builder.Build()
}

// SecureDialOption returns the gRPC dial option to use for the
// given client connection. It is either using TLS, or Insecure if
// nothing is set.
func SecureDialOption(cert, key, ca, crl, name string) (grpc.DialOption, error) {
	// No security options set, just return.
	if (cert == "" || key == "") && ca == "" {
		return grpc.WithTransportCredentials(insecure.NewCredentials()), nil
	}

	// Load the config. At this point we know
	// we want a strict config with verify identity.
	config, err := vttls.ClientConfig(vttls.VerifyIdentity, cert, key, ca, crl, name, tls.VersionTLS12)
	if err != nil {
		return nil, err
	}

	// Create the creds server options.
	creds := credentials.NewTLS(config)
	return grpc.WithTransportCredentials(creds), nil
}

// Allows for building a chain of interceptors without knowing the total size up front
type clientInterceptorBuilder struct {
	unaryInterceptors  []grpc.UnaryClientInterceptor
	streamInterceptors []grpc.StreamClientInterceptor
}

// Add adds interceptors to the chain of interceptors
func (collector *clientInterceptorBuilder) Add(s grpc.StreamClientInterceptor, u grpc.UnaryClientInterceptor) {
	collector.unaryInterceptors = append(collector.unaryInterceptors, u)
	collector.streamInterceptors = append(collector.streamInterceptors, s)
}

// Build returns DialOptions to add to the grpc.Dial call
func (collector *clientInterceptorBuilder) Build() []grpc.DialOption {
	switch len(collector.unaryInterceptors) + len(collector.streamInterceptors) {
	case 0:
		return []grpc.DialOption{}
	default:
		return []grpc.DialOption{
			grpc.WithUnaryInterceptor(grpc_middleware.ChainUnaryClient(collector.unaryInterceptors...)),
			grpc.WithStreamInterceptor(grpc_middleware.ChainStreamClient(collector.streamInterceptors...)),
		}
	}
}
