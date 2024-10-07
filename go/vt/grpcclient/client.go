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
	"net"
	"sync"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/spf13/pflag"
	"golang.org/x/sync/semaphore"
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
	grpcDialOptionsMu     sync.Mutex
	keepaliveTime         = 10 * time.Second
	keepaliveTimeout      = 10 * time.Second
	initialConnWindowSize int
	initialWindowSize     int

	// `dialConcurrencyLimit` tells us how many tablet grpc connections can be dialed concurrently.
	// This should be less than the golang max thread limit of 10000.
	dialConcurrencyLimit int64 = 1024

	// every vitess binary that makes grpc client-side calls.
	grpcclientBinaries = []string{
		"mysqlctld",
		"vtadmin",
		"vtbackup",
		"vtbench",
		"vtclient",
		"vtctl",
		"vtctlclient",
		"vtctld",
		"vtgate",
		"vtgateclienttest",
		"vtorc",
		"vttablet",
		"vttestserver",
	}
)

func RegisterFlags(fs *pflag.FlagSet) {
	fs.DurationVar(&keepaliveTime, "grpc_keepalive_time", keepaliveTime, "After a duration of this time, if the client doesn't see any activity, it pings the server to see if the transport is still alive.")
	fs.DurationVar(&keepaliveTimeout, "grpc_keepalive_timeout", keepaliveTimeout, "After having pinged for keepalive check, the client waits for a duration of Timeout and if no activity is seen even after that the connection is closed.")
	fs.IntVar(&initialConnWindowSize, "grpc_initial_conn_window_size", initialConnWindowSize, "gRPC initial connection window size")
	fs.IntVar(&initialWindowSize, "grpc_initial_window_size", initialWindowSize, "gRPC initial window size")
	fs.StringVar(&compression, "grpc_compression", compression, "Which protocol to use for compressing gRPC. Default: nothing. Supported: snappy")

	fs.StringVar(&credsFile, "grpc_auth_static_client_creds", credsFile, "When using grpc_static_auth in the server, this file provides the credentials to use to authenticate with server.")
}

func RegisterDeprecatedDialConcurrencyFlagsHealthcheck(fs *pflag.FlagSet) {
	fs.Int64Var(&dialConcurrencyLimit, "healthcheck-dial-concurrency", 1024, "Maximum concurrency of new healthcheck connections. This should be less than the golang max thread limit of 10000.")
	fs.MarkDeprecated("healthcheck-dial-concurrency", "This option is deprecated and will be removed in a future release. Use --grpc-dial-concurrency-limit instead.")
}

func RegisterDeprecatedDialConcurrencyFlagsHealthcheckForVtcombo(fs *pflag.FlagSet) {
	fs.Int64Var(&dialConcurrencyLimit, "healthcheck-dial-concurrency", 1024, "Maximum concurrency of new healthcheck connections. This should be less than the golang max thread limit of 10000.")
	fs.MarkDeprecated("healthcheck-dial-concurrency", "This option is deprecated and will be removed in a future release.")
}

func RegisterDialConcurrencyFlags(fs *pflag.FlagSet) {
	fs.Int64Var(&dialConcurrencyLimit, "grpc-dial-concurrency-limit", 1024, "Maximum concurrency of grpc dial operations. This should be less than the golang max thread limit of 10000.")
}

func init() {
	for _, cmd := range grpcclientBinaries {
		servenv.OnParseFor(cmd, RegisterFlags)

		if cmd == "vtgate" || cmd == "vtctld" {
			servenv.OnParseFor(cmd, RegisterDeprecatedDialConcurrencyFlagsHealthcheck)
		}

		servenv.OnParseFor(cmd, RegisterDialConcurrencyFlags)
	}

	// vtcombo doesn't really use grpc, but we need to expose this flag for backwards compat
	servenv.OnParseFor("vtcombo", RegisterDeprecatedDialConcurrencyFlagsHealthcheckForVtcombo)
}

// FailFast is a self-documenting type for the grpc.FailFast.
type FailFast bool

// grpcDialOptions is a registry of functions that append grpcDialOption to use when dialing a service
var grpcDialOptions []func(opts []grpc.DialOption) ([]grpc.DialOption, error)

// RegisterGRPCDialOptions registers an implementation of AuthServer.
func RegisterGRPCDialOptions(grpcDialOptionsFunc func(opts []grpc.DialOption) ([]grpc.DialOption, error)) {
	grpcDialOptionsMu.Lock()
	defer grpcDialOptionsMu.Unlock()
	grpcDialOptions = append(grpcDialOptions, grpcDialOptionsFunc)
}

// DialContext creates a grpc connection to the given target. Setup steps are
// covered by the context deadline, and, if WithBlock is specified in the dial
// options, connection establishment steps are covered by the context as well.
//
// failFast is a non-optional parameter because callers are required to specify
// what that should be.
func DialContext(ctx context.Context, target string, failFast FailFast, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	msgSize := grpccommon.MaxMessageSize()
	newopts := []grpc.DialOption{
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(msgSize),
			grpc.MaxCallSendMsgSize(msgSize),
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

	if dialConcurrencyLimit > 0 {
		newopts = append(newopts, dialConcurrencyLimitOption())
	}

	newopts = append(newopts, opts...)
	var err error
	grpcDialOptionsMu.Lock()
	for _, grpcDialOptionInitializer := range grpcDialOptions {
		newopts, err = grpcDialOptionInitializer(newopts)
		if err != nil {
			log.Fatalf("There was an error initializing client grpc.DialOption: %v", err)
		}
	}
	grpcDialOptionsMu.Unlock()

	newopts = append(newopts, interceptors()...)

	return grpc.DialContext(ctx, target, newopts...) // nolint:staticcheck
}

func interceptors() []grpc.DialOption {
	builder := &clientInterceptorBuilder{}
	if grpccommon.EnableGRPCPrometheus() {
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

var dialConcurrencyLimitOpt grpc.DialOption

// withDialerContextOnce ensures grpc.WithDialContext() is added once to the options.
var dialConcurrencyLimitOnce sync.Once

func dialConcurrencyLimitOption() grpc.DialOption {
	dialConcurrencyLimitOnce.Do(func() {
		// This semaphore is used to limit how many grpc connections can be dialed to tablets simultanously.
		// This does not limit how many tablet connections can be open at the same time.
		sem := semaphore.NewWeighted(dialConcurrencyLimit)

		dialConcurrencyLimitOpt = grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			// Limit the number of grpc connections opened in parallel to avoid high OS-thread
			// usage due to blocking networking syscalls (eg: DNS lookups, TCP connection opens,
			// etc). Without this limit it is possible for vtgates watching >10k tablets to hit
			// the panic: 'runtime: program exceeds 10000-thread limit'.
			if err := sem.Acquire(ctx, 1); err != nil {
				return nil, err
			}
			defer sem.Release(1)

			var dialer net.Dialer
			return dialer.DialContext(ctx, "tcp", addr)
		})
	})

	return dialConcurrencyLimitOpt
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
