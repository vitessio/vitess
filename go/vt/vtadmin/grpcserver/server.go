/*
Copyright 2020 The Vitess Authors.

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

package grpcserver

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/mux"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	otgrpc "github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/soheilhy/cmux"
	"google.golang.org/grpc"
	channelz "google.golang.org/grpc/channelz/service"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/reflection"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vterrors"

	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// Options defines the set of configurations for a gRPC server.
type Options struct {
	// Addr is the network address to listen on.
	Addr string
	// CMuxReadTimeout bounds the amount of time spent muxing connections between
	// gRPC and HTTP. A zero-value specifies unbounded muxing.
	CMuxReadTimeout time.Duration
	// LameDuckDuration specifies the length of the lame duck period during
	// graceful shutdown. If non-zero, the Server will mark itself unhealthy to
	// stop new incoming connections while continuing to serve existing
	// connections.
	LameDuckDuration time.Duration
	// AllowReflection specifies whether to register the gRPC server for
	// reflection. This is required to use with tools like grpc_cli.
	AllowReflection bool
	// EnableTracing specifies whether to install opentracing interceptors on
	// the gRPC server.
	EnableTracing bool
	// EnableChannelz specifies whether to register the channelz service on the
	// gRPC server.
	EnableChannelz bool

	// MetricsEndpoint is the route to serve promhttp metrics on, including
	// those collected be grpc_prometheus interceptors.
	//
	// It is the user's responsibility to ensure this does not conflict with
	// other endpoints.
	//
	// Omit to not export metrics.
	MetricsEndpoint string

	StreamInterceptors []grpc.StreamServerInterceptor
	UnaryInterceptors  []grpc.UnaryServerInterceptor
}

// Server provides a multiplexed gRPC/HTTP server.
type Server struct {
	name string

	gRPCServer   *grpc.Server
	healthServer *health.Server
	router       *mux.Router
	serving      bool
	m            sync.RWMutex // this locks the serving bool

	opts Options
}

// New returns a new server. See Options for documentation on configuration
// options.
//
// The underlying gRPC server always has the following interceptors:
//   - prometheus
//   - recovery: this handles recovering from panics.
//
// The full list of interceptors is as follows:
// - (optional) interceptors defined on the Options struct
// - prometheus
// - (optional) opentracing, if opts.EnableTracing is set
// - recovery
func New(name string, opts Options) *Server {
	streamInterceptors := append(opts.StreamInterceptors, grpc_prometheus.StreamServerInterceptor)
	unaryInterceptors := append(opts.UnaryInterceptors, grpc_prometheus.UnaryServerInterceptor)

	if opts.EnableTracing {
		tracer := opentracing.GlobalTracer()
		streamInterceptors = append(streamInterceptors, otgrpc.StreamServerInterceptor(otgrpc.WithTracer(tracer)))
		unaryInterceptors = append(unaryInterceptors, otgrpc.UnaryServerInterceptor(otgrpc.WithTracer(tracer)))
	}

	recoveryHandler := grpc_recovery.WithRecoveryHandler(func(p any) (err error) {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "panic triggered: %v", p)
	})

	streamInterceptors = append(streamInterceptors, grpc_recovery.StreamServerInterceptor(recoveryHandler))
	unaryInterceptors = append(unaryInterceptors, grpc_recovery.UnaryServerInterceptor(recoveryHandler))

	gserv := grpc.NewServer(
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(streamInterceptors...)),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(unaryInterceptors...)),
	)

	if opts.AllowReflection {
		reflection.Register(gserv)
	}

	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(gserv, healthServer)

	if opts.EnableChannelz {
		channelz.RegisterChannelzServiceToServer(gserv)
	}

	return &Server{
		name:         name,
		gRPCServer:   gserv,
		healthServer: healthServer,
		router:       mux.NewRouter(),
		opts:         opts,
	}
}

// GRPCServer returns the gRPC Server.
func (s *Server) GRPCServer() *grpc.Server {
	return s.gRPCServer
}

// Router returns the mux.Router powering the HTTP side of the server.
func (s *Server) Router() *mux.Router {
	return s.router
}

// MustListenAndServe calls ListenAndServe and panics if an error occurs.
func (s *Server) MustListenAndServe() {
	if err := s.ListenAndServe(); err != nil {
		panic(err)
	}
}

// listenFunc is extracted to mock out in tests.
var listenFunc = net.Listen // nolint:gochecknoglobals

// ListenAndServe sets up a listener, multiplexes it into gRPC and non-gRPC
// requests, and binds the gRPC server and mux.Router to them, respectively. It
// then installs a signal handler on SIGTERM and SIGQUIT, and runs until either
// a signal or an unrecoverable error occurs.
//
// On shutdown, it may begin a lame duck period (see Options) before beginning
// a graceful shutdown of the gRPC server and closing listeners.
func (s *Server) ListenAndServe() error { // nolint:funlen
	lis, err := listenFunc("tcp", s.opts.Addr)
	if err != nil {
		return err
	}
	defer lis.Close()

	lmux := cmux.New(lis)

	if s.opts.CMuxReadTimeout > 0 {
		lmux.SetReadTimeout(s.opts.CMuxReadTimeout)
	}

	grpcLis := lmux.MatchWithWriters(cmux.HTTP2MatchHeaderFieldSendSettings("content-type", "application/grpc"))
	anyLis := lmux.Match(cmux.Any())

	shutdown := make(chan error, 16)

	signals := make(chan os.Signal, 8)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGQUIT)

	// listen for signals
	go func() {
		sig := <-signals
		err := fmt.Errorf("received signal: %v", sig) // nolint:goerr113
		log.Warning(err)
		shutdown <- err
	}()

	if s.opts.MetricsEndpoint != "" {
		if !strings.HasPrefix(s.opts.MetricsEndpoint, "/") {
			s.opts.MetricsEndpoint = "/" + s.opts.MetricsEndpoint
		}

		grpc_prometheus.Register(s.gRPCServer)
		s.router.Handle(s.opts.MetricsEndpoint, promhttp.Handler())
	}

	// Start the servers
	go func() {
		err := s.gRPCServer.Serve(grpcLis)
		err = fmt.Errorf("grpc server stopped: %w", err)
		log.Warning(err)
		shutdown <- err
	}()

	go func() {
		err := http.Serve(anyLis, s.router)
		err = fmt.Errorf("http server stopped: %w", err)
		log.Warning(err)
		shutdown <- err
	}()

	// Start muxing connections
	go func() {
		err := lmux.Serve()
		err = fmt.Errorf("listener closed: %w", err)
		log.Warning(err)
		shutdown <- err
	}()

	for service := range s.gRPCServer.GetServiceInfo() {
		s.healthServer.SetServingStatus(service, healthpb.HealthCheckResponse_SERVING)
	}

	s.setServing(true)
	log.Infof("server %s listening on %s", s.name, s.opts.Addr)

	reason := <-shutdown
	log.Warningf("graceful shutdown triggered by: %v", reason)

	if s.opts.LameDuckDuration > 0 {
		log.Infof("entering lame duck period for %v", s.opts.LameDuckDuration)
		s.healthServer.Shutdown()
		time.Sleep(s.opts.LameDuckDuration)
	} else {
		log.Infof("lame duck disabled")
	}

	log.Info("beginning graceful shutdown")
	s.gRPCServer.GracefulStop()
	log.Info("graceful shutdown complete")

	s.setServing(false)

	return nil
}

func (s *Server) setServing(state bool) {
	s.m.Lock()
	defer s.m.Unlock()

	s.serving = state
}

func (s *Server) isServing() bool {
	s.m.RLock()
	defer s.m.RUnlock()

	return s.serving
}
