package grpcserver

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gorilla/mux"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	otgrpc "github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/opentracing/opentracing-go"
	"github.com/soheilhy/cmux"
	"google.golang.org/grpc"
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
}

// Server provides a multiplexed gRPC/HTTP server.
type Server struct {
	name string

	gRPCServer   *grpc.Server
	healthServer *health.Server
	router       *mux.Router

	opts Options
}

// New returns a new server. See Options for documentation on configuration
// options.
//
// The underlying gRPC server always has the following interceptors:
//	- prometheus
//	- recovery: this handles recovering from panics.
func New(name string, opts Options) *Server {
	streamInterceptors := []grpc.StreamServerInterceptor{grpc_prometheus.StreamServerInterceptor}
	unaryInterceptors := []grpc.UnaryServerInterceptor{grpc_prometheus.UnaryServerInterceptor}

	if opts.EnableTracing {
		tracer := opentracing.GlobalTracer()
		streamInterceptors = append(streamInterceptors, otgrpc.StreamServerInterceptor(otgrpc.WithTracer(tracer)))
		unaryInterceptors = append(unaryInterceptors, otgrpc.UnaryServerInterceptor(otgrpc.WithTracer(tracer)))
	}

	recoveryHandler := grpc_recovery.WithRecoveryHandler(func(p interface{}) (err error) {
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

// ListenAndServe sets up a listener, multiplexes it into gRPC and non-gRPC
// requests, and binds the gRPC server and mux.Router to them, respectively. It
// then installs a signal handler on SIGTERM and SIGQUIT, and runs until either
// a signal or an unrecoverable error occurs.
//
// On shutdown, it may begin a lame duck period (see Options) before beginning
// a graceful shutdown of the gRPC server and closing listeners.
func (s *Server) ListenAndServe() error { // nolint:funlen
	lis, err := net.Listen("tcp", s.opts.Addr)
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

	return nil
}
