package grpcserver

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"golang.org/x/net/nettest"
	"google.golang.org/grpc"

	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func TestServer(t *testing.T) {
	lis, err := nettest.NewLocalListener("tcp")
	listenFunc = func(network, address string) (net.Listener, error) {
		return lis, err
	}

	defer lis.Close()

	s := New("testservice", Options{
		EnableTracing:   true,
		AllowReflection: true,
		CMuxReadTimeout: time.Second,
	})

	go func() { err := s.ListenAndServe(); assert.NoError(t, err) }()

	readyCh := make(chan bool)

	go func() {
		for !s.isServing() {
		}
		readyCh <- true
	}()

	serveStart := time.Now()
	select {
	case <-readyCh:
	case serveStop := <-time.After(time.Millisecond * 500):
		t.Errorf("server did not start within %s", serveStop.Sub(serveStart))
		return
	}
	close(readyCh)

	conn, err := grpc.Dial(lis.Addr().String(), grpc.WithInsecure(), grpc.WithBlock())
	assert.NoError(t, err)

	defer conn.Close()

	healthclient := healthpb.NewHealthClient(conn)
	resp, err := healthclient.Check(context.Background(), &healthpb.HealthCheckRequest{Service: "grpc.health.v1.Health"})
	assert.NoError(t, err)
	assert.NotNil(t, resp)
}

func TestLameduck(t *testing.T) {
	lis, err := nettest.NewLocalListener("tcp")
	listenFunc = func(network, address string) (net.Listener, error) {
		return lis, err
	}

	ldd := time.Millisecond * 50

	s := New("testservice", Options{LameDuckDuration: ldd})

	go func() { err := s.ListenAndServe(); assert.NoError(t, err) }()

	readyCh := make(chan bool)

	go func() {
		for !s.isServing() {
		}
		readyCh <- true
	}()

	serveStart := time.Now()
	select {
	case <-readyCh:
	case serveStop := <-time.After(time.Millisecond * 500):
		t.Errorf("server did not start within %s", serveStop.Sub(serveStart))
		return
	}

	stoppedCh := make(chan bool)

	go func() {
		for s.isServing() {
		}
		stoppedCh <- true
	}()

	lis.Close()

	shutdownStart := time.Now()
	select {
	case <-stoppedCh:
	case <-time.After(ldd):
	}

	shutdownDuration := time.Since(shutdownStart)
	assert.LessOrEqual(t, int64(ldd), int64(shutdownDuration),
		"should have taken at least %s to shutdown, took only %s", ldd, shutdownDuration)
}

func TestError(t *testing.T) {
	listenFunc = func(network, address string) (net.Listener, error) { return nil, assert.AnError }
	s := New("testservice", Options{})
	errCh := make(chan error)

	// This has to happen in a goroutine. In normal operation, this function
	// blocks until externally signalled, and we don't want to hold up the
	// tests.
	go func() {
		errCh <- s.ListenAndServe()
	}()

	start := time.Now()
	select {
	case err := <-errCh:
		assert.Error(t, err)
	case ti := <-time.After(time.Millisecond * 10):
		assert.Fail(t, "timed out waiting for error after %s", ti.Sub(start))
	}
}
