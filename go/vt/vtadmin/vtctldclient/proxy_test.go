/*
Copyright 2021 The Vitess Authors.

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

package vtctldclient

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"vitess.io/vitess/go/vt/vtadmin/cluster/discovery/fakediscovery"
	"vitess.io/vitess/go/vt/vtadmin/cluster/resolver"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtctlservicepb "vitess.io/vitess/go/vt/proto/vtctlservice"
)

type fakeVtctld struct {
	vtctlservicepb.VtctldServer
	addr string
}

// GetKeyspace is used for tests to detect what addr the VtctldServer is
// listening on. The addr will always be stored as resp.Keyspace.Name, and the
// actual request is ignored.
func (fake *fakeVtctld) GetKeyspace(ctx context.Context, req *vtctldatapb.GetKeyspaceRequest) (*vtctldatapb.GetKeyspaceResponse, error) {
	return &vtctldatapb.GetKeyspaceResponse{
		Keyspace: &vtctldatapb.Keyspace{
			Name: fake.addr,
		},
	}, nil
}

func initVtctldServer() (net.Listener, *grpc.Server, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, nil, err
	}

	vtctld := &fakeVtctld{
		addr: listener.Addr().String(),
	}
	server := grpc.NewServer()
	vtctlservicepb.RegisterVtctldServer(server, vtctld)

	return listener, server, err
}

func TestDial(t *testing.T) {
	listener, server, err := initVtctldServer()
	require.NoError(t, err)

	defer listener.Close()

	go server.Serve(listener)
	defer server.Stop()

	disco := fakediscovery.New()
	disco.AddTaggedVtctlds(nil, &vtadminpb.Vtctld{
		Hostname: listener.Addr().String(),
	})

	proxy, err := New(context.Background(), &Config{
		Cluster: &vtadminpb.Cluster{
			Id:   "test",
			Name: "testcluster",
		},
		ResolverOptions: &resolver.Options{
			Discovery:        disco,
			DiscoveryTimeout: 50 * time.Millisecond,
		},
	})
	require.NoError(t, err)

	defer proxy.Close() // prevents grpc-core from logging a bunch of "connection errors" after deferred listener.Close() above.

	resp, err := proxy.GetKeyspace(context.Background(), &vtctldatapb.GetKeyspaceRequest{})
	require.NoError(t, err)
	assert.Equal(t, listener.Addr().String(), resp.Keyspace.Name)
}

type testdisco struct {
	*fakediscovery.Fake
	notify chan struct{}
	fired  chan struct{}
	m      sync.Mutex
}

func (d *testdisco) DiscoverVtctldAddrs(ctx context.Context, tags []string) ([]string, error) {
	d.m.Lock()
	defer d.m.Unlock()

	select {
	case <-d.notify:
		defer func() {
			go func() { d.fired <- struct{}{} }()
		}()
	default:
	}
	return d.Fake.DiscoverVtctldAddrs(ctx, tags)
}

// TestRedial tests that vtadmin-api is able to recover from a lost connection to
// a vtctld by rediscovering and redialing a new one.
func TestRedial(t *testing.T) {
	// Initialize vtctld #1
	listener1, server1, err := initVtctldServer()
	require.NoError(t, err)

	defer listener1.Close()

	go server1.Serve(listener1)
	defer server1.Stop()

	// Initialize vtctld #2
	listener2, server2, err := initVtctldServer()
	require.NoError(t, err)

	defer listener2.Close()

	go server2.Serve(listener2)
	defer server2.Stop()

	reResolveFired := make(chan struct{}, 1)

	// Register both vtctlds with VTAdmin
	disco := &testdisco{
		Fake:   fakediscovery.New(),
		notify: make(chan struct{}),
		fired:  reResolveFired,
	}
	disco.AddTaggedVtctlds(nil, &vtadminpb.Vtctld{
		Hostname: listener1.Addr().String(),
	}, &vtadminpb.Vtctld{
		Hostname: listener2.Addr().String(),
	})

	proxy, err := New(context.Background(), &Config{
		Cluster: &vtadminpb.Cluster{
			Id:   "test",
			Name: "testcluster",
		},
		ResolverOptions: &resolver.Options{
			Discovery:            disco,
			DiscoveryTimeout:     50 * time.Millisecond,
			MinDiscoveryInterval: 0,
			BackoffStrategy:      "none",
		},
	})
	require.NoError(t, err)

	// vtadmin's fakediscovery package discovers vtctlds in random order. Rather
	// than force some cumbersome sequential logic, we can just do a switcheroo
	// here in the test to determine our "current" and (expected) "next" vtctlds.
	var currentVtctld *grpc.Server
	var nextAddr string

	// Check for a successful connection to whichever vtctld we discover first.
	resp, err := proxy.GetKeyspace(context.Background(), &vtctldatapb.GetKeyspaceRequest{})
	require.NoError(t, err)

	proxyHost := resp.Keyspace.Name
	switch proxyHost {
	case listener1.Addr().String():
		currentVtctld = server1
		nextAddr = listener2.Addr().String()

	case listener2.Addr().String():
		currentVtctld = server2
		nextAddr = listener1.Addr().String()
	default:
		t.Fatalf("invalid proxy host %s", proxyHost)
	}

	// Shut down the vtctld we're connected to, then await re-resolution.

	// 1. First, block calls to DiscoverVtctldAddrs so we don't race with the
	// background resolver watcher.
	disco.m.Lock()

	// 2. Force an ungraceful shutdown of the gRPC server to which we're
	// connected.
	currentVtctld.Stop()

	// 3. Remove the shut down vtctld from VTAdmin's service discovery
	// (clumsily). Otherwise, when redialing, we may redial the vtctld that we
	// just shut down.
	disco.Clear()
	disco.AddTaggedVtctlds(nil, &vtadminpb.Vtctld{
		Hostname: nextAddr,
	})

	// 4. Notify our wrapped DiscoverVtctldAddrs function to start signaling on
	// its `fired` channel when called.
	close(disco.notify)
	// 5. Unblock calls to DiscoverVtctldAddrs, and move on to our assertions.
	disco.m.Unlock()

	maxWait := time.Second
	select {
	case <-reResolveFired:
	case <-time.After(maxWait):
		require.FailNowf(t, "forced shutdown of vtctld should trigger grpc re-resolution", "did not receive re-resolve signal within %s", maxWait)
	}

	// Finally, check that we discover + establish a new connection to the remaining vtctld.
	resp, err = proxy.GetKeyspace(context.Background(), &vtctldatapb.GetKeyspaceRequest{})
	require.NoError(t, err)
	assert.Equal(t, nextAddr, resp.Keyspace.Name)
}
