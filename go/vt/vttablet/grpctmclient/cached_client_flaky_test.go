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

package grpctmclient

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/nettest"
	"google.golang.org/grpc"

	"vitess.io/vitess/go/vt/vttablet/grpctmserver"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager"
	"vitess.io/vitess/go/vt/vttablet/tmrpctest"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func grpcTestServer(t testing.TB, tm tabletmanager.RPCTM) (*net.TCPAddr, func()) {
	t.Helper()

	lis, err := nettest.NewLocalListener("tcp")
	if err != nil {
		t.Fatalf("Cannot listen: %v", err)
	}

	s := grpc.NewServer()
	grpctmserver.RegisterForTest(s, tm)
	go s.Serve(lis)

	var shutdownOnce sync.Once

	return lis.Addr().(*net.TCPAddr), func() {
		shutdownOnce.Do(func() {
			s.Stop()
			lis.Close()
		})
	}
}

func BenchmarkCachedConnClientSteadyState(b *testing.B) {
	tmserv := tmrpctest.NewFakeRPCTM(b)
	tablets := make([]*topodatapb.Tablet, 1000)
	for i := 0; i < len(tablets); i++ {
		addr, shutdown := grpcTestServer(b, tmserv)
		defer shutdown()

		tablets[i] = &topodatapb.Tablet{
			Alias: &topodatapb.TabletAlias{
				Cell: "test",
				Uid:  uint32(addr.Port),
			},
			Hostname: addr.IP.String(),
			PortMap: map[string]int32{
				"grpc": int32(addr.Port),
			},
		}
	}

	client := NewCachedConnClient(100)
	defer client.Close()

	// fill the pool
	for i := 0; i < 100; i++ {
		err := client.Ping(context.Background(), tablets[i])
		require.NoError(b, err)
	}

	procs := runtime.GOMAXPROCS(0) / 4
	if procs == 0 {
		procs = 2
	}

	pingsPerProc := len(tablets) / procs
	if pingsPerProc == 0 {
		pingsPerProc = 2
	}

	b.ResetTimer()

	// Begin the benchmark
	for i := 0; i < b.N; i++ {
		ctx, cancel := context.WithCancel(context.Background())

		var wg sync.WaitGroup
		for j := 0; j < procs; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				for k := 0; k < pingsPerProc; k++ {
					func() {
						ctx, cancel := context.WithTimeout(ctx, time.Second*5)
						defer cancel()

						x := rand.Intn(len(tablets))
						err := client.Ping(ctx, tablets[x])
						assert.NoError(b, err)
					}()
				}
			}()
		}

		wg.Wait()
		cancel()
	}
}

func BenchmarkCachedConnClientSteadyStateRedials(b *testing.B) {
	tmserv := tmrpctest.NewFakeRPCTM(b)
	tablets := make([]*topodatapb.Tablet, 1000)
	for i := 0; i < len(tablets); i++ {
		addr, shutdown := grpcTestServer(b, tmserv)
		defer shutdown()

		tablets[i] = &topodatapb.Tablet{
			Alias: &topodatapb.TabletAlias{
				Cell: "test",
				Uid:  uint32(addr.Port),
			},
			Hostname: addr.IP.String(),
			PortMap: map[string]int32{
				"grpc": int32(addr.Port),
			},
		}
	}

	client := NewCachedConnClient(1000)
	defer client.Close()

	// fill the pool
	for i := 0; i < 1000; i++ {
		err := client.Ping(context.Background(), tablets[i])
		require.NoError(b, err)
	}

	procs := runtime.GOMAXPROCS(0) / 4
	if procs == 0 {
		procs = 2
	}

	pingsPerProc := len(tablets) / procs
	if pingsPerProc == 0 {
		pingsPerProc = 2
	}

	b.ResetTimer()

	// Begin the benchmark
	for i := 0; i < b.N; i++ {
		ctx, cancel := context.WithCancel(context.Background())

		var wg sync.WaitGroup
		for j := 0; j < procs; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				for k := 0; k < pingsPerProc; k++ {
					func() {
						ctx, cancel := context.WithTimeout(ctx, time.Second*5)
						defer cancel()

						x := rand.Intn(len(tablets))
						err := client.Ping(ctx, tablets[x])
						assert.NoError(b, err)
					}()
				}
			}()
		}

		wg.Wait()
		cancel()
	}
}

func BenchmarkCachedConnClientSteadyStateEvictions(b *testing.B) {
	tmserv := tmrpctest.NewFakeRPCTM(b)
	tablets := make([]*topodatapb.Tablet, 1000)
	for i := 0; i < len(tablets); i++ {
		addr, shutdown := grpcTestServer(b, tmserv)
		defer shutdown()

		tablets[i] = &topodatapb.Tablet{
			Alias: &topodatapb.TabletAlias{
				Cell: "test",
				Uid:  uint32(addr.Port),
			},
			Hostname: addr.IP.String(),
			PortMap: map[string]int32{
				"grpc": int32(addr.Port),
			},
		}
	}

	client := NewCachedConnClient(100)
	defer client.Close()

	// fill the pool
	for i := 0; i < 100; i++ {
		err := client.Ping(context.Background(), tablets[i])
		require.NoError(b, err)
	}

	assert.Equal(b, len(client.dialer.(*cachedConnDialer).conns), 100)

	procs := runtime.GOMAXPROCS(0) / 4
	if procs == 0 {
		procs = 2
	}

	start := 100
	b.ResetTimer()

	// Begin the benchmark
	for i := 0; i < b.N; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		ch := make(chan int, 100) // 100 dials per iteration

		var wg sync.WaitGroup
		for j := 0; j < procs; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				for idx := range ch {
					func() {
						ctx, cancel := context.WithTimeout(ctx, time.Second*5)
						defer cancel()

						err := client.Ping(ctx, tablets[idx])
						assert.NoError(b, err)
					}()
				}
			}()
		}

		for j := 0; j < cap(ch); j++ {
			start = (start + j) % 1000 // go in increasing order, wrapping around
			ch <- start
		}

		close(ch)
		wg.Wait()
		cancel()
	}
}

func TestCachedConnClient(t *testing.T) {
	t.Parallel()

	testCtx, testCancel := context.WithCancel(context.Background())
	wg := sync.WaitGroup{}
	procs := 0

	wg.Add(1)
	go func() {
		defer wg.Done()
		procs = runtime.NumGoroutine()

		for {
			select {
			case <-testCtx.Done():
				return
			case <-time.After(time.Millisecond * 100):
				newProcs := runtime.NumGoroutine()
				if newProcs > procs {
					procs = newProcs
				}
			}
		}
	}()

	numTablets := 100
	numGoroutines := 8

	tmserv := tmrpctest.NewFakeRPCTM(t)
	tablets := make([]*topodatapb.Tablet, numTablets)
	for i := 0; i < len(tablets); i++ {
		addr, shutdown := grpcTestServer(t, tmserv)
		defer shutdown()

		tablets[i] = &topodatapb.Tablet{
			Alias: &topodatapb.TabletAlias{
				Cell: "test",
				Uid:  uint32(addr.Port),
			},
			Hostname: addr.IP.String(),
			PortMap: map[string]int32{
				"grpc": int32(addr.Port),
			},
		}
	}

	poolSize := int(float64(numTablets) * 0.5)
	client := NewCachedConnClient(poolSize)
	defer client.Close()

	var dialAttempts, dialErrors atomic.Int64

	longestDials := make(chan time.Duration, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			attempts := 0
			jitter := time.Second * 0
			longestDial := time.Duration(0)

			for {
				select {
				case <-testCtx.Done():
					dialAttempts.Add(int64(attempts))
					longestDials <- longestDial
					return
				case <-time.After(jitter):
					jitter = time.Millisecond * (time.Duration(rand.Intn(11) + 50))
					attempts++

					tablet := tablets[rand.Intn(len(tablets))]
					start := time.Now()
					_, closer, err := client.dialer.dial(context.Background(), tablet)
					if err != nil {
						dialErrors.Add(1)
						continue
					}

					dialDuration := time.Since(start)
					if dialDuration > longestDial {
						longestDial = dialDuration
					}

					closer.Close()
				}
			}
		}()
	}

	time.Sleep(time.Minute)
	testCancel()
	wg.Wait()
	close(longestDials)

	longestDial := time.Duration(0)
	for dialDuration := range longestDials {
		if dialDuration > longestDial {
			longestDial = dialDuration
		}
	}

	attempts, errors := dialAttempts.Load(), dialErrors.Load()
	assert.Less(t, float64(errors)/float64(attempts), 0.001, fmt.Sprintf("fewer than 0.1%% of dial attempts should fail (attempts = %d, errors = %d, max running procs = %d)", attempts, errors, procs))
	assert.Less(t, errors, int64(1), "at least one dial attempt failed (attempts = %d, errors = %d)", attempts, errors)
	assert.Less(t, longestDial.Milliseconds(), int64(50))
}

func TestCachedConnClient_evictions(t *testing.T) {
	tmserv := tmrpctest.NewFakeRPCTM(t)
	tablets := make([]*topodatapb.Tablet, 5)
	for i := 0; i < len(tablets); i++ {
		addr, shutdown := grpcTestServer(t, tmserv)
		defer shutdown()

		tablets[i] = &topodatapb.Tablet{
			Alias: &topodatapb.TabletAlias{
				Cell: "test",
				Uid:  uint32(addr.Port),
			},
			Hostname: addr.IP.String(),
			PortMap: map[string]int32{
				"grpc": int32(addr.Port),
			},
		}
	}

	testCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	connHoldContext, connHoldCancel := context.WithCancel(testCtx)

	client := NewCachedConnClient(len(tablets) - 1)
	for i := 0; i < len(tablets)-1; i++ {
		_, closer, err := client.dialer.dial(context.Background(), tablets[i])
		t.Logf("holding connection open to %d", tablets[i].Alias.Uid)
		require.NoError(t, err)

		ctx := testCtx
		if i == 0 {
			ctx = connHoldContext
		}
		go func(ctx context.Context, closer io.Closer) {
			// Hold on to one connection until the test is done.
			// In the case of tablets[0], hold on to the connection until we
			// signal to close it.
			<-ctx.Done()
			closer.Close()
		}(ctx, closer)
	}

	dialCtx, dialCancel := context.WithTimeout(testCtx, time.Millisecond*50)
	defer dialCancel()

	err := client.Ping(dialCtx, tablets[0]) // this should take the rlock_fast path
	assert.NoError(t, err, "could not redial on inuse cached connection")

	err = client.Ping(dialCtx, tablets[4]) // this will enter the poll loop until context timeout
	assert.Error(t, err, "should have timed out waiting for an eviction, while all conns were held")

	// free up a connection
	connHoldCancel()

	dialCtx, dialCancel = context.WithTimeout(testCtx, time.Millisecond*100)
	defer dialCancel()

	err = client.Ping(dialCtx, tablets[4]) // this will enter the poll loop and evict a connection
	assert.NoError(t, err, "should have evicted a conn and succeeded to dial")
}
