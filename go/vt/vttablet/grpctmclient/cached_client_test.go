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
	"math/rand"
	"net"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/nettest"
	"google.golang.org/grpc"

	"vitess.io/vitess/go/sync2"
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

func BenchmarkCachedConnClient(b *testing.B) {
	tmserv := tmrpctest.NewFakeRPCTM(b)
	tablets := make([]*topodatapb.Tablet, 4)
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

	b.ResetTimer()
	client := NewCachedConnClient(5)
	defer client.Close()

	for i := 0; i < b.N; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
		x := rand.Intn(len(tablets))
		err := client.Ping(ctx, tablets[x])
		if err != nil {
			b.Errorf("error pinging tablet %v: %w", tablets[x].Hostname, err)
		}

		cancel()
	}
}

func TestCachedConnClient(t *testing.T) {
	t.Parallel()

	if os.Getenv("VT_PPROF_TEST") != "" {
		file, err := os.Create(fmt.Sprintf("%s.profile.out", t.Name()))
		require.NoError(t, err)
		defer file.Close()
		if err := pprof.StartCPUProfile(file); err != nil {
			t.Errorf("failed to start cpu profile: %v", err)
			return
		}
		defer pprof.StopCPUProfile()
	}

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

	dialAttempts := sync2.NewAtomicInt64(0)
	dialErrors := sync2.NewAtomicInt64(0)

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

	attempts, errors := dialAttempts.Get(), dialErrors.Get()
	assert.Less(t, float64(errors)/float64(attempts), 0.001, fmt.Sprintf("fewer than 0.1%% of dial attempts should fail (attempts = %d, errors = %d, max running procs = %d)", attempts, errors, procs))
	assert.Less(t, errors, int64(1), "at least one dial attempt failed (attempts = %d, errors = %d)", attempts, errors)
	assert.Less(t, longestDial.Milliseconds(), int64(50))
}
