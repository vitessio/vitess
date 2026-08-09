/*
Copyright 2026 The Vitess Authors.

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
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseProfileFlag(t *testing.T) {
	tests := []struct {
		arg     string
		want    *profile
		wantErr bool
	}{
		{"", nil, false},
		{"mem", &profile{mode: profileMemHeap, rate: 4096}, false},
		{"mem,rate=1234", &profile{mode: profileMemHeap, rate: 1234}, false},
		{"mem,rate", nil, true},
		{"mem,rate=foobar", nil, true},
		{"mem=allocs", &profile{mode: profileMemAllocs, rate: 4096}, false},
		{"mem=allocs,rate=420", &profile{mode: profileMemAllocs, rate: 420}, false},
		{"block", &profile{mode: profileBlock, rate: 1}, false},
		{"block,rate=4", &profile{mode: profileBlock, rate: 4}, false},
		{"cpu", &profile{mode: profileCPU}, false},
		{"cpu,quiet", &profile{mode: profileCPU, quiet: true}, false},
		{"cpu,quiet=true", &profile{mode: profileCPU, quiet: true}, false},
		{"cpu,quiet=false", &profile{mode: profileCPU, quiet: false}, false},
		{"cpu,quiet=foobar", nil, true},
		{"cpu,path=", &profile{mode: profileCPU, path: ""}, false},
		{"cpu,path", nil, true},
		{"cpu,path=a", &profile{mode: profileCPU, path: "a"}, false},
		{"cpu,path=a/b/c/d", &profile{mode: profileCPU, path: "a/b/c/d"}, false},
		{"cpu,waitSig", &profile{mode: profileCPU, waitSig: true}, false},
		{"cpu,path=a/b,waitSig", &profile{mode: profileCPU, waitSig: true, path: "a/b"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.arg, func(t *testing.T) {
			var profileFlag []string
			if tt.arg != "" {
				profileFlag = strings.Split(tt.arg, ",")
			}
			got, err := parseProfileFlag(profileFlag)
			if (err != nil) != tt.wantErr {
				assert.Failf(t, "parseProfileFlag() unexpected error", "error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			assert.Equalf(t, tt.want, got, "parseProfileFlag() got = %v, want %v", got, tt.want)
		})
	}
}

// toggleProfiling sends SIGUSR1 and waits for profileStarted to reach want.
// The signal is re-sent on each poll because a SIGUSR1 that arrives between
// the listener's signal.Reset and signal.Notify is dropped by the OS.
func toggleProfiling(t *testing.T, want uint32) {
	t.Helper()
	require.Eventually(t, func() bool {
		if atomic.LoadUint32(&profileStarted) == want {
			return true
		}
		syscall.Kill(syscall.Getpid(), syscall.SIGUSR1)
		return atomic.LoadUint32(&profileStarted) == want
	}, 30*time.Second, 50*time.Millisecond)
}

// with waitSig, we should start with profiling off and toggle on-off-on-off
func TestPProfInitWithWaitSig(t *testing.T) {
	signal.Reset(syscall.SIGUSR1)

	oldFlag := pprofFlag
	t.Cleanup(func() { pprofFlag = oldFlag })
	pprofFlag = strings.Split("cpu,waitSig", ",")

	stop := startPprof()
	require.NotNil(t, stop)
	t.Cleanup(stop)

	assert.Eventually(t, func() bool {
		return atomic.LoadUint32(&profileStarted) == 0
	}, 30*time.Second, 10*time.Millisecond)

	toggleProfiling(t, 1)
	toggleProfiling(t, 0)
	toggleProfiling(t, 1)
	toggleProfiling(t, 0)
}

// without waitSig, we should start with profiling on and toggle off-on-off
func TestPProfInitWithoutWaitSig(t *testing.T) {
	signal.Reset(syscall.SIGUSR1)

	oldFlag := pprofFlag
	t.Cleanup(func() { pprofFlag = oldFlag })
	pprofFlag = strings.Split("cpu", ",")

	stop := startPprof()
	require.NotNil(t, stop)
	t.Cleanup(stop)

	assert.Eventually(t, func() bool {
		return atomic.LoadUint32(&profileStarted) == 1
	}, 30*time.Second, 10*time.Millisecond)

	toggleProfiling(t, 0)
	toggleProfiling(t, 1)
	toggleProfiling(t, 0)
}
