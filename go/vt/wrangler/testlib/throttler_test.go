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

package testlib

import (
	"fmt"
	"net"
	"strings"
	"testing"

	"google.golang.org/grpc"

	"github.com/youtube/vitess/go/vt/throttler"
	"github.com/youtube/vitess/go/vt/throttler/grpcthrottlerserver"
	"github.com/youtube/vitess/go/vt/topo/memorytopo"

	// The test uses the gRPC throttler client and server implementations.
	_ "github.com/youtube/vitess/go/vt/throttler/grpcthrottlerclient"
)

// TestVtctlThrottlerCommands tests all vtctl commands from the
// "Resharding Throttler" group.
func TestVtctlThrottlerCommands(t *testing.T) {
	// Run a throttler server using the default process throttle manager.
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Cannot listen: %v", err)
	}
	s := grpc.NewServer()
	grpcthrottlerserver.RegisterServer(s, throttler.GlobalManager)
	go s.Serve(listener)

	addr := fmt.Sprintf("localhost:%v", listener.Addr().(*net.TCPAddr).Port)

	ts := memorytopo.NewServer("cell1", "cell2")
	vp := NewVtctlPipe(t, ts)
	defer vp.Close()

	// Get and set rate commands do not fail when no throttler is registered.
	{
		got, err := vp.RunAndOutput([]string{"ThrottlerMaxRates", "-server", addr})
		if err != nil {
			t.Fatalf("VtctlPipe.RunAndStreamOutput() failed: %v", err)
		}
		want := "no active throttlers"
		if !strings.Contains(got, want) {
			t.Fatalf("ThrottlerMaxRates() = %v,  want substring = %v", got, want)
		}
	}

	{
		got, err := vp.RunAndOutput([]string{"ThrottlerSetMaxRate", "-server", addr, "23"})
		if err != nil {
			t.Fatalf("VtctlPipe.RunAndStreamOutput() failed: %v", err)
		}
		want := "no active throttlers"
		if !strings.Contains(got, want) {
			t.Fatalf("ThrottlerSetMaxRate(23) = %v, want substring = %v", got, want)
		}
	}

	// Add a throttler and check the commands again.
	t1, err := throttler.NewThrottler("t1", "TPS", 1 /* threadCount */, 2323, throttler.ReplicationLagModuleDisabled)
	if err != nil {
		t.Fatal(err)
	}
	defer t1.Close()
	// MaxRates() will return the initial rate.
	expectRate(t, vp, addr, "2323")

	// Disable the module by setting the rate to 'unlimited'.
	setRate(t, vp, addr, "unlimited")
	expectRate(t, vp, addr, "unlimited")

	// Re-enable it by setting a limit.
	setRate(t, vp, addr, "9999")
	expectRate(t, vp, addr, "9999")
}

func setRate(t *testing.T, vp *VtctlPipe, addr, rateStr string) {
	got, err := vp.RunAndOutput([]string{"ThrottlerSetMaxRate", "-server", addr, rateStr})
	if err != nil {
		t.Fatalf("VtctlPipe.RunAndStreamOutput() failed: %v", err)
	}
	want := "t1"
	if !strings.Contains(got, want) {
		t.Fatalf("ThrottlerSetMaxRate(%v) = %v, want substring = %v", rateStr, got, want)
	}
}

func expectRate(t *testing.T, vp *VtctlPipe, addr, rateStr string) {
	got, err := vp.RunAndOutput([]string{"ThrottlerMaxRates", "-server", addr})
	if err != nil {
		t.Fatalf("VtctlPipe.RunAndStreamOutput() failed: %v", err)
	}
	want := "1 active throttler"
	if !strings.Contains(got, want) {
		t.Fatalf("ThrottlerMaxRates() = %v, want substring = %v", got, want)
	}
	want2 := fmt.Sprintf("| %v |", rateStr)
	if !strings.Contains(got, want2) {
		t.Fatalf("ThrottlerMaxRates() = %v, want substring = %v", got, want2)
	}
}
