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

package testlib

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtctl/grpcvtctlserver"
	"vitess.io/vitess/go/vt/vtctl/vtctlclient"

	// we need to import the grpcvtctlclient library so the gRPC
	// vtctl client is registered and can be used.
	_ "vitess.io/vitess/go/vt/vtctl/grpcvtctlclient"
)

var servenvInitialized sync.Once

// VtctlPipe is a vtctl server based on a topo server, and a client that
// is connected to it via gRPC.
type VtctlPipe struct {
	listener net.Listener
	client   vtctlclient.VtctlClient
	t        *testing.T
}

// NewVtctlPipe creates a new VtctlPipe based on the given topo server.
func NewVtctlPipe(t *testing.T, ts *topo.Server) *VtctlPipe {
	// Register all vtctl commands
	servenvInitialized.Do(func() {
		// make sure we use the right protocol
		flag.Set("vtctl_client_protocol", "grpc")

		// Enable all query groups
		flag.Set("enable_queries", "true")
		servenv.FireRunHooks()
	})

	// Listen on a random port
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Cannot listen: %v", err)
	}

	// Create a gRPC server and listen on the port
	server := grpc.NewServer()
	grpcvtctlserver.StartServer(server, ts)
	go server.Serve(listener)

	// Create a VtctlClient gRPC client to talk to the fake server
	client, err := vtctlclient.New(listener.Addr().String())
	if err != nil {
		t.Fatalf("Cannot create client: %v", err)
	}

	return &VtctlPipe{
		listener: listener,
		client:   client,
		t:        t,
	}
}

// Close will stop listening and free up all resources.
func (vp *VtctlPipe) Close() {
	vp.client.Close()
	vp.listener.Close()
}

// Run executes the provided command remotely, logs the output in the
// test logs, and returns the command error.
func (vp *VtctlPipe) Run(args []string) error {
	return vp.run(args, func(line string) {
		vp.t.Log(line)
	})
}

// RunAndOutput is similar to Run, but returns the output as a multi-line string
// instead of logging it.
func (vp *VtctlPipe) RunAndOutput(args []string) (string, error) {
	var output bytes.Buffer
	err := vp.run(args, func(line string) {
		output.WriteString(line)
	})
	return output.String(), err
}

func (vp *VtctlPipe) run(args []string, outputFunc func(string)) error {
	actionTimeout := 30 * time.Second
	ctx := context.Background()

	stream, err := vp.client.ExecuteVtctlCommand(ctx, args, actionTimeout)
	if err != nil {
		return fmt.Errorf("VtctlPipe.Run() failed: %v", err)
	}
	for {
		le, err := stream.Recv()
		switch err {
		case nil:
			outputFunc(logutil.EventString(le))
		case io.EOF:
			return nil
		default:
			return err
		}
	}
}

// RunAndStreamOutput returns the output of the vtctl command as a channel.
// When the channcel is closed, the command did finish.
func (vp *VtctlPipe) RunAndStreamOutput(args []string) (logutil.EventStream, error) {
	actionTimeout := 30 * time.Second
	ctx := context.Background()

	return vp.client.ExecuteVtctlCommand(ctx, args, actionTimeout)
}
