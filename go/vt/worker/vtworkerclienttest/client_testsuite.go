// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package vtworkerclienttest contains the testsuite against which each
// RPC implementation of the vtworkerclient interface must be tested.
package vtworkerclienttest

// NOTE: This file is not test-only code because it is referenced by tests in
//			 other packages and therefore it has to be regularly visible.

// NOTE: This code is in its own package such that its dependencies (e.g.
//       zookeeper) won't be drawn into production binaries as well.

import (
	"strings"
	"testing"
	"time"

	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/tabletmanager/tmclient"
	"github.com/youtube/vitess/go/vt/worker"
	"github.com/youtube/vitess/go/vt/worker/vtworkerclient"

	"github.com/youtube/vitess/go/vt/zktopo"
	"golang.org/x/net/context"

	// import the gRPC client implementation for tablet manager
	_ "github.com/youtube/vitess/go/vt/tabletmanager/grpctmclient"
)

func init() {
	// enforce we will use the right protocol (gRPC) (note the
	// client is unused, but it is initialized, so it needs to exist)
	*tmclient.TabletManagerProtocol = "grpc"
}

// CreateWorkerInstance returns a properly configured vtworker instance.
func CreateWorkerInstance(t *testing.T) *worker.Instance {
	ts := zktopo.NewTestServer(t, []string{"cell1", "cell2"})
	return worker.NewInstance(context.Background(), ts, "cell1", 1*time.Second)
}

// TestSuite runs the test suite on the given vtworker and vtworkerclient
func TestSuite(t *testing.T, wi *worker.Instance, c vtworkerclient.Client) {
	commandSucceeds(t, c)

	commandErrors(t, c)

	commandPanics(t, c)
}

func commandSucceeds(t *testing.T, client vtworkerclient.Client) {
	logs, errFunc, err := client.ExecuteVtworkerCommand(context.Background(), []string{"Ping", "pong"})
	if err != nil {
		t.Fatalf("Cannot execute remote command: %v", err)
	}

	count := 0
	for e := range logs {
		expected := "Ping command was called with message: 'pong'.\n"
		if logutil.EventString(e) != expected {
			t.Errorf("Got unexpected log line '%v' expected '%v'", e.String(), expected)
		}
		count++
	}
	if count != 1 {
		t.Errorf("Didn't get expected log line only, got %v lines", count)
	}

	if err := errFunc(); err != nil {
		t.Fatalf("Remote error: %v", err)
	}

	logs, errFunc, err = client.ExecuteVtworkerCommand(context.Background(), []string{"Reset"})
	if err != nil {
		t.Fatalf("Cannot execute remote command: %v", err)
	}
	for _ = range logs {
	}
	if err := errFunc(); err != nil {
		t.Fatalf("Cannot execute remote command: %v", err)
	}
}

func commandErrors(t *testing.T, client vtworkerclient.Client) {
	logs, errFunc, err := client.ExecuteVtworkerCommand(context.Background(), []string{"NonexistingCommand"})
	// The expected error could already be seen now or after the output channel is closed.
	// To avoid checking for the same error twice, we don't check it here yet.

	if err == nil {
		// Don't check for errors until the output channel is closed.
		// We expect the usage to be sent as output. However, we have to consider it
		// optional and do not test for it because not all RPC implementations send
		// the output after an error.
		for {
			if _, ok := <-logs; !ok {
				break
			}
		}
		err = errFunc()
	}

	expected := "unknown command: NonexistingCommand"
	if err == nil || !strings.Contains(err.Error(), expected) {
		t.Fatalf("Unexpected remote error, got: '%v' was expecting to find '%v'", err, expected)
	}
}

func commandPanics(t *testing.T, client vtworkerclient.Client) {
	logs, errFunc, err := client.ExecuteVtworkerCommand(context.Background(), []string{"Panic"})
	// The expected error could already be seen now or after the output channel is closed.
	// To avoid checking for the same error twice, we don't check it here yet.

	if err == nil {
		// Don't check for errors until the output channel is closed.
		// No output expected in this case.
		if e, ok := <-logs; ok {
			t.Errorf("Got unexpected line for logs: %v", e.String())
		}
		err = errFunc()
	}

	expected := "uncaught vtworker panic: Panic command was called. This should be caught by the vtworker framework and logged as an error."
	if err == nil || !strings.Contains(err.Error(), expected) {
		t.Fatalf("Unexpected remote error, got: '%v' was expecting to find '%v'", err, expected)
	}
}
