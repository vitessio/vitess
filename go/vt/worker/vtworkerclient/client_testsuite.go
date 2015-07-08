// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtworkerclient

// NOTE: This file is not test-only code because it is referenced by tests in
//			 other packages and therefore it has to be regularly visible.

import (
	"strings"
	"testing"
	"time"

	// register the go rpc tablet manager client
	_ "github.com/youtube/vitess/go/vt/tabletmanager/gorpctmclient"
	"github.com/youtube/vitess/go/vt/worker"
	"github.com/youtube/vitess/go/vt/zktopo"
	"golang.org/x/net/context"
)

// CreateWorkerInstance returns a properly configured vtworker instance.
func CreateWorkerInstance(t *testing.T) *worker.Instance {
	ts := zktopo.NewTestServer(t, []string{"cell1", "cell2"})
	return worker.NewInstance(ts, "cell1", 30*time.Second, 1*time.Second)
}

// TestSuite runs the test suite on the given vtworker and vtworkerclient
func TestSuite(t *testing.T, wi *worker.Instance, c VtworkerClient) {
	commandSucceeds(t, c)

	commandErrors(t, c)

	commandPanics(t, c)
}

func commandSucceeds(t *testing.T, client VtworkerClient) {
	logs, errFunc := client.ExecuteVtworkerCommand(context.Background(), []string{"Ping", "pong"})
	if err := errFunc(); err != nil {
		t.Fatalf("Cannot execute remote command: %v", err)
	}

	count := 0
	for e := range logs {
		expected := "Ping command was called with message: 'pong'.\n"
		if e.String() != expected {
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

	_, errFuncReset := client.ExecuteVtworkerCommand(context.Background(), []string{"Reset"})
	if err := errFuncReset(); err != nil {
		t.Fatalf("Cannot execute remote command: %v", err)
	}
}

func commandErrors(t *testing.T, client VtworkerClient) {
	logs, errFunc := client.ExecuteVtworkerCommand(context.Background(), []string{"NonexistingCommand"})
	if err := errFunc(); err != nil {
		t.Fatalf("Cannot execute remote command: %v", err)
	}

	outputNotEmpty := false
	for {
		// Consume all results to make sure we'll see the error at the end.
		_, ok := <-logs
		if ok {
			outputNotEmpty = true
		} else {
			break
		}
	}
	if !outputNotEmpty {
		t.Fatal("Expected usage as output, but received nothing.")
	}

	expected := "unknown command: NonexistingCommand"
	if err := errFunc(); err == nil || !strings.Contains(err.Error(), expected) {
		t.Fatalf("Unexpected remote error, got: '%v' was expecting to find '%v'", err, expected)
	}
}

func commandPanics(t *testing.T, client VtworkerClient) {
	logs, errFunc := client.ExecuteVtworkerCommand(context.Background(), []string{"Panic"})
	if err := errFunc(); err != nil {
		t.Fatalf("Cannot execute remote command: %v", err)
	}
	if e, ok := <-logs; ok {
		t.Errorf("Got unexpected line for logs: %v", e.String())
	}
	expected := "uncaught vtworker panic: Panic command was called. This should be caught by the vtworker framework and logged as an error."
	if err := errFunc(); err == nil || !strings.Contains(err.Error(), expected) {
		t.Fatalf("Unexpected remote error, got: '%v' was expecting to find '%v'", err, expected)
	}
}
