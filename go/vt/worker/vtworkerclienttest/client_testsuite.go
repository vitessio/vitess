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

// Package vtworkerclienttest contains the testsuite against which each
// RPC implementation of the vtworkerclient interface must be tested.
package vtworkerclienttest

// NOTE: This file is not test-only code because it is referenced by
// tests in other packages and therefore it has to be regularly
// visible.

// NOTE: This code is in its own package such that its dependencies
// (e.g.  zookeeper) won't be drawn into production binaries as well.

import (
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"context"

	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	"vitess.io/vitess/go/vt/worker"
	"vitess.io/vitess/go/vt/worker/vtworkerclient"

	// Import the gRPC client implementation for tablet manager because the real
	// vtworker implementation requires it.
	_ "vitess.io/vitess/go/vt/vttablet/grpctmclient"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

func init() {
	// enforce we will use the right protocol (gRPC) (note the
	// client is unused, but it is initialized, so it needs to exist)
	*tmclient.TabletManagerProtocol = "grpc"
}

// CreateWorkerInstance returns a properly configured vtworker instance.
func CreateWorkerInstance(t *testing.T) *worker.Instance {
	ts := memorytopo.NewServer("cell1", "cell2")
	return worker.NewInstance(ts, "cell1", 1*time.Second)
}

// TestSuite runs the test suite on the given vtworker and vtworkerclient.
func TestSuite(t *testing.T, c vtworkerclient.Client) {
	commandSucceeds(t, c)

	commandErrors(t, c)

	commandErrorsBecauseBusy(t, c, false /* client side cancelation */)

	commandErrorsBecauseBusy(t, c, true /* server side cancelation */)

	commandPanics(t, c)
}

func commandSucceeds(t *testing.T, client vtworkerclient.Client) {
	stream, err := client.ExecuteVtworkerCommand(context.Background(), []string{"Ping", "pong"})
	if err != nil {
		t.Fatalf("Cannot execute remote command: %v", err)
	}

	got, err := stream.Recv()
	if err != nil {
		t.Fatalf("failed to get first line: %v", err)
	}
	expected := "Ping command was called with message: 'pong'.\n"
	if logutil.EventString(got) != expected {
		t.Errorf("Got unexpected log line '%v' expected '%v'", got.String(), expected)
	}
	_, err = stream.Recv()
	if err != io.EOF {
		t.Fatalf("Didn't get EOF as expected: %v", err)
	}

	// Reset vtworker for the next test function.
	if err := runVtworkerCommand(client, []string{"Reset"}); err != nil {
		t.Fatal(err)
	}
}

func runVtworkerCommand(client vtworkerclient.Client, args []string) error {
	stream, err := client.ExecuteVtworkerCommand(context.Background(), args)
	if err != nil {
		return vterrors.Wrap(err, "cannot execute remote command")
	}

	for {
		_, err := stream.Recv()
		switch err {
		case nil:
			// Consume next response.
		case io.EOF:
			return nil
		default:
			return vterrors.Wrap(err, "unexpected error when reading the stream")
		}
	}
}

// commandErrorsBecauseBusy tests that concurrent commands are rejected with
// TRANSIENT_ERROR while a command is already running.
// It also tests the correct propagation of the CANCELED error code.
func commandErrorsBecauseBusy(t *testing.T, client vtworkerclient.Client, serverSideCancelation bool) {
	// Run the vtworker "Block" command which blocks until we cancel the context.
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	// blockCommandStarted will be closed after we're sure that vtworker is
	// running the "Block" command.
	blockCommandStarted := make(chan struct{})
	var errorCodeCheck error
	wg.Add(1)
	errChan := make(chan error, 1)
	defer close(errChan)
	go func() {
		defer wg.Done()
		stream, err := client.ExecuteVtworkerCommand(ctx, []string{"Block"})
		if err != nil {
			errChan <- err
			close(blockCommandStarted)
			return
		} else {
			errChan <- nil
		}

		firstLineReceived := false
		for {
			if _, err := stream.Recv(); err != nil {
				// We see CANCELED from the RPC client (client side cancelation) or
				// from vtworker itself (server side cancelation).
				if vterrors.Code(err) != vtrpcpb.Code_CANCELED {
					errorCodeCheck = vterrors.Wrap(err, "Block command should only error due to canceled context")
				}
				// Stream has finished.
				break
			}

			if !firstLineReceived {
				firstLineReceived = true
				// The first log line will come from the "Block" command, so we are sure
				// now that vtworker is actually executing it.
				close(blockCommandStarted)
			}
		}
	}()

	// Try to run a second, concurrent vtworker command.
	// vtworker should send an error back that it's busy and we should retry later.
	<-blockCommandStarted
	gotErr := runVtworkerCommand(client, []string{"Ping", "Are you busy?"})
	wantCode := vtrpcpb.Code_UNAVAILABLE
	if gotCode := vterrors.Code(gotErr); gotCode != wantCode {
		t.Fatalf("wrong error code for second cmd: got = %v, want = %v, err: %v", gotCode, wantCode, gotErr)
	}

	// Cancel running "Block" command.
	if serverSideCancelation {
		if err := runVtworkerCommand(client, []string{"Cancel"}); err != nil {
			t.Fatal(err)
		}
	}
	// Always cancel the context to not leak it (regardless of client or server
	// side cancelation).
	cancel()

	wg.Wait()
	if err := <-errChan; err != nil {
		t.Fatalf("Block command should not have failed: %v", err)
	}
	if errorCodeCheck != nil {
		t.Fatalf("Block command did not return the CANCELED error code: %v", errorCodeCheck)
	}

	// vtworker is now in a special state where the current job is already
	// canceled but not reset yet. New commands are still failing with a
	// retryable error.
	gotErr2 := runVtworkerCommand(client, []string{"Ping", "canceled and still busy?"})
	wantCode2 := vtrpcpb.Code_UNAVAILABLE
	if gotCode2 := vterrors.Code(gotErr2); gotCode2 != wantCode2 {
		t.Fatalf("wrong error code for second cmd before reset: got = %v, want = %v, err: %v", gotCode2, wantCode2, gotErr2)
	}

	// Reset vtworker for the next test function.
	if err := resetVtworker(t, client); err != nil {
		t.Fatal(err)
	}

	// Second vtworker command should succeed now after the first has finished.
	if err := runVtworkerCommand(client, []string{"Ping", "You should not be busy anymore!"}); err != nil {
		t.Fatalf("second cmd should not have failed: %v", err)
	}

	// Reset vtworker for the next test function.
	if err := runVtworkerCommand(client, []string{"Reset"}); err != nil {
		t.Fatal(err)
	}
}

// resetVtworker will retry to "Reset" vtworker until it succeeds.
// Retries are necessary to cope with the following race:
// a) RPC started vtworker command e.g. "Block".
// b) client cancels RPC and triggers vtworker to cancel the running command.
// c) RPC returns with a response after cancelation was received by vtworker.
// d) vtworker is still canceling and shutting down the command.
// e) A new vtworker command e.g. "Reset" would fail at this point with
// "vtworker still executing" until the cancelation is complete.
func resetVtworker(t *testing.T, client vtworkerclient.Client) error {
	start := time.Now()
	attempts := 0
	for {
		attempts++
		err := runVtworkerCommand(client, []string{"Reset"})

		if err == nil {
			return nil
		}

		if time.Since(start) > 5*time.Second {
			return vterrors.Wrapf(err, "Reset was not successful after 5s and %d attempts", attempts)
		}

		if !strings.Contains(err.Error(), "worker still executing") {
			return vterrors.Wrap(err, "Reset must not fail")
		}

		t.Logf("retrying to Reset vtworker because the previous command has not finished yet. got err: %v", err)
		continue
	}
}

func commandErrors(t *testing.T, client vtworkerclient.Client) {
	stream, err := client.ExecuteVtworkerCommand(context.Background(), []string{"NonexistingCommand"})
	// The expected error could already be seen now or after the output channel is closed.
	// To avoid checking for the same error twice, we don't check it here yet.

	if err == nil {
		// Don't check for errors until the output channel is closed.
		// We expect the usage to be sent as output. However, we have to consider it
		// optional and do not test for it because not all RPC implementations send
		// the output after an error.
		for {
			_, err = stream.Recv()
			if err != nil {
				break
			}
		}
	}

	expected := "unknown command: NonexistingCommand"
	if err == nil || !strings.Contains(err.Error(), expected) {
		t.Fatalf("Unexpected remote error, got: '%v' was expecting to find '%v'", err, expected)
	}
}

func commandPanics(t *testing.T, client vtworkerclient.Client) {
	stream, err := client.ExecuteVtworkerCommand(context.Background(), []string{"Panic"})
	// The expected error could already be seen now or after the output channel is closed.
	// To avoid checking for the same error twice, we don't check it here yet.

	if err == nil {
		// Don't check for errors until the output channel is closed.
		// No output expected in this case.
		_, err = stream.Recv()
	}

	expected := "uncaught vtworker panic: Panic command was called. This should be caught by the vtworker framework and logged as an error."
	if err == nil || !strings.Contains(err.Error(), expected) {
		t.Fatalf("Unexpected remote error, got: '%v' was expecting to find '%v'", err, expected)
	}
}
