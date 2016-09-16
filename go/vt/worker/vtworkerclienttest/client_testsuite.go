// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package vtworkerclienttest contains the testsuite against which each
// RPC implementation of the vtworkerclient interface must be tested.
package vtworkerclienttest

// NOTE: This file is not test-only code because it is referenced by
// tests in other packages and therefore it has to be regularly
// visible.

// NOTE: This code is in its own package such that its dependencies
// (e.g.  zookeeper) won't be drawn into production binaries as well.

import (
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/logutil"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
	"github.com/youtube/vitess/go/vt/tabletmanager/tmclient"
	"github.com/youtube/vitess/go/vt/vterrors"
	"github.com/youtube/vitess/go/vt/worker"
	"github.com/youtube/vitess/go/vt/worker/vtworkerclient"
	"github.com/youtube/vitess/go/vt/zktopo/zktestserver"

	// Import the gRPC client implementation for tablet manager because the real
	// vtworker implementation requires it.
	_ "github.com/youtube/vitess/go/vt/tabletmanager/grpctmclient"
)

func init() {
	// enforce we will use the right protocol (gRPC) (note the
	// client is unused, but it is initialized, so it needs to exist)
	*tmclient.TabletManagerProtocol = "grpc"
}

// CreateWorkerInstance returns a properly configured vtworker instance.
func CreateWorkerInstance(t *testing.T) *worker.Instance {
	ts := zktestserver.New(t, []string{"cell1", "cell2"})
	return worker.NewInstance(context.Background(), ts, "cell1", 1*time.Second)
}

// TestSuite runs the test suite on the given vtworker and vtworkerclient.
func TestSuite(t *testing.T, c vtworkerclient.Client) {
	commandSucceeds(t, c)

	commandErrors(t, c)

	commandErrorsBecauseBusy(t, c)

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
	got, err = stream.Recv()
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
		return fmt.Errorf("cannot execute remote command: %v", err)
	}

	for {
		_, err := stream.Recv()
		switch err {
		case nil:
			// Consume next response.
		case io.EOF:
			return nil
		default:
			return vterrors.WithPrefix("unexpected error when reading the stream: ", err)
		}
	}
}

func commandErrorsBecauseBusy(t *testing.T, client vtworkerclient.Client) {
	// Run the vtworker "Block" command which blocks until we cancel the context.
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	// blockCommandStarted will be closed after we're sure that vtworker is
	// running the "Block" command.
	blockCommandStarted := make(chan struct{})
	var blockErr error
	wg.Add(1)
	go func() {
		stream, err := client.ExecuteVtworkerCommand(ctx, []string{"Block"})
		if err != nil {
			t.Fatalf("Block command should not have failed: %v", err)
		}

		firstLineReceived := false
		for {
			if _, err := stream.Recv(); err != nil {
				if vterrors.RecoverVtErrorCode(err) != vtrpcpb.ErrorCode_CANCELLED {
					blockErr = fmt.Errorf("Block command should only error due to canceled context: %v", err)
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
		wg.Done()
	}()

	// Try to run a second, concurrent vtworker command.
	// vtworker should send an error back that it's busy and we should retry later.
	<-blockCommandStarted
	gotErr := runVtworkerCommand(client, []string{"Ping", "Are you busy?"})
	wantCode := vtrpcpb.ErrorCode_TRANSIENT_ERROR
	if gotCode := vterrors.RecoverVtErrorCode(gotErr); gotCode != wantCode {
		t.Fatalf("wrong error code for second cmd: got = %v, want = %v, err: %v", gotCode, wantCode, gotErr)
	}

	// Cancel Block.
	cancel()
	wg.Wait()
	if blockErr != nil {
		t.Fatalf("Block command should not have failed: %v", blockErr)
	}
	// It looks like gRPC cancels the RPC only on the client-side. Therefore, we
	// have to explicitly cancel the (still) running vtworker command.
	if err := runVtworkerCommand(client, []string{"Cancel"}); err != nil {
		t.Fatal(err)
	}
	// vtworker is now in a special state where the current job is already
	// canceled but not reset yet. New commands are still failing with a
	// retryable error.
	gotErr2 := runVtworkerCommand(client, []string{"Ping", "canceled and still busy?"})
	wantCode2 := vtrpcpb.ErrorCode_TRANSIENT_ERROR
	if gotCode2 := vterrors.RecoverVtErrorCode(gotErr2); gotCode2 != wantCode2 {
		t.Fatalf("wrong error code for second cmd before reset: got = %v, want = %v, err: %v", gotCode2, wantCode2, gotErr2)
	}

	if err := runVtworkerCommand(client, []string{"Reset"}); err != nil {
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
