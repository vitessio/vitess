// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fakevtctlclient

import (
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"

	logutilpb "github.com/youtube/vitess/go/vt/proto/logutil"
)

func TestStreamOutputAndError(t *testing.T) {
	fake := NewFakeLoggerEventStreamingClient()
	args := []string{"CopySchemaShard", "test_keyspace/0", "test_keyspace/2"}
	output := []string{"event1", "event2"}
	wantErr := errors.New("something went wrong")

	err := fake.RegisterResult(args, strings.Join(output, "\n"), wantErr)
	if err != nil {
		t.Fatalf("Failed to register fake result for: %v err: %v", args, err)
	}

	verifyStreamOutputAndError(t, fake, "" /* addr */, args, output, wantErr)
}

func TestStreamOutput(t *testing.T) {
	fake := NewFakeLoggerEventStreamingClient()
	args := []string{"CopySchemaShard", "test_keyspace/0", "test_keyspace/2"}
	output := []string{"event1", "event2"}
	var wantErr error

	err := fake.RegisterResult(args, strings.Join(output, "\n"), wantErr)
	if err != nil {
		t.Fatalf("Failed to register fake result for: %v err: %v", args, err)
	}

	verifyStreamOutputAndError(t, fake, "" /* addr */, args, output, wantErr)
}

// TestStreamOutputForAddr is similar to TestStreamOutput but also tests that
// the correct server address was used by the client.
func TestStreamOutputForAddr(t *testing.T) {
	fake := NewFakeLoggerEventStreamingClient()
	addr := "localhost:12345"
	args := []string{"CopySchemaShard", "test_keyspace/0", "test_keyspace/2"}
	output := []string{"event1", "event2"}
	var wantErr error

	// Used address matches.
	err := fake.RegisterResultForAddr(addr, args, strings.Join(output, "\n"), wantErr)
	if err != nil {
		t.Fatalf("Failed to register fake result for: %v err: %v", args, err)
	}
	verifyStreamOutputAndError(t, fake, addr, args, output, wantErr)

	// Used address does not match.
	err = fake.RegisterResultForAddr(addr, args, strings.Join(output, "\n"), wantErr)
	if err != nil {
		t.Fatalf("Failed to register fake result for: %v err: %v", args, err)
	}
	_, err = fake.StreamResult("different-addr", args)
	if err == nil || !strings.Contains(err.Error(), "client sent request to wrong server address") {
		t.Fatalf("fake should have failed because the client used the wrong address: %v", err)
	}
}

func verifyStreamOutputAndError(t *testing.T, fake *FakeLoggerEventStreamingClient, addr string, args, output []string, wantErr error) {
	stream, err := fake.StreamResult(addr, args)
	if err != nil {
		t.Fatalf("Failed to stream result: %v", err)
	}

	// Verify output and error.
	i := 0
	for {
		var event *logutilpb.Event
		event, err = stream.Recv()
		if err != nil {
			break
		}
		if i > len(output) {
			t.Fatalf("Received more events than expected. got: %v want: %v", i, len(output))
		}
		if event.Value != output[i] {
			t.Errorf("Received event is not identical to the received one. got: %v want: %v", event.Value, output[i])
		}
		t.Logf("Received event: %v", event)
		i++
	}
	if i != len(output) {
		t.Errorf("Number of received events mismatches. got: %v want: %v", i, len(output))
	}
	if err == io.EOF {
		err = nil
	}
	if err != wantErr {
		t.Errorf("Wrong error received. got: %v want: %v", err, wantErr)
	}
}

func TestNoResultRegistered(t *testing.T) {
	fake := NewFakeLoggerEventStreamingClient()
	stream, err := fake.StreamResult("" /* addr */, []string{"ListShardTablets", "test_keyspace/0"})
	if stream != nil {
		t.Fatalf("No stream should have been returned because no matching result is registered.")
	}
	wantErr := "no response was registered for args: [ListShardTablets test_keyspace/0]"
	if err.Error() != wantErr {
		t.Errorf("Wrong error for missing result was returned. got: '%v' want: '%v'", err, wantErr)
	}
}

func TestResultAlreadyRegistered(t *testing.T) {
	fake := NewFakeLoggerEventStreamingClient()
	errFirst := fake.RegisterResult([]string{"ListShardTablets", "test_keyspace/0"}, "output1", nil)
	if errFirst != nil {
		t.Fatalf("Registering the result should have been successful. Error: %v", errFirst)
	}

	errSecond := fake.RegisterResult([]string{"ListShardTablets", "test_keyspace/0"}, "output2", nil)
	if errSecond == nil {
		t.Fatal("Registering a duplicate, different result should not have been successful.")
	}
	want := ") is already registered for command: "
	if !strings.Contains(errSecond.Error(), want) {
		t.Fatalf("Wrong error message: got: '%v' want: '%v'", errSecond, want)
	}
}

func TestRegisterMultipleResultsForSameCommand(t *testing.T) {
	fake := NewFakeLoggerEventStreamingClient()
	args := []string{"CopySchemaShard", "test_keyspace/0", "test_keyspace/2"}
	output := []string{"event1", "event2"}
	var wantErr error

	// Register first result.
	err := fake.RegisterResult(args, strings.Join(output, "\n"), wantErr)
	if err != nil {
		t.Fatalf("Failed to register fake result for: %v err: %v", args, err)
	}
	registeredCommands := []string{strings.Join(args, " ")}
	verifyListOfRegisteredCommands(t, fake, registeredCommands)

	// Register second result.
	err = fake.RegisterResult(args, strings.Join(output, "\n"), wantErr)
	if err != nil {
		t.Fatalf("Failed to register fake result for: %v err: %v", args, err)
	}
	verifyListOfRegisteredCommands(t, fake, registeredCommands)

	// Consume first result.
	verifyStreamOutputAndError(t, fake, "" /* addr */, args, output, wantErr)
	verifyListOfRegisteredCommands(t, fake, registeredCommands)

	// Consume second result.
	verifyStreamOutputAndError(t, fake, "" /* addr */, args, output, wantErr)
	verifyListOfRegisteredCommands(t, fake, []string{})
}

func verifyListOfRegisteredCommands(t *testing.T, fake *FakeLoggerEventStreamingClient, want []string) {
	got := fake.RegisteredCommands()
	if len(got) == 0 && len(want) == 0 {
		return
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("fake.RegisteredCommands() = %v, want: %v", got, want)
	}
}
