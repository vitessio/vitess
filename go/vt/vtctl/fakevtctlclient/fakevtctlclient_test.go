// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package fakevtctlclient implements the vtctlclient interface and is used as mock in unit tests.
package fakevtctlclient

import (
	"errors"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/context"
)

func TestStreamOutputAndError(t *testing.T) {
	verifyStreamOutputAndError(t, errors.New("something went wrong"))
}

func TestStreamOutput(t *testing.T) {
	verifyStreamOutputAndError(t, nil)
}

func verifyStreamOutputAndError(t *testing.T, wantErr error) {
	fake := NewFakeVtctlClient()
	args := []string{"CopySchemaShard", "test_keyspace/0", "test_keyspace/2"}
	output := []string{"event1", "event2"}
	err := fake.RegisterResult(args,
		strings.Join(output, "\n"),
		wantErr)
	if err != nil {
		t.Fatal(err)
	}

	stream, errFunc := fake.ExecuteVtctlCommand(context.Background(), args, time.Hour, 10*time.Second)

	// Verify output and error.
	i := 0
	for event := range stream {
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
	if errFunc() != wantErr {
		t.Errorf("Wrong error received. got: %v want: %v", errFunc(), wantErr)
	}
}

func TestNoResultRegistered(t *testing.T) {
	fake := NewFakeVtctlClient()
	stream, errFunc := fake.ExecuteVtctlCommand(context.Background(), []string{"ListShardTablets", "test_keyspace/0"}, time.Hour, 10*time.Second)
	if stream != nil {
		t.Fatalf("No stream should have been returned because no matching result is registered.")
	}
	if errFunc() == nil {
		t.Fatalf("Executing the command should fail because no matching result is registered.")
	}
	wantErr := "No response was registered for args: [ListShardTablets test_keyspace/0]"
	if errFunc().Error() != wantErr {
		t.Errorf("Wrong error for missing result was returned. got: '%v' want: '%v'", errFunc(), wantErr)
	}
}

func TestResultAlreadyRegistered(t *testing.T) {
	fake := NewFakeVtctlClient()
	errFirst := fake.RegisterResult([]string{"ListShardTablets", "test_keyspace/0"}, "output1", nil)
	if errFirst != nil {
		t.Fatalf("Registering the result should have been successful. Error: %v", errFirst)
	}

	errSecond := fake.RegisterResult([]string{"ListShardTablets", "test_keyspace/0"}, "output1", nil)
	if errSecond == nil {
		t.Fatal("Registering a duplicate result should not have been successful.")
	}
	want := "Result is already registered as: {output1 <nil>}"
	if errSecond.Error() != want {
		t.Fatalf("Wrong error message: got: '%v' want: '%v'", errSecond, want)
	}
}
