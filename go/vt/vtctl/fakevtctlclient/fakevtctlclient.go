// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package fakevtctlclient contains a fake for the vtctlclient interface.
package fakevtctlclient

import (
	"fmt"
	"strings"
	"time"

	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/vtctl/vtctlclient"
	"golang.org/x/net/context"
)

// FakeVtctlClient is a fake which implements the vtctlclient interface.
// The fake can be used to return a specific result for a given command.
// If the command is not registered, an error will be thrown.
type FakeVtctlClient struct {
	results map[argsKey]result
}

// argsKey is used as map KeyType because []string is not supported.
type argsKey struct {
	args string
}

// fromSlice returns a map key for a []string.
func fromSlice(args []string) argsKey {
	return argsKey{strings.Join(args, "|")}
}

// result contains the result the fake should respond for a given command.
type result struct {
	output string
	err    error
}

// NewFakeVtctlClient creates a FakeVtclClient struct.
func NewFakeVtctlClient() *FakeVtctlClient {
	return &FakeVtctlClient{make(map[argsKey]result)}
}

// FakeVtctlClientFactory always returns the current instance.
func (f *FakeVtctlClient) FakeVtctlClientFactory(addr string, dialTimeout time.Duration) (vtctlclient.VtctlClient, error) {
	return f, nil
}

// ExecuteVtctlCommand is part of the vtctlclient interface.
func (f *FakeVtctlClient) ExecuteVtctlCommand(ctx context.Context, args []string, actionTimeout, lockTimeout time.Duration) (<-chan *logutil.LoggerEvent, vtctlclient.ErrFunc) {
	result, ok := f.results[fromSlice(args)]
	if !ok {
		return nil, func() error { return fmt.Errorf("No response was registered for args: %v", args) }
	}

	stream := make(chan *logutil.LoggerEvent)
	go func() {
		// Each line of the multi-line string "output" is streamed as console text.
		for _, line := range strings.Split(result.output, "\n") {
			stream <- &logutil.LoggerEvent{
				Time:  time.Now(),
				Level: logutil.LOGGER_CONSOLE,
				File:  "fakevtctlclient",
				Line:  -1,
				Value: line,
			}
		}
		close(stream)
	}()

	if result.err != nil {
		return stream, func() error { return result.err }
	}

	return stream, func() error { return nil }
}

// Close is part of the vtctlclient interface.
func (f *FakeVtctlClient) Close() {}

// RegisterResult registers for a given command (args) the result which the fake should return.
func (f *FakeVtctlClient) RegisterResult(args []string, output string, err error) error {
	argsKey := fromSlice(args)
	if result, ok := f.results[argsKey]; ok {
		return fmt.Errorf("Result is already registered as: %v", result)
	}
	f.results[argsKey] = result{output, err}
	return nil
}
