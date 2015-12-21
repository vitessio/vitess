// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fakevtctlclient

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/youtube/vitess/go/vt/logutil"

	logutilpb "github.com/youtube/vitess/go/vt/proto/logutil"
)

// FakeLoggerEventStreamingClient is the base for the fakes for the vtctlclient and vtworkerclient.
// It allows to register a (multi-)line string for a given command and return the result as channel which streams it back.
type FakeLoggerEventStreamingClient struct {
	results map[string]*result
	// mu guards all fields of the structs.
	mu sync.Mutex
}

// NewFakeLoggerEventStreamingClient creates a new fake.
func NewFakeLoggerEventStreamingClient() *FakeLoggerEventStreamingClient {
	return &FakeLoggerEventStreamingClient{results: make(map[string]*result)}
}

// generateKey returns a map key for a []string.
// ([]string is not supported as map key.)
func generateKey(args []string) string {
	return strings.Join(args, " ")
}

// result contains the result the fake should respond for a given command.
type result struct {
	output string
	err    error
	// count is the number of times this result is registered for the same
	// command. With each stream of this result, count will be decreased by one.
	count int
}

func (r1 result) Equals(r2 result) bool {
	return r1.output == r2.output &&
		((r1.err == nil && r2.err == nil) ||
			(r1.err != nil && r2.err != nil && r1.err.Error() == r2.err.Error()))
}

// RegisterResult registers for a given command (args) the result which the fake should return.
// Once the result was returned, it will be automatically deregistered.
func (f *FakeLoggerEventStreamingClient) RegisterResult(args []string, output string, err error) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	k := generateKey(args)
	v := result{output, err, 1}
	if result, ok := f.results[k]; ok {
		if result.Equals(v) {
			result.count++
			return nil
		}
		return fmt.Errorf("A different result (%v) is already registered for command: %v", result, args)
	}
	f.results[k] = &v
	return nil
}

// RegisteredCommands returns a list of commands which are currently registered.
// This is useful to check that all registered results have been consumed.
func (f *FakeLoggerEventStreamingClient) RegisteredCommands() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	var commands []string
	for k := range f.results {
		commands = append(commands, k)
	}
	return commands
}

// StreamResult returns a channel which streams back a registered result as logging events.
func (f *FakeLoggerEventStreamingClient) StreamResult(args []string) (<-chan *logutilpb.Event, func() error, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	k := generateKey(args)
	result, ok := f.results[k]
	if !ok {
		return nil, nil, fmt.Errorf("No response was registered for args: %v", args)
	}
	result.count--
	if result.count == 0 {
		delete(f.results, k)
	}

	stream := make(chan *logutilpb.Event)
	go func() {
		// Each line of the multi-line string "output" is streamed as console text.
		for _, line := range strings.Split(result.output, "\n") {
			stream <- &logutilpb.Event{
				Time:  logutil.TimeToProto(time.Now()),
				Level: logutilpb.Level_CONSOLE,
				File:  "fakevtctlclient",
				Line:  -1,
				Value: line,
			}
		}
		close(stream)
	}()

	return stream, func() error { return result.err }, nil
}
