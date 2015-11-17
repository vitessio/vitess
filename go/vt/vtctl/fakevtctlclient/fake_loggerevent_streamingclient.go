// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fakevtctlclient

import (
	"fmt"
	"strings"
	"time"

	"github.com/youtube/vitess/go/vt/logutil"

	logutilpb "github.com/youtube/vitess/go/vt/proto/logutil"
)

// FakeLoggerEventStreamingClient is the base for the fakes for the vtctlclient and vtworkerclient.
// It allows to register a (multi-)line string for a given command and return the result as channel which streams it back.
type FakeLoggerEventStreamingClient struct {
	results map[argsKey]result
}

// NewFakeLoggerEventStreamingClient creates a new fake.
func NewFakeLoggerEventStreamingClient() FakeLoggerEventStreamingClient {
	return FakeLoggerEventStreamingClient{make(map[argsKey]result)}
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

// RegisterResult registers for a given command (args) the result which the fake should return.
func (f *FakeLoggerEventStreamingClient) RegisterResult(args []string, output string, err error) error {
	argsKey := fromSlice(args)
	if result, ok := f.results[argsKey]; ok {
		return fmt.Errorf("Result is already registered as: %v", result)
	}
	f.results[argsKey] = result{output, err}
	return nil
}

// StreamResult returns a channel which streams back a registered result as logging events.
func (f *FakeLoggerEventStreamingClient) StreamResult(args []string) (<-chan *logutilpb.Event, func() error, error) {
	result, ok := f.results[fromSlice(args)]
	if !ok {
		return nil, nil, fmt.Errorf("No response was registered for args: %v", args)
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
