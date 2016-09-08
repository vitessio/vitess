// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fakevtctlclient

import (
	"fmt"
	"io"
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
	// addr optionally specifies which server address is expected from the client.
	addr string
}

func (r1 result) Equals(r2 result) bool {
	return r1.output == r2.output &&
		((r1.err == nil && r2.err == nil) ||
			(r1.err != nil && r2.err != nil && r1.err.Error() == r2.err.Error()))
}

// RegisterResult registers for a given command (args) the result which the fake should return.
// Once the result was returned, it will be automatically deregistered.
func (f *FakeLoggerEventStreamingClient) RegisterResult(args []string, output string, err error) error {
	return f.RegisterResultForAddr("" /* addr */, args, output, err)
}

// RegisterResultForAddr is identical to RegisterResult but also expects that
// the client did dial "addr" as server address.
func (f *FakeLoggerEventStreamingClient) RegisterResultForAddr(addr string, args []string, output string, err error) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	k := generateKey(args)
	v := result{output, err, 1, addr}
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

type streamResultAdapter struct {
	lines []string
	index int
	err   error
}

func (s *streamResultAdapter) Recv() (*logutilpb.Event, error) {
	if s.index < len(s.lines) {
		result := &logutilpb.Event{
			Time:  logutil.TimeToProto(time.Now()),
			Level: logutilpb.Level_CONSOLE,
			File:  "fakevtctlclient",
			Line:  -1,
			Value: s.lines[s.index],
		}
		s.index++
		return result, nil
	}
	if s.err == nil {
		return nil, io.EOF
	}
	return nil, s.err
}

// StreamResult returns an EventStream which streams back a registered result as logging events.
// "addr" is the server address which the client dialed and may be empty.
func (f *FakeLoggerEventStreamingClient) StreamResult(addr string, args []string) (logutil.EventStream, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	k := generateKey(args)
	result, ok := f.results[k]
	if !ok {
		return nil, fmt.Errorf("no response was registered for args: %v", args)
	}
	if result.addr != "" && addr != result.addr {
		return nil, fmt.Errorf("client sent request to wrong server address. got: %v want: %v", addr, result.addr)
	}
	result.count--
	if result.count == 0 {
		delete(f.results, k)
	}

	return &streamResultAdapter{
		lines: strings.Split(result.output, "\n"),
		index: 0,
		err:   result.err,
	}, nil
}
