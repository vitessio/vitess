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

// Package fakevtworkerclient contains a fake for the vtworkerclient interface.
package fakevtworkerclient

import (
	"context"

	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/vtctl/fakevtctlclient"
	"vitess.io/vitess/go/vt/worker/vtworkerclient"
)

// FakeVtworkerClient is a fake which implements the vtworkerclient interface.
// The fake can be used to return a specific result for a given command.
// If the command is not registered, an error will be thrown.
type FakeVtworkerClient struct {
	*fakevtctlclient.FakeLoggerEventStreamingClient
}

// NewFakeVtworkerClient creates a FakeVtworkerClient struct.
func NewFakeVtworkerClient() *FakeVtworkerClient {
	return &FakeVtworkerClient{fakevtctlclient.NewFakeLoggerEventStreamingClient()}
}

// FakeVtworkerClientFactory returns the current instance and stores the
// dialed server address in an outer struct.
func (f *FakeVtworkerClient) FakeVtworkerClientFactory(addr string) (vtworkerclient.Client, error) {
	return &perAddrFakeVtworkerClient{f, addr}, nil
}

// perAddrFakeVtworkerClient is a client instance which captures the server
// address which was dialed by the client.
type perAddrFakeVtworkerClient struct {
	*FakeVtworkerClient
	addr string
}

// ExecuteVtworkerCommand is part of the vtworkerclient interface.
func (c *perAddrFakeVtworkerClient) ExecuteVtworkerCommand(ctx context.Context, args []string) (logutil.EventStream, error) {
	return c.FakeLoggerEventStreamingClient.StreamResult(c.addr, args)
}

// Close is part of the vtworkerclient interface.
func (c *perAddrFakeVtworkerClient) Close() {}
