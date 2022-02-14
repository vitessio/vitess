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
