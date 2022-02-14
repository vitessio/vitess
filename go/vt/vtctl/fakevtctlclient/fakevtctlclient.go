// Package fakevtctlclient contains a fake for the vtctlclient interface.
package fakevtctlclient

import (
	"time"

	"context"

	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/vtctl/vtctlclient"
)

// FakeVtctlClient is a fake which implements the vtctlclient interface.
// The fake can be used to return a specific result for a given command.
// If the command is not registered, an error will be thrown.
type FakeVtctlClient struct {
	*FakeLoggerEventStreamingClient
}

// NewFakeVtctlClient creates a FakeVtctlClient struct.
func NewFakeVtctlClient() *FakeVtctlClient {
	return &FakeVtctlClient{NewFakeLoggerEventStreamingClient()}
}

// FakeVtctlClientFactory always returns the current instance.
func (f *FakeVtctlClient) FakeVtctlClientFactory(addr string) (vtctlclient.VtctlClient, error) {
	return f, nil
}

// ExecuteVtctlCommand is part of the vtctlclient interface.
func (f *FakeVtctlClient) ExecuteVtctlCommand(ctx context.Context, args []string, actionTimeout time.Duration) (logutil.EventStream, error) {
	return f.FakeLoggerEventStreamingClient.StreamResult("" /* addr */, args)
}

// Close is part of the vtctlclient interface.
func (f *FakeVtctlClient) Close() {}
